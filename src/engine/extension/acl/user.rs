//! ACL user management (L2 layer).
//! Provides `AclUser`, `AclSelector`, and `acl_users()` global registry.

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::OnceLock;

use crate::engine::extension::acl::command::{is_valid_sha256_hex, sha256_hex, AclCommand};
use crate::engine::extension::acl::permissions::{selector_db_denial, AclSelector};

/// Key pattern permission flags, mirroring C `ACL_READ_PERMISSION` /
/// `ACL_WRITE_PERMISSION`. `~pat` grants both; `%R~`/`%W~` grant one.
pub(crate) const ACL_READ_PERMISSION: u8 = 1;
pub(crate) const ACL_WRITE_PERMISSION: u8 = 2;
pub(crate) const ACL_ALL_PERMISSION: u8 = ACL_READ_PERMISSION | ACL_WRITE_PERMISSION;

/// A command ACL denial, mirroring C's `ACLCheckAllUserCommandPerm` result.
/// Error precedence is `Db < Command < Key` (channels are checked separately by
/// callers). `arg_index` on `Db` is the 0-based `cmd_args` position naming the
/// offending database, or `None` when it is the connection's current db (whose
/// object name is the command name, as in C).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AclDenial {
    Db { dbid: i64, arg_index: Option<usize> },
    Command,
    Key(usize),
}

/// Which databases a command touches, for ACL `db=` enforcement. Mirrors C's
/// `ACLSelectorCheckCmd` database-check preamble, which derives the set of
/// dbids to validate from `cmd->get_dbid_args`, the `CMD_ALL_DBS` flag, or
/// `shouldRestrictCmd`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DbRequirement {
    /// Command is db-agnostic (keyless and not in KEYSPACE/READ/WRITE): no db
    /// check applies. Mirrors C leaving `dbid` untouched.
    None,
    /// Command must be able to access every database (`CMD_ALL_DBS`, e.g.
    /// FLUSHALL). The paired value is the total number of databases so the
    /// caller can expand `0..num_dbs`.
    All,
    /// One concrete database without a heap allocation. This is the common
    /// path for key-bearing commands executed in the connection's current DB.
    Single(i64, Option<usize>),
    /// Command touches a specific set of databases (SELECT/SWAPDB/MOVE/COPY
    /// db args, or the current db for `shouldRestrictCmd` commands). Each entry
    /// is `(dbid, argv_index)` where `argv_index` is the 0-based position in
    /// `cmd_args` naming that db (used for ACL log/error reporting), or `None`
    /// when it is the connection's current db.
    Ids(Vec<(i64, Option<usize>)>),
}

/// Parse the `DB` id arguments of SELECT/SWAPDB/MOVE/COPY, mirroring C's
/// `selectDbIdArgs`/`swapdbDbIdArgs`/`moveDbIdArgs`/`copyDbIdArgs`. Returns the
/// `(dbid, cmd_args_index)` pairs to validate, or `None` when the command is
/// not one of these, or when any named db fails to parse / is out of range —
/// in which case C skips the ACL db check and lets the command's own handler
/// raise the range/syntax error. `cmd_args` excludes the command name, so a C
/// `argv[i]` maps to `cmd_args[i - 1]`.
fn db_id_args(cmd_name: &str, cmd_args: &[Bytes], num_dbs: usize) -> Option<Vec<(i64, usize)>> {
    let parse_db = |idx: usize| -> Option<i64> {
        let raw = cmd_args.get(idx)?;
        let dbid = std::str::from_utf8(raw).ok()?.trim().parse::<i64>().ok()?;
        if dbid < 0 || dbid >= num_dbs as i64 {
            return None;
        }
        Some(dbid)
    };
    if cmd_name.eq_ignore_ascii_case("select") {
        // argv[1] == cmd_args[0]
        let dbid = std::str::from_utf8(cmd_args.first()?)
            .ok()?
            .parse::<u32>()
            .ok()?;
        if usize::try_from(dbid).ok()? >= num_dbs {
            return None;
        }
        Some(vec![(i64::from(dbid), 0)])
    } else if cmd_name.eq_ignore_ascii_case("swapdb") {
        // argv[1], argv[2] == cmd_args[0], cmd_args[1]
        Some(vec![(parse_db(0)?, 0), (parse_db(1)?, 1)])
    } else if cmd_name.eq_ignore_ascii_case("move") {
        // argv[2] == cmd_args[1]
        Some(vec![(parse_db(1)?, 1)])
    } else if cmd_name.eq_ignore_ascii_case("copy") {
        // COPY src dst [DB n] [REPLACE]; scan argv[3..] == cmd_args[2..].
        // DB may appear more than once; validate every occurrence.
        if cmd_args.len() < 4 {
            return None;
        }
        let mut positions = Vec::new();
        let mut j = 2usize;
        while j < cmd_args.len() {
            let tok = std::str::from_utf8(&cmd_args[j]).ok()?;
            if tok.eq_ignore_ascii_case("replace") {
                j += 1;
            } else if tok.eq_ignore_ascii_case("db") && j + 1 < cmd_args.len() {
                let dbid = parse_db(j + 1)?;
                positions.push((dbid, j + 1));
                j += 2;
            } else {
                return None;
            }
        }
        if positions.is_empty() {
            None
        } else {
            Some(positions)
        }
    } else {
        None
    }
}

/// Compute which databases a command touches for ACL `db=` enforcement,
/// mirroring the preamble of C `ACLSelectorCheckCmd`.
/// - `is_all_dbs`: command carries `CMD_ALL_DBS` (e.g. FLUSHALL).
/// - `should_restrict`: command is subject to db restriction because it is in
///   the KEYSPACE/READ/WRITE ACL categories or declares keys (C `shouldRestrictCmd`).
pub(crate) fn compute_db_requirement(
    cmd_name: &str,
    cmd_args: &[Bytes],
    is_all_dbs: bool,
    should_restrict: bool,
    current_dbid: i64,
    num_dbs: usize,
) -> DbRequirement {
    if let Some(positions) = db_id_args(cmd_name, cmd_args, num_dbs) {
        if cmd_name.eq_ignore_ascii_case("move") || cmd_name.eq_ignore_ascii_case("copy") {
            let mut ids: Vec<_> = positions
                .into_iter()
                .map(|(db, idx)| (db, Some(idx)))
                .collect();
            ids.push((current_dbid, None));
            return DbRequirement::Ids(ids);
        }
        if positions.len() == 1 {
            let (dbid, arg_index) = positions[0];
            return DbRequirement::Single(dbid, Some(arg_index));
        }
        return DbRequirement::Ids(
            positions
                .into_iter()
                .map(|(db, idx)| (db, Some(idx)))
                .collect(),
        );
    }
    if is_all_dbs {
        return DbRequirement::All;
    }
    if should_restrict {
        return DbRequirement::Single(current_dbid, None);
    }
    DbRequirement::None
}

/// Merge whitespace-separated ACL tokens into rule strings, coalescing
/// parenthesized selectors (`(`...`)`) that span multiple tokens into a single
/// `(...)` rule. Mirrors C `ACLMergeSelectorArguments`. Returns `Err(())` when a
/// `(` is never closed. Shared by ACL file loading (startup and `ACL LOAD`).
pub(crate) fn merge_acl_tokens(tokens: &[&str]) -> Result<Vec<String>, ()> {
    let mut merged: Vec<String> = Vec::new();
    let mut open = false;
    let mut selector = String::new();
    for tok in tokens {
        if !open && tok.starts_with('(') && !tok.ends_with(')') {
            selector = (*tok).to_string();
            open = true;
            continue;
        }
        if open {
            selector.push(' ');
            selector.push_str(tok);
            if tok.ends_with(')') {
                open = false;
                merged.push(std::mem::take(&mut selector));
            }
            continue;
        }
        merged.push((*tok).to_string());
    }
    if open {
        return Err(());
    }
    Ok(merged)
}

fn parse_selector_rules(opset: &str) -> Option<Vec<String>> {
    let opset = opset.trim();
    if opset.len() < 2 || !opset.starts_with('(') || !opset.ends_with(')') {
        return None;
    }
    let inner = &opset[1..opset.len() - 1];
    let rules: Vec<String> = inner.split_whitespace().map(|s| s.to_string()).collect();
    Some(rules)
}

/// ACL user information.
#[derive(Clone)]
pub(crate) struct AclUser {
    pub(crate) flags: String,
    pub(crate) passwords: Vec<String>,
    pub(crate) nopass: bool,
    /// Root selector (always present, created by reset).
    pub(crate) root_selector: AclSelector,
    /// Additional selectors defined via `(...)` syntax.
    pub(crate) selectors: Vec<AclSelector>,
}

impl AclUser {
    pub(crate) fn references_command(&self, command: &str) -> bool {
        let command = command.to_ascii_lowercase();
        let prefix = format!("{}|", command);
        self.root_selector
            .categories
            .iter()
            .chain(
                self.selectors
                    .iter()
                    .flat_map(|selector| selector.categories.iter()),
            )
            .any(|rule| {
                let rule = rule.get(1..).unwrap_or_default();
                rule.eq_ignore_ascii_case(&command) || rule.starts_with(&prefix)
            })
    }

    pub(crate) fn can_execute_command_with_args(&self, command: AclCommand<'_>) -> bool {
        self.root_selector.can_execute_command_with_args(command)
            || self
                .selectors
                .iter()
                .any(|selector| selector.can_execute_command_with_args(command))
    }

    pub(crate) fn can_access_key_bits(&self, key: &[u8], required: u8) -> bool {
        self.root_selector.can_access_key_bits(key, required)
            || self
                .selectors
                .iter()
                .any(|selector| selector.can_access_key_bits(key, required))
    }

    pub(crate) fn new_disabled() -> Self {
        let mut root = AclSelector::new();
        let _ = root.apply_rule("-@all");
        Self {
            flags: "off".to_string(),
            passwords: vec![],
            nopass: false,
            root_selector: root,
            selectors: vec![],
        }
    }

    pub(crate) fn is_on(&self) -> bool {
        self.flags == "on"
    }

    fn has_nopass(&self) -> bool {
        self.nopass
    }

    pub(crate) fn check_password(&self, password: &[u8]) -> bool {
        if self.has_nopass() {
            return true;
        }
        let hashed = sha256_hex(password);
        self.passwords.iter().any(|p| p == &hashed)
    }

    pub(crate) fn can_access_db(&self, dbid: i64) -> bool {
        // In Valkey's model, if any selector grants db access, it's allowed.
        // The root selector is always checked.
        if self.root_selector.can_access_db(dbid) {
            return true;
        }
        self.selectors.iter().any(|s| s.can_access_db(dbid))
    }

    pub(crate) fn apply_rule(&mut self, rule: &str) -> Result<(), &'static str> {
        self.apply_rule_with_pubsub_default(rule, false)
    }

    pub(crate) fn apply_rule_with_pubsub_default(
        &mut self,
        rule: &str,
        default_allchannels: bool,
    ) -> Result<(), &'static str> {
        match rule {
            _ if rule.eq_ignore_ascii_case("on") => {
                self.flags = "on".to_string();
                Ok(())
            }
            _ if rule.eq_ignore_ascii_case("off") => {
                self.flags = "off".to_string();
                Ok(())
            }
            "nopass" => {
                self.nopass = true;
                self.passwords.clear();
                Ok(())
            }
            "resetpass" => {
                self.passwords.clear();
                self.nopass = false;
                Ok(())
            }
            _ if rule.starts_with('>') || rule.starts_with('#') => {
                let raw = &rule[1..];
                self.nopass = false;
                if rule.starts_with('>') {
                    let hash = sha256_hex(raw.as_bytes());
                    self.passwords.push(hash);
                } else if is_valid_sha256_hex(raw) {
                    self.passwords.push(raw.to_string());
                }
                Ok(())
            }
            _ if rule.starts_with('<') || rule.starts_with('!') => {
                let raw = &rule[1..];
                if rule.starts_with('<') {
                    let hash = sha256_hex(raw.as_bytes());
                    self.passwords.retain(|p| p != &hash);
                } else if is_valid_sha256_hex(raw) {
                    self.passwords.retain(|p| p != raw);
                }
                Ok(())
            }
            _ if rule == "clearselectors" => {
                self.selectors.clear();
                Ok(())
            }
            _ if rule.starts_with('(') && rule.ends_with(')') => {
                crate::server_log!(debug, "ACL: parsing selector rule: {}", rule);
                let rules = parse_selector_rules(rule).unwrap_or_default();
                crate::server_log!(debug, "ACL: parsed rules: {:?}", rules);
                // An empty selector `()` still yields one selector with no
                // permissions (-@all), matching C aclCreateSelectorFromOpSet.
                let mut selector = AclSelector::new();
                let _ = selector.apply_rule("-@all");
                if default_allchannels {
                    let _ = selector.apply_rule("allchannels");
                }
                for r in &rules {
                    crate::server_log!(debug, "ACL: applying selector rule: {}", r);
                    selector.apply_rule(r)?;
                }
                crate::server_log!(
                    debug,
                    "ACL: selector created, categories: {:?}, keys: {}",
                    selector.categories,
                    selector.keys_str()
                );
                self.selectors.push(selector);
                Ok(())
            }
            _ if rule == "reset" => {
                self.passwords.clear();
                self.nopass = false;
                self.root_selector = AclSelector::new();
                let _ = self.root_selector.apply_rule("-@all");
                self.selectors.clear();
                self.flags = "off".to_string();
                Ok(())
            }
            // All other rules apply to root selector
            _ => self.root_selector.apply_rule(rule),
        }
    }

    /// Generate ACL rule string for saving to ACL file (ACL SAVE).
    pub(crate) fn to_acl_rule_string(&self, username: &str) -> String {
        let mut parts = Vec::new();
        parts.push(self.flags.clone());
        if self.nopass {
            parts.push("nopass".to_string());
        }
        for pw in &self.passwords {
            parts.push(format!("#{}", pw));
        }
        for pat in &self.root_selector.patterns {
            parts.push(pat.to_rule_string());
        }
        for pat in &self.root_selector.pubsub_patterns {
            if pat == "*" {
                parts.push("allchannels".to_string());
            } else {
                parts.push(format!("&{}", pat));
            }
        }
        for cat in &self.root_selector.categories {
            parts.push(cat.clone());
        }
        if let Some(dbs) = &self.root_selector.allowed_dbs {
            if !dbs.is_empty() {
                let db_str = dbs
                    .iter()
                    .map(|d| format!("db={}", d))
                    .collect::<Vec<_>>()
                    .join(" ");
                parts.push(db_str);
            }
        }
        // Additional selectors: serialize as (...)
        for s in &self.selectors {
            let mut sel_parts = Vec::new();
            for pat in &s.patterns {
                sel_parts.push(pat.to_rule_string());
            }
            for pat in &s.pubsub_patterns {
                sel_parts.push(format!("&{}", pat));
            }
            for cat in &s.categories {
                sel_parts.push(cat.clone());
            }
            if let Some(dbs) = &s.allowed_dbs {
                if !dbs.is_empty() {
                    let db_str = dbs
                        .iter()
                        .map(|d| format!("db={}", d))
                        .collect::<Vec<_>>()
                        .join(" ");
                    sel_parts.push(db_str);
                }
            }
            parts.push(format!("({})", sel_parts.join(" ")));
        }
        format!("{} {}", username, parts.join(" "))
    }

    /// Describe the user as an ACL rule string in C `ACLDescribeUser` order:
    /// flags, passwords, root selector, then each additional selector wrapped
    /// in `(...)`. Used by ACL LIST so the output round-trips through the
    /// parser and exposes additional selectors.
    pub(crate) fn describe(&self, username: &str) -> String {
        let mut parts: Vec<String> = Vec::new();
        parts.push(self.flags.clone());
        if self.nopass {
            parts.push("nopass".to_string());
        }
        for pw in &self.passwords {
            parts.push(format!("#{}", pw));
        }
        parts.push(self.root_selector.describe());
        for s in &self.selectors {
            parts.push(format!("({})", s.describe()));
        }
        format!("{} {}", username, parts.join(" "))
    }

    /// Check command permission with args, honoring the command's database
    /// requirement. Mirrors C `ACLSelectorCheckCmd`: a selector must grant both
    /// the required database access and the command.
    pub(crate) fn can_execute_command_with_args_db(
        &self,
        command: AclCommand<'_>,
        db_req: &DbRequirement,
        num_dbs: usize,
    ) -> bool {
        let allows = |s: &AclSelector| {
            s.satisfies_db_requirement(db_req, num_dbs) && s.can_execute_command_with_args(command)
        };
        allows(&self.root_selector) || self.selectors.iter().any(allows)
    }

    /// Check command, database, and static key permissions using one selector.
    /// ACL selectors are permission bundles. Command access from one selector
    /// must not be combined with key access from another selector.
    pub(crate) fn can_execute_command_with_args_and_keys_db(
        &self,
        command: AclCommand<'_>,
        db_req: &DbRequirement,
        num_dbs: usize,
        keys: &[(&[u8], bool)],
    ) -> bool {
        let selector_allows = |selector: &AclSelector| {
            selector.satisfies_db_requirement(db_req, num_dbs)
                && selector.can_execute_command_with_args(command)
                && keys
                    .iter()
                    .all(|(key, is_write)| selector.can_access_key(key, *is_write))
        };
        selector_allows(&self.root_selector) || self.selectors.iter().any(selector_allows)
    }

    /// Whether the user can run `cmd_name` with a selector that *also* grants
    /// the given permission bits to the entire keyspace. Mirrors C
    /// `ACLUserCheckCmdWithUnrestrictedKeyAccess`: the command permission and the
    /// full-keyspace access must come from the *same* selector. Used by SORT to
    /// authorize implicit BY/GET pattern key lookups (which require full
    /// keyspace READ access), matching `~*` / `%R~*`.
    pub(crate) fn has_cmd_with_unrestricted_key_access(
        &self,
        command: AclCommand<'_>,
        current_dbid: i64,
        required: u8,
    ) -> bool {
        let selector_allows = |selector: &AclSelector| {
            selector.can_access_db(current_dbid)
                && selector.can_execute_command_with_args(command)
                && selector.has_unrestricted_key_access(required)
        };
        selector_allows(&self.root_selector) || self.selectors.iter().any(selector_allows)
    }

    /// Check command + database + key permissions across all selectors,
    /// returning the highest-precedence denial (or `None` if allowed), mirroring
    /// C `ACLCheckAllUserCommandPerm`. Precedence: `Db < Command < Key`; within
    /// the same kind the highest offending index wins.
    /// `keys` carry explicit required permission bits. Channel checks are done
    /// separately by callers (as in C, where channels are a distinct pass).
    pub(crate) fn check_command_perm(
        &self,
        command: AclCommand<'_>,
        db_req: &DbRequirement,
        num_dbs: usize,
        keys: &[(&[u8], u8)],
    ) -> Option<AclDenial> {
        self.check_command_perm_iter(command, db_req, num_dbs, keys.iter().copied())
    }

    pub(crate) fn check_command_perm_iter<'a, I>(
        &self,
        command: AclCommand<'_>,
        db_req: &DbRequirement,
        num_dbs: usize,
        keys: I,
    ) -> Option<AclDenial>
    where
        I: Iterator<Item = (&'a [u8], u8)> + Clone,
    {
        // C initializes `relevant_error = ACL_DENIED_DB` (the lowest priority),
        // so a command allowed by no selector defaults to a DB denial only when
        // no selector even clears the command bit.
        let mut best: Option<AclDenial> = None;
        // Rank for precedence comparison (higher = reported): Db=1, Command=2, Key=3.
        let rank = |d: &AclDenial| match d {
            AclDenial::Db { .. } => 1,
            AclDenial::Command => 2,
            AclDenial::Key(_) => 3,
        };
        let idx_of = |d: &AclDenial| match d {
            AclDenial::Db { arg_index, .. } => arg_index.map_or(0, |i| i + 1),
            AclDenial::Command => 0,
            AclDenial::Key(i) => *i,
        };
        let mut consider = |cand: AclDenial| {
            let take = match &best {
                None => true,
                Some(cur) => {
                    rank(&cand) > rank(cur)
                        || (rank(&cand) == rank(cur) && idx_of(&cand) > idx_of(cur))
                }
            };
            if take {
                best = Some(cand);
            }
        };

        for selector in std::iter::once(&self.root_selector).chain(self.selectors.iter()) {
            // 1) Database check (C runs this first, before the command bit).
            if let Some(denial) = selector_db_denial(selector, db_req, num_dbs) {
                consider(denial);
                continue;
            }
            // 2) Command check.
            if !selector.can_execute_command_with_args(command) {
                consider(AclDenial::Command);
                continue;
            }
            // 3) Key check.
            let mut key_denied: Option<usize> = None;
            for (i, (key, required)) in keys.clone().enumerate() {
                if !selector.can_access_key_bits(key, required) {
                    key_denied = Some(i);
                    break;
                }
            }
            {
                let i = key_denied?;
                consider(AclDenial::Key(i))
            }
        }
        best
    }

    /// Check key access within a specific database context.
    pub(crate) fn can_access_key_db(&self, key: &[u8], is_write: bool, dbid: i64) -> bool {
        if self.root_selector.can_access_db(dbid)
            && self.root_selector.can_access_key(key, is_write)
        {
            return true;
        }
        self.selectors
            .iter()
            .any(|s| s.can_access_db(dbid) && s.can_access_key(key, is_write))
    }

    pub(crate) fn can_access_channel(&self, channel: &[u8]) -> bool {
        if self.root_selector.can_access_channel(channel) {
            return true;
        }
        self.selectors.iter().any(|s| s.can_access_channel(channel))
    }

    pub(crate) fn can_access_channel_for_command(
        &self,
        cmd_name: &str,
        cmd_args: &[Bytes],
    ) -> bool {
        let cmd = cmd_name.to_ascii_lowercase();
        let check = |s: &AclSelector| -> bool {
            match cmd.as_str() {
                "publish" | "spublish" => {
                    cmd_args.first().is_none_or(|ch| s.can_access_channel(ch))
                }
                "subscribe" | "ssubscribe" => cmd_args.iter().all(|ch| s.can_access_channel(ch)),
                "psubscribe" => cmd_args.iter().all(|pat| {
                    s.pubsub_patterns
                        .iter()
                        .any(|allowed| allowed == "*" || allowed.as_bytes() == pat.as_ref())
                }),
                _ => true,
            }
        };
        if check(&self.root_selector) {
            return true;
        }
        self.selectors.iter().any(check)
    }

    pub(crate) fn has_denied_subscriptions(
        &self,
        pubsub_channels: &[Bytes],
        pubsub_patterns: &[Bytes],
        pubsub_shard_channels: &[Bytes],
    ) -> bool {
        let has_allchannels = self.root_selector.pubsub_patterns.iter().any(|p| p == "*")
            || self
                .selectors
                .iter()
                .any(|s| s.pubsub_patterns.iter().any(|p| p == "*"));
        for ch in pubsub_channels {
            if !self.can_access_channel(ch) {
                return true;
            }
        }
        for pat in pubsub_patterns {
            if !has_allchannels {
                let allowed = self
                    .root_selector
                    .pubsub_patterns
                    .iter()
                    .any(|a| a.as_bytes() == pat.as_ref())
                    || self.selectors.iter().any(|s| {
                        s.pubsub_patterns
                            .iter()
                            .any(|a| a.as_bytes() == pat.as_ref())
                    });
                if !allowed {
                    return true;
                }
            }
        }
        for ch in pubsub_shard_channels {
            if !self.can_access_channel(ch) {
                return true;
            }
        }
        false
    }

    /// Get the categories string for the root selector (used by ACL GETUSER).
    pub(crate) fn root_categories_str(&self) -> String {
        self.root_selector.categories_str()
    }

    /// Get the root selector's key-pattern rule string (used by ACL GETUSER).
    pub(crate) fn root_keys_str(&self) -> String {
        self.root_selector.keys_str()
    }

    /// Get the root selector's channels rule string (used by ACL GETUSER).
    pub(crate) fn root_channels_str(&self) -> String {
        self.root_selector.channels_str()
    }

    /// Get the root selector's databases rule string (used by ACL GETUSER).
    pub(crate) fn root_databases_str(&self) -> String {
        self.root_selector.databases_str()
    }

    /// Get the selectors for ACL GETUSER display.
    pub(crate) fn get_selectors(&self) -> &[AclSelector] {
        &self.selectors
    }
}

/// Global ACL user registry.
pub(crate) fn acl_users() -> &'static Mutex<HashMap<String, AclUser>> {
    static USERS: OnceLock<Mutex<HashMap<String, AclUser>>> = OnceLock::new();
    USERS.get_or_init(|| {
        let mut m = HashMap::new();
        let mut default_user = AclUser {
            flags: "on".to_string(),
            passwords: vec![],
            nopass: true,
            root_selector: AclSelector::new(),
            selectors: vec![],
        };
        let _ = default_user.root_selector.apply_rule("+@all");
        let _ = default_user.root_selector.apply_rule("~*");
        let _ = default_user.root_selector.apply_rule("&*");
        m.insert("default".to_string(), default_user);
        Mutex::new(m)
    })
}

pub(crate) fn default_user_has_nopass() -> bool {
    acl_users()
        .lock()
        .unwrap()
        .get("default")
        .is_some_and(|user| user.nopass)
}

pub(crate) fn set_default_user_requirepass(password: Option<&[u8]>) {
    let mut users = acl_users().lock().unwrap();
    let Some(default_user) = users.get_mut("default") else {
        return;
    };
    apply_requirepass(default_user, password);
}

fn apply_requirepass(user: &mut AclUser, password: Option<&[u8]>) {
    match password {
        Some(password) => {
            user.nopass = false;
            user.passwords = vec![sha256_hex(password)];
        }
        None => {
            user.nopass = true;
            user.passwords.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{apply_requirepass, compute_db_requirement, db_id_args, AclUser, DbRequirement};
    use bytes::Bytes;

    #[test]
    fn select_database_acl_matches_handler_index_validity() {
        for (raw, expected_db) in [
            (b"-0".as_slice(), None),
            (b" 0", None),
            (b"0 ", None),
            (b"-1", None),
            (b"16", None),
            (b"4294967295", None),
            (b"4294967296", None),
            (b"9223372036854775807", None),
            (b"", None),
            (b"\xff", None),
            (b"0", Some(0)),
            (b"1", Some(1)),
            (b"15", Some(15)),
        ] {
            let args = [Bytes::from_static(raw)];
            assert_eq!(
                db_id_args("select", &args, 16),
                expected_db.map(|db_id| vec![(db_id, 0)]),
                "SELECT {}",
                String::from_utf8_lossy(raw)
            );
            let requirement = expected_db.map_or(DbRequirement::None, |db_id| {
                DbRequirement::Single(db_id, Some(0))
            });
            assert_eq!(
                compute_db_requirement("select", &args, false, false, 1, 16),
                requirement
            );
        }
        assert_eq!(db_id_args("select", &[], 16), None);
        assert_eq!(db_id_args("select", &[Bytes::from_static(b"0")], 0), None);
    }

    #[test]
    fn requirepass_replaces_default_user_password_and_restores_nopass() {
        let mut user = AclUser::new_disabled();
        apply_requirepass(&mut user, Some(b"secret"));
        assert!(!user.nopass);
        assert!(!user.check_password(b"wrong"));
        assert!(user.check_password(b"secret"));

        apply_requirepass(&mut user, None);
        assert!(user.nopass);
        assert!(user.check_password(b"anything"));
    }
}
