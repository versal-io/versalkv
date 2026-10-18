//! ACL selector permission model.
//! Keeps key/channel/db/command selector rules separate from user lifecycle and
//! the global ACL registry.

use crate::engine::extension::acl::command::AclCommand;

use super::user::{
    AclDenial, DbRequirement, ACL_ALL_PERMISSION, ACL_READ_PERMISSION, ACL_WRITE_PERMISSION,
};

/// A key pattern with its read/write permission flags. Mirrors C `keyPattern`,
/// keeping key patterns in one ordered list so serialization preserves
/// insertion order and `%R~foo` + `%W~foo` collapses into a single `~foo`.
#[derive(Clone)]
pub(crate) struct KeyPattern {
    pub(crate) flags: u8,
    pub(crate) pattern: String,
}

impl KeyPattern {
    /// Render as `~pat` / `%R~pat` / `%W~pat`, matching C `sdsCatPatternString`.
    pub(crate) fn to_rule_string(&self) -> String {
        match self.flags {
            ACL_ALL_PERMISSION => format!("~{}", self.pattern),
            ACL_READ_PERMISSION => format!("%R~{}", self.pattern),
            ACL_WRITE_PERMISSION => format!("%W~{}", self.pattern),
            _ => format!("~{}", self.pattern),
        }
    }
}

/// ACL permission selector.
/// Each user has a root selector and optionally additional selectors defined via `(...)` syntax.
#[derive(Clone)]
pub(crate) struct AclSelector {
    /// Key patterns in insertion order, each carrying read/write flags.
    pub(crate) patterns: Vec<KeyPattern>,
    pub(crate) pubsub_patterns: Vec<String>,
    pub(crate) categories: Vec<String>,
    pub(crate) allowed_dbs: Option<Vec<i64>>,
}

impl AclSelector {
    pub(crate) fn new() -> Self {
        Self {
            patterns: vec![],
            pubsub_patterns: vec![],
            categories: vec![],
            allowed_dbs: None,
        }
    }

    /// Add a key pattern, OR-ing flags into an existing identical pattern so
    /// `%R~foo` followed by `%W~foo` becomes a single `~foo`. Mirrors C
    /// `ACLSetSelector` key handling.
    fn add_key_pattern(&mut self, flags: u8, pattern: &str) {
        if let Some(existing) = self.patterns.iter_mut().find(|p| p.pattern == pattern) {
            existing.flags |= flags;
        } else {
            self.patterns.push(KeyPattern {
                flags,
                pattern: pattern.to_string(),
            });
        }
    }

    pub(crate) fn apply_rule(&mut self, rule: &str) -> Result<(), &'static str> {
        match rule {
            _ if rule.starts_with("+@") => {
                let cat = &rule[2..];
                let cat_lower = cat.to_ascii_lowercase();
                let cat_str = format!("+@{}", cat_lower);
                if cat_lower == "all" {
                    // +@all absorbs everything, then pushes itself.
                    self.categories.clear();
                } else {
                    self.categories.retain(|c| {
                        if c.starts_with("+@") || c.starts_with("-@") {
                            c[2..].to_ascii_lowercase() != cat_lower
                        } else {
                            true
                        }
                    });
                }
                self.categories.push(cat_str);
                Ok(())
            }
            _ if rule.starts_with("-@") => {
                let cat = &rule[2..];
                let cat_lower = cat.to_ascii_lowercase();
                let cat_str = format!("-@{}", cat_lower);
                if cat_lower == "all" {
                    // -@all absorbs everything, then pushes itself.
                    self.categories.clear();
                } else {
                    self.categories.retain(|c| {
                        if c.starts_with("+@") || c.starts_with("-@") {
                            c[2..].to_ascii_lowercase() != cat_lower
                        } else {
                            true
                        }
                    });
                }
                self.categories.push(cat_str);
                Ok(())
            }
            _ if rule.starts_with('+') && rule.len() > 1 && !rule.starts_with("+@") => {
                let rest = &rule[1..];
                if let Some(pipe_pos) = rest.rfind('|') {
                    let cmd_part = &rest[..pipe_pos];
                    let sub_part = &rest[pipe_pos + 1..];
                    if cmd_part.contains('|') {
                        return Err("Unknown command or category name in ACL");
                    }
                    let cmd = cmd_part.to_ascii_lowercase();
                    let rule_str = format!("+{}|{}", cmd, sub_part.to_ascii_lowercase());
                    if self.categories.iter().any(|c| c == &format!("+{}", cmd)) {
                        return Ok(());
                    }
                    if !self.categories.contains(&rule_str) {
                        self.categories.push(rule_str);
                    }
                } else {
                    let cmd = rest.to_ascii_lowercase();
                    let rule_str = format!("+{}", cmd);
                    self.categories.retain(|c| c != &rule_str);
                    self.categories.push(rule_str);
                    self.categories.retain(|c| {
                        !c.starts_with(&format!("+{}|", cmd))
                            && !c.starts_with(&format!("-{}|", cmd))
                    });
                    self.categories.retain(|c| c != &format!("-{}", cmd));
                }
                Ok(())
            }
            _ if rule.starts_with('-') && rule.len() > 1 && !rule.starts_with("-@") => {
                let rest = &rule[1..];
                if let Some(pipe_pos) = rest.rfind('|') {
                    let cmd_part = &rest[..pipe_pos];
                    let sub_part = &rest[pipe_pos + 1..];
                    if cmd_part.contains('|') {
                        return Err("Unknown command or category name in ACL");
                    }
                    let cmd = cmd_part.to_ascii_lowercase();
                    let rule_str = format!("-{}|{}", cmd, sub_part.to_ascii_lowercase());
                    if self.categories.iter().any(|c| c == &format!("-{}", cmd)) {
                        return Ok(());
                    }
                    if !self.categories.contains(&rule_str) {
                        self.categories.push(rule_str);
                    }
                } else {
                    let cmd = rest.to_ascii_lowercase();
                    let rule_str = format!("-{}", cmd);
                    self.categories.retain(|c| c != &rule_str);
                    self.categories.push(rule_str);
                    self.categories.retain(|c| {
                        !c.starts_with(&format!("+{}|", cmd))
                            && !c.starts_with(&format!("-{}|", cmd))
                    });
                    self.categories.retain(|c| c != &format!("+{}", cmd));
                }
                Ok(())
            }
            _ if rule.starts_with('%') || rule.starts_with('~') => {
                // Parse optional %R/%W/%RW permission prefix, then the key
                // pattern. Mirrors C ACLSetSelector: a bare `%`, `%~`, or an
                // unrecognized permission char is a syntax error (CVE-2024-51741).
                let bytes = rule.as_bytes();
                let (mut is_read, mut is_write) = (false, false);
                let pattern = if bytes[0] == b'%' {
                    let mut offset = 1;
                    let mut perm_ok = true;
                    while offset < bytes.len() {
                        let c = bytes[offset];
                        if (c == b'R' || c == b'r') && !is_read {
                            is_read = true;
                        } else if (c == b'W' || c == b'w') && !is_write {
                            is_write = true;
                        } else if c == b'~' {
                            offset += 1;
                            break;
                        } else {
                            perm_ok = false;
                            break;
                        }
                        offset += 1;
                    }
                    if (!is_read && !is_write) || !perm_ok {
                        return Err("Syntax error");
                    }
                    &rule[offset..]
                } else {
                    is_read = true;
                    is_write = true;
                    &rule[1..]
                };
                if pattern.contains(char::is_whitespace) {
                    return Err("Syntax error");
                }
                let mut flags = 0u8;
                if is_read {
                    flags |= ACL_READ_PERMISSION;
                }
                if is_write {
                    flags |= ACL_WRITE_PERMISSION;
                }
                self.add_key_pattern(flags, pattern);
                Ok(())
            }
            _ if rule.starts_with('&') => {
                if self.pubsub_patterns.iter().any(|p| p == "*") {
                    return Err("Adding a pattern after the * pattern (or the 'allchannels' flag) is not valid and does not have any effect. Try 'resetchannels' to start with an empty list of channels");
                }
                let pat = &rule[1..];
                self.pubsub_patterns.push(pat.to_string());
                Ok(())
            }
            _ if rule == "resetkeys" => {
                self.patterns.clear();
                Ok(())
            }
            _ if rule == "resetchannels" => {
                self.pubsub_patterns.clear();
                Ok(())
            }
            _ if rule.starts_with("db=") => {
                let dbs_str = &rule[3..];
                // Reject empty list, leading or trailing commas.
                if dbs_str.is_empty() || dbs_str.starts_with(',') || dbs_str.ends_with(',') {
                    return Err("Syntax error");
                }
                let mut dbs = Vec::new();
                for part in dbs_str.split(',') {
                    // Reject empty tokens.
                    if part.is_empty() {
                        return Err("Syntax error");
                    }
                    match part.parse::<i64>() {
                        Ok(dbid) if (0..=i64::from(i32::MAX)).contains(&dbid) => dbs.push(dbid),
                        Ok(_) => return Err("The provided database ID is out of range"),
                        Err(_) => return Err("Syntax error"),
                    }
                }
                // Duplicates are silently collapsed and the list is kept sorted,
                // mirroring C's intset-backed `selector->dbs`.
                dbs.sort_unstable();
                dbs.dedup();
                self.allowed_dbs = Some(dbs);
                Ok(())
            }
            _ if rule == "allcommands" => {
                self.categories
                    .retain(|c| !c.starts_with("+@") && !c.starts_with("-@"));
                self.categories.push("+@all".to_string());
                Ok(())
            }
            _ if rule == "nocommands" => {
                self.categories
                    .retain(|c| !c.starts_with("+@") && !c.starts_with("-@"));
                self.categories.push("-@all".to_string());
                Ok(())
            }
            _ if rule == "alldbs" => {
                self.allowed_dbs = None;
                Ok(())
            }
            _ if rule == "resetdbs" => {
                self.allowed_dbs = Some(vec![]);
                Ok(())
            }
            _ if rule == "allkeys" => {
                self.patterns.clear();
                self.add_key_pattern(ACL_ALL_PERMISSION, "*");
                Ok(())
            }
            _ if rule == "allchannels" => {
                self.pubsub_patterns.clear();
                self.pubsub_patterns.push("*".to_string());
                Ok(())
            }
            // Deprecated no-op modifiers, accepted for backwards compatibility.
            _ if rule.eq_ignore_ascii_case("sanitize-payload")
                || rule.eq_ignore_ascii_case("skip-sanitize-payload") =>
            {
                Ok(())
            }
            _ => Err("Syntax error"),
        }
    }

    fn can_execute_command(&self, command: AclCommand<'_>) -> bool {
        let cmd_lower = command.name.to_ascii_lowercase();

        if self
            .categories
            .iter()
            .any(|c| c == &format!("+{}", cmd_lower))
        {
            return true;
        }

        let mut allowed = false;
        for rule in &self.categories {
            if rule == "+@all" {
                allowed = true;
            } else if rule == "-@all" {
                allowed = false;
            } else if let Some(category) = rule.strip_prefix("+@") {
                if command.belongs_to(category) {
                    allowed = true;
                }
            } else if let Some(category) = rule.strip_prefix("-@") {
                if command.belongs_to(category) {
                    allowed = false;
                }
            } else if rule == &format!("-{}", cmd_lower) {
                allowed = false;
            }
        }
        allowed
    }

    pub(crate) fn can_execute_command_with_args(&self, command: AclCommand<'_>) -> bool {
        let cmd_lower = command.name.to_ascii_lowercase();

        if let Some(first_arg) = command.args.first() {
            if let Ok(first_str) = std::str::from_utf8(first_arg) {
                let allow_sub = format!("+{}|{}", cmd_lower, first_str.to_ascii_lowercase());
                if self.categories.iter().any(|c| c == &allow_sub) {
                    return true;
                }
            }
        }

        if let Some(first_arg) = command.args.first() {
            if let Ok(first_str) = std::str::from_utf8(first_arg) {
                let deny_sub = format!("-{}|{}", cmd_lower, first_str.to_ascii_lowercase());
                if self.categories.iter().any(|c| c == &deny_sub) {
                    return false;
                }
            }
        }

        self.can_execute_command(command)
    }

    pub(crate) fn can_access_key(&self, key: &[u8], is_write: bool) -> bool {
        let required = if is_write {
            ACL_WRITE_PERMISSION
        } else {
            ACL_READ_PERMISSION
        };
        self.can_access_key_bits(key, required)
    }

    /// Check key access for an explicit set of required permission bits.
    /// Mirrors C `ACLSelectorCheckKey`: a pattern satisfies the request only if
    /// it grants every required bit (`(pattern->flags & key_flags) == key_flags`).
    /// `required == 0` means any matching pattern grants access.
    pub(crate) fn can_access_key_bits(&self, key: &[u8], required: u8) -> bool {
        self.patterns.iter().any(|pat| {
            (pat.flags & required) == required
                && crate::engine::distributed::pubsub::pattern_match(pat.pattern.as_bytes(), key)
        })
    }

    pub(crate) fn can_access_channel(&self, channel: &[u8]) -> bool {
        if self.pubsub_patterns.is_empty() {
            return false;
        }
        self.pubsub_patterns
            .iter()
            .any(|pat| crate::engine::distributed::pubsub::pattern_match(pat.as_bytes(), channel))
    }

    /// Whether this selector grants the given permission bits to *every* key in
    /// the keyspace. Mirrors C `ACLSelectorHasUnrestrictedKeyAccess`: only a `*`
    /// pattern (i.e. `~*`, or `%R~*`/`%W~*` when it carries the required bits)
    /// counts as full keyspace access; a narrower matching pattern does not.
    pub(crate) fn has_unrestricted_key_access(&self, required: u8) -> bool {
        self.patterns
            .iter()
            .any(|pat| (pat.flags & required) == required && pat.pattern == "*")
    }

    pub(crate) fn can_access_db(&self, dbid: i64) -> bool {
        match &self.allowed_dbs {
            None => true,
            Some(dbs) => dbs.contains(&dbid),
        }
    }

    /// Whether this selector satisfies a command's database requirement,
    /// mirroring the db-check preamble of C `ACLSelectorCheckCmd`.
    pub(crate) fn satisfies_db_requirement(&self, req: &DbRequirement, num_dbs: usize) -> bool {
        match req {
            DbRequirement::None => true,
            DbRequirement::All => (0..num_dbs as i64).all(|d| self.can_access_db(d)),
            DbRequirement::Single(dbid, _) => self.can_access_db(*dbid),
            DbRequirement::Ids(ids) => ids.iter().all(|(db, _)| self.can_access_db(*db)),
        }
    }

    pub(crate) fn categories_str(&self) -> String {
        let mut cmds: Vec<&str> = self.categories.iter().map(|c| c.as_str()).collect();
        let has_plus_all = cmds.contains(&"+@all");
        let has_minus_all = cmds.contains(&"-@all");
        if !has_plus_all && !has_minus_all {
            cmds.insert(0, "-@all");
        }
        cmds.join(" ")
    }

    /// Render the selector's key patterns as a space-separated rule string in
    /// insertion order (`~pat` / `%R~pat` / `%W~pat`), matching C
    /// `ACLDescribeSelector`. Used by ACL GETUSER / LIST / SAVE.
    pub(crate) fn keys_str(&self) -> String {
        self.patterns
            .iter()
            .map(KeyPattern::to_rule_string)
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Render the databases field for ACL GETUSER: `alldbs` when unrestricted,
    /// empty for `resetdbs` (empty list), otherwise `db=<list>`.
    pub(crate) fn databases_str(&self) -> String {
        match &self.allowed_dbs {
            None => "alldbs".to_string(),
            Some(dbs) if dbs.is_empty() => String::new(),
            Some(dbs) => format!(
                "db={}",
                dbs.iter()
                    .map(|d| d.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        }
    }

    /// Render channels field for ACL GETUSER: `&*` for allchannels, otherwise
    /// the `&pat` list.
    pub(crate) fn channels_str(&self) -> String {
        self.pubsub_patterns
            .iter()
            .map(|p| {
                if p == "*" {
                    "&*".to_string()
                } else {
                    format!("&{}", p)
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Render the full selector rule string (keys, channels, dbs, commands) in
    /// C `ACLDescribeSelector` order. Used by ACL LIST and ACL SAVE so the
    /// output round-trips through the parser.
    pub(crate) fn describe(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        // Keys.
        for pat in &self.patterns {
            parts.push(pat.to_rule_string());
        }
        // Channels: `&*` for allchannels, otherwise `resetchannels` + list.
        if self.pubsub_patterns.iter().any(|p| p == "*") {
            parts.push("&*".to_string());
        } else {
            parts.push("resetchannels".to_string());
            for pat in &self.pubsub_patterns {
                parts.push(format!("&{}", pat));
            }
        }
        // Databases: alldbs is the default and omitted; empty is `resetdbs`.
        match &self.allowed_dbs {
            None => {}
            Some(dbs) if dbs.is_empty() => parts.push("resetdbs".to_string()),
            Some(dbs) => parts.push(format!(
                "db={}",
                dbs.iter()
                    .map(|d| d.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            )),
        }
        // Command rules.
        parts.push(self.categories_str());
        parts.join(" ")
    }
}

/// Return the first database in `req` that `selector` cannot access, as an
/// `AclDenial::Db`, or `None` if the selector satisfies the requirement.
/// Mirrors the per-selector db loop of C `ACLSelectorCheckCmd`.
pub(crate) fn selector_db_denial(
    selector: &AclSelector,
    req: &DbRequirement,
    num_dbs: usize,
) -> Option<AclDenial> {
    match req {
        DbRequirement::None => None,
        DbRequirement::All => (0..num_dbs as i64)
            .find(|d| !selector.can_access_db(*d))
            .map(|dbid| AclDenial::Db {
                dbid,
                arg_index: None,
            }),
        DbRequirement::Single(dbid, arg_index) => {
            (!selector.can_access_db(*dbid)).then_some(AclDenial::Db {
                dbid: *dbid,
                arg_index: *arg_index,
            })
        }
        DbRequirement::Ids(ids) => {
            ids.iter()
                .find(|(db, _)| !selector.can_access_db(*db))
                .map(|(dbid, arg_index)| AclDenial::Db {
                    dbid: *dbid,
                    arg_index: *arg_index,
                })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AclSelector;
    use crate::engine::extension::acl::command::{
        register_module_acl_category, remove_module_acl_category_all, should_restrict_cmd,
        AclCommand,
    };

    #[test]
    fn generated_categories_follow_acl_rule_order() {
        let mut selector = AclSelector::new();
        selector.apply_rule("+@read").unwrap();
        selector.apply_rule("-@string").unwrap();
        let get = AclCommand::new("get", &[], &["FAST", "READ", "STRING"]);

        assert!(!selector.can_execute_command_with_args(get));

        selector.apply_rule("+@read").unwrap();
        assert!(selector.can_execute_command_with_args(get));
    }

    #[test]
    fn generated_categories_drive_database_restriction() {
        assert!(should_restrict_cmd("get", &["READ", "STRING"], false));
        assert!(!should_restrict_cmd("ping", &["FAST", "CONNECTION"], false));
        assert!(should_restrict_cmd("module-key-command", &[], true));
    }

    #[test]
    fn module_categories_remain_available_without_generated_metadata() {
        const CATEGORY: &str = "VERSAL_TEST_MODULE_CATEGORY";
        const COMMAND: &str = "VERSAL.TEST.MODULE.COMMAND";
        register_module_acl_category(CATEGORY, COMMAND);

        let mut selector = AclSelector::new();
        selector
            .apply_rule("+@versal_test_module_category")
            .unwrap();
        let command = AclCommand::new(COMMAND, &[], &[]);
        let allowed = selector.can_execute_command_with_args(command);

        remove_module_acl_category_all(CATEGORY);
        assert!(allowed);
    }
}
