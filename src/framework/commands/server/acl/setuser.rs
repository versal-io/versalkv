use crate::engine::extension::acl::AclUser;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};

fn users() -> std::sync::MutexGuard<'static, std::collections::HashMap<String, AclUser>> {
    crate::engine::extension::acl::acl_users().lock().unwrap()
}

/// Validate an ACL rule against the command registry and generated command tables.
/// Returns Ok(()) if the rule is valid, or an error message string.
/// Matches valkey C `ACLSetSelector` validation logic.
pub fn validate_acl_rule(rule: &str, ctx: &ServerContext) -> Result<(), &'static str> {
    validate_acl_rule_bytes(rule.as_bytes(), ctx)
}

pub(crate) fn validate_acl_rule_bytes(
    rule: &[u8],
    ctx: &ServerContext,
) -> Result<(), &'static str> {
    if rule.is_empty() {
        return Ok(());
    }
    if rule.starts_with(b"(") && rule.ends_with(b")") {
        for selector_rule in
            crate::engine::extension::acl::user::parse_selector_rules(rule).unwrap_or_default()
        {
            validate_acl_rule_bytes(&selector_rule, ctx)?;
        }
        return Ok(());
    }
    let op = rule[0] as char;
    if op != '+' && op != '-' {
        return Ok(());
    }
    if rule.len() < 2 {
        return Ok(());
    }
    let rule = std::str::from_utf8(rule).map_err(|_| "Unknown command or category name in ACL")?;
    let rest = &rule[1..];
    if let Some(category) = rest.strip_prefix('@') {
        return if category.eq_ignore_ascii_case("all")
            || crate::engine::extension::acl::is_builtin_acl_category(category)
            || crate::engine::extension::acl::module_acl_category_commands(category).is_some()
        {
            Ok(())
        } else {
            Err("Unknown command or category name in ACL")
        };
    }
    if !rest.contains('|') {
        let lookup = rest.to_ascii_lowercase();
        if !crate::framework::generated::commands::TOP_LEVEL_COMMANDS
            .iter()
            .any(|c| c.eq_ignore_ascii_case(&lookup))
            && !ctx.module_registry().command_exists(&lookup)
        {
            return Err("Unknown command or category name in ACL");
        }
        return Ok(());
    }
    let pipe_pos = rest.rfind('|').unwrap();
    let parent = &rest[..pipe_pos];
    let sub = &rest[pipe_pos + 1..];
    if parent.contains('|') {
        let parent_lower = parent.to_ascii_lowercase();
        if crate::framework::generated::commands::SUBCOMMAND_NAMES
            .iter()
            .any(|c| c.eq_ignore_ascii_case(&parent_lower))
        {
            return Err("Allowing first-arg of a subcommand is not supported");
        }
        return Err("Unknown command or category name in ACL");
    }
    let parent_lower = parent.to_ascii_lowercase();
    if !crate::framework::generated::commands::TOP_LEVEL_COMMANDS
        .iter()
        .any(|c| c.eq_ignore_ascii_case(&parent_lower))
        && !ctx.module_registry().command_exists(&parent_lower)
    {
        return Err("Unknown command or category name in ACL");
    }
    let parent_is_subcmd = crate::framework::generated::commands::SUBCOMMAND_NAMES
        .iter()
        .any(|c| {
            let parts: Vec<&str> = c.split('|').collect();
            parts.len() == 2 && parts[0].eq_ignore_ascii_case(&parent_lower)
        })
        || !ctx
            .module_registry()
            .subcommand_names(&parent_lower)
            .is_empty();
    if op == '+' {
        let full_name = format!("{}|{}", parent_lower, sub.to_ascii_lowercase());
        if parent_is_subcmd
            && !crate::framework::generated::commands::SUBCOMMAND_NAMES
                .iter()
                .any(|c| c.eq_ignore_ascii_case(&full_name))
            && !ctx.module_registry().command_exists(&full_name)
        {
            return Err("Unknown command or category name in ACL");
        }
    } else {
        let full_name = format!("{}|{}", parent_lower, sub.to_ascii_lowercase());
        if !crate::framework::generated::commands::SUBCOMMAND_NAMES
            .iter()
            .any(|c| c.eq_ignore_ascii_case(&full_name))
            && !ctx.module_registry().command_exists(&full_name)
        {
            return Err("Unknown command or category name in ACL");
        }
    }
    Ok(())
}

/// Merge selector arguments spread across multiple space-separated args into a
/// single `(...)` modifier. Mirrors C `ACLMergeSelectorArguments`. Returns the
/// merged rule list, or `Err(idx)` with the index of the unmatched opening
/// parenthesis argument.
pub(super) fn merge_selector_arguments(args: &[&str]) -> Result<Vec<String>, usize> {
    let mut merged: Vec<String> = Vec::new();
    let mut open_start: Option<usize> = None;
    let mut selector = String::new();
    for (j, op) in args.iter().enumerate() {
        if open_start.is_none() && op.starts_with('(') && !op.ends_with(')') {
            selector = (*op).to_string();
            open_start = Some(j);
            continue;
        }
        if open_start.is_some() {
            selector.push(' ');
            selector.push_str(op);
            if op.ends_with(')') {
                open_start = None;
                merged.push(std::mem::take(&mut selector));
            }
            continue;
        }
        merged.push((*op).to_string());
    }
    if let Some(idx) = open_start {
        return Err(idx);
    }
    Ok(merged)
}

pub fn acl_setuser(args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("acl setuser");
    }
    let username = match std::str::from_utf8(&args.args[1]) {
        Ok(s) => s,
        Err(_) => return CommandError::syntax_error(),
    };
    if username.contains(' ') || username.contains('\0') {
        return Value::error("ERR Usernames can't contain spaces or null characters or user names that can't be used");
    }

    // Decode the rule arguments up front so a bad UTF-8 arg fails cleanly.
    let mut raw_rules: Vec<&str> = Vec::with_capacity(args.args.len() - 2);
    for arg in &args.args[2..] {
        match std::str::from_utf8(arg) {
            Ok(s) => raw_rules.push(s),
            Err(_) => return CommandError::syntax_error(),
        }
    }

    let merged = match merge_selector_arguments(&raw_rules) {
        Ok(m) => m,
        Err(idx) => {
            return Value::error(format!(
                "ERR Unmatched parenthesis in acl selector starting at '{}'.",
                raw_rules[idx]
            ));
        }
    };

    let mut users = users();
    let is_new = !users.contains_key(username);

    // Stage all changes against a temporary user; only commit if every rule
    // is valid. Matches C ACLStringSetUser's temp-user semantics so a failed
    // modifier neither mutates an existing user nor creates a new one.
    let mut staged = match users.get(username) {
        Some(existing) => existing.clone(),
        None => {
            let mut u = AclUser::new_disabled();
            let _ = u.apply_rule("reset");
            let _ = u.apply_rule("alldbs");
            let _ = u.apply_rule("off");
            let _ = u.apply_rule("clearselectors");
            if ctx.config().server.acl_pubsub_default == "allchannels" {
                let _ = u.apply_rule("allchannels");
            }
            u
        }
    };

    for rule_str in &merged {
        if let Some(raw) = rule_str.strip_prefix('#') {
            if !crate::engine::extension::acl::is_valid_sha256_hex(raw) {
                return Value::error(format!(
                    "ERR Error in ACL SETUSER modifier '{}': Wrong format",
                    rule_str
                ));
            }
        }
        if let Err(e) = validate_acl_rule(rule_str, ctx) {
            return Value::error(format!(
                "ERR Error in ACL SETUSER modifier '{}': {}",
                rule_str, e
            ));
        }
        if let Err(e) = staged.apply_rule_with_pubsub_default(
            rule_str,
            ctx.config().server.acl_pubsub_default == "allchannels",
        ) {
            return Value::error(format!(
                "ERR Error in ACL SETUSER modifier '{}': {}",
                rule_str, e
            ));
        }
        if rule_str == "reset" && ctx.config().server.acl_pubsub_default == "allchannels" {
            let _ = staged.apply_rule("allchannels");
        }
    }

    // All modifiers valid: commit the staged user.
    let _ = is_new;
    users.insert(username.to_string(), staged);
    ctx.pending_acl_client_kills_mut()
        .push(username.to_string());
    drop(users);
    if !ctx.sentinel_state().sentinel_mode && ctx.config().server.config_file.is_some() {
        let _ = crate::framework::commands::server::config::config_rewrite(ctx);
    }
    Value::ok()
}
