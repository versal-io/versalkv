//! Staged ACL file parsing, with roles resolved before user memberships.

use std::collections::HashMap;

use super::permissions::quote_acl_bytes;
use super::{
    acl_roles, acl_users, apply_role_rule_bytes, merge_acl_tokens, validate_role_name, AclUser,
};

pub(crate) struct AclFileState {
    pub(crate) users: HashMap<String, AclUser>,
    pub(crate) roles: HashMap<String, AclUser>,
    pub(crate) has_default_user: bool,
    pub(crate) declared_rules: Vec<Vec<u8>>,
}

pub(crate) fn parse_acl_file(
    content: &[u8],
    default_allchannels: bool,
) -> Result<AclFileState, String> {
    let lines = content
        .split(|byte| *byte == b'\n')
        .map(|line| {
            let is_trim_byte = |byte: &u8| matches!(byte, b' ' | b'\t' | b'\r' | b'\n');
            let start = line
                .iter()
                .position(|byte| !is_trim_byte(byte))
                .unwrap_or(line.len());
            let end = line
                .iter()
                .rposition(|byte| !is_trim_byte(byte))
                .map_or(start, |index| index + 1);
            &line[start..end]
        })
        .filter(|line| !line.is_empty())
        // ACL files use sdssplitlen(" "), not config/RESP quote syntax.
        .map(|line| {
            line.split(|byte| *byte == b' ')
                .map(<[u8]>::to_vec)
                .collect()
        })
        .collect();
    parse_acl_byte_declarations(lines, default_allchannels)
}

pub(crate) fn parse_acl_declarations(
    lines: Vec<Vec<String>>,
    default_allchannels: bool,
) -> Result<AclFileState, String> {
    parse_acl_byte_declarations(
        lines
            .into_iter()
            .map(|line| line.into_iter().map(String::into_bytes).collect())
            .collect(),
        default_allchannels,
    )
}

fn parse_acl_byte_declarations(
    lines: Vec<Vec<Vec<u8>>>,
    default_allchannels: bool,
) -> Result<AclFileState, String> {
    let mut state = AclFileState {
        users: HashMap::new(),
        roles: HashMap::new(),
        has_default_user: false,
        declared_rules: Vec::new(),
    };
    for parts in &lines {
        let parts: Vec<_> = parts.iter().map(Vec::as_slice).collect();
        if parts.first() != Some(&b"role".as_slice()) {
            continue;
        }
        let Some(name) = parts.get(1) else {
            return Err("Error loading ACL file: role line requires a role name".to_string());
        };
        validate_role_name(name).map_err(|error| format!("invalid role name: {error}"))?;
        let name = std::str::from_utf8(name).map_err(|_| "Invalid role name".to_string())?;
        if state.roles.contains_key(name) {
            return Err(format!("Duplicate role '{name}' found in the ACL file"));
        }
        let mut role = AclUser::new_disabled();
        if default_allchannels {
            role.apply_rule("allchannels").map_err(str::to_string)?;
        }
        let rules = merge_acl_tokens(&parts[2..]).map_err(|()| {
            format!("Error loading ACL file: unmatched parenthesis for role '{name}'")
        })?;
        for rule in rules {
            state.declared_rules.push(rule.clone());
            apply_role_rule_bytes(&mut role, &rule, default_allchannels).map_err(|error| {
                format!(
                    "Error in ACL SETROLE modifier {}: {error}",
                    quote_acl_bytes(&rule)
                )
            })?;
        }
        state.roles.insert(name.to_string(), role);
    }
    for parts in &lines {
        let parts: Vec<_> = parts.iter().map(Vec::as_slice).collect();
        if parts.first() == Some(&b"role".as_slice()) {
            continue;
        }
        if parts.first() != Some(&b"user".as_slice()) || parts.len() < 2 {
            return Err("Error loading ACL file: expected a user or role declaration".to_string());
        }
        let name = std::str::from_utf8(parts[1]).map_err(|_| "Invalid user name".to_string())?;
        if state.users.contains_key(name) {
            return Err(format!("Duplicate user '{name}' found in the ACL file"));
        }
        let mut user = AclUser::new_disabled();
        if default_allchannels {
            user.apply_rule("allchannels").map_err(str::to_string)?;
        }
        let rules = merge_acl_tokens(&parts[2..]).map_err(|()| {
            format!("Error loading ACL file: unmatched parenthesis for user '{name}'")
        })?;
        for rule in rules {
            state.declared_rules.push(rule.clone());
            let result = if rule
                .get(..5)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"role="))
            {
                std::str::from_utf8(&rule[5..])
                    .map_err(|_| "Syntax error")
                    .and_then(|roles| user.set_roles(roles, &state.roles))
            } else {
                user.apply_rule_bytes_with_pubsub_default(&rule, default_allchannels)
            };
            result.map_err(|error| {
                format!(
                    "Error in ACL SETUSER modifier {}: {error}",
                    quote_acl_bytes(&rule)
                )
            })?;
            if rule == b"reset" && default_allchannels {
                user.apply_rule("allchannels").map_err(str::to_string)?;
            }
        }
        state.users.insert(name.to_string(), user);
    }
    state.has_default_user = state.users.contains_key("default");
    if !state.users.contains_key("default") {
        let mut user = AclUser::new_disabled();
        for rule in ["on", "nopass", "+@all", "~*", "&*"] {
            user.apply_rule(rule).map_err(str::to_string)?;
        }
        state.users.insert("default".to_string(), user);
    }
    Ok(state)
}

pub(crate) fn replace_acl_state(state: AclFileState) -> Result<Vec<String>, &'static str> {
    let mut users = acl_users()
        .lock()
        .map_err(|_| "ACL user registry is unavailable")?;
    let mut roles = acl_roles()
        .lock()
        .map_err(|_| "ACL role registry is unavailable")?;
    let existing = users.keys().cloned().collect();
    *users = state.users;
    *roles = state.roles;
    Ok(existing)
}
