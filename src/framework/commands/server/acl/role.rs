use bytes::Bytes;

use crate::engine::extension::acl::{
    acl_roles, acl_users, apply_role_rule, validate_role_name, AclUser,
};
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};

use super::setuser::{merge_selector_arguments, validate_acl_rule};
use super::user::selector_description;

pub(super) fn acl_setrole(args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("acl|setrole");
    }
    if let Err(error) = validate_role_name(&args.args[1]) {
        return Value::error(format!("ERR {error}"));
    }
    let name = std::str::from_utf8(&args.args[1]).unwrap_or_default();
    let raw = match args.args[2..]
        .iter()
        .map(|arg| std::str::from_utf8(arg))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(raw) => raw,
        Err(_) => return CommandError::syntax_error(),
    };
    let rules = match merge_selector_arguments(&raw) {
        Ok(rules) => rules,
        Err(index) => {
            return Value::error(format!(
                "ERR Unmatched parenthesis in selector definition starting at '{}'.",
                raw[index]
            ));
        }
    };
    let users = match acl_users().lock() {
        Ok(users) => users,
        Err(_) => return Value::error("ERR ACL user registry is unavailable"),
    };
    let mut roles = match acl_roles().lock() {
        Ok(roles) => roles,
        Err(_) => return Value::error("ERR ACL role registry is unavailable"),
    };
    let default_allchannels = ctx.config().server.acl_pubsub_default == "allchannels";
    let mut staged = roles.get(name).cloned().unwrap_or_else(|| {
        let mut role = AclUser::new_disabled();
        if default_allchannels {
            let _ = role.apply_rule("allchannels");
        }
        role
    });
    for rule in rules {
        if let Err(error) = validate_acl_rule(&rule, ctx)
            .and_then(|()| apply_role_rule(&mut staged, &rule, default_allchannels))
        {
            return Value::error(format!(
                "ERR Error in ACL SETROLE modifier '{rule}': {error}"
            ));
        }
    }
    roles.insert(name.to_string(), staged);
    for (username, user) in users.iter() {
        if user.roles.iter().any(|role| role == name) {
            ctx.pending_acl_client_kills_mut().push(username.clone());
        }
    }
    Value::ok()
}

pub(super) fn acl_delrole(args: &CommandArgs) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("acl|delrole");
    }
    let users = match acl_users().lock() {
        Ok(users) => users,
        Err(_) => return Value::error("ERR ACL user registry is unavailable"),
    };
    let mut roles = match acl_roles().lock() {
        Ok(roles) => roles,
        Err(_) => return Value::error("ERR ACL role registry is unavailable"),
    };
    // Validate every target before deleting any role.
    for name in &args.args[1..] {
        let name = std::str::from_utf8(name).unwrap_or_default();
        if roles.contains_key(name)
            && users
                .values()
                .any(|user| user.roles.iter().any(|r| r == name))
        {
            return Value::error(format!(
                "ERR Role '{name}' is assigned to one or more users. Remove it from them first."
            ));
        }
    }
    let count = args.args[1..]
        .iter()
        .filter(|name| {
            roles
                .remove(std::str::from_utf8(name).unwrap_or_default())
                .is_some()
        })
        .count();
    Value::integer(count as i64)
}

pub(super) fn acl_getrole(args: &CommandArgs) -> Value {
    if args.args.len() != 2 {
        return CommandError::wrong_arity("acl|getrole");
    }
    let name = std::str::from_utf8(&args.args[1]).unwrap_or_default();
    let users = match acl_users().lock() {
        Ok(users) => users,
        Err(_) => return Value::error("ERR ACL user registry is unavailable"),
    };
    let roles = match acl_roles().lock() {
        Ok(roles) => roles,
        Err(_) => return Value::error("ERR ACL role registry is unavailable"),
    };
    let Some(role) = roles.get(name) else {
        return Value::Null;
    };
    let mut result = selector_description(&role.root_selector);
    result.push(Value::bulk(Bytes::from_static(b"selectors")));
    result.push(Value::Array(
        role.selectors
            .iter()
            .map(|selector| Value::Array(selector_description(selector)))
            .collect(),
    ));
    let mut members: Vec<_> = users
        .iter()
        .filter(|(_, user)| user.roles.iter().any(|r| r == name))
        .map(|(name, _)| name.clone())
        .collect();
    members.sort();
    result.push(Value::bulk(Bytes::from_static(b"users")));
    result.push(Value::Array(
        members
            .into_iter()
            .map(|name| Value::bulk(Bytes::from(name)))
            .collect(),
    ));
    Value::Array(result)
}

pub(super) fn acl_roles_list(args: &CommandArgs) -> Value {
    if args.args.len() != 1 {
        return CommandError::wrong_arity("acl|roles");
    }
    let roles = match acl_roles().lock() {
        Ok(roles) => roles,
        Err(_) => return Value::error("ERR ACL role registry is unavailable"),
    };
    let mut names: Vec<_> = roles.keys().cloned().collect();
    names.sort();
    Value::Array(
        names
            .into_iter()
            .map(|name| Value::bulk(Bytes::from(name)))
            .collect(),
    )
}
