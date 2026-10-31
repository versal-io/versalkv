use crate::engine::extension::acl::{
    acl_roles, acl_users, parse_acl_file, replace_acl_state, AclFileState,
};
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;

pub fn acl_load(ctx: &mut ServerContext) -> Value {
    if ctx.config().server.aclfile.is_empty() {
        return Value::error("ERR This instance is not configured to use an ACL file. To use an ACL file, specify the 'aclfile' configuration directive.");
    }
    let path = &ctx.config().server.aclfile;
    let content = match std::fs::read(path) {
        Ok(content) => content,
        Err(error) => {
            return Value::error(format!("ERR Could not read ACL file '{path}': {error}"))
        }
    };
    let state = match parse_acl_file(
        &content,
        ctx.config().server.acl_pubsub_default == "allchannels",
    ) {
        Ok(state) => state,
        Err(error) => return Value::error(format!("ERR {error}")),
    };
    if let Err(error) = validate_acl_file(&state, ctx) {
        return Value::error(format!("ERR {error}"));
    }
    match replace_acl_state(state) {
        Ok(existing) => ctx.pending_acl_client_kills_mut().extend(existing),
        Err(error) => return Value::error(format!("ERR {error}")),
    }
    Value::ok()
}

pub(crate) fn validate_acl_file(state: &AclFileState, ctx: &ServerContext) -> Result<(), String> {
    for rule in &state.declared_rules {
        super::setuser::validate_acl_rule_bytes(rule, ctx).map_err(|error| {
            format!(
                "Error loading ACL rule {}: {error}",
                crate::engine::extension::acl::permissions::quote_acl_bytes(rule)
            )
        })?;
    }
    Ok(())
}

pub fn acl_save(ctx: &ServerContext) -> Value {
    if ctx.config().server.aclfile.is_empty() {
        return Value::error("ERR This instance is not configured to use an ACL file. To use an ACL file, specify the 'aclfile' configuration directive.");
    }
    let users = match acl_users().lock() {
        Ok(users) => users,
        Err(_) => return Value::error("ERR ACL user registry is unavailable"),
    };
    let roles = match acl_roles().lock() {
        Ok(roles) => roles,
        Err(_) => return Value::error("ERR ACL role registry is unavailable"),
    };
    let mut role_entries: Vec<_> = roles.iter().collect();
    role_entries.sort_by_key(|(name, _)| *name);
    let mut user_entries: Vec<_> = users.iter().collect();
    user_entries.sort_by_key(|(name, _)| *name);
    let mut content = Vec::new();
    for (name, role) in role_entries {
        content.extend_from_slice(b"role ");
        content.extend_from_slice(&role.describe_role_bytes(name));
        content.push(b'\n');
    }
    for (name, user) in user_entries {
        content.extend_from_slice(b"user ");
        content.extend_from_slice(&user.describe(name));
        content.push(b'\n');
    }
    drop(roles);
    drop(users);
    if let Err(error) = std::fs::write(&ctx.config().server.aclfile, content) {
        return Value::error(format!(
            "ERR Could not write ACL file '{}': {error}",
            ctx.config().server.aclfile
        ));
    }
    Value::ok()
}
