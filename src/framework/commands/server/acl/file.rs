use crate::framework::context::ServerContext;
use crate::engine::extension::acl::AclUser;
use crate::foundation::io::protocol::Value;

fn users_lock() -> std::sync::MutexGuard<'static, std::collections::HashMap<String, AclUser>> {
    crate::engine::extension::acl::acl_users().lock().unwrap()
}

pub fn acl_load(ctx: &mut ServerContext) -> Value {
    if ctx.config().server.aclfile.is_empty() {
        return Value::error("ERR This instance is not configured to use an ACL file. To use an ACL file, specify the 'aclfile' configuration directive.");
    }
    let path = &ctx.config().server.aclfile;
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => return Value::error(format!("ERR Could not read ACL file '{}': {}", path, e)),
    };
    let mut users = users_lock();
    let existing_usernames: Vec<String> = users.keys().cloned().collect();
    let mut seen_users: std::collections::HashSet<String> = std::collections::HashSet::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 2 || parts[0] != "user" {
            continue;
        }
        let username = parts[1].to_string();
        if !seen_users.insert(username.clone()) {
            drop(users);
            return Value::error(format!("ERR Duplicate user '{}' found in the ACL file", username));
        }
    }
    users.clear();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 2 || parts[0] != "user" {
            continue;
        }
        let username = parts[1].to_string();
        let mut user = AclUser::new_disabled();
        let rules = match crate::engine::extension::acl::merge_acl_tokens(&parts[2..]) {
            Ok(r) => r,
            Err(()) => {
                drop(users);
                return Value::error(format!(
                    "ERR Error loading ACL file: unmatched parenthesis for user '{}'",
                    username
                ));
            }
        };
        for rule in &rules {
            let _ = user.apply_rule(rule);
        }
        users.insert(username, user);
    }
    if !users.contains_key("default") {
        let mut default_user = AclUser::new_disabled();
        let _ = default_user.apply_rule("on");
        let _ = default_user.apply_rule("+@all");
        let _ = default_user.apply_rule("~*");
        let _ = default_user.apply_rule("&*");
        let _ = default_user.apply_rule("nopass");
        users.insert("default".to_string(), default_user);
    }
    drop(users);
    for name in existing_usernames {
        ctx.pending_acl_client_kills_mut().push(name);
    }
    Value::ok()
}

pub fn acl_save(ctx: &ServerContext) -> Value {
    if ctx.config().server.aclfile.is_empty() {
        return Value::error("ERR This instance is not configured to use an ACL file. To use an ACL file, specify the 'aclfile' configuration directive.");
    }
    let users = users_lock();
    let mut content = String::new();
    let mut entries: Vec<(&String, &AclUser)> = users.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    for (username, user) in &entries {
        content.push_str(&format!("user {}\n", user.to_acl_rule_string(username)));
    }
    drop(users);
    if let Err(e) = std::fs::write(&ctx.config().server.aclfile, &content) {
        return Value::error(format!(
            "ERR Could not write ACL file '{}': {}",
            ctx.config().server.aclfile, e
        ));
    }
    Value::ok()
}
