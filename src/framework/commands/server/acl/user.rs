use crate::engine::extension::acl::AclUser;
use crate::foundation::io::protocol::Value;
use bytes::Bytes;

fn users() -> std::sync::MutexGuard<'static, std::collections::HashMap<String, AclUser>> {
    crate::engine::extension::acl::acl_users().lock().unwrap()
}

pub fn acl_list() -> Value {
    let users = users();
    let mut result = Vec::with_capacity(users.len());
    for (name, user) in users.iter() {
        result.push(Value::bulk(Bytes::from(format!(
            "user {}",
            user.describe(name)
        ))));
    }
    Value::Array(result)
}

pub fn acl_users_list() -> Value {
    let users = users();
    let mut names: Vec<&String> = users.keys().collect();
    names.sort();
    let result: Vec<Value> = names
        .into_iter()
        .map(|k| Value::bulk(Bytes::from(k.clone())))
        .collect();
    Value::Array(result)
}

pub fn acl_getuser(username: &str) -> Value {
    let users = users();
    if let Some(user) = users.get(username) {
        let mut result = Vec::new();
        result.push(Value::bulk(Bytes::from_static(b"flags")));
        result.push(Value::Array(vec![Value::bulk(Bytes::copy_from_slice(
            user.flags.as_bytes(),
        ))]));
        result.push(Value::bulk(Bytes::from_static(b"passwords")));
        let pass_list: Vec<Value> = user
            .passwords
            .iter()
            .map(|p| Value::bulk(Bytes::copy_from_slice(p.as_bytes())))
            .collect();
        result.push(Value::Array(pass_list));
        result.push(Value::bulk(Bytes::from_static(b"commands")));
        result.push(Value::bulk(Bytes::from(user.root_categories_str())));
        result.push(Value::bulk(Bytes::from_static(b"keys")));
        result.push(Value::bulk(Bytes::from(user.root_keys_str())));
        result.push(Value::bulk(Bytes::from_static(b"channels")));
        result.push(Value::bulk(Bytes::from(user.root_channels_str())));
        result.push(Value::bulk(Bytes::from_static(b"databases")));
        result.push(Value::bulk(Bytes::from(user.root_databases_str())));
        result.push(Value::bulk(Bytes::from_static(b"selectors")));
        let selectors = user.get_selectors();
        let sel_list: Vec<Value> = selectors
            .iter()
            .map(|s| {
                let sel_result = vec![
                    Value::bulk(Bytes::from_static(b"keys")),
                    Value::bulk(Bytes::from(s.keys_str())),
                    Value::bulk(Bytes::from_static(b"channels")),
                    Value::bulk(Bytes::from(s.channels_str())),
                    Value::bulk(Bytes::from_static(b"commands")),
                    Value::bulk(Bytes::from(s.categories_str())),
                    Value::bulk(Bytes::from_static(b"databases")),
                    Value::bulk(Bytes::from(s.databases_str())),
                ];
                Value::Array(sel_result)
            })
            .collect();
        result.push(Value::Array(sel_list));
        Value::Array(result)
    } else {
        Value::Null
    }
}

pub fn acl_deluser(
    conn: &mut dyn crate::engine::command::context::CommandConn,
    args: &crate::framework::CommandArgs,
) -> Value {
    let count = if args.args.len() > 1 {
        let mut removed = 0i64;
        let mut users = users();
        for i in 1..args.args.len() {
            let name = std::str::from_utf8(&args.args[i]).unwrap_or("");
            if name.eq_ignore_ascii_case("default") {
                return Value::error("ERR The 'default' user cannot be removed");
            }
            if users.remove(name).is_some() {
                removed += 1;
                if conn.username() == name {
                    conn.mark_closing();
                }
            }
        }
        removed
    } else {
        0
    };
    Value::integer(count)
}
