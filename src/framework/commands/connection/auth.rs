use crate::framework::{CommandArgs, CommandError};
use crate::engine::extension::acl::{acl_users, add_acl_log_entry, ACL_DENIED_AUTH, ACL_LOG_CTX_TOPLEVEL};
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use command_macros::command;

#[command("AUTH")]
pub fn auth(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() > 2 {
        return CommandError::syntax_error();
    }

    if args.args.len() == 2 {
        let username = match std::str::from_utf8(&args.args[0]) {
            Ok(s) => s,
            Err(_) => return CommandError::syntax_error(),
        };
        let password = &args.args[1];
        let users = acl_users().lock().unwrap();
        let result = match users.get(username) {
            Some(user) if user.is_on() => {
                if user.check_password(password) {
                    drop(users);
                    conn.notify_module_auth_changed();
                    conn.set_module_auth_user(None);
                    conn.set_authenticated(true);
                    conn.set_ever_authenticated(true);
                    conn.set_username(username.to_string());
                    Value::ok()
                } else {
                    CommandError::wrongpass()
                }
            }
            Some(_user) => CommandError::wrongpass(),
            None => CommandError::wrongpass(),
        };
        // Log AUTH failures
        if matches!(&result, Value::Error(_)) {
            let client_info = format!("addr={} cmd=AUTH user={}", conn.peer_addr(), username);
            add_acl_log_entry(
                ACL_DENIED_AUTH,
                ACL_LOG_CTX_TOPLEVEL,
                "AUTH".to_string(),
                username.to_string(),
                client_info,
            );
        }
        result
    } else {
        let expected = match &ctx.config().security.requirepass {
            Some(p) => p,
            None => return CommandError::no_password_set(),
        };

        let provided = &args.args[0];

        if provided.as_ref() != expected.as_slice() {
            let client_info = format!("addr={} cmd=AUTH user=default", conn.peer_addr());
            add_acl_log_entry(
                ACL_DENIED_AUTH,
                ACL_LOG_CTX_TOPLEVEL,
                "AUTH".to_string(),
                "default".to_string(),
                client_info,
            );
            return CommandError::wrongpass();
        }

        conn.notify_module_auth_changed();
        conn.set_module_auth_user(None);
        conn.set_authenticated(true);
        conn.set_ever_authenticated(true);
        conn.set_username("default".to_string());
        Value::ok()
    }
}
