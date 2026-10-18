use crate::framework::CommandArgs;
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use command_macros::command;

#[command("MONITOR")]
pub fn monitor(conn: &mut dyn CommandConn, _args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if conn.deny_blocking() {
        return Value::error("ERR MONITOR isn't allowed for DENY BLOCKING client");
    }
    if conn.monitoring() {
        conn.suppress_reply();
        return Value::Null;
    }
    ctx.register_monitor_client(conn.client_id());
    conn.set_monitoring(true);
    Value::ok()
}

#[cfg(test)]
mod tests {
    use super::monitor;
    use crate::engine::command::context::CommandConn;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::commands::connection::reset::reset;
    use crate::framework::context::ServerContext;
    use crate::framework::CommandArgs;
    use crate::foundation::io::protocol::Value;
    use bytes::Bytes;

    fn args(name: &'static [u8]) -> CommandArgs {
        CommandArgs::from_parts(vec![Bytes::from_static(name)])
    }

    #[test]
    fn monitor_rejects_deny_blocking_clients() {
        let mut conn = Connection::new_fake();
        let mut ctx = ServerContext::new(1);
        conn.set_deny_blocking(true);

        assert_eq!(
            monitor(&mut conn, &args(b"MONITOR"), &mut ctx),
            Value::error("ERR MONITOR isn't allowed for DENY BLOCKING client")
        );
        assert!(!conn.monitoring());
        assert!(!ctx.has_monitor_clients());
    }

    #[test]
    fn repeated_monitor_suppresses_reply_without_double_registration() {
        let mut conn = Connection::new_fake();
        let mut ctx = ServerContext::new(1);

        assert_eq!(
            monitor(&mut conn, &args(b"MONITOR"), &mut ctx),
            Value::ok()
        );
        assert_eq!(
            monitor(&mut conn, &args(b"MONITOR"), &mut ctx),
            Value::Null
        );
        assert!(conn.reply_suppressed());
        assert!(ctx.has_monitor_clients());
    }

    #[test]
    fn reset_leaves_monitor_mode_and_unregisters_client() {
        let mut conn = Connection::new_fake();
        let mut ctx = ServerContext::new(1);
        monitor(&mut conn, &args(b"MONITOR"), &mut ctx);

        assert_eq!(
            reset(&mut conn, &args(b"RESET"), &mut ctx),
            Value::SimpleString(Bytes::from_static(b"RESET"))
        );
        assert!(!conn.monitoring());
        assert!(!ctx.has_monitor_clients());
    }
}
