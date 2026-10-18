use crate::framework::CommandArgs;
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use command_macros::command;

#[command("READWRITE")]
pub fn readwrite(
    conn: &mut dyn CommandConn,
    _args: &CommandArgs,
    _ctx: &mut ServerContext,
) -> Value {
    conn.set_readonly(false);
    Value::ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use bytes::Bytes;

    #[test]
    fn readwrite_clears_readonly_mode() {
        let mut conn = Connection::new(Token(1), ([127, 0, 0, 1], 6379).into(), false);
        conn.set_readonly(true);
        let args = CommandArgs::from_parts(vec![Bytes::from_static(b"READWRITE")]);
        let mut ctx = ServerContext::new(1);

        let reply = readwrite(&mut conn, &args, &mut ctx);

        assert_eq!(reply, Value::ok());
        assert!(!conn.readonly());
    }
}
