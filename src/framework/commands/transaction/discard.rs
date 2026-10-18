use crate::framework::CommandArgs;
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use command_macros::command;

#[command("DISCARD")]
pub fn discard(conn: &mut dyn CommandConn, _args: &CommandArgs, _ctx: &mut ServerContext) -> Value {
    if !conn.in_transaction() {
        return Value::error("ERR DISCARD without MULTI");
    }
    conn.discard_transaction();
    conn.set_blocked(None);
    Value::ok()
}
