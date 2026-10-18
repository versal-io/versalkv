use crate::framework::CommandArgs;
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use command_macros::command;

#[command("UNWATCH")]
pub fn unwatch(conn: &mut dyn CommandConn, _args: &CommandArgs, _ctx: &mut ServerContext) -> Value {
    if conn.in_transaction() {
        conn.mark_transaction_queue_error();
        return Value::error("ERR Command 'unwatch' not allowed inside a transaction");
    }
    conn.clear_watched_keys();
    Value::ok()
}
