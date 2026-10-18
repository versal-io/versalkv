use crate::framework::CommandArgs;
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use command_macros::command;

#[command("MULTI")]
pub fn multi(conn: &mut dyn CommandConn, _args: &CommandArgs, _ctx: &mut ServerContext) -> Value {
    if !conn.begin_transaction() {
        return Value::error("ERR Command 'multi' not allowed inside a transaction");
    }
    Value::ok()
}
