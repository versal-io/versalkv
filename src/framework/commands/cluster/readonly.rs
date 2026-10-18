use crate::framework::CommandArgs;
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use command_macros::command;

#[command("READONLY")]
pub fn readonly(
    conn: &mut dyn CommandConn,
    _args: &CommandArgs,
    _ctx: &mut ServerContext,
) -> Value {
    conn.set_readonly(true);
    Value::ok()
}
