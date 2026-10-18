use crate::framework::CommandArgs;
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use command_macros::command;

#[command("PSYNC")]
pub fn psync(_conn: &mut dyn CommandConn, _args: &CommandArgs, _ctx: &mut ServerContext) -> Value {
    Value::error("ERR PSYNC is not supported in Rust main mode")
}
