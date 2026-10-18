
use command_macros::command;

use crate::framework::CommandArgs;
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;

#[command("EVALSHA", get_keys = crate::framework::commands::server::command::getkeys_helpers::eval_get_keys, hosted = crate::framework::host_tasks::prepare_lua_task)]
pub fn evalsha(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    match crate::framework::host_tasks::prepare_lua_task(conn, args, ctx) {
        Ok(crate::framework::host_tasks::PreparedHostTask::Lua(task)) => {
            crate::framework::host_tasks::run_lua_task_inline(task, conn, ctx)
        }
        Ok(_) => Value::error("ERR invalid hosted task for EVALSHA"),
        Err(reply) => reply,
    }
}

