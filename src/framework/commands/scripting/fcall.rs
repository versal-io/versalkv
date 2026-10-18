
use command_macros::command;

use crate::framework::CommandArgs;
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;

#[command("FCALL", get_keys = crate::framework::commands::server::command::getkeys_helpers::eval_get_keys, hosted = crate::framework::host_tasks::prepare_lua_task)]
pub fn fcall(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    fcall_impl(conn, args, ctx, false)
}

#[command("FCALL_RO", get_keys = crate::framework::commands::server::command::getkeys_helpers::eval_ro_get_keys, hosted = crate::framework::host_tasks::prepare_lua_task)]
pub fn fcall_ro(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    fcall_impl(conn, args, ctx, true)
}

fn fcall_impl(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
    _readonly: bool,
) -> Value {
    match crate::framework::host_tasks::prepare_lua_task(conn, args, ctx) {
        Ok(crate::framework::host_tasks::PreparedHostTask::Lua(task)) => {
            crate::framework::host_tasks::run_lua_task_inline(task, conn, ctx)
        }
        Ok(_) => Value::error("ERR invalid hosted task for FCALL"),
        Err(reply) => reply,
    }
}
