//!       `EVALSHA_RO <sha1> <numkeys> [key ...] [arg ...]`

use command_macros::command;

use crate::framework::CommandArgs;
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;

#[command("EVAL_RO", get_keys = crate::framework::commands::server::command::getkeys_helpers::eval_ro_get_keys, hosted = crate::framework::host_tasks::prepare_lua_task)]
pub fn eval_ro(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    run_inline(conn, args, ctx)
}

#[command("EVALSHA_RO", get_keys = crate::framework::commands::server::command::getkeys_helpers::eval_ro_get_keys, hosted = crate::framework::host_tasks::prepare_lua_task)]
pub fn evalsha_ro(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    run_inline(conn, args, ctx)
}

fn run_inline(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    match crate::framework::host_tasks::prepare_lua_task(conn, args, ctx) {
        Ok(crate::framework::host_tasks::PreparedHostTask::Lua(task)) => {
            crate::framework::host_tasks::run_lua_task_inline(task, conn, ctx)
        }
        Ok(_) => Value::error("ERR invalid hosted task for EVAL_RO"),
        Err(reply) => reply,
    }
}

