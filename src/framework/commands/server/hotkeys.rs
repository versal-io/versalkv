use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("HOTKEYS")]
pub fn hotkeys(_conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    let Some(subcommand) = args.args.first() else {
        return CommandError::wrong_arity("hotkeys");
    };
    if args.args.len() != 1 {
        return Value::error("ERR unknown subcommand or wrong number of arguments. Try HOTKEYS HELP.");
    }
    if subcommand.eq_ignore_ascii_case(b"get") {
        Value::Array(ctx.hotkeys().get().into_iter().map(|entry| {
            Value::Map(vec![
                (Value::bulk("key"), Value::bulk(entry.key)),
                (Value::bulk("db"), Value::Integer(i64::from(entry.db_id))),
                (Value::bulk("qps"), Value::Integer(entry.qps.min(i64::MAX as u64) as i64)),
            ])
        }).collect())
    } else if subcommand.eq_ignore_ascii_case(b"reset") {
        ctx.hotkeys().reset();
        Value::ok()
    } else if subcommand.eq_ignore_ascii_case(b"help") {
        Value::Array([
            "HOTKEYS <subcommand>. Subcommands are:",
            "GET",
            "    Return the hottest keys of the last completed window, ordered by",
            "    estimated accesses per second (descending). Each entry reports the",
            "    key name, the database it was accessed in, and the estimated QPS.",
            "RESET",
            "    Clear all collected hot key statistics.",
        ].into_iter().map(Value::bulk).collect())
    } else {
        Value::error("ERR unknown subcommand or wrong number of arguments. Try HOTKEYS HELP.")
    }
}
