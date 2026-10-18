use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::stream::runtime_support::parse_stream_id;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("XACK", runtime)]
pub fn xack(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 3 {
        return CommandOutcome::reply(CommandError::wrong_arity("xack"));
    }
    let mut ids = Vec::with_capacity(args.args.len() - 2);
    for raw in &args.args[2..] {
        match parse_stream_id(raw, false) {
            Ok(id) => ids.push(id),
            Err(_) => {
                return CommandOutcome::reply(Value::error(
                    "ERR Invalid stream ID specified as stream command argument",
                ))
            }
        }
    }
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let stream = match db.get_stream_mut(&args.args[0]) {
        Ok(Some(stream)) => stream,
        Ok(None) => return CommandOutcome::reply(Value::integer(0)),
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    let acked = stream.ack_group_pending(&args.args[1], &ids).unwrap_or(0);
    if acked == 0 {
        return CommandOutcome::reply(Value::integer(0));
    }
    CommandOutcome {
        reply: Value::integer(acked as i64),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, args.args[0].clone())],
            events: vec![CoreEvent {
                kind: "xack",
                key: args.args[0].clone(),
            }],
            ..CommandEffects::default()
        },
    }
}
