use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("ZREM", runtime)]
pub fn zrem(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];
    let zset = match db.get_zset_object_mut(key) {
        Ok(Some(zset)) => zset,
        Ok(None) => return CommandOutcome::reply(Value::integer(0)),
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    let mut removed = 0i64;
    for m in &args.args[1..] {
        if zset.zset_remove(m) {
            removed += 1;
        }
    }
    let is_empty = zset.zset_is_empty();
    let _ = zset;
    if is_empty {
        db.del(key);
    }
    if removed == 0 {
        return CommandOutcome::reply(Value::integer(0));
    }
    changed(Value::integer(removed), db_idx, key, "zrem")
}

pub(super) fn changed(
    reply: Value,
    db_idx: u32,
    key: &bytes::Bytes,
    event: &'static str,
) -> CommandOutcome {
    CommandOutcome {
        reply,
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: event,
                key: key.clone(),
            }],
            ..CommandEffects::default()
        },
    }
}
