use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command(
    "RENAMENX",
    get_keys = crate::framework::commands::key_extractors::renamenx_get_keys,
    runtime
)]
pub fn renamenx(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let src = &args.args[0];
    let dst = &args.args[1];
    let db_id = runtime.selected_db();
    if src == dst {
        return CommandOutcome::reply(Value::integer(0));
    }
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    if !db.exists(src) {
        return CommandOutcome::reply(Value::error("ERR no such key"));
    }
    if db.exists(dst) {
        return CommandOutcome::reply(Value::integer(0));
    }
    let val = match db.get(src) {
        Some(v) => v.clone(),
        None => return CommandOutcome::reply(Value::integer(0)),
    };
    let ttl = db.get_expire(src);
    db.del(src);
    db.set(dst.to_vec(), val);
    if let Some(expire) = ttl {
        db.set_expire(dst, expire);
    }
    CommandOutcome {
        reply: Value::integer(1),
        effects: CommandEffects {
            changed_keys: vec![(db_id, src.clone()), (db_id, dst.clone())],
            events: vec![
                CoreEvent {
                    kind: "rename_from",
                    key: src.clone(),
                },
                CoreEvent {
                    kind: "rename_to",
                    key: dst.clone(),
                },
            ],
            ..CommandEffects::default()
        },
    }
}
