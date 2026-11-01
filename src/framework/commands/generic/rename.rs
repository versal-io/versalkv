use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command(
    "RENAME",
    get_keys = crate::framework::commands::key_extractors::rename_get_keys,
    runtime
)]
pub fn rename(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let src = &args.args[0];
    let dst = &args.args[1];
    let db_id = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    if src == dst {
        if db.exists(src) {
            return CommandOutcome::reply(Value::ok());
        } else {
            return CommandOutcome::reply(Value::error("ERR no such key"));
        }
    }
    if !db.exists(src) {
        return CommandOutcome::reply(Value::error("ERR no such key"));
    }
    let (val, ttl, hfe) = match db.take(src) {
        Some(entry) => entry,
        None => return CommandOutcome::reply(Value::error("ERR no such key")),
    };
    db.del(dst);
    db.set(dst.to_vec(), val);
    if let Some(expire) = ttl {
        db.set_expire(dst, expire);
    }
    db.set_hash_field_expires_map(dst.to_vec(), hfe);
    CommandOutcome {
        reply: Value::ok(),
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
