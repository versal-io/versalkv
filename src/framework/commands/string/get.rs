use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("GET", runtime)]
pub fn get(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("get"));
    };
    let read_expired_values = runtime.read_expired_values();
    let suppress_lazy_expire = runtime.suppress_lazy_expire();
    let should_touch = runtime.should_touch_read_keys();
    let now_ms = runtime.now_ms();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    db.set_no_lazy_expire(suppress_lazy_expire);
    let value = if read_expired_values {
        db.get_allow_expired(key).cloned()
    } else {
        db.get_at(key, now_ms).cloned()
    };
    if should_touch && value.is_some() {
        db.touch_access_time(key);
    }
    db.set_no_lazy_expire(false);
    match value {
        Some(value) if value.is_string() => {
            CommandOutcome::reply(Value::bulk(value.as_bytes().to_vec()))
        }
        Some(_) => CommandOutcome::reply(CommandError::wrong_type()),
        None => CommandOutcome {
            reply: Value::null(),
            effects: CommandEffects {
                events: vec![CoreEvent {
                    kind: "keymiss",
                    key: key.clone(),
                }],
                ..CommandEffects::default()
            },
        },
    }
}
