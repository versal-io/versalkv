use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("MGET", runtime)]
pub fn mget(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.is_empty() {
        return CommandOutcome::reply(CommandError::wrong_arity("mget"));
    }
    let now_ms = runtime.now_ms();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(Value::Array(vec![Value::null(); args.len()]));
    };
    let mut values = Vec::with_capacity(args.len());
    let mut events = Vec::new();
    for key in &args.args {
        match db.get_at(key, now_ms) {
            Some(value) if value.is_string() => values.push(Value::bulk(value.as_bytes().to_vec())),
            Some(_) => values.push(Value::null()),
            None => {
                values.push(Value::null());
                events.push(CoreEvent {
                    kind: "keymiss",
                    key: key.clone(),
                });
            }
        }
    }
    CommandOutcome {
        reply: Value::Array(values),
        effects: CommandEffects {
            events,
            ..CommandEffects::default()
        },
    }
}
