use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("DELIFEQ", runtime)]
pub fn delifeq(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let key = &args.args[0];
    let value = &args.args[1];
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    match db.get_string(key) {
        Err(_) => CommandOutcome::reply(CommandError::wrong_type()),
        Ok(Some(bytes)) if bytes == value.as_ref() => {
            db.del(key);
            CommandOutcome {
                reply: Value::integer(1),
                effects: CommandEffects {
                    changed_keys: vec![(db_idx, key.clone())],
                    events: vec![CoreEvent {
                        kind: "del",
                        key: key.clone(),
                    }],
                    custom_propagations: vec![vec![
                        Bytes::from_static(b"DEL"),
                        key.clone(),
                    ]],
                    ..CommandEffects::default()
                },
            }
        }
        Ok(None) | Ok(Some(_)) => CommandOutcome {
            reply: Value::integer(0),
            effects: CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            },
        },
    }
}
