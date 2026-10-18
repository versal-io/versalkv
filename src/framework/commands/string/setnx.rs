use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("SETNX", runtime)]
pub fn setnx(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let key = &args.args[0];
    let db_idx = runtime.selected_db();
    let existed = {
        let db = match runtime.db_mut() {
            Some(d) => d,
            None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
        };
        if db.exists(key) {
            true
        } else {
            let val = crate::foundation::io::store::Robj::string(args.args[1].clone());
            db.set(key.to_vec(), val);
            false
        }
    };
    if existed {
        CommandOutcome {
            reply: Value::integer(0),
            effects: CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            },
        }
    } else {
        CommandOutcome {
            reply: Value::integer(1),
            effects: CommandEffects {
                changed_keys: vec![(db_idx, key.clone())],
                events: vec![CoreEvent {
                    kind: "set",
                    key: key.clone(),
                }],
                ..CommandEffects::default()
            },
        }
    }
}
