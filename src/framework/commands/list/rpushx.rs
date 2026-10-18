use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

fn pushx(runtime: &mut dyn CommandRuntime, args: &CommandArgs, left: bool) -> CommandOutcome {
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];
    let list = match db.get_list_mut(key) {
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        Ok(Some(l)) => l,
        Ok(None) => return skipped(),
    };
    for v in &args.args[1..] {
        if left {
            list.push_front(v.clone());
        } else {
            list.push_back(v.clone());
        }
    }
    CommandOutcome {
        reply: Value::integer(list.len() as i64),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "rpush",
                key: key.clone(),
            }],
            ..CommandEffects::default()
        },
    }
}

fn skipped() -> CommandOutcome {
    CommandOutcome {
        reply: Value::integer(0),
        effects: CommandEffects {
            skip_propagate: true,
            ..CommandEffects::default()
        },
    }
}

#[command("RPUSHX", runtime)]
pub fn rpushx(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    pushx(runtime, args, false)
}
