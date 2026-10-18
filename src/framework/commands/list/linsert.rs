use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("LINSERT", runtime)]
pub fn linsert(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];
    let where_str = match std::str::from_utf8(&args.args[1]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return CommandOutcome::reply(CommandError::syntax_error()),
    };
    let before = match where_str.as_str() {
        "BEFORE" => true,
        "AFTER" => false,
        _ => return CommandOutcome::reply(CommandError::syntax_error()),
    };
    let pivot = &args.args[2];
    let element = &args.args[3];
    let list = match db.get_list_mut(key) {
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        Ok(Some(l)) => l,
        Ok(None) => return skipped(Value::integer(0)),
    };
    let pos = list.iter().position(|x| x == pivot);
    let reply = match pos {
        None => return skipped(Value::integer(-1)),
        Some(idx) => {
            let insert_idx = if before { idx } else { idx + 1 };
            list.insert(insert_idx, element.clone());
            Value::integer(list.len() as i64)
        }
    };
    CommandOutcome {
        reply,
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "linsert",
                key: key.clone(),
            }],
            ..CommandEffects::default()
        },
    }
}

fn skipped(reply: Value) -> CommandOutcome {
    CommandOutcome {
        reply,
        effects: CommandEffects {
            skip_propagate: true,
            ..CommandEffects::default()
        },
    }
}
