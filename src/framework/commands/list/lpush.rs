use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("LPUSH", runtime)]
pub fn lpush(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let Some((key, values)) = args.args.split_first() else {
        return CommandOutcome::reply(CommandError::wrong_arity("lpush"));
    };
    if values.is_empty() {
        return CommandOutcome::reply(CommandError::wrong_arity("lpush"));
    }
    let db_index = runtime.selected_db();
    let max_listpack_size = runtime.list_max_listpack_size();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let list = match db.get_or_create_list(key) {
        Ok(list) => list,
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    for value in values {
        list.push_front(value.clone());
    }
    list.update_encoding_after_grow(max_listpack_size);
    CommandOutcome {
        reply: Value::integer(list.len() as i64),
        effects: CommandEffects {
            changed_keys: vec![(db_index, key.clone())],
            events: vec![CoreEvent {
                kind: "lpush",
                key: key.clone(),
            }],
            ..CommandEffects::default()
        },
    }
}
