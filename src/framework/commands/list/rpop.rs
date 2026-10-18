use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("RPOP", runtime)]
pub fn rpop(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let (key, count) = match args.args.as_slice() {
        [key] => (key, None),
        [key, count] => match std::str::from_utf8(count)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
        {
            Some(value) if value >= 0 => (key, Some(value as usize)),
            Some(_) => {
                return CommandOutcome::reply(Value::error(
                    "ERR value is out of range, must be positive",
                ))
            }
            None => {
                return CommandOutcome::reply(Value::error(
                    "ERR value is not an integer or out of range",
                ))
            }
        },
        _ => return CommandOutcome::reply(CommandError::wrong_arity("rpop")),
    };
    let db_index = runtime.selected_db();
    let max_listpack_size = runtime.list_max_listpack_size();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let Some(list) = (match db.get_list_mut(key) {
        Ok(list) => list,
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    }) else {
        return CommandOutcome::reply(if count.is_some() {
            Value::null_array()
        } else {
            Value::null()
        });
    };
    list.note_encoding_before_shrink(max_listpack_size);
    let value = match count {
        None => list
            .pop_back()
            .map_or_else(Value::null, |value| Value::bulk(value.to_vec())),
        Some(count) => Value::Array(
            (0..count)
                .map_while(|_| list.pop_back())
                .map(|value| Value::bulk(value.to_vec()))
                .collect(),
        ),
    };
    let is_empty = list.is_empty();
    if is_empty {
        db.del(key);
    } else {
        list.update_encoding_after_shrink(max_listpack_size);
    }
    match value {
        Value::Null | Value::NullArray => CommandOutcome::reply(value),
        reply => CommandOutcome {
            reply,
            effects: CommandEffects {
                changed_keys: vec![(db_index, key.clone())],
                events: vec![CoreEvent {
                    kind: "rpop",
                    key: key.clone(),
                }],
                ..CommandEffects::default()
            },
        },
    }
}
