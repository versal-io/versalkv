use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("SREM", runtime)]
pub fn srem(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let Some((key, members)) = args.args.split_first() else {
        return CommandOutcome::reply(CommandError::wrong_arity("srem"));
    };
    if members.is_empty() {
        return CommandOutcome::reply(CommandError::wrong_arity("srem"));
    }
    let db_index = runtime.selected_db();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let Some(set) = (match db.get_set_mut(key) {
        Ok(set) => set,
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    }) else {
        return CommandOutcome::reply(Value::integer(0));
    };
    let removed = members
        .iter()
        .filter(|member| set.data.remove(*member).is_some())
        .count() as i64;
    let empty = set.data.is_empty();
    if empty {
        db.del(key);
    }
    CommandOutcome {
        reply: Value::integer(removed),
        effects: CommandEffects {
            changed_keys: if removed > 0 {
                vec![(db_index, key.clone())]
            } else {
                Vec::new()
            },
            events: if removed > 0 {
                vec![CoreEvent {
                    kind: "srem",
                    key: key.clone(),
                }]
            } else {
                Vec::new()
            },
            ..CommandEffects::default()
        },
    }
}
