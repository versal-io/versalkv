use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("SADD", runtime)]
pub fn sadd(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let Some((key, members)) = args.args.split_first() else {
        return CommandOutcome::reply(CommandError::wrong_arity("sadd"));
    };
    if members.is_empty() {
        return CommandOutcome::reply(CommandError::wrong_arity("sadd"));
    }
    let db_index = runtime.selected_db();
    let limits = runtime.set_encoding_limits();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let set = match db.get_or_create_set(key) {
        Ok(set) => set,
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    let added = members
        .iter()
        .filter(|member| set.insert_with_encoding((*member).clone(), limits.0, limits.1, limits.2))
        .count() as i64;
    CommandOutcome {
        reply: Value::integer(added),
        effects: CommandEffects {
            changed_keys: if added > 0 {
                vec![(db_index, key.clone())]
            } else {
                Vec::new()
            },
            events: vec![CoreEvent {
                kind: "sadd",
                key: key.clone(),
            }],
            ..CommandEffects::default()
        },
    }
}
