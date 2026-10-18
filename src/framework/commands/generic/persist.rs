use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("PERSIST", runtime)]
pub fn persist(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("persist"));
    };
    let db_index = runtime.selected_db();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let persisted = db.persist(key);
    let mut outcome = CommandOutcome::reply(Value::integer(i64::from(persisted)));
    if persisted {
        outcome.effects.changed_keys.push((db_index, key.clone()));
    }
    outcome
}
