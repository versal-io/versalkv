use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("EXISTS", runtime)]
pub fn exists(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.is_empty() {
        return CommandOutcome::reply(CommandError::wrong_arity("exists"));
    }
    let now_ms = runtime.now_ms();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    CommandOutcome::reply(Value::integer(
        args.args
            .iter()
            .filter(|key| db.exists_at(key, now_ms))
            .count() as i64,
    ))
}
