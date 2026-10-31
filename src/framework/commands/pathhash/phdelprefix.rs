use command_macros::command;

use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;

use super::runtime_support::write_outcome;

#[command("PHDELPREFIX", runtime)]
pub fn phdelprefix(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key, prefix] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("phdelprefix"));
    };
    let db_index = runtime.selected_db();
    let should_read_expired_values = runtime.read_expired_values();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let deleted = match db.mutate_path_hash(key, false, should_read_expired_values, |tree| {
        tree.delete_prefix(prefix)
    }) {
        Ok(deleted) => deleted.unwrap_or(0),
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    write_outcome(
        db_index,
        key,
        "phdelprefix",
        deleted,
        Value::integer(deleted as i64),
    )
}
