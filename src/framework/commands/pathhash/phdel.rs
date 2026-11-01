use command_macros::command;

use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;

use super::runtime_support::write_outcome;

#[command("PHDEL", runtime)]
pub fn phdel(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 2 {
        return CommandOutcome::reply(CommandError::wrong_arity("phdel"));
    }
    let key = &args.args[0];
    let path = &args.args[1];
    let db_index = runtime.selected_db();
    let should_read_expired_values = runtime.read_expired_values();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let deleted = match db.mutate_path_hash(key, false, should_read_expired_values, |tree| {
        if args.args.len() == 2 {
            usize::from(tree.delete_path(path))
        } else {
            tree.delete_fields(path, &args.args[2..])
        }
    }) {
        Ok(deleted) => deleted.unwrap_or(0),
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    write_outcome(
        db_index,
        key,
        "phdel",
        deleted,
        Value::integer(deleted as i64),
    )
}
