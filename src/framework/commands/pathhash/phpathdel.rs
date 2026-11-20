use command_macros::command;

use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;

use super::runtime_support::{is_option, write_outcome};

#[command("PHPATHDEL", runtime)]
pub fn phpathdel(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 2 {
        return CommandOutcome::reply(CommandError::wrong_arity("phpathdel"));
    }
    let has_subpaths = args.args.len() == 3 && is_option(&args.args[2], b"WITHSUBPATHS");
    if args.args.len() != 2 && !has_subpaths {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    let key = &args.args[0];
    let path = &args.args[1];
    let db_index = runtime.selected_db();
    let should_read_expired_values = runtime.read_expired_values();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let deleted = match db.mutate_path_hash(key, false, should_read_expired_values, |tree| {
        if has_subpaths {
            tree.delete_prefix(path)
        } else {
            usize::from(tree.delete_path(path))
        }
    }) {
        Ok(deleted) => deleted.unwrap_or(0),
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    let is_key_removed = deleted > 0 && db.get_allow_expired(key).is_none();
    let mut outcome = write_outcome(
        db_index,
        key,
        "phpathdel",
        deleted,
        Value::integer(deleted as i64),
    );
    if is_key_removed {
        outcome.effects.events.push(crate::engine::core::CoreEvent {
            kind: "del",
            key: key.clone(),
        });
    }
    outcome
}
