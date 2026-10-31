use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("HSTRLEN", runtime)]
pub fn hstrlen(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let now_ms = runtime.now_ms();
    let ignore_expiry = runtime.read_expired_values();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];
    let field = &args.args[1];
    let value = match db.get_hash_object(key) {
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        Ok(None) => None,
        Ok(Some(hash)) => hash.hash_get(field),
    };
    let len = value
        .filter(|_| ignore_expiry || !db.hash_field_expired_at(key, field, now_ms))
        .map_or(0, |value| value.len());
    CommandOutcome::reply(Value::integer(len as i64))
}
