use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("HGET", runtime)]
pub fn hget(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key, field] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("hget"));
    };
    let read_expired_values = runtime.read_expired_values();
    let now_ms = runtime.now_ms();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let value = match db.get_hash_object(key) {
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        Ok(None) => return CommandOutcome::reply(Value::null()),
        Ok(Some(hash)) => match hash.hash_get(field) {
            Some(value) => value,
            None => return CommandOutcome::reply(Value::null()),
        },
    };
    if !read_expired_values && db.hash_field_expired_at(key, field, now_ms) {
        return CommandOutcome::reply(Value::null());
    }
    CommandOutcome::reply(Value::bulk(value))
}
