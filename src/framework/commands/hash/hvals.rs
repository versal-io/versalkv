use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("HVALS", runtime)]
pub fn hvals(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let now_ms = runtime.now_ms();
    let ignore_expiry = runtime.read_expired_values();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];
    let entries = match db.get_hash_object(key) {
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        Ok(None) => return CommandOutcome::reply(Value::Array(Vec::new())),
        Ok(Some(hash)) => hash.hash_entries(),
    };
    let mut out = Vec::with_capacity(entries.len());
    for (field, value) in entries {
        if ignore_expiry || !db.hash_field_expired_at(key, &field, now_ms) {
            out.push(Value::bulk(value));
        }
    }
    CommandOutcome::reply(Value::Array(out))
}
