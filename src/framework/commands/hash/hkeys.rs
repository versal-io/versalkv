use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("HKEYS", runtime)]
pub fn hkeys(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    CommandOutcome::reply(match db.get_hash_object(&args.args[0]) {
        Err(_) => CommandError::wrong_type(),
        Ok(None) => Value::Array(Vec::new()),
        Ok(Some(hash)) => {
            let entries = hash.hash_entries();
            let mut out: Vec<Value> = Vec::with_capacity(entries.len());
            for (field, _) in entries {
                out.push(Value::bulk(field));
            }
            Value::Array(out)
        }
    })
}
