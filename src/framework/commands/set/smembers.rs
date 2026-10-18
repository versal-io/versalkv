use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("SMEMBERS", runtime)]
pub fn smembers(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("smembers"));
    };
    let resp3 = runtime.resp3();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let members = match db.get_set(key) {
        Ok(Some(set)) => {
            let mut members: Vec<_> = set.data.keys().cloned().collect();
            members.sort();
            members
        }
        Ok(None) => Vec::new(),
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    let values = members.into_iter().map(Value::bulk).collect();
    CommandOutcome::reply(if resp3 {
        Value::Set(values)
    } else {
        Value::Array(values)
    })
}
