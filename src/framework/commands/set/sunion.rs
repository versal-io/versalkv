use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;
use std::collections::HashSet;

#[command("SUNION", runtime)]
pub fn sunion(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(sunion_reply(runtime, args))
}

fn sunion_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    let Some(db) = runtime.db_mut() else {
        return CommandError::db_index_out_of_range();
    };
        let mut result: HashSet<Bytes> = HashSet::new();

        for key in &args.args[0..] {
            match db.get_set(key) {
                Ok(Some(s)) => {
                    for m in s.data.keys() {
                        result.insert(m.clone());
                    }
                }
                Ok(None) => continue,
                Err(_) => return CommandError::wrong_type(),
            }
        }

        Value::Array(result.into_iter().map(Value::bulk).collect())
}
