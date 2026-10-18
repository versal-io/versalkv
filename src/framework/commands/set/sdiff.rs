use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("SDIFF", runtime)]
pub fn sdiff(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(sdiff_reply(runtime, args))
}

fn sdiff_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    let Some(db) = runtime.db_mut() else {
        return CommandError::db_index_out_of_range();
    };
        for key in &args.args[0..] {
            if db.get_set(key).is_err() {
                return CommandError::wrong_type();
            }
        }

        let first_set = match db.get_set(&args.args[0]) {
            Ok(Some(s)) => s.data.clone(),
            Ok(None) => return Value::Array(Vec::new()),
            Err(_) => return CommandError::wrong_type(),
        };

        let mut result = first_set;

        for key in &args.args[1..] {
            match db.get_set(key) {
                Ok(Some(s)) => {
                    for m in s.data.keys() {
                        result.remove(m);
                    }
                }
                Ok(None) => continue,
                Err(_) => return CommandError::wrong_type(),
            }
        }

        Value::Array(result.keys().cloned().map(Value::bulk).collect())
}
