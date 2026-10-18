use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("SINTER", runtime)]
pub fn sinter(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(sinter_reply(runtime, args))
}

fn sinter_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    let Some(db) = runtime.db_mut() else {
        return CommandError::db_index_out_of_range();
    };
        for key in &args.args[0..] {
            if db.get_set(key).is_err() {
                return CommandError::wrong_type();
            }
        }

        let mut result = None;

        for key in &args.args[0..] {
            match db.get_set(key) {
                Ok(None) => return Value::Array(Vec::new()),
                Ok(Some(s)) => {
                    if result.is_none() {
                        result = Some(s.data.clone());
                    } else if let Some(ref mut r) = result {
                        let to_remove: Vec<bytes::Bytes> = r
                            .keys()
                            .filter(|m| !s.data.contains_key(*m))
                            .cloned()
                            .collect();
                        for k in to_remove {
                            r.remove(&k);
                        }
                    }
                }
                Err(_) => return CommandError::wrong_type(),
            }
        }

        let result = result.unwrap_or_default();
        Value::Array(result.keys().cloned().map(Value::bulk).collect())
}
