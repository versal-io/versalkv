use crate::engine::core::{CommandOutcome, Value};
use crate::foundation::io::sys::rand;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("SRANDMEMBER", runtime)]
pub fn srandmember(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    CommandOutcome::reply(srandmember_reply(runtime, args))
}

fn srandmember_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    let Some(db) = runtime.db_mut() else {
        return CommandError::db_index_out_of_range();
    };
        let key = &args.args[0];

        if args.args.len() == 1 {
            return match db.get_set(key) {
                Ok(Some(s)) => {
                    if s.data.is_empty() {
                        Value::Null
                    } else {
                        let members: Vec<Bytes> = s.data.keys().cloned().collect();
                        let idx = rand::random_usize(members.len());
                        Value::bulk(members[idx].clone())
                    }
                }
                Ok(None) => Value::Null,
                Err(_) => CommandError::wrong_type(),
            };
        }

        let count_str = match std::str::from_utf8(&args.args[1]) {
            Ok(s) => s,
            Err(_) => return Value::error("ERR value is not an integer or out of range"),
        };
        let count_val: i128 = match count_str.parse() {
            Ok(v) => v,
            Err(_) => return Value::error("ERR value is not an integer or out of range"),
        };
        if count_val < i64::MIN as i128 || count_val > i64::MAX as i128 {
            return Value::error("ERR value is out of range");
        }
        if count_val.to_string() != count_str {
            return Value::error("ERR value is not an integer or out of range");
        }
        let count = count_val as i64;

        let set = match db.get_set(key) {
            Ok(Some(s)) => s,
            Ok(None) => return Value::Array(Vec::new()),
            Err(_) => return CommandError::wrong_type(),
        };

        if set.data.is_empty() {
            return Value::Array(Vec::new());
        }

        let members: Vec<Bytes> = set.data.keys().cloned().collect();

        if count >= 0 {
            let abs_count = count as usize;
            let mut indices: Vec<usize> = (0..members.len()).collect();
            rand::shuffle(&mut indices);
            let take = abs_count.min(members.len());
            Value::Array(
                indices[..take]
                    .iter()
                    .map(|&i| Value::bulk(members[i].clone()))
                    .collect(),
            )
        } else {
            let abs_count = count.unsigned_abs();
            if abs_count > i32::MAX as u64 {
                return Value::error("ERR value is out of range");
            }
            let abs_count = abs_count as usize;
            Value::Array(
                (0..abs_count)
                    .map(|_| {
                        let idx = rand::random_usize(members.len());
                        Value::bulk(members[idx].clone())
                    })
                    .collect(),
            )
        }
}
