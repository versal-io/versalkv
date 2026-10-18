use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("LPOS", runtime)]
pub fn lpos(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(lpos_reply(runtime, args))
}

fn lpos_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let key = &args.args[0];
    let element = &args.args[1];
    let mut rank: i64 = 1;
    let mut count: Option<i64> = None;
    let mut maxlen: i64 = 0;
    let mut i = 2;
    while i < args.args.len() {
        let opt = match std::str::from_utf8(&args.args[i]) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => return CommandError::syntax_error(),
        };
        match opt.as_str() {
            "COUNT" => {
                if i + 1 >= args.args.len() {
                    return CommandError::syntax_error();
                }
                let n = match std::str::from_utf8(&args.args[i + 1])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(n) if n >= 0 => n,
                    _ => return Value::error("ERR value is not an integer or out of range"),
                };
                count = Some(n);
                i += 2;
            }
            "RANK" => {
                if i + 1 >= args.args.len() {
                    return CommandError::syntax_error();
                }
                let n = match std::str::from_utf8(&args.args[i + 1])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(0) => {
                        return Value::error(
                            "ERR RANK can't be zero: use 1 to start from \
                             the first match, 2 from the second ... \
                             or use negative to start from the end of the list",
                        )
                    }
                    Some(i64::MIN) => {
                        return Value::error("ERR value is out of range");
                    }
                    Some(n) => n,
                    _ => return Value::error("ERR value is not an integer or out of range"),
                };
                rank = n;
                i += 2;
            }
            "MAXLEN" => {
                if i + 1 >= args.args.len() {
                    return CommandError::syntax_error();
                }
                let n = match std::str::from_utf8(&args.args[i + 1])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(n) if n >= 0 => n,
                    _ => return Value::error("ERR value is not an integer or out of range"),
                };
                maxlen = n;
                i += 2;
            }
            _ => return CommandError::syntax_error(),
        }
    }
    let list = match db.get_list(key) {
        Err(_) => return CommandError::wrong_type(),
        Ok(Some(l)) => l,
        Ok(None) => {
            return if count.is_some() {
                Value::Array(Vec::new())
            } else {
                Value::null()
            };
        }
    };

    let from_tail = rank < 0;
    let rank_abs = rank.unsigned_abs() as usize;
    let maxlen_usize = if maxlen == 0 {
        usize::MAX
    } else {
        maxlen as usize
    };

    match count {
        None => {
            let mut matches = 0;
            let mut result: Option<usize> = None;
            let iter: Box<dyn Iterator<Item = (usize, &Bytes)>> = if from_tail {
                Box::new(list.iter().enumerate().rev())
            } else {
                Box::new(list.iter().enumerate())
            };
            for (idx, x) in iter.take(maxlen_usize) {
                if x == element {
                    matches += 1;
                    if matches >= rank_abs {
                        result = Some(idx);
                        break;
                    }
                }
            }
            match result {
                Some(idx) => Value::integer(idx as i64),
                None => Value::null(),
            }
        }
        Some(c) => {
            let mut out = Vec::new();
            let mut matches = 0;
            let count_usize = if c == 0 { usize::MAX } else { c as usize };
            let iter: Box<dyn Iterator<Item = (usize, &Bytes)>> = if from_tail {
                Box::new(list.iter().enumerate().rev())
            } else {
                Box::new(list.iter().enumerate())
            };
            for (idx, x) in iter.take(maxlen_usize) {
                if x == element {
                    matches += 1;
                    if matches >= rank_abs {
                        out.push(Value::integer(idx as i64));
                        if out.len() >= count_usize {
                            break;
                        }
                    }
                }
            }
            Value::Array(out)
        }
    }
}
