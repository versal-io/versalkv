use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::kv_engine::Db;
use crate::framework::context::ServerContext;
use crate::framework::CommandArgs;
use bytes::Bytes;
use command_macros::command;

#[derive(Clone, Copy)]
enum Condition {
    Equal,
    NotEqual,
    Missing,
    Exists,
}

fn parse_condition(args: &[Bytes], index: &mut usize) -> Option<Condition> {
    let token = args.get(*index)?;
    let (condition, width) = if token.eq_ignore_ascii_case(b"ifeq") {
        (Condition::Equal, 3)
    } else if token.eq_ignore_ascii_case(b"ifne") {
        (Condition::NotEqual, 3)
    } else if token.eq_ignore_ascii_case(b"nx") {
        (Condition::Missing, 2)
    } else if token.eq_ignore_ascii_case(b"xx") {
        (Condition::Exists, 2)
    } else {
        return None;
    };
    if args.len() - *index < width {
        return None;
    }
    *index += width;
    Some(condition)
}

pub(crate) fn exec_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    let mut index = 0;
    let mut keys = Vec::new();
    while index < args.len() {
        let key_index = index + 1;
        if parse_condition(args, &mut index).is_none() {
            return Vec::new();
        }
        keys.push((args[key_index].as_ref(), vec!["RO", "access"]));
    }
    keys
}

/// Validate all syntax before reading keys, then short-circuit on a mismatch.
/// Raw DB reads deliberately do not update keyspace hit/miss or LRU counters.
pub(crate) fn check_conditions(args: &[Bytes], db: &mut Db) -> Result<bool, &'static str> {
    let mut index = 0;
    while index < args.len() {
        parse_condition(args, &mut index).ok_or("invalid check condition syntax")?;
    }
    index = 0;
    while index < args.len() {
        let start = index;
        let condition =
            parse_condition(args, &mut index).ok_or("invalid check condition syntax")?;
        let value = db.get(&args[start + 1]);
        let matches = match condition {
            Condition::Equal | Condition::NotEqual => {
                if value.is_some_and(|value| !value.is_string()) {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
                let is_equal =
                    value.is_some_and(|value| value.as_bytes() == args[start + 2].as_ref());
                if matches!(condition, Condition::Equal) {
                    is_equal
                } else {
                    !is_equal
                }
            }
            Condition::Missing => value.is_none(),
            Condition::Exists => value.is_some(),
        };
        if !matches {
            return Ok(false);
        }
    }
    Ok(true)
}

#[command("EXEC", get_keys = exec_get_keys)]
pub fn exec(_conn: &mut dyn CommandConn, _args: &CommandArgs, _ctx: &mut ServerContext) -> Value {
    Value::error("ERR EXEC without MULTI")
}
