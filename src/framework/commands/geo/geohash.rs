use super::geohash_util::geohash_to_base32;
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("GEOHASH", runtime)]
pub fn geohash(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(geohash_reply(runtime, args))
}

fn geohash_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.is_empty() {
        return CommandError::wrong_arity("geohash");
    }
    if args.args.len() == 1 {
        return Value::Array(Vec::new());
    }
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let map = match db.get_zset(&args.args[0]) {
        Ok(None) => {
            let n = args.args.len() - 1;
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                out.push(Value::null());
            }
            return Value::Array(out);
        }
        Ok(Some(m)) => m,
        Err(_) => return CommandError::wrong_type(),
    };
    let mut out = Vec::new();
    for i in 1..args.args.len() {
        match map.dict().get(&args.args[i]) {
            Some(&score) => match geohash_to_base32(score as u64) {
                Some(s) => out.push(Value::bulk(Bytes::from(s))),
                None => out.push(Value::null()),
            },
            None => out.push(Value::null()),
        }
    }
    Value::Array(out)
}
