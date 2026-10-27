use super::geohash_util::{format_distance, geohash_decode, geohash_distance, unit_to_meters};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("GEODIST", runtime)]
pub fn geodist(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(geodist_reply(runtime, args))
}

fn geodist_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.len() < 3 {
        return CommandError::wrong_arity("geodist");
    }
    let mut to_meters = 1.0f64;
    if args.args.len() >= 4 {
        if args.args.len() > 4 {
            return CommandError::syntax_error();
        }
        match unit_to_meters(&args.args[4 - 1]) {
            Some(f) => to_meters = f,
            None => return Value::error("ERR unsupported unit provided. please use M, KM, FT, MI"),
        }
    }
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let map = match db.get_zset_object(&args.args[0]) {
        Ok(None) => return Value::null(),
        Ok(Some(m)) => m,
        Err(_) => return CommandError::wrong_type(),
    };
    let score1 = match map.zset_score(&args.args[1]) {
        Some(s) => s,
        None => return Value::null(),
    };
    let score2 = match map.zset_score(&args.args[2]) {
        Some(s) => s,
        None => return Value::null(),
    };
    let xy1 = match geohash_decode(score1 as u64) {
        Some(p) => p,
        None => return Value::null(),
    };
    let xy2 = match geohash_decode(score2 as u64) {
        Some(p) => p,
        None => return Value::null(),
    };
    let dist = geohash_distance(xy1.0, xy1.1, xy2.0, xy2.1) / to_meters;
    Value::bulk(Bytes::from(format_distance(dist)))
}
