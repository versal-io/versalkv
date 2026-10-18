use super::geohash_util::*;
use super::georadius::georadius_generic;
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

pub enum MemberCenterResult {
    NotFound,
    MemberMissing,
    Ok(f64, f64),
}

pub fn get_member_center(
    args: &CommandArgs,
    runtime: &mut dyn CommandRuntime,
    key_pos: usize,
    member_pos: usize,
) -> Result<MemberCenterResult, Value> {
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return Err(CommandError::db_index_out_of_range()),
    };
    let score = match db.get_zset(&args.args[key_pos]) {
        Ok(None) => return Ok(MemberCenterResult::NotFound),
        Ok(Some(m)) => match m.dict().get(&args.args[member_pos]) {
            Some(&s) => s,
            None => return Ok(MemberCenterResult::MemberMissing),
        },
        Err(_) => return Err(CommandError::wrong_type()),
    };
    match geohash_decode(score as u64) {
        Some(p) => Ok(MemberCenterResult::Ok(p.0, p.1)),
        None => Err(Value::error("ERR could not decode requested zset member")),
    }
}

fn handle_not_found(
    args: &CommandArgs,
    runtime: &mut dyn CommandRuntime,
    key_pos: usize,
    opt_start: usize,
    allow_store: bool,
) -> Value {
    let (_, storekey) =
        parse_radius_optional_args(args, opt_start, allow_store, "georadiusbymember")
            .unwrap_or((SearchResultOptions::default(), None));
    if let Some((skey, _)) = storekey {
        let db = runtime.db_mut().unwrap();
        db.del(&skey);
        let _ = key_pos;
        return Value::integer(0);
    }
    Value::Array(Vec::new())
}

#[command(
    "GEORADIUSBYMEMBER",
    runtime,
    get_keys = crate::framework::commands::key_extractors::georadius_get_keys
)]
pub fn georadiusbymember(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    CommandOutcome::reply(georadiusbymember_reply(runtime, args))
}

fn georadiusbymember_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.len() < 4 {
        return CommandError::wrong_arity("georadiusbymember");
    }
    let radius_raw = match parse_double(&args.args[2]) {
        Some(v) if v >= 0.0 => v,
        _ => return Value::error("ERR radius cannot be negative"),
    };
    match get_member_center(args, runtime, 0, 1) {
        Ok(MemberCenterResult::Ok(lon, lat)) => georadius_generic(
            args,
            runtime,
            lon,
            lat,
            radius_raw,
            3,
            4,
            true,
        ),
        Ok(MemberCenterResult::NotFound) => handle_not_found(args, runtime, 0, 4, true),
        Ok(MemberCenterResult::MemberMissing) => {
            let member_str = String::from_utf8_lossy(&args.args[1]);
            Value::error(format!("ERR member {} does not exist", member_str))
        }
        Err(e) => e,
    }
}
