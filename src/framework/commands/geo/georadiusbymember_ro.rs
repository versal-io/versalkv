use super::geohash_util::*;
use super::georadius::georadius_generic;
use super::georadiusbymember::{get_member_center, MemberCenterResult};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("GEORADIUSBYMEMBER_RO", runtime)]
pub fn georadiusbymember_ro(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    CommandOutcome::reply(georadiusbymember_ro_reply(runtime, args))
}

fn georadiusbymember_ro_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.len() < 4 {
        return CommandError::wrong_arity("georadiusbymember_ro");
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
            false,
        ),
        Ok(MemberCenterResult::NotFound) => Value::Array(Vec::new()),
        Ok(MemberCenterResult::MemberMissing) => {
            let member_str = String::from_utf8_lossy(&args.args[1]);
            Value::error(format!("ERR member {} does not exist", member_str))
        }
        Err(e) => e,
    }
}
