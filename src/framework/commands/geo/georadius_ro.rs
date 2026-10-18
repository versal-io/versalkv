use super::geohash_util::*;
use super::georadius::georadius_generic;
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("GEORADIUS_RO", runtime)]
pub fn georadius_ro(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    CommandOutcome::reply(georadius_ro_reply(runtime, args))
}

fn georadius_ro_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.len() < 5 {
        return CommandError::wrong_arity("georadius_ro");
    }
    let lon = match parse_double(&args.args[1]) {
        Some(v) => v,
        None => return Value::error("ERR invalid longitude"),
    };
    let lat = match parse_double(&args.args[2]) {
        Some(v) => v,
        None => return Value::error("ERR invalid latitude"),
    };
    if !(GEO_LONG_MIN..=GEO_LONG_MAX).contains(&lon) || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat) {
        return Value::error(format!(
            "ERR invalid longitude,latitude pair {:.6},{:.6}",
            lon, lat
        ));
    }
    let radius_raw = match parse_double(&args.args[3]) {
        Some(v) if v >= 0.0 => v,
        _ => return Value::error("ERR radius cannot be negative"),
    };
    georadius_generic(
        args,
        runtime,
        lon,
        lat,
        radius_raw,
        4,
        5,
        false,
    )
}
