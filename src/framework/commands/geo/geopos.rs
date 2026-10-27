use super::geohash_util::geohash_decode;
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

fn fmt_coord(v: f64) -> Bytes {
    if v.is_infinite() {
        return Bytes::from_static(if v > 0.0 { b"inf" } else { b"-inf" });
    }
    if v == v.trunc() && v.abs() < 1e17 {
        return Bytes::from(format!("{}", v as i64));
    }
    Bytes::from(
        format!("{:.17}", v)
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string(),
    )
}

#[command("GEOPOS", runtime)]
pub fn geopos(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(geopos_reply(runtime, args))
}

fn geopos_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.is_empty() {
        return CommandError::wrong_arity("geopos");
    }
    if args.args.len() == 1 {
        return Value::Array(Vec::new());
    }
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let map = match db.get_zset_object(&args.args[0]) {
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
        match map.zset_score(&args.args[i]) {
            Some(score) => match geohash_decode(score as u64) {
                Some((lon, lat)) => {
                    out.push(Value::Array(vec![
                        Value::bulk(fmt_coord(lon)),
                        Value::bulk(fmt_coord(lat)),
                    ]));
                }
                None => out.push(Value::null()),
            },
            None => out.push(Value::null()),
        }
    }
    Value::Array(out)
}
