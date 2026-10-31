
use super::runtime_support::parse_query_fields;
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("HPEXPIRETIME", runtime)]
pub fn hpexpiretime(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    CommandOutcome::reply(hpexpiretime_reply(runtime, args))
}

fn hpexpiretime_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    let now_ms = runtime.now_ms();
    let ignore_expiry = runtime.read_expired_values();
    if args.args.len() < 2 {
        return CommandError::wrong_arity("hpexpiretime");
    }
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let key = &args.args[0];

    let (consumed, fields) = match parse_query_fields(&args.args, 1) {
        Ok(v) => v,
        Err(e) => return e,
    };
    if 1 + consumed != args.args.len() {
        return CommandError::syntax_error();
    }

    let key_exists = {
        let hash = match db.get_hash_object(key) {
            Ok(Some(hash)) => hash,
            Ok(None) => {
                let mut out = Vec::with_capacity(fields.len());
                for _ in 0..fields.len() {
                    out.push(Value::integer(-2));
                }
                return Value::Array(out);
            }
            Err(_) => return CommandError::wrong_type(),
        };
        let mut exists = Vec::with_capacity(fields.len());
        for field in &fields {
            exists.push(hash.hash_contains(field.as_ref()));
        }
        exists
    };

    let mut out = Vec::with_capacity(fields.len());
    for (i, field) in fields.iter().enumerate() {
        if !key_exists[i] {
            out.push(Value::integer(-2));
        } else {
            let ms = if !ignore_expiry && db.hash_field_expired_at(key, field, now_ms) {
                -2
            } else {
                db.get_hash_field_expire(key, field).map_or(-1, |ms| ms as i64)
            };
            out.push(Value::integer(ms));
        }
    }
    Value::Array(out)
}
