
use super::runtime_support::parse_fields;
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("HTTL", runtime)]
pub fn httl(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(httl_reply(runtime, args))
}

fn httl_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("httl");
    }
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let key = &args.args[0];

    let (consumed, fields) = match parse_fields(&args.args, 1) {
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
            let ms = db.hash_field_ttl_ms(key, field.as_ref());
            if ms < 0 {
                out.push(Value::integer(ms));
            } else {
                out.push(Value::integer((ms + 500) / 1000));
            }
        }
    }
    Value::Array(out)
}
