
use super::runtime_support::parse_query_fields;
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("HEXPIRETIME", runtime)]
pub fn hexpiretime(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    CommandOutcome::reply(hexpiretime_reply(runtime, args))
}

fn hexpiretime_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("hexpiretime");
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
            let ms = db.hash_field_expiretime_ms(key, field.as_ref());
            if ms < 0 {
                out.push(Value::integer(ms));
            } else {
                out.push(Value::integer((ms + 500) / 1000));
            }
        }
    }
    Value::Array(out)
}

#[cfg(test)]
mod tests {
    use super::hexpiretime;
    use crate::foundation::io::protocol::Value;
    use crate::foundation::io::store::kv_engine::now_ms;
    #[cfg(not(feature = "wasm"))]
    use crate::foundation::io::sys::time::enter_command_time_snapshot;
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
    use crate::framework::commands::hash::hpexpiretime::hpexpiretime;
    use crate::framework::CommandArgs;
    use bytes::Bytes;

    #[test]
    fn expiry_seconds_round_at_half_second_boundary() {
        #[cfg(not(feature = "wasm"))]
        let _time = enter_command_time_snapshot(1_000_000);
        let now_ms = now_ms();
        let base_ms = (now_ms / 1_000 + 1) * 1_000;
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"HEXPIRETIME"),
            Bytes::from_static(b"hash"),
            Bytes::from_static(b"FIELDS"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"field"),
        ]);
        for (remainder_ms, rounded_increment_sec) in [
            (0, 0),
            (499, 0),
            (500, 1),
            (999, 1),
        ] {
            let expire_at_ms = base_ms + remainder_ms;
            let expected_sec = base_ms as i64 / 1_000 + rounded_increment_sec;
            let mut runtime = TestCommandRuntime::new(1, now_ms);
            let db = runtime.db_mut().unwrap();
            db.get_or_create_hash(b"hash")
                .unwrap()
                .insert(Bytes::from_static(b"field"), Bytes::from_static(b"value"));
            db.set_hash_field_expire(b"hash", b"field", expire_at_ms);

            assert_eq!(
                hpexpiretime(&mut runtime, &args).reply,
                Value::Array(vec![Value::integer(expire_at_ms as i64)])
            );
            assert_eq!(
                hexpiretime(&mut runtime, &args).reply,
                Value::Array(vec![Value::integer(expected_sec)]),
                "expiry at {expire_at_ms} ms"
            );
        }
    }

    #[test]
    fn expiry_seconds_preserve_missing_and_persistent_field_sentinels() {
        #[cfg(not(feature = "wasm"))]
        let _time = enter_command_time_snapshot(1_000_000);
        let now_ms = now_ms();
        let mut runtime = TestCommandRuntime::new(1, now_ms);
        let db = runtime.db_mut().unwrap();
        let hash = db.get_or_create_hash(b"hash").unwrap();
        hash.insert(Bytes::from_static(b"persistent"), Bytes::from_static(b"value"));
        hash.insert(Bytes::from_static(b"expired"), Bytes::from_static(b"value"));
        db.set_hash_field_expire(b"hash", b"expired", now_ms - 1);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"HEXPIRETIME"),
            Bytes::from_static(b"hash"),
            Bytes::from_static(b"FIELDS"),
            Bytes::from_static(b"3"),
            Bytes::from_static(b"persistent"),
            Bytes::from_static(b"expired"),
            Bytes::from_static(b"missing"),
        ]);

        assert_eq!(
            hexpiretime(&mut runtime, &args).reply,
            Value::Array(vec![Value::integer(-1), Value::integer(-2), Value::integer(-2)])
        );
        runtime.db_mut().unwrap().del(b"hash");
        assert_eq!(
            hexpiretime(&mut runtime, &args).reply,
            Value::Array(vec![Value::integer(-2); 3])
        );
    }
}
