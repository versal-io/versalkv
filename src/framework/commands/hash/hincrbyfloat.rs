use super::runtime_support::{
    format_float, hash_field_value_propagation, parse_float, NOT_FLOAT,
};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("HINCRBYFLOAT", runtime)]
pub fn hincrbyfloat(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    let db_idx = runtime.selected_db();
    let read_expired_values = runtime.read_expired_values();
    let (max_entries, max_value_size) = runtime.hash_encoding_limits();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];
    let field = &args.args[1];
    let incr = match parse_float(&args.args[2]) {
        Some(f) => f,
        None => return CommandOutcome::reply(Value::error(NOT_FLOAT)),
    };
    if incr.is_nan() || incr.is_infinite() {
        return CommandOutcome::reply(Value::error("ERR value is NaN or Infinity"));
    }
    let field_expiry = db.get_hash_field_expire(key, field);
    let field_expired = !read_expired_values && db.clear_expired_hash_field_ttl(key, field);
    let cur = if field_expired {
        0.0f64
    } else {
        let existing = match db.get_hash_object(key) {
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            Ok(None) => None,
            Ok(Some(hash)) => hash.hash_get(field),
        };
        match existing {
            Some(value) => match parse_float(&value) {
                Some(f) => f,
                None => return CommandOutcome::reply(Value::error(NOT_FLOAT)),
            },
            None => 0.0f64,
        }
    };
    let new_val = cur + incr;
    if new_val.is_nan() || new_val.is_infinite() {
        return CommandOutcome::reply(Value::error("ERR value is NaN or Infinity"));
    }
    let s = format_float(new_val);
    if db
        .hash_set_with_limits(key, field, s.as_bytes(), max_entries, max_value_size)
        .is_err()
    {
        return CommandOutcome::reply(CommandError::wrong_type());
    }
    let mut events = Vec::new();
    if field_expired {
        events.push(CoreEvent {
            kind: "hexpired",
            key: key.clone(),
        });
    }
    events.push(CoreEvent {
        kind: "hincrbyfloat",
        key: key.clone(),
    });
    let expiry = (!field_expired).then_some(field_expiry).flatten();
    CommandOutcome {
        reply: Value::bulk(bytes::Bytes::from(s.clone())),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events,
            skip_propagate: true,
            custom_propagations: vec![hash_field_value_propagation(
                key,
                field,
                s.as_bytes(),
                expiry,
            )],
            expired_hash_fields: u64::from(field_expired),
            ..CommandEffects::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::hincrbyfloat;
    use crate::engine::command::CommandArgs;
    use crate::foundation::io::store::kv_engine::now_ms;
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
    use bytes::Bytes;

    #[test]
    fn expired_field_is_recreated_and_propagated_as_final_hset() {
        let now_ms = now_ms();
        let mut runtime = TestCommandRuntime::new(1, now_ms);
        let db = runtime.db_mut().unwrap();
        db.get_or_create_hash(b"hash")
            .unwrap()
            .insert(Bytes::from_static(b"field"), Bytes::from_static(b"1"));
        db.set_hash_field_expire(b"hash", b"field", now_ms.saturating_sub(1));
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"HINCRBYFLOAT"),
            Bytes::from_static(b"hash"),
            Bytes::from_static(b"field"),
            Bytes::from_static(b"1.5"),
        ]);

        let outcome = hincrbyfloat(&mut runtime, &args);
        assert_eq!(outcome.reply, crate::foundation::io::protocol::Value::bulk(b"1.5".as_slice()));
        assert_eq!(
            outcome.effects.custom_propagations,
            vec![vec![
                Bytes::from_static(b"HSET"),
                Bytes::from_static(b"hash"),
                Bytes::from_static(b"field"),
                Bytes::from_static(b"1.5"),
            ]]
        );
        let payloads = outcome
            .effects
            .events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>();
        assert_eq!(payloads, vec!["hexpired", "hincrbyfloat"]);
    }
}
