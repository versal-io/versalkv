use super::runtime_support::{hash_field_value_propagation, NOT_INT};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::structure::string::encoding::int_encodable;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("HINCRBY", runtime)]
pub fn hincrby(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db_idx = runtime.selected_db();
    let read_expired_values = runtime.read_expired_values();
    let (max_entries, max_value_size) = runtime.hash_encoding_limits();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];
    let field = &args.args[1];
    let delta = match int_encodable(&args.args[2]) {
        Some(n) => n,
        None => return CommandOutcome::reply(Value::error(NOT_INT)),
    };
    let had_volatile_fields = db.hash_key_has_volatile_fields(key);
    let field_expiry = db.get_hash_field_expire(key, field);
    let field_expired = !read_expired_values && db.clear_expired_hash_field_ttl(key, field);
    let cur = if field_expired {
        0
    } else {
        let existing = match db.get_hash_object(key) {
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            Ok(None) => None,
            Ok(Some(hash)) => hash.hash_get(field),
        };
        match existing {
            Some(value) => match int_encodable(&value) {
                Some(value) => value,
                None => return CommandOutcome::reply(Value::error(NOT_INT)),
            },
            None => 0,
        }
    };
    let new_val = match cur.checked_add(delta) {
        Some(v) => v,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR increment or decrement would overflow",
            ))
        }
    };
    let new_value = new_val.to_string();
    if db
        .hash_set_with_limits(
            key,
            field,
            new_value.as_bytes(),
            max_entries,
            max_value_size,
        )
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
        kind: "hincrby",
        key: key.clone(),
    });
    let mut effects = CommandEffects {
        changed_keys: vec![(db_idx, key.clone())],
        events,
        expired_hash_fields: u64::from(field_expired),
        ..CommandEffects::default()
    };
    if had_volatile_fields {
        let expiry = (!field_expired).then_some(field_expiry).flatten();
        effects.skip_propagate = true;
        effects.custom_propagations.push(hash_field_value_propagation(
            key,
            field,
            new_value.as_bytes(),
            expiry,
        ));
    }
    CommandOutcome {
        reply: Value::integer(new_val),
        effects,
    }
}

#[cfg(test)]
mod tests {
    use super::hincrby;
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
            Bytes::from_static(b"HINCRBY"),
            Bytes::from_static(b"hash"),
            Bytes::from_static(b"field"),
            Bytes::from_static(b"1"),
        ]);

        let outcome = hincrby(&mut runtime, &args);
        assert_eq!(outcome.reply, crate::foundation::io::protocol::Value::integer(1));
        assert_eq!(
            outcome.effects.custom_propagations,
            vec![vec![
                Bytes::from_static(b"HSET"),
                Bytes::from_static(b"hash"),
                Bytes::from_static(b"field"),
                Bytes::from_static(b"1"),
            ]]
        );
        let payloads = outcome
            .effects
            .events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>();
        assert_eq!(payloads, vec!["hexpired", "hincrby"]);
    }
}
