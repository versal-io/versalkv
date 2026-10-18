use super::runtime_support::hash_field_value_propagation;
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("HSETNX", runtime)]
pub fn hsetnx(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db_idx = runtime.selected_db();
    let read_expired_values = runtime.read_expired_values();
    let (max_entries, max_value_size) = runtime.hash_encoding_limits();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];
    let field = &args.args[1];
    let value = &args.args[2];
    let field_expired = !read_expired_values && db.clear_expired_hash_field_ttl(key, field);
    let field_exists = match db.get_hash_object(key) {
        Ok(Some(hash)) => hash.hash_contains(field),
        Ok(None) => false,
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    if !field_expired && field_exists {
        return CommandOutcome {
            reply: Value::integer(0),
            effects: CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            },
        };
    }
    if db
        .hash_set_with_limits(key, field, value, max_entries, max_value_size)
        .is_err()
    {
        return CommandOutcome::reply(CommandError::wrong_type());
    }
    let mut events = Vec::new();
    let mut custom_propagations = Vec::new();
    if field_expired {
        events.push(CoreEvent {
            kind: "hexpired",
            key: key.clone(),
        });
        custom_propagations.push(hash_field_value_propagation(key, field, value, None));
    }
    events.push(CoreEvent {
        kind: "hset",
        key: key.clone(),
    });
    CommandOutcome {
        reply: Value::integer(1),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events,
            skip_propagate: field_expired,
            custom_propagations,
            expired_hash_fields: u64::from(field_expired),
            ..CommandEffects::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::hsetnx;
    use crate::engine::command::CommandArgs;
    use crate::foundation::io::protocol::Value;
    use crate::foundation::io::store::kv_engine::now_ms;
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
    use bytes::Bytes;

    #[test]
    fn expired_field_is_recreated_and_propagated_as_hset() {
        let now_ms = now_ms();
        let mut runtime = TestCommandRuntime::new(1, now_ms);
        let db = runtime.db_mut().unwrap();
        db.get_or_create_hash(b"hash")
            .unwrap()
            .insert(Bytes::from_static(b"field"), Bytes::from_static(b"old"));
        db.set_hash_field_expire(b"hash", b"field", now_ms.saturating_sub(1));
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"HSETNX"),
            Bytes::from_static(b"hash"),
            Bytes::from_static(b"field"),
            Bytes::from_static(b"new"),
        ]);

        let outcome = hsetnx(&mut runtime, &args);
        assert_eq!(outcome.reply, Value::integer(1));
        assert_eq!(
            outcome.effects.custom_propagations,
            vec![vec![
                Bytes::from_static(b"HSET"),
                Bytes::from_static(b"hash"),
                Bytes::from_static(b"field"),
                Bytes::from_static(b"new"),
            ]]
        );
        let payloads = outcome
            .effects
            .events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>();
        assert_eq!(payloads, vec!["hexpired", "hset"]);
    }
}
