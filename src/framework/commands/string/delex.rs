use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("DELEX", runtime)]
pub fn delex(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key, condition, value] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("delex"));
    };
    let should_equal = if condition.eq_ignore_ascii_case(b"IFEQ") {
        true
    } else if condition.eq_ignore_ascii_case(b"IFNE") {
        false
    } else {
        return CommandOutcome::reply(CommandError::syntax_error());
    };
    super::delifeq::delete_if_value(runtime, key, value, should_equal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::core::Value;
    use crate::foundation::io::store::Robj;
    use crate::framework::commands::command_runtime::TestCommandRuntime;
    use bytes::Bytes;

    fn import_runtime() -> TestCommandRuntime {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        runtime.read_expired_values = true;
        let db = runtime.db_mut().unwrap();
        db.set(
            b"expired".to_vec(),
            Robj::string(Bytes::from_static(b"original")),
        );
        db.set_expire(b"expired", 1);
        runtime
    }

    #[test]
    fn import_ifne_equal_preserves_expired_value() {
        let mut runtime = import_runtime();
        let args = CommandArgs::from_parts(
            ["DELEX", "expired", "IFNE", "original"]
                .into_iter()
                .map(Bytes::from)
                .collect(),
        );

        let outcome = delex(&mut runtime, &args);

        assert_eq!(outcome.reply, Value::integer(0));
        assert!(outcome.effects.skip_propagate);
        assert!(outcome.effects.changed_keys.is_empty());
        assert!(outcome.effects.events.is_empty());
        assert!(outcome.effects.custom_propagations.is_empty());
        let db = runtime.db_mut().unwrap();
        assert_eq!(
            db.get_string_allow_expired(b"expired"),
            Ok(Some(b"original".as_slice()))
        );
        assert_eq!(db.get_expire(b"expired"), Some(1));
        assert!(db.expired_deletions.is_empty());
    }

    #[test]
    fn import_ifeq_match_deletes_expired_value() {
        let mut runtime = import_runtime();
        let args = CommandArgs::from_parts(
            ["DELEX", "expired", "IFEQ", "original"]
                .into_iter()
                .map(Bytes::from)
                .collect(),
        );

        let outcome = delex(&mut runtime, &args);

        assert_eq!(outcome.reply, Value::integer(1));
        assert!(!outcome.effects.skip_propagate);
        assert_eq!(
            outcome.effects.changed_keys,
            vec![(0, Bytes::from_static(b"expired"))]
        );
        assert_eq!(outcome.effects.events.len(), 1);
        assert_eq!(outcome.effects.events[0].kind, "del");
        assert_eq!(
            outcome.effects.custom_propagations,
            vec![vec![
                Bytes::from_static(b"DEL"),
                Bytes::from_static(b"expired")
            ]]
        );
        let db = runtime.db_mut().unwrap();
        assert!(db.get_allow_expired(b"expired").is_none());
        assert_eq!(db.get_expire(b"expired"), None);
        assert!(db.expired_deletions.is_empty());
    }

    #[test]
    fn import_comparisons_use_retained_expired_bytes() {
        for (condition, comparison, expected) in
            [("IFEQ", "different", 0), ("IFNE", "different", 1)]
        {
            let mut runtime = import_runtime();
            let args = CommandArgs::from_parts(
                ["DELEX", "expired", condition, comparison]
                    .into_iter()
                    .map(Bytes::from)
                    .collect(),
            );

            let outcome = delex(&mut runtime, &args);

            assert_eq!(outcome.reply, Value::integer(expected), "{condition}");
            assert_eq!(outcome.effects.skip_propagate, expected == 0);
            assert_eq!(outcome.effects.changed_keys.len(), expected as usize);
            assert_eq!(outcome.effects.events.len(), expected as usize);
            assert_eq!(outcome.effects.custom_propagations.len(), expected as usize);
            let db = runtime.db_mut().unwrap();
            assert_eq!(
                db.get_string_allow_expired(b"expired"),
                Ok((expected == 0).then_some(b"original".as_slice()))
            );
            assert_eq!(db.get_expire(b"expired"), (expected == 0).then_some(1));
            assert!(db.expired_deletions.is_empty());
        }
    }

    #[test]
    fn ordinary_expired_reads_obey_suppression_and_restore_lazy_expire() {
        for should_suppress_expire in [false, true] {
            let mut runtime = import_runtime();
            runtime.read_expired_values = false;
            runtime.suppress_lazy_expire = should_suppress_expire;
            let args = CommandArgs::from_parts(
                ["DELEX", "expired", "IFEQ", "original"]
                    .into_iter()
                    .map(Bytes::from)
                    .collect(),
            );

            let outcome = delex(&mut runtime, &args);

            assert_eq!(outcome.reply, Value::integer(0));
            assert!(outcome.effects.skip_propagate);
            assert!(outcome.effects.changed_keys.is_empty());
            assert!(outcome.effects.events.is_empty());
            assert!(outcome.effects.custom_propagations.is_empty());
            let db = runtime.db_mut().unwrap();
            assert_eq!(
                db.get_string_allow_expired(b"expired"),
                Ok(should_suppress_expire.then_some(b"original".as_slice()))
            );
            assert_eq!(db.expired_deletions.is_empty(), should_suppress_expire);
            db.expired_deletions.clear();
            db.set(b"probe".to_vec(), Robj::string("value"));
            db.set_expire(b"probe", 1);
            assert!(db.get(b"probe").is_none());
            assert!(db.get_allow_expired(b"probe").is_none());
            assert_eq!(db.expired_deletions, vec![b"probe".to_vec()]);
        }
    }

    #[test]
    fn live_values_and_missing_keys_follow_comparison_conditions() {
        for (is_present, condition, comparison, expected) in [
            (true, "IFEQ", "original", 1),
            (true, "IFEQ", "different", 0),
            (true, "IFNE", "original", 0),
            (true, "IFNE", "different", 1),
            (false, "IFEQ", "original", 0),
            (false, "IFNE", "different", 0),
        ] {
            let mut runtime = TestCommandRuntime::new(1, 1_000);
            if is_present {
                runtime
                    .db_mut()
                    .unwrap()
                    .set(b"key".to_vec(), Robj::string("original"));
            }
            let args = CommandArgs::from_parts(
                ["DELEX", "key", condition, comparison]
                    .into_iter()
                    .map(Bytes::from)
                    .collect(),
            );

            let outcome = delex(&mut runtime, &args);

            assert_eq!(
                outcome.reply,
                Value::integer(expected),
                "{condition} {comparison}"
            );
            assert_eq!(outcome.effects.skip_propagate, expected == 0);
            assert_eq!(
                runtime.db_mut().unwrap().get_string_allow_expired(b"key"),
                Ok((is_present && expected == 0).then_some(b"original".as_slice()))
            );
        }
    }

    #[test]
    fn wrong_type_restores_lazy_expire_and_preserves_object() {
        for should_read_expired in [false, true] {
            let mut runtime = TestCommandRuntime::new(1, 1_000);
            runtime.read_expired_values = should_read_expired;
            runtime.suppress_lazy_expire = true;
            let object = Robj::ListQuicklist(Default::default());
            let db = runtime.db_mut().unwrap();
            db.set(b"key".to_vec(), object.clone());
            if should_read_expired {
                db.set_expire(b"key", 1);
            }
            let args = CommandArgs::from_parts(
                ["DELEX", "key", "IFEQ", "value"]
                    .into_iter()
                    .map(Bytes::from)
                    .collect(),
            );

            let outcome = delex(&mut runtime, &args);

            assert_eq!(outcome.reply, CommandError::wrong_type());
            assert!(outcome.effects.changed_keys.is_empty());
            assert!(outcome.effects.events.is_empty());
            assert!(outcome.effects.custom_propagations.is_empty());
            let db = runtime.db_mut().unwrap();
            assert_eq!(db.get_allow_expired(b"key"), Some(&object));
            assert!(db.expired_deletions.is_empty());
            db.set(b"probe".to_vec(), Robj::string("value"));
            db.set_expire(b"probe", 1);
            assert!(db.get(b"probe").is_none());
            assert!(db.get_allow_expired(b"probe").is_none());
            assert_eq!(db.expired_deletions, vec![b"probe".to_vec()]);
        }
    }
}
