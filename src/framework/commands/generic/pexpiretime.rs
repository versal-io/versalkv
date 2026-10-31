use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::CommandArgs;
use command_macros::command;

#[command("PEXPIRETIME", runtime)]
pub fn pexpiretime(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    super::expiretime::expiretime_impl(runtime, args, true)
}

#[cfg(test)]
mod tests {
    use super::pexpiretime;
    use crate::engine::command::CommandArgs;
    use crate::engine::core::Value;
    use crate::foundation::io::store::Robj;
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
    use bytes::Bytes;

    fn key_args(key: &'static [u8]) -> CommandArgs {
        CommandArgs::from_parts(vec![
            Bytes::from_static(b"PEXPIRETIME"),
            Bytes::from_static(key),
        ])
    }

    #[test]
    fn import_source_reads_expired_absolute_milliseconds_without_deleting() {
        for deadline in [0, 1, 1_500] {
            let mut runtime = TestCommandRuntime::new(1, 2_000);
            runtime.read_expired_values = true;
            let db = runtime.db_mut().unwrap();
            db.set(b"key".to_vec(), Robj::string("value"));
            db.set_expire(b"key", deadline);

            let outcome = pexpiretime(&mut runtime, &key_args(b"key"));

            assert_eq!(outcome.reply, Value::integer(deadline as i64));
            assert!(outcome.effects.changed_keys.is_empty());
            assert!(outcome.effects.custom_propagations.is_empty());
            let db = runtime.db_mut().unwrap();
            assert_eq!(db.get_allow_expired(b"key").unwrap().as_bytes(), b"value");
            assert_eq!(db.get_expire(b"key"), Some(deadline));
            assert!(db.expired_deletions.is_empty());
        }
    }

    #[test]
    fn ordinary_import_reads_hide_expired_milliseconds_without_deleting() {
        let mut runtime = TestCommandRuntime::new(1, 2_000);
        runtime.suppress_lazy_expire = true;
        let db = runtime.db_mut().unwrap();
        db.set(b"key".to_vec(), Robj::string("value"));
        db.set_expire(b"key", 1_500);

        let outcome = pexpiretime(&mut runtime, &key_args(b"key"));

        assert_eq!(outcome.reply, Value::integer(-2));
        let db = runtime.db_mut().unwrap();
        assert_eq!(db.get_allow_expired(b"key").unwrap().as_bytes(), b"value");
        assert_eq!(db.get_expire(b"key"), Some(1_500));
        assert!(db.expired_deletions.is_empty());
        assert!(!db.exists_at(b"key", 2_000));
        assert_eq!(db.expired_deletions, vec![b"key".to_vec()]);
    }

    #[test]
    fn absolute_milliseconds_preserve_live_deadlines_and_sentinels() {
        let mut runtime = TestCommandRuntime::new(1, 2_000);
        assert_eq!(
            pexpiretime(&mut runtime, &key_args(b"missing")).reply,
            Value::integer(-2)
        );
        runtime
            .db_mut()
            .unwrap()
            .set(b"key".to_vec(), Robj::string("value"));
        assert_eq!(
            pexpiretime(&mut runtime, &key_args(b"key")).reply,
            Value::integer(-1)
        );
        runtime
            .db_mut()
            .unwrap()
            .set_expire(b"key", 4_000_000_000_500);
        assert_eq!(
            pexpiretime(&mut runtime, &key_args(b"key")).reply,
            Value::integer(4_000_000_000_500)
        );
        runtime.selected_db = 1;
        assert_eq!(
            pexpiretime(&mut runtime, &key_args(b"key")).reply,
            crate::engine::command::CommandError::db_index_out_of_range()
        );
    }
}
