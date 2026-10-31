use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("EXPIRETIME", runtime)]
pub fn expiretime(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    expiretime_impl(runtime, args, false)
}

pub(crate) fn expiretime_impl(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
    milliseconds: bool,
) -> CommandOutcome {
    let read_expired_values = runtime.read_expired_values();
    let suppress_lazy_expire = runtime.suppress_lazy_expire();
    let now_ms = runtime.now_ms();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];
    db.set_no_lazy_expire(suppress_lazy_expire);
    let has_key = if read_expired_values {
        db.get_allow_expired(key).is_some()
    } else {
        db.exists_at(key, now_ms)
    };
    let ms = if has_key {
        db.get_expire(key).map_or(-1, |deadline| deadline as i64)
    } else {
        -2
    };
    db.set_no_lazy_expire(false);
    CommandOutcome::reply(if milliseconds || ms < 0 {
        Value::integer(ms)
    } else {
        Value::integer(ms / 1_000 + i64::from(ms % 1_000 >= 500))
    })
}

#[cfg(test)]
mod tests {
    use super::expiretime;
    use crate::engine::command::CommandArgs;
    use crate::engine::core::Value;
    use crate::foundation::io::store::Robj;
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
    use bytes::Bytes;

    fn key_args(key: &'static [u8]) -> CommandArgs {
        CommandArgs::from_parts(vec![
            Bytes::from_static(b"EXPIRETIME"),
            Bytes::from_static(key),
        ])
    }

    #[test]
    fn import_source_reads_expired_absolute_seconds_without_deleting() {
        let mut runtime = TestCommandRuntime::new(1, 2_000);
        runtime.read_expired_values = true;
        let db = runtime.db_mut().unwrap();
        db.set(b"key".to_vec(), Robj::string("value"));
        db.set_expire(b"key", 1_500);

        let outcome = expiretime(&mut runtime, &key_args(b"key"));

        assert_eq!(outcome.reply, Value::integer(2));
        assert!(outcome.effects.changed_keys.is_empty());
        assert!(outcome.effects.custom_propagations.is_empty());
        let db = runtime.db_mut().unwrap();
        assert_eq!(db.get_allow_expired(b"key").unwrap().as_bytes(), b"value");
        assert_eq!(db.get_expire(b"key"), Some(1_500));
        assert!(db.expired_deletions.is_empty());
    }

    #[test]
    fn ordinary_import_reads_hide_expired_seconds_without_deleting() {
        let mut runtime = TestCommandRuntime::new(1, 2_000);
        runtime.suppress_lazy_expire = true;
        let db = runtime.db_mut().unwrap();
        db.set(b"key".to_vec(), Robj::string("value"));
        db.set_expire(b"key", 1_500);

        let outcome = expiretime(&mut runtime, &key_args(b"key"));

        assert_eq!(outcome.reply, Value::integer(-2));
        let db = runtime.db_mut().unwrap();
        assert_eq!(db.get_allow_expired(b"key").unwrap().as_bytes(), b"value");
        assert_eq!(db.get_expire(b"key"), Some(1_500));
        assert!(db.expired_deletions.is_empty());
        assert!(!db.exists_at(b"key", 2_000));
        assert_eq!(db.expired_deletions, vec![b"key".to_vec()]);
    }

    #[test]
    fn absolute_seconds_round_at_the_half_second_boundary() {
        for (deadline, expected) in [
            (4_000_000_000_499, 4_000_000_000),
            (4_000_000_000_500, 4_000_000_001),
        ] {
            let mut runtime = TestCommandRuntime::new(1, 2_000);
            let db = runtime.db_mut().unwrap();
            db.set(b"key".to_vec(), Robj::string("value"));
            db.set_expire(b"key", deadline);

            assert_eq!(
                expiretime(&mut runtime, &key_args(b"key")).reply,
                Value::integer(expected)
            );
            assert_eq!(runtime.db_mut().unwrap().get_expire(b"key"), Some(deadline));
        }
    }

    #[test]
    fn absolute_seconds_preserve_missing_and_persistent_sentinels() {
        let mut runtime = TestCommandRuntime::new(1, 2_000);
        assert_eq!(
            expiretime(&mut runtime, &key_args(b"missing")).reply,
            Value::integer(-2)
        );
        runtime
            .db_mut()
            .unwrap()
            .set(b"persistent".to_vec(), Robj::string("value"));
        assert_eq!(
            expiretime(&mut runtime, &key_args(b"persistent")).reply,
            Value::integer(-1)
        );
        runtime.selected_db = 1;
        assert_eq!(
            expiretime(&mut runtime, &key_args(b"persistent")).reply,
            crate::engine::command::CommandError::db_index_out_of_range()
        );
    }
}
