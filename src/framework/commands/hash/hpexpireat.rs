use super::hexpire::{hash_expire, HashExpireMode};
use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::CommandArgs;
use command_macros::command;

#[command("HPEXPIREAT", runtime)]
pub fn hpexpireat(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    hash_expire(runtime, args, HashExpireMode::UnixMilliseconds)
}

#[cfg(test)]
mod tests {
    use super::hpexpireat;
    use crate::engine::command::CommandArgs;
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
    use crate::foundation::io::protocol::Value;
    use bytes::Bytes;

    #[test]
    fn replication_source_installs_past_expiry_without_deleting_field() {
        let mut runtime = TestCommandRuntime::new(1, 1);
        runtime
            .db_mut()
            .unwrap()
            .get_or_create_hash(b"hash")
            .unwrap()
            .insert(Bytes::from_static(b"field"), Bytes::from_static(b"value"));
        runtime.read_expired_values = true;
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"HPEXPIREAT"),
            Bytes::from_static(b"hash"),
            Bytes::from_static(b"0"),
            Bytes::from_static(b"FIELDS"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"field"),
        ]);

        assert_eq!(
            hpexpireat(&mut runtime, &args).reply,
            Value::Array(vec![Value::integer(1)])
        );
        assert!(runtime
            .db_mut()
            .unwrap()
            .get_hash(b"hash")
            .unwrap()
            .is_some_and(|hash| hash.contains_key(b"field".as_slice())));
        assert_eq!(
            runtime
                .db_mut()
                .unwrap()
                .get_hash_field_expire(b"hash", b"field"),
            Some(0)
        );
    }
}
