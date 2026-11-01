use crate::engine::core::{CommandOutcome, Value};
use crate::foundation::io::store::kv_engine::Db;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("HEXISTS", runtime)]
pub fn hexists(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let active_expire_enabled = runtime.active_expire_enabled();
    let read_expired_values = runtime.read_expired_values();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];
    let field = &args.args[1];
    let (reply, _) =
        hexists_reply(db, key, field, active_expire_enabled, read_expired_values);
    CommandOutcome::reply(reply)
}

fn hexists_reply(
    db: &mut Db,
    key: &[u8],
    field: &[u8],
    _active_expire_enabled: bool,
    read_expired_values: bool,
) -> (Value, bool) {
    let exists = match db.get_hash_object(key) {
        Err(_) => return (CommandError::wrong_type(), false),
        Ok(None) => return (Value::integer(0), false),
        Ok(Some(hash)) => hash.hash_contains(field),
    };
    if !exists {
        return (Value::integer(0), false);
    }
    let is_expired = !read_expired_values && db.hash_field_expired(key, field);
    if !is_expired {
        return (Value::integer(1), false);
    }
    (Value::integer(0), false)
}

#[cfg(test)]
mod tests {
    use super::hexists_reply;
    use crate::foundation::io::protocol::Value;
    use crate::foundation::io::store::kv_engine::now_ms;
    use bytes::Bytes;

    #[test]
    fn active_expiry_disabled_hides_but_retains_an_expired_field() {
        let mut db = crate::foundation::io::store::kv_engine::Db::new();
        db.get_or_create_hash(b"hash")
            .unwrap()
            .insert(Bytes::from_static(b"field"), Bytes::from_static(b"value"));
        db.set_hash_field_expire(b"hash", b"field", now_ms().saturating_sub(1));
        assert_eq!(
            hexists_reply(&mut db, b"hash", b"field", false, false),
            (Value::integer(0), false)
        );
        assert!(db.get_allow_expired(b"hash").is_some());
        assert!(db
            .get_hash_field_expire(b"hash", b"field")
            .is_some());
    }
}
