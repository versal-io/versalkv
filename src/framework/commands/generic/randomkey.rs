use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::CommandArgs;
use command_macros::command;

#[command("RANDOMKEY", runtime)]
pub fn randomkey(
    runtime: &mut dyn CommandRuntime,
    _args: &CommandArgs,
) -> CommandOutcome {
    let no_lazy_expire = runtime.suppress_lazy_expire();
    runtime.reset_replication_selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(Value::null()),
    };
    db.set_no_lazy_expire(no_lazy_expire);
    let result = match db.random_key() {
        Some(key) => Value::bulk(key.to_vec()),
        None => Value::null(),
    };
    db.set_no_lazy_expire(false);
    CommandOutcome::reply(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::store::Robj;
    use crate::framework::commands::command_runtime::TestCommandRuntime;
    use bytes::Bytes;

    #[test]
    fn randomkey_command_does_not_expire_the_returned_key_early() {
        #[cfg(not(feature = "wasm"))]
        let _time = crate::foundation::io::sys::time::enter_command_time_snapshot(1_000);
        let now = u64::try_from(crate::foundation::io::sys::time::mstime()).unwrap();
        let mut runtime = TestCommandRuntime::new(1, now);
        let db = runtime.dbs.get_mut(0).unwrap();
        db.set(b"future".to_vec(), Robj::string("value"));
        db.set_expire(b"future", now + 50);
        let args = CommandArgs::from_parts(vec![Bytes::from_static(b"RANDOMKEY")]);

        assert_eq!(randomkey(&mut runtime, &args).reply, Value::bulk("future"));
        let db = runtime.dbs.get(0).unwrap();
        assert!(db.get_allow_expired(b"future").is_some());
        assert_eq!(db.get_expire(b"future"), Some(now + 50));
        assert!(db.expired_deletions.is_empty());
    }
}
