use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::zset::basic;
use crate::framework::commands::zset::zunionstore;
use bytes::Bytes;
use command_macros::command;

#[command("ZUNION", runtime, get_keys = crate::framework::commands::key_extractors::numkeys_ro_get_keys)]
pub fn zunion(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 2 {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    let _numkeys = match std::str::from_utf8(&args.args[0])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        Some(n) if n > 0 => n,
        _ => {
            return CommandOutcome::reply(Value::error(
                "ERR at least 1 input key is needed for 'zunion' command",
            ))
        }
    };
    let mut has_scores = false;
    let mut store_args: Vec<Bytes> = Vec::new();
    let mut i = 0;
    while i < args.args.len() {
        if args.args[i].eq_ignore_ascii_case(b"withscores") {
            has_scores = true;
            i += 1;
        } else {
            store_args.push(args.args[i].clone());
            i += 1;
        }
    }
    let (key_weights, aggregate) = match basic::parse_set_operation_args(&store_args, 0, "zunion") {
        Ok(parsed) => parsed,
        Err(error) => return CommandOutcome::reply(error),
    };
    let is_resp3 = runtime.resp3();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let result = match zunionstore::collect_union(db, &key_weights, aggregate) {
        Ok(result) => result,
        Err(error) => return CommandOutcome::reply(error),
    };
    CommandOutcome::reply(basic::set_operation_reply(result, has_scores, is_resp3))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::engine::command::CommandArgs;
    use crate::engine::core::{CommandOutcome, Value};
    use crate::foundation::io::store::kv_engine;
    use crate::framework::commands::command_runtime;
    use crate::framework::commands::command_runtime::TestCommandRuntime;

    fn execute(runtime: &mut TestCommandRuntime, parts: &[&str]) -> CommandOutcome {
        let args = CommandArgs::from_parts(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part.as_bytes()))
                .collect(),
        );
        command_runtime::execute(runtime, &args)
    }

    #[test]
    fn readonly_set_operations_preserve_existing_keys_and_expirations() {
        for (command, old_temporary_key) in
            [("ZUNION", "__tmp_zunion__"), ("ZINTER", "__tmp_zinter__")]
        {
            let now_ms = kv_engine::now_ms();
            let mut runtime = TestCommandRuntime::new(1, now_ms);
            assert_eq!(
                execute(&mut runtime, &["SET", old_temporary_key, "valuable"]).reply,
                Value::ok()
            );
            assert_eq!(
                execute(&mut runtime, &["PEXPIRE", old_temporary_key, "60000"]).reply,
                Value::integer(1)
            );
            assert_eq!(
                execute(&mut runtime, &["ZADD", "source", "2", "b", "1", "a"]).reply,
                Value::integer(2)
            );
            let expiration = runtime
                .dbs
                .get(0)
                .expect("database zero")
                .expire_of(old_temporary_key.as_bytes());

            let outcome = execute(&mut runtime, &[command, "1", "source", "WITHSCORES"]);

            assert_eq!(
                outcome.reply,
                Value::Array(vec![
                    Value::bulk("a"),
                    Value::bulk("1"),
                    Value::bulk("b"),
                    Value::bulk("2"),
                ])
            );
            assert!(
                outcome.effects.changed_keys.is_empty(),
                "read-only operation changed keys"
            );
            assert!(
                outcome.effects.events.is_empty(),
                "read-only operation emitted write events"
            );
            assert_eq!(
                execute(&mut runtime, &["GET", old_temporary_key]).reply,
                Value::bulk("valuable")
            );
            assert_eq!(
                runtime
                    .dbs
                    .get(0)
                    .expect("database zero")
                    .expire_of(old_temporary_key.as_bytes()),
                expiration
            );

            let empty = execute(&mut runtime, &[command, "1", "missing"]);
            assert_eq!(empty.reply, Value::Array(Vec::new()));
            assert_eq!(
                execute(&mut runtime, &["GET", old_temporary_key]).reply,
                Value::bulk("valuable")
            );
        }
    }

    #[test]
    fn readonly_set_operations_preserve_inputs_named_like_former_temporary_keys() {
        for (command, key) in [("ZUNION", "__tmp_zunion__"), ("ZINTER", "__tmp_zinter__")] {
            let mut runtime = TestCommandRuntime::new(1, kv_engine::now_ms());
            assert_eq!(
                execute(&mut runtime, &["ZADD", key, "3", "member"]).reply,
                Value::integer(1)
            );
            assert_eq!(
                execute(&mut runtime, &[command, "1", key]).reply,
                Value::Array(vec![Value::bulk("member")])
            );
            assert_eq!(
                execute(&mut runtime, &["ZSCORE", key, "member"]).reply,
                Value::bulk("3")
            );
        }
    }
}
