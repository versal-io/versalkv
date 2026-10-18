use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::zset::basic::score_value;
use crate::framework::commands::zset::zunionstore::zunionstore;
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
    let mut withscores = false;
    let mut store_args: Vec<Bytes> = Vec::new();
    let mut i = 0;
    while i < args.args.len() {
        if args.args[i].eq_ignore_ascii_case(b"withscores") {
            withscores = true;
            i += 1;
        } else {
            store_args.push(args.args[i].clone());
            i += 1;
        }
    }
    let tmp_key = Bytes::from("__tmp_zunion__");
    let mut full_store_args = vec![tmp_key.clone()];
    full_store_args.extend(store_args);
    let tmp_args = CommandArgs {
        name: Bytes::from_static(b"zunionstore"),
        args: full_store_args,
    };
    let resp3 = runtime.resp3();
    let store_result = zunionstore(runtime, &tmp_args);
    if matches!(&store_result.reply, Value::Error(_)) {
        return store_result;
    }
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let result = match db.get_zset_object(&tmp_key) {
        Ok(None) => Value::Array(Vec::new()),
        Ok(Some(m)) => {
            let entries = m.zset_entries();
            let mut out = Vec::new();
            for entry in &entries {
                if withscores && resp3 {
                    out.push(Value::Array(vec![
                        Value::bulk(entry.0.clone()),
                        score_value(entry.1, resp3),
                    ]));
                } else {
                    out.push(Value::bulk(entry.0.clone()));
                    if withscores {
                        out.push(score_value(entry.1, resp3));
                    }
                }
            }
            Value::Array(out)
        }
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    db.del(&tmp_key);
    CommandOutcome::reply(result)
}
