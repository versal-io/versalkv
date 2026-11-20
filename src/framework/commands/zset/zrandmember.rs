use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::CommandOutcome;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::zset::basic::{score_value, simple_random_usize};
use bytes::Bytes;
use command_macros::command;

#[command("ZRANDMEMBER", runtime)]
pub fn zrandmember(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.is_empty() {
        return CommandOutcome::reply(CommandError::wrong_arity("zrandmember"));
    }
    let has_count = args.args.len() >= 2;
    let resp3 = runtime.resp3();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let zset = match db.get_zset_object_mut(&args.args[0]) {
        Ok(None) => {
            if has_count {
                return CommandOutcome::reply(Value::Array(Vec::new()));
            } else {
                return CommandOutcome::reply(Value::null());
            }
        }
        Ok(Some(m)) => m,
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    if !has_count {
        if zset.zset_is_empty() {
            return CommandOutcome::reply(Value::null());
        }
        return CommandOutcome::reply(
            random_entry(zset, None)
                .map(|(member, _)| Value::bulk(member))
                .unwrap_or_else(Value::null),
        );
    }
    let count = match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(n) => n,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR value is not an integer or out of range",
            ))
        }
    };
    if count == i64::MIN {
        return CommandOutcome::reply(Value::error("ERR value is out of range"));
    }
    let abs_count = count.unsigned_abs() as usize;
    if abs_count > 1_000_000_000 {
        return CommandOutcome::reply(Value::error("ERR value is out of range"));
    }
    let mut withscores = false;
    if args.args.len() == 3 {
        if args.args[2].eq_ignore_ascii_case(b"withscores") {
            withscores = true;
        } else {
            return CommandOutcome::reply(CommandError::syntax_error());
        }
    } else if args.args.len() > 3 {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    if zset.zset_is_empty() || count == 0 {
        return CommandOutcome::reply(Value::Array(Vec::new()));
    }
    let abs_count = count.unsigned_abs() as usize;
    // Decode listpack entries once per command, rather than once per sample.
    let listpack_entries = matches!(zset, Robj::ZSetListpack(_)).then(|| zset.zset_entries());
    let result: Vec<(Bytes, f64)> = if count == 1 || (count > 0 && abs_count < zset.zset_len() / 3)
    {
        let mut selected = std::collections::HashSet::with_capacity(abs_count);
        let mut result = Vec::with_capacity(abs_count);
        while result.len() < abs_count {
            let Some(entry) = random_entry(zset, listpack_entries.as_deref()) else {
                break;
            };
            if selected.insert(entry.0.clone()) {
                result.push(entry);
            }
        }
        result
    } else if count >= 0 {
        let entries = listpack_entries.unwrap_or_else(|| zset.zset_entries());
        let mut indices: Vec<usize> = (0..entries.len()).collect();
        for i in (1..indices.len()).rev() {
            let j = simple_random_usize(i + 1);
            indices.swap(i, j);
        }
        let take = abs_count.min(entries.len());
        indices[..take]
            .iter()
            .map(|&i| entries[i].clone())
            .collect()
    } else {
        (0..abs_count)
            .filter_map(|_| random_entry(zset, listpack_entries.as_deref()))
            .collect()
    };
    let mut out = Vec::new();
    for entry in &result {
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
    CommandOutcome::reply(Value::Array(out))
}

fn random_entry(
    zset: &mut Robj,
    listpack_entries: Option<&[(Bytes, f64)]>,
) -> Option<(Bytes, f64)> {
    if let Robj::ZSetSkiplist(tree) = zset {
        tree.random_entry()
    } else if let Some(entries) = listpack_entries {
        entries.get(simple_random_usize(entries.len())).cloned()
    } else {
        let entries = zset.zset_entries();
        entries.get(simple_random_usize(entries.len())).cloned()
    }
}
