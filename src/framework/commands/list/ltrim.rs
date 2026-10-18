use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("LTRIM", runtime)]
pub fn ltrim(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let max_lp_size = runtime.list_max_listpack_size();
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let parse = |b: &Bytes| {
        std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
    };
    let start = match parse(&args.args[1]) {
        Some(n) => n,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR value is not an integer or out of range",
            ))
        }
    };
    let stop = match parse(&args.args[2]) {
        Some(n) => n,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR value is not an integer or out of range",
            ))
        }
    };
    let key = &args.args[0];
    let mut delete_all = false;
    let mut delete_if_empty = false;
    {
        let list = match db.get_list_mut(key) {
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            Ok(Some(l)) => l,
            Ok(None) => return CommandOutcome::reply(Value::ok()),
        };
        let len = list.len() as i64;
        let mut s = if start < 0 { len + start } else { start };
        let mut e = if stop < 0 { len + stop } else { stop };
        if s < 0 {
            s = 0;
        }
        if s > e || s >= len {
            delete_all = true;
        } else {
            if e >= len {
                e = len - 1;
            }
            let ltrim = s as usize;
            let rtrim = len as usize - e as usize - 1;
            list.note_encoding_before_shrink(max_lp_size);
            list.drain(..ltrim);
            let keep = list.len();
            if rtrim > 0 && keep >= rtrim {
                list.drain(keep - rtrim..);
            }
            if list.is_empty() {
                delete_if_empty = true;
            } else if ltrim > 0 || rtrim > 0 {
                list.update_encoding_after_shrink(max_lp_size);
            }
        }
    }
    if delete_all || delete_if_empty {
        db.del(key);
    }
    CommandOutcome {
        reply: Value::ok(),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "ltrim",
                key: key.clone(),
            }],
            ..CommandEffects::default()
        },
    }
}
