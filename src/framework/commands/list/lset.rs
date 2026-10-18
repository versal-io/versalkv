use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::structure::list::{would_grow_exceed_limit, would_shrink_fit_limit};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("LSET", runtime)]
pub fn lset(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
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
    let index = match parse(&args.args[1]) {
        Some(n) => n,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR value is not an integer or out of range",
            ))
        }
    };
    let key = &args.args[0];
    let element = &args.args[2];
    let list = match db.get_list_mut(key) {
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        Ok(Some(l)) => l,
        Ok(None) => return CommandOutcome::reply(Value::error("ERR no such key")),
    };
    let len = list.len() as i64;
    let idx = if index < 0 { len + index } else { index };
    if idx < 0 || idx >= len {
        return CommandOutcome::reply(Value::error("ERR index out of range"));
    }
    if list.encoding_name(max_lp_size) == "listpack"
        && would_grow_exceed_limit(&list.inner, element, max_lp_size)
    {
        list.set_encoding_override("quicklist");
    }
    list[idx as usize] = element.clone();
    if list.encoding_name(max_lp_size) == "quicklist"
        && would_shrink_fit_limit(&list.inner, max_lp_size)
    {
        list.clear_encoding_override();
    }
    CommandOutcome {
        reply: Value::ok(),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "lset",
                key: key.clone(),
            }],
            ..CommandEffects::default()
        },
    }
}
