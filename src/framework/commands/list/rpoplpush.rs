use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("RPOPLPUSH", runtime)]
pub fn rpoplpush(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let src = &args.args[0];
    let dst = &args.args[1];
    let max_lp_size = runtime.list_max_listpack_size();
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    match db.get_list(src) {
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        Ok(None) => return skipped(),
        Ok(Some(_)) => {}
    }
    if db.get_list(dst).is_err() {
        return CommandOutcome::reply(CommandError::wrong_type());
    }
    let elem;
    let mut src_empty = false;
    {
        let src_list = match db.get_list_mut(src) {
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            Ok(Some(l)) => l,
            Ok(None) => return skipped(),
        };
        src_list.note_encoding_before_shrink(max_lp_size);
        elem = match src_list.pop_back() {
            Some(e) => e,
            None => return skipped(),
        };
        if src_list.is_empty() {
            src_empty = true;
        } else {
            src_list.update_encoding_after_shrink(max_lp_size);
        }
    }
    if src_empty {
        db.del(src);
    }
    let result = elem.to_vec();
    {
        let dst_list = match db.get_or_create_list(dst) {
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            Ok(v) => v,
        };
        dst_list.push_front(elem);
        dst_list.update_encoding_after_grow(max_lp_size);
    }
    CommandOutcome {
        reply: Value::bulk(result),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, src.clone()), (db_idx, dst.clone())],
            events: vec![
                CoreEvent {
                    kind: "rpop",
                    key: src.clone(),
                },
                CoreEvent {
                    kind: "lpush",
                    key: dst.clone(),
                },
            ],
            ..CommandEffects::default()
        },
    }
}

fn skipped() -> CommandOutcome {
    CommandOutcome {
        reply: Value::null(),
        effects: CommandEffects {
            skip_propagate: true,
            ..CommandEffects::default()
        },
    }
}
