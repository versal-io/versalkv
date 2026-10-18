use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::structure::set::ValkeySet;
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;
use std::collections::HashSet;

#[command("SUNIONSTORE", runtime)]
pub fn sunionstore(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    let (max_intset, max_lp_entries, max_lp_value) = runtime.set_encoding_limits();
    let db_idx = runtime.selected_db();
    let dst_key = args.args[0].clone();
    let reply = {
        let Some(db) = runtime.db_mut() else {
            return CommandOutcome::reply(CommandError::db_index_out_of_range());
        };
        let dst_key = &args.args[0];
        let src_keys = &args.args[1..];

        let mut result: HashSet<Bytes> = HashSet::new();

        for key in src_keys {
            match db.get_set(key) {
                Ok(Some(s)) => {
                    for m in s.data.keys() {
                        result.insert(m.clone());
                    }
                }
                Ok(None) => continue,
                Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            }
        }

        let count = result.len() as i64;

        if result.is_empty() {
            db.del(dst_key);
        } else {
            db.set(
                dst_key.to_vec(),
                Robj::SetHashtable(ValkeySet::from_hashset_with_thresholds(
                    result,
                    max_intset,
                    max_lp_entries,
                    max_lp_value,
                )),
            );
        }

        Value::integer(count)
    };
    CommandOutcome {
        reply,
        effects: CommandEffects {
            changed_keys: vec![(db_idx, dst_key.clone())],
            events: vec![CoreEvent {
                kind: "sunionstore",
                key: dst_key,
            }],
            ..CommandEffects::default()
        },
    }
}
