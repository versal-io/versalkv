use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::structure::dict::Dict;
use crate::foundation::io::store::structure::set::ValkeySet;
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;
use std::collections::HashSet;

#[command("SDIFFSTORE", runtime)]
pub fn sdiffstore(
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

        let mut result: Option<Dict<Bytes, ()>> = None;

        for (i, key) in src_keys.iter().enumerate() {
            match db.get_set(key) {
                Ok(None) => {
                    if i == 0 {
                        result = Some(Dict::new());
                    }
                }
                Ok(Some(s)) => {
                    if i == 0 {
                        result = Some((*s.data).clone());
                    } else if let Some(ref mut r) = result {
                        for m in s.data.keys() {
                            r.remove(m);
                        }
                    }
                }
                Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            }
        }

        let result = result.unwrap_or_default();
        let count = result.len() as i64;

        if result.is_empty() {
            db.del(dst_key);
        } else {
            let hs: HashSet<Bytes> = result.keys().cloned().collect();
            db.set(
                dst_key.to_vec(),
                Robj::SetHashtable(ValkeySet::from_hashset_with_thresholds(
                    hs,
                    max_intset,
                    max_lp_entries,
                    max_lp_value,
                )),
            );
        }

        Value::integer(count)
    };
    set_store_outcome(reply, db_idx, dst_key, "sdiffstore")
}

fn set_store_outcome(
    reply: Value,
    db_idx: u32,
    key: Bytes,
    event: &'static str,
) -> CommandOutcome {
    CommandOutcome {
        reply,
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent { kind: event, key }],
            ..CommandEffects::default()
        },
    }
}
