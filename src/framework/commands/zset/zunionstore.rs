use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::zset::basic::{aggregate_score, parse_set_operation_args};
use bytes::Bytes;
use command_macros::command;
use std::collections::HashMap;

use crate::foundation::io::store::structure::zset::ZSetSl;

#[command("ZUNIONSTORE", runtime, get_keys = crate::framework::commands::key_extractors::zsetstore_get_keys)]
pub fn zunionstore(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 3 {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    let (key_weights, aggregate) = match parse_set_operation_args(&args.args, 1, "zunionstore") {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };
    let db_index = runtime.selected_db();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let dest = args.args[0].clone();
    let mut result: HashMap<Bytes, f64> = HashMap::new();
    for (key, weight) in &key_weights {
        match db.get_zset_object(key) {
            Ok(Some(m)) => {
                for (member, score) in m.zset_entries() {
                    let mut weighted = score * weight;
                    if weighted.is_nan() {
                        weighted = 0.0;
                    }
                    match result.get_mut(&member) {
                        Some(existing) => {
                            *existing = aggregate_score(*existing, weighted, aggregate);
                        }
                        None => {
                            result.insert(member.clone(), weighted);
                        }
                    }
                }
            }
            Ok(None) => continue,
            Err(()) => {
                // Check if it's a Set
                match db.get(key) {
                    Some(Robj::SetHashtable(s)) => {
                        for member in s.data.keys() {
                            let mut weighted = 1.0 * weight;
                            if weighted.is_nan() {
                                weighted = 0.0;
                            }
                            match result.get_mut(member) {
                                Some(existing) => {
                                    *existing = aggregate_score(*existing, weighted, aggregate);
                                }
                                None => {
                                    result.insert(member.clone(), weighted);
                                }
                            }
                        }
                    }
                    _ => return CommandOutcome::reply(CommandError::wrong_type()),
                }
            }
        }
    }
    let count = result.len() as i64;
    let mut changed = db.del(&dest);
    if !result.is_empty() {
        use crate::foundation::io::store::structure::hashtable::HashTable;
        let mut ht = HashTable::new();
        for (k, v) in result {
            ht.insert(k, v);
        }
        let zset = ZSetSl::from_dict(ht);
        db.set(dest.to_vec(), Robj::ZSetSkiplist(zset));
        changed = true;
    }
    CommandOutcome {
        reply: Value::integer(count),
        effects: CommandEffects {
            changed_keys: changed
                .then(|| (db_index, dest.clone()))
                .into_iter()
                .collect(),
            events: changed
                .then(|| CoreEvent {
                    kind: "zunionstore",
                    key: dest,
                })
                .into_iter()
                .collect(),
            skip_propagate: !changed,
            ..CommandEffects::default()
        },
    }
}
