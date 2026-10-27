use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::structure::hashtable::HashTable;
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::zset::zrange::{collect_range_entries, ZRangeRequest};
use bytes::Bytes;
use command_macros::command;

#[command("ZRANGESTORE", runtime)]
pub fn zrangestore(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let request = match ZRangeRequest::parse_store(args) {
        Ok(request) => request,
        Err(error) => return CommandOutcome::reply(error),
    };
    let (max_entries, max_value_size) = runtime.zset_encoding_limits();
    let destination = args.args[0].clone();
    let db_index = runtime.selected_db();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let entries = match db.get_zset_object(&request.key) {
        Ok(None) => Vec::new(),
        Ok(Some(zset)) => collect_range_entries(zset, &request),
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    let (reply, changed) = store_range(db, &destination, entries, max_entries, max_value_size);
    CommandOutcome {
        reply,
        effects: CommandEffects {
            changed_keys: changed
                .then(|| (db_index, destination.clone()))
                .into_iter()
                .collect(),
            events: changed
                .then(|| CoreEvent {
                    kind: "zrangestore",
                    key: destination,
                })
                .into_iter()
                .collect(),
            skip_propagate: !changed,
            ..CommandEffects::default()
        },
    }
}

fn store_range(
    db: &mut crate::foundation::io::store::kv_engine::Db,
    destination: &[u8],
    entries: Vec<(Bytes, f64)>,
    max_entries: usize,
    max_value_size: usize,
) -> (Value, bool) {
    let count = entries.len() as i64;
    let changed = if entries.is_empty() {
        db.del(destination)
    } else {
        let mut result = HashTable::new();
        for (member, score) in entries {
            result.insert(member, score);
        }
        db.set(
            destination.to_vec(),
            Robj::zset_from_dict_with_limits(result, max_entries, max_value_size),
        );
        true
    };
    (Value::integer(count), changed)
}
