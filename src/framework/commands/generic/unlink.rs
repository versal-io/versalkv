use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::kv_engine::Db;
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("UNLINK", runtime)]
pub fn unlink(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let removed = unlink_keys(db, &args.args);
    let changed_keys = removed
        .iter()
        .map(|(key, _)| (db_idx, key.clone()))
        .collect::<Vec<_>>();
    let events = removed
        .iter()
        .map(|(key, _)| CoreEvent {
            kind: "del",
            key: key.clone(),
        })
        .collect();
    let deferred_objects = removed.into_iter().map(|(_, object)| object).collect();
    CommandOutcome {
        reply: Value::integer(changed_keys.len() as i64),
        effects: CommandEffects {
            skip_propagate: changed_keys.is_empty(),
            changed_keys,
            events,
            deferred_objects,
            ..CommandEffects::default()
        },
    }
}

fn unlink_keys(db: &mut Db, keys: &[Bytes]) -> Vec<(Bytes, Robj)> {
    keys.iter()
        .filter_map(|key| db.take(key).map(|(object, _, _)| (key.clone(), object)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::unlink_keys;
    use crate::foundation::io::store::kv_engine::Db;
    use crate::foundation::io::store::Robj;
    use crate::foundation::io::store::structure::list::ListValue;
    use bytes::Bytes;
    use std::collections::VecDeque;

    #[test]
    fn unlink_defers_large_value_even_when_user_del_lazyfree_is_disabled() {
        let mut db = Db::new();
        db.set(
            b"key".to_vec(),
            Robj::ListQuicklist(ListValue::from_vecdeque(VecDeque::from(
                (0..65)
                    .map(|_| Bytes::from_static(b"value"))
                    .collect::<Vec<_>>(),
            ))),
        );
        let removed = unlink_keys(&mut db, &[Bytes::from_static(b"key")]);
        assert_eq!(removed.len(), 1);
        assert!(db.get_no_expire(b"key").is_none());
    }
}
