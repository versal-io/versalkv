use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent};
use crate::foundation::io::protocol::Value;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::zset::basic::{parse_count, score_value};
use command_macros::command;

#[command("ZPOPMIN", runtime)]
pub fn zpopmin(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let (count, has_count) = match parse_count(args) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };
    let db_index = runtime.selected_db();
    let resp3 = runtime.resp3();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let key = &args.args[0];
    let zset = match db
        .get_zset_object_mut(key)
        .map_err(|_| CommandError::wrong_type())
    {
        Ok(Some(m)) => m,
        Ok(None) => return CommandOutcome::reply(Value::Array(Vec::new())),
        Err(e) => return CommandOutcome::reply(e),
    };
    if count == 0 {
        return CommandOutcome::reply(Value::Array(Vec::new()));
    }
    let popped = zset.zset_pop_min(count);
    if zset.zset_is_empty() {
        db.del(key);
    }
    let mut out = Vec::new();
    if resp3 && has_count {
        for entry in &popped {
            out.push(Value::Array(vec![
                Value::bulk(entry.0.clone()),
                score_value(entry.1, resp3),
            ]));
        }
    } else {
        for entry in &popped {
            out.push(Value::bulk(entry.0.clone()));
            out.push(score_value(entry.1, resp3));
        }
    }
    let changed = !popped.is_empty();
    CommandOutcome {
        reply: Value::Array(out),
        effects: CommandEffects {
            changed_keys: changed
                .then(|| (db_index, key.clone()))
                .into_iter()
                .collect(),
            events: changed
                .then(|| CoreEvent {
                    kind: "zpopmin",
                    key: key.clone(),
                })
                .into_iter()
                .collect(),
            skip_propagate: !changed,
            ..CommandEffects::default()
        },
    }
}
