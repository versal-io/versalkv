use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("DEL", runtime)]
pub fn del(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.is_empty() {
        return CommandOutcome::reply(CommandError::wrong_arity("del"));
    }
    let db_index = runtime.selected_db();
    let defer_lazyfree = runtime.should_defer_lazyfree_user_del();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let mut effects = CommandEffects::default();
    for key in &args.args {
        if let Some((object, _, _)) = db.take(key) {
            effects.changed_keys.push((db_index, key.clone()));
            effects.events.push(CoreEvent {
                kind: "del",
                key: key.clone(),
            });
            if defer_lazyfree {
                effects.deferred_objects.push(object);
            }
        }
    }
    if effects.changed_keys.is_empty() {
        effects.skip_propagate = true;
    }
    CommandOutcome {
        reply: Value::integer(effects.changed_keys.len() as i64),
        effects,
    }
}
