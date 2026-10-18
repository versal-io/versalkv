use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("MSET", runtime)]
pub fn mset(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return CommandOutcome::reply(CommandError::wrong_arity("mset"));
    }
    let db_index = runtime.selected_db();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let mut effects = CommandEffects::default();
    for pair in args.args.as_chunks::<2>().0 {
        db.set(
            pair[0].to_vec(),
            crate::foundation::io::store::Robj::string(pair[1].clone()),
        );
        effects.changed_keys.push((db_index, pair[0].clone()));
        effects.events.push(CoreEvent {
            kind: "set",
            key: pair[0].clone(),
        });
    }
    CommandOutcome {
        reply: Value::ok(),
        effects,
    }
}
