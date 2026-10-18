use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use bytes::Bytes;
use command_macros::command;

#[command("MSETNX", runtime)]
pub fn msetnx(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.is_empty() || !args.args.len().is_multiple_of(2) {
        return CommandOutcome::reply(CommandError::wrong_arity("msetnx"));
    }
    let db_index = runtime.selected_db();
    let now_ms = runtime.now_ms();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    if args
        .args
        .as_chunks::<2>()
        .0
        .iter()
        .any(|pair| db.exists_at(&pair[0], now_ms))
    {
        return CommandOutcome {
            reply: Value::integer(0),
            effects: CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            },
        };
    }

    let mut effects = CommandEffects::default();
    let mut propagation = Vec::with_capacity(args.args.len() + 1);
    propagation.push(Bytes::from_static(b"MSET"));
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
        propagation.push(pair[0].clone());
        propagation.push(pair[1].clone());
    }
    effects.custom_propagations.push(propagation);
    CommandOutcome {
        reply: Value::integer(1),
        effects,
    }
}
