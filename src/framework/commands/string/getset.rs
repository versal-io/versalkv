use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("GETSET", runtime)]
pub fn getset(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let key = &args.args[0];
    let db_idx = runtime.selected_db();
    let old_obj = {
        let db = match runtime.db_mut() {
            Some(d) => d,
            None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
        };
        db.get(key).cloned()
    };
    if let Some(ref obj) = old_obj {
        if !obj.is_string() {
            return CommandOutcome::reply(CommandError::wrong_type());
        }
    }
    let val = crate::foundation::io::store::Robj::string(args.args[1].clone());
    {
        let db = match runtime.db_mut() {
            Some(d) => d,
            None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
        };
        db.set(key.to_vec(), val);
    }
    let reply = match old_obj {
        Some(obj) => Value::bulk(obj.as_bytes().to_vec()),
        None => Value::null(),
    };
    CommandOutcome {
        reply,
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "set",
                key: key.clone(),
            }],
            skip_propagate: true,
            custom_propagations: vec![vec![
                Bytes::from_static(b"SET"),
                key.clone(),
                args.args[1].clone(),
            ]],
            ..CommandEffects::default()
        },
    }
}
