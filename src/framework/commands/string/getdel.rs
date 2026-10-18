use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("GETDEL", runtime)]
pub fn getdel(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let key = &args.args[0];
    let db_idx = runtime.selected_db();
    let obj = {
        let db = match runtime.db_mut() {
            Some(d) => d,
            None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
        };
        match db.get(key) {
            Some(obj) => {
                if !obj.is_string() {
                    return CommandOutcome::reply(CommandError::wrong_type());
                }
                obj.clone()
            }
            None => return CommandOutcome::reply(Value::null()),
        }
    };
    {
        let db = match runtime.db_mut() {
            Some(d) => d,
            None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
        };
        db.del(key);
    }
    CommandOutcome {
        reply: Value::bulk(obj.as_bytes().to_vec()),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "del",
                key: key.clone(),
            }],
            custom_propagations: vec![vec![Bytes::from_static(b"DEL"), key.clone()]],
            ..CommandEffects::default()
        },
    }
}
