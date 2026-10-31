use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("PSETEX", runtime)]
pub fn psetex(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let milliseconds =
        match crate::foundation::io::store::structure::string::encoding::int_encodable(
            &args.args[1],
        ) {
            Some(v) => v,
            None => {
                return CommandOutcome::reply(Value::error(
                    "ERR value is not an integer or out of range",
                ))
            }
        };
    if milliseconds <= 0 {
        return CommandOutcome::reply(Value::error("ERR invalid expire time in 'psetex' command"));
    }
    let abs_ms = match milliseconds.checked_add(runtime.now_ms() as i64) {
        Some(v) => v,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR invalid expire time in 'psetex' command",
            ))
        }
    };
    let db_idx = runtime.selected_db();
    let key = args.args[0].clone();
    let val = crate::foundation::io::store::Robj::string(args.args[2].clone());
    {
        let db = match runtime.db_mut() {
            Some(d) => d,
            None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
        };
        db.set(key.to_vec(), val);
        db.set_expire(&key, abs_ms as u64);
    }
    CommandOutcome {
        reply: Value::ok(),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![
                CoreEvent {
                    kind: "set",
                    key: key.clone(),
                },
                CoreEvent {
                    kind: "expire",
                    key: key.clone(),
                },
            ],
            custom_propagations: vec![vec![
                Bytes::from_static(b"SET"),
                key,
                args.args[2].clone(),
                Bytes::from_static(b"PXAT"),
                abs_ms.to_string().into(),
            ]],
            ..CommandEffects::default()
        },
    }
}
