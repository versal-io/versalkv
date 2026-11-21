use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("SMEMBERS", runtime)]
pub fn smembers(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("smembers"));
    };
    let resp3 = runtime.resp3();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let members = match db.get_set(key) {
        Ok(Some(set)) => {
            let mut members: Vec<_> = set.data.keys().cloned().collect();
            members.sort();
            members
        }
        Ok(None) => {
            return CommandOutcome {
                reply: if resp3 {
                    Value::Set(Vec::new())
                } else {
                    Value::Array(Vec::new())
                },
                effects: CommandEffects {
                    events: vec![CoreEvent {
                        kind: "keymiss",
                        key: key.clone(),
                    }],
                    ..CommandEffects::default()
                },
            };
        }
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    let values = members.into_iter().map(Value::bulk).collect();
    CommandOutcome::reply(if resp3 {
        Value::Set(values)
    } else {
        Value::Array(values)
    })
}

#[cfg(test)]
mod tests {
    use super::smembers;
    use crate::engine::command::CommandArgs;
    use crate::framework::commands::command_runtime::TestCommandRuntime;
    use bytes::Bytes;

    #[test]
    fn missing_set_records_keymiss_event() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"SMEMBERS"),
            Bytes::from_static(b"missing"),
        ]);

        let outcome = smembers(&mut runtime, &args);

        assert_eq!(
            outcome
                .effects
                .events
                .iter()
                .map(|event| event.kind)
                .collect::<Vec<_>>(),
            vec!["keymiss"]
        );
    }
}
