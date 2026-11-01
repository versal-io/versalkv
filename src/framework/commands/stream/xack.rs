use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::stream::runtime_support::parse_stream_id;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("XACK", runtime)]
pub fn xack(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 3 {
        return CommandOutcome::reply(CommandError::wrong_arity("xack"));
    }
    let mut ids = Vec::with_capacity(args.args.len() - 2);
    for raw in &args.args[2..] {
        match parse_stream_id(raw, false) {
            Ok(id) => ids.push(id),
            Err(_) => {
                return CommandOutcome::reply(Value::error(
                    "ERR Invalid stream ID specified as stream command argument",
                ))
            }
        }
    }
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let acked = match db.get_stream_mut(&args.args[0]) {
        Ok(Some(stream)) => stream.ack_group_pending(&args.args[1], &ids).unwrap_or(0),
        Ok(None) => 0,
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    if acked == 0 {
        return CommandOutcome {
            reply: Value::integer(0),
            effects: CommandEffects {
                skip_propagate: true,
                dirty_delta: Some(0),
                ..CommandEffects::default()
            },
        };
    }
    CommandOutcome {
        reply: Value::integer(acked as i64),
        effects: CommandEffects {
            dirty_delta: Some(acked as i64),
            changed_keys: vec![(db_idx, args.args[0].clone())],
            events: vec![CoreEvent {
                kind: "xack",
                key: args.args[0].clone(),
            }],
            ..CommandEffects::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::foundation::io::store::structure::stream::StreamID;
    use crate::framework::commands::command_runtime::TestCommandRuntime;

    #[test]
    fn acknowledgement_effects_exclude_repeated_or_missing_pending_ids() {
        let mut runtime = TestCommandRuntime::new(1, 10);
        let stream = runtime.db_mut().unwrap().get_or_create_stream(b"s").unwrap();
        stream.create_group(Bytes::from_static(b"g"), StreamID::zero(), None).unwrap();
        stream.append(StreamID::new(1, 0), vec![], true, false, 0).unwrap();
        stream.read_group_new(b"g", b"c", 1, false, 0).unwrap();
        let args = CommandArgs::from_parts(
            [b"XACK".as_slice(), b"s", b"g", b"1-0", b"1-0", b"2-0"]
                .into_iter().map(Bytes::copy_from_slice).collect());
        let outcome = xack(&mut runtime, &args);
        assert_eq!(outcome.reply, Value::integer(1));
        assert_eq!(outcome.effects.changed_keys, vec![(0, Bytes::from_static(b"s"))]);
        let outcome = xack(&mut runtime, &args);
        assert_eq!(outcome.reply, Value::integer(0));
        assert!(outcome.effects.changed_keys.is_empty());
        assert!(outcome.effects.events.is_empty());
        assert!(outcome.effects.skip_propagate);
        assert_eq!(outcome.effects.dirty_delta, Some(0));
    }
}
