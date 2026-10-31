use super::runtime_support::parse_stream_id_strict;
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("XDEL", runtime)]
pub fn xdel(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 2 {
        return CommandOutcome::reply(CommandError::wrong_arity("xdel"));
    }
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = args.args[0].clone();
    let stream = match db.get_stream_mut(&key) {
        Ok(Some(s)) => s,
        Ok(None) => return no_change(),
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };

    let ids = match args.args[1..]
        .iter()
        .map(|raw| parse_stream_id_strict(raw))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(ids) => ids,
        Err(error) => return CommandOutcome::reply(error),
    };
    let deleted = stream.delete_multiple(&ids) as i64;

    if deleted == 0 {
        return no_change();
    }
    CommandOutcome {
        reply: Value::integer(deleted),
        effects: CommandEffects {
            dirty_delta: Some(deleted),
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent { kind: "xdel", key }],
            ..CommandEffects::default()
        },
    }
}

fn no_change() -> CommandOutcome {
    CommandOutcome {
        reply: Value::integer(0),
        effects: CommandEffects {
            skip_propagate: true,
            dirty_delta: Some(0),
            ..CommandEffects::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::xdel;
    use crate::foundation::io::protocol::Value;
    use crate::foundation::io::store::structure::stream::StreamID;
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
    use crate::framework::CommandArgs;
    use bytes::Bytes;

    #[test]
    fn xdel_validates_all_ids_before_deleting() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        let stream = runtime
            .db_mut()
            .unwrap()
            .get_or_create_stream(b"s")
            .unwrap();
        for ms in 1..=3 {
            stream
                .append(StreamID::new(ms, 0), vec![], true, false, 0)
                .unwrap();
        }
        let args = CommandArgs::from_parts(
            [b"XDEL".as_slice(), b"s", b"1-0", b"bad"]
                .into_iter()
                .map(Bytes::copy_from_slice)
                .collect(),
        );
        assert_eq!(
            xdel(&mut runtime, &args).reply,
            Value::error("ERR Invalid stream ID specified as stream command argument")
        );
        let stream = runtime.db_mut().unwrap().get_stream(b"s").unwrap().unwrap();
        assert_eq!(stream.len(), 3);
        assert_eq!(stream.max_deleted_entry_id, StreamID::zero());
    }

    #[test]
    fn xdel_rejects_range_ids_and_missing_key_hides_bad_ids() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        for id in [b"+".as_slice(), b"-", b"(1-0", b"1-*"] {
            let args = CommandArgs::from_parts(
                [b"XDEL".as_slice(), b"s", id]
                    .into_iter()
                    .map(Bytes::copy_from_slice)
                    .collect(),
            );
            assert_eq!(xdel(&mut runtime, &args).reply, Value::integer(0));
            runtime
                .db_mut()
                .unwrap()
                .get_or_create_stream(b"s")
                .unwrap();
            assert_eq!(
                xdel(&mut runtime, &args).reply,
                Value::error("ERR Invalid stream ID specified as stream command argument")
            );
            runtime.db_mut().unwrap().del(b"s");
        }
    }
}
