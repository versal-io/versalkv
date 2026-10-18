use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::structure::stream::StreamID;
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::stream::runtime_support::parse_stream_id;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

fn xgroup_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    if args.len() < 2 {
        return Vec::new();
    }
    vec![(args[1].as_ref(), vec!["RO", "access"])]
}

#[command("XGROUP", runtime, get_keys = xgroup_get_keys)]
pub fn xgroup(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db_idx = runtime.selected_db();
    let reply = xgroup_reply(runtime, args);
    let subcommand = args
        .args
        .first()
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    let changed = match (subcommand.as_slice(), &reply) {
        (b"create" | b"setid", Value::SimpleString(_)) => true,
        (b"destroy" | b"createconsumer" | b"delconsumer", Value::Integer(value)) => *value > 0,
        _ => false,
    };
    if !changed {
        return CommandOutcome::reply(reply);
    }
    let key = args.args.get(1).cloned().unwrap_or_default();
    let event = match subcommand.as_slice() {
        b"create" => "xgroup-create",
        b"setid" => "xgroup-setid",
        b"destroy" => "xgroup-destroy",
        b"createconsumer" => "xgroup-createconsumer",
        b"delconsumer" => "xgroup-delconsumer",
        _ => "xgroup",
    };
    let mut effects = CommandEffects {
        changed_keys: vec![(db_idx, key.clone())],
        events: vec![CoreEvent { kind: event, key }],
        ..CommandEffects::default()
    };
    if subcommand.as_slice() == b"create" {
        let mut propagation = vec![Bytes::from_static(b"XGROUP")];
        propagation.extend(args.args.iter().cloned());
        effects.custom_propagations.push(propagation);
    }
    CommandOutcome { reply, effects }
}

fn xgroup_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    // XGROUP CREATE key group id|$ [MKSTREAM] [ENTRIESREAD entries_read]
    // XGROUP SETID key group id|$
    // XGROUP DESTROY key group
    // XGROUP CREATECONSUMER key group consumer
    // XGROUP DELCONSUMER key group consumer
    if args
        .args
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case(b"HELP"))
    {
        if args.args.len() != 1 {
            return CommandError::wrong_arity("xgroup|help");
        }
        return Value::bulk(bytes::Bytes::from_static(
            b"XGROUP <subcommand> [<arg> [value] [opt] ...]",
        ));
    }
    if args.args.len() < 3 {
        return CommandError::wrong_arity("xgroup");
    }
    let subcmd = std::str::from_utf8(&args.args[0])
        .unwrap_or("")
        .to_ascii_lowercase();
    match subcmd.as_str() {
        "create" => xgroup_create(runtime, args),
        "setid" => xgroup_setid(runtime, args),
        "destroy" => xgroup_destroy(runtime, args),
        "createconsumer" => {
            if args.args.len() != 4 {
                return CommandError::wrong_arity("xgroup|createconsumer");
            }
            let key = &args.args[1];
            let consumer = &args.args[3];
            let now_ms = runtime.now_ms() as i64;
            let db = match runtime.db_mut() {
                Some(db) => db,
                None => return CommandError::db_index_out_of_range(),
            };
            if matches!(db.get_stream(key), Ok(None)) {
                return Value::error("ERR The XGROUP subcommand requires the key to exist");
            }
            let created = match stream_mut_for_group_cmd(runtime, key) {
                Ok(stream) => match stream.create_consumer(&args.args[2], consumer, now_ms) {
                    Ok(created) => created,
                    Err(_) => return nogroup(),
                },
                Err(v) => return v,
            };
            Value::integer(if created { 1 } else { 0 })
        }
        "delconsumer" => {
            if args.args.len() != 4 {
                return CommandError::wrong_arity("xgroup|delconsumer");
            }
            let key = &args.args[1];
            let consumer = &args.args[3];
            let removed = match stream_mut_for_group_cmd(runtime, key) {
                Ok(stream) => match stream.delete_consumer(&args.args[2], consumer) {
                    Ok(removed) => removed,
                    Err(_) => return nogroup(),
                },
                Err(v) => return v,
            };
            Value::integer(removed as i64)
        }
        _ => Value::error("ERR Unknown subcommand or wrong number of arguments for 'XGROUP'"),
    }
}

fn nogroup() -> Value {
    Value::error("NOGROUP No such key or consumer group")
}

fn stream_mut_for_group_cmd<'a>(
    runtime: &'a mut dyn CommandRuntime,
    key: &[u8],
) -> Result<&'a mut crate::foundation::io::store::structure::stream::ValkeyStream, Value> {
    let db = runtime
        .db_mut()
        .ok_or_else(CommandError::db_index_out_of_range)?;
    match db.get_stream_mut(key) {
        Ok(Some(stream)) => Ok(stream),
        Ok(None) => Err(nogroup()),
        Err(_) => Err(CommandError::wrong_type()),
    }
}

fn parse_group_id(id_arg: &[u8], stream_last_id: StreamID) -> Result<StreamID, Value> {
    if id_arg == b"$" {
        Ok(stream_last_id)
    } else {
        parse_stream_id(id_arg, false)
    }
}

fn parse_entries_read(args: &CommandArgs, idx: usize) -> Result<i64, Value> {
    let entries_read: i64 = args
        .parse(idx)
        .map_err(|_| Value::error("ERR value for ENTRIESREAD must be positive or -1"))?;
    if entries_read < -1 {
        return Err(Value::error(
            "ERR value for ENTRIESREAD must be positive or -1",
        ));
    }
    Ok(entries_read)
}

fn xgroup_create(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.len() < 4 {
        return CommandError::wrong_arity("xgroup|create");
    }
    let key = args.args[1].clone();
    let group = args.args[2].clone();
    let id_arg = args.args[3].clone();
    let mut mkstream = false;
    let mut entries_read: Option<u64> = None;

    let mut i = 4;
    while i < args.args.len() {
        if args.args[i].eq_ignore_ascii_case(b"MKSTREAM") {
            mkstream = true;
            i += 1;
        } else if args.args[i].eq_ignore_ascii_case(b"ENTRIESREAD") {
            if i + 1 >= args.args.len() {
                return CommandError::syntax_error();
            }
            let parsed = match parse_entries_read(args, i + 1) {
                Ok(v) => v,
                Err(e) => return e,
            };
            entries_read = if parsed == -1 {
                None
            } else {
                Some(parsed as u64)
            };
            i += 2;
        } else {
            return CommandError::syntax_error();
        }
    }

    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    if db.get_stream(&key).is_err() {
        return CommandError::wrong_type();
    }
    if matches!(db.get_stream(&key), Ok(None)) {
        if !mkstream {
            return Value::error("ERR The XGROUP subcommand requires the key to exist");
        }
        db.set(key.to_vec(), Robj::StreamListpack(Box::default()));
    }

    let stream = match db.get_stream_mut(&key) {
        Ok(Some(stream)) => stream,
        Ok(None) => return Value::error("ERR The XGROUP subcommand requires the key to exist"),
        Err(_) => return CommandError::wrong_type(),
    };
    let last_id = match parse_group_id(&id_arg, stream.recorded_last_id()) {
        Ok(id) => id,
        Err(e) => return e,
    };
    if stream.create_group(group, last_id, entries_read).is_err() {
        return Value::error("BUSYGROUP Consumer Group name already exists");
    }

    Value::ok()
}

fn xgroup_setid(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.len() != 4 && args.args.len() != 6 {
        return CommandError::wrong_arity("xgroup|setid");
    }
    let key = args.args[1].clone();
    let group = args.args[2].clone();
    let entries_read = if args.args.len() == 6 {
        if !args.args[4].eq_ignore_ascii_case(b"ENTRIESREAD") {
            return CommandError::syntax_error();
        }
        match parse_entries_read(args, 5) {
            Ok(-1) => None,
            Ok(v) => Some(v as u64),
            Err(e) => return e,
        }
    } else {
        None
    };
    let stream = match stream_mut_for_group_cmd(runtime, &key) {
        Ok(stream) => stream,
        Err(v) => return v,
    };
    let last_id = match parse_group_id(&args.args[3], stream.recorded_last_id()) {
        Ok(id) => id,
        Err(e) => return e,
    };
    if stream.set_group_id(&group, last_id, entries_read).is_err() {
        return nogroup();
    }
    Value::ok()
}

fn xgroup_destroy(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> Value {
    if args.args.len() != 3 {
        return CommandError::wrong_arity("xgroup|destroy");
    }
    let key = args.args[1].clone();
    let stream = match stream_mut_for_group_cmd(runtime, &key) {
        Ok(stream) => stream,
        Err(v) => return v,
    };
    let removed = stream.destroy_group(&args.args[2]);
    Value::integer(if removed { 1 } else { 0 })
}
