use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use crate::foundation::io::store::structure::stream::StreamID;
use bytes::Bytes;
use command_macros::command;

fn parse_stream_id_raw(s: &[u8], is_end: bool) -> Result<StreamID, Value> {
    if s == b"-" {
        return Ok(StreamID::zero());
    }
    if s == b"+" {
        return Ok(StreamID::max());
    }
    let s_str = std::str::from_utf8(s)
        .map_err(|_| Value::error("ERR Invalid stream ID specified as stream command argument"))?;
    if s_str.contains('-') {
        StreamID::parse(s).ok_or_else(|| {
            Value::error("ERR Invalid stream ID specified as stream command argument")
        })
    } else {
        let ms: u64 = s_str.parse().map_err(|_| {
            Value::error("ERR Invalid stream ID specified as stream command argument")
        })?;
        if is_end {
            Ok(StreamID::new(ms, u64::MAX))
        } else {
            Ok(StreamID::new(ms, 0))
        }
    }
}

fn parse_stream_id_ext(s: &[u8], is_end: bool) -> Result<(StreamID, bool), Value> {
    let mut exclusive = false;
    let id_str = if s.starts_with(b"(") {
        exclusive = true;
        &s[1..]
    } else {
        s
    };
    if id_str == b"-" {
        if exclusive {
            return Err(Value::error("ERR The - ID can't be exclusive"));
        }
        return Ok((StreamID::zero(), false));
    }
    if id_str == b"+" {
        if exclusive {
            return Err(Value::error("ERR The + ID can't be exclusive"));
        }
        return Ok((StreamID::max(), false));
    }
    let id = parse_stream_id_raw(id_str, is_end)?;
    if exclusive && id == StreamID::zero() && is_end {
        return Err(Value::error(
            "ERR The 0-0 ID can't be exclusive as end interval",
        ));
    }
    if exclusive && id == StreamID::max() && !is_end {
        return Err(Value::error(
            "ERR The greatest ID can't be exclusive as start interval",
        ));
    }
    Ok((id, exclusive))
}

fn entry_to_value(id: &StreamID, fields: &[(Bytes, Bytes)]) -> Value {
    let mut entry = Vec::with_capacity(2);
    entry.push(Value::bulk(Bytes::from(id.fmt_id())));
    let mut field_arr = Vec::with_capacity(fields.len() * 2);
    for (k, v) in fields {
        field_arr.push(Value::bulk(k.clone()));
        field_arr.push(Value::bulk(v.clone()));
    }
    entry.push(Value::Array(field_arr));
    Value::Array(entry)
}

fn eq_ignore_case(arg: &Bytes, target: &[u8]) -> bool {
    arg.eq_ignore_ascii_case(target)
}

fn xrange_generic(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
    rev: bool,
) -> Value {
    if args.args.len() < 3 {
        return CommandError::wrong_arity(if rev { "xrevrange" } else { "xrange" });
    }
    let key = args.args[0].clone();

    let (lower_id, lower_excl, upper_id, upper_excl) = if rev {
        let upper_arg = &args.args[1];
        let lower_arg = &args.args[2];
        let (uid, uexcl) = match parse_stream_id_ext(upper_arg, true) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let (lid, lexcl) = match parse_stream_id_ext(lower_arg, false) {
            Ok(p) => p,
            Err(e) => return e,
        };
        (lid, lexcl, uid, uexcl)
    } else {
        let lower_arg = &args.args[1];
        let upper_arg = &args.args[2];
        let (lid, lexcl) = match parse_stream_id_ext(lower_arg, false) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let (uid, uexcl) = match parse_stream_id_ext(upper_arg, true) {
            Ok(p) => p,
            Err(e) => return e,
        };
        (lid, lexcl, uid, uexcl)
    };

    if lower_id > upper_id {
        return Value::Array(vec![]);
    }
    if lower_id == upper_id && (lower_excl || upper_excl) {
        return Value::Array(vec![]);
    }

    let mut count: Option<usize> = None;
    let mut i = 3;
    while i < args.args.len() {
        if eq_ignore_case(&args.args[i], b"COUNT") && i + 1 < args.args.len() {
            i += 1;
            let c: i64 = match args.parse(i) {
                Ok(c) => c,
                Err(_) => return Value::error("ERR value is not an integer or out of range"),
            };
            if c < 0 {
                return Value::error("ERR value is not an integer or out of range");
            }
            count = Some(c as usize);
            i += 1;
        } else {
            return CommandError::syntax_error();
        }
    }

    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };

    match db.get_stream(&key) {
        Ok(None) => Value::Array(Vec::new()),
        Ok(Some(stream)) => {
            let entries = stream.range(&lower_id, &upper_id, lower_excl, upper_excl, count, rev);
            let mut result = Vec::with_capacity(entries.len());
            for e in entries {
                result.push(entry_to_value(&e.id, &e.fields));
            }
            Value::Array(result)
        }
        Err(_) => CommandError::wrong_type(),
    }
}

#[command("XREVRANGE", runtime)]
pub fn xrevrange(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(xrange_generic(runtime, args, true))
}
