use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::structure::stream::StreamID;

command_macros::portable_command_support!();

pub(crate) fn parse_stream_id_raw(s: &[u8], is_end: bool) -> Result<StreamID, Value> {
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
    let exclusive = s.starts_with(b"(");
    let id_str = if exclusive { &s[1..] } else { s };
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

pub(crate) fn parse_stream_id(s: &[u8], is_end: bool) -> Result<StreamID, Value> {
    parse_stream_id_ext(s, is_end).map(|(id, _)| id)
}
