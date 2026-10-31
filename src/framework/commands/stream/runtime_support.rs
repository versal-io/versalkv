use crate::engine::core::{CommandEffects, CoreEvent};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::structure::stream::groups::ClaimGroupResult;
use crate::foundation::io::store::structure::stream::StreamID;
use bytes::Bytes;

command_macros::portable_command_support!();

#[cfg(test)]
#[path = "claim_effects_tests.rs"]
mod claim_effects_tests;

#[path = "strict_id.rs"]
mod strict_id;
pub(crate) use strict_id::{parse_signed_integer, parse_stream_id_strict, parse_xadd_id};

#[path = "reference_delete.rs"]
mod reference_delete;
pub(crate) use reference_delete::delete_references;

#[path = "trim_args.rs"]
mod trim_args;
pub(crate) use trim_args::parse_add_trim_args;

pub(crate) fn claim_effects(
    db_idx: u32,
    key: &Bytes,
    group_name: &Bytes,
    last_id: StreamID,
    entries_read: Option<u64>,
    result: &ClaimGroupResult,
) -> CommandEffects {
    let mut custom_propagations: Vec<Vec<Bytes>> = result
        .pending_updates
        .iter()
        .map(|(id, pending)| {
            vec![
                Bytes::from_static(b"XCLAIM"),
                key.clone(),
                group_name.clone(),
                pending.consumer.clone(),
                Bytes::from_static(b"0"),
                Bytes::from(id.fmt_id()),
                Bytes::from_static(b"TIME"),
                Bytes::from(pending.delivery_time_ms.to_string()),
                Bytes::from_static(b"RETRYCOUNT"),
                Bytes::from((pending.delivery_count as i64).to_string()),
                Bytes::from_static(b"FORCE"),
                Bytes::from_static(b"JUSTID"),
                Bytes::from_static(b"LASTID"),
                Bytes::from(last_id.fmt_id()),
            ]
        })
        .collect();
    let should_propagate_last_id = result.last_id_changed && custom_propagations.is_empty();
    let dirty_delta = result.pending_updates.len() as i64
        + i64::from(result.consumer_created)
        + i64::from(should_propagate_last_id);
    if should_propagate_last_id {
        custom_propagations.push(vec![
            Bytes::from_static(b"XGROUP"),
            Bytes::from_static(b"SETID"),
            key.clone(),
            group_name.clone(),
            Bytes::from(last_id.fmt_id()),
            Bytes::from_static(b"ENTRIESREAD"),
            Bytes::from(entries_read.map_or_else(|| "-1".to_owned(), |count| count.to_string())),
        ]);
    }
    CommandEffects {
        skip_propagate: true,
        dirty_delta: Some(dirty_delta),
        custom_propagations,
        events: result
            .consumer_created
            .then(|| CoreEvent {
                kind: "xgroup-createconsumer",
                key: key.clone(),
            })
            .into_iter()
            .collect(),
        changed_keys: (dirty_delta > 0)
            .then(|| (db_idx, key.clone()))
            .into_iter()
            .collect(),
        ..CommandEffects::default()
    }
}

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
