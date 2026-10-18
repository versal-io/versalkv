use super::basic::score_value;
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::kv_engine::Db;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ZSetPopSide {
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ZSetPopRequest {
    pub keys: Vec<Bytes>,
    pub side: ZSetPopSide,
    pub count: usize,
    pub timeout_ms: Option<u64>,
}

impl ZSetPopRequest {
    fn parse(args: &CommandArgs) -> Result<Self, Value> {
        Self::parse_with(args, 0, None)
    }

    fn parse_with(
        args: &CommandArgs,
        start: usize,
        timeout_ms: Option<u64>,
    ) -> Result<Self, Value> {
        let numkeys = args.parse_positive::<usize>(start, "numkeys")?;
        let side_index = start + 1 + numkeys;
        if side_index >= args.len() {
            return Err(CommandError::syntax_error());
        }
        let side = parse_side(&args.args[side_index])?;
        let count = parse_count(args, side_index + 1)?;
        Ok(Self {
            keys: args.args[start + 1..side_index].to_vec(),
            side,
            count,
            timeout_ms,
        })
    }
}

pub(super) fn parse_side(value: &[u8]) -> Result<ZSetPopSide, Value> {
    if value.eq_ignore_ascii_case(b"min") {
        Ok(ZSetPopSide::Min)
    } else if value.eq_ignore_ascii_case(b"max") {
        Ok(ZSetPopSide::Max)
    } else {
        Err(CommandError::syntax_error())
    }
}

pub(super) fn parse_count(args: &CommandArgs, index: usize) -> Result<usize, Value> {
    if index == args.len() {
        return Ok(1);
    }
    if index + 2 != args.len() || !args.args[index].eq_ignore_ascii_case(b"count") {
        return Err(CommandError::syntax_error());
    }
    match args.parse::<usize>(index + 1) {
        Ok(count) if count > 0 => Ok(count),
        _ => Err(Value::error("ERR count value is not an integer or out of range")),
    }
}

pub(super) fn execute_zset_pop(
    db: &mut Db,
    request: &ZSetPopRequest,
) -> Result<Option<(Bytes, Vec<(Bytes, f64)>)>, Value> {
    for key in &request.keys {
        let zset = match db.get_zset_object_mut(key) {
            Err(_) => return Err(CommandError::wrong_type()),
            Ok(Some(zset)) => zset,
            Ok(None) => continue,
        };
        if zset.zset_is_empty() {
            continue;
        }
        let popped = match request.side {
            ZSetPopSide::Min => zset.zset_pop_min(request.count),
            ZSetPopSide::Max => zset.zset_pop_max(request.count),
        };
        let empty = zset.zset_is_empty();
        let _ = zset;
        if empty {
            db.del(key);
        }
        return Ok(Some((key.clone(), popped)));
    }
    Ok(None)
}

pub(super) fn format_zset_pop(key: &Bytes, entries: &[(Bytes, f64)], resp3: bool) -> Value {
    Value::Array(vec![
        Value::bulk(key.clone()),
        Value::Array(
            entries
                .iter()
                .map(|(member, score)| {
                    Value::Array(vec![
                        Value::bulk(member.clone()),
                        score_value(*score, resp3),
                    ])
                })
                .collect(),
        ),
    ])
}

#[command(
    "ZMPOP",
    runtime,
    get_keys = crate::framework::commands::key_extractors::mpop_get_keys
)]
pub fn zmpop(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let request = match ZSetPopRequest::parse(args) {
        Ok(request) => request,
        Err(error) => return CommandOutcome::reply(error),
    };
    let db_idx = runtime.selected_db();
    let resp3 = runtime.resp3();
    let result = {
        let Some(db) = runtime.db_mut() else {
            return CommandOutcome::reply(CommandError::db_index_out_of_range());
        };
        execute_zset_pop(db, &request)
    };
    match result {
        Ok(Some((key, entries))) => {
            let count = entries.len();
            let (command, event) = match request.side {
                ZSetPopSide::Min => (Bytes::from_static(b"ZPOPMIN"), "zpopmin"),
                ZSetPopSide::Max => (Bytes::from_static(b"ZPOPMAX"), "zpopmax"),
            };
            CommandOutcome {
                reply: format_zset_pop(&key, &entries, resp3),
                effects: CommandEffects {
                    changed_keys: vec![(db_idx, key.clone())],
                    events: vec![CoreEvent {
                        kind: event,
                        key: key.clone(),
                    }],
                    custom_propagations: vec![vec![command, key, count.to_string().into()]],
                    ..CommandEffects::default()
                },
            }
        }
        Ok(None) => CommandOutcome {
            reply: Value::null_array(),
            effects: CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            },
        },
        Err(error) => CommandOutcome::reply(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_parses_typed_max_side_and_count() {
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"ZMPOP"),
            Bytes::from_static(b"2"),
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"MAX"),
            Bytes::from_static(b"COUNT"),
            Bytes::from_static(b"3"),
        ]);

        let request = ZSetPopRequest::parse(&args).unwrap();
        assert_eq!(request.side, ZSetPopSide::Max);
        assert_eq!(request.count, 3);
        assert_eq!(request.keys.len(), 2);
    }
}
