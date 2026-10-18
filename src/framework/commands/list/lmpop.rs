use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::kv_engine::Db;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ListSide {
    Left,
    Right,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ListPopRequest {
    pub keys: Vec<Bytes>,
    pub side: ListSide,
    pub count: usize,
    pub timeout_ms: Option<u64>,
}

impl ListPopRequest {
    fn parse(args: &CommandArgs) -> Result<Self, Value> {
        Self::parse_with(args, 0, None, "lmpop")
    }

    fn parse_with(
        args: &CommandArgs,
        start: usize,
        timeout_ms: Option<u64>,
        command: &'static str,
    ) -> Result<Self, Value> {
        if args.args.len() < start + 3 {
            return Err(CommandError::wrong_arity(command));
        }
        let numkeys = parse_positive(&args.args[start], "numkeys")?;
        let side_index = start + 1 + numkeys;
        if side_index >= args.args.len() {
            return Err(CommandError::wrong_arity(command));
        }
        let side = parse_side(&args.args[side_index])?;
        let count = parse_count_option(args, side_index + 1)?;
        Ok(Self {
            keys: args.args[start + 1..side_index].to_vec(),
            side,
            count,
            timeout_ms,
        })
    }
}

pub(super) fn parse_positive(value: &[u8], name: &str) -> Result<usize, Value> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .map(|value| value as usize)
        .ok_or_else(|| Value::error(format!("ERR {name} should be greater than 0")))
}

pub(super) fn parse_side(value: &[u8]) -> Result<ListSide, Value> {
    if value.eq_ignore_ascii_case(b"left") {
        Ok(ListSide::Left)
    } else if value.eq_ignore_ascii_case(b"right") {
        Ok(ListSide::Right)
    } else {
        Err(CommandError::syntax_error())
    }
}

pub(super) fn parse_count_option(args: &CommandArgs, start: usize) -> Result<usize, Value> {
    if start == args.args.len() {
        return Ok(1);
    }
    if start + 2 != args.args.len() || !args.args[start].eq_ignore_ascii_case(b"count") {
        return Err(CommandError::syntax_error());
    }
    parse_positive(&args.args[start + 1], "count")
}

pub(super) fn execute_list_pop(
    db: &mut Db,
    request: &ListPopRequest,
) -> Result<Option<(Bytes, Vec<Bytes>)>, Value> {
    for key in &request.keys {
        let list = match db.get_list_mut(key) {
            Err(_) => return Err(CommandError::wrong_type()),
            Ok(Some(list)) => list,
            Ok(None) => continue,
        };
        let values = pop_values(list, request.side, request.count);
        let empty = list.is_empty();
        let _ = list;
        if empty {
            db.del(key);
        }
        if values.is_empty() {
            continue;
        }
        return Ok(Some((key.clone(), values)));
    }
    Ok(None)
}

fn pop_values(
    list: &mut crate::foundation::io::store::structure::list::ListValue,
    side: ListSide,
    count: usize,
) -> Vec<Bytes> {
    let mut values = Vec::new();
    for _ in 0..count {
        let value = match side {
            ListSide::Left => list.pop_front(),
            ListSide::Right => list.pop_back(),
        };
        match value {
            Some(value) => values.push(value),
            None => break,
        }
    }
    values
}

pub(super) fn format_pop_result(key: &Bytes, values: Vec<Bytes>) -> Value {
    Value::Array(vec![
        Value::bulk(key.clone()),
        Value::Array(values.into_iter().map(Value::bulk).collect()),
    ])
}

#[command(
    "LMPOP",
    runtime,
    get_keys = crate::framework::commands::key_extractors::mpop_get_keys
)]
pub fn lmpop(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let request = match ListPopRequest::parse(args) {
        Ok(request) => request,
        Err(error) => return CommandOutcome::reply(error),
    };
    let db_idx = runtime.selected_db();
    let result = {
        let Some(db) = runtime.db_mut() else {
            return CommandOutcome::reply(CommandError::db_index_out_of_range());
        };
        execute_list_pop(db, &request)
    };
    match result {
        Ok(Some((key, values))) => {
            let count = values.len();
            let command = match request.side {
                ListSide::Left => Bytes::from_static(b"LPOP"),
                ListSide::Right => Bytes::from_static(b"RPOP"),
            };
            CommandOutcome {
                reply: format_pop_result(&key, values),
                effects: CommandEffects {
                    changed_keys: vec![(db_idx, key.clone())],
                    events: vec![CoreEvent {
                        kind: if request.side == ListSide::Left {
                            "lpop"
                        } else {
                            "rpop"
                        },
                        key: key.clone(),
                    }],
                    custom_propagations: vec![vec![command, key, count.to_string().into()]],
                    ..CommandEffects::default()
                },
            }
        }
        Ok(None) => CommandOutcome {
            reply: Value::null(),
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
    fn request_parses_keys_side_and_count() {
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"LMPOP"),
            Bytes::from_static(b"2"),
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"RIGHT"),
            Bytes::from_static(b"COUNT"),
            Bytes::from_static(b"3"),
        ]);

        let request = ListPopRequest::parse(&args).unwrap();
        assert_eq!(request.timeout_ms, None);
        assert_eq!(request.side, ListSide::Right);
        assert_eq!(request.count, 3);
        assert_eq!(
            request.keys,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
        );
    }
}
