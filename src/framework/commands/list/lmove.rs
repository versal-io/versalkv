use super::lmpop::ListSide;
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::kv_engine::Db;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

/// LMOVE / BLMOVE key extraction: source is read/delete, destination inserts.
pub(crate) fn lmove_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    let mut result = Vec::new();
    if !args.is_empty() {
        result.push((args[0].as_ref(), vec!["RW", "access", "delete"]));
    }
    if args.len() >= 2 {
        result.push((args[1].as_ref(), vec!["RW", "insert"]));
    }
    result
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ListMoveRequest {
    pub source: Bytes,
    pub destination: Bytes,
    pub from: ListSide,
    pub to: ListSide,
}

impl ListMoveRequest {
    pub(super) fn parse(args: &CommandArgs) -> Result<Self, Value> {
        if args.args.len() < 4 {
            return Err(CommandError::syntax_error());
        }
        Ok(Self {
            source: args.args[0].clone(),
            destination: args.args[1].clone(),
            from: parse_side(&args.args[2])?,
            to: parse_side(&args.args[3])?,
        })
    }
}

fn parse_side(value: &[u8]) -> Result<ListSide, Value> {
    if value.eq_ignore_ascii_case(b"left") {
        Ok(ListSide::Left)
    } else if value.eq_ignore_ascii_case(b"right") {
        Ok(ListSide::Right)
    } else {
        Err(CommandError::syntax_error())
    }
}

pub(super) fn execute_list_move(
    db: &mut Db,
    request: &ListMoveRequest,
) -> Result<Option<Bytes>, Value> {
    match db.get_list(&request.source) {
        Err(_) => return Err(CommandError::wrong_type()),
        Ok(None) => return Ok(None),
        Ok(Some(list)) if list.is_empty() => return Ok(None),
        Ok(Some(_)) => {}
    }
    if db.get_list(&request.destination).is_err() {
        return Err(CommandError::wrong_type());
    }
    let (element, source_empty) = pop_source(db, request)?;
    if source_empty {
        db.del(&request.source);
    }
    let result = element.clone();
    let destination = db
        .get_or_create_list(&request.destination)
        .map_err(|_| CommandError::wrong_type())?;
    match request.to {
        ListSide::Left => destination.push_front(element),
        ListSide::Right => destination.push_back(element),
    }
    Ok(Some(result))
}

fn pop_source(
    db: &mut crate::foundation::io::store::kv_engine::Db,
    request: &ListMoveRequest,
) -> Result<(Bytes, bool), Value> {
    let source = db
        .get_list_mut(&request.source)
        .map_err(|_| CommandError::wrong_type())?
        .ok_or_else(Value::null)?;
    let element = match request.from {
        ListSide::Left => source.pop_front(),
        ListSide::Right => source.pop_back(),
    }
    .ok_or_else(Value::null)?;
    Ok((element, source.is_empty()))
}

#[command("LMOVE", runtime, get_keys = lmove_get_keys)]
pub fn lmove(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let request = match ListMoveRequest::parse(args) {
        Ok(request) => request,
        Err(error) => return CommandOutcome::reply(error),
    };
    let db_idx = runtime.selected_db();
    let result = {
        let Some(db) = runtime.db_mut() else {
            return CommandOutcome::reply(CommandError::db_index_out_of_range());
        };
        execute_list_move(db, &request)
    };
    match result {
        Ok(Some(value)) => CommandOutcome {
            reply: Value::bulk(value),
            effects: CommandEffects {
                changed_keys: vec![
                    (db_idx, request.source.clone()),
                    (db_idx, request.destination.clone()),
                ],
                events: vec![
                    CoreEvent {
                        kind: "lmove_from",
                        key: request.source,
                    },
                    CoreEvent {
                        kind: "lmove_to",
                        key: request.destination,
                    },
                ],
                ..CommandEffects::default()
            },
        },
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
    fn request_parses_typed_source_and_destination_sides() {
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"LMOVE"),
            Bytes::from_static(b"source"),
            Bytes::from_static(b"destination"),
            Bytes::from_static(b"RIGHT"),
            Bytes::from_static(b"LEFT"),
        ]);

        let request = ListMoveRequest::parse(&args).unwrap();
        assert_eq!(request.from, ListSide::Right);
        assert_eq!(request.to, ListSide::Left);
    }
}
