use super::lmove::{execute_list_move, ListMoveRequest};
use super::lmpop::ListSide;
use crate::engine::command::context::{BlockedCmd, BlockedState, CommandConn};
use crate::engine::distributed::replication::protocol::encode_command;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::sys::time::mstime;
use crate::framework::context::ServerContext;
use crate::framework::{parse_hex_or_float, CommandArgs, CommandError};
use command_macros::command;

#[command("BLMOVE", get_keys = super::lmove::lmove_get_keys)]
pub fn blmove(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 5 {
        return CommandError::wrong_arity("blmove");
    }
    let request = match ListMoveRequest::parse(args) {
        Ok(request) => request,
        Err(error) => return error,
    };
    let timeout_ms = match parse_timeout(&args.args[4]) {
        Ok(timeout) => timeout,
        Err(error) => return error,
    };
    let move_result = match ctx.db_mut(conn.selected_db()) {
        Some(db) => execute_list_move(db, &request),
        None => return CommandError::db_index_out_of_range(),
    };
    match move_result {
        Ok(Some(value)) => {
            record_lmove(conn, &request, args);
            return Value::bulk(value);
        }
        Err(error) => return error,
        Ok(None) => {}
    }
    if conn.in_transaction() || ctx.has_running_script() {
        return Value::Null;
    }
    conn.set_blocked(Some(BlockedState {
        cmd: BlockedCmd::Blmove {
            dst_key: request.destination,
            wherefrom: request.from == ListSide::Left,
            whereto: request.to == ListSide::Left,
        },
        keys: vec![request.source],
        timeout_ms,
        start_time_ms: mstime(),
        orig_args: args.clone(),
        req_bytes: conn.req_bytes_curr_cmd(),
        orig_wrapped: None,
    }));
    Value::Null
}

fn parse_timeout(value: &[u8]) -> Result<u64, Value> {
    let seconds = std::str::from_utf8(value).ok().and_then(parse_hex_or_float);
    let timeout = match seconds {
        Some(value) if value >= 0.0 => (value * 1_000.0) as u64,
        Some(_) => return Err(Value::error("ERR timeout is negative")),
        None => return Err(Value::error("ERR timeout is not a float or out of range")),
    };
    if timeout > i32::MAX as u64 {
        Err(Value::error("ERR timeout is out of range"))
    } else {
        Ok(timeout)
    }
}

fn record_lmove(conn: &mut dyn CommandConn, request: &ListMoveRequest, args: &CommandArgs) {
    conn.record_custom_propagation(encode_command(&[
        b"LMOVE",
        &request.source,
        &request.destination,
        &args.args[2],
        &args.args[3],
    ]));
}
