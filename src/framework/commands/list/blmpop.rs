use super::lmpop::{
    execute_list_pop, format_pop_result, parse_count_option, parse_positive, parse_side,
    ListPopRequest, ListSide,
};
use crate::engine::command::context::{BlockedCmd, BlockedState, CommandConn};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::sys::time::mstime;
use crate::engine::distributed::replication::protocol::encode_command;
use crate::framework::context::ServerContext;
use crate::framework::{parse_hex_or_float, CommandArgs, CommandError};
use command_macros::command;

#[command("BLMPOP", get_keys = crate::framework::commands::server::command::getkeys_helpers::bmpop_get_keys)]
pub fn blmpop(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    let request = match parse_blocking_request(args) {
        Ok(request) => request,
        Err(error) => return error,
    };
    let db_index = conn.selected_db();
    let pop_result = match ctx.db_mut(db_index) {
        Some(db) => execute_list_pop(db, &request),
        None => return crate::framework::CommandError::db_index_out_of_range(),
    };
    match pop_result {
        Ok(Some((key, values))) => {
            let command: &[u8] = match request.side {
                ListSide::Left => b"LPOP",
                ListSide::Right => b"RPOP",
            };
            let count = values.len().to_string();
            conn.record_dirty_key((db_index, key.clone()));
            conn.record_custom_propagation(encode_command(&[
                command,
                key.as_ref(),
                count.as_bytes(),
            ]));
            return format_pop_result(&key, values);
        }
        Err(error) => return error,
        Ok(None) => {}
    }
    if conn.in_transaction() || ctx.has_running_script() {
        return Value::NullArray;
    }
    conn.set_blocked(Some(BlockedState {
        cmd: BlockedCmd::Blmpop {
            direction: matches!(request.side, super::lmpop::ListSide::Left),
            count: request.count,
        },
        keys: request.keys,
        timeout_ms: request.timeout_ms.unwrap_or_default(),
        start_time_ms: mstime(),
        orig_args: args.clone(),
        req_bytes: conn.req_bytes_curr_cmd(),
        orig_wrapped: None,
    }));
    Value::Null
}

fn parse_blocking_request(args: &CommandArgs) -> Result<ListPopRequest, Value> {
    if args.args.len() < 4 {
        return Err(CommandError::wrong_arity("blmpop"));
    }
    let seconds = std::str::from_utf8(&args.args[0])
        .ok()
        .and_then(parse_hex_or_float);
    let timeout_ms = match seconds {
        Some(value) if value >= 0.0 => (value * 1_000.0) as u64,
        Some(_) => return Err(Value::error("ERR timeout is negative")),
        None => return Err(Value::error("ERR timeout is not a float or out of range")),
    };
    if timeout_ms > i32::MAX as u64 {
        return Err(Value::error("ERR timeout is out of range"));
    }
    let numkeys = parse_positive(&args.args[1], "numkeys")?;
    let side_index = 2 + numkeys;
    if side_index >= args.args.len() {
        return Err(CommandError::wrong_arity("blmpop"));
    }
    Ok(ListPopRequest {
        keys: args.args[2..side_index].to_vec(),
        side: parse_side(&args.args[side_index])?,
        count: parse_count_option(args, side_index + 1)?,
        timeout_ms: Some(timeout_ms),
    })
}
