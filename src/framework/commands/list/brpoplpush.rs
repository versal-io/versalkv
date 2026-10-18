use crate::framework::{parse_hex_or_float, CommandArgs, CommandError};
use crate::framework::context::ServerContext;
use crate::engine::distributed::replication::protocol::encode_command;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::{BlockedCmd, BlockedState, CommandConn};
use crate::foundation::io::sys::time::mstime;
use command_macros::command;

#[command("BRPOPLPUSH")]
pub fn brpoplpush(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    let src_key = &args.args[0];
    let dst_key = &args.args[1];
    let timeout_str = &args.args[2];
    let timeout_sec = match std::str::from_utf8(timeout_str)
        .ok()
        .and_then(parse_hex_or_float)
    {
        Some(v) if v >= 0.0 => v,
        Some(v) if v < 0.0 => return Value::error("ERR timeout is negative"),
        _ => return Value::error("ERR timeout is not a float or out of range"),
    };
    let timeout_ms = (timeout_sec * 1000.0) as u64;
    if timeout_ms > i32::MAX as u64 {
        return Value::error("ERR timeout is out of range");
    }
    let db = match ctx.db_mut(conn.selected_db()) {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    match db.get_list(src_key) {
        Err(_) => return CommandError::wrong_type(),
        Ok(None) => {}
        Ok(Some(l)) if l.is_empty() => {}
        Ok(Some(_)) => {
            if db.get_list(dst_key).is_err() {
                return CommandError::wrong_type();
            }
            let elem;
            let mut src_empty = false;
            {
                let src_list = match db.get_list_mut(src_key) {
                    Err(_) => return CommandError::wrong_type(),
                    Ok(Some(l)) => l,
                    Ok(None) => unreachable!(),
                };
                elem = match src_list.pop_back() {
                    Some(e) => e,
                    None => return Value::null(),
                };
                if src_list.is_empty() {
                    src_empty = true;
                }
            }
            if src_empty {
                db.del(src_key);
            }
            let result = elem.to_vec();
            {
                let dst_list = match db.get_or_create_list(dst_key) {
                    Err(_) => return CommandError::wrong_type(),
                    Ok(v) => v,
                };
                dst_list.push_front(elem);
            }
            conn.record_custom_propagation(encode_command(&[
                b"RPOPLPUSH" as &[u8],
                src_key.as_ref(),
                dst_key.as_ref(),
            ]));
            return Value::bulk(result);
        }
    }
    if conn.in_transaction() {
        return Value::Null;
    }
    if ctx.has_running_script() {
        return Value::Null;
    }
    conn.set_blocked(Some(BlockedState {
        cmd: BlockedCmd::Brpoplpush {
            dst_key: dst_key.clone(),
        },
        keys: vec![src_key.clone()],
        timeout_ms,
        start_time_ms: mstime(),
        orig_args: args.clone(),
        req_bytes: conn.req_bytes_curr_cmd(),
        orig_wrapped: None,
    }));
    Value::Null
}
