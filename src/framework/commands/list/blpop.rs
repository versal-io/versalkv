use crate::framework::{parse_hex_or_float, CommandArgs, CommandError};
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::distributed::replication::protocol::encode_command;
use crate::engine::command::context::{BlockedCmd, BlockedState, CommandConn};
use crate::foundation::io::sys::time::mstime;
use bytes::Bytes;
use command_macros::command;

fn blocking_pop(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
    left: bool,
) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity(if left { "blpop" } else { "brpop" });
    }
    let n = args.args.len();
    let timeout_str = &args.args[n - 1];
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
    let keys: Vec<&Bytes> = args.args[..n - 1].iter().collect();
    let db = match ctx.db_mut(conn.selected_db()) {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    for key in &keys {
        let list = match db.get_list(key) {
            Err(_) => return CommandError::wrong_type(),
            Ok(Some(l)) => l,
            Ok(None) => continue,
        };
        if list.is_empty() {
            continue;
        }
        let key_owned = (*key).clone();
        let list = match db.get_list_mut(key) {
            Err(_) => return CommandError::wrong_type(),
            Ok(Some(l)) => l,
            Ok(None) => unreachable!(),
        };
        let elem = if left {
            list.pop_front()
        } else {
            list.pop_back()
        };
        let elem = match elem {
            Some(e) => e,
            None => continue,
        };
        let empty = list.is_empty();
        let result = elem.to_vec();
        let db_id = conn.selected_db();
        if empty {
            db.del(key);
        }
        conn.record_dirty_key((db_id, key_owned.clone()));
        let pop_cmd: &[u8] = if left { b"LPOP" } else { b"RPOP" };
        conn.record_custom_propagation(encode_command(&[pop_cmd, key_owned.as_ref()]));
        return Value::Array(vec![
            Value::bulk(key_owned.to_vec()),
            Value::bulk(result),
        ]);
    }
    if conn.in_transaction() {
        return Value::NullArray;
    }
    // Scripts can't block - return null immediately instead of blocking
    if ctx.has_running_script() {
        return Value::NullArray;
    }
    let keys_vec: Vec<Bytes> = keys.iter().map(|k| (*k).clone()).collect();
    conn.set_blocked(Some(BlockedState {
        cmd: if left {
            BlockedCmd::Blpop
        } else {
            BlockedCmd::Brpop
        },
        keys: keys_vec,
        timeout_ms,
        start_time_ms: mstime(),
        orig_args: args.clone(),
        req_bytes: conn.req_bytes_curr_cmd(),
        orig_wrapped: None,
    }));
    Value::Null
}

#[command("BLPOP")]
pub fn blpop(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    blocking_pop(conn, args, ctx, true)
}
