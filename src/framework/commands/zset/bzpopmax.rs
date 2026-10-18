use super::basic::score_value;
use crate::engine::command::context::{BlockedCmd, CommandConn};
use crate::engine::distributed::replication::protocol::encode_command;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("BZPOPMAX")]
pub fn bzpopmax(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.len() < 2 {
        return CommandError::syntax_error();
    }
    let timeout_ms = match args.parse_timeout_ms(args.len() - 1) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let keys = &args.args[..args.len() - 1];
    let db_id = conn.selected_db();
    let db = match ctx.db_mut(db_id) {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    for key in keys {
        let zset = match db
            .get_zset_object_mut(key)
            .map_err(|_| CommandError::wrong_type())
        {
            Ok(Some(m)) => m,
            Ok(None) => continue,
            Err(e) => return e,
        };
        if zset.zset_is_empty() {
            continue;
        }
        let popped = zset.zset_pop_max(1);
        if popped.is_empty() {
            continue;
        }
        let key_name = key.clone();
        if zset.zset_is_empty() {
            db.del(key);
        }
        conn.record_dirty_key((db_id, key_name.clone()));
        conn.record_custom_propagation(encode_command(&[b"ZPOPMAX" as &[u8], key_name.as_ref()]));
        let resp3 = conn.resp_version() >= 3;
        let mut result = vec![Value::bulk(key_name)];
        result.push(Value::bulk(popped[0].0.clone()));
        result.push(score_value(popped[0].1, resp3));
        return Value::Array(result);
    }
    if ctx.has_running_script() {
        return Value::NullArray;
    }
    conn.enter_blocking(
        BlockedCmd::Bzpopmax,
        keys.iter().map(|k| (*k).clone()).collect(),
        timeout_ms,
        args,
        conn.req_bytes_curr_cmd(),
    )
}
