use super::zmpop::{
    execute_zset_pop, format_zset_pop, parse_count, parse_side, ZSetPopRequest, ZSetPopSide,
};
use crate::engine::command::context::{BlockedCmd, CommandConn};
use crate::engine::distributed::replication::protocol::encode_command;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("BZMPOP", get_keys = crate::framework::commands::server::command::getkeys_helpers::bmpop_get_keys)]
pub fn bzmpop(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    let request = match parse_blocking_request(args) {
        Ok(request) => request,
        Err(error) => return error,
    };
    let db_index = conn.selected_db();
    let pop_result = match ctx.db_mut(db_index) {
        Some(db) => execute_zset_pop(db, &request),
        None => return crate::framework::CommandError::db_index_out_of_range(),
    };
    match pop_result {
        Ok(Some((key, entries))) => {
            let command: &[u8] = match request.side {
                ZSetPopSide::Min => b"ZPOPMIN",
                ZSetPopSide::Max => b"ZPOPMAX",
            };
            let count = entries.len().to_string();
            conn.record_dirty_key((db_index, key.clone()));
            conn.record_custom_propagation(encode_command(&[
                command,
                key.as_ref(),
                count.as_bytes(),
            ]));
            return format_zset_pop(&key, &entries, conn.resp_version() >= 3);
        }
        Err(error) => return error,
        Ok(None) => {}
    }
    if ctx.has_running_script() {
        return Value::NullArray;
    }
    conn.enter_blocking(
        BlockedCmd::Bzmpop {
            is_min: request.side == ZSetPopSide::Min,
            count: request.count,
        },
        request.keys,
        request.timeout_ms.unwrap_or_default(),
        args,
        conn.req_bytes_curr_cmd(),
    )
}

fn parse_blocking_request(args: &CommandArgs) -> Result<ZSetPopRequest, Value> {
    if args.len() < 4 {
        return Err(CommandError::syntax_error());
    }
    let timeout_ms = args.parse_timeout_ms(0)?;
    let numkeys = args.parse_positive::<usize>(1, "numkeys")?;
    let side_index = 2 + numkeys;
    if side_index >= args.len() {
        return Err(CommandError::syntax_error());
    }
    Ok(ZSetPopRequest {
        keys: args.args[2..side_index].to_vec(),
        side: parse_side(&args.args[side_index])?,
        count: parse_count(args, side_index + 1)?,
        timeout_ms: Some(timeout_ms),
    })
}
