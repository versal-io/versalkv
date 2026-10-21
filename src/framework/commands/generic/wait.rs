use crate::engine::command::context::{BlockedCmd, BlockedState, CommandConn};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::sys::time::mstime;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

const MAX_WAIT_REPLICAS: i64 = i32::MAX as i64;
const MAX_WAIT_TIMEOUT_MS: i64 = i64::MAX - 1;

#[command("WAIT")]
pub fn wait(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() != 2 {
        return CommandError::wrong_arity("wait");
    }
    let required = match parse_wait_replicas(&args.args[0]) {
        Ok(value) => value,
        Err(err) => return Value::error(err),
    };
    let timeout_ms = match parse_wait_timeout_ms(&args.args[1]) {
        Ok(value) => value,
        Err(err) => return Value::error(err),
    };

    if required == 0 {
        return Value::integer(ctx.replication().live_replica_count() as i64);
    }

    let target_offset = conn.last_write_offset();
    let acked = ctx.replication().count_replicas_acked_to(target_offset);
    if acked >= required || conn.in_transaction() || ctx.has_running_script() {
        return Value::integer(acked);
    }

    conn.set_blocked(Some(BlockedState {
        cmd: BlockedCmd::Wait {
            required,
            target_offset,
        },
        keys: Vec::new(),
        timeout_ms,
        start_time_ms: mstime(),
        orig_args: args.clone(),
        req_bytes: conn.req_bytes_curr_cmd(),
        orig_wrapped: None,
    }));
    Value::Null
}

pub(crate) fn parse_wait_replicas(raw: &[u8]) -> Result<i64, &'static str> {
    match std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
    {
        Some(value) if (0..=MAX_WAIT_REPLICAS).contains(&value) => Ok(value),
        _ => Err("ERR value is not an integer or out of range"),
    }
}

pub(crate) fn parse_wait_timeout_ms(raw: &[u8]) -> Result<u64, &'static str> {
    match std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
    {
        Some(value) if value < 0 => Err("ERR timeout is negative"),
        Some(value) if value <= MAX_WAIT_TIMEOUT_MS => Ok(value as u64),
        Some(_) => Err("ERR timeout is out of range"),
        None => Err("ERR timeout is not an integer or out of range"),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_wait_replicas, parse_wait_timeout_ms};

    #[test]
    fn parse_wait_replicas_accepts_int_range_only() {
        assert_eq!(parse_wait_replicas(b"0"), Ok(0));
        assert_eq!(parse_wait_replicas(b"2147483647"), Ok(2_147_483_647));
        assert_eq!(
            parse_wait_replicas(b"-1"),
            Err("ERR value is not an integer or out of range")
        );
        assert_eq!(
            parse_wait_replicas(b"2147483648"),
            Err("ERR value is not an integer or out of range")
        );
        assert_eq!(
            parse_wait_replicas(b"9223372036854775808"),
            Err("ERR value is not an integer or out of range")
        );
    }

    #[test]
    fn parse_wait_timeout_matches_valkey_boundary_errors() {
        assert_eq!(parse_wait_timeout_ms(b"0"), Ok(0));
        assert_eq!(parse_wait_timeout_ms(b"-1"), Err("ERR timeout is negative"));
        assert_eq!(
            parse_wait_timeout_ms(b"9223372036854775807"),
            Err("ERR timeout is out of range")
        );
        assert_eq!(
            parse_wait_timeout_ms(b"9223372036854775808"),
            Err("ERR timeout is not an integer or out of range")
        );
    }
}
