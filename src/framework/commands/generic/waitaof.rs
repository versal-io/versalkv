//! WAITAOF — wait for local AOF durability and replica acknowledgements.

use crate::engine::command::context::{BlockedCmd, BlockedState, CommandConn};
use crate::foundation::io::persistence::AofFsync;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::sys::time::mstime;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

use super::wait::{parse_wait_replicas, parse_wait_timeout_ms};

const WAITAOF_DISABLED_ERROR: &str =
    "ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.";

#[command("WAITAOF")]
pub fn waitaof(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() != 3 {
        return CommandError::wrong_arity("waitaof");
    }

    let numlocal = match parse_waitaof_numlocal(&args.args[0]) {
        Ok(value) => value,
        Err(err) => return Value::error(err),
    };
    let numreplicas = match parse_wait_replicas(&args.args[1]) {
        Ok(value) => value,
        Err(err) => return Value::error(err),
    };
    let timeout_ms = match parse_wait_timeout_ms(&args.args[2]) {
        Ok(value) => value,
        Err(err) => return Value::error(err),
    };

    if numlocal == 1 && !ctx.config().persistence.aof_enabled {
        return Value::error(WAITAOF_DISABLED_ERROR);
    }

    let target_offset = conn.last_write_offset();
    let local = waitaof_local_ack(ctx, target_offset);
    let replicas = ctx.replication().count_replicas_aof_acked_to(target_offset);
    if (local >= numlocal && replicas >= numreplicas)
        || conn.in_transaction()
        || ctx.has_running_script()
    {
        return waitaof_reply(local, replicas);
    }

    conn.set_blocked(Some(BlockedState {
        cmd: BlockedCmd::WaitAof {
            numlocal,
            numreplicas,
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

fn parse_waitaof_numlocal(raw: &[u8]) -> Result<i64, &'static str> {
    match std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
    {
        Some(value @ (0 | 1)) => Ok(value),
        _ => Err("ERR value is not an integer or out of range"),
    }
}

pub(crate) fn waitaof_local_ack(
    ctx: &ServerContext,
    target_offset: crate::engine::distributed::replication::ReplicationOffset,
) -> i64 {
    if !ctx.config().persistence.aof_enabled
        || ctx.aof().journal.is_none()
        || ctx.config().persistence.aof_fsync == AofFsync::No
    {
        return 0;
    }
    i64::from(
        ctx.aof()
            .fsynced_repl_offset
            .is_some_and(|offset| offset >= target_offset.get()),
    )
}

fn waitaof_reply(local: i64, replicas: i64) -> Value {
    Value::Array(vec![Value::integer(local), Value::integer(replicas)])
}

#[cfg(test)]
mod tests {
    use super::{parse_waitaof_numlocal, waitaof};
    use crate::engine::command::context::BlockedCmd;
    use crate::engine::distributed::replication::ReplicationOffset;
    use crate::foundation::io::protocol::Value;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::context::ServerContext;
    use crate::framework::CommandArgs;
    use bytes::Bytes;

    #[test]
    fn parse_waitaof_numlocal_accepts_only_zero_or_one() {
        assert_eq!(parse_waitaof_numlocal(b"0"), Ok(0));
        assert_eq!(parse_waitaof_numlocal(b"1"), Ok(1));
        assert_eq!(
            parse_waitaof_numlocal(b"-1"),
            Err("ERR value is not an integer or out of range")
        );
        assert_eq!(
            parse_waitaof_numlocal(b"2"),
            Err("ERR value is not an integer or out of range")
        );
        assert_eq!(
            parse_waitaof_numlocal(b"9223372036854775808"),
            Err("ERR value is not an integer or out of range")
        );
    }

    #[test]
    fn waitaof_uses_the_calling_connections_last_write_offset() {
        let mut ctx = ServerContext::new(1);
        ctx.replication_mut().backlog.activate();
        ctx.replication_mut()
            .backlog
            .append(b"other-client-write")
            .expect("append replication history");
        let mut conn = Connection::new_fake();
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"WAITAOF"),
            Bytes::from_static(b"0"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"1000"),
        ]);

        assert_eq!(waitaof(&mut conn, &args, &mut ctx), Value::Null);

        let blocked = conn.blocked.expect("WAITAOF should block");
        let BlockedCmd::WaitAof { target_offset, .. } = blocked.cmd else {
            panic!("expected WAITAOF blocked state");
        };
        assert_eq!(
            target_offset,
            ReplicationOffset::ZERO,
            "writes from another connection must not change this client's target"
        );
    }
}
