use crate::engine::distributed::replication::ReplicationRole;
use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

struct FailoverOptions {
    target: Option<(String, u16)>,
    timeout_ms: i64,
    force: bool,
}

fn parse_options(args: &CommandArgs) -> Result<FailoverOptions, Value> {
    let mut target = None;
    let mut timeout_ms = None;
    let mut force = false;
    let mut index = 0;
    while index < args.args.len() {
        if args.args[index].eq_ignore_ascii_case(b"TO") && target.is_none() {
            let Some((host, port)) = args.args.get(index + 1).zip(args.args.get(index + 2)) else {
                return Err(CommandError::syntax_error());
            };
            let host = std::str::from_utf8(host)
                .map_err(|_| CommandError::syntax_error())?
                .to_ascii_lowercase();
            let port = std::str::from_utf8(port)
                .ok()
                .and_then(|port| port.parse::<u16>().ok())
                .ok_or_else(CommandError::syntax_error)?;
            target = Some((host, port));
            index += 3;
        } else if args.args[index].eq_ignore_ascii_case(b"TIMEOUT") && timeout_ms.is_none() {
            let Some(value) = args.args.get(index + 1) else {
                return Err(CommandError::syntax_error());
            };
            timeout_ms = Some(
                std::str::from_utf8(value)
                    .ok()
                    .and_then(|value| value.parse::<i64>().ok())
                    .filter(|value| *value > 0)
                    .ok_or_else(|| Value::error("ERR FAILOVER timeout must be greater than 0"))?,
            );
            index += 2;
        } else if args.args[index].eq_ignore_ascii_case(b"FORCE") && !force {
            force = true;
            index += 1;
        } else {
            return Err(CommandError::syntax_error());
        }
    }
    Ok(FailoverOptions {
        target,
        timeout_ms: timeout_ms.unwrap_or(0),
        force,
    })
}

fn clear_failover(ctx: &mut ServerContext) {
    ctx.replication_mut().pending_failover_target = None;
    ctx.replication_mut().failover_sync_offset = None;
    ctx.replication_mut().failover_state = "no-failover".to_string();
    ctx.replication_mut().failover_end_time_ms = 0;
    ctx.replication_mut().force_failover = false;
    if ctx.client_pause().reason == "failover_in_progress" {
        ctx.client_pause_mut().action = crate::framework::context::PauseAction::None;
        ctx.client_pause_mut().end_time_ms = 0;
        ctx.client_pause_mut().reason = "none";
        ctx.set_pending_unpause(true);
    }
}

#[command("FAILOVER")]
pub fn failover(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if ctx.config().server.cluster_enabled {
        return Value::error(
            "ERR FAILOVER not allowed in cluster mode. Use CLUSTER FAILOVER command instead.",
        );
    }
    if args.args.len() == 1 && args.args[0].eq_ignore_ascii_case(b"ABORT") {
        if ctx.replication().failover_state == "no-failover" {
            return Value::error("ERR No failover in progress.");
        }
        clear_failover(ctx);
        return Value::ok();
    }
    let options = match parse_options(args) {
        Ok(options) => options,
        Err(error) => return error,
    };
    if ctx.replication().failover_state != "no-failover" {
        return Value::error("ERR FAILOVER already in progress.");
    }
    if ctx.replication().role == ReplicationRole::Replica {
        return Value::error("ERR FAILOVER is not valid when server is a replica.");
    }
    if ctx.replication().view.replica_info.is_empty() {
        return Value::error("ERR FAILOVER requires connected replicas.");
    }
    if options.force && (options.timeout_ms == 0 || options.target.is_none()) {
        return Value::error(
            "ERR FAILOVER with force option requires both a timeout and target HOST and IP.",
        );
    }
    if let Some((host, port)) = &options.target {
        let Some(replica) = ctx.replication().view.replica_info.values().find(|replica| {
            replica.ip.eq_ignore_ascii_case(host) && replica.port == *port
        }) else {
            return Value::error("ERR FAILOVER target HOST and PORT is not a replica.");
        };
        if replica.state != "online" {
            return Value::error("ERR FAILOVER target replica is not online.");
        }
    }
    ctx.replication_mut().pending_failover_target = options.target;
    ctx.replication_mut().failover_sync_offset = None;
    ctx.replication_mut().failover_state = "waiting-for-sync".to_string();
    let now = crate::foundation::io::sys::time::mstime();
    ctx.replication_mut().failover_end_time_ms = if options.timeout_ms > 0 {
        now + options.timeout_ms
    } else {
        0
    };
    ctx.replication_mut().force_failover = options.force;
    // Sentinel wraps coordinated FAILOVER in an explicit CLIENT PAUSE. Keep
    // that independently owned pause so it survives the PSYNC handoff until
    // Sentinel disconnects stale clients and issues CLIENT UNPAUSE.
    if ctx.client_pause().is_paused(now) == crate::framework::context::PauseAction::None {
        ctx.client_pause_mut().action = crate::framework::context::PauseAction::Write;
        ctx.client_pause_mut().end_time_ms = i64::MAX;
        ctx.client_pause_mut().reason = "failover_in_progress";
    }
    ctx.paused_clients_mut().shift_remove(&conn.token());
    Value::ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::distributed::replication::ReplicaStatus;
    use crate::framework::capabilities::connection::Connection;
    use bytes::Bytes;

    fn offset(value: i64) -> crate::engine::distributed::replication::ReplicationOffset {
        crate::engine::distributed::replication::ReplicationOffset::try_from(value)
            .expect("valid test offset")
    }

    fn args(parts: &[&'static [u8]]) -> CommandArgs {
        CommandArgs::from_parts(parts.iter().map(|part| Bytes::from_static(part)).collect())
    }

    fn add_replica(ctx: &mut ServerContext, state: &str) {
        ctx.replication_mut().view.replica_info.insert(
            7,
            ReplicaStatus {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                offset: Some(offset(42)),
                state: state.to_string(),
                lag_ms: 0,
            },
        );
    }

    #[test]
    fn rejects_failover_without_connected_replicas() {
        let mut conn = Connection::new_fake();
        let mut ctx = ServerContext::new(1);

        let reply = failover(
            &mut conn,
            &args(&[b"FAILOVER", b"TO", b"127.0.0.1", b"6380"]),
            &mut ctx,
        );

        assert!(matches!(reply, Value::Error(message) if message.as_ref() == b"ERR FAILOVER requires connected replicas."));
        assert_eq!(ctx.replication().failover_state, "no-failover");
    }

    #[test]
    fn rejects_failover_on_a_replica() {
        let mut conn = Connection::new_fake();
        let mut ctx = ServerContext::new(1);
        ctx.replication_mut().role = ReplicationRole::Replica;
        add_replica(&mut ctx, "online");

        let reply = failover(
            &mut conn,
            &args(&[b"FAILOVER", b"TO", b"127.0.0.1", b"6380"]),
            &mut ctx,
        );

        assert!(matches!(reply, Value::Error(message) if message.as_ref() == b"ERR FAILOVER is not valid when server is a replica."));
    }

    #[test]
    fn rejects_unknown_or_offline_target_replica() {
        let mut conn = Connection::new_fake();
        let mut ctx = ServerContext::new(1);
        add_replica(&mut ctx, "wait_bgsave");

        let unknown = failover(
            &mut conn,
            &args(&[b"FAILOVER", b"TO", b"invalidhost", b"6380"]),
            &mut ctx,
        );
        assert!(matches!(unknown, Value::Error(message) if message.as_ref() == b"ERR FAILOVER target HOST and PORT is not a replica."));

        let offline = failover(
            &mut conn,
            &args(&[b"FAILOVER", b"TO", b"127.0.0.1", b"6380"]),
            &mut ctx,
        );
        assert!(matches!(offline, Value::Error(message) if message.as_ref() == b"ERR FAILOVER target replica is not online."));
    }

    #[test]
    fn plain_failover_waits_for_any_connected_replica() {
        let mut conn = Connection::new_fake();
        let mut ctx = ServerContext::new(1);
        add_replica(&mut ctx, "online");

        let reply = failover(&mut conn, &args(&[b"FAILOVER"]), &mut ctx);

        assert_eq!(reply, Value::ok());
        assert_eq!(ctx.replication().pending_failover_target, None);
        assert_eq!(ctx.replication().failover_state, "waiting-for-sync");
    }

    #[test]
    fn failover_preserves_an_existing_client_pause() {
        let mut conn = Connection::new_fake();
        let mut ctx = ServerContext::new(1);
        add_replica(&mut ctx, "online");
        let pause_end = crate::foundation::io::sys::time::mstime() + 180_000;
        ctx.client_pause_mut().action = crate::framework::context::PauseAction::Write;
        ctx.client_pause_mut().end_time_ms = pause_end;
        ctx.client_pause_mut().reason = "client_pause";

        let reply = failover(
            &mut conn,
            &args(&[b"FAILOVER", b"TO", b"127.0.0.1", b"6380"]),
            &mut ctx,
        );

        assert_eq!(reply, Value::ok());
        assert_eq!(
            ctx.client_pause().action,
            crate::framework::context::PauseAction::Write
        );
        assert_eq!(ctx.client_pause().end_time_ms, pause_end);
        assert_eq!(ctx.client_pause().reason, "client_pause");
    }

    #[test]
    fn abort_requires_an_active_failover() {
        let mut conn = Connection::new_fake();
        let mut ctx = ServerContext::new(1);

        let reply = failover(
            &mut conn,
            &args(&[b"FAILOVER", b"ABORT"]),
            &mut ctx,
        );

        assert!(matches!(reply, Value::Error(message) if message.as_ref() == b"ERR No failover in progress."));
    }

    #[test]
    fn rejects_standalone_failover_in_cluster_mode() {
        let mut conn = Connection::new_fake();
        let mut ctx = ServerContext::new(1);
        ctx.config_mut().server.cluster_enabled = true;
        add_replica(&mut ctx, "online");

        let reply = failover(&mut conn, &args(&[b"FAILOVER"]), &mut ctx);

        assert!(matches!(reply, Value::Error(message) if message.as_ref() == b"ERR FAILOVER not allowed in cluster mode. Use CLUSTER FAILOVER command instead."));
        assert_eq!(ctx.replication().failover_state, "no-failover");
    }
}
