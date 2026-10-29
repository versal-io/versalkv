use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::context::RuntimeConfig;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

fn replicaof_handler(args: &CommandArgs, rcfg: &mut RuntimeConfig) -> Value {
    let host = match std::str::from_utf8(&args.args[0]) {
        Ok(h) => h.to_ascii_lowercase(),
        Err(_) => return CommandError::syntax_error(),
    };
    if host == "no" || host == "none" {
        rcfg.replication.master_host = None;
        rcfg.replication.master_port = 0;
        rcfg.replication.master_link_status = "down".to_string();
        return Value::ok();
    }
    let port_str = match std::str::from_utf8(&args.args[1]) {
        Ok(p) => p,
        Err(_) => return CommandError::syntax_error(),
    };
    let port: u16 = match port_str.parse() {
        Ok(p) => p,
        Err(_) => return CommandError::syntax_error(),
    };
    rcfg.replication.master_host = Some(host.to_string());
    rcfg.replication.master_port = port;
    rcfg.replication.master_link_status = "down".to_string();
    Value::ok()
}

#[command("REPLICAOF", extra_flags = crate::framework::CMD_LOADING)]
pub fn replicaof(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    let previous_target = (
        ctx.config().replication.master_host.clone(),
        ctx.config().replication.master_port,
    );
    let previous_link_status = ctx.config().replication.master_link_status.clone();
    let was_replica = matches!(
        ctx.replication().role,
        crate::engine::distributed::replication::ReplicationRole::Replica
    );
    let result = replicaof_handler(args, ctx.config_mut());
    if !matches!(result, Value::Error(_)) {
        let current_target = (
            ctx.config().replication.master_host.clone(),
            ctx.config().replication.master_port,
        );
        if current_target == previous_target {
            ctx.config_mut().replication.master_link_status = previous_link_status;
        }
        let is_now_replica = ctx.config().replication.master_host.is_some();
        if is_now_replica {
            if !was_replica {
                ctx.replication_mut().switch_to_replica();
            } else if current_target != previous_target {
                ctx.replication_mut().rebind_replica_resume_on_next_reconnect = true;
            }
        } else if was_replica {
            ctx.replication_mut().switch_to_primary();
        }
        if current_target != previous_target {
            ctx.replication_mut().pending_replicaof_request = true;
        }
        if is_now_replica {
            let blocked_tokens: Vec<_> = ctx
                .blocking_registry()
                .blocked_tokens()
                .into_iter()
                .collect();
            ctx.pending_blocked_to_reject_mut().extend(blocked_tokens);
        }
    }
    let _ = crate::framework::commands::server::config::set_oom_score_adj(ctx.config_mut(), -1);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::CMD_LOADING;
    use bytes::Bytes;

    fn offset(value: i64) -> crate::engine::distributed::replication::ReplicationOffset {
        crate::engine::distributed::replication::ReplicationOffset::try_from(value)
            .expect("valid test offset")
    }

    #[test]
    fn replicaof_can_abort_blocking_rdb_loading() {
        assert_ne!(REPLICAOF_SPEC.flags & CMD_LOADING, 0);
    }

    #[test]
    fn demoting_primary_publishes_its_known_replication_offset() {
        let mut conn = Connection::new_fake();
        let mut ctx = ServerContext::new(1);
        ctx.replication_mut().backlog.activate();
        ctx.replication_mut().backlog.append(b"known-history").unwrap();
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"REPLICAOF"),
            Bytes::from_static(b"127.0.0.1"),
            Bytes::from_static(b"6380"),
        ]);

        assert_eq!(replicaof(&mut conn, &args, &mut ctx), Value::ok());
        assert_eq!(ctx.replication().slave_repl_offset, offset(13));
        assert_eq!(ctx.replication().slave_read_repl_offset, offset(13));
    }
}
