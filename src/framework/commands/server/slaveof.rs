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

#[command("SLAVEOF", extra_flags = crate::framework::CMD_LOADING)]
pub fn slaveof(_conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
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
        if ctx.config().replication.master_host.is_some() {
            if !was_replica {
                ctx.replication_mut().switch_to_replica();
            } else if current_target != previous_target {
                ctx.replication_mut().rebind_replica_resume_on_next_reconnect = true;
            }
        } else if was_replica {
            ctx.replication_mut().backlog.shift_replication_id();
            ctx.replication_mut().role =
                crate::engine::distributed::replication::ReplicationRole::Primary;
            crate::foundation::io::sys::log::set_log_role_primary();
        }
        if current_target != previous_target {
            ctx.replication_mut().pending_replicaof_request = true;
        }
        ctx.replication_mut().pending_failover_target = None;
        ctx.replication_mut().failover_sync_offset = None;
        ctx.replication_mut().failover_state = "no-failover".to_string();
        ctx.replication_mut().failover_end_time_ms = 0;
        ctx.replication_mut().force_failover = false;
        ctx.client_pause_mut().action = crate::framework::context::PauseAction::None;
        ctx.client_pause_mut().end_time_ms = 0;
        ctx.set_pending_unpause(true);
        let blocked_tokens: Vec<_> = ctx
            .blocking_registry()
            .blocked_tokens()
            .into_iter()
            .collect();
        ctx.pending_blocked_to_reject_mut().extend(blocked_tokens);
        let paused: Vec<_> = ctx.paused_clients().iter().copied().collect();
        ctx.pending_acl_tokens_to_kill_mut().extend(paused);
        ctx.paused_clients_mut().clear();
    }
    let _ = crate::framework::commands::server::config::set_oom_score_adj(ctx.config_mut(), -1);
    result
}

#[cfg(test)]
mod tests {
    use super::SLAVEOF_SPEC;
    use crate::framework::CMD_LOADING;

    #[test]
    fn slaveof_can_abort_blocking_rdb_loading() {
        assert_ne!(SLAVEOF_SPEC.flags & CMD_LOADING, 0);
    }
}
