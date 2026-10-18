use crate::engine::command::context::CommandConn;
use crate::engine::distributed::sentinel::{DownState, FailoverState};
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};

pub(super) fn reset(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("sentinel|reset");
    }
    let pattern = String::from_utf8_lossy(&args.args[1]).to_lowercase();
    let state = ctx.sentinel_state_mut();
    let names: Vec<String> = if pattern == "*" {
        state.registry.master_names().cloned().collect()
    } else {
        state
            .registry
            .master_names()
            .filter(|name| name.to_lowercase().contains(&pattern))
            .cloned()
            .collect()
    };
    for name in &names {
        let (replicas, sentinels) = match state.registry.master_mut(name) {
            Some(master) => {
                master.peer_states.clear();
                master.config_epoch = 0;
                master.failover_state = FailoverState::None;
                master.failover_state_change_time = 0;
                master.failover_start_time = 0;
                master.promoted_replica = None;
                master.leader.clear();
                master.leader_epoch = 0;
                master.down_state = DownState::Up;
                master.sdown_since_time = 0;
                master.sdown_time = 0;
                master.odown_since_time = 0;
                let replicas = std::mem::take(&mut master.replicas);
                let sentinels = std::mem::take(&mut master.sentinels);
                (replicas, sentinels)
            }
            None => continue,
        };
        let removed_ids = replicas.into_iter().chain(sentinels).collect::<Vec<_>>();
        for id in &removed_ids {
            if let Some(instance) = state.registry.instances().get(id) {
                if !state.pending_link_close.contains(&instance.addr) {
                    state.pending_link_close.push(instance.addr.clone());
                }
            }
        }
        state.link_rebuild_needed = true;
        for id in removed_ids {
            state.registry.remove_instance(id);
        }
    }
    Value::Integer(names.len() as i64)
}
