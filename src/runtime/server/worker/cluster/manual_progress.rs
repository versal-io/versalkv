use crate::engine::distributed::replication::ReplicationOffset;
use crate::foundation::io::sys::time::mstime;
use crate::runtime::server::worker::ServerWorkerState;

impl ServerWorkerState {
    pub(in crate::runtime::server::worker) fn advance_manual_failover_before_sleep(&mut self) {
        if self
            .ctx
            .cluster_state()
            .as_deref()
            .is_none_or(|state| !state.manual_failover.is_active())
        {
            return;
        }
        let mut state_opt = self.ctx.cluster_state_mut().take();
        if let Some(state) = state_opt.as_deref_mut() {
            // Commit reparenting before examining a catchup target belonging
            // to the previous primary or election session.
            self.cluster_apply_pending_replication_switch(state);
            let now = mstime();
            let (status, applied_offset) = self.local_replica_failover_inputs(now);
            state.observe_local_replication(applied_offset, status.priority);
            let actions = if status.loading {
                state.manual_failover.on_timeout(now)
            } else if let Ok(offset) = ReplicationOffset::try_from(applied_offset) {
                state.manual_failover_tick(now, offset)
            } else {
                state.manual_failover.on_timeout(now)
            };
            self.execute_cluster_actions(state, actions, now);
            let node_timeout = self.ctx.config().server.cluster_node_timeout.max(1_000);
            let actions = self
                .cluster_bus
                .reevaluate_automatic_failover_after_receive(state, now, node_timeout, &status);
            self.execute_cluster_actions(state, actions.actions, now);
        }
        *self.ctx.cluster_state_mut() = state_opt;
        // This bounded check can enqueue the auth request without a fresh bus
        // readable event. Install write readiness in the current turn.
        self.schedule_cluster_bus_writes();
    }
}

#[cfg(test)]
#[path = "manual_progress_tests.rs"]
mod tests;
