#[cfg(test)]
use crate::foundation::io::sys::time::MonotonicInstant;
use crate::runtime::server::worker::reactor::transport::RuntimeEvent;

use super::ServerWorkerState;
#[cfg(test)]
use super::TurnTracePoint;

#[cfg(test)]
#[path = "primary_io_tests.rs"]
mod primary_io_tests;

impl ServerWorkerState {
    #[cfg(test)]
    pub(super) fn prioritize_upstream_readiness(
        &self,
        events: &mut [(
            crate::framework::capabilities::io::Token,
            bool,
            bool,
            bool,
            bool,
        )],
    ) {
        events.sort_by_key(|(token, _, _, _, _)| self.upstream_readiness_rank(*token));
    }

    fn prioritize_runtime_events(&self, events: &mut [RuntimeEvent]) {
        events.sort_by_key(|event| {
            (
                !self.is_priority_connection(event.token),
                self.upstream_readiness_rank(event.token),
            )
        });
    }

    fn upstream_readiness_rank(&self, token: crate::framework::capabilities::io::Token) -> u8 {
        let connection_id = crate::framework::distributed::replication::ConnectionId(token.0);
        if self
            .replication_runtime
            .replica_rdb_link()
            .is_some_and(|link| link.connection_id() == connection_id)
        {
            0
        } else if self.is_master_link(token.0) {
            1
        } else {
            2
        }
    }

    #[cfg(test)]
    pub(super) fn poll_ready(
        &mut self,
    ) -> std::io::Result<Option<(std::time::Instant, Vec<RuntimeEvent>)>> {
        self.sync_module_timer_event();
        let poll_timeout = self.next_poll_timeout(MonotonicInstant::now());
        self.poll_ready_with_timeout(poll_timeout)
    }

    pub(crate) fn poll_ready_with_timeout(
        &mut self,
        poll_timeout: Option<std::time::Duration>,
    ) -> std::io::Result<Option<(std::time::Instant, Vec<RuntimeEvent>)>> {
        self.refresh_connection_priorities();
        self.events_buf.clear();
        #[cfg(test)]
        self.trace_turn_for_test(TurnTracePoint::Poll);
        crate::framework::capabilities::module::fire_module_event_loop(
            &mut self.ctx,
            crate::engine::extension::module::VALKEYMODULE_SUBEVENT_EVENTLOOP_BEFORE_SLEEP,
        );
        self.apply_pending_module_effects();
        let (mut events, deferred_io_completions) = self.poll_with_io_threads(poll_timeout)?;
        // Snapshot read ownership before completions can release the lease.
        // An ordinary LT read already in flight covers this observation; unread
        // or newly arrived bytes remain visible to the next poll or continuation.
        // Keep terminal and write readiness independent of that read lease.
        for event in &mut events {
            if event.readable
                && !event.read_hup
                && !event.error
                && self.connection_registry.contains(event.token)
                && !self.io_threads.is_cluster_bus(event.token)
                && self
                    .replication_runtime
                    .role(crate::framework::distributed::replication::ConnectionId(
                        event.token.0,
                    ))
                    .is_none()
                && self.connection_registry[event.token]
                    .slot_migration_role
                    .is_none()
                && self.io_threads.read_inflight_id(event.token).is_some()
            {
                event.readable = false;
            }
        }
        self.events_buf = events;
        crate::framework::capabilities::module::fire_module_event_loop(
            &mut self.ctx,
            crate::engine::extension::module::VALKEYMODULE_SUBEVENT_EVENTLOOP_AFTER_SLEEP,
        );
        self.apply_pending_module_effects();
        self.io_threads_after_poll(self.events_buf.len());
        self.apply_deferred_io_completions(deferred_io_completions);
        self.drain_io_completions();
        #[cfg(test)]
        self.trace_turn_for_test(TurnTracePoint::AfterPoll);
        let cycle_start = std::time::Instant::now();
        if self.exit_if_shutdown_requested() {
            self.shutdown_io_threads();
            return Ok(None);
        }
        let mut events = std::mem::take(&mut self.events_buf);
        self.prioritize_runtime_events(&mut events);
        Ok(Some((cycle_start, events)))
    }

    pub(super) fn collect_preemptive_priority_events(
        &mut self,
        last_poll: &mut std::time::Instant,
    ) -> std::io::Result<()> {
        let interval_us = self
            .ctx
            .config()
            .server
            .priority_preemptive_poll_interval_us;
        if interval_us == 0
            || last_poll.elapsed() < std::time::Duration::from_micros(interval_us)
            || self.transport.poll_inflight()
            || self.transport.shutdown_started()
        {
            return Ok(());
        }
        // Poll synchronously without moving the multiplexer to an I/O worker.
        // Enqueue all returned readiness: EventBatch preserves lane ordering,
        // coalescing, and the existing current/next-turn dispatch budgets.
        *last_poll = std::time::Instant::now();
        let events = self
            .transport
            .poll_events(Some(std::time::Duration::ZERO))?;
        self.collect_after_poll_events(Some((*last_poll, events)));
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn dispatch_ready_batch(&mut self, events: Vec<RuntimeEvent>) {
        self.collect_after_poll_events(Some((std::time::Instant::now(), events)));
        self.dispatch_ready_phase(super::DispatchContext::Normal)
            .expect("dispatch test readiness batch");
    }
}
