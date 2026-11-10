#[cfg(test)]
use crate::foundation::io::sys::time::MonotonicInstant;
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::event_loop::transport::RuntimeEvent;
use std::collections::HashSet;

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

    /// Non-blocking drain of the priority epoll. Returns RuntimeEvents
    /// converted from PriorityEvents.
    fn drain_priority_poller(&mut self) -> std::io::Result<Vec<RuntimeEvent>> {
        let priority_events = self
            .priority_poller
            .poll_events(Some(std::time::Duration::ZERO))?;
        Ok(priority_events
            .into_iter()
            .map(|pe| RuntimeEvent {
                token: pe.token,
                readable: pe.readable,
                writable: pe.writable,
                read_hup: false,
                error: false,
            })
            .collect())
    }

    fn sort_events_by_subnet_priority(&self, events: &mut [RuntimeEvent]) {
        events.sort_by_key(|event| {
            (
                !self.is_client_subnet_priority(event.token),
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
        self.scan_and_apply_priority_subnets();
        self.events_buf.clear();
        #[cfg(test)]
        self.trace_turn_for_test(TurnTracePoint::Poll);
        crate::framework::capabilities::module::fire_module_event_loop(
            &mut self.ctx,
            crate::engine::extension::module::VALKEYMODULE_SUBEVENT_EVENTLOOP_BEFORE_SLEEP,
        );
        self.apply_pending_module_effects();
        // Drain the priority epoll first (non-blocking). Priority events
        // arrive independently and must precede normal transport events.
        let front_priority_events = self.drain_priority_poller()?;
        let priority_tokens: HashSet<Token> =
            front_priority_events.iter().map(|e| e.token).collect();
        // If we already have priority events, do a non-blocking main poll to
        // avoid unnecessary latency.
        let effective_timeout = if priority_tokens.is_empty() {
            poll_timeout
        } else {
            Some(std::time::Duration::ZERO)
        };
        let (mut events, deferred_io_completions) = self.poll_with_io_threads(effective_timeout)?;
        // Filter out priority tokens from the main poll: these fds are
        // registered in both pollers, but the PriorityPoller already
        // captured their readiness.
        events.retain(|e| !priority_tokens.contains(&e.token));
        // Drain any priority events that arrived during the main poll.
        let back_priority_events = self.drain_priority_poller()?;
        let back_tokens: HashSet<Token> = back_priority_events.iter().map(|e| e.token).collect();
        events.retain(|e| !back_tokens.contains(&e.token));
        // Merge: priority events first, then normal transport events, then
        // late-arriving priority events.
        let mut all_events = Vec::with_capacity(
            front_priority_events.len() + events.len() + back_priority_events.len(),
        );
        all_events.extend(front_priority_events);
        all_events.append(&mut events);
        all_events.extend(back_priority_events);
        events = all_events;
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
        // With dual-epoll, priority events already arrive first; sorting by
        // subnet priority is no longer necessary for ordering, but the
        // upstream readiness rank (replica RDB > master link > other) still
        // provides useful intra-class ordering.
        self.sort_events_by_subnet_priority(&mut events);
        Ok(Some((cycle_start, events)))
    }

    pub(super) fn poll_priority_events_preemptively(
        &mut self,
        last_poll: &mut std::time::Instant,
        context: &super::DispatchContext,
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
        // Poll both the priority epoll (for production priority fds) and
        // the main transport (for test event queues and non-priority
        // sources). Dispatch each readiness event immediately so that
        // priority work executes before returning to the batch loop.
        *last_poll = std::time::Instant::now();
        let mut events = self.drain_priority_poller()?;
        let mut transport_events = self
            .transport
            .poll_events(Some(std::time::Duration::ZERO))?;
        events.append(&mut transport_events);
        for event in events {
            self.dispatch_readiness_now(
                context.clone(),
                super::ReadinessEvent {
                    token: event.token,
                    origin: super::ReadinessOrigin::Polled,
                    readable: event.readable,
                    writable: event.writable,
                    read_closed: event.read_hup,
                    error: event.error,
                },
            );
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn dispatch_ready_batch(&mut self, events: Vec<RuntimeEvent>) {
        self.collect_after_poll_events(Some((std::time::Instant::now(), events)));
        self.dispatch_ready_phase(super::DispatchContext::Normal)
            .expect("dispatch test readiness batch");
    }
}
