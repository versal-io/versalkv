use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::{ConnectionId, ReplicationConnection};
use crate::runtime::server::worker::event_loop::transport::RuntimeEvent;

use super::super::ServerWorkerState;
use super::{DispatchContext, DispatchOutcome, ReadinessEvent, ReadinessOrigin, RunnableKey};

impl ServerWorkerState {
    pub(crate) fn processable_classic_replica_input_token(&self) -> Option<Token> {
        let link = self.replication_runtime.replica_link()?;
        let token = Token(link.connection_id().0);
        (link.has_buffered_input()
            && !self.replication_runtime.replica_main_read_paused()
            && (link.has_pending_main_decompression()
                || (!self.ctx.is_script_busy()
                    && link.has_pending_rdb_receive()
                    && !link.is_dual_rdb_mode_active()))
            && matches!(
                self.replication_runtime.role(ConnectionId(token.0)),
                Some(ReplicationConnection::Replica)
            )
            && self.connection_registry.contains(token)
            && self.transport.is_registered(token))
        .then_some(token)
    }

    pub(crate) fn collect_before_sleep_pending_events(&mut self) -> Vec<RuntimeEvent> {
        let mut events = self.transport.process_pending_data();
        if let Some(token) = self.processable_classic_replica_input_token() {
            if !events
                .iter()
                .any(|event| event.token == token && event.readable)
            {
                events.push(RuntimeEvent {
                    token,
                    readable: true,
                    writable: false,
                    read_hup: false,
                    error: false,
                });
            }
        }
        events.sort_by_key(|event| !self.is_client_subnet_priority(event.token));
        events
    }

    fn dispatch_pending_data(&mut self) -> bool {
        let events = self.collect_before_sleep_pending_events();
        for event in events {
            let outcome = self.dispatch_readiness_now(
                DispatchContext::Normal,
                ReadinessEvent {
                    token: event.token,
                    origin: ReadinessOrigin::PendingData,
                    readable: event.readable,
                    writable: event.writable,
                    read_closed: event.read_hup,
                    error: event.error,
                },
            );
            if matches!(outcome, DispatchOutcome::ShutdownRequested)
                && self.ctx.is_shutdown_requested()
            {
                return true;
            }
        }
        false
    }

    pub(super) fn drain_pending_input(&mut self) -> bool {
        self.dispatch_pending_data() || self.exit_if_shutdown_requested()
    }

    pub(crate) fn replay_deferred_readiness(&mut self, token: Token) {
        let Some(event) = self.blocked_events.take_deferred(token) else {
            return;
        };
        if event.readable
            && !event.error
            && self
                .pending_replication_replay
                .as_ref()
                .is_some_and(|continuation| continuation.main.0 == event.token.0)
        {
            self.blocked_events.defer(event);
            self.schedule_runtime_runnable_next_turn(RunnableKey::DeferredReadiness(token));
            return;
        }
        if !self.transport.is_registered(event.token) {
            return;
        }
        self.dispatch_readiness_now(
            DispatchContext::Normal,
            ReadinessEvent {
                token: event.token,
                origin: ReadinessOrigin::PendingData,
                readable: event.readable,
                writable: event.writable,
                error: event.error,
                read_closed: event.read_hup,
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn replay_one_deferred_readiness(&mut self) {
        let Some(event) = self.blocked_events.pop_front_deferred() else {
            return;
        };
        let token = event.token;
        self.blocked_events.defer(event);
        self.replay_deferred_readiness(token);
    }
}
