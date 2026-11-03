use super::{
    BlockedRoundPhase, DispatchContext, DispatchOutcome, ReadinessEvent, ReadinessOrigin,
    ReadyEvent, ServerWorkerState, BLOCKED_EVENT_SERVICE_ROUNDS,
};
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::reactor::blocked_events::{
    BlockedOperation, BlockedServiceResult,
};
use std::time::Duration;

impl ServerWorkerState {
    pub(crate) fn process_events_while_blocked(
        &mut self,
        operation: BlockedOperation,
    ) -> BlockedServiceResult {
        let _command_time = crate::foundation::io::sys::time::enter_fresh_command_time_snapshot();
        let mut result = BlockedServiceResult::default();
        self.blocked_events.push_scope(operation);
        for round in 0..BLOCKED_EVENT_SERVICE_ROUNDS {
            let _round_time = crate::foundation::io::sys::time::enter_fresh_command_time_snapshot();
            self.prepare_blocked_round_write_phase(round);
            let mut did_useful_work = false;
            // Nested servicing is deliberately limited to completion drains,
            // blocked-safe producers, and transport dispatch. It must not run
            // normal time events, cron, child finalizers, or module hooks.
            self.drain_blocked_round_initial_completions();
            self.record_blocked_round_phase(BlockedRoundPhase::PendingData);
            let pending_events = self.transport.process_pending_data();
            let mut stop_after_epilogue = false;
            for event in pending_events {
                let outcome = self.dispatch_hosted_readiness(ReadinessEvent {
                    token: event.token,
                    origin: ReadinessOrigin::PendingData,
                    readable: event.readable,
                    writable: event.writable,
                    read_closed: event.read_hup,
                    error: event.error,
                });
                did_useful_work |= blocked_outcome_did_work(outcome);
                result.record_outcome(outcome);
                if self.ctx.is_shutdown_requested() {
                    stop_after_epilogue = true;
                    break;
                }
            }
            if !stop_after_epilogue {
                self.flush_blocked_round_aof_barrier();
                did_useful_work |= self.flush_blocked_round_eligible_writes();
                self.record_blocked_round_phase(BlockedRoundPhase::Poll);
                match self.transport.poll_events(Some(Duration::ZERO)) {
                    Ok(events) => {
                        self.run_blocked_round_restricted_after_sleep(events.len());
                        if !events.is_empty() {
                            result.poll_rounds += 1;
                            self.record_blocked_round_phase(BlockedRoundPhase::PolledReadiness);
                            for event in events {
                                let outcome = self.dispatch_hosted_readiness(ReadinessEvent {
                                    token: event.token,
                                    origin: ReadinessOrigin::Polled,
                                    readable: event.readable,
                                    writable: event.writable,
                                    read_closed: event.read_hup,
                                    error: event.error,
                                });
                                did_useful_work |= blocked_outcome_did_work(outcome);
                                result.record_outcome(outcome);
                                if self.ctx.is_shutdown_requested() {
                                    stop_after_epilogue = true;
                                    break;
                                }
                            }
                        }
                    }
                    Err(error) => {
                        crate::server_log!(warning, "blocked event poll failed: {}", error);
                        stop_after_epilogue = true;
                    }
                }
            }
            self.drain_blocked_round_final_completions();
            if stop_after_epilogue {
                break;
            }
            if !did_useful_work {
                break;
            }
        }
        self.run_while_blocked_cron();
        debug_assert_eq!(self.blocked_events.current(), Some(operation));
        self.blocked_events.pop_scope();
        result
    }

    fn dispatch_hosted_readiness(&mut self, readiness: ReadinessEvent) -> DispatchOutcome {
        self.dispatch_readiness_now(
            DispatchContext::HostedBlocked {
                protected_resources: self.blocked_events.protected_resources(),
            },
            readiness,
        )
    }

    pub(super) fn dispatch_blocked_writable(&mut self, token: Token) -> bool {
        !matches!(
            self.dispatch_transport_event_with_outcome(
                ReadyEvent {
                    token,
                    readable: false,
                    writable: true,
                    error: false,
                    read_hup: false,
                },
                true,
            )
            .outcome,
            DispatchOutcome::Stale
        )
    }

    pub(super) fn run_while_blocked_cron(&mut self) {
        #[cfg(test)]
        {
            self.blocked_cron_calls += 1;
        }
        self.check_pending_signal();
        self.drive_pending_shutdown();
    }

    pub(super) fn prepare_blocked_round_write_phase(&mut self, _round: usize) {
        self.event_batch.activate_next_turn_before_poll_slots(&[
            crate::runtime::server::worker::reactor::BeforePollSlot::DistributedWrites,
            crate::runtime::server::worker::reactor::BeforePollSlot::OrdinaryWrites,
        ]);
    }

    pub(super) fn drain_blocked_round_initial_completions(&mut self) {
        self.record_blocked_round_phase(BlockedRoundPhase::InitialCompletions);
        self.drain_io_completions();
    }

    pub(super) fn flush_blocked_round_aof_barrier(&mut self) {
        self.record_blocked_round_phase(BlockedRoundPhase::AofBarrier);
        self.flush_aof_if_needed();
    }

    pub(super) fn flush_blocked_round_eligible_writes(&mut self) -> bool {
        self.record_blocked_round_phase(BlockedRoundPhase::EligibleWrites);
        let has_pending_writes = self
            .connection_registry
            .iter()
            .any(|(_, connection)| connection.has_pending_output());
        self.schedule_pending_replica_writes_before_sleep();
        self.drain_current_slot_export_writes();
        self.drain_current_ordinary_writes();
        has_pending_writes
    }

    pub(super) fn run_blocked_round_restricted_after_sleep(&mut self, event_count: usize) {
        self.record_blocked_round_phase(BlockedRoundPhase::RestrictedAfterSleep);
        self.io_threads_after_poll(event_count);
    }

    pub(super) fn drain_blocked_round_final_completions(&mut self) {
        self.record_blocked_round_phase(BlockedRoundPhase::FinalCompletions);
        self.drain_io_completions();
    }

    pub(super) fn record_blocked_round_phase(&mut self, phase: BlockedRoundPhase) {
        #[cfg(test)]
        self.blocked_round_trace.push(phase);
        #[cfg(not(test))]
        let _ = phase;
    }

    #[cfg(test)]
    pub(crate) fn clear_blocked_round_trace_for_test(&mut self) {
        self.blocked_round_trace.clear();
    }

    #[cfg(test)]
    pub(crate) fn blocked_round_trace_for_test(&self) -> &[BlockedRoundPhase] {
        &self.blocked_round_trace
    }

    #[cfg(test)]
    pub(crate) fn blocked_cron_calls_for_test(&self) -> usize {
        self.blocked_cron_calls
    }
}

fn blocked_outcome_did_work(outcome: DispatchOutcome) -> bool {
    matches!(
        outcome,
        DispatchOutcome::Handled
            | DispatchOutcome::Deferred
            | DispatchOutcome::OwnerClosed
            | DispatchOutcome::ShutdownRequested
    )
}
