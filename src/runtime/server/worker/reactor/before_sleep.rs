use crate::foundation::io::sys::time::MonotonicInstant;
use crate::runtime::server::worker::reactor::timers::{RuntimeTimeEvent, TimeEventAction};
use crate::runtime::server::worker::EventLoopCycleStart;
use std::time::Duration;

use super::super::ServerWorkerState;
#[cfg(test)]
use super::TurnTracePoint;
use super::{
    server_cron_interval, BeforePollSlot, DispatchContext, EventLoopEvent, EventOwner, EventPhase,
    LoopControl, RunnableKey,
};

impl ServerWorkerState {
    pub(super) fn sync_module_timer_event(&mut self) {
        if let Some(id) = self.module_timer_event.take() {
            self.time_events.delete_time_event(id);
        }
        let now = MonotonicInstant::now();
        let Some(delay) = self.ctx.module_registry().next_timer_timeout(now) else {
            return;
        };
        let id =
            self.time_events
                .create_time_event(now, delay, None, RuntimeTimeEvent::ModuleTimer);
        self.module_timer_event = Some(id);
    }

    pub(crate) fn register_server_cron(&mut self) {
        self.time_events.create_time_event(
            MonotonicInstant::now(),
            Duration::ZERO,
            None,
            RuntimeTimeEvent::ServerCron,
        );
    }

    #[cfg(test)]
    pub(crate) fn create_test_time_event(
        &mut self,
        callback: impl FnOnce(&mut ServerWorkerState) + 'static,
    ) {
        let index = self.test_time_event_callbacks.len();
        self.test_time_event_callbacks
            .push(Some(Box::new(callback)));
        self.time_events.create_time_event(
            MonotonicInstant::now(),
            Duration::ZERO,
            None,
            RuntimeTimeEvent::Test(index),
        );
    }

    pub(super) fn recompute_cluster_state_before_sleep(&mut self) {
        // Cluster packet handlers defer election decisions across the prior
        // ready and time phases. Settle them at the same beforeSleep boundary
        // C uses for CLUSTER_TODO_HANDLE_FAILOVER.
        self.finish_cluster_bus_receive_batch();
        if let Some(state) = self.ctx.cluster_state_mut().as_deref_mut() {
            state.recompute_state_if_dirty();
        }
    }

    pub(crate) fn collect_due_time_events(&mut self) {
        let now = MonotonicInstant::now();
        let due_events = self.time_events.take_due(now);
        for (offset, event) in due_events.into_iter().enumerate() {
            let event_id = event.id;
            let payload = event.kind;
            self.pending_due_time_events.insert(event_id, event);
            let deadline = now.saturating_add(Duration::from_nanos(offset as u64));
            self.enqueue_due_timer(deadline, event_id, payload);
        }
    }

    pub(crate) fn run_before_poll_phase(&mut self) -> std::io::Result<LoopControl> {
        #[cfg(test)]
        self.trace_turn_for_test(TurnTracePoint::BeforePoll);
        self.register_active_persistence_child();
        self.collect_child_completion_events();
        if self.drain_pending_input() {
            self.event_batch.finish_phase(EventPhase::BeforePoll);
            return Ok(LoopControl::Stop);
        }
        if self.dispatch_before_poll_slot(BeforePollSlot::PendingInput, DispatchContext::Normal)?
            == LoopControl::Stop
        {
            return Ok(LoopControl::Stop);
        }
        if self.dispatch_before_poll_slot(BeforePollSlot::PreAof, DispatchContext::Normal)?
            == LoopControl::Stop
        {
            return Ok(LoopControl::Stop);
        }
        if self.exit_if_shutdown_requested() {
            self.event_batch.finish_phase(EventPhase::BeforePoll);
            return Ok(LoopControl::Stop);
        }
        if self.ctx.needs_eviction() {
            self.schedule_eviction();
        }
        if self.ctx.blocking_registry().count() > 0 {
            self.check_blocked_timeouts();
        }
        self.drain_protected_clients();
        self.drain_blocked_role_rejections();
        if self.ctx.cluster_state().is_some() {
            self.schedule_runtime_runnable(RunnableKey::ClusterBeforeSleep);
        }
        if self.dispatch_before_poll_slot(BeforePollSlot::Maintenance, DispatchContext::Normal)?
            == LoopControl::Stop
        {
            return Ok(LoopControl::Stop);
        }
        self.record_completed_event_loop_cycle();
        if self.expire_grace_psync_waits() > 0 {
            self.refresh_replication_view();
        }
        self.sync_module_timer_event();
        self.evict_clients();
        self.flush_aof_if_needed();
        if self.dispatch_before_poll_slot(BeforePollSlot::AofBarrier, DispatchContext::Normal)?
            == LoopControl::Stop
        {
            return Ok(LoopControl::Stop);
        }
        if self
            .dispatch_before_poll_slot(BeforePollSlot::OrdinaryWrites, DispatchContext::Normal)?
            == LoopControl::Stop
        {
            return Ok(LoopControl::Stop);
        }
        self.drain_acl_kills();
        self.schedule_pending_replica_writes_before_sleep();
        if self
            .dispatch_before_poll_slot(BeforePollSlot::DistributedWrites, DispatchContext::Normal)?
            == LoopControl::Stop
        {
            return Ok(LoopControl::Stop);
        }
        self.io_threads_before_sleep();
        if self
            .dispatch_before_poll_slot(BeforePollSlot::PollPreparation, DispatchContext::Normal)?
            == LoopControl::Stop
        {
            return Ok(LoopControl::Stop);
        }
        self.event_batch.finish_phase(EventPhase::BeforePoll);
        if self.exit_if_shutdown_requested() {
            Ok(LoopControl::Stop)
        } else {
            Ok(LoopControl::Continue)
        }
    }

    pub(crate) fn collect_child_completion_events(&mut self) -> usize {
        self.child_registry.collect_exited_children();
        let completions = self.child_registry.drain_completions();
        let count = completions.len();
        for completion in completions {
            self.enqueue_child_completion(completion);
        }
        count
    }

    pub(crate) fn dispatch_timer_event(&mut self, event: EventLoopEvent) -> super::DispatchOutcome {
        let EventOwner::Timer(id) = event.owner else {
            return super::DispatchOutcome::Stale;
        };
        let Some(due) = self.pending_due_time_events.remove(&id) else {
            return super::DispatchOutcome::Stale;
        };
        let now = MonotonicInstant::now();
        let action = match due.kind {
            RuntimeTimeEvent::ModuleTimer => {
                self.ctx.run_expired_module_timers(now);
                TimeEventAction::Delete
            }
            RuntimeTimeEvent::ServerCron => {
                let start = std::time::Instant::now();
                self.run_background_tasks();
                let duration_usec = start.elapsed().as_micros() as u64;
                self.ctx.record_eventloop_cron(duration_usec);
                TimeEventAction::RescheduleAfter(server_cron_interval(self.ctx.config().server.hz))
            }
            RuntimeTimeEvent::EvictionContinuation => {
                self.run_eviction();
                TimeEventAction::Delete
            }
            RuntimeTimeEvent::BlockedTimeout | RuntimeTimeEvent::ReplicationReconnect => {
                TimeEventAction::Delete
            }
            #[cfg(test)]
            RuntimeTimeEvent::Test(index) => {
                if let Some(callback) = self
                    .test_time_event_callbacks
                    .get_mut(index)
                    .and_then(Option::take)
                {
                    callback(self);
                }
                TimeEventAction::Delete
            }
        };
        self.time_events.complete(due, now, action);
        super::DispatchOutcome::Handled
    }

    pub(crate) fn finish_turn_pending_outputs(&mut self) {
        self.flush_aof_if_needed();
        self.schedule_pending_replica_writes_before_sleep();
        self.event_batch.activate_next_turn_before_poll_slots(&[
            BeforePollSlot::OrdinaryWrites,
            BeforePollSlot::DistributedWrites,
        ]);
        self.drive_pending_ordinary_writes();
        self.drive_pending_slot_export_writes();
    }

    pub(crate) fn start_event_loop_cycle(
        &mut self,
        cycle_start: std::time::Instant,
        cycle_cmd_start: u64,
        cycle_processed_start: u64,
    ) {
        self.event_loop_cycle_start = Some(EventLoopCycleStart {
            started_at: cycle_start,
            command_time_usec: cycle_cmd_start,
            commands_processed: cycle_processed_start,
        });
    }

    pub(super) fn record_completed_event_loop_cycle(&mut self) {
        let Some(cycle) = self.event_loop_cycle_start.take() else {
            return;
        };
        let cycle_duration_usec = cycle.started_at.elapsed().as_micros() as u64;
        let cycle_cmd_duration_usec = self
            .ctx
            .stats()
            .active_time_usec
            .saturating_sub(cycle.command_time_usec);
        let cycle_cmd_count = self
            .ctx
            .stats()
            .total_commands_processed
            .saturating_sub(cycle.commands_processed);
        self.ctx.record_eventloop_cycle(
            cycle_duration_usec.max(1),
            cycle_cmd_duration_usec,
            cycle_cmd_count,
        );
    }
}
