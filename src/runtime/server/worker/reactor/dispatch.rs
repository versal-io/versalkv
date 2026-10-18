#[cfg(test)]
use super::TurnTracePoint;
use super::{
    decide_dispatch, BeforePollSlot, DispatchContext, DispatchDecision, DispatchOutcome,
    EventDependencies, EventLane, EventLoopEvent, EventPayload, EventPayloadKind, EventPhase,
    LoopControl, ReadinessEvent, ReadinessOrigin, ReadyEvent, RunnableKey, ServerWorkerState,
    TransportEventDispatchOutcome,
};
use crate::foundation::io::sys::time::mstime;
use crate::foundation::memory::metrics::stats::EventLoopEventOutcome;
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::ConnectionId;
use crate::runtime::server::worker::reactor::blocked_events::DeferredReadyEvent;
use crate::runtime::server::worker::reactor::sources::{SourceOwner, SourceRoute};
use crate::runtime::server::worker::reactor::transport::{PendingRemovalOwner, RuntimeSentinelIo};
use crate::runtime::server::worker::sessions::ClientReadMode;

impl ServerWorkerState {
    pub(crate) fn dispatch_before_poll_slot(
        &mut self,
        slot: BeforePollSlot,
        context: DispatchContext,
    ) -> std::io::Result<LoopControl> {
        while let Some(event) = self.event_batch.pop_before_poll_slot(slot) {
            self.dispatch_event(event, &context);
            if self.exit_if_shutdown_requested() {
                self.event_batch.finish_phase(EventPhase::BeforePoll);
                return Ok(LoopControl::Stop);
            }
        }
        self.event_batch.finish_before_poll_slot(slot);
        Ok(LoopControl::Continue)
    }

    pub(crate) fn register_child_process(
        &mut self,
        owner: crate::runtime::server::worker::reactor::sources::ChildOwner,
        pid: i32,
    ) -> crate::runtime::server::worker::reactor::sources::ChildHandle {
        let child = self
            .child_registry
            .child_for_pid(pid)
            .unwrap_or_else(|| self.child_registry.register_child(owner, pid));
        if !self.child_registry.mark_pidfd_registered(child) {
            return child;
        }
        match crate::foundation::io::sys::process::open_pidfd(pid) {
            Ok(pidfd) => {
                let source = crate::foundation::io::transport::OwnedPollSource::completion(
                    std::fs::File::from(pidfd),
                );
                match self.install_runtime_source(
                    source,
                    SourceOwner::ChildSignal(
                        crate::runtime::server::worker::reactor::sources::ChildSignalOwner::PidFd(
                            child,
                        ),
                    ),
                ) {
                    Ok(handle) => self.child_registry.set_pidfd_source(child, handle),
                    Err(error) => {
                        crate::server_log!(
                            warning,
                            "runtime source lifecycle token=child owner=child signal operation=pidfd-register pid={} error={}",
                            pid,
                            error
                        );
                    }
                }
                self.child_registry.collect_child_exit(child);
            }
            Err(error) => {
                crate::server_log!(
                    debug,
                    "child pidfd unavailable pid={} error={}, using SIGCHLD fallback",
                    pid,
                    error
                );
            }
        }
        child
    }

    pub(crate) fn take_child_completion_for_pid(
        &mut self,
        pid: i32,
    ) -> Option<crate::runtime::server::worker::reactor::sources::ChildCompletionEvent> {
        let child = self.child_registry.child_for_pid(pid)?;
        self.child_registry.collect_child_exit(child);
        self.child_registry.take_completion(child)
    }

    pub(crate) fn take_child_exit_success(
        &mut self,
        owner: crate::runtime::server::worker::reactor::sources::ChildOwner,
        pid: i32,
    ) -> Option<(
        crate::runtime::server::worker::reactor::sources::ChildHandle,
        bool,
    )> {
        self.register_child_process(owner, pid);
        let completion = self.take_child_completion_for_pid(pid)?;
        let succeeded = matches!(
            completion.outcome,
            crate::runtime::server::worker::reactor::sources::ChildExitOutcome::Exited(0)
        );
        Some((completion.child, succeeded))
    }

    pub(crate) fn finalize_child_registry_entry(
        &mut self,
        child: crate::runtime::server::worker::reactor::sources::ChildHandle,
    ) {
        if let Ok(Some(source)) = self.child_registry.finalize_completion(child) {
            self.retire_runtime_source(source.token);
            self.deregister_or_defer(source.token, PendingRemovalOwner::RuntimeSource);
        }
    }

    pub(crate) fn register_active_persistence_child(&mut self) {
        let Some(child) = self.ctx.persistence().child() else {
            let sources = self.child_registry.retain_only_pid(None);
            self.retire_child_sources(sources);
            return;
        };
        let active_pid = match child {
            crate::engine::persistence::PersistenceChild::Rdb { pid, .. }
            | crate::engine::persistence::PersistenceChild::AofRewrite { pid, .. }
            | crate::engine::persistence::PersistenceChild::DisklessSync { pid }
            | crate::engine::persistence::PersistenceChild::DiskSync { pid }
            | crate::engine::persistence::PersistenceChild::SlotMigration { pid } => pid,
        };
        let sources = self.child_registry.retain_only_pid(Some(active_pid));
        self.retire_child_sources(sources);
        match child {
            crate::engine::persistence::PersistenceChild::Rdb { pid, .. } => {
                self.register_child_process(
                    crate::runtime::server::worker::reactor::sources::ChildOwner::RdbSave,
                    pid,
                );
            }
            crate::engine::persistence::PersistenceChild::AofRewrite { pid, .. } => {
                self.register_child_process(
                    crate::runtime::server::worker::reactor::sources::ChildOwner::AofRewrite,
                    pid,
                );
            }
            crate::engine::persistence::PersistenceChild::DisklessSync { pid } => {
                self.register_child_process(
                    crate::runtime::server::worker::reactor::sources::ChildOwner::DisklessReplication,
                    pid,
                );
            }
            crate::engine::persistence::PersistenceChild::DiskSync { pid } => {
                self.register_child_process(
                    crate::runtime::server::worker::reactor::sources::ChildOwner::DiskSyncReplication,
                    pid,
                );
            }
            crate::engine::persistence::PersistenceChild::SlotMigration { .. } => {}
        }
    }

    fn retire_child_sources(
        &mut self,
        sources: Vec<crate::runtime::server::worker::reactor::sources::RuntimeSourceHandle>,
    ) {
        for source in sources {
            self.retire_runtime_source(source.token);
            self.deregister_or_defer(source.token, PendingRemovalOwner::RuntimeSource);
        }
    }

    #[cfg(test)]
    pub(crate) fn handle_transport_failure(&mut self, token: Token) {
        let Some(route) = self.runtime_sources.resolve_token(token).cloned() else {
            return;
        };
        self.handle_source_failure(token, &route.owner);
    }

    #[cfg(test)]
    pub(crate) fn dispatch_transport_event_is_stale_for_test(
        &mut self,
        ready: ReadyEvent,
        flush_after_readable: bool,
    ) -> bool {
        matches!(
            self.dispatch_transport_event_with_outcome(ready, flush_after_readable),
            TransportEventDispatchOutcome {
                outcome: DispatchOutcome::Stale,
                ..
            }
        )
    }

    fn handle_source_failure(&mut self, token: Token, owner: &SourceOwner) {
        match owner {
            SourceOwner::Sentinel => {
                let mut sentinel_io = RuntimeSentinelIo::new(
                    &mut self.transport,
                    &mut self.runtime_sources,
                    &mut self.pending_transport_removals,
                );
                if let Err(error) = self
                    .sentinel_links
                    .remove(&mut sentinel_io, token, mstime())
                {
                    crate::server_log!(
                        warning,
                        "SENTINEL: retaining link {} after deregistration failure: {}",
                        token.0,
                        error
                    );
                }
            }
            SourceOwner::ClusterBus => {
                if self.cluster_bus.is_listener_token(token) {
                    self.remove_cluster_bus_listener(token);
                } else {
                    self.remove_cluster_bus_connection(token);
                }
            }
            SourceOwner::Listener => self.remove_listener(token),
            SourceOwner::Connection => self.handle_connection_transport_failure(token),
            SourceOwner::IoWorkerWake => self.drain_io_completions(),
            SourceOwner::InternalPipe(_) => self.retire_failed_internal_source(token, "readiness"),
            // Later tasks install these source classes together with their
            // concrete teardown state machines.
            SourceOwner::ControlWake | SourceOwner::ChildSignal(_) => {}
        }
    }

    fn handle_connection_transport_failure(&mut self, token: Token) {
        if self.is_pending_transport_removal(token) {
            return;
        }
        if let Some(main) = self.replica_rdb_main_for_failure(token) {
            self.tear_down_master_link(main.0);
            return;
        }
        self.handle_slot_export_connect_failure(token.0);
        crate::server_log!(
            debug,
            "transport failure token={} role={:?} master_link={}",
            token.0,
            self.replication_runtime.role(
                crate::framework::distributed::replication::ConnectionId(token.0)
            ),
            self.is_master_link(token.0)
        );
        if self.is_master_link(token.0) {
            self.ctx.config_mut().replication.master_link_status = "down".to_string();
        }
        self.remove_connection(token.0);
    }

    fn replica_rdb_main_for_failure(&self, token: Token) -> Option<ConnectionId> {
        self.replication_runtime
            .replica_rdb_link()
            .filter(|link| link.connection_id().0 == token.0)?;
        self.replication_runtime
            .channel_pair_for(ConnectionId(token.0))
    }

    #[cfg(test)]
    pub(crate) fn dispatch_transport_event(
        &mut self,
        ready: ReadyEvent,
        flush_after_readable: bool,
    ) -> bool {
        self.dispatch_transport_event_with_outcome(ready, flush_after_readable)
            .may_request_shutdown
    }

    pub(crate) fn collect_after_poll_events(
        &mut self,
        poll_result: Option<(
            std::time::Instant,
            Vec<crate::runtime::server::worker::reactor::transport::RuntimeEvent>,
        )>,
    ) {
        let Some((_cycle_start, events)) = poll_result else {
            return;
        };
        self.ctx.record_eventloop_event_collected(events.len());
        for event in events {
            let lane = self.ready_lane_for(event.token);
            self.event_batch.push(EventLoopEvent::readiness(
                EventPhase::Ready,
                lane,
                ReadinessEvent {
                    token: event.token,
                    origin: ReadinessOrigin::Polled,
                    readable: event.readable,
                    writable: event.writable,
                    read_closed: event.read_hup,
                    error: event.error,
                },
            ));
        }
    }

    pub(crate) fn dispatch_ready_phase(
        &mut self,
        context: DispatchContext,
    ) -> std::io::Result<LoopControl> {
        #[cfg(test)]
        self.trace_turn_for_test(TurnTracePoint::ReadyDispatch);
        while let Some(event) = self.event_batch.pop_ready() {
            self.dispatch_event(event, &context);
            if self.exit_if_shutdown_requested() {
                self.event_batch.finish_phase(EventPhase::Ready);
                return Ok(LoopControl::Stop);
            }
        }
        self.event_batch.finish_phase(EventPhase::Ready);
        Ok(LoopControl::Continue)
    }

    pub(crate) fn dispatch_time_phase(&mut self) -> std::io::Result<LoopControl> {
        #[cfg(test)]
        self.trace_turn_for_test(TurnTracePoint::TimeDispatch);
        while let Some(event) = self.event_batch.pop_time() {
            self.dispatch_event(event, &DispatchContext::Normal);
            if self.exit_if_shutdown_requested() {
                self.event_batch.finish_phase(EventPhase::Time);
                return Ok(LoopControl::Stop);
            }
        }
        self.event_batch.finish_phase(EventPhase::Time);
        Ok(LoopControl::Continue)
    }

    pub(super) fn dispatch_transport_event_with_outcome(
        &mut self,
        ready: ReadyEvent,
        flush_after_readable: bool,
    ) -> TransportEventDispatchOutcome {
        let token = ready.token;
        if self.is_pending_transport_removal(token) || !self.transport.is_registered(token) {
            return TransportEventDispatchOutcome::stale();
        }
        let Some(route) = self.runtime_sources.resolve_token(token).cloned() else {
            return TransportEventDispatchOutcome::stale();
        };
        if ready.is_writable() {
            self.writes.clear_replica_writable_wait(token);
            self.cancel_runtime_runnable(RunnableKey::ClientWrite(token));
            self.cancel_runtime_runnable(RunnableKey::SlotExportWrite(token));
        }
        if matches!(route.owner, SourceOwner::Connection) {
            let read_blocked = (ready.is_readable() || ready.read_hup || ready.error)
                && !self.socket_read_owned_by_main_thread(token);
            let write_blocked =
                ready.is_writable() && !self.socket_write_owned_by_main_thread(token);
            if read_blocked || write_blocked {
                if ready.read_hup || ready.error {
                    self.log_readiness_failure(ready, &route.owner);
                    self.handle_source_failure(token, &route.owner);
                    return self.source_dispatch_outcome(token, &route.owner, false);
                }
                return TransportEventDispatchOutcome::handled();
            }
        }
        if let SourceOwner::InternalPipe(
            crate::runtime::server::worker::reactor::sources::InternalPipeOwner::DisklessRdb {
                transfer_id,
            },
        ) = route.owner.clone()
        {
            if ready.error {
                self.log_readiness_failure(ready, &route.owner);
                return TransportEventDispatchOutcome {
                    outcome: self.on_diskless_pipe_error(transfer_id),
                    may_request_shutdown: false,
                };
            }
        }
        if let SourceOwner::InternalPipe(
            crate::runtime::server::worker::reactor::sources::InternalPipeOwner::SlotSnapshot {
                job_id,
            },
        ) = route.owner.clone()
        {
            if ready.error {
                self.log_readiness_failure(ready, &route.owner);
                return TransportEventDispatchOutcome {
                    outcome: self.on_slot_snapshot_pipe_error(job_id),
                    may_request_shutdown: false,
                };
            }
        }
        if is_terminal_failure(&route.owner, ready) {
            self.log_readiness_failure(ready, &route.owner);
            self.handle_source_failure(token, &route.owner);
            return self.source_dispatch_outcome(token, &route.owner, false);
        }
        self.dispatch_source(route, ready, flush_after_readable)
    }

    fn dispatch_event(
        &mut self,
        event: EventLoopEvent,
        context: &DispatchContext,
    ) -> DispatchOutcome {
        let started_at = std::time::Instant::now();
        let payload_kind = match event.payload {
            EventPayload::Readiness(_) => EventPayloadKind::Readiness,
            EventPayload::Timer(_) => EventPayloadKind::Timer,
            EventPayload::Runnable(_) => EventPayloadKind::Runnable,
            EventPayload::WorkerCompletion(_) => EventPayloadKind::WorkerCompletion,
            EventPayload::Control(_) => EventPayloadKind::Control,
            EventPayload::ChildCompletion(_) => EventPayloadKind::ChildCompletion,
        };
        let dependencies = self.event_dependencies(&event);
        let outcome = if let Some(outcome) = self.dispatch_protected_readiness(context, &event) {
            outcome
        } else {
            match decide_dispatch(context, &dependencies, payload_kind) {
                DispatchDecision::Dispatch => match event.payload {
                    EventPayload::Readiness(readiness) => self.dispatch_queued_readiness(readiness),
                    EventPayload::Timer(_) => self.dispatch_timer_event(event),
                    EventPayload::Runnable(runnable) => self.dispatch_runnable(runnable),
                    EventPayload::WorkerCompletion(completion) => {
                        self.apply_worker_completion(completion);
                        DispatchOutcome::Handled
                    }
                    EventPayload::Control(control) => self.dispatch_control_event(control),
                    EventPayload::ChildCompletion(completion) => {
                        self.dispatch_child_completion(completion, context)
                    }
                },
                DispatchDecision::Defer => self.defer_event(event, dependencies),
                // Blocked loops never collect Time events. This fallback preserves
                // the due event for the normal loop without running its callback.
                DispatchDecision::Suppress if payload_kind == EventPayloadKind::Readiness => {
                    DispatchOutcome::Stale
                }
                DispatchDecision::Suppress => DispatchOutcome::Requeued,
            }
        };
        let outcome = if payload_kind == EventPayloadKind::Readiness
            && (self.ctx.is_shutdown_requested() || self.ctx.shutdown_state().asap)
        {
            DispatchOutcome::ShutdownRequested
        } else {
            outcome
        };
        self.ctx.record_eventloop_event_dispatch(
            self.metrics_outcome(outcome),
            started_at.elapsed().as_micros() as u64,
        );
        outcome
    }

    fn dispatch_protected_readiness(
        &mut self,
        context: &DispatchContext,
        event: &EventLoopEvent,
    ) -> Option<DispatchOutcome> {
        let EventPayload::Readiness(readiness) = event.payload else {
            return None;
        };
        self.protected_readiness_dispatch(context, readiness)
    }

    fn protected_readiness_context(&self, context: &DispatchContext, token: Token) -> bool {
        match context {
            DispatchContext::Normal => false,
            DispatchContext::CommandBlocked { protected_client } => {
                *protected_client == token || self.blocked_events.is_read_protected(token)
            }
            DispatchContext::HostedBlocked {
                protected_resources,
            } => protected_resources.protects_read(token),
        }
    }

    fn defer_protected_readiness(&mut self, readiness: ReadinessEvent) -> DispatchOutcome {
        self.blocked_events.defer(DeferredReadyEvent::new(
            readiness.token,
            readiness.readable,
            false,
            readiness.read_closed,
            false,
        ));
        self.schedule_runtime_runnable_next_turn(RunnableKey::DeferredReadiness(readiness.token));
        if readiness.writable {
            self.dispatch_blocked_writable(readiness.token);
        }
        DispatchOutcome::Deferred
    }

    fn protected_readiness_dispatch(
        &mut self,
        context: &DispatchContext,
        readiness: ReadinessEvent,
    ) -> Option<DispatchOutcome> {
        if !self.protected_readiness_context(context, readiness.token) {
            return None;
        }
        if readiness.error {
            return Some(self.dispatch_queued_readiness(readiness));
        }
        if readiness.readable
            || (readiness.read_closed && self.blocked_events.has_deferred_read(readiness.token))
        {
            return Some(self.defer_protected_readiness(readiness));
        }
        Some(self.dispatch_queued_readiness(readiness))
    }

    fn event_dependencies(&self, event: &EventLoopEvent) -> EventDependencies {
        match event.payload {
            EventPayload::Readiness(readiness) => self
                .runtime_sources
                .resolve_token(readiness.token)
                .map(|route| {
                    route.dependencies(
                        readiness.token,
                        readiness.readable,
                        readiness.writable,
                        readiness.read_closed,
                        readiness.error,
                    )
                })
                .unwrap_or_else(|| {
                    if readiness.readable || readiness.read_closed || readiness.error {
                        EventDependencies::client_read(readiness.token)
                    } else if readiness.writable {
                        EventDependencies::client_write(readiness.token)
                    } else {
                        EventDependencies::none()
                    }
                }),
            EventPayload::Timer(_) => EventDependencies::none(),
            EventPayload::WorkerCompletion(_)
            | EventPayload::Control(_)
            | EventPayload::ChildCompletion(_) => EventDependencies::safe_producer(),
            EventPayload::Runnable(runnable) => match runnable.key {
                super::RunnableKey::ClientRead(token) => EventDependencies::client_read(token),
                super::RunnableKey::ClientWrite(token)
                | super::RunnableKey::SlotExportWrite(token) => {
                    EventDependencies::client_write(token)
                }
                super::RunnableKey::DeferredReadiness(_) => EventDependencies::safe_producer(),
                super::RunnableKey::ClusterBeforeSleep
                | super::RunnableKey::ReplicationAction(_) => EventDependencies::safe_producer(),
                #[cfg(test)]
                super::RunnableKey::Test(_) => EventDependencies::none(),
            },
        }
    }

    fn defer_event(
        &mut self,
        event: EventLoopEvent,
        _dependencies: EventDependencies,
    ) -> DispatchOutcome {
        let EventPayload::Readiness(readiness) = event.payload else {
            return DispatchOutcome::Requeued;
        };
        if readiness.readable || readiness.read_closed || readiness.error {
            self.blocked_events.defer(DeferredReadyEvent::new(
                readiness.token,
                readiness.readable,
                false,
                readiness.read_closed || readiness.error,
                false,
            ));
            self.schedule_runtime_runnable_next_turn(RunnableKey::DeferredReadiness(
                readiness.token,
            ));
        }
        if readiness.writable {
            self.dispatch_blocked_writable(readiness.token);
        }
        DispatchOutcome::Deferred
    }

    fn dispatch_runnable(&mut self, runnable: super::RunnableEvent) -> DispatchOutcome {
        match runnable.key {
            RunnableKey::ClientRead(token) => self.drive_pending_client_read(token),
            RunnableKey::ClientWrite(token) => {
                let connection_id = ConnectionId(token.0);
                let was_serviced = self.replication_runtime.role(connection_id).is_none()
                    && self.writes.ordinary.was_serviced(token);
                if was_serviced {
                    self.schedule_runtime_runnable_next_turn(runnable.key);
                } else {
                    #[cfg(test)]
                    if self.replication_runtime.role(connection_id).is_none() {
                        self.trace_turn_for_test(TurnTracePoint::OrdinaryWrite);
                    }
                    self.handle_writable(token);
                }
            }
            RunnableKey::SlotExportWrite(token) => {
                if self.enforce_client_output_buffer_limit_for(token.0) {
                    return DispatchOutcome::OwnerClosed;
                }
                if self.writes.slot_exports.was_serviced(token) {
                    self.schedule_runtime_runnable_next_turn(runnable.key);
                } else {
                    self.handle_writable(token);
                }
            }
            RunnableKey::DeferredReadiness(token) => self.replay_deferred_readiness(token),
            RunnableKey::ClusterBeforeSleep => self.recompute_cluster_state_before_sleep(),
            RunnableKey::ReplicationAction(action) => {
                self.drive_pending_replication_action(action);
            }
            #[cfg(test)]
            RunnableKey::Test(_) => {}
        }
        if self.event_batch.contains_runnable(runnable.key) {
            DispatchOutcome::Requeued
        } else {
            DispatchOutcome::Handled
        }
    }

    fn dispatch_control_event(&mut self, control: super::ControlEvent) -> DispatchOutcome {
        match control {
            super::ControlEvent::IoWorkerWake => self.drain_io_completions(),
            super::ControlEvent::Shutdown
            | super::ControlEvent::ScriptKill
            | super::ControlEvent::FunctionKill
            | super::ControlEvent::ChildSignalWake => {}
        }
        DispatchOutcome::Handled
    }

    fn dispatch_child_completion(
        &mut self,
        completion: crate::runtime::server::worker::reactor::sources::ChildCompletionEvent,
        context: &DispatchContext,
    ) -> DispatchOutcome {
        if !matches!(context, DispatchContext::Normal) {
            self.child_registry.defer_completion(completion.child);
            return DispatchOutcome::Handled;
        }
        if matches!(
            self.child_registry.owner(completion.child),
            Some(crate::runtime::server::worker::reactor::sources::ChildOwner::DisklessReplication)
        ) {
            return self.on_diskless_child_completed(completion.child, completion.outcome);
        }
        if let Some(crate::runtime::server::worker::reactor::sources::ChildOwner::SlotSnapshot(
            id,
        )) = self.child_registry.owner(completion.child).cloned()
        {
            return self.on_slot_snapshot_child_completed(id, completion.outcome);
        }
        if matches!(
            self.child_registry.owner(completion.child),
            Some(crate::runtime::server::worker::reactor::sources::ChildOwner::DiskSyncReplication)
        ) {
            return self.on_disk_sync_child_completed(completion.child, completion.outcome);
        }
        if matches!(
            self.child_registry.owner(completion.child),
            Some(crate::runtime::server::worker::reactor::sources::ChildOwner::DirectDualRdb)
        ) {
            return self.on_direct_dual_child_completed(completion.child, completion.outcome);
        }
        DispatchOutcome::Handled
    }

    #[cfg(test)]
    pub(crate) fn dispatch_child_completion_for_test(
        &mut self,
        completion: crate::runtime::server::worker::reactor::sources::ChildCompletionEvent,
        context: DispatchContext,
    ) -> DispatchOutcome {
        self.dispatch_child_completion(completion, &context)
    }

    fn dispatch_internal_pipe_owner(
        &mut self,
        owner: crate::runtime::server::worker::reactor::sources::InternalPipeOwner,
    ) -> DispatchOutcome {
        match owner {
            crate::runtime::server::worker::reactor::sources::InternalPipeOwner::DisklessRdb { transfer_id } => {
                self.on_diskless_pipe_readable(transfer_id)
            }
            crate::runtime::server::worker::reactor::sources::InternalPipeOwner::SlotSnapshot { job_id } => {
                self.on_slot_snapshot_pipe_readable(job_id)
            }
            crate::runtime::server::worker::reactor::sources::InternalPipeOwner::DirectDualPhase { child } => {
                self.on_direct_dual_phase_readable(child)
            }
        }
    }

    fn retire_failed_internal_source(&mut self, token: Token, operation: &str) {
        self.retire_runtime_source(token);
        self.transport.quarantine_registered(token);
        if let Err(error) = self.transport.set_source_enabled(token, false) {
            crate::server_log!(
                warning,
                "runtime source lifecycle token={} owner=internal pipe operation=quarantine-{} error={}",
                token.0,
                operation,
                error
            );
        }
        self.deregister_or_defer(token, PendingRemovalOwner::RuntimeSource);
    }

    fn dispatch_queued_readiness(&mut self, readiness: ReadinessEvent) -> DispatchOutcome {
        let ready = ReadyEvent {
            token: readiness.token,
            readable: readiness.readable,
            writable: readiness.writable,
            error: readiness.error,
            read_hup: readiness.read_closed,
        };
        self.dispatch_transport_event_with_outcome(
            ready,
            self.flush_after_readable(readiness.origin),
        )
        .outcome
    }

    pub(super) fn dispatch_readiness_in_context(
        &mut self,
        context: DispatchContext,
        readiness: ReadinessEvent,
    ) -> DispatchOutcome {
        let lane = self.ready_lane_for(readiness.token);
        self.dispatch_event(
            EventLoopEvent::readiness(EventPhase::Ready, lane, readiness),
            &context,
        )
    }

    fn flush_after_readable(&self, origin: ReadinessOrigin) -> bool {
        matches!(origin, ReadinessOrigin::Polled) && !self.ctx.config().persistence.aof_enabled
    }

    fn ready_lane_for(&self, token: Token) -> EventLane {
        match self
            .runtime_sources
            .resolve_token(token)
            .map(|route| &route.owner)
        {
            Some(
                SourceOwner::IoWorkerWake | SourceOwner::ControlWake | SourceOwner::ChildSignal(_),
            ) => EventLane::Control,
            _ if self.is_master_link(token.0) => EventLane::Upstream,
            _ => EventLane::Normal,
        }
    }

    fn metrics_outcome(&self, outcome: DispatchOutcome) -> EventLoopEventOutcome {
        match outcome {
            DispatchOutcome::Handled | DispatchOutcome::ShutdownRequested => {
                EventLoopEventOutcome::Handled
            }
            DispatchOutcome::Deferred => EventLoopEventOutcome::Deferred,
            DispatchOutcome::Requeued => EventLoopEventOutcome::Requeued,
            DispatchOutcome::Stale => EventLoopEventOutcome::Stale,
            DispatchOutcome::OwnerClosed => EventLoopEventOutcome::OwnerClosed,
        }
    }

    fn dispatch_source(
        &mut self,
        route: SourceRoute,
        ready: ReadyEvent,
        flush_after_readable: bool,
    ) -> TransportEventDispatchOutcome {
        match route.owner {
            SourceOwner::IoWorkerWake => {
                self.dispatch_event(
                    EventLoopEvent::control(super::ControlEvent::IoWorkerWake),
                    &DispatchContext::Normal,
                );
                TransportEventDispatchOutcome::handled()
            }
            SourceOwner::Listener => self.dispatch_listener_readiness(ready),
            SourceOwner::Connection => {
                self.dispatch_connection_readiness(ready.token, ready, flush_after_readable)
            }
            SourceOwner::ClusterBus => self.dispatch_cluster_readiness(ready.token, ready),
            SourceOwner::Sentinel => self.dispatch_sentinel_readiness(ready.token, ready),
            // Task 5 keeps pre-migration producer work behind the compatibility
            // relay until Tasks 9-11 install concrete pipe handlers.
            SourceOwner::InternalPipe(owner) => {
                let _ = self.dispatch_internal_pipe_owner(owner);
                TransportEventDispatchOutcome::handled()
            }
            SourceOwner::ControlWake => TransportEventDispatchOutcome::stale(),
            SourceOwner::ChildSignal(owner) => {
                let produced_completion = match owner {
                    crate::runtime::server::worker::reactor::sources::ChildSignalOwner::SelfPipe => {
                        self.dispatch_child_signal_wake(ready.token)
                    }
                    crate::runtime::server::worker::reactor::sources::ChildSignalOwner::PidFd(child) => {
                        self.dispatch_child_pidfd_ready(child)
                    }
                };
                if produced_completion {
                    TransportEventDispatchOutcome::handled()
                } else {
                    TransportEventDispatchOutcome::stale()
                }
            }
        }
    }

    fn dispatch_child_pidfd_ready(
        &mut self,
        child: crate::runtime::server::worker::reactor::sources::ChildHandle,
    ) -> bool {
        self.child_registry.collect_child_exit(child);
        self.collect_child_completion_events() > 0
    }

    fn dispatch_child_signal_wake(&mut self, token: Token) -> bool {
        let mut buffer = [0u8; 64];
        loop {
            match self.transport.read_source(token, &mut buffer) {
                Ok(0) => break,
                Ok(_) => continue,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => {
                    crate::server_log!(
                        warning,
                        "runtime source lifecycle token={} owner=child signal operation=read-wake error={}",
                        token.0,
                        error
                    );
                    self.retire_failed_internal_source(token, "child-signal-read");
                    return false;
                }
            }
        }
        if crate::foundation::io::sys::signal::take_child_signal_pending() {
            return self.collect_child_completion_events() > 0;
        }
        false
    }

    fn dispatch_listener_readiness(&mut self, ready: ReadyEvent) -> TransportEventDispatchOutcome {
        if ready.is_readable() {
            self.handle_listener_token(ready.token);
        }
        self.finish_readable_hup(ready.token, &SourceOwner::Listener, ready.read_hup);
        self.source_dispatch_outcome(ready.token, &SourceOwner::Listener, false)
    }

    fn dispatch_connection_readiness(
        &mut self,
        token: Token,
        ready: ReadyEvent,
        flush_after_readable: bool,
    ) -> TransportEventDispatchOutcome {
        let aof_reply_barrier = self.ctx.config().persistence.aof_enabled;
        let preflush_writable_before_read =
            aof_reply_barrier && ready.is_readable() && ready.is_writable();
        if preflush_writable_before_read {
            self.handle_writable(token);
        }
        if ready.is_readable()
            && self
                .pending_replication_replay
                .as_ref()
                .is_some_and(|continuation| continuation.main.0 == token.0)
        {
            // RDB completion may disable this main channel after poll() has
            // already returned a readable event for it. Keep that event until
            // the older pending stream is drained, matching C's temporary
            // removal of the primary read handler around streamReplDataBufToDb.
            self.blocked_events.defer(DeferredReadyEvent::new(
                token,
                true,
                false,
                ready.read_hup,
                false,
            ));
            self.schedule_runtime_runnable_next_turn(RunnableKey::DeferredReadiness(token));
            if ready.is_writable() {
                self.handle_writable(token);
            }
            return TransportEventDispatchOutcome::deferred();
        }
        if ready.is_readable() && self.handle_slot_export_readable(token.0) {
            if ready.is_writable()
                && self
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(token.0))
            {
                self.handle_writable(token);
            }
            self.finish_readable_hup(token, &SourceOwner::Connection, ready.read_hup);
            return self.source_dispatch_outcome(token, &SourceOwner::Connection, false);
        }
        if ready.has_readable_hup() && self.replica_rdb_main_for_failure(token).is_some() {
            if let Some(connection_token) = self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(token.0))
                .then_some(token.0)
            {
                self.recv_rdb_channel_streaming_after_hup(token.0, connection_token);
            }
        } else if ready.has_readable_hup() && !self.is_master_link(token.0) {
            self.drain_client_readable_after_hup(token.0);
            return self.source_dispatch_outcome(token, &SourceOwner::Connection, true);
        } else if ready.is_readable() {
            self.handle_readable(token.0, ClientReadMode::Fair);
            if flush_after_readable
                && !preflush_writable_before_read
                && !self.slot_migration_runtime.is_export_connection(token.0)
            {
                self.handle_writable(token);
            }
        }
        if ready.is_writable() && !preflush_writable_before_read {
            self.handle_writable(token);
        }
        self.finish_readable_hup(token, &SourceOwner::Connection, ready.read_hup);
        self.source_dispatch_outcome(token, &SourceOwner::Connection, true)
    }

    fn dispatch_sentinel_readiness(
        &mut self,
        token: Token,
        ready: ReadyEvent,
    ) -> TransportEventDispatchOutcome {
        if ready.is_readable() {
            let mut sentinel_io = RuntimeSentinelIo::new(
                &mut self.transport,
                &mut self.runtime_sources,
                &mut self.pending_transport_removals,
            );
            if ready.read_hup {
                self.sentinel_links.readable_after_hup(
                    &mut self.ctx,
                    &mut sentinel_io,
                    token,
                    mstime(),
                );
            } else {
                self.sentinel_links
                    .readable(&mut self.ctx, &mut sentinel_io, token, mstime());
            }
        }
        if ready.is_writable() {
            let mut sentinel_io = RuntimeSentinelIo::new(
                &mut self.transport,
                &mut self.runtime_sources,
                &mut self.pending_transport_removals,
            );
            self.sentinel_links
                .writable(&self.ctx, &mut sentinel_io, token, mstime());
        }
        self.finish_readable_hup(token, &SourceOwner::Sentinel, ready.read_hup);
        self.source_dispatch_outcome(token, &SourceOwner::Sentinel, false)
    }

    fn dispatch_cluster_readiness(
        &mut self,
        token: Token,
        ready: ReadyEvent,
    ) -> TransportEventDispatchOutcome {
        let is_listener = self.cluster_bus.is_listener_token(token);
        let reply_pending = self
            .cluster_bus
            .connections
            .get(&token.0)
            .is_some_and(|connection| !connection.write_buf.is_empty());
        if !is_listener && (ready.is_writable() || reply_pending) {
            self.handle_cluster_bus_writable(token);
        }
        if self.is_pending_transport_removal(token) || !self.transport.is_registered(token) {
            return self.source_dispatch_outcome(token, &SourceOwner::ClusterBus, false);
        }
        if ready.is_readable() {
            if is_listener {
                self.handle_cluster_bus_listener(token);
            } else {
                self.handle_cluster_bus_readable(token);
            }
        }
        self.finish_readable_hup(token, &SourceOwner::ClusterBus, ready.read_hup);
        self.source_dispatch_outcome(token, &SourceOwner::ClusterBus, false)
    }

    fn finish_readable_hup(&mut self, token: Token, owner: &SourceOwner, read_hup: bool) {
        if read_hup
            && !self.is_pending_transport_removal(token)
            && self.transport.is_registered(token)
        {
            self.handle_source_failure(token, owner);
        }
    }

    fn log_readiness_failure(&mut self, ready: ReadyEvent, owner: &SourceOwner) {
        let error = if matches!(owner, SourceOwner::Connection)
            && self.is_master_link(ready.token.0)
            && self.ctx.config().replication.master_link_status == "connect"
        {
            match self.transport.take_connect_error(ready.token) {
                Ok(Some(error)) | Err(error) => error.to_string(),
                Ok(None) => "primary connect readiness failed without a socket error".to_string(),
            }
        } else if ready.read_hup {
            "transport read side closed".to_string()
        } else {
            "transport reported error readiness".to_string()
        };
        crate::server_log!(
            warning,
            "runtime source lifecycle token={} owner={} operation=readiness-failure error={}",
            ready.token.0,
            owner.label(),
            error
        );
    }

    fn source_dispatch_outcome(
        &self,
        token: Token,
        owner: &SourceOwner,
        may_request_shutdown: bool,
    ) -> TransportEventDispatchOutcome {
        let owner_closed =
            self.is_pending_transport_removal(token) || !self.transport.is_registered(token);
        let outcome = match owner {
            SourceOwner::IoWorkerWake | SourceOwner::ControlWake | SourceOwner::ChildSignal(_)
                if owner_closed =>
            {
                DispatchOutcome::Handled
            }
            _ if owner_closed => DispatchOutcome::OwnerClosed,
            _ => DispatchOutcome::Handled,
        };
        TransportEventDispatchOutcome {
            outcome,
            may_request_shutdown,
        }
    }
}

fn is_terminal_failure(owner: &SourceOwner, ready: ReadyEvent) -> bool {
    match owner {
        SourceOwner::InternalPipe(
            crate::runtime::server::worker::reactor::sources::InternalPipeOwner::DisklessRdb {
                ..
            },
        ) => ready.error,
        SourceOwner::InternalPipe(
            crate::runtime::server::worker::reactor::sources::InternalPipeOwner::SlotSnapshot {
                ..
            },
        ) => ready.error,
        SourceOwner::InternalPipe(
            crate::runtime::server::worker::reactor::sources::InternalPipeOwner::DirectDualPhase {
                ..
            },
        ) => ready.error,
        _ => ready.is_failure(),
    }
}
