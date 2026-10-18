use std::time::Duration;

pub(crate) mod io_workers;
pub(crate) mod timers;
pub(crate) mod transport;

use self::blocked_events::BlockedOperation;
#[cfg(test)]
use self::sources::ChildOwner;
use self::sources::{ChildCompletionEvent, ChildHandle};
use super::ServerWorkerState;
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::reactor::io_workers::WorkerCompletionEvent;
use crate::runtime::server::worker::reactor::timers::TimeEventId;
use crate::runtime::server::worker::reactor::transport::RuntimeEvent;

mod before_sleep;
pub(crate) mod blocked_events;
mod blocked_service;
mod dispatch;
mod pending_input;
mod poll;
pub(crate) mod queue;
pub(crate) mod sources;

pub(super) const BLOCKED_EVENT_SERVICE_ROUNDS: usize = 4;

/// The only phases admitted by either nested blocked-event loop.
///
/// Keeping the contract explicit prevents command and hosted re-entry from
/// quietly regaining normal before-sleep or after-sleep work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BlockedRoundPhase {
    InitialCompletions,
    PendingData,
    AofBarrier,
    EligibleWrites,
    Poll,
    RestrictedAfterSleep,
    PolledReadiness,
    FinalCompletions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum EventPhase {
    BeforePoll,
    Ready,
    Time,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LoopControl {
    Continue,
    Stop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EventLane {
    Control,
    Upstream,
    Normal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum BeforePollSlot {
    PendingInput,
    PreAof,
    Maintenance,
    AofBarrier,
    OrdinaryWrites,
    DistributedWrites,
    PollPreparation,
}

#[derive(Debug)]
pub(crate) struct EventLoopEvent {
    pub(crate) sequence: u64,
    pub(crate) phase: EventPhase,
    pub(crate) lane: EventLane,
    pub(crate) owner: EventOwner,
    pub(crate) payload: EventPayload,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReadinessEvent {
    pub(crate) token: Token,
    pub(crate) origin: ReadinessOrigin,
    pub(crate) readable: bool,
    pub(crate) writable: bool,
    pub(crate) read_closed: bool,
    pub(crate) error: bool,
}

#[cfg(test)]
impl ReadinessEvent {
    pub(crate) fn readable(token: Token, origin: ReadinessOrigin) -> Self {
        Self {
            token,
            origin,
            readable: true,
            writable: false,
            read_closed: false,
            error: false,
        }
    }

    pub(crate) fn writable(token: Token, origin: ReadinessOrigin) -> Self {
        Self {
            token,
            origin,
            readable: false,
            writable: true,
            read_closed: false,
            error: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReadinessOrigin {
    PendingData,
    Polled,
}

#[derive(Debug)]
pub(crate) enum EventPayload {
    Readiness(ReadinessEvent),
    Timer(TimerEvent),
    Runnable(RunnableEvent),
    WorkerCompletion(WorkerCompletionEvent),
    Control(ControlEvent),
    ChildCompletion(ChildCompletionEvent),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ControlEvent {
    IoWorkerWake,
    // TODO(dead_code): install dedicated control producers for these reserved events.
    #[allow(dead_code)]
    Shutdown,
    #[allow(dead_code)]
    ScriptKill,
    #[allow(dead_code)]
    FunctionKill,
    #[allow(dead_code)]
    ChildSignalWake,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ControlSource {
    SharedWorkerWake,
    // TODO(dead_code): install dedicated source registrations for these reserved controls.
    #[allow(dead_code)]
    Shutdown,
    #[allow(dead_code)]
    ScriptKill,
    #[allow(dead_code)]
    FunctionKill,
    #[allow(dead_code)]
    ChildSignalWake,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RunnableKey {
    ClientRead(Token),
    ClientWrite(Token),
    SlotExportWrite(Token),
    DeferredReadiness(Token),
    ClusterBeforeSleep,
    ReplicationAction(u64),
    #[cfg(test)]
    Test(usize),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RunnableEvent {
    pub(crate) key: RunnableKey,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TimerEvent {
    ServerCron,
    ModuleTimer,
    EvictionContinuation,
    // Reserved for the timer migrations that follow the continuation cutover.
    // TODO(dead_code): remove these local allows when their owners use TimeEventRegistry.
    #[allow(dead_code)]
    BlockedTimeout,
    #[allow(dead_code)]
    ReplicationReconnect,
    #[cfg(test)]
    Test(usize),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum EventOwner {
    Transport(Token),
    Timer(TimeEventId),
    Runnable(RunnableKey),
    Control(ControlSource),
    Child(ChildHandle),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DispatchOutcome {
    Handled,
    Deferred,
    Requeued,
    Stale,
    OwnerClosed,
    ShutdownRequested,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DispatchContext {
    Normal,
    CommandBlocked {
        protected_client: Token,
    },
    HostedBlocked {
        protected_resources: ProtectedResources,
    },
}

/// Read resources that a nested hosted operation owns until it returns to the
/// outer event loop. A vector keeps nested hosted scopes exact instead of
/// collapsing different replication channels into one synthetic token.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProtectedResources {
    protected_reads: Vec<Token>,
}

impl ProtectedResources {
    #[cfg(test)]
    pub(crate) fn for_hosted_operation(operation: BlockedOperation) -> Self {
        Self {
            protected_reads: vec![operation.protected_read_token()],
        }
    }

    pub(crate) fn for_hosted_operations(operations: &[BlockedOperation]) -> Self {
        let mut protected_reads = Vec::with_capacity(operations.len());
        for operation in operations {
            let token = operation.protected_read_token();
            if !protected_reads.contains(&token) {
                protected_reads.push(token);
            }
        }
        Self { protected_reads }
    }

    fn protects_read(&self, token: Token) -> bool {
        self.protected_reads.contains(&token)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EventResource {
    None,
    ClientRead(Token),
    ClientWrite(Token),
    SafeProducer,
    #[cfg(test)]
    ShutdownControl,
}

/// Dependencies are assembled by source routing or by a nontransport event
/// producer before the dispatcher makes its context-only decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EventDependencies {
    resource: EventResource,
}

impl EventDependencies {
    pub(crate) fn none() -> Self {
        Self {
            resource: EventResource::None,
        }
    }

    pub(crate) fn client_read(token: Token) -> Self {
        Self {
            resource: EventResource::ClientRead(token),
        }
    }

    pub(crate) fn client_write(token: Token) -> Self {
        Self {
            resource: EventResource::ClientWrite(token),
        }
    }

    pub(crate) fn safe_producer() -> Self {
        Self {
            resource: EventResource::SafeProducer,
        }
    }

    #[cfg(test)]
    pub(crate) fn shutdown_control() -> Self {
        Self {
            resource: EventResource::ShutdownControl,
        }
    }

    fn protected_read(self) -> Option<Token> {
        match self.resource {
            EventResource::ClientRead(token) => Some(token),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EventPayloadKind {
    Readiness,
    Timer,
    Runnable,
    WorkerCompletion,
    Control,
    ChildCompletion,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DispatchDecision {
    Dispatch,
    Defer,
    Suppress,
}

pub(crate) fn decide_dispatch(
    context: &DispatchContext,
    dependencies: &EventDependencies,
    payload_kind: EventPayloadKind,
) -> DispatchDecision {
    if !matches!(context, DispatchContext::Normal) && payload_kind == EventPayloadKind::Timer {
        return DispatchDecision::Suppress;
    }
    let protected = match context {
        DispatchContext::Normal => return DispatchDecision::Dispatch,
        DispatchContext::CommandBlocked { protected_client } => {
            dependencies.protected_read() == Some(*protected_client)
        }
        DispatchContext::HostedBlocked {
            protected_resources,
        } => dependencies
            .protected_read()
            .is_some_and(|token| protected_resources.protects_read(token)),
    };
    if protected {
        DispatchDecision::Defer
    } else {
        DispatchDecision::Dispatch
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TurnTracePoint {
    BeforePoll,
    Poll,
    AfterPoll,
    ReadyDispatch,
    TimeDispatch,
    AofFlush,
    OrdinaryWrite,
}

pub(crate) struct EventLimits {
    pub(crate) max_events_per_phase_per_turn: usize,
    pub(crate) max_same_owner_events_per_turn: usize,
}

impl Default for EventLimits {
    fn default() -> Self {
        Self {
            max_events_per_phase_per_turn: 4096,
            max_same_owner_events_per_turn: 64,
        }
    }
}

impl EventLoopEvent {
    pub(crate) fn runnable(key: RunnableKey) -> Self {
        Self {
            sequence: 0,
            phase: EventPhase::BeforePoll,
            lane: EventLane::Normal,
            owner: EventOwner::Runnable(key),
            payload: EventPayload::Runnable(RunnableEvent { key }),
        }
    }

    pub(crate) fn readiness(phase: EventPhase, lane: EventLane, readiness: ReadinessEvent) -> Self {
        Self {
            sequence: 0,
            phase,
            lane,
            owner: EventOwner::Transport(readiness.token),
            payload: EventPayload::Readiness(readiness),
        }
    }

    pub(crate) fn worker_completion(completion: WorkerCompletionEvent) -> Self {
        Self {
            sequence: 0,
            phase: EventPhase::Ready,
            lane: EventLane::Control,
            owner: EventOwner::Control(ControlSource::SharedWorkerWake),
            payload: EventPayload::WorkerCompletion(completion),
        }
    }

    pub(crate) fn control(control: ControlEvent) -> Self {
        Self {
            sequence: 0,
            phase: EventPhase::Ready,
            lane: EventLane::Control,
            owner: EventOwner::Control(ControlSource::SharedWorkerWake),
            payload: EventPayload::Control(control),
        }
    }

    pub(crate) fn child_completion(completion: ChildCompletionEvent) -> Self {
        Self {
            sequence: 0,
            phase: EventPhase::Ready,
            lane: EventLane::Control,
            owner: EventOwner::Child(completion.child),
            payload: EventPayload::ChildCompletion(completion),
        }
    }

    #[cfg(test)]
    pub(crate) fn runnable_for_test(key: usize) -> Self {
        Self::runnable(RunnableKey::Test(key))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TransportEventDispatchOutcome {
    pub(super) outcome: DispatchOutcome,
    may_request_shutdown: bool,
}

/// Readiness for one transport token, kept together so dispatch code can reason
/// about semantic states instead of a loose collection of boolean arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReadyEvent {
    pub(crate) token: Token,
    pub(crate) readable: bool,
    pub(crate) writable: bool,
    pub(crate) error: bool,
    pub(crate) read_hup: bool,
}

impl ReadyEvent {
    pub(crate) fn is_failure(self) -> bool {
        self.error || (self.read_hup && !self.readable)
    }

    pub(crate) fn has_readable_hup(self) -> bool {
        self.readable && self.read_hup
    }

    pub(crate) fn is_readable(self) -> bool {
        self.readable
    }

    pub(crate) fn is_writable(self) -> bool {
        self.writable
    }
}

impl From<&RuntimeEvent> for ReadyEvent {
    fn from(event: &RuntimeEvent) -> Self {
        Self {
            token: event.token,
            readable: event.readable,
            writable: event.writable,
            error: event.error,
            read_hup: event.read_hup,
        }
    }
}

impl TransportEventDispatchOutcome {
    fn handled() -> Self {
        Self {
            outcome: DispatchOutcome::Handled,
            may_request_shutdown: false,
        }
    }

    fn deferred() -> Self {
        Self {
            outcome: DispatchOutcome::Deferred,
            may_request_shutdown: false,
        }
    }

    fn stale() -> Self {
        Self {
            outcome: DispatchOutcome::Stale,
            may_request_shutdown: false,
        }
    }
}

impl super::ServerWorkerState {
    pub(crate) fn exit_if_shutdown_requested(&mut self) -> bool {
        self.check_pending_signal();
        if self.ctx.pause_cron() != 0 {
            // A paused cron must not advance a signal-triggered shutdown:
            // SHUTDOWN ABORT relies on being able to cancel the staged signal
            // before cron resumes. Explicit shutdown requests are still
            // honored below.
            if self.ctx.is_shutdown_requested() {
                self.perform_shutdown_handoff_if_needed();
                crate::server_log!(notice, "Shutdown requested, exiting event loop");
                self.cancel_disk_sync_batch();
                self.abort_direct_dual_rdb_child_for_shutdown();
                self.close_listeners_for_shutdown();
                return true;
            }
            return false;
        }
        self.drive_pending_shutdown();
        if !self.ctx.is_shutdown_requested() {
            return false;
        }
        self.perform_shutdown_handoff_if_needed();
        crate::server_log!(notice, "Shutdown requested, exiting event loop");
        self.cancel_disk_sync_batch();
        self.abort_direct_dual_rdb_child_for_shutdown();
        self.close_listeners_for_shutdown();
        true
    }

    #[cfg(test)]
    pub(crate) fn run_one_turn_for_test(&mut self) -> std::io::Result<Vec<TurnTracePoint>> {
        self.turn_trace.clear();
        self.writes.begin_turn();
        if self.run_before_poll_phase()? == LoopControl::Stop {
            return Ok(self.turn_trace.clone());
        }
        let poll_result = self.poll_ready()?;
        self.collect_after_poll_events(poll_result);
        if self.dispatch_ready_phase(DispatchContext::Normal)? == LoopControl::Stop {
            self.finish_turn_pending_outputs();
            return Ok(self.turn_trace.clone());
        }
        self.collect_due_time_events();
        let _ = self.dispatch_time_phase()?;
        self.finish_turn_pending_outputs();
        self.event_batch.begin_next_turn();
        self.writes.begin_turn();
        Ok(self.turn_trace.clone())
    }

    #[cfg(test)]
    pub(crate) fn trace_turn_for_test(&mut self, point: TurnTracePoint) {
        self.turn_trace.push(point);
    }

    #[cfg(test)]
    pub(crate) fn install_test_child(&mut self, owner: ChildOwner, pid: i32) -> ChildHandle {
        self.child_registry.register_test_child(owner, pid)
    }
}

fn server_cron_interval(hz: i32) -> Duration {
    Duration::from_millis(1_000u64.saturating_div(hz.clamp(1, 500) as u64))
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

#[cfg(test)]
mod ready_tests {
    use super::ReadyEvent;
    use crate::framework::capabilities::io::Token;
    use crate::runtime::server::worker::reactor::transport::RuntimeEvent;

    fn event(readable: bool, writable: bool, error: bool, read_hup: bool) -> RuntimeEvent {
        RuntimeEvent {
            token: Token(7),
            readable,
            writable,
            error,
            read_hup,
        }
    }

    #[test]
    fn classifies_failure_and_readable_hup() {
        let hup = ReadyEvent::from(&event(true, false, false, true));
        assert!(hup.has_readable_hup());
        assert!(!hup.is_failure());

        let failure = ReadyEvent::from(&event(false, false, true, false));
        assert!(failure.is_failure());
    }

    #[test]
    fn exposes_read_and_write_interest() {
        let ready = ReadyEvent::from(&event(true, true, false, false));
        assert!(ready.is_readable());
        assert!(ready.is_writable());
    }
}
