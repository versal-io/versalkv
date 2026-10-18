use super::ProtectedResources;
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::ConnectionId;
use std::collections::VecDeque;

mod command;

#[derive(Default)]
pub(crate) struct RdbLoadProgress {
    payload_bytes: u64,
    replica_session_id: Option<u64>,
}

impl RdbLoadProgress {
    pub(crate) fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    pub(crate) fn begin_session(&mut self, replica_session_id: u64) {
        if self.replica_session_id != Some(replica_session_id) {
            self.payload_bytes = 0;
            self.replica_session_id = Some(replica_session_id);
        }
    }

    pub(crate) fn read_budget(&self, maximum: usize, interval: u64) -> usize {
        if maximum == 0 || interval == 0 {
            return maximum;
        }
        let remainder = self.payload_bytes % interval;
        let until = if remainder == 0 {
            interval
        } else {
            interval - remainder
        };
        maximum
            .min(usize::try_from(until).unwrap_or(usize::MAX))
            .max(1)
    }

    pub(crate) fn record_payload(&mut self, bytes: usize, interval: u64) -> bool {
        if interval == 0 {
            return false;
        }
        let before = self.payload_bytes;
        self.payload_bytes = self.payload_bytes.saturating_add(bytes as u64);
        before / interval != self.payload_bytes / interval
    }

    pub(crate) fn reset(&mut self) {
        self.payload_bytes = 0;
        self.replica_session_id = None;
    }
}

pub(crate) struct ReplayProgress {
    processed_bytes: u64,
    last_service_ms: i64,
}

impl ReplayProgress {
    pub(crate) fn new(now_ms: i64) -> Self {
        Self {
            processed_bytes: 0,
            last_service_ms: now_ms,
        }
    }

    pub(crate) fn record(&mut self, bytes: usize, interval: u64, now_ms: i64) -> bool {
        if interval == 0 {
            return false;
        }
        let before = self.processed_bytes;
        self.processed_bytes = self.processed_bytes.saturating_add(bytes as u64);
        let crossed = before / interval != self.processed_bytes / interval;
        if crossed && now_ms.saturating_sub(self.last_service_ms) > 100 {
            self.last_service_ms = now_ms;
            return true;
        }
        false
    }

    pub(crate) fn processed_bytes(&self) -> u64 {
        self.processed_bytes
    }
}

pub(crate) struct PendingReplayContinuation {
    pub(crate) main: ConnectionId,
    pub(crate) session_id: u64,
    pub(crate) selected_db_applied: bool,
    pub(crate) buffered_getack: bool,
    pub(crate) progress: ReplayProgress,
}

impl PendingReplayContinuation {
    pub(crate) fn new(main: ConnectionId, session_id: u64, now_ms: i64) -> Self {
        Self {
            main,
            session_id,
            selected_db_applied: false,
            buffered_getack: false,
            progress: ReplayProgress::new(now_ms),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(main: ConnectionId, session_id: u64, now_ms: i64) -> Self {
        Self::new(main, session_id, now_ms)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BlockedOperation {
    ClassicRdb {
        main: ConnectionId,
    },
    DualRdb {
        main: ConnectionId,
        rdb: ConnectionId,
    },
    PendingReplay {
        main: ConnectionId,
    },
}

pub(crate) enum ResumeDecision<T> {
    Resume(T),
    Aborted,
}

impl BlockedOperation {
    pub(crate) fn protected_read_token(self) -> Token {
        match self {
            BlockedOperation::ClassicRdb { main } => Token(main.0),
            BlockedOperation::DualRdb { rdb, .. } => Token(rdb.0),
            BlockedOperation::PendingReplay { main } => Token(main.0),
        }
    }

    fn protects_read(self, token: Token) -> bool {
        self.protected_read_token() == token
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DeferredReadyEvent {
    pub(crate) token: Token,
    pub(crate) readable: bool,
    pub(crate) writable: bool,
    pub(crate) read_hup: bool,
    pub(crate) error: bool,
}

impl DeferredReadyEvent {
    pub(crate) fn new(
        token: Token,
        readable: bool,
        writable: bool,
        read_hup: bool,
        error: bool,
    ) -> Self {
        Self {
            token,
            readable,
            writable,
            read_hup,
            error,
        }
    }
}

#[derive(Default)]
pub(crate) struct BlockedEventState {
    scopes: Vec<BlockedOperation>,
    deferred: VecDeque<DeferredReadyEvent>,
}

impl BlockedEventState {
    pub(crate) fn push_scope(&mut self, operation: BlockedOperation) {
        self.scopes.push(operation);
    }

    pub(crate) fn pop_scope(&mut self) -> Option<BlockedOperation> {
        self.scopes.pop()
    }

    #[cfg(test)]
    pub(crate) fn depth(&self) -> usize {
        self.scopes.len()
    }

    pub(crate) fn current(&self) -> Option<BlockedOperation> {
        self.scopes.last().copied()
    }

    pub(crate) fn is_read_protected(&self, token: Token) -> bool {
        self.scopes.iter().any(|scope| scope.protects_read(token))
    }

    pub(crate) fn protected_resources(&self) -> ProtectedResources {
        ProtectedResources::for_hosted_operations(&self.scopes)
    }

    pub(crate) fn defer(&mut self, event: DeferredReadyEvent) {
        if let Some(existing) = self
            .deferred
            .iter_mut()
            .find(|deferred| deferred.token == event.token)
        {
            existing.readable |= event.readable;
            existing.writable |= event.writable;
            existing.read_hup |= event.read_hup;
            existing.error |= event.error;
            return;
        }
        self.deferred.push_back(event);
    }

    pub(crate) fn has_deferred_read(&self, token: Token) -> bool {
        self.deferred
            .iter()
            .any(|event| event.token == token && event.readable)
    }

    #[cfg(test)]
    pub(crate) fn pop_front_deferred(&mut self) -> Option<DeferredReadyEvent> {
        self.deferred.pop_front()
    }

    pub(crate) fn take_deferred(&mut self, token: Token) -> Option<DeferredReadyEvent> {
        let position = self
            .deferred
            .iter()
            .position(|event| event.token == token)?;
        self.deferred.remove(position)
    }

    pub(crate) fn remove_deferred(&mut self, token: Token) -> bool {
        let before = self.deferred.len();
        self.deferred.retain(|event| event.token != token);
        self.deferred.len() != before
    }
}

impl crate::runtime::server::worker::ServerWorkerState {
    pub(crate) fn set_pending_replication_replay(
        &mut self,
        continuation: PendingReplayContinuation,
    ) {
        let session_id = continuation.session_id;
        self.pending_replication_replay = Some(continuation);
        self.schedule_runtime_runnable_next_turn(super::RunnableKey::ReplicationAction(session_id));
    }

    pub(crate) fn clear_pending_replication_replay(&mut self) {
        let session_id = self
            .pending_replication_replay
            .as_ref()
            .map(|continuation| continuation.session_id);
        self.pending_replication_replay = None;
        if let Some(session_id) = session_id {
            self.cancel_runtime_runnable(super::RunnableKey::ReplicationAction(session_id));
        }
    }

    pub(crate) fn drive_pending_replication_action(&mut self, session_id: u64) {
        if self
            .pending_replication_replay
            .as_ref()
            .is_some_and(|continuation| continuation.session_id == session_id)
        {
            self.drive_one_pending_replication_replay();
        }
    }
}

#[derive(Default)]
pub(crate) struct BlockedServiceResult {
    pub(crate) poll_rounds: usize,
    pub(crate) dispatched_events: usize,
    pub(crate) deferred_events: usize,
    pub(crate) outcomes: BlockedDispatchOutcomes,
}

#[derive(Default)]
pub(crate) struct BlockedDispatchOutcomes {
    pub(crate) handled: usize,
    pub(crate) deferred: usize,
    pub(crate) requeued: usize,
    pub(crate) stale: usize,
    pub(crate) owner_closed: usize,
    pub(crate) shutdown_requested: usize,
}

impl BlockedServiceResult {
    pub(crate) fn record_outcome(&mut self, outcome: super::DispatchOutcome) {
        match outcome {
            super::DispatchOutcome::Handled => {
                self.dispatched_events += 1;
                self.outcomes.handled += 1;
            }
            super::DispatchOutcome::Deferred => {
                self.deferred_events += 1;
                self.outcomes.deferred += 1;
            }
            super::DispatchOutcome::Requeued => self.outcomes.requeued += 1,
            super::DispatchOutcome::Stale => self.outcomes.stale += 1,
            super::DispatchOutcome::OwnerClosed => {
                self.dispatched_events += 1;
                self.outcomes.owner_closed += 1;
            }
            super::DispatchOutcome::ShutdownRequested => {
                self.dispatched_events += 1;
                self.outcomes.shutdown_requested += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockedEventState, BlockedOperation, RdbLoadProgress, ReplayProgress};
    use crate::framework::capabilities::io::Token;
    use crate::framework::distributed::replication::ConnectionId;

    #[test]
    fn nested_scopes_protect_the_union_of_classic_dual_and_replay_reads() {
        let mut state = BlockedEventState::default();
        let classic = BlockedOperation::ClassicRdb {
            main: ConnectionId(41),
        };
        let dual = BlockedOperation::DualRdb {
            main: ConnectionId(42),
            rdb: ConnectionId(43),
        };
        let replay = BlockedOperation::PendingReplay {
            main: ConnectionId(44),
        };
        for scope in [classic, dual, replay] {
            state.push_scope(scope);
        }

        assert!(state.is_read_protected(Token(41)));
        assert!(!state.is_read_protected(Token(42)));
        assert!(state.is_read_protected(Token(43)));
        assert!(state.is_read_protected(Token(44)));
        assert_eq!(state.pop_scope(), Some(replay));
        assert_eq!(state.pop_scope(), Some(dual));
        assert_eq!(state.pop_scope(), Some(classic));
        assert_eq!(state.depth(), 0);
    }

    #[test]
    fn rdb_progress_caps_reads_at_the_next_estimated_boundary() {
        let mut progress = RdbLoadProgress::default();
        assert_eq!(progress.read_budget(128 * 1024, 1024), 1024);
        assert!(!progress.record_payload(1000, 1024));
        assert_eq!(progress.read_budget(128 * 1024, 1024), 24);
        assert!(progress.record_payload(24, 1024));
        assert_eq!(progress.read_budget(128 * 1024, 1024), 1024);
    }

    #[test]
    fn rdb_progress_does_not_exceed_zero_read_capacity() {
        let progress = RdbLoadProgress::default();
        assert_eq!(progress.read_budget(0, 1024), 0);
    }

    #[test]
    fn zero_interval_disables_rdb_progress_capping() {
        let mut progress = RdbLoadProgress::default();
        assert_eq!(progress.read_budget(128 * 1024, 0), 128 * 1024);
        assert!(!progress.record_payload(4 * 1024 * 1024, 0));
    }

    #[test]
    fn resetting_rdb_progress_clears_payload_bytes() {
        let mut progress = RdbLoadProgress::default();
        assert!(!progress.record_payload(1000, 1024));
        assert_eq!(progress.read_budget(128 * 1024, 1024), 24);

        progress.reset();

        assert_eq!(progress.payload_bytes, 0);
        assert_eq!(progress.read_budget(128 * 1024, 1024), 1024);
    }

    #[test]
    fn rdb_progress_resets_only_when_replica_session_changes() {
        let mut progress = RdbLoadProgress::default();
        progress.begin_session(41);
        assert!(!progress.record_payload(1000, 1024));

        progress.begin_session(41);
        assert_eq!(progress.read_budget(128 * 1024, 1024), 24);

        progress.begin_session(42);
        assert_eq!(progress.read_budget(128 * 1024, 1024), 1024);
    }

    #[test]
    fn replay_progress_uses_byte_and_time_gates() {
        let mut progress = ReplayProgress::new(1_000);
        assert!(!progress.record(1024, 1024, 1_050));
        assert!(progress.record(1024, 1024, 1_101));
        assert!(!progress.record(1024, 1024, 1_150));
    }

    #[test]
    fn replay_progress_requires_more_than_one_hundred_milliseconds() {
        let mut progress = ReplayProgress::new(1_000);
        assert!(!progress.record(1024, 1024, 1_100));
        assert_eq!(progress.last_service_ms, 1_000);
        assert!(progress.record(1024, 1024, 1_101));
        assert_eq!(progress.last_service_ms, 1_101);
    }

    #[test]
    fn zero_interval_disables_replay_progress() {
        let mut progress = ReplayProgress::new(1_000);
        assert!(!progress.record(1024, 0, 1_101));
        assert_eq!(progress.processed_bytes, 0);
        assert_eq!(progress.last_service_ms, 1_000);
    }
}
