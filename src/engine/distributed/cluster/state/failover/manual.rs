use crate::engine::distributed::cluster::action::{ClusterAction, Promotion};
use crate::engine::distributed::cluster::NodeId;
use crate::engine::distributed::replication::ReplicationOffset;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManualFailoverMode {
    Coordinated,
    Force,
    Takeover,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ManualFailoverPhase {
    #[default]
    Idle,
    WaitingPrimaryPause,
    WaitingReplicaCatchup,
    ReadyForElection,
    Promoting,
    PausingPrimary,
    TimedOut,
}

#[derive(Clone, Debug, Default)]
pub struct ManualFailover {
    phase: ManualFailoverPhase,
    peer: Option<NodeId>,
    deadline_ms: i64,
    primary_offset: Option<ReplicationOffset>,
    primary_pause_committed: bool,
}

impl ManualFailover {
    #[cfg(test)]
    pub fn phase(&self) -> ManualFailoverPhase {
        self.phase
    }

    pub fn is_active(&self) -> bool {
        !matches!(
            self.phase,
            ManualFailoverPhase::Idle | ManualFailoverPhase::TimedOut
        )
    }

    pub fn is_ready_for_election(&self) -> bool {
        self.phase == ManualFailoverPhase::ReadyForElection
    }

    pub fn is_takeover(&self) -> bool {
        self.phase == ManualFailoverPhase::Promoting
    }

    pub fn is_pausing_primary(&self) -> bool {
        self.phase == ManualFailoverPhase::PausingPrimary && self.primary_pause_committed
    }

    pub(crate) fn pausing_replica(&self) -> Option<&NodeId> {
        (self.phase == ManualFailoverPhase::PausingPrimary)
            .then_some(self.peer.as_ref())
            .flatten()
    }

    pub fn on_primary_pause_committed(&mut self) {
        if self.phase == ManualFailoverPhase::PausingPrimary {
            self.primary_pause_committed = true;
        }
    }

    pub fn start_replica(
        &mut self,
        mode: ManualFailoverMode,
        primary: NodeId,
        promotion: Promotion,
        now_ms: i64,
        timeout_ms: i64,
    ) -> Vec<ClusterAction> {
        self.reset();
        self.peer = Some(primary.clone());
        self.deadline_ms = now_ms.saturating_add(timeout_ms.max(1));
        match mode {
            ManualFailoverMode::Coordinated => {
                self.phase = ManualFailoverPhase::WaitingPrimaryPause;
                vec![ClusterAction::SendManualFailoverStart { primary }]
            }
            ManualFailoverMode::Force => {
                self.phase = ManualFailoverPhase::ReadyForElection;
                vec![ClusterAction::BeginManualElection]
            }
            ManualFailoverMode::Takeover => {
                self.phase = ManualFailoverPhase::Promoting;
                vec![ClusterAction::PromoteSelf(promotion)]
            }
        }
    }

    pub fn accept_on_primary(
        &mut self,
        requester: NodeId,
        now_ms: i64,
        timeout_ms: i64,
    ) -> Vec<ClusterAction> {
        self.reset();
        self.phase = ManualFailoverPhase::PausingPrimary;
        self.peer = Some(requester.clone());
        self.deadline_ms = now_ms.saturating_add(timeout_ms.max(1));
        vec![ClusterAction::PauseWrites {
            requester,
            until_ms: now_ms.saturating_add(timeout_ms.max(1).saturating_mul(2)),
        }]
    }

    pub fn on_primary_paused(
        &mut self,
        primary: &NodeId,
        offset: ReplicationOffset,
    ) -> Vec<ClusterAction> {
        if self.phase != ManualFailoverPhase::WaitingPrimaryPause
            || self.peer.as_ref() != Some(primary)
        {
            return Vec::new();
        }
        self.primary_offset = Some(offset);
        self.phase = ManualFailoverPhase::WaitingReplicaCatchup;
        Vec::new()
    }

    pub fn on_replica_caught_up(
        &mut self,
        applied_offset: ReplicationOffset,
    ) -> Vec<ClusterAction> {
        if self.phase != ManualFailoverPhase::WaitingReplicaCatchup
            || self
                .primary_offset
                .is_none_or(|primary_offset| applied_offset < primary_offset)
        {
            return Vec::new();
        }
        self.phase = ManualFailoverPhase::ReadyForElection;
        vec![ClusterAction::BeginManualElection]
    }

    pub fn on_timeout(&mut self, now_ms: i64) -> Vec<ClusterAction> {
        if !self.is_active() || now_ms <= self.deadline_ms {
            return Vec::new();
        }
        self.phase = ManualFailoverPhase::TimedOut;
        self.peer = None;
        self.primary_offset = None;
        self.primary_pause_committed = false;
        vec![ClusterAction::ResumeWrites { timed_out: true }]
    }

    pub fn on_peer_disconnect(&mut self, peer: &NodeId) -> Vec<ClusterAction> {
        if !matches!(
            self.phase,
            ManualFailoverPhase::WaitingPrimaryPause
                | ManualFailoverPhase::WaitingReplicaCatchup
                | ManualFailoverPhase::PausingPrimary
        ) || self.peer.as_ref() != Some(peer)
        {
            return Vec::new();
        }
        self.reset();
        vec![ClusterAction::ResumeWrites { timed_out: false }]
    }

    pub fn on_role_change_committed(&mut self) -> Vec<ClusterAction> {
        if !self.is_active() {
            return Vec::new();
        }
        self.reset();
        vec![ClusterAction::ResumeWrites { timed_out: false }]
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::distributed::cluster::action::{ClusterAction, Promotion};
    use crate::engine::distributed::cluster::NodeId;
    use crate::engine::distributed::replication::ReplicationOffset;

    use super::{ManualFailover, ManualFailoverMode, ManualFailoverPhase};

    fn node(byte: u8) -> NodeId {
        NodeId::parse(&[byte; 40]).unwrap()
    }

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("valid test offset")
    }

    fn promotion() -> Promotion {
        let mut slots = vec![false; 16_384];
        slots[7] = true;
        Promotion {
            old_primary: node(b'p'),
            shard_id: node(b's'),
            slots,
            config_epoch: 10,
        }
    }

    #[test]
    fn coordinated_failover_waits_until_replica_covers_paused_primary_offset() {
        let mut failover = ManualFailover::default();
        assert_eq!(
            failover.start_replica(
                ManualFailoverMode::Coordinated,
                node(b'p'),
                promotion(),
                1_000,
                5_000,
            ),
            vec![ClusterAction::SendManualFailoverStart {
                primary: node(b'p')
            }]
        );
        assert_eq!(failover.phase(), ManualFailoverPhase::WaitingPrimaryPause);
        assert!(failover
            .on_primary_paused(&node(b'p'), offset(42))
            .is_empty());
        assert_eq!(failover.phase(), ManualFailoverPhase::WaitingReplicaCatchup);
        assert!(failover.on_replica_caught_up(offset(41)).is_empty());
        assert_eq!(
            failover.on_replica_caught_up(offset(43)),
            vec![ClusterAction::BeginManualElection]
        );
        assert!(failover.is_ready_for_election());
    }

    #[test]
    fn force_starts_election_and_takeover_promotes_without_quorum() {
        let mut failover = ManualFailover::default();
        assert_eq!(
            failover.start_replica(
                ManualFailoverMode::Force,
                node(b'p'),
                promotion(),
                1_000,
                5_000,
            ),
            vec![ClusterAction::BeginManualElection]
        );
        assert!(failover.is_ready_for_election());

        assert_eq!(
            failover.start_replica(
                ManualFailoverMode::Takeover,
                node(b'p'),
                promotion(),
                2_000,
                5_000,
            ),
            vec![ClusterAction::PromoteSelf(promotion())]
        );
        assert_eq!(failover.phase(), ManualFailoverPhase::Promoting);
    }

    #[test]
    fn primary_pause_and_every_terminal_path_resume_writes() {
        let mut failover = ManualFailover::default();
        assert_eq!(
            failover.accept_on_primary(node(b'r'), 1_000, 5_000),
            vec![ClusterAction::PauseWrites {
                requester: node(b'r'),
                until_ms: 11_000,
            }]
        );
        assert_eq!(failover.phase(), ManualFailoverPhase::PausingPrimary);
        assert!(!failover.is_pausing_primary());
        failover.on_primary_pause_committed();
        assert!(failover.is_pausing_primary());
        assert_eq!(
            failover.on_timeout(6_001),
            vec![ClusterAction::ResumeWrites { timed_out: true }]
        );
        assert_eq!(failover.phase(), ManualFailoverPhase::TimedOut);

        failover.accept_on_primary(node(b'r'), 10_000, 5_000);
        assert_eq!(
            failover.on_peer_disconnect(&node(b'r')),
            vec![ClusterAction::ResumeWrites { timed_out: false }]
        );
        assert_eq!(failover.phase(), ManualFailoverPhase::Idle);
    }
}
