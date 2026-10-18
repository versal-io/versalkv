use std::collections::{HashMap, HashSet};
use std::fmt;

use super::{ClusterLinkDirection, ClusterState, CLUSTER_SLOTS};
use crate::engine::distributed::cluster::migration::{
    MigrationOperation, MigrationPhase, SlotRanges,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterInvariantError {
    SlotOwnership(SlotOwnershipInvariantViolation),
    NodeIdentity(NodeIdentityInvariantViolation),
    EpochMonotonicity(EpochInvariantViolation),
    MigrationPhase(MigrationInvariantViolation),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SlotOwnershipInvariantViolation {
    VectorLength {
        field: &'static str,
        actual: usize,
    },
    SlotCount {
        node_id: String,
        bitmap: usize,
        declared: usize,
    },
    OpenSlotOutOfBounds {
        kind: &'static str,
        slot: usize,
    },
    OpenSlotUnknownNode {
        kind: &'static str,
        slot: usize,
        node_id: String,
    },
    CanonicalBitmapMismatch {
        slot: usize,
    },
    CanonicalOwnerMismatch {
        slot: usize,
        owner: Option<String>,
        claimant: Option<String>,
    },
    DuplicateSlotClaim {
        slot: usize,
        first: String,
        second: String,
    },
    WithdrawnNodeDoesNotClaim {
        slot: usize,
        node_id: String,
    },
    InvalidOpenSlotOwnership {
        kind: &'static str,
        slot: usize,
        node_id: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeIdentityInvariantViolation {
    LocalNodeInMap,
    MapKeyMismatch {
        key: String,
        node_id: String,
    },
    InvalidNodeId {
        context: &'static str,
        node_id: String,
    },
    LinkKeyMismatch {
        key_node_id: String,
        key_direction: ClusterLinkDirection,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EpochInvariantViolation {
    CurrentBeforeNode {
        node_id: String,
        current: u64,
        node: u64,
    },
    VoteAfterCurrent {
        vote: u64,
        current: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrationInvariantViolation {
    OperationPhaseMismatch {
        job_id: String,
        operation: MigrationOperation,
        phase: MigrationPhase,
    },
    DuplicateActiveId {
        job_id: String,
    },
    ActiveRangeOverlap {
        operation: MigrationOperation,
        slot: u16,
    },
}

impl fmt::Display for ClusterInvariantError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "cluster invariant violation: {self:?}")
    }
}

impl std::error::Error for ClusterInvariantError {}

impl ClusterState {
    /// Move the observed cluster epoch forward. Returns whether it changed.
    pub(crate) fn advance_current_epoch(&mut self, observed: u64) -> bool {
        if observed <= self.current_epoch {
            return false;
        }
        self.current_epoch = observed;
        true
    }

    /// Persist a vote only when its epoch is representable by current state.
    pub(crate) fn record_vote_epoch(&mut self, epoch: u64) -> bool {
        if epoch > self.current_epoch || epoch <= self.last_vote_epoch {
            return false;
        }
        self.last_vote_epoch = epoch;
        true
    }

    /// Validate cross-field state assumptions at explicit transition boundaries.
    /// Runtime identities use strict `NodeId` validation. Test builds also
    /// accept lowercase-alphanumeric fixture IDs so legacy focused tests can
    /// validate structure without rewriting unrelated fixtures.
    pub fn validate_invariants(&self) -> Result<(), ClusterInvariantError> {
        self.validate_slot_ownership()?;
        self.validate_node_identity()?;
        self.validate_epochs()?;
        self.validate_migrations()
    }

    /// Allocation-free checks suitable for production transition boundaries.
    pub fn validate_shape_fast(&self) -> Result<(), ClusterInvariantError> {
        for (field, actual) in [
            ("slots", self.slots.len()),
            ("slot_owners", self.slot_ownership.owners.len()),
            (
                "slot_delete_epochs",
                self.slot_ownership.delete_epochs.len(),
            ),
            (
                "slot_not_claimed_by",
                self.slot_ownership.withdrawn_by.len(),
            ),
            ("myself.slots", self.myself.slots.len()),
        ] {
            if actual != CLUSTER_SLOTS {
                return Err(ClusterInvariantError::SlotOwnership(
                    SlotOwnershipInvariantViolation::VectorLength { field, actual },
                ));
            }
        }
        if self.myself.config_epoch > self.current_epoch {
            return Err(ClusterInvariantError::EpochMonotonicity(
                EpochInvariantViolation::CurrentBeforeNode {
                    node_id: self.myself.name.clone(),
                    current: self.current_epoch,
                    node: self.myself.config_epoch,
                },
            ));
        }
        if self.last_vote_epoch > self.current_epoch {
            return Err(ClusterInvariantError::EpochMonotonicity(
                EpochInvariantViolation::VoteAfterCurrent {
                    vote: self.last_vote_epoch,
                    current: self.current_epoch,
                },
            ));
        }
        Ok(())
    }

    /// Check constant-time structural invariants at selected transitions.
    /// These checks remain active in release builds. Debug builds additionally
    /// validate the complete topology and migration state.
    pub(crate) fn debug_assert_invariants(&self) {
        let result = self.validate_shape_fast();
        assert!(
            result.is_ok(),
            "cluster state transition violated an invariant: {result:?}"
        );
        #[cfg(debug_assertions)]
        {
            let full = self.validate_invariants();
            debug_assert!(
                full.is_ok(),
                "cluster state transition violated a full invariant: {full:?}"
            );
        }
    }

    pub(crate) fn assert_fast_invariants(&self) -> Result<(), ClusterInvariantError> {
        self.validate_shape_fast()
    }

    fn validate_slot_ownership(&self) -> Result<(), ClusterInvariantError> {
        for (field, actual) in [
            ("slots", self.slots.len()),
            ("slot_owners", self.slot_ownership.owners.len()),
            (
                "slot_delete_epochs",
                self.slot_ownership.delete_epochs.len(),
            ),
            (
                "slot_not_claimed_by",
                self.slot_ownership.withdrawn_by.len(),
            ),
            ("myself.slots", self.myself.slots.len()),
        ] {
            if actual != CLUSTER_SLOTS {
                return Err(ClusterInvariantError::SlotOwnership(
                    SlotOwnershipInvariantViolation::VectorLength { field, actual },
                ));
            }
        }
        validate_node_slots(&self.myself)?;
        for node in self.nodes.values() {
            if node.slots.len() != CLUSTER_SLOTS {
                return Err(ClusterInvariantError::SlotOwnership(
                    SlotOwnershipInvariantViolation::VectorLength {
                        field: "node.slots",
                        actual: node.slots.len(),
                    },
                ));
            }
            validate_node_slots(node)?;
        }
        for slot in 0..CLUSTER_SLOTS {
            let mut claimant = self.myself.slots[slot].then(|| self.myself.name.clone());
            for node in self.nodes.values().filter(|node| node.slots[slot]) {
                if let Some(first) = claimant.as_ref() {
                    return Err(ClusterInvariantError::SlotOwnership(
                        SlotOwnershipInvariantViolation::DuplicateSlotClaim {
                            slot,
                            first: first.clone(),
                            second: node.name.clone(),
                        },
                    ));
                }
                claimant = Some(node.name.clone());
            }
            let owner = self.canonical_slot_owner_id(slot).map(str::to_string);
            if owner != claimant {
                return Err(ClusterInvariantError::SlotOwnership(
                    SlotOwnershipInvariantViolation::CanonicalOwnerMismatch {
                        slot,
                        owner,
                        claimant,
                    },
                ));
            }
            let covered = self.owner_contributes_coverage(&self.slot_ownership.owners[slot]);
            if self.slots[slot] != covered {
                return Err(ClusterInvariantError::SlotOwnership(
                    SlotOwnershipInvariantViolation::CanonicalBitmapMismatch { slot },
                ));
            }
            if let Some(node_id) = &self.slot_ownership.withdrawn_by[slot] {
                let claimant = if node_id == &self.myself.name {
                    Some(&self.myself)
                } else {
                    self.nodes.get(node_id)
                };
                if !claimant.is_some_and(|node| node.slots[slot]) {
                    return Err(ClusterInvariantError::SlotOwnership(
                        SlotOwnershipInvariantViolation::WithdrawnNodeDoesNotClaim {
                            slot,
                            node_id: node_id.clone(),
                        },
                    ));
                }
            }
        }
        for (kind, open_slots) in [
            ("migrating", &self.slot_ownership.migrating_to),
            ("importing", &self.slot_ownership.importing_from),
        ] {
            for (&slot, node_id) in open_slots {
                if slot >= CLUSTER_SLOTS {
                    return Err(ClusterInvariantError::SlotOwnership(
                        SlotOwnershipInvariantViolation::OpenSlotOutOfBounds { kind, slot },
                    ));
                }
                if node_id != &self.myself.name && !self.nodes.contains_key(node_id) {
                    return Err(ClusterInvariantError::SlotOwnership(
                        SlotOwnershipInvariantViolation::OpenSlotUnknownNode {
                            kind,
                            slot,
                            node_id: node_id.clone(),
                        },
                    ));
                }
                let peer = if node_id == &self.myself.name {
                    &self.myself
                } else {
                    &self.nodes[node_id]
                };
                let ownership_is_valid = peer.is_primary()
                    && match kind {
                        "migrating" => self.myself.slots[slot] && !peer.slots[slot],
                        "importing" => !self.myself.slots[slot],
                        _ => false,
                    };
                if !ownership_is_valid {
                    return Err(ClusterInvariantError::SlotOwnership(
                        SlotOwnershipInvariantViolation::InvalidOpenSlotOwnership {
                            kind,
                            slot,
                            node_id: node_id.clone(),
                        },
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_slot_transition(
        &self,
        slot: usize,
    ) -> Result<(), ClusterInvariantError> {
        if slot >= CLUSTER_SLOTS {
            return Err(ClusterInvariantError::SlotOwnership(
                SlotOwnershipInvariantViolation::OpenSlotOutOfBounds {
                    kind: "owner",
                    slot,
                },
            ));
        }
        let owner = self.canonical_slot_owner_ref(slot);
        if owner.is_some_and(|node| !node.slots[slot]) {
            return Err(ClusterInvariantError::SlotOwnership(
                SlotOwnershipInvariantViolation::CanonicalOwnerMismatch {
                    slot,
                    owner: self.canonical_slot_owner_id(slot).map(str::to_string),
                    claimant: None,
                },
            ));
        }
        let covered = owner.is_some_and(|node| {
            !node.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL)
        });
        if self.slots[slot] != covered {
            return Err(ClusterInvariantError::SlotOwnership(
                SlotOwnershipInvariantViolation::CanonicalBitmapMismatch { slot },
            ));
        }
        Ok(())
    }

    fn validate_node_identity(&self) -> Result<(), ClusterInvariantError> {
        if self.nodes.contains_key(&self.myself.name) {
            return Err(ClusterInvariantError::NodeIdentity(
                NodeIdentityInvariantViolation::LocalNodeInMap,
            ));
        }
        validate_strict_node_id("myself", &self.myself.name)?;
        for (key, node) in &self.nodes {
            if key != &node.name {
                return Err(ClusterInvariantError::NodeIdentity(
                    NodeIdentityInvariantViolation::MapKeyMismatch {
                        key: key.clone(),
                        node_id: node.name.clone(),
                    },
                ));
            }
            validate_strict_node_id("node", key)?;
        }
        for ((node_id, direction), view) in &self.link_views {
            if !is_strict_node_id(node_id) {
                return Err(ClusterInvariantError::NodeIdentity(
                    NodeIdentityInvariantViolation::InvalidNodeId {
                        context: "link",
                        node_id: node_id.clone(),
                    },
                ));
            }
            if node_id != view.node_id.as_str() || direction != &view.direction {
                return Err(ClusterInvariantError::NodeIdentity(
                    NodeIdentityInvariantViolation::LinkKeyMismatch {
                        key_node_id: node_id.clone(),
                        key_direction: *direction,
                    },
                ));
            }
        }
        Ok(())
    }

    fn validate_epochs(&self) -> Result<(), ClusterInvariantError> {
        for node in std::iter::once(&self.myself).chain(self.nodes.values()) {
            if node.config_epoch > self.current_epoch {
                return Err(ClusterInvariantError::EpochMonotonicity(
                    EpochInvariantViolation::CurrentBeforeNode {
                        node_id: node.name.clone(),
                        current: self.current_epoch,
                        node: node.config_epoch,
                    },
                ));
            }
        }
        if self.last_vote_epoch > self.current_epoch {
            return Err(ClusterInvariantError::EpochMonotonicity(
                EpochInvariantViolation::VoteAfterCurrent {
                    vote: self.last_vote_epoch,
                    current: self.current_epoch,
                },
            ));
        }
        Ok(())
    }

    fn validate_migrations(&self) -> Result<(), ClusterInvariantError> {
        let active = self
            .migrations
            .views()
            .into_iter()
            .filter(|job| !job.phase.is_terminal())
            .collect::<Vec<_>>();
        let mut ids = HashSet::new();
        let mut ranges: HashMap<MigrationOperationKey, Vec<SlotRanges>> = HashMap::new();
        for job in &active {
            let phase_matches = matches!(
                (job.operation, job.phase),
                (MigrationOperation::Import, MigrationPhase::Import(_))
                    | (MigrationOperation::Export, MigrationPhase::Export(_))
            );
            if !phase_matches {
                return Err(ClusterInvariantError::MigrationPhase(
                    MigrationInvariantViolation::OperationPhaseMismatch {
                        job_id: job.id.to_string(),
                        operation: job.operation,
                        phase: job.phase,
                    },
                ));
            }
            if !ids.insert(job.id.as_str()) {
                return Err(ClusterInvariantError::MigrationPhase(
                    MigrationInvariantViolation::DuplicateActiveId {
                        job_id: job.id.to_string(),
                    },
                ));
            }
            let key = MigrationOperationKey::from(job.operation);
            for previous in ranges.entry(key).or_default().iter() {
                if let Some(slot) = previous.overlaps(&job.ranges) {
                    return Err(ClusterInvariantError::MigrationPhase(
                        MigrationInvariantViolation::ActiveRangeOverlap {
                            operation: job.operation,
                            slot,
                        },
                    ));
                }
            }
            ranges
                .get_mut(&key)
                .expect("operation entry exists")
                .push(job.ranges.clone());
        }
        Ok(())
    }

    pub(crate) fn debug_assert_migration_invariants(&self) {
        let result = self.validate_migrations();
        debug_assert!(
            result.is_ok(),
            "cluster migration transition violated an invariant: {result:?}"
        );
    }

    pub(crate) fn debug_assert_link_invariants(&self) {
        debug_assert!(
            self.link_views.iter().all(|((node_id, direction), view)| {
                is_strict_node_id(node_id)
                    && node_id == view.node_id.as_str()
                    && direction == &view.direction
            }),
            "cluster link transition violated an identity invariant"
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum MigrationOperationKey {
    Import,
    Export,
}

impl From<MigrationOperation> for MigrationOperationKey {
    fn from(value: MigrationOperation) -> Self {
        match value {
            MigrationOperation::Import => Self::Import,
            MigrationOperation::Export => Self::Export,
        }
    }
}

fn validate_node_slots(
    node: &crate::engine::distributed::cluster::node::ClusterNode,
) -> Result<(), ClusterInvariantError> {
    let bitmap = node.count_slots();
    if bitmap != node.numslots {
        return Err(ClusterInvariantError::SlotOwnership(
            SlotOwnershipInvariantViolation::SlotCount {
                node_id: node.name.clone(),
                bitmap,
                declared: node.numslots,
            },
        ));
    }
    Ok(())
}

fn validate_strict_node_id(
    context: &'static str,
    node_id: &str,
) -> Result<(), ClusterInvariantError> {
    let is_valid = is_strict_node_id(node_id);
    if !is_valid {
        return Err(ClusterInvariantError::NodeIdentity(
            NodeIdentityInvariantViolation::InvalidNodeId {
                context,
                node_id: node_id.to_string(),
            },
        ));
    }
    Ok(())
}

fn is_strict_node_id(node_id: &str) -> bool {
    node_id.len() == 40
        && node_id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::distributed::cluster::migration::{
        ImportPhase, MigrationJob, MigrationJobId, MigrationOperation, MigrationPhase, SlotRanges,
    };
    use crate::engine::distributed::cluster::node::ClusterNode;
    use crate::engine::distributed::cluster::state::ClusterState;
    use crate::engine::distributed::cluster::{NodeId, SlotRange};

    fn node_id(byte: u8) -> String {
        String::from_utf8(vec![byte; 40]).expect("ASCII test node ID")
    }

    fn remote_node(id: &str) -> ClusterNode {
        ClusterNode::new(id.to_string(), "127.0.0.1".to_string(), 6380, 16_380, true)
    }

    #[test]
    fn classifies_slot_ownership_corruption() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.slots.pop();

        assert!(matches!(
            state.validate_invariants(),
            Err(ClusterInvariantError::SlotOwnership(
                SlotOwnershipInvariantViolation::VectorLength { field: "slots", .. }
            ))
        ));
    }

    #[test]
    fn rejects_cross_field_canonical_slot_corruption() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.myself.slots[9] = true;
        state.myself.numslots = 1;

        assert!(matches!(
            state.validate_invariants(),
            Err(ClusterInvariantError::SlotOwnership(
                SlotOwnershipInvariantViolation::CanonicalOwnerMismatch { slot: 9, .. }
            ))
        ));
    }

    #[test]
    fn rejects_duplicate_slot_claims_even_when_coverage_matches() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.add_slot(9).unwrap();
        let id = node_id(b'a');
        let mut duplicate = remote_node(&id);
        duplicate.slots[9] = true;
        duplicate.numslots = 1;
        state.nodes.insert(id, duplicate);

        assert!(matches!(
            state.validate_invariants(),
            Err(ClusterInvariantError::SlotOwnership(
                SlotOwnershipInvariantViolation::DuplicateSlotClaim { slot: 9, .. }
            ))
        ));
    }

    #[test]
    fn current_epoch_mutator_never_moves_backward() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        assert!(state.advance_current_epoch(9));
        assert!(!state.advance_current_epoch(4));
        assert_eq!(state.current_epoch, 9);
        assert!(state.record_vote_epoch(8));
        assert!(!state.record_vote_epoch(10));
        assert_eq!(state.last_vote_epoch, 8);
    }

    #[test]
    fn classifies_node_identity_corruption() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let id = node_id(b'a');
        state
            .nodes
            .insert("wrong-map-key".to_string(), remote_node(&id));

        assert!(matches!(
            state.validate_invariants(),
            Err(ClusterInvariantError::NodeIdentity(
                NodeIdentityInvariantViolation::MapKeyMismatch { .. }
            ))
        ));
    }

    #[test]
    fn rejects_malformed_remote_node_identity_even_when_map_key_matches() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state
            .nodes
            .insert("short".to_string(), remote_node("short"));

        assert!(matches!(
            state.validate_invariants(),
            Err(ClusterInvariantError::NodeIdentity(
                NodeIdentityInvariantViolation::InvalidNodeId {
                    context: "node",
                    ..
                }
            ))
        ));
    }

    #[test]
    fn classifies_epoch_monotonicity_corruption() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.current_epoch = 3;
        state.myself.config_epoch = 4;

        assert!(matches!(
            state.validate_invariants(),
            Err(ClusterInvariantError::EpochMonotonicity(
                EpochInvariantViolation::CurrentBeforeNode { .. }
            ))
        ));
    }

    #[test]
    fn classifies_migration_phase_corruption() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let source = NodeId::parse(node_id(b'a').as_bytes()).unwrap();
        let target = NodeId::parse(node_id(b'b').as_bytes()).unwrap();
        let id = MigrationJobId::parse(node_id(b'c').as_bytes()).unwrap();
        let ranges = SlotRanges::new(vec![SlotRange::new(1, 2).unwrap()]).unwrap();
        let mut job = MigrationJob::new(id, source, target, ranges, MigrationOperation::Export, 0);
        job.phase = MigrationPhase::Import(ImportPhase::WaitingForAck);
        state.migrations.start(job).unwrap();

        assert!(matches!(
            state.validate_invariants(),
            Err(ClusterInvariantError::MigrationPhase(
                MigrationInvariantViolation::OperationPhaseMismatch { .. }
            ))
        ));
    }
}
