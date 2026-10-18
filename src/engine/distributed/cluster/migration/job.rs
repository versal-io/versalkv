use std::fmt;

use crate::engine::distributed::cluster::node::generate_node_id;
use crate::engine::distributed::cluster::{NodeId, NodeIdError};

use super::{ExportPhase, ImportPhase, MigrationPhase, SlotRanges};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MigrationJobId(String);

impl MigrationJobId {
    pub fn generate() -> Self {
        Self(generate_node_id())
    }

    pub fn parse(value: &[u8]) -> Result<Self, NodeIdError> {
        NodeId::parse(value).map(|id| Self(id.as_str().to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MigrationJobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationOperation {
    Import,
    Export,
}

impl MigrationOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Import => "IMPORT",
            Self::Export => "EXPORT",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationJob {
    pub id: MigrationJobId,
    pub source: NodeId,
    pub target: NodeId,
    pub ranges: SlotRanges,
    pub operation: MigrationOperation,
    pub phase: MigrationPhase,
    pub create_time: i64,
    pub last_update_time: i64,
    pub last_ack_time: i64,
    pub message: Option<String>,
    pub cow_size: u64,
    pub remaining_repl_size: usize,
}

impl MigrationJob {
    pub fn new(
        id: MigrationJobId,
        source: NodeId,
        target: NodeId,
        ranges: SlotRanges,
        operation: MigrationOperation,
        now: i64,
    ) -> Self {
        let phase = match operation {
            MigrationOperation::Import => MigrationPhase::Import(ImportPhase::WaitingForAck),
            MigrationOperation::Export => MigrationPhase::Export(ExportPhase::Connecting),
        };
        Self {
            id,
            source,
            target,
            ranges,
            operation,
            phase,
            create_time: now,
            last_update_time: now,
            last_ack_time: now,
            message: None,
            cow_size: 0,
            remaining_repl_size: 0,
        }
    }

    pub fn is_active(&self) -> bool {
        !self.phase.is_terminal()
    }

    /// Returns true for import jobs that are actively importing on this node.
    /// `OccurringOnPrimary` is a passive tracking state on replicas and is not
    /// considered importing for overlap/ownership purposes (matches C's
    /// `clusterIsSlotImporting` which excludes the occurring-on-primary state).
    pub fn is_active_import(&self) -> bool {
        self.operation == super::MigrationOperation::Import
            && self.is_active()
            && !matches!(
                self.phase,
                super::MigrationPhase::Import(super::ImportPhase::OccurringOnPrimary)
            )
    }

    pub fn new_tracking_import(
        id: MigrationJobId,
        source: NodeId,
        target: NodeId,
        ranges: SlotRanges,
        now: i64,
    ) -> Self {
        let mut job = Self::new(id, source, target, ranges, MigrationOperation::Import, now);
        job.phase = MigrationPhase::Import(ImportPhase::OccurringOnPrimary);
        job
    }

    pub fn view(&self) -> MigrationJobView {
        MigrationJobView {
            id: self.id.clone(),
            source: self.source.clone(),
            target: self.target.clone(),
            ranges: self.ranges.clone(),
            operation: self.operation,
            phase: self.phase,
            create_time: self.create_time,
            last_update_time: self.last_update_time,
            last_ack_time: self.last_ack_time,
            message: self.message.clone(),
            cow_size: self.cow_size,
            remaining_repl_size: self.remaining_repl_size,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationJobView {
    pub id: MigrationJobId,
    pub source: NodeId,
    pub target: NodeId,
    pub ranges: SlotRanges,
    pub operation: MigrationOperation,
    pub phase: MigrationPhase,
    pub create_time: i64,
    pub last_update_time: i64,
    pub last_ack_time: i64,
    pub message: Option<String>,
    pub cow_size: u64,
    pub remaining_repl_size: usize,
}
