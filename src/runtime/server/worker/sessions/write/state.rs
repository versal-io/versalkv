use crate::foundation::io::transport::WriteOutcome;
use crate::framework::capabilities::io::Token;

/// Classifies a writable connection by its role so `handle_writable` can
/// dispatch by type instead of stacking boolean flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WritableTarget {
    OrdinaryClient,
    ReplicaMain,
    ReplicaRdb,
    DownstreamReplica,
    SlotExport,
}

#[derive(Clone, Copy)]
pub(super) struct WritableContext {
    pub(super) token: Token,
    pub(super) target: WritableTarget,
}

impl WritableContext {
    pub(super) fn is_ordinary(&self) -> bool {
        matches!(self.target, WritableTarget::OrdinaryClient)
    }

    pub(super) fn is_replica_rdb(&self) -> bool {
        matches!(self.target, WritableTarget::ReplicaRdb)
    }

    pub(super) fn is_master_link(&self) -> bool {
        matches!(self.target, WritableTarget::ReplicaMain)
    }

    pub(super) fn is_slot_export(&self) -> bool {
        matches!(self.target, WritableTarget::SlotExport)
    }

    pub(super) fn is_downstream_replica(&self) -> bool {
        matches!(self.target, WritableTarget::DownstreamReplica)
    }
}

impl WritableTarget {
    pub(super) fn as_str(&self) -> &'static str {
        match self {
            Self::OrdinaryClient => "ordinary-client",
            Self::ReplicaMain => "replica-main",
            Self::ReplicaRdb => "replica-rdb",
            Self::DownstreamReplica => "downstream-replica",
            Self::SlotExport => "slot-export",
        }
    }
}

pub(super) struct WritableFlush {
    pub(super) result: WriteOutcome,
    pub(super) flushed_dual_channel_sync: bool,
    pub(super) flushed_dual_rdb_client_id: bool,
    pub(super) replication_written: usize,
    pub(super) written: usize,
}
