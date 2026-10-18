use crate::engine::distributed::replication::SnapshotBoundary;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReplicaId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationConnection {
    Primary(ReplicaId),
    Replica,
    RdbChannelCandidate,
    RdbChannel { main: ConnectionId },
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaConnClass {
    Main,
    Rdb,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RdbFilterMode {
    #[default]
    All,
    Functions,
    Empty,
}

/// Effective replica full-sync install strategy. The runtime selects this once
/// before admitting any RDB payload and the link keeps it immutable for the
/// lifetime of that receive session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdbLoadMode {
    Direct,
    Swap,
    DiskBacked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaConnectionInfo {
    pub ip: String,
    pub port: u16,
    pub capability: Vec<String>,
    pub rdb_filter_mode: RdbFilterMode,
}

impl ReplicaConnectionInfo {
    /// Format the replica identity the same way C's `replicationGetReplicaName`
    /// (replication.c:87) and `formatAddr` (connection.h:346) do: `ip:port` for
    /// IPv4, `[ip]:port` for IPv6, or `ip:<unknown-replica-port>` when the
    /// replica never announced a listening port.
    pub fn display_name(&self) -> String {
        if self.port != 0 {
            if self.ip.contains(':') {
                format!("[{}]:{}", self.ip, self.port)
            } else {
                format!("{}:{}", self.ip, self.port)
            }
        } else {
            format!("{}:<unknown-replica-port>", self.ip)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrimaryLinkState {
    AwaitingPsync,
    PreparingFullResync,
    SendingRdb,
    WaitingForReplicaAck,
    Streaming,
    Closing,
}

// NOTE: dead_code allow applies only to the variants reserved for later tasks:
// SendingRdb, WaitingForMainPsync, WaitingForReplicaAck, Online, Aborting.
// Disabled / Negotiating / WaitingForRdbChannel are used in production today.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrimaryDualChannelState {
    Disabled,
    Negotiating,
    WaitingForRdbChannel,
    SendingRdb,
    WaitingForMainPsync,
    WaitingForReplicaAck,
    Online,
    Aborting,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrimarySyncPlan {
    /// Partial resync accepted. If `new_replid` is set, the replica matched
    /// via `second_replid` and must adopt `new_replid` as the current primary id
    /// (master sends `+CONTINUE <new_replid>`).
    Continue {
        new_replid: Option<String>,
    },
    FullResync {
        boundary: SnapshotBoundary,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkError {
    BacklogEvicted,
    Authentication,
    /// A target rejected the special PSYNC FAILOVER handoff request.
    FailoverRejected,
    /// The primary cannot answer PSYNC yet; reconnect and retry later.
    TryLater,
    Protocol,
    Apply,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterInfo {
    pub host: String,
    pub port: u16,
}

/// Why the current upstream session ended, used to select C-compatible retry timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MasterReconnectCause {
    RdbTransferFailed,
    EstablishedLinkLost,
    AdministrativeClientKill,
    ReplicationTimeout,
    ExplicitTargetChange,
    HandshakeFailed,
    TryLater,
    ConnectFailed,
    ResolveFailed,
    ClusterPrimaryUnavailable,
}

impl MasterReconnectCause {
    pub(crate) fn delay_ms(self) -> i64 {
        match self {
            Self::RdbTransferFailed
            | Self::EstablishedLinkLost
            | Self::ReplicationTimeout
            | Self::ExplicitTargetChange => 0,
            Self::HandshakeFailed
            | Self::TryLater
            | Self::ConnectFailed
            | Self::ResolveFailed
            | Self::ClusterPrimaryUnavailable
            | Self::AdministrativeClientKill => 1_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaLinkState {
    Connecting,
    WaitAuthReply,
    WaitPong,
    WaitReplconfReplies { remaining: u8 },
    WaitDualRdbClientIdReply,
    WaitPsyncReply,
    ReceivingRdbHeader,
    ReceivingRdb { remaining: usize },
    ReceivingRdbEof { marker: [u8; 40] },
    Streaming,
}

#[allow(dead_code)] // Wired into dual-channel negotiation in later slices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaRdbChannelState {
    None,
    Connecting,
    Handshake,
    WaitingForEndOffset,
    ReceivingRdb,
    RdbLoaded,
    Aborting,
}

impl PrimaryDualChannelState {
    pub fn transition(self, next: Self) -> Result<Self, LinkError> {
        let valid = matches!(
            (self, next),
            (Self::Disabled, Self::Disabled | Self::Negotiating)
                | (
                    Self::Negotiating,
                    Self::WaitingForRdbChannel | Self::Aborting
                )
                | (
                    Self::WaitingForRdbChannel,
                    Self::SendingRdb | Self::WaitingForMainPsync | Self::Aborting
                )
                | (Self::SendingRdb, Self::WaitingForMainPsync | Self::Aborting)
                | (
                    Self::WaitingForMainPsync,
                    Self::WaitingForReplicaAck | Self::Aborting
                )
                | (Self::WaitingForReplicaAck, Self::Online | Self::Aborting)
                | (Self::Online, Self::Aborting)
                | (Self::Aborting, Self::Disabled)
        );
        valid.then_some(next).ok_or(LinkError::Protocol)
    }
}

#[allow(dead_code)] // Wired into dual-channel negotiation in later slices.
impl ReplicaRdbChannelState {
    pub fn transition(self, next: Self) -> Result<Self, LinkError> {
        let valid = matches!(
            (self, next),
            (Self::None, Self::None | Self::Connecting)
                | (Self::Connecting, Self::Handshake | Self::Aborting)
                | (Self::Handshake, Self::WaitingForEndOffset | Self::Aborting)
                | (
                    Self::WaitingForEndOffset,
                    Self::ReceivingRdb | Self::Aborting
                )
                | (Self::ReceivingRdb, Self::RdbLoaded | Self::Aborting)
                | (Self::RdbLoaded, Self::None | Self::Aborting)
                | (Self::Aborting, Self::None)
        );
        valid.then_some(next).ok_or(LinkError::Protocol)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicaAction {
    Send(Vec<u8>),
    /// Main-channel PSYNC that joins a dual-channel RDB transfer. C Valkey's
    /// `DEBUG PAUSE-AFTER-FORK` stops the replica immediately before this write.
    SendDualPsync(Vec<u8>),
    #[allow(dead_code)]
    OpenRdbChannel {
        initial_frames: Vec<Vec<u8>>,
    },
}

#[derive(Clone)]
pub enum ReplicaEvent {
    /// A decoded top-level command from the replication stream, applied in
    /// order. The full-sync RDB is no longer carried as an event: it is decoded
    /// incrementally in the bounded-read receive path and committed there once
    /// the streaming decoder reaches EOF (see `rdb_recv_path.rs`).
    Apply {
        apply: crate::framework::distributed::replication::ReplicationApply,
        end_offset: crate::engine::distributed::replication::ReplicationOffset,
    },
}
