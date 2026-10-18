use super::MigrationJobId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SlotMigrationConnectionRole {
    Export(MigrationJobId),
    Import(MigrationJobId),
}

impl SlotMigrationConnectionRole {
    pub fn job_id(&self) -> &MigrationJobId {
        match self {
            Self::Export(id) | Self::Import(id) => id,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationEvent {
    Connected { auth_required: bool },
    AuthCommandSent,
    AuthAccepted,
    EstablishCommandSent,
    Established,
    SnapshotStarted,
    SnapshotFinished,
    Ack,
    SnapshotEof,
    PauseRequested,
    OutputDrained,
    Paused,
    FailoverGranted,
    OwnershipCommitted,
}

impl MigrationEvent {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Connected { .. } => "connected",
            Self::AuthCommandSent => "auth-command-sent",
            Self::AuthAccepted => "auth-accepted",
            Self::EstablishCommandSent => "establish-command-sent",
            Self::Established => "established",
            Self::SnapshotStarted => "snapshot-started",
            Self::SnapshotFinished => "snapshot-finished",
            Self::Ack => "ack",
            Self::SnapshotEof => "snapshot-eof",
            Self::PauseRequested => "pause-requested",
            Self::OutputDrained => "output-drained",
            Self::Paused => "paused",
            Self::FailoverGranted => "failover-granted",
            Self::OwnershipCommitted => "ownership-committed",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationProtocolMessage {
    Ack,
    RequestPause,
    Paused,
    RequestFailover,
    FailoverGranted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrationAction {
    Connect(MigrationJobId),
    CreateImport(MigrationJobId),
    SendAuth(MigrationJobId),
    SendEstablish(MigrationJobId),
    Send(MigrationJobId, MigrationProtocolMessage),
    StartSnapshot(MigrationJobId),
    Cleanup(MigrationJobId),
}
