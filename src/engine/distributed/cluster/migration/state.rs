#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImportPhase {
    WaitingForAck,
    ReceivingSnapshot,
    WaitingForPaused,
    FailoverRequested,
    FailoverGranted,
    OccurringOnPrimary,
}

impl ImportPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WaitingForAck => "waiting-for-ack",
            Self::ReceivingSnapshot => "receiving-snapshot",
            Self::WaitingForPaused => "waiting-for-paused",
            Self::FailoverRequested => "failover-requested",
            Self::FailoverGranted => "failover-granted",
            Self::OccurringOnPrimary => "occurring-on-primary",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExportPhase {
    Connecting,
    SendingAuthCommand,
    ReadingAuthResponse,
    SendingEstablishCommand,
    ReadingEstablishResponse,
    WaitingToSnapshot,
    Snapshotting,
    Streaming,
    WaitingToPause,
    FailoverPaused,
    FailoverGranted,
}

impl ExportPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connecting => "connecting",
            Self::SendingAuthCommand => "sending-auth-command",
            Self::ReadingAuthResponse => "reading-auth-response",
            Self::SendingEstablishCommand => "sending-establish-command",
            Self::ReadingEstablishResponse => "reading-establish-response",
            Self::WaitingToSnapshot => "waiting-to-snapshot",
            Self::Snapshotting => "snapshotting",
            Self::Streaming => "replicating",
            Self::WaitingToPause => "waiting-to-pause",
            Self::FailoverPaused => "failover-paused",
            Self::FailoverGranted => "failover-granted",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalPhase {
    Success,
    Cancelled,
    Failed,
}

impl TerminalPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationPhase {
    Import(ImportPhase),
    Export(ExportPhase),
    Terminal(TerminalPhase),
}

impl MigrationPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Import(phase) => phase.as_str(),
            Self::Export(phase) => phase.as_str(),
            Self::Terminal(phase) => phase.as_str(),
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Terminal(_))
    }
}
