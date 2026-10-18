mod job;
mod log;
mod protocol;
mod range;
mod state;

pub use job::{MigrationJob, MigrationJobId, MigrationJobView, MigrationOperation};
pub use log::MigrationRegistry;
pub use protocol::{
    MigrationAction, MigrationEvent, MigrationProtocolMessage, SlotMigrationConnectionRole,
};
pub use range::{SlotRanges, SlotRangesError};
pub use state::{ExportPhase, ImportPhase, MigrationPhase, TerminalPhase};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrationError {
    DuplicateJob(MigrationJobId),
    UnknownJob(MigrationJobId),
    ActiveSlotOverlap {
        slot: u16,
    },
    InvalidTransition {
        phase: &'static str,
        event: &'static str,
    },
}

#[cfg(test)]
mod tests;
