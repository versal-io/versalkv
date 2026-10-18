//! Shutdown state and protocol constants.
//! Process, file, signal, and persistence orchestration lives in
//! `runtime::extension::shutdown`.

pub mod messages;
pub mod state;
#[cfg(test)]
mod tests;

pub use state::{
    abort_shutdown, format_sig_flags, parse_sig_flags, replicas_are_ready, request_shutdown,
    select_shutdown_handoff, ShutdownHandoffDecision, ShutdownReplicaFacts, ShutdownState,
    SHUTDOWN_FAILOVER, SHUTDOWN_FORCE, SHUTDOWN_NOFLAGS, SHUTDOWN_NOSAVE, SHUTDOWN_NOW,
    SHUTDOWN_SAFE, SHUTDOWN_SAVE,
};
