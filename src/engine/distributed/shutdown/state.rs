//! Shutdown state fields owned by ServerContext, plus SHUTDOWN flag bitmask
//! constants and their comma-separated string parse/format helpers.

pub const SHUTDOWN_NOFLAGS: u32 = 0;
pub const SHUTDOWN_SAVE: u32 = 1 << 0;
pub const SHUTDOWN_NOSAVE: u32 = 1 << 1;
pub const SHUTDOWN_NOW: u32 = 1 << 2;
pub const SHUTDOWN_FORCE: u32 = 1 << 3;
pub const SHUTDOWN_SAFE: u32 = 1 << 4;
pub const SHUTDOWN_FAILOVER: u32 = 1 << 5;

pub const MIN_SHUTDOWN_FAILOVER_VERSION: u32 = 0x09_00_00;

#[derive(Clone, Copy, Debug)]
pub struct ShutdownReplicaFacts<'a> {
    pub connection_id: usize,
    pub node_id: Option<&'a str>,
    pub version: u32,
    pub is_online: bool,
    pub ack_offset: Option<crate::engine::distributed::replication::ReplicationOffset>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShutdownHandoffDecision {
    Selected(crate::engine::distributed::cluster::NodeId),
    LegacyReplica,
    NoCandidate,
}

pub fn select_shutdown_handoff(
    primary_offset: crate::engine::distributed::replication::ReplicationOffset,
    replicas: &[ShutdownReplicaFacts<'_>],
) -> ShutdownHandoffDecision {
    if replicas
        .iter()
        .any(|replica| replica.version < MIN_SHUTDOWN_FAILOVER_VERSION)
    {
        return ShutdownHandoffDecision::LegacyReplica;
    }

    replicas
        .iter()
        .filter(|replica| replica.is_online && replica.ack_offset == Some(primary_offset))
        .filter_map(|replica| {
            crate::engine::distributed::cluster::NodeId::parse(replica.node_id?.as_bytes())
                .ok()
                .map(|node_id| (replica.connection_id, node_id))
        })
        .max_by_key(|(connection_id, _)| *connection_id)
        .map(|(_, node_id)| ShutdownHandoffDecision::Selected(node_id))
        .unwrap_or(ShutdownHandoffDecision::NoCandidate)
}

/// C checks every registered replica's ACK cursor regardless of its sync phase.
/// A replica waiting for an RDB at the current zero-byte boundary must not make
/// shutdown wait for the RDB child, which shutdown is responsible for killing.
pub fn replicas_are_ready(
    primary_offset: crate::engine::distributed::replication::ReplicationOffset,
    replicas: &[(
        bool,
        Option<crate::engine::distributed::replication::ReplicationOffset>,
    )],
) -> bool {
    let zero = crate::engine::distributed::replication::ReplicationOffset::try_from(0)
        .expect("zero is a valid replication offset");
    replicas
        .iter()
        .all(|(_is_online, ack_offset)| ack_offset.unwrap_or(zero) == primary_offset)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ShutdownState {
    /// Signal-driven "shutdown requested but not yet initiated" flag.
    pub asap: bool,
    /// Flags currently in effect if a shutdown is initiated (else 0).
    pub flags: u32,
    /// Millisecond deadline for the graceful replica-wait window.
    pub mstime_deadline: i64,
    /// Whether the shutdown GETACK request has already been propagated.
    pub replica_getack_sent: bool,
    /// True after the event loop has attempted the optional Cluster handoff.
    pub cluster_handoff_done: bool,
    /// Number of client connections blocked in the SHUTDOWN command.
    pub blocked_clients: usize,
    /// Last signal that scheduled a shutdown (SIGTERM/SIGINT/0).
    pub last_sig: i32,
    /// Flags translated from `last_sig` and staged for the next cron tick.
    /// Kept separate from `flags` because `flags` is only populated once
    /// `prepare_for_shutdown` has actually initiated a shutdown.
    pub pending_flags: u32,
    /// True once `finish_shutdown` has committed and the event loop should
    /// exit. Mirrored into `ctx.shutdown_requested`.
    pub initiated: bool,
    /// Monotonic mstime() snapshot captured when `request_shutdown` first
    /// marks the shutdown as pending; drives INFO `shutdown_in_milliseconds`.
    pub start_time_ms: i64,
}

pub fn request_shutdown(
    state: &mut ShutdownState,
    flags: u32,
    has_initial_aof_rewrite: bool,
) -> Result<(), &'static str> {
    if has_initial_aof_rewrite && flags & (SHUTDOWN_NOW | SHUTDOWN_FORCE) == 0 {
        state.asap = false;
        state.pending_flags = SHUTDOWN_NOFLAGS;
        return Err(super::messages::ERR_AOF_INIT_CANT_EXIT);
    }
    state.asap = true;
    state.pending_flags = flags;
    if state.start_time_ms == 0 {
        state.start_time_ms = crate::foundation::io::sys::time::mstime();
    }
    Ok(())
}

pub fn abort_shutdown(state: &mut ShutdownState) -> bool {
    let had_request = state.asap || state.initiated;
    *state = ShutdownState::default();
    had_request
}

/// Parse a comma-separated bitmask string like "nosave,force" or "default".
/// Returns None on unknown token. `default` maps to SHUTDOWN_NOFLAGS.
pub fn parse_sig_flags(s: &str) -> Option<u32> {
    if s.trim().is_empty() {
        return Some(SHUTDOWN_NOFLAGS);
    }
    let mut acc = SHUTDOWN_NOFLAGS;
    for tok in s.split(&[',', ' ']).filter(|t| !t.is_empty()) {
        acc |= match tok.to_ascii_lowercase().as_str() {
            "default" => SHUTDOWN_NOFLAGS,
            "save" => SHUTDOWN_SAVE,
            "nosave" => SHUTDOWN_NOSAVE,
            "now" => SHUTDOWN_NOW,
            "force" => SHUTDOWN_FORCE,
            "safe" => SHUTDOWN_SAFE,
            "failover" => SHUTDOWN_FAILOVER,
            _ => return None,
        };
    }
    Some(acc)
}

pub fn format_sig_flags(flags: u32) -> String {
    if flags == SHUTDOWN_NOFLAGS {
        return "default".to_string();
    }
    let mut parts = Vec::with_capacity(6);
    if flags & SHUTDOWN_SAVE != 0 {
        parts.push("save");
    }
    if flags & SHUTDOWN_NOSAVE != 0 {
        parts.push("nosave");
    }
    if flags & SHUTDOWN_NOW != 0 {
        parts.push("now");
    }
    if flags & SHUTDOWN_FORCE != 0 {
        parts.push("force");
    }
    if flags & SHUTDOWN_SAFE != 0 {
        parts.push("safe");
    }
    if flags & SHUTDOWN_FAILOVER != 0 {
        parts.push("failover");
    }
    parts.join(" ")
}
