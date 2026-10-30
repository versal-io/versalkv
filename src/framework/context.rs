use crate::engine::command::SessionId;
use crate::engine::distributed::cluster::ClusterState;
use crate::engine::distributed::pubsub::PubSubState;
use crate::engine::distributed::replication::ReplicationState;
use crate::engine::distributed::sentinel::SentinelState;
use crate::engine::distributed::shutdown::ShutdownState;
use crate::engine::extension::module::ModuleRegistry;
use crate::engine::persistence::PersistenceState;
use crate::engine::state::db::DbContext;
use crate::foundation::memory::metrics::memory_stats::MemoryCategoryTracker;
use crate::foundation::memory::metrics::stats::ServerStats;
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::migration::transport::MigrateSocketCache;
use crate::framework::observability::commandlog::CommandLog;
use crate::framework::observability::slot_metrics::SlotMetrics;
use crate::framework::observability::slowlog::SlowLog;

mod control;
mod data;
mod distributed;
mod hotkeys;
mod lifecycle;
mod memory;
mod persistence;
mod runtime_config;
mod scripting;
mod slot_migration;
mod state;
mod telemetry;
pub use runtime_config::RuntimeConfig;
pub use state::{
    ClientPauseState, IoThreadInfo, LatencyState, MonitorEvent, PauseAction, ScriptExecutionState,
    TrackingState,
};

pub struct ServerContext {
    db: DbContext,
    stats: ServerStats,
    rconfig: RuntimeConfig,
    pubsub: PubSubState,
    persistence: PersistenceState,
    replication: ReplicationState,
    shutdown_requested: bool,
    /// Loading owned by replica full-sync lifecycle transitions.
    loading: bool,
    /// Loading owned by a synchronous RDB/AOF host task. Keep this separate
    /// because `REPLICAOF`, which C permits during loading, may tear down a
    /// replica session while an outer startup/debug load is still running.
    host_task_loading: bool,
    shutdown_state: ShutdownState,
    mem_stats: &'static MemoryCategoryTracker,
    slowlog: SlowLog,
    commandlog: CommandLog,
    hotkeys: std::rc::Rc<crate::engine::extension::hotkeys::Hotkeys>,
    slot_metrics: SlotMetrics,
    migrate_sockets: MigrateSocketCache,
    script: ScriptExecutionState,
    /// Module ABI callbacks may register commands synchronously into the L5
    /// dispatch table. This pointer is never used by Lua or hosted tasks.
    module_command_registry: *mut crate::framework::CommandRegistry,
    sentinel_state: SentinelState,
    module_registry: Box<ModuleRegistry>,
    cluster_state: Option<Box<ClusterState>>,
    slot_imports: std::collections::HashMap<
        crate::engine::distributed::cluster::migration::MigrationJobId,
        Box<crate::framework::distributed::migration::ImportDataset>,
    >,
    client_pause: ClientPauseState,
    // Match C's tail-queued `server.postponed_clients`: unpause must replay
    // commands in postponement order so an earlier blocking pop is registered
    // before a later push makes the key ready.
    paused_clients: indexmap::IndexSet<Token>,
    forkless_clients: indexmap::IndexSet<Token>,
    throttled_clients: indexmap::IndexSet<Token>,
    pending_unpause: bool,
    needs_eviction: bool,
    suppress_eviction_until_write: bool,
    slot_export_output_memory: usize,
    pending_acl_client_kills: Vec<String>,
    pending_acl_tokens_to_kill: Vec<Token>,
    pending_blocked_to_reject: Vec<SessionId>,
    latency: LatencyState,
    tracking: TrackingState,
    normal_client_memory: usize,
    io_thread_info: IoThreadInfo,
    pause_cron: u32,
    protected_clients: Vec<u64>,
    pending_tcp_rebind: Option<PendingTcpRebind>,
    pending_tls_reconfigure: Option<PendingTlsReconfigure>,
    pending_io_threads_reconfigure: Option<PendingIoThreadsReconfigure>,
    pending_server_restart: Option<RestartIntent>,
    startup_args: Vec<std::ffi::OsString>,
    /// Raw fds of all listening sockets (TCP / Unix / cluster bus). Recorded at
    /// bind time so fork children (BGSAVE / AOF rewrite) can close them, keeping
    /// the port free for the parent to rebind — aligns with C valkey
    /// `closeChildUnusedResourceAfterFork`.
    listener_fds: Vec<i32>,
}

#[derive(Clone)]
pub(crate) struct PendingTcpRebind {
    pub(crate) previous_config: RuntimeConfig,
    pub(crate) previous_oom_score_adj: Option<i32>,
    pub(crate) bind: String,
    pub(crate) port: u16,
    pub(crate) changed_parameter: &'static str,
}

#[derive(Clone)]
pub(crate) struct PendingTlsReconfigure {
    pub(crate) previous_config: RuntimeConfig,
    pub(crate) port_changed: bool,
    pub(crate) reconnect_replica: bool,
    pub(crate) rebind_cluster_bus: bool,
    pub(crate) changed_parameter: &'static str,
}

#[derive(Clone)]
pub(crate) struct PendingIoThreadsReconfigure {
    pub(crate) previous: RuntimeConfig,
    pub(crate) requested: usize,
}

/// Pending in-place process replacement requested by DEBUG RESTART /
/// CRASH-AND-RECOVER. Mirrors C valkey's `RESTART_SERVER_*` flags:
/// RESTART requests a graceful shutdown plus a config rewrite first, while
/// CRASH-AND-RECOVER uses `RESTART_SERVER_NONE` (hard replacement, no save).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RestartIntent {
    pub(crate) delay_ms: u64,
    pub(crate) graceful: bool,
    pub(crate) config_rewrite: bool,
}

#[cfg(test)]
mod tests;
