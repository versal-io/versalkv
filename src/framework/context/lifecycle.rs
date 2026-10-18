use super::{
    ClientPauseState, LatencyState, PauseAction, ScriptExecutionState, ServerContext, TrackingState,
};
use crate::engine::command::{CMD_SKIP_COMMANDLOG, CMD_SKIP_SLOWLOG};
use crate::engine::distributed::pubsub::PubSubState;
use crate::engine::distributed::replication::protocol::generate_replid;
use crate::engine::distributed::replication::ReplicationState;
use crate::engine::distributed::sentinel::SentinelState;
use crate::engine::distributed::shutdown::ShutdownState;
use crate::engine::extension::module::ModuleRegistry;
use crate::engine::persistence::PersistenceState;
use crate::engine::state::db::DbContext;
use crate::foundation::memory::metrics::memory_stats::get_tracker;
use crate::foundation::memory::metrics::stats::ServerStats;
use crate::framework::context::RuntimeConfig;
use crate::framework::distributed::migration::transport::MigrateSocketCache;
use crate::framework::observability::commandlog::CommandLog;
use crate::framework::observability::slot_metrics::SlotMetrics;
use crate::framework::observability::slowlog::SlowLog;
use bytes::Bytes;
use std::net::SocketAddr;

impl ServerContext {
    pub fn new(num_dbs: u32) -> Self {
        Self::from_db_context(DbContext::new(num_dbs))
    }

    pub fn new_with_slot_router(
        num_dbs: u32,
        slot_bits: u32,
        slot_router: fn(&[u8]) -> usize,
    ) -> Self {
        Self::from_db_context(DbContext::new_with_slot_router(
            num_dbs,
            slot_bits,
            slot_router,
        ))
    }

    pub(crate) fn new_import_context(
        dbs: crate::foundation::io::store::kv_engine::DbSet,
        mut config: RuntimeConfig,
    ) -> Self {
        config.server.import_mode = true;
        let db = DbContext {
            dbs,
            blocking_registry: crate::engine::state::blocking_registry::BlockingClientRegistry::new(
            ),
            client_infos: std::collections::HashMap::new(),
        };
        let mut context = Self::from_db_context(db);
        context.rconfig = config;
        context
    }

    fn from_db_context(db: DbContext) -> Self {
        let rconfig = RuntimeConfig::new();
        let replid = generate_replid();
        let repl_backlog_size = rconfig.server.repl_backlog_size.max(0) as usize;
        let slowlog = SlowLog::new(
            rconfig.server.slowlog_log_slower_than,
            rconfig.server.slowlog_max_len,
        );
        let commandlog = CommandLog::new(
            rconfig.server.commandlog_slow_threshold,
            rconfig.server.commandlog_slow_max_len,
            rconfig.server.large_request_threshold,
            rconfig.server.large_request_max_len,
            rconfig.server.large_reply_threshold,
            rconfig.server.large_reply_max_len,
        );
        let module_registry = Box::new(ModuleRegistry::new());
        Self {
            db,
            stats: ServerStats::new(),
            rconfig,
            pubsub: PubSubState::new(),
            persistence: PersistenceState::new(),
            replication: ReplicationState::new(replid, repl_backlog_size),
            shutdown_requested: false,
            loading: false,
            host_task_loading: false,
            shutdown_state: ShutdownState::default(),
            mem_stats: get_tracker(),
            slowlog,
            commandlog,
            slot_metrics: SlotMetrics::new(),
            migrate_sockets: MigrateSocketCache::new(),
            script: ScriptExecutionState::new(),
            module_command_registry: std::ptr::null_mut(),
            sentinel_state: SentinelState::new(),
            cluster_state: None,
            slot_imports: std::collections::HashMap::new(),
            module_registry,
            client_pause: ClientPauseState {
                action: PauseAction::None,
                end_time_ms: 0,
                reason: "none",
            },
            paused_clients: indexmap::IndexSet::new(),
            pending_unpause: false,
            needs_eviction: false,
            slot_export_output_memory: 0,
            pending_acl_client_kills: Vec::new(),
            pending_acl_tokens_to_kill: Vec::new(),
            pending_blocked_to_reject: Vec::new(),
            latency: LatencyState::new(),
            tracking: TrackingState::new(),
            normal_client_memory: 0,
            io_thread_info: crate::framework::context::IoThreadInfo::default(),
            pause_cron: 0,
            protected_clients: Vec::new(),
            pending_tcp_rebind: None,
            pending_tls_reconfigure: None,
            pending_io_threads_reconfigure: None,
            pending_server_restart: None,
            startup_args: Vec::new(),
            listener_fds: Vec::new(),
        }
    }

    pub fn push_cmd_logs(
        &mut self,
        argv: Vec<Bytes>,
        duration_us: u64,
        peer_addr: &SocketAddr,
        client_name: Option<Bytes>,
        req_bytes: usize,
        reply_bytes: usize,
        command_flags: u64,
    ) {
        let addr_str = peer_addr.to_string();
        let dur = duration_us as i64;
        let req = req_bytes as i64;
        let rep = reply_bytes as i64;
        if command_flags & CMD_SKIP_SLOWLOG == 0 {
            self.slowlog
                .push(argv.clone(), dur, addr_str.clone(), client_name.clone());
        }
        if command_flags & CMD_SKIP_COMMANDLOG == 0 {
            self.commandlog
                .slow
                .push(argv.clone(), dur, addr_str.clone(), client_name.clone());
            self.commandlog.large_request.push(
                argv.clone(),
                req,
                addr_str.clone(),
                client_name.clone(),
            );
            self.commandlog
                .large_reply
                .push(argv, rep, addr_str, client_name);
        }
    }

    pub fn push_cmd_log_slow_only(
        &mut self,
        argv: Vec<Bytes>,
        duration_us: u64,
        peer_addr: &SocketAddr,
        client_name: Option<Bytes>,
    ) {
        let addr_str = peer_addr.to_string();
        let dur = duration_us as i64;
        self.slowlog
            .push(argv.clone(), dur, addr_str.clone(), client_name.clone());
        self.commandlog.slow.push(argv, dur, addr_str, client_name);
    }
}
