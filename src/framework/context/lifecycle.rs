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
use crate::foundation::memory::util_hash::FxIndexSet;
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
            client_infos: crate::foundation::memory::util_hash::FxHashMap::default(),
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
            hotkeys: std::rc::Rc::new(crate::engine::extension::hotkeys::Hotkeys::default()),
            slot_metrics: SlotMetrics::new(),
            migrate_sockets: MigrateSocketCache::new(),
            script: ScriptExecutionState::new(),
            module_command_registry: std::ptr::null_mut(),
            sentinel_state: SentinelState::new(),
            cluster_state: None,
            slot_imports: std::collections::HashMap::new(),
            slot_import_infrastructure_memory: 0,
            module_registry,
            client_pause: ClientPauseState {
                action: PauseAction::None,
                end_time_ms: 0,
                reason: "none",
            },
            paused_clients: FxIndexSet::default(),
            forkless_clients: FxIndexSet::default(),
            throttled_clients: FxIndexSet::default(),
            pending_unpause: false,
            needs_eviction: false,
            suppress_eviction_until_write: false,
            slot_export_output_memory: 0,
            pending_acl_client_kills: Vec::new(),
            pending_acl_tokens_to_kill: Vec::new(),
            pending_blocked_to_reject: Vec::new(),
            latency: LatencyState::new(),
            tracking: TrackingState::new(),
            normal_client_memory: 0,
            io_thread_info: crate::framework::context::IoThreadInfo::default(),
            tls_info: crate::foundation::memory::tls::TlsInfo::default(),
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

    /// Returns whether any slowlog/commandlog category would record a command
    /// with the given duration and request/reply sizes. Used on the hot path to
    /// skip the `CmdLogEntry` and `redacted_argv` allocations for fast
    /// commands (e.g. PING) before they are built.
    pub fn would_log_command(
        &self,
        duration_us: u64,
        req_bytes: usize,
        reply_bytes: usize,
        command_flags: u64,
    ) -> bool {
        let dur = duration_us as i64;
        let req = req_bytes as i64;
        let rep = reply_bytes as i64;
        (command_flags & CMD_SKIP_SLOWLOG == 0 && self.slowlog.would_record(dur))
            || (command_flags & CMD_SKIP_COMMANDLOG == 0 && self.commandlog.slow.would_record(dur))
            || (command_flags & CMD_SKIP_COMMANDLOG == 0
                && self.commandlog.large_request.would_record(req))
            || (command_flags & CMD_SKIP_COMMANDLOG == 0
                && self.commandlog.large_reply.would_record(rep))
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
        let dur = duration_us as i64;
        let req = req_bytes as i64;
        let rep = reply_bytes as i64;

        // Determine which logs would actually record this entry before
        // allocating `peer_addr.to_string()` or cloning `argv`. On the common
        // fast-command path (e.g. PING) all categories are below threshold and
        // we return without any heap allocation.
        let slowlog_record =
            command_flags & CMD_SKIP_SLOWLOG == 0 && self.slowlog.would_record(dur);
        let cmdlog_slow_record =
            command_flags & CMD_SKIP_COMMANDLOG == 0 && self.commandlog.slow.would_record(dur);
        let cmdlog_req_record = command_flags & CMD_SKIP_COMMANDLOG == 0
            && self.commandlog.large_request.would_record(req);
        let cmdlog_rep_record = command_flags & CMD_SKIP_COMMANDLOG == 0
            && self.commandlog.large_reply.would_record(rep);

        if !slowlog_record && !cmdlog_slow_record && !cmdlog_req_record && !cmdlog_rep_record {
            return;
        }

        let addr_str = peer_addr.to_string();
        if slowlog_record {
            self.slowlog
                .push(argv.clone(), dur, addr_str.clone(), client_name.clone());
        }
        if cmdlog_slow_record || cmdlog_req_record || cmdlog_rep_record {
            if cmdlog_slow_record {
                self.commandlog
                    .slow
                    .push(argv.clone(), dur, addr_str.clone(), client_name.clone());
            }
            if cmdlog_req_record {
                self.commandlog.large_request.push(
                    argv.clone(),
                    req,
                    addr_str.clone(),
                    client_name.clone(),
                );
            }
            if cmdlog_rep_record {
                self.commandlog
                    .large_reply
                    .push(argv, rep, addr_str, client_name);
            }
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
