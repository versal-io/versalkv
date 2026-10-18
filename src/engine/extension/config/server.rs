pub const CONFIG_OOM_PRIMARY: usize = 0;
pub const CONFIG_OOM_REPLICA: usize = 1;
pub const CONFIG_OOM_BGCHILD: usize = 2;
pub const CONFIG_OOM_COUNT: usize = 3;

pub const OOM_SCORE_ADJ_NO: i32 = 0;
pub const OOM_SCORE_RELATIVE: i32 = 1;
pub const OOM_SCORE_ADJ_ABSOLUTE: i32 = 2;

pub const OOM_SCORE_ADJ_DEFAULTS: [i32; CONFIG_OOM_COUNT] = [0, 200, 800];

pub const DEFAULT_LATENCY_PERCENTILES: &[f64] = &[50.0, 99.0, 99.9];

/// Maximum configured thread count, including the main thread.
/// Matches Valkey's `IO_THREADS_MAX_NUM`.
pub const IO_THREADS_MAX: i64 = 256;

pub const CONFIG_DEFAULT_HZ: i32 = 10;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ProtectedAction {
    No,
    Yes,
    Local,
}

impl ProtectedAction {
    pub fn as_str(self) -> &'static str {
        match self {
            ProtectedAction::No => "no",
            ProtectedAction::Yes => "yes",
            ProtectedAction::Local => "local",
        }
    }

    pub fn parse(value: &str) -> ProtectedAction {
        match value.to_ascii_lowercase().as_str() {
            "local" => ProtectedAction::Local,
            "yes" | "true" | "1" => ProtectedAction::Yes,
            _ => ProtectedAction::No,
        }
    }
}

pub const SHUTDOWN_SIG_ENUM_VALUES: &[&str] = &[
    "default", "save", "nosave", "now", "force", "safe", "failover",
];

pub const LOGLEVEL_ENUM_VALUES: &[&str] = &["debug", "verbose", "notice", "warning", "nothing"];

pub const MAXMEMORY_POLICY_ENUM_VALUES: &[&str] = &[
    "noeviction",
    "allkeys-lru",
    "allkeys-lfu",
    "allkeys-random",
    "volatile-lru",
    "volatile-lfu",
    "volatile-random",
    "volatile-ttl",
];

#[derive(Clone)]
pub struct ServerConfig {
    pub listening_port: u16,
    /// Dedicated TLS listener port. Zero leaves TLS disabled.
    pub tls_port: u16,
    pub tls_cert_file: String,
    pub tls_key_file: String,
    pub tls_ca_cert_file: String,
    pub tls_ca_cert_dir: String,
    pub tls_auth_clients: String,
    /// Whitespace-separated allowed TLS protocol versions; empty selects the
    /// C-compatible TLS 1.2/1.3 default policy.
    pub tls_protocols: String,
    /// OpenSSL cipher-list policy for TLS 1.2 and older protocols.
    pub tls_ciphers: String,
    /// OpenSSL cipher-suite policy for TLS 1.3.
    pub tls_ciphersuites: String,
    pub tls_prefer_server_ciphers: bool,
    pub tls_session_caching: bool,
    pub tls_session_cache_size: i32,
    pub tls_replication: bool,
    pub tls_cluster: bool,
    pub tls_client_cert_file: String,
    pub tls_client_key_file: String,
    pub daemonize: bool,
    pub supervised: String,
    pub supervised_active: bool,
    pub server_cpulist: String,
    pub bio_cpulist: String,
    pub aof_rewrite_cpulist: String,
    pub bgsave_cpulist: String,
    pub ignore_warnings: String,
    pub sentinel_mode: bool,
    pub active_expire_enabled: bool,
    /// C `active-expire-effort`, from 1 (default) through 10.
    pub active_expire_effort: u8,
    pub activedefrag: bool,
    pub active_defrag_cycle_min: u8,
    pub active_defrag_cycle_max: u8,
    pub active_defrag_threshold_lower: u16,
    pub active_defrag_threshold_upper: u16,
    pub active_defrag_cycle_us: u64,
    pub active_defrag_ignore_bytes: usize,
    pub active_defrag_max_scan_fields: usize,
    /// C `activerehashing`: permit server-cron to advance in-progress hash
    /// table migrations while idle.
    pub active_rehashing: bool,
    pub dict_resizing: bool,
    pub import_mode: bool,
    pub notify_keyspace_events: String,
    pub lazyfree_lazy_eviction: bool,
    pub lazyfree_lazy_expire: bool,
    pub lazyfree_lazy_server_del: bool,
    pub lazyfree_lazy_user_del: bool,
    pub lazyfree_lazy_user_flush: bool,
    pub slowlog_log_slower_than: i64,
    pub slowlog_max_len: i64,
    pub latency_monitor_threshold: i64,
    pub proto_max_bulk_len: i64,
    /// TCP keepalive idle interval in seconds; zero disables it for newly accepted clients.
    pub tcp_keepalive: u64,
    /// C `prefetch-batch-max-size`: zero disables pipeline key warming.
    pub prefetch_batch_max_size: usize,
    pub availability_zone: String,
    pub config_file: Option<String>,
    /// Optional pidfile path; an empty value disables pidfile creation.
    pub pidfile: String,
    pub commandlog_slow_threshold: i64,
    pub commandlog_slow_max_len: i64,
    pub large_request_threshold: i64,
    pub large_request_max_len: i64,
    pub large_reply_threshold: i64,
    pub large_reply_max_len: i64,
    pub loglevel: String,
    pub log_format: String,
    pub log_timestamp_format: String,
    pub debug_context: String,
    pub crash_log_enabled: bool,
    pub syslog_enabled: bool,
    pub syslog_ident: String,
    pub syslog_facility: String,
    pub hide_user_data_from_log: bool,
    pub watchdog_period: u64,
    pub crash_memcheck_enabled: bool,
    pub use_exit_on_panic: bool,
    pub enforce_reply_list: bool,
    pub lua_time_limit: u64,
    pub lua_enable_insecure_api: bool,
    pub maxmemory: u64,
    pub maxmemory_policy: String,
    pub maxmemory_samples: i64,
    pub maxmemory_eviction_tenacity: i64,
    pub lfu_log_factor: i64,
    pub lfu_decay_time: i64,
    pub oom_score_adj: i32,
    pub oom_score_adj_values: [i32; CONFIG_OOM_COUNT],
    pub mptcp: bool,
    pub latency_tracking: bool,
    pub latency_tracking_info_percentiles: Vec<f64>,
    pub skip_checksum_validation: bool,
    pub debug_pause_after_fork: bool,
    /// DEBUG DELAY-RDB-CLIENT-FREE-SECONDS, used by dual-channel grace tests.
    pub debug_delay_rdb_client_free_seconds: i64,
    pub maxclients: u32,
    /// C `max-new-connections-per-cycle`, shared per TCP listener callback.
    pub max_new_connections_per_cycle: u16,
    /// C `max-new-tls-connections-per-cycle`, applied per TLS listener callback.
    pub max_new_tls_connections_per_cycle: u16,
    /// C `timeout`: idle timeout in seconds for ordinary clients; zero disables it.
    pub client_timeout: u64,
    /// C `tcp-backlog`: startup-only listen queue size.
    pub tcp_backlog: u32,
    pub enable_module_command: ProtectedAction,
    pub enable_protected_configs: ProtectedAction,
    pub enable_debug_command: ProtectedAction,
    pub protected_mode: bool,
    pub shutdown_on_sigterm: u32,
    pub shutdown_on_sigint: u32,
    pub shutdown_timeout: u64,
    pub repl_backlog_size: i64,
    pub maxmemory_clients: String,
    /// C `client-query-buffer-limit`, a validated byte limit for incomplete
    /// ordinary-client requests. The configuration surface accepts memory
    /// suffixes but stores the canonical byte count.
    pub client_query_buffer_limit: usize,
    /// Maximum keys retained by CLIENT TRACKING's invalidation table.
    pub tracking_table_max_keys: usize,
    pub hll_sparse_max_bytes: i64,
    pub extended_redis_compatibility: bool,
    pub oom_score_adj_adjusted: bool,
    pub oom_score_adj_base: i32,

    pub cluster_enabled: bool,
    pub cluster_port: u16,
    pub cluster_ping_interval: u64,
    pub cluster_node_timeout: u64,
    pub cluster_blacklist_ttl: u64,
    pub cluster_config_file: String,
    pub cluster_announce_ip: String,
    pub cluster_announce_client_ipv4: String,
    pub cluster_announce_client_ipv6: String,
    pub cluster_announce_hostname: String,
    pub cluster_announce_human_nodename: String,
    pub cluster_announce_port: u16,
    pub cluster_announce_tls_port: u16,
    pub cluster_announce_client_port: u16,
    pub cluster_announce_client_tls_port: u16,
    pub cluster_announce_bus_port: u16,
    pub cluster_config_save_behavior: String,
    pub cluster_message_gossip_perc: i64,
    pub cluster_slot_stats_enabled: bool,
    pub cluster_databases: u16,
    pub cluster_replica_no_failover: bool,
    pub cluster_require_full_coverage: bool,
    pub cluster_allow_reads_when_down: bool,
    pub cluster_allow_pubsubshard_when_down: bool,
    pub cluster_allow_replica_migration: bool,
    pub cluster_replica_validity_factor: i64,
    pub cluster_migration_barrier: i64,
    pub cluster_manual_failover_timeout: i64,
    pub cluster_link_sendbuf_limit: u64,
    pub cluster_slot_migration_log_max_len: u64,
    pub cluster_preferred_endpoint_type: String,
    pub acllog_max_len: usize,
    pub acl_pubsub_default: String,
    pub aclfile: String,
    pub hz: i32,
    pub databases: i64,
    pub proc_title_template: String,
    /// Whether startup may overwrite argv with the configured process title.
    pub set_proc_title: bool,
    /// Whether startup must emit the banner even when stdout is not a TTY.
    pub always_show_logo: bool,
    /// Configured IO thread count, including the main thread.
    pub io_threads: i64,
    /// Debug policy that activates every configured IO worker on socket work.
    pub io_threads_always_active: bool,
    pub unixsocket: String,
    pub unixsocketgroup: String,
    pub unixsocketperm: u32,
    pub bindaddr: String,
    pub bind: String,
    pub bind_source_addr: String,
    pub slot_migration_max_failover_repl_bytes: i64,
    pub rename_commands: Vec<(String, String)>,
    pub hash_seed: String,
}

impl ServerConfig {
    pub fn new() -> Self {
        Self {
            listening_port: 0,
            tls_port: 0,
            tls_cert_file: String::new(),
            tls_key_file: String::new(),
            tls_ca_cert_file: String::new(),
            tls_ca_cert_dir: String::new(),
            tls_auth_clients: "yes".to_string(),
            tls_protocols: String::new(),
            tls_ciphers: String::new(),
            tls_ciphersuites: String::new(),
            tls_prefer_server_ciphers: false,
            tls_session_caching: true,
            tls_session_cache_size: 20 * 1024,
            tls_replication: false,
            tls_cluster: false,
            tls_client_cert_file: String::new(),
            tls_client_key_file: String::new(),
            daemonize: false,
            supervised: "no".to_string(),
            supervised_active: false,
            server_cpulist: String::new(),
            bio_cpulist: String::new(),
            aof_rewrite_cpulist: String::new(),
            bgsave_cpulist: String::new(),
            ignore_warnings: String::new(),
            sentinel_mode: false,
            active_expire_enabled: true,
            active_expire_effort: 1,
            activedefrag: false,
            active_defrag_cycle_min: 1,
            active_defrag_cycle_max: 25,
            active_defrag_threshold_lower: 10,
            active_defrag_threshold_upper: 100,
            active_defrag_cycle_us: 500,
            active_defrag_ignore_bytes: 100 << 20,
            active_defrag_max_scan_fields: 1000,
            active_rehashing: true,
            dict_resizing: true,
            import_mode: false,
            notify_keyspace_events: String::new(),
            lazyfree_lazy_eviction: true,
            lazyfree_lazy_expire: true,
            lazyfree_lazy_server_del: true,
            lazyfree_lazy_user_del: true,
            lazyfree_lazy_user_flush: true,
            slowlog_log_slower_than: 10000,
            slowlog_max_len: 128,
            latency_monitor_threshold: 0,
            proto_max_bulk_len: 512 * 1024 * 1024,
            tcp_keepalive: 300,
            prefetch_batch_max_size: 16,
            availability_zone: String::new(),
            config_file: None,
            pidfile: String::new(),
            commandlog_slow_threshold: 10000,
            commandlog_slow_max_len: 128,
            large_request_threshold: 1024 * 1024,
            large_request_max_len: 128,
            large_reply_threshold: 1024 * 1024,
            large_reply_max_len: 128,
            loglevel: "notice".to_string(),
            log_format: "legacy".to_string(),
            log_timestamp_format: "legacy".to_string(),
            debug_context: String::new(),
            crash_log_enabled: true,
            syslog_enabled: false,
            syslog_ident: "valkey".to_string(),
            syslog_facility: "local0".to_string(),
            hide_user_data_from_log: true,
            watchdog_period: 0,
            crash_memcheck_enabled: true,
            use_exit_on_panic: false,
            enforce_reply_list: false,
            lua_time_limit: 5000,
            lua_enable_insecure_api: false,
            maxmemory: 0,
            maxmemory_policy: "noeviction".to_string(),
            maxmemory_samples: 5,
            maxmemory_eviction_tenacity: 10,
            lfu_log_factor: 10,
            lfu_decay_time: 1,
            oom_score_adj: OOM_SCORE_ADJ_NO,
            oom_score_adj_values: OOM_SCORE_ADJ_DEFAULTS,
            mptcp: false,
            latency_tracking: true,
            latency_tracking_info_percentiles: DEFAULT_LATENCY_PERCENTILES.to_vec(),
            skip_checksum_validation: false,
            debug_pause_after_fork: false,
            debug_delay_rdb_client_free_seconds: 0,
            maxclients: 10000,
            max_new_connections_per_cycle: 10,
            max_new_tls_connections_per_cycle: 1,
            client_timeout: 0,
            tcp_backlog: 511,
            enable_module_command: ProtectedAction::No,
            enable_protected_configs: ProtectedAction::No,
            enable_debug_command: ProtectedAction::No,
            protected_mode: true,
            shutdown_on_sigterm: crate::engine::distributed::shutdown::SHUTDOWN_NOFLAGS,
            shutdown_on_sigint: crate::engine::distributed::shutdown::SHUTDOWN_NOFLAGS,
            shutdown_timeout: 10,
            repl_backlog_size: 1048576,
            maxmemory_clients: "0".to_string(),
            client_query_buffer_limit: 1024 * 1024 * 1024,
            tracking_table_max_keys: 1_000_000,
            hll_sparse_max_bytes: 3000,
            extended_redis_compatibility: false,
            oom_score_adj_adjusted: false,
            oom_score_adj_base: 0,
            cluster_enabled: false,
            cluster_port: 0,
            cluster_ping_interval: 0,
            cluster_node_timeout: 15000,
            cluster_blacklist_ttl: 60,
            cluster_config_file: "nodes.conf".to_string(),
            cluster_announce_ip: String::new(),
            cluster_announce_client_ipv4: String::new(),
            cluster_announce_client_ipv6: String::new(),
            cluster_announce_hostname: String::new(),
            cluster_announce_human_nodename: String::new(),
            cluster_announce_port: 0,
            cluster_announce_tls_port: 0,
            cluster_announce_client_port: 0,
            cluster_announce_client_tls_port: 0,
            cluster_announce_bus_port: 0,
            cluster_config_save_behavior: "sync".to_string(),
            cluster_message_gossip_perc: 10,
            cluster_slot_stats_enabled: false,
            cluster_databases: 1,
            cluster_replica_no_failover: false,
            cluster_require_full_coverage: true,
            cluster_allow_reads_when_down: false,
            cluster_allow_pubsubshard_when_down: true,
            cluster_allow_replica_migration: true,
            cluster_replica_validity_factor: 10,
            cluster_migration_barrier: 1,
            cluster_manual_failover_timeout: 5000,
            cluster_link_sendbuf_limit: 0,
            cluster_slot_migration_log_max_len: 1000,
            cluster_preferred_endpoint_type: "ip".to_string(),
            acllog_max_len: 128,
            acl_pubsub_default: "resetchannels".to_string(),
            aclfile: String::new(),
            hz: CONFIG_DEFAULT_HZ,
            databases: 16,
            proc_title_template: "{title} {listen-addr} {server-mode}".to_string(),
            set_proc_title: true,
            always_show_logo: false,
            io_threads: 1,
            io_threads_always_active: false,
            unixsocket: String::new(),
            unixsocketgroup: String::new(),
            unixsocketperm: 0,
            bindaddr: String::new(),
            bind: "* -::*".to_string(),
            bind_source_addr: String::new(),
            slot_migration_max_failover_repl_bytes: 0,
            rename_commands: Vec::new(),
            hash_seed: String::new(),
        }
    }

    pub fn set_bind(&mut self, bind: String) {
        self.bindaddr = bind
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_start_matches('-')
            .to_string();
        self.bind = bind;
    }

    pub fn proc_title_vars(
        &self,
        title: &str,
    ) -> crate::foundation::io::sys::proctitle::ProcTitleVars {
        use crate::foundation::io::sys::proctitle::ProcTitleVars;
        let listen_addr = if self.listening_port != 0 {
            let addr = if self.bindaddr.is_empty() {
                "*"
            } else {
                &self.bindaddr
            };
            format!("{}:{}", addr, self.listening_port)
        } else {
            format!("unixsocket:{}", self.unixsocket)
        };
        let server_mode = if self.cluster_enabled {
            "[cluster]".to_string()
        } else if self.sentinel_mode {
            "[sentinel]".to_string()
        } else {
            String::new()
        };
        let config_file = self.config_file.clone().unwrap_or_else(|| "-".to_string());
        ProcTitleVars {
            title: title.to_string(),
            listen_addr,
            server_mode,
            config_file,
            port: self.listening_port.to_string(),
            tls_port: self.tls_port.to_string(),
            unixsocket: self.unixsocket.clone(),
        }
    }
}

impl crate::foundation::io::sys::proctitle::ProcTitleVars {
    /// Validation-time placeholders: each field filled with a non-empty dummy
    /// so structurally valid templates (using only the 7 known variables)
    /// produce a non-empty string and pass validation. Truly invalid templates
    /// (unknown variable names or unclosed `{`) still yield None and are rejected.
    /// This is required for CONFIG SANITY round-trips (GET → SET → GET) not to
    /// fail on the default template when the runtime context for real values
    /// isn't available during validation; otherwise unit/introspection.tcl
    /// CONFIG SANITY fails with a spurious template-format error.
    pub fn validation() -> Self {
        Self {
            title: "versal-server".to_string(),
            listen_addr: "127.0.0.1:6379".to_string(),
            server_mode: "standalone".to_string(),
            config_file: "/etc/valkey.conf".to_string(),
            port: "6379".to_string(),
            tls_port: "0".to_string(),
            unixsocket: "/tmp/valkey.sock".to_string(),
        }
    }
}

pub fn refresh_proc_title(cfg: &ServerConfig) {
    if !cfg.set_proc_title {
        return;
    }
    use crate::foundation::io::sys::proctitle;
    let title = proctitle::argv0().unwrap_or_default();
    let vars = cfg.proc_title_vars(&title);
    if let Some(t) = proctitle::expand_proc_title_template(&cfg.proc_title_template, &vars) {
        proctitle::set_proc_title(t.trim());
    }
}
