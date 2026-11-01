use clap::Parser;
use std::fs;
use std::path::Path;

use crate::engine::extension::config::server::ProtectedAction;
use crate::foundation::io::persistence::AofFsync;
use crate::framework::context::RuntimeConfig;

/// A resolved TCP listen target derived from the `bind` directive. `optional`
/// mirrors C valkey's leading-`-` marker (anet.c `listenToPort`): a failure to
/// bind an optional address (e.g. no IPv6 stack for `-::*`) is non-fatal.
#[derive(Debug, Clone)]
pub struct BindTarget {
    pub addr: String,
    pub optional: bool,
}

#[derive(Parser, Debug, Clone)]
#[command(
    name = "versal-server",
    about = "VersalKV server with Valkey compatibility"
)]
// C valkey's argv parser (server.c) conceptually appends every CLI directive
// after the config file with a "last one wins" rule, so passing the same
// option twice (e.g. `--dir ./ --dir ./`) is accepted. Clap otherwise rejects
// repeated single-value args with `ArgumentConflict`; `args_override_self`
// restores the last-wins behaviour without erroring.
#[command(args_override_self = true)]
pub struct Config {
    #[arg(long, default_value_t = 6379)]
    pub port: u16,

    #[arg(skip = 0u16)]
    pub tls_port: u16,
    #[arg(skip)]
    pub tls_cert_file: String,
    #[arg(skip)]
    pub tls_key_file: String,
    #[arg(skip)]
    pub tls_key_file_pass: crate::foundation::memory::tls::TlsPassword,
    #[arg(skip)]
    pub tls_alt_cert_file: String,
    #[arg(skip)]
    pub tls_alt_key_file: String,
    #[arg(skip)]
    pub tls_alt_key_file_pass: crate::foundation::memory::tls::TlsPassword,
    #[arg(skip)]
    pub tls_auto_reload_interval: i32,
    #[arg(skip)]
    pub tls_ca_cert_file: String,
    #[arg(skip)]
    pub tls_ca_cert_dir: String,
    #[arg(skip = "yes")]
    pub tls_auth_clients: String,
    #[arg(skip = "off")]
    pub tls_auth_clients_user: String,
    #[arg(skip)]
    pub tls_protocols: String,
    #[arg(skip)]
    pub tls_ciphers: String,
    #[arg(skip)]
    pub tls_ciphersuites: String,
    #[arg(skip)]
    pub tls_prefer_server_ciphers: bool,
    #[arg(skip = true)]
    pub tls_session_caching: bool,
    #[arg(skip = 20 * 1024)]
    pub tls_session_cache_size: i32,
    #[arg(skip)]
    pub tls_replication: bool,
    #[arg(skip)]
    pub tls_cluster: bool,
    #[arg(skip)]
    pub tls_client_cert_file: String,
    #[arg(skip)]
    pub tls_client_key_file: String,
    #[arg(skip)]
    pub tls_client_key_file_pass: crate::foundation::memory::tls::TlsPassword,

    // C valkey's compiled-in default is CONFIG_DEFAULT_BINDADDR {"*", "-::*"}
    // (server.h) — bind every IPv4 interface plus, optionally, every IPv6 one.
    // A config file without a `bind` directive (e.g. tests/assets/minimal.conf)
    // therefore listens on all interfaces; defaulting to loopback-only here made
    // non-loopback clients (get_nonloopback_client) get connection-refused.
    #[arg(long, default_value = "* -::*")]
    pub bind: String,

    #[arg(long, default_value = "notice")]
    pub loglevel: String,

    #[arg(long, default_value = "")]
    pub logfile: String,
    #[arg(long, default_value = "legacy")]
    pub log_format: String,
    #[arg(skip = "legacy")]
    pub log_timestamp_format: String,
    #[arg(skip)]
    pub debug_context: String,
    #[arg(skip = true)]
    pub crash_log_enabled: bool,
    #[arg(skip)]
    pub syslog_enabled: bool,
    #[arg(skip = "valkey")]
    pub syslog_ident: String,
    #[arg(skip = "local0")]
    pub syslog_facility: String,
    #[arg(long, default_value = "yes")]
    pub hide_user_data_from_log: String,
    #[arg(skip)]
    pub availability_zone: String,

    #[arg(skip = 0u64)]
    pub watchdog_period: u64,
    #[arg(skip = true)]
    pub crash_memcheck_enabled: bool,
    #[arg(skip = false)]
    pub use_exit_on_panic: bool,

    #[arg(long, default_value_t = false)]
    pub daemonize: bool,

    #[arg(skip = "no")]
    pub supervised: String,
    #[arg(skip)]
    pub supervised_active: bool,

    #[arg(skip)]
    pub server_cpulist: String,
    #[arg(skip)]
    pub bio_cpulist: String,
    #[arg(skip)]
    pub aof_rewrite_cpulist: String,
    #[arg(skip)]
    pub bgsave_cpulist: String,
    #[arg(skip)]
    pub ignore_warnings: String,

    #[arg(long, default_value_t = false)]
    pub sentinel: bool,

    #[arg(long, default_value_t = 16)]
    pub databases: u32,

    #[arg(default_value = "")]
    pub config_file: String,

    #[arg(skip)]
    pub requirepass: Option<Vec<u8>>,

    #[arg(long, default_value = "./")]
    pub dir: String,

    #[arg(long, default_value = "dump.rdb")]
    pub dbfilename: String,

    #[arg(long, num_args = 0.., default_missing_value = "", require_equals = false)]
    pub save: Option<Vec<String>>,

    #[arg(long, num_args = 0.., default_missing_value = "", require_equals = false)]
    pub shutdown_on_sigterm: Vec<String>,
    #[arg(long, num_args = 0.., default_missing_value = "", require_equals = false)]
    pub shutdown_on_sigint: Vec<String>,
    #[arg(long, default_value_t = 10)]
    pub shutdown_timeout: u64,

    #[arg(long, default_value_t = false)]
    pub appendonly: bool,

    #[arg(skip = true)]
    pub rdb_save_incremental_fsync: bool,

    #[arg(skip)]
    pub rdb_del_sync_files: bool,

    #[arg(skip = true)]
    pub aof_rewrite_incremental_fsync: bool,

    #[arg(long, default_value = "appendonly.aof")]
    pub appendfilename: String,

    #[arg(long, default_value = "appendonlydir")]
    pub appenddirname: String,

    #[arg(long, default_value = "everysec")]
    pub appendfsync: String,

    #[arg(skip)]
    pub no_appendfsync_on_rewrite: bool,

    #[arg(skip = true)]
    pub lazyfree_lazy_expire: bool,

    #[arg(skip = true)]
    pub lazyfree_lazy_eviction: bool,

    #[arg(skip = true)]
    pub lazyfree_lazy_server_del: bool,

    #[arg(skip = true)]
    pub lazyfree_lazy_user_del: bool,

    #[arg(skip = true)]
    pub lazyfree_lazy_user_flush: bool,

    #[arg(skip = 1u8)]
    pub active_expire_effort: u8,
    #[arg(skip)]
    pub activedefrag: bool,
    #[arg(skip = 1u8)]
    pub active_defrag_cycle_min: u8,
    #[arg(skip = 25u8)]
    pub active_defrag_cycle_max: u8,
    #[arg(skip = 10u16)]
    pub active_defrag_threshold_lower: u16,
    #[arg(skip = 100u16)]
    pub active_defrag_threshold_upper: u16,
    #[arg(skip = 500u64)]
    pub active_defrag_cycle_us: u64,
    #[arg(skip = 100usize << 20)]
    pub active_defrag_ignore_bytes: usize,
    #[arg(skip = 1000usize)]
    pub active_defrag_max_scan_fields: usize,

    #[arg(skip = "disabled")]
    pub repl_diskless_load: String,

    #[arg(skip = 60u64)]
    pub repl_timeout: u64,

    #[arg(skip = 0u16)]
    pub replica_announce_port: u16,

    #[arg(skip = true)]
    pub replica_ignore_maxmemory: bool,

    #[arg(skip)]
    pub replica_ignore_disk_write_errors: bool,

    #[arg(skip = true)]
    pub replica_lazy_flush: bool,

    #[arg(skip = false)]
    pub repl_disable_tcp_nodelay: bool,

    #[arg(skip)]
    pub repl_mptcp: bool,

    #[arg(skip = true)]
    pub aof_use_rdb_preamble: bool,
    #[arg(skip = true)]
    pub aof_load_truncated: bool,

    #[arg(skip = false)]
    pub aof_timestamp_enabled: bool,

    #[arg(skip = false)]
    pub aof_disable_auto_gc: bool,

    #[arg(skip = true)]
    pub repl_diskless_sync: bool,

    #[arg(skip = 5i64)]
    pub repl_diskless_sync_delay: i64,

    #[arg(long, default_value = "strict")]
    pub rdb_version_check: String,

    #[arg(skip)]
    pub rdb_key_save_delay: i64,

    #[arg(skip)]
    pub key_load_delay: i64,

    #[arg(skip = 2 * 1024 * 1024u64)]
    pub loading_process_events_interval_bytes: u64,

    #[arg(skip = 300u64)]
    pub tcp_keepalive: u64,

    #[arg(skip = 16usize)]
    pub prefetch_batch_max_size: usize,

    #[arg(skip = -2)]
    pub list_max_listpack_size: i64,

    #[arg(skip = 512)]
    pub set_max_intset_entries: i64,

    #[arg(skip = 128)]
    pub set_max_listpack_entries: i64,

    #[arg(skip = 64)]
    pub set_max_listpack_value: i64,

    #[arg(skip = 100)]
    pub stream_node_max_entries: i64,

    #[arg(skip = 4096)]
    pub stream_node_max_bytes: i64,

    #[arg(skip)]
    pub mptcp: bool,

    #[arg(skip)]
    pub pidfile: String,

    #[arg(long, default_value_t = 10000)]
    pub maxclients: u32,

    #[arg(skip = 10u16)]
    pub max_new_connections_per_cycle: u16,

    #[arg(skip = 1u16)]
    pub max_new_tls_connections_per_cycle: u16,

    #[arg(skip = 0u64)]
    pub client_timeout: u64,

    #[arg(skip = 511u32)]
    pub tcp_backlog: u32,

    #[arg(long, default_value = "yes")]
    pub enable_module_command: String,

    #[arg(long, default_value = "no")]
    pub enable_protected_configs: String,

    #[arg(long, default_value = "no")]
    pub enable_debug_command: String,

    #[arg(long, default_value = "yes")]
    pub protected_mode: String,

    #[arg(skip = 10000)]
    pub commandlog_slow_threshold: i64,

    #[arg(skip = 128)]
    pub commandlog_slow_max_len: i64,

    #[arg(skip = 1024 * 1024)]
    pub commandlog_large_request_threshold: i64,

    #[arg(skip = 128)]
    pub commandlog_large_request_max_len: i64,

    #[arg(skip = 1024 * 1024)]
    pub commandlog_large_reply_threshold: i64,

    #[arg(skip = 128)]
    pub commandlog_large_reply_max_len: i64,

    #[arg(long, default_value_t = false)]
    pub cluster_enabled: bool,

    #[arg(skip)]
    pub cluster_port: u16,

    #[arg(skip = 0u64)]
    pub cluster_ping_interval: u64,

    #[arg(skip = 15000u64)]
    pub cluster_node_timeout: u64,

    #[arg(skip = 60u64)]
    pub cluster_blacklist_ttl: u64,

    #[arg(skip = 1i64)]
    pub cluster_migration_barrier: i64,

    #[arg(skip = "nodes.conf")]
    pub cluster_config_file: String,

    #[arg(skip)]
    pub cluster_announce_ip: String,

    #[arg(skip)]
    pub cluster_announce_client_ipv4: String,

    #[arg(skip)]
    pub cluster_announce_client_ipv6: String,

    #[arg(skip)]
    pub cluster_announce_hostname: String,

    #[arg(skip)]
    pub cluster_announce_human_nodename: String,

    #[arg(skip = 0u16)]
    pub cluster_announce_port: u16,

    #[arg(skip = 0u16)]
    pub cluster_announce_tls_port: u16,

    #[arg(skip = 0u16)]
    pub cluster_announce_client_port: u16,

    #[arg(skip = 0u16)]
    pub cluster_announce_client_tls_port: u16,

    #[arg(skip = 0u16)]
    pub cluster_announce_bus_port: u16,

    #[arg(skip = "sync")]
    pub cluster_config_save_behavior: String,

    #[arg(skip = 10i64)]
    pub cluster_message_gossip_perc: i64,

    #[arg(skip)]
    pub cluster_slot_stats_enabled: bool,

    #[arg(skip = 1u16)]
    pub cluster_databases: u16,

    #[arg(skip = true)]
    pub cluster_allow_pubsubshard_when_down: bool,

    #[arg(skip = "resetchannels")]
    pub acl_pubsub_default: String,

    #[arg(skip)]
    pub aclfile: String,

    /// Config-file `replicaof <host> <port>` target. `Some("")` is distinct
    /// from `None` because C accepts an explicitly empty primary host.
    #[arg(skip)]
    pub replicaof_host: Option<String>,
    #[arg(skip)]
    pub replicaof_port: u16,

    #[arg(skip)]
    pub sentinel_directives: Vec<String>,

    #[arg(skip)]
    pub rename_commands: Vec<(String, String)>,

    #[arg(skip)]
    pub load_modules: Vec<(String, Vec<String>)>,

    #[arg(skip)]
    pub unixsocket: String,

    #[arg(skip)]
    pub unixsocketgroup: String,

    #[arg(skip)]
    pub unixsocketperm: u32,

    #[arg(skip = "{title} {listen-addr} {server-mode}")]
    pub proc_title_template: String,

    #[arg(skip = true)]
    pub set_proc_title: bool,

    #[arg(skip)]
    pub always_show_logo: bool,

    /// Unknown CLI `--flag value` directives that are not part of the clap
    /// `Config` struct. These arrive via a two-stage parse in
    /// `server::env` and are re-applied to `RuntimeConfig` through
    /// `ConfigRegistry` in `Server::apply_config`, matching C valkey's
    /// behaviour of accepting *any* configuration-file directive directly as
    /// a `--name value` CLI flag.
    #[arg(skip)]
    pub extra_cli_configs: Vec<(String, String)>,

    /// ACL declarations are installed after modules load, with roles first.
    #[arg(skip)]
    pub acl_declarations: Vec<String>,

    /// Config keys the user explicitly set on the command line (normalised,
    /// e.g. `shutdown-on-sigterm`, `dir`). C valkey loads the config file
    /// first and appends CLI options after it, so CLI always wins. Our clap
    /// parse runs *before* `load_from_file`, so the file would otherwise
    /// clobber CLI-provided values; `load_from_file` skips any directive whose
    /// key is in this set to restore the last-wins (CLI-wins) precedence.
    #[arg(skip)]
    pub cli_set_keys: Vec<String>,

    /// Config keys explicitly provided by config text, including keys whose
    /// value is empty. Some startup merges must distinguish an explicit clear
    /// from an unspecified built-in default.
    #[arg(skip)]
    pub explicit_config_keys: Vec<String>,
}

impl Config {
    pub(crate) fn is_explicitly_set(&self, key: &str) -> bool {
        self.cli_set_keys.iter().any(|configured| configured == key)
            || self
                .explicit_config_keys
                .iter()
                .any(|configured| configured == key)
    }

    pub(crate) fn cluster_bus_port(&self) -> Option<u16> {
        if self.cluster_port == 0 {
            self.port.checked_add(10_000)
        } else {
            Some(self.cluster_port)
        }
    }

    pub fn apply_to(&self, rconfig: &mut RuntimeConfig) {
        self.apply_security_config(rconfig);
        self.apply_persistence_config(rconfig);
        self.apply_encoding_config(rconfig);
        self.apply_replication_config(rconfig);
        self.apply_server_config(rconfig);
        self.apply_logging_config(rconfig);
        self.apply_cluster_config(rconfig);
    }

    fn apply_security_config(&self, rconfig: &mut RuntimeConfig) {
        rconfig.security.requirepass = self.requirepass.clone();
        rconfig.server.acl_pubsub_default = self.acl_pubsub_default.clone();
        rconfig.server.aclfile = self.aclfile.clone();
    }

    fn apply_persistence_config(&self, rconfig: &mut RuntimeConfig) {
        rconfig.persistence.dir = self.dir.clone();
        rconfig.persistence.dbfilename = self.dbfilename.clone();
        if let Some(values) = &self.save {
            rconfig.persistence.saveparams = values
                .as_chunks::<2>()
                .0
                .iter()
                .filter_map(|pair| {
                    Some(crate::engine::extension::config::persistence::SaveParam {
                        seconds: pair[0].parse().ok()?,
                        changes: pair[1].parse().ok()?,
                    })
                })
                .collect();
        }
        rconfig.persistence.rdb_version_check_relaxed =
            self.rdb_version_check.eq_ignore_ascii_case("relaxed");
        rconfig.persistence.rdb_save_incremental_fsync = self.rdb_save_incremental_fsync;
        rconfig.persistence.rdb_del_sync_files = self.rdb_del_sync_files;
        rconfig.persistence.aof_rewrite_incremental_fsync = self.aof_rewrite_incremental_fsync;
        rconfig.persistence.rdb_key_save_delay = self.rdb_key_save_delay;
        rconfig.persistence.key_load_delay = self.key_load_delay;
        rconfig.persistence.loading_process_events_interval_bytes =
            self.loading_process_events_interval_bytes;
        rconfig.persistence.aof_enabled = self.appendonly;
        rconfig.persistence.aof_filename = self.appendfilename.clone();
        rconfig.persistence.aof_appenddirname = self.appenddirname.clone();
        rconfig.persistence.aof_fsync = AofFsync::from_str(&self.appendfsync);
        rconfig.persistence.aof_no_fsync_on_rewrite = self.no_appendfsync_on_rewrite;
        rconfig.persistence.aof_use_rdb_preamble = self.aof_use_rdb_preamble;
        rconfig.persistence.aof_load_truncated = self.aof_load_truncated;
        rconfig.persistence.aof_timestamp_enabled = self.aof_timestamp_enabled;
        rconfig.persistence.aof_disable_auto_gc = self.aof_disable_auto_gc;
    }

    fn apply_encoding_config(&self, rconfig: &mut RuntimeConfig) {
        rconfig.encoding.list_max_listpack_size = self.list_max_listpack_size;
        rconfig.encoding.set_max_intset_entries = self.set_max_intset_entries;
        rconfig.encoding.set_max_listpack_entries = self.set_max_listpack_entries;
        rconfig.encoding.set_max_listpack_value = self.set_max_listpack_value;
        rconfig.encoding.stream_node_max_entries = self.stream_node_max_entries;
        rconfig.encoding.stream_node_max_bytes = self.stream_node_max_bytes;
    }

    fn apply_replication_config(&self, rconfig: &mut RuntimeConfig) {
        // Startup replication must be installed before RDB AUX restoration.
        rconfig
            .replication
            .master_host
            .clone_from(&self.replicaof_host);
        if self.replicaof_host.is_some() {
            rconfig.replication.master_port = self.replicaof_port;
            rconfig.replication.master_link_status = "down".to_string();
        } else {
            rconfig.replication.master_port = 0;
        }
        rconfig.replication.repl_diskless_load = self.repl_diskless_load.clone();
        rconfig.replication.repl_timeout = self.repl_timeout;
        rconfig.replication.replica_announce_port = self.replica_announce_port;
        rconfig.replication.replica_ignore_maxmemory = self.replica_ignore_maxmemory;
        rconfig.replication.replica_ignore_disk_write_errors =
            self.replica_ignore_disk_write_errors;
        rconfig.replication.replica_lazy_flush = self.replica_lazy_flush;
        rconfig.replication.repl_disable_tcp_nodelay = self.repl_disable_tcp_nodelay;
        rconfig.replication.repl_mptcp = self.repl_mptcp;
        rconfig.replication.repl_diskless_sync = self.repl_diskless_sync;
        rconfig.replication.repl_diskless_sync_delay = self.repl_diskless_sync_delay;
        rconfig.replication.repl_diskless_sync_delay_explicit =
            self.is_explicitly_set("repl-diskless-sync-delay");
    }

    fn apply_server_config(&self, rconfig: &mut RuntimeConfig) {
        rconfig.server.shutdown_on_sigterm =
            parse_shutdown_flags("shutdown-on-sigterm", &self.shutdown_on_sigterm);
        rconfig.server.shutdown_on_sigint =
            parse_shutdown_flags("shutdown-on-sigint", &self.shutdown_on_sigint);
        rconfig.server.shutdown_timeout = self.shutdown_timeout;
        rconfig.server.listening_port = self.port;
        rconfig.server.tls_port = self.tls_port;
        rconfig.server.tls_cert_file = self.tls_cert_file.clone();
        rconfig.server.tls_key_file = self.tls_key_file.clone();
        rconfig.server.tls_key_file_pass = self.tls_key_file_pass.clone();
        rconfig.server.tls_alt_cert_file = self.tls_alt_cert_file.clone();
        rconfig.server.tls_alt_key_file = self.tls_alt_key_file.clone();
        rconfig.server.tls_alt_key_file_pass = self.tls_alt_key_file_pass.clone();
        rconfig.server.tls_auto_reload_interval = self.tls_auto_reload_interval;
        rconfig.server.tls_ca_cert_file = self.tls_ca_cert_file.clone();
        rconfig.server.tls_ca_cert_dir = self.tls_ca_cert_dir.clone();
        rconfig.server.tls_auth_clients = self.tls_auth_clients.clone();
        rconfig.server.tls_auth_clients_user = self.tls_auth_clients_user.clone();
        rconfig.server.tls_protocols = self.tls_protocols.clone();
        rconfig.server.tls_ciphers = self.tls_ciphers.clone();
        rconfig.server.tls_ciphersuites = self.tls_ciphersuites.clone();
        rconfig.server.tls_prefer_server_ciphers = self.tls_prefer_server_ciphers;
        rconfig.server.tls_session_caching = self.tls_session_caching;
        rconfig.server.tls_session_cache_size = self.tls_session_cache_size;
        rconfig.server.tls_replication = self.tls_replication;
        rconfig.server.tls_cluster = self.tls_cluster;
        rconfig.server.tls_client_cert_file = self.tls_client_cert_file.clone();
        rconfig.server.tls_client_key_file = self.tls_client_key_file.clone();
        rconfig.server.tls_client_key_file_pass = self.tls_client_key_file_pass.clone();
        rconfig.server.daemonize = self.daemonize;
        rconfig.server.supervised = self.supervised.clone();
        rconfig.server.supervised_active = self.supervised_active;
        rconfig.server.server_cpulist = self.server_cpulist.clone();
        rconfig.server.bio_cpulist = self.bio_cpulist.clone();
        rconfig.server.aof_rewrite_cpulist = self.aof_rewrite_cpulist.clone();
        rconfig.server.bgsave_cpulist = self.bgsave_cpulist.clone();
        rconfig.server.ignore_warnings = self.ignore_warnings.clone();
        rconfig.server.sentinel_mode = self.sentinel;
        rconfig.server.databases = self.databases as i64;
        rconfig.server.mptcp = self.mptcp;
        rconfig.server.pidfile = self.pidfile.clone();
        rconfig.server.tcp_keepalive = self.tcp_keepalive;
        rconfig.server.prefetch_batch_max_size = self.prefetch_batch_max_size;
        rconfig.server.lazyfree_lazy_eviction = self.lazyfree_lazy_eviction;
        rconfig.server.lazyfree_lazy_expire = self.lazyfree_lazy_expire;
        rconfig.server.lazyfree_lazy_server_del = self.lazyfree_lazy_server_del;
        rconfig.server.lazyfree_lazy_user_del = self.lazyfree_lazy_user_del;
        rconfig.server.lazyfree_lazy_user_flush = self.lazyfree_lazy_user_flush;
        rconfig.server.active_expire_effort = self.active_expire_effort;
        rconfig.server.activedefrag = self.activedefrag;
        rconfig.server.active_defrag_cycle_min = self.active_defrag_cycle_min;
        rconfig.server.active_defrag_cycle_max = self.active_defrag_cycle_max;
        rconfig.server.active_defrag_threshold_lower = self.active_defrag_threshold_lower;
        rconfig.server.active_defrag_threshold_upper = self.active_defrag_threshold_upper;
        rconfig.server.active_defrag_cycle_us = self.active_defrag_cycle_us;
        rconfig.server.active_defrag_ignore_bytes = self.active_defrag_ignore_bytes;
        rconfig.server.active_defrag_max_scan_fields = self.active_defrag_max_scan_fields;
        rconfig.server.maxclients = self.maxclients;
        rconfig.server.max_new_connections_per_cycle = self.max_new_connections_per_cycle;
        rconfig.server.max_new_tls_connections_per_cycle = self.max_new_tls_connections_per_cycle;
        rconfig.server.client_timeout = self.client_timeout;
        rconfig.server.tcp_backlog = self.tcp_backlog;
        rconfig.server.set_bind(self.bind.clone());
        self.apply_server_protection_config(rconfig);
    }

    fn apply_server_protection_config(&self, rconfig: &mut RuntimeConfig) {
        rconfig.server.enable_module_command =
            match self.enable_module_command.to_ascii_lowercase().as_str() {
                "local" => ProtectedAction::Local,
                "no" | "false" | "0" => ProtectedAction::No,
                _ => ProtectedAction::Yes,
            };
        rconfig.server.enable_protected_configs =
            ProtectedAction::parse(&self.enable_protected_configs);
        rconfig.server.enable_debug_command = ProtectedAction::parse(&self.enable_debug_command);
        rconfig.server.protected_mode = matches!(
            self.protected_mode.to_ascii_lowercase().as_str(),
            "yes" | "true" | "1"
        );
        if !self.config_file.is_empty() {
            rconfig.server.config_file = Some(self.config_file.clone());
        }
        rconfig.server.unixsocket = self.unixsocket.clone();
        rconfig.server.unixsocketgroup = self.unixsocketgroup.clone();
        rconfig.server.unixsocketperm = self.unixsocketperm;
        rconfig.server.proc_title_template = self.proc_title_template.clone();
        rconfig.server.set_proc_title = self.set_proc_title;
        rconfig.server.always_show_logo = self.always_show_logo;
        rconfig
            .server
            .rename_commands
            .clone_from(&self.rename_commands);
    }

    fn apply_logging_config(&self, rconfig: &mut RuntimeConfig) {
        rconfig.server.loglevel = self.loglevel.clone();
        rconfig.server.log_format = self.log_format.clone();
        rconfig.server.log_timestamp_format = self.log_timestamp_format.clone();
        rconfig.server.debug_context = self.debug_context.clone();
        rconfig.server.crash_log_enabled = self.crash_log_enabled;
        rconfig.server.syslog_enabled = self.syslog_enabled;
        rconfig.server.syslog_ident = self.syslog_ident.clone();
        rconfig.server.syslog_facility = self.syslog_facility.clone();
        rconfig.server.hide_user_data_from_log = matches!(
            self.hide_user_data_from_log.to_ascii_lowercase().as_str(),
            "yes" | "true" | "1"
        );
        rconfig.server.availability_zone = self.availability_zone.clone();
        rconfig.server.watchdog_period = self.watchdog_period;
        rconfig.server.crash_memcheck_enabled = self.crash_memcheck_enabled;
        rconfig.server.use_exit_on_panic = self.use_exit_on_panic;
        rconfig.server.commandlog_slow_threshold = self.commandlog_slow_threshold;
        rconfig.server.commandlog_slow_max_len = self.commandlog_slow_max_len;
        rconfig.server.slowlog_log_slower_than = self.commandlog_slow_threshold;
        rconfig.server.slowlog_max_len = self.commandlog_slow_max_len;
        rconfig.server.large_request_threshold = self.commandlog_large_request_threshold;
        rconfig.server.large_request_max_len = self.commandlog_large_request_max_len;
        rconfig.server.large_reply_threshold = self.commandlog_large_reply_threshold;
        rconfig.server.large_reply_max_len = self.commandlog_large_reply_max_len;
    }

    fn apply_cluster_config(&self, rconfig: &mut RuntimeConfig) {
        rconfig.server.cluster_blacklist_ttl = self.cluster_blacklist_ttl;
        if !self.cluster_enabled {
            return;
        }
        rconfig.server.cluster_enabled = true;
        // Startup rejects an invalid derived port; mapping must not panic
        // before that validation can produce the normal configuration error.
        rconfig.server.cluster_port = self.cluster_bus_port().unwrap_or(0);
        rconfig.server.cluster_ping_interval = self.cluster_ping_interval;
        rconfig.server.cluster_node_timeout = self.cluster_node_timeout;
        rconfig.server.cluster_migration_barrier = self.cluster_migration_barrier;
        rconfig.server.cluster_config_file = self.cluster_config_file.clone();
        rconfig.server.cluster_announce_ip = self.cluster_announce_ip.clone();
        rconfig.server.cluster_announce_client_ipv4 = self.cluster_announce_client_ipv4.clone();
        rconfig.server.cluster_announce_client_ipv6 = self.cluster_announce_client_ipv6.clone();
        rconfig.server.cluster_announce_hostname = self.cluster_announce_hostname.clone();
        rconfig.server.cluster_announce_human_nodename =
            self.cluster_announce_human_nodename.clone();
        rconfig.server.cluster_announce_port = self.cluster_announce_port;
        rconfig.server.cluster_announce_tls_port = self.cluster_announce_tls_port;
        rconfig.server.cluster_announce_client_port = self.cluster_announce_client_port;
        rconfig.server.cluster_announce_client_tls_port = self.cluster_announce_client_tls_port;
        rconfig.server.cluster_announce_bus_port = self.cluster_announce_bus_port;
        rconfig.server.cluster_config_save_behavior = self.cluster_config_save_behavior.clone();
        rconfig.server.cluster_message_gossip_perc = self.cluster_message_gossip_perc;
        rconfig.server.cluster_slot_stats_enabled = self.cluster_slot_stats_enabled;
        rconfig.server.cluster_databases = self.cluster_databases;
        rconfig.server.cluster_allow_pubsubshard_when_down =
            self.cluster_allow_pubsubshard_when_down;
    }

    pub fn log_level(&self) -> crate::foundation::io::sys::log::LogLevel {
        match self.loglevel.as_str() {
            "debug" => crate::foundation::io::sys::log::LogLevel::Debug,
            "verbose" => crate::foundation::io::sys::log::LogLevel::Verbose,
            "warning" | "warn" => crate::foundation::io::sys::log::LogLevel::Warning,
            _ => crate::foundation::io::sys::log::LogLevel::Notice,
        }
    }

    /// Return the concrete listen targets with their optional flag, so the
    /// bind loop can tolerate `EADDRNOTAVAIL`/unsupported-family errors on
    /// optional (`-`-prefixed) addresses — matching C valkey's default
    /// `* -::*`, where the IPv6 wildcard bind is best-effort.
    /// C valkey's wildcard tokens are translated to concrete listen addresses:
    /// `*` → `0.0.0.0`, `::*` → `::` (anet.c maps these to `NULL`/`AI_PASSIVE`).
    /// IPv6 addresses are wrapped in brackets (`[::1]:6379`).
    pub fn bind_targets(&self) -> Vec<BindTarget> {
        bind_targets(&self.bind, self.port)
    }

    pub fn default_advertised_ip(&self) -> String {
        let first = self
            .bind
            .split_whitespace()
            .next()
            .unwrap_or("127.0.0.1")
            .trim_start_matches('-');
        match first {
            "*" | "0.0.0.0" | "::*" | "::" => "127.0.0.1".to_string(),
            address => address.to_string(),
        }
    }

    /// Sentinel mode: if port is still the default 6379, set it to 26379.
    pub fn load_from_file(&mut self, path: &str) -> std::io::Result<()> {
        if path.is_empty() {
            return Ok(());
        }
        let p = Path::new(path);
        if !p.exists() {
            return Ok(());
        }
        let contents = fs::read_to_string(p)?;
        super::parse::validate_config_file_directives(&contents)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        // C valkey applies the config file first and appends CLI options after
        // it (last wins → CLI wins). Our clap parse already ran, so skip any
        // directive the user set on the command line to avoid clobbering it.
        self.parse_config_string_filtered(&contents, true);
        Ok(())
    }
}

fn parse_shutdown_flags(signal_name: &str, values: &[String]) -> u32 {
    let configured = values.join(" ");
    crate::engine::distributed::shutdown::parse_sig_flags(&configured).unwrap_or_else(|| {
        log::warn!(
            "invalid {} value {:?}, falling back to default",
            signal_name,
            configured
        );
        crate::engine::distributed::shutdown::SHUTDOWN_NOFLAGS
    })
}

pub(crate) fn bind_targets(bind: &str, port: u16) -> Vec<BindTarget> {
    bind.split_whitespace()
        .map(|token| {
            let optional = token.starts_with('-');
            let raw = token.strip_prefix('-').unwrap_or(token);
            let ip = match raw {
                "*" => "0.0.0.0",
                "::*" => "::",
                other => other,
            };
            let addr = if ip.contains(':') {
                format!("[{ip}]:{port}")
            } else {
                format!("{ip}:{port}")
            };
            BindTarget { addr, optional }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn default_config() -> Config {
        Config::parse_from(["versal-server"])
    }

    #[test]
    fn derived_cluster_port_rejects_overflow_and_preserves_explicit_port() {
        let mut config = default_config();
        config.cluster_enabled = true;
        config.port = 55_535;
        assert_eq!(config.cluster_bus_port(), Some(65_535));
        config.port = 55_536;
        assert_eq!(config.cluster_bus_port(), None);
        let mut runtime = RuntimeConfig::new();
        config.apply_to(&mut runtime);
        assert_eq!(runtime.server.cluster_port, 0);
        config.cluster_port = 16_379;
        assert_eq!(config.cluster_bus_port(), Some(16_379));
        config.apply_to(&mut runtime);
        assert_eq!(runtime.server.cluster_port, 16_379);
    }

    #[test]
    fn defaults_map_to_runtime_config() {
        let cfg = default_config();
        let mut runtime = RuntimeConfig::new();

        cfg.apply_to(&mut runtime);

        assert_eq!(runtime.server.listening_port, 6379);
        assert_eq!(runtime.persistence.dir, "./");
        assert_eq!(runtime.persistence.dbfilename, "dump.rdb");
        assert_eq!(runtime.persistence.aof_filename, "appendonly.aof");
        assert_eq!(runtime.persistence.aof_appenddirname, "appendonlydir");
        assert_eq!(runtime.persistence.aof_fsync, AofFsync::EverySec);
        assert!(runtime.persistence.aof_use_rdb_preamble);
        assert_eq!(runtime.server.maxclients, 10000);
        assert!(runtime.server.enable_module_command == ProtectedAction::Yes);
        assert!(!runtime.server.cluster_enabled);
    }

    #[test]
    fn default_bind_targets_expand_wildcards_and_mark_optional_ipv6() {
        // The compiled default `* -::*` must expand to an IPv4 wildcard listen
        // plus an optional IPv6 wildcard, matching C's CONFIG_DEFAULT_BINDADDR.
        let cfg = default_config();
        let targets = cfg.bind_targets();
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].addr, "0.0.0.0:6379");
        assert!(!targets[0].optional);
        assert_eq!(targets[1].addr, "[::]:6379");
        assert!(targets[1].optional);
    }

    #[test]
    fn default_advertised_ip_normalizes_wildcards() {
        let mut cfg = default_config();
        cfg.bind = "127.0.0.1 ::1".to_string();
        assert_eq!(cfg.default_advertised_ip(), "127.0.0.1");

        cfg.bind = "* -::*".to_string();
        assert_eq!(cfg.default_advertised_ip(), "127.0.0.1");

        cfg.bind = "0.0.0.0".to_string();
        assert_eq!(cfg.default_advertised_ip(), "127.0.0.1");

        cfg.bind = "::".to_string();
        assert_eq!(cfg.default_advertised_ip(), "127.0.0.1");

        cfg.bind = "127.0.0.2 ::1".to_string();
        assert_eq!(cfg.default_advertised_ip(), "127.0.0.2");
    }

    #[test]
    fn cli_arguments_override_defaults_and_bind_ipv6_is_bracketed() {
        let cfg = Config::parse_from([
            "versal-server",
            "--port",
            "6380",
            "--bind",
            "127.0.0.1 ::1",
            "--loglevel",
            "debug",
            "--databases",
            "4",
            "--appendonly",
            "--appendfsync",
            "always",
            "--maxclients",
            "42",
            "--enable-module-command",
            "no",
            "--cluster-enabled",
        ]);
        let mut runtime = RuntimeConfig::new();

        cfg.apply_to(&mut runtime);

        assert_eq!(
            cfg.bind_targets()
                .iter()
                .map(|t| t.addr.clone())
                .collect::<Vec<_>>(),
            vec!["127.0.0.1:6380", "[::1]:6380"]
        );
        assert_eq!(
            cfg.log_level(),
            crate::foundation::io::sys::log::LogLevel::Debug
        );
        assert_eq!(runtime.server.listening_port, 6380);
        assert_eq!(runtime.persistence.aof_fsync, AofFsync::Always);
        assert_eq!(runtime.server.maxclients, 42);
        assert!(runtime.server.enable_module_command == ProtectedAction::No);
        assert!(runtime.server.cluster_enabled);
        assert_eq!(runtime.server.cluster_port, 16380);
    }

    #[test]
    fn log_level_accepts_verbose_warning_warn_and_defaults_to_notice() {
        let mut cfg = default_config();

        cfg.loglevel = "verbose".to_string();
        assert_eq!(
            cfg.log_level(),
            crate::foundation::io::sys::log::LogLevel::Verbose
        );
        cfg.loglevel = "warning".to_string();
        assert_eq!(
            cfg.log_level(),
            crate::foundation::io::sys::log::LogLevel::Warning
        );
        cfg.loglevel = "warn".to_string();
        assert_eq!(
            cfg.log_level(),
            crate::foundation::io::sys::log::LogLevel::Warning
        );
        cfg.loglevel = "anything".to_string();
        assert_eq!(
            cfg.log_level(),
            crate::foundation::io::sys::log::LogLevel::Notice
        );
    }

    #[test]
    fn parse_config_string_handles_quotes_empty_values_aliases_and_invalid_numbers() {
        let mut cfg = default_config();
        cfg.parse_config_string(
            r#"
            # comments and blanks are skipped
            port 7000
            bind "127.0.0.1 ::1"
            loglevel 'warning'
            logfile server.log
            databases 32
            daemonize yes
            requirepass 'secret'
            dir /tmp/valkey
            dbfilename custom.rdb
            save 60 1000
            save 300 10
            shutdown-on-sigterm now
            shutdown-on-sigint no
            appendonly yes
            appendfilename append.aof
            appenddirname aofdir
            appendfsync no
            no-appendfsync-on-rewrite yes
            aof-use-rdb-preamble no
            rdb-version-check relaxed
            list-max-ziplist-size -4
            set-max-intset-entries 16
            set-max-listpack-entries 8
            set-max-listpack-value 32
            stream-node-max-entries 10
            mptcp true
            maxclients 0
            maxclients 99
            enable-module-command false
            commandlog-execution-slower-than 10
            commandlog-slow-execution-max-len 11
            commandlog-request-larger-than 12
            commandlog-large-request-max-len 13
            commandlog-reply-larger-than 14
            commandlog-large-reply-max-len 15
            sentinel monitor mymaster 127.0.0.1 6379 2
            cluster-enabled yes
            cluster-port 17000
            cluster-node-timeout 1234
            cluster-config-file cluster.conf
            cluster-announce-ip 10.0.0.1
            cluster-announce-hostname node.local
            cluster-announce-human-nodename node-a
            cluster-announce-port 7001
            cluster-announce-bus-port 17001
            cluster-slot-stats-enabled yes
            cluster-databases 3
            unknown ignored
            "#,
        );

        assert_eq!(cfg.port, 7000);
        assert_eq!(cfg.bind, "127.0.0.1 ::1");
        assert_eq!(cfg.loglevel, "warning");
        assert_eq!(cfg.logfile, "server.log");
        assert_eq!(cfg.databases, 32);
        assert!(cfg.daemonize);
        assert_eq!(cfg.requirepass.as_deref(), Some(b"secret".as_slice()));
        assert_eq!(cfg.dir, "/tmp/valkey");
        assert_eq!(cfg.dbfilename, "custom.rdb");
        assert_eq!(
            cfg.save.as_ref().unwrap(),
            &vec![
                "300".to_string(),
                "10".to_string(),
                "60".to_string(),
                "1000".to_string()
            ]
        );
        assert_eq!(cfg.shutdown_on_sigterm, vec!["now".to_string()]);
        assert_eq!(cfg.shutdown_on_sigint, vec!["no".to_string()]);
        assert!(cfg.appendonly);
        assert_eq!(cfg.appendfilename, "append.aof");
        assert_eq!(cfg.appenddirname, "aofdir");
        assert_eq!(cfg.appendfsync, "no");
        assert!(cfg.no_appendfsync_on_rewrite);
        assert!(!cfg.aof_use_rdb_preamble);
        assert_eq!(cfg.rdb_version_check, "relaxed");
        assert_eq!(cfg.list_max_listpack_size, -4);
        assert_eq!(cfg.set_max_intset_entries, 16);
        assert_eq!(cfg.set_max_listpack_entries, 8);
        assert_eq!(cfg.set_max_listpack_value, 32);
        assert_eq!(cfg.stream_node_max_entries, 10);
        assert!(cfg.mptcp);
        assert_eq!(cfg.maxclients, 99);
        assert_eq!(cfg.enable_module_command, "false");
        assert_eq!(cfg.commandlog_slow_threshold, 10);
        assert_eq!(cfg.commandlog_slow_max_len, 11);
        assert_eq!(cfg.commandlog_large_request_threshold, 12);
        assert_eq!(cfg.commandlog_large_request_max_len, 13);
        assert_eq!(cfg.commandlog_large_reply_threshold, 14);
        assert_eq!(cfg.commandlog_large_reply_max_len, 15);
        assert_eq!(
            cfg.sentinel_directives,
            vec!["monitor mymaster 127.0.0.1 6379 2".to_string()]
        );
        assert!(cfg.cluster_enabled);
        assert_eq!(cfg.cluster_port, 17000);
        assert_eq!(cfg.cluster_node_timeout, 1234);
        assert_eq!(cfg.cluster_config_file, "cluster.conf");
        assert_eq!(cfg.cluster_announce_ip, "10.0.0.1");
        assert_eq!(cfg.cluster_announce_hostname, "node.local");
        assert_eq!(cfg.cluster_announce_human_nodename, "node-a");
        assert_eq!(cfg.cluster_announce_port, 7001);
        assert_eq!(cfg.cluster_announce_bus_port, 17001);
        assert!(cfg.cluster_slot_stats_enabled);
        assert_eq!(cfg.cluster_databases, 3);
    }

    #[test]
    fn empty_requirepass_and_save_clear_previous_values() {
        let mut cfg = default_config();
        cfg.requirepass = Some(b"secret".to_vec());
        cfg.save = Some(vec!["60".to_string(), "1".to_string()]);

        cfg.parse_config_string(
            r#"
            requirepass ""
            save
            "#,
        );

        assert!(cfg.requirepass.is_none());
        assert_eq!(cfg.save, Some(Vec::new()));
    }

    #[test]
    fn table_driven_setters_cover_str_bool_and_parsed_classes() {
        let mut cfg = default_config();
        cfg.parse_config_string(
            "dir /x\nport notanumber\nport 1234\ndaemonize TRUE\nappendfsync ALWAYS\nunknown-key val\n",
        );
        assert_eq!(cfg.dir, "/x"); // Str
        assert_eq!(cfg.port, 1234); // ParsedU16: bad line ignored, good line applied
        assert!(cfg.daemonize); // Bool: "TRUE" (case-insensitive) → true
        assert_eq!(cfg.appendfsync, "always"); // Lower
    }

    #[test]
    fn apply_to_transfers_cluster_and_commandlog_options() {
        let mut cfg = default_config();
        cfg.config_file = "valkey.conf".to_string();
        cfg.requirepass = Some(b"pw".to_vec());
        cfg.port = 7000;
        cfg.sentinel = true;
        cfg.mptcp = true;
        cfg.cluster_enabled = true;
        cfg.cluster_port = 17000;
        cfg.cluster_node_timeout = 2222;
        cfg.cluster_config_file = "nodes-test.conf".to_string();
        cfg.cluster_announce_ip = "10.0.0.2".to_string();
        cfg.cluster_announce_hostname = "host".to_string();
        cfg.cluster_announce_human_nodename = "human".to_string();
        cfg.cluster_announce_port = 7001;
        cfg.cluster_announce_bus_port = 17001;
        cfg.cluster_slot_stats_enabled = true;
        cfg.cluster_databases = 2;
        cfg.commandlog_slow_threshold = 1;
        cfg.commandlog_slow_max_len = 2;
        cfg.commandlog_large_request_threshold = 3;
        cfg.commandlog_large_request_max_len = 4;
        cfg.commandlog_large_reply_threshold = 5;
        cfg.commandlog_large_reply_max_len = 6;
        cfg.stream_node_max_entries = 10;
        let mut runtime = RuntimeConfig::new();

        cfg.apply_to(&mut runtime);

        assert_eq!(runtime.security.requirepass, Some(b"pw".to_vec()));
        assert_eq!(runtime.server.config_file.as_deref(), Some("valkey.conf"));
        assert_eq!(runtime.server.listening_port, 7000);
        assert_eq!(runtime.encoding.stream_node_max_entries, 10);
        assert!(runtime.server.sentinel_mode);
        assert!(runtime.server.mptcp);
        assert!(runtime.server.cluster_enabled);
        assert_eq!(runtime.server.cluster_port, 17000);
        assert_eq!(runtime.server.cluster_node_timeout, 2222);
        assert_eq!(runtime.server.cluster_config_file, "nodes-test.conf");
        assert_eq!(runtime.server.cluster_announce_ip, "10.0.0.2");
        assert_eq!(runtime.server.cluster_announce_hostname, "host");
        assert_eq!(runtime.server.cluster_announce_human_nodename, "human");
        assert_eq!(runtime.server.cluster_announce_port, 7001);
        assert_eq!(runtime.server.cluster_announce_bus_port, 17001);
        assert!(runtime.server.cluster_slot_stats_enabled);
        assert_eq!(runtime.server.cluster_databases, 2);
        assert_eq!(runtime.server.commandlog_slow_threshold, 1);
        assert_eq!(runtime.server.commandlog_slow_max_len, 2);
        assert_eq!(runtime.server.large_request_threshold, 3);
        assert_eq!(runtime.server.large_request_max_len, 4);
        assert_eq!(runtime.server.large_reply_threshold, 5);
        assert_eq!(runtime.server.large_reply_max_len, 6);
    }

    #[test]
    fn load_from_file_ignores_empty_and_missing_paths_and_reads_existing_file() {
        let mut cfg = default_config();
        cfg.load_from_file("").unwrap();
        cfg.load_from_file("/tmp/valkey-rust-config-missing-do-not-create")
            .unwrap();

        let path = std::env::temp_dir().join(format!(
            "valkey-rust-config-{}-{}.conf",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, "port 7111\nappendonly yes\n").unwrap();
        cfg.load_from_file(path.to_str().unwrap()).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(cfg.port, 7111);
        assert!(cfg.appendonly);
    }
}
