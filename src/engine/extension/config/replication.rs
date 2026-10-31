#[derive(Clone)]
pub struct ReplicationConfig {
    pub master_host: Option<String>,
    pub master_port: u16,
    pub master_link_status: String,
    pub repl_timeout: u64,
    pub repl_ping_replica_period: u64,
    pub replica_priority: i64,
    pub replica_announce_ip: String,
    /// C `replica-announce-port`; zero advertises the TCP listening port.
    pub replica_announce_port: u16,
    pub replica_announced: bool,
    pub dual_channel_replication: bool,
    pub repl_compression: String,
    pub replica_read_only: bool,
    /// C `replica-ignore-maxmemory`, enabled by default to preserve primary parity.
    pub replica_ignore_maxmemory: bool,
    /// C `replica-ignore-disk-write-errors`, disabled by default so replica
    /// AOF durability failures remain fatal.
    pub replica_ignore_disk_write_errors: bool,
    /// C `replica-lazy-flush`, enabled by default for full-sync replacement.
    pub replica_lazy_flush: bool,
    pub repl_disable_tcp_nodelay: bool,
    pub repl_throttling_enabled: bool,
    /// C `repl-mptcp`, immutable because an existing upstream socket cannot
    /// change protocol in place.
    pub repl_mptcp: bool,
    pub repl_diskless_load: String,
    pub repl_diskless_sync_delay: i64,
    pub repl_diskless_sync_delay_explicit: bool,
    pub repl_diskless_sync_max_replicas: i64,
    pub repl_backlog_ttl: i64,
    pub min_replicas_to_write: i64,
    pub min_replicas_max_lag: i64,
    pub propagation_error_behavior: String,
    pub replica_serve_stale_data: bool,
    pub repl_diskless_sync: bool,
    pub replica_output_buffer_hard_limit: usize,
    pub replica_output_buffer_soft_limit: usize,
    pub replica_output_buffer_soft_limit_seconds: u64,
    pub normal_output_buffer_hard_limit: usize,
    pub normal_output_buffer_soft_limit: usize,
    pub normal_output_buffer_soft_limit_seconds: u64,
    pub pubsub_output_buffer_hard_limit: usize,
    pub pubsub_output_buffer_soft_limit: usize,
    pub pubsub_output_buffer_soft_limit_seconds: u64,
}

impl ReplicationConfig {
    pub fn new() -> Self {
        Self {
            master_host: None,
            master_port: 0,
            master_link_status: "down".to_string(),
            repl_timeout: 60,
            repl_ping_replica_period: 10,
            replica_priority: 100,
            replica_announce_ip: String::new(),
            replica_announce_port: 0,
            replica_announced: true,
            dual_channel_replication: false,
            repl_compression: "no".to_string(),
            replica_read_only: true,
            replica_ignore_maxmemory: true,
            replica_ignore_disk_write_errors: false,
            replica_lazy_flush: true,
            repl_disable_tcp_nodelay: false,
            repl_throttling_enabled: false,
            repl_mptcp: false,
            repl_diskless_load: "disabled".to_string(),
            repl_diskless_sync_delay: 5,
            repl_diskless_sync_delay_explicit: false,
            repl_diskless_sync_max_replicas: 0,
            repl_backlog_ttl: 0,
            min_replicas_to_write: 0,
            min_replicas_max_lag: 10,
            propagation_error_behavior: "ignore".to_string(),
            replica_serve_stale_data: true,
            repl_diskless_sync: false,
            replica_output_buffer_hard_limit: 268_435_456,
            replica_output_buffer_soft_limit: 67_108_864,
            replica_output_buffer_soft_limit_seconds: 60,
            normal_output_buffer_hard_limit: 0,
            normal_output_buffer_soft_limit: 0,
            normal_output_buffer_soft_limit_seconds: 0,
            pubsub_output_buffer_hard_limit: 33_554_432,
            pubsub_output_buffer_soft_limit: 8_388_608,
            pubsub_output_buffer_soft_limit_seconds: 60,
        }
    }

    pub fn effective_replica_announce_port(&self, listening_port: u16) -> u16 {
        if self.replica_announce_port == 0 {
            listening_port
        } else {
            self.replica_announce_port
        }
    }

    /// C `afterErrorReply` only turns propagated command errors into a panic
    /// for read-only replicas, or for AOF replay under the stronger `panic`
    /// policy. A writable replica and `panic-on-replicas` AOF replay continue
    /// after logging the error.
    pub(crate) fn should_panic_on_propagation_error(&self, is_aof_replay: bool) -> bool {
        if is_aof_replay {
            return self.propagation_error_behavior == "panic";
        }
        match self.propagation_error_behavior.as_str() {
            "panic" | "panic-on-replicas" => self.replica_read_only,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ReplicationConfig;

    #[test]
    fn propagation_error_policy_matches_valkey_replica_and_aof_rules() {
        let mut config = ReplicationConfig::new();
        assert!(!config.should_panic_on_propagation_error(false));

        config.propagation_error_behavior = "panic-on-replicas".to_string();
        config.replica_read_only = false;
        assert!(!config.should_panic_on_propagation_error(false));
        assert!(!config.should_panic_on_propagation_error(true));
        config.replica_read_only = true;
        assert!(config.should_panic_on_propagation_error(false));
        assert!(!config.should_panic_on_propagation_error(true));

        config.propagation_error_behavior = "panic".to_string();
        config.replica_read_only = false;
        assert!(!config.should_panic_on_propagation_error(false));
        assert!(config.should_panic_on_propagation_error(true));
        config.replica_read_only = true;
        assert!(config.should_panic_on_propagation_error(false));
    }
}
