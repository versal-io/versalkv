use crate::engine::extension::config::RuntimeConfig;

#[derive(Debug, Clone)]
pub enum ConfigValue {
    Bool(bool),
    Numeric(i64),
    /// Unsigned numeric (C `unsigned long long` configs like `maxmemory`),
    /// which can legitimately exceed `i64::MAX` (e.g. 9223372036854775808).
    UNumeric(u64),
    String(String),
}

impl ConfigValue {
    pub fn as_str(&self) -> String {
        match self {
            ConfigValue::Bool(v) => {
                if *v {
                    "yes".to_string()
                } else {
                    "no".to_string()
                }
            }
            ConfigValue::Numeric(v) => v.to_string(),
            ConfigValue::UNumeric(v) => v.to_string(),
            ConfigValue::String(v) => v.clone(),
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ConfigValue::Numeric(v) => Some(*v),
            ConfigValue::UNumeric(v) => Some(*v as i64),
            ConfigValue::Bool(v) => Some(if *v { 1 } else { 0 }),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConfigType {
    Bool,
    Numeric,
    String,
    Enum,
    Special,
}

pub struct ConfigSpec {
    pub name: &'static str,
    pub type_: ConfigType,
    pub enum_values: &'static [&'static str],
    pub numeric_min: i64,
    pub numeric_max: i64,
    pub flags: &'static [&'static str],
    pub alias: Option<&'static str>,
    pub get: fn(&RuntimeConfig) -> ConfigValue,
    pub set: fn(&mut RuntimeConfig, &ConfigValue) -> Result<(), String>,
}

impl ConfigSpec {
    #[cfg(test)]
    pub fn is_sensitive(&self) -> bool {
        self.flags.contains(&"sensitive")
    }

    pub fn is_immutable(&self) -> bool {
        self.flags.contains(&"immutable") || self.flags.contains(&"readonly")
    }

    pub fn is_hidden(&self) -> bool {
        self.flags.contains(&"hidden")
    }

    pub fn is_protected(&self) -> bool {
        self.flags.contains(&"protected")
    }

    pub fn accepts_memory_units(&self) -> bool {
        self.flags.contains(&"memory")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::extension::config::spec_registry::ConfigRegistry;

    #[test]
    fn test_registry_lookup() {
        let reg = ConfigRegistry::new();
        let spec = reg.lookup("dir").expect("dir should exist");
        assert_eq!(spec.name, "dir");
        assert_eq!(spec.type_, ConfigType::String);
    }

    #[test]
    fn test_registry_lookup_unknown() {
        let reg = ConfigRegistry::new();
        assert!(reg.lookup("nonexistent").is_none());
    }

    #[test]
    fn syslog_specs_match_valkey_startup_defaults() {
        let reg = ConfigRegistry::new();
        let rcfg = RuntimeConfig::new();

        let enabled = reg.lookup("syslog-enabled").expect("syslog-enabled config");
        let ident = reg.lookup("syslog-ident").expect("syslog-ident config");
        let facility = reg
            .lookup("syslog-facility")
            .expect("syslog-facility config");

        assert!(enabled.is_immutable());
        assert!(ident.is_immutable());
        assert!(facility.is_immutable());
        assert_eq!((enabled.get)(&rcfg).as_str(), "no");
        assert_eq!((ident.get)(&rcfg).as_str(), "valkey");
        assert_eq!((facility.get)(&rcfg).as_str(), "local0");
        assert_eq!(
            facility.enum_values,
            [
                "user", "local0", "local1", "local2", "local3", "local4", "local5", "local6",
                "local7",
            ]
        );
    }

    #[test]
    fn test_bool_get_set() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        let spec = reg.lookup("appendonly").unwrap();
        let val = (spec.get)(&rcfg);
        assert_eq!(val.as_str(), "no");

        rcfg.persistence.aof_enabled = true;
        let val = (spec.get)(&rcfg);
        assert_eq!(val.as_str(), "yes");
    }

    #[test]
    fn test_numeric_get_set() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        let spec = reg.lookup("maxclients").unwrap();
        let val = (spec.get)(&rcfg);
        assert_eq!(val.as_str(), "10000");

        (spec.set)(&mut rcfg, &ConfigValue::Numeric(5000)).unwrap();
        let val = (spec.get)(&rcfg);
        assert_eq!(val.as_str(), "5000");
    }

    #[test]
    fn io_thread_configs_match_valkey_defaults_and_ranges() {
        let reg = ConfigRegistry::new();
        let rcfg = RuntimeConfig::new();
        let threads = reg.lookup("io-threads").expect("io-threads config");
        let always_active = reg
            .lookup("io-threads-always-active")
            .expect("io-threads-always-active config");

        assert_eq!((threads.get)(&rcfg).as_str(), "1");
        assert_eq!((threads.numeric_min, threads.numeric_max), (1, 256));
        assert_eq!((always_active.get)(&rcfg).as_str(), "no");
        assert!(always_active.is_hidden());
    }

    #[test]
    fn prefetch_batch_size_matches_valkey_bounds_and_default() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg
            .lookup("prefetch-batch-max-size")
            .expect("prefetch-batch-max-size config");

        assert_eq!((spec.numeric_min, spec.numeric_max), (0, 128));
        assert_eq!((spec.get)(&rcfg).as_str(), "16");
        reg.try_set(&mut rcfg, "prefetch-batch-max-size", "0")
            .expect("prefetch config must be registered")
            .expect("zero disables prefetching");
        assert_eq!(rcfg.server.prefetch_batch_max_size, 0);
        assert!(reg
            .try_set(&mut rcfg, "prefetch-batch-max-size", "129")
            .expect("prefetch config must be registered")
            .is_err());
    }

    #[test]
    fn repl_diskless_delay_marks_explicit_batch_configuration() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        assert!(!rcfg.replication.repl_diskless_sync_delay_explicit);

        let spec = reg.lookup("repl-diskless-sync-delay").unwrap();
        (spec.set)(&mut rcfg, &ConfigValue::Numeric(5)).unwrap();

        assert_eq!(rcfg.replication.repl_diskless_sync_delay, 5);
        assert!(rcfg.replication.repl_diskless_sync_delay_explicit);
    }

    #[test]
    fn repl_ping_replica_period_is_runtime_configurable() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        assert_eq!(
            reg.try_set(&mut rcfg, "repl-ping-replica-period", "1"),
            Some(Ok(()))
        );
        assert_eq!(rcfg.replication.repl_ping_replica_period, 1);
        assert_eq!(
            reg.lookup("repl-ping-replica-period")
                .map(|spec| (spec.get)(&rcfg).as_str()),
            Some("1".to_string())
        );
    }

    #[test]
    fn replica_priority_is_runtime_configurable_with_slave_alias() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        assert_eq!(
            reg.try_set(&mut rcfg, "replica-priority", "50"),
            Some(Ok(()))
        );
        assert_eq!(rcfg.replication.replica_priority, 50);
        assert_eq!(
            reg.lookup("slave-priority")
                .map(|spec| (spec.get)(&rcfg).as_str()),
            Some("50".to_string())
        );
    }

    #[test]
    fn client_output_buffer_limit_updates_replica_hard_limit() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg.lookup("client-output-buffer-limit").unwrap();
        (spec.set)(
            &mut rcfg,
            &ConfigValue::String("normal 0 0 0 replica 1100k 16383 0 pubsub 0 0 0".to_string()),
        )
        .unwrap();
        assert_eq!(rcfg.replication.replica_output_buffer_hard_limit, 1_126_400);
        assert_eq!(rcfg.replication.replica_output_buffer_soft_limit, 16_383);
        assert_eq!(rcfg.replication.replica_output_buffer_soft_limit_seconds, 0);

        (spec.set)(
            &mut rcfg,
            &ConfigValue::String("replica 256mb 0 0".to_string()),
        )
        .unwrap();
        assert_eq!(
            rcfg.replication.replica_output_buffer_hard_limit,
            268_435_456
        );
        assert_eq!(rcfg.replication.replica_output_buffer_soft_limit, 0);
        assert_eq!(rcfg.replication.replica_output_buffer_soft_limit_seconds, 0);
    }

    #[test]
    fn test_lua_time_limit_updates_runtime_config() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        let spec = reg.lookup("lua-time-limit").unwrap();
        (spec.set)(&mut rcfg, &ConfigValue::Numeric(10)).unwrap();

        assert_eq!(rcfg.server.lua_time_limit, 10);
        assert_eq!((spec.get)(&rcfg).as_str(), "10");
    }

    #[test]
    fn lua_enable_insecure_api_is_runtime_configurable() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg
            .lookup("lua-enable-insecure-api")
            .expect("lua-enable-insecure-api should be registered");

        assert_eq!((spec.get)(&rcfg).as_str(), "no");
        reg.try_set(&mut rcfg, "lua-enable-insecure-api", "yes")
            .expect("config should exist")
            .expect("yes should be accepted");
        assert!(rcfg.server.lua_enable_insecure_api);
    }

    #[test]
    fn test_enum_get_set() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        let spec = reg.lookup("appendfsync").unwrap();
        let val = (spec.get)(&rcfg);
        assert_eq!(val.as_str(), "everysec");

        (spec.set)(&mut rcfg, &ConfigValue::String("always".to_string())).unwrap();
        let val = (spec.get)(&rcfg);
        assert_eq!(val.as_str(), "always");
    }

    #[test]
    fn rdb_version_check_matches_valkey_enum_semantics() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        let spec = reg
            .lookup("rdb-version-check")
            .expect("rdb-version-check should be registered");
        assert_eq!(spec.type_, ConfigType::Enum);
        assert_eq!(spec.enum_values, ["strict", "relaxed"]);
        assert_eq!((spec.get)(&rcfg).as_str(), "strict");

        reg.try_set(&mut rcfg, "rdb-version-check", "relaxed")
            .expect("rdb-version-check should use the registry")
            .expect("relaxed should be accepted");
        assert!(rcfg.persistence.rdb_version_check_relaxed);

        let error = reg
            .try_set(&mut rcfg, "rdb-version-check", "permissive")
            .expect("rdb-version-check should use the registry")
            .expect_err("unknown rdb-version-check values must be rejected");
        assert!(error.contains("rdb-version-check"));
    }

    #[test]
    fn repl_diskless_load_accepts_swapdb_and_round_trips() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        let result = reg
            .try_set(&mut rcfg, "repl-diskless-load", "swapdb")
            .expect("repl-diskless-load should be registered");
        assert!(result.is_ok());

        let spec = reg.lookup("repl-diskless-load").unwrap();
        assert_eq!((spec.get)(&rcfg).as_str(), "swapdb");
    }

    #[test]
    fn test_notify_keyspace_events_canonicalizes_all_flag() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        let spec = reg.lookup("notify-keyspace-events").unwrap();
        (spec.set)(&mut rcfg, &ConfigValue::String("KA".to_string())).unwrap();
        assert_eq!((spec.get)(&rcfg).as_str(), "AK");

        (spec.set)(&mut rcfg, &ConfigValue::String("EA".to_string())).unwrap();
        assert_eq!((spec.get)(&rcfg).as_str(), "AE");

        (spec.set)(&mut rcfg, &ConfigValue::String("nmAE".to_string())).unwrap();
        assert_eq!((spec.get)(&rcfg).as_str(), "AEm");

        assert!((spec.set)(&mut rcfg, &ConfigValue::String("K?".to_string())).is_err());

        (spec.set)(&mut rcfg, &ConfigValue::String("$lshzxeKE".to_string())).unwrap();
        assert_eq!((spec.get)(&rcfg).as_str(), "$lshzxeKE");
    }

    #[test]
    fn test_sensitive_flag() {
        let reg = ConfigRegistry::new();
        let spec = reg.lookup("requirepass").unwrap();
        assert!(spec.is_sensitive());
    }

    #[test]
    fn test_port_is_modifiable_but_rejects_unapplied_changes() {
        let reg = ConfigRegistry::new();
        let spec = reg.lookup("port").unwrap();
        let mut rcfg = RuntimeConfig::new();
        let listening_port = rcfg.server.listening_port as i64;

        assert!(!spec.is_immutable());
        (spec.set)(&mut rcfg, &ConfigValue::Numeric(listening_port)).unwrap();
        assert!((spec.set)(&mut rcfg, &ConfigValue::Numeric(6380)).is_err());
    }

    #[test]
    fn lazyfree_lazy_eviction_is_runtime_configurable() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        assert_eq!(
            (reg.lookup("lazyfree-lazy-eviction").unwrap().get)(&rcfg).as_str(),
            "yes"
        );

        reg.try_set(&mut rcfg, "lazyfree-lazy-eviction", "no")
            .unwrap()
            .unwrap();
        assert!(!rcfg.server.lazyfree_lazy_eviction);
    }

    #[test]
    fn lazyfree_user_controls_match_valkey_defaults_and_are_independent() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        for name in ["lazyfree-lazy-user-del", "lazyfree-lazy-user-flush"] {
            assert_eq!(
                (reg.lookup(name)
                    .expect("Valkey lazyfree user control must be registered")
                    .get)(&rcfg)
                .as_str(),
                "yes"
            );
        }

        reg.try_set(&mut rcfg, "lazyfree-lazy-user-del", "no")
            .unwrap()
            .unwrap();
        assert!(!rcfg.server.lazyfree_lazy_user_del);
        assert!(rcfg.server.lazyfree_lazy_user_flush);

        reg.try_set(&mut rcfg, "lazyfree-lazy-user-flush", "no")
            .unwrap()
            .unwrap();
        assert!(!rcfg.server.lazyfree_lazy_user_flush);
    }

    #[test]
    fn stream_node_max_bytes_matches_valkey_default_and_zero_boundary_mode() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg
            .lookup("stream-node-max-bytes")
            .expect("stream-node-max-bytes must be registered");

        assert_eq!((spec.numeric_min, spec.numeric_max), (0, i64::MAX));
        assert_eq!((spec.get)(&rcfg).as_str(), "4096");
        reg.try_set(&mut rcfg, "stream-node-max-bytes", "0")
            .unwrap()
            .unwrap();
        assert_eq!(rcfg.encoding.stream_node_max_bytes, 0);
    }

    #[test]
    fn stream_node_max_entries_allows_zero_to_disable_entry_boundaries() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg
            .lookup("stream-node-max-entries")
            .expect("stream-node-max-entries must be registered");

        assert_eq!(spec.numeric_min, 0);
        reg.try_set(&mut rcfg, "stream-node-max-entries", "0")
            .unwrap()
            .unwrap();
        assert_eq!(rcfg.encoding.stream_node_max_entries, 0);
    }

    #[test]
    fn cluster_blacklist_ttl_matches_valkey_default_and_zero() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg
            .lookup("cluster-blacklist-ttl")
            .expect("cluster-blacklist-ttl must be registered");

        assert_eq!((spec.numeric_min, spec.numeric_max), (0, i64::MAX));
        assert_eq!((spec.get)(&rcfg).as_str(), "60");
        reg.try_set(&mut rcfg, "cluster-blacklist-ttl", "0")
            .unwrap()
            .unwrap();
        assert_eq!(rcfg.server.cluster_blacklist_ttl, 0);
    }

    #[test]
    fn rdb_incremental_fsync_matches_valkey_default_and_runtime_set() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg
            .lookup("rdb-save-incremental-fsync")
            .expect("rdb-save-incremental-fsync must be registered");

        assert_eq!((spec.get)(&rcfg).as_str(), "yes");
        reg.try_set(&mut rcfg, "rdb-save-incremental-fsync", "no")
            .unwrap()
            .unwrap();
        assert!(!rcfg.persistence.rdb_save_incremental_fsync);
    }

    #[test]
    fn rdb_del_sync_files_matches_valkey_default_and_runtime_set() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg
            .lookup("rdb-del-sync-files")
            .expect("rdb-del-sync-files must be registered");

        assert_eq!((spec.get)(&rcfg).as_str(), "no");
        reg.try_set(&mut rcfg, "rdb-del-sync-files", "yes")
            .unwrap()
            .unwrap();
        assert!(rcfg.persistence.rdb_del_sync_files);
    }

    #[test]
    fn replica_ignore_disk_write_errors_matches_valkey_default_and_runtime_set() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg
            .lookup("replica-ignore-disk-write-errors")
            .expect("replica-ignore-disk-write-errors must be registered");

        assert_eq!((spec.get)(&rcfg).as_str(), "no");
        reg.try_set(&mut rcfg, "replica-ignore-disk-write-errors", "yes")
            .unwrap()
            .unwrap();
        assert!(rcfg.replication.replica_ignore_disk_write_errors);
    }

    #[test]
    fn replica_lazy_flush_matches_valkey_default_and_alias() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg
            .lookup("replica-lazy-flush")
            .expect("replica-lazy-flush must be registered");

        assert_eq!((spec.get)(&rcfg).as_str(), "yes");
        reg.try_set(&mut rcfg, "slave-lazy-flush", "no")
            .unwrap()
            .unwrap();
        assert!(!rcfg.replication.replica_lazy_flush);
    }

    #[test]
    fn aof_rewrite_incremental_fsync_matches_valkey_default_and_runtime_set() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg
            .lookup("aof-rewrite-incremental-fsync")
            .expect("aof-rewrite-incremental-fsync must be registered");

        assert_eq!((spec.get)(&rcfg).as_str(), "yes");
        reg.try_set(&mut rcfg, "aof-rewrite-incremental-fsync", "no")
            .unwrap()
            .unwrap();
        assert!(!rcfg.persistence.aof_rewrite_incremental_fsync);
    }

    #[test]
    fn repl_mptcp_is_startup_only_with_a_disabled_default() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg
            .lookup("repl-mptcp")
            .expect("repl-mptcp must be registered");

        assert!(spec.is_immutable());
        assert_eq!((spec.get)(&rcfg).as_str(), "no");
        assert_eq!(
            reg.try_set_startup(&mut rcfg, "repl-mptcp", "yes"),
            Some(Ok(()))
        );
        assert!(rcfg.replication.repl_mptcp);
    }

    #[test]
    fn mptcp_is_startup_only_with_a_disabled_default() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg.lookup("mptcp").expect("mptcp must be registered");

        assert!(spec.is_immutable());
        assert_eq!((spec.get)(&rcfg).as_str(), "no");
        assert_eq!(reg.try_set_startup(&mut rcfg, "mptcp", "yes"), Some(Ok(())));
        assert!(rcfg.server.mptcp);
    }

    #[test]
    fn active_expire_effort_matches_valkey_bounds_and_default() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg
            .lookup("active-expire-effort")
            .expect("active-expire-effort should be registered");

        assert_eq!((spec.numeric_min, spec.numeric_max), (1, 10));
        assert_eq!((spec.get)(&rcfg).as_str(), "1");
        reg.try_set(&mut rcfg, "active-expire-effort", "10")
            .unwrap()
            .unwrap();
        assert_eq!(rcfg.server.active_expire_effort, 10);
        assert!(reg
            .try_set(&mut rcfg, "active-expire-effort", "11")
            .unwrap()
            .is_err());
    }

    #[test]
    fn active_defrag_configs_match_valkey_defaults_and_bounds() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        assert_eq!(
            (reg.lookup("activedefrag").unwrap().get)(&rcfg).as_str(),
            "no"
        );
        for (name, default, min, max) in [
            ("active-defrag-cycle-min", "1", 1, 99),
            ("active-defrag-cycle-max", "25", 1, 99),
            ("active-defrag-threshold-lower", "10", 0, 1000),
            ("active-defrag-threshold-upper", "100", 0, 1000),
            ("active-defrag-cycle-us", "500", 0, 100_000),
        ] {
            let spec = reg.lookup(name).unwrap();
            assert_eq!((spec.get)(&rcfg).as_str(), default);
            assert_eq!((spec.numeric_min, spec.numeric_max), (min, max));
        }
        reg.try_set(&mut rcfg, "activedefrag", "yes")
            .unwrap()
            .unwrap();
        reg.try_set(&mut rcfg, "active-defrag-max-scan-fields", "1")
            .unwrap()
            .unwrap();
        reg.try_set(&mut rcfg, "active-defrag-ignore-bytes", "1")
            .unwrap()
            .unwrap();
        assert!(rcfg.server.activedefrag);
        assert_eq!(rcfg.server.active_defrag_max_scan_fields, 1);
        assert_eq!(rcfg.server.active_defrag_ignore_bytes, 1);
    }

    #[test]
    fn tls_listener_configs_round_trip_through_the_registry() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        for (name, value) in [
            ("tls-port", "6380"),
            ("tls-cert-file", "/etc/versal/server.crt"),
            ("tls-key-file", "/etc/versal/server.key"),
            ("tls-ca-cert-file", "/etc/versal/ca.crt"),
            ("tls-ca-cert-dir", "/etc/versal/ca"),
            ("tls-auth-clients", "optional"),
            ("tls-protocols", "TLSv1.2 TLSv1.3"),
            ("tls-ciphers", "ECDHE-RSA-AES128-GCM-SHA256"),
            ("tls-ciphersuites", "TLS_AES_128_GCM_SHA256"),
            ("tls-prefer-server-ciphers", "yes"),
            ("tls-session-caching", "no"),
            ("tls-session-cache-size", "123"),
            ("tls-replication", "yes"),
            ("tls-client-cert-file", "/etc/versal/client.crt"),
            ("tls-client-key-file", "/etc/versal/client.key"),
        ] {
            reg.try_set(&mut rcfg, name, value)
                .expect("TLS config should be registered")
                .expect("TLS config value should be valid");
            assert_eq!((reg.lookup(name).unwrap().get)(&rcfg).as_str(), value);
        }
        assert!(reg
            .try_set(&mut rcfg, "tls-port", "65536")
            .expect("tls-port should be registered")
            .is_err());
    }

    #[test]
    fn max_new_connections_per_cycle_matches_valkey_bounds_and_default() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg
            .lookup("max-new-connections-per-cycle")
            .expect("max-new-connections-per-cycle should be registered");

        assert_eq!((spec.numeric_min, spec.numeric_max), (1, 1000));
        assert_eq!((spec.get)(&rcfg).as_str(), "10");
        reg.try_set(&mut rcfg, "max-new-connections-per-cycle", "25")
            .unwrap()
            .unwrap();
        assert_eq!(rcfg.server.max_new_connections_per_cycle, 25);
        assert!(reg
            .try_set(&mut rcfg, "max-new-connections-per-cycle", "0")
            .unwrap()
            .is_err());
    }

    #[test]
    fn max_new_tls_connections_per_cycle_matches_valkey_bounds_and_default() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg
            .lookup("max-new-tls-connections-per-cycle")
            .expect("max-new-tls-connections-per-cycle should be registered");

        assert_eq!((spec.numeric_min, spec.numeric_max), (1, 1000));
        assert_eq!((spec.get)(&rcfg).as_str(), "1");
        reg.try_set(&mut rcfg, "max-new-tls-connections-per-cycle", "7")
            .unwrap()
            .unwrap();
        assert_eq!(rcfg.server.max_new_tls_connections_per_cycle, 7);
        assert!(reg
            .try_set(&mut rcfg, "max-new-tls-connections-per-cycle", "0")
            .unwrap()
            .is_err());
    }

    #[test]
    fn client_timeout_matches_valkey_default_and_range() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg.lookup("timeout").expect("timeout should be registered");

        assert_eq!((spec.numeric_min, spec.numeric_max), (0, i32::MAX as i64));
        assert_eq!((spec.get)(&rcfg).as_str(), "0");
        reg.try_set(&mut rcfg, "timeout", "30").unwrap().unwrap();
        assert_eq!(rcfg.server.client_timeout, 30);
    }

    #[test]
    fn activerehashing_is_a_live_cron_control() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        assert_eq!(
            (reg.lookup("activerehashing").unwrap().get)(&rcfg).as_str(),
            "yes"
        );
        reg.try_set(&mut rcfg, "activerehashing", "no")
            .unwrap()
            .unwrap();
        assert!(!rcfg.server.active_rehashing);
    }

    #[test]
    fn client_query_buffer_limit_matches_c_memory_contract() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        let spec = reg
            .lookup("client-query-buffer-limit")
            .expect("client-query-buffer-limit should be registered");

        assert_eq!(spec.type_, ConfigType::Numeric);
        assert_eq!(spec.numeric_min, 1024 * 1024);
        assert_eq!(spec.numeric_max, i64::MAX);
        assert_eq!((spec.get)(&rcfg).as_str(), "1073741824");
        reg.try_set(&mut rcfg, "client-query-buffer-limit", "10m")
            .unwrap()
            .unwrap();
        assert_eq!(rcfg.server.client_query_buffer_limit, 10 * 1024 * 1024);
        assert!(reg
            .try_set(&mut rcfg, "client-query-buffer-limit", "1023kb")
            .unwrap()
            .is_err());
        assert!(reg
            .try_set(&mut rcfg, "client-query-buffer-limit", "not-a-size")
            .unwrap()
            .is_err());
    }

    #[test]
    fn replica_announce_port_uses_configured_port_or_listener_fallback() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        assert_eq!(rcfg.replication.effective_replica_announce_port(6379), 6379);

        reg.try_set(&mut rcfg, "slave-announce-port", "16379")
            .unwrap()
            .unwrap();
        assert_eq!(
            rcfg.replication.effective_replica_announce_port(6379),
            16379
        );
        assert_eq!(
            reg.lookup("slave-announce-port").unwrap().name,
            "replica-announce-port"
        );
    }

    #[test]
    fn replica_ignore_maxmemory_defaults_true_and_supports_legacy_alias() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        assert!(rcfg.replication.replica_ignore_maxmemory);

        reg.try_set(&mut rcfg, "slave-ignore-maxmemory", "no")
            .unwrap()
            .unwrap();
        assert!(!rcfg.replication.replica_ignore_maxmemory);
    }

    #[test]
    fn repl_disable_tcp_nodelay_round_trips_as_a_runtime_boolean() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        assert_eq!(
            (reg.lookup("repl-disable-tcp-nodelay").unwrap().get)(&rcfg).as_str(),
            "no"
        );

        reg.try_set(&mut rcfg, "repl-disable-tcp-nodelay", "yes")
            .unwrap()
            .unwrap();
        assert!(rcfg.replication.repl_disable_tcp_nodelay);
    }

    #[test]
    fn tcp_backlog_is_startup_only_with_valkey_default() {
        let reg = ConfigRegistry::new();
        let rcfg = RuntimeConfig::new();
        let spec = reg
            .lookup("tcp-backlog")
            .expect("tcp-backlog should be registered");

        assert!(spec.is_immutable());
        assert_eq!((spec.get)(&rcfg).as_str(), "511");
    }

    #[test]
    fn all_specs_are_sorted_and_current_values_round_trip_through_setters() {
        let reg = ConfigRegistry::new();
        let specs = reg.all_specs();
        let mut names: Vec<&str> = specs.iter().map(|spec| spec.name).collect();
        let sorted = {
            let mut sorted = names.clone();
            sorted.sort();
            sorted
        };
        assert_eq!(names, sorted);
        assert!(names.len() > 80, "expected broad config registry coverage");

        let mut rcfg = RuntimeConfig::new();
        for spec in specs {
            let current = (spec.get)(&rcfg);
            let _ = (spec.set)(&mut rcfg, &current);
            assert_eq!(reg.lookup(spec.name).unwrap().name, spec.name);
        }

        names.dedup();
        assert_eq!(names.len(), reg.all_specs().len());
    }

    #[test]
    fn aliases_resolve_to_canonical_specs() {
        let reg = ConfigRegistry::new();

        assert_eq!(reg.lookup("masterauth").unwrap().name, "primaryauth");
        assert_eq!(
            reg.lookup("list-max-ziplist-size").unwrap().name,
            "list-max-listpack-size"
        );
        assert_eq!(
            reg.lookup("hash-max-ziplist-entries").unwrap().name,
            "hash-max-listpack-entries"
        );
        assert_eq!(
            reg.lookup("zset-max-ziplist-value").unwrap().name,
            "zset-max-listpack-value"
        );
    }

    #[test]
    fn try_set_parses_bool_numeric_memory_units_and_enums() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        assert!(reg.try_set(&mut rcfg, "appendonly", "yes").is_none());
        assert!(reg
            .try_set(&mut rcfg, "rdb-key-save-delay", "0")
            .unwrap()
            .is_ok());

        assert!(reg
            .try_set(&mut rcfg, "lua-time-limit", "2000")
            .unwrap()
            .is_ok());
        assert!(reg
            .try_set(&mut rcfg, "appendfsync", "always")
            .unwrap()
            .is_ok());
        assert_eq!(
            rcfg.persistence.aof_fsync,
            crate::foundation::io::persistence::AofFsync::Always
        );
        assert!(reg.try_set(&mut rcfg, "masterauth", "pw").unwrap().is_ok());
        assert_eq!(rcfg.security.primaryauth, Some(b"pw".to_vec()));
    }

    #[test]
    fn registry_handles_special_configs_and_rejects_runtime_aclfile_changes() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        let error = reg
            .try_set(&mut rcfg, "aclfile", "users.acl")
            .expect("aclfile should be handled by the registry")
            .expect_err("aclfile is immutable after startup");
        assert!(error.contains("immutable"));
        assert_eq!(rcfg.server.aclfile, "");

        reg.try_set(&mut rcfg, "maxmemory-clients", "25%")
            .expect("maxmemory-clients should be handled by the registry")
            .expect("valid percentage should be accepted");
        assert_eq!(rcfg.server.maxmemory_clients, "25%");

        let error = reg
            .try_set(&mut rcfg, "maxmemory-clients", "101%")
            .expect("maxmemory-clients should be handled by the registry")
            .expect_err("percentage over 100 must be rejected");
        assert!(error.contains("less or equal to 100"));
        assert_eq!(rcfg.server.maxmemory_clients, "25%");

        reg.try_set(&mut rcfg, "save", "60 1000 300 10")
            .expect("save should be handled by the registry")
            .expect("valid save pairs should be accepted");
        assert_eq!(rcfg.persistence.saveparams.len(), 2);
        assert_eq!(rcfg.persistence.saveparams[0].seconds, 60);
        assert_eq!(rcfg.persistence.saveparams[0].changes, 1000);
        assert_eq!(rcfg.persistence.saveparams[1].seconds, 300);
        assert_eq!(rcfg.persistence.saveparams[1].changes, 10);

        let error = reg
            .try_set(&mut rcfg, "save", "60")
            .expect("save should be handled by the registry")
            .expect_err("odd save token count must be rejected");
        assert!(error.contains("odd number of tokens"));
    }

    #[test]
    fn cluster_slot_stats_enabled_updates_runtime_config() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();
        assert!(!rcfg.server.cluster_slot_stats_enabled);

        reg.try_set(&mut rcfg, "cluster-slot-stats-enabled", "yes")
            .unwrap()
            .unwrap();
        assert!(rcfg.server.cluster_slot_stats_enabled);

        reg.try_set(&mut rcfg, "cluster-slot-stats-enabled", "no")
            .unwrap()
            .unwrap();
        assert!(!rcfg.server.cluster_slot_stats_enabled);
    }

    #[test]
    fn cluster_allow_pubsubshard_when_down_matches_valkey_default() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        let spec = reg
            .lookup("cluster-allow-pubsubshard-when-down")
            .expect("cluster sharded Pub/Sub availability config should exist");
        assert_eq!((spec.get)(&rcfg).as_str(), "yes");
        reg.try_set(&mut rcfg, "cluster-allow-pubsubshard-when-down", "no")
            .expect("config should be registered")
            .expect("no should be accepted");
        assert!(!rcfg.server.cluster_allow_pubsubshard_when_down);
    }

    #[test]
    fn c_commandlog_and_lua_aliases_share_their_canonical_runtime_state() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        assert_eq!(
            reg.lookup("slowlog-log-slower-than").map(|spec| spec.name),
            Some("commandlog-execution-slower-than")
        );
        assert_eq!(
            reg.lookup("slowlog-max-len").map(|spec| spec.name),
            Some("commandlog-slow-execution-max-len")
        );
        assert_eq!(
            reg.lookup("lua-enable-deprecated-api")
                .map(|spec| spec.name),
            Some("lua-enable-insecure-api")
        );

        reg.try_set(&mut rcfg, "slowlog-log-slower-than", "42")
            .expect("slowlog alias should be registered")
            .expect("slowlog alias should update its canonical setting");
        reg.try_set(&mut rcfg, "slowlog-max-len", "17")
            .expect("slowlog alias should be registered")
            .expect("slowlog alias should update its canonical setting");

        assert_eq!(rcfg.server.commandlog_slow_threshold, 42);
        assert_eq!(rcfg.server.slowlog_log_slower_than, 42);
        assert_eq!(rcfg.server.commandlog_slow_max_len, 17);
        assert_eq!(rcfg.server.slowlog_max_len, 17);
    }

    #[test]
    fn try_set_reports_unknown_bad_numeric_range_and_type_errors() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        assert!(reg.try_set(&mut rcfg, "does-not-exist", "1").is_none());
        let err = reg
            .try_set(&mut rcfg, "maxclients", "0")
            .unwrap()
            .unwrap_err();
        assert!(err.contains("out of range"));
        let err = reg
            .try_set(&mut rcfg, "maxclients", "not-a-number")
            .unwrap()
            .unwrap_err();
        assert!(err.contains("must be numeric"));
        let err = reg
            .try_set(&mut rcfg, "appendfsync", "sometimes")
            .unwrap()
            .unwrap_err();
        assert!(err.contains("appendfsync"));
        let err = reg
            .try_set(&mut rcfg, "cluster-node-timeout", "-1")
            .unwrap()
            .unwrap_err();
        assert!(err.contains("out of range"));
        let err = reg
            .try_set(&mut rcfg, "cluster-ping-interval", "-1")
            .unwrap()
            .unwrap_err();
        assert!(err.contains("out of range"));
        assert!(reg.try_set(&mut rcfg, "maxmemory", "2kb").is_none());
    }

    #[test]
    fn propagation_error_behavior_is_a_runtime_replication_setting() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        let spec = reg
            .lookup("propagation-error-behavior")
            .expect("propagation-error-behavior should be registered");
        assert_eq!((spec.get)(&rcfg).as_str(), "ignore");
        reg.try_set(&mut rcfg, "propagation-error-behavior", "ignore")
            .expect("config should be known")
            .expect("ignore should be accepted");
        assert_eq!(rcfg.replication.propagation_error_behavior, "ignore");
    }

    #[test]
    fn propagation_error_behavior_accepts_supported_values() {
        let reg = ConfigRegistry::new();
        for value in ["ignore", "panic", "panic-on-replicas"] {
            let mut rcfg = RuntimeConfig::new();
            reg.try_set_startup(&mut rcfg, "propagation-error-behavior", value)
                .expect("config should be known")
                .expect("supported propagation behavior should be accepted");
            assert_eq!(rcfg.replication.propagation_error_behavior, value);
        }
    }

    #[test]
    fn immutable_rdbchecksum_is_startup_settable_but_runtime_rejected() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        assert_eq!(
            reg.try_set_startup(&mut rcfg, "rdbchecksum", "no"),
            Some(Ok(()))
        );
        assert!(!rcfg.persistence.rdbchecksum);

        let err = reg
            .try_set(&mut rcfg, "rdbchecksum", "yes")
            .expect("rdbchecksum should be registered")
            .expect_err("rdbchecksum should remain immutable at runtime");
        assert!(err.contains("can't set immutable config"));
        assert!(!rcfg.persistence.rdbchecksum);
    }

    #[test]
    fn legacy_replication_config_aliases_and_backlog_ttl_are_supported() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        assert_eq!(
            reg.lookup("slave-read-only").unwrap().name,
            "replica-read-only"
        );
        reg.try_set(&mut rcfg, "slave-read-only", "no")
            .unwrap()
            .unwrap();
        assert!(!rcfg.replication.replica_read_only);
        reg.try_set(&mut rcfg, "repl-backlog-ttl", "60")
            .unwrap()
            .unwrap();
        assert_eq!(rcfg.replication.repl_backlog_ttl, 60);
    }

    #[test]
    fn security_replication_encoding_and_cluster_specs_update_runtime_config() {
        let reg = ConfigRegistry::new();
        let mut rcfg = RuntimeConfig::new();

        for (name, value) in [
            ("requirepass", "secret"),
            ("primaryuser", "replica-user"),
            ("set-max-intset-entries", "16"),
            ("set-max-listpack-entries", "8"),
            ("set-max-listpack-value", "32"),
            ("hash-max-listpack-entries", "17"),
            ("hash-max-listpack-value", "33"),
            ("zset-max-listpack-entries", "18"),
            ("zset-max-listpack-value", "34"),
            ("cluster-ping-interval", "100"),
            ("cluster-node-timeout", "15000"),
            ("cluster-announce-ip", "10.0.0.10"),
            ("cluster-announce-client-ipv4", "192.0.2.10"),
            ("cluster-announce-client-ipv6", "2001:db8::10"),
            ("cluster-announce-hostname", "node.local"),
            ("cluster-announce-human-nodename", "node-a"),
            ("cluster-announce-port", "7000"),
            ("cluster-announce-tls-port", "7443"),
            ("cluster-announce-client-port", "7001"),
            ("cluster-announce-client-tls-port", "7444"),
            ("cluster-announce-bus-port", "17001"),
            ("availability-zone", "zone-a"),
            ("cluster-config-save-behavior", "best-effort"),
            ("cluster-message-gossip-perc", "80"),
            ("cluster-slot-stats-enabled", "yes"),
        ] {
            reg.try_set(&mut rcfg, name, value)
                .unwrap_or_else(|| panic!("{name} should exist"))
                .unwrap_or_else(|err| panic!("{name} failed: {err}"));
        }

        // Immutable cluster configs must be rejected at runtime
        for (name, value) in [
            ("cluster-enabled", "yes"),
            ("cluster-port", "17000"),
            ("cluster-config-file", "nodes-test.conf"),
            ("cluster-databases", "2"),
        ] {
            let err = reg
                .try_set(&mut rcfg, name, value)
                .unwrap_or_else(|| panic!("{name} should exist"))
                .expect_err(&format!("{name} should be rejected as immutable"));
            assert!(err.contains("immutable"), "{name}: {err}");
        }

        assert_eq!(rcfg.security.requirepass, Some(b"secret".to_vec()));
        assert_eq!(rcfg.security.primaryuser, Some(b"replica-user".to_vec()));
        assert_eq!(rcfg.encoding.set_max_intset_entries, 16);
        assert_eq!(rcfg.encoding.set_max_listpack_entries, 8);
        assert_eq!(rcfg.encoding.set_max_listpack_value, 32);
        assert_eq!(rcfg.encoding.hash_max_listpack_entries, 17);
        assert_eq!(rcfg.encoding.hash_max_listpack_value, 33);
        assert_eq!(rcfg.encoding.zset_max_ziplist_entries, 18);
        assert_eq!(rcfg.encoding.zset_max_ziplist_value, 34);
        assert_eq!(rcfg.server.cluster_announce_ip, "10.0.0.10");
        assert_eq!(rcfg.server.cluster_announce_client_ipv4, "192.0.2.10");
        assert_eq!(rcfg.server.cluster_announce_client_ipv6, "2001:db8::10");
        assert_eq!(rcfg.server.cluster_announce_hostname, "node.local");
        assert_eq!(rcfg.server.cluster_announce_human_nodename, "node-a");
        assert_eq!(rcfg.server.cluster_announce_port, 7000);
        assert_eq!(rcfg.server.cluster_announce_tls_port, 7443);
        assert_eq!(rcfg.server.cluster_announce_client_port, 7001);
        assert_eq!(rcfg.server.cluster_announce_client_tls_port, 7444);
        assert_eq!(rcfg.server.cluster_announce_bus_port, 17001);
        assert_eq!(rcfg.server.availability_zone, "zone-a");
        assert_eq!(rcfg.server.cluster_config_save_behavior, "best-effort");
        assert_eq!(rcfg.server.cluster_message_gossip_perc, 80);
    }

    #[test]
    fn cluster_metadata_specs_have_upstream_defaults_ranges_and_single_registration() {
        let reg = ConfigRegistry::new();
        let rcfg = RuntimeConfig::new();
        let cases = [
            ("cluster-announce-ip", "", 0, 0),
            ("cluster-announce-client-ipv4", "", 0, 0),
            ("cluster-announce-client-ipv6", "", 0, 0),
            ("cluster-announce-hostname", "", 0, 0),
            ("cluster-announce-human-nodename", "", 0, 0),
            ("cluster-announce-port", "0", 0, 65535),
            ("cluster-announce-client-port", "0", 0, 65535),
            ("cluster-announce-client-tls-port", "0", 0, 65535),
            ("cluster-announce-tls-port", "0", 0, 65535),
            ("cluster-announce-bus-port", "0", 0, 65535),
            ("availability-zone", "", 0, 0),
            ("cluster-config-save-behavior", "sync", 0, 0),
            ("cluster-message-gossip-perc", "10", 1, 100),
        ];
        for (name, expected, min, max) in cases {
            let spec = reg.lookup(name).unwrap_or_else(|| panic!("missing {name}"));
            assert_eq!((spec.get)(&rcfg).as_str(), expected, "{name}");
            assert_eq!((spec.numeric_min, spec.numeric_max), (min, max), "{name}");
            assert_eq!(
                reg.all_specs()
                    .iter()
                    .filter(|item| item.name == name)
                    .count(),
                1,
                "{name} must be registered once"
            );
        }

        let mut rcfg = RuntimeConfig::new();
        for (name, value) in [
            ("cluster-announce-client-port", "65536"),
            ("cluster-announce-client-tls-port", "-1"),
            ("cluster-announce-tls-port", "65536"),
            ("cluster-message-gossip-perc", "0"),
            ("cluster-message-gossip-perc", "101"),
            ("cluster-config-save-behavior", "ignore"),
        ] {
            assert!(
                reg.try_set(&mut rcfg, name, value).unwrap().is_err(),
                "{name}"
            );
        }

        reg.try_set(&mut rcfg, "cluster-config-save-behavior", "SYNC")
            .unwrap()
            .unwrap();
        assert_eq!(rcfg.server.cluster_config_save_behavior, "sync");
    }
}
