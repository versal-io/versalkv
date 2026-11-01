use crate::engine::extension::config::spec::{ConfigSpec, ConfigType, ConfigValue};
use crate::engine::extension::config::spec_registry::ConfigRegistry;

pub(crate) fn register_cluster_configs(reg: &mut ConfigRegistry) {
    reg.register(&ConfigSpec {
        name: "cluster-enabled",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.cluster_enabled),
        set: |_rcfg, _val| {
            Err("ERR CONFIG SET failed (immutable config 'cluster-enabled')".to_string())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-port",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 65535,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.cluster_port as i64),
        set: |_rcfg, _val| {
            Err("ERR CONFIG SET failed (immutable config 'cluster-port')".to_string())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-ping-interval",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.cluster_ping_interval as i64),
        set: |rcfg, val| {
            let v = val
                .as_i64()
                .ok_or("cluster-ping-interval must be numeric")?;
            if v < 0 {
                return Err("cluster-ping-interval must be >= 0".into());
            }
            rcfg.server.cluster_ping_interval = v as u64;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-node-timeout",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.cluster_node_timeout as i64),
        set: |rcfg, val| {
            let v = val.as_i64().ok_or("cluster-node-timeout must be numeric")?;
            if v < 0 {
                return Err("cluster-node-timeout must be >= 0".into());
            }
            rcfg.server.cluster_node_timeout = v as u64;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-blacklist-ttl",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.cluster_blacklist_ttl as i64),
        set: |rcfg, val| {
            let value = val
                .as_i64()
                .ok_or("cluster-blacklist-ttl must be numeric")?;
            if value < 0 {
                return Err("cluster-blacklist-ttl must be >= 0".to_string());
            }
            rcfg.server.cluster_blacklist_ttl = value as u64;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-config-file",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.cluster_config_file.clone()),
        set: |_rcfg, _val| {
            Err("ERR CONFIG SET failed (immutable config 'cluster-config-file')".to_string())
        },
    });
    super::cluster_metadata::register_cluster_metadata_configs(reg);
    reg.register(&ConfigSpec {
        name: "cluster-slot-stats-enabled",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.cluster_slot_stats_enabled),
        set: |rcfg, val| {
            let ConfigValue::Bool(enabled) = val else {
                return Err("cluster-slot-stats-enabled must be a boolean".to_string());
            };
            rcfg.server.cluster_slot_stats_enabled = *enabled;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-databases",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: 255,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.cluster_databases as i64),
        set: |_rcfg, _val| {
            Err("ERR CONFIG SET failed (immutable config 'cluster-databases')".to_string())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-replica-no-failover",
        type_: ConfigType::Enum,
        enum_values: &["no", "yes", "if-empty"],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: Some("cluster-slave-no-failover"),
        get: |rcfg| {
            ConfigValue::String(rcfg.server.cluster_replica_no_failover.as_str().to_string())
        },
        set: |rcfg, val| {
            use crate::engine::extension::config::server::ClusterReplicaNoFailover;
            let value = val.as_str();
            rcfg.server.cluster_replica_no_failover = if value.eq_ignore_ascii_case("no") {
                ClusterReplicaNoFailover::No
            } else if value.eq_ignore_ascii_case("yes") {
                ClusterReplicaNoFailover::Yes
            } else if value.eq_ignore_ascii_case("if-empty") {
                ClusterReplicaNoFailover::IfEmpty
            } else {
                return Err("cluster-replica-no-failover must be no, yes or if-empty".to_string());
            };
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-replica-priority",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i32::MAX as i64,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(i64::from(rcfg.server.cluster_replica_priority)),
        set: |rcfg, val| {
            let value = val
                .as_i64()
                .ok_or("cluster-replica-priority must be numeric")?;
            if !(0..=i64::from(i32::MAX)).contains(&value) {
                return Err(format!("argument must be between 0 and {}", i32::MAX));
            }
            rcfg.server.cluster_replica_priority = value as u32;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-require-full-coverage",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.cluster_require_full_coverage),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.server.cluster_require_full_coverage = *v;
                Ok(())
            } else {
                Err("cluster-require-full-coverage must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-allow-reads-when-down",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.cluster_allow_reads_when_down),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.server.cluster_allow_reads_when_down = *v;
                Ok(())
            } else {
                Err("cluster-allow-reads-when-down must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-allow-pubsubshard-when-down",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.cluster_allow_pubsubshard_when_down),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.server.cluster_allow_pubsubshard_when_down = *v;
                Ok(())
            } else {
                Err("cluster-allow-pubsubshard-when-down must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-allow-replica-migration",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.cluster_allow_replica_migration),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.server.cluster_allow_replica_migration = *v;
                Ok(())
            } else {
                Err("cluster-allow-replica-migration must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-replica-validity-factor",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: Some("cluster-slave-validity-factor"),
        get: |rcfg| ConfigValue::Numeric(rcfg.server.cluster_replica_validity_factor),
        set: |rcfg, val| {
            let v = val
                .as_i64()
                .ok_or("cluster-replica-validity-factor must be numeric")?;
            rcfg.server.cluster_replica_validity_factor = v;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-migration-barrier",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.cluster_migration_barrier),
        set: |rcfg, val| {
            let v = val
                .as_i64()
                .ok_or("cluster-migration-barrier must be numeric")?;
            rcfg.server.cluster_migration_barrier = v;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-manual-failover-timeout",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.cluster_manual_failover_timeout),
        set: |rcfg, val| {
            let v = val
                .as_i64()
                .ok_or("cluster-manual-failover-timeout must be numeric")?;
            rcfg.server.cluster_manual_failover_timeout = v;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-link-sendbuf-limit",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.cluster_link_sendbuf_limit as i64),
        set: |rcfg, val| {
            let v = val
                .as_i64()
                .ok_or("cluster-link-sendbuf-limit must be numeric")?;
            rcfg.server.cluster_link_sendbuf_limit = v as u64;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-slot-migration-log-max-len",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.cluster_slot_migration_log_max_len as i64),
        set: |rcfg, val| {
            let v = val
                .as_i64()
                .ok_or("cluster-slot-migration-log-max-len must be numeric")?;
            rcfg.server.cluster_slot_migration_log_max_len = v as u64;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-preferred-endpoint-type",
        type_: ConfigType::Enum,
        enum_values: &["ip", "hostname", "unknown-endpoint"],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.cluster_preferred_endpoint_type.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                rcfg.server.cluster_preferred_endpoint_type = v.clone();
                Ok(())
            } else {
                Err("cluster-preferred-endpoint-type must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "slot-migration-max-failover-repl-bytes",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: i64::MIN,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.slot_migration_max_failover_repl_bytes),
        set: |rcfg, val| {
            let v = val
                .as_i64()
                .ok_or("slot-migration-max-failover-repl-bytes must be numeric")?;
            if v < -1 {
                return Err(format!("argument must be between -1 and {}", i64::MAX));
            }
            rcfg.server.slot_migration_max_failover_repl_bytes = v;
            Ok(())
        },
    });
}
