use crate::engine::extension::config::spec::{ConfigSpec, ConfigType, ConfigValue};
use crate::engine::extension::config::spec_registry::ConfigRegistry;

pub(crate) fn register_replication_configs(reg: &mut ConfigRegistry) {
    reg.register(&ConfigSpec {
        name: "replica-announce-ip",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: Some("slave-announce-ip"),
        get: |rcfg| ConfigValue::String(rcfg.replication.replica_announce_ip.clone()),
        set: |rcfg, val| match val {
            ConfigValue::String(value) => {
                rcfg.replication.replica_announce_ip.clone_from(value);
                Ok(())
            }
            _ => Err("replica-announce-ip must be a string".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "replica-announced",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.replication.replica_announced),
        set: |rcfg, val| match val {
            ConfigValue::Bool(value) => {
                rcfg.replication.replica_announced = *value;
                Ok(())
            }
            _ => Err("replica-announced must be a boolean".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "replica-announce-port",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 65535,
        flags: &[],
        alias: Some("slave-announce-port"),
        get: |rcfg| ConfigValue::Numeric(rcfg.replication.replica_announce_port as i64),
        set: |rcfg, val| match val {
            ConfigValue::Numeric(value) => {
                rcfg.replication.replica_announce_port = *value as u16;
                Ok(())
            }
            _ => Err("replica-announce-port must be numeric".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "replica-priority",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: Some("slave-priority"),
        get: |rcfg| ConfigValue::Numeric(rcfg.replication.replica_priority),
        set: |rcfg, val| match val {
            ConfigValue::Numeric(value) => {
                rcfg.replication.replica_priority = *value;
                Ok(())
            }
            _ => Err("replica-priority must be numeric".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "repl-ping-replica-period",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: i64::MAX,
        flags: &[],
        alias: Some("repl-ping-slave-period"),
        get: |rcfg| ConfigValue::Numeric(rcfg.replication.repl_ping_replica_period as i64),
        set: |rcfg, val| match val {
            ConfigValue::Numeric(value) => {
                rcfg.replication.repl_ping_replica_period = *value as u64;
                Ok(())
            }
            _ => Err("repl-ping-replica-period must be numeric".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "repl-timeout",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.replication.repl_timeout as i64),
        set: |rcfg, val| match val {
            ConfigValue::Numeric(value) => {
                rcfg.replication.repl_timeout = *value as u64;
                Ok(())
            }
            _ => Err("repl-timeout must be numeric".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "repl-diskless-sync",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.replication.repl_diskless_sync),
        set: |rcfg, val| match val {
            ConfigValue::Bool(value) => {
                rcfg.replication.repl_diskless_sync = *value;
                Ok(())
            }
            _ => Err("repl-diskless-sync must be a boolean".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "repl-diskless-load",
        type_: ConfigType::Enum,
        enum_values: &["disabled", "on-empty-db", "swapdb", "flush-before-load"],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.replication.repl_diskless_load.clone()),
        set: |rcfg, val| match val {
            ConfigValue::String(value) => {
                rcfg.replication.repl_diskless_load.clone_from(value);
                Ok(())
            }
            _ => Err("repl-diskless-load must be a string".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "repl-diskless-sync-delay",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.replication.repl_diskless_sync_delay),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.replication.repl_diskless_sync_delay = *v;
                rcfg.replication.repl_diskless_sync_delay_explicit = true;
                Ok(())
            } else {
                Err("repl-diskless-sync-delay must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "repl-diskless-sync-max-replicas",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.replication.repl_diskless_sync_max_replicas),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.replication.repl_diskless_sync_max_replicas = *v;
                Ok(())
            } else {
                Err("repl-diskless-sync-max-replicas must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "repl-backlog-size",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &["memory"],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.repl_backlog_size),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.server.repl_backlog_size = *v;
                Ok(())
            } else {
                Err("repl-backlog-size must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "repl-backlog-ttl",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.replication.repl_backlog_ttl),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(value) = val {
                rcfg.replication.repl_backlog_ttl = *value;
                Ok(())
            } else {
                Err("repl-backlog-ttl must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "replica-read-only",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: Some("slave-read-only"),
        get: |rcfg| ConfigValue::Bool(rcfg.replication.replica_read_only),
        set: |rcfg, val| match val {
            ConfigValue::Bool(value) => {
                rcfg.replication.replica_read_only = *value;
                Ok(())
            }
            _ => Err("replica-read-only must be a boolean".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "replica-ignore-maxmemory",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: Some("slave-ignore-maxmemory"),
        get: |rcfg| ConfigValue::Bool(rcfg.replication.replica_ignore_maxmemory),
        set: |rcfg, val| match val {
            ConfigValue::Bool(value) => {
                rcfg.replication.replica_ignore_maxmemory = *value;
                Ok(())
            }
            _ => Err("replica-ignore-maxmemory must be a boolean".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "replica-ignore-disk-write-errors",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.replication.replica_ignore_disk_write_errors),
        set: |rcfg, val| match val {
            ConfigValue::Bool(value) => {
                rcfg.replication.replica_ignore_disk_write_errors = *value;
                Ok(())
            }
            _ => Err("replica-ignore-disk-write-errors must be a boolean".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "replica-lazy-flush",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: Some("slave-lazy-flush"),
        get: |rcfg| ConfigValue::Bool(rcfg.replication.replica_lazy_flush),
        set: |rcfg, val| match val {
            ConfigValue::Bool(value) => {
                rcfg.replication.replica_lazy_flush = *value;
                Ok(())
            }
            _ => Err("replica-lazy-flush must be a boolean".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "repl-disable-tcp-nodelay",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.replication.repl_disable_tcp_nodelay),
        set: |rcfg, val| match val {
            ConfigValue::Bool(value) => {
                rcfg.replication.repl_disable_tcp_nodelay = *value;
                Ok(())
            }
            _ => Err("repl-disable-tcp-nodelay must be a boolean".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "repl-mptcp",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.replication.repl_mptcp),
        set: |rcfg, val| match val {
            ConfigValue::Bool(value) => {
                rcfg.replication.repl_mptcp = *value;
                Ok(())
            }
            _ => Err("repl-mptcp must be a boolean".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "dual-channel-replication-enabled",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.replication.dual_channel_replication),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.replication.dual_channel_replication = *v;
                Ok(())
            } else {
                Err("dual-channel-replication-enabled must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "propagation-error-behavior",
        type_: ConfigType::Enum,
        enum_values: &["ignore", "panic", "panic-on-replicas"],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.replication.propagation_error_behavior.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(value) = val {
                rcfg.replication
                    .propagation_error_behavior
                    .clone_from(value);
                Ok(())
            } else {
                Err("propagation-error-behavior must be an enum".to_string())
            }
        },
    });
}
