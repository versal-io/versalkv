use crate::engine::extension::config::spec::{ConfigSpec, ConfigType, ConfigValue};
use crate::engine::extension::config::spec_registry::ConfigRegistry;

pub(crate) fn register_persistence_configs(reg: &mut ConfigRegistry) {
    reg.register(&ConfigSpec {
        name: "forkless-infrastructure-enabled",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |config| ConfigValue::Bool(config.persistence.forkless_infrastructure_enabled),
        set: |config, value| {
            let ConfigValue::Bool(value) = value else {
                return Err("forkless-infrastructure-enabled must be a boolean".to_string());
            };
            config.persistence.forkless_infrastructure_enabled = *value;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "bgsave-default-method",
        type_: ConfigType::Enum,
        enum_values: &["fork", "forkless"],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |config| {
            ConfigValue::String(
                config
                    .persistence
                    .bgsave_default_method
                    .as_str()
                    .to_string(),
            )
        },
        set: |config, value| {
            let ConfigValue::String(value) = value else {
                return Err("bgsave-default-method must be an enum".to_string());
            };
            config.persistence.bgsave_default_method =
                crate::engine::extension::config::persistence::BgsaveMethod::parse(value)
                    .ok_or_else(|| "Invalid background save method".to_string())?;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "dir",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["protected"],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.persistence.dir.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                rcfg.persistence.dir = v.clone();
                Ok(())
            } else {
                Err("dir must be a string".to_string())
            }
        },
    });

    reg.register(&ConfigSpec {
        name: "dbfilename",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["protected"],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.persistence.dbfilename.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                rcfg.persistence.dbfilename = v.clone();
                Ok(())
            } else {
                Err("dbfilename must be a string".to_string())
            }
        },
    });

    reg.register(&ConfigSpec {
        name: "appendonly",
        type_: ConfigType::Special,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.persistence.aof_enabled),
        set: |_rcfg, _val| Ok(()),
    });

    reg.register(&ConfigSpec {
        name: "appendfsync",
        type_: ConfigType::Enum,
        enum_values: &["no", "everysec", "always"],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| {
            let s = match rcfg.persistence.aof_fsync {
                crate::foundation::io::persistence::AofFsync::No => "no",
                crate::foundation::io::persistence::AofFsync::EverySec => "everysec",
                crate::foundation::io::persistence::AofFsync::Always => "always",
            };
            ConfigValue::String(s.to_string())
        },
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                match v.as_str() {
                    "no" => {
                        rcfg.persistence.aof_fsync =
                            crate::foundation::io::persistence::AofFsync::No
                    }
                    "everysec" => {
                        rcfg.persistence.aof_fsync =
                            crate::foundation::io::persistence::AofFsync::EverySec
                    }
                    "always" => {
                        rcfg.persistence.aof_fsync =
                            crate::foundation::io::persistence::AofFsync::Always
                    }
                    _ => return Err("appendfsync must be one of: no, everysec, always".to_string()),
                }
                Ok(())
            } else {
                Err("appendfsync must be a string".to_string())
            }
        },
    });

    reg.register(&ConfigSpec {
        name: "appendfilename",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.persistence.aof_filename.clone()),
        set: |_rcfg, _val| {
            Err("ERR CONFIG SET failed (immutable config 'appendfilename')".to_string())
        },
    });
    reg.register(&ConfigSpec {
        name: "appenddirname",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.persistence.aof_appenddirname.clone()),
        set: |_rcfg, _val| {
            Err("ERR CONFIG SET failed (immutable config 'appenddirname')".to_string())
        },
    });
    reg.register(&ConfigSpec {
        name: "no-appendfsync-on-rewrite",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.persistence.aof_no_fsync_on_rewrite),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.persistence.aof_no_fsync_on_rewrite = *v;
                Ok(())
            } else {
                Err("no-appendfsync-on-rewrite must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "aof-use-rdb-preamble",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.persistence.aof_use_rdb_preamble),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.persistence.aof_use_rdb_preamble = *v;
                Ok(())
            } else {
                Err("aof-use-rdb-preamble must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "aof-timestamp-enabled",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.persistence.aof_timestamp_enabled),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.persistence.aof_timestamp_enabled = *v;
                Ok(())
            } else {
                Err("aof-timestamp-enabled must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "aof-disable-auto-gc",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.persistence.aof_disable_auto_gc),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.persistence.aof_disable_auto_gc = *v;
                Ok(())
            } else {
                Err("aof-disable-auto-gc must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "aof-load-truncated",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.persistence.aof_load_truncated),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.persistence.aof_load_truncated = *v;
                Ok(())
            } else {
                Err("aof-load-truncated must be a boolean".to_string())
            }
        },
    });

    reg.register(&ConfigSpec {
        name: "save",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| {
            if rcfg.persistence.saveparams.is_empty() {
                ConfigValue::String(String::new())
            } else {
                let s = rcfg
                    .persistence
                    .saveparams
                    .iter()
                    .map(|sp| format!("{} {}", sp.seconds, sp.changes))
                    .collect::<Vec<_>>()
                    .join(" ");
                ConfigValue::String(s)
            }
        },
        set: |rcfg, val| {
            let ConfigValue::String(value) = val else {
                return Err("save must be a string".to_string());
            };
            let tokens: Vec<&str> = value.split_whitespace().collect();
            if !tokens.len().is_multiple_of(2) {
                return Err("Invalid save parameters: odd number of tokens".to_string());
            }
            let mut saveparams = Vec::with_capacity(tokens.len() / 2);
            for pair in tokens.as_chunks::<2>().0 {
                let seconds = pair[0]
                    .parse::<i64>()
                    .map_err(|_| "Invalid save parameters".to_string())?;
                let changes = pair[1]
                    .parse::<i64>()
                    .map_err(|_| "Invalid save parameters".to_string())?;
                if seconds < 0 || changes < 0 {
                    return Err("Invalid save parameters".to_string());
                }
                saveparams.push(crate::engine::extension::config::persistence::SaveParam {
                    seconds,
                    changes,
                });
            }
            rcfg.persistence.saveparams = saveparams;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "rdbcompression",
        type_: ConfigType::Enum,
        enum_values: &["no", "yes", "lzf", "lz4", "zstd"],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.persistence.rdbcompression.as_str().to_string()),
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                if v.eq_ignore_ascii_case("zstd") {
                    return Err("Zstandard compression is not available in this build".to_string());
                }
                rcfg.persistence.rdbcompression =
                    crate::foundation::io::persistence::rdb::compression::RdbCompression::parse(v)
                        .ok_or_else(|| "Invalid RDB compression mode".to_string())?;
                Ok(())
            } else {
                Err("rdbcompression must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "rdb-version-check",
        type_: ConfigType::Enum,
        enum_values: &["strict", "relaxed"],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| {
            ConfigValue::String(
                if rcfg.persistence.rdb_version_check_relaxed {
                    "relaxed"
                } else {
                    "strict"
                }
                .to_string(),
            )
        },
        set: |rcfg, val| {
            let ConfigValue::String(value) = val else {
                return Err("rdb-version-check must be an enum".to_string());
            };
            rcfg.persistence.rdb_version_check_relaxed = value.eq_ignore_ascii_case("relaxed");
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "rdbchecksum",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.persistence.rdbchecksum),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.persistence.rdbchecksum = *v;
                Ok(())
            } else {
                Err("rdbchecksum must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "rdb-save-incremental-fsync",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.persistence.rdb_save_incremental_fsync),
        set: |rcfg, val| {
            if let ConfigValue::Bool(value) = val {
                rcfg.persistence.rdb_save_incremental_fsync = *value;
                Ok(())
            } else {
                Err("rdb-save-incremental-fsync must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "rdb-del-sync-files",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.persistence.rdb_del_sync_files),
        set: |rcfg, val| {
            if let ConfigValue::Bool(value) = val {
                rcfg.persistence.rdb_del_sync_files = *value;
                Ok(())
            } else {
                Err("rdb-del-sync-files must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "stop-writes-on-bgsave-error",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.persistence.stop_writes_on_bgsave_error),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.persistence.stop_writes_on_bgsave_error = *v;
                Ok(())
            } else {
                Err("stop-writes-on-bgsave-error must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "rdb-key-save-delay",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: i32::MIN as i64,
        numeric_max: i32::MAX as i64,
        flags: &["hidden"],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.persistence.rdb_key_save_delay),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.persistence.rdb_key_save_delay = *v;
                Ok(())
            } else {
                Err("rdb-key-save-delay must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "key-load-delay",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: i32::MIN as i64,
        numeric_max: i32::MAX as i64,
        flags: &["hidden"],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.persistence.key_load_delay),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.persistence.key_load_delay = *v;
                Ok(())
            } else {
                Err("key-load-delay must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "loading-process-events-interval-bytes",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1024,
        numeric_max: i32::MAX as i64,
        flags: &["hidden"],
        alias: None,
        get: |rcfg| {
            ConfigValue::Numeric(rcfg.persistence.loading_process_events_interval_bytes as i64)
        },
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.persistence.loading_process_events_interval_bytes = *v as u64;
                Ok(())
            } else {
                Err("loading-process-events-interval-bytes must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "auto-aof-rewrite-percentage",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.persistence.aof_rewrite_perc),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                if *v >= 0 {
                    rcfg.persistence.aof_rewrite_perc = *v;
                }
                Ok(())
            } else {
                Err("auto-aof-rewrite-percentage must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "auto-aof-rewrite-min-size",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &["memory"],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.persistence.aof_rewrite_min_size as i64),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                if *v >= 0 {
                    rcfg.persistence.aof_rewrite_min_size = *v as u64;
                }
                Ok(())
            } else {
                Err("auto-aof-rewrite-min-size must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "aof-rewrite-incremental-fsync",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.persistence.aof_rewrite_incremental_fsync),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.persistence.aof_rewrite_incremental_fsync = *v;
                Ok(())
            } else {
                Err("aof-rewrite-incremental-fsync must be a boolean".to_string())
            }
        },
    });
}

#[cfg(test)]
mod compression_tests {
    use crate::engine::extension::config::spec_registry::ConfigRegistry;
    use crate::engine::extension::config::RuntimeConfig;

    #[test]
    fn rdb_zstd_returns_unavailable_without_changing_mode() {
        let registry = ConfigRegistry::new();
        let mut config = RuntimeConfig::new();
        let spec = registry.lookup("rdbcompression").expect("RDB config");
        let previous = (spec.get)(&config).as_str();
        let error = registry
            .try_set(&mut config, "rdbcompression", "zstd")
            .expect("registered config")
            .expect_err("unsupported codec");
        assert!(error.contains("Zstandard compression is not available in this build"));
        assert_eq!((spec.get)(&config).as_str(), previous);
        for mode in ["no", "yes", "lzf", "lz4"] {
            registry
                .try_set(&mut config, "rdbcompression", mode)
                .expect("registered config")
                .expect("supported mode");
            assert_eq!((spec.get)(&config).as_str(), mode);
        }
    }
}
