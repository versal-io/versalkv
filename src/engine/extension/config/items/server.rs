use crate::engine::extension::config::server::{
    IO_THREADS_MAX, LOGLEVEL_ENUM_VALUES, MAXMEMORY_POLICY_ENUM_VALUES, SHUTDOWN_SIG_ENUM_VALUES,
};
use crate::engine::extension::config::spec::{ConfigSpec, ConfigType, ConfigValue};
use crate::engine::extension::config::spec_registry::ConfigRegistry;
use crate::foundation::io::sys::proctitle::{expand_proc_title_template, ProcTitleVars};

/// Unix socket perm max is octal 0777 = 511 decimal.
const UNIX_SOCKET_PERM_MAX: i64 = 0o777_i64;
/// C uses UINT_MAX for maxclients upper bound; we use a practical limit.
const MAXCLIENTS_MAX: i64 = i64::MAX;
/// C uses INT_MAX for databases upper bound.
const DATABASES_MAX: i64 = i32::MAX as i64;

fn parse_memory_limit(value: &str) -> Option<usize> {
    let value = value
        .strip_suffix('b')
        .or_else(|| value.strip_suffix('B'))
        .unwrap_or(value);
    let (digits, multiplier) = match value.as_bytes().last().copied()? {
        b'k' | b'K' => (&value[..value.len() - 1], 1_024usize),
        b'm' | b'M' => (&value[..value.len() - 1], 1_024usize.pow(2)),
        b'g' | b'G' => (&value[..value.len() - 1], 1_024usize.pow(3)),
        _ => (value, 1),
    };
    digits.parse::<usize>().ok()?.checked_mul(multiplier)
}

fn normalize_notify_keyspace_events(value: &str) -> Result<String, String> {
    if value.chars().any(|flag| {
        !matches!(
            flag,
            'A' | 'g'
                | '$'
                | 'l'
                | 's'
                | 'h'
                | 'z'
                | 'x'
                | 'e'
                | 'K'
                | 'E'
                | 't'
                | 'm'
                | 'd'
                | 'n'
                | 'p'
        )
    }) {
        return Err("Invalid event class character. Use 'Ag$lshzxeKEtmdnp'.".to_string());
    }
    let mut out = String::new();
    if value.contains('A') {
        out.push('A');
    } else {
        for flag in ['g', '$', 'l', 's', 'h', 'z', 'x', 'e', 't', 'd', 'p', 'n'] {
            if value.contains(flag) {
                out.push(flag);
            }
        }
    }
    for flag in ['K', 'E', 'm'] {
        if value.contains(flag) {
            out.push(flag);
        }
    }
    Ok(out)
}

pub(crate) fn register_server_configs(reg: &mut ConfigRegistry) {
    reg.register(&ConfigSpec {
        name: "server-cpulist",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: Some("server_cpulist"),
        get: |r| ConfigValue::String(r.server.server_cpulist.clone()),
        set: |r, v| {
            if let ConfigValue::String(v) = v {
                r.server.server_cpulist = v.clone();
                Ok(())
            } else {
                Err("server-cpulist must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "bio-cpulist",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: Some("bio_cpulist"),
        get: |r| ConfigValue::String(r.server.bio_cpulist.clone()),
        set: |r, v| {
            if let ConfigValue::String(v) = v {
                r.server.bio_cpulist = v.clone();
                Ok(())
            } else {
                Err("bio-cpulist must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "aof-rewrite-cpulist",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: Some("aof_rewrite_cpulist"),
        get: |r| ConfigValue::String(r.server.aof_rewrite_cpulist.clone()),
        set: |r, v| {
            if let ConfigValue::String(v) = v {
                r.server.aof_rewrite_cpulist = v.clone();
                Ok(())
            } else {
                Err("aof-rewrite-cpulist must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "bgsave-cpulist",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: Some("bgsave_cpulist"),
        get: |r| ConfigValue::String(r.server.bgsave_cpulist.clone()),
        set: |r, v| {
            if let ConfigValue::String(v) = v {
                r.server.bgsave_cpulist = v.clone();
                Ok(())
            } else {
                Err("bgsave-cpulist must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "supervised",
        type_: ConfigType::Enum,
        enum_values: &["no", "systemd", "upstart", "auto"],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.supervised.clone()),
        set: |rcfg, val| {
            let ConfigValue::String(value) = val else {
                return Err("supervised must be an enum".to_string());
            };
            let value = value.to_ascii_lowercase();
            if !["no", "systemd", "upstart", "auto"].contains(&value.as_str()) {
                return Err("supervised must be no, systemd, upstart, or auto".to_string());
            }
            rcfg.server.supervised = value;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "daemonize",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.daemonize),
        set: |rcfg, val| match val {
            ConfigValue::Bool(value) => {
                rcfg.server.daemonize = *value;
                Ok(())
            }
            _ => Err("daemonize must be a boolean".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "bind",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.bind.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(value) = val {
                rcfg.server.set_bind(value.clone());
            }
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "bind-source-addr",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.bind_source_addr.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(value) = val {
                if !value.is_empty() && value.parse::<std::net::IpAddr>().is_err() {
                    return Err("Invalid bind source address".to_string());
                }
                rcfg.server.bind_source_addr = value.clone();
            }
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "maxclients",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: MAXCLIENTS_MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.maxclients as i64),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                if *v < 1 {
                    return Err("maxclients must be >= 1".to_string());
                }
                rcfg.server.maxclients = *v as u32;
                Ok(())
            } else {
                Err("maxclients must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "max-new-connections-per-cycle",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: 1000,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.max_new_connections_per_cycle as i64),
        set: |rcfg, val| {
            let ConfigValue::Numeric(value) = val else {
                return Err("max-new-connections-per-cycle must be numeric".to_string());
            };
            rcfg.server.max_new_connections_per_cycle = *value as u16;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "max-new-tls-connections-per-cycle",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: 1000,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.max_new_tls_connections_per_cycle as i64),
        set: |rcfg, val| {
            let ConfigValue::Numeric(value) = val else {
                return Err("max-new-tls-connections-per-cycle must be numeric".to_string());
            };
            rcfg.server.max_new_tls_connections_per_cycle = *value as u16;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "timeout",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i32::MAX as i64,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.client_timeout as i64),
        set: |rcfg, val| {
            let ConfigValue::Numeric(value) = val else {
                return Err("timeout must be numeric".to_string());
            };
            rcfg.server.client_timeout = *value as u64;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "tcp-backlog",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i32::MAX as i64,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.tcp_backlog as i64),
        set: |rcfg, val| {
            let ConfigValue::Numeric(value) = val else {
                return Err("tcp-backlog must be numeric".to_string());
            };
            rcfg.server.tcp_backlog = *value as u32;
            Ok(())
        },
    });

    reg.register(&ConfigSpec {
        name: "databases",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: DATABASES_MAX,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.databases),
        set: |_rcfg, _val| Err("ERR CONFIG SET failed (immutable config 'databases')".to_string()),
    });

    reg.register(&ConfigSpec {
        name: "port",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 65535,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.listening_port as i64),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                if *v == rcfg.server.listening_port as i64 {
                    Ok(())
                } else {
                    Err("ERR CONFIG SET failed (possibly related to argument 'port') - Unable to listen on this port".to_string())
                }
            } else {
                Err("port must be numeric".to_string())
            }
        },
    });

    reg.register(&ConfigSpec {
        name: "lazyfree-lazy-eviction",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.lazyfree_lazy_eviction),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.server.lazyfree_lazy_eviction = *v;
                Ok(())
            } else {
                Err("lazyfree-lazy-eviction must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "lazyfree-lazy-server-del",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.lazyfree_lazy_server_del),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.server.lazyfree_lazy_server_del = *v;
                Ok(())
            } else {
                Err("lazyfree-lazy-server-del must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "lazyfree-lazy-user-del",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.lazyfree_lazy_user_del),
        set: |rcfg, val| {
            let ConfigValue::Bool(value) = val else {
                return Err("lazyfree-lazy-user-del must be a boolean".to_string());
            };
            rcfg.server.lazyfree_lazy_user_del = *value;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "lazyfree-lazy-user-flush",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.lazyfree_lazy_user_flush),
        set: |rcfg, val| {
            let ConfigValue::Bool(value) = val else {
                return Err("lazyfree-lazy-user-flush must be a boolean".to_string());
            };
            rcfg.server.lazyfree_lazy_user_flush = *value;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "lazyfree-lazy-expire",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.lazyfree_lazy_expire),
        set: |rcfg, val| {
            let ConfigValue::Bool(value) = val else {
                return Err("lazyfree-lazy-expire must be a boolean".to_string());
            };
            rcfg.server.lazyfree_lazy_expire = *value;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "active-expire-effort",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: 10,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.active_expire_effort as i64),
        set: |rcfg, val| {
            let ConfigValue::Numeric(value) = val else {
                return Err("active-expire-effort must be numeric".to_string());
            };
            rcfg.server.active_expire_effort = *value as u8;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "activedefrag",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.activedefrag),
        set: |rcfg, value| {
            let ConfigValue::Bool(value) = value else {
                return Err("activedefrag must be a boolean".to_string());
            };
            rcfg.server.activedefrag = *value;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "active-defrag-cycle-min",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: 99,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.active_defrag_cycle_min as i64),
        set: |rcfg, value| {
            let ConfigValue::Numeric(value) = value else {
                return Err("active-defrag-cycle-min must be numeric".to_string());
            };
            rcfg.server.active_defrag_cycle_min = *value as u8;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "active-defrag-cycle-max",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: 99,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.active_defrag_cycle_max as i64),
        set: |rcfg, value| {
            let ConfigValue::Numeric(value) = value else {
                return Err("active-defrag-cycle-max must be numeric".to_string());
            };
            rcfg.server.active_defrag_cycle_max = *value as u8;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "active-defrag-threshold-lower",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 1000,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.active_defrag_threshold_lower as i64),
        set: |rcfg, value| {
            let ConfigValue::Numeric(value) = value else {
                return Err("active-defrag-threshold-lower must be numeric".to_string());
            };
            rcfg.server.active_defrag_threshold_lower = *value as u16;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "active-defrag-threshold-upper",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 1000,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.active_defrag_threshold_upper as i64),
        set: |rcfg, value| {
            let ConfigValue::Numeric(value) = value else {
                return Err("active-defrag-threshold-upper must be numeric".to_string());
            };
            rcfg.server.active_defrag_threshold_upper = *value as u16;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "active-defrag-cycle-us",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 100_000,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.active_defrag_cycle_us as i64),
        set: |rcfg, value| {
            let ConfigValue::Numeric(value) = value else {
                return Err("active-defrag-cycle-us must be numeric".to_string());
            };
            rcfg.server.active_defrag_cycle_us = *value as u64;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "active-defrag-ignore-bytes",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.active_defrag_ignore_bytes as i64),
        set: |rcfg, value| {
            let ConfigValue::Numeric(value) = value else {
                return Err("active-defrag-ignore-bytes must be numeric".to_string());
            };
            if *value <= 0 || *value as u64 > usize::MAX as u64 {
                return Err("active-defrag-ignore-bytes is out of range".to_string());
            }
            rcfg.server.active_defrag_ignore_bytes = *value as usize;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "active-defrag-max-scan-fields",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.active_defrag_max_scan_fields as i64),
        set: |rcfg, value| {
            let ConfigValue::Numeric(value) = value else {
                return Err("active-defrag-max-scan-fields must be numeric".to_string());
            };
            if *value <= 0 || *value as u64 > usize::MAX as u64 {
                return Err("active-defrag-max-scan-fields is out of range".to_string());
            }
            rcfg.server.active_defrag_max_scan_fields = *value as usize;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "notify-keyspace-events",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.notify_keyspace_events.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                rcfg.server.notify_keyspace_events = normalize_notify_keyspace_events(v)?;
                Ok(())
            } else {
                Err("notify-keyspace-events must be a string".to_string())
            }
        },
    });

    reg.register(&ConfigSpec {
        name: "availability-zone",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.availability_zone.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                rcfg.server.availability_zone = v.clone();
                Ok(())
            } else {
                Err("availability-zone must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "loglevel",
        type_: ConfigType::Enum,
        enum_values: LOGLEVEL_ENUM_VALUES,
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.loglevel.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                rcfg.server.loglevel = v.clone();
                Ok(())
            } else {
                Err("loglevel must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "hz",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i32::MAX as i64,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.hz as i64),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(value) = val {
                rcfg.server.hz = (*value as i32).clamp(1, 500);
                Ok(())
            } else {
                Err("hz must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "import-mode",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.import_mode),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.server.import_mode = *v;
                Ok(())
            } else {
                Err("import-mode must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "maxmemory",
        type_: ConfigType::Special,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::UNumeric(rcfg.server.maxmemory),
        set: |_rcfg, _val| Ok(()),
    });
    reg.register(&ConfigSpec {
        name: "maxmemory-scripts",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| {
            ConfigValue::String(
                crate::engine::extension::config::server::scripts_memory::format(
                    rcfg.server.maxmemory_scripts,
                ),
            )
        },
        set: |rcfg, val| {
            let ConfigValue::String(value) = val else {
                return Err("argument must be a memory or percent value".to_string());
            };
            rcfg.server.maxmemory_scripts =
                crate::engine::extension::config::server::scripts_memory::parse(value)?;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "maxmemory-policy",
        type_: ConfigType::Enum,
        enum_values: MAXMEMORY_POLICY_ENUM_VALUES,
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.maxmemory_policy.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                rcfg.server.maxmemory_policy = v.clone();
                Ok(())
            } else {
                Err("maxmemory-policy must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "maxmemory-eviction-tenacity",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 100,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.maxmemory_eviction_tenacity),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.server.maxmemory_eviction_tenacity = *v;
                Ok(())
            } else {
                Err("maxmemory-eviction-tenacity must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "maxmemory-samples",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: 64,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.maxmemory_samples),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(value) = val {
                rcfg.server.maxmemory_samples = *value;
                Ok(())
            } else {
                Err("maxmemory-samples must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "lfu-log-factor",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i32::MAX as i64,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.lfu_log_factor),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(value) = val {
                rcfg.server.lfu_log_factor = *value;
                Ok(())
            } else {
                Err("lfu-log-factor must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "lfu-decay-time",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i32::MAX as i64,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.lfu_decay_time),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(value) = val {
                rcfg.server.lfu_decay_time = *value;
                Ok(())
            } else {
                Err("lfu-decay-time must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "maxmemory-clients",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.maxmemory_clients.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                if let Some(percent) = v.strip_suffix('%') {
                    if percent.parse::<u64>().unwrap_or(101) > 100 {
                        return Err("percentage argument must be less or equal to 100".to_string());
                    }
                }
                rcfg.server.maxmemory_clients = v.clone();
                Ok(())
            } else {
                Err("maxmemory-clients must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "busy-reply-threshold",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: Some("lua-time-limit"),
        get: |rcfg| ConfigValue::Numeric(rcfg.server.lua_time_limit as i64),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(n) = val {
                rcfg.server.lua_time_limit = *n as u64;
                Ok(())
            } else {
                Err("ERR invalid value type".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "lua-enable-insecure-api",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["hidden", "protected"],
        alias: Some("lua-enable-deprecated-api"),
        get: |rcfg| ConfigValue::Bool(rcfg.server.lua_enable_insecure_api),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.server.lua_enable_insecure_api = *v;
                Ok(())
            } else {
                Err("lua-enable-insecure-api must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "min-replicas-to-write",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: Some("min-slaves-to-write"),
        get: |rcfg| ConfigValue::Numeric(rcfg.replication.min_replicas_to_write),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(n) = val {
                rcfg.replication.min_replicas_to_write = *n;
                Ok(())
            } else {
                Err("ERR invalid value type".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "min-replicas-max-lag",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: Some("min-slaves-max-lag"),
        get: |rcfg| ConfigValue::Numeric(rcfg.replication.min_replicas_max_lag),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(n) = val {
                rcfg.replication.min_replicas_max_lag = *n;
                Ok(())
            } else {
                Err("ERR invalid value type".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "replica-serve-stale-data",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: Some("slave-serve-stale-data"),
        get: |rcfg| ConfigValue::Bool(rcfg.replication.replica_serve_stale_data),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.replication.replica_serve_stale_data = *v;
                Ok(())
            } else {
                Err("replica-serve-stale-data must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "io-threads",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: IO_THREADS_MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.io_threads),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.server.io_threads = *v;
            }
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "io-threads-always-active",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["hidden"],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.io_threads_always_active),
        set: |rcfg, val| {
            let ConfigValue::Bool(value) = val else {
                return Err("io-threads-always-active must be a boolean".to_string());
            };
            rcfg.server.io_threads_always_active = *value;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "prefetch-batch-max-size",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 128,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.prefetch_batch_max_size as i64),
        set: |rcfg, val| {
            let ConfigValue::Numeric(value) = val else {
                return Err("prefetch-batch-max-size must be numeric".to_string());
            };
            if !(0..=128).contains(value) {
                return Err("prefetch-batch-max-size must be between 0 and 128".to_string());
            }
            rcfg.server.prefetch_batch_max_size = *value as usize;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "ignore-warnings",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.ignore_warnings.clone()),
        set: |rcfg, value| {
            let ConfigValue::String(value) = value else {
                return Err("ignore-warnings must be a string".to_string());
            };
            rcfg.server.ignore_warnings = value.clone();
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "client-output-buffer-limit",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| {
            ConfigValue::String(format!(
                "normal {} {} {} slave {} {} {} pubsub {} {} {}",
                rcfg.replication.normal_output_buffer_hard_limit,
                rcfg.replication.normal_output_buffer_soft_limit,
                rcfg.replication.normal_output_buffer_soft_limit_seconds,
                rcfg.replication.replica_output_buffer_hard_limit,
                rcfg.replication.replica_output_buffer_soft_limit,
                rcfg.replication.replica_output_buffer_soft_limit_seconds,
                rcfg.replication.pubsub_output_buffer_hard_limit,
                rcfg.replication.pubsub_output_buffer_soft_limit,
                rcfg.replication.pubsub_output_buffer_soft_limit_seconds,
            ))
        },
        set: |rcfg, val| {
            let ConfigValue::String(value) = val else {
                return Err("client-output-buffer-limit must be a string".to_string());
            };
            let fields: Vec<_> = value.split_whitespace().collect();
            if fields.is_empty() || !fields.len().is_multiple_of(4) {
                return Err("Wrong number of arguments".to_string());
            }
            let mut replica_limits = None;
            for chunk in fields.as_chunks::<4>().0 {
                let class = chunk[0];
                if !matches!(
                    class.to_ascii_lowercase().as_str(),
                    "normal" | "replica" | "slave" | "pubsub"
                ) {
                    return Err(format!("Invalid client class '{}'", class));
                }
                let hard_limit = parse_memory_limit(chunk[1]).ok_or_else(|| {
                    format!("Error client-output-buffer-limit {class} hard limit is invalid")
                })?;
                let soft_limit = parse_memory_limit(chunk[2]).ok_or_else(|| {
                    format!("Error client-output-buffer-limit {class} soft limit is invalid")
                })?;
                let soft_seconds = chunk[3].parse::<u64>().map_err(|_| {
                    format!("Error client-output-buffer-limit {class} soft_seconds is invalid")
                })?;
                if matches!(class.to_ascii_lowercase().as_str(), "replica" | "slave") {
                    replica_limits = Some((hard_limit, soft_limit, soft_seconds));
                }
                match class.to_ascii_lowercase().as_str() {
                    "normal" => {
                        rcfg.replication.normal_output_buffer_hard_limit = hard_limit;
                        rcfg.replication.normal_output_buffer_soft_limit = soft_limit;
                        rcfg.replication.normal_output_buffer_soft_limit_seconds = soft_seconds;
                    }
                    "pubsub" => {
                        rcfg.replication.pubsub_output_buffer_hard_limit = hard_limit;
                        rcfg.replication.pubsub_output_buffer_soft_limit = soft_limit;
                        rcfg.replication.pubsub_output_buffer_soft_limit_seconds = soft_seconds;
                    }
                    _ => {}
                }
            }
            if let Some((hard_limit, soft_limit, soft_seconds)) = replica_limits {
                rcfg.replication.replica_output_buffer_hard_limit = hard_limit;
                rcfg.replication.replica_output_buffer_soft_limit = soft_limit;
                rcfg.replication.replica_output_buffer_soft_limit_seconds = soft_seconds;
            }
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "unixsocket",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.unixsocket.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                rcfg.server.unixsocket = v.clone();
            }
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "unixsocketgroup",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.unixsocketgroup.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                rcfg.server.unixsocketgroup = v.clone();
            }
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "hash-seed",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable", "hidden"],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.hash_seed.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                if v.len() > 256 {
                    return Err(
                        "hash-seed must be less than or equal to 256 characters".to_string()
                    );
                }
                rcfg.server.hash_seed = v.clone();
            }
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "unixsocketperm",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: UNIX_SOCKET_PERM_MAX,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.unixsocketperm as i64),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.server.unixsocketperm = *v as u32;
            }
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "min-string-size-avoid-copy-reply",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |_rcfg| ConfigValue::Numeric(16384),
        set: |_rcfg, _val| Ok(()),
    });
    reg.register(&ConfigSpec {
        name: "min-string-size-avoid-copy-reply-threaded",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |_rcfg| ConfigValue::Numeric(65536),
        set: |_rcfg, _val| Ok(()),
    });
    reg.register(&ConfigSpec {
        name: "min-io-threads-avoid-copy-reply",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |_rcfg| ConfigValue::Numeric(0),
        set: |_rcfg, _val| Ok(()),
    });
    reg.register(&ConfigSpec {
        name: "enable-module-command",
        type_: ConfigType::Enum,
        enum_values: &["no", "yes", "local"],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.enable_module_command.as_str().to_string()),
        set: |_rcfg, _val| {
            Err("ERR CONFIG SET failed (immutable config 'enable-module-command')".to_string())
        },
    });
    reg.register(&ConfigSpec {
        name: "enable-protected-configs",
        type_: ConfigType::Enum,
        enum_values: &["no", "yes", "local"],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.enable_protected_configs.as_str().to_string()),
        set: |_rcfg, _val| {
            Err("ERR CONFIG SET failed (immutable config 'enable-protected-configs')".to_string())
        },
    });
    reg.register(&ConfigSpec {
        name: "enable-debug-command",
        type_: ConfigType::Enum,
        enum_values: &["no", "yes", "local"],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.enable_debug_command.as_str().to_string()),
        set: |_rcfg, _val| {
            Err("ERR CONFIG SET failed (immutable config 'enable-debug-command')".to_string())
        },
    });
    reg.register(&ConfigSpec {
        name: "protected-mode",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.protected_mode),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.server.protected_mode = *v;
                Ok(())
            } else {
                Err("protected-mode must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "shutdown-timeout",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.shutdown_timeout as i64),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.server.shutdown_timeout = (*v).max(0) as u64;
                Ok(())
            } else {
                Err("shutdown-timeout must be a numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "shutdown-on-sigterm",
        type_: ConfigType::String,
        enum_values: SHUTDOWN_SIG_ENUM_VALUES,
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| {
            ConfigValue::String(crate::engine::distributed::shutdown::format_sig_flags(
                rcfg.server.shutdown_on_sigterm,
            ))
        },
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                match crate::engine::distributed::shutdown::parse_sig_flags(v) {
                    Some(bits) => {
                        rcfg.server.shutdown_on_sigterm = bits;
                        Ok(())
                    }
                    None => Err("argument couldn't be parsed into flags".to_string()),
                }
            } else {
                Err("shutdown-on-sigterm must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "shutdown-on-sigint",
        type_: ConfigType::String,
        enum_values: SHUTDOWN_SIG_ENUM_VALUES,
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| {
            ConfigValue::String(crate::engine::distributed::shutdown::format_sig_flags(
                rcfg.server.shutdown_on_sigint,
            ))
        },
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                match crate::engine::distributed::shutdown::parse_sig_flags(v) {
                    Some(bits) => {
                        rcfg.server.shutdown_on_sigint = bits;
                        Ok(())
                    }
                    None => Err("argument couldn't be parsed into flags".to_string()),
                }
            } else {
                Err("shutdown-on-sigint must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "oom-score-adj",
        type_: ConfigType::Special,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| {
            let s = match rcfg.server.oom_score_adj {
                crate::engine::extension::config::server::OOM_SCORE_ADJ_NO => "no",
                // C's enum keeps `relative` as an accepted alias but reports
                // its canonical first spelling, `yes`, through CONFIG GET.
                crate::engine::extension::config::server::OOM_SCORE_RELATIVE => "yes",
                crate::engine::extension::config::server::OOM_SCORE_ADJ_ABSOLUTE => "absolute",
                _ => "no",
            };
            ConfigValue::String(s.to_string())
        },
        set: |_rcfg, _val| Ok(()),
    });
    reg.register(&ConfigSpec {
        name: "oom-score-adj-values",
        type_: ConfigType::Special,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| {
            let v = &rcfg.server.oom_score_adj_values;
            ConfigValue::String(format!("{} {} {}", v[0], v[1], v[2]))
        },
        set: |_rcfg, _val| Ok(()),
    });
    reg.register(&ConfigSpec {
        name: "mptcp",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.mptcp),
        set: |rcfg, val| match val {
            ConfigValue::Bool(value) => {
                rcfg.server.mptcp = *value;
                Ok(())
            }
            _ => Err("mptcp must be a boolean".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "pidfile",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.pidfile.clone()),
        set: |rcfg, val| match val {
            ConfigValue::String(value) => {
                rcfg.server.pidfile.clone_from(value);
                Ok(())
            }
            _ => Err("pidfile must be a string".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "extended-redis-compatibility",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.extended_redis_compatibility),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.server.extended_redis_compatibility = *v;
                Ok(())
            } else {
                Err("extended-redis-compatibility must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "always-show-logo",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.always_show_logo),
        set: |rcfg, val| match val {
            ConfigValue::Bool(value) => {
                rcfg.server.always_show_logo = *value;
                Ok(())
            }
            _ => Err("always-show-logo must be a boolean".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "set-proc-title",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.set_proc_title),
        set: |rcfg, val| match val {
            ConfigValue::Bool(value) => {
                rcfg.server.set_proc_title = *value;
                Ok(())
            }
            _ => Err("set-proc-title must be a boolean".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "proc-title-template",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.proc_title_template.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(s) = val {
                // Validate template: unknown variables or empty after expansion
                // are rejected (mirrors C's validateProcTitleTemplate).
                match expand_proc_title_template(s, &ProcTitleVars::validation()) {
                    Some(res) if !res.trim().is_empty() => {
                        rcfg.server.proc_title_template = s.clone();
                        crate::engine::extension::config::server::refresh_proc_title(&rcfg.server);
                        Ok(())
                    }
                    _ => Err(
                        "ERR CONFIG SET failed (possibly related to argument 'proc-title-template') - template format is invalid or unsafe".to_string()
                    ),
                }
            } else {
                Err("proc-title-template must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "activerehashing",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.active_rehashing),
        set: |rcfg, value| {
            let ConfigValue::Bool(value) = value else {
                return Err("activerehashing must be a boolean".to_string());
            };
            rcfg.server.active_rehashing = *value;
            Ok(())
        },
    });

    reg.register(&ConfigSpec {
        name: "hll-sparse-max-bytes",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.hll_sparse_max_bytes),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.server.hll_sparse_max_bytes = *v;
                Ok(())
            } else {
                Err("hll-sparse-max-bytes must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "tracking-table-max-keys",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.tracking_table_max_keys as i64),
        set: |rcfg, val| {
            let ConfigValue::Numeric(value) = val else {
                return Err("tracking-table-max-keys must be numeric".to_string());
            };
            rcfg.server.tracking_table_max_keys = *value as usize;
            Ok(())
        },
    });
}
