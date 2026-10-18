use crate::engine::extension::config::spec::{ConfigSpec, ConfigType, ConfigValue};
use crate::engine::extension::config::spec_registry::ConfigRegistry;

pub(crate) fn register_logs_configs(reg: &mut ConfigRegistry) {
    reg.register(&ConfigSpec {
        name: "syslog-enabled",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |r| ConfigValue::Bool(r.server.syslog_enabled),
        set: |r, v| {
            if let ConfigValue::Bool(v) = v {
                r.server.syslog_enabled = *v;
                Ok(())
            } else {
                Err("syslog-enabled must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "syslog-ident",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |r| ConfigValue::String(r.server.syslog_ident.clone()),
        set: |r, v| {
            if let ConfigValue::String(v) = v {
                r.server.syslog_ident = v.clone();
                Ok(())
            } else {
                Err("syslog-ident must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "syslog-facility",
        type_: ConfigType::Enum,
        enum_values: &[
            "user", "local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7",
        ],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["immutable"],
        alias: None,
        get: |r| ConfigValue::String(r.server.syslog_facility.clone()),
        set: |r, v| {
            if let ConfigValue::String(v) = v {
                let v = v.to_ascii_lowercase();
                if ![
                    "user", "local0", "local1", "local2", "local3", "local4", "local5", "local6",
                    "local7",
                ]
                .contains(&v.as_str())
                {
                    return Err("invalid syslog-facility".to_string());
                }
                r.server.syslog_facility = v;
                Ok(())
            } else {
                Err("syslog-facility must be an enum".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "debug-context",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["hidden"],
        alias: None,
        get: |r| ConfigValue::String(r.server.debug_context.clone()),
        set: |r, v| {
            if let ConfigValue::String(v) = v {
                r.server.debug_context = v.clone();
                crate::foundation::io::sys::log::set_debug_context(v.clone());
                Ok(())
            } else {
                Err("debug-context must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "crash-log-enabled",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |r| ConfigValue::Bool(r.server.crash_log_enabled),
        set: |r, v| {
            if let ConfigValue::Bool(v) = v {
                r.server.crash_log_enabled = *v;
                crate::foundation::io::sys::log::set_crash_log_enabled(*v);
                Ok(())
            } else {
                Err("crash-log-enabled must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "log-timestamp-format",
        type_: ConfigType::Enum,
        enum_values: &["legacy", "iso8601", "milliseconds"],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.log_timestamp_format.clone()),
        set: |rcfg, val| {
            let ConfigValue::String(value) = val else {
                return Err("log-timestamp-format must be an enum".to_string());
            };
            let value = value.to_ascii_lowercase();
            if !["legacy", "iso8601", "milliseconds"].contains(&value.as_str()) {
                return Err(
                    "log-timestamp-format must be legacy, iso8601, or milliseconds".to_string(),
                );
            }
            rcfg.server.log_timestamp_format = value.clone();
            crate::foundation::io::sys::log::set_log_timestamp_format(
                crate::foundation::io::sys::log::LogTimestampFormat::parse(&value),
            );
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "log-format",
        type_: ConfigType::Enum,
        enum_values: &["legacy", "logfmt", "json"],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.log_format.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                let v = v.to_ascii_lowercase();
                if !["legacy", "logfmt", "json"].contains(&v.as_str()) {
                    return Err("log-format must be legacy, logfmt, or json".to_string());
                }
                crate::foundation::io::sys::log::set_log_format(
                    crate::foundation::io::sys::log::LogFormat::parse(&v),
                );
                rcfg.server.log_format = v;
                Ok(())
            } else {
                Err("log-format must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "hide-user-data-from-log",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.hide_user_data_from_log),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.server.hide_user_data_from_log = *v;
                Ok(())
            } else {
                Err("hide-user-data-from-log must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "watchdog-period",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.watchdog_period as i64),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                if *v < 0 {
                    return Err("watchdog-period must be non-negative".to_string());
                }
                let requested = *v as u64;
                // C debug.c applyWatchdogPeriod keeps the one-shot alarm at
                // least two cron ticks away; shorter values can expire before
                // the event loop has a chance to refresh it.
                let min_period_ms = (1_000 / rcfg.server.hz.max(1) as u64) * 2;
                rcfg.server.watchdog_period = if requested == 0 {
                    0
                } else {
                    requested.max(min_period_ms)
                };
                Ok(())
            } else {
                Err("watchdog-period must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "crash-memcheck-enabled",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.crash_memcheck_enabled),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.server.crash_memcheck_enabled = *v;
                Ok(())
            } else {
                Err("crash-memcheck-enabled must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "use-exit-on-panic",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.use_exit_on_panic),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.server.use_exit_on_panic = *v;
                crate::foundation::io::sys::log::set_use_exit_on_panic(*v);
                Ok(())
            } else {
                Err("use-exit-on-panic must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "acllog-max-len",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.acllog_max_len as i64),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                if *v < 0 {
                    return Err("argument must be a non-negative integer".to_string());
                }
                rcfg.server.acllog_max_len = *v as usize;
                Ok(())
            } else {
                Err("acllog-max-len must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "acl-pubsub-default",
        type_: ConfigType::Enum,
        enum_values: &["allchannels", "resetchannels"],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.acl_pubsub_default.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(s) = val {
                match s.to_ascii_lowercase().as_str() {
                    "allchannels" | "resetchannels" => {
                        rcfg.server.acl_pubsub_default = s.to_ascii_lowercase();
                        Ok(())
                    }
                    _ => Err(
                        "acl-pubsub-default must be 'allchannels' or 'resetchannels'".to_string(),
                    ),
                }
            } else {
                Err("acl-pubsub-default must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "aclfile",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        // ACL files are loaded during startup assembly. Runtime replacement
        // would leave the active ACL registry stale, so match C's immutable
        // config contract instead of accepting a no-op CONFIG SET.
        flags: &["immutable"],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.aclfile.clone()),
        set: |rcfg, val| {
            if let ConfigValue::String(value) = val {
                rcfg.server.aclfile = value.clone();
                Ok(())
            } else {
                Err("aclfile must be a string".to_string())
            }
        },
    });

    reg.register(&ConfigSpec {
        name: "commandlog-execution-slower-than",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: i64::MIN,
        numeric_max: i64::MAX,
        flags: &[],
        // Valkey's historical SLOWLOG spelling is an alias of the commandlog
        // slow category, not a second independently configurable threshold.
        alias: Some("slowlog-log-slower-than"),
        get: |rcfg| ConfigValue::Numeric(rcfg.server.commandlog_slow_threshold),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                if *v < -1 {
                    return Err(format!("argument must be between -1 and {}", i64::MAX));
                }
                rcfg.server.commandlog_slow_threshold = *v;
                rcfg.server.slowlog_log_slower_than = *v;
                Ok(())
            } else {
                Err("commandlog-execution-slower-than must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "commandlog-request-larger-than",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: i64::MIN,
        numeric_max: i64::MAX,
        flags: &["memory"],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.large_request_threshold),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                if *v < -1 {
                    return Err(format!("argument must be between -1 and {}", i64::MAX));
                }
                rcfg.server.large_request_threshold = *v;
                Ok(())
            } else {
                Err("commandlog-request-larger-than must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "commandlog-reply-larger-than",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: i64::MIN,
        numeric_max: i64::MAX,
        flags: &["memory"],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.large_reply_threshold),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                if *v < -1 {
                    return Err(format!("argument must be between -1 and {}", i64::MAX));
                }
                rcfg.server.large_reply_threshold = *v;
                Ok(())
            } else {
                Err("commandlog-reply-larger-than must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "commandlog-slow-execution-max-len",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: Some("slowlog-max-len"),
        get: |rcfg| ConfigValue::Numeric(rcfg.server.commandlog_slow_max_len),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                if *v < 0 {
                    return Err("value is out of range".to_string());
                }
                rcfg.server.commandlog_slow_max_len = *v;
                rcfg.server.slowlog_max_len = *v;
                Ok(())
            } else {
                Err("commandlog-slow-execution-max-len must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "commandlog-large-request-max-len",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.large_request_max_len),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                if *v < 0 {
                    return Err("value is out of range".to_string());
                }
                rcfg.server.large_request_max_len = *v;
                Ok(())
            } else {
                Err("commandlog-large-request-max-len must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "commandlog-large-reply-max-len",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.large_reply_max_len),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                if *v < 0 {
                    return Err("value is out of range".to_string());
                }
                rcfg.server.large_reply_max_len = *v;
                Ok(())
            } else {
                Err("commandlog-large-reply-max-len must be numeric".to_string())
            }
        },
    });

    reg.register(&ConfigSpec {
        name: "latency-monitor-threshold",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.latency_monitor_threshold),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                if *v < 0 {
                    return Err(format!("argument must be between 0 and {}", i64::MAX));
                }
                rcfg.server.latency_monitor_threshold = *v;
                Ok(())
            } else {
                Err("latency-monitor-threshold must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "latency-tracking",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.latency_tracking),
        set: |rcfg, val| {
            if let ConfigValue::Bool(v) = val {
                rcfg.server.latency_tracking = *v;
                Ok(())
            } else {
                Err("latency-tracking must be a boolean".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "latency-tracking-info-percentiles",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| {
            let s: Vec<String> = rcfg
                .server
                .latency_tracking_info_percentiles
                .iter()
                .map(|v| {
                    if v.fract() == 0.0 {
                        format!("{}", *v as i64)
                    } else {
                        format!("{}", v)
                    }
                })
                .collect();
            ConfigValue::String(s.join(" "))
        },
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                let s = v.trim_matches('"').trim_matches('\'');
                let mut new_vals = Vec::new();
                for tok in s.split_whitespace() {
                    let f: f64 = tok.parse().map_err(|_| {
                        "Invalid percentiles value: must be space-separated float percentiles"
                            .to_string()
                    })?;
                    if !(0.0..=100.0).contains(&f) {
                        return Err(
                            "Invalid percentiles value: percentile must be between 0 and 100"
                                .to_string(),
                        );
                    }
                    new_vals.push(f);
                }
                // C treats a single empty argument as an explicit empty
                // percentile configuration, suppressing distribution output.
                rcfg.server.latency_tracking_info_percentiles = new_vals;
                Ok(())
            } else {
                Err("latency-tracking-info-percentiles must be a string".to_string())
            }
        },
    });
}
