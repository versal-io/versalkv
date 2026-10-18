use crate::engine::extension::config::spec::{ConfigSpec, ConfigType, ConfigValue};
use crate::engine::extension::config::spec_registry::ConfigRegistry;

const CLIENT_QUERY_BUFFER_LIMIT_MAX: i64 = if usize::BITS >= 64 {
    i64::MAX
} else {
    i32::MAX as i64
};

pub(crate) fn register_network_configs(reg: &mut ConfigRegistry) {
    reg.register(&ConfigSpec {
        name: "tls-cluster",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.tls_cluster),
        set: |rcfg, value| {
            let ConfigValue::Bool(value) = value else {
                return Err("tls-cluster must be a boolean".to_string());
            };
            rcfg.server.tls_cluster = *value;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "tls-replication",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.tls_replication),
        set: |rcfg, value| {
            let ConfigValue::Bool(value) = value else {
                return Err("tls-replication must be a boolean".to_string());
            };
            rcfg.server.tls_replication = *value;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "tls-client-cert-file",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.tls_client_cert_file.clone()),
        set: |rcfg, value| {
            let ConfigValue::String(value) = value else {
                return Err("tls-client-cert-file must be a string".to_string());
            };
            rcfg.server.tls_client_cert_file = value.clone();
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "tls-client-key-file",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.tls_client_key_file.clone()),
        set: |rcfg, value| {
            let ConfigValue::String(value) = value else {
                return Err("tls-client-key-file must be a string".to_string());
            };
            rcfg.server.tls_client_key_file = value.clone();
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "tls-auth-clients",
        type_: ConfigType::Enum,
        enum_values: &["no", "yes", "optional"],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.tls_auth_clients.clone()),
        set: |rcfg, value| {
            let ConfigValue::String(value) = value else {
                return Err("tls-auth-clients must be no, yes, or optional".to_string());
            };
            let value = value.to_ascii_lowercase();
            if !["no", "yes", "optional"].contains(&value.as_str()) {
                return Err("tls-auth-clients must be no, yes, or optional".to_string());
            }
            rcfg.server.tls_auth_clients = value;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "tls-port",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: u16::MAX as i64,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.tls_port as i64),
        set: |rcfg, value| {
            let ConfigValue::Numeric(value) = value else {
                return Err("tls-port must be numeric".to_string());
            };
            rcfg.server.tls_port = *value as u16;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "tls-cert-file",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.tls_cert_file.clone()),
        set: |rcfg, value| {
            let ConfigValue::String(value) = value else {
                return Err("tls-cert-file must be a string".to_string());
            };
            rcfg.server.tls_cert_file = value.clone();
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "tls-key-file",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.tls_key_file.clone()),
        set: |rcfg, value| {
            let ConfigValue::String(value) = value else {
                return Err("tls-key-file must be a string".to_string());
            };
            rcfg.server.tls_key_file = value.clone();
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "tls-ca-cert-file",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.tls_ca_cert_file.clone()),
        set: |rcfg, value| {
            let ConfigValue::String(value) = value else {
                return Err("tls-ca-cert-file must be a string".to_string());
            };
            rcfg.server.tls_ca_cert_file = value.clone();
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "tls-ca-cert-dir",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.tls_ca_cert_dir.clone()),
        set: |rcfg, value| {
            let ConfigValue::String(value) = value else {
                return Err("tls-ca-cert-dir must be a string".to_string());
            };
            rcfg.server.tls_ca_cert_dir = value.clone();
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "tls-protocols",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.tls_protocols.clone()),
        set: |rcfg, value| {
            let ConfigValue::String(value) = value else {
                return Err("tls-protocols must be a string".to_string());
            };
            rcfg.server.tls_protocols = value.clone();
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "tls-ciphers",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.tls_ciphers.clone()),
        set: |rcfg, value| {
            let ConfigValue::String(value) = value else {
                return Err("tls-ciphers must be a string".to_string());
            };
            rcfg.server.tls_ciphers = value.clone();
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "tls-ciphersuites",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.tls_ciphersuites.clone()),
        set: |rcfg, value| {
            let ConfigValue::String(value) = value else {
                return Err("tls-ciphersuites must be a string".to_string());
            };
            rcfg.server.tls_ciphersuites = value.clone();
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "tls-prefer-server-ciphers",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.tls_prefer_server_ciphers),
        set: |rcfg, value| {
            let ConfigValue::Bool(value) = value else {
                return Err("tls-prefer-server-ciphers must be a boolean".to_string());
            };
            rcfg.server.tls_prefer_server_ciphers = *value;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "tls-session-caching",
        type_: ConfigType::Bool,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Bool(rcfg.server.tls_session_caching),
        set: |rcfg, value| {
            let ConfigValue::Bool(value) = value else {
                return Err("tls-session-caching must be a boolean".to_string());
            };
            rcfg.server.tls_session_caching = *value;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "tls-session-cache-size",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i32::MAX as i64,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.tls_session_cache_size as i64),
        set: |rcfg, value| {
            let ConfigValue::Numeric(value) = value else {
                return Err("tls-session-cache-size must be numeric".to_string());
            };
            rcfg.server.tls_session_cache_size = *value as i32;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "proto-max-bulk-len",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1024 * 1024,
        numeric_max: i64::MAX,
        flags: &["memory"],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.proto_max_bulk_len),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.server.proto_max_bulk_len = *v;
                Ok(())
            } else {
                Err("proto-max-bulk-len must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "client-query-buffer-limit",
        type_: ConfigType::Numeric,
        enum_values: &[],
        // C config.c stores this in size_t and requires at least 1 MiB.
        // The Rust cap matches the active target's addressable byte range.
        numeric_min: 1024 * 1024,
        numeric_max: CLIENT_QUERY_BUFFER_LIMIT_MAX,
        flags: &["memory"],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.client_query_buffer_limit as i64),
        set: |rcfg, val| match val {
            ConfigValue::Numeric(value) => {
                rcfg.server.client_query_buffer_limit = *value as usize;
                Ok(())
            }
            _ => Err("client-query-buffer-limit must be numeric".to_string()),
        },
    });
    reg.register(&ConfigSpec {
        name: "tcp-keepalive",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.tcp_keepalive as i64),
        set: |rcfg, val| {
            let ConfigValue::Numeric(value) = val else {
                return Err("tcp-keepalive must be numeric".to_string());
            };
            rcfg.server.tcp_keepalive = *value as u64;
            Ok(())
        },
    });
}
