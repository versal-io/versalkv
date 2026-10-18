use crate::engine::distributed::cluster::{ClusterEndpoints, EndpointError};
use crate::engine::extension::config::spec::{ConfigSpec, ConfigType, ConfigValue};
use crate::engine::extension::config::spec_registry::ConfigRegistry;

fn config_error(name: &str, detail: &str) -> String {
    format!("ERR CONFIG SET failed (possibly related to argument '{name}') - {detail}")
}

fn announced_ip_error(error: EndpointError) -> String {
    let detail = match error {
        EndpointError::AnnouncedIpTooLong => "cluster-announce-ip is too long",
        EndpointError::InvalidAuxCharacter | EndpointError::InvalidUtf8 => {
            "cluster-announce-ip contains invalid character"
        }
        EndpointError::InvalidAnnouncedIp | EndpointError::InvalidHostname => {
            "cluster-announce-ip is not a valid IP address or hostname"
        }
        _ => "cluster-announce-ip is invalid",
    };
    config_error("cluster-announce-ip", detail)
}

pub(super) fn register_cluster_metadata_configs(reg: &mut ConfigRegistry) {
    reg.register(&ConfigSpec {
        name: "cluster-announce-ip",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.cluster_announce_ip.clone()),
        set: |rcfg, val| {
            let ConfigValue::String(value) = val else {
                return Err("cluster-announce-ip must be a string".to_string());
            };
            ClusterEndpoints::validate_announced_ip(value.as_bytes())
                .map_err(announced_ip_error)?;
            rcfg.server.cluster_announce_ip = value.clone();
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-announce-client-ipv4",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.cluster_announce_client_ipv4.clone()),
        set: |rcfg, val| {
            let ConfigValue::String(value) = val else {
                return Err("cluster-announce-client-ipv4 must be a string".to_string());
            };
            ClusterEndpoints::validate_ipv4(value.as_bytes()).map_err(|_| {
                config_error("cluster-announce-client-ipv4", "Invalid IPv4 address")
            })?;
            rcfg.server.cluster_announce_client_ipv4 = value.clone();
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-announce-client-ipv6",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.cluster_announce_client_ipv6.clone()),
        set: |rcfg, val| {
            let ConfigValue::String(value) = val else {
                return Err("cluster-announce-client-ipv6 must be a string".to_string());
            };
            ClusterEndpoints::validate_ipv6(value.as_bytes()).map_err(|_| {
                config_error("cluster-announce-client-ipv6", "Invalid IPv6 address")
            })?;
            rcfg.server.cluster_announce_client_ipv6 = value.clone();
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-announce-hostname",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.cluster_announce_hostname.clone()),
        set: |rcfg, val| {
            let ConfigValue::String(value) = val else {
                return Err("cluster-announce-hostname must be a string".to_string());
            };
            ClusterEndpoints::validate_hostname(value.as_bytes()).map_err(|error| {
                let detail = match error {
                    EndpointError::HostnameTooLong => "Hostnames must be less than 256 characters",
                    _ => "Hostnames may only contain alphanumeric characters, hyphens or dots",
                };
                config_error("cluster-announce-hostname", detail)
            })?;
            rcfg.server.cluster_announce_hostname = value.clone();
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-announce-human-nodename",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.cluster_announce_human_nodename.clone()),
        set: |rcfg, val| {
            let ConfigValue::String(value) = val else {
                return Err("cluster-announce-human-nodename must be a string".to_string());
            };
            ClusterEndpoints::validate_aux_value(value.as_bytes()).map_err(|_| {
                config_error(
                    "cluster-announce-human-nodename",
                    "invalid character in announced human node name",
                )
            })?;
            rcfg.server.cluster_announce_human_nodename = value.clone();
            Ok(())
        },
    });
    register_port_configs(reg);
    register_policy_configs(reg);
}

macro_rules! port_spec {
    ($name:literal, $field:ident) => {
        ConfigSpec {
            name: $name,
            type_: ConfigType::Numeric,
            enum_values: &[],
            numeric_min: 0,
            numeric_max: 65535,
            flags: &[],
            alias: None,
            get: |rcfg| ConfigValue::Numeric(rcfg.server.$field as i64),
            set: |rcfg, val| {
                rcfg.server.$field = val.as_i64().ok_or(concat!($name, " must be numeric"))? as u16;
                Ok(())
            },
        }
    };
}

static CLUSTER_ANNOUNCE_PORT: ConfigSpec =
    port_spec!("cluster-announce-port", cluster_announce_port);
static CLUSTER_ANNOUNCE_TLS_PORT: ConfigSpec =
    port_spec!("cluster-announce-tls-port", cluster_announce_tls_port);
static CLUSTER_ANNOUNCE_CLIENT_PORT: ConfigSpec =
    port_spec!("cluster-announce-client-port", cluster_announce_client_port);
static CLUSTER_ANNOUNCE_CLIENT_TLS_PORT: ConfigSpec = port_spec!(
    "cluster-announce-client-tls-port",
    cluster_announce_client_tls_port
);
static CLUSTER_ANNOUNCE_BUS_PORT: ConfigSpec =
    port_spec!("cluster-announce-bus-port", cluster_announce_bus_port);

fn register_port_configs(reg: &mut ConfigRegistry) {
    reg.register(&CLUSTER_ANNOUNCE_PORT);
    reg.register(&CLUSTER_ANNOUNCE_TLS_PORT);
    reg.register(&CLUSTER_ANNOUNCE_CLIENT_PORT);
    reg.register(&CLUSTER_ANNOUNCE_CLIENT_TLS_PORT);
    reg.register(&CLUSTER_ANNOUNCE_BUS_PORT);
}

fn register_policy_configs(reg: &mut ConfigRegistry) {
    reg.register(&ConfigSpec {
        name: "cluster-config-save-behavior",
        type_: ConfigType::Enum,
        enum_values: &["sync", "best-effort"],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::String(rcfg.server.cluster_config_save_behavior.clone()),
        set: |rcfg, val| {
            let ConfigValue::String(value) = val else {
                return Err("cluster-config-save-behavior must be a string".to_string());
            };
            rcfg.server.cluster_config_save_behavior = value.to_ascii_lowercase();
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "cluster-message-gossip-perc",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: 100,
        flags: &["hidden"],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.server.cluster_message_gossip_perc),
        set: |rcfg, val| {
            rcfg.server.cluster_message_gossip_perc = val
                .as_i64()
                .ok_or("cluster-message-gossip-perc must be numeric")?;
            Ok(())
        },
    });
}
