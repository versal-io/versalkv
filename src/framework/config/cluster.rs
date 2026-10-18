use super::Config;
use crate::engine::distributed::cluster::ClusterEndpoints;
use crate::engine::extension::config::server::ServerConfig;

pub(crate) fn merge_persisted_cluster_endpoint_defaults(
    startup: &Config,
    runtime: &mut ServerConfig,
    endpoints: &ClusterEndpoints,
) {
    let persisted_ip = (endpoints.ip() != runtime.bindaddr).then_some(endpoints.ip());
    merge_persisted_strings(startup, runtime, endpoints, persisted_ip);
    merge_persisted_ports(startup, runtime, endpoints);
}

fn merge_persisted_strings(
    startup: &Config,
    runtime: &mut ServerConfig,
    endpoints: &ClusterEndpoints,
    persisted_ip: Option<&str>,
) {
    for (key, target, persisted) in [
        (
            "cluster-announce-ip",
            &mut runtime.cluster_announce_ip,
            persisted_ip,
        ),
        (
            "cluster-announce-hostname",
            &mut runtime.cluster_announce_hostname,
            endpoints.hostname(),
        ),
        (
            "cluster-announce-human-nodename",
            &mut runtime.cluster_announce_human_nodename,
            endpoints.human_name(),
        ),
        (
            "cluster-announce-client-ipv4",
            &mut runtime.cluster_announce_client_ipv4,
            endpoints.client_ipv4(),
        ),
    ] {
        restore_string(startup, key, target, persisted);
    }
    merge_persisted_labels(startup, runtime, endpoints);
}

fn merge_persisted_labels(
    startup: &Config,
    runtime: &mut ServerConfig,
    endpoints: &ClusterEndpoints,
) {
    for (key, target, persisted) in [
        (
            "cluster-announce-client-ipv6",
            &mut runtime.cluster_announce_client_ipv6,
            endpoints.client_ipv6(),
        ),
        (
            "availability-zone",
            &mut runtime.availability_zone,
            endpoints.availability_zone(),
        ),
    ] {
        restore_string(startup, key, target, persisted);
    }
}

fn merge_persisted_ports(
    startup: &Config,
    runtime: &mut ServerConfig,
    endpoints: &ClusterEndpoints,
) {
    for (key, target, persisted) in [
        (
            "cluster-announce-tls-port",
            &mut runtime.cluster_announce_tls_port,
            endpoints.tls_port(),
        ),
        (
            "cluster-announce-client-port",
            &mut runtime.cluster_announce_client_port,
            endpoints.client_port(),
        ),
        (
            "cluster-announce-client-tls-port",
            &mut runtime.cluster_announce_client_tls_port,
            endpoints.client_tls_port(),
        ),
    ] {
        restore_port(startup, key, target, persisted);
    }
}

fn restore_string(startup: &Config, key: &str, target: &mut String, persisted: Option<&str>) {
    if !startup.is_explicitly_set(key) && target.is_empty() {
        *target = persisted.unwrap_or_default().to_string();
    }
}

fn restore_port(startup: &Config, key: &str, target: &mut u16, persisted: u16) {
    if !startup.is_explicitly_set(key) && *target == 0 {
        *target = persisted;
    }
}
