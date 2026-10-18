use crate::engine::extension::config::spec_registry::ConfigRegistry;

mod cluster;
mod cluster_metadata;
mod encoding;
mod logs;
mod network;
mod persistence;
mod replication;
mod security;
mod server;

pub fn register_all_configs(reg: &mut ConfigRegistry) {
    persistence::register_persistence_configs(reg);
    security::register_security_configs(reg);
    server::register_server_configs(reg);
    encoding::register_encoding_configs(reg);
    logs::register_logs_configs(reg);
    network::register_network_configs(reg);
    replication::register_replication_configs(reg);
    cluster::register_cluster_configs(reg);
}
