use super::{
    bind_listener_with_restart_grace, cluster_slot_router, is_optional_bind_soft_error, Server,
};
use crate::framework::commands;
use crate::framework::commands::server::config::apply_cluster_config_snapshot;
use crate::framework::config::merge_persisted_cluster_endpoint_defaults;
use crate::runtime::server::worker::ServerWorkerState;
use crate::runtime::server::ServerRuntime;
use std::net::SocketAddr;
use std::str::FromStr;

impl Server {
    pub(super) fn create_backend(&mut self) -> std::io::Result<ServerRuntime> {
        let cluster_enabled = self.config.cluster_enabled && !self.config.sentinel;
        let mut backend = if cluster_enabled {
            ServerRuntime::new_with_slot_router(self.config.databases, 14, cluster_slot_router)?
        } else {
            ServerRuntime::new(self.config.databases)?
        };
        backend
            .ctx
            .persistence_mut()
            .record_rdb_save((crate::foundation::io::store::kv_engine::now_ms() / 1000) as i64);
        Ok(backend)
    }

    pub(super) fn register_commands(&mut self, backend: &mut ServerWorkerState) {
        commands::register_all(&mut backend.registry);
        for (old_name, new_name) in &self.config.rename_commands {
            backend.registry.rename(old_name, new_name);
        }
        backend.ctx.set_command_registry_snapshot(&backend.registry);
        backend
            .ctx
            .set_module_command_registry(&mut backend.registry as *mut _);
    }

    pub(super) fn load_modules(&mut self, backend: &mut ServerWorkerState) -> std::io::Result<()> {
        for (path, args) in self.config.load_modules.clone() {
            let module_args: Vec<&[u8]> = args.iter().map(String::as_bytes).collect();
            let result = crate::framework::capabilities::module::load_module(
                &path,
                &module_args,
                &mut backend.ctx,
            );
            match result {
                Ok(name) => {
                    // Keep the original directive so CONFIG REWRITE can recreate
                    // the module after a restart.
                    backend
                        .ctx
                        .module_registry_mut()
                        .set_load_path(&name, path, args);
                }
                Err(error) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        error.to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(super) fn bind_listeners(
        &mut self,
        backend: &mut ServerWorkerState,
    ) -> std::io::Result<Vec<String>> {
        let targets = self.config.bind_targets();
        let mut bound = Vec::with_capacity(targets.len());
        for target in &targets {
            let addr = SocketAddr::from_str(&target.addr)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            let bind = if self.config.mptcp {
                ServerWorkerState::bind_mptcp
            } else {
                ServerWorkerState::bind
            };
            match bind_listener_with_restart_grace(backend, addr, bind) {
                Ok(()) => bound.push(target.addr.clone()),
                // Optional binds (leading `-`, e.g. the default `-::*` IPv6
                // wildcard) are best-effort: an unavailable address or missing
                // protocol family is non-fatal, matching C valkey's
                // `listenToPort` (server.c) EADDRNOTAVAIL/EAFNOSUPPORT handling.
                Err(error) if target.optional && is_optional_bind_soft_error(&error) => {
                    crate::server_log!(
                        warning,
                        "Could not create server TCP listening socket {}: {}",
                        target.addr,
                        error
                    );
                }
                Err(error) => {
                    return Err(std::io::Error::new(
                        error.kind(),
                        format!(
                            "failed binding {} listener {}: {error}",
                            if self.config.mptcp { "MPTCP" } else { "TCP" },
                            target.addr
                        ),
                    ));
                }
            }
        }
        let tls_port = backend.ctx.config().server.tls_port;
        Self::configure_tls_server(backend)?;
        if tls_port != 0 {
            for target in &targets {
                let mut addr = SocketAddr::from_str(&target.addr)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
                addr.set_port(tls_port);
                match bind_listener_with_restart_grace(backend, addr, ServerWorkerState::bind_tls) {
                    Ok(()) => bound.push(addr.to_string()),
                    Err(error) if target.optional && is_optional_bind_soft_error(&error) => {
                        crate::server_log!(
                            warning,
                            "Could not create server TLS listening socket {}: {}",
                            addr,
                            error
                        );
                    }
                    Err(error) => {
                        return Err(std::io::Error::new(
                            error.kind(),
                            format!("failed binding TLS listener {addr}: {error}"),
                        ));
                    }
                }
            }
        }
        backend.ctx.set_tcp_port(self.config.port);
        // Bind an optional Unix domain socket listener.
        let unixsocket = backend.ctx.config_mut().server.unixsocket.clone();
        if !unixsocket.is_empty() {
            let perm = backend.ctx.config_mut().server.unixsocketperm;
            let group = backend.ctx.config_mut().server.unixsocketgroup.clone();
            backend
                .bind_unix(&unixsocket, perm, &group)
                .map_err(|error| {
                    std::io::Error::new(
                        error.kind(),
                        format!("failed binding Unix socket {unixsocket}: {error}"),
                    )
                })?;
        }
        Ok(bound)
    }

    fn configure_tls_server(backend: &mut ServerWorkerState) -> std::io::Result<()> {
        backend.configure_tls_contexts_from_config()
    }

    pub(super) fn init_observability(&mut self, backend: &mut ServerWorkerState) {
        let threshold = backend.ctx.config_mut().server.latency_monitor_threshold;
        backend.ctx.latency_mut().set_threshold(threshold);
        // C's slowlog-* names alias the commandlog slow category, so both
        // observability owners receive the single startup value.
        backend.ctx.slowlog_mut().reconfigure(
            self.config.commandlog_slow_threshold,
            self.config.commandlog_slow_max_len,
        );
        backend.ctx.commandlog_mut().reconfigure_all(
            self.config.commandlog_slow_threshold,
            self.config.commandlog_slow_max_len,
            self.config.commandlog_large_request_threshold,
            self.config.commandlog_large_request_max_len,
            self.config.commandlog_large_reply_threshold,
            self.config.commandlog_large_reply_max_len,
        );
    }

    pub(super) fn init_sentinel_identity(&mut self, backend: &mut ServerWorkerState) {
        backend.ctx.sentinel_state_mut().sentinel_mode = self.config.sentinel;
        backend.ctx.sentinel_state_mut().announce_addr = format!(
            "{}:{}",
            self.config.default_advertised_ip(),
            self.config.port
        );
        if !self.config.config_file.is_empty() {
            backend.ctx.sentinel_state_mut().config_file = self.config.config_file.clone();
        }
    }

    pub(super) fn init_cluster(&mut self, backend: &mut ServerWorkerState) -> std::io::Result<()> {
        if self.config.cluster_enabled {
            let cluster_port = self.config.cluster_bus_port().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Valkey port number too high. Cluster communication port is 10,000 port numbers higher than your Valkey port. Your Valkey port number must be 55535 or less.",
                )
            })?;
            let cluster_config_path = std::path::Path::new(&self.config.dir)
                .join(&backend.ctx.config().server.cluster_config_file);
            backend.lock_cluster_config(cluster_config_path.clone())?;
            let cluster_default_ip = self.config.default_advertised_ip();
            let loaded_config = std::fs::read_to_string(&cluster_config_path).ok();
            let mut cluster_state = match loaded_config.as_deref() {
                Some(content) if !content.trim().is_empty() => {
                    crate::engine::distributed::cluster::config::parse_cluster_config(
                        content,
                        &cluster_default_ip,
                        self.config.port,
                        cluster_port,
                    )
                    .map_err(|error| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("cluster config {}: {error}", cluster_config_path.display()),
                        )
                    })?
                }
                _ => crate::engine::distributed::cluster::ClusterState::new(
                    &cluster_default_ip,
                    self.config.port,
                    cluster_port,
                ),
            };
            cluster_state.set_require_full_coverage(
                backend.ctx.config().server.cluster_require_full_coverage,
            );
            if loaded_config
                .as_deref()
                .is_some_and(|content| !content.trim().is_empty())
            {
                merge_persisted_cluster_endpoint_defaults(
                    &self.config,
                    &mut backend.ctx.config_mut().server,
                    &cluster_state.myself.endpoints,
                );
            }
            Self::restore_cluster_replication_role(backend, &cluster_state);
            let cluster_state = Box::new(cluster_state);
            let node_id = cluster_state.myself.name.clone();
            *backend.ctx.cluster_state_mut() = Some(cluster_state);
            apply_cluster_config_snapshot(&mut backend.ctx)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            backend.save_cluster_config_now()?;
            crate::server_log!(notice, "Cluster mode enabled (node_id={})", node_id);

            let mut bus_addresses = std::collections::HashSet::new();
            for target in self.config.bind_targets() {
                let client_addr = SocketAddr::from_str(&target.addr)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
                let bus_addr = SocketAddr::new(client_addr.ip(), cluster_port);
                if !bus_addresses.insert(bus_addr) {
                    continue;
                }
                let use_tls = backend.ctx.config().server.tls_cluster;
                let bind_result = if use_tls {
                    Self::configure_tls_server(backend)
                } else {
                    Ok(())
                }
                .and_then(|_| backend.bind_cluster_bus_listener(bus_addr, use_tls));
                match bind_result {
                    Ok(()) => {
                        crate::server_log!(
                            notice,
                            "Cluster bus listening on {} ({})",
                            bus_addr,
                            if use_tls { "TLS" } else { "TCP" }
                        );
                    }
                    Err(error) if target.optional && is_optional_bind_soft_error(&error) => {
                        crate::server_log!(
                            warning,
                            "Could not create optional Cluster Bus socket {}: {}",
                            bus_addr,
                            error
                        );
                    }
                    Err(error) => return Err(Self::report_listener_error(error)),
                }
            }
        }
        Ok(())
    }

    pub(super) fn restore_cluster_replication_role(
        backend: &mut ServerWorkerState,
        cluster_state: &crate::engine::distributed::cluster::ClusterState,
    ) {
        if !cluster_state.myself.is_replica() {
            // nodes.conf is authoritative for Cluster role. A stale replicaof
            // directive can otherwise leave the runtime in replica mode while
            // Cluster has restored this node as a primary, causing it to reject
            // downstream PSYNC requests with NOMASTERLINK after a restart.
            backend.ctx.config_mut().replication.master_host = None;
            backend.ctx.config_mut().replication.master_port = 0;
            backend.ctx.config_mut().replication.master_link_status = "down".to_string();
            backend.ctx.replication_mut().switch_to_primary();
            return;
        }
        let Some(primary) = cluster_state.nodes.get(&cluster_state.myself.replicaof) else {
            return;
        };
        backend.ctx.config_mut().replication.master_host = Some(primary.ip.clone());
        backend.ctx.config_mut().replication.master_port = primary.port;
        backend.ctx.config_mut().replication.master_link_status = "down".to_string();
        backend.ctx.replication_mut().role =
            crate::engine::distributed::replication::ReplicationRole::Replica;
    }

    pub(super) fn init_sentinel_directives(&mut self, backend: &mut ServerWorkerState) {
        if !self.config.sentinel_directives.is_empty() {
            if let Err(e) = crate::framework::distributed::sentinel::load_config_directives(
                backend.ctx.sentinel_state_mut(),
                &self.config.sentinel_directives,
            ) {
                crate::server_log!(warning, "Failed to load sentinel config: {}", e);
            }
        }
        if backend.ctx.sentinel_state_mut().sentinel_mode {
            backend.ctx.sentinel_state_mut().initialized = true;
        }
    }
}
