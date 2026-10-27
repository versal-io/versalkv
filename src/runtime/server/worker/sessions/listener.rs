use super::super::{PendingRemovalOwner, ServerWorkerState};
use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::reactor::sources::SourceOwner;
use crate::runtime::server::worker::reactor::transport::RuntimeListenerHandle;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;

#[derive(Default)]
pub(crate) struct ListenerSet {
    pub(crate) all: Vec<Token>,
    pub(crate) tcp: Vec<Token>,
    pub(crate) tls: Vec<Token>,
    pub(crate) inherited_fds: HashMap<usize, std::os::fd::RawFd>,
    pub(crate) bind_addrs: Vec<SocketAddr>,
}

impl ServerWorkerState {
    pub(crate) fn remove_listener(&mut self, token: Token) {
        self.retire_runtime_source(token);
        self.listeners.all.retain(|candidate| *candidate != token);
        self.listeners.tls.retain(|candidate| *candidate != token);
        if let Some(index) = self
            .listeners
            .tcp
            .iter()
            .position(|candidate| *candidate == token)
        {
            self.listeners.tcp.remove(index);
            self.listeners.bind_addrs.remove(index);
        }
        if let Some(fd) = self.listeners.inherited_fds.remove(&token.0) {
            self.ctx.replace_listener_fds(&[fd], &[]);
        }
        self.deregister_or_defer(token, PendingRemovalOwner::Listener);
    }

    pub(crate) fn resolve_tcp_rebind_request(&mut self, connection_token: usize) -> Option<Value> {
        let request = self.ctx.take_tcp_rebind_request()?;
        self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .set_custom_reply(false);
        Some(
            match self.rebind_tcp_listeners(&request.bind, request.port) {
                Ok(()) => {
                    match crate::framework::commands::server::config::apply_cluster_config_snapshot(
                        &mut self.ctx,
                    ) {
                        Ok(()) => Value::ok(),
                        Err(error) => Value::error(error),
                    }
                }
                Err(error) => {
                    self.restore_runtime_config(request.previous_config);
                    if let Some(previous) = request.previous_oom_score_adj {
                        let _ = crate::framework::commands::server::config::write_oom_score_adj(
                            previous,
                        );
                    }
                    let detail = if request.changed_parameter == "bind" {
                        format!("Failed to bind to specified addresses: {error}")
                    } else {
                        format!("Unable to listen on this port: {error}")
                    };
                    Value::error(format!(
                        "ERR CONFIG SET failed (possibly related to argument '{}') - {}",
                        request.changed_parameter, detail
                    ))
                }
            },
        )
    }

    pub(crate) fn resolve_tls_reconfigure_request(
        &mut self,
        connection_token: usize,
    ) -> Option<Value> {
        let request = self.ctx.take_tls_reconfigure_request()?;
        let previous_config = request.previous_config.clone();
        self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .set_custom_reply(false);
        let result: io::Result<()> = (|| {
            if request.port_changed {
                self.rebind_tls_listeners()?;
            } else {
                self.configure_tls_server_from_config()?;
            }
            if request.reconnect_replica && self.ctx.config().server.tls_replication {
                self.configure_tls_client_from_config()?;
            }
            if request.rebind_cluster_bus {
                self.rebind_cluster_bus_transport()?;
            }
            Ok(())
        })();
        Some(match result {
            Ok(()) => {
                if request.reconnect_replica && self.ctx.config().replication.master_host.is_some()
                {
                    self.abort_replica_session_for_role_change();
                }
                Value::ok()
            }
            Err(error) => {
                self.restore_runtime_config(previous_config);
                let _ = self.configure_tls_server_from_config();
                if request.rebind_cluster_bus {
                    let _ = self.rebind_cluster_bus_transport();
                }
                Value::error(format!(
                    "ERR CONFIG SET failed (possibly related to argument '{}') - Unable to update TLS configuration: {}",
                    request.changed_parameter, error
                ))
            }
        })
    }

    fn restore_runtime_config(
        &mut self,
        previous_config: crate::framework::context::RuntimeConfig,
    ) {
        *self.ctx.config_mut() = previous_config;
        if let Err(error) =
            crate::framework::commands::server::config::apply_cluster_config_snapshot(&mut self.ctx)
        {
            crate::server_log!(
                warning,
                "Failed restoring local cluster configuration after listener reconfiguration error: {}",
                error
            );
        }
    }

    pub(crate) fn rebind_tcp_listeners(&mut self, bind: &str, port: u16) -> io::Result<()> {
        let targets = crate::framework::config::bind_targets(bind, port);
        // Validate every requested address before retiring the old listener.
        // A syntax error has no kernel-side partial state to roll back, and
        // returning it after `changeListener`-style teardown would leave the
        // server reachable only through an already-accepted control socket.
        let targets = targets
            .into_iter()
            .map(|target| {
                target
                    .addr
                    .parse::<SocketAddr>()
                    .map(|address| (target, address))
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
            })
            .collect::<io::Result<Vec<_>>>()?;
        let mut addresses = Vec::with_capacity(targets.len());
        let mut new_handles = Vec::with_capacity(targets.len());

        // A wildcard listener overlaps every concrete address on its port, so
        // the replacement cannot be bound while the old registration remains.
        // C's changeListener closes first. Retain the old endpoint snapshot so
        // a failed replacement can restore service before CONFIG rolls back.
        let old_tokens = std::mem::take(&mut self.listeners.tcp);
        let old_addresses = std::mem::take(&mut self.listeners.bind_addrs);
        let old_fds: Vec<_> = old_tokens
            .iter()
            .filter_map(|token| self.listeners.inherited_fds.remove(&token.0))
            .collect();
        self.listeners
            .all
            .retain(|token| !old_tokens.contains(token));
        for token in old_tokens {
            self.retire_runtime_source(token);
            if let Err(error) =
                self.deregister_or_defer_with_result(token, PendingRemovalOwner::Listener)
            {
                if let Err(restore_error) = self.restore_tcp_listeners(&old_addresses, &old_fds) {
                    crate::server_log!(
                        warning,
                        "Failed restoring TCP listeners after deregistration error: {}",
                        restore_error
                    );
                }
                return Err(error);
            }
        }

        if port != 0 {
            for (target, address) in targets {
                match self
                    .transport
                    .bind_tcp(address)
                    .and_then(|handle| self.route_listener_handle(handle, SourceOwner::Listener))
                {
                    Ok(handle) => {
                        addresses.push(address);
                        new_handles.push(handle);
                    }
                    Err(error)
                        if target.optional
                            && crate::server::is_optional_bind_soft_error(&error) =>
                    {
                        crate::server_log!(
                            warning,
                            "Could not create server TCP listening socket {}: {}",
                            target.addr,
                            error
                        );
                    }
                    Err(error) => {
                        for handle in new_handles {
                            self.retire_runtime_source(handle.token);
                            self.deregister_or_defer(handle.token, PendingRemovalOwner::Listener);
                        }
                        if let Err(restore_error) =
                            self.restore_tcp_listeners(&old_addresses, &old_fds)
                        {
                            crate::server_log!(
                                warning,
                                "Failed restoring TCP listeners after replacement error: {}",
                                restore_error
                            );
                        }
                        return Err(error);
                    }
                }
            }
        }

        self.install_tcp_listeners(addresses, new_handles, &old_fds);
        self.ctx.config_mut().server.listening_port = port;
        self.ctx.config_mut().server.set_bind(bind.to_string());
        self.ctx.set_tcp_port(port);
        Ok(())
    }

    fn restore_tcp_listeners(
        &mut self,
        old_addresses: &[SocketAddr],
        old_fds: &[std::os::fd::RawFd],
    ) -> io::Result<()> {
        let mut handles = Vec::with_capacity(old_addresses.len());
        for &address in old_addresses {
            match self
                .transport
                .bind_tcp(address)
                .and_then(|handle| self.route_listener_handle(handle, SourceOwner::Listener))
            {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    for handle in handles {
                        self.retire_runtime_source(handle.token);
                        self.deregister_or_defer(handle.token, PendingRemovalOwner::Listener);
                    }
                    return Err(error);
                }
            }
        }
        self.install_tcp_listeners(old_addresses.to_vec(), handles, old_fds);
        Ok(())
    }

    fn install_tcp_listeners(
        &mut self,
        addresses: Vec<SocketAddr>,
        handles: Vec<RuntimeListenerHandle>,
        old_fds: &[std::os::fd::RawFd],
    ) {
        let new_tokens: Vec<_> = handles.iter().map(|handle| handle.token).collect();
        let new_fds: Vec<_> = handles.iter().map(|handle| handle.inherited_fd).collect();
        self.listeners.all.extend(new_tokens.iter().copied());
        self.listeners.tcp = new_tokens;
        self.listeners.bind_addrs = addresses;
        self.ctx.replace_listener_fds(old_fds, &new_fds);
        for handle in handles {
            self.listeners
                .inherited_fds
                .insert(handle.token.0, handle.inherited_fd);
        }
    }

    fn rebind_tls_listeners(&mut self) -> io::Result<()> {
        let tls_port = self.ctx.config().server.tls_port;
        let bind = self.ctx.config().server.bind.clone();
        let targets = crate::framework::config::bind_targets(&bind, tls_port);
        let mut new_handles = Vec::with_capacity(targets.len());
        if tls_port != 0 {
            self.configure_tls_server_from_config()?;
            for target in targets {
                let address = target
                    .addr
                    .parse::<SocketAddr>()
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
                match self
                    .transport
                    .bind_tls(address)
                    .and_then(|handle| self.route_listener_handle(handle, SourceOwner::Listener))
                {
                    Ok(handle) => new_handles.push(handle),
                    Err(error)
                        if target.optional
                            && crate::server::is_optional_bind_soft_error(&error) =>
                    {
                        crate::server_log!(
                            warning,
                            "Could not create server TLS listening socket {}: {}",
                            target.addr,
                            error
                        );
                    }
                    Err(error) => {
                        for handle in new_handles {
                            self.retire_runtime_source(handle.token);
                            self.deregister_or_defer(handle.token, PendingRemovalOwner::Listener);
                        }
                        return Err(error);
                    }
                }
            }
        }

        let old_tokens = std::mem::take(&mut self.listeners.tls);
        let old_fds: Vec<_> = old_tokens
            .iter()
            .filter_map(|token| self.listeners.inherited_fds.remove(&token.0))
            .collect();
        let new_tokens: Vec<_> = new_handles.iter().map(|handle| handle.token).collect();
        let new_fds: Vec<_> = new_handles
            .iter()
            .map(|handle| handle.inherited_fd)
            .collect();
        self.listeners
            .all
            .retain(|token| !old_tokens.contains(token));
        self.listeners.all.extend(new_tokens.iter().copied());
        self.listeners.tls = new_tokens;
        self.ctx.replace_listener_fds(&old_fds, &new_fds);
        for handle in &new_handles {
            self.listeners
                .inherited_fds
                .insert(handle.token.0, handle.inherited_fd);
        }
        for token in old_tokens {
            self.retire_runtime_source(token);
            self.deregister_or_defer(token, PendingRemovalOwner::Listener);
        }
        Ok(())
    }
}
