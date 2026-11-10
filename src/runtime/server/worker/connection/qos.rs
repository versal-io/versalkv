use std::net::SocketAddr;

use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::ServerWorkerState;

pub(crate) struct PriorityCycle {
    started_at: std::time::Instant,
    reset_generation: u64,
    commands_processed: u64,
}

/// Describes a pending fd move between the main transport poller and the
/// PriorityPoller. Migration is deferred when the connection has an inflight
/// IO operation.
pub(crate) enum FdMigration {
    /// Register this token's fd in the PriorityPoller (was non-priority, now
    /// priority).
    ToPriority { token: Token },
    /// Delete this token's fd from the PriorityPoller (was priority, now
    /// non-priority).
    FromPriority { token: Token },
}

#[derive(Default)]
pub(crate) struct QosState {
    pub(crate) subnets_generation: u64,
    pub(crate) active_command_token: Option<Token>,
    pub(crate) active_command_is_nested: bool,
    pub(crate) command_depth: usize,
    pub(crate) active_cycle: Option<PriorityCycle>,
    /// Fd migration entries deferred because the connection had an inflight
    /// IO operation. Processed when the inflight operation completes.
    pub(crate) pending_migrations: Vec<FdMigration>,
}

impl ServerWorkerState {
    pub(crate) fn is_peer_in_priority_subnet(&self, address: SocketAddr) -> bool {
        address.port() != 0
            && self
                .ctx
                .config()
                .server
                .compiled_priority_subnets
                .iter()
                .any(|subnet| subnet.contains(address.ip()))
    }

    pub(crate) fn scan_and_apply_priority_subnets(&mut self) {
        let config = &self.ctx.config().server;
        if self.qos.subnets_generation != config.priority_subnets_generation {
            let subnets = &config.compiled_priority_subnets;
            let mut priority_clients = 0;
            let mut migrations: Vec<(Token, bool, bool)> = Vec::new();
            for (token, connection) in self.connection_registry.iter_mut() {
                let was_priority = connection.is_priority;
                connection.is_priority = connection.peer_addr.port() != 0
                    && subnets
                        .iter()
                        .any(|subnet| subnet.contains(connection.peer_addr.ip()));
                priority_clients += u64::from(connection.is_priority);
                if was_priority != connection.is_priority {
                    migrations.push((token, was_priority, connection.is_priority));
                }
            }
            self.qos.subnets_generation = config.priority_subnets_generation;
            self.ctx.stats_mut().connected_priority_clients = priority_clients;
            for (token, was_priority, is_priority) in migrations {
                self.migrate_connection_fd(token, was_priority, is_priority);
            }
        }
        if let Some(token) = self.qos.active_command_token {
            let is_priority = self.is_client_subnet_priority(token);
            self.ctx
                .stats_mut()
                .set_priority_command(is_priority && !self.qos.active_command_is_nested);
        }
    }

    pub(crate) fn mark_client_subnet_priority(&mut self, token: Token) {
        self.scan_and_apply_priority_subnets();
        if let Some(connection) = self.connection_registry.get_mut(token) {
            if !connection.is_priority {
                self.ctx.stats_mut().connected_priority_clients += 1;
            }
            connection.is_priority = true;
            // Ensure the fd is registered in the priority epoll so
            // readiness arrives through the dedicated poller.
            if let Ok(fd) = self.transport.raw_fd(token) {
                let _ = self.priority_poller.register_fd(token, fd);
            }
        }
        if self.qos.active_command_token == Some(token) {
            self.ctx
                .stats_mut()
                .set_priority_command(!self.qos.active_command_is_nested);
        }
    }

    pub(crate) fn is_client_subnet_priority(&self, token: Token) -> bool {
        self.connection_registry
            .get(token)
            .is_some_and(|connection| connection.is_priority)
    }

    pub(crate) fn check_client_capacity(&self, is_priority: bool) -> bool {
        let config = &self.ctx.config().server;
        let clients = self
            .connected_clients()
            .saturating_add(if config.cluster_enabled {
                self.ctx
                    .cluster_state()
                    .as_ref()
                    .map_or(0, |state| state.connection_budget())
            } else {
                0
            });
        if clients >= config.maxclients as usize {
            return false;
        }
        if is_priority
            || config.maxclients_reserved == 0
            || config.compiled_priority_subnets.is_empty()
        {
            return true;
        }
        let normal_clients =
            clients.saturating_sub(self.ctx.stats().connected_priority_clients as usize);
        normal_clients < config.maxclients.saturating_sub(config.maxclients_reserved) as usize
    }

    pub(crate) fn start_priority_cycle(&mut self) -> bool {
        if self.qos.active_cycle.is_some() {
            return false;
        }
        self.qos.active_cycle = Some(PriorityCycle {
            started_at: std::time::Instant::now(),
            reset_generation: self.ctx.stats().reset_generation,
            commands_processed: self.ctx.stats().priority_commands_processed,
        });
        true
    }

    pub(crate) fn finish_priority_cycle(&mut self, owns_cycle: bool) {
        if let Some(cycle) = owns_cycle.then(|| self.qos.active_cycle.take()).flatten() {
            let stats = self.ctx.stats_mut();
            let was_reset = cycle.reset_generation != stats.reset_generation;
            let started_at = if was_reset {
                stats.last_reset_at
            } else {
                cycle.started_at
            };
            stats.record_eventloop_priority_cycle(
                (started_at.elapsed().as_micros() as u64).max(1),
                stats
                    .priority_commands_processed
                    .saturating_sub(if was_reset {
                        0
                    } else {
                        cycle.commands_processed
                    }),
            );
        }
    }

    /// Attempt to migrate a connection's fd between the main transport and
    /// the PriorityPoller. If the connection has an inflight IO operation,
    /// defer the migration until the operation completes.
    fn migrate_connection_fd(&mut self, token: Token, was_priority: bool, is_priority: bool) {
        if was_priority == is_priority {
            return;
        }
        let inflight = self.io_threads.read_inflight_id(token).is_some()
            || self.io_threads.write_inflight_id(token).is_some();
        if inflight {
            self.qos.pending_migrations.push(if is_priority {
                FdMigration::ToPriority { token }
            } else {
                FdMigration::FromPriority { token }
            });
            return;
        }
        self.apply_fd_migration(token, was_priority, is_priority);
    }

    /// Execute a single fd migration immediately.
    fn apply_fd_migration(&mut self, token: Token, from_priority: bool, to_priority: bool) {
        if from_priority {
            let _ = self.priority_poller.delete_fd(token);
        }
        if to_priority {
            if let Ok(fd) = self.transport.raw_fd(token) {
                let _ = self.priority_poller.register_fd(token, fd);
            }
        }
    }

    /// Process any fd migrations that were deferred because the connection
    /// had an inflight IO operation. Call this after IO completions are
    /// drained to retry migrations whose IO operations have finished.
    pub(crate) fn process_pending_fd_migrations(&mut self) {
        let pending: Vec<FdMigration> = std::mem::take(&mut self.qos.pending_migrations);
        for migration in pending {
            let (token, to_priority) = match &migration {
                FdMigration::ToPriority { token } => (*token, true),
                FdMigration::FromPriority { token } => (*token, false),
            };
            let inflight = self.io_threads.read_inflight_id(token).is_some()
                || self.io_threads.write_inflight_id(token).is_some();
            if inflight {
                self.qos.pending_migrations.push(migration);
                continue;
            }
            self.apply_fd_migration(token, !to_priority, to_priority);
        }
    }
}
