use std::net::SocketAddr;

use crate::framework::capabilities::connection::ConnectionPriority;
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::event_loop::EventLane;
use crate::runtime::server::worker::ServerWorkerState;

pub(crate) struct PriorityCycle {
    started_at: std::time::Instant,
    reset_generation: u64,
    commands_processed: u64,
}

impl ServerWorkerState {
    pub(crate) fn peer_is_priority(&self, address: SocketAddr) -> bool {
        address.port() != 0
            && self
                .ctx
                .config()
                .server
                .compiled_priority_subnets
                .iter()
                .any(|subnet| subnet.contains(address.ip()))
    }

    pub(crate) fn refresh_connection_priorities(&mut self) {
        // Subnet classification is fixed at admission. Internal priority
        // connections still participate in command accounting and scheduling.
        if self.priority_connections == 0 {
            return;
        }
        if let Some(token) = self.priority_command_token {
            let is_priority = self.is_priority_connection(token);
            self.ctx
                .stats_mut()
                .set_priority_command(is_priority && !self.priority_command_is_nested);
        }
    }

    pub(crate) fn prioritize_connection(&mut self, token: Token) {
        self.refresh_connection_priorities();
        if let Some(connection) = self.connection_registry.get_mut(token) {
            if !connection.priority.is_priority() {
                self.priority_connections += 1;
                connection.priority = ConnectionPriority::Internal;
            }
            self.event_batch
                .reclassify_readiness(token, EventLane::Upstream);
        }
        if self.priority_command_token == Some(token) {
            self.ctx
                .stats_mut()
                .set_priority_command(!self.priority_command_is_nested);
        }
    }

    pub(crate) fn is_priority_connection(&self, token: Token) -> bool {
        // Include internal connections in the scheduling fast-path count.
        if self.priority_connections == 0 {
            return false;
        }
        self.connection_registry
            .get(token)
            .is_some_and(|connection| connection.priority.is_priority())
    }

    pub(crate) fn has_client_capacity(&self, is_priority: bool) -> bool {
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

    pub(crate) fn begin_priority_cycle(&mut self) -> bool {
        if self.priority_cycle.is_some() {
            return false;
        }
        self.priority_cycle = Some(PriorityCycle {
            started_at: std::time::Instant::now(),
            reset_generation: self.ctx.stats().reset_generation,
            commands_processed: self.ctx.stats().priority_commands_processed,
        });
        true
    }

    pub(crate) fn finish_priority_cycle(&mut self, owns_cycle: bool) {
        if let Some(cycle) = owns_cycle.then(|| self.priority_cycle.take()).flatten() {
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
}
