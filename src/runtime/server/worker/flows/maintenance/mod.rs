use super::ServerWorkerState;
use crate::engine::distributed::replication::protocol as replication_svc;
use crate::engine::extension::script::PropagationOp;
use crate::foundation::io::sys::time::mstime;
use crate::framework::capabilities::io::Token;
use crate::framework::context::PauseAction;
use crate::framework::distributed::sentinel::timer::sentinel_cron;
use crate::runtime::server::worker::distributed::propagation::{
    sequence_propagation, SequencedPropagation,
};
use bytes::Bytes;
use std::time::{Duration, Instant};

pub(crate) fn parse_client_memory_limit(value: &str) -> Option<usize> {
    let value = value.trim().to_ascii_lowercase();
    let split_at = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(split_at);
    let multiplier = match unit {
        "" | "b" => 1,
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        _ => return None,
    };
    number.parse::<usize>().ok()?.checked_mul(multiplier)
}

fn client_output_buffer_usage(
    conn: &crate::framework::capabilities::connection::Connection,
) -> usize {
    conn.client_memory_usage().output
}

fn output_buffer_limits_for_connection(
    conn: &crate::framework::capabilities::connection::Connection,
    config: &crate::engine::extension::config::ReplicationConfig,
    repl_backlog_size: usize,
) -> (usize, usize, u64) {
    let is_slot_export = matches!(
        conn.slot_migration_role,
        Some(
            crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Export(_)
        )
    );
    if is_slot_export {
        let hard_limit = config.replica_output_buffer_hard_limit;
        let soft_limit = config.replica_output_buffer_soft_limit;
        return (
            if hard_limit > 0 {
                hard_limit.max(repl_backlog_size)
            } else {
                0
            },
            if soft_limit > 0 {
                soft_limit.max(repl_backlog_size)
            } else {
                0
            },
            config.replica_output_buffer_soft_limit_seconds,
        );
    }
    let is_pubsub =
        conn.sub_channel_count > 0 || conn.psub_channel_count > 0 || conn.ssub_channel_count > 0;
    if is_pubsub {
        (
            config.pubsub_output_buffer_hard_limit,
            config.pubsub_output_buffer_soft_limit,
            config.pubsub_output_buffer_soft_limit_seconds,
        )
    } else {
        (
            config.normal_output_buffer_hard_limit,
            config.normal_output_buffer_soft_limit,
            config.normal_output_buffer_soft_limit_seconds,
        )
    }
}

mod cluster_actions;
mod cluster_config;
mod cluster_demote_empty_primary;
mod cluster_demote_stale_primary;
mod cluster_pending;
mod cluster_replica_migration;
mod cluster_roles;
pub(crate) mod eviction;
pub(crate) mod lazyfree;

pub(crate) fn enqueue_expired_key_notifications(
    rcfg: &crate::engine::extension::config::RuntimeConfig,
    pending_publish: &mut Vec<(Bytes, Bytes)>,
    db_idx: u32,
    key: &[u8],
) {
    crate::engine::distributed::pubsub::PubSubState::publish_keyspace_event(
        rcfg,
        pending_publish,
        db_idx,
        key,
        'x',
        "expired",
    );
}

pub(crate) fn enqueue_evicted_key_notifications(
    rcfg: &crate::engine::extension::config::RuntimeConfig,
    pending_publish: &mut Vec<(Bytes, Bytes)>,
    db_idx: u32,
    key: &[u8],
) {
    crate::engine::distributed::pubsub::PubSubState::publish_keyspace_event(
        rcfg,
        pending_publish,
        db_idx,
        key,
        'e',
        "evicted",
    );
}

impl ServerWorkerState {
    #[cfg(test)]
    fn slot_export_output_memory(&self) -> usize {
        self.writes.slot_export_allocation_bytes
    }

    #[cfg(test)]
    fn memory_used_for_eviction(&self, _client_memory: usize) -> usize {
        self.ctx.maxmemory_state().logical
    }

    pub(crate) fn run_background_tasks(&mut self) {
        let watchdog_period = self.ctx.config().server.watchdog_period;
        if watchdog_period == 0 || !self.ctx.is_loading() {
            crate::foundation::io::sys::signal::schedule_watchdog(watchdog_period);
        }
        self.check_pending_signal();
        if self.ctx.pause_cron() != 0 {
            return;
        }
        self.drive_pending_shutdown();
        self.retry_pending_transport_removals();
        self.drive_slot_migration_actions();
        self.drive_slot_migration_progress();
        self.send_slot_migration_periodic_acks();

        let now = crate::foundation::io::sys::time::mstime();
        self.ctx.sample_instantaneous_metrics(now);
        self.cleanup_closed();
        self.ctx.migrate_sockets_mut().expire_idle(Instant::now());
        self.reap_bgsave_child();
        let dirty = self.ctx.persistence().dirty();
        self.ctx.persistence_mut().sample_rdb_metrics(dirty);
        self.reap_aof_child();
        self.check_scheduled_aof_rewrite();
        self.ctx.update_dict_resize_policy();
        self.disconnect_idle_clients();
        self.enforce_client_output_buffer_limits();
        self.check_auto_bgsave();
        self.check_auto_aof_rewrite();
        self.flush_aof_if_needed();
        self.check_blocked_timeouts();
        self.check_xreadgroup_nogroup_wakeups();
        self.check_pause_state();
        self.apply_deferred_replica_events();
        self.check_script_kill_state();
        // Reap a completed diskless child before scheduling a new full sync.
        // The relay EOF path clears the relay before waitpid(WNOHANG) can
        // observe the child's exit; ordering this before drive_replication
        // prevents the stale child slot from forcing a synchronous RDB fallback.
        self.reap_direct_dual_rdb_child();
        self.collect_idle_diskless_child_exit();
        self.send_replication_keepalives();
        self.send_replica_periodic_ack();
        self.drive_replication();
        self.expire_idle_replication_backlog();
        self.drive_pending_dual_rdb_snapshots();
        self.refresh_replication_buffer_accounting_if_dirty();
        self.refresh_replica_output_buffer_bytes();
        self.enforce_replica_output_buffer_limits();
        self.drive_shutdown_progress();
        self.drive_deferred_full_resyncs();
        self.disconnect_timedout_replicas();
        self.collect_idle_disk_sync_child_exit();
        self.pump_full_resync_transfers();
        self.process_pending_failover();
        if self.exit_if_shutdown_requested() {
            return;
        }
        sentinel_cron(&mut self.ctx);
        let mut sentinel_io =
            crate::runtime::server::worker::reactor::transport::RuntimeSentinelIo::new(
                &mut self.transport,
                &mut self.runtime_sources,
                &mut self.pending_transport_removals,
            );
        self.sentinel_links
            .cron(&mut self.ctx, &mut sentinel_io, mstime());

        self.run_cluster_cron();
        self.run_expire_cycle();
        self.run_active_defrag();
        self.run_dict_resize();
        self.process_expired_deletions();
        self.update_pause_skip_expire();
        self.clients_cron();
        if self.ctx.maxmemory_state().is_over_limit() {
            self.schedule_eviction();
        }
    }

    fn clients_cron(&mut self) {
        for (_, conn) in self.connection_registry.iter_mut() {
            conn.shrink_idle_query_buffer();
        }
        self.refresh_all_client_memory_usage();
        self.evict_clients();
    }

    pub(crate) fn expire_idle_replication_backlog(&mut self) {
        let now = mstime();
        let ttl_sec = self.ctx.config().replication.repl_backlog_ttl;
        let has_attached_replicas = !self.replication_runtime.primary_links().is_empty()
            || self.replication_runtime.has_psync_wait_owners();
        let is_primary = self.ctx.replication().role
            == crate::engine::distributed::replication::ReplicationRole::Primary;
        let backlog = &self.ctx.replication().backlog;
        if ttl_sec <= 0 || !is_primary || has_attached_replicas || !backlog.is_active() {
            self.ctx.replication_mut().backlog_idle_deadline_ms = 0;
            return;
        }
        let deadline = self.ctx.replication().backlog_idle_deadline_ms;
        if deadline == 0 {
            self.ctx.replication_mut().backlog_idle_deadline_ms =
                now.saturating_add(ttl_sec.saturating_mul(1_000));
        } else if now >= deadline {
            self.ctx.replication_mut().backlog.expire_history();
            self.ctx.replication_mut().backlog_idle_deadline_ms = 0;
        }
    }

    pub(crate) fn evict_clients(&mut self) {
        let Some(limit) = self.effective_client_memory_limit() else {
            return;
        };
        let mut total = self.ctx.normal_client_memory();
        if total <= limit {
            return;
        }

        let mut candidates = Vec::new();
        for (connection_token, conn) in self.connection_registry.iter() {
            if matches!(
                    conn.slot_migration_role,
                    Some(
                        crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Export(_)
                    )
                )
                || self.is_master_link(conn.token().0)
                || self.is_replica_connection(conn.token().0)
            {
                continue;
            }
            let memory = conn.accounted_normal_memory();
            if !conn.no_evict && !conn.import_source {
                candidates.push((connection_token.0, conn.token().0, memory));
            }
        }
        candidates.sort_unstable_by_key(|(_, _, memory)| *memory);
        while total > limit {
            let Some((connection_token, token_value, memory)) = candidates.pop() else {
                break;
            };
            if self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(connection_token))
            {
                self.remove_connection(token_value);
                total = total.saturating_sub(memory);
                self.ctx.incr_evicted_clients(1);
            }
        }
    }

    fn disconnect_idle_clients(&mut self) {
        let timeout = self.ctx.config().server.client_timeout;
        if timeout == 0 {
            return;
        }
        let timeout = Duration::from_secs(timeout);
        let timed_out: Vec<usize> = self
            .connection_registry
            .iter()
            .filter(|(_, connection)| {
                !connection.monitoring
                    && connection.blocked.is_none()
                    && connection.sub_channel_count == 0
                    && connection.psub_channel_count == 0
                    && connection.ssub_channel_count == 0
                    && connection.slot_migration_role.is_none()
                    && !self.is_master_link(connection.token().0)
                    && !self.is_replica_connection(connection.token().0)
                    && connection.last_interaction.elapsed() > timeout
            })
            .map(|(_, connection)| connection.token().0)
            .collect();
        for token in timed_out {
            crate::server_log!(verbose, "Closing idle client token={}", token);
            self.remove_connection(token);
        }
    }

    fn enforce_client_output_buffer_limits(&mut self) {
        let tokens: Vec<_> = self
            .connection_registry
            .iter()
            .map(|(_, conn)| conn.token().0)
            .collect();
        for token in tokens {
            self.enforce_client_output_buffer_limit_for(token);
        }
    }

    pub(crate) fn enforce_client_output_buffer_limit_for(&mut self, token_value: usize) -> bool {
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token_value))
            .then_some(token_value)
        else {
            return false;
        };
        let config = &self.ctx.config().replication;
        let repl_backlog_size = self.ctx.config().server.repl_backlog_size.max(0) as usize;
        let now_ms = mstime();
        // Slot exports use the replica limit class, but unlike ordinary
        // replicas their C output-buffer usage comes from the export client's
        // ready replies plus snapshot-deferred mutations. Snapshot pipe chunks
        // and bytes already accepted by the kernel are not client output.
        let slot_export_id = self.connection_registry.get(crate::framework::capabilities::io::Token(connection_token)).and_then(|conn| {
            let crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Export(
                id,
            ) = conn.slot_migration_role.as_ref()?
            else {
                return None;
            };
            Some(id.clone())
        });
        let owned_slot_export_bytes = slot_export_id
            .as_ref()
            .map(|id| self.slot_export_output_memory_usage(id, token_value));
        let output_buffer_usage = owned_slot_export_bytes.unwrap_or_else(|| {
            client_output_buffer_usage(
                &self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)],
            )
        });
        let should_close = self
            .connection_registry
            .get_mut(crate::framework::capabilities::io::Token(connection_token))
            .is_some_and(|conn| {
                let (hard_limit, soft_limit, soft_seconds) =
                    output_buffer_limits_for_connection(conn, config, repl_backlog_size);
                let bytes = output_buffer_usage;
                if hard_limit > 0 && bytes >= hard_limit {
                    return true;
                }
                if soft_limit == 0 || bytes < soft_limit {
                    conn.output.soft_limit_reached_at_ms = None;
                    return false;
                }
                let reached_at = conn.output.soft_limit_reached_at_ms.get_or_insert(now_ms);
                soft_seconds == 0
                    || now_ms.saturating_sub(*reached_at)
                        >= soft_seconds.saturating_mul(1000) as i64
            });
        if should_close {
            self.ctx
                .stats_mut()
                .client_output_buffer_limit_disconnections += 1;
            let conn = &self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)];
            crate::server_log!(
                warning,
                "Client id={} addr={} token={} scheduled to be closed ASAP for overcoming of output buffer limits. output-buffer-memory={}",
                conn.client_id(),
                conn.peer_addr,
                token_value,
                output_buffer_usage
            );
            self.remove_connection(token_value);
        }
        should_close
    }

    fn run_cluster_cron(&mut self) {
        if !self.ctx.config_mut().server.cluster_enabled {
            return;
        }
        self.refresh_replication_view();
        let now = mstime();
        let node_timeout = self.ctx.config().server.cluster_node_timeout;
        let ping_interval = self.ctx.config().server.cluster_ping_interval;
        let allow_replica_migration = self.ctx.config().server.cluster_allow_replica_migration;
        let manual_failover_timeout = self.ctx.config().server.cluster_manual_failover_timeout;
        let primary_link_connected = self
            .replication_runtime
            .replica_link()
            .is_some_and(|link| link.is_streaming());
        // Match C `replicationSetPrimary`: merely polling before the new
        // upstream has connected or failed must leave `repl_down_since` at
        // zero. Actual teardown records the first disconnect baseline.
        if self.ctx.replication().is_replica() && primary_link_connected {
            self.ctx.replication_mut().observe_primary_connected();
        }
        let local_failover_status =
            crate::engine::distributed::cluster::state::failover::LocalReplicaFailoverStatus {
                loading: self.ctx.is_loading() || self.ctx.replication().async_loading,
                priority: self.ctx.config().replication.replica_priority,
                data_age_ms: self.ctx.replication().primary_failover_data_age_ms(now),
                repl_ping_period_ms: self
                    .ctx
                    .config()
                    .replication
                    .repl_ping_replica_period
                    .saturating_mul(1_000),
                replica_validity_factor: self
                    .ctx
                    .config()
                    .server
                    .cluster_replica_validity_factor
                    .max(0) as u64,
            };
        self.cluster_bus.ping_interval = if ping_interval > 0 {
            ping_interval.min(i64::MAX as u64) as i64
        } else {
            (node_timeout / 2).max(1).min(i64::MAX as u64) as i64
        };
        let mut state_opt = self.ctx.cluster_state_mut().take();
        if let Some(ref mut state) = state_opt {
            // Match C clusterUpdateState(): the two-second vote/write safety
            // window starts at the first cluster cron evaluation, even when
            // that tick does not otherwise recompute topology.
            state.observe_topology_evaluation(now);
            state.set_manual_failover_timeout_ms(manual_failover_timeout);
            let expired_handshakes = state.expire_handshake_nodes(now, node_timeout);
            for node_id in expired_handshakes {
                let tokens: Vec<usize> = self
                    .cluster_bus
                    .connections
                    .iter()
                    .filter_map(|(token_value, connection)| {
                        (connection.node_name == node_id
                            || connection.handshake_node_name == node_id)
                            .then_some(*token_value)
                    })
                    .collect();
                for token_value in tokens {
                    self.teardown_cluster_bus_connection_with_state(
                        state,
                        Token(token_value),
                        false,
                    );
                }
            }
            self.cluster_bus
                .schedule_asymmetric_meets(state, now, node_timeout);
            state.set_replica_migration_allowed(allow_replica_migration);
            let zero = crate::engine::distributed::replication::ReplicationOffset::try_from(0)
                .expect("zero is a valid replication offset");
            let local_replication_offset = if self.ctx.replication().is_replica() {
                // C's replicationAttachToNewPrimary() discards the cached
                // primary client in the same event-loop turn that calls
                // startLoading() (both disk and socket RDB loads), so the
                // offset observable through cluster bus packets becomes zero
                // exactly when `loading` flips on. Keep advertising the
                // cached offset while the RDB is still being received or the
                // primary link is plain disconnected, otherwise peers hide a
                // non-empty replica from CLUSTER SLOTS and rank it as empty
                // before the dataset load actually starts.
                if self.ctx.is_loading() || self.ctx.replication().async_loading {
                    zero
                } else {
                    self.ctx.replication().slave_repl_offset
                }
            } else {
                self.ctx.replication().primary_repl_offset()
            };
            state.observe_local_replication(
                local_replication_offset.get(),
                local_failover_status.priority,
            );
            let mut repl_offsets: std::collections::HashMap<String, i64> =
                std::collections::HashMap::new();
            repl_offsets.insert(state.myself.name.clone(), state.myself.repl_offset);
            for (name, node) in &state.nodes {
                repl_offsets.insert(name.clone(), node.repl_offset);
            }

            self.cluster_process_pending_ops(state);

            let sendbuf_limit = self.ctx.config().server.cluster_link_sendbuf_limit;
            let manual_actions = state.manual_failover_tick(now, local_replication_offset);
            state.queue_cluster_actions(manual_actions);
            let broadcast_offsets = self
                .cluster_bus
                .connections
                .iter()
                .map(|(token, connection)| (*token, connection.write_buf.len()))
                .collect();
            let hints = self.cluster_bus.cron_with_replica_status(
                state,
                now,
                node_timeout,
                &repl_offsets,
                &local_failover_status,
            );
            self.execute_cluster_actions(state, hints.actions, now);
            // C frees stale links synchronously in clusterCron. Retire this
            // tick's closures before the receive pump can confirm a fresh
            // PONG; a deferred node-ID closure would otherwise tear down the
            // recovered link during the next cron.
            let actions = crate::engine::distributed::cluster::reducer::reduce(
                state,
                crate::engine::distributed::cluster::event::ClusterEvent::DrainLinkCloses,
            );
            self.execute_cluster_link_actions(state, actions);
            let actions = crate::engine::distributed::cluster::reducer::reduce(
                state,
                crate::engine::distributed::cluster::event::ClusterEvent::BroadcastTopology,
            );
            self.execute_cluster_bus_actions_at_offsets(state, actions, &broadcast_offsets);
            self.cluster_apply_pending_replication_switch(state);
            self.cluster_maybe_orphan_to_replica(state);

            self.cluster_demote_guard_empty_primary(state);
            if cluster_demote_stale_primary::should_run_stale_primary_guard(
                &mut self.last_stale_primary_guard_ms,
                now,
            ) {
                self.cluster_demote_stale_primary_guard(state);
            }

            self.cluster_migrate_replica_to_orphan(state, now);
            self.refresh_cluster_link_views_for_state(state);
            self.enforce_cluster_bus_sendbuf_limit_for_state(state, sendbuf_limit);
            self.persist_cluster_config_if_changed(state);
        }
        *self.ctx.cluster_state_mut() = state_opt;
        // C performs blocked-client redirection after the cluster cron and
        // its role transitions. Checking before `cluster_maybe_orphan_to_replica`
        // observes the new owner while we still look like a primary, which
        // incorrectly redirects a READONLY XREAD that remains valid once the
        // demotion commits.
        self.check_blocked_cluster_redirects();
        let changed_shard_slots = self
            .ctx
            .cluster_state_mut()
            .as_deref_mut()
            .map(|state| state.take_shard_slot_cleanup_requests())
            .unwrap_or_default();
        for &slot in &changed_shard_slots {
            self.ctx.slot_metrics_mut().reset_slot(slot as usize);
        }
        self.prune_unserved_shard_subscriptions(&changed_shard_slots);
        self.flush_cluster_bus_writes();
        self.pump_cluster_bus_reads();
        self.flush_cluster_bus_writes();
        self.refresh_cluster_link_views();
    }

    fn run_expire_cycle(&mut self) {
        if !self.ctx.config_mut().server.active_expire_enabled
            || self.ctx.config_mut().server.import_mode
        {
            return;
        }
        if self.ctx.replication().is_replica() && self.ctx.config().replication.replica_read_only {
            // Read-only replicas retain logically expired keys until the primary
            // sends its synthesized DEL/UNLINK, preserving replication order.
            return;
        }
        let now = mstime();
        let paused = self.ctx.client_pause().is_paused(now) != PauseAction::None;
        if paused {
            return;
        }
        let expire_start = mstime();
        let num_dbs = self.ctx.num_dbs();
        let replica_local_only =
            self.ctx.replication().is_replica() && !self.ctx.config().replication.replica_read_only;
        let expire_effort = self.ctx.config().server.active_expire_effort;
        for db_idx in 0..num_dbs {
            if let Some(db) = self.ctx.db_mut(db_idx as u32) {
                db.set_replica_local_expire_only(replica_local_only);
                crate::foundation::io::store::kv_engine::expire::active_expire_cycle_with_effort(
                    db,
                    false,
                    expire_effort,
                );
            }
        }
        let elapsed = mstime() - expire_start;
        if elapsed > 0 {
            self.ctx.latency_mut().add_sample("expire-cycle", elapsed);
        }
    }

    fn run_active_defrag(&mut self) {
        let Some((budget, max_keys)) = self.active_defrag_budget() else {
            self.active_defrag = Default::default();
            let stats = self.ctx.stats_mut();
            stats.active_defrag_running = false;
            stats.current_active_defrag_time = 0;
            return;
        };
        let started = Instant::now();
        let mut relocated = 0u64;
        let mut missed = 0u64;
        for _ in 0..max_keys {
            if started.elapsed() >= budget {
                break;
            }
            let Some((dbid, key)) = self.next_active_defrag_key() else {
                break;
            };
            if self
                .ctx
                .db_mut(dbid)
                .is_some_and(|db| db.relocate_entry(&key))
            {
                relocated = relocated.saturating_add(1);
            } else {
                missed = missed.saturating_add(1);
            }
        }
        let elapsed = started.elapsed().as_micros() as u64;
        let stats = self.ctx.stats_mut();
        stats.active_defrag_hits = stats.active_defrag_hits.saturating_add(relocated);
        stats.active_defrag_misses = stats.active_defrag_misses.saturating_add(missed);
        stats.active_defrag_key_hits = stats.active_defrag_key_hits.saturating_add(relocated);
        stats.active_defrag_key_misses = stats.active_defrag_key_misses.saturating_add(missed);
        stats.total_active_defrag_time = stats.total_active_defrag_time.saturating_add(elapsed);
        stats.current_active_defrag_time = elapsed;
        stats.active_defrag_running = relocated > 0 || missed > 0;
    }

    fn active_defrag_budget(&self) -> Option<(Duration, usize)> {
        let config = &self.ctx.config().server;
        if !config.activedefrag {
            return None;
        }
        let allocator = crate::foundation::memory::util_alloc::allocator_stats();
        let fragment_bytes = allocator.active.saturating_sub(allocator.allocated);
        let fragment_percent = fragment_bytes
            .saturating_mul(100)
            .checked_div(allocator.allocated)
            .unwrap_or(0);
        if fragment_bytes < config.active_defrag_ignore_bytes
            || fragment_percent < usize::from(config.active_defrag_threshold_lower)
        {
            return None;
        }
        let lower = usize::from(config.active_defrag_threshold_lower);
        let upper = usize::from(config.active_defrag_threshold_upper).max(lower + 1);
        let min = u64::from(config.active_defrag_cycle_min);
        let max = u64::from(
            config
                .active_defrag_cycle_max
                .max(config.active_defrag_cycle_min),
        );
        let progress = fragment_percent.saturating_sub(lower).min(upper - lower) as u64;
        let effort =
            min.saturating_add(progress.saturating_mul(max - min) / (upper - lower) as u64);
        let budget_us = config.active_defrag_cycle_us.max(1).saturating_mul(effort) / max.max(1);
        Some((
            Duration::from_micros(budget_us.max(1)),
            config.active_defrag_max_scan_fields.max(1),
        ))
    }

    fn next_active_defrag_key(&mut self) -> Option<(u32, Vec<u8>)> {
        let db_count = self.ctx.num_dbs();
        for _ in 0..db_count {
            if self.active_defrag.keys.is_empty() {
                self.active_defrag.keys = self
                    .ctx
                    .db(self.active_defrag.db_index as u32)?
                    .all_keys_sorted();
                self.active_defrag.key_index = 0;
            }
            let dbid = self.active_defrag.db_index as u32;
            if let Some(key) = self
                .active_defrag
                .keys
                .get(self.active_defrag.key_index)
                .cloned()
            {
                self.active_defrag.key_index += 1;
                if self.active_defrag.key_index == self.active_defrag.keys.len() {
                    self.active_defrag.keys.clear();
                    self.active_defrag.key_index = 0;
                    self.active_defrag.db_index = (self.active_defrag.db_index + 1) % db_count;
                }
                return Some((dbid, key));
            }
            self.active_defrag.db_index = (self.active_defrag.db_index + 1) % db_count;
            self.active_defrag.keys.clear();
        }
        None
    }

    fn run_dict_resize(&mut self) {
        if !self.ctx.config_mut().server.import_mode {
            self.ctx.refresh_dict_resize_budget();
            self.ctx
                .try_resize_all_dbs(self.ctx.config().server.active_rehashing);
        }
    }

    fn _sequence_background_unlink(&mut self, db_idx: u32, key: &[u8]) -> SequencedPropagation {
        if self.ctx.persistence_mut().take_rewrite_force_select() {
            self.ctx.persistence_mut().reset_aof_selected_db();
        }
        let op = PropagationOp::both(db_idx, replication_svc::encode_command(&[b"UNLINK", key]));
        let sequenced = sequence_propagation(
            &[op],
            self.ctx.repl_seldb(),
            self.ctx.persistence().aof_selected_db(),
        );
        self.ctx.set_repl_seldb(sequenced.replication_selected_db);
        self.ctx
            .persistence_mut()
            .set_aof_selected_db(sequenced.aof_selected_db);
        sequenced
    }

    pub(crate) fn _propagate_background_unlink(&mut self, db_idx: u32, key: &[u8]) {
        let migration_op =
            PropagationOp::both(db_idx, replication_svc::encode_command(&[b"UNLINK", key]));
        self.queue_slot_migration_mutations(std::slice::from_ref(&migration_op));
        let sequenced = self._sequence_background_unlink(db_idx, key);
        for frame in &sequenced.replication_frames {
            self.propagate_to_replicas(frame);
        }
        for frame in &sequenced.aof_frames {
            self.feed_aof(frame);
        }
    }

    fn process_expired_deletions(&mut self) {
        if !self.ctx.config().server.active_expire_enabled {
            return;
        }
        let now = mstime();
        let paused = self.ctx.client_pause().is_paused(now) != PauseAction::None;
        if paused {
            return;
        }
        let expired = self.ctx.drain_active_expired_deletions();
        let expired_hash_fields = self.ctx.drain_lazy_expired_hash_fields();
        let hexpired = self.ctx.drain_pending_hexpired();
        if expired.is_empty() && hexpired.is_empty() {
            return;
        }
        let mut pending_publish = Vec::new();
        let mut dirty_keys: Vec<(u32, Bytes)> = expired
            .iter()
            .map(|(db_idx, key)| (*db_idx, Bytes::from(key.clone())))
            .collect();
        let mut hash_expired_keys = std::collections::HashSet::new();
        for (db_idx, key, _) in &hexpired {
            if hash_expired_keys.insert((*db_idx, key.clone())) {
                dirty_keys.push((*db_idx, Bytes::from(key.clone())));
            }
        }
        let mut pending_tracking = Vec::new();
        self.collect_tracking_invalidations(&dirty_keys, None, &mut pending_tracking);
        self.ctx.incr_expired_keys(expired.len() as u64);
        self.ctx
            .persistence_mut()
            .add_dirty((expired.len() + hexpired.len()) as i64);
        if expired_hash_fields > 0 {
            self.ctx
                .stats_mut()
                .incr_expired_fields(expired_hash_fields);
        }
        for (db_idx, key_bytes) in &dirty_keys {
            for (_, conn) in self.connection_registry.iter_mut() {
                if conn.transaction.watched_keys().is_empty() {
                    continue;
                }
                conn.transaction
                    .touch_watched_key(*db_idx, key_bytes, false);
            }
        }
        for (db_idx, key) in expired {
            self._propagate_background_unlink(db_idx, &key);
            crate::runtime::server::worker::flows::maintenance::enqueue_expired_key_notifications(
                self.ctx.config(),
                &mut pending_publish,
                db_idx,
                &key,
            );
        }
        let mut notified_hash_keys = std::collections::HashSet::new();
        let mut deleted_hash_keys = std::collections::HashSet::new();
        for (db_idx, key, field) in hexpired {
            let frame = replication_svc::encode_command(&[b"HDEL", &key, &field]);
            let op = PropagationOp::both(db_idx, frame);
            self.queue_slot_migration_mutations(std::slice::from_ref(&op));
            let sequenced = sequence_propagation(
                &[op],
                self.ctx.repl_seldb(),
                self.ctx.persistence().aof_selected_db(),
            );
            self.ctx.set_repl_seldb(sequenced.replication_selected_db);
            self.ctx
                .persistence_mut()
                .set_aof_selected_db(sequenced.aof_selected_db);
            for frame in &sequenced.replication_frames {
                self.propagate_to_replicas(frame);
            }
            for frame in &sequenced.aof_frames {
                self.feed_aof(frame);
            }
            if notified_hash_keys.insert((db_idx, key.clone())) {
                let db_str = db_idx.to_string();
                if self.ctx.config_mut().is_event_type_enabled('K') {
                    let channel =
                        format!("__keyspace@{}__:{}", db_str, String::from_utf8_lossy(&key));
                    pending_publish.push((Bytes::from(channel), Bytes::from_static(b"hexpired")));
                }
                if self.ctx.config_mut().is_event_type_enabled('E') {
                    let channel = format!("__keyevent@{}__:hexpired", db_str);
                    pending_publish.push((Bytes::from(channel), Bytes::from(key.clone())));
                }
                let key_deleted = self
                    .ctx
                    .db(db_idx)
                    .map(|db| db.get_allow_expired(&key).is_none())
                    .unwrap_or(true);
                if key_deleted {
                    deleted_hash_keys.insert((db_idx, key));
                }
            }
        }
        for (db_idx, key) in deleted_hash_keys {
            self._propagate_background_unlink(db_idx, &key);
            let db_str = db_idx.to_string();
            if self.ctx.config_mut().is_event_type_enabled('K') {
                let channel = format!("__keyspace@{}__:{}", db_str, String::from_utf8_lossy(&key));
                pending_publish.push((Bytes::from(channel), Bytes::from_static(b"del")));
            }
            if self.ctx.config_mut().is_event_type_enabled('E') {
                let channel = format!("__keyevent@{}__:del", db_str);
                pending_publish.push((Bytes::from(channel), Bytes::from(key)));
            }
        }
        if !pending_publish.is_empty() {
            self.deliver_published(pending_publish, Vec::new());
        }
        for (target_id, frame) in pending_tracking {
            self.queue_client_reply(Token(target_id), frame);
        }
    }

    fn update_pause_skip_expire(&mut self) {
        let now = mstime();
        let paused = self.ctx.client_pause().is_paused(now) != PauseAction::None
            || (self.ctx.replication().is_replica()
                && self.ctx.config().replication.replica_read_only);
        let num_dbs = self.ctx.num_dbs();
        for db_idx in 0..num_dbs {
            if let Some(db) = self.ctx.db_mut(db_idx as u32) {
                db.set_pause_skip_expire(paused);
            }
        }
    }

    pub(crate) fn run_eviction(&mut self) {
        let outcome = self.perform_evictions();
        for (db_idx, key) in outcome.evicted {
            let mut pending_publish = Vec::new();
            self._propagate_background_unlink(db_idx, &key);
            crate::runtime::server::worker::flows::maintenance::enqueue_evicted_key_notifications(
                self.ctx.config(),
                &mut pending_publish,
                db_idx,
                &key,
            );
            if !pending_publish.is_empty() {
                self.deliver_published(pending_publish, Vec::new());
            }
        }
    }

    pub(crate) fn check_pending_signal(&mut self) {
        if crate::foundation::io::sys::signal::take_second_sigint() {
            crate::server_log!(
                warning,
                "{}",
                crate::engine::distributed::shutdown::messages::YOU_INSIST
            );
            let dir = self.ctx.config().persistence.dir.clone();
            if let Some(pid) = self.ctx.persistence().rdb_child_pid() {
                let _ = std::fs::remove_file(
                    std::path::Path::new(&dir).join(format!("temp-{}.rdb", pid)),
                );
            }
            self.cancel_disk_sync_batch();
            std::process::exit(1);
        }
        let sig = match crate::foundation::io::sys::signal::take_pending_signal() {
            None => return,
            Some(s) => s,
        };
        let flags = match sig {
            libc::SIGTERM => self.ctx.config_mut().server.shutdown_on_sigterm,
            libc::SIGINT => self.ctx.config_mut().server.shutdown_on_sigint,
            _ => crate::engine::distributed::shutdown::SHUTDOWN_NOFLAGS,
        };
        self.ctx.shutdown_state_mut().asap = true;
        self.ctx.shutdown_state_mut().last_sig = sig;
        self.ctx.shutdown_state_mut().pending_flags = flags;
    }

    pub(crate) fn drive_pending_shutdown(&mut self) {
        if !self.ctx.shutdown_state().asap || self.ctx.shutdown_state().initiated {
            return;
        }
        // C whileBlockedCron() discards configured signal flags and requests
        // NOSAVE while loading, so a partial dataset is never persisted and
        // SAFE/SAVE cannot delay termination at a loading checkpoint.
        let flags = if self.ctx.is_loading() {
            crate::engine::distributed::shutdown::SHUTDOWN_NOSAVE
        } else {
            self.ctx.shutdown_state().pending_flags
        };
        self.register_active_persistence_child();
        let shutdown_result = crate::runtime::server::supervisor::shutdown::prepare_for_shutdown(
            &mut self.ctx,
            flags,
        );
        self.register_active_persistence_child();
        if shutdown_result.is_err() {
            crate::runtime::server::supervisor::shutdown::clear_shutdown_pause(&mut self.ctx);
            self.reply_to_shutdown_clients();
        } else {
            self.send_shutdown_getack_if_needed();
        }
    }
}

#[cfg(test)]
mod active_defrag_tests {
    use crate::foundation::io::store::Robj;
    use bytes::Bytes;

    #[test]
    fn active_defrag_relocates_values_within_the_configured_cycle_budget() {
        let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
        let config = &mut runtime.ctx.config_mut().server;
        config.activedefrag = true;
        config.active_defrag_threshold_lower = 0;
        config.active_defrag_threshold_upper = 1;
        // Test-only direct config override: production registry keeps the C
        // minimum of one byte, while this isolates relocation from allocator
        // fragmentation state shared by parallel tests.
        config.active_defrag_ignore_bytes = 0;
        config.active_defrag_cycle_min = 1;
        config.active_defrag_cycle_max = 99;
        config.active_defrag_cycle_us = 100_000;
        config.active_defrag_max_scan_fields = 1;
        runtime.ctx.db_mut(0).unwrap().set(
            b"defrag-key".to_vec(),
            Robj::string_raw(Bytes::from(vec![b'x'; 128])),
        );
        let before = runtime
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"defrag-key")
            .unwrap()
            .as_bytes()
            .as_ptr();

        runtime.run_background_tasks();

        let after = runtime
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"defrag-key")
            .unwrap()
            .as_bytes()
            .as_ptr();
        assert_ne!(before, after);
        assert_eq!(runtime.ctx.stats().active_defrag_key_hits, 1);
        assert!(runtime.ctx.stats().total_active_defrag_time > 0);
    }

    #[test]
    fn disabled_active_defrag_does_not_relocate_values() {
        let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
        runtime.ctx.db_mut(0).unwrap().set(
            b"defrag-key".to_vec(),
            Robj::string_raw(Bytes::from(vec![b'x'; 128])),
        );
        let before = runtime
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"defrag-key")
            .unwrap()
            .as_bytes()
            .as_ptr();

        runtime.run_background_tasks();

        let after = runtime
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"defrag-key")
            .unwrap()
            .as_bytes()
            .as_ptr();
        assert_eq!(before, after);
        assert_eq!(runtime.ctx.stats().active_defrag_key_hits, 0);
    }
}

#[cfg(test)]
mod tests {
    use super::ServerWorkerState;
    use crate::engine::distributed::cluster::node::{
        ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
    };
    use crate::engine::distributed::cluster::state::ClusterState;
    use crate::engine::distributed::replication::protocol::encode_command;
    use crate::engine::distributed::replication::ReplicationOffset;
    use crate::framework::distributed::replication::{ConnectionId, MasterInfo, ReplicaLink};
    use std::net::{TcpListener, TcpStream};

    const EVICTION_EXPORT_KEY_BYTES: usize = 2 * 1024 * 1024;

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("valid test offset")
    }

    #[test]
    fn client_eviction_disabled_does_not_refresh_connection_memory() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().server.maxmemory_clients = "0".to_string();
        let mut connection = crate::framework::capabilities::connection::Connection::new_fake();
        connection.queue_reply(vec![b'x'; 32 * 1024]);
        let connection_token = runtime.connection_registry.insert(connection);

        runtime.evict_clients();

        assert_eq!(runtime.ctx.normal_client_memory(), 0);
        assert_eq!(
            runtime.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .accounted_normal_memory(),
            0
        );
    }

    #[test]
    fn primary_active_empty_backlog_is_scheduled_and_expires() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().replication.repl_backlog_ttl = 1;
        backend.ctx.replication_mut().backlog.activate();
        let old_replid = backend.ctx.replication().backlog.replid().to_string();
        assert!(backend.ctx.replication().backlog.allocation_bytes() > 0);

        backend.expire_idle_replication_backlog();

        assert!(backend.ctx.replication().backlog_idle_deadline_ms > 0);
        assert_eq!(backend.ctx.replication().backlog.replid(), old_replid);
        assert!(backend.ctx.replication().backlog.is_active());

        backend.ctx.replication_mut().backlog_idle_deadline_ms =
            crate::foundation::io::sys::time::mstime().saturating_sub(1);
        backend.expire_idle_replication_backlog();

        assert_ne!(backend.ctx.replication().backlog.replid(), old_replid);
        assert!(!backend.ctx.replication().backlog.is_active());
        assert_eq!(backend.ctx.replication().backlog.history_len(), 0);
        assert_eq!(backend.ctx.replication().backlog.allocation_bytes(), 0);
        assert_eq!(backend.ctx.replication().backlog_idle_deadline_ms, 0);
    }

    #[test]
    fn replica_without_downstreams_never_expires_its_backlog() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().replication.repl_backlog_ttl = 1;
        backend
            .ctx
            .replication_mut()
            .backlog
            .append(b"replica-history")
            .unwrap();
        backend.ctx.replication_mut().backlog.shift_replication_id();
        backend.ctx.replication_mut().switch_to_replica();
        let replid = backend.ctx.replication().backlog.replid().to_string();
        let second_replid = backend
            .ctx
            .replication()
            .backlog
            .second_replid()
            .to_string();
        let boundary = backend.ctx.replication().backlog.snapshot_boundary();
        let history_len = backend.ctx.replication().backlog.history_len();
        let allocation = backend.ctx.replication().backlog.allocation_bytes();
        backend.ctx.replication_mut().backlog_idle_deadline_ms =
            crate::foundation::io::sys::time::mstime().saturating_sub(1);

        backend.expire_idle_replication_backlog();

        assert_eq!(backend.ctx.replication().backlog.replid(), replid);
        assert_eq!(
            backend.ctx.replication().backlog.second_replid(),
            second_replid
        );
        assert!(backend.ctx.replication().backlog.is_active());
        assert_eq!(backend.ctx.replication().backlog.history_len(), history_len);
        assert_eq!(
            backend.ctx.replication().backlog.allocation_bytes(),
            allocation
        );
        assert_eq!(
            backend.ctx.replication().backlog.snapshot_boundary(),
            boundary
        );
        assert_eq!(backend.ctx.replication().backlog_idle_deadline_ms, 0);
    }

    #[test]
    fn downstream_replica_suppresses_backlog_expiry() {
        use crate::framework::distributed::replication::{RdbFilterMode, ReplicaConnectionInfo};

        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().replication.repl_backlog_ttl = 1;
        backend.ctx.replication_mut().backlog.activate();
        let replid = backend.ctx.replication().backlog.replid().to_string();
        backend.replication_runtime.register_primary(
            ConnectionId(99),
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: Vec::new(),
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        backend.ctx.replication_mut().backlog_idle_deadline_ms =
            crate::foundation::io::sys::time::mstime().saturating_sub(1);

        backend.expire_idle_replication_backlog();

        assert_eq!(backend.ctx.replication().backlog.replid(), replid);
        assert!(backend.ctx.replication().backlog.is_active());
        assert_eq!(backend.ctx.replication().backlog_idle_deadline_ms, 0);
    }

    #[test]
    fn grace_psync_owner_defers_ttl_until_live_owner_removal() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().replication.repl_backlog_ttl = 1;
        backend
            .ctx
            .replication_mut()
            .backlog
            .append(b"grace-owned-history")
            .unwrap();
        let main = ConnectionId(99);
        let old_replid = backend.ctx.replication().backlog.replid().to_string();
        let boundary = backend.ctx.replication().backlog.snapshot_boundary();
        let allocation = backend.ctx.replication().backlog.allocation_bytes();
        backend
            .replication_runtime
            .insert_grace_psync_wait_for_test(main, i64::MAX);
        backend.refresh_replication_view();
        assert_eq!(backend.ctx.replication().view.replicas_waiting_psync, 1);
        backend.ctx.replication_mut().backlog_idle_deadline_ms =
            crate::foundation::io::sys::time::mstime().saturating_sub(1);

        backend.expire_idle_replication_backlog();

        assert_eq!(backend.ctx.replication().backlog.replid(), old_replid);
        assert!(backend.ctx.replication().backlog.is_active());
        assert_eq!(
            backend.ctx.replication().backlog.snapshot_boundary(),
            boundary
        );
        assert_eq!(
            backend.ctx.replication().backlog.allocation_bytes(),
            allocation
        );
        assert_eq!(backend.ctx.replication().backlog_idle_deadline_ms, 0);

        assert_eq!(
            backend
                .replication_runtime
                .take_expired_grace_psync_waits(i64::MAX),
            vec![main]
        );
        assert_eq!(
            backend.ctx.replication().view.replicas_waiting_psync,
            1,
            "the cached view is intentionally stale until the normal refresh boundary"
        );

        backend.expire_idle_replication_backlog();

        assert!(backend.ctx.replication().backlog_idle_deadline_ms > 0);
        backend.ctx.replication_mut().backlog_idle_deadline_ms =
            crate::foundation::io::sys::time::mstime().saturating_sub(1);
        backend.expire_idle_replication_backlog();
        assert_ne!(backend.ctx.replication().backlog.replid(), old_replid);
        assert!(!backend.ctx.replication().backlog.is_active());
        assert_eq!(backend.ctx.replication().backlog.allocation_bytes(), 0);
    }

    #[test]
    fn retained_rdb_wait_blocks_backlog_ttl_until_its_grace_deadline_expires() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().replication.repl_backlog_ttl = 1;
        backend
            .ctx
            .replication_mut()
            .backlog
            .append(b"retained-wait-history")
            .unwrap();
        let rdb = ConnectionId(101);
        let public_client_id = 20_101;
        let old_replid = backend.ctx.replication().backlog.replid().to_string();
        let boundary = backend.ctx.replication().backlog.snapshot_boundary();
        backend
            .replication_runtime
            .register_rdb_candidate(rdb)
            .unwrap();
        backend
            .replication_runtime
            .insert_unpaired_rdb_psync_wait_for_test(rdb, boundary);
        backend
            .replication_runtime
            .backfill_waiting_psync_pins(boundary.stream_cursor());
        assert!(backend
            .replication_runtime
            .retain_completed_unpaired_rdb_wait(
                rdb,
                public_client_id,
                crate::foundation::io::sys::time::mstime() + 10_000,
            ));
        backend.replication_runtime.remove_connection(rdb);
        backend.refresh_replication_view();
        backend.ctx.replication_mut().backlog_idle_deadline_ms =
            crate::foundation::io::sys::time::mstime().saturating_sub(1);

        backend.expire_idle_replication_backlog();

        assert_eq!(backend.ctx.replication().backlog.replid(), old_replid);
        assert_eq!(backend.ctx.replication().backlog_idle_deadline_ms, 0);
        assert_eq!(backend.ctx.replication().view.replicas_waiting_psync, 1);

        assert_eq!(
            backend
                .replication_runtime
                .take_expired_retained_rdb_psync_waits(i64::MAX),
            vec![(public_client_id, None)]
        );
        backend.refresh_replication_view();
        backend.expire_idle_replication_backlog();
        assert!(backend.ctx.replication().backlog_idle_deadline_ms > 0);
        backend.ctx.replication_mut().backlog_idle_deadline_ms =
            crate::foundation::io::sys::time::mstime().saturating_sub(1);
        backend.expire_idle_replication_backlog();

        assert_ne!(backend.ctx.replication().backlog.replid(), old_replid);
        assert!(!backend.ctx.replication().backlog.is_active());
        assert_eq!(backend.ctx.replication().view.replicas_waiting_psync, 0);
    }

    #[test]
    fn unpaired_unpinned_rdb_wait_owner_defers_empty_backlog_ttl() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().replication.repl_backlog_ttl = 1;
        backend.ctx.replication_mut().backlog.activate();
        let rdb = ConnectionId(100);
        let replid = backend.ctx.replication().backlog.replid().to_string();
        let allocation = backend.ctx.replication().backlog.allocation_bytes();
        let boundary = backend.ctx.replication().backlog.snapshot_boundary();
        backend
            .replication_runtime
            .insert_unpaired_rdb_psync_wait_for_test(rdb, boundary);
        assert_eq!(
            backend.replication_runtime.min_protected_backlog_cursor(),
            None
        );
        backend.ctx.replication_mut().backlog_idle_deadline_ms =
            crate::foundation::io::sys::time::mstime().saturating_sub(1);

        backend.expire_idle_replication_backlog();

        assert_eq!(backend.ctx.replication().backlog.replid(), replid);
        assert!(backend.ctx.replication().backlog.is_active());
        assert_eq!(backend.ctx.replication().backlog.history_len(), 0);
        assert_eq!(
            backend.ctx.replication().backlog.allocation_bytes(),
            allocation
        );
        assert_eq!(backend.ctx.replication().backlog_idle_deadline_ms, 0);
        assert_eq!(
            backend
                .replication_runtime
                .rebuild_view()
                .replicas_waiting_psync,
            1
        );
    }

    #[test]
    fn inactive_backlog_clears_an_obsolete_expiry_deadline() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().replication.repl_backlog_ttl = 1;
        let replid = backend.ctx.replication().backlog.replid().to_string();
        backend.ctx.replication_mut().backlog_idle_deadline_ms =
            crate::foundation::io::sys::time::mstime().saturating_sub(1);

        backend.expire_idle_replication_backlog();

        assert_eq!(backend.ctx.replication().backlog.replid(), replid);
        assert!(!backend.ctx.replication().backlog.is_active());
        assert_eq!(backend.ctx.replication().backlog_idle_deadline_ms, 0);
    }

    #[test]
    fn client_timeout_closes_only_idle_ordinary_clients() {
        use crate::framework::capabilities::connection::Connection;
        use crate::framework::capabilities::io::Token;
        use std::time::{Duration, Instant};

        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().server.client_timeout = 1;

        let ordinary_token = Token(41);
        let mut ordinary = Connection::new(ordinary_token, ([127, 0, 0, 1], 6379).into(), false);
        ordinary.last_interaction = Instant::now() - Duration::from_secs(2);
        let _ordinary_slab = runtime.connection_registry.insert(ordinary);

        let pubsub_token = Token(42);
        let mut pubsub = Connection::new(pubsub_token, ([127, 0, 0, 1], 6380).into(), false);
        pubsub.last_interaction = Instant::now() - Duration::from_secs(2);
        pubsub.sub_channel_count = 1;
        let _pubsub_slab = runtime.connection_registry.insert(pubsub);

        runtime.disconnect_idle_clients();

        assert!(!runtime
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(ordinary_token.0)));
        assert!(runtime
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(pubsub_token.0)));
    }

    #[test]
    fn clients_cron_refreshes_connection_memory_when_eviction_is_disabled() {
        use crate::framework::capabilities::connection::Connection;
        use crate::framework::capabilities::io::Token;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().server.maxmemory_clients = "0".to_string();
        let token = Token(43);
        let mut connection = Connection::new(token, ([127, 0, 0, 1], 6379).into(), false);
        connection.queue_reply(vec![b'x'; 32 * 1024]);
        let expected = connection.client_memory_usage().total;
        let connection_token = runtime.connection_registry.insert(connection);

        runtime.clients_cron();

        assert_eq!(runtime.ctx.normal_client_memory(), expected);
        assert_eq!(
            runtime.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .accounted_normal_memory(),
            expected
        );
    }

    #[test]
    fn client_eviction_uses_cached_largest_first_contributions() {
        use crate::framework::capabilities::connection::Connection;
        use crate::framework::capabilities::io::Token;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().server.maxmemory_clients = "128kb".to_string();
        let small_token = Token(44);
        let large_token = Token(45);
        let small = Connection::new(small_token, ([127, 0, 0, 1], 6379).into(), false);
        let mut large = Connection::new(large_token, ([127, 0, 0, 1], 6380).into(), false);
        large.queue_reply(vec![b'x'; 200 * 1024]);
        let small_slab = runtime.connection_registry.insert(small);
        let _large_slab = runtime.connection_registry.insert(large);
        runtime.refresh_all_client_memory_usage();
        let expected_remaining = runtime.connection_registry
            [crate::framework::capabilities::io::Token(small_slab)]
        .accounted_normal_memory();

        runtime.evict_clients();

        assert!(runtime
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(small_token.0)));
        assert!(!runtime
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(large_token.0)));
        assert_eq!(runtime.ctx.normal_client_memory(), expected_remaining);
        assert_eq!(runtime.ctx.stats().evicted_clients, 1);
    }

    fn run_allocator_sensitive_test_in_isolation(test_name: &str, marker: &str) -> bool {
        if std::env::var_os(marker).is_some() {
            return false;
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test_name])
            .env(marker, "1")
            .status()
            .expect("run allocator-sensitive eviction test in isolation");
        assert_eq!(
            status.code(),
            Some(86),
            "isolated eviction regression did not execute its test body"
        );
        true
    }

    fn finish_allocator_sensitive_test(marker: &str) {
        if std::env::var_os(marker).is_some() {
            std::process::exit(86);
        }
    }

    fn slot_export_eviction_key(suffix: u8) -> Vec<u8> {
        let mut key = Vec::with_capacity(EVICTION_EXPORT_KEY_BYTES);
        key.extend_from_slice(b"{slot-export-eviction}:");
        key.resize(EVICTION_EXPORT_KEY_BYTES, suffix);
        key
    }

    fn register_streaming_export_transport(
        runtime: &mut ServerWorkerState,
        id: &crate::engine::distributed::cluster::migration::MigrationJobId,
        initial_export_bytes: usize,
    ) -> (TcpStream, usize, usize) {
        use crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole;
        use crate::framework::distributed::migration::SlotExportLink;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind export target");
        let target_addr = listener.local_addr().expect("export target address");
        let export = runtime
            .transport
            .connect_tcp(target_addr)
            .expect("connect export transport");
        let (target, _) = listener.accept().expect("accept export transport");
        let token_value = export.token.0;
        runtime.add_connection(export);
        let connection_token = token_value;
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .slot_migration_role = Some(SlotMigrationConnectionRole::Export(id.clone()));
        if initial_export_bytes > 0 {
            runtime.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .queue_replication_reply(vec![b'x'; initial_export_bytes]);
        }
        runtime
            .slot_migration_runtime
            .register_export(SlotExportLink::new(id.clone(), token_value));
        runtime.track_slot_export_connection_output(connection_token);
        (target, token_value, connection_token)
    }

    fn runtime_with_streaming_export(
        initial_export_bytes: usize,
    ) -> (ServerWorkerState, TcpStream, usize) {
        use crate::engine::distributed::cluster::migration::{
            ExportPhase, MigrationJob, MigrationJobId, MigrationOperation, MigrationPhase,
            SlotRanges,
        };
        use crate::engine::distributed::cluster::{NodeId, SlotRange};

        let first_key = slot_export_eviction_key(b'a');
        let second_key = slot_export_eviction_key(b'b');
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(&first_key);
        assert_eq!(
            slot,
            crate::engine::distributed::cluster::node::key_hash_slot(&second_key)
        );

        let mut runtime = ServerWorkerState::new_with_slot_router(1, 14, |key| {
            crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
        })
        .unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.config_mut().server.maxmemory_clients = "1%".to_string();
        runtime.ctx.config_mut().server.maxmemory_policy = "allkeys-random".to_string();
        runtime.ctx.config_mut().server.maxmemory_eviction_tenacity = 100;
        runtime.ctx.db_mut(0).unwrap().set(
            first_key,
            crate::foundation::io::store::Robj::string(b"v".as_slice()),
        );
        runtime.ctx.db_mut(0).unwrap().set(
            second_key,
            crate::foundation::io::store::Robj::string(b"v".as_slice()),
        );

        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut job = MigrationJob::new(
            id.clone(),
            NodeId::parse(state.myself.name.as_bytes()).unwrap(),
            NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
            SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap(),
            MigrationOperation::Export,
            1,
        );
        job.phase = MigrationPhase::Export(ExportPhase::Streaming);
        state.migrations.start(job).unwrap();
        *runtime.ctx.cluster_state_mut() = Some(Box::new(state));

        let (target, token_value, _) =
            register_streaming_export_transport(&mut runtime, &id, initial_export_bytes);
        (runtime, target, token_value)
    }

    fn _assert_unwrapped_unlink(frames: &[Vec<u8>], expected: &[Vec<u8>], sink_name: &str) {
        assert_eq!(frames, expected, "unexpected {sink_name} frames");
        assert!(
            frames
                .iter()
                .all(|frame| frame != &encode_command(&[b"MULTI"])
                    && frame != &encode_command(&[b"EXEC"])),
            "{sink_name} must not wrap a single expired effect"
        );
    }

    #[test]
    fn cluster_cron_applies_explicit_ping_interval() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().server.cluster_enabled = true;
        backend.ctx.config_mut().server.cluster_ping_interval = 100;
        backend.ctx.config_mut().server.cluster_node_timeout = 3_000;

        backend.run_cluster_cron();

        assert_eq!(backend.cluster_bus.ping_interval, 100);
    }

    #[test]
    fn cluster_cron_defaults_ping_interval_to_half_timeout() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().server.cluster_enabled = true;
        backend.ctx.config_mut().server.cluster_ping_interval = 0;
        backend.ctx.config_mut().server.cluster_node_timeout = 3_000;

        backend.run_cluster_cron();

        assert_eq!(backend.cluster_bus.ping_interval, 1_500);
    }

    #[test]
    fn cluster_cron_closes_stale_bus_link_in_the_same_tick() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().server.cluster_enabled = true;
        backend.ctx.config_mut().server.cluster_node_timeout = 1_000;
        let peer = backend.transport.connect_tcp(addr).unwrap();
        let (_server, _) = listener.accept().unwrap();
        backend
            .route_runtime_source(
                peer.token,
                crate::runtime::server::worker::reactor::sources::SourceOwner::ClusterBus,
            )
            .unwrap();
        let now_ms = crate::foundation::io::sys::time::mstime();
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let mut node = ClusterNode::new(
            node_id.to_string(),
            addr.ip().to_string(),
            addr.port(),
            addr.port(),
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.ping_sent = now_ms - 750;
        node.data_received = now_ms - 750;
        node.pong_received = now_ms - 750;
        state.nodes.insert(node_id.to_string(), node);
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend.cluster_bus.register_outgoing_for_node(
            peer.token,
            addr,
            false,
            node_id.to_string(),
        );
        let connection = backend
            .cluster_bus
            .connections
            .get_mut(&peer.token.0)
            .unwrap();
        connection.created_at = now_ms - 2_000;
        connection.handshake_sent = true;

        backend.run_cluster_cron();

        assert!(
            !backend.cluster_bus.connections.contains_key(&peer.token.0),
            "cron must retire its stale link before a receive pump can observe a recovered peer"
        );
        assert!(
            backend
                .ctx
                .cluster_state()
                .as_ref()
                .unwrap()
                .pending_link_closes
                .is_empty(),
            "a stale-link decision must not survive into the next cron"
        );
    }

    fn cluster_replica_runtime() -> ServerWorkerState {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().server.cluster_enabled = true;
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.replication_mut().slave_repl_offset = offset(42);
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA;
        state.myself.replicaof = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
        let mut primary = ClusterNode::new(
            state.myself.replicaof.clone(),
            "127.0.0.1".to_string(),
            6379,
            16_379,
            true,
        );
        primary.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(primary.name.clone(), primary);
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend
    }

    #[test]
    fn cluster_cron_preserves_unobserved_primary_link_failover_sentinel() {
        let mut backend = cluster_replica_runtime();
        let before_ms = crate::foundation::io::sys::time::mstime();
        assert_eq!(
            backend
                .ctx
                .replication()
                .primary_failover_data_age_ms(before_ms),
            u64::MAX
        );

        backend.run_cluster_cron();

        assert_eq!(
            backend
                .ctx
                .replication()
                .primary_failover_data_age_ms(crate::foundation::io::sys::time::mstime()),
            u64::MAX,
            "polling before a primary-link outcome must not make a freshly demoted replica eligible"
        );
    }

    #[test]
    fn cluster_cron_advertises_zero_replica_offset_while_full_sync_is_loading() {
        let mut backend = cluster_replica_runtime();
        backend.ctx.set_loading(true);

        backend.run_cluster_cron();

        assert_eq!(
            backend
                .ctx
                .cluster_state()
                .as_deref()
                .unwrap()
                .myself
                .repl_offset,
            0
        );
    }

    #[test]
    fn cluster_cron_advertises_zero_replica_offset_while_async_full_sync_is_loading() {
        let mut backend = cluster_replica_runtime();
        backend.ctx.replication_mut().async_loading = true;

        backend.run_cluster_cron();

        assert_eq!(
            backend
                .ctx
                .cluster_state()
                .as_deref()
                .unwrap()
                .myself
                .repl_offset,
            0
        );
    }

    #[test]
    fn cluster_cron_retains_cached_replica_offset_after_fullresync_before_rdb_load() {
        let mut backend = cluster_replica_runtime();
        let mut link = ReplicaLink::new(
            ConnectionId(42),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        link.connected(None, None);
        link.on_bytes(
            b"+PONG\r\n+OK\r\n+OK\r\n+FULLRESYNC 0123456789012345678901234567890123456789 99\r\n",
            6380,
        )
        .expect("FULLRESYNC header must enter the RDB receive phase");
        assert!(link.is_receiving_rdb());
        backend.replication_runtime.set_replica_link(link);

        backend.run_cluster_cron();

        // C discards the cached primary in replicationAttachToNewPrimary(),
        // which runs in the same event-loop turn as startLoading(). While the
        // RDB payload is still being received the cached offset stays visible,
        // so peers must not hide the replica before `loading` flips on.
        assert_eq!(
            backend
                .ctx
                .cluster_state()
                .as_deref()
                .unwrap()
                .myself
                .repl_offset,
            42
        );
    }

    #[test]
    fn cluster_cron_retains_replica_offset_while_disconnected() {
        let mut backend = cluster_replica_runtime();

        backend.run_cluster_cron();

        assert_eq!(
            backend
                .ctx
                .cluster_state()
                .as_deref()
                .unwrap()
                .myself
                .repl_offset,
            42
        );
    }

    #[test]
    fn cluster_cron_starts_the_primary_vote_safety_window_before_topology_changes() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().server.cluster_enabled = true;
        *backend.ctx.cluster_state_mut() =
            Some(Box::new(ClusterState::new("127.0.0.1", 6379, 16_379)));

        backend.run_cluster_cron();
        let after = crate::foundation::io::sys::time::mstime();

        assert!(backend
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .safe_to_vote(after.saturating_add(2_000)));
    }

    #[test]
    fn cluster_cron_forgets_the_handshake_connection_created_by_meet() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let meet_addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().server.cluster_enabled = true;
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let meet_ip = meet_addr.ip().to_string();
        let handshake_id = state.ensure_handshake_node(
            &meet_ip,
            meet_addr.port(),
            meet_addr.port(),
            crate::foundation::io::sys::time::mstime(),
        );
        state
            .pending_meets
            .push((meet_ip.clone(), meet_addr.port(), meet_addr.port()));
        state
            .pending_forgets
            .extend([handshake_id, "unknown-pending-forget".to_string()]);
        state.set_debug_disable_reconnection(true);
        let pending_connect = (meet_ip, meet_addr.port(), false);
        state.pending_connects.push(pending_connect.clone());
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));

        backend.run_cluster_cron();

        let state = backend
            .ctx
            .cluster_state()
            .as_deref()
            .expect("cluster state is returned after cron");
        assert!(state.pending_meets.is_empty());
        assert!(backend.cluster_bus.connections.is_empty());
        assert!(state.pending_forgets.is_empty());
        assert_eq!(state.pending_connects, vec![pending_connect]);
    }

    #[test]
    fn cluster_cron_keeps_asymmetric_meet_repair_after_slot_ownership() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().server.cluster_enabled = true;
        backend.ctx.config_mut().server.cluster_node_timeout = 1_000;

        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.add_slot(0).unwrap();
        let node_id = "peer".to_string();
        let mut peer =
            ClusterNode::new(node_id.clone(), "127.0.0.1".to_string(), 6380, 16380, true);
        peer.flags &= !CLUSTER_NODE_MYSELF;
        peer.asymmetric_meet_started_at = 1;
        peer.inbound_link_missing_since = 1;
        state.nodes.insert(node_id.clone(), peer);
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = backend.transport.connect_tcp(addr).unwrap();
        let _peer = listener.accept().unwrap();
        backend
            .cluster_bus
            .register_outgoing_for_node(handle.token, addr, false, node_id.clone());
        backend
            .cluster_bus
            .connections
            .get_mut(&handle.token.0)
            .unwrap()
            .handshake_sent = true;

        backend.run_cluster_cron();

        let state = backend.ctx.cluster_state().as_ref().unwrap();
        assert!(state.nodes[&node_id].asymmetric_meet_started_at > 0);
        assert!(state.nodes[&node_id].meet_pending);
        assert!(backend.cluster_bus.connections[&handle.token.0].handshake_sent);
    }

    #[test]
    fn active_expiry_sequences_unwrapped_unlink_from_independent_cursors() {
        let mut backend = ServerWorkerState::new(16).unwrap();
        backend.ctx.set_repl_seldb(10);
        backend.ctx.persistence_mut().set_aof_selected_db(11);
        let unlink = encode_command(&[b"UNLINK", b"expired"]);
        let select = encode_command(&[b"SELECT", b"10"]);

        let sequenced = backend._sequence_background_unlink(10, b"expired");

        _assert_unwrapped_unlink(
            &sequenced.replication_frames,
            std::slice::from_ref(&unlink),
            "replication",
        );
        _assert_unwrapped_unlink(&sequenced.aof_frames, &[select, unlink], "AOF");
        assert_eq!(backend.ctx.repl_seldb(), 10);
        assert_eq!(backend.ctx.persistence().aof_selected_db(), 10);
    }

    #[test]
    fn replica_background_expiry_keeps_read_only_replica_keys() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.config_mut().replication.replica_read_only = true;
        let db = backend.ctx.db_mut(0).expect("database 0 exists");
        db.set(
            b"expired".to_vec(),
            crate::foundation::io::store::Robj::string(b"v".as_slice()),
        );
        db.set_expire(
            b"expired",
            crate::foundation::io::store::kv_engine::now_ms().saturating_sub(1),
        );

        backend.run_expire_cycle();

        assert!(backend
            .ctx
            .db(0)
            .unwrap()
            .get_allow_expired(b"expired")
            .is_some());
    }

    #[test]
    fn replica_lazy_expiry_keeps_read_only_replica_keys() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.config_mut().replication.replica_read_only = true;
        let db = backend.ctx.db_mut(0).expect("database 0 exists");
        db.set(
            b"expired".to_vec(),
            crate::foundation::io::store::Robj::string(b"v".as_slice()),
        );
        db.set_expire(
            b"expired",
            crate::foundation::io::store::kv_engine::now_ms().saturating_sub(1),
        );
        backend.ctx.dbs_mut().set_pause_skip_expire_all(true);

        assert!(!backend.ctx.db_mut(0).unwrap().exists(b"expired"));
        assert!(backend
            .ctx
            .db(0)
            .unwrap()
            .get_allow_expired(b"expired")
            .is_some());
    }

    #[test]
    fn writable_replica_set_expire_is_tracked_for_local_expiry() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.config_mut().replication.replica_read_only = false;
        let mut conn = crate::framework::capabilities::connection::Connection::new_fake();
        conn.selected_db = 0;
        let args = crate::engine::command::CommandArgs::from_parts(vec![
            bytes::Bytes::from_static(b"SET"),
            bytes::Bytes::from_static(b"local"),
            bytes::Bytes::from_static(b"1"),
            bytes::Bytes::from_static(b"EX"),
            bytes::Bytes::from_static(b"1"),
        ]);
        crate::framework::commands::command_runtime::execute_server_command(
            &mut conn,
            &args,
            &mut backend.ctx,
            crate::framework::commands::string::set::set,
        );
        backend.ctx.db_mut(0).unwrap().set_expire(
            b"local",
            crate::foundation::io::store::kv_engine::now_ms().saturating_sub(1),
        );

        backend.run_expire_cycle();

        assert!(backend
            .ctx
            .db(0)
            .unwrap()
            .get_allow_expired(b"local")
            .is_none());
    }

    #[test]
    fn background_eviction_sequences_unwrapped_unlink_from_independent_cursors() {
        let mut backend = ServerWorkerState::new(16).unwrap();
        backend.ctx.set_repl_seldb(11);
        backend.ctx.persistence_mut().set_aof_selected_db(10);
        let unlink = encode_command(&[b"UNLINK", b"evicted"]);
        let select = encode_command(&[b"SELECT", b"10"]);

        let sequenced = backend._sequence_background_unlink(10, b"evicted");

        _assert_unwrapped_unlink(
            &sequenced.replication_frames,
            &[select, unlink.clone()],
            "replication",
        );
        _assert_unwrapped_unlink(&sequenced.aof_frames, std::slice::from_ref(&unlink), "AOF");
        assert_eq!(backend.ctx.repl_seldb(), 10);
        assert_eq!(backend.ctx.persistence().aof_selected_db(), 10);
    }

    #[test]
    fn background_unlink_is_forwarded_to_matching_slot_export() {
        use crate::engine::distributed::cluster::migration::{
            ExportPhase, MigrationJob, MigrationJobId, MigrationOperation, MigrationPhase,
            SlotRanges,
        };
        use crate::engine::distributed::cluster::{ClusterState, NodeId, SlotRange};

        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        let key = b"{atomic}expired";
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut job = MigrationJob::new(
            id.clone(),
            NodeId::parse(state.myself.name.as_bytes()).unwrap(),
            NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
            SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap(),
            MigrationOperation::Export,
            1,
        );
        job.phase = MigrationPhase::Export(ExportPhase::Streaming);
        state.migrations.start(job).unwrap();
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        let (_target, _token_value, connection_token) =
            register_streaming_export_transport(&mut backend, &id, 0);

        backend._propagate_background_unlink(0, key);

        let queued = backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .output
        .queue
        .iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
        assert!(queued.windows(6).any(|window| window == b"UNLINK"));
        assert!(queued.windows(key.len()).any(|window| window == key));
    }

    #[test]
    fn slot_export_uses_replica_output_buffer_limits() {
        use crate::engine::distributed::cluster::migration::{
            MigrationJobId, SlotMigrationConnectionRole,
        };
        use crate::framework::capabilities::connection::Connection;

        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut connection = Connection::new_fake();
        connection.slot_migration_role = Some(SlotMigrationConnectionRole::Export(
            MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
        ));
        backend
            .ctx
            .config_mut()
            .replication
            .replica_output_buffer_hard_limit = 10;
        backend
            .ctx
            .config_mut()
            .replication
            .replica_output_buffer_soft_limit = 20;
        backend.ctx.config_mut().server.repl_backlog_size = 16;

        assert_eq!(
            super::output_buffer_limits_for_connection(
                &connection,
                &backend.ctx.config().replication,
                16,
            ),
            (
                16,
                20,
                backend
                    .ctx
                    .config()
                    .replication
                    .replica_output_buffer_soft_limit_seconds
            )
        );
    }

    #[test]
    fn slot_import_uses_normal_limits_while_export_uses_replica_floor() {
        use crate::engine::distributed::cluster::migration::{
            MigrationJobId, SlotMigrationConnectionRole,
        };
        use crate::framework::capabilities::connection::Connection;

        let mut backend = ServerWorkerState::new(1).unwrap();
        let import_id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let export_id = MigrationJobId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
        let mut import = Connection::new_fake();
        import.slot_migration_role = Some(SlotMigrationConnectionRole::Import(import_id));
        let mut export = Connection::new_fake();
        export.slot_migration_role = Some(SlotMigrationConnectionRole::Export(export_id));
        let config = &mut backend.ctx.config_mut().replication;
        config.normal_output_buffer_hard_limit = 3;
        config.normal_output_buffer_soft_limit = 4;
        config.normal_output_buffer_soft_limit_seconds = 5;
        config.replica_output_buffer_hard_limit = 10;
        config.replica_output_buffer_soft_limit = 12;
        config.replica_output_buffer_soft_limit_seconds = 13;
        let repl_backlog_size = 16;

        assert_eq!(
            super::output_buffer_limits_for_connection(
                &import,
                &backend.ctx.config().replication,
                repl_backlog_size,
            ),
            (3, 4, 5),
            "slot imports remain ordinary clients without a backlog floor"
        );
        assert_eq!(
            super::output_buffer_limits_for_connection(
                &export,
                &backend.ctx.config().replication,
                repl_backlog_size,
            ),
            (16, 16, 13),
            "slot exports use the replica class and its backlog floor"
        );
    }

    #[test]
    fn slot_export_output_queue_is_excluded_from_maxmemory() {
        use crate::engine::distributed::cluster::migration::{
            MigrationJobId, SlotMigrationConnectionRole,
        };
        use crate::framework::capabilities::connection::Connection;
        use crate::framework::capabilities::io::Token;

        let mut backend = ServerWorkerState::new(1).unwrap();
        let export_token = Token(99);
        let mut export = Connection::new_fake();
        export.set_token_for_test(export_token);
        export.slot_migration_role = Some(SlotMigrationConnectionRole::Export(
            MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
        ));
        export.queue_reply(vec![b's'; 11]);
        export.queue_replication_reply(vec![b'm'; 13]);
        let export_slab = backend.connection_registry.insert(export);
        let export_id = match backend.connection_registry
            [crate::framework::capabilities::io::Token(export_slab)]
        .slot_migration_role
        .as_ref()
        .unwrap()
        {
            SlotMigrationConnectionRole::Export(id) => id.clone(),
            SlotMigrationConnectionRole::Import(_) => unreachable!(),
        };
        backend.slot_migration_runtime.register_export(
            crate::framework::distributed::migration::SlotExportLink::new(
                export_id.clone(),
                backend.connection_registry[crate::framework::capabilities::io::Token(export_slab)]
                    .token()
                    .0,
            ),
        );
        assert!(backend
            .slot_migration_runtime
            .queue_export_mutation(&export_id, 0, vec![b'd'; 19], true)
            .is_empty());
        let deferred_bytes = crate::engine::distributed::replication::protocol::encode_command(&[
            b"SELECT".as_slice(),
            b"0",
        ])
        .len()
            + 19;
        let mut ordinary = Connection::new_fake();
        ordinary.queue_reply(vec![b'o'; 17]);
        backend.connection_registry.insert(ordinary);

        backend.track_slot_export_connection_output(export_slab);
        assert_eq!(
            backend.slot_export_output_memory(),
            backend.connection_registry[crate::framework::capabilities::io::Token(export_slab)]
                .output_buffer_allocation_bytes()
                .saturating_add(
                    backend
                        .slot_migration_runtime
                        .deferred_output_memory_usage(&export_id)
                ),
            "all slot-export allocations are excluded, but ordinary client output is not"
        );
        assert!(backend.slot_export_output_memory() >= 24 + deferred_bytes);
    }

    #[test]
    fn eviction_refreshes_slot_export_exclusion_after_each_propagated_unlink() {
        const MARKER: &str = "VALKEY_TEST_SLOT_EXPORT_EXCLUSION_GROWTH";
        if run_allocator_sensitive_test_in_isolation(
            "runtime::server::worker::flows::maintenance::tests::eviction_refreshes_slot_export_exclusion_after_each_propagated_unlink",
            MARKER,
        ) {
            return;
        }
        let (mut backend, _target, _) = runtime_with_streaming_export(0);
        let initial_used = backend.memory_used_for_eviction(0);
        backend.ctx.config_mut().server.maxmemory =
            initial_used.saturating_sub(EVICTION_EXPORT_KEY_BYTES / 2) as u64;

        backend.run_eviction();

        assert_eq!(backend.ctx.db(0).unwrap().dbsize(), 1);
        assert!(backend.slot_export_output_memory() >= EVICTION_EXPORT_KEY_BYTES);
        finish_allocator_sensitive_test(MARKER);
    }

    #[test]
    fn eviction_stops_subtracting_slot_export_bytes_after_export_closes() {
        const MARKER: &str = "VALKEY_TEST_SLOT_EXPORT_EXCLUSION_REMOVAL";
        if run_allocator_sensitive_test_in_isolation(
            "runtime::server::worker::flows::maintenance::tests::eviction_stops_subtracting_slot_export_bytes_after_export_closes",
            MARKER,
        ) {
            return;
        }
        let initial_export_bytes = 4 * EVICTION_EXPORT_KEY_BYTES;
        let (mut backend, _target, token_value) =
            runtime_with_streaming_export(initial_export_bytes);
        backend
            .ctx
            .config_mut()
            .replication
            .replica_output_buffer_hard_limit = initial_export_bytes;
        backend.ctx.config_mut().server.repl_backlog_size = 0;
        let initial_used = backend.memory_used_for_eviction(0);
        backend.ctx.config_mut().server.maxmemory = initial_used
            .saturating_sub(EVICTION_EXPORT_KEY_BYTES + EVICTION_EXPORT_KEY_BYTES / 2)
            as u64;

        backend.run_eviction();

        assert!(!backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token_value)));
        assert_eq!(backend.ctx.db(0).unwrap().dbsize(), 0);
        finish_allocator_sensitive_test(MARKER);
    }

    #[test]
    fn maxmemory_exclusion_retains_full_allocation_after_partial_drain() {
        const MARKER: &str = "VALKEY_TEST_SLOT_EXPORT_PARTIAL_ALLOCATION";
        if run_allocator_sensitive_test_in_isolation(
            "runtime::server::worker::flows::maintenance::tests::maxmemory_exclusion_retains_full_allocation_after_partial_drain",
            MARKER,
        ) {
            return;
        }
        use crate::engine::distributed::cluster::migration::{
            MigrationJobId, SlotMigrationConnectionRole,
        };
        use crate::framework::capabilities::connection::Connection;

        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().server.maxmemory_clients = "1%".to_string();
        let mut connection = Connection::new_fake();
        connection.slot_migration_role = Some(SlotMigrationConnectionRole::Export(
            MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
        ));
        connection.queue_replication_reply(vec![b'm'; EVICTION_EXPORT_KEY_BYTES]);
        let connection_token = backend.connection_registry.insert(connection);
        backend.track_slot_export_connection_output(connection_token);
        let before = backend.memory_used_for_eviction(0);
        let queued_before = backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .output
        .queue
        .front()
        .unwrap()
        .len();
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .output
            .queue
            .front_mut()
            .unwrap()
            .drain(..64 * 1024);
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .consume_written_bytes(64 * 1024, queued_before)
            .unwrap();

        assert_eq!(backend.memory_used_for_eviction(0), before);
        finish_allocator_sensitive_test(MARKER);
    }
}
