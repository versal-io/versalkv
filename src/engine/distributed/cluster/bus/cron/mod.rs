mod connections;
mod election;
#[cfg(test)]
mod election_tests;
mod failure_detection;

use crate::engine::distributed::cluster::action::ClusterActionBatch;
use crate::engine::distributed::cluster::event::ClusterEvent;
use crate::engine::distributed::cluster::reducer::reduce;
use crate::engine::distributed::cluster::state::failover::LocalReplicaFailoverStatus;
use crate::engine::distributed::cluster::state::ClusterState;
use crate::foundation::io::sys::rand::random_usize;

use super::super::types::ClusterBusManager;

fn effective_node_timeout_ms(configured_timeout: u64) -> u64 {
    // Align with Valkey cluster cron semantics: a configured timeout of 0 is
    // legal, but operational paths still use at least a 1s floor so FAIL/PFAIL
    // propagation and elections have time to converge.
    configured_timeout.max(1_000)
}

impl ClusterBusManager {
    /// Re-run only the failover part of cluster cron after a Cluster Bus
    /// packet updated election facts. C reaches the same state machine from
    /// `clusterBeforeSleep`, so a due rank delay is not held behind unrelated
    /// work in the next full cron pass.
    pub(crate) fn reevaluate_automatic_failover_after_receive(
        &mut self,
        state: &mut ClusterState,
        now: i64,
        node_timeout: u64,
        local_status: &LocalReplicaFailoverStatus,
    ) -> ClusterActionBatch {
        let primary_failed = state
            .nodes
            .get(&state.myself.replicaof)
            .is_some_and(|primary| {
                primary.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL)
            });
        let failover_in_progress = state.automatic_failover.phase()
            != crate::engine::distributed::cluster::state::failover::AutomaticFailoverPhase::Idle;
        if !state.myself.is_replica()
            || (!primary_failed
                && !failover_in_progress
                && !state.manual_failover.is_ready_for_election())
        {
            return ClusterActionBatch::default();
        }

        self.tick_automatic_failover(
            state,
            now,
            effective_node_timeout_ms(node_timeout),
            &std::collections::HashMap::new(),
            local_status,
        )
    }

    /// Periodic gossip cron. `node_timeout` is read from the registry's config
    /// (server.cluster_node_timeout); pass 0 to fall back to 15s default.
    /// Returns optional side-effect hints for the event loop caller:
    ///   - promoted = a replica promoted itself to primary this tick
    ///   - orphan_replicate_master = when I was demoted to replica of this master id
    ///
    /// Torn-down bus connections are removed from `self.connections` by
    /// centralized L5 teardown before their L1 deregister is retried, so cron
    /// never observes a pending owner and needs no per-token exclusion.
    #[cfg(test)]
    pub fn cron(
        &mut self,
        state: &mut ClusterState,
        now: i64,
        node_timeout: u64,
        repl_offsets: &std::collections::HashMap<String, i64>,
        _sendbuf_limit: u64,
    ) -> ClusterActionBatch {
        self.cron_with_replica_status(
            state,
            now,
            node_timeout,
            repl_offsets,
            &LocalReplicaFailoverStatus::default(),
        )
    }

    pub fn cron_with_replica_status(
        &mut self,
        state: &mut ClusterState,
        now: i64,
        node_timeout: u64,
        repl_offsets: &std::collections::HashMap<String, i64>,
        local_status: &LocalReplicaFailoverStatus,
    ) -> ClusterActionBatch {
        let node_timeout_ms = effective_node_timeout_ms(node_timeout);
        state.set_failure_report_node_timeout_ms(node_timeout_ms);
        let mut pending_actions = state.take_manual_failover_actions();
        pending_actions.append(&mut self.pending_failover_actions);
        if !state.manual_failover.is_takeover()
            && pending_actions.iter().any(|action| {
                matches!(
                    action,
                    crate::engine::distributed::cluster::action::ClusterAction::PromoteSelf(_)
                )
            })
        {
            let facts = self.automatic_failover_facts(
                state,
                now,
                node_timeout_ms,
                repl_offsets,
                local_status,
            );
            state.automatic_failover_on_tick(&facts);
            let promotion_is_valid = state.automatic_failover.phase()
                == crate::engine::distributed::cluster::state::failover::AutomaticFailoverPhase::Promoting;
            if !promotion_is_valid {
                pending_actions.retain(|action| {
                    !matches!(
                        action,
                        crate::engine::distributed::cluster::action::ClusterAction::PromoteSelf(_)
                    )
                });
            }
        }
        let mut hints = self.execute_failover_actions(state, pending_actions, now, None);
        hints
            .actions
            .extend(reduce(state, ClusterEvent::BroadcastTopology).actions);
        // Keep the configured value visible to the rest of the runtime, but
        // use the C-compatible operation floor for failure detection and
        // failover timing. `cluster-node-timeout 0` is a valid config in the
        // Tcl suite and must not collapse fail report validity to 2ms.
        let effective_timeout = node_timeout_ms;

        self.queue_manual_failover_ping(state, now);
        self.queue_random_ping(state, now);
        self.queue_targeted_pings(state, now);

        // ── Step 1: PFAIL marking from an unanswered active PING ─────────────
        // Collect all node names first so we can borrow state mutably between
        // per-node checks (the borrow checker otherwise forbids calling
        // state.* while iterating nodes.values_mut()).
        let (fail_flags_changed, reachability_changed) =
            self.detect_failures(state, now, effective_timeout);

        // ── Step 2: upgrade PFAIL→FAIL when quorum ──────────────────
        let fail_msgs: Vec<Vec<u8>> = fail_flags_changed
            .iter()
            .map(|name| {
                state.mark_fail(name);
                let failing = state.nodes.get(name).cloned();
                match failing {
                    Some(ref n) => self.build_fail(state, n),
                    None => Vec::new(),
                }
            })
            .collect();
        if reachability_changed {
            // C schedules clusterUpdateState whenever PFAIL changes. A node
            // can enter a minority partition before failure reports reach
            // FAIL quorum, so waiting for mark_fail leaves stale OK state.
            state.recompute_state();
        }

        // ── Step 3: Drive the explicit failover state machine ──────────────
        if state.myself.is_replica() && !state.myself.replicaof.is_empty() {
            let master_id = state.myself.replicaof.clone();
            let master_down = state.is_master_objectively_down(&master_id);
            // Manual failover is active when the handler set mf_end_time and
            // that timeout hasn't elapsed. In that mode we drive the election
            // even if the master is objectively alive.
            // Sticky: remember last observed master_down state to emit a log
            // only on transitions (cron runs ~10Hz, trace is useless inline).
            if let Some(prev) = state.observe_master_down(master_down) {
                let me = &state.myself.name[..state.myself.name.len().min(8)];
                let mshort = &master_id[..master_id.len().min(8)];
                let flags = state.nodes.get(&master_id).map(|n| n.flags).unwrap_or(0);
                crate::server_log!(
                    notice,
                    "[CLUSTER-FAILOVER] replica={} master_down TRANSITION {}→{} (master={} flags=0x{:04x} auth_time={} epoch={})",
                    me,
                    prev,
                    master_down,
                    mshort,
                    flags,
                    state.automatic_failover.deadline_ms(),
                    state.automatic_failover.election_epoch(),
                );
            }
        }
        let failover_hints =
            self.tick_automatic_failover(state, now, effective_timeout, repl_offsets, local_status);
        hints.actions.extend(failover_hints.actions);

        // ── Step 4: Broadcast newly reached FAIL decisions ──────────────────
        let targets: Vec<usize> = self
            .connections
            .iter()
            .filter_map(|(token, connection)| connection.is_outgoing.then_some(*token))
            .collect();
        for tv in &targets {
            if let Some(conn) = self.connections.get_mut(tv) {
                if conn.connected && !conn.node_name.is_empty() && conn.handshake_sent {
                    for fm in &fail_msgs {
                        if !fm.is_empty() {
                            conn.write_buf.extend_from_slice(fm);
                        }
                    }
                }
            }
        }

        // ── Step 5: Auto-connect to known nodes without active connections ─
        if !state.debug_disable_reconnection() {
            self.schedule_missing_outgoing_connections(state, now);
        }

        // Check for minority partition: if more than half of the primaries
        // are unreachable (PFAIL or FAIL), emit the expected log message.
        // This is done here rather than in recompute_state() to avoid
        // calling recompute_state() on every cron tick.
        let total_primaries = state.nodes.values().filter(|n| n.is_primary()).count() + 1;
        let alive_primaries = state
            .nodes
            .values()
            .filter(|n| {
                n.is_primary()
                    && !n.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_PFAIL)
                    && !n.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL)
            })
            .count()
            + 1;
        let minority = alive_primaries < total_primaries / 2 + 1;
        if state.observe_reachability_minority(minority) == Some(false) {
            crate::server_log!(notice, "... minority partition ...");
        }

        hints
    }

    fn queue_random_ping(&mut self, state: &mut ClusterState, now: i64) {
        const RANDOM_PING_PERIOD_MS: i64 = 1_000;
        if state.debug_disable_random_ping()
            || now.saturating_sub(self.last_random_ping_time) < RANDOM_PING_PERIOD_MS
        {
            return;
        }
        self.last_random_ping_time = now;
        let node_names: Vec<String> = state.nodes.keys().cloned().collect();
        let mut selected: Option<(String, i64)> = None;
        for _ in 0..5 {
            let Some(node_name) = node_names.get(random_usize(node_names.len())) else {
                break;
            };
            let Some(node) = state.nodes.get(node_name) else {
                continue;
            };
            let token = self.outgoing_ping_token(node_name);
            if node.ping_sent != 0
                || node.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE)
                || token.is_none()
            {
                continue;
            }
            if selected
                .as_ref()
                .is_none_or(|(_, oldest_pong)| node.pong_received < *oldest_pong)
            {
                selected = Some((node_name.clone(), node.pong_received));
            }
        }
        if let Some((node_name, _)) = selected {
            if let Some(token) = self.outgoing_ping_token(&node_name) {
                if self.queue_ping(state, token, now) {
                    self.trace_ping(state, &node_name, true);
                }
            }
        }
    }

    fn queue_manual_failover_ping(&mut self, state: &mut ClusterState, now: i64) {
        if !state.myself.is_primary() {
            return;
        }
        let Some(node_name) = state
            .manual_failover
            .pausing_replica()
            .map(ToString::to_string)
        else {
            return;
        };
        let Some(token) = self.outgoing_ping_token(&node_name) else {
            return;
        };
        let ping = self.build_ping(state);
        self.connections
            .get_mut(&token)
            .expect("manual failover PING target must remain connected")
            .write_buf
            .extend_from_slice(&ping);
        state.refresh_manual_failover_probe(&node_name, now);
        state.record_bus_message_sent(
            crate::engine::distributed::cluster::msg::ClusterMsgType::Ping,
        );
    }

    fn queue_targeted_pings(&mut self, state: &mut ClusterState, now: i64) {
        let targets: Vec<usize> = self
            .connections
            .iter()
            .filter_map(|(token, connection)| {
                let node = state.nodes.get(&connection.node_name)?;
                (connection.is_outgoing
                    && connection.connected
                    && connection.handshake_sent
                    && node.ping_sent == 0
                    && !node.has_flag(
                        crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE
                            | crate::engine::distributed::cluster::node::CLUSTER_NODE_NOADDR,
                    )
                    && now.saturating_sub(node.pong_received) > self.ping_interval)
                    .then_some(*token)
            })
            .collect();
        for token in targets {
            let node_name = self.connections[&token].node_name.clone();
            if self.queue_ping(state, token, now) {
                self.trace_ping(state, &node_name, false);
            }
        }
    }

    fn trace_ping(&mut self, state: &ClusterState, node_name: &str, periodic: bool) {
        let Some(display_name) = self.ping_trace_display(state, node_name, periodic) else {
            return;
        };
        crate::server_log!(
            debug,
            "Sending ping packet to node {} ({}) on outbound link",
            node_name,
            display_name
        );
    }

    fn ping_trace_display(
        &mut self,
        state: &ClusterState,
        node_name: &str,
        periodic: bool,
    ) -> Option<String> {
        let display_name = state
            .nodes
            .get(node_name)
            .expect("queued PING target must remain in ClusterState")
            .display_name();
        if !periodic
            && self
                .ping_trace_displays
                .get(node_name)
                .is_some_and(|previous| previous == &display_name)
        {
            return None;
        }
        self.ping_trace_displays
            .insert(node_name.to_string(), display_name.clone());
        Some(display_name)
    }

    fn outgoing_ping_token(&self, node_name: &str) -> Option<usize> {
        self.connections.iter().find_map(|(token, connection)| {
            (connection.is_outgoing
                && connection.connected
                && connection.handshake_sent
                && connection.node_name == node_name)
                .then_some(*token)
        })
    }

    /// Get bus connection tokens for writable check.
    pub fn writable_tokens(&self) -> Vec<usize> {
        self.connections
            .iter()
            .filter(|(_, c)| !c.write_buf.is_empty() || (c.is_outgoing && !c.handshake_sent))
            .map(|(k, _)| *k)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_ping_runs_at_most_once_per_second() {
        use crate::engine::command::context::Token;
        use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};

        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut node = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.pong_received = 1_000;
        state.nodes.insert(node_id.to_string(), node);
        bus.register_outgoing_for_node(
            Token(20),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id.to_string(),
        );
        let connection = bus.connections.get_mut(&20).unwrap();
        connection.handshake_sent = true;
        connection.connected = true;

        bus.queue_random_ping(&mut state, 1_000);
        assert_eq!(state.nodes[node_id].ping_sent, 1_000);
        assert!(!bus.connections[&20].write_buf.is_empty());

        state.nodes.get_mut(node_id).unwrap().ping_sent = 0;
        bus.connections.get_mut(&20).unwrap().write_buf.clear();
        bus.queue_random_ping(&mut state, 1_001);

        assert_eq!(state.nodes[node_id].ping_sent, 0);
        assert!(bus.connections[&20].write_buf.is_empty());
    }

    #[test]
    fn ping_trace_policy_reports_targeted_identity_once_and_random_identity_periodically() {
        use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};

        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let node_id = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
        let mut node = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(node_id.to_string(), node);
        let display_name = state.nodes[node_id].display_name();

        assert_eq!(
            bus.ping_trace_display(&state, node_id, false),
            Some(display_name.clone())
        );
        assert_eq!(
            bus.ping_trace_display(&state, node_id, false),
            None,
            "repeated targeted probes must not create a debug-log storm"
        );
        assert_eq!(
            bus.ping_trace_display(&state, node_id, true),
            Some(display_name),
            "the once-per-second random probe must retain the periodic debug trace"
        );
    }

    #[test]
    fn handshake_node_is_not_marked_failed_before_identity_confirmation() {
        use crate::engine::distributed::cluster::node::{
            ClusterNode, CLUSTER_NODE_FAIL, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_MYSELF,
            CLUSTER_NODE_PFAIL,
        };

        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut handshake = ClusterNode::new(
            "handshake".to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        handshake.flags &= !CLUSTER_NODE_MYSELF;
        handshake.flags |= CLUSTER_NODE_HANDSHAKE;
        handshake.pong_received = 1;
        state.nodes.insert(handshake.name.clone(), handshake);

        bus.cron(
            &mut state,
            2_000,
            1_000,
            &std::collections::HashMap::new(),
            0,
        );

        let handshake = state.nodes.get("handshake").unwrap();
        assert!(!handshake.has_flag(CLUSTER_NODE_PFAIL | CLUSTER_NODE_FAIL));
    }

    #[test]
    fn zero_timeout_uses_operation_floor_for_fail_quorum() {
        use crate::engine::distributed::cluster::node::{
            ClusterNode, CLUSTER_NODE_FAIL, CLUSTER_NODE_MYSELF,
        };

        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.add_slot(0).unwrap();

        let mut target = ClusterNode::new(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        target.flags &= !CLUSTER_NODE_MYSELF;
        target.ping_sent = 1;
        target.pong_received = 1;
        target.slots[1] = true;
        target.numslots = 1;

        let mut witness = ClusterNode::new(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            "127.0.0.1".to_string(),
            6381,
            16381,
            true,
        );
        witness.flags &= !CLUSTER_NODE_MYSELF;
        witness.pong_received = 1_001;
        witness.slots[2] = true;
        witness.numslots = 1;

        let target_id = target.name.clone();
        let witness_id = witness.name.clone();
        state.nodes.insert(target_id.clone(), target);
        state.nodes.insert(witness_id.clone(), witness);
        state.add_fail_report(&target_id, &witness_id, 501, 15_000);

        let generation = state.topology_generation();
        bus.cron(&mut state, 1_002, 0, &std::collections::HashMap::new(), 0);

        let target = state.nodes.get(&target_id).unwrap();
        assert!(target.has_flag(CLUSTER_NODE_FAIL));
        assert!(state.topology_generation() > generation);
    }

    #[test]
    fn pfail_minority_recomputes_cluster_state_without_fail_quorum() {
        use crate::engine::distributed::cluster::node::{
            ClusterNode, CLUSTER_NODE_FAIL, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PFAIL,
        };
        use crate::engine::distributed::cluster::state::CLUSTER_OK;

        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        for slot in 0..5_462 {
            state.myself.slots[slot] = true;
        }
        state.myself.numslots = 5_462;

        for (node_id, start, end) in [
            ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5_462, 10_923),
            ("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 10_923, 16_384),
        ] {
            let mut node = ClusterNode::new(
                node_id.to_string(),
                "127.0.0.1".to_string(),
                6380,
                16_380,
                true,
            );
            node.flags &= !CLUSTER_NODE_MYSELF;
            node.ping_sent = 1;
            node.pong_received = 1;
            for slot in start..end {
                node.slots[slot] = true;
            }
            node.numslots = end - start;
            state.nodes.insert(node_id.to_string(), node);
        }
        state.recompute_state();
        assert_eq!(state.state, CLUSTER_OK);

        bus.cron(
            &mut state,
            2_000,
            1_000,
            &std::collections::HashMap::new(),
            0,
        );

        assert_ne!(state.state, CLUSTER_OK);
        assert!(state
            .nodes
            .values()
            .all(|node| node.has_flag(CLUSTER_NODE_PFAIL)));
        assert!(state
            .nodes
            .values()
            .all(|node| !node.has_flag(CLUSTER_NODE_FAIL)));
    }

    #[test]
    fn cron_marks_active_probes_for_ping_and_missing_outbound_links() {
        use crate::engine::command::context::Token;
        use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};

        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

        let mut connected = ClusterNode::new(
            "connected-node".to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        connected.flags &= !CLUSTER_NODE_MYSELF;
        connected.pong_received = 1;

        let mut disconnected = ClusterNode::new(
            "disconnected-node".to_string(),
            "127.0.0.1".to_string(),
            6381,
            16381,
            true,
        );
        disconnected.flags &= !CLUSTER_NODE_MYSELF;
        disconnected.pong_received = 1;

        state.nodes.insert(connected.name.clone(), connected);
        state.nodes.insert(disconnected.name.clone(), disconnected);
        state.set_debug_disable_random_ping(true);

        bus.register_outgoing_for_node(
            Token(21),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            "connected-node".to_string(),
        );
        if let Some(conn) = bus.connections.get_mut(&21) {
            conn.handshake_sent = true;
            conn.connected = true;
        }

        bus.cron(
            &mut state,
            1_002,
            1_000,
            &std::collections::HashMap::new(),
            0,
        );

        assert_eq!(state.nodes.get("connected-node").unwrap().ping_sent, 1_002);
        assert_eq!(
            state.nodes.get("disconnected-node").unwrap().ping_sent,
            1_002
        );
        assert!(!bus.connections[&21].write_buf.is_empty());
    }

    #[test]
    fn cron_does_not_resend_or_retime_an_active_ping() {
        use crate::engine::command::context::Token;
        use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};

        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut node = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.ping_sent = 500;
        node.pong_received = 1;
        state.nodes.insert(node_id.to_string(), node);
        bus.register_outgoing_for_node(
            Token(30),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id.to_string(),
        );
        let connection = bus.connections.get_mut(&30).unwrap();
        connection.handshake_sent = true;
        connection.connected = true;

        bus.cron(
            &mut state,
            1_001,
            2_000,
            &std::collections::HashMap::new(),
            0,
        );

        assert_eq!(state.nodes[node_id].ping_sent, 500);
        assert!(bus.connections[&30].write_buf.is_empty());
    }

    #[test]
    fn cron_queues_regular_ping_only_on_outbound_link() {
        use crate::engine::command::context::Token;
        use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};

        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut node = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.pong_received = 1_000;
        state.nodes.insert(node_id.to_string(), node);
        bus.register_outgoing_for_node(
            Token(31),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id.to_string(),
        );
        bus.register_incoming(Token(32), "127.0.0.1:26380".parse().unwrap());
        for token in [31, 32] {
            let connection = bus.connections.get_mut(&token).unwrap();
            connection.node_name = node_id.to_string();
            connection.handshake_sent = true;
        }

        bus.cron(
            &mut state,
            1_001,
            1_000,
            &std::collections::HashMap::new(),
            0,
        );

        assert!(!bus.connections[&31].write_buf.is_empty());
        assert!(bus.connections[&32].write_buf.is_empty());
    }

    #[test]
    fn debug_disable_random_ping_still_allows_targeted_ping() {
        use crate::engine::command::context::Token;
        use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};

        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut node = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.pong_received = 1;
        state.nodes.insert(node_id.to_string(), node);
        state.set_debug_disable_random_ping(true);
        bus.register_outgoing_for_node(
            Token(22),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id.to_string(),
        );
        let connection = bus.connections.get_mut(&22).unwrap();
        connection.handshake_sent = true;
        connection.connected = true;

        bus.cron(
            &mut state,
            1_002,
            1_000,
            &std::collections::HashMap::new(),
            0,
        );

        assert_eq!(state.nodes[node_id].ping_sent, 1_002);
        assert!(!bus.connections[&22].write_buf.is_empty());
    }

    #[test]
    fn failure_detection_runs_between_ping_probe_intervals() {
        use crate::engine::distributed::cluster::node::{
            ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PFAIL,
        };

        let mut bus = ClusterBusManager::new();
        bus.last_random_ping_time = 2_000;
        bus.ping_interval = 10_000;
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut node = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.ping_sent = 500;
        node.pong_received = 1;
        state.nodes.insert(node_id.to_string(), node);

        bus.cron(
            &mut state,
            2_000,
            1_000,
            &std::collections::HashMap::new(),
            0,
        );

        assert!(state.nodes[node_id].has_flag(CLUSTER_NODE_PFAIL));
    }

    #[test]
    fn pausing_primary_continuously_pings_the_failover_replica() {
        use crate::engine::command::context::Token;
        use crate::engine::distributed::cluster::msg::{ClusterBusMessage, ClusterMsgType};
        use crate::engine::distributed::cluster::node::{
            ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };
        use crate::engine::distributed::cluster::NodeId;

        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let replica_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut replica = ClusterNode::new(
            replica_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            false,
        );
        replica.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
        replica.flags |= CLUSTER_NODE_REPLICA;
        replica.replicaof = state.myself.name.clone();
        replica.ping_sent = 500;
        replica.pong_received = 1_000;
        state.nodes.insert(replica_id.to_string(), replica);
        state.manual_failover.accept_on_primary(
            NodeId::parse(replica_id.as_bytes()).unwrap(),
            1_000,
            5_000,
        );
        state.manual_failover.on_primary_pause_committed();
        state.set_debug_disable_random_ping(true);
        bus.register_outgoing_for_node(
            Token(40),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            replica_id.to_string(),
        );
        let connection = bus.connections.get_mut(&40).unwrap();
        connection.handshake_sent = true;
        connection.connected = true;

        bus.cron(
            &mut state,
            1_100,
            1_000,
            &std::collections::HashMap::new(),
            0,
        );

        assert_eq!(state.nodes[replica_id].ping_sent, 1_100);
        let (message, _) = ClusterBusMessage::decode(&bus.connections[&40].write_buf).unwrap();
        assert_eq!(message.msg_type, ClusterMsgType::Ping);
        assert!(message.has_message_flag(
            crate::engine::distributed::cluster::msg::ClusterMessageFlags::PAUSED
        ));
    }

    #[test]
    fn inbound_link_does_not_suppress_outbound_connection() {
        use crate::engine::command::context::Token;
        use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};

        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut node = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(node_id.to_string(), node);
        bus.register_incoming(Token(7), "127.0.0.1:26380".parse().unwrap());
        bus.connections.get_mut(&7).unwrap().node_name = node_id.to_string();

        bus.cron(
            &mut state,
            1_000,
            15_000,
            &std::collections::HashMap::new(),
            0,
        );

        assert!(state
            .pending_connects
            .contains(&("127.0.0.1".to_string(), 16380, false)));
        assert_eq!(state.nodes[node_id].ping_sent, 1_000);
    }
}
