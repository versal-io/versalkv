use crate::engine::distributed::cluster::node::{CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_NOADDR};
use crate::engine::distributed::cluster::state::ClusterState;
use crate::engine::distributed::cluster::ClusterLinkDirection;

use super::super::super::types::ClusterBusManager;

impl ClusterBusManager {
    fn queue_stale_outgoing_link_close(
        &self,
        state: &mut ClusterState,
        node_name: &str,
        now: i64,
        timeout_ms: i64,
        data_received: i64,
    ) {
        let Some(node) = state.nodes.get(node_name) else {
            return;
        };
        if node.is_myself()
            || node.has_flag(CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR)
            || node.ping_sent == 0
            || now.saturating_sub(node.ping_sent) <= timeout_ms / 2
            || now.saturating_sub(data_received) <= timeout_ms / 2
        {
            return;
        }
        let has_stale_link = self.connections.values().any(|connection| {
            connection.is_outgoing
                && connection.connected
                && connection.node_name == node_name
                && now.saturating_sub(connection.created_at) > timeout_ms
        });
        let close = (node_name.to_string(), ClusterLinkDirection::To);
        if has_stale_link {
            state.queue_link_close_once(close);
        }
    }

    pub(super) fn detect_failures(
        &self,
        state: &mut ClusterState,
        now: i64,
        node_timeout_ms: u64,
    ) -> (Vec<String>, bool) {
        let timeout_ms = node_timeout_ms.min(i64::MAX as u64) as i64;
        let node_names: Vec<String> = state.nodes.keys().cloned().collect();
        let mut failed = Vec::new();
        let mut reachability_changed = false;
        for node_name in &node_names {
            let data_received = self
                .connections
                .values()
                .filter(|connection| connection.node_name == *node_name)
                .map(|connection| connection.last_recv)
                .max()
                .unwrap_or(0);
            self.queue_stale_outgoing_link_close(state, node_name, now, timeout_ms, data_received);
            let Some((newly_pfailed, pfail_and_not_fail, changed)) =
                state.observe_node_failure_timeout(node_name, now, data_received, timeout_ms)
            else {
                continue;
            };
            reachability_changed |= changed;
            if !pfail_and_not_fail {
                continue;
            }
            let (count, quorum) = state.count_fail_reports(node_name, now, node_timeout_ms);
            let short = &node_name[..node_name.len().min(8)];
            // Match C's one-shot PFAIL notice. Re-emitting the report set from
            // every 100ms cron pass creates a cross-process log storm while the
            // node remains PFAIL.
            if newly_pfailed {
                crate::server_log!(notice, "NODE {} possibly failing.", short,);
            }
            if count >= quorum {
                crate::server_log!(
                    notice,
                    "[CLUSTER-FAILOVER] PFAIL→FAIL UPGRADE target={} count={}≥quorum={}",
                    short,
                    count,
                    quorum,
                );
                failed.push(node_name.clone());
            }
        }
        (failed, reachability_changed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::command::context::Token;
    use crate::engine::distributed::cluster::node::{
        ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PFAIL,
    };

    fn state_with_peer(ping_sent: i64, pong_received: i64) -> (ClusterState, String) {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
        let mut node =
            ClusterNode::new(node_id.clone(), "127.0.0.1".to_string(), 6380, 16_380, true);
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.ping_sent = ping_sent;
        node.pong_received = pong_received;
        state.nodes.insert(node_id.clone(), node);
        (state, node_id)
    }

    #[test]
    fn stale_pong_without_an_active_ping_does_not_mark_pfail() {
        let bus = ClusterBusManager::new();
        let (mut state, node_id) = state_with_peer(0, 1);

        bus.detect_failures(&mut state, 2_000, 1_000);

        assert!(!state.nodes[&node_id].has_flag(CLUSTER_NODE_PFAIL));
    }

    #[test]
    fn recent_bus_data_delays_pfail_for_an_active_ping() {
        let mut bus = ClusterBusManager::new();
        let (mut state, node_id) = state_with_peer(500, 1);
        bus.register_incoming(Token(41), "127.0.0.1:26380".parse().unwrap());
        let connection = bus.connections.get_mut(&41).unwrap();
        connection.node_name = node_id.clone();
        connection.last_recv = 1_500;

        bus.detect_failures(&mut state, 2_000, 1_000);

        assert!(!state.nodes[&node_id].has_flag(CLUSTER_NODE_PFAIL));
    }

    #[test]
    fn stale_outgoing_link_is_closed_after_half_timeout_without_bus_traffic() {
        let mut bus = ClusterBusManager::new();
        let (mut state, node_id) = state_with_peer(1_000, 1);
        bus.register_outgoing_for_node(
            Token(42),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id.clone(),
        );
        let connection = bus.connections.get_mut(&42).unwrap();
        connection.created_at = 500;
        connection.last_recv = 1_000;

        bus.detect_failures(&mut state, 1_501, 1_000);

        assert_eq!(
            state.pending_link_closes,
            vec![(node_id, ClusterLinkDirection::To)]
        );
    }

    #[test]
    fn outgoing_link_is_kept_until_every_reconnect_threshold_is_met() {
        for (created_at, ping_sent, last_recv) in [
            (501, 1_000, 1_000),
            (500, 1_001, 1_000),
            (500, 1_000, 1_001),
            (500, 0, 1_000),
        ] {
            let mut bus = ClusterBusManager::new();
            let (mut state, node_id) = state_with_peer(ping_sent, 1);
            bus.register_outgoing_for_node(
                Token(43),
                "127.0.0.1:16380".parse().unwrap(),
                false,
                node_id,
            );
            let connection = bus.connections.get_mut(&43).unwrap();
            connection.created_at = created_at;
            connection.last_recv = last_recv;

            bus.detect_failures(&mut state, 1_501, 1_000);

            assert!(state.pending_link_closes.is_empty());
        }
    }
}
