use crate::engine::distributed::cluster::node::CLUSTER_NODE_NOADDR;
use crate::engine::distributed::cluster::state::ClusterState;

use super::super::super::types::ClusterBusManager;

impl ClusterBusManager {
    pub(super) fn schedule_missing_outgoing_connections(&self, state: &mut ClusterState, now: i64) {
        // Match C clusterNodeCronHandleReconnect: it dials every linkless node
        // except myself/NOADDR — including HANDSHAKE placeholders. An inbound
        // MEET creates the placeholder with no MEET flag, so that outbound link
        // carries a normal PING (is_meet=false); only a node that independently
        // owns the MEET state sends a MEET. Skipping handshake nodes here is what
        // previously forced the inline dial on inbound MEET, which opened a
        // reciprocal MEET too early and broke the multiple-meets sequencing.
        let nodes_to_connect: Vec<(String, String, u16, bool)> = state
            .nodes
            .values()
            .filter(|node| {
                !node.is_myself()
                    && !node.has_flag(CLUSTER_NODE_NOADDR)
                    && node.cport() != 0
                    && !self.connections.values().any(|connection| {
                        connection.is_outgoing
                            && (connection.node_name == node.name
                                || connection.handshake_node_name == node.name)
                    })
                    && !state
                        .pending_connects()
                        .iter()
                        .any(|(ip, port, _)| *port == node.cport() && *ip == node.ip())
            })
            .map(|node| {
                let is_meet = node.meet_pending;
                (
                    node.name.clone(),
                    node.ip().to_string(),
                    node.cport(),
                    is_meet,
                )
            })
            .collect();

        for (node_name, ip, cport, is_meet) in nodes_to_connect {
            state.queue_connect((ip, cport, is_meet));
            // C clusterNodeCronHandleReconnect starts the failure clock when a
            // connection attempt fails before it can send PING.
            state.start_node_probe_if_idle(&node_name, now);
        }
    }
}
