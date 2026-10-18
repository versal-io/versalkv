use super::{ClusterLinkDirection, ClusterState};
use crate::engine::distributed::cluster::node::{
    CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
};

impl ClusterState {
    pub(crate) fn promote_endpoint_handshake_replica(
        &mut self,
        ip: &str,
        port: u16,
        node_id: &str,
    ) -> bool {
        if node_id == self.myself.name || self.nodes.contains_key(node_id) {
            return false;
        }
        let mut matches = self.nodes.values().filter(|node| {
            node.has_flag(CLUSTER_NODE_HANDSHAKE) && node.ip() == ip && node.port() == port
        });
        let Some(temporary_id) = matches.next().map(|node| node.name.clone()) else {
            return false;
        };
        if matches.next().is_some() {
            return false;
        }
        let Some(mut replica) = self.nodes.remove(&temporary_id) else {
            return false;
        };

        replica.name = node_id.to_string();
        replica.flags &= !(CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_PRIMARY);
        replica.flags |= CLUSTER_NODE_REPLICA;
        replica.replicaof = self.myself.name.clone();
        replica.shard_id = self.myself.shard_id.clone();
        self.nodes.insert(node_id.to_string(), replica);
        self.observe_link_close(&temporary_id, ClusterLinkDirection::All);
        self.queue_link_close_once((temporary_id, ClusterLinkDirection::All));
        self.mark_topology_changed();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};

    const REPLICA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HANDSHAKE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn insert_handshake(state: &mut ClusterState, id: &str, ip: &str, port: u16) {
        let mut node = ClusterNode::new(id.to_string(), ip.to_string(), port, port + 10_000, true);
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.flags |= CLUSTER_NODE_HANDSHAKE;
        state.nodes.insert(id.to_string(), node);
    }

    #[test]
    fn endpoint_handshake_becomes_local_replica_identity() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        insert_handshake(&mut state, HANDSHAKE, "127.0.0.1", 6380);
        let generation = state.topology_generation();

        assert!(state.promote_endpoint_handshake_replica("127.0.0.1", 6380, REPLICA));

        assert!(!state.nodes.contains_key(HANDSHAKE));
        let replica = &state.nodes[REPLICA];
        assert!(replica.is_replica());
        assert_eq!(replica.replicaof, state.myself.name);
        assert_eq!(replica.shard_id, state.myself.shard_id);
        assert!(state
            .pending_link_closes
            .contains(&(HANDSHAKE.to_string(), ClusterLinkDirection::All)));
        assert!(state.topology_generation() > generation);
    }

    #[test]
    fn endpoint_handshake_promotion_rejects_missing_or_ambiguous_matches() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        assert!(!state.promote_endpoint_handshake_replica("127.0.0.1", 6380, REPLICA));

        insert_handshake(&mut state, HANDSHAKE, "127.0.0.1", 6380);
        insert_handshake(
            &mut state,
            "cccccccccccccccccccccccccccccccccccccccc",
            "127.0.0.1",
            6380,
        );
        assert!(!state.promote_endpoint_handshake_replica("127.0.0.1", 6380, REPLICA));
        assert!(state.nodes.contains_key(HANDSHAKE));
        assert!(!state.nodes.contains_key(REPLICA));
    }
}
