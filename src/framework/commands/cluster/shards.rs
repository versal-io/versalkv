use crate::engine::distributed::cluster::{
    ClientTransport, ClusterTopologySnapshot, PreferredEndpoint,
};
use crate::foundation::io::protocol::Value;

#[cfg(test)]
mod snapshot_reply_tests {
    use super::*;
    use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};
    use crate::engine::distributed::cluster::ClusterState;

    fn field<'a>(fields: &'a [(Value, Value)], key: &[u8]) -> &'a Value {
        fields
            .iter()
            .find_map(|(candidate, value)| {
                matches!(candidate, Value::Bulk(name) if name.as_ref() == key).then_some(value)
            })
            .unwrap()
    }

    #[test]
    fn shard_slots_follow_canonical_snapshot_owner_instead_of_stale_bitmap() {
        const STALE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        const OWNER: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut stale = ClusterNode::new(
            STALE.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        stale.flags &= !CLUSTER_NODE_MYSELF;
        stale.config_epoch = 1;
        stale.slots[7] = true;
        stale.numslots = 1;
        let mut owner = ClusterNode::new(
            OWNER.to_string(),
            "127.0.0.1".to_string(),
            6381,
            16381,
            true,
        );
        owner.flags &= !CLUSTER_NODE_MYSELF;
        owner.config_epoch = 2;
        owner.slots[7] = true;
        owner.numslots = 1;
        state.nodes.insert(stale.name.clone(), stale);
        state.nodes.insert(owner.name.clone(), owner);

        let shards = super::super::topology_reply::render_shards(
            &state.topology_snapshot(),
            PreferredEndpoint::Ip,
            ClientTransport::Tcp,
            false,
        );
        let owner_shard = shards
            .iter()
            .find_map(|value| match value {
                Value::Map(fields) if field(fields, b"id") == &Value::bulk(OWNER) => Some(fields),
                _ => None,
            })
            .unwrap();
        let stale_shard = shards
            .iter()
            .find_map(|value| match value {
                Value::Map(fields) if field(fields, b"id") == &Value::bulk(STALE) => Some(fields),
                _ => None,
            })
            .unwrap();

        assert_eq!(
            field(owner_shard, b"slots"),
            &Value::Array(vec![Value::integer(7), Value::integer(7)])
        );
        assert_eq!(field(stale_shard, b"slots"), &Value::Array(Vec::new()));
    }
}

pub(super) fn cluster_shards(
    snapshot: &ClusterTopologySnapshot,
    preference: PreferredEndpoint,
    transport: ClientTransport,
    prefer_ipv6: bool,
) -> Vec<Value> {
    super::topology_reply::render_shards(snapshot, preference, transport, prefer_ipv6)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::distributed::cluster::node::{
        ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PFAIL, CLUSTER_NODE_PRIMARY,
        CLUSTER_NODE_REPLICA,
    };
    use crate::engine::distributed::cluster::{
        ClusterNodeHealth, ClusterNodeRole, ClusterState, ClusterTopologySnapshot, NodeId, NodeView,
    };

    fn render_state(state: &ClusterState) -> Vec<Value> {
        cluster_shards(
            &state.topology_snapshot(),
            PreferredEndpoint::Ip,
            ClientTransport::Tcp,
            false,
        )
    }

    fn render_node(node: ClusterNode, role: ClusterNodeRole) -> Value {
        let state = ClusterState::new("127.0.0.1", 6399, 16399);
        let health = if node.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL)
        {
            ClusterNodeHealth::Fail
        } else if role == ClusterNodeRole::Replica && node.repl_offset == 0 {
            ClusterNodeHealth::Loading
        } else if node.has_flag(CLUSTER_NODE_PFAIL) {
            ClusterNodeHealth::PFail
        } else {
            ClusterNodeHealth::Online
        };
        let view = NodeView {
            id: NodeId::parse(if role == ClusterNodeRole::Primary {
                b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            } else {
                b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            })
            .unwrap(),
            shard_id: NodeId::parse(b"cccccccccccccccccccccccccccccccccccccccc").unwrap(),
            endpoints: node.endpoints,
            role,
            health,
            flags: node.flags,
            replica_of: None,
            config_epoch: node.config_epoch,
            replication_offset: node.repl_offset,
            ping_sent: node.ping_sent,
            pong_received: node.pong_received,
            outbound_connected: false,
            slots: node.slots,
        };
        let snapshot = ClusterTopologySnapshot {
            generation: state.topology_generation(),
            health: crate::engine::distributed::cluster::ClusterHealth::Fail,
            slot_owners: vec![None; crate::engine::distributed::cluster::state::CLUSTER_SLOTS],
            nodes: vec![view.clone()],
            shards: vec![crate::engine::distributed::cluster::ShardView {
                id: view.shard_id.clone(),
                slot_ranges: Vec::new(),
                nodes: vec![view],
            }],
            migrating_slots_to: Vec::new(),
            importing_slots_from: Vec::new(),
        };
        let mut shards = cluster_shards(
            &snapshot,
            PreferredEndpoint::Ip,
            ClientTransport::Tcp,
            false,
        );
        let Value::Map(shard) = shards.remove(0) else {
            panic!("expected shard map")
        };
        let Value::Array(nodes) = field(&shard, b"nodes") else {
            panic!("expected nodes array")
        };
        nodes[0].clone()
    }

    #[test]
    fn groups_primary_and_replica_with_online_health() {
        const REPLICA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.add_slot(0).unwrap();
        let primary_id = state.myself.name.clone();
        let mut replica = ClusterNode::new(
            REPLICA.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            false,
        );
        replica.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
        replica.flags |= CLUSTER_NODE_REPLICA;
        replica.replicaof = primary_id;
        state.nodes.insert(replica.name.clone(), replica);

        let shards = render_state(&state);

        assert_eq!(shards.len(), 1);
        let Value::Map(shard) = &shards[0] else {
            panic!("expected shard map")
        };
        let nodes = shard
            .iter()
            .find_map(|(key, value)| {
                matches!(key, Value::Bulk(name) if name.as_ref() == b"nodes").then_some(value)
            })
            .unwrap();
        assert!(matches!(nodes, Value::Array(entries) if entries.len() == 2));
    }

    #[test]
    fn empty_primary_is_reported_as_a_shard() {
        let state = ClusterState::new("127.0.0.1", 6379, 16379);

        let shards = render_state(&state);

        assert_eq!(shards.len(), 1);
        let Value::Map(fields) = &shards[0] else {
            panic!("expected shard map")
        };
        assert_eq!(field(fields, b"slots"), &Value::Array(Vec::new()));
        assert_eq!(
            field(fields, b"id"),
            &Value::bulk(state.myself.shard_id.clone())
        );
    }

    #[test]
    fn pfail_node_remains_online_until_fail_is_confirmed() {
        let mut node = ClusterNode::new(
            "node".to_string(),
            "127.0.0.1".to_string(),
            6379,
            16379,
            true,
        );
        node.flags |= CLUSTER_NODE_PFAIL;

        let Value::Map(fields) = render_node(node, ClusterNodeRole::Primary) else {
            panic!("expected node map")
        };
        let health = fields
            .iter()
            .find_map(|(key, value)| {
                matches!(key, Value::Bulk(name) if name.as_ref() == b"health").then_some(value)
            })
            .unwrap();

        assert!(matches!(health, Value::Bulk(value) if value.as_ref() == b"online"));
    }

    fn field<'a>(fields: &'a [(Value, Value)], key: &[u8]) -> &'a Value {
        fields
            .iter()
            .find_map(|(k, value)| {
                matches!(k, Value::Bulk(name) if name.as_ref() == key).then_some(value)
            })
            .unwrap()
    }

    #[test]
    fn replica_with_zero_offset_reports_loading_health() {
        // Align with C cluster_legacy.c:7337-7343: a replica whose replication
        // offset is still 0 (full sync not yet applied) reports `loading`, not
        // `online`. wait_for_cluster_propagation relies on this to avoid racing
        // the replica's loading window.
        let mut replica = ClusterNode::new(
            "replica".to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            false,
        );
        replica.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
        replica.flags |= CLUSTER_NODE_REPLICA;
        replica.repl_offset = 0;

        let Value::Map(fields) = render_node(replica, ClusterNodeRole::Replica) else {
            panic!("expected node map")
        };
        assert!(matches!(field(&fields, b"health"), Value::Bulk(v) if v.as_ref() == b"loading"));
        assert!(matches!(
            field(&fields, b"replication-offset"),
            Value::Integer(0)
        ));
    }

    #[test]
    fn replica_with_nonzero_offset_reports_online_health() {
        let mut replica = ClusterNode::new(
            "replica".to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            false,
        );
        replica.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
        replica.flags |= CLUSTER_NODE_REPLICA;
        replica.repl_offset = 4242;

        let Value::Map(fields) = render_node(replica, ClusterNodeRole::Replica) else {
            panic!("expected node map")
        };
        assert!(matches!(field(&fields, b"health"), Value::Bulk(v) if v.as_ref() == b"online"));
        assert!(matches!(
            field(&fields, b"replication-offset"),
            Value::Integer(4242)
        ));
    }

    #[test]
    fn primary_with_zero_offset_reports_online_health() {
        // Only replicas with a zero offset are `loading`; a primary at offset 0
        // (e.g. fresh cluster before writes) is `online`.
        let primary = ClusterNode::new(
            "primary".to_string(),
            "127.0.0.1".to_string(),
            6379,
            16379,
            true,
        );

        let Value::Map(fields) = render_node(primary, ClusterNodeRole::Primary) else {
            panic!("expected node map")
        };
        assert!(matches!(field(&fields, b"health"), Value::Bulk(v) if v.as_ref() == b"online"));
    }

    #[test]
    fn node_map_includes_endpoint_hostname_and_availability_zone() {
        let mut node = ClusterNode::new(
            "node".to_string(),
            "127.0.0.1".to_string(),
            6379,
            16379,
            true,
        );
        node.endpoints.set_client_ipv4(b"192.0.2.10").unwrap();
        node.endpoints.set_client_port(7000);
        node.endpoints.set_tls_port(7443);
        node.endpoints.set_hostname(b"node.example.com").unwrap();
        node.endpoints.set_availability_zone(b"zone-a").unwrap();

        let Value::Map(fields) = render_node(node, ClusterNodeRole::Primary) else {
            panic!("expected node map")
        };

        assert_eq!(field(&fields, b"port"), &Value::integer(7000));
        assert_eq!(field(&fields, b"tls-port"), &Value::integer(7443));
        assert_eq!(field(&fields, b"ip"), &Value::bulk("192.0.2.10"));
        assert_eq!(field(&fields, b"endpoint"), &Value::bulk("192.0.2.10"));
        assert_eq!(
            field(&fields, b"hostname"),
            &Value::bulk("node.example.com")
        );
        assert_eq!(field(&fields, b"availability-zone"), &Value::bulk("zone-a"));
    }
}
