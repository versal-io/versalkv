use super::*;
use crate::engine::distributed::cluster::node::{
    ClusterNode, CLUSTER_NODE_FAIL, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PFAIL,
};
use crate::engine::distributed::cluster::ClusterState;
use std::collections::BTreeMap;
use std::sync::Arc;

fn assert_matches_owned_projection(state: &ClusterState) {
    let replication = BTreeMap::new();
    let owned = state.topology_snapshot_with_local_replication_offset(&replication, 9876);
    let cached = state.management_topology(&replication, 9876);
    for preference in [
        PreferredEndpoint::Ip,
        PreferredEndpoint::Hostname,
        PreferredEndpoint::Unknown,
    ] {
        for transport in [ClientTransport::Tcp, ClientTransport::Tls] {
            for ipv6 in [false, true] {
                assert_eq!(
                    render_management_slots(&cached, preference, transport, ipv6),
                    render_slots(&owned, preference, transport, ipv6)
                        .into_iter()
                        .map(Value::Array)
                        .collect::<Vec<_>>()
                );
                assert_eq!(
                    render_management_shards(&cached, preference, transport, ipv6),
                    render_shards(&owned, preference, transport, ipv6)
                );
            }
        }
    }
}

#[test]
fn management_structure_reuses_live_fields_and_rebuilds_without_generation_bump() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.add_slot(1).unwrap();
    state.add_slot(2).unwrap();
    let empty = BTreeMap::new();
    let initial = state.management_topology(&empty, 1);
    state.myself.repl_offset = 42;
    state.myself.pong_received = 100;
    state.myself.flags |= CLUSTER_NODE_PFAIL;
    state.myself.endpoints.set_hostname(b"live.example.com").unwrap();
    let live = state.management_topology(&empty, 42);
    assert!(Arc::ptr_eq(&initial.structure, &live.structure));
    assert_eq!(live.nodes[0].replication_offset, 42);
    assert_eq!(live.nodes[0].pong_received, 100);
    assert_matches_owned_projection(&state);

    let generation = state.topology_generation();
    let mut replica = ClusterNode::new(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        false,
    );
    replica.flags &= !CLUSTER_NODE_MYSELF;
    replica.replicaof = state.myself.name.clone();
    replica.shard_id = state.myself.shard_id.clone();
    state.nodes.insert(replica.name.clone(), replica);
    assert_eq!(generation, state.topology_generation());
    let inserted = state.management_topology(&empty, 42);
    assert!(!Arc::ptr_eq(&live.structure, &inserted.structure));
    assert_matches_owned_projection(&state);
    let replica = state.nodes.values_mut().next().unwrap();
    replica.repl_offset = 100;
    replica.pong_received = 200;
    let offset_only = state.management_topology(&empty, 42);
    assert!(Arc::ptr_eq(&inserted.structure, &offset_only.structure));
    assert_matches_owned_projection(&state);
    state.nodes.values_mut().next().unwrap().flags |= CLUSTER_NODE_FAIL;
    assert_matches_owned_projection(&state);
    state.del_slot(1).unwrap();
    assert_matches_owned_projection(&state);
    state.del_slot(2).unwrap();
    assert_matches_owned_projection(&state);
}

#[test]
fn management_fallback_and_detached_shards_match_owned_projection() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    assert_matches_owned_projection(&state);
    let mut primary = ClusterNode::new(
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
        "127.0.0.1".to_string(),
        6381,
        16381,
        true,
    );
    primary.flags &= !CLUSTER_NODE_MYSELF;
    primary.config_epoch = 10;
    state.nodes.insert(primary.name.clone(), primary);
    assert_matches_owned_projection(&state);
    state.nodes.values_mut().next().unwrap().flags |= CLUSTER_NODE_FAIL;
    assert_matches_owned_projection(&state);
}
