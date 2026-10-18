use super::{ClusterRouteDecision, RouteRequest};
use crate::engine::command::{CMD_ADMIN, CMD_ASKING, CMD_PUBSUB, CMD_READONLY, CMD_WRITE};
use crate::engine::distributed::cluster::node::{
    key_hash_slot, ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
};
use crate::engine::distributed::cluster::state::{ClusterState, CLUSTER_OK};
use crate::engine::distributed::cluster::PreferredEndpoint;

const REMOTE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const OTHER: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn state_with_remote_owner(key: &[u8]) -> ClusterState {
    let slot = key_hash_slot(key) as usize;
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let mut remote = ClusterNode::new(
        REMOTE.to_string(),
        "192.0.2.1".to_string(),
        6380,
        16380,
        true,
    );
    remote.flags &= !CLUSTER_NODE_MYSELF;
    remote.slots[slot] = true;
    remote.numslots = 1;
    state.nodes.insert(remote.name.clone(), remote);
    state.state = CLUSTER_OK;
    state
}

fn request<'a>(keys: &'a [&'a [u8]]) -> RouteRequest<'a> {
    RouteRequest {
        keys,
        ..RouteRequest::default()
    }
}

#[test]
fn stable_owner_and_keyless_routes_are_pure_data_decisions() {
    let key = b"{same}a";
    let mut state = state_with_remote_owner(key);
    let slot = key_hash_slot(key);

    assert_eq!(
        state.decide_route(request(&[key])),
        ClusterRouteDecision::Moved {
            slot,
            endpoint: state.nodes[REMOTE]
                .endpoints
                .redirect_endpoint(PreferredEndpoint::Ip),
        }
    );
    state
        .set_slot_node(slot as usize, &state.myself.name.clone())
        .unwrap();
    assert_eq!(
        state.decide_route(request(&[key])),
        ClusterRouteDecision::ExecuteLocal
    );
    assert_eq!(
        state.decide_route(request(&[])),
        ClusterRouteDecision::ExecuteLocal
    );
}

#[test]
fn keyless_replica_routes_follow_redirect_capability_and_readonly_mode() {
    let key = b"replica-keyless";
    let mut state = state_with_remote_owner(key);
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = REMOTE.to_string();

    assert_eq!(
        state.decide_route(RouteRequest {
            command_flags: CMD_READONLY,
            redirect_keyless: true,
            ..request(&[])
        }),
        ClusterRouteDecision::Redirect {
            endpoint: state.nodes[REMOTE]
                .endpoints
                .redirect_endpoint(PreferredEndpoint::Ip),
        }
    );
    assert_eq!(
        state.decide_route(RouteRequest {
            command_flags: CMD_READONLY,
            redirect_keyless: true,
            client_readonly: true,
            ..request(&[])
        }),
        ClusterRouteDecision::ExecuteLocal
    );
    assert_eq!(
        state.decide_route(RouteRequest {
            command_flags: CMD_WRITE,
            ..request(&[])
        }),
        ClusterRouteDecision::ReadOnly
    );
}

#[test]
fn route_table_covers_crossslot_unserved_and_admin_bypass() {
    let key = b"{same}a";
    let state = state_with_remote_owner(key);
    assert_eq!(
        state.decide_route(request(&[b"{one}a", b"{two}b"])),
        ClusterRouteDecision::CrossSlot
    );
    let empty = ClusterState::new("127.0.0.1", 6379, 16379);
    assert_eq!(
        empty.decide_route(RouteRequest {
            require_cluster_ok: true,
            ..request(&[key])
        }),
        ClusterRouteDecision::ClusterDown
    );
    for flags in [CMD_ADMIN, CMD_PUBSUB] {
        assert_eq!(
            state.decide_route(RouteRequest {
                command_flags: flags,
                ..request(&[key])
            }),
            ClusterRouteDecision::ExecuteLocal
        );
    }
}

#[test]
fn open_slot_routes_ask_tryagain_and_importing_asking() {
    let keys = [b"{same}a".as_slice(), b"{same}b".as_slice()];
    let slot = key_hash_slot(keys[0]);
    let mut state = state_with_remote_owner(keys[0]);
    state
        .set_slot_node(slot as usize, &state.myself.name.clone())
        .unwrap();
    state.set_slot_migrating(slot as usize, REMOTE).unwrap();

    assert!(matches!(
        state.decide_route(RouteRequest {
            existing_keys: Some(&[false, false]),
            ..request(&keys)
        }),
        ClusterRouteDecision::Ask { slot: actual, .. } if actual == slot
    ));
    assert_eq!(
        state.decide_route(RouteRequest {
            existing_keys: Some(&[true, false]),
            ..request(&keys)
        }),
        ClusterRouteDecision::TryAgain
    );

    state.set_slot_stable(slot as usize).unwrap();
    state.set_slot_node(slot as usize, REMOTE).unwrap();
    state.set_slot_importing(slot as usize, REMOTE).unwrap();
    assert_eq!(
        state.decide_route(RouteRequest {
            command_flags: CMD_ASKING,
            existing_keys: Some(&[true]),
            ..request(&keys[..1])
        }),
        ClusterRouteDecision::ExecuteLocal
    );
}

#[test]
fn open_slot_command_policy_allows_migrate_and_rejects_move() {
    let key = b"{same}missing";
    let slot = key_hash_slot(key);
    let mut state = state_with_remote_owner(key);
    state
        .set_slot_node(slot as usize, &state.myself.name.clone())
        .unwrap();
    state.seed_migrating_slot(slot as usize, REMOTE);

    assert_eq!(
        state.decide_route(RouteRequest {
            existing_keys: Some(&[false]),
            open_slot_policy: super::OpenSlotPolicy::ExecuteLocal,
            ..request(&[key])
        }),
        ClusterRouteDecision::ExecuteLocal
    );
    assert_eq!(
        state.decide_route(RouteRequest {
            existing_keys: Some(&[true]),
            open_slot_policy: super::OpenSlotPolicy::RejectUnstable,
            ..request(&[key])
        }),
        ClusterRouteDecision::TryAgain
    );
}

#[test]
fn replica_readonly_and_write_routes_follow_owner_snapshot() {
    let key = b"replica-route";
    let mut state = state_with_remote_owner(key);
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = REMOTE.to_string();

    assert_eq!(
        state.decide_route(RouteRequest {
            command_flags: CMD_READONLY,
            allow_replica_read: true,
            ..request(&[key])
        }),
        ClusterRouteDecision::ExecuteLocal
    );
    assert!(matches!(
        state.decide_route(RouteRequest {
            command_flags: CMD_WRITE,
            allow_replica_read: true,
            ..request(&[key])
        }),
        ClusterRouteDecision::Moved { .. }
    ));
}

#[test]
fn readonly_replica_applies_its_primary_migrating_slot_routes() {
    let keys = [b"{same}a".as_slice(), b"{same}b".as_slice()];
    let slot = key_hash_slot(keys[0]);
    let mut state = state_with_remote_owner(keys[0]);
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = REMOTE.to_string();
    let mut target = ClusterNode::new(
        OTHER.to_string(),
        "192.0.2.2".to_string(),
        6381,
        16381,
        true,
    );
    target.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(target.name.clone(), target);
    state.seed_migrating_slot(slot as usize, OTHER);

    assert!(matches!(
        state.decide_route(RouteRequest {
            existing_keys: Some(&[false]),
            allow_replica_read: true,
            client_readonly: true,
            ..request(&keys[..1])
        }),
        ClusterRouteDecision::Ask { slot: actual, .. } if actual == slot
    ));
    assert_eq!(
        state.decide_route(RouteRequest {
            existing_keys: Some(&[true, false]),
            allow_replica_read: true,
            client_readonly: true,
            ..request(&keys)
        }),
        ClusterRouteDecision::TryAgain
    );
}

#[test]
fn changed_owner_is_visible_to_the_next_route_decision() {
    let key = b"route-generation";
    let slot = key_hash_slot(key) as usize;
    let mut state = state_with_remote_owner(key);
    let mut replacement = ClusterNode::new(
        OTHER.to_string(),
        "192.0.2.2".to_string(),
        6381,
        16381,
        true,
    );
    replacement.flags &= !CLUSTER_NODE_MYSELF;
    replacement.config_epoch = 2;
    replacement.slots[slot] = true;
    replacement.numslots = 1;
    state.nodes.insert(replacement.name.clone(), replacement);
    state.mark_topology_changed();

    assert!(matches!(
        state.decide_route(request(&[key])),
        ClusterRouteDecision::Moved { ref endpoint, .. } if endpoint.port == 6381
    ));
}

#[test]
fn unserved_slot_is_clusterdown_and_unknown_endpoint_is_preserved() {
    let key = b"failed-owner";
    let slot = key_hash_slot(key) as usize;
    let empty = ClusterState::new("127.0.0.1", 6379, 16379);
    assert_eq!(
        empty.decide_route(request(&[key])),
        ClusterRouteDecision::ClusterDown
    );

    let state = state_with_remote_owner(key);
    assert!(matches!(
        state.decide_route(RouteRequest {
            preferred_endpoint: PreferredEndpoint::Unknown,
            ..request(&[key])
        }),
        ClusterRouteDecision::Moved { slot: actual, ref endpoint }
            if actual as usize == slot && endpoint.host.is_empty()
    ));
}

#[test]
fn withdrawn_remote_owner_remains_the_live_redirect_target() {
    let key = b"withdrawn-remote";
    let slot = key_hash_slot(key) as usize;
    let mut state = state_with_remote_owner(key);
    state.slot_ownership.withdrawn_by[slot] = Some(REMOTE.to_string());

    assert!(matches!(
        state.decide_route(request(&[key])),
        ClusterRouteDecision::Moved { slot: actual, ref endpoint }
            if actual as usize == slot && endpoint.port == 6380
    ));
}

#[test]
fn withdrawn_local_owner_keeps_serving_until_rebound() {
    let key = b"withdrawn-local";
    let slot = key_hash_slot(key) as usize;
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.add_slot(slot).unwrap();
    state.slot_ownership.withdrawn_by[slot] = Some(state.myself.name.clone());

    assert_eq!(
        state.decide_route(request(&[key])),
        ClusterRouteDecision::ExecuteLocal
    );
}

#[test]
fn route_error_messages_match_resp_contract() {
    let endpoint = crate::engine::distributed::cluster::AdvertisedEndpoint {
        host: "node.example.com".to_string(),
        port: 6379,
    };
    let cases = [
        (
            ClusterRouteDecision::Moved {
                slot: 42,
                endpoint: endpoint.clone(),
            },
            "MOVED 42 node.example.com:6379",
        ),
        (
            ClusterRouteDecision::Ask { slot: 7, endpoint },
            "ASK 7 node.example.com:6379",
        ),
        (
            ClusterRouteDecision::Redirect {
                endpoint: crate::engine::distributed::cluster::AdvertisedEndpoint {
                    host: "node.example.com".to_string(),
                    port: 6380,
                },
            },
            "REDIRECT node.example.com:6380",
        ),
        (
            ClusterRouteDecision::TryAgain,
            "TRYAGAIN Multiple keys request during rehashing of slot",
        ),
        (
            ClusterRouteDecision::CrossSlot,
            "CROSSSLOT Keys in request don't hash to the same slot",
        ),
        (
            ClusterRouteDecision::ClusterDown,
            "CLUSTERDOWN The cluster is down",
        ),
        (
            ClusterRouteDecision::ReadOnly,
            "READONLY You can't write against a read only replica.",
        ),
    ];

    for (decision, expected) in cases {
        assert_eq!(decision.error_message().as_deref(), Some(expected));
    }
    assert_eq!(ClusterRouteDecision::ExecuteLocal.error_message(), None);
}
