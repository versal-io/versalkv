use super::{ClusterNodeHealth, ClusterState, RedirectOptions, SlotRange, CLUSTER_SLOTS};
use crate::engine::command::CMD_READONLY;
use crate::engine::distributed::cluster::node::{
    key_hash_slot, ClusterNode, CLUSTER_NODE_FAIL, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_MYSELF,
    CLUSTER_NODE_NOADDR, CLUSTER_NODE_PFAIL, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
};
use crate::foundation::io::protocol::Value;

fn topology_node(id: &str, port: u16, epoch: u64) -> ClusterNode {
    let mut node = ClusterNode::new(
        id.to_string(),
        "127.0.0.1".to_string(),
        port,
        port + 10_000,
        true,
    );
    node.flags &= !CLUSTER_NODE_MYSELF;
    node.config_epoch = epoch;
    node
}

#[test]
fn topology_snapshot_selects_one_epoch_ranked_owner_and_preserves_node_health() {
    const LOWER: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const PFAIL: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const FAILED: &str = "cccccccccccccccccccccccccccccccccccccccc";
    const PROMOTED: &str = "dddddddddddddddddddddddddddddddddddddddd";
    const LOADING: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    const NOADDR: &str = "ffffffffffffffffffffffffffffffffffffffff";
    const HANDSHAKE: &str = "gggggggggggggggggggggggggggggggggggggggg";
    const SHARD: &str = "ssssssssssssssssssssssssssssssssssssssss";
    const TIED: &str = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";

    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

    let mut lower = topology_node(LOWER, 6380, 4);
    lower.slots[10] = true;
    lower.slots[11] = true;
    lower.slots[13] = true;
    lower.numslots = 3;

    let mut pfail = topology_node(PFAIL, 6381, 5);
    pfail.flags |= CLUSTER_NODE_PFAIL;
    pfail.slots[10] = true;
    pfail.slots[11] = true;
    pfail.numslots = 2;

    let mut tied = topology_node(TIED, 6387, 5);
    tied.slots[10] = true;
    tied.numslots = 1;

    let mut failed = topology_node(FAILED, 6382, 8);
    failed.flags |= CLUSTER_NODE_FAIL;
    failed.shard_id = SHARD.to_string();
    failed.slots[12] = true;
    failed.numslots = 1;

    let mut promoted = topology_node(PROMOTED, 6383, 9);
    promoted.shard_id = SHARD.to_string();
    promoted
        .endpoints
        .set_hostname(b"promoted.example.com")
        .unwrap();

    let mut loading = topology_node(LOADING, 6384, 0);
    loading.flags &= !CLUSTER_NODE_PRIMARY;
    loading.flags |= CLUSTER_NODE_REPLICA;
    loading.replicaof = PROMOTED.to_string();
    loading.shard_id = SHARD.to_string();
    loading.repl_offset = 0;

    let mut noaddr = topology_node(NOADDR, 6385, 1);
    noaddr.flags |= CLUSTER_NODE_NOADDR;
    noaddr.slots[14] = true;
    noaddr.numslots = 1;

    let mut handshake = topology_node(HANDSHAKE, 6386, 1);
    handshake.flags |= CLUSTER_NODE_HANDSHAKE;
    handshake.config_epoch = 4;
    handshake.slots[13] = true;
    handshake.numslots = 1;

    for node in [
        lower, pfail, tied, failed, promoted, loading, noaddr, handshake,
    ] {
        state.nodes.insert(node.name.clone(), node);
    }

    let snapshot = state.topology_snapshot();

    assert_eq!(snapshot.slot_owners[10].as_ref().unwrap().as_str(), PFAIL);
    assert_eq!(snapshot.slot_owners[11].as_ref().unwrap().as_str(), PFAIL);
    assert_eq!(
        snapshot.slot_owners[12].as_ref().unwrap().as_str(),
        PROMOTED
    );
    assert_eq!(snapshot.slot_owners[13].as_ref().unwrap().as_str(), LOWER);
    assert!(snapshot.slot_owners[14].is_none());

    let health = |id: &str| {
        snapshot
            .nodes
            .iter()
            .find(|node| node.id.as_str() == id)
            .map(|node| node.health)
            .unwrap()
    };
    assert_eq!(health(PFAIL), ClusterNodeHealth::PFail);
    assert_eq!(health(FAILED), ClusterNodeHealth::Fail);
    assert_eq!(health(LOADING), ClusterNodeHealth::Loading);
    assert_eq!(health(NOADDR), ClusterNodeHealth::NoAddr);
    assert_eq!(health(HANDSHAKE), ClusterNodeHealth::Handshake);

    let shard = snapshot
        .shards
        .iter()
        .find(|shard| shard.id.as_str() == SHARD)
        .unwrap();
    assert_eq!(
        shard
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>(),
        vec![FAILED, PROMOTED, LOADING]
    );
    assert_eq!(shard.slot_ranges, vec![SlotRange::new(12, 12).unwrap()]);
    let promoted_view = shard
        .nodes
        .iter()
        .find(|node| node.id.as_str() == PROMOTED)
        .unwrap();
    assert_eq!(
        promoted_view.endpoints.hostname(),
        Some("promoted.example.com")
    );
}

#[test]
fn topology_snapshot_generation_changes_only_after_mutation() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let first = state.topology_snapshot();
    let second = state.topology_snapshot();

    assert_eq!(first.generation, second.generation);

    state.add_slot(42).unwrap();
    let changed = state.topology_snapshot();

    assert!(changed.generation > second.generation);
    assert_eq!(
        changed.slot_owners[42].as_ref().map(|id| id.as_str()),
        Some(state.myself.name.as_str())
    );
}

#[test]
fn topology_snapshot_generation_tracks_open_slots_roles_and_health() {
    const PRIMARY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let mut primary = topology_node(PRIMARY, 6380, 1);
    primary.slots[42] = true;
    primary.numslots = 1;
    state.nodes.insert(primary.name.clone(), primary);

    let before_open = state.topology_generation();
    state.set_slot_importing(42, PRIMARY).unwrap();
    assert!(state.topology_generation() > before_open);

    let before_stable = state.topology_generation();
    state.set_slot_stable(42).unwrap();
    assert!(state.topology_generation() > before_stable);

    let before_role = state.topology_generation();
    state.replicate(PRIMARY.as_bytes()).unwrap();
    assert!(state.topology_generation() > before_role);

    let before_health = state.topology_generation();
    assert!(state.mark_fail(PRIMARY));
    assert!(state.topology_generation() > before_health);
}

#[test]
fn topology_snapshot_owner_matches_existing_owner_consumers() {
    const LOWER: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const WINNER: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let mut lower = topology_node(LOWER, 6380, 4);
    let mut winner = topology_node(WINNER, 6381, 5);
    for slot in [0, 1] {
        lower.slots[slot] = true;
        winner.slots[slot] = true;
    }
    lower.numslots = 2;
    winner.numslots = 2;
    state.nodes.insert(lower.name.clone(), lower);
    state.nodes.insert(winner.name.clone(), winner);

    let snapshot = state.topology_snapshot();
    let node_ranges = ClusterState::slot_ranges_for_node(&snapshot.slot_owners, WINNER);
    let slots = state.slots_array();
    let slots_owner = match &slots[0][2] {
        Value::Array(entry) => match &entry[2] {
            Value::Bulk(id) => std::str::from_utf8(id).unwrap(),
            other => panic!("expected slot owner id, got {other:?}"),
        },
        other => panic!("expected slot owner entry, got {other:?}"),
    };

    assert_eq!(snapshot.slot_owners[0].as_ref().unwrap().as_str(), WINNER);
    assert_eq!(node_ranges, "0-1");
    assert_eq!(slots_owner, WINNER);
    assert_eq!(state.get_node_by_slot(0).unwrap().name, WINNER);
    assert_eq!(
        state
            .check_redirect_with_keys(&[b"{06S}"], 0, None, RedirectOptions::default())
            .unwrap_err(),
        "MOVED 0 127.0.0.1:6381"
    );
}

#[test]
fn slot_range_is_inclusive_and_rejects_invalid_bounds() {
    let range = SlotRange::new(7, 9).unwrap();

    assert!(range.contains(7));
    assert!(range.contains(9));
    assert!(!range.contains(10));
    assert_eq!(range.to_string(), "7-9");
    assert_eq!(SlotRange::new(7, 7).unwrap().to_string(), "7");
    assert!(SlotRange::new(9, 7).is_err());
    assert!(SlotRange::new(0, CLUSTER_SLOTS as u16).is_err());
}

#[test]
fn readonly_client_can_read_primary_owned_slot_from_its_replica() {
    let key = b"replica-read";
    let slot = key_hash_slot(key) as usize;
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let mut primary = ClusterNode::new(
        primary_id.clone(),
        "127.0.0.1".to_string(),
        6379,
        16379,
        true,
    );
    primary.flags &= !CLUSTER_NODE_MYSELF;
    primary.slots[slot] = true;
    primary.numslots = 1;
    state.nodes.insert(primary_id.clone(), primary);
    state.slots[slot] = true;
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = primary_id;

    assert!(state
        .check_redirect_with_keys(&[key], CMD_READONLY, None, RedirectOptions::default())
        .is_err());
    assert!(state
        .check_redirect_with_keys(
            &[key],
            CMD_READONLY,
            None,
            RedirectOptions {
                allow_replica_read: true,
                ..RedirectOptions::default()
            },
        )
        .is_ok());
}

#[test]
fn readonly_replica_serves_local_key_while_replicaof_converges() {
    const OLD_PRIMARY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const NEW_PRIMARY: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let key = b"replica-read-convergence";
    let slot = key_hash_slot(key) as usize;
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let mut old_primary = ClusterNode::new(
        OLD_PRIMARY.to_string(),
        "127.0.0.1".to_string(),
        6378,
        16378,
        true,
    );
    old_primary.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(old_primary.name.clone(), old_primary);
    let mut owner = ClusterNode::new(
        NEW_PRIMARY.to_string(),
        "127.0.0.1".to_string(),
        6379,
        16379,
        true,
    );
    owner.flags &= !CLUSTER_NODE_MYSELF;
    owner.slots[slot] = true;
    owner.numslots = 1;
    state.nodes.insert(owner.name.clone(), owner);
    state.slots[slot] = true;
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = OLD_PRIMARY.to_string();

    assert!(state
        .check_redirect_with_keys(
            &[key],
            CMD_READONLY,
            Some(&[true]),
            RedirectOptions {
                allow_replica_read: true,
                ..RedirectOptions::default()
            },
        )
        .is_ok());
    assert!(state
        .check_redirect_with_keys(
            &[key],
            CMD_READONLY,
            Some(&[false]),
            RedirectOptions {
                allow_replica_read: true,
                ..RedirectOptions::default()
            },
        )
        .is_ok());
}

#[test]
fn unknown_endpoint_redirect_omits_owner_host() {
    const OWNER: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let key = b"unknown-endpoint";
    let slot = key_hash_slot(key) as usize;
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let mut owner = ClusterNode::new(
        OWNER.to_string(),
        "127.0.0.1".to_string(),
        6379,
        16379,
        true,
    );
    owner.flags &= !CLUSTER_NODE_MYSELF;
    owner.slots[slot] = true;
    owner.numslots = 1;
    state.nodes.insert(owner.name.clone(), owner);
    state.slots[slot] = true;

    assert_eq!(
        state
            .check_redirect_with_keys(
                &[key],
                0,
                None,
                RedirectOptions {
                    preferred_endpoint:
                        crate::engine::distributed::cluster::PreferredEndpoint::Unknown,
                    ..RedirectOptions::default()
                },
            )
            .unwrap_err(),
        format!("MOVED {} :6379", slot)
    );
}

#[test]
fn hostname_endpoint_redirect_uses_the_announced_hostname() {
    const OWNER: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let key = b"hostname-endpoint";
    let slot = key_hash_slot(key) as usize;
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let mut owner = ClusterNode::new(
        OWNER.to_string(),
        "127.0.0.1".to_string(),
        6379,
        16379,
        true,
    );
    owner.flags &= !CLUSTER_NODE_MYSELF;
    owner.endpoints.set_hostname(b"owner.example.com").unwrap();
    owner.slots[slot] = true;
    owner.numslots = 1;
    state.nodes.insert(owner.name.clone(), owner);

    assert_eq!(
        state
            .check_redirect_with_keys(
                &[key],
                0,
                None,
                RedirectOptions {
                    preferred_endpoint:
                        crate::engine::distributed::cluster::PreferredEndpoint::Hostname,
                    ..RedirectOptions::default()
                },
            )
            .unwrap_err(),
        format!("MOVED {} owner.example.com:6379", slot)
    );
}

#[test]
fn blocked_redirect_requires_healthy_cluster() {
    let key = b"blocked-cluster-down";
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let slot = key_hash_slot(key) as usize;
    state.add_slot(slot).unwrap();

    assert_eq!(
        state
            .check_redirect_with_keys(
                &[key],
                0,
                None,
                RedirectOptions {
                    require_cluster_ok: true,
                    ..RedirectOptions::default()
                },
            )
            .unwrap_err(),
        "CLUSTERDOWN The cluster is down"
    );
}

#[test]
fn pfail_primary_remains_authoritative_slot_owner() {
    use crate::engine::distributed::cluster::node::CLUSTER_NODE_PFAIL;

    let key = b"pfail-owner";
    let slot = key_hash_slot(key) as usize;
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let mut owner = ClusterNode::new(
        "owner".to_string(),
        "127.0.0.1".to_string(),
        6379,
        16379,
        true,
    );
    owner.flags &= !CLUSTER_NODE_MYSELF;
    owner.flags |= CLUSTER_NODE_PFAIL;
    owner.slots[slot] = true;
    owner.numslots = 1;
    state.nodes.insert(owner.name.clone(), owner);
    let mut higher_epoch = ClusterNode::new(
        "higher-epoch".to_string(),
        "127.0.0.1".to_string(),
        6381,
        16381,
        true,
    );
    higher_epoch.flags &= !CLUSTER_NODE_MYSELF;
    higher_epoch.config_epoch = 10;
    state.nodes.insert(higher_epoch.name.clone(), higher_epoch);

    assert_eq!(state.get_node_by_slot(slot).unwrap().name, "owner");
}

#[test]
fn zero_claimant_slots_render_full_coverage_in_slots_array() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.myself.config_epoch = 1;
    let (covered_count, range0_id) = {
        let slots_array = state.slots_array();
        let mut covered_count = 0usize;
        for range in slots_array.iter() {
            if let (Some(Value::Integer(s)), Some(Value::Integer(e))) =
                (range.first(), range.get(1))
            {
                covered_count += (*e as usize).saturating_sub(*s as usize) + 1;
            }
        }
        let first_range_owner_id = slots_array.first().and_then(|r| {
            r.get(2).and_then(|col| match col {
                Value::Array(row) => row.get(2).and_then(|cell| match cell {
                    Value::Bulk(b) => String::from_utf8(b.as_ref().to_vec()).ok(),
                    _ => None,
                }),
                _ => None,
            })
        });
        (covered_count, first_range_owner_id)
    };
    assert_eq!(
        covered_count, CLUSTER_SLOTS,
        "CLUSTER SLOTS must render full 0..16383 coverage even when no node has \
         explicitly claimed any slot yet (render-only Pass 5 zero-claimant fill)"
    );
    assert_eq!(
        range0_id.as_deref(),
        Some(state.myself.name.as_str()),
        "render-only fallback should pick myself as the global epoch leader"
    );
}

#[test]
fn partial_topology_does_not_assign_unclaimed_slots_to_a_fallback_owner() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.myself.config_epoch = 1;
    for slot in 0..10_922 {
        state.myself.slots[slot] = true;
    }
    state.myself.numslots = 10_922;

    let slots = state.slots_array();

    let covered = slots
        .iter()
        .filter_map(|range| match (range.first(), range.get(1)) {
            (Some(Value::Integer(start)), Some(Value::Integer(end))) => {
                Some((*end - *start + 1) as usize)
            }
            _ => None,
        })
        .sum::<usize>();
    assert_eq!(covered, 10_922);
}

#[test]
fn cluster_slots_node_entry_includes_hostname_and_availability_zone_metadata() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.add_slot(0).unwrap();
    state
        .myself
        .endpoints
        .set_hostname(b"node.example.com")
        .unwrap();
    state
        .myself
        .endpoints
        .set_availability_zone(b"zone-a")
        .unwrap();

    let slots = state.slots_array();
    let Value::Array(owner) = &slots[0][2] else {
        panic!("expected owner array")
    };

    assert_eq!(owner.len(), 4);
    assert_eq!(
        owner[3],
        Value::Map(vec![
            (Value::bulk("hostname"), Value::bulk("node.example.com")),
            (Value::bulk("availability-zone"), Value::bulk("zone-a")),
        ])
    );
}

#[test]
fn zero_claimant_slots_render_same_full_coverage_in_nodes() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.myself.config_epoch = 1;

    let nodes = state.nodes_str();

    assert!(
        nodes.split_whitespace().any(|field| field == "0-16383"),
        "CLUSTER NODES and CLUSTER SLOTS must expose the same render-safe coverage; got '{nodes}'"
    );
}

#[test]
fn disabled_full_coverage_keeps_failed_declared_owner_for_moved() {
    use crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL;

    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let mut owner = ClusterNode::new(
        "failed-owner".to_string(),
        "127.0.0.1".to_string(),
        6379,
        16379,
        true,
    );
    owner.flags &= !CLUSTER_NODE_MYSELF;
    owner.flags |= CLUSTER_NODE_FAIL;
    owner.slots[0] = true;
    owner.numslots = 1;
    state.nodes.insert(owner.name.clone(), owner);
    state.set_require_full_coverage(false);

    assert_eq!(
        state.get_node_by_slot(0).map(|node| node.name.as_str()),
        Some("failed-owner")
    );
}

#[test]
fn failed_owner_without_a_related_promotee_stays_unassigned() {
    const FAILED: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const UNRELATED: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.myself.config_epoch = 9;

    let mut failed = topology_node(FAILED, 6380, 1);
    failed.flags |= CLUSTER_NODE_FAIL;
    failed.shard_id = FAILED.to_string();
    failed.slots[0] = true;
    failed.numslots = 1;

    let mut unrelated = topology_node(UNRELATED, 6381, 8);
    unrelated.shard_id = UNRELATED.to_string();
    unrelated.slots[1] = true;
    unrelated.numslots = 1;

    state.nodes.insert(failed.name.clone(), failed);
    state.nodes.insert(unrelated.name.clone(), unrelated);

    let snapshot = state.topology_snapshot();

    assert!(
        snapshot.slot_owners[0].is_none(),
        "a healthy primary from another shard must not inherit a failed owner's slot"
    );
    assert_eq!(
        snapshot.slot_owners[1].as_ref().map(|id| id.as_str()),
        Some(UNRELATED)
    );
}

#[test]
fn local_scan_range_stops_at_higher_epoch_remote_owner() {
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    state.myself.config_epoch = 1;
    state.myself.slots[0] = true;
    state.myself.slots[1] = true;
    state.myself.numslots = 2;

    let mut remote = ClusterNode::new(
        "higher-epoch-owner".to_string(),
        "127.0.0.1".to_string(),
        6379,
        16379,
        true,
    );
    remote.flags &= !CLUSTER_NODE_MYSELF;
    remote.config_epoch = 2;
    remote.slots[1] = true;
    remote.numslots = 1;
    state.nodes.insert(remote.name.clone(), remote);

    assert_eq!(
        state.local_slot_range_end(0),
        Some(0),
        "a stale local bitmap must not extend scanning across a canonical MOVED boundary"
    );
}

#[test]
fn small_unclaimed_hole_is_visible_in_slots_and_nodes() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.myself.config_epoch = 1;
    for slot in 0..CLUSTER_SLOTS {
        if slot != 42 {
            state.myself.slots[slot] = true;
            state.myself.numslots += 1;
        }
    }

    let slots_cover_hole = state.slots_array().iter().any(|range| {
        matches!(
            (range.first(), range.get(1)),
            (Some(Value::Integer(start)), Some(Value::Integer(end)))
                if *start <= 42 && 42 <= *end
        )
    });
    let nodes = state.nodes_str();

    assert!(
        !slots_cover_hole,
        "CLUSTER SLOTS must preserve a DELSLOTS hole"
    );
    assert!(
        !nodes
            .split_whitespace()
            .any(|field| field == "0-16383" || field == "42"),
        "CLUSTER NODES must preserve the same DELSLOTS hole; got '{nodes}'"
    );
}
