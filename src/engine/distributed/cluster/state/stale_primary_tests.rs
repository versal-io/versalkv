use super::{ClusterState, DemotionReason, StalePrimaryDecision};
use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};
use crate::engine::distributed::cluster::NodeId;

fn node_id(byte: u8) -> String {
    String::from_utf8(vec![byte; 40]).expect("ASCII node id")
}

fn state_with_local_slots(epoch: u64, shard_id: &str, slots: usize) -> ClusterState {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.myself.name = node_id(b'z');
    state.myself.shard_id = shard_id.to_string();
    state.myself.config_epoch = epoch;
    state.myself.slots.fill(false);
    for slot in 0..slots {
        state.myself.slots[slot] = true;
    }
    state.myself.numslots = slots;
    state
}

fn add_primary(
    state: &mut ClusterState,
    id: u8,
    epoch: u64,
    shard_id: &str,
    slots: std::ops::Range<usize>,
    replicaof: &str,
) {
    let id = node_id(id);
    let mut node = ClusterNode::new(id.clone(), "127.0.0.1".to_string(), 6380, 16_380, true);
    node.flags &= !CLUSTER_NODE_MYSELF;
    node.config_epoch = epoch;
    node.shard_id = shard_id.to_string();
    node.replicaof = replicaof.to_string();
    for slot in slots {
        node.slots[slot] = true;
        node.numslots += 1;
    }
    state.nodes.insert(id, node);
}

#[test]
fn disabled_or_slotless_local_primary_is_kept() {
    let mut disabled = state_with_local_slots(1, &node_id(b's'), 1_000);
    disabled.set_replica_migration_allowed(false);
    assert_eq!(
        disabled.decide_stale_primary(),
        StalePrimaryDecision::KeepPrimary
    );

    let slotless = state_with_local_slots(1, &node_id(b's'), 0);
    assert_eq!(
        slotless.decide_stale_primary(),
        StalePrimaryDecision::KeepPrimary
    );
}

#[test]
fn same_shard_higher_epoch_primary_wins_before_bitmap_reconciliation() {
    let shard = node_id(b's');
    let mut state = state_with_local_slots(1, &shard, 1_000);
    add_primary(&mut state, b'a', 5, &shard, 2_000..2_001, "");

    assert_eq!(
        state.decide_stale_primary(),
        StalePrimaryDecision::FollowPrimary {
            target: NodeId::parse(node_id(b'a').as_bytes()).unwrap(),
            reason: DemotionReason::ShardEpochSuperseder,
            overlap: 1_000,
        }
    );
}

#[test]
fn bitmap_free_claimant_uses_the_highest_epoch_primary() {
    let shard = node_id(b's');
    let mut state = state_with_local_slots(1, "", 1_200);
    add_primary(&mut state, b'a', 4, &shard, 0..1_000, "");
    add_primary(&mut state, b'b', 6, &shard, 2_000..2_001, "");

    assert_eq!(
        state.decide_stale_primary(),
        StalePrimaryDecision::FollowPrimary {
            target: NodeId::parse(node_id(b'b').as_bytes()).unwrap(),
            reason: DemotionReason::BitmapFreeStaleClaimant {
                slotted_primaries: 3,
                distinct_shards: 1,
                duplicate_shard: true,
                extra_claimant: true,
                lowest_epoch: 1,
            },
            overlap: 1_200,
        }
    );
}

#[test]
fn overlap_fallback_prefers_larger_overlap_over_newer_epoch() {
    let mut state = state_with_local_slots(2, "", 1_500);
    add_primary(&mut state, b'b', 4, "", 0..1_200, "");
    add_primary(&mut state, b'a', 5, "", 0..1_000, "");

    assert_eq!(
        state.decide_stale_primary(),
        StalePrimaryDecision::FollowPrimary {
            target: NodeId::parse(node_id(b'b').as_bytes()).unwrap(),
            reason: DemotionReason::SlotOverlap,
            overlap: 1_200,
        }
    );
}

#[test]
fn overlap_fallback_breaks_equal_overlap_by_epoch_then_node_id() {
    let mut state = state_with_local_slots(2, "", 1_500);
    add_primary(&mut state, b'b', 4, "", 0..1_200, "");
    add_primary(&mut state, b'c', 5, "", 0..1_200, "");
    add_primary(&mut state, b'a', 5, "", 0..1_200, "");

    assert_eq!(
        state.decide_stale_primary(),
        StalePrimaryDecision::FollowPrimary {
            target: NodeId::parse(node_id(b'a').as_bytes()).unwrap(),
            reason: DemotionReason::SlotOverlap,
            overlap: 1_200,
        }
    );
}

#[test]
fn candidate_that_still_reports_local_as_primary_is_not_followed() {
    let shard = node_id(b's');
    let mut state = state_with_local_slots(1, &shard, 1_000);
    let local = state.myself.name.clone();
    add_primary(&mut state, b'a', 5, &shard, 0..1_000, &local);

    assert_eq!(
        state.decide_stale_primary(),
        StalePrimaryDecision::KeepPrimary
    );
}
