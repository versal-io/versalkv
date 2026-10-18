use super::ClusterBusManager;
use crate::engine::distributed::cluster::msg::{
    slots_to_bytes, ClusterBusBody, ClusterMsg, ClusterMsgType, SlotOwnershipUpdate,
};
use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};
use crate::engine::distributed::cluster::state::{
    ClusterState, OpenSlotUpdate, PendingReplicationSwitch, ReplicationSwitchMode,
    SlotClaimDecision, SlotOwnerUpdate,
};
use crate::engine::distributed::cluster::NodeId;

const TARGET_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const OLD_PRIMARY_ID: &str = "cccccccccccccccccccccccccccccccccccccccc";
const PROMOTED_ID: &str = "dddddddddddddddddddddddddddddddddddddddd";
const STALE_PRIMARY_ID: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

fn topology_message(
    message_type: ClusterMsgType,
    node: &ClusterNode,
    current_epoch: u64,
    config_epoch: u64,
) -> ClusterMsg {
    let body = if message_type == ClusterMsgType::Update {
        ClusterBusBody::Update(SlotOwnershipUpdate {
            config_epoch,
            node_id: NodeId::parse(node.name.as_bytes()).unwrap(),
            slots: slots_to_bytes(&node.slots),
        })
    } else {
        ClusterBusBody::Gossip(Vec::new())
    };
    let mut message = ClusterMsg::from_node(message_type, node, current_epoch, body);
    message.header.config_epoch = config_epoch;
    message
}

fn remote_primary(node_id: &str, shard_id: &str, config_epoch: u64) -> ClusterNode {
    let mut node = ClusterNode::new(
        node_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    node.flags &= !CLUSTER_NODE_MYSELF;
    node.shard_id = shard_id.to_string();
    node.config_epoch = config_epoch;
    node
}

#[test]
fn promoted_target_rewrites_unclaimed_migrating_slot_destination() {
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};

    const OPEN_SLOT: usize = 609;
    const CLAIMED_SLOT: usize = 700;
    const TARGET_SHARD: &str = "target-shard";

    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.add_slot(OPEN_SLOT).unwrap();
    state.seed_migrating_slot(OPEN_SLOT, OLD_PRIMARY_ID);

    let mut old_target = remote_primary(OLD_PRIMARY_ID, TARGET_SHARD, 4);
    old_target.flags &= !CLUSTER_NODE_PRIMARY;
    old_target.flags |= CLUSTER_NODE_REPLICA;
    old_target.replicaof = PROMOTED_ID.to_string();
    state.nodes.insert(old_target.name.clone(), old_target);

    let mut promoted = remote_primary(PROMOTED_ID, TARGET_SHARD, 5);
    promoted.slots[CLAIMED_SLOT] = true;
    promoted.numslots = 1;
    state.nodes.insert(promoted.name.clone(), promoted.clone());
    let message = topology_message(ClusterMsgType::Update, &promoted, 5, 5);

    ClusterBusManager::process_update_slot_claims(&mut state, &message, true);

    assert_eq!(state.migrating_slot_target(OPEN_SLOT), Some(PROMOTED_ID));
}

#[test]
fn promoted_source_rewrites_claimed_importing_slot_source() {
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};

    const OPEN_SLOT: usize = 609;
    const SOURCE_SHARD: &str = "source-shard";

    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.seed_importing_slot(OPEN_SLOT, OLD_PRIMARY_ID);

    let mut old_source = remote_primary(OLD_PRIMARY_ID, SOURCE_SHARD, 4);
    old_source.flags &= !CLUSTER_NODE_PRIMARY;
    old_source.flags |= CLUSTER_NODE_REPLICA;
    old_source.replicaof = PROMOTED_ID.to_string();
    old_source.slots[OPEN_SLOT] = true;
    old_source.numslots = 1;
    state.nodes.insert(old_source.name.clone(), old_source);

    let mut promoted = remote_primary(PROMOTED_ID, SOURCE_SHARD, 5);
    promoted.slots[OPEN_SLOT] = true;
    promoted.numslots = 1;
    state.nodes.insert(promoted.name.clone(), promoted.clone());
    let message = topology_message(ClusterMsgType::Update, &promoted, 5, 5);

    ClusterBusManager::process_update_slot_claims(&mut state, &message, true);

    assert_eq!(state.importing_slot_source(OPEN_SLOT), Some(PROMOTED_ID));
}

#[test]
fn importing_primary_claims_slot_when_source_stops_advertising_it() {
    const OPEN_SLOT: usize = 609;

    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.seed_importing_slot(OPEN_SLOT, OLD_PRIMARY_ID);
    let mut source = remote_primary(OLD_PRIMARY_ID, OLD_PRIMARY_ID, 4);
    source.slots[OPEN_SLOT] = true;
    source.numslots = 1;
    state.nodes.insert(source.name.clone(), source.clone());

    source.slots[OPEN_SLOT] = false;
    source.numslots = 0;
    let message = topology_message(ClusterMsgType::Pong, &source, 5, 4);
    ClusterBusManager::process_update_slot_claims(&mut state, &message, false);

    assert!(state.myself.slots[OPEN_SLOT]);
    assert_eq!(state.myself.numslots, 1);
    assert!(state.importing_slot_source(OPEN_SLOT).is_none());
}

#[test]
fn disabled_replica_migration_keeps_dethroned_primary_empty() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.current_epoch = 5;
    state.myself.config_epoch = 4;
    state.add_slot(0).unwrap();
    state.set_replica_migration_allowed(false);
    let mut target = ClusterNode::new(
        TARGET_ID.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    target.flags &= !CLUSTER_NODE_MYSELF;
    target.config_epoch = 5;
    state.nodes.insert(target.name.clone(), target.clone());
    target.slots[0] = true;
    target.numslots = 1;
    let message = topology_message(ClusterMsgType::Update, &target, 5, 5);

    ClusterBusManager::process_update_slot_claims(&mut state, &message, true);

    assert!(state.myself.is_primary());
    assert_eq!(state.myself.numslots, 0);
    assert!(state.pending_replication_switch.is_none());
}

#[test]
fn sub_replica_follows_new_owner_even_when_migration_is_disabled() {
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};

    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.current_epoch = 5;
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = OLD_PRIMARY_ID.to_string();
    state.myself.shard_id = "shared-shard".to_string();
    state.set_replica_migration_allowed(false);
    let mut old_primary = ClusterNode::new(
        OLD_PRIMARY_ID.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    old_primary.flags &= !CLUSTER_NODE_MYSELF;
    old_primary.config_epoch = 4;
    old_primary.shard_id = "shared-shard".to_string();
    old_primary.slots[0] = true;
    old_primary.numslots = 1;
    state.insert_node(old_primary);
    let mut target = ClusterNode::new(
        TARGET_ID.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16381,
        true,
    );
    target.flags &= !CLUSTER_NODE_MYSELF;
    target.config_epoch = 5;
    target.shard_id = "shared-shard".to_string();
    state.nodes.insert(target.name.clone(), target.clone());
    target.slots[0] = true;
    target.numslots = 1;
    let message = topology_message(ClusterMsgType::Update, &target, 5, 5);

    ClusterBusManager::process_update_slot_claims(&mut state, &message, true);

    assert_eq!(state.myself.replicaof, OLD_PRIMARY_ID);
    assert_eq!(
        state.pending_replication_switch,
        Some(PendingReplicationSwitch {
            node_id: TARGET_ID.to_string(),
            mode: ReplicationSwitchMode::TryPartial,
        })
    );
}

#[test]
fn preloaded_sender_bitmap_still_counts_current_primary_migration() {
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};

    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = OLD_PRIMARY_ID.to_string();
    state.myself.shard_id = "shared-shard".to_string();
    state.set_replica_migration_allowed(false);

    let mut old_primary = ClusterNode::new(
        OLD_PRIMARY_ID.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    old_primary.flags &= !CLUSTER_NODE_MYSELF;
    old_primary.config_epoch = 4;
    old_primary.shard_id = "shared-shard".to_string();
    old_primary.slots[0] = true;
    old_primary.numslots = 1;
    state.insert_node(old_primary);

    let mut target = ClusterNode::new(
        TARGET_ID.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16381,
        true,
    );
    target.flags &= !CLUSTER_NODE_MYSELF;
    target.config_epoch = 5;
    target.shard_id = "shared-shard".to_string();
    target.slots[0] = true;
    target.numslots = 1;
    state.nodes.insert(target.name.clone(), target.clone());

    let message = topology_message(ClusterMsgType::Update, &target, 5, 5);

    ClusterBusManager::process_update_slot_claims(&mut state, &message, true);

    assert!(state.nodes[OLD_PRIMARY_ID].slots.iter().all(|owned| !owned));
    assert_eq!(state.myself.replicaof, OLD_PRIMARY_ID);
    assert_eq!(
        state.pending_replication_switch,
        Some(PendingReplicationSwitch {
            node_id: TARGET_ID.to_string(),
            mode: ReplicationSwitchMode::TryPartial,
        })
    );
}

#[test]
fn empty_primary_ignores_sender_without_migrated_current_primary_slots() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.current_epoch = 10;
    state.myself.config_epoch = 10;
    state.add_slot(0).unwrap();
    state.del_slot(0).unwrap();

    let mut sender = ClusterNode::new(
        TARGET_ID.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !CLUSTER_NODE_MYSELF;
    sender.config_epoch = 2;
    sender.replicaof = state.myself.name.clone();
    sender.shard_id = "different-primary-shard".to_string();
    sender.slots[0] = true;
    sender.numslots = 1;
    state.nodes.insert(sender.name.clone(), sender.clone());

    let message = topology_message(ClusterMsgType::Update, &sender, 10, 2);

    ClusterBusManager::process_update_slot_claims(&mut state, &message, true);

    assert!(state.myself.is_primary());
    assert!(state.myself.replicaof.is_empty());
    assert!(state.pending_replication_switch.is_none());
}

#[test]
fn stale_duplicate_claim_does_not_reparent_the_new_slot_owner() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.current_epoch = 22;
    state.myself.config_epoch = 22;
    state.myself.shard_id = "new-shard".to_string();
    state.add_slot(0).unwrap();

    let mut sender = remote_primary(TARGET_ID, "old-shard", 21);
    state.insert_node(sender.clone());
    sender.slots[0] = true;
    sender.numslots = 1;
    let message = topology_message(ClusterMsgType::Update, &sender, 21, 21);

    ClusterBusManager::process_update_slot_claims(&mut state, &message, false);

    assert!(state.myself.is_primary());
    assert!(state.myself.replicaof.is_empty());
    assert!(state.pending_replication_switch.is_none());
}

#[test]
fn empty_old_primary_follows_same_shard_owner_after_third_party_stripped_slots() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.current_epoch = 13;
    state.myself.config_epoch = 7;
    state.myself.shard_id = "shared-shard".to_string();
    state.add_slot(0).unwrap();

    // Model the ordering seen during TAKEOVER convergence: an update relayed
    // by another node has already removed our old slot bitmap before the new
    // primary's own announcement reaches us.
    state.myself.slots[0] = false;
    state.myself.numslots = 0;

    let mut promoted = remote_primary(PROMOTED_ID, "shared-shard", 13);
    // The local topology still remembers the sender as our replica while its
    // direct primary announcement carries the post-TAKEOVER slot bitmap.
    promoted.replicaof = state.myself.name.clone();
    promoted.slots[0] = true;
    promoted.numslots = 1;
    state.nodes.insert(promoted.name.clone(), promoted.clone());
    let message = topology_message(ClusterMsgType::Update, &promoted, 13, 13);

    ClusterBusManager::process_update_slot_claims(&mut state, &message, true);

    assert_eq!(
        state.pending_replication_switch,
        Some(PendingReplicationSwitch {
            node_id: PROMOTED_ID.to_string(),
            mode: ReplicationSwitchMode::TryPartial,
        })
    );
}

#[test]
fn stale_claim_does_not_cross_local_slot_delete_barrier() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.current_epoch = 10;
    state.myself.config_epoch = 10;
    let mut sender = ClusterNode::new(
        TARGET_ID.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !CLUSTER_NODE_MYSELF;
    sender.config_epoch = 2;
    sender.slots[0] = true;
    sender.numslots = 1;
    state.insert_node(sender.clone());

    state.del_slot(0).unwrap();
    assert_eq!(state.slot_delete_epoch(0), 11);
    let message = topology_message(ClusterMsgType::Pong, &sender, 2, 2);

    ClusterBusManager::process_update_slot_claims(&mut state, &message, false);

    assert!(state.declared_node_by_slot(0).is_none());
}

#[test]
fn epoch_zero_claim_is_accepted_without_a_delete_barrier() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let mut sender = ClusterNode::new(
        TARGET_ID.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !CLUSTER_NODE_MYSELF;
    sender.slots[0] = true;
    sender.numslots = 1;
    let mut registered_sender = sender.clone();
    registered_sender.slots[0] = false;
    registered_sender.numslots = 0;
    state
        .nodes
        .insert(registered_sender.name.clone(), registered_sender);

    let message = topology_message(ClusterMsgType::Pong, &sender, 0, 0);

    let generation = state.topology_generation();
    ClusterBusManager::process_update_slot_claims(&mut state, &message, false);

    assert_eq!(
        state
            .declared_node_by_slot(0)
            .map(|node| node.name.as_str()),
        Some(TARGET_ID)
    );
    assert!(state.topology_generation() > generation);
}

#[test]
fn same_shard_update_strips_stale_primary_even_if_epoch_drifted_higher() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.current_epoch = 22;
    state.myself.config_epoch = 18;
    state.myself.shard_id = "shared-shard".to_string();
    state.add_slot(0).unwrap();

    let mut promoted = ClusterNode::new(
        PROMOTED_ID.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    promoted.flags &= !CLUSTER_NODE_MYSELF;
    promoted.config_epoch = 14;
    promoted.shard_id = "shared-shard".to_string();
    promoted.replicaof = state.myself.name.clone();
    promoted.slots[0] = true;
    promoted.numslots = 1;
    state.nodes.insert(promoted.name.clone(), promoted.clone());

    let message = topology_message(ClusterMsgType::Update, &promoted, 22, 14);

    ClusterBusManager::process_update_slot_claims(&mut state, &message, true);

    assert!(
        state.take_dirty_slot_cleanup_requests().is_empty(),
        "same-shard promotion must retain the dataset for replication handoff"
    );

    assert!(state.myself.is_primary());
    assert!(state.myself.replicaof.is_empty());
    assert_eq!(
        state.pending_replication_switch,
        Some(PendingReplicationSwitch {
            node_id: PROMOTED_ID.to_string(),
            mode: ReplicationSwitchMode::TryPartial,
        })
    );
}

#[test]
fn stale_same_shard_primary_cannot_reclaim_promoted_primary_slots() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.current_epoch = 7;
    state.myself.config_epoch = 7;
    state.myself.shard_id = "shared-shard".to_string();
    state.add_slot(0).unwrap();

    let mut stale_primary = ClusterNode::new(
        STALE_PRIMARY_ID.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    stale_primary.flags &= !CLUSTER_NODE_MYSELF;
    stale_primary.config_epoch = 5;
    stale_primary.shard_id = "shared-shard".to_string();
    stale_primary.slots[0] = true;
    stale_primary.numslots = 1;
    state
        .nodes
        .insert(stale_primary.name.clone(), stale_primary.clone());

    let message = topology_message(ClusterMsgType::Update, &stale_primary, 7, 5);

    ClusterBusManager::process_update_slot_claims(&mut state, &message, true);

    assert!(state.myself.is_primary());
    assert!(state.myself.slots[0]);
    assert!(state.pending_replication_switch.is_none());
}

#[test]
fn ping_like_same_shard_stale_primary_cannot_demote_promoted_primary() {
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};

    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.current_epoch = 6;
    state.myself.flags &= !CLUSTER_NODE_REPLICA;
    state.myself.flags |= CLUSTER_NODE_PRIMARY;
    state.myself.replicaof.clear();
    state.myself.config_epoch = 6;
    state.myself.shard_id = "shared-shard".to_string();
    state.myself.slots[0] = true;
    state.myself.numslots = 1;

    let mut stale_primary = ClusterNode::new(
        STALE_PRIMARY_ID.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    stale_primary.flags &= !CLUSTER_NODE_MYSELF;
    stale_primary.config_epoch = 1;
    stale_primary.shard_id = "shared-shard".to_string();
    stale_primary.slots[0] = true;
    stale_primary.numslots = 1;
    state
        .nodes
        .insert(stale_primary.name.clone(), stale_primary.clone());

    let message = topology_message(ClusterMsgType::Pong, &stale_primary, 6, 1);

    ClusterBusManager::process_update_slot_claims(&mut state, &message, false);

    assert!(state.myself.is_primary());
    assert!(state.myself.replicaof.is_empty());
    assert_eq!(state.myself.numslots, 1);
    assert!(state.pending_replication_switch.is_none());
}

#[test]
fn unchanged_primary_ping_skips_slot_reconciliation() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let mut sender = ClusterNode::new(
        TARGET_ID.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !CLUSTER_NODE_MYSELF;
    sender.config_epoch = 7;
    sender.slots[42] = true;
    sender.numslots = 1;
    sender.advertised_slots = slots_to_bytes(&sender.slots);
    sender.has_advertised_slots = true;
    state.nodes.insert(sender.name.clone(), sender.clone());

    let unchanged = topology_message(ClusterMsgType::Pong, &sender, 7, 7);
    assert!(ClusterBusManager::sender_slot_claim_deltas(&state, &unchanged).is_empty());

    sender.slots[43] = true;
    sender.numslots = 2;
    let changed = topology_message(ClusterMsgType::Pong, &sender, 7, 7);
    assert!(!ClusterBusManager::sender_slot_claim_deltas(&state, &changed).is_empty());
}

#[test]
fn unknown_replica_header_never_becomes_a_slot_claim() {
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};

    let state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let mut replica = remote_primary(TARGET_ID, "target-shard", 7);
    replica.flags &= !CLUSTER_NODE_PRIMARY;
    replica.flags |= CLUSTER_NODE_REPLICA;
    replica.replicaof = OLD_PRIMARY_ID.to_string();
    replica.slots[42] = true;
    replica.numslots = 1;
    let heartbeat = topology_message(ClusterMsgType::Pong, &replica, 7, 7);

    assert!(ClusterBusManager::sender_slot_claim_deltas(&state, &heartbeat).is_empty());
}

#[test]
fn unchanged_advertisement_reconciles_slots_removed_by_a_third_party_claim() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let mut advertised_sender = remote_primary(TARGET_ID, "target-shard", 36);
    advertised_sender.slots[42] = true;
    advertised_sender.slots[43] = true;
    advertised_sender.numslots = 2;

    let mut observed_sender = advertised_sender.clone();
    observed_sender.slots[43] = false;
    observed_sender.numslots = 1;
    observed_sender.advertised_slots = slots_to_bytes(&advertised_sender.slots);
    observed_sender.has_advertised_slots = true;
    state
        .nodes
        .insert(observed_sender.name.clone(), observed_sender);

    let announcement = topology_message(ClusterMsgType::Pong, &advertised_sender, 36, 36);
    let claims = ClusterBusManager::sender_slot_claim_deltas(&state, &announcement);

    assert_eq!(
        claims.len(),
        crate::engine::distributed::cluster::state::CLUSTER_SLOTS,
        "the accepted ownership bitmap, not the historical advertisement, controls the fast path"
    );
    ClusterBusManager::process_changed_slot_claims(&mut state, &announcement, false, &claims);
    assert!(state.nodes[TARGET_ID].slots[43]);
}

#[test]
fn unchanged_bitmap_reassertion_clears_withdrawn_sender_claim() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let mut sender = remote_primary(TARGET_ID, "target-shard", 6);
    sender.slots[0] = true;
    sender.numslots = 1;
    sender.advertised_slots = slots_to_bytes(&sender.slots);
    sender.has_advertised_slots = true;
    state.nodes.insert(sender.name.clone(), sender.clone());
    state.set_slot_claim_withdrawn(0, Some(TARGET_ID.to_string()));

    let announcement = topology_message(ClusterMsgType::Pong, &sender, 6, 6);
    let generation = state.topology_generation();
    let claims = ClusterBusManager::sender_slot_claim_deltas(&state, &announcement);

    assert_eq!(
        claims.len(),
        crate::engine::distributed::cluster::state::CLUSTER_SLOTS,
        "a sender reasserting a withdrawn claim must bypass the stable-bitmap fast path"
    );
    ClusterBusManager::process_changed_slot_claims(&mut state, &announcement, false, &claims);
    assert!(state.slot_claim_withdrawn_by(0).is_none());
    assert!(state.topology_generation() > generation);
    assert_eq!(
        state
            .declared_node_by_slot(0)
            .map(|owner| owner.name.as_str()),
        Some(TARGET_ID)
    );
}

#[test]
fn promoted_replica_reconciles_an_unchanged_primary_bitmap() {
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};

    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.myself.config_epoch = 5;
    state.add_slot(42).unwrap();

    let mut promoted = remote_primary(TARGET_ID, "shared-shard", 6);
    promoted.flags &= !CLUSTER_NODE_PRIMARY;
    promoted.flags |= CLUSTER_NODE_REPLICA;
    promoted.replicaof = OLD_PRIMARY_ID.to_string();
    promoted.slots[42] = true;
    promoted.numslots = 1;
    promoted.advertised_slots = slots_to_bytes(&promoted.slots);
    promoted.has_advertised_slots = true;
    state.nodes.insert(promoted.name.clone(), promoted.clone());

    promoted.flags &= !CLUSTER_NODE_REPLICA;
    promoted.flags |= CLUSTER_NODE_PRIMARY;
    promoted.replicaof.clear();
    let announcement = topology_message(ClusterMsgType::Pong, &promoted, 6, 6);

    let claims = ClusterBusManager::sender_slot_claim_deltas(&state, &announcement);

    assert_eq!(
        claims.len(),
        crate::engine::distributed::cluster::state::CLUSTER_SLOTS
    );
}

#[test]
fn unrelated_slot_claim_does_not_normalize_other_owners() {
    // C's clusterUpdateSlotsConfigWith only reconciles slots advertised by
    // the sender. A topology update for slot 1 must not scan and rewrite an
    // unrelated duplicate claim on slot 0. The latter is resolved when its
    // owner advertises the authoritative bitmap.
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.myself.config_epoch = 5;
    state.add_slot(0).unwrap();

    let mut stale = remote_primary(OLD_PRIMARY_ID, "other-shard", 4);
    stale.slots[0] = true;
    stale.numslots = 1;
    state.nodes.insert(stale.name.clone(), stale);

    let mut sender = remote_primary(TARGET_ID, "target-shard", 6);
    sender.slots[1] = true;
    sender.numslots = 1;
    state.nodes.insert(sender.name.clone(), sender.clone());
    let message = topology_message(ClusterMsgType::Pong, &sender, 6, 6);

    ClusterBusManager::process_update_slot_claims(&mut state, &message, false);

    assert!(state.myself.slots[0]);
    assert!(state.nodes[OLD_PRIMARY_ID].slots[0]);
    assert!(state.nodes[TARGET_ID].slots[1]);
}

#[test]
fn unchanged_slot_claim_does_not_recompute_cluster_health() {
    // C only schedules clusterUpdateState after an ownership/open-slot
    // mutation. A repeated PING/PONG claim must not turn into a global
    // 16K-slot health scan.
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let mut sender = remote_primary(TARGET_ID, "target-shard", 1);
    sender.slots.fill(true);
    sender.numslots = crate::engine::distributed::cluster::state::CLUSTER_SLOTS;
    state.nodes.insert(sender.name.clone(), sender.clone());
    let message = topology_message(ClusterMsgType::Pong, &sender, 1, 1);

    ClusterBusManager::process_update_slot_claims(&mut state, &message, false);

    assert_eq!(state.state, 1);
}

#[test]
fn changed_slot_claim_reconciles_the_sender_bitmap() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.myself.config_epoch = 5;
    state.add_slot(0).unwrap();

    let mut stale = remote_primary(OLD_PRIMARY_ID, "other-shard", 4);
    stale.slots[0] = true;
    stale.numslots = 1;
    state.nodes.insert(stale.name.clone(), stale);

    let mut sender = remote_primary(TARGET_ID, "target-shard", 6);
    sender.slots[1] = true;
    sender.numslots = 1;
    state.nodes.insert(sender.name.clone(), sender.clone());
    let message = topology_message(ClusterMsgType::Pong, &sender, 6, 6);

    let claims = ClusterBusManager::sender_slot_claim_deltas(&state, &message);
    assert_eq!(
        claims.len(),
        crate::engine::distributed::cluster::state::CLUSTER_SLOTS
    );
    ClusterBusManager::process_changed_slot_claims(&mut state, &message, false, &claims);

    assert!(state.myself.slots[0]);
    assert!(state.nodes[OLD_PRIMARY_ID].slots[0]);
    assert!(state.nodes[TARGET_ID].slots[1]);
}

#[test]
fn changed_claim_reasserts_all_sender_slots_after_an_epoch_bump() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.myself.config_epoch = 5;
    state.add_slot(0).unwrap();

    let mut sender = remote_primary(TARGET_ID, "target-shard", 5);
    sender.slots[0] = true;
    sender.slots[1] = true;
    sender.numslots = 2;
    state.nodes.insert(sender.name.clone(), sender.clone());
    let message = topology_message(ClusterMsgType::Pong, &sender, 6, 6);

    // Slot 1 is the wire delta. Slot 0 was already present in the sender's
    // cached bitmap, but its new epoch must still reassert ownership over the
    // local lower-epoch claim.
    let claims = ClusterBusManager::sender_slot_claim_deltas(&state, &message);
    assert_eq!(
        claims.len(),
        crate::engine::distributed::cluster::state::CLUSTER_SLOTS
    );
    ClusterBusManager::process_changed_slot_claims(&mut state, &message, false, &claims);

    assert!(!state.myself.slots[0]);
    assert!(state.nodes[TARGET_ID].slots[0]);
    assert!(state.nodes[TARGET_ID].slots[1]);
}

#[test]
fn withdrawn_sender_keeps_live_slot_binding_until_successor_claims() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let mut sender = remote_primary(TARGET_ID, "target-shard", 6);
    sender.slots[0] = true;
    sender.numslots = 1;
    sender.advertised_slots = slots_to_bytes(&sender.slots);
    sender.has_advertised_slots = true;
    state.nodes.insert(sender.name.clone(), sender.clone());

    sender.slots[0] = false;
    sender.numslots = 0;
    let message = topology_message(ClusterMsgType::Pong, &sender, 6, 6);
    let claims = ClusterBusManager::sender_slot_claim_deltas(&state, &message);
    ClusterBusManager::process_changed_slot_claims(&mut state, &message, false, &claims);

    assert_eq!(state.slot_claim_withdrawn_by(0), Some(TARGET_ID));
    assert!(
        state.nodes[TARGET_ID].slots[0],
        "history bitmap is retained"
    );
    assert_eq!(
        state
            .declared_node_by_slot(0)
            .map(|owner| owner.name.as_str()),
        None
    );
    assert_eq!(
        state.slot_owner_ref(0).map(|owner| owner.name.as_str()),
        Some(TARGET_ID),
        "C getNodeByQuery keeps routing through the retained cluster->slots binding"
    );
}

#[test]
fn withdrawn_owner_slot_accepts_a_lower_epoch_successor_claim() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let mut withdrawn_owner = remote_primary(OLD_PRIMARY_ID, "source-shard", 10);
    withdrawn_owner.slots[0] = true;
    withdrawn_owner.numslots = 1;
    withdrawn_owner.advertised_slots = slots_to_bytes(&withdrawn_owner.slots);
    withdrawn_owner.has_advertised_slots = true;
    state.insert_node(withdrawn_owner.clone());

    withdrawn_owner.slots[0] = false;
    withdrawn_owner.numslots = 0;
    let withdrawal = topology_message(ClusterMsgType::Pong, &withdrawn_owner, 10, 10);
    let withdrawn_claims = ClusterBusManager::sender_slot_claim_deltas(&state, &withdrawal);
    ClusterBusManager::process_changed_slot_claims(
        &mut state,
        &withdrawal,
        false,
        &withdrawn_claims,
    );
    assert_eq!(state.slot_claim_withdrawn_by(0), Some(OLD_PRIMARY_ID));

    let mut successor = remote_primary(TARGET_ID, "target-shard", 5);
    successor.slots[0] = true;
    successor.numslots = 1;
    state
        .nodes
        .insert(successor.name.clone(), successor.clone());
    let claim = topology_message(ClusterMsgType::Update, &successor, 10, 5);

    ClusterBusManager::process_update_slot_claims(&mut state, &claim, false);

    assert!(state.nodes[TARGET_ID].slots[0]);
    assert!(!state.nodes[OLD_PRIMARY_ID].slots[0]);
    assert!(state.slot_claim_withdrawn_by(0).is_none());
}

#[test]
fn slot_claim_decision_updates_owner_and_open_slot_as_one_transition() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let old_target = remote_primary(OLD_PRIMARY_ID, "old-shard", 4);
    let replacement = remote_primary(TARGET_ID, "new-shard", 5);
    state.insert_node(old_target);
    state.insert_node(replacement);
    state.add_slot(0).unwrap();
    state.set_slot_migrating(0, OLD_PRIMARY_ID).unwrap();

    let outcome = state
        .apply_slot_claim_decision(SlotClaimDecision {
            slot: 0,
            owner: SlotOwnerUpdate::AssignRemote(NodeId::parse(TARGET_ID.as_bytes()).unwrap()),
            migrating: OpenSlotUpdate::Clear,
            importing: OpenSlotUpdate::Preserve,
        })
        .unwrap();

    assert!(outcome.topology_changed);
    assert!(outcome.open_slots_changed);
    assert!(outcome.stopped_serving);
    assert_eq!(
        state.slot_owner_ref(0).map(|owner| owner.name.as_str()),
        Some(TARGET_ID)
    );
    assert!(!state.myself.slots[0]);
    assert!(state.nodes[TARGET_ID].slots[0]);
    assert!(state.migrating_slot_target(0).is_none());
}
