use crate::engine::distributed::cluster::msg::{self, bytes_to_slots, ClusterMsg};
use crate::engine::distributed::cluster::state::{
    ClusterState, OpenSlotUpdate, PendingReplicationSwitch, SlotClaimDecision, SlotOwnerUpdate,
};
use crate::engine::distributed::cluster::NodeId;

use super::slotclaim_demotion::{
    decide_demotion, DemotionDecision, DemotionFacts, LocalClusterRole,
};
use super::types::ClusterBusManager;

struct SlotClaimContext {
    sender_id: String,
    sender_node_id: NodeId,
    sender_epoch: u64,
    sender_shard: String,
    sender_replicaof: String,
    sender_is_primary: bool,
    sender_total_claimed: usize,
    current_primary_id: Option<String>,
    current_primary_epoch: u64,
    allow_same_shard_epoch_override: bool,
}

#[derive(Default)]
struct SlotClaimProgress {
    topology_changed: bool,
    open_slots_changed: bool,
    migrated_current_primary_slots: usize,
}

struct PlannedSlotClaim {
    decision: SlotClaimDecision,
    migrated_current_primary: bool,
    cleanup_dirty_slot: bool,
}

impl ClusterBusManager {
    pub(super) fn sender_slot_claim_deltas(
        state: &ClusterState,
        msg: &ClusterMsg,
    ) -> Vec<(usize, bool)> {
        let slots = msg.topology_slots();
        if slots.len() != msg::SLOT_BYTES {
            return Vec::new();
        }
        let incoming_is_primary = msg.replicaof.is_empty()
            && (msg.flags & crate::engine::distributed::cluster::node::CLUSTER_NODE_PRIMARY) != 0;
        if !incoming_is_primary {
            return Vec::new();
        }
        let Some(sender) = state.nodes.get(msg.topology_node_id()) else {
            return (0..crate::engine::distributed::cluster::state::CLUSTER_SLOTS)
                .map(|slot| (slot, slots[slot / 8] & (1 << (7 - slot % 8)) != 0))
                .collect();
        };
        if Self::sender_claim_is_stable(state, sender, slots) {
            return Vec::new();
        }
        // Match C `clusterProcessPacket`: a changed primary bitmap enters
        // `clusterUpdateSlotsConfigWith` with the complete sender bitmap.
        // Existing claims must be reasserted together with new slots, or
        // repeated resharding leaves peers with divergent owners. Stable
        // heartbeats return above without scanning any slot.
        (0..crate::engine::distributed::cluster::state::CLUSTER_SLOTS)
            .map(|slot| (slot, slots[slot / 8] & (1 << (7 - slot % 8)) != 0))
            .collect()
    }

    fn sender_claim_is_stable(
        state: &ClusterState,
        sender: &crate::engine::distributed::cluster::node::ClusterNode,
        slots: &[u8],
    ) -> bool {
        if sender.is_replica() || !sender.replicaof.is_empty() || !sender.has_advertised_slots {
            return false;
        }
        sender.slots.iter().enumerate().all(|(slot, &claimed)| {
            let advertised = slots[slot / 8] & (1 << (7 - slot % 8)) != 0;
            claimed == advertised
                && (!advertised
                    || state.slot_claim_withdrawn_by(slot) != Some(sender.name.as_str()))
        })
    }

    /// Apply sender's claimed slots from an UPDATE message against every
    /// claimant (including myself).  Aligns with C clusterProcessUpdateMessage
    /// (cluster_legacy.c:3085-3360): for each slot the sender claims, if the
    /// slot is currently unclaimed or the existing owner has a strictly
    /// smaller configEpoch than the sender, reassign it to the sender.
    /// Side effects:
    ///   - Clears the slot bitmap on the previous owner and updates numslots.
    ///   - Self-demote: when my shard's primary ends up with zero slots and
    ///     they all moved to the UPDATE sender (failover or replica-migration),
    ///     flip role via `state.pending_replication_switch` to start
    ///     replicating to the sender.
    pub(super) fn process_update_slot_claims(
        state: &mut ClusterState,
        msg: &ClusterMsg,
        allow_same_shard_epoch_override: bool,
    ) {
        if !matches!(
            msg.body,
            crate::engine::distributed::cluster::msg::ClusterBusBody::Update(_)
        ) && !msg.replicaof.is_empty()
        {
            return;
        }
        if msg.topology_slots().len() != msg::SLOT_BYTES {
            return;
        }
        let sender_slots = bytes_to_slots(msg.topology_slots());
        let sender_total_claimed = sender_slots.iter().filter(|&&claimed| claimed).count();
        Self::process_slot_claims(
            state,
            msg,
            allow_same_shard_epoch_override,
            sender_slots.into_iter().enumerate(),
            sender_total_claimed,
        );
    }

    /// Reconcile a changed direct PING/PONG bitmap. C skips stable heartbeats
    /// but reconciles the sender's full bitmap when it changes.
    pub(super) fn process_changed_slot_claims(
        state: &mut ClusterState,
        msg: &ClusterMsg,
        allow_same_shard_epoch_override: bool,
        changed_slots: &[(usize, bool)],
    ) {
        if changed_slots.is_empty() {
            return;
        }
        let sender_total_claimed = state
            .nodes
            .get(msg.topology_node_id())
            .map(|node| node.numslots)
            .unwrap_or_default();
        Self::process_slot_claims(
            state,
            msg,
            allow_same_shard_epoch_override,
            changed_slots.iter().copied(),
            sender_total_claimed,
        );
    }

    fn process_slot_claims(
        state: &mut ClusterState,
        msg: &ClusterMsg,
        allow_same_shard_epoch_override: bool,
        claimed_slots: impl IntoIterator<Item = (usize, bool)>,
        sender_total_claimed: usize,
    ) {
        let Some(context) = Self::slot_claim_context(
            state,
            msg,
            sender_total_claimed,
            allow_same_shard_epoch_override,
        ) else {
            return;
        };
        let mut progress = SlotClaimProgress::default();
        for (slot, claimed) in claimed_slots {
            let planned = Self::plan_slot_claim(state, &context, slot, claimed);
            if planned.cleanup_dirty_slot {
                state.request_dirty_slot_cleanup(slot);
            }
            let outcome = state
                .apply_slot_claim_decision(planned.decision)
                .expect("validated Cluster Bus sender must remain known");
            progress.topology_changed |= outcome.topology_changed;
            progress.open_slots_changed |= outcome.open_slots_changed;
            progress.migrated_current_primary_slots += planned.migrated_current_primary as usize;
        }
        Self::finish_slot_claims(state, context, progress);
    }

    fn slot_claim_context(
        state: &ClusterState,
        msg: &ClusterMsg,
        sender_total_claimed: usize,
        allow_same_shard_epoch_override: bool,
    ) -> Option<SlotClaimContext> {
        let sender_id = msg.topology_node_id().to_string();
        if sender_id.is_empty() || sender_id == state.myself.name {
            return None;
        }
        let sender_node_id = NodeId::parse(sender_id.as_bytes()).ok()?;
        let sender = state.nodes.get(&sender_id)?;
        let current_primary_id = if state.myself.is_primary() {
            Some(state.myself.name.clone())
        } else if state.myself.replicaof.is_empty() {
            None
        } else {
            Some(state.myself.replicaof.clone())
        };
        let current_primary_epoch = current_primary_id
            .as_deref()
            .and_then(|id| state.node_config_epoch(id))
            .unwrap_or_default();
        Some(SlotClaimContext {
            sender_id,
            sender_node_id,
            sender_epoch: msg.topology_config_epoch(),
            sender_shard: sender.shard_id.clone(),
            sender_replicaof: sender.replicaof.clone(),
            sender_is_primary: sender.is_primary(),
            sender_total_claimed,
            current_primary_id,
            current_primary_epoch,
            allow_same_shard_epoch_override,
        })
    }

    fn plan_slot_claim(
        state: &ClusterState,
        context: &SlotClaimContext,
        slot: usize,
        claimed: bool,
    ) -> PlannedSlotClaim {
        if claimed {
            Self::plan_present_slot_claim(state, context, slot)
        } else {
            Self::plan_missing_slot_claim(state, context, slot)
        }
    }

    fn plan_missing_slot_claim(
        state: &ClusterState,
        context: &SlotClaimContext,
        slot: usize,
    ) -> PlannedSlotClaim {
        let finalize_import = state.myself.is_primary()
            && state.importing_slot_source(slot) == Some(context.sender_id.as_str());
        let owner = if finalize_import {
            SlotOwnerUpdate::FinalizeImportToMyself
        } else if state
            .declared_node_by_slot(slot)
            .is_some_and(|owner| owner.name == context.sender_id)
            && state.slot_claim_withdrawn_by(slot) != Some(context.sender_id.as_str())
        {
            SlotOwnerUpdate::Withdraw(context.sender_node_id.clone())
        } else {
            SlotOwnerUpdate::Preserve
        };
        PlannedSlotClaim {
            decision: SlotClaimDecision {
                slot,
                owner,
                migrating: if finalize_import {
                    OpenSlotUpdate::Clear
                } else {
                    Self::plan_migrating_retarget(state, context, slot)
                },
                importing: if finalize_import {
                    OpenSlotUpdate::Clear
                } else {
                    OpenSlotUpdate::Preserve
                },
            },
            migrated_current_primary: false,
            cleanup_dirty_slot: false,
        }
    }

    fn plan_migrating_retarget(
        state: &ClusterState,
        context: &SlotClaimContext,
        slot: usize,
    ) -> OpenSlotUpdate {
        if !context.sender_is_primary {
            return OpenSlotUpdate::Preserve;
        }
        let Some(old_target_id) = state.migrating_slot_target(slot) else {
            return OpenSlotUpdate::Preserve;
        };
        if old_target_id == context.sender_id {
            return OpenSlotUpdate::Preserve;
        }
        let Some(old_target) = state.nodes.get(old_target_id) else {
            return OpenSlotUpdate::Preserve;
        };
        if old_target.shard_id != context.sender_shard
            || (old_target.is_primary() && old_target.config_epoch >= context.sender_epoch)
        {
            return OpenSlotUpdate::Preserve;
        }
        OpenSlotUpdate::Retarget(context.sender_node_id.clone())
    }

    fn plan_present_slot_claim(
        state: &ClusterState,
        context: &SlotClaimContext,
        slot: usize,
    ) -> PlannedSlotClaim {
        let delete_epoch = state.slot_delete_epoch(slot);
        if delete_epoch != 0 && context.sender_epoch <= delete_epoch {
            let owner = if state.nodes[&context.sender_id].slots[slot] {
                SlotOwnerUpdate::RejectRemoteClaim(context.sender_node_id.clone())
            } else {
                SlotOwnerUpdate::Preserve
            };
            return Self::preserving_open_slots(slot, owner);
        }
        let current_owner = state.canonical_slot_owner_id(slot).map(str::to_string);
        if !Self::claim_needs_reassignment(state, context, current_owner.as_deref(), slot) {
            return Self::preserving_open_slots(slot, SlotOwnerUpdate::Preserve);
        }
        let sender_already_owner = current_owner.as_deref() == Some(context.sender_id.as_str());
        let migrated_current_primary = current_owner.as_deref()
            == context.current_primary_id.as_deref()
            || (sender_already_owner && context.sender_shard == state.myself.shard_id);
        PlannedSlotClaim {
            decision: SlotClaimDecision {
                slot,
                owner: SlotOwnerUpdate::AssignRemote(context.sender_node_id.clone()),
                migrating: Self::plan_migrating_after_claim(state, context, slot),
                importing: Self::plan_importing_after_claim(state, context, slot),
            },
            migrated_current_primary,
            cleanup_dirty_slot: current_owner.as_deref() == Some(state.myself.name.as_str())
                && context.sender_shard != state.myself.shard_id,
        }
    }

    fn preserving_open_slots(slot: usize, owner: SlotOwnerUpdate) -> PlannedSlotClaim {
        PlannedSlotClaim {
            decision: SlotClaimDecision {
                slot,
                owner,
                migrating: OpenSlotUpdate::Preserve,
                importing: OpenSlotUpdate::Preserve,
            },
            migrated_current_primary: false,
            cleanup_dirty_slot: false,
        }
    }

    fn claim_needs_reassignment(
        state: &ClusterState,
        context: &SlotClaimContext,
        current_owner: Option<&str>,
        slot: usize,
    ) -> bool {
        let Some(owner_id) = current_owner else {
            return true;
        };
        if state.slot_claim_withdrawn_by(slot) == Some(owner_id)
            || owner_id == context.sender_id
            || Self::sender_supersedes_owner(state, context, owner_id)
        {
            return true;
        }
        state
            .node_config_epoch(owner_id)
            .is_some_and(|owner_epoch| owner_epoch < context.sender_epoch)
    }

    fn sender_supersedes_owner(
        state: &ClusterState,
        context: &SlotClaimContext,
        owner_id: &str,
    ) -> bool {
        if !context.allow_same_shard_epoch_override || !context.sender_is_primary {
            return false;
        }
        let (owner_shard, owner_replicaof) = if owner_id == state.myself.name {
            (&state.myself.shard_id, &state.myself.replicaof)
        } else if let Some(owner) = state.nodes.get(owner_id) {
            (&owner.shard_id, &owner.replicaof)
        } else {
            return false;
        };
        owner_id != context.sender_id
            && (context.sender_replicaof == owner_id || owner_replicaof == &context.sender_id)
            && !context.sender_shard.is_empty()
            && context.sender_shard == *owner_shard
    }

    fn plan_migrating_after_claim(
        state: &ClusterState,
        context: &SlotClaimContext,
        _slot: usize,
    ) -> OpenSlotUpdate {
        if context.sender_shard != state.myself.shard_id {
            OpenSlotUpdate::Clear
        } else {
            OpenSlotUpdate::Preserve
        }
    }

    fn plan_importing_after_claim(
        state: &ClusterState,
        context: &SlotClaimContext,
        slot: usize,
    ) -> OpenSlotUpdate {
        let Some(old_source_id) = state.importing_slot_source(slot) else {
            return OpenSlotUpdate::Preserve;
        };
        if old_source_id == context.sender_id {
            return OpenSlotUpdate::Preserve;
        }
        if state
            .nodes
            .get(old_source_id)
            .is_some_and(|old_source| old_source.shard_id == context.sender_shard)
        {
            OpenSlotUpdate::Retarget(context.sender_node_id.clone())
        } else {
            OpenSlotUpdate::Clear
        }
    }

    fn finish_slot_claims(
        state: &mut ClusterState,
        context: SlotClaimContext,
        progress: SlotClaimProgress,
    ) {
        if progress.topology_changed || progress.open_slots_changed {
            state.mark_topology_changed();
        }
        if progress.open_slots_changed {
            state.request_config_fsync();
        }
        state.reconcile_slot_exports(crate::foundation::io::sys::time::unixtime());
        if context.current_primary_id.as_deref() == Some(context.sender_id.as_str()) {
            return;
        }
        Self::apply_post_claim_transition(state, &context, &progress);
    }

    fn decide_post_claim_transition(
        state: &ClusterState,
        context: &SlotClaimContext,
        progress: &SlotClaimProgress,
    ) -> DemotionDecision {
        let current_primary_slots_after = context
            .current_primary_id
            .as_deref()
            .and_then(|id| state.node_slot_count(id))
            .unwrap_or_default();
        let same_shard =
            !context.sender_shard.is_empty() && context.sender_shard == state.myself.shard_id;
        let facts = DemotionFacts {
            local_role: if state.myself.is_primary() {
                LocalClusterRole::Primary
            } else {
                LocalClusterRole::Replica
            },
            current_primary_slots_after,
            local_slots_after: state.myself.numslots,
            sender_slots: context.sender_total_claimed,
            migrated_current_primary_slots: progress.migrated_current_primary_slots,
            same_shard,
            sender_epoch_is_newer: context.sender_epoch > context.current_primary_epoch,
            local_ever_owned_slots: state.has_ever_owned_slots(),
            sender_was_local_replica: context
                .current_primary_id
                .as_deref()
                .is_some_and(|primary_id| context.sender_replicaof == primary_id),
            sender_wins_empty_tie_break: state.myself.replicaof != context.sender_id
                && context.sender_total_claimed == 0
                && state.myself.count_slots() == 0
                && context.sender_epoch > context.current_primary_epoch
                && same_shard,
            allow_replica_migration: state.is_replica_migration_allowed(),
        };
        decide_demotion(facts)
    }

    fn apply_post_claim_transition(
        state: &mut ClusterState,
        context: &SlotClaimContext,
        progress: &SlotClaimProgress,
    ) {
        if !state.nodes[&context.sender_id].is_primary() {
            return;
        }
        let mode = match Self::decide_post_claim_transition(state, context, progress) {
            DemotionDecision::None => return,
            DemotionDecision::BecomeEmptyPrimary => {
                crate::server_log!(
                    notice,
                    "My last slot was migrated to node {}. I am now an empty primary.",
                    context.sender_id
                );
                return;
            }
            DemotionDecision::FollowSender { mode } => mode,
        };
        if state.myself.replicaof == context.sender_id {
            return;
        }
        crate::server_log!(
            notice,
            "[CLUSTER-ROLE] UPDATE self-demote: primary {:.8} reclaimed_by={:.8}@epoch {}. Scheduling coordinated replication switch in shard {} same_shard={}",
            context.current_primary_id.as_ref().unwrap_or(&state.myself.name),
            context.sender_id,
            context.sender_epoch,
            context.sender_shard,
            context.sender_shard == state.myself.shard_id,
        );
        state.set_pending_replication_switch(PendingReplicationSwitch {
            node_id: context.sender_id.clone(),
            mode,
        });
    }
}
