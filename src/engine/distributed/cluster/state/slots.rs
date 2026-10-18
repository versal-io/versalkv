#[cfg(test)]
use super::types::CLUSTER_FAIL;
use super::types::{ClusterState, SlotOwner, CLUSTER_SLOTS};
use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MIGRATE_TO};
use crate::engine::distributed::cluster::NodeId;
use std::collections::HashMap;

const OPEN_SLOTS_AUX_NODE_ID_LEN: usize = 40;

#[derive(Debug)]
struct OpenSlotState {
    slot: usize,
    importing: bool,
    node_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SlotOwnerUpdate {
    Preserve,
    Withdraw(NodeId),
    RejectRemoteClaim(NodeId),
    AssignRemote(NodeId),
    FinalizeImportToMyself,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OpenSlotUpdate {
    Preserve,
    Clear,
    Retarget(NodeId),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SlotClaimDecision {
    pub slot: usize,
    pub owner: SlotOwnerUpdate,
    pub migrating: OpenSlotUpdate,
    pub importing: OpenSlotUpdate,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SlotClaimOutcome {
    pub topology_changed: bool,
    pub open_slots_changed: bool,
    pub stopped_serving: bool,
}

fn apply_open_slot_update(slots: &mut HashMap<usize, String>, slot: usize, update: OpenSlotUpdate) {
    match update {
        OpenSlotUpdate::Preserve => {}
        OpenSlotUpdate::Clear => {
            slots.remove(&slot);
        }
        OpenSlotUpdate::Retarget(node_id) => {
            slots.insert(slot, node_id.to_string());
        }
    }
}

impl ClusterState {
    pub(crate) fn apply_slot_claim_decision(
        &mut self,
        decision: SlotClaimDecision,
    ) -> Result<SlotClaimOutcome, String> {
        let slot = decision.slot;
        let served_before = self.serves_shard_slot(slot);
        let topology_changed = self.apply_slot_owner_update(slot, decision.owner)?;
        let open_slots_changed =
            self.apply_claim_open_updates(slot, decision.migrating, decision.importing);
        let stopped_serving = served_before && !self.serves_shard_slot(slot);
        if stopped_serving {
            self.request_shard_slot_cleanup(slot);
        }
        Ok(SlotClaimOutcome {
            topology_changed,
            open_slots_changed,
            stopped_serving,
        })
    }

    fn apply_claim_open_updates(
        &mut self,
        slot: usize,
        migrating: OpenSlotUpdate,
        importing: OpenSlotUpdate,
    ) -> bool {
        let before = (
            self.slot_ownership.migrating_to.get(&slot).cloned(),
            self.slot_ownership.importing_from.get(&slot).cloned(),
        );
        apply_open_slot_update(&mut self.slot_ownership.migrating_to, slot, migrating);
        apply_open_slot_update(&mut self.slot_ownership.importing_from, slot, importing);
        before
            != (
                self.slot_ownership.migrating_to.get(&slot).cloned(),
                self.slot_ownership.importing_from.get(&slot).cloned(),
            )
    }

    pub(crate) fn slot_delete_epoch(&self, slot: usize) -> u64 {
        self.slot_ownership.delete_epochs[slot]
    }

    pub(crate) fn slot_claim_withdrawn_by(&self, slot: usize) -> Option<&str> {
        self.slot_ownership.withdrawn_by[slot].as_deref()
    }

    pub(crate) fn migrating_slot_target(&self, slot: usize) -> Option<&str> {
        self.slot_ownership
            .migrating_to
            .get(&slot)
            .map(String::as_str)
    }

    pub(crate) fn importing_slot_source(&self, slot: usize) -> Option<&str> {
        self.slot_ownership
            .importing_from
            .get(&slot)
            .map(String::as_str)
    }

    pub(crate) fn has_migrating_slots(&self) -> bool {
        !self.slot_ownership.migrating_to.is_empty()
    }

    pub(crate) fn has_importing_slots(&self) -> bool {
        !self.slot_ownership.importing_from.is_empty()
    }

    pub(crate) fn is_slot_open(&self, slot: usize) -> bool {
        self.migrating_slot_target(slot).is_some() || self.importing_slot_source(slot).is_some()
    }

    pub(crate) fn migrating_slots(&self) -> impl Iterator<Item = (usize, &str)> {
        self.slot_ownership
            .migrating_to
            .iter()
            .map(|(slot, node_id)| (*slot, node_id.as_str()))
    }

    pub(crate) fn importing_slots(&self) -> impl Iterator<Item = (usize, &str)> {
        self.slot_ownership
            .importing_from
            .iter()
            .map(|(slot, node_id)| (*slot, node_id.as_str()))
    }

    pub(crate) fn restore_open_slot_maps(
        &mut self,
        migrating: HashMap<usize, String>,
        importing: HashMap<usize, String>,
    ) {
        self.slot_ownership.migrating_to = migrating;
        self.slot_ownership.importing_from = importing;
    }

    pub(super) fn clear_open_slot_states(&mut self) {
        self.slot_ownership.migrating_to.clear();
        self.slot_ownership.importing_from.clear();
    }

    pub(super) fn unassign_observation_removed_slots(&mut self, node_id: &str, slots: &[usize]) {
        for &slot in slots {
            self.unassign_slot_if_owned_by(slot, node_id);
        }
    }

    pub(crate) fn clear_slot_claim_withdrawn_for(&mut self, slot: usize, node_ids: &[&str]) {
        if self
            .slot_claim_withdrawn_by(slot)
            .is_some_and(|withdrawn| node_ids.contains(&withdrawn))
        {
            self.slot_ownership.withdrawn_by[slot] = None;
        }
    }

    pub(crate) fn retarget_open_slots_after_primary_demotion(
        &mut self,
        old_primary: &str,
        new_primary: &NodeId,
    ) -> (Vec<usize>, Vec<usize>) {
        let new_primary = new_primary.to_string();
        let importing = retarget_open_slot_map(
            &mut self.slot_ownership.importing_from,
            old_primary,
            &new_primary,
        );
        let migrating = retarget_open_slot_map(
            &mut self.slot_ownership.migrating_to,
            old_primary,
            &new_primary,
        );
        (importing, migrating)
    }

    #[cfg(test)]
    pub(crate) fn seed_migrating_slot(&mut self, slot: usize, node_id: &str) {
        self.slot_ownership
            .migrating_to
            .insert(slot, node_id.to_string());
    }

    #[cfg(test)]
    pub(crate) fn seed_importing_slot(&mut self, slot: usize, node_id: &str) {
        self.slot_ownership
            .importing_from
            .insert(slot, node_id.to_string());
    }

    #[cfg(test)]
    pub(crate) fn set_slot_claim_withdrawn(&mut self, slot: usize, node_id: Option<String>) {
        self.slot_ownership.withdrawn_by[slot] = node_id;
    }

    pub(crate) fn node_config_epoch(&self, node_id: &str) -> Option<u64> {
        if node_id == self.myself.name {
            Some(self.myself.config_epoch)
        } else {
            self.nodes.get(node_id).map(|node| node.config_epoch)
        }
    }

    pub(crate) fn node_slot_count(&self, node_id: &str) -> Option<usize> {
        if node_id == self.myself.name {
            Some(self.myself.numslots)
        } else {
            self.nodes.get(node_id).map(|node| node.numslots)
        }
    }

    fn apply_slot_owner_update(
        &mut self,
        slot: usize,
        update: SlotOwnerUpdate,
    ) -> Result<bool, String> {
        match update {
            SlotOwnerUpdate::Preserve => Ok(false),
            SlotOwnerUpdate::Withdraw(node_id) => {
                let changed =
                    self.slot_ownership.withdrawn_by[slot].as_deref() != Some(node_id.as_str());
                self.slot_ownership.withdrawn_by[slot] = Some(node_id.to_string());
                Ok(changed)
            }
            SlotOwnerUpdate::RejectRemoteClaim(node_id) => {
                let changed = self
                    .nodes
                    .get(node_id.as_str())
                    .is_some_and(|node| node.slots[slot]);
                self.set_remote_slot_claim(node_id.as_str(), slot, false);
                Ok(changed)
            }
            SlotOwnerUpdate::AssignRemote(node_id) => {
                let had_withdrawal = self.slot_ownership.withdrawn_by[slot].take().is_some();
                self.assign_slot_to_remote_id(slot, &node_id)
                    .map(|changed| changed || had_withdrawal)
            }
            SlotOwnerUpdate::FinalizeImportToMyself => {
                let myself = self.myself.name.clone();
                self.set_slot_node(slot, &myself).map(|()| true)
            }
        }
    }

    pub(crate) fn canonical_slot_owner_id(&self, slot: usize) -> Option<&str> {
        match self.slot_ownership.owners.get(slot)? {
            SlotOwner::Unassigned => None,
            SlotOwner::Myself => Some(self.myself.name.as_str()),
            SlotOwner::Remote(node_id) => Some(node_id.as_str()),
        }
    }

    pub(crate) fn canonical_slot_owner_ref(&self, slot: usize) -> Option<&ClusterNode> {
        match self.slot_ownership.owners.get(slot)? {
            SlotOwner::Unassigned => None,
            SlotOwner::Myself => Some(&self.myself),
            SlotOwner::Remote(node_id) => self.nodes.get(node_id.as_str()),
        }
    }

    fn clear_owner_bitmap(&mut self, slot: usize, owner: &SlotOwner) {
        match owner {
            SlotOwner::Unassigned => {}
            SlotOwner::Myself => {
                if self.myself.slots[slot] {
                    self.myself.slots[slot] = false;
                    self.myself.numslots = self.myself.numslots.saturating_sub(1);
                }
            }
            SlotOwner::Remote(node_id) => {
                if let Some(node) = self.nodes.get_mut(node_id.as_str()) {
                    if node.slots[slot] {
                        node.slots[slot] = false;
                        node.numslots = node.numslots.saturating_sub(1);
                    }
                }
            }
        }
    }

    fn set_owner_bitmap(&mut self, slot: usize, owner: &SlotOwner) -> Result<(), String> {
        match owner {
            SlotOwner::Unassigned => {}
            SlotOwner::Myself => {
                if !self.myself.slots[slot] {
                    self.myself.slots[slot] = true;
                    self.myself.numslots += 1;
                }
            }
            SlotOwner::Remote(node_id) => {
                let node = self
                    .nodes
                    .get_mut(node_id.as_str())
                    .ok_or_else(|| format!("Unknown node {}", node_id.as_str()))?;
                if !node.slots[slot] {
                    node.slots[slot] = true;
                    node.numslots += 1;
                }
            }
        }
        Ok(())
    }

    pub(super) fn owner_contributes_coverage(&self, owner: &SlotOwner) -> bool {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_FAIL, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_NOADDR,
        };

        match owner {
            SlotOwner::Unassigned => false,
            SlotOwner::Myself => !self.myself.has_flag(CLUSTER_NODE_FAIL),
            SlotOwner::Remote(node_id) => self.nodes.get(node_id.as_str()).is_some_and(|node| {
                !node.has_flag(CLUSTER_NODE_FAIL | CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR)
            }),
        }
    }

    fn replace_slot_owner(&mut self, slot: usize, owner: SlotOwner) -> Result<SlotOwner, String> {
        let previous = self.slot_ownership.owners[slot].clone();
        if previous == owner {
            self.set_owner_bitmap(slot, &owner)?;
            self.slots[slot] = self.owner_contributes_coverage(&owner);
            return Ok(previous);
        }
        self.clear_owner_bitmap(slot, &previous);
        self.set_owner_bitmap(slot, &owner)?;
        self.slot_ownership.owners[slot] = owner;
        self.slots[slot] = self.owner_contributes_coverage(&self.slot_ownership.owners[slot]);
        self.slot_ownership.withdrawn_by[slot] = None;
        self.topology_state_dirty = true;
        #[cfg(debug_assertions)]
        {
            debug_assert!(self.validate_slot_transition(slot).is_ok());
            let previous_node = match &previous {
                SlotOwner::Unassigned => None,
                SlotOwner::Myself => Some(&self.myself),
                SlotOwner::Remote(node_id) => self.nodes.get(node_id.as_str()),
            };
            if let Some(previous_node) = previous_node {
                debug_assert!(!previous_node.slots[slot]);
                if previous_node.numslots == 0 {
                    debug_assert!(!previous_node.slots.iter().any(|owned| *owned));
                }
            }
        }
        Ok(previous)
    }

    pub(crate) fn assign_slot_to_remote(
        &mut self,
        slot: usize,
        node_id: &str,
    ) -> Result<bool, String> {
        let node_id = NodeId::parse(node_id.as_bytes()).map_err(|error| error.to_string())?;
        self.assign_slot_to_remote_id(slot, &node_id)
    }

    pub(crate) fn assign_slot_to_remote_id(
        &mut self,
        slot: usize,
        node_id: &NodeId,
    ) -> Result<bool, String> {
        let next = SlotOwner::Remote(node_id.clone());
        let changed = self.slot_ownership.owners[slot] != next;
        self.replace_slot_owner(slot, next)?;
        Ok(changed)
    }

    pub(crate) fn assign_slot_to_myself(&mut self, slot: usize) -> Result<bool, String> {
        let changed = self.slot_ownership.owners[slot] != SlotOwner::Myself;
        self.replace_slot_owner(slot, SlotOwner::Myself)?;
        Ok(changed)
    }

    pub(crate) fn unassign_slot_if_owned_by(&mut self, slot: usize, node_id: &str) -> bool {
        if self.canonical_slot_owner_id(slot) != Some(node_id) {
            return false;
        }
        self.replace_slot_owner(slot, SlotOwner::Unassigned)
            .expect("unassigning a slot cannot reference an unknown owner");
        true
    }

    pub(crate) fn unassign_slots_owned_by(&mut self, node_id: &str) -> usize {
        let slots = (0..CLUSTER_SLOTS)
            .filter(|&slot| self.canonical_slot_owner_id(slot) == Some(node_id))
            .collect::<Vec<_>>();
        for &slot in &slots {
            self.replace_slot_owner(slot, SlotOwner::Unassigned)
                .expect("unassigning slots cannot reference a new owner");
        }
        slots.len()
    }

    pub(crate) fn rebuild_slot_owners_from_bitmaps(&mut self) -> Result<(), String> {
        let mut owners = vec![SlotOwner::Unassigned; CLUSTER_SLOTS];
        let remote_ids = self
            .nodes
            .keys()
            .map(|node_id| {
                NodeId::parse(node_id.as_bytes())
                    .map(|parsed| (node_id.as_str(), parsed))
                    .map_err(|error| error.to_string())
            })
            .collect::<Result<std::collections::HashMap<_, _>, _>>()?;
        for (slot, owner_entry) in owners.iter_mut().enumerate() {
            let mut best: Option<&ClusterNode> = self.myself.slots[slot].then_some(&self.myself);
            for node in self.nodes.values().filter(|node| node.slots[slot]) {
                if let Some(existing) = best {
                    return Err(format!(
                        "slot {slot} is claimed by both {} and {}",
                        existing.name, node.name
                    ));
                }
                best = Some(node);
            }
            *owner_entry = match best {
                None => SlotOwner::Unassigned,
                Some(node) if node.is_myself() => SlotOwner::Myself,
                Some(node) => SlotOwner::Remote(
                    remote_ids
                        .get(node.name.as_str())
                        .expect("restored node ID was validated")
                        .clone(),
                ),
            };
        }
        self.slot_ownership.owners = owners;
        Ok(())
    }

    pub(crate) fn index_inserted_node_slots(&mut self, node_id: &str) {
        let Ok(owner_id) = NodeId::parse(node_id.as_bytes()) else {
            return;
        };
        let Some(node) = self.nodes.get(node_id) else {
            return;
        };
        let claimed = node.slots.clone();
        let is_available = !node.has_flag(
            crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL
                | crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE
                | crate::engine::distributed::cluster::node::CLUSTER_NODE_NOADDR,
        );
        let mut changed = false;
        for (slot, claimed) in claimed.into_iter().enumerate() {
            if claimed && self.slot_ownership.owners[slot] == SlotOwner::Unassigned {
                self.slot_ownership.owners[slot] = SlotOwner::Remote(owner_id.clone());
                self.slots[slot] = is_available;
                changed = true;
            }
        }
        self.topology_state_dirty |= changed;
    }

    /// Resolve active exports immediately after a topology ownership update.
    /// The source remains active while it owns every requested slot, succeeds
    /// once the target owns all of them, and fails on any other ownership.
    pub fn reconcile_slot_exports(&mut self, now: i64) {
        use crate::engine::distributed::cluster::migration::{MigrationOperation, TerminalPhase};

        let resolutions = self
            .migrations
            .views()
            .into_iter()
            .filter(|job| !job.phase.is_terminal() && job.operation == MigrationOperation::Export)
            .filter_map(|job| {
                let mut all_source = true;
                let mut all_target = true;
                for range in job.ranges.iter() {
                    for slot in range.start..=range.end {
                        let owner = self
                            .declared_node_by_slot(slot as usize)
                            .map(|node| node.name.as_str());
                        all_source &= owner == Some(job.source.as_str());
                        all_target &= owner == Some(job.target.as_str());
                    }
                }
                if all_source {
                    None
                } else if all_target {
                    Some((job.id, TerminalPhase::Success, None))
                } else {
                    Some((
                        job.id,
                        TerminalPhase::Failed,
                        Some("Slots are no longer owned by myself".to_string()),
                    ))
                }
            })
            .collect::<Vec<_>>();
        for (id, terminal, message) in resolutions {
            if let Ok(actions) = self.finish_migration(&id, terminal, message, now) {
                self.queue_migration_actions(actions);
            }
        }
    }

    pub fn encode_open_slots_aux_field(&self) -> Option<Vec<u8>> {
        let mut importing = self
            .slot_ownership
            .importing_from
            .iter()
            .collect::<Vec<_>>();
        let mut migrating = self.slot_ownership.migrating_to.iter().collect::<Vec<_>>();
        importing.sort_unstable_by_key(|(slot, _)| **slot);
        migrating.sort_unstable_by_key(|(slot, _)| **slot);
        if importing.is_empty() && migrating.is_empty() {
            return None;
        }
        let mut encoded = Vec::new();
        for (states, marker) in [(importing, b'<'), (migrating, b'>')] {
            for (slot, node_id) in states {
                encoded.extend_from_slice(slot.to_string().as_bytes());
                encoded.push(marker);
                encoded.extend_from_slice(node_id.as_bytes());
                encoded.push(b',');
            }
        }
        Some(encoded)
    }

    pub fn apply_open_slots_aux_field(&mut self, encoded: &[u8]) -> Result<(), String> {
        let states = parse_open_slot_states(encoded)?;
        let changed = !self.slot_ownership.importing_from.is_empty()
            || !self.slot_ownership.migrating_to.is_empty()
            || !states.is_empty();
        self.slot_ownership.importing_from.clear();
        self.slot_ownership.migrating_to.clear();
        for state in states {
            self.ensure_open_slot_node(&state.node_id);
            let slots = if state.importing {
                &mut self.slot_ownership.importing_from
            } else {
                &mut self.slot_ownership.migrating_to
            };
            slots.insert(state.slot, state.node_id);
        }
        if changed {
            self.mark_topology_changed();
        }
        Ok(())
    }

    pub fn validate_open_slots_aux_field(encoded: &[u8]) -> Result<(), String> {
        parse_open_slot_states(encoded).map(|_| ())
    }

    fn ensure_open_slot_node(&mut self, node_id: &str) {
        if node_id == self.myself.name || self.nodes.contains_key(node_id) {
            return;
        }
        let mut node = ClusterNode::new(node_id.to_string(), String::new(), 0, 0, true);
        node.flags = 0;
        self.nodes.insert(node_id.to_string(), node);
    }

    fn primaries_have_replicas(&self) -> bool {
        self.myself.is_replica()
            || self.nodes.values().any(|node| {
                node.is_replica()
                    && !node.replicaof.is_empty()
                    && (node.replicaof == self.myself.name
                        || self.nodes.contains_key(&node.replicaof))
            })
    }

    /// Add a slot to myself.
    pub fn add_slot(&mut self, slot: usize) -> Result<(), String> {
        if slot >= CLUSTER_SLOTS {
            return Err("Slot out of range".to_string());
        }
        if self.slot_ownership.owners[slot] != SlotOwner::Unassigned
            && self.slot_ownership.owners[slot] != SlotOwner::Myself
        {
            return Err("Slot is already assigned".to_string());
        }
        if self.slot_ownership.owners[slot] == SlotOwner::Unassigned {
            let is_first_slot = self.myself.numslots == 0;
            self.replace_slot_owner(slot, SlotOwner::Myself)?;
            if is_first_slot && self.primaries_have_replicas() {
                self.myself.flags |= CLUSTER_NODE_MIGRATE_TO;
            }
            self.mark_slot_owned();
            self.mark_topology_changed();
        }
        debug_assert!(self.myself.slots[slot] && self.slots[slot]);
        debug_assert_eq!(self.myself.numslots, self.myself.count_slots());
        Ok(())
    }

    /// Apply a validated ADDSLOTS batch as one topology transition, matching
    /// C `clusterUpdateSlots`: individual bindings update local ownership,
    /// while persistence/state work is scheduled once after the command.
    pub fn add_slots_batch(&mut self, slots: &[usize]) -> Result<(), String> {
        for &slot in slots {
            if slot >= CLUSTER_SLOTS {
                return Err("Slot out of range".to_string());
            }
            if self.slot_ownership.owners[slot] != SlotOwner::Unassigned
                && self.slot_ownership.owners[slot] != SlotOwner::Myself
            {
                return Err("Slot is already assigned".to_string());
            }
        }
        let mut changed = false;
        for &slot in slots {
            if self.slot_ownership.owners[slot] == SlotOwner::Myself {
                continue;
            }
            let is_first_slot = self.myself.numslots == 0;
            self.replace_slot_owner(slot, SlotOwner::Myself)?;
            if is_first_slot && self.primaries_have_replicas() {
                self.myself.flags |= CLUSTER_NODE_MIGRATE_TO;
            }
            self.mark_slot_owned();
            changed = true;
        }
        if changed {
            self.mark_topology_changed();
        }
        Ok(())
    }

    /// Remove a slot from the cluster.
    /// Mirrors C `clusterDelSlot`: clear the single canonical owner and its
    /// compatibility bitmap.
    pub fn del_slot(&mut self, slot: usize) -> Result<(), String> {
        if slot >= CLUSTER_SLOTS {
            return Err("Slot out of range".to_string());
        }
        let served_before = self.serves_shard_slot(slot);
        if self.slot_ownership.owners[slot] == SlotOwner::Unassigned {
            return Err("Slot is already unassigned".to_string());
        }
        self.replace_slot_owner(slot, SlotOwner::Unassigned)?;
        self.bump_epoch();
        self.slot_ownership.delete_epochs[slot] = self.myself.config_epoch;
        self.slot_ownership.withdrawn_by[slot] = None;
        self.request_topology_broadcast();
        self.mark_topology_changed();
        if served_before && !self.serves_shard_slot(slot) {
            self.request_shard_slot_cleanup(slot);
        }
        debug_assert!(!self.myself.slots[slot]);
        Ok(())
    }

    /// Set a slot as migrating to the specified node.
    pub fn set_slot_migrating(&mut self, slot: usize, node_id: &str) -> Result<(), String> {
        self.set_slot_migrating_inner(slot, node_id, false)
    }

    pub fn apply_replicated_slot_migrating(
        &mut self,
        slot: usize,
        node_id: &str,
    ) -> Result<(), String> {
        self.set_slot_migrating_inner(slot, node_id, true)
    }

    fn set_slot_migrating_inner(
        &mut self,
        slot: usize,
        node_id: &str,
        is_replication: bool,
    ) -> Result<(), String> {
        if slot >= CLUSTER_SLOTS {
            return Err("Slot out of range".to_string());
        }
        if self.myself.is_replica() && !is_replication {
            return Err("Please use SETSLOT only with masters.".to_string());
        }
        if !is_replication && !self.myself.slots[slot] {
            return Err(format!("I'm not the owner of hash slot {}", slot));
        }
        if node_id == self.myself.name {
            return Err("Target node is myself".to_string());
        }
        let node = self
            .nodes
            .get(node_id)
            .ok_or_else(|| format!("I don't know about node {}", node_id))?;
        if node.is_replica() {
            return Err("Target node is not a master".to_string());
        }
        if self
            .slot_ownership
            .migrating_to
            .insert(slot, node_id.to_string())
            .as_deref()
            != Some(node_id)
        {
            self.mark_topology_changed();
        }
        debug_assert!(self.assert_fast_invariants().is_ok());
        Ok(())
    }

    /// Set a slot as importing from the specified node.
    pub fn set_slot_importing(&mut self, slot: usize, node_id: &str) -> Result<(), String> {
        self.set_slot_importing_inner(slot, node_id, false)
    }

    pub fn apply_replicated_slot_importing(
        &mut self,
        slot: usize,
        node_id: &str,
    ) -> Result<(), String> {
        self.set_slot_importing_inner(slot, node_id, true)
    }

    fn set_slot_importing_inner(
        &mut self,
        slot: usize,
        node_id: &str,
        is_replication: bool,
    ) -> Result<(), String> {
        if slot >= CLUSTER_SLOTS {
            return Err("Slot out of range".to_string());
        }
        if self.myself.is_replica() && !is_replication {
            return Err("Please use SETSLOT only with masters.".to_string());
        }
        if self.myself.slots[slot] {
            return Err(format!("I'm already the owner of hash slot {}", slot));
        }
        if node_id == self.myself.name {
            return Err("Target node is myself".to_string());
        }
        let node = self
            .nodes
            .get(node_id)
            .ok_or_else(|| format!("I don't know about node {}", node_id))?;
        if node.is_replica() {
            return Err("Target node is not a master".to_string());
        }
        if self
            .slot_ownership
            .importing_from
            .insert(slot, node_id.to_string())
            .as_deref()
            != Some(node_id)
        {
            self.mark_topology_changed();
        }
        debug_assert!(self.assert_fast_invariants().is_ok());
        Ok(())
    }

    /// Set a slot as stable (clear migrating/importing state).
    pub fn set_slot_stable(&mut self, slot: usize) -> Result<(), String> {
        self.set_slot_stable_inner(slot, false)
    }

    pub fn apply_replicated_slot_stable(&mut self, slot: usize) -> Result<(), String> {
        self.set_slot_stable_inner(slot, true)
    }

    fn set_slot_stable_inner(&mut self, slot: usize, is_replication: bool) -> Result<(), String> {
        if slot >= CLUSTER_SLOTS {
            return Err("Slot out of range".to_string());
        }
        if self.myself.is_replica() && !is_replication {
            return Err("Please use SETSLOT only with masters.".to_string());
        }
        let changed = self.slot_ownership.migrating_to.remove(&slot).is_some()
            | self.slot_ownership.importing_from.remove(&slot).is_some();
        if changed {
            self.mark_topology_changed();
        }
        debug_assert!(self.assert_fast_invariants().is_ok());
        Ok(())
    }

    /// Assign a slot to the specified node.
    pub fn set_slot_node(&mut self, slot: usize, node_id: &str) -> Result<(), String> {
        self.set_slot_node_inner(slot, node_id, false)
    }

    pub fn apply_replicated_slot_node(&mut self, slot: usize, node_id: &str) -> Result<(), String> {
        self.set_slot_node_inner(slot, node_id, true)
    }

    fn set_slot_node_inner(
        &mut self,
        slot: usize,
        node_id: &str,
        is_replication: bool,
    ) -> Result<(), String> {
        self.validate_slot_node_request(slot, node_id, is_replication)?;
        let served_before = self.serves_shard_slot(slot);
        let is_myself = node_id == self.myself.name;
        let is_finalizing_import =
            is_myself && self.slot_ownership.importing_from.contains_key(&slot);
        let mark_migration_target = self.primaries_have_replicas();
        self.assign_setslot_owner(slot, node_id, is_myself, mark_migration_target)?;
        self.slot_ownership.migrating_to.remove(&slot);
        self.slot_ownership.importing_from.remove(&slot);
        self.slot_ownership.withdrawn_by[slot] = None;
        self.finish_setslot_transition(is_finalizing_import)?;
        if served_before && !self.serves_shard_slot(slot) {
            self.request_shard_slot_cleanup(slot);
        }
        Ok(())
    }

    fn validate_slot_node_request(
        &self,
        slot: usize,
        node_id: &str,
        is_replication: bool,
    ) -> Result<(), String> {
        if slot >= CLUSTER_SLOTS {
            return Err("Slot out of range".to_string());
        }
        if self.myself.is_replica() && !is_replication {
            return Err("Please use SETSLOT only with masters.".to_string());
        }
        if node_id == self.myself.name {
            return Ok(());
        }
        let node = self
            .nodes
            .get(node_id)
            .ok_or_else(|| format!("Unknown node {}", node_id))?;
        if node.is_replica() {
            return Err("Target node is not a master".to_string());
        }
        Ok(())
    }

    fn assign_setslot_owner(
        &mut self,
        slot: usize,
        node_id: &str,
        is_myself: bool,
        mark_migration_target: bool,
    ) -> Result<(), String> {
        if is_myself {
            let is_first_slot = self.myself.numslots == 0;
            self.replace_slot_owner(slot, SlotOwner::Myself)?;
            if is_first_slot && mark_migration_target {
                self.myself.flags |= CLUSTER_NODE_MIGRATE_TO;
            }
            self.mark_slot_owned();
        } else {
            let is_first_slot = self.nodes[node_id].numslots == 0;
            self.assign_slot_to_remote(slot, node_id)?;
            if is_first_slot && mark_migration_target {
                self.nodes
                    .get_mut(node_id)
                    .expect("remote slot owner was validated above")
                    .flags |= CLUSTER_NODE_MIGRATE_TO;
            }
        }
        Ok(())
    }

    fn finish_setslot_transition(&mut self, is_finalizing_import: bool) -> Result<(), String> {
        if is_finalizing_import {
            // C `clusterCommandSetSlot` always schedules BROADCAST_ALL after
            // an IMPORTING -> NODE finalization. The epoch bump is only needed
            // for the first imported slot, but every later owner transition
            // must still be advertised promptly. Rust's bus bitmap is cached
            // by topology generation, so a no-op epoch bump still has to mark
            // the slot mutation before broadcasting it.
            if !self.bump_config_epoch_without_consensus()? {
                self.mark_topology_changed();
            }
            self.request_topology_broadcast();
        } else {
            self.mark_topology_changed();
        }
        Ok(())
    }

    /// Finalize slot changes (call after bulk add/del to recompute state).
    pub fn finalize_slot_changes(&mut self) {
        self.recompute_state();
        self.debug_assert_invariants();
    }
}

fn retarget_open_slot_map(
    slots: &mut HashMap<usize, String>,
    old_primary: &str,
    new_primary: &str,
) -> Vec<usize> {
    let mut changed = Vec::new();
    for (slot, node_id) in slots.iter_mut() {
        if node_id == old_primary {
            *node_id = new_primary.to_string();
            changed.push(*slot);
        }
    }
    changed
}

fn parse_open_slot_states(mut encoded: &[u8]) -> Result<Vec<OpenSlotState>, String> {
    let mut states = Vec::new();
    while !encoded.is_empty() {
        let marker = encoded
            .iter()
            .position(|byte| matches!(byte, b'<' | b'>'))
            .ok_or_else(|| "Invalid cluster slot states AUX field".to_string())?;
        let slot = std::str::from_utf8(&encoded[..marker])
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|slot| *slot < CLUSTER_SLOTS)
            .ok_or_else(|| "Invalid cluster slot in AUX field".to_string())?;
        let importing = encoded[marker] == b'<';
        let node_start = marker + 1;
        let node_end = node_start + OPEN_SLOTS_AUX_NODE_ID_LEN;
        let node_bytes = encoded
            .get(node_start..node_end)
            .ok_or_else(|| "Invalid cluster node ID in AUX field".to_string())?;
        let node_id = NodeId::parse(node_bytes)
            .map_err(|_| "Invalid cluster node ID in AUX field".to_string())?
            .to_string();
        if encoded.get(node_end) != Some(&b',') {
            return Err("Invalid cluster slot states AUX delimiter".to_string());
        }
        states.push(OpenSlotState {
            slot,
            importing,
            node_id,
        });
        encoded = &encoded[node_end + 1..];
    }
    Ok(states)
}

#[cfg(test)]
mod tests {
    use super::ClusterState;
    use crate::engine::distributed::cluster::node::{
        ClusterNode, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_MIGRATE_TO, CLUSTER_NODE_MYSELF,
        CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
    };
    use crate::engine::distributed::cluster::state::{CLUSTER_OK, CLUSTER_SLOTS};
    use crate::engine::distributed::cluster::{NodeId, SlotRange};

    const SOURCE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const TARGET: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    #[test]
    fn open_slot_aux_round_trip_replaces_existing_state() {
        let mut source = ClusterState::new("127.0.0.1", 6379, 16_379);
        source
            .slot_ownership
            .importing_from
            .insert(13, SOURCE.to_string());
        source
            .slot_ownership
            .migrating_to
            .insert(7, TARGET.to_string());

        let encoded = source.encode_open_slots_aux_field().unwrap();
        assert_eq!(
            encoded,
            b"13<aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,7>bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,"
        );

        let mut restored = ClusterState::new("127.0.0.1", 6380, 16_380);
        restored
            .slot_ownership
            .migrating_to
            .insert(99, SOURCE.to_string());
        restored.apply_open_slots_aux_field(&encoded).unwrap();

        assert_eq!(
            restored
                .slot_ownership
                .importing_from
                .get(&13)
                .map(String::as_str),
            Some(SOURCE)
        );
        assert_eq!(
            restored
                .slot_ownership
                .migrating_to
                .get(&7)
                .map(String::as_str),
            Some(TARGET)
        );
        assert!(!restored.slot_ownership.migrating_to.contains_key(&99));
        assert!(restored.nodes.contains_key(SOURCE));
        assert!(restored.nodes.contains_key(TARGET));
    }

    #[test]
    fn malformed_open_slot_aux_does_not_replace_existing_state() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state
            .slot_ownership
            .migrating_to
            .insert(7, TARGET.to_string());

        assert!(state.apply_open_slots_aux_field(b"8>short,").is_err());
        assert_eq!(
            state
                .slot_ownership
                .migrating_to
                .get(&7)
                .map(String::as_str),
            Some(TARGET)
        );
    }

    #[test]
    fn first_slot_marks_primary_as_migration_target_when_cluster_has_replicas() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.myself.flags &= !CLUSTER_NODE_REPLICA;
        state.myself.flags |= CLUSTER_NODE_PRIMARY;
        let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let replica_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mut primary = ClusterNode::new(
            primary_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        primary.flags &= !CLUSTER_NODE_MYSELF;
        let mut replica = ClusterNode::new(
            replica_id.to_string(),
            "127.0.0.1".to_string(),
            6381,
            16_381,
            false,
        );
        replica.flags &= !CLUSTER_NODE_MYSELF;
        replica.replicaof = primary_id.to_string();
        state.nodes.insert(primary_id.to_string(), primary);
        state.nodes.insert(replica_id.to_string(), replica);

        state.add_slot(0).unwrap();

        assert!(state.myself.has_flag(CLUSTER_NODE_MIGRATE_TO));
    }

    #[test]
    fn setslot_node_marks_recovered_empty_primary_as_migration_target() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let old_owner_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let replica_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mut old_owner = ClusterNode::new(
            old_owner_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        old_owner.flags &= !CLUSTER_NODE_MYSELF;
        old_owner.slots[0] = true;
        old_owner.numslots = 1;
        let mut replica = ClusterNode::new(
            replica_id.to_string(),
            "127.0.0.1".to_string(),
            6381,
            16_381,
            false,
        );
        replica.flags &= !CLUSTER_NODE_MYSELF;
        replica.replicaof = old_owner_id.to_string();
        state.nodes.insert(old_owner_id.to_string(), old_owner);
        state.nodes.insert(replica_id.to_string(), replica);

        state
            .apply_replicated_slot_node(0, &state.myself.name.clone())
            .unwrap();

        assert!(state.myself.has_flag(CLUSTER_NODE_MIGRATE_TO));
    }

    #[test]
    fn assigning_slot_to_remote_node_updates_remote_slot_view() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.add_slot(0).unwrap();
        let target_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut target = ClusterNode::new(
            target_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        target.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(target.name.clone(), target);

        state.set_slot_node(0, target_id).unwrap();

        assert!(!state.myself.slots[0]);
        assert!(state.nodes[target_id].slots[0]);
        assert_eq!(state.nodes[target_id].numslots, 1);
    }

    #[test]
    fn handshake_owner_does_not_contribute_slot_coverage() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let mut handshake = ClusterNode::new(
            SOURCE.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        handshake.flags &= !CLUSTER_NODE_MYSELF;
        handshake.flags |= CLUSTER_NODE_HANDSHAKE;
        handshake.slots[7] = true;
        handshake.numslots = 1;

        state.insert_node(handshake);

        assert_eq!(state.canonical_slot_owner_id(7), Some(SOURCE));
        assert!(!state.slots[7]);
    }

    #[test]
    fn canonical_owner_tracks_single_slot_reassignments_without_recompute() {
        const FIRST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        const SECOND: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        for (node_id, port) in [(FIRST, 6380), (SECOND, 6381)] {
            let mut node = ClusterNode::new(
                node_id.to_string(),
                "127.0.0.1".to_string(),
                port,
                port + 10_000,
                true,
            );
            node.flags &= !CLUSTER_NODE_MYSELF;
            state.insert_node(node);
        }
        let local_id = state.myself.name.clone();

        assert_eq!(state.canonical_slot_owner_id(7), None);
        state.add_slot(7).unwrap();
        assert_eq!(state.canonical_slot_owner_id(7), Some(local_id.as_str()));

        state.set_slot_node(7, FIRST).unwrap();
        assert_eq!(state.canonical_slot_owner_id(7), Some(FIRST));
        assert_eq!(state.myself.numslots, 0);
        assert_eq!(state.nodes[FIRST].numslots, 1);

        state.set_slot_node(7, SECOND).unwrap();
        assert_eq!(state.canonical_slot_owner_id(7), Some(SECOND));
        assert_eq!(state.nodes[FIRST].numslots, 0);
        assert_eq!(state.nodes[SECOND].numslots, 1);

        state.set_slot_node(7, &local_id).unwrap();
        assert_eq!(state.canonical_slot_owner_id(7), Some(local_id.as_str()));
        assert_eq!(state.myself.numslots, 1);
        assert_eq!(state.nodes[SECOND].numslots, 0);
    }

    #[test]
    fn slot_mutations_defer_cluster_health_recomputation() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.set_require_full_coverage(false);
        state.recompute_state_if_dirty();
        assert_eq!(state.state, CLUSTER_OK);
        assert!(!state.topology_state_is_dirty());

        state.add_slot(7).unwrap();

        assert_eq!(state.state, CLUSTER_OK);
        assert!(state.topology_state_is_dirty());
        state.recompute_state_if_dirty();
        assert!(!state.topology_state_is_dirty());
        assert_eq!(state.state, CLUSTER_OK);
    }

    #[test]
    fn finalizing_import_updates_the_canonical_owner_immediately() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let mut source = ClusterNode::new(
            SOURCE.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        source.flags &= !CLUSTER_NODE_MYSELF;
        state.insert_node(source);
        state.set_slot_node(7, SOURCE).unwrap();
        state.set_slot_importing(7, SOURCE).unwrap();
        let local_id = state.myself.name.clone();

        state.set_slot_node(7, &local_id).unwrap();

        assert_eq!(state.canonical_slot_owner_id(7), Some(local_id.as_str()));
        assert!(!state.slot_ownership.importing_from.contains_key(&7));
    }

    #[test]
    fn canonical_owner_retains_pfail_and_fail_bindings_while_coverage_tracks_health() {
        use crate::engine::distributed::cluster::node::{CLUSTER_NODE_FAIL, CLUSTER_NODE_PFAIL};

        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let mut owner = ClusterNode::new(
            SOURCE.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        owner.flags &= !CLUSTER_NODE_MYSELF;
        owner.slots[7] = true;
        owner.numslots = 1;
        state.insert_node(owner);

        state.nodes.get_mut(SOURCE).unwrap().flags |= CLUSTER_NODE_PFAIL;
        state.mark_topology_changed();
        state.recompute_state_if_dirty();
        assert_eq!(state.canonical_slot_owner_id(7), Some(SOURCE));
        assert!(state.slots[7]);

        let owner = state.nodes.get_mut(SOURCE).unwrap();
        owner.flags &= !CLUSTER_NODE_PFAIL;
        owner.flags |= CLUSTER_NODE_FAIL;
        state.mark_topology_changed();
        state.recompute_state_if_dirty();
        assert_eq!(state.canonical_slot_owner_id(7), Some(SOURCE));
        assert!(!state.slots[7]);
    }

    #[test]
    fn setslot_node_defers_cluster_state_recomputation() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let target_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut target = ClusterNode::new(
            target_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        target.flags &= !CLUSTER_NODE_MYSELF;
        target.slots[1] = true;
        target.numslots = 1;
        state.nodes.insert(target_id.to_string(), target);
        for slot in 0..CLUSTER_SLOTS {
            state.myself.slots[slot] = true;
        }
        state.myself.numslots = CLUSTER_SLOTS;
        state.recompute_state();
        state.state = super::CLUSTER_FAIL;

        state.set_slot_node(0, target_id).unwrap();

        assert_eq!(state.state, super::CLUSTER_FAIL);
        assert!(state.topology_state_is_dirty());
        state.recompute_state_if_dirty();
        assert_eq!(state.state, CLUSTER_OK);
    }

    #[test]
    fn finalizing_import_advances_topology_generation_when_epoch_already_leads() {
        const SOURCE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.current_epoch = 2;
        state.myself.config_epoch = 2;
        let mut source = ClusterNode::new(
            SOURCE.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        source.flags &= !CLUSTER_NODE_MYSELF;
        source.config_epoch = 1;
        source.slots[0] = true;
        source.numslots = 1;
        state.nodes.insert(source.name.clone(), source);
        state
            .slot_ownership
            .importing_from
            .insert(0, SOURCE.to_string());
        let generation = state.topology_generation();
        let local_id = state.myself.name.clone();

        state.set_slot_node(0, &local_id).unwrap();

        assert!(state.myself.slots[0]);
        assert!(state.topology_generation() > generation);
        assert!(state.take_topology_broadcast_request());
    }

    #[test]
    fn deleting_a_slot_only_clears_the_canonical_owner() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.myself.config_epoch = 1;
        state.add_slot(0).unwrap();
        let mut peer = ClusterNode::new(
            "peer".to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        peer.flags &= !CLUSTER_NODE_MYSELF;
        peer.slots[0] = true;
        peer.numslots = 1;
        state.nodes.insert(peer.name.clone(), peer);

        state.del_slot(0).unwrap();

        assert!(!state.myself.slots[0]);
        assert!(state.nodes["peer"].slots[0]);
        assert_eq!(state.nodes["peer"].numslots, 1);
    }

    #[test]
    fn finalizing_imported_slot_bumps_target_config_epoch() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.current_epoch = 4;
        state.myself.config_epoch = 1;
        let source_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut source = ClusterNode::new(
            source_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        source.flags &= !CLUSTER_NODE_MYSELF;
        source.config_epoch = 4;
        state.nodes.insert(source.name.clone(), source);
        state.set_slot_importing(0, source_id).unwrap();
        let myself_id = state.myself.name.clone();

        state.set_slot_node(0, &myself_id).unwrap();

        assert_eq!(state.current_epoch, 5);
        assert_eq!(state.myself.config_epoch, 5);
    }

    #[test]
    fn finalizing_more_imported_slots_reuses_the_already_dominant_epoch() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.current_epoch = 4;
        state.myself.config_epoch = 1;
        let source_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut source = ClusterNode::new(
            source_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        source.flags &= !CLUSTER_NODE_MYSELF;
        source.config_epoch = 4;
        state.nodes.insert(source.name.clone(), source);
        let myself_id = state.myself.name.clone();

        state.set_slot_importing(0, source_id).unwrap();
        state.set_slot_node(0, &myself_id).unwrap();
        let generation_after_first_slot = state.topology_generation();
        assert!(state.take_topology_broadcast_request());
        state.set_slot_importing(1, source_id).unwrap();
        state.set_slot_node(1, &myself_id).unwrap();

        assert_eq!(state.current_epoch, 5);
        assert_eq!(state.myself.config_epoch, 5);
        assert!(state.topology_generation() > generation_after_first_slot);
        assert!(
            state.take_topology_broadcast_request(),
            "each imported slot finalization must request prompt owner propagation"
        );
    }

    #[test]
    fn ownership_change_finishes_export_immediately_when_target_owns_all_slots() {
        use crate::engine::distributed::cluster::migration::{
            ExportPhase, MigrationAction, MigrationJob, MigrationJobId, MigrationOperation,
            MigrationPhase, SlotRanges, TerminalPhase,
        };

        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.myself.name = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
        state.add_slot(1).unwrap();
        let mut target = ClusterNode::new(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        target.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(target.name.clone(), target);
        let id = MigrationJobId::parse(b"cccccccccccccccccccccccccccccccccccccccc").unwrap();
        let mut job = MigrationJob::new(
            id.clone(),
            NodeId::parse(state.myself.name.as_bytes()).unwrap(),
            NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
            SlotRanges::new(vec![SlotRange::new(1, 1).unwrap()]).unwrap(),
            MigrationOperation::Export,
            1,
        );
        job.phase = MigrationPhase::Export(ExportPhase::FailoverGranted);
        state.migrations.start(job).unwrap();
        state
            .apply_replicated_slot_node(1, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
            .unwrap();

        state.reconcile_slot_exports(2);

        assert_eq!(
            state.migrations.get(&id).unwrap().phase,
            MigrationPhase::Terminal(TerminalPhase::Success)
        );
        assert_eq!(
            state.take_migration_actions(),
            vec![MigrationAction::Cleanup(id)]
        );
    }
}
