use super::{ClusterState, CLUSTER_SLOTS};

impl ClusterState {
    pub fn serves_shard_slot(&self, slot: usize) -> bool {
        if slot >= CLUSTER_SLOTS {
            return false;
        }
        if self.myself.is_primary() {
            return self.myself.slots[slot]
                && self.slot_ownership.withdrawn_by[slot].as_deref()
                    != Some(self.myself.name.as_str());
        }
        self.nodes
            .get(&self.myself.replicaof)
            .is_some_and(|primary| {
                primary.slots[slot]
                    && self.slot_ownership.withdrawn_by[slot].as_deref()
                        != Some(primary.name.as_str())
            })
    }

    pub(crate) fn served_shard_slots(&self) -> Vec<usize> {
        (0..CLUSTER_SLOTS)
            .filter(|slot| self.serves_shard_slot(*slot))
            .collect()
    }

    pub(crate) fn request_lost_shard_slots(&mut self, previously_served: &[usize]) {
        for &slot in previously_served {
            if !self.serves_shard_slot(slot) {
                self.request_shard_slot_cleanup(slot);
            }
        }
    }

    pub(crate) fn request_shard_slot_cleanup(&mut self, slot: usize) {
        let Ok(slot) = u16::try_from(slot) else {
            return;
        };
        self.pending_shard_slot_cleanup.insert(slot);
    }

    pub(crate) fn take_shard_slot_cleanup_requests(&mut self) -> Vec<u16> {
        std::mem::take(&mut self.pending_shard_slot_cleanup)
            .into_iter()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::distributed::cluster::node::{
        key_hash_slot, ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
    };

    #[test]
    fn service_follows_primary_ownership_and_replica_master() {
        let slot = key_hash_slot(b"shard-channel") as usize;
        let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
        state.add_slot(slot).unwrap();
        assert!(state.serves_shard_slot(slot));

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
        state.nodes.insert(primary_id.clone(), primary);
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA;
        state.myself.replicaof = primary_id.clone();
        state.myself.slots[slot] = false;
        assert!(state.serves_shard_slot(slot));

        state.nodes.get_mut(&primary_id).unwrap().slots[slot] = false;
        assert!(!state.serves_shard_slot(slot));
    }

    #[test]
    fn losing_service_queues_each_changed_slot_once() {
        let first = key_hash_slot(b"ch3") as usize;
        let second = key_hash_slot(b"ch7") as usize;
        let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
        state.add_slot(first).unwrap();
        state.add_slot(second).unwrap();
        assert!(state.take_shard_slot_cleanup_requests().is_empty());

        state.del_slot(first).unwrap();
        state.request_shard_slot_cleanup(first);

        assert_eq!(state.take_shard_slot_cleanup_requests(), vec![first as u16]);
        assert!(state.serves_shard_slot(second));
    }

    #[test]
    fn reparenting_replica_queues_slots_no_longer_served_by_its_shard() {
        const OLD_PRIMARY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        const NEW_PRIMARY: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let old_slot = key_hash_slot(b"old-shard") as usize;
        let new_slot = key_hash_slot(b"new-shard") as usize;
        let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
        for (id, slot) in [(OLD_PRIMARY, old_slot), (NEW_PRIMARY, new_slot)] {
            let mut primary =
                ClusterNode::new(id.to_string(), "127.0.0.1".to_string(), 6379, 16379, true);
            primary.flags &= !CLUSTER_NODE_MYSELF;
            primary.slots[slot] = true;
            primary.numslots = 1;
            state.nodes.insert(id.to_string(), primary);
        }
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA;
        state.myself.replicaof = OLD_PRIMARY.to_string();
        state.mark_topology_changed();
        assert!(state.take_shard_slot_cleanup_requests().is_empty());

        state.replicate(NEW_PRIMARY.as_bytes()).unwrap();

        assert_eq!(
            state.take_shard_slot_cleanup_requests(),
            vec![old_slot as u16]
        );
        assert!(state.serves_shard_slot(new_slot));
    }
}
