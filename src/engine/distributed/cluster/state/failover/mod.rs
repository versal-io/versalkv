use super::super::types::{short_name, ClusterState, CLUSTER_SLOTS};
use super::{evaluate_failure, FailureDecision, FailureFacts, FailureReport};
use crate::engine::distributed::cluster::action::Promotion;

impl ClusterState {
    /// Number of known primaries (including myself if I'm a primary with slots).
    /// Used for replica-election quorum computation.
    /// Matches C `server.cluster->size` semantics (L6823-L6825 cluster_legacy.c):
    /// counts only primaries that own at least one slot.
    pub fn primary_count(&self) -> usize {
        let mut n = 0usize;
        if Self::is_voting_primary(&self.myself) {
            n += 1;
        }
        for node in self.nodes.values() {
            if node.is_myself()
                || node.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE)
                || node.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_NOADDR)
            {
                continue;
            }
            if Self::is_voting_primary(node) {
                n += 1;
            }
        }
        n
    }

    /// Failover-election quorum: majority of primaries with slots.
    /// `floor(primary_count / 2) + 1`.
    pub fn quorum(&self) -> usize {
        let n = self.primary_count().max(1);
        n / 2 + 1
    }

    /// PFAIL→FAIL aggregation quorum. Matches C `markNodeAsFailingIfNeeded`
    /// (cluster_legacy.c:2624) exactly: `needed_quorum = size / 2 + 1` where
    /// `size` is the count of voting primaries (aka `primary_count`).
    pub fn fail_quorum(&self) -> usize {
        self.quorum()
    }

    pub fn is_voting_primary(
        node: &crate::engine::distributed::cluster::node::ClusterNode,
    ) -> bool {
        node.is_primary() && node.numslots > 0
    }

    /// Record a failure report for `target_node_id` coming from `reporter_id`.
    /// Mirrors C `clusterNodeAddFailureReport`: the report is only accepted
    /// if the reporter is a voting primary (primary with at least one slot).
    /// PFail reports live for `node_timeout * 2` ms.
    pub fn add_fail_report(
        &mut self,
        target_id: &str,
        reporter_id: &str,
        now: i64,
        node_timeout: u64,
    ) {
        // C cluster_legacy.c:2857 accepts only voting-primary reporters.
        let reporter_is_voting_primary = if reporter_id == self.myself.name {
            Self::is_voting_primary(&self.myself)
        } else {
            self.nodes
                .get(reporter_id)
                .map(Self::is_voting_primary)
                .unwrap_or(false)
        };
        if !reporter_is_voting_primary {
            return;
        }
        if reporter_id == self.myself.name {
            return;
        }
        let entry = self.fail_reports.entry(target_id.to_string()).or_default();
        entry.insert(reporter_id.to_string(), now);
        // Expire old reports
        let cutoff = now.saturating_sub(node_timeout.saturating_mul(2).min(i64::MAX as u64) as i64);
        entry.retain(|_, &mut t| t > cutoff);
    }

    /// Count live, distinct voting-primary reports plus the local vote.
    /// The caller has already established PFAIL and excluded FAIL.
    pub fn count_fail_reports(
        &self,
        target_id: &str,
        now: i64,
        node_timeout: u64,
    ) -> (usize, usize) {
        let reports = self
            .fail_reports
            .get(target_id)
            .into_iter()
            .flat_map(|reporters| reporters.iter())
            .filter_map(|(reporter_id, reported_at_ms)| {
                Some(FailureReport {
                    reporter: crate::engine::distributed::cluster::NodeId::parse(
                        reporter_id.as_bytes(),
                    )
                    .ok()?,
                    // Eligibility was checked when the report was recorded.
                    // C retains that report until its timestamp expires even
                    // if the reporter changes role in the meantime.
                    reporter_is_primary: true,
                    reporter_slots: 1,
                    reported_at_ms: *reported_at_ms,
                })
            })
            .collect();
        let facts = FailureFacts {
            target_is_pfail: true,
            target_is_fail: false,
            local_is_voting_primary: Self::is_voting_primary(&self.myself),
            voting_primaries: self.primary_count(),
            now_ms: now,
            node_timeout_ms: node_timeout,
            reports,
        };
        match evaluate_failure(&facts) {
            FailureDecision::AwaitQuorum { votes, quorum }
            | FailureDecision::MarkFail { votes, quorum } => (votes, quorum),
            FailureDecision::Reachable | FailureDecision::AlreadyFailed => (0, self.fail_quorum()),
        }
    }

    pub fn remove_fail_report(&mut self, target_id: &str, reporter_id: &str) -> bool {
        let Some(reporters) = self.fail_reports.get_mut(target_id) else {
            return false;
        };
        let removed = reporters.remove(reporter_id).is_some();
        if reporters.is_empty() {
            self.fail_reports.remove(target_id);
        }
        removed
    }

    /// Mark a node FAIL and broadcast the change via pending FAIL messages.
    /// Called once the quorum of PFAIL reports has been reached.
    pub fn mark_fail(&mut self, node_id: &str) -> bool {
        use crate::engine::distributed::cluster::node::{CLUSTER_NODE_FAIL, CLUSTER_NODE_PFAIL};
        let changed = if let Some(node) = self.nodes.get_mut(node_id) {
            let was = node.has_flag(CLUSTER_NODE_FAIL);
            node.flags &= !CLUSTER_NODE_PFAIL;
            node.flags |= CLUSTER_NODE_FAIL;
            if !was {
                node.fail_time = crate::foundation::io::sys::time::mstime();
            }
            // Retain slot claims for the promotion payload; routing already
            // excludes FAIL owners until a replica reclaims those slots.
            !was
        } else {
            false
        };
        if changed {
            if self.myself.is_replica() && self.myself.replicaof == node_id {
                self.myself.flags |=
                    crate::engine::distributed::cluster::node::CLUSTER_NODE_MY_PRIMARY_FAIL;
            }
            crate::server_log!(notice, "Cluster node {} is now FAIL", node_id);
            self.mark_topology_changed();
            self.recompute_state();
        }
        changed
    }

    /// Check if a master is objectively down after failure quorum.
    pub fn is_master_objectively_down(&self, master_id: &str) -> bool {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL;
        if master_id == self.myself.name {
            return false;
        }
        match self.nodes.get(master_id) {
            Some(n) => n.has_flag(CLUSTER_NODE_FAIL),
            None => {
                // If we don't even know about the master, it's "down" from our POV.
                true
            }
        }
    }

    /// Promote myself from replica to primary and take over the master's slots.
    /// Returns true if the promotion was performed.
    #[cfg(test)]
    pub fn promote_self_to_primary(&mut self, now_ms: i64) -> bool {
        if !self.myself.is_replica() {
            return false;
        }
        let master_id = self.myself.replicaof.clone();
        if master_id.is_empty() {
            return false;
        }
        // Steal the master's slots (from my view of the master).
        let mut new_slots: Vec<bool> = vec![false; CLUSTER_SLOTS];
        let master_shard_id = if let Some(m) = self.nodes.get(&master_id) {
            new_slots.copy_from_slice(&m.slots);
            m.shard_id.clone()
        } else {
            return false;
        };
        let promotion = Promotion {
            old_primary: crate::engine::distributed::cluster::NodeId::parse(master_id.as_bytes())
                .expect("known primary node ID must be valid"),
            shard_id: crate::engine::distributed::cluster::NodeId::parse(
                master_shard_id.as_bytes(),
            )
            .expect("known primary shard ID must be valid"),
            slots: new_slots,
            config_epoch: self.current_epoch.saturating_add(1),
        };
        if !self.apply_promotion(&promotion, now_ms) {
            return false;
        }

        true
    }

    /// Apply an election result without recomputing identity, slots, or epoch.
    pub fn apply_promotion(&mut self, promotion: &Promotion, _now_ms: i64) -> bool {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_MIGRATE_TO, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };
        if !self.myself.is_replica()
            || self.myself.replicaof != promotion.old_primary.as_str()
            || promotion.slots.len() != CLUSTER_SLOTS
            || self
                .validate_failover_slot_claim(promotion.old_primary.as_str(), &promotion.slots)
                .is_err()
        {
            return false;
        }

        let me = short_name(&self.myself.name);
        let old_primary = short_name(promotion.old_primary.as_str());
        let num_slots = promotion.slots.iter().filter(|&&slot| slot).count();
        crate::server_log!(
            notice,
            "[CLUSTER-ROLE] node={} PROMOTE: was replica of {} (new_num_slots={} epoch_from={} -> epoch_to={})",
            me,
            old_primary,
            num_slots,
            self.myself.config_epoch,
            promotion.config_epoch
        );
        self.myself.flags &= !CLUSTER_NODE_REPLICA;
        self.myself.flags |= CLUSTER_NODE_PRIMARY;
        self.clear_local_primary_failure_view();
        if self.nodes.values().any(|node| node.is_replica()) {
            self.myself.flags |= CLUSTER_NODE_MIGRATE_TO;
        }
        self.myself.replicaof.clear();
        self.myself.shard_id = promotion.shard_id.to_string();
        self.myself.slots.fill(false);
        self.myself.numslots = 0;
        self.claim_slots_after_failover(promotion.old_primary.as_str(), &promotion.slots)
            .expect("validated promotion references the failed primary");
        self.advance_current_epoch(promotion.config_epoch);
        self.myself.config_epoch = promotion.config_epoch;

        self.mark_topology_changed();
        self.recompute_state();
        crate::server_log!(
            notice,
            "Cluster failover: promoted self to primary (epoch {})",
            self.myself.config_epoch
        );
        self.debug_assert_invariants();
        true
    }

    pub(crate) fn observe_master_down(&mut self, is_down: bool) -> Option<bool> {
        if self.last_observed_master_down == is_down {
            return None;
        }
        Some(std::mem::replace(
            &mut self.last_observed_master_down,
            is_down,
        ))
    }

    pub(crate) fn observe_minority(&mut self, is_minority: bool) -> Option<bool> {
        if self.last_observed_minority == is_minority {
            return None;
        }
        Some(std::mem::replace(
            &mut self.last_observed_minority,
            is_minority,
        ))
    }

    pub(crate) fn observe_reachability_minority(&mut self, is_minority: bool) -> Option<bool> {
        if self.last_observed_reachability_minority == is_minority {
            return None;
        }
        Some(std::mem::replace(
            &mut self.last_observed_reachability_minority,
            is_minority,
        ))
    }

    /// Select the higher-epoch primary that owns every local slot. The runtime
    /// coordinator performs the actual role transition.
    pub fn orphan_replication_target(&self) -> Option<String> {
        if !self.myself.is_primary() || self.myself.numslots == 0 {
            return None;
        }
        // Find the single other primary that covers all of my slots with higher epoch.
        let mut winner: Option<(String, u64)> = None;
        for i in 0..CLUSTER_SLOTS {
            if !self.myself.slots[i] {
                continue;
            }
            let mut best_for_slot: Option<(String, u64)> = None;
            for node in self.nodes.values() {
                if !node.is_primary() {
                    continue;
                }
                if !node.slots[i] {
                    continue;
                }
                if node.config_epoch <= self.myself.config_epoch {
                    continue;
                }
                let better = match &best_for_slot {
                    None => true,
                    Some((_, epoch)) => node.config_epoch > *epoch,
                };
                if better {
                    best_for_slot = Some((node.name.clone(), node.config_epoch));
                }
            }
            let (wid, wepoch) = best_for_slot?;
            match &winner {
                None => winner = Some((wid, wepoch)),
                Some((cur_id, _)) if cur_id != &wid => return None, // multi-way, bail
                _ => {}
            }
        }
        let (wid, wepoch) = winner?;
        let me = short_name(&self.myself.name);
        let wshort = short_name(&wid);
        let prev_slots = self.myself.numslots;
        crate::server_log!(
            notice,
            "[CLUSTER-ROLE] node={} ORPHAN_DEMOTE: prev_slots={} winner={} winner_epoch={} (my_epoch={})",
            me, prev_slots, wshort, wepoch, self.myself.config_epoch
        );
        Some(wid)
    }
}
