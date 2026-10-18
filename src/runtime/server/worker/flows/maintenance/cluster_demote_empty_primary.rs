use crate::engine::distributed::cluster::action::ReplicationSwitchMode;
use crate::runtime::server::worker::ServerWorkerState;

#[derive(Clone, Copy)]
struct EmptyPrimaryActivationSignals {
    cluster_ok: bool,
    my_shard_known: bool,
    cluster_has_any_replicas: bool,
    my_shard_recognized: bool,
    prior_epoch_evidence_strict: bool,
    any_slotted_ge_2: bool,
    has_evidence: bool,
    fresh_boot: bool,
    min_epoch_ok: bool,
    has_ever_owned_slots: bool,
}

fn should_activate_empty_primary_demote(signals: EmptyPrimaryActivationSignals) -> bool {
    let post_failover_empty = signals.has_ever_owned_slots && signals.any_slotted_ge_2;
    let post_setup_stripped_empty = signals.cluster_has_any_replicas && post_failover_empty;
    let genuine_participation = signals.my_shard_recognized
        || signals.prior_epoch_evidence_strict
        || post_setup_stripped_empty;
    let cluster_ok_safe = signals.cluster_ok
        && (signals.my_shard_known || signals.cluster_has_any_replicas)
        && genuine_participation;
    // Late stress-failover cycles can leave a node as an empty primary with a
    // high local epoch, so the "fresh boot / min epoch" heuristic no longer
    // identifies it as stale even though the cluster has already seen real
    // failover epochs and replica topology. Allow the empty-primary chooser to run in
    // that degraded state once we have both post-failover evidence and either
    // shard recognition or an existing replica topology to anchor the choice.
    let degraded_post_failover_empty = !signals.cluster_ok
        && post_failover_empty
        && (signals.my_shard_recognized || signals.cluster_has_any_replicas);
    let restarted_old = signals.has_evidence
        && (signals.fresh_boot || signals.min_epoch_ok)
        && genuine_participation;

    cluster_ok_safe || restarted_old || degraded_post_failover_empty
}

impl ServerWorkerState {
    pub(super) fn cluster_demote_guard_empty_primary(
        &mut self,
        state: &mut crate::engine::distributed::cluster::state::ClusterState,
    ) {
        if !state.is_replica_migration_allowed() || state.pending_replication_switch.is_some() {
            return;
        }
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_REPLICA;
        use crate::engine::distributed::cluster::state::CLUSTER_OK;
        use std::collections::HashMap;

        let me = &state.myself;
        let eligible = me.is_primary() && me.count_slots() == 0 && state.has_ever_owned_slots();
        if eligible {
            let slotted_primaries: Vec<_> = state
                .nodes
                .values()
                .filter(|n| n.is_primary() && n.numslots > 0)
                .collect();
            if !slotted_primaries.is_empty() {
                let min_remote_epoch = slotted_primaries
                    .iter()
                    .map(|n| n.config_epoch)
                    .min()
                    .unwrap_or(0);
                let cluster_ok = state.state == CLUSTER_OK;
                let fresh_boot = state.myself.config_epoch <= 1;
                let min_epoch_ok = state.myself.config_epoch <= min_remote_epoch;
                // restarted_old targets the real old-primary case: a
                // legit former primary that was killed mid-failover and
                // has since restarted. It must have EVIDENCE of prior
                // participation — either a non-empty shard_id, or
                // min_epoch_ok (my epoch <= ALL slotted peers
                // proving I was once part of this cluster before the
                // current primaries bumped their epochs via
                // failover). `fresh_boot=true` with shard_id=="" is
                // a brand-new replica that was killed during a
                // mid-cluster setup window; those must NOT free-pick
                // a shard via restarted_old — they should wait for
                // CLUSTER REPLICATE from the harness, and only
                // fire under cluster_ok (which guarantees topology is
                // stable and shard sizes are canonical).
                let has_evidence = !state.myself.shard_id.is_empty() || min_epoch_ok;
                // Setup-race guard: "cluster OK + empty shard_id + no
                // replicas allocated yet" means the harness is still in
                // the MEET → ADDSLOTS → REPLICATE pipeline and one of
                // the 5 primaries hasn't finished ADDSLOTS yet, while
                // the other 4 pushed cluster_state to OK. We must NOT
                // auto-demote that primary to a shard in that window —
                // CLUSTER REPLICATE from the harness will assign it
                // explicitly. The simplest proxy for "harness has
                // allocated replicas" is: at least one replica entry
                // exists whose replicaof points to a known slotted
                // primary.
                let cluster_has_any_replicas = state
                    .nodes
                    .values()
                    .any(|n| n.has_flag(CLUSTER_NODE_REPLICA) && !n.replicaof.is_empty());
                // Real-participation signal (defeats auto-gen shard_id +
                // cascade epoch drift).
                //
                // Context:
                //  • ClusterState::new() generates a random shard_id for
                //    every fresh boot → !shard_id.empty() is ALWAYS true
                //    for setup primaries.
                //  • Any cascade self-demotion (even one false positive in
                //    the whole cluster) bumps current_epoch for other
                //    primaries on the gossip mesh, so min_epoch_ok is
                //    trivially true and cluster_has_any_replicas flips to
                //    true, making weaker signals fire a chain reaction of
                //    setup primary demotions.
                //
                // STRICT genuine participation = EITHER:
                //   (a) my_shard_recognized — at least one OTHER node in
                //       the cluster carries my exact shard_id. Autogen
                //       shard_ids are cryptographically unique per boot,
                //       so this is zero false-positive during setup.
                //   (b) prior_epoch_evidence_strict — my config_epoch is
                //       STRICTLY less than the minimum epoch among all
                //       slotted primaries. In setup every primary has
                //       epoch==1, so "my_epoch < min(others' epoch)" is
                //       impossible.
                //   (c) post_setup_stripped_empty — cluster already has
                //       replicas (proves harness REPLICATE phase is done)
                //       AND at least one slotted primary has epoch>=2,
                //       proving we've seen at least one failover or
                //       role transition in this cluster lifecycle. This
                //       covers the restarted old-primary case: it loads
                //       3277 slots from RDB, the UPDATE strip path zeros
                //       them within one gossip cycle, leaving me as
                //       empty primary with epoch=1 (strict min check
                //       fails because other shards' primaries are also
                //       at epoch 1) AND a new autogen shard_id (so
                //       nothing recognizes me). The epoch>=2 evidence
                //       breaks the tie with setup because setup has
                //       ALL nodes at epoch 1 and 0 replicas.
                let my_shard_recognized = !state.myself.shard_id.is_empty()
                    && state.nodes.values().any(|n| {
                        if n.name == state.myself.name {
                            return false;
                        }
                        n.shard_id == state.myself.shard_id
                    });
                let prior_epoch_evidence_strict = state.myself.config_epoch < min_remote_epoch;
                let any_slotted_ge_2 = slotted_primaries.iter().any(|n| n.config_epoch >= 2);
                if should_activate_empty_primary_demote(EmptyPrimaryActivationSignals {
                    cluster_ok,
                    my_shard_known: !state.myself.shard_id.is_empty(),
                    cluster_has_any_replicas,
                    my_shard_recognized,
                    prior_epoch_evidence_strict,
                    any_slotted_ge_2,
                    has_evidence,
                    fresh_boot,
                    min_epoch_ok,
                    has_ever_owned_slots: state.has_ever_owned_slots(),
                }) {
                    // Case A: I know my shard → strict same-shard only.
                    let mut target_info: Option<(String, usize, u64)> = None;
                    if !me.shard_id.is_empty() {
                        let mut best: Option<(String, u64)> = None;
                        for n in &slotted_primaries {
                            if n.shard_id != me.shard_id {
                                continue;
                            }
                            let replace = match &best {
                                None => true,
                                Some((_, be)) => {
                                    n.config_epoch > *be
                                        || (n.config_epoch == *be
                                            && n.name < best.as_ref().unwrap().0)
                                }
                            };
                            if replace {
                                best = Some((n.name.clone(), n.config_epoch));
                            }
                        }
                        // Case A fallback: no primary advertises my
                        // shard_id yet (gossip propagation still
                        // converging after restart). Try to locate my
                        // shard's primary by scanning EXISTING
                        // replicas with my shard_id: their replicaof
                        // field points to the current primary. This
                        // handles the 3-restarted-replicas case where
                        // all cluster bus participants have just
                        // restarted and nobody's shard_id has
                        // propagated to primaries yet.
                        if best.is_none() {
                            let mut primary_votes: HashMap<String, usize> = HashMap::new();
                            for n in state.nodes.values() {
                                if n.shard_id != me.shard_id {
                                    continue;
                                }
                                if !n.replicaof.is_empty() {
                                    *primary_votes.entry(n.replicaof.clone()).or_insert(0) += 1;
                                }
                            }
                            // Myself also votes: I am an empty primary
                            // of shard X, but I don't know replicaof.
                            // If no replicaof exists yet, pick the
                            // slotted primary whose shard assignment
                            // is the most "distinct": smallest overlap
                            // with other primaries' replicas'
                            // replicaof primaries. Fallback: highest
                            // epoch slotted primary.
                            if let Some((winner, _votes)) =
                                primary_votes.into_iter().max_by_key(|(_, v)| *v)
                            {
                                if let Some(pnode) = state.nodes.get(&winner) {
                                    if pnode.is_primary() && pnode.numslots > 0 {
                                        best = Some((winner.clone(), pnode.config_epoch));
                                    }
                                }
                            }
                        }
                        if let Some((tid, tepoch)) = best {
                            let reps = state
                                .nodes
                                .values()
                                .filter(|n| {
                                    n.has_flag(CLUSTER_NODE_REPLICA)
                                        && !n.replicaof.is_empty()
                                        && n.shard_id == me.shard_id
                                })
                                .count();
                            target_info = Some((tid, reps, tepoch));
                        }
                    }
                    // Case B: I don't know my shard → free-pick by
                    // smallest replica count (under-replicated shard
                    // priority), then highest epoch, then smallest
                    // name deterministic tie-break.
                    //
                    // Setup-phase guard: Case B must NOT fire when NO
                    // shard has any replica yet. All-zero replica counts
                    // means the harness hasn't called CLUSTER REPLICATE —
                    // free-picking then steals setup primaries and causes
                    // "ERR not a master" in default_replica_allocation.
                    if target_info.is_none() && me.shard_id.is_empty() {
                        let mut shard_replica_count: HashMap<String, usize> = HashMap::new();
                        for n in state.nodes.values() {
                            if n.has_flag(CLUSTER_NODE_REPLICA) && !n.replicaof.is_empty() {
                                *shard_replica_count.entry(n.shard_id.clone()).or_insert(0) += 1;
                            }
                        }
                        let any_shard_has_replicas = shard_replica_count.values().any(|&c| c > 0);
                        if !any_shard_has_replicas {
                            // No established shard yet. Wait for
                            // harness CLUSTER REPLICATE.
                        } else {
                            let mut candidates: Vec<(String, String, usize, u64)> =
                                slotted_primaries
                                    .iter()
                                    .map(|n| {
                                        let shard = if n.shard_id.is_empty() {
                                            n.name.clone()
                                        } else {
                                            n.shard_id.clone()
                                        };
                                        let reps =
                                            shard_replica_count.get(&shard).copied().unwrap_or(0);
                                        (n.name.clone(), shard, reps, n.config_epoch)
                                    })
                                    .collect();
                            candidates.sort_by(|a, b| {
                                a.2.cmp(&b.2).then(b.3.cmp(&a.3)).then(a.0.cmp(&b.0))
                            });
                            if let Some((tid, _shard, reps, tepoch)) = candidates.first() {
                                target_info = Some((tid.clone(), *reps, *tepoch));
                            }
                        } // close any_shard_has_replicas else block
                    }
                    if let Some((target_id, reps, tepoch)) = target_info {
                        if let Some(new_master) = state.nodes.get(&target_id) {
                            // A promoted primary may still carry its former
                            // `replicaof` in our local cache when its slot
                            // claim arrives before the direct role heartbeat.
                            // Its current primary role, owned slots and shard
                            // identity are authoritative for this recovery.
                            let old_state = if cluster_ok {
                                "cluster-ok"
                            } else {
                                "restarted-old"
                            };
                            crate::server_log!(
                                notice,
                                "[DEMOTE-GUARD] empty-primary-cron empty-primary demotion: \
                                     myself={:.8} epoch={} reason={} \
                                     my_shard={} target={:.8} tgt_shard={} \
                                     shard_size={} primary_epoch={} \
                                     cluster_state={:?}",
                                state.myself.name,
                                state.myself.config_epoch,
                                old_state,
                                if me.shard_id.is_empty() {
                                    "?"
                                } else {
                                    &me.shard_id[..me.shard_id.len().min(8)]
                                },
                                target_id,
                                if new_master.shard_id.is_empty() {
                                    "?"
                                } else {
                                    &new_master.shard_id[..new_master.shard_id.len().min(8)]
                                },
                                reps,
                                tepoch,
                                state.state
                            );
                            let target_address = (new_master.ip().to_string(), new_master.port());
                            if self.follow_known_cluster_primary(
                                state,
                                &target_id,
                                ReplicationSwitchMode::TryPartial,
                            ) {
                                crate::server_log!(
                                    notice,
                                    "Cluster empty-primary cron self-heal: empty-primary -> replica of {}:{}",
                                    target_address.0,
                                    target_address.1
                                );
                            } else {
                                crate::server_log!(
                                    notice,
                                    "[DEMOTE-GUARD] empty-primary-cron replicate({:.8}) rejected",
                                    target_id
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{should_activate_empty_primary_demote, EmptyPrimaryActivationSignals};
    use crate::engine::distributed::cluster::action::ReplicationSwitchMode;
    use crate::engine::distributed::cluster::node::{
        ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
    };
    use crate::engine::distributed::cluster::state::{
        ClusterState, PendingReplicationSwitch, CLUSTER_OK,
    };
    use crate::runtime::server::worker::ServerWorkerState;

    #[test]
    fn degraded_post_failover_empty_primary_still_activates_empty_primary() {
        assert!(should_activate_empty_primary_demote(
            EmptyPrimaryActivationSignals {
                cluster_ok: false,
                my_shard_known: true,
                cluster_has_any_replicas: true,
                my_shard_recognized: false,
                prior_epoch_evidence_strict: false,
                any_slotted_ge_2: true,
                has_evidence: true,
                fresh_boot: false,
                min_epoch_ok: false,
                has_ever_owned_slots: true,
            }
        ));
    }

    #[test]
    fn setup_empty_primary_without_failover_evidence_stays_blocked() {
        assert!(!should_activate_empty_primary_demote(
            EmptyPrimaryActivationSignals {
                cluster_ok: true,
                my_shard_known: true,
                cluster_has_any_replicas: false,
                my_shard_recognized: false,
                prior_epoch_evidence_strict: false,
                any_slotted_ge_2: false,
                has_evidence: true,
                fresh_boot: true,
                min_epoch_ok: true,
                has_ever_owned_slots: false,
            }
        ));
    }

    #[test]
    fn history_alone_does_not_bypass_no_replica_guard() {
        assert!(!should_activate_empty_primary_demote(
            EmptyPrimaryActivationSignals {
                cluster_ok: false,
                my_shard_known: true,
                cluster_has_any_replicas: false,
                my_shard_recognized: false,
                prior_epoch_evidence_strict: false,
                any_slotted_ge_2: true,
                has_evidence: true,
                fresh_boot: false,
                min_epoch_ok: false,
                has_ever_owned_slots: true,
            }
        ));
    }

    #[test]
    fn deterministic_pending_switch_blocks_empty_primary_heuristic_reparenting() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        state.add_slot(0).unwrap();
        state.myself.slots.fill(false);
        state.myself.numslots = 0;
        state.state = CLUSTER_OK;

        let desired_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let other_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let replica_id = "cccccccccccccccccccccccccccccccccccccccc";
        for (id, slot, epoch) in [(desired_id, 1, 8), (other_id, 2, 9)] {
            let mut primary = ClusterNode::new(
                id.to_string(),
                "127.0.0.1".to_string(),
                6381 + slot as u16,
                16_381 + slot as u16,
                true,
            );
            primary.flags &= !CLUSTER_NODE_MYSELF;
            primary.slots[slot] = true;
            primary.numslots = 1;
            primary.config_epoch = epoch;
            state.nodes.insert(id.to_string(), primary);
        }
        let mut replica = ClusterNode::new(
            replica_id.to_string(),
            "127.0.0.1".to_string(),
            6384,
            16_384,
            false,
        );
        replica.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
        replica.flags |= CLUSTER_NODE_REPLICA;
        replica.replicaof = desired_id.to_string();
        state.nodes.insert(replica_id.to_string(), replica);
        state.pending_replication_switch = Some(PendingReplicationSwitch {
            node_id: desired_id.to_string(),
            mode: ReplicationSwitchMode::TryPartial,
        });

        runtime.cluster_demote_guard_empty_primary(&mut state);

        assert!(state.myself.is_primary());
        assert_eq!(
            state
                .pending_replication_switch
                .as_ref()
                .map(|pending| pending.node_id.as_str()),
            Some(desired_id)
        );
    }

    #[test]
    fn stale_promoted_primary_replicaof_does_not_block_empty_primary_demotion() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        state.add_slot(0).unwrap();
        state.myself.slots.fill(false);
        state.myself.numslots = 0;
        state.state = CLUSTER_OK;

        let promoted_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let sibling_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let local_id = state.myself.name.clone();
        let local_shard = state.myself.shard_id.clone();
        let mut promoted = ClusterNode::new(
            promoted_id.to_string(),
            "127.0.0.1".to_string(),
            6381,
            16_381,
            true,
        );
        promoted.flags &= !CLUSTER_NODE_MYSELF;
        promoted.shard_id = local_shard.clone();
        promoted.replicaof = local_id;
        promoted.config_epoch = 11;
        promoted.slots[0] = true;
        promoted.numslots = 1;
        state.nodes.insert(promoted_id.to_string(), promoted);

        let mut sibling = ClusterNode::new(
            sibling_id.to_string(),
            "127.0.0.1".to_string(),
            6382,
            16_382,
            false,
        );
        sibling.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
        sibling.flags |= CLUSTER_NODE_REPLICA;
        sibling.shard_id = local_shard;
        sibling.replicaof = promoted_id.to_string();
        state.nodes.insert(sibling_id.to_string(), sibling);

        runtime.cluster_demote_guard_empty_primary(&mut state);

        assert!(state.myself.is_replica());
        assert_eq!(state.myself.replicaof, promoted_id);
    }

    #[test]
    fn known_shard_without_a_slot_owner_does_not_free_pick_another_shard() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        state.add_slot(0).unwrap();
        state.del_slot(0).unwrap();
        state.state = CLUSTER_OK;
        state.myself.config_epoch = 1;

        let foreign_primary = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let foreign_replica = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mut primary = ClusterNode::new(
            foreign_primary.to_string(),
            "127.0.0.1".to_string(),
            6381,
            16_381,
            true,
        );
        primary.flags &= !CLUSTER_NODE_MYSELF;
        primary.config_epoch = 2;
        primary.slots[1] = true;
        primary.numslots = 1;
        state.nodes.insert(foreign_primary.to_string(), primary);

        let mut replica = ClusterNode::new(
            foreign_replica.to_string(),
            "127.0.0.1".to_string(),
            6382,
            16_382,
            false,
        );
        replica.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
        replica.flags |= CLUSTER_NODE_REPLICA;
        replica.replicaof = foreign_primary.to_string();
        state.nodes.insert(foreign_replica.to_string(), replica);

        runtime.cluster_demote_guard_empty_primary(&mut state);

        assert!(state.myself.is_primary());
        assert!(state.myself.replicaof.is_empty());
    }
}
