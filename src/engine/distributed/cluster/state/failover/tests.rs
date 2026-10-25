use super::{
    evaluate_failure, failover_election_delay_ms, rank_replicas, FailureDecision, FailureFacts,
    FailureReport, ReplicaEligibility, ReplicaRankKey,
};
use crate::engine::distributed::cluster::node::{
    ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_MY_PRIMARY_FAIL, CLUSTER_NODE_PRIMARY,
    CLUSTER_NODE_REPLICA,
};
use crate::engine::distributed::cluster::state::ClusterState;
use crate::engine::distributed::cluster::NodeId;

fn node_id(byte: u8) -> NodeId {
    NodeId::parse(&[byte; 40]).expect("fixture node id is valid")
}

fn report(reporter: u8, is_primary: bool, slots: usize, at_ms: i64) -> FailureReport {
    FailureReport {
        reporter: node_id(reporter),
        reporter_is_primary: is_primary,
        reporter_slots: slots,
        reported_at_ms: at_ms,
    }
}

#[test]
fn failure_decisions_follow_voting_primary_and_expiry_table() {
    struct Case {
        name: &'static str,
        target_is_pfail: bool,
        target_is_fail: bool,
        reports: Vec<FailureReport>,
        expected: FailureDecision,
    }

    let cases = [
        Case {
            name: "slot primary report plus local vote reaches quorum",
            target_is_pfail: true,
            target_is_fail: false,
            reports: vec![report(b'a', true, 1, 10_001)],
            expected: FailureDecision::MarkFail {
                votes: 2,
                quorum: 2,
            },
        },
        Case {
            name: "replica report is ignored",
            target_is_pfail: true,
            target_is_fail: false,
            reports: vec![report(b'b', false, 0, 10_001)],
            expected: FailureDecision::AwaitQuorum {
                votes: 1,
                quorum: 2,
            },
        },
        Case {
            name: "slotless primary report is ignored",
            target_is_pfail: true,
            target_is_fail: false,
            reports: vec![report(b'c', true, 0, 10_001)],
            expected: FailureDecision::AwaitQuorum {
                votes: 1,
                quorum: 2,
            },
        },
        Case {
            name: "report at the validity cutoff is expired",
            target_is_pfail: true,
            target_is_fail: false,
            reports: vec![report(b'd', true, 1, 10_000)],
            expected: FailureDecision::AwaitQuorum {
                votes: 1,
                quorum: 2,
            },
        },
        Case {
            name: "reachable target cannot become fail from reports alone",
            target_is_pfail: false,
            target_is_fail: false,
            reports: vec![report(b'e', true, 1, 10_001)],
            expected: FailureDecision::Reachable,
        },
        Case {
            name: "already failed target is stable",
            target_is_pfail: false,
            target_is_fail: true,
            reports: vec![report(b'f', true, 1, 10_001)],
            expected: FailureDecision::AlreadyFailed,
        },
    ];

    for case in cases {
        let facts = FailureFacts {
            target_is_pfail: case.target_is_pfail,
            target_is_fail: case.target_is_fail,
            local_is_voting_primary: true,
            voting_primaries: 3,
            now_ms: 20_000,
            node_timeout_ms: 5_000,
            reports: case.reports,
        };
        assert_eq!(evaluate_failure(&facts), case.expected, "{}", case.name);
    }
}

fn eligible_replica(id: u8, offset: i64) -> ReplicaEligibility {
    ReplicaEligibility {
        node_id: node_id(id),
        no_failover: false,
        cluster_link_connected: true,
        cluster_view_stale: false,
        loading: false,
        priority: 100,
        replication_offset: offset,
        data_age_ms: 25_000,
        node_timeout_ms: 5_000,
        repl_ping_period_ms: 10_000,
        replica_validity_factor: 2,
    }
}

#[test]
fn replica_eligibility_rejects_each_disqualifying_fact() {
    struct Case {
        name: &'static str,
        mutate: fn(&mut ReplicaEligibility),
        expected: bool,
    }

    let cases = [
        Case {
            name: "healthy replica",
            mutate: |_| {},
            expected: true,
        },
        Case {
            name: "nofailover",
            mutate: |candidate| candidate.no_failover = true,
            expected: false,
        },
        Case {
            name: "disconnected cluster link",
            mutate: |candidate| candidate.cluster_link_connected = false,
            expected: true,
        },
        Case {
            name: "stale cluster view",
            mutate: |candidate| candidate.cluster_view_stale = true,
            expected: false,
        },
        Case {
            name: "loading",
            mutate: |candidate| candidate.loading = true,
            expected: false,
        },
        Case {
            name: "priority zero",
            mutate: |candidate| candidate.priority = 0,
            expected: false,
        },
        Case {
            name: "data older than validity window",
            mutate: |candidate| candidate.data_age_ms = 25_001,
            expected: false,
        },
        Case {
            name: "zero validity factor disables age rejection",
            mutate: |candidate| {
                candidate.data_age_ms = u64::MAX;
                candidate.replica_validity_factor = 0;
            },
            expected: true,
        },
    ];

    for case in cases {
        let mut candidate = eligible_replica(b'a', 10);
        (case.mutate)(&mut candidate);
        assert_eq!(candidate.is_eligible(), case.expected, "{}", case.name);
    }
}

#[test]
fn replica_ranking_uses_offset_and_node_id_and_only_skips_nofailover() {
    let mut zero_priority = eligible_replica(b'd', 1);
    zero_priority.priority = 0;
    let high_offset = eligible_replica(b'c', 20);
    let low_id = eligible_replica(b'a', 10);
    let high_id = eligible_replica(b'b', 10);
    let mut loading = eligible_replica(b'e', 1_000);
    loading.loading = true;
    let mut disconnected = eligible_replica(b'f', 2_000);
    disconnected.cluster_link_connected = false;
    let mut stale = eligible_replica(b'g', 3_000);
    stale.cluster_view_stale = true;
    let mut ignored = eligible_replica(b'h', 4_000);
    ignored.no_failover = true;

    assert_eq!(
        rank_replicas(&[
            high_id,
            ignored,
            low_id,
            high_offset,
            zero_priority,
            loading,
            disconnected,
            stale,
        ]),
        vec![
            node_id(b'g'),
            node_id(b'f'),
            node_id(b'e'),
            node_id(b'c'),
            node_id(b'a'),
            node_id(b'b'),
            node_id(b'd'),
        ]
    );
}

#[test]
fn rank_key_is_explicit() {
    let established = eligible_replica(b'a', 1);

    assert_eq!(
        established.rank_key(),
        Some(ReplicaRankKey {
            replication_offset: 1,
            node_id: node_id(b'a'),
        })
    );
}

#[test]
fn election_delay_uses_c_rank_and_new_replica_rules() {
    assert_eq!(failover_election_delay_ms(15_000, 0, 1, false), 0);
    assert_eq!(failover_election_delay_ms(15_000, 1, 1, false), 1_000);
    assert_eq!(failover_election_delay_ms(3_000, 2, 1, false), 400);
    assert_eq!(failover_election_delay_ms(15_000, 0, 0, false), 500);
    assert_eq!(failover_election_delay_ms(15_000, 9, 0, true), 0);
}

#[test]
fn state_failure_reports_accept_only_slot_primaries_and_expire_at_two_timeouts() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.add_slot(0).unwrap();
    let target_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let voter_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let replica_id = "cccccccccccccccccccccccccccccccccccccccc";
    let slotless_id = "dddddddddddddddddddddddddddddddddddddddd";

    let mut target = ClusterNode::new(target_id.to_string(), "127.0.0.1".into(), 6380, 16380, true);
    target.flags &= !CLUSTER_NODE_MYSELF;
    let mut voter = ClusterNode::new(voter_id.to_string(), "127.0.0.1".into(), 6381, 16381, true);
    voter.flags &= !CLUSTER_NODE_MYSELF;
    voter.slots[1] = true;
    voter.numslots = 1;
    let mut replica = ClusterNode::new(
        replica_id.to_string(),
        "127.0.0.1".into(),
        6382,
        16382,
        false,
    );
    replica.flags &= !CLUSTER_NODE_MYSELF;
    let mut slotless = ClusterNode::new(
        slotless_id.to_string(),
        "127.0.0.1".into(),
        6383,
        16383,
        true,
    );
    slotless.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(target_id.to_string(), target);
    state.nodes.insert(voter_id.to_string(), voter);
    state.nodes.insert(replica_id.to_string(), replica);
    state.nodes.insert(slotless_id.to_string(), slotless);

    state.add_fail_report(target_id, voter_id, 10_000, 5_000);
    state.add_fail_report(target_id, replica_id, 10_001, 5_000);
    state.add_fail_report(target_id, slotless_id, 10_001, 5_000);

    assert_eq!(state.count_fail_reports(target_id, 19_999, 5_000).0, 2);
    assert_eq!(state.count_fail_reports(target_id, 20_000, 5_000).0, 1);
}

#[test]
fn marking_own_primary_failed_advertises_the_local_failure_view() {
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut primary = ClusterNode::new(
        primary_id.to_string(),
        "127.0.0.1".to_string(),
        6379,
        16379,
        true,
    );
    primary.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(primary_id.to_string(), primary);
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = primary_id.to_string();

    assert!(state.mark_fail(primary_id));
    assert!(state.myself.has_flag(CLUSTER_NODE_MY_PRIMARY_FAIL));
}

#[test]
fn failover_observations_are_owned_by_each_cluster_state() {
    let mut first = ClusterState::new("127.0.0.1", 6379, 16379);
    let mut second = ClusterState::new("127.0.0.1", 6380, 16380);

    assert_eq!(first.observe_master_down(true), Some(false));
    assert_eq!(first.observe_master_down(true), None);
    assert_eq!(second.observe_master_down(true), Some(false));
    assert_eq!(first.observe_minority(true), Some(false));
    assert_eq!(second.observe_minority(true), Some(false));
}

#[test]
fn restarting_primary_waits_before_it_can_vote() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

    state.observe_topology_evaluation(1_000);
    assert!(!state.safe_to_vote(1_000));
    state.state = crate::engine::distributed::cluster::state::CLUSTER_OK;
    assert!(state.safe_to_vote(1_001));
    state.state = crate::engine::distributed::cluster::state::types::CLUSTER_FAIL;
    assert!(!state.safe_to_vote(2_999));
    assert!(state.safe_to_vote(3_000));
    assert!(state.safe_to_vote(3_001));
}

#[test]
fn promoted_replica_keeps_master_shard_id() {
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    let master_id = "mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm";
    let shard_id = "ssssssssssssssssssssssssssssssssssssssss";
    state.myself.replicaof = master_id.to_string();
    state.myself.shard_id = "rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr".to_string();

    let mut master = ClusterNode::new(
        master_id.to_string(),
        "127.0.0.1".to_string(),
        6379,
        16379,
        true,
    );
    master.flags &= !CLUSTER_NODE_MYSELF;
    master.shard_id = shard_id.to_string();
    master.slots[7] = true;
    master.numslots = 1;
    state.nodes.insert(master.name.clone(), master);

    assert!(state.promote_self_to_primary(1_000));
    assert_eq!(state.myself.shard_id, shard_id);
    assert!(state.myself.is_primary());
    assert!(state.myself.slots[7]);
}

#[test]
fn promotion_clears_withdrawn_marker_for_inherited_slots() {
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    let master_id = "mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm";
    state.myself.replicaof = master_id.to_string();

    let mut master = ClusterNode::new(
        master_id.to_string(),
        "127.0.0.1".to_string(),
        6379,
        16379,
        true,
    );
    master.flags &= !CLUSTER_NODE_MYSELF;
    master.slots[7] = true;
    master.numslots = 1;
    state.nodes.insert(master.name.clone(), master);
    state.slot_ownership.withdrawn_by[7] = Some(master_id.to_string());

    assert!(state.promote_self_to_primary(1_000));
    assert!(state.slot_ownership.withdrawn_by[7].is_none());
    assert!(state.validate_invariants().is_ok());
}

#[test]
fn unilateral_epoch_bump_exceeds_failed_peer_and_reports_still_when_ahead() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.current_epoch = 4;
    state.myself.config_epoch = 4;
    let mut failed = ClusterNode::new(
        "ffffffffffffffffffffffffffffffffffffffff".to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    failed.flags &= !CLUSTER_NODE_MYSELF;
    failed.flags |= crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL;
    failed.config_epoch = 999_999;
    state.nodes.insert(failed.name.clone(), failed);

    assert_eq!(state.bump_config_epoch_without_consensus(), Ok(true));
    assert_eq!(state.current_epoch, 1_000_000);
    assert_eq!(state.myself.config_epoch, 1_000_000);
    assert_eq!(state.bump_config_epoch_without_consensus(), Ok(false));
}

mod automatic {
    use super::{eligible_replica, node_id};
    use crate::engine::distributed::cluster::action::{ClusterAction, Promotion};
    use crate::engine::distributed::cluster::state::failover::{
        AutomaticFailover, AutomaticFailoverFacts, AutomaticFailoverPhase, FailedPrimaryFacts,
        VoteRequestFacts,
    };

    fn primary(id: u8, failed: bool) -> FailedPrimaryFacts {
        let mut slots = vec![false; 16_384];
        slots[7] = true;
        FailedPrimaryFacts {
            node_id: node_id(id),
            shard_id: node_id(b's'),
            slots,
            is_failed: failed,
        }
    }

    fn facts(now_ms: i64) -> AutomaticFailoverFacts {
        let local = eligible_replica(b'r', 42);
        AutomaticFailoverFacts {
            now_ms,
            node_timeout_ms: 5_000,
            current_epoch: 9,
            voting_primaries: 3,
            failed_primary_rank: 0,
            all_replicas_primary_failed: true,
            local: Some(local.clone()),
            primary: Some(primary(b'p', true)),
            siblings: vec![local],
            manual: false,
        }
    }

    #[test]
    fn best_ranked_replica_waits_without_full_shard_failure_confirmation() {
        let mut failover = AutomaticFailover::default();
        let mut initial = facts(1_000);
        initial.all_replicas_primary_failed = false;

        assert!(failover.on_tick(&initial).is_empty());
        assert_eq!(failover.phase(), AutomaticFailoverPhase::WaitingRankDelay);
        assert!(failover.deadline_ms() > initial.now_ms);

        let mut due = initial;
        due.now_ms = failover.deadline_ms();
        assert_eq!(
            failover.on_tick(&due),
            vec![ClusterAction::BroadcastFailoverAuthRequest {
                election_epoch: 10,
                force_ack: false,
            }]
        );
    }

    #[test]
    fn lower_ranked_replica_keeps_c_propagation_delay() {
        let mut failover = AutomaticFailover::default();
        let mut current = facts(1_000);
        current.siblings.push(eligible_replica(b'a', 43));

        assert!(failover.on_tick(&current).is_empty());
        assert_eq!(failover.rank(), 1);
        // C adds fixed and random propagation delays before the rank penalty.
        assert!((1_498..1_664).contains(&failover.deadline_ms()));
        let first_deadline = failover.deadline_ms();
        current.now_ms += 1;
        assert!(failover.on_tick(&current).is_empty());
        assert_eq!(failover.deadline_ms(), first_deadline);

        current.siblings.push(eligible_replica(b'b', 44));
        assert!(failover.on_tick(&current).is_empty());
        assert_eq!(failover.deadline_ms(), first_deadline + 332);
    }

    #[test]
    fn election_moves_through_phases_and_emits_complete_promotion() {
        let mut failover = AutomaticFailover::default();
        assert!(failover.on_tick(&facts(1_000)).is_empty());
        assert_eq!(failover.phase(), AutomaticFailoverPhase::WaitingRankDelay);

        assert_eq!(
            failover.on_tick(&facts(1_000)),
            vec![ClusterAction::BroadcastFailoverAuthRequest {
                election_epoch: 10,
                force_ack: true,
            }]
        );
        assert_eq!(failover.phase(), AutomaticFailoverPhase::RequestingVotes);

        assert!(failover.on_vote_ack(node_id(b'a'), 9, true).is_empty());
        assert!(failover.on_vote_ack(node_id(b'a'), 10, false).is_empty());
        assert!(failover.on_vote_ack(node_id(b'a'), 10, true).is_empty());
        assert!(failover.on_vote_ack(node_id(b'a'), 10, true).is_empty());

        assert_eq!(
            failover.on_vote_ack(node_id(b'b'), 10, true),
            vec![ClusterAction::PromoteSelf(Promotion {
                old_primary: node_id(b'p'),
                shard_id: node_id(b's'),
                slots: {
                    let mut slots = vec![false; 16_384];
                    slots[7] = true;
                    slots
                },
                config_epoch: 10,
            })]
        );
        assert_eq!(failover.phase(), AutomaticFailoverPhase::Promoting);

        assert_eq!(
            failover.on_topology_change(true, true),
            vec![
                ClusterAction::PersistClusterConfig,
                ClusterAction::BroadcastTopology,
            ]
        );
        assert_eq!(failover.phase(), AutomaticFailoverPhase::Broadcasting);
        let mut primary_facts = facts(1_001);
        primary_facts.local = None;
        assert!(failover.on_tick(&primary_facts).is_empty());
        assert_eq!(failover.phase(), AutomaticFailoverPhase::Idle);
    }

    #[test]
    fn promotion_action_commits_the_exact_epoch_shard_and_slots() {
        use crate::engine::distributed::cluster::node::{
            ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };
        use crate::engine::distributed::cluster::state::ClusterState;

        let old_primary = node_id(b'p');
        let shard_id = node_id(b's');
        let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA;
        state.myself.replicaof = old_primary.to_string();
        state.current_epoch = 9;
        let mut slots = vec![false; 16_384];
        slots[7] = true;
        let mut previous_primary = ClusterNode::new(
            old_primary.to_string(),
            "127.0.0.1".to_string(),
            6379,
            16_379,
            true,
        );
        previous_primary.flags &= !CLUSTER_NODE_MYSELF;
        previous_primary.slots[7] = true;
        previous_primary.numslots = 1;
        state
            .nodes
            .insert(previous_primary.name.clone(), previous_primary);

        assert!(state.apply_promotion(
            &Promotion {
                old_primary: old_primary.clone(),
                shard_id: shard_id.clone(),
                slots,
                config_epoch: 10,
            },
            1_000,
        ));

        assert!(state.myself.is_primary());
        assert!(state.myself.replicaof.is_empty());
        assert_eq!(state.current_epoch, 10);
        assert_eq!(state.myself.config_epoch, 10);
        assert_eq!(state.myself.shard_id, shard_id.as_str());
        assert!(state.myself.slots[7]);
        assert_eq!(state.myself.numslots, 1);
        assert!(!state.nodes[old_primary.as_str()].slots[7]);
        assert_eq!(state.nodes[old_primary.as_str()].numslots, 0);
    }

    #[test]
    fn promotion_with_missing_primary_leaves_replica_unchanged() {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };
        use crate::engine::distributed::cluster::state::ClusterState;

        let old_primary = node_id(b'p');
        let shard_id = node_id(b's');
        let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA;
        state.myself.replicaof = old_primary.to_string();
        let mut slots = vec![false; 16_384];
        slots[7] = true;

        assert!(!state.apply_promotion(
            &Promotion {
                old_primary: old_primary.clone(),
                shard_id,
                slots,
                config_epoch: 10,
            },
            1_000,
        ));
        assert!(state.myself.is_replica());
        assert_eq!(state.myself.replicaof, old_primary.as_str());
        assert_eq!(state.myself.numslots, 0);
    }

    #[test]
    fn primary_recovery_and_ineligible_replica_cancel_election() {
        let mut failover = AutomaticFailover::default();
        failover.on_tick(&facts(1_000));
        assert_eq!(failover.phase(), AutomaticFailoverPhase::WaitingRankDelay);
        assert!(failover.on_topology_change(false, false).is_empty());
        assert_eq!(failover.phase(), AutomaticFailoverPhase::Idle);

        let mut nofailover = facts(2_000);
        nofailover.local.as_mut().unwrap().no_failover = true;
        assert!(failover.on_tick(&nofailover).is_empty());
        assert_eq!(failover.phase(), AutomaticFailoverPhase::Idle);
    }

    #[test]
    fn timed_out_manual_election_remains_visible_until_replaced() {
        let mut failover = AutomaticFailover::default();
        let mut manual = facts(1_000);
        manual.manual = true;
        assert!(failover.on_tick(&manual).is_empty());
        assert!(matches!(
            failover.on_tick(&manual).as_slice(),
            [ClusterAction::BroadcastFailoverAuthRequest { .. }]
        ));
        assert_eq!(failover.phase(), AutomaticFailoverPhase::RequestingVotes);

        let mut after_manual_timeout = manual;
        after_manual_timeout.now_ms = 6_001;
        after_manual_timeout.manual = false;
        after_manual_timeout.primary.as_mut().unwrap().is_failed = false;

        assert!(failover.on_tick(&after_manual_timeout).is_empty());
        assert_eq!(failover.phase(), AutomaticFailoverPhase::RequestingVotes);
    }

    #[test]
    fn primary_recovery_cancels_a_queued_promotion() {
        let mut failover = AutomaticFailover::default();
        failover.on_tick(&facts(1_000));
        failover.on_tick(&facts(1_000));
        failover.on_vote_ack(node_id(b'a'), 10, true);
        assert!(!failover.on_vote_ack(node_id(b'b'), 10, true).is_empty());
        assert_eq!(failover.phase(), AutomaticFailoverPhase::Promoting);

        let mut recovered = facts(1_001);
        recovered.primary.as_mut().unwrap().is_failed = false;
        assert!(failover.on_tick(&recovered).is_empty());
        assert_eq!(failover.phase(), AutomaticFailoverPhase::Idle);
    }

    #[test]
    fn disconnected_from_voters_preserves_a_queued_promotion() {
        let mut failover = AutomaticFailover::default();
        failover.on_tick(&facts(1_000));
        failover.on_tick(&facts(1_000));
        failover.on_vote_ack(node_id(b'a'), 10, true);
        assert!(!failover.on_vote_ack(node_id(b'b'), 10, true).is_empty());

        let mut disconnected = facts(1_001);
        disconnected.local.as_mut().unwrap().cluster_link_connected = false;
        assert!(failover.on_tick(&disconnected).is_empty());
        assert_eq!(failover.phase(), AutomaticFailoverPhase::Promoting);
    }

    #[test]
    fn vote_timeout_is_measured_from_request_broadcast() {
        let mut failover = AutomaticFailover::default();
        let mut initial = facts(1_000);
        initial.siblings.push(eligible_replica(
            b'a',
            initial.local.as_ref().unwrap().replication_offset + 1,
        ));

        assert!(failover.on_tick(&initial).is_empty());
        assert!(failover.deadline_ms() > initial.now_ms);
        let deadline = failover.deadline_ms();
        initial.now_ms = deadline;
        assert!(!failover.on_tick(&initial).is_empty());
        initial.now_ms = 11_001;
        assert!(failover.on_tick(&initial).is_empty());
        assert_eq!(failover.phase(), AutomaticFailoverPhase::RequestingVotes);
        initial.now_ms = deadline + 10_001;
        assert!(failover.on_tick(&initial).is_empty());
        assert_eq!(failover.phase(), AutomaticFailoverPhase::WaitingRankDelay);
    }

    #[test]
    fn expired_election_waits_for_retry_window_before_restarting() {
        let mut failover = AutomaticFailover::default();
        let mut current = facts(1_000);
        failover.on_tick(&current);
        failover.on_tick(&current);
        assert_eq!(failover.phase(), AutomaticFailoverPhase::RequestingVotes);

        current.now_ms = 11_001;
        assert!(failover.on_tick(&current).is_empty());
        assert_eq!(failover.phase(), AutomaticFailoverPhase::WaitingRankDelay);
        assert_eq!(failover.deadline_ms(), 21_000);

        current.now_ms = 20_999;
        assert!(failover.on_tick(&current).is_empty());
        assert_eq!(failover.phase(), AutomaticFailoverPhase::WaitingRankDelay);
        current.now_ms = 21_000;
        current.current_epoch = 10;
        assert_eq!(
            failover.on_tick(&current),
            vec![ClusterAction::BroadcastFailoverAuthRequest {
                election_epoch: 11,
                force_ack: true,
            }]
        );
    }

    #[test]
    fn expired_election_reapplies_replica_delay_after_retry_window() {
        let mut failover = AutomaticFailover::default();
        let mut current = facts(1_000);
        current.local.as_mut().unwrap().replication_offset = 0;
        current.siblings.push(eligible_replica(b'a', 43));
        current.all_replicas_primary_failed = false;

        assert!(failover.on_tick(&current).is_empty());
        let first_deadline = failover.deadline_ms();
        assert!((1_998..2_164).contains(&first_deadline));

        current.now_ms = first_deadline;
        assert!(matches!(
            failover.on_tick(&current).as_slice(),
            [ClusterAction::BroadcastFailoverAuthRequest { .. }]
        ));
        let request_started_at = current.now_ms;

        current.now_ms = request_started_at + 10_001;
        assert!(failover.on_tick(&current).is_empty());
        assert_eq!(failover.phase(), AutomaticFailoverPhase::WaitingRankDelay);
        let retry_delay = failover.deadline_ms() - request_started_at - 20_000;
        assert!(
            (998..1_164).contains(&retry_delay),
            "C reapplies propagation, rank and zero-offset delays after the retry window"
        );
    }

    #[test]
    fn retry_caches_current_ranks_without_adjusting_deadline_twice() {
        let mut failover = AutomaticFailover::default();
        let mut current = facts(1_000);

        assert!(failover.on_tick(&current).is_empty());
        current.now_ms = failover.deadline_ms();
        assert!(matches!(
            failover.on_tick(&current).as_slice(),
            [ClusterAction::BroadcastFailoverAuthRequest { .. }]
        ));

        current.local.as_mut().unwrap().replication_offset = 0;
        current.siblings.push(eligible_replica(b'a', 43));
        current.failed_primary_rank = 2;
        current.all_replicas_primary_failed = false;
        current.now_ms += 10_001;
        assert!(failover.on_tick(&current).is_empty());
        let retry_deadline = failover.deadline_ms();

        current.now_ms += 1;
        assert!(failover.on_tick(&current).is_empty());
        assert_eq!(
            failover.deadline_ms(),
            retry_deadline,
            "retry facts must be cached when the next attempt is scheduled"
        );
    }

    #[test]
    fn waiting_election_adjusts_delay_when_replica_rank_changes() {
        let mut failover = AutomaticFailover::default();
        let mut current = facts(1_000);
        let mut sibling = eligible_replica(b'a', 43);
        current.siblings.push(sibling.clone());
        failover.on_tick(&current);
        let delayed_deadline = failover.deadline_ms();
        assert_eq!(failover.rank(), 1);

        sibling.replication_offset = 41;
        current.siblings[1] = sibling;
        current.now_ms = 1_001;
        assert_eq!(
            failover.on_tick(&current),
            vec![ClusterAction::BroadcastFailoverAuthRequest {
                election_epoch: 10,
                force_ack: true,
            }]
        );

        assert_eq!(failover.rank(), 0);
        assert!(failover.deadline_ms() < delayed_deadline);
        assert_eq!(failover.phase(), AutomaticFailoverPhase::RequestingVotes);
    }

    #[test]
    fn better_sibling_election_restarts_lower_rank_delay_once() {
        let mut failover = AutomaticFailover::default();
        let mut current = facts(1_000);
        current.siblings.push(eligible_replica(b'a', 43));

        assert!(failover.on_tick(&current).is_empty());
        assert_eq!(failover.rank(), 1);
        let old_rank_deadline = failover.deadline_ms();

        assert!(failover.defer_for_better_replica_election(10, 1_100, current.node_timeout_ms,));
        let auth_deadline = failover.deadline_ms();
        assert_eq!(auth_deadline, 11_100);

        current.now_ms = old_rank_deadline.max(1_101);
        assert!(failover.on_tick(&current).is_empty());
        assert_eq!(failover.phase(), AutomaticFailoverPhase::WaitingRankDelay);

        assert!(!failover.defer_for_better_replica_election(10, 1_200, current.node_timeout_ms,));
        assert_eq!(failover.deadline_ms(), auth_deadline);

        current.now_ms = auth_deadline;
        assert!(matches!(
            failover.on_tick(&current).as_slice(),
            [ClusterAction::BroadcastFailoverAuthRequest { .. }]
        ));
    }

    #[test]
    fn rank_change_at_deadline_is_applied_before_requesting_votes() {
        let mut failover = AutomaticFailover::default();
        let mut current = facts(1_000);
        current.all_replicas_primary_failed = false;
        failover.on_tick(&current);
        let original_deadline = failover.deadline_ms();

        current.siblings.push(eligible_replica(b'a', 43));
        current.now_ms = original_deadline;
        assert!(failover.on_tick(&current).is_empty());
        assert_eq!(failover.phase(), AutomaticFailoverPhase::WaitingRankDelay);
        assert_eq!(failover.rank(), 1);
        assert!(failover.deadline_ms() > original_deadline);
    }

    #[test]
    fn simultaneous_primary_failures_keep_independent_elections() {
        let mut first = AutomaticFailover::default();
        let mut second = AutomaticFailover::default();
        let first_facts = facts(1_000);
        let mut second_facts = facts(1_000);
        second_facts.primary = Some(primary(b'q', true));

        first.on_tick(&first_facts);
        second.on_tick(&second_facts);

        assert_eq!(first.failed_primary(), Some(&node_id(b'p')));
        assert_eq!(second.failed_primary(), Some(&node_id(b'q')));
    }

    #[test]
    fn failed_primary_rank_staggers_simultaneous_elections() {
        let mut first = AutomaticFailover::default();
        let mut second = AutomaticFailover::default();
        let first_facts = facts(1_000);
        let mut second_facts = facts(1_000);
        second_facts.failed_primary_rank = 2;

        first.on_tick(&first_facts);
        second.on_tick(&second_facts);

        assert_eq!(first.deadline_ms(), 1_000);
        assert!((1_498..1_664).contains(&second.deadline_ms()));
    }

    #[test]
    fn voting_primary_grants_at_most_one_vote_per_epoch() {
        let mut failover = AutomaticFailover::default();
        let request = |requester, epoch| VoteRequestFacts {
            requester: node_id(requester),
            local_is_voting_primary: true,
            safe_to_vote: true,
            requester_is_replica: true,
            requester_primary_known: true,
            requester_primary_failed: true,
            force_ack: false,
            request_epoch: epoch,
            current_epoch: epoch,
            request_config_epoch: 7,
            maximum_claimed_slot_epoch: 7,
            persisted_last_vote_epoch: 0,
        };

        assert_eq!(
            failover.on_vote_request(&request(b'a', 10)),
            vec![
                ClusterAction::RecordVote { epoch: 10 },
                ClusterAction::SendFailoverAuthAck {
                    recipient: node_id(b'a'),
                    election_epoch: 10,
                },
            ]
        );
        assert!(failover.on_vote_request(&request(b'b', 10)).is_empty());
        assert!(failover.on_vote_request(&request(b'c', 9)).is_empty());
        assert_eq!(
            failover.on_vote_request(&request(b'd', 11)),
            vec![
                ClusterAction::RecordVote { epoch: 11 },
                ClusterAction::SendFailoverAuthAck {
                    recipient: node_id(b'd'),
                    election_epoch: 11,
                },
            ]
        );
    }

    #[test]
    fn higher_epoch_ack_counts_for_the_active_election() {
        let mut failover = AutomaticFailover::default();
        failover.on_tick(&facts(1_000));
        failover.on_tick(&facts(1_000));

        assert!(failover.on_vote_ack(node_id(b'a'), 11, true).is_empty());
        assert_eq!(
            failover.on_vote_ack(node_id(b'b'), 11, true),
            vec![ClusterAction::PromoteSelf(Promotion {
                old_primary: node_id(b'p'),
                shard_id: node_id(b's'),
                slots: {
                    let mut slots = vec![false; 16_384];
                    slots[7] = true;
                    slots
                },
                config_epoch: 10,
            })]
        );
    }

    #[test]
    fn exhausted_epoch_space_does_not_start_an_epoch_zero_election() {
        let mut failover = AutomaticFailover::default();
        let mut exhausted = facts(1_000);
        exhausted.current_epoch = u64::MAX;

        assert!(failover.on_tick(&exhausted).is_empty());
        assert!(failover.on_tick(&exhausted).is_empty());
        assert_eq!(failover.phase(), AutomaticFailoverPhase::Idle);
        assert_eq!(failover.election_epoch(), 0);
    }

    #[test]
    fn vote_request_requires_valid_role_epoch_primary_and_slot_claims() {
        let base = VoteRequestFacts {
            requester: node_id(b'a'),
            local_is_voting_primary: true,
            safe_to_vote: true,
            requester_is_replica: true,
            requester_primary_known: true,
            requester_primary_failed: true,
            force_ack: false,
            request_epoch: 10,
            current_epoch: 10,
            request_config_epoch: 7,
            maximum_claimed_slot_epoch: 7,
            persisted_last_vote_epoch: 0,
        };
        let mut cases = Vec::new();
        cases.push({
            let mut facts = base.clone();
            facts.local_is_voting_primary = false;
            facts
        });
        cases.push({
            let mut facts = base.clone();
            facts.safe_to_vote = false;
            facts
        });
        cases.push({
            let mut facts = base.clone();
            facts.requester_is_replica = false;
            facts
        });
        cases.push({
            let mut facts = base.clone();
            facts.requester_primary_known = false;
            facts
        });
        cases.push({
            let mut facts = base.clone();
            facts.requester_primary_failed = false;
            facts
        });
        cases.push({
            let mut facts = base.clone();
            facts.request_epoch = 9;
            facts
        });
        cases.push({
            let mut facts = base;
            facts.maximum_claimed_slot_epoch = 8;
            facts
        });

        for facts in cases {
            let mut failover = AutomaticFailover::default();
            assert!(failover.on_vote_request(&facts).is_empty(), "{facts:?}");
        }
    }
}
