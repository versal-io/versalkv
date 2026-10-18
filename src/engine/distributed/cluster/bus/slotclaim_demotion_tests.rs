use super::slotclaim_demotion::{
    decide_demotion, DemotionDecision, DemotionFacts, LocalClusterRole,
};
use crate::engine::distributed::cluster::state::ReplicationSwitchMode;

struct Case {
    name: &'static str,
    facts: DemotionFacts,
    expected: DemotionDecision,
}

#[test]
fn decisions_follow_migrated_current_primary_facts() {
    let cases = [
        Case {
            name: "fresh empty primary is not demoted by unrelated owner",
            facts: DemotionFacts {
                local_role: LocalClusterRole::Primary,
                current_primary_slots_after: 0,
                local_slots_after: 0,
                sender_slots: 100,
                migrated_current_primary_slots: 0,
                same_shard: false,
                sender_epoch_is_newer: false,
                local_ever_owned_slots: false,
                sender_was_local_replica: false,
                sender_wins_empty_tie_break: false,
                allow_replica_migration: true,
            },
            expected: DemotionDecision::None,
        },
        Case {
            name: "setup primary is not demoted only because a same-shard view converged",
            facts: DemotionFacts {
                local_role: LocalClusterRole::Primary,
                current_primary_slots_after: 0,
                local_slots_after: 0,
                sender_slots: 100,
                migrated_current_primary_slots: 0,
                same_shard: true,
                sender_epoch_is_newer: true,
                local_ever_owned_slots: true,
                sender_was_local_replica: false,
                sender_wins_empty_tie_break: false,
                allow_replica_migration: true,
            },
            expected: DemotionDecision::None,
        },
        Case {
            name: "same-shard primary migration tries partial sync",
            facts: DemotionFacts {
                local_role: LocalClusterRole::Primary,
                current_primary_slots_after: 0,
                local_slots_after: 0,
                sender_slots: 1,
                migrated_current_primary_slots: 1,
                same_shard: true,
                sender_epoch_is_newer: true,
                local_ever_owned_slots: true,
                sender_was_local_replica: true,
                sender_wins_empty_tie_break: false,
                allow_replica_migration: false,
            },
            expected: DemotionDecision::FollowSender {
                mode: ReplicationSwitchMode::TryPartial,
            },
        },
        Case {
            name: "cross-shard primary migration disabled leaves empty primary",
            facts: DemotionFacts {
                local_role: LocalClusterRole::Primary,
                current_primary_slots_after: 0,
                local_slots_after: 0,
                sender_slots: 1,
                migrated_current_primary_slots: 1,
                same_shard: false,
                sender_epoch_is_newer: true,
                local_ever_owned_slots: true,
                sender_was_local_replica: false,
                sender_wins_empty_tie_break: false,
                allow_replica_migration: false,
            },
            expected: DemotionDecision::BecomeEmptyPrimary,
        },
        Case {
            name: "cross-shard primary migration forces full sync when allowed",
            facts: DemotionFacts {
                local_role: LocalClusterRole::Primary,
                current_primary_slots_after: 0,
                local_slots_after: 0,
                sender_slots: 1,
                migrated_current_primary_slots: 1,
                same_shard: false,
                sender_epoch_is_newer: true,
                local_ever_owned_slots: true,
                sender_was_local_replica: false,
                sender_wins_empty_tie_break: false,
                allow_replica_migration: true,
            },
            expected: DemotionDecision::FollowSender {
                mode: ReplicationSwitchMode::ForceFull,
            },
        },
        Case {
            name: "same-shard replica follows replacement primary",
            facts: DemotionFacts {
                local_role: LocalClusterRole::Replica,
                current_primary_slots_after: 0,
                local_slots_after: 0,
                sender_slots: 1,
                migrated_current_primary_slots: 1,
                same_shard: true,
                sender_epoch_is_newer: true,
                local_ever_owned_slots: false,
                sender_was_local_replica: true,
                sender_wins_empty_tie_break: false,
                allow_replica_migration: false,
            },
            expected: DemotionDecision::FollowSender {
                mode: ReplicationSwitchMode::TryPartial,
            },
        },
        Case {
            name: "cross-shard replica stays on current shard when migration is disabled",
            facts: DemotionFacts {
                local_role: LocalClusterRole::Replica,
                current_primary_slots_after: 0,
                local_slots_after: 0,
                sender_slots: 1,
                migrated_current_primary_slots: 1,
                same_shard: false,
                sender_epoch_is_newer: true,
                local_ever_owned_slots: false,
                sender_was_local_replica: false,
                sender_wins_empty_tie_break: false,
                allow_replica_migration: false,
            },
            expected: DemotionDecision::None,
        },
        Case {
            name: "empty same-shard tie break follows sender",
            facts: DemotionFacts {
                local_role: LocalClusterRole::Primary,
                current_primary_slots_after: 0,
                local_slots_after: 0,
                sender_slots: 0,
                migrated_current_primary_slots: 0,
                same_shard: true,
                sender_epoch_is_newer: true,
                local_ever_owned_slots: true,
                sender_was_local_replica: false,
                sender_wins_empty_tie_break: true,
                allow_replica_migration: true,
            },
            expected: DemotionDecision::FollowSender {
                mode: ReplicationSwitchMode::TryPartial,
            },
        },
    ];

    for case in cases {
        assert_eq!(decide_demotion(case.facts), case.expected, "{}", case.name);
    }
}
