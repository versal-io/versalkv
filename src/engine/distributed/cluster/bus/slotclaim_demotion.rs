use crate::engine::distributed::cluster::state::ReplicationSwitchMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LocalClusterRole {
    Primary,
    Replica,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DemotionFacts {
    pub local_role: LocalClusterRole,
    pub current_primary_slots_after: usize,
    pub local_slots_after: usize,
    pub sender_slots: usize,
    pub migrated_current_primary_slots: usize,
    pub same_shard: bool,
    pub sender_epoch_is_newer: bool,
    pub local_ever_owned_slots: bool,
    pub sender_was_local_replica: bool,
    pub sender_wins_empty_tie_break: bool,
    pub allow_replica_migration: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DemotionDecision {
    None,
    FollowSender { mode: ReplicationSwitchMode },
    BecomeEmptyPrimary,
}

pub(super) fn decide_demotion(f: DemotionFacts) -> DemotionDecision {
    if f.current_primary_slots_after != 0 {
        return DemotionDecision::None;
    }

    let mode = if f.same_shard {
        ReplicationSwitchMode::TryPartial
    } else {
        ReplicationSwitchMode::ForceFull
    };

    if f.migrated_current_primary_slots > 0 {
        if f.allow_replica_migration || f.same_shard {
            return DemotionDecision::FollowSender { mode };
        }
        if f.local_role == LocalClusterRole::Primary
            && f.sender_slots >= f.migrated_current_primary_slots
        {
            return DemotionDecision::BecomeEmptyPrimary;
        }
    }

    // A relayed topology update may remove the old primary's final slots
    // before the promoted primary's own announcement arrives. In that
    // ordering there are no slots left for this message to count as migrated,
    // but the newer same-shard owner is still authoritative. Restrict this
    // fallback to a primary that previously owned slots so cluster bootstrap
    // cannot attach a fresh empty primary to an arbitrary peer.
    if f.local_role == LocalClusterRole::Primary
        && f.local_slots_after == 0
        && f.sender_slots > 0
        && f.same_shard
        && f.sender_epoch_is_newer
        && f.local_ever_owned_slots
        && f.sender_was_local_replica
    {
        return DemotionDecision::FollowSender {
            mode: ReplicationSwitchMode::TryPartial,
        };
    }

    if f.local_slots_after == 0
        && f.sender_slots == 0
        && f.same_shard
        && f.sender_wins_empty_tie_break
    {
        return DemotionDecision::FollowSender {
            mode: ReplicationSwitchMode::TryPartial,
        };
    }

    DemotionDecision::None
}
