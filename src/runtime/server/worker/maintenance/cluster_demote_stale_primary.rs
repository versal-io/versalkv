use crate::engine::distributed::cluster::action::ReplicationSwitchMode;
use crate::engine::distributed::cluster::{ClusterState, DemotionReason, StalePrimaryDecision};
use crate::runtime::server::worker::ServerWorkerState;

const STALE_PRIMARY_GUARD_INTERVAL_MS: i64 = 1_000;

pub(super) fn should_run_stale_primary_guard(last_run_ms: &mut Option<i64>, now_ms: i64) -> bool {
    if last_run_ms.is_some_and(|last| now_ms.saturating_sub(last) < STALE_PRIMARY_GUARD_INTERVAL_MS)
    {
        return false;
    }
    *last_run_ms = Some(now_ms);
    true
}

fn short_name(name: &str) -> String {
    name.chars().take(8).collect()
}

fn log_demotion_reason(
    state: &ClusterState,
    target: &str,
    target_epoch: u64,
    overlap: usize,
    reason: &DemotionReason,
) {
    match reason {
        DemotionReason::ShardEpochSuperseder => crate::server_log!(
            notice,
            "[DEMOTE-GUARD] stale-primary shard-id superseder demotion: myself={} my_epoch={} shard={} winner={} w_epoch={} overlap_proxy={}",
            short_name(&state.myself.name),
            state.myself.config_epoch,
            short_name(&state.myself.shard_id),
            short_name(target),
            target_epoch,
            overlap
        ),
        DemotionReason::BitmapFreeStaleClaimant {
            ..
        } => log_bitmap_free_reason(state, target, target_epoch, reason),
        DemotionReason::SlotOverlap => {}
    }
}

fn log_bitmap_free_reason(
    state: &ClusterState,
    target: &str,
    target_epoch: u64,
    reason: &DemotionReason,
) {
    let DemotionReason::BitmapFreeStaleClaimant {
        slotted_primaries,
        distinct_shards,
        duplicate_shard,
        extra_claimant,
        lowest_epoch,
    } = reason
    else {
        return;
    };
    crate::server_log!(
        notice,
        "[DEMOTE-GUARD] bitmap-free stale-primary stale-claimant demotion: myself={} epoch={} slots={} slotted={} distinct_shards={} duplicate_sid={} extra_claimant={} lowest_epoch={} target={} t_epoch={}",
        short_name(&state.myself.name),
        state.myself.config_epoch,
        state.myself.numslots,
        slotted_primaries,
        distinct_shards,
        duplicate_shard,
        extra_claimant,
        lowest_epoch,
        short_name(target),
        target_epoch
    );
}

impl ServerWorkerState {
    pub(super) fn cluster_demote_stale_primary_guard(&mut self, state: &mut ClusterState) {
        let StalePrimaryDecision::FollowPrimary {
            target,
            reason,
            overlap,
        } = state.decide_stale_primary()
        else {
            return;
        };
        let Some(primary) = state.nodes.get(target.as_str()) else {
            return;
        };
        let target_epoch = primary.config_epoch;
        let target_address = (primary.ip().to_string(), primary.port());
        log_demotion_reason(state, target.as_str(), target_epoch, overlap, &reason);
        crate::server_log!(
            notice,
            "[DEMOTE-GUARD] stale-primary cron overlap demotion: myself={} my_epoch={} my_slots={} target={} tgt_epoch={} overlap={} cluster_state={:?}",
            short_name(&state.myself.name),
            state.myself.config_epoch,
            state.myself.numslots,
            short_name(target.as_str()),
            target_epoch,
            overlap,
            state.state
        );
        if self.follow_known_cluster_primary(
            state,
            target.as_str(),
            ReplicationSwitchMode::TryPartial,
        ) {
            crate::server_log!(
                notice,
                "Cluster stale-primary cron self-heal: stale-primary (overlap={}/{} slots) -> replica of {}:{}",
                overlap,
                state.myself.numslots,
                target_address.0,
                target_address.1
            );
        } else {
            crate::server_log!(
                notice,
                "[DEMOTE-GUARD] stale-primary cron replicate({:.8}) rejected",
                target.as_str()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::should_run_stale_primary_guard;
    use crate::engine::distributed::cluster::node::{
        ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY,
    };
    use crate::engine::distributed::cluster::state::ClusterState;
    use crate::runtime::server::worker::ServerWorkerState;

    #[test]
    fn stale_primary_guard_runs_immediately_then_at_a_bounded_interval() {
        let mut last_run_ms = None;

        assert!(should_run_stale_primary_guard(&mut last_run_ms, 10_000));
        assert!(!should_run_stale_primary_guard(&mut last_run_ms, 10_999));
        assert!(should_run_stale_primary_guard(&mut last_run_ms, 11_000));
    }

    #[test]
    fn remote_overlap_does_not_demote_an_unrelated_low_epoch_primary() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.myself.config_epoch = 1;
        state
            .add_slots_batch(&(0..1_000).collect::<Vec<_>>())
            .unwrap();

        for (id, epoch) in [
            ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 3),
            ("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 4),
        ] {
            let mut node =
                ClusterNode::new(id.to_string(), "127.0.0.1".to_string(), 6380, 16_380, true);
            node.flags &= !CLUSTER_NODE_MYSELF;
            node.flags |= CLUSTER_NODE_PRIMARY;
            node.config_epoch = epoch;
            for slot in 0..1_000 {
                node.slots[slot] = true;
            }
            node.numslots = 1_000;
            state.nodes.insert(id.to_string(), node);
        }

        runtime.cluster_demote_stale_primary_guard(&mut state);

        assert!(state.myself.is_primary());
        assert!(state.myself.slots[0]);
        assert!(state.pending_replication_switch.is_none());
    }
}
