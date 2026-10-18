use std::collections::HashMap;

use crate::engine::distributed::sentinel::{
    DownState, FailoverState, InstanceRole, SentinelInstance, SentinelInstanceId,
    SRI_COORD_FAILOVER, SRI_FAILOVER_IN_PROGRESS, SRI_FORCE_FAILOVER,
};

mod election;
mod topology;

pub use election::{elect_leader, start_failover, vote_for_leader};
pub use topology::{find_misconfigured_replica, finish_failover, switch_primary_address};

const SENTINEL_PING_PERIOD_MS: i64 = 1_000;
const SENTINEL_INFO_PERIOD_MS: i64 = 10_000;
const SENTINEL_ELECTION_MIN_WAIT_MS: i64 = 1_000; //
const SENTINEL_ELECTION_RETRY_BACKOFF_MS: i64 = 5_000;

pub fn select_replica(
    master: &SentinelInstance,
    instances: &HashMap<SentinelInstanceId, SentinelInstance>,
    now: i64,
) -> Option<SentinelInstanceId> {
    let primary_is_down = matches!(master.down_state, DownState::SDown | DownState::ODown);
    let ping_period = if primary_is_down {
        SENTINEL_PING_PERIOD_MS * 5
    } else {
        SENTINEL_INFO_PERIOD_MS * 3
    };
    master
        .replicas
        .iter()
        .filter_map(|id| instances.get(id))
        .filter(|replica| {
            replica.down_state == DownState::Up
                && replica.slave_priority != 0
                && replica.link_state == 0
                && now - replica.last_ping_ok <= ping_period
                && now - replica.last_info_time
                    <= if primary_is_down {
                        SENTINEL_PING_PERIOD_MS * 5
                    } else {
                        SENTINEL_INFO_PERIOD_MS * 3
                    }
        })
        .min_by(|left, right| {
            left.slave_priority
                .cmp(&right.slave_priority)
                .then_with(|| right.repl_offset.cmp(&left.repl_offset))
                .then_with(|| left.runid.cmp(&right.runid))
        })
        .map(|replica| replica.id)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverAction {
    Promote(SentinelInstanceId),
    Reconfigure(SentinelInstanceId),
    SendFailover(SentinelInstanceId),
    HandoffClients(SentinelInstanceId),
}

pub fn promotion_sent(master: &mut SentinelInstance, now: i64) {
    master.failover_state = FailoverState::WaitPromotion;
    master.failover_state_change_time = now;
}

#[cfg(test)]
pub fn reconfiguration_sent(
    instances: &mut HashMap<SentinelInstanceId, SentinelInstance>,
    replica_id: SentinelInstanceId,
    now: i64,
) {
    if let Some(replica) = instances.get_mut(&replica_id) {
        replica.reconfig_sent = true;
        replica.reconfig_sent_time = now;
    }
}

#[cfg(test)]
pub(crate) fn should_crash_after_election(simfailure_flags: u32) -> bool {
    (simfailure_flags & 1) != 0
}

pub fn abort_failover(master: &mut SentinelInstance, now: i64) {
    master.failover_state = FailoverState::None;
    master.flags &= !(SRI_FAILOVER_IN_PROGRESS | SRI_FORCE_FAILOVER | SRI_COORD_FAILOVER);
    master.failover_state_change_time = now;
    master.promoted_replica = None;
}

pub fn advance_failover(
    myid: &str,
    current_epoch: i64,
    debug_election_timeout: i64,
    debug_replica_reconf_timeout: i64,
    simfailure_flags: u32,
    master: &mut SentinelInstance,
    instances: &HashMap<SentinelInstanceId, SentinelInstance>,
    now: i64,
) -> Option<FailoverAction> {
    match master.failover_state {
        FailoverState::WaitStart => wait_start(
            myid,
            current_epoch,
            debug_election_timeout,
            simfailure_flags,
            master,
            now,
        ),
        FailoverState::SelectReplica => {
            select_for_promotion(simfailure_flags, master, instances, now)
        }
        FailoverState::SendReplicaOfNoOne => send_replicaof_no_one(master, now),
        FailoverState::SendFailover => send_coordinated_failover(master, now),
        FailoverState::WaitPromotion => wait_promotion(master, instances, now),
        FailoverState::ReconfigureReplicas => {
            reconfigure_replicas(master, instances, debug_replica_reconf_timeout, now)
        }
        _ => None,
    }
}

fn wait_start(
    myid: &str,
    current_epoch: i64,
    election_timeout: i64,
    _simfailure_flags: u32,
    master: &mut SentinelInstance,
    now: i64,
) -> Option<FailoverAction> {
    if now < master.failover_start_time {
        return None;
    }
    let is_leader = master.leader == myid && master.leader_epoch == master.failover_epoch;
    if !is_leader && (master.flags & SRI_FORCE_FAILOVER) == 0 {
        let timeout =
            election::effective_election_timeout(election_timeout, master.failover_timeout);
        if now - master.failover_start_time > timeout {
            abort_failover(master, now);
        } else if now - master.failover_state_change_time >= SENTINEL_ELECTION_MIN_WAIT_MS {
            let elected = elect_leader(myid, current_epoch, master);
            master.failover_state_change_time = if elected {
                now
            } else {
                now.saturating_add(SENTINEL_ELECTION_RETRY_BACKOFF_MS)
            };
        }
    } else {
        master.failover_state = FailoverState::SelectReplica;
        master.failover_state_change_time = now;
    }
    None
}

fn select_for_promotion(
    _simfailure_flags: u32,
    master: &mut SentinelInstance,
    instances: &HashMap<SentinelInstanceId, SentinelInstance>,
    now: i64,
) -> Option<FailoverAction> {
    let Some(replica_id) = select_replica(master, instances, now) else {
        abort_failover(master, now);
        return None;
    };
    master.promoted_replica = Some(replica_id);
    let is_coordinated = master.flags & SRI_COORD_FAILOVER != 0;
    master.failover_state = if is_coordinated {
        FailoverState::SendFailover
    } else {
        FailoverState::SendReplicaOfNoOne
    };
    master.failover_state_change_time = now;
    Some(if is_coordinated {
        FailoverAction::SendFailover(master.id)
    } else {
        FailoverAction::Promote(replica_id)
    })
}

fn send_replicaof_no_one(master: &mut SentinelInstance, now: i64) -> Option<FailoverAction> {
    if failover_timed_out(master, now, "REPLICAOF NO ONE") {
        return None;
    }
    if master.flags & SRI_COORD_FAILOVER != 0 {
        master.failover_state = FailoverState::SendFailover;
        master.failover_state_change_time = now;
        Some(FailoverAction::SendFailover(master.id))
    } else {
        master.promoted_replica.map(FailoverAction::Promote)
    }
}

fn send_coordinated_failover(master: &mut SentinelInstance, now: i64) -> Option<FailoverAction> {
    (!failover_timed_out(master, now, "SEND_FAILOVER"))
        .then_some(FailoverAction::SendFailover(master.id))
}

fn wait_promotion(
    master: &mut SentinelInstance,
    instances: &HashMap<SentinelInstanceId, SentinelInstance>,
    now: i64,
) -> Option<FailoverAction> {
    if failover_timed_out(master, now, "promotion of replica") {
        return None;
    }
    let promoted = instances.get(&master.promoted_replica?)?;
    if promoted.role_reported == InstanceRole::Master {
        master.config_epoch = master.failover_epoch;
        master.failover_state = FailoverState::ReconfigureReplicas;
        master.failover_state_change_time = now;
        if master.flags & SRI_COORD_FAILOVER != 0 {
            return Some(FailoverAction::HandoffClients(promoted.id));
        }
    }
    None
}

fn reconfigure_replicas(
    master: &mut SentinelInstance,
    instances: &HashMap<SentinelInstanceId, SentinelInstance>,
    reconf_timeout: i64,
    now: i64,
) -> Option<FailoverAction> {
    let promoted_addr = master
        .promoted_replica
        .and_then(|id| instances.get(&id))
        .map(|instance| instance.addr.as_str());
    if let Some(replica_id) = master.replicas.iter().find_map(|id| {
        let replica = instances.get(id)?;
        let timed_out =
            replica.reconfig_sent_time > 0 && now - replica.reconfig_sent_time > reconf_timeout;
        (Some(*id) != master.promoted_replica
            && Some(replica.addr.as_str()) != promoted_addr
            && replica.down_state == DownState::Up
            && !replica_points_to_primary(replica, promoted_addr)
            && (!replica.reconfig_sent || timed_out))
            .then_some(*id)
    }) {
        return Some(FailoverAction::Reconfigure(replica_id));
    }
    let all_done = master.replicas.iter().all(|id| {
        Some(*id) == master.promoted_replica
            || instances.get(id).is_none_or(|replica| {
                Some(replica.addr.as_str()) == promoted_addr
                    || replica_points_to_primary(replica, promoted_addr)
                    || replica.down_state == DownState::SDown
            })
    });
    if all_done {
        master.failover_state = FailoverState::UpdateConfig;
        master.failover_state_change_time = now;
    }
    None
}

fn replica_points_to_primary(replica: &SentinelInstance, primary_addr: Option<&str>) -> bool {
    primary_addr.is_some_and(|primary_addr| {
        replica.role_reported == InstanceRole::Replica
            && replica.master_name == primary_addr
            && replica.master_link_status == "up"
    })
}

fn failover_timed_out(master: &mut SentinelInstance, now: i64, waiting_for: &str) -> bool {
    if now - master.failover_state_change_time <= master.failover_timeout {
        return false;
    }
    crate::server_log!(
        warning,
        "FAILOVER: timeout waiting for {} for {}",
        waiting_for,
        master.name
    );
    abort_failover(master, now);
    true
}

#[cfg(test)]
#[path = "failover/tests.rs"]
mod tests;
