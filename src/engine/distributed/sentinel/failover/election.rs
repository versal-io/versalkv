use std::collections::HashMap;

use crate::engine::distributed::sentinel::{
    FailoverState, SentinelInstance, SentinelState, SRI_FAILOVER_IN_PROGRESS,
};
use crate::foundation::io::sys::rand::random_usize;

pub(super) fn effective_election_timeout(election_timeout: i64, failover_timeout: i64) -> i64 {
    election_timeout.min(failover_timeout)
}

pub fn start_failover(
    current_epoch: &mut i64,
    debug_ask_period: i64,
    master: &mut SentinelInstance,
    now: i64,
) {
    *current_epoch += 1;
    master.failover_epoch = *current_epoch;
    master.failover_state = FailoverState::WaitStart;
    master.flags |= SRI_FAILOVER_IN_PROGRESS;
    master.failover_state_change_time = now;
    const SENTINEL_MAX_DESYNC: i64 = 1_000;
    let desync_delay = random_usize(SENTINEL_MAX_DESYNC as usize) as i64;
    master.failover_start_time = now + desync_delay;
    master.promoted_replica = None;
    master.leader = String::new();
    master.leader_epoch = 0;
    for peer_state in master.peer_states.values_mut() {
        peer_state.last_master_down_reply = now - debug_ask_period - 1;
    }
}

pub fn elect_leader(myid: &str, current_epoch: i64, master: &mut SentinelInstance) -> bool {
    let voters = master.sentinels.len() as i64 + 1;
    let required_votes = std::cmp::max(voters / 2 + 1, master.quorum);
    let mut counters: HashMap<String, i64> = HashMap::new();
    for sentinel_id in &master.sentinels {
        if let Some(peer_state) = master.peer_states.get(sentinel_id) {
            if !peer_state.leader.is_empty() && peer_state.leader_epoch == current_epoch {
                *counters.entry(peer_state.leader.clone()).or_insert(0) += 1;
            }
        }
    }
    let peer_winner = counters
        .iter()
        .max_by(|left, right| left.1.cmp(right.1).then_with(|| right.0.cmp(left.0)))
        .map(|(candidate, votes)| (candidate.clone(), *votes));
    let election_epoch = if current_epoch > master.failover_epoch {
        master.leader.clear();
        master.leader_epoch = 0;
        master.failover_epoch = current_epoch;
        current_epoch
    } else {
        master.failover_epoch
    };
    if master.leader_epoch < election_epoch && current_epoch <= election_epoch {
        match peer_winner {
            Some((candidate, _)) if candidate == myid => {
                master.leader = myid.to_string();
                master.leader_epoch = election_epoch;
            }
            Some((candidate, votes)) if votes >= required_votes => {
                master.leader = candidate;
                master.leader_epoch = election_epoch;
            }
            Some((candidate, _)) => {
                master.leader = candidate;
                master.leader_epoch = election_epoch;
            }
            None => {
                master.leader = myid.to_string();
                master.leader_epoch = election_epoch;
            }
        }
    }
    if master.leader_epoch == election_epoch && !master.leader.is_empty() {
        *counters.entry(master.leader.clone()).or_insert(0) += 1;
    }
    let (winner, votes) = match counters
        .iter()
        .max_by(|left, right| left.1.cmp(right.1).then_with(|| right.0.cmp(left.0)))
    {
        Some((candidate, votes)) => (candidate.clone(), *votes),
        None => return false,
    };
    crate::server_log!(
        notice,
        "ELECTION: winner={} votes={} required={} for master {} (epoch={}, sentinels={})",
        winner,
        votes,
        required_votes,
        master.name,
        current_epoch,
        master.sentinels.len()
    );
    if winner != myid || votes < required_votes {
        return false;
    }
    master.failover_state = FailoverState::SelectReplica;
    true
}

pub fn vote_for_leader(
    state: &mut SentinelState,
    master: &mut SentinelInstance,
    request_epoch: i64,
    candidate: &str,
    now: i64,
) -> (String, i64) {
    if request_epoch > state.current_epoch {
        state.current_epoch = request_epoch;
    }
    if candidate != "*"
        && master.leader_epoch < request_epoch
        && state.current_epoch <= request_epoch
    {
        master.leader = candidate.to_string();
        master.leader_epoch = request_epoch;
        if candidate != state.myid {
            const SENTINEL_MAX_DESYNC: i64 = 1_000;
            master.failover_start_time = now + random_usize(SENTINEL_MAX_DESYNC as usize) as i64;
        }
    }
    (master.leader.clone(), master.leader_epoch)
}

#[cfg(test)]
mod tests {
    use super::effective_election_timeout;

    #[test]
    fn election_timeout_is_capped_by_failover_timeout() {
        assert_eq!(effective_election_timeout(10_000, 3_000), 3_000);
        assert_eq!(effective_election_timeout(10_000, 30_000), 10_000);
    }
}
