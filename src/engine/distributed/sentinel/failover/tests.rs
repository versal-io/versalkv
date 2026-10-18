use super::{
    abort_failover, advance_failover, elect_leader, find_misconfigured_replica, finish_failover,
    promotion_sent, reconfiguration_sent, select_replica, should_crash_after_election,
    start_failover, switch_primary_address, vote_for_leader, FailoverAction,
};
use crate::engine::distributed::sentinel::{
    DownState, FailoverState, InstanceRole, SentinelInstance, SentinelPeerMasterState,
    SentinelState, MONITORED_INSTANCE_FAILOVER, SRI_COORD_FAILOVER, SRI_FAILOVER_IN_PROGRESS,
    SRI_FORCE_FAILOVER, SRI_PRIMARY_REBOOT,
};
use std::collections::HashMap;

fn replica(
    id: u64,
    priority: i64,
    offset: i64,
    runid: &str,
    down_state: DownState,
) -> SentinelInstance {
    let mut instance = SentinelInstance::new_master(id, runid, "127.0.0.1:6380", 1);
    instance.role = InstanceRole::Replica;
    instance.role_reported = InstanceRole::Replica;
    instance.slave_priority = priority;
    instance.repl_offset = offset;
    instance.runid = runid.to_string();
    instance.down_state = down_state;
    instance.last_ping_ok = 10_000;
    instance.last_info_time = 10_000;
    instance
}

#[test]
fn select_replica_prefers_priority_offset_and_runid() {
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
    master.replicas = vec![2, 3, 4, 5];

    let mut instances = HashMap::new();
    instances.insert(2, replica(2, 100, 120, "z-runid", DownState::Up));
    instances.insert(3, replica(3, 100, 120, "a-runid", DownState::Up));
    instances.insert(4, replica(4, 50, 100, "late", DownState::Up));
    instances.insert(5, replica(5, 1, 1000, "down", DownState::SDown));

    assert_eq!(select_replica(&master, &instances, 10_000), Some(4));
}

#[test]
fn select_replica_treats_odown_primary_as_subjectively_down() {
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
    master.down_state = DownState::ODown;
    master.replicas = vec![2, 3];

    let mut instances = HashMap::new();
    let mut stale_replica = replica(2, 1, 200, "stale", DownState::Up);
    stale_replica.last_ping_ok = 4_999;
    stale_replica.last_info_time = 8_000;
    instances.insert(2, stale_replica);

    let mut healthy_replica = replica(3, 100, 100, "healthy", DownState::Up);
    healthy_replica.last_ping_ok = 9_000;
    healthy_replica.last_info_time = 9_000;
    instances.insert(3, healthy_replica);

    assert_eq!(select_replica(&master, &instances, 10_000), Some(3));
}

#[test]
fn start_failover_increments_epoch_and_elects_local_sentinel() {
    let mut state = SentinelState::new();
    state.myid = "local-sentinel".to_string();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);

    start_failover(
        &mut state.current_epoch,
        state.tunables.ask_period_ms,
        &mut master,
        10_000,
    );

    assert_eq!(state.current_epoch, 1);
    assert_eq!(master.failover_epoch, 1);
    assert_eq!(master.failover_state, FailoverState::WaitStart);
    assert!(elect_leader(&state.myid, state.current_epoch, &mut master));
    assert_eq!(master.failover_state, FailoverState::SelectReplica);
    assert_eq!(master.leader, "local-sentinel");
}

#[test]
fn elected_leader_selects_replica_and_requests_promotion() {
    let state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
    master.failover_state = FailoverState::SelectReplica;
    master.replicas = vec![2];
    let mut instances = HashMap::new();
    instances.insert(2, replica(2, 100, 10, "replica", DownState::Up));

    assert_eq!(
        advance_failover(
            &state.myid,
            state.current_epoch,
            state.tunables.election_timeout_ms,
            state.tunables.replica_reconf_timeout_ms,
            state.simfailure_flags,
            &mut master,
            &instances,
            10_000
        ),
        Some(FailoverAction::Promote(2))
    );
    assert_eq!(master.promoted_replica, Some(2));
    assert_eq!(master.failover_state, FailoverState::SendReplicaOfNoOne);
}

#[test]
fn select_replica_aborts_when_no_candidate_is_available() {
    let state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
    master.failover_state = FailoverState::SelectReplica;
    master.flags |= SRI_FAILOVER_IN_PROGRESS;

    assert_eq!(
        advance_failover(
            &state.myid,
            state.current_epoch,
            state.tunables.election_timeout_ms,
            state.tunables.replica_reconf_timeout_ms,
            state.simfailure_flags,
            &mut master,
            &HashMap::new(),
            10_000
        ),
        None
    );
    assert_eq!(master.failover_state, FailoverState::None);
    assert_eq!(master.flags & SRI_FAILOVER_IN_PROGRESS, 0);
}

#[test]
fn coordinated_failover_honors_crash_after_election() {
    assert!(should_crash_after_election(1));
}

#[test]
fn coordinated_promotion_confirmation_requests_client_handoff() {
    let state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
    master.flags |= SRI_COORD_FAILOVER | SRI_FAILOVER_IN_PROGRESS;
    master.failover_state = FailoverState::WaitPromotion;
    master.promoted_replica = Some(2);

    let mut instances = HashMap::new();
    let mut promoted = replica(2, 100, 10, "promoted", DownState::Up);
    promoted.role_reported = InstanceRole::Master;
    instances.insert(2, promoted);

    let action = advance_failover(
        &state.myid,
        state.current_epoch,
        state.tunables.election_timeout_ms,
        state.tunables.replica_reconf_timeout_ms,
        state.simfailure_flags,
        &mut master,
        &instances,
        10_000,
    );

    assert_eq!(action, Some(FailoverAction::HandoffClients(2)));
    assert_eq!(master.failover_state, FailoverState::ReconfigureReplicas);
}

#[test]
fn elected_leader_waits_for_desynchronized_start_time() {
    let state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
    master.failover_state = FailoverState::WaitStart;
    master.failover_epoch = 7;
    master.failover_start_time = 11_000;
    master.leader = state.myid.clone();
    master.leader_epoch = 7;

    assert_eq!(
        advance_failover(
            &state.myid,
            state.current_epoch,
            state.tunables.election_timeout_ms,
            state.tunables.replica_reconf_timeout_ms,
            state.simfailure_flags,
            &mut master,
            &HashMap::new(),
            10_500,
        ),
        None
    );
    assert_eq!(master.failover_state, FailoverState::WaitStart);
}

#[test]
fn failed_election_recheck_is_rate_limited() {
    let state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 3);
    master.failover_state = FailoverState::WaitStart;
    master.failover_epoch = 7;
    master.failover_start_time = 10_000;
    master.failover_state_change_time = 10_000;
    master.sentinels = vec![2, 3, 4, 5];

    assert_eq!(
        advance_failover(
            &state.myid,
            state.current_epoch,
            state.tunables.election_timeout_ms,
            state.tunables.replica_reconf_timeout_ms,
            state.simfailure_flags,
            &mut master,
            &HashMap::new(),
            11_000,
        ),
        None
    );
    assert_eq!(master.failover_state_change_time, 16_000);
}

#[test]
fn abort_preserves_start_time_to_back_off_the_next_election() {
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
    master.failover_state = FailoverState::WaitStart;
    master.flags |= SRI_FAILOVER_IN_PROGRESS | SRI_FORCE_FAILOVER | SRI_COORD_FAILOVER;
    master.failover_start_time = 10_000;
    master.leader = "stale-leader".to_string();
    master.leader_epoch = 7;

    abort_failover(&mut master, 11_000);

    assert_eq!(master.failover_start_time, 10_000);
    assert_eq!(master.leader, "stale-leader");
    assert_eq!(master.leader_epoch, 7);
    assert_eq!(
        master.flags & (SRI_FAILOVER_IN_PROGRESS | SRI_FORCE_FAILOVER | SRI_COORD_FAILOVER),
        0
    );
}

#[test]
fn confirmed_promotion_reconfigures_replicas_and_switches_master() {
    let state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
    master.promoted_replica = Some(2);
    master.failover_epoch = 7;
    master.last_hello_sent = 10_000;
    master.down_after_ms = 1_000;
    master.replicas = vec![2, 3];
    let mut instances = HashMap::new();
    let mut old_primary = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
    old_primary.runid = "old-primary".to_string();
    old_primary.role_reported_time = 1_000;
    old_primary.flags = SRI_PRIMARY_REBOOT;
    old_primary.down_state = DownState::ODown;
    instances.insert(1, old_primary);
    let mut promoted = replica(2, 100, 10, "promoted", DownState::Up);
    promoted.role = InstanceRole::Master;
    promoted.role_reported = InstanceRole::Master;
    promoted.addr = "127.0.0.1:6380".to_string();
    instances.insert(2, promoted);
    let mut follower = replica(3, 100, 10, "follower", DownState::Up);
    follower.addr = "127.0.0.1:6381".to_string();
    instances.insert(3, follower);

    promotion_sent(&mut master, 10_000);
    assert_eq!(
        advance_failover(
            &state.myid,
            state.current_epoch,
            state.tunables.election_timeout_ms,
            state.tunables.replica_reconf_timeout_ms,
            state.simfailure_flags,
            &mut master,
            &instances,
            10_001
        ),
        None
    );
    assert_eq!(master.failover_state, FailoverState::ReconfigureReplicas);
    assert_eq!(
        advance_failover(
            &state.myid,
            state.current_epoch,
            state.tunables.election_timeout_ms,
            state.tunables.replica_reconf_timeout_ms,
            state.simfailure_flags,
            &mut master,
            &instances,
            10_002
        ),
        Some(FailoverAction::Reconfigure(3))
    );

    reconfiguration_sent(&mut instances, 3, 10_003);
    assert_eq!(
        advance_failover(
            &state.myid,
            state.current_epoch,
            state.tunables.election_timeout_ms,
            state.tunables.replica_reconf_timeout_ms,
            state.simfailure_flags,
            &mut master,
            &instances,
            10_003
        ),
        None
    );
    assert_eq!(master.failover_state, FailoverState::ReconfigureReplicas);

    let follower = instances.get_mut(&3).unwrap();
    follower.master_name = "127.0.0.1:6380".to_string();
    follower.master_link_status = "up".to_string();
    assert_eq!(
        advance_failover(
            &state.myid,
            state.current_epoch,
            state.tunables.election_timeout_ms,
            state.tunables.replica_reconf_timeout_ms,
            state.simfailure_flags,
            &mut master,
            &instances,
            10_004
        ),
        None
    );
    assert_eq!(master.failover_state, FailoverState::UpdateConfig);
    finish_failover(&mut master, &mut instances, 10_004);
    assert_eq!(master.addr, "127.0.0.1:6380");
    assert_eq!(master.config_epoch, 7);
    assert_eq!(master.failover_state, FailoverState::None);
    let mirrored_master = instances.get(&2).unwrap();
    assert_eq!(mirrored_master.addr, master.addr);
    assert_eq!(mirrored_master.config_epoch, master.config_epoch);
    assert_eq!(mirrored_master.replicas, master.replicas);
    assert_eq!(master.last_hello_sent, 0);
    assert_eq!(master.down_state, DownState::Up);
    assert_eq!(master.sdown_since_time, 0);
    assert_eq!(master.odown_since_time, 0);
    assert_eq!(master.replicas, vec![3, 1]);
    let old_primary = instances.get(&1).unwrap();
    assert_eq!(old_primary.role, InstanceRole::Replica);
    assert_eq!(old_primary.role_reported, InstanceRole::Replica);
    assert_eq!(old_primary.role_reported_time, 10_004);
    assert!(old_primary.runid.is_empty());
    assert_eq!(old_primary.flags, 0);
    assert_eq!(old_primary.down_state, DownState::Up);
    assert!(old_primary.master_name.is_empty());
    assert_eq!(instances.get(&2).unwrap().down_after_ms, 1_000);
    assert!(!master.replicas.contains(&2));
}

#[test]
fn reconfiguration_retries_until_info_confirms_new_primary() {
    let state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
    master.promoted_replica = Some(2);
    master.failover_state = FailoverState::ReconfigureReplicas;
    master.replicas = vec![2, 3];
    let mut instances = HashMap::new();
    let mut promoted = replica(2, 100, 10, "promoted", DownState::Up);
    promoted.addr = "127.0.0.1:6380".to_string();
    promoted.role = InstanceRole::Master;
    promoted.role_reported = InstanceRole::Master;
    instances.insert(2, promoted);
    let mut follower = replica(3, 100, 10, "follower", DownState::Up);
    follower.addr = "127.0.0.1:6381".to_string();
    instances.insert(3, follower);

    assert_eq!(
        advance_failover(
            &state.myid,
            state.current_epoch,
            state.tunables.election_timeout_ms,
            1_000,
            state.simfailure_flags,
            &mut master,
            &instances,
            10_000
        ),
        Some(FailoverAction::Reconfigure(3))
    );
    reconfiguration_sent(&mut instances, 3, 10_000);
    assert_eq!(
        advance_failover(
            &state.myid,
            state.current_epoch,
            state.tunables.election_timeout_ms,
            1_000,
            state.simfailure_flags,
            &mut master,
            &instances,
            10_500
        ),
        None
    );
    assert_eq!(master.failover_state, FailoverState::ReconfigureReplicas);
    assert_eq!(
        advance_failover(
            &state.myid,
            state.current_epoch,
            state.tunables.election_timeout_ms,
            1_000,
            state.simfailure_flags,
            &mut master,
            &instances,
            11_001
        ),
        Some(FailoverAction::Reconfigure(3))
    );
}

#[test]
fn reconfiguration_waits_for_replica_link_up() {
    let state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
    master.promoted_replica = Some(2);
    master.failover_state = FailoverState::ReconfigureReplicas;
    master.replicas = vec![2, 3];

    let mut instances = HashMap::new();
    let mut promoted = replica(2, 100, 10, "promoted", DownState::Up);
    promoted.addr = "127.0.0.1:6380".to_string();
    promoted.role = InstanceRole::Master;
    promoted.role_reported = InstanceRole::Master;
    instances.insert(2, promoted);

    let mut follower = replica(3, 100, 10, "follower", DownState::Up);
    follower.addr = "127.0.0.1:6381".to_string();
    follower.master_name = "127.0.0.1:6380".to_string();
    follower.master_link_status = "down".to_string();
    instances.insert(3, follower);

    assert_eq!(
        advance_failover(
            &state.myid,
            state.current_epoch,
            state.tunables.election_timeout_ms,
            state.tunables.replica_reconf_timeout_ms,
            state.simfailure_flags,
            &mut master,
            &instances,
            10_000
        ),
        Some(FailoverAction::Reconfigure(3))
    );

    instances.get_mut(&3).unwrap().master_link_status = "up".to_string();

    assert_eq!(
        advance_failover(
            &state.myid,
            state.current_epoch,
            state.tunables.election_timeout_ms,
            state.tunables.replica_reconf_timeout_ms,
            state.simfailure_flags,
            &mut master,
            &instances,
            10_100
        ),
        None
    );
    assert_eq!(master.failover_state, FailoverState::UpdateConfig);
}

#[test]
fn reconfiguration_skips_duplicate_entry_for_promoted_address() {
    let state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
    master.promoted_replica = Some(2);
    master.failover_state = FailoverState::ReconfigureReplicas;
    master.replicas = vec![2, 4, 3];

    let mut instances = HashMap::new();
    let mut promoted = replica(2, 100, 10, "promoted", DownState::Up);
    promoted.addr = "127.0.0.1:6380".to_string();
    promoted.role = InstanceRole::Master;
    promoted.role_reported = InstanceRole::Master;
    instances.insert(2, promoted);

    let mut duplicate = replica(4, 100, 10, "duplicate", DownState::Up);
    duplicate.addr = "127.0.0.1:6380".to_string();
    instances.insert(4, duplicate);
    let mut follower = replica(3, 100, 10, "follower", DownState::Up);
    follower.addr = "127.0.0.1:6381".to_string();
    instances.insert(3, follower);

    assert_eq!(
        advance_failover(
            &state.myid,
            state.current_epoch,
            state.tunables.election_timeout_ms,
            state.tunables.replica_reconf_timeout_ms,
            state.simfailure_flags,
            &mut master,
            &instances,
            10_000
        ),
        Some(FailoverAction::Reconfigure(3))
    );
}

#[test]
fn finds_recovered_old_primary_that_must_follow_current_primary() {
    let mut master = SentinelInstance::new_master(2, "primary", "127.0.0.1:6380", 2);
    master.config_epoch = 1;
    master.replicas = vec![1, 3];
    let mut instances = HashMap::new();
    let mut current_primary = SentinelInstance::new_master(2, "primary", "127.0.0.1:6380", 2);
    current_primary.last_ping_ok = 10_000;
    instances.insert(2, current_primary);

    let mut old_primary = SentinelInstance::new_master(1, "127.0.0.1:6379", "127.0.0.1:6379", 2);
    old_primary.role = InstanceRole::Master;
    old_primary.role_reported = InstanceRole::Master;
    old_primary.role_reported_time = 1_000;
    old_primary.down_state = DownState::Up;
    instances.insert(1, old_primary);

    let mut healthy_replica = replica(3, 100, 10, "replica", DownState::Up);
    healthy_replica.master_name = "127.0.0.1:6380".to_string();
    instances.insert(3, healthy_replica);

    assert_eq!(
        find_misconfigured_replica(&master, &instances, 10_100),
        Some(1)
    );
}

#[test]
fn switching_primary_clears_reconfiguration_from_previous_topology() {
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
    master.config_epoch = 1;
    master.replicas = vec![2, 3];
    master.failover_start_time = 9_000;
    let mut instances = HashMap::new();
    instances.insert(1, master.clone());

    let mut promoted = SentinelInstance::new_master(2, "promoted", "127.0.0.1:6380", 2);
    promoted.role = InstanceRole::Master;
    promoted.role_reported = InstanceRole::Master;
    instances.insert(2, promoted);

    let mut replica = replica(3, 100, 10, "replica", DownState::Up);
    replica.master_name = "127.0.0.1:6379".to_string();
    replica.reconfig_sent = true;
    replica.reconfig_sent_time = 9_000;
    instances.insert(3, replica);

    assert!(switch_primary_address(
        &mut master,
        &mut instances,
        2,
        10_000
    ));

    let replica = &instances[&3];
    assert_eq!(master.failover_start_time, 0);
    assert!(!replica.reconfig_sent);
    assert_eq!(replica.reconfig_sent_time, 0);
    assert_eq!(
        find_misconfigured_replica(&master, &instances, 10_000),
        None
    );
}

#[test]
fn waits_for_reported_master_role_to_stabilize_before_reconfiguring() {
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
    master.config_epoch = 1;
    master.replicas = vec![2];
    let mut instances = HashMap::new();
    let mut current_primary = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
    current_primary.last_ping_ok = 10_000;
    instances.insert(1, current_primary);
    let mut reported_master = SentinelInstance::new_master(2, "replica", "127.0.0.1:6380", 2);
    reported_master.role = InstanceRole::Master;
    reported_master.role_reported = InstanceRole::Master;
    reported_master.role_reported_time = 9_000;
    instances.insert(2, reported_master);

    assert_eq!(
        find_misconfigured_replica(&master, &instances, 10_100),
        None
    );
    assert_eq!(
        find_misconfigured_replica(&master, &instances, 17_100),
        Some(2)
    );
}

#[test]
fn stale_epoch_zero_topology_does_not_demote_reported_master() {
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
    master.replicas = vec![2];
    let mut instances = HashMap::new();
    let mut current_primary = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
    current_primary.last_ping_ok = 10_000;
    instances.insert(1, current_primary);
    let mut reported_master = SentinelInstance::new_master(2, "replica", "127.0.0.1:6380", 2);
    reported_master.role = InstanceRole::Replica;
    reported_master.role_reported = InstanceRole::Master;
    reported_master.role_reported_time = 9_000;
    instances.insert(2, reported_master);

    assert_eq!(
        find_misconfigured_replica(&master, &instances, 17_100),
        None
    );
}

#[test]
fn stale_epoch_zero_topology_does_not_repair_loaded_replica_state() {
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
    master.config_epoch = 0;
    master.replicas = vec![2];
    let mut instances = HashMap::new();
    let mut current_primary = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
    current_primary.last_ping_ok = 10_000;
    instances.insert(1, current_primary);
    let mut loaded_replica = replica(2, 100, 10, "loaded-replica", DownState::Up);
    loaded_replica.master_name = "primary".to_string();
    instances.insert(2, loaded_replica);

    assert_eq!(
        find_misconfigured_replica(&master, &instances, 17_100),
        None
    );
}

#[test]
fn does_not_reconfigure_promoted_replica_toward_a_down_stale_primary() {
    let mut stale_master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
    stale_master.down_state = DownState::SDown;
    stale_master.replicas = vec![2];
    let mut instances = HashMap::new();
    let mut old_primary = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
    old_primary.down_state = DownState::SDown;
    old_primary.last_ping_ok = 9_000;
    instances.insert(1, old_primary);
    let mut promoted = SentinelInstance::new_master(2, "promoted", "127.0.0.1:6380", 2);
    promoted.role = InstanceRole::Master;
    promoted.role_reported = InstanceRole::Master;
    instances.insert(2, promoted);

    assert_eq!(
        find_misconfigured_replica(&stale_master, &instances, 10_100),
        None
    );
}

#[test]
fn finds_replica_stuck_in_failover_after_timeout() {
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
    master.failover_timeout = 10_000;
    master.replicas = vec![2];
    let mut instances = HashMap::new();
    let mut replica = replica(2, 100, 10, "replica", DownState::Up);
    replica.monitored_instance_failover_state = MONITORED_INSTANCE_FAILOVER;
    replica.monitored_instance_failover_change_time = 1_000;
    instances.insert(2, replica);

    assert_eq!(
        find_misconfigured_replica(&master, &instances, 10_999),
        None
    );
    assert_eq!(
        find_misconfigured_replica(&master, &instances, 11_000),
        Some(2)
    );
}

#[test]
fn vote_for_leader_is_single_vote_per_epoch() {
    let mut state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);

    let first = vote_for_leader(&mut state, &mut master, 9, "candidate-a", 10_000);
    let second = vote_for_leader(&mut state, &mut master, 9, "candidate-b", 11_000);

    assert_eq!(first, ("candidate-a".to_string(), 9));
    assert_eq!(second, ("candidate-a".to_string(), 9));
    assert_eq!(state.current_epoch, 9);
}

#[test]
fn voting_for_peer_delays_local_failover_retry() {
    let mut state = SentinelState::new();
    state.myid = "self".to_string();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);

    vote_for_leader(&mut state, &mut master, 9, "peer", 10_000);

    assert!(master.failover_start_time >= 10_000);
    assert!(master.failover_start_time < 11_000);
}

#[test]
fn elect_leader_requires_peer_majority_votes() {
    let mut state = SentinelState::new();
    state.myid = "self".to_string();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
    master.failover_epoch = 7;
    master.sentinels = vec![2, 3];
    master.peer_states.insert(
        2,
        SentinelPeerMasterState {
            reported_master_down: false,
            last_master_down_reply: 0,
            leader: "self".to_string(),
            leader_epoch: 7,
        },
    );
    state
        .registry
        .insert_instance(SentinelInstance::new_master(
            2,
            "peer-a",
            "127.0.0.1:26380",
            1,
        ))
        .unwrap();
    state
        .registry
        .insert_instance(SentinelInstance::new_master(
            3,
            "peer-b",
            "127.0.0.1:26381",
            1,
        ))
        .unwrap();

    assert!(elect_leader(&state.myid, 7, &mut master));
    assert_eq!(master.failover_state, FailoverState::SelectReplica);
}

#[test]
fn elect_leader_counts_an_existing_local_vote() {
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 3);
    master.failover_epoch = 7;
    master.leader = "self".to_string();
    master.leader_epoch = 7;
    master.sentinels = vec![2, 3, 4, 5];
    for peer_id in [2, 3] {
        master.peer_states.insert(
            peer_id,
            SentinelPeerMasterState {
                reported_master_down: true,
                last_master_down_reply: 10_000,
                leader: "self".to_string(),
                leader_epoch: 7,
            },
        );
    }

    assert!(elect_leader("self", 7, &mut master));
    assert_eq!(master.failover_state, FailoverState::SelectReplica);
}

#[test]
fn failed_election_preserves_the_local_vote_for_the_epoch() {
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 3);
    master.failover_epoch = 7;
    master.sentinels = vec![2, 3, 4, 5];

    assert!(!elect_leader("self", 7, &mut master));
    assert_eq!(master.leader, "self");
    assert_eq!(master.leader_epoch, 7);
}

#[test]
fn elect_leader_follows_the_peer_plurality_before_casting_local_vote() {
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 3);
    master.failover_epoch = 7;
    master.sentinels = vec![2, 3, 4, 5];
    for peer_id in [2, 3] {
        master.peer_states.insert(
            peer_id,
            SentinelPeerMasterState {
                reported_master_down: true,
                last_master_down_reply: 10_000,
                leader: "peer-winner".to_string(),
                leader_epoch: 7,
            },
        );
    }

    assert!(!elect_leader("self", 7, &mut master));
    assert_eq!(master.leader, "peer-winner");
    assert_eq!(master.leader_epoch, 7);
    assert_eq!(master.failover_state, FailoverState::None);
}

#[test]
fn elect_leader_uses_votes_for_the_current_master_only() {
    let mut state = SentinelState::new();
    state.myid = "self".to_string();
    state
        .registry
        .insert_instance(SentinelInstance::new_master(
            2,
            "peer-a",
            "127.0.0.1:26380",
            1,
        ))
        .unwrap();

    let mut master_without_vote = SentinelInstance::new_master(1, "primary-a", "127.0.0.1:6379", 2);
    master_without_vote.failover_epoch = 9;
    master_without_vote.sentinels = vec![2];

    let mut master_with_vote = SentinelInstance::new_master(3, "primary-b", "127.0.0.1:6380", 2);
    master_with_vote.failover_epoch = 9;
    master_with_vote.sentinels = vec![2];
    master_with_vote.peer_states.insert(
        2,
        SentinelPeerMasterState {
            reported_master_down: true,
            last_master_down_reply: 10_000,
            leader: "self".to_string(),
            leader_epoch: 9,
        },
    );

    assert!(!elect_leader(&state.myid, 9, &mut master_without_vote));
    assert!(elect_leader(&state.myid, 9, &mut master_with_vote));
}

#[test]
fn newer_epoch_restarts_local_vote_for_a_stale_failover() {
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
    master.failover_epoch = 3;
    master.sentinels = vec![2, 3];
    for peer_id in [2, 3] {
        master.peer_states.insert(
            peer_id,
            SentinelPeerMasterState {
                reported_master_down: true,
                last_master_down_reply: 10_000,
                leader: "self".to_string(),
                leader_epoch: 4,
            },
        );
    }

    assert!(elect_leader("self", 4, &mut master));
    assert_eq!(master.leader, "self");
    assert_eq!(master.leader_epoch, 4);
    assert_eq!(master.failover_state, FailoverState::SelectReplica);
}
