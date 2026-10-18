use super::{ReplicationOffset, ReplicationRole, ReplicationState};

fn offset(value: i64) -> ReplicationOffset {
    ReplicationOffset::try_from(value).expect("valid test offset")
}

#[test]
fn replication_role_is_canonical_runtime_state() {
    let mut state = ReplicationState::new("replid".to_string(), 16);
    assert_eq!(state.role, ReplicationRole::Primary);
    assert!(!state.is_replica());

    state.role = ReplicationRole::Replica;
    assert!(state.is_replica());
}

#[test]
fn switching_primary_to_replica_preserves_the_known_stream_offset() {
    let mut state = ReplicationState::new("replid".to_string(), 16);
    state.backlog.activate();
    state.backlog.append(&[b'x'; 42]).unwrap();
    state.slave_repl_offset = offset(7);
    state.slave_read_repl_offset = offset(7);
    state.observe_primary_disconnected(200);

    state.switch_to_replica();

    assert_eq!(state.role, ReplicationRole::Replica);
    assert_eq!(state.slave_repl_offset, offset(42));
    assert_eq!(state.slave_read_repl_offset, offset(42));
    assert_eq!(state.primary_data_age_ms(1_000), 0);
}

#[test]
fn role_changes_restart_backlog_expiry_timing() {
    let mut state = ReplicationState::new("replid".to_string(), 16);
    state.backlog_idle_deadline_ms = 123;

    state.switch_to_replica();

    assert_eq!(state.backlog_idle_deadline_ms, 0);
    state.backlog_idle_deadline_ms = 456;

    assert!(state.switch_to_primary());

    assert_eq!(state.backlog_idle_deadline_ms, 0);
}

#[test]
fn primary_data_age_tracks_connected_and_disconnected_observations() {
    let mut state = ReplicationState::new("replid".to_string(), 16);

    assert_eq!(state.primary_data_age_ms(1_000), 0);
    state.observe_primary_connected();
    state.observe_primary_disconnected(700);
    assert_eq!(state.primary_data_age_ms(1_000), 300);
    state.observe_primary_disconnected(1_100);
    assert_eq!(state.primary_data_age_ms(1_600), 900);
    state.observe_primary_connected();
    assert_eq!(state.primary_data_age_ms(2_000), 0);
}

#[test]
fn never_observed_replica_is_stale_until_a_link_disconnect_or_connection() {
    let mut state = ReplicationState::new("replid".to_string(), 16);
    state.switch_to_replica();

    assert_eq!(state.primary_failover_data_age_ms(1_000), u64::MAX);

    state.observe_primary_disconnected(1_500);
    assert_eq!(state.primary_failover_data_age_ms(2_000), 500);

    state.observe_primary_connected();
    assert_eq!(state.primary_failover_data_age_ms(2_500), 0);

    state.observe_primary_disconnected(3_000);
    assert_eq!(state.primary_failover_data_age_ms(3_500), 500);
}

#[test]
fn reparenting_replica_preserves_the_observed_disconnect_baseline() {
    let mut state = ReplicationState::new("replid".to_string(), 16);
    state.switch_to_replica();
    state.observe_primary_connected();
    state.observe_primary_disconnected(1_500);

    state.switch_to_replica();

    assert_eq!(state.primary_failover_data_age_ms(2_000), 500);
}

#[test]
fn failover_transitions_keep_related_state_consistent() {
    let mut state = ReplicationState::new("replid".to_string(), 16);

    state.pending_failover_target = Some(("127.0.0.1".to_string(), 6380));
    state.failover_state = "waiting-for-sync".to_string();
    state.failover_end_time_ms = 1_000;
    state.force_failover = true;
    state.set_failover_sync_offset(offset(42));
    assert!(state.is_waiting_for_failover_sync());
    assert_eq!(
        state.pending_failover_target(),
        Some(&("127.0.0.1".to_string(), 6380))
    );
    assert_eq!(state.failover_sync_offset(), Some(offset(42)));
    assert_eq!(state.failover_end_time_ms(), 1_000);
    assert!(state.is_forced_failover());

    state.enter_failover_handoff();
    assert!(state.is_replica());
    assert!(state.is_failover_in_progress());
    assert_eq!(state.pending_failover_target(), None);
    assert_eq!(state.failover_sync_offset(), None);
    assert_eq!(state.failover_end_time_ms(), 0);
    assert!(!state.is_forced_failover());
    assert_eq!(state.disconnect_clients_until_ms, 0);

    state.reset_failover_and_disconnect_window();
    assert_eq!(state.failover_state, "no-failover");
}
