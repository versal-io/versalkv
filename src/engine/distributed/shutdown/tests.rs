use super::state::*;
use super::{abort_shutdown, messages, request_shutdown};

fn offset(value: i64) -> crate::engine::distributed::replication::ReplicationOffset {
    crate::engine::distributed::replication::ReplicationOffset::try_from(value)
        .expect("valid test offset")
}

#[test]
fn parse_default() {
    assert_eq!(parse_sig_flags("default"), Some(SHUTDOWN_NOFLAGS));
}

#[test]
fn parse_single() {
    assert_eq!(parse_sig_flags("nosave"), Some(SHUTDOWN_NOSAVE));
    assert_eq!(parse_sig_flags("now"), Some(SHUTDOWN_NOW));
    assert_eq!(parse_sig_flags("force"), Some(SHUTDOWN_FORCE));
    assert_eq!(parse_sig_flags("save"), Some(SHUTDOWN_SAVE));
}

#[test]
fn parse_bitmask() {
    assert_eq!(
        parse_sig_flags("nosave,force"),
        Some(SHUTDOWN_NOSAVE | SHUTDOWN_FORCE)
    );
    assert_eq!(
        parse_sig_flags("now force"),
        Some(SHUTDOWN_NOW | SHUTDOWN_FORCE)
    );
    assert_eq!(
        parse_sig_flags("safe failover"),
        Some(SHUTDOWN_SAFE | SHUTDOWN_FAILOVER)
    );
}

#[test]
fn parse_unknown() {
    assert!(parse_sig_flags("bogus").is_none());
    assert!(parse_sig_flags("save,unknown").is_none());
}

#[test]
fn format_roundtrip() {
    assert_eq!(format_sig_flags(SHUTDOWN_NOFLAGS), "default");
    let combo = SHUTDOWN_NOSAVE | SHUTDOWN_FORCE;
    assert_eq!(format_sig_flags(combo), "nosave force");
    let cluster_combo = SHUTDOWN_SAFE | SHUTDOWN_FAILOVER;
    assert_eq!(format_sig_flags(cluster_combo), "safe failover");
}

fn handoff_fact(
    connection_id: usize,
    node_id: Option<&str>,
    version: u32,
    is_online: bool,
    ack_offset: i64,
) -> ShutdownReplicaFacts<'_> {
    ShutdownReplicaFacts {
        connection_id,
        node_id,
        version,
        is_online,
        ack_offset: Some(offset(ack_offset)),
    }
}

#[test]
fn shutdown_handoff_selects_a_fully_synced_current_replica_deterministically() {
    let low_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let high_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let replicas = [
        handoff_fact(7, Some(low_id), 0x09_00_00, true, 42),
        handoff_fact(9, Some(high_id), 0x09_01_00, true, 42),
    ];

    assert_eq!(
        select_shutdown_handoff(offset(42), &replicas),
        ShutdownHandoffDecision::Selected(
            crate::engine::distributed::cluster::NodeId::parse(high_id.as_bytes()).unwrap()
        )
    );
}

#[test]
fn shutdown_handoff_ignores_ineligible_replicas() {
    let valid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let replicas = [
        handoff_fact(1, Some(valid), 0x09_00_00, true, 41),
        handoff_fact(2, Some(valid), 0x09_00_00, false, 42),
        handoff_fact(3, None, 0x09_00_00, true, 42),
        handoff_fact(4, Some("short"), 0x09_00_00, true, 42),
    ];

    assert_eq!(
        select_shutdown_handoff(offset(42), &replicas),
        ShutdownHandoffDecision::NoCandidate
    );
}

#[test]
fn shutdown_handoff_legacy_replica_blocks_all_candidates() {
    let valid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let replicas = [
        handoff_fact(1, Some(valid), 0x09_00_00, true, 42),
        handoff_fact(2, Some(valid), 0x08_01_00, false, 0),
    ];

    assert_eq!(
        select_shutdown_handoff(offset(42), &replicas),
        ShutdownHandoffDecision::LegacyReplica
    );
}

#[test]
fn state_default_is_idle() {
    let s = ShutdownState::default();
    assert!(!s.asap && !s.initiated && s.flags == 0);
}

#[test]
fn replicas_are_ready_when_every_registered_replica_matches_primary_offset() {
    assert!(!replicas_are_ready(
        offset(10),
        &[(true, Some(offset(10))), (true, Some(offset(9)))]
    ));
    assert!(replicas_are_ready(offset(10), &[(false, Some(offset(10)))]));
    assert!(!replicas_are_ready(offset(10), &[(true, Some(offset(11)))]));
    assert!(replicas_are_ready(offset(10), &[]));
}

#[test]
fn zero_offset_shutdown_matches_c_for_replica_without_an_ack_yet() {
    assert!(replicas_are_ready(offset(0), &[(false, None)]));
    assert!(!replicas_are_ready(offset(1), &[(false, None)]));
}

#[test]
fn request_rejects_initial_aof_rewrite_without_force() {
    let mut state = ShutdownState::default();

    let result = request_shutdown(&mut state, SHUTDOWN_NOSAVE, true);

    assert_eq!(result, Err(messages::ERR_AOF_INIT_CANT_EXIT));
    assert!(!state.asap);
    assert_eq!(state.pending_flags, SHUTDOWN_NOFLAGS);
}

#[test]
fn request_stages_flags_for_runtime_pipeline() {
    let mut state = ShutdownState::default();

    assert_eq!(
        request_shutdown(&mut state, SHUTDOWN_NOSAVE | SHUTDOWN_NOW, false),
        Ok(())
    );
    assert!(state.asap);
    assert!(!state.initiated);
    assert_eq!(state.pending_flags, SHUTDOWN_NOSAVE | SHUTDOWN_NOW);
}

#[test]
fn abort_clears_a_staged_request() {
    let mut state = ShutdownState {
        asap: true,
        pending_flags: SHUTDOWN_NOSAVE,
        ..ShutdownState::default()
    };

    assert!(abort_shutdown(&mut state));
    assert!(!state.asap);
    assert_eq!(state.pending_flags, SHUTDOWN_NOFLAGS);
}
