use super::*;
use crate::framework::capabilities::connection::{CommandEffectDrain, Connection};

#[test]
fn generic_dirty_counter_replaces_default_and_drains_nonpropagating_effects() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"k"),
        Bytes::from_static(b"v"),
    ]);
    for (delta, skip, rejected, expected) in [
        (None, false, false, 1),
        (Some(7), false, false, 7),
        (Some(0), false, false, 0),
        (Some(2), true, false, 2),
        (None, false, true, 0),
    ] {
        backend.ctx.persistence_mut().set_dirty(0);
        let mut conn = Connection::new_fake();
        conn.resume_command_effects();
        if let Some(delta) = delta {
            conn.record_dirty_delta(delta);
        }
        let mut effects = conn
            .take_command_effects(CommandEffectDrain::Completed)
            .unwrap();
        let state = ConnectionPropagationState {
            swapdb: None,
            selected_db: 0,
            has_explicit_dirty_keys: true,
            has_script_ops: false,
            force_propagate: false,
            skip_propagation: skip,
        };
        let plan = CommandPropagationPlan {
            args: &args,
            rejected,
            is_write: true,
            may_replicate: false,
            touches_arbitrary_keys: false,
            preserve_expired_replica_key: false,
            metric_slots: vec![],
            streaming_replicas: 0,
        };
        let mut fx = CommandEffects::default();
        let _ = backend.take_command_propagation_ops(
            &plan,
            &state,
            &mut effects,
            &mut fx,
            PropagationCaptureMode::Direct,
        );
        assert_eq!(backend.ctx.persistence().dirty(), expected);
        assert_eq!(effects.take_dirty_delta(), None);
    }
}

#[test]
fn active_slot_export_counts_as_propagation_consumer_without_replica_or_aof() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    // Standalone baseline: inactive backlog, AOF disabled, no cluster state.
    assert!(!backend.has_propagation_consumers());

    let mut state =
        crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16_379);
    let id = crate::engine::distributed::cluster::migration::MigrationJobId::parse(
        b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .unwrap();
    let job = crate::engine::distributed::cluster::migration::MigrationJob::new(
        id,
        crate::engine::distributed::cluster::NodeId::parse(state.myself.name.as_bytes()).unwrap(),
        crate::engine::distributed::cluster::NodeId::parse(&[b'b'; 40]).unwrap(),
        crate::engine::distributed::cluster::migration::SlotRanges::new(vec![
            crate::engine::distributed::cluster::SlotRange::new(16383, 16383).unwrap(),
        ])
        .unwrap(),
        crate::engine::distributed::cluster::migration::MigrationOperation::Export,
        1,
    );
    state.migrations.start(job).unwrap();
    *backend.ctx.cluster_state_mut() = Some(Box::new(state));

    // An in-progress export streams every source write to the import target,
    // so propagation frames must still be encoded with zero replicas and AOF
    // disabled instead of being dropped by the standalone fast path.
    assert!(backend.has_propagation_consumers());
}
