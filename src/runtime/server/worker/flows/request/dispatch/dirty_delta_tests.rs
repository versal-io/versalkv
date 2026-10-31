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
