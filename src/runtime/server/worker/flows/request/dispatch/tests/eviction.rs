use super::*;

#[test]
fn command_time_eviction_without_an_active_sink_does_not_buffer_unlink() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.set_repl_seldb(10);
    backend.ctx.persistence_mut().set_aof_selected_db(11);
    let mut effects = DispatchSideEffects::default();

    backend._queue_eviction_unlink(10, b"victim", &mut effects);

    assert!(effects.replication_only.is_empty());
    assert!(effects.aof_only.is_empty());
    assert_eq!(backend.ctx.repl_seldb(), 10);
    assert_eq!(backend.ctx.persistence().aof_selected_db(), 11);
}

#[test]
fn command_time_eviction_does_not_retain_keyspace_notifications() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().server.notify_keyspace_events = "KEA".to_string();
    let mut effects = DispatchSideEffects::default();
    let evicted = (0..16)
        .map(|index| (0, Bytes::from(format!("key-{index}"))))
        .collect();

    backend.apply_command_evictions(42, evicted, &mut effects);

    assert!(effects.publish.is_empty());
}
