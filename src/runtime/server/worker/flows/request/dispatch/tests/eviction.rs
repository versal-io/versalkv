use super::*;
use crate::engine::command::context::CommandConn;

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
fn command_time_evictions_reuse_the_queued_cursor_after_one_explicit_select() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.ctx.replication_mut().backlog.activate();
    let mut effects = DispatchSideEffects::default();
    let select =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SELECT", b"10"]);
    let first =
        crate::engine::distributed::replication::protocol::encode_command(&[b"UNLINK", b"first"]);
    let second =
        crate::engine::distributed::replication::protocol::encode_command(&[b"UNLINK", b"second"]);

    let evicted = vec![
        (10, Bytes::from_static(b"first")),
        (10, Bytes::from_static(b"second")),
    ];
    backend.apply_command_evictions(42, evicted.clone(), &mut effects);

    let expected = vec![select, first, second];
    assert_eq!(effects.replication_only, expected);
    assert_eq!(effects.aof_only, effects.replication_only);
    assert_eq!(effects.watch_dirty, evicted);
}

#[test]
fn command_time_eviction_reuses_the_global_replication_cursor() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.replication_mut().backlog.activate();
    backend.ctx.set_repl_seldb(9);
    let mut effects = DispatchSideEffects::default();
    let unlink =
        crate::engine::distributed::replication::protocol::encode_command(&[b"UNLINK", b"victim"]);

    backend.apply_command_evictions(42, vec![(9, Bytes::from_static(b"victim"))], &mut effects);

    assert_eq!(effects.replication_only, vec![unlink]);
    assert_eq!(backend.ctx.repl_seldb(), 9);
}

#[test]
fn command_time_eviction_is_counted_before_write_offset() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.replication_mut().backlog.activate();
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[Token(connection_token)].set_skip_propagation(false);
    let initial_offset = backend.ctx.replication().primary_repl_offset().get();
    let mut effects = DispatchSideEffects::default();

    backend.apply_command_evictions(42, vec![(10, Bytes::from_static(b"victim"))], &mut effects);

    let args = make_args(&[b"SET", b"key", b"value"]);
    let flags = backend.registry.lookup(b"set").unwrap().flags;
    let connection_effects = take_connection_command_effects(&mut backend, connection_token);
    backend.build_command_propagation(
        CommandPropagationInput {
            connection_token,
            token_value: 42,
            args: &args,
            name: "set",
            command_flags: flags,
            rejected: false,
        },
        connection_effects,
        &mut effects,
    );

    let propagation_bytes: usize = effects.replication_only.iter().map(Vec::len).sum();
    let expected_offset = initial_offset + propagation_bytes as i64;
    assert_eq!(
        backend.connection_registry[Token(connection_token)]
            .last_write_offset()
            .get(),
        expected_offset
    );
    assert_eq!(
        backend.ctx.replication().primary_repl_offset().get(),
        expected_offset
    );
}

#[test]
fn aof_only_command_time_eviction_advances_logical_replication_offset() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().persistence.aof_enabled = true;
    let initial_offset = backend.ctx.replication().primary_repl_offset();
    let mut effects = DispatchSideEffects::default();

    backend.apply_command_evictions(42, vec![(0, Bytes::from_static(b"victim"))], &mut effects);

    assert!(effects.replication_only.is_empty());
    assert!(!effects.aof_only.is_empty());
    assert_eq!(
        backend.ctx.replication().primary_repl_offset(),
        initial_offset.checked_advance(1).unwrap()
    );
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
