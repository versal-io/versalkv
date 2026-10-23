use super::*;
use crate::engine::command::context::CommandConn;

#[test]
fn aof_write_without_backlog_records_one_logical_offset_on_the_connection() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().persistence.aof_enabled = true;
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[Token(connection_token)].set_skip_propagation(false);
    let args = make_args(&[b"SET", b"key", b"value"]);
    let flags = backend.registry.lookup(b"set").unwrap().flags;
    let mut effects = DispatchSideEffects::default();
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

    assert_eq!(
        backend.connection_registry[Token(connection_token)]
            .last_write_offset()
            .get(),
        1
    );
    assert!(!backend.ctx.replication().backlog.is_active());
    assert_eq!(backend.ctx.replication().backlog.history_len(), 0);
}

#[test]
fn may_replicate_command_builds_propagation_without_dirty_write() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[Token(connection_token)].set_skip_propagation(false);
    let args = make_args(&[b"SPUBLISH", b"channel", b"message"]);
    let flags = backend.registry.lookup(b"spublish").unwrap().flags;
    let mut effects = DispatchSideEffects::default();
    let connection_effects = take_connection_command_effects(&mut backend, connection_token);

    backend.build_command_propagation(
        CommandPropagationInput {
            connection_token,
            token_value: 42,
            args: &args,
            name: "spublish",
            command_flags: flags,
            rejected: false,
        },
        connection_effects,
        &mut effects,
    );

    assert_eq!(effects.replication_only.len(), 2);
    assert_eq!(effects.aof_only, effects.replication_only);
    assert!(effects.replication_only[1]
        .windows(8)
        .any(|part| part == b"SPUBLISH"));
    assert_eq!(backend.ctx.persistence().dirty(), 0);
}

#[test]
fn may_replicate_command_commits_dirty_keys_when_propagation_is_disabled() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    let key = Bytes::from_static(b"local-only-key");
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .set_skip_propagation(true);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .record_dirty_key((0, key.clone()));
    let args = make_args(&[b"EVAL", b"return 1", b"0"]);
    let flags = backend.registry.lookup(b"eval").unwrap().flags;
    let mut effects = CommandEffects::default();
    let connection_effects = take_connection_command_effects(&mut backend, connection_token);

    backend.build_command_propagation(
        CommandPropagationInput {
            connection_token,
            token_value: 42,
            args: &args,
            name: "eval",
            command_flags: flags,
            rejected: false,
        },
        connection_effects,
        &mut effects,
    );

    assert_eq!(effects.watch_dirty, vec![(0, key)]);
    assert!(backend.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)]
    .dirty_keys()
    .is_empty());
    assert!(effects.replication_only.is_empty());
    assert!(effects.aof_only.is_empty());
}

#[test]
fn completed_keyed_command_records_slot_metrics_once() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().server.cluster_enabled = true;
    backend.ctx.config_mut().server.cluster_slot_stats_enabled = true;
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .set_custom_reply_enabled(false);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .set_request_bytes_for_test(33);
    let args = make_args(&[b"SET", b"{one}key", b"value"]);
    let flags = backend.registry.lookup(b"SET").unwrap().flags;
    let mut effects = DispatchSideEffects::default();
    let mut connection_effects = take_connection_command_effects(&mut backend, connection_token);

    let disposition = backend.complete_command(
        CommandCompletion {
            invocation: CommandInvocation::direct(
                CommandTarget::new(Token(42)),
                &args,
                "set",
                flags,
                DispatchGroup::Default,
                &[],
            ),
            reply: Value::ok(),
            status: CommandExecutionStatus::Executed,
            duration_us: 7,
            was_blocked_before: false,
        },
        Some(&mut connection_effects),
        &mut effects,
    );
    assert_eq!(disposition, CommandDisposition::Completed);

    let slot = crate::engine::distributed::cluster::node::key_hash_slot(b"{one}key") as usize;
    let counters = backend.ctx.slot_metrics().counters(slot).unwrap();
    assert_eq!(counters.cpu_usec, 7);
    assert_eq!(counters.network_bytes_in, 33);
    assert_eq!(counters.network_bytes_out, 5);
}

#[test]
fn postloop_preserves_blocked_command_input_until_unblock() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_fake_connection(&mut backend);
    let args = make_args(&[b"BLPOP", b"key", b"0"]);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .set_request_bytes_for_test(31);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .blocked = Some(crate::engine::command::context::BlockedState {
        cmd: BlockedCmd::Blpop,
        keys: vec![Bytes::from_static(b"key")],
        timeout_ms: 0,
        start_time_ms: crate::foundation::io::sys::time::mstime(),
        orig_args: args,
        req_bytes: 31,
        orig_wrapped: None,
    });

    backend.finalize_handle_readable(connection_token, 42, ReadBatchEffects::default());

    assert_eq!(
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .request_bytes_current_command(),
        31
    );
}

#[test]
fn replica_link_reply_is_dropped_and_marks_the_connection_closing() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_fake_connection(&mut backend);
    backend.replication_runtime.register_primary(
        ConnectionId(42),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let args = make_args(&[b"PING"]);
    let fullresync = b"+FULLRESYNC replid 0\r\n".to_vec();
    let mut batch = ReadBatchEffects::default();
    batch.replies.push(fullresync.clone());
    let mut effects = CommandEffects::default();
    let mut connection_effects = take_connection_command_effects(&mut backend, connection_token);
    let disposition = backend.complete_command(
        CommandCompletion {
            invocation: CommandInvocation::direct(
                CommandTarget::new(Token(42)),
                &args,
                "ping",
                0,
                DispatchGroup::Default,
                &[],
            ),
            reply: Value::SimpleString(Bytes::from_static(b"PONG")),
            status: CommandExecutionStatus::Executed,
            duration_us: 1,
            was_blocked_before: false,
        },
        Some(&mut connection_effects),
        &mut effects,
    );
    assert_eq!(disposition, CommandDisposition::Closing);
    batch.commit(effects);

    assert_eq!(batch.replies, vec![fullresync]);
    assert!(batch.mark_closing);
    assert_eq!(
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .state,
        crate::framework::capabilities::connection::ConnectionState::Closing
    );
}

#[test]
fn exec_aggregate_reply_is_accounted_once_at_the_command_boundary() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    let token = Token(connection_token);
    backend.connection_registry[token].set_custom_reply_enabled(false);
    assert!(backend.connection_registry[token].transaction.begin(0, 0));
    assert!(backend.connection_registry[token]
        .transaction
        .queue(make_args(&[b"PING"])));
    let mut effects = ReadBatchEffects::default();

    backend.process_parsed_command(
        token,
        make_args(&[b"EXEC"]),
        "exec".to_string(),
        false,
        &mut effects,
    );

    let expected: usize = effects.replies.iter().map(Vec::len).sum();
    assert!(expected > 0);
    assert_eq!(
        backend.connection_registry[token].reply_bytes_current_command(),
        expected
    );
}

#[test]
fn direct_reply_is_not_double_counted_by_the_command_boundary() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    let token = Token(connection_token);
    backend.connection_registry[token].set_custom_reply_enabled(false);
    let mut effects = ReadBatchEffects::default();

    backend.process_parsed_command(
        token,
        make_args(&[b"PING"]),
        "ping".to_string(),
        false,
        &mut effects,
    );

    let expected: usize = effects.replies.iter().map(Vec::len).sum();
    assert_eq!(expected, b"+PONG\r\n".len());
    assert_eq!(
        backend.connection_registry[token].reply_bytes_current_command(),
        expected
    );
}

#[test]
fn queued_transaction_reply_is_accounted_once_at_the_command_boundary() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    let token = Token(connection_token);
    backend.connection_registry[token].set_custom_reply_enabled(false);
    assert!(backend.connection_registry[token].transaction.begin(0, 0));
    let mut effects = ReadBatchEffects::default();

    backend.process_parsed_command(
        token,
        make_args(&[b"SET", b"key", b"value"]),
        "set".to_string(),
        false,
        &mut effects,
    );

    assert_eq!(effects.replies, [b"+QUEUED\r\n".to_vec()]);
    assert_eq!(
        backend.connection_registry[token].reply_bytes_current_command(),
        b"+QUEUED\r\n".len()
    );
}

#[test]
fn publish_reply_is_accounted_without_charging_async_delivery() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    let token = Token(connection_token);
    backend.connection_registry[token].set_custom_reply_enabled(false);
    let mut effects = ReadBatchEffects::default();

    backend.process_parsed_command(
        token,
        make_args(&[b"PUBLISH", b"channel", b"message"]),
        "publish".to_string(),
        false,
        &mut effects,
    );

    let expected: usize = effects.replies.iter().map(Vec::len).sum();
    assert!(expected > 0);
    assert_eq!(
        backend.connection_registry[token].reply_bytes_current_command(),
        expected
    );
    backend.queue_client_reply(token, b"async-push".to_vec());
    assert_eq!(
        backend.connection_registry[token].reply_bytes_current_command(),
        expected
    );
}
