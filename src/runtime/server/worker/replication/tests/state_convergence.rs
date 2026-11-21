use super::*;
use crate::foundation::io::protocol::Value;
use crate::runtime::server::worker::command::invocation::CommandOrigin;

fn config_batch(backend: &mut ServerWorkerState, values: &[&str], origin: CommandOrigin) -> Value {
    let reply = config_handler(backend, values);
    backend.apply_post_command_intents(origin, 99, None, reply)
}

fn config_handler(backend: &mut ServerWorkerState, values: &[&str]) -> Value {
    let client = 99;
    if !backend.connection_registry.contains(Token(client)) {
        add_connection(backend, client);
    }
    let args = CommandArgs::from_parts(
        ["CONFIG", "SET"]
            .into_iter()
            .chain(values.iter().copied())
            .map(|value| Bytes::copy_from_slice(value.as_bytes()))
            .collect(),
    );
    crate::framework::commands::server::config::config_set(
        &mut backend.connection_registry[Token(client)],
        &args,
        &mut backend.ctx,
    )
}

fn plain_downstream(backend: &mut ServerWorkerState, token: usize) {
    let (_, id) = setup_primary(backend, token, vec!["lz4".to_string()], false, false);
    backend
        .replication_runtime
        .primary_link_mut(id)
        .unwrap()
        .set_state(PrimaryLinkState::Streaming);
}

#[test]
fn state_convergence_config_switches_all_online_links_without_cron() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    plain_downstream(&mut backend, 41);
    plain_downstream(&mut backend, 42);
    assert_eq!(
        config_batch(
            &mut backend,
            &["repl-compression", "lz4"],
            CommandOrigin::Direct
        ),
        Value::ok()
    );
    assert!(!backend.connection_registry.contains(Token(41)));
    assert!(!backend.connection_registry.contains(Token(42)));
}

#[test]
fn state_convergence_noop_keeps_online_codec_and_output() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    plain_downstream(&mut backend, 41);
    backend.connection_registry[Token(41)].queue_replication_reply(b"queued".to_vec());
    assert_eq!(
        config_batch(
            &mut backend,
            &["repl-compression", "no"],
            CommandOrigin::Direct
        ),
        Value::ok()
    );
    assert!(backend.connection_registry.contains(Token(41)));
    assert_eq!(_queued(&backend.connection_registry[Token(41)]), b"queued");
}

#[test]
fn state_convergence_failover_abort_rechecks_deferred_compression() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    plain_downstream(&mut backend, 41);
    backend.ctx.replication_mut().failover_state = "failover-in-progress".to_string();
    assert_eq!(
        config_batch(
            &mut backend,
            &["repl-compression", "lz4"],
            CommandOrigin::Direct
        ),
        Value::ok()
    );
    assert!(backend.connection_registry.contains(Token(41)));
    backend.clear_pending_failover();
    assert!(!backend.connection_registry.contains(Token(41)));
}

#[test]
fn state_convergence_failed_handler_batch_keeps_compression() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    plain_downstream(&mut backend, 41);
    assert!(matches!(
        config_batch(
            &mut backend,
            &["repl-compression", "lz4", "repl-compression", "no"],
            CommandOrigin::Direct
        ),
        Value::Error(_)
    ));
    assert_eq!(backend.ctx.config().replication.repl_compression, "no");
    assert!(backend.connection_registry.contains(Token(41)));
    assert!(matches!(
        config_batch(
            &mut backend,
            &["repl-compression", "lz4", "maxmemory-policy", "invalid"],
            CommandOrigin::Direct
        ),
        Value::Error(_)
    ));
    assert_eq!(backend.ctx.config().replication.repl_compression, "no");
    assert!(backend.connection_registry.contains(Token(41)));
}

fn defer_compression(backend: &mut ServerWorkerState) {
    plain_downstream(backend, 41);
    backend.ctx.replication_mut().failover_state = "failover-in-progress".to_string();
    assert_eq!(
        config_batch(backend, &["repl-compression", "lz4"], CommandOrigin::Direct),
        Value::ok()
    );
    assert_eq!(backend.ctx.deferred_replication_compression(), Some("lz4"));
}

#[test]
fn state_convergence_failed_and_noop_batches_preserve_old_deferred_intent() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    defer_compression(&mut backend);
    for values in [
        &["repl-compression", "lz4"][..],
        &["repl-compression", "no", "maxmemory-policy", "invalid"][..],
        &["repl-compression", "no", "bind", "invalid-address"][..],
    ] {
        let reply = config_batch(&mut backend, values, CommandOrigin::Direct);
        if values.len() == 2 {
            assert_eq!(reply, Value::ok());
        } else {
            assert!(matches!(reply, Value::Error(_)));
        }
        assert_eq!(backend.ctx.config().replication.repl_compression, "lz4");
        assert_eq!(backend.ctx.deferred_replication_compression(), Some("lz4"));
        assert!(backend.connection_registry.contains(Token(41)));
    }
    backend.clear_pending_failover();
    assert!(!backend.connection_registry.contains(Token(41)));
}

#[test]
fn state_convergence_tls_failure_preserves_old_deferred_intent() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    defer_compression(&mut backend);
    backend.ctx.config_mut().server.enable_protected_configs =
        crate::engine::extension::config::server::ProtectedAction::Yes;
    let reply = config_handler(
        &mut backend,
        &[
            "repl-compression",
            "no",
            "tls-port",
            "16379",
            "tls-cert-file",
            "/missing/task3/cert",
            "tls-key-file",
            "/missing/task3/key",
        ],
    );
    assert_eq!(
        reply,
        Value::Null,
        "TLS setup must reach the runtime apply boundary"
    );
    let reply = backend.apply_post_command_intents(CommandOrigin::Direct, 99, None, reply);
    assert!(matches!(reply, Value::Error(_)));
    assert_eq!(backend.ctx.config().replication.repl_compression, "lz4");
    assert_eq!(backend.ctx.deferred_replication_compression(), Some("lz4"));
    backend.clear_pending_failover();
    assert!(!backend.connection_registry.contains(Token(41)));
}

#[test]
fn state_convergence_transaction_waits_for_io_apply_and_preserves_old_intent_on_failure() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    defer_compression(&mut backend);
    assert_eq!(
        config_batch(
            &mut backend,
            &["repl-compression", "no", "io-threads", "2"],
            CommandOrigin::TransactionItem,
        ),
        Value::ok()
    );
    assert_eq!(backend.ctx.deferred_replication_compression(), Some("lz4"));
    assert!(backend.connection_registry.contains(Token(41)));
    let request = backend.ctx.take_io_threads_reconfigure_request().unwrap();
    // Exercise the real resolver's invalid-count failure after the EXEC item
    // deferred its runtime application.
    backend
        .ctx
        .request_io_threads_reconfigure(request.previous, 0);
    let reply = backend.apply_post_command_intents(CommandOrigin::Direct, 99, None, Value::ok());
    assert!(matches!(reply, Value::Error(_)));
    assert_eq!(backend.ctx.config().replication.repl_compression, "lz4");
    assert_eq!(backend.ctx.deferred_replication_compression(), Some("lz4"));
    backend.clear_pending_failover();
    assert!(!backend.connection_registry.contains(Token(41)));
}

#[test]
fn state_convergence_accepted_failover_rechecks_deferred_intent() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    defer_compression(&mut backend);
    let mut link = ReplicaLink::new(
        ConnectionId(50),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6380,
        },
    );
    link.set_resume_position(
        "0123456789abcdef0123456789abcdef01234567".to_string(),
        offset(42),
        0,
    );
    link.set_failover_request(true);
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6379).unwrap();
    link.on_bytes(
        b"+CONTINUE fedcba9876543210fedcba9876543210fedcba98\r\n",
        6379,
    )
    .unwrap();
    backend.replication_runtime.set_replica_link(link);
    backend.clear_accepted_failover();
    assert!(!backend.connection_registry.contains(Token(41)));
    assert_eq!(backend.ctx.replication().failover_state, "no-failover");
}

fn failover_read_peer(backend: &mut ServerWorkerState) -> (TcpStream, Token) {
    use crate::runtime::server::worker::event_loop::transport::RuntimeConnectedPeer;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let primary_addr = listener.local_addr().unwrap();
    let peer = backend.transport.connect_tcp(primary_addr).unwrap();
    let (primary, _) = listener.accept().unwrap();
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr: primary_addr,
    });
    let mut link = ReplicaLink::new(
        ConnectionId(peer.token.0),
        MasterInfo {
            host: primary_addr.ip().to_string(),
            port: primary_addr.port(),
        },
    );
    link.set_dual_channel_enabled(false);
    link.set_resume_position(
        "0123456789abcdef0123456789abcdef01234567".to_string(),
        offset(42),
        0,
    );
    link.set_failover_request(true);
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6379).unwrap();
    backend.replication_runtime.set_replica_link(link);
    backend.ctx.replication_mut().failover_state = "failover-in-progress".to_string();
    backend.ctx.replication_mut().failover_end_time_ms = 99;
    backend.ctx.replication_mut().force_failover = true;
    backend.ctx.client_pause_mut().action = crate::framework::context::PauseAction::Write;
    backend.ctx.client_pause_mut().end_time_ms = i64::MAX;
    backend.ctx.client_pause_mut().reason = "failover_in_progress";
    let blocked_session = crate::engine::command::SessionId::from_u64(77);
    backend.ctx.blocking_registry_mut().register_client(
        0,
        &[Bytes::from_static(b"blocked-key")],
        blocked_session,
    );
    add_connection(backend, 88);
    backend.ctx.paused_clients_mut().insert(Token(88));
    assert_eq!(
        config_batch(backend, &["repl-compression", "lz4"], CommandOrigin::Direct),
        Value::ok()
    );
    assert_eq!(backend.ctx.deferred_replication_compression(), Some("lz4"));
    assert!(backend.replication_runtime.replica_link().is_some());
    (primary, peer.token)
}

fn assert_failover_read_finished(backend: &mut ServerWorkerState, main: Token) {
    use crate::framework::context::PauseAction;

    assert_eq!(
        backend.ctx.client_pause().action,
        PauseAction::None,
        "accepted failover must unpause before compression reconnect drops its link"
    );
    assert_eq!(backend.ctx.client_pause().end_time_ms, 0);
    assert_eq!(backend.ctx.client_pause().reason, "none");
    assert!(backend.ctx.pending_unpause());
    assert_eq!(backend.ctx.replication().failover_state, "no-failover");
    assert_eq!(backend.ctx.replication().failover_end_time_ms, 0);
    assert!(!backend.ctx.replication().force_failover);
    let blocked_session = crate::engine::command::SessionId::from_u64(77);
    assert_eq!(backend.ctx.pending_blocked_to_reject(), &[blocked_session]);
    assert!(backend.connection_registry.contains(Token(88)));
    assert!(backend.ctx.paused_clients().contains(&Token(88)));
    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(!backend.connection_registry.contains(main));
    assert_eq!(backend.ctx.deferred_replication_compression(), None);

    backend.ctx.set_pending_unpause(false);
    backend.clear_accepted_failover();
    backend.reconcile_replication_compression();
    assert!(!backend.ctx.pending_unpause());
    assert_eq!(backend.ctx.pending_blocked_to_reject(), &[blocked_session]);
}

#[test]
fn state_convergence_network_continue_finishes_failover_before_compression_reconnect() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let (mut primary, main) = failover_read_peer(&mut backend);
    primary
        .write_all(b"+CONTINUE fedcba9876543210fedcba9876543210fedcba98\r\n")
        .unwrap();
    backend.transport.wait_readable_for_test(main);

    backend.handle_master_link_readable(main.0, main.0);

    assert_failover_read_finished(&mut backend, main);
}

#[test]
fn state_convergence_network_fullresync_finishes_failover_before_compression_reconnect() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().replication.repl_diskless_load = "flush-before-load".to_string();
    let (mut primary, main) = failover_read_peer(&mut backend);
    let rdb = crate::foundation::io::persistence::serialize_all(backend.ctx.dbs(), 0);
    let mut frame = b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0\r\n".to_vec();
    frame.extend_from_slice(format!("${}\r\n", rdb.len()).as_bytes());
    frame.extend_from_slice(&rdb);
    primary.write_all(&frame).unwrap();
    backend.transport.wait_readable_for_test(main);

    backend.handle_master_link_readable(main.0, main.0);

    assert!(!backend.ctx.is_loading());
    assert_failover_read_finished(&mut backend, main);
}

#[test]
fn state_convergence_network_stream_continue_finishes_failover_before_compression_reconnect() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let (mut primary, main) = failover_read_peer(&mut backend);
    primary
        .write_all(b"+CONTINUE fedcba9876543210fedcba9876543210fedcba98\r\n")
        .unwrap();
    backend.transport.wait_readable_for_test(main);

    backend.recv_replication_stream(main.0, main.0);
    backend.clear_accepted_failover();

    assert_failover_read_finished(&mut backend, main);
}

#[test]
fn state_convergence_network_fullresync_defers_reconnect_until_loading_finishes() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().replication.repl_diskless_load = "flush-before-load".to_string();
    let (mut primary, main) = failover_read_peer(&mut backend);
    let rdb = crate::foundation::io::persistence::serialize_all(backend.ctx.dbs(), 0);
    let split = rdb.len() / 2;
    let mut frame = b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0\r\n".to_vec();
    frame.extend_from_slice(format!("${}\r\n", rdb.len()).as_bytes());
    frame.extend_from_slice(&rdb[..split]);
    primary.write_all(&frame).unwrap();
    backend.transport.wait_readable_for_test(main);
    backend.handle_master_link_readable(main.0, main.0);

    assert!(backend.ctx.is_loading());
    assert!(backend
        .replication_runtime
        .replica_link()
        .is_some_and(|link| link.has_pending_rdb_receive()));
    assert!(backend.connection_registry.contains(main));
    assert_eq!(backend.ctx.deferred_replication_compression(), Some("lz4"));
    assert_eq!(backend.ctx.replication().failover_state, "no-failover");
    assert_eq!(
        backend.ctx.client_pause().action,
        crate::framework::context::PauseAction::None
    );
    assert!(backend.ctx.pending_unpause());
    assert_eq!(
        backend.ctx.pending_blocked_to_reject(),
        &[crate::engine::command::SessionId::from_u64(77)]
    );

    primary.write_all(&rdb[split..]).unwrap();
    backend.transport.wait_readable_for_test(main);
    backend.handle_master_link_readable(main.0, main.0);

    assert!(!backend.ctx.is_loading());
    assert_failover_read_finished(&mut backend, main);
}

#[test]
fn state_convergence_network_continue_preserves_explicit_pause_during_compression_reconnect() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let (mut primary, main) = failover_read_peer(&mut backend);
    backend.ctx.client_pause_mut().end_time_ms = i64::MAX - 1;
    backend.ctx.client_pause_mut().reason = "client_pause";
    primary
        .write_all(b"+CONTINUE fedcba9876543210fedcba9876543210fedcba98\r\n")
        .unwrap();
    backend.transport.wait_readable_for_test(main);
    backend.handle_master_link_readable(main.0, main.0);

    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(!backend.connection_registry.contains(main));
    assert_eq!(backend.ctx.deferred_replication_compression(), None);
    assert_eq!(backend.ctx.replication().failover_state, "no-failover");
    assert_eq!(backend.ctx.replication().failover_end_time_ms, 0);
    assert!(!backend.ctx.replication().force_failover);
    assert_eq!(
        backend.ctx.pending_blocked_to_reject(),
        &[crate::engine::command::SessionId::from_u64(77)]
    );
    assert_eq!(
        backend.ctx.client_pause().action,
        crate::framework::context::PauseAction::Write
    );
    assert_eq!(backend.ctx.client_pause().end_time_ms, i64::MAX - 1);
    assert_eq!(backend.ctx.client_pause().reason, "client_pause");
    assert!(!backend.ctx.pending_unpause());
    assert!(backend.connection_registry.contains(Token(88)));
    assert!(backend.ctx.paused_clients().contains(&Token(88)));
}

fn waiting_dual(
    backend: &mut ServerWorkerState,
) -> crate::framework::distributed::replication::ReplicaId {
    let mut caps = dual_channel_caps();
    caps.push("lz4".to_string());
    let (_, id) = setup_primary(backend, 41, caps, true, true);
    add_connection(backend, 42);
    let main = ConnectionId(41);
    let rdb = ConnectionId(42);
    backend
        .replication_runtime
        .primary_link_mut(id)
        .unwrap()
        .transition_to_waiting_for_rdb_channel()
        .unwrap();
    backend
        .replication_runtime
        .register_rdb_candidate(rdb)
        .unwrap();
    backend
        .replication_runtime
        .insert_unpaired_rdb_psync_wait_for_test(
            rdb,
            backend.ctx.replication().backlog.snapshot_boundary(),
        );
    backend
        .replication_runtime
        .associate_rdb_channel(main, rdb)
        .unwrap();
    backend
        .replication_runtime
        .finalize_dual_channel_rdb_association(main, rdb)
        .unwrap();
    let backlog = &backend.ctx.replication().backlog;
    let link = backend.replication_runtime.primary_link_mut(id).unwrap();
    assert!(link.complete_dual_channel_main_psync(
        backlog.replid(),
        crate::engine::distributed::replication::PsyncRequestOffset::try_from_raw(1).unwrap(),
        backlog,
    ));
    backend
        .replication_runtime
        .insert_grace_psync_wait_for_test(main, i64::MAX);
    id
}

fn acknowledge_main(backend: &mut ServerWorkerState) {
    let ack = CommandArgs::from_parts(vec![
        Bytes::from_static(b"REPLCONF"),
        Bytes::from_static(b"ACK"),
        Bytes::from_static(b"0"),
    ]);
    assert_eq!(
        backend.try_handle_transport_cmd(
            41,
            41,
            &ack,
            "replconf",
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        ),
        Some(false)
    );
}

#[test]
fn state_convergence_rdb_last_runtime_cleanup_precedes_compression_reconnect() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let id = waiting_dual(&mut backend);
    plain_downstream(&mut backend, 43);
    assert_eq!(
        config_batch(
            &mut backend,
            &["repl-compression", "lz4"],
            CommandOrigin::Direct
        ),
        Value::ok()
    );
    assert!(!backend.connection_registry.contains(Token(43)));
    assert!(backend.connection_registry.contains(Token(41)));
    assert_eq!(backend.ctx.deferred_replication_compression(), Some("lz4"));
    acknowledge_main(&mut backend);
    assert_eq!(
        backend
            .replication_runtime
            .primary_link(id)
            .unwrap()
            .dual_state(),
        PrimaryDualChannelState::WaitingForReplicaAck
    );
    assert!(backend
        .replication_runtime
        .has_grace_psync_wait_for_main(ConnectionId(41)));
    backend
        .replication_runtime
        .mark_rdb_snapshot_complete(ConnectionId(42));
    backend.finish_pending_dual_promotions();
    assert!(!backend
        .replication_runtime
        .has_grace_psync_wait_for_main(ConnectionId(41)));
    assert!(!backend.connection_registry.contains(Token(41)));
    assert!(backend.ctx.deferred_replication_compression().is_none());
    backend
        .replication_runtime
        .mark_rdb_snapshot_complete(ConnectionId(42));
    backend.finish_pending_dual_promotions();
    assert!(backend
        .replication_runtime
        .take_dual_promotions()
        .is_empty());
}

#[test]
fn state_convergence_rdb_last_preserves_debug_grace_until_the_next_ack() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let id = waiting_dual(&mut backend);
    backend.ctx.config_mut().server.debug_pause_after_fork = true;
    acknowledge_main(&mut backend);
    backend
        .replication_runtime
        .mark_rdb_snapshot_complete(ConnectionId(42));
    backend.finish_pending_dual_promotions();
    assert_eq!(
        backend
            .replication_runtime
            .primary_link(id)
            .unwrap()
            .dual_state(),
        PrimaryDualChannelState::Online
    );
    assert!(backend
        .replication_runtime
        .has_grace_psync_wait_for_main(ConnectionId(41)));
    backend.finish_pending_dual_promotions();
    assert!(backend
        .replication_runtime
        .has_grace_psync_wait_for_main(ConnectionId(41)));
    acknowledge_main(&mut backend);
    assert!(!backend
        .replication_runtime
        .has_grace_psync_wait_for_main(ConnectionId(41)));
}

#[test]
fn state_convergence_retired_promotion_does_not_touch_reused_main_slot() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let old_id = waiting_dual(&mut backend);
    acknowledge_main(&mut backend);
    backend
        .replication_runtime
        .mark_rdb_snapshot_complete(ConnectionId(42));
    backend.remove_connection(41);
    plain_downstream(&mut backend, 41);
    backend
        .replication_runtime
        .insert_grace_psync_wait_for_test(ConnectionId(41), i64::MAX);
    backend.finish_pending_dual_promotions();
    assert!(backend.replication_runtime.primary_link(old_id).is_none());
    assert!(backend.connection_registry.contains(Token(41)));
    assert!(backend
        .replication_runtime
        .has_grace_psync_wait_for_main(ConnectionId(41)));
}

#[test]
fn state_convergence_handler_error_cannot_commit_an_older_pending_compression_candidate() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    defer_compression(&mut backend);
    assert_eq!(
        config_batch(
            &mut backend,
            &["repl-compression", "no", "io-threads", "2"],
            CommandOrigin::TransactionItem,
        ),
        Value::ok()
    );
    let handler_reply = config_handler(&mut backend, &["maxmemory-policy", "invalid"]);
    assert!(matches!(handler_reply, Value::Error(_)));
    let reply =
        backend.apply_post_command_intents(CommandOrigin::Direct, 99, None, handler_reply.clone());
    assert_eq!(
        reply, handler_reply,
        "an older I/O apply result must not replace the current handler error"
    );
    assert_eq!(backend.ctx.deferred_replication_compression(), Some("lz4"));
    assert_eq!(backend.ctx.config().replication.repl_compression, "lz4");
}

#[test]
fn state_convergence_tcp_failure_after_io_success_does_not_commit_compression() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    defer_compression(&mut backend);
    let reply = config_handler(
        &mut backend,
        &[
            "repl-compression",
            "no",
            "io-threads",
            "2",
            "bind",
            "invalid-address",
        ],
    );
    assert_eq!(
        reply,
        Value::Null,
        "bind validation must reach the runtime apply boundary"
    );
    let reply = backend.apply_post_command_intents(CommandOrigin::Direct, 99, None, reply);
    assert!(matches!(reply, Value::Error(_)));
    assert_eq!(backend.ctx.config().replication.repl_compression, "lz4");
    assert_eq!(backend.ctx.deferred_replication_compression(), Some("lz4"));
}

#[test]
fn state_convergence_online_upstream_reconnects_without_cron() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    add_connection(&mut backend, 50);
    let link = ReplicaLink::streaming(
        ConnectionId(50),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6380,
        },
        "0123456789abcdef0123456789abcdef01234567".to_string(),
        offset(42),
    );
    backend.replication_runtime.set_replica_link(link);
    assert_eq!(
        config_batch(
            &mut backend,
            &["repl-compression", "lz4"],
            CommandOrigin::Direct
        ),
        Value::ok()
    );
    assert!(!backend.connection_registry.contains(Token(50)));
    assert!(backend.ctx.deferred_replication_compression().is_none());
}

#[test]
fn state_convergence_compressed_noop_keeps_codec_and_queued_output() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let (_, id) = setup_primary(&mut backend, 41, vec!["lz4".to_string()], false, false);
    let link = backend.replication_runtime.primary_link_mut(id).unwrap();
    link.set_state(PrimaryLinkState::Streaming);
    link.freeze_compression("lz4");
    backend.ctx.config_mut().replication.repl_compression = "lz4".to_string();
    backend.connection_registry[Token(41)].queue_replication_reply(b"compressed-output".to_vec());
    assert_eq!(
        config_batch(
            &mut backend,
            &["repl-compression", "lz4"],
            CommandOrigin::Direct
        ),
        Value::ok()
    );
    assert!(backend
        .replication_runtime
        .primary_link(id)
        .unwrap()
        .is_compressed());
    assert_eq!(
        _queued(&backend.connection_registry[Token(41)]),
        b"compressed-output"
    );
}

fn lz4_upstream(backend: &mut ServerWorkerState) -> u64 {
    add_connection(backend, 50);
    let mut link = ReplicaLink::new(
        ConnectionId(50),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6380,
        },
    );
    link.set_dual_channel_enabled(false);
    link.set_compression_mode("lz4");
    link.set_resume_position(
        "0123456789abcdef0123456789abcdef01234567".to_string(),
        offset(42),
        0,
    );
    link.connected(None, None);
    let (_, actions) = link.on_bytes(b"+PONG\r\n", 6379).unwrap();
    assert!(actions.iter().any(|action| {
        matches!(action, crate::framework::distributed::replication::ReplicaAction::Send(frame)
            if *frame == protocol::build_replconf_capabilities_with_inband(false, "lz4", false))
    }));
    link.on_bytes(
        b"+OK\r\n+OK\r\n+CONTINUE 0123456789abcdef0123456789abcdef01234567\r\n",
        6379,
    )
    .unwrap();
    assert!(link.is_streaming());
    assert!(!link.has_pending_rdb_receive());
    assert_eq!(link.rdb_channel_state(), ReplicaRdbChannelState::None);
    assert!(!link.compression_is_plaintext());
    let session = link.session_id();
    backend.replication_runtime.set_replica_link(link);
    backend.ctx.config_mut().replication.repl_compression = "lz4".to_string();
    backend.ctx.config_mut().replication.master_link_status = "up".to_string();
    session
}

fn assert_lz4_upstream(backend: &ServerWorkerState, session: u64) {
    assert!(
        backend.connection_registry.contains(Token(50)),
        "paused compression reconciliation must retain the upstream connection"
    );
    let link = backend.replication_runtime.replica_link().unwrap();
    assert_eq!(link.connection_id(), ConnectionId(50));
    assert_eq!(link.session_id(), session);
    assert_eq!(link.advertised_compression_mode(), "lz4");
    assert!(!link.compression_is_plaintext());
    assert_eq!(link.applied_position(), offset(42));
    assert_eq!(backend.ctx.config().replication.master_link_status, "up");
}

#[test]
fn state_convergence_compression_pause_config_roundtrip_keeps_upstream_session() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let session = lz4_upstream(&mut backend);
    backend.ctx.set_pause_cron(1);
    for mode in ["no", "lz4"] {
        assert_eq!(
            config_batch(
                &mut backend,
                &["repl-compression", mode],
                CommandOrigin::Direct,
            ),
            Value::ok()
        );
        assert_lz4_upstream(&backend, session);
        assert_eq!(backend.ctx.config().replication.repl_compression, mode);
        assert_eq!(backend.ctx.deferred_replication_compression(), Some(mode));
        backend.reconcile_replication_compression();
        backend.reconcile_deferred_replication_compression();
        assert_lz4_upstream(&backend, session);
        assert_eq!(backend.ctx.deferred_replication_compression(), Some(mode));
    }
    backend.ctx.set_pause_cron(0);
    backend.reconcile_replication_compression();
    assert_lz4_upstream(&backend, session);
    assert_eq!(backend.ctx.deferred_replication_compression(), None);
    backend.reconcile_deferred_replication_compression();
    assert_lz4_upstream(&backend, session);
}

#[test]
fn state_convergence_compression_pause_config_no_retires_upstream_once_after_resume() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let session = lz4_upstream(&mut backend);
    backend.ctx.set_pause_cron(1);
    assert_eq!(
        config_batch(
            &mut backend,
            &["repl-compression", "no"],
            CommandOrigin::Direct,
        ),
        Value::ok()
    );
    assert_lz4_upstream(&backend, session);
    assert_eq!(backend.ctx.deferred_replication_compression(), Some("no"));
    backend.ctx.set_pause_cron(0);
    backend.reconcile_deferred_replication_compression();
    assert!(!backend.connection_registry.contains(Token(50)));
    assert!(backend.replication_runtime.replica_link().is_none());
    assert_eq!(backend.ctx.config().replication.master_link_status, "down");
    assert_eq!(backend.ctx.deferred_replication_compression(), None);
    let master = MasterInfo {
        host: "127.0.0.1".to_string(),
        port: 6380,
    };
    assert_eq!(
        backend.replication_runtime.replica_resume_for(&master),
        Some(("0123456789abcdef0123456789abcdef01234567", offset(42), 0))
    );
    add_connection(&mut backend, 50);
    let replacement = ReplicaLink::streaming(
        ConnectionId(50),
        master,
        "0123456789abcdef0123456789abcdef01234567".to_string(),
        offset(42),
    );
    let replacement_session = replacement.session_id();
    assert_ne!(replacement_session, session);
    backend.replication_runtime.set_replica_link(replacement);
    backend.reconcile_replication_compression();
    backend.reconcile_deferred_replication_compression();
    assert!(backend.connection_registry.contains(Token(50)));
    assert_eq!(
        backend
            .replication_runtime
            .replica_link()
            .unwrap()
            .session_id(),
        replacement_session
    );
    assert_eq!(backend.ctx.deferred_replication_compression(), None);
}

fn paused_downstream_reconciliation(reconcile: fn(&mut ServerWorkerState)) {
    use crate::framework::context::{
        PostCommandApplyOutcome, ReplicationCompressionReconcileRoute,
    };

    let mut backend = ServerWorkerState::new(1).unwrap();
    plain_downstream(&mut backend, 41);
    plain_downstream(&mut backend, 42);
    backend.connection_registry[Token(41)].queue_replication_reply(b"queued".to_vec());
    backend.ctx.set_pause_cron(2);
    assert_eq!(
        config_handler(&mut backend, &["repl-compression", "lz4"]),
        Value::ok()
    );
    assert!(matches!(
        backend
            .ctx
            .settle_replication_compression(PostCommandApplyOutcome::Succeeded),
        ReplicationCompressionReconcileRoute::Reconcile
    ));
    reconcile(&mut backend);
    assert!(backend.connection_registry.contains(Token(41)));
    assert!(backend.connection_registry.contains(Token(42)));
    assert_eq!(_queued(&backend.connection_registry[Token(41)]), b"queued");
    assert_eq!(backend.ctx.deferred_replication_compression(), Some("lz4"));
    backend.ctx.set_pause_cron(0);
    reconcile(&mut backend);
    assert!(!backend.connection_registry.contains(Token(41)));
    assert!(!backend.connection_registry.contains(Token(42)));
    assert_eq!(backend.ctx.deferred_replication_compression(), None);
}

#[test]
fn state_convergence_compression_pause_shared_entry_retains_downstream_intent() {
    paused_downstream_reconciliation(ServerWorkerState::reconcile_replication_compression);
}

#[test]
fn state_convergence_compression_pause_dedicated_entry_retains_downstream_intent() {
    paused_downstream_reconciliation(ServerWorkerState::reconcile_deferred_replication_compression);
}

#[test]
fn state_convergence_compression_pause_failed_batch_does_not_commit_or_disconnect() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let session = lz4_upstream(&mut backend);
    backend.ctx.set_pause_cron(1);
    assert!(matches!(
        config_batch(
            &mut backend,
            &["repl-compression", "no", "maxmemory-policy", "invalid"],
            CommandOrigin::Direct,
        ),
        Value::Error(_)
    ));
    assert_eq!(backend.ctx.config().replication.repl_compression, "lz4");
    assert_eq!(backend.ctx.deferred_replication_compression(), None);
    backend.reconcile_replication_compression();
    backend.reconcile_deferred_replication_compression();
    assert_lz4_upstream(&backend, session);
    backend.ctx.set_pause_cron(0);
    backend.reconcile_replication_compression();
    assert_lz4_upstream(&backend, session);
    assert_eq!(backend.ctx.deferred_replication_compression(), None);
}

#[test]
fn state_convergence_compression_pause_shared_entry_still_clears_accepted_failover() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let (_primary, main) = failover_read_peer(&mut backend);
    backend.ctx.set_pause_cron(1);
    let link = backend.replication_runtime.replica_link_mut().unwrap();
    let session = link.session_id();
    link.on_bytes(
        b"+CONTINUE fedcba9876543210fedcba9876543210fedcba98\r\n",
        6379,
    )
    .unwrap();
    backend.reconcile_replication_compression();
    assert_eq!(backend.ctx.replication().failover_state, "no-failover");
    assert_eq!(backend.ctx.replication().failover_end_time_ms, 0);
    assert!(!backend.ctx.replication().force_failover);
    assert_eq!(
        backend.ctx.client_pause().action,
        crate::framework::context::PauseAction::None
    );
    assert_eq!(backend.ctx.client_pause().end_time_ms, 0);
    assert_eq!(backend.ctx.client_pause().reason, "none");
    assert!(backend.ctx.pending_unpause());
    let blocked_session = crate::engine::command::SessionId::from_u64(77);
    assert_eq!(backend.ctx.pending_blocked_to_reject(), &[blocked_session]);
    assert!(backend.connection_registry.contains(main));
    assert_eq!(
        backend
            .replication_runtime
            .replica_link()
            .unwrap()
            .session_id(),
        session
    );
    assert_eq!(backend.ctx.deferred_replication_compression(), Some("lz4"));
    backend.ctx.set_pending_unpause(false);
    backend.reconcile_replication_compression();
    assert!(!backend.ctx.pending_unpause());
    assert_eq!(backend.ctx.pending_blocked_to_reject(), &[blocked_session]);
    assert!(backend.connection_registry.contains(main));
    backend.ctx.set_pause_cron(0);
    backend.reconcile_replication_compression();
    assert!(!backend.connection_registry.contains(main));
    assert!(backend.replication_runtime.replica_link().is_none());
    assert_eq!(backend.ctx.deferred_replication_compression(), None);
}
