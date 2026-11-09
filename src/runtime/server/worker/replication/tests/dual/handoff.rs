use super::*;
use crate::framework::distributed::replication::PendingReplicationBuffer;
use std::net::TcpStream;

#[test]
fn blocked_readiness_matrix_dual_main_readable_enters_pending_buffer() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_loading(true);
    let (main_token, _main_slab, rdb_token, mut primary_stream) =
        setup_dual_blocked_main_receiver(&mut backend);
    {
        let mut link = backend.replication_runtime.take_replica_link().unwrap();
        let mut rdb_payload = minimal_valid_rdb_body().to_vec();
        rdb_payload.extend_from_slice(b"0123456789abcdef0123456789abcdef01234567");
        link.on_rdb_bytes(&rdb_payload)
            .expect("queue complete RDB payload without driving decoder");
        assert!(link.has_pending_rdb_receive());
        assert!(link.rdb_framing_is_complete());
        backend.replication_runtime.set_replica_link(link);
    }

    let frame =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SET", b"k", b"v"]);
    primary_stream
        .write_all(&frame)
        .expect("write post-snapshot main frame");
    backend.transport.wait_readable_for_test(Token(main_token));
    backend.transport.queue_test_transport_event_batch(&[
        (Token(main_token), true, false, false, false),
        (Token(rdb_token), true, false, false, false),
    ]);

    let result = backend.process_events_while_blocked(BlockedOperation::DualRdb {
        main: ConnectionId(main_token),
        rdb: ConnectionId(rdb_token),
    });

    assert!(result.dispatched_events >= 1);
    assert!(result.deferred_events >= 1);
    assert_eq!(
        backend
            .replication_runtime
            .replica_pending_buffer()
            .expect("main stream frame must be buffered while RDB is loading")
            .len(),
        1
    );
    let deferred = backend
        .blocked_events
        .pop_front_deferred()
        .expect("RDB readiness must be deferred, not recursively dispatched");
    assert_eq!(deferred.token, Token(rdb_token));
    assert!(deferred.readable);
    let link = backend
        .replication_runtime
        .replica_link()
        .expect("main link must remain active");
    assert!(
        link.has_pending_rdb_receive(),
        "main-channel blocked dispatch must not drive the RDB decoder"
    );
    assert_eq!(
        link.rdb_channel_state(),
        ReplicaRdbChannelState::ReceivingRdb
    );
    assert_eq!(link.applied_position().get(), 100);
    assert_eq!(
        backend
            .replication_runtime
            .channel_pair_for(ConnectionId(main_token)),
        Some(ConnectionId(rdb_token))
    );
    assert!(backend.ctx.is_loading());
    assert!(backend.transport.is_registered(Token(main_token)));
    assert!(backend.transport.is_registered(Token(rdb_token)));
}

#[test]
fn blocked_dual_main_limit_remains_paused_after_post_read_flush() {
    use crate::foundation::io::transport::Interest;

    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = 1;
    let (main_token, main_slab, rdb_token, mut primary_stream) =
        setup_dual_blocked_main_receiver(&mut backend);
    let first =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SET", b"k1", b"v1"]);
    primary_stream.write_all(&first).unwrap();
    backend.transport.wait_readable_for_test(Token(main_token));
    backend.transport.clear_interest_requests_for_test();
    backend.transport.queue_test_transport_event_batch(&[(
        Token(main_token),
        true,
        false,
        false,
        false,
    )]);

    backend.process_events_while_blocked(BlockedOperation::DualRdb {
        main: ConnectionId(main_token),
        rdb: ConnectionId(rdb_token),
    });

    assert!(backend.replication_runtime.replica_main_read_paused());
    let received_offset = backend
        .replication_runtime
        .replica_link()
        .unwrap()
        .received_position()
        .get();
    assert!(backend.queue_replica_loading_newline(ConnectionId(main_token)));
    assert_eq!(
        backend.connection_registry[crate::framework::capabilities::io::Token(main_slab)]
            .output
            .queue
            .len(),
        1
    );
    assert_eq!(
        backend.transport.last_set_interest_calls_for_test().last(),
        Some(&(Token(main_token), Interest::Writable)),
        "a later RDB progress newline must not re-enable main reads"
    );
    let second =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SET", b"k2", b"v2"]);
    primary_stream.write_all(&second).unwrap();
    backend
        .transport
        .wait_pending_bytes_for_test(Token(main_token), second.len());

    backend.process_events_while_blocked(BlockedOperation::DualRdb {
        main: ConnectionId(main_token),
        rdb: ConnectionId(rdb_token),
    });

    assert_eq!(
        backend
            .replication_runtime
            .replica_pending_buffer()
            .unwrap()
            .len(),
        1,
        "a paused main must not consume another readable batch"
    );
    assert_eq!(
        backend
            .replication_runtime
            .replica_link()
            .unwrap()
            .received_position()
            .get(),
        received_offset
    );
}

#[test]
fn dual_main_pause_interest_failure_aborts_without_false_paused_state() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = 1;
    let (main_token, main_slab, rdb_token, mut primary_stream) =
        setup_dual_blocked_main_receiver(&mut backend);
    let frame =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SET", b"k", b"v"]);
    primary_stream.write_all(&frame).unwrap();
    backend.transport.wait_readable_for_test(Token(main_token));
    backend
        .transport
        .deregister_for_test(Token(main_token))
        .unwrap();

    let result = backend.recv_dual_main_while_rdb_loading(main_token, main_slab);

    assert!(result.is_err());
    assert!(!backend.replication_runtime.replica_main_read_paused());
    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(main_token)));
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_token)));
}

#[test]
fn blocked_readiness_matrix_dual_rdb_readable_hup_aborts_incomplete_pair() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_loading(true);
    let (main_token, _main_slab, rdb_token, _primary_stream, mut rdb_primary_stream) =
        setup_dual_blocked_main_receiver_with_rdb_peer(&mut backend);
    rdb_primary_stream
        .write_all(b"VALKEY080")
        .expect("write incomplete RDB tail");
    rdb_primary_stream
        .shutdown(std::net::Shutdown::Write)
        .expect("half-close incomplete RDB peer");
    backend.transport.queue_test_transport_event_batch(&[(
        Token(rdb_token),
        true,
        false,
        true,
        false,
    )]);

    let result = backend.process_events_while_blocked(BlockedOperation::DualRdb {
        main: ConnectionId(main_token),
        rdb: ConnectionId(rdb_token),
    });

    assert!(result.deferred_events >= 1);
    assert!(backend.replication_runtime.replica_link().is_some());

    backend.replay_one_deferred_readiness();

    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(backend.replication_runtime.replica_rdb_link().is_none());
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    assert!(!backend.transport.is_registered(Token(main_token)));
    assert!(!backend.transport.is_registered(Token(rdb_token)));
    assert!(!backend.ctx.is_loading());
    assert_eq!(backend.ctx.replication().slave_repl_offset, offset(0));
}

#[test]
fn blocked_readiness_matrix_dual_main_readable_hup_buffers_tail_then_aborts() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_loading(true);
    let (main_token, _main_slab, rdb_token, mut primary_stream) =
        setup_dual_blocked_main_receiver(&mut backend);
    let frame =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SET", b"k", b"v"]);
    primary_stream
        .write_all(&frame)
        .expect("write post-snapshot main tail");
    primary_stream
        .shutdown(std::net::Shutdown::Write)
        .expect("half-close dual main peer");
    backend.transport.queue_test_transport_event_batch(&[(
        Token(main_token),
        true,
        false,
        true,
        false,
    )]);

    backend.process_events_while_blocked(BlockedOperation::DualRdb {
        main: ConnectionId(main_token),
        rdb: ConnectionId(rdb_token),
    });

    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(backend.replication_runtime.replica_rdb_link().is_none());
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    assert!(backend.ctx.db(0).unwrap().get_no_expire(b"k").is_none());
    assert!(!backend.transport.is_registered(Token(main_token)));
    assert!(!backend.transport.is_registered(Token(rdb_token)));
    assert!(!backend.ctx.is_loading());
    assert_eq!(backend.ctx.replication().slave_repl_offset, offset(0));
}

#[test]
fn dual_blocked_service_main_hup_aborts_session() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let (main_token, _main_slab, rdb_token, primary_stream) =
        setup_dual_blocked_main_receiver(&mut backend);
    drop(primary_stream);
    backend.transport.queue_test_transport_event_batch(&[(
        Token(main_token),
        true,
        false,
        true,
        false,
    )]);

    backend.process_events_while_blocked(BlockedOperation::DualRdb {
        main: ConnectionId(main_token),
        rdb: ConnectionId(rdb_token),
    });

    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(!backend.ctx.is_loading());
    assert!(!backend.transport.is_registered(Token(main_token)));
    assert!(!backend.transport.is_registered(Token(rdb_token)));
}

#[test]
fn blocked_readiness_matrix_pending_replay_main_readable_waits_for_fifo() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.replication_mut().switch_to_replica();
    backend
        .ctx
        .config_mut()
        .persistence
        .loading_process_events_interval_bytes = 1;

    let main_slab = add_connection(&mut backend, 951);
    let (_main_peer, main_slab) = register_live_transport_for_slab(&mut backend, main_slab);
    let main_token = backend.connection_registry
        [crate::framework::capabilities::io::Token(main_slab)]
    .token()
    .0;
    let main = ConnectionId(main_token);
    let selected_db = 0;
    let snapshot_end = 100;
    let replid = "0123456789abcdef0123456789abcdef01234567".to_string();

    let mut link = ReplicaLink::new(
        main,
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.set_resume_position(replid.clone(), offset(snapshot_end + 3), selected_db);
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n+CONTINUE\r\n", 6379)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)
        .unwrap();
    link.provisional_primary = Some(ProvisionalPrimary {
        replid,
        end_offset: offset(snapshot_end),
        db_id: selected_db,
        rdb_client_id: 0,
    });
    let replay_session_id = link.session_id();
    backend.replication_runtime.set_replica_link(link);
    let _ = backend.replication_runtime.set_replica_pending_buffer(
        replay_session_id,
        PendingReplicationBuffer::new(1 << 20, offset(snapshot_end), selected_db),
    );
    let mut link = backend.replication_runtime.take_replica_link().unwrap();
    for (idx, key) in [b"k1".as_slice(), b"k2".as_slice(), b"k3".as_slice()]
        .into_iter()
        .enumerate()
    {
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"SET"),
            Bytes::copy_from_slice(key),
            Bytes::from_static(b"v"),
        ]);
        backend
            .buffer_replica_events_into_pending(
                main_slab,
                &mut link,
                vec![ReplicaEvent::Apply {
                    apply: ReplicationApply::Command(args),
                    end_offset: offset(snapshot_end + 1 + idx as i64),
                }],
            )
            .unwrap();
    }
    link.transition_rdb_channel_state(ReplicaRdbChannelState::RdbLoaded)
        .unwrap();
    backend.replication_runtime.set_replica_link(link);
    assert_eq!(
        backend
            .replication_runtime
            .replica_pending_buffer()
            .unwrap()
            .len(),
        3
    );
    let buffered_bytes = backend
        .replication_runtime
        .replica_pending_buffer()
        .unwrap()
        .used_bytes();
    assert_eq!(
        backend.ctx.replication().replica_output_buffer_bytes,
        buffered_bytes
    );
    backend.pending_replication_replay = Some(
        crate::runtime::server::worker::event_loop::blocked_events::PendingReplayContinuation::new_for_test(
            main,
            replay_session_id,
            crate::foundation::io::sys::time::mstime().saturating_sub(101),
        ),
    );
    backend.transport.queue_test_transport_event_batch(&[(
        Token(main_token),
        true,
        false,
        false,
        false,
    )]);

    backend.drive_one_pending_replication_replay();

    assert_eq!(
        backend
            .replication_runtime
            .replica_pending_buffer()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        backend
            .replication_runtime
            .replica_link()
            .unwrap()
            .applied_position()
            .get(),
        snapshot_end + 1
    );
    assert!(backend
        .ctx
        .db(selected_db)
        .unwrap()
        .get_no_expire(b"k1")
        .is_some());
    assert!(backend
        .ctx
        .db(selected_db)
        .unwrap()
        .get_no_expire(b"k2")
        .is_none());
    let remaining_bytes = backend
        .replication_runtime
        .replica_pending_buffer()
        .unwrap()
        .used_bytes();
    assert!(remaining_bytes < buffered_bytes);
    assert_eq!(
        backend.ctx.replication().replica_output_buffer_bytes,
        remaining_bytes,
        "replay progress must refresh retained replication-buffer memory"
    );

    backend.replay_one_deferred_readiness();
    let deferred = backend
        .blocked_events
        .pop_front_deferred()
        .expect("main readable must wait until older pending entries finish");
    assert_eq!(deferred.token, Token(main_token));
    backend.blocked_events.defer(deferred);

    backend.drive_one_pending_replication_replay();
    assert!(backend.pending_replication_replay.is_none());
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    assert_eq!(backend.ctx.replication().replica_output_buffer_bytes, 0);
    assert_eq!(
        backend
            .replication_runtime
            .replica_link()
            .unwrap()
            .applied_position()
            .get(),
        snapshot_end + 3
    );
    assert!(backend
        .ctx
        .db(selected_db)
        .unwrap()
        .get_no_expire(b"k3")
        .is_some());

    backend.replay_one_deferred_readiness();
    assert!(backend.blocked_events.pop_front_deferred().is_none());
    assert!(backend.replication_runtime.replica_link().is_some());
    assert!(backend.transport.is_registered(Token(main_token)));
    assert!(!backend.ctx.is_loading());
}

#[test]
fn blocked_readiness_matrix_pending_replay_hup_only_aborts_without_resurrection() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let main_slab = add_connection(&mut backend, 953);
    let main_slab = register_transport_for_slab(&mut backend, main_slab);
    let main_token = backend.connection_registry
        [crate::framework::capabilities::io::Token(main_slab)]
    .token()
    .0;
    let main = ConnectionId(main_token);
    let snapshot_end = 100;
    let replid = "0123456789abcdef0123456789abcdef01234567".to_string();
    let mut link = ReplicaLink::streaming(
        main,
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        replid,
        offset(snapshot_end),
    );
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::RdbLoaded)
        .unwrap();
    let replay_session_id = link.session_id();
    backend.replication_runtime.set_replica_link(link);
    let _ = backend.replication_runtime.set_replica_pending_buffer(
        replay_session_id,
        PendingReplicationBuffer::new(1 << 20, offset(snapshot_end), 0),
    );
    backend.pending_replication_replay = Some(
        crate::runtime::server::worker::event_loop::blocked_events::PendingReplayContinuation::new_for_test(
            main,
            replay_session_id,
            crate::foundation::io::sys::time::mstime().saturating_sub(101),
        ),
    );
    backend.transport.queue_test_transport_event_batch(&[(
        Token(main_token),
        false,
        false,
        true,
        false,
    )]);

    let result = backend.process_events_while_blocked(BlockedOperation::PendingReplay { main });
    assert_eq!(result.dispatched_events, 1);
    assert_eq!(result.deferred_events, 0);
    assert!(backend.blocked_events.pop_front_deferred().is_none());
    backend.drive_one_pending_replication_replay();

    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    assert!(backend.pending_replication_replay.is_none());
    assert!(!backend.transport.is_registered(Token(main_token)));
    assert!(!backend.ctx.is_loading());
    assert_eq!(backend.ctx.replication().slave_repl_offset, offset(0));
}

#[test]
fn pending_replay_does_not_resurrect_cancelled_session() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.replication_mut().switch_to_replica();
    let main_slab = add_connection(&mut backend, 952);
    let (_main_peer, main_slab) = register_live_transport_for_slab(&mut backend, main_slab);
    let main = ConnectionId(
        backend.connection_registry[crate::framework::capabilities::io::Token(main_slab)]
            .token()
            .0,
    );
    let mut link = ReplicaLink::streaming(
        main,
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "0123456789abcdef0123456789abcdef01234567".to_string(),
        offset(100),
    );
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::RdbLoaded)
        .unwrap();
    let replay_session_id = link.session_id();
    backend.replication_runtime.set_replica_link(link);
    let mut pending = PendingReplicationBuffer::new(1 << 20, offset(100), 0);
    pending
        .push(ReplicaEvent::Apply {
            apply: ReplicationApply::Ping,
            end_offset: offset(101),
        })
        .unwrap();
    let _ = backend
        .replication_runtime
        .set_replica_pending_buffer(replay_session_id, pending);
    backend.refresh_replica_output_buffer_bytes();
    assert!(backend.ctx.replication().replica_output_buffer_bytes > 0);
    backend.pending_replication_replay = Some(
        crate::runtime::server::worker::event_loop::blocked_events::PendingReplayContinuation::new_for_test(
            main,
            replay_session_id,
            crate::foundation::io::sys::time::mstime().saturating_sub(101),
        ),
    );

    backend.remove_connection(main.0);

    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    assert!(backend.pending_replication_replay.is_none());
    assert_eq!(backend.ctx.replication().replica_output_buffer_bytes, 0);
}

#[test]
fn pending_replay_retake_rejects_replaced_same_token_session() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = ConnectionId(954);
    let original = ReplicaLink::new(
        main,
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let original_session_id = original.session_id();
    backend.replication_runtime.set_replica_link(original);

    let replacement = ReplicaLink::new(
        main,
        MasterInfo {
            host: "127.0.0.2".to_string(),
            port: 6380,
        },
    );
    let replacement_session_id = replacement.session_id();
    backend.replication_runtime.set_replica_link(replacement);

    assert!(backend
        .retake_pending_replay_session(main, original_session_id)
        .is_none());
    assert_eq!(
        backend
            .replication_runtime
            .replica_link()
            .expect("replacement session must remain runtime-owned")
            .session_id(),
        replacement_session_id
    );
}

#[test]
fn dual_mode_pending_buffer_ordering_a_main_commands_before_rdb_loaded() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_connection(&mut backend, 201);

    let snapshot_end_offset: i64 = 100;
    let selected_db: u32 = 0;
    let replid = "0123456789abcdef0123456789abcdef01234567".to_string();

    let mut link = ReplicaLink::new(
        ConnectionId(201),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.set_resume_position(replid.clone(), offset(snapshot_end_offset), selected_db);
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)
        .unwrap();
    link.provisional_primary = Some(ProvisionalPrimary {
        replid,
        end_offset: offset(snapshot_end_offset),
        db_id: selected_db,
        rdb_client_id: 201,
    });

    assert!(
        link.is_dual_rdb_mode_active(),
        "ReceivingRdb + provisional_primary => dual mode must be active"
    );
    assert_eq!(link.applied_position().get(), snapshot_end_offset);

    let events = vec![
        ReplicaEvent::Apply {
            apply: ReplicationApply::Ping,
            end_offset: offset(101),
        },
        ReplicaEvent::Apply {
            apply: ReplicationApply::Ping,
            end_offset: offset(102),
        },
    ];

    let result = backend.buffer_replica_events_into_pending(connection_token, &mut link, events);
    assert!(result.is_ok());

    assert_eq!(
        link.applied_position().get(),
        snapshot_end_offset,
        "applied_offset must NOT advance after buffering into pending"
    );

    let buffer = backend
        .replication_runtime
        .replica_pending_buffer()
        .expect("pending buffer must be created");
    assert_eq!(buffer.len(), 2, "pending buffer must contain 2 events");
    assert_eq!(buffer.snapshot_end_offset().get(), snapshot_end_offset);
    assert_eq!(buffer.selected_db(), selected_db);

    let entries: Vec<PendingEntry> = {
        let buf = backend
            .replication_runtime
            .replica_pending_buffer_mut_for(link.session_id())
            .unwrap();
        std::iter::from_fn(|| buf.pop_front()).collect()
    };
    assert_eq!(entries.len(), 2);
    match &entries[0].event {
        ReplicaEvent::Apply { end_offset, .. } => assert_eq!(end_offset.get(), 101),
    }
    match &entries[1].event {
        ReplicaEvent::Apply { end_offset, .. } => assert_eq!(end_offset.get(), 102),
    }

    assert!(
        backend
            .replication_runtime
            .replica_pending_buffer()
            .unwrap()
            .is_empty(),
        "after drain, buffer must be empty but still retained"
    );

    link.transition_rdb_channel_state(ReplicaRdbChannelState::RdbLoaded)
        .unwrap();
    assert!(
        link.is_dual_rdb_mode_active(),
        "RdbLoaded + provisional_primary => dual mode must still be active before replay"
    );
}

#[test]
fn dual_mode_pending_buffer_ordering_b_rdb_loaded_before_main_commands() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_connection(&mut backend, 202);

    let snapshot_end_offset: i64 = 200;
    let max_end_offset: i64 = 202;
    let selected_db: u32 = 1;
    let replid = "abcdef0123456789abcdef0123456789abcdef01".to_string();

    let mut link = ReplicaLink::new(
        ConnectionId(202),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.set_resume_position(replid.clone(), offset(max_end_offset), selected_db);
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)
        .unwrap();
    link.provisional_primary = Some(ProvisionalPrimary {
        replid,
        end_offset: offset(snapshot_end_offset),
        db_id: selected_db,
        rdb_client_id: 202,
    });

    let events = vec![
        ReplicaEvent::Apply {
            apply: ReplicationApply::GetAck,
            end_offset: offset(201),
        },
        ReplicaEvent::Apply {
            apply: ReplicationApply::Ping,
            end_offset: offset(202),
        },
    ];

    let result = backend.buffer_replica_events_into_pending(connection_token, &mut link, events);
    assert!(result.is_ok());

    assert_eq!(
        link.applied_position().get(),
        max_end_offset,
        "applied_offset must NOT advance after buffering into pending (still in ReceivingRdb)"
    );

    let buffer = backend
        .replication_runtime
        .replica_pending_buffer()
        .expect("pending buffer must be created in ReceivingRdb ordering B");
    assert_eq!(
        buffer.len(),
        2,
        "pending buffer must contain 2 events in ordering B"
    );
    assert_eq!(buffer.snapshot_end_offset().get(), snapshot_end_offset);
    assert_eq!(buffer.selected_db(), selected_db);

    let entries: Vec<PendingEntry> = {
        let buf = backend
            .replication_runtime
            .replica_pending_buffer_mut_for(link.session_id())
            .unwrap();
        std::iter::from_fn(|| buf.pop_front()).collect()
    };
    assert_eq!(entries.len(), 2);
    match &entries[0].event {
        ReplicaEvent::Apply { end_offset, apply } => {
            assert_eq!(end_offset.get(), 201);
            assert!(matches!(apply, ReplicationApply::GetAck));
        }
    }
    match &entries[1].event {
        ReplicaEvent::Apply { end_offset, apply } => {
            assert_eq!(end_offset.get(), 202);
            assert!(matches!(apply, ReplicationApply::Ping));
        }
    }

    link.transition_rdb_channel_state(ReplicaRdbChannelState::RdbLoaded)
        .unwrap();
    assert!(
        link.is_dual_rdb_mode_active(),
        "RdbLoaded + provisional_primary => dual mode must be active after transition"
    );
}

#[test]
fn dual_commit_handoff_ordering_a_buffered_then_commit() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let main_token = 301usize;
    let rdb_token = 302usize;
    let main_slab = add_connection(&mut backend, main_token);
    let (_main_peer, main_slab) = register_live_transport_for_slab(&mut backend, main_slab);
    let main_token = backend.connection_registry
        [crate::framework::capabilities::io::Token(main_slab)]
    .token()
    .0;
    let rdb_slab = add_connection(&mut backend, rdb_token);
    let (_rdb_peer, rdb_slab) = register_live_transport_for_slab(&mut backend, rdb_slab);
    let rdb_token = backend.connection_registry
        [crate::framework::capabilities::io::Token(rdb_slab)]
    .token()
    .0;

    let snapshot_end_offset: i64 = 100;
    let max_end_offset: i64 = 103;
    let selected_db: u32 = 0;
    let replid = "0123456789abcdef0123456789abcdef01234567".to_string();

    let mut link = ReplicaLink::streaming(
        ConnectionId(main_token),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        replid.clone(),
        offset(max_end_offset),
    );
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)
        .unwrap();
    link.provisional_primary = Some(ProvisionalPrimary {
        replid: replid.clone(),
        end_offset: offset(snapshot_end_offset),
        db_id: selected_db,
        rdb_client_id: rdb_token as u64,
    });
    backend.replication_runtime.set_replica_link(link);
    backend
        .replication_runtime
        .set_replica_rdb_link(ConnectionId(rdb_token), ConnectionId(main_token))
        .unwrap();
    let mut link = backend.replication_runtime.take_replica_link().unwrap();

    let k1_args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"k1"),
        Bytes::from_static(b"v1"),
    ]);
    let k2_args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"k2"),
        Bytes::from_static(b"v2"),
    ]);
    let events = vec![
        ReplicaEvent::Apply {
            apply: ReplicationApply::Command(k1_args),
            end_offset: offset(101),
        },
        ReplicaEvent::Apply {
            apply: ReplicationApply::GetAck,
            end_offset: offset(102),
        },
        ReplicaEvent::Apply {
            apply: ReplicationApply::Command(k2_args),
            end_offset: offset(103),
        },
    ];

    backend
        .buffer_replica_events_into_pending(main_slab, &mut link, events)
        .unwrap();
    assert_eq!(
        backend
            .replication_runtime
            .replica_pending_buffer()
            .unwrap()
            .len(),
        3
    );

    let empty_dbs = backend.ctx.dbs().empty_like();
    let mut rdb_recv = RdbReceive::new(
        true,
        crate::framework::distributed::replication::RdbLoadMode::Swap,
    );
    rdb_recv.set_swap(empty_dbs);
    let commit: RdbCommit = rdb_recv.finish();

    link.transition_rdb_channel_state(ReplicaRdbChannelState::RdbLoaded)
        .unwrap();
    backend
        .commit_streamed_rdb(rdb_slab, &mut link, commit)
        .unwrap();
    assert!(
        backend.transport.is_registered(Token(rdb_token)),
        "pending replay must retain the RDB transport until terminal handoff"
    );
    assert!(backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_slab)));
    assert!(backend.replication_runtime.replica_rdb_link().is_some());
    backend.replication_runtime.set_replica_link(link);
    backend.drive_one_pending_replication_replay();
    let mut link = backend.replication_runtime.take_replica_link().unwrap();

    assert_eq!(link.applied_position().get(), max_end_offset);
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    assert_eq!(link.rdb_channel_state(), ReplicaRdbChannelState::None);
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_token)));
    assert_eq!(
        backend.ctx.replication().slave_repl_offset,
        link.applied_position()
    );
    assert_eq!(link.selected_db(), selected_db);
    assert_eq!(
        backend
            .ctx
            .db(selected_db)
            .expect("selected DB must exist after handoff")
            .get_no_expire(b"k1")
            .map(|value| value.as_bytes()),
        Some(&b"v1"[..]),
        "the first pending command must remain first after handoff"
    );
    assert_eq!(
        backend
            .ctx
            .db(selected_db)
            .expect("selected DB must exist after handoff")
            .get_no_expire(b"k2")
            .map(|value| value.as_bytes()),
        Some(&b"v2"[..]),
        "the second pending command must follow the first after handoff"
    );
    let queued_main: Vec<u8> = backend.connection_registry
        [crate::framework::capabilities::io::Token(main_slab)]
    .output
    .queue
    .iter()
    .flat_map(|chunk| chunk.iter().copied())
    .collect();
    let expected_terminal_ack =
        crate::engine::distributed::replication::protocol::build_replconf_ack(
            offset(max_end_offset),
            None,
        );
    assert_eq!(
        queued_main, expected_terminal_ack,
        "terminal ACK must satisfy buffered GETACK after replay reaches steady state"
    );

    backend
        .commit_and_replay_dual_handoff_only(main_slab, &mut link)
        .unwrap();
    let queued_after_repeat: Vec<u8> = backend.connection_registry
        [crate::framework::capabilities::io::Token(main_slab)]
    .output
    .queue
    .iter()
    .flat_map(|chunk| chunk.iter().copied())
    .collect();
    assert_eq!(
        queued_after_repeat, expected_terminal_ack,
        "terminal dual handoff must be idempotent after RDB state becomes None"
    );

    assert!(
        backend.ctx.db(selected_db).is_some(),
        "db {} must exist after streamed install (ordering A)",
        selected_db
    );
}

#[test]
fn empty_dual_pending_buffer_finalizes_handoff() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let main_token = 401usize;
    let rdb_token = 402usize;
    let main_slab = add_connection(&mut backend, main_token);
    let rdb_slab = add_connection(&mut backend, rdb_token);

    let snapshot_end_offset: i64 = 200;
    let selected_db: u32 = 0;
    let replid = "abcdef0123456789abcdef0123456789abcdef01".to_string();

    let mut link = ReplicaLink::streaming(
        ConnectionId(main_token),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        replid.clone(),
        offset(snapshot_end_offset),
    );
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)
        .unwrap();
    link.provisional_primary = Some(ProvisionalPrimary {
        replid,
        end_offset: offset(snapshot_end_offset),
        db_id: selected_db,
        rdb_client_id: rdb_token as u64,
    });
    backend.replication_runtime.set_replica_link(link);
    backend
        .replication_runtime
        .set_replica_rdb_link(ConnectionId(rdb_token), ConnectionId(main_token))
        .unwrap();
    let mut link = backend.replication_runtime.take_replica_link().unwrap();

    let empty_dbs = backend.ctx.dbs().empty_like();
    let mut rdb_recv = RdbReceive::new(
        true,
        crate::framework::distributed::replication::RdbLoadMode::Swap,
    );
    rdb_recv.set_swap(empty_dbs);
    let commit: RdbCommit = rdb_recv.finish();

    link.transition_rdb_channel_state(ReplicaRdbChannelState::RdbLoaded)
        .unwrap();
    backend
        .commit_streamed_rdb(rdb_slab, &mut link, commit)
        .unwrap();

    assert!(
        backend
            .replication_runtime
            .replica_pending_buffer()
            .is_none(),
        "zero pending commands must not require a buffer allocation"
    );
    assert!(
        backend.replication_runtime.replica_rdb_link().is_none(),
        "completed empty handoff must detach the RDB link"
    );
    assert_eq!(
        backend
            .replication_runtime
            .channel_pair_for(ConnectionId(main_token)),
        None,
        "completed empty handoff must remove the main/RDB channel pair"
    );
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_slab)));
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_token)));
    assert_eq!(link.rdb_channel_state(), ReplicaRdbChannelState::None);
    assert_eq!(backend.ctx.config().replication.master_link_status, "up");

    let queued_main: Vec<u8> = backend.connection_registry
        [crate::framework::capabilities::io::Token(main_slab)]
    .output
    .queue
    .iter()
    .flat_map(|chunk| chunk.iter().copied())
    .collect();
    let expected_ack = crate::engine::distributed::replication::protocol::build_replconf_ack(
        offset(snapshot_end_offset),
        None,
    );
    assert_eq!(
        queued_main, expected_ack,
        "completed empty handoff must queue exactly one ACK"
    );
}

#[test]
fn dual_handoff_drains_main_socket_before_terminal_ack() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.replication_mut().switch_to_replica();

    let main_slab = add_connection(&mut backend, 1701);
    let (mut primary_stream, main_slab) = register_live_transport_for_slab(&mut backend, main_slab);
    let main_token = backend.connection_registry[Token(main_slab)].token().0;
    let selected_db: u32 = 0;
    let snapshot_end: i64 = 100;
    let replid = "0123456789abcdef0123456789abcdef01234567".to_string();

    let mut link = ReplicaLink::streaming(
        ConnectionId(main_token),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        replid.clone(),
        offset(snapshot_end),
    );
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::RdbLoaded)
        .unwrap();
    link.provisional_primary = Some(ProvisionalPrimary {
        replid,
        end_offset: offset(snapshot_end),
        db_id: selected_db,
        rdb_client_id: 0,
    });

    let frame = crate::engine::distributed::replication::protocol::encode_command(&[
        b"SET",
        b"post-snapshot",
        b"v",
    ]);
    primary_stream
        .write_all(&frame)
        .expect("write post-snapshot main frame before handoff");
    backend.transport.wait_readable_for_test(Token(main_slab));

    backend
        .commit_and_replay_dual_handoff_only(main_slab, &mut link)
        .unwrap();

    assert!(
        backend.pending_replication_replay.is_some(),
        "main-stream data drained during handoff must replay before terminal ACK"
    );
    assert_eq!(
        backend
            .replication_runtime
            .replica_pending_buffer()
            .expect("drained main-stream data must be buffered")
            .len(),
        1
    );
    assert!(
        backend.connection_registry[Token(main_slab)]
            .output
            .queue
            .is_empty(),
        "terminal ACK must wait until pending replay is applied"
    );

    backend.replication_runtime.set_replica_link(link);
    backend.drive_one_pending_replication_replay();
    let link = backend.replication_runtime.take_replica_link().unwrap();

    assert_eq!(link.rdb_channel_state(), ReplicaRdbChannelState::None);
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    assert_eq!(
        backend
            .ctx
            .db(selected_db)
            .expect("selected DB must exist after handoff")
            .get_no_expire(b"post-snapshot")
            .map(|value| value.as_bytes()),
        Some(&b"v"[..])
    );
    assert!(
        !backend.connection_registry[Token(main_slab)]
            .output
            .queue
            .is_empty(),
        "terminal ACK must be queued after pending replay"
    );

    backend.replication_runtime.set_replica_link(link);
}

#[test]
fn dual_commit_handoff_ordering_b_commit_then_buffered_triggers_handoff() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().replication.master_link_status = "sync".to_string();
    let main_token = 401usize;
    let rdb_token = 402usize;
    let main_slab = add_connection(&mut backend, main_token);
    let _rdb_slab = add_connection(&mut backend, rdb_token);
    backend.ctx.replication_mut().async_loading = true;

    let snapshot_end_offset: i64 = 200;
    let max_end_offset: i64 = 202;
    let selected_db: u32 = 0;
    let replid = "abcdef0123456789abcdef0123456789abcdef01".to_string();

    let mut link = ReplicaLink::new(
        ConnectionId(main_token),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.set_resume_position(replid.clone(), offset(max_end_offset), 0);
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)
        .unwrap();
    link.provisional_primary = Some(ProvisionalPrimary {
        replid: replid.clone(),
        end_offset: offset(snapshot_end_offset),
        db_id: selected_db,
        rdb_client_id: rdb_token as u64,
    });
    backend.replication_runtime.set_replica_link(link);
    backend
        .replication_runtime
        .set_replica_rdb_link(ConnectionId(rdb_token), ConnectionId(main_token))
        .unwrap();
    let mut link = backend.replication_runtime.take_replica_link().unwrap();

    let empty_dbs = backend.ctx.dbs().empty_like();
    let mut rdb_recv = RdbReceive::new(
        true,
        crate::framework::distributed::replication::RdbLoadMode::Swap,
    );
    rdb_recv.set_swap(empty_dbs);
    let commit: RdbCommit = rdb_recv.finish();

    link.transition_rdb_channel_state(ReplicaRdbChannelState::RdbLoaded)
        .unwrap();
    backend
        .commit_streamed_rdb(main_slab, &mut link, commit)
        .unwrap();

    assert_eq!(backend.ctx.config().replication.master_link_status, "sync");
    assert!(
        backend.ctx.replication().async_loading,
        "the RDB dataset is committed, but a dual-channel replica remains loading until main PSYNC completes"
    );
    assert!(
        backend.connection_registry[crate::framework::capabilities::io::Token(main_slab)]
            .output
            .queue
            .is_empty()
    );

    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n", 6379).unwrap();
    link.on_bytes(b"+OK\r\n", 6379).unwrap();
    link.on_bytes(b"+OK\r\n", 6379).unwrap();
    link.on_bytes(b"+CONTINUE\r\n", 6379).unwrap();
    assert!(link.is_streaming());
    backend
        .commit_and_replay_dual_handoff_only(main_slab, &mut link)
        .unwrap();

    assert!(!backend.ctx.replication().async_loading);
    assert_eq!(link.applied_position().get(), max_end_offset);
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    assert_eq!(
        link.rdb_channel_state(),
        ReplicaRdbChannelState::None,
        "terminal handoff must close the RDB phase before later main-stream events"
    );
    assert!(
        link.provisional_primary().is_none(),
        "terminal handoff must not leave later main-stream events in dual buffering mode"
    );

    let k3_args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"k3"),
        Bytes::from_static(b"v3"),
    ]);
    let k4_args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"k4"),
        Bytes::from_static(b"v4"),
    ]);
    let events = vec![
        ReplicaEvent::Apply {
            apply: ReplicationApply::Command(k3_args),
            end_offset: offset(201),
        },
        ReplicaEvent::Apply {
            apply: ReplicationApply::Command(k4_args),
            end_offset: offset(202),
        },
    ];
    assert_eq!(
        link.rdb_channel_state(),
        ReplicaRdbChannelState::None,
        "post-handoff main-stream events must not reactivate the RDB phase"
    );

    backend
        .buffer_replica_events_into_pending(main_slab, &mut link, events)
        .unwrap();

    assert_eq!(link.applied_position().get(), max_end_offset);
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    assert_eq!(link.selected_db(), selected_db);
    assert_eq!(
        backend.connection_registry[crate::framework::capabilities::io::Token(main_slab)]
            .selected_db,
        selected_db
    );
    assert_eq!(backend.ctx.config().replication.master_link_status, "up");
    let queued_main: Vec<u8> = backend.connection_registry
        [crate::framework::capabilities::io::Token(main_slab)]
    .output
    .queue
    .iter()
    .flat_map(|chunk| chunk.iter().copied())
    .collect();
    assert_eq!(
        queued_main,
        crate::engine::distributed::replication::protocol::build_replconf_ack(
            offset(max_end_offset),
            None,
        ),
        "terminal dual handoff must queue exactly one ACK"
    );

    assert!(
        backend.ctx.db(selected_db).is_some(),
        "db {} must exist after streamed install (ordering B)",
        selected_db
    );
}

#[test]
fn over_limit_retains_entire_decoded_batch_and_pauses_main_once() {
    use crate::foundation::io::transport::Interest;
    let entry_overhead = std::mem::size_of::<PendingEntry>();

    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);

    let main_token = 201usize;
    let main_slab = add_connection(&mut backend, main_token);
    let (_main_peer, main_slab) = register_live_transport_for_slab(&mut backend, main_slab);
    let main_token = backend.connection_registry
        [crate::framework::capabilities::io::Token(main_slab)]
    .token()
    .0;

    let main_connection_id = ConnectionId(main_token);
    let selected_db: u32 = 0;
    let snapshot_end: i64 = 100;

    let replid = "0123456789abcdef0123456789abcdef01234567".to_string();
    let mut link = ReplicaLink::streaming(
        main_connection_id,
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        replid.clone(),
        offset(snapshot_end),
    );
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)
        .unwrap();
    link.provisional_primary = Some(ProvisionalPrimary {
        replid,
        end_offset: offset(snapshot_end),
        db_id: selected_db,
        rdb_client_id: 0,
    });

    backend.replication_runtime.set_replica_link(link);

    let mut link = backend.replication_runtime.take_replica_link().unwrap();
    let ev1_args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"k1"),
        Bytes::from_static(b"v1"),
    ]);
    let first_event_bytes = entry_overhead
        + ev1_args.name.len()
        + ev1_args.args.iter().map(Bytes::len).sum::<usize>()
        + ev1_args.args.capacity() * std::mem::size_of::<Bytes>();
    let buffer = crate::framework::distributed::replication::PendingReplicationBuffer::new(
        first_event_bytes,
        offset(snapshot_end),
        selected_db,
    );
    let _ = backend
        .replication_runtime
        .set_replica_pending_buffer(link.session_id(), buffer);
    let frame1 =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SET", b"k1", b"v1"]);
    let (events1, actions1) = link.on_bytes(&frame1, 6379).unwrap();
    assert!(actions1.is_empty());
    backend
        .buffer_replica_events_into_pending(main_slab, &mut link, events1)
        .unwrap();

    {
        let buf = backend
            .replication_runtime
            .replica_pending_buffer()
            .unwrap();
        assert_eq!(buf.len(), 1);
        assert!(buf.is_full());
    }

    backend.transport.clear_interest_requests_for_test();
    let main_conn_token =
        backend.connection_registry[crate::framework::capabilities::io::Token(main_slab)].token();

    let mut batch =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SET", b"k2", b"v2"]);
    batch.extend_from_slice(
        &crate::engine::distributed::replication::protocol::encode_command(&[b"SET", b"k3", b"v3"]),
    );
    let (events2, actions2) = link.on_bytes(&batch, 6379).unwrap();
    assert!(actions2.is_empty());
    assert_eq!(
        events2.len(),
        2,
        "both commands must be decoded in one batch"
    );
    backend
        .buffer_replica_events_into_pending(main_slab, &mut link, events2)
        .unwrap();

    assert!(
        backend.replication_runtime.replica_main_read_paused(),
        "paused flag must be true after OverLimit"
    );

    let interest_calls = backend.transport.last_set_interest_calls_for_test();
    assert_eq!(interest_calls.len(), 1, "main readable must be paused once");
    let last_call = interest_calls.last().unwrap();
    assert_eq!(
        last_call.0, main_conn_token,
        "interest update must target main channel token"
    );
    assert_eq!(
        last_call.1,
        Interest::Writable,
        "interest must be WRITABLE only (no READABLE) after OverLimit, got {:?}",
        last_call.1
    );

    assert!(
        backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(main_conn_token.0)),
        "connection must remain registered (no remove/close)"
    );

    let buf = backend
        .replication_runtime
        .replica_pending_buffer()
        .unwrap();
    assert_eq!(
        buf.len(),
        3,
        "all already-decoded events must remain in the pending FIFO"
    );
    assert!(
        buf.used_bytes() > buf.max_bytes(),
        "the pending buffer may overshoot by the retained crossing entry"
    );

    let mut pending = backend
        .replication_runtime
        .take_replica_pending_buffer()
        .unwrap();
    let mut replay = Vec::new();
    while let Some(entry) = pending.pop_front() {
        replay.push(entry.event);
    }
    link.reset_applied_offset_to_dual_snapshot();
    backend
        .apply_replica_events(main_slab, &mut link, replay)
        .unwrap();
    assert_eq!(link.applied_position(), link.received_position());
    assert_eq!(
        backend
            .ctx
            .db(selected_db)
            .unwrap()
            .get_no_expire(b"k1")
            .map(|value| value.as_bytes()),
        Some(&b"v1"[..])
    );
    assert_eq!(
        backend
            .ctx
            .db(selected_db)
            .unwrap()
            .get_no_expire(b"k2")
            .map(|value| value.as_bytes()),
        Some(&b"v2"[..])
    );
    assert_eq!(
        backend
            .ctx
            .db(selected_db)
            .unwrap()
            .get_no_expire(b"k3")
            .map(|value| value.as_bytes()),
        Some(&b"v3"[..])
    );

    backend.replication_runtime.set_replica_link(link);
}

#[test]
fn handoff_resets_main_channel_interest_back_to_readable() {
    use crate::foundation::io::transport::Interest;

    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);

    let main_token = 301usize;
    let main_slab = add_connection(&mut backend, main_token);
    let (_main_peer, main_slab) = register_live_transport_for_slab(&mut backend, main_slab);

    let main_connection_id = ConnectionId(
        backend.connection_registry[crate::framework::capabilities::io::Token(main_slab)]
            .token()
            .0,
    );
    let selected_db: u32 = 0;
    let snapshot_end: i64 = 100;

    let mut link = ReplicaLink::new(
        main_connection_id,
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)
        .unwrap();
    let replid = "0123456789abcdef0123456789abcdef01234567".to_string();
    link.provisional_primary = Some(ProvisionalPrimary {
        replid: replid.clone(),
        end_offset: offset(snapshot_end),
        db_id: selected_db,
        rdb_client_id: 0,
    });
    link.set_resume_position(replid, offset(snapshot_end), selected_db);
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n", 6379).unwrap();
    link.on_bytes(b"+OK\r\n", 6379).unwrap();
    link.on_bytes(b"+OK\r\n", 6379).unwrap();
    link.on_bytes(b"+CONTINUE\r\n", 6379).unwrap();
    assert!(link.is_streaming());

    backend.replication_runtime.set_replica_link(link);

    let mut link = backend.replication_runtime.take_replica_link().unwrap();
    let buffer = crate::framework::distributed::replication::PendingReplicationBuffer::new(
        1 << 20,
        offset(snapshot_end),
        selected_db,
    );
    let _ = backend
        .replication_runtime
        .set_replica_pending_buffer(link.session_id(), buffer);

    backend.transport.clear_interest_requests_for_test();
    backend
        .replication_runtime
        .set_replica_main_read_paused(true);
    let main_conn_token =
        backend.connection_registry[crate::framework::capabilities::io::Token(main_slab)].token();

    let _ = backend
        .transport
        .set_interest(main_conn_token, Interest::Writable);

    link.transition_rdb_channel_state(ReplicaRdbChannelState::RdbLoaded)
        .unwrap();
    assert_eq!(link.connection_id().0, main_conn_token.0);
    assert!(backend.connection_registry.contains(main_conn_token));

    let result = backend.commit_and_replay_dual_handoff_only(main_slab, &mut link);
    assert!(
        result.is_ok(),
        "commit_and_replay_dual_handoff_only should succeed, got {:?}",
        result
    );
    assert_eq!(
        backend
            .pending_replication_replay
            .as_ref()
            .expect("empty pending buffer schedules replay finalization")
            .session_id,
        link.session_id()
    );
    assert!(backend
        .connection_registry
        .contains(Token(link.connection_id().0)));
    assert!(link.is_streaming());
    assert_eq!(link.rdb_channel_state(), ReplicaRdbChannelState::RdbLoaded);
    backend.replication_runtime.set_replica_link(link);
    backend.drive_one_pending_replication_replay();
    let link = backend.replication_runtime.take_replica_link().unwrap();
    assert_eq!(link.rdb_channel_state(), ReplicaRdbChannelState::None);

    assert!(
        !backend.replication_runtime.replica_main_read_paused(),
        "paused flag must be reset to false after handoff"
    );

    let interest_calls = backend.transport.last_set_interest_calls_for_test();
    assert!(
        !interest_calls.is_empty(),
        "set_interest must be called during handoff to restore READABLE"
    );
    let last_call = interest_calls.last().unwrap();
    assert_eq!(
        last_call.0, main_conn_token,
        "interest restore must target main channel"
    );
    assert_eq!(
        last_call.1,
        Interest::ReadWritable,
        "interest must be ReadWritable after handoff complete, got {:?}",
        last_call.1
    );

    assert!(
        backend
            .replication_runtime
            .replica_pending_buffer()
            .is_none(),
        "pending buffer must be taken (None) after handoff"
    );

    assert!(
        backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(main_conn_token.0)),
        "main connection must remain registered after handoff"
    );

    backend.replication_runtime.set_replica_link(link);
}

fn setup_paused_loaded_handoff(
    backend: &mut ServerWorkerState,
    with_pending_buffer: bool,
) -> (usize, usize, ReplicaLink, TcpStream) {
    let (main_token, main_slab, _rdb_token, primary_stream) =
        setup_dual_blocked_main_receiver(backend);
    let mut link = backend.replication_runtime.take_replica_link().unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::RdbLoaded)
        .unwrap();
    if with_pending_buffer {
        backend.replication_runtime.set_replica_pending_buffer(
            link.session_id(),
            PendingReplicationBuffer::new(1 << 20, offset(100), 0),
        );
    }
    backend
        .replication_runtime
        .set_replica_main_read_paused(true);
    backend.ctx.config_mut().replication.master_link_status = "sync".to_string();
    (main_token, main_slab, link, primary_stream)
}

#[test]
fn direct_handoff_resume_failure_keeps_paused_and_not_up() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    let (main_token, main_slab, mut link, _primary_stream) =
        setup_paused_loaded_handoff(&mut backend, false);
    backend
        .transport
        .deregister_for_test(Token(main_token))
        .unwrap();

    let result = backend.commit_and_replay_dual_handoff_only(main_slab, &mut link);

    assert!(result.is_err());
    assert!(backend.replication_runtime.replica_main_read_paused());
    assert_ne!(backend.ctx.config().replication.master_link_status, "up");
}

#[test]
fn pending_replay_resume_failure_keeps_paused_and_not_up() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let (main_token, main_slab, mut link, _primary_stream) =
        setup_paused_loaded_handoff(&mut backend, true);
    backend
        .transport
        .deregister_for_test(Token(main_token))
        .unwrap();

    let result = backend.commit_and_replay_dual_handoff_only(main_slab, &mut link);

    assert!(result.is_ok());
    backend.replication_runtime.set_replica_link(link);
    backend.drive_one_pending_replication_replay();
    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(backend.pending_replication_replay.is_none());
    assert!(!backend.replication_runtime.replica_main_read_paused());
    assert_ne!(backend.ctx.config().replication.master_link_status, "up");
}

#[test]
fn pending_replay_apply_failure_refreshes_memory_accounting() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.replication_mut().switch_to_replica();
    let (_main_token, main_slab, mut link, _primary_stream) =
        setup_paused_loaded_handoff(&mut backend, false);
    let mut pending = PendingReplicationBuffer::new(1 << 20, offset(100), 99);
    pending
        .push(ReplicaEvent::Apply {
            apply: ReplicationApply::Ping,
            end_offset: offset(101),
        })
        .unwrap();
    let _ = backend
        .replication_runtime
        .set_replica_pending_buffer(link.session_id(), pending);
    backend.refresh_replica_output_buffer_bytes();
    assert!(backend.ctx.replication().replica_output_buffer_bytes > 0);

    let result = backend.commit_and_replay_dual_handoff_only(main_slab, &mut link);

    assert!(result.is_ok());
    backend.replication_runtime.set_replica_link(link);
    backend.drive_one_pending_replication_replay();
    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(backend.pending_replication_replay.is_none());
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    assert_eq!(backend.ctx.replication().replica_output_buffer_bytes, 0);
}

#[test]
fn finalization_after_candidate_endoff_transitions_once_without_a_second_frame() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main_token = 201usize;
    let (main_slab, replica_id) =
        setup_primary(&mut backend, main_token, dual_channel_caps(), true, true);

    let _replies = run_psync(&mut backend, main_slab, main_token, b"PSYNC");
    {
        let link = backend
            .replication_runtime
            .primary_link(replica_id)
            .unwrap();
        assert_eq!(
            link.dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForRdbChannel
        );
    }

    let rdb_token = 202usize;
    let rdb_slab = add_connection(&mut backend, rdb_token);
    backend
        .replication_runtime
        .register_rdb_candidate(crate::framework::distributed::replication::ConnectionId(
            rdb_token,
        ))
        .unwrap();

    let main_id = crate::framework::distributed::replication::ConnectionId(main_token);
    let rdb_id = crate::framework::distributed::replication::ConnectionId(rdb_token);
    let snapshot_offset = backend.ctx.replication().backlog.reported_offset().get();
    send_endoff_for_test(&mut backend, rdb_id);
    let replid = backend.ctx.replication().backlog.replid().to_string();
    backend
        .replication_runtime
        .associate_rdb_channel(main_id, rdb_id)
        .unwrap();

    let protected_before_first;
    {
        let backlog = &backend.ctx.replication().backlog;
        protected_before_first = backlog.min_protected_cursor();
    }

    {
        let ServerWorkerState {
            ref mut replication_runtime,
            ..
        } = backend;
        replication_runtime
            .finalize_dual_channel_rdb_association(main_id, rdb_id)
            .unwrap();
    }

    {
        let link = backend
            .replication_runtime
            .primary_link(replica_id)
            .unwrap();
        assert_eq!(
            link.dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForMainPsync
        );
    }
    let protected_after_first;
    {
        let backlog = &backend.ctx.replication().backlog;
        protected_after_first = backlog.min_protected_cursor();
    }
    assert_eq!(
        protected_after_first, None,
        "an empty C replication-buffer tail is pinned only after its first frame"
    );
    assert_eq!(
        protected_before_first, protected_after_first,
        "finalization must not change the candidate's backlog protection"
    );

    let queued_rdb_after_first: Vec<u8> = backend.connection_registry
        [crate::framework::capabilities::io::Token(rdb_slab)]
    .output
    .queue
    .iter()
    .flat_map(|c| c.iter().copied())
    .collect();
    let expected_endoff = crate::engine::distributed::replication::protocol::encode_end_offset(
        offset(snapshot_offset),
        &replid,
        0,
        rdb_token as u64,
    );
    let first_count = queued_rdb_after_first
        .windows(expected_endoff.len())
        .filter(|w| *w == expected_endoff.as_slice())
        .count();
    assert_eq!(
        first_count,
        1,
        "candidate registration must queue exactly one $ENDOFF frame, count={first_count}, bytes={:?}",
        String::from_utf8_lossy(&queued_rdb_after_first)
    );

    let result_second = {
        let ServerWorkerState {
            ref mut replication_runtime,
            ..
        } = backend;
        replication_runtime.finalize_dual_channel_rdb_association(main_id, rdb_id)
    };
    assert!(
        result_second.is_ok(),
        "duplicate finalize should be a silent NOOP (Ok), not an Err. duplicate Err semantics are also OK but NOOP is preferred. got: {:?}",
        result_second.err()
    );

    let protected_after_second;
    {
        let backlog = &backend.ctx.replication().backlog;
        protected_after_second = backlog.min_protected_cursor();
    }
    assert_eq!(
        protected_after_second, protected_after_first,
        "duplicate finalize must not change backlog protection: before={protected_after_first:?} after={protected_after_second:?}"
    );

    let queued_rdb_after_second: Vec<u8> = backend.connection_registry
        [crate::framework::capabilities::io::Token(rdb_slab)]
    .output
    .queue
    .iter()
    .flat_map(|c| c.iter().copied())
    .collect();
    let second_count = queued_rdb_after_second
        .windows(expected_endoff.len())
        .filter(|w| *w == expected_endoff.as_slice())
        .count();
    assert_eq!(
        second_count, 1,
        "duplicate finalize must NOT queue another $ENDOFF frame (still 1 total). count={second_count}, bytes={:?}",
        String::from_utf8_lossy(&queued_rdb_after_second)
    );

    let different_rdb_token = 203usize;
    let _different_slab = add_connection(&mut backend, different_rdb_token);
    backend
        .replication_runtime
        .register_rdb_candidate(crate::framework::distributed::replication::ConnectionId(
            different_rdb_token,
        ))
        .unwrap();
    let different_rdb_id =
        crate::framework::distributed::replication::ConnectionId(different_rdb_token);

    let result_diff = {
        let ServerWorkerState {
            ref mut replication_runtime,
            ..
        } = backend;
        replication_runtime.finalize_dual_channel_rdb_association(main_id, different_rdb_id)
    };
    assert!(
        result_diff.is_ok(),
        "finalize with a DIFFERENT rdb id while already WaitingForMainPsync must also be NOOP, got Err: {:?}",
        result_diff.err()
    );
    let protected_after_diff;
    {
        let backlog = &backend.ctx.replication().backlog;
        protected_after_diff = backlog.min_protected_cursor();
    }
    assert_eq!(
        protected_after_diff, protected_after_first,
        "associating different rdb id must NOT re-pin backlog"
    );
}
#[test]
fn rdb_open_pair_rollback_defers_failed_transport_deregistration() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main_connection_id = ConnectionId(41);
    let main_slab = add_connection(&mut backend, main_connection_id.0);
    backend
        .replication_runtime
        .set_replica_link(ReplicaLink::new(
            main_connection_id,
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        ));

    let rdb_transport = backend
        .transport
        .bind_tcp("127.0.0.1:0".parse().unwrap())
        .unwrap();
    let rdb_connection_id = ConnectionId(rdb_transport.token.0);
    backend
        .route_runtime_source(
            rdb_transport.token,
            crate::runtime::server::worker::event_loop::sources::SourceOwner::Connection,
        )
        .expect("route the synthetic RDB connection");
    backend.replication_runtime.register_primary(
        rdb_connection_id,
        crate::framework::distributed::replication::ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .transport
        .deregister_for_test(rdb_transport.token)
        .unwrap();

    let error = backend
        .open_replica_rdb_channel_for_test(main_connection_id, rdb_connection_id)
        .expect_err("conflicting role must reject RDB pairing");

    assert_eq!(error.stage(), ReplicaRdbOpenStage::Pair);
    assert!(backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(main_slab)));
    assert_eq!(
        backend.replication_runtime.role(main_connection_id),
        Some(ReplicationConnection::Replica)
    );
    assert!(backend.replication_runtime.replica_rdb_link().is_none());
    assert!(backend
        .replication_runtime
        .channel_pair_for(main_connection_id)
        .is_none());
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(
            rdb_connection_id.0
        )));
    assert_eq!(
        backend
            .pending_transport_removals
            .get(&Token(rdb_connection_id.0))
            .map(|removal| removal.owner_kind.label()),
        Some("replica RDB")
    );
    assert!(backend
        .runtime_sources
        .resolve_token(Token(rdb_connection_id.0))
        .is_none());
    assert!(backend.transport.is_registered(rdb_transport.token));

    backend
        .transport
        .reregister_for_test(rdb_transport.token)
        .unwrap();
    backend.retry_pending_transport_removals();

    assert!(!backend
        .pending_transport_removals
        .contains_key(&Token(rdb_connection_id.0)));
    assert!(!backend.transport.is_registered(rdb_transport.token));
}
