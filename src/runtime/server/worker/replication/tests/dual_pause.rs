use super::*;
use crate::foundation::io::transport::{Interest, ReadOutcome, WriteOutcome};
use crate::runtime::server::worker::connection::read::ClientReadMode;
use crate::runtime::server::worker::event_loop::sources::{
    ChildExitOutcome, ChildOwner, TransferId,
};
use crate::runtime::server::worker::event_loop::RunnableKey;
use crate::runtime::server::worker::replication::diskless_child::DisklessRelay;

fn install_paused_relay(backend: &mut ServerWorkerState) {
    backend.ctx.config_mut().server.debug_pause_after_fork = true;
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id: TransferId(0),
        source: None,
        child_release_write_end: None,
        targets: Vec::new(),
        marker: Vec::new(),
        complete_full_resync: false,
        not_before_ms: 0,
        eof_reached: false,
        marker_queued_targets: Default::default(),
        marker_sent_targets: Default::default(),
        completion_announced: false,
        completion_state: Default::default(),
    });
}

fn threaded_ordinary_client() -> (ServerWorkerState, usize, TcpStream) {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_command_registry_snapshot(&backend.registry);
    backend.ctx.config_mut().server.io_threads = 2;
    backend.ctx.config_mut().server.io_threads_always_active = true;
    backend.configure_io_threads_from_config().unwrap();
    let (ordinary, peer) = add_runtime_connection(&mut backend);
    (backend, ordinary, peer)
}

#[test]
fn threaded_write_completion_keeps_the_dual_pause_barrier() {
    use crate::runtime::server::worker::event_loop::io_workers::WorkerCompletionEvent;
    let (mut backend, ordinary, _peer) = threaded_ordinary_client();
    let token = Token(ordinary);
    backend.connection_registry[token].queue_reply(b"+OK\r\n".to_vec());
    backend.dispatch_client_write(token);
    let WorkerCompletionEvent::SocketWrite(completion) = backend
        .recv_worker_completion_for_test(std::time::Duration::from_secs(1))
        .unwrap()
    else {
        panic!("expected the submitted ordinary write");
    };
    install_paused_relay(&mut backend);

    backend.complete_io_write(completion);

    assert_eq!(backend.io_threads.write_inflight_id(token), None);
    assert!(backend.socket_write_owned_by_main_thread(token));
    assert_eq!(
        backend.transport.last_set_interest_calls_for_test().last(),
        Some(&(token, Interest::None))
    );
    assert!(backend.full_sync.paused_dual_clients.contains(&token));
}

#[test]
fn dual_pause_fork_reclaims_threaded_writes_without_admitting_queued_reads() {
    assert_pause_fork_reclaims_threaded_writes(false);
}

#[test]
fn dual_pause_fork_reclaims_writes_submitted_before_replica_role_conversion() {
    assert_pause_fork_reclaims_threaded_writes(true);
}

fn assert_pause_fork_reclaims_threaded_writes(has_replica_role: bool) {
    use crate::runtime::server::worker::event_loop::io_workers::{
        IoCompletion, IoJob, WorkerCompletionEvent,
    };
    let (mut backend, ordinary, mut peer) = threaded_ordinary_client();
    let token = Token(ordinary);
    backend.connection_registry[token].queue_reply(b"+OK\r\n".to_vec());
    backend.dispatch_client_write(token);
    let write_completion = backend
        .recv_worker_completion_for_test(std::time::Duration::from_secs(1))
        .unwrap();
    assert!(matches!(
        write_completion,
        WorkerCompletionEvent::SocketWrite(_)
    ));
    if has_replica_role {
        boundary_replconf(
            &mut backend,
            ordinary,
            vec![
                Bytes::from_static(b"listening-port"),
                Bytes::from_static(b"6381"),
            ],
        );
        assert!(backend
            .replication_runtime
            .primary_for_connection(ConnectionId(ordinary))
            .is_some());
    }
    peer.write_all(&protocol::encode_command(&[b"PING"]))
        .unwrap();
    backend.transport.wait_pending_bytes_for_test(token, 14);
    let job = backend
        .io_threads
        .take_read_job(token, 65536, false)
        .unwrap();
    backend.acquire_io_worker_read_socket_lease(job.id);
    let IoCompletion::Read(read_completion) = IoJob::Read(job).execute().unwrap() else {
        panic!("expected a completed queued read");
    };
    backend.enqueue_worker_completion(WorkerCompletionEvent::SocketRead(read_completion));
    backend.enqueue_worker_completion(write_completion);
    // Model a read handler entering the pause while another write completion
    // is already staged in the same dispatch batch.
    backend.worker_completions.stage_pending();
    backend.ctx.config_mut().server.debug_pause_after_fork = true;
    let options = crate::foundation::io::persistence::rdb::compression::RdbSnapshotOptions {
        compression: backend.ctx.config().persistence.rdbcompression,
        checksum: backend.ctx.config().persistence.rdbchecksum,
    };

    let (read_end, marker, release) = backend
        .try_begin_diskless_fork_with_options(&[], options, "dual-channel", false)
        .unwrap();
    let write_still_owned = backend.io_threads.write_inflight_id(token).is_some();
    let read_was_admitted = backend.ctx.stats().cmd_stats.contains_key("ping");
    let (rdb, _rdb_peer) = add_runtime_connection(&mut backend);
    backend
        .replication_runtime
        .register_rdb_candidate(ConnectionId(rdb))
        .unwrap();
    let target = super::super::diskless_child::DisklessTarget::Rdb {
        connection_id: ConnectionId(rdb),
        client_id: backend.connection_registry[Token(rdb)].client_id(),
    };
    backend.store_diskless_transport_relay(read_end, release, marker, vec![target], false, 0);
    let transfer = backend
        .full_sync
        .diskless_relay
        .as_ref()
        .unwrap()
        .transfer_id;
    backend.on_diskless_pipe_error(transfer);

    assert!(
        !write_still_owned,
        "the pause boundary must reclaim all ordinary writes"
    );
    assert!(
        !read_was_admitted,
        "draining writes must not execute queued commands"
    );
    assert!(backend.io_threads.read_inflight_id(token).is_some());
    backend.drain_io_completions();
    assert!(backend.io_threads.read_inflight_id(token).is_none());
    assert_eq!(backend.ctx.stats().cmd_stats["ping"].calls, 1);
}

#[test]
fn failed_dual_relay_restores_paused_client_reads_and_async_writes() {
    for failure in 0..3 {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let (ordinary, mut peer) = add_runtime_connection(&mut backend);
        install_paused_relay(&mut backend);
        backend.process_client_read_outcome(
            Token(ordinary),
            ClientReadMode::Fair,
            protocol::encode_command(&[b"PING"]),
            ReadOutcome::Quiescent,
        );
        backend.connection_registry[Token(ordinary)].queue_reply(b"$-1\r\n".to_vec());
        if failure == 0 {
            backend.on_diskless_pipe_error(TransferId(0));
        } else {
            let pid = i32::MAX;
            let child = backend
                .child_registry
                .register_test_child(ChildOwner::DisklessReplication, pid);
            backend.child_registry.mark_pidfd_registered(child);
            backend
                .child_registry
                .record_exit_for_test(child, ChildExitOutcome::Exited(1));
            backend
                .ctx
                .persistence_mut()
                .begin_diskless_child(pid)
                .unwrap();
            if failure == 1 {
                backend.collect_idle_diskless_child_exit();
            } else {
                backend.abort_diskless_relay_for_dataset_flush();
            }
        }
        assert!(backend.full_sync.diskless_relay.is_none());
        assert!(backend.full_sync.paused_dual_clients.is_empty());
        assert!(backend
            .event_batch
            .contains_runnable(RunnableKey::ClientRead(Token(ordinary))));
        assert!(backend
            .event_batch
            .contains_runnable(RunnableKey::ClientWrite(Token(ordinary))));
        backend.drive_pending_ordinary_writes();
        peer.set_read_timeout(Some(std::time::Duration::from_millis(100)))
            .unwrap();
        let mut reply = [0; 5];
        peer.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"$-1\r\n");
    }
}

#[test]
fn paused_dual_relay_blocks_async_output_and_stale_writable_events() {
    for route in 0..3 {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let (ordinary, mut peer) = add_runtime_connection(&mut backend);
        install_paused_relay(&mut backend);
        if route == 0 {
            backend.queue_client_reply(Token(ordinary), b"$-1\r\n".to_vec());
            backend.drive_pending_ordinary_writes();
        } else {
            backend.connection_registry[Token(ordinary)].queue_reply(b"$-1\r\n".to_vec());
            if route == 1 {
                backend.dispatch_client_write(Token(ordinary));
            } else {
                assert!(matches!(
                    backend.flush_connection_output(ordinary),
                    WriteOutcome::Backpressured
                ));
            }
        }
        peer.set_nonblocking(true).unwrap();
        let mut byte = [0];
        assert!(
            matches!(peer.read(&mut byte), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
        assert!(backend
            .full_sync
            .paused_dual_clients
            .contains(&Token(ordinary)));
        assert_eq!(
            backend.transport.last_set_interest_calls_for_test().last(),
            Some(&(Token(ordinary), Interest::None))
        );
        assert!(!backend
            .event_batch
            .contains_runnable(RunnableKey::ClientWrite(Token(ordinary))));
        backend.on_diskless_pipe_error(TransferId(0));
        assert!(backend
            .event_batch
            .contains_runnable(RunnableKey::ClientWrite(Token(ordinary))));
    }
}

fn pending_paused_main(backend: &mut ServerWorkerState) -> (usize, TcpStream, u64) {
    backend
        .ctx
        .config_mut()
        .replication
        .dual_channel_replication = true;
    backend.ctx.config_mut().replication.repl_diskless_sync = true;
    backend
        .ctx
        .config_mut()
        .replication
        .repl_diskless_sync_delay = 30;
    let (main, peer) = boundary_waiting_main(backend);
    let (rdb, _rdb_peer) = boundary_sync_candidate(backend, b"");
    backend
        .replication_runtime
        .take_endoff_for_rdb_candidate_if_unambiguous(
            ConnectionId(rdb),
            true,
            &mut backend.ctx.replication_mut().backlog,
        )
        .unwrap();
    install_paused_relay(backend);
    let client_id = backend.connection_registry[Token(rdb)].client_id();
    (main, peer, client_id)
}

fn associated_paused_main(backend: &mut ServerWorkerState) -> (usize, TcpStream) {
    let (main, peer, client_id) = pending_paused_main(backend);
    boundary_replconf(
        backend,
        main,
        vec![
            Bytes::from_static(b"set-rdb-client-id"),
            Bytes::from(client_id.to_string()),
        ],
    );
    // Keep the artificial relay alive independently of the RDB test peer.
    (main, peer)
}

#[test]
fn association_and_psync_in_one_batch_keep_the_association_reply_first() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let (main, _peer, client_id) = pending_paused_main(&mut backend);
    let id_text = client_id.to_string();
    let mut pipeline =
        protocol::encode_command(&[b"REPLCONF", b"set-rdb-client-id", id_text.as_bytes()]);
    pipeline.extend_from_slice(&protocol::encode_command(&[b"PSYNC", b"?", b"-1"]));

    backend.process_client_read_outcome(
        Token(main),
        ClientReadMode::Fair,
        pipeline,
        ReadOutcome::Quiescent,
    );

    assert!(backend.connection_registry[Token(main)]
        .parser
        .has_pending_data());
    assert!(!backend.connection_registry[Token(main)].has_pending_output());
    assert_eq!(
        backend.full_sync.paused_dual_main_replies[&Token(main)],
        vec![b"+OK\r\n".to_vec()]
    );
    assert_eq!(
        backend
            .replication_runtime
            .primary_for_connection(ConnectionId(main))
            .unwrap()
            .dual_state(),
        PrimaryDualChannelState::WaitingForMainPsync
    );
}

#[test]
fn association_installs_reply_barrier_before_batch_final_state_changes() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let (main, _peer) = associated_paused_main(&mut backend);
    assert!(backend
        .full_sync
        .paused_dual_main_replies
        .contains_key(&Token(main)));
    let mut replies = vec![b"+OK\r\n".to_vec()];
    backend.queue_origin_replies(main, main, &mut replies);
    backend
        .replication_runtime
        .primary_for_connection_mut(ConnectionId(main))
        .unwrap()
        .set_state(PrimaryLinkState::WaitingForReplicaAck);
    let mut later = vec![b"+CONTINUE\r\n".to_vec()];
    backend.queue_origin_replies(main, main, &mut later);
    assert!(!backend.connection_registry[Token(main)].has_pending_output());
    assert_eq!(
        backend.full_sync.paused_dual_main_replies[&Token(main)],
        vec![b"+OK\r\n".to_vec(), b"+CONTINUE\r\n".to_vec()]
    );
}

#[test]
fn association_reply_barrier_defers_already_received_psync_pipeline() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let (main, _peer) = associated_paused_main(&mut backend);
    let mut replies = vec![b"+OK\r\n".to_vec()];
    backend.queue_origin_replies(main, main, &mut replies);
    backend.process_client_read_outcome(
        Token(main),
        ClientReadMode::Fair,
        protocol::encode_command(&[b"PSYNC", b"?", b"-1"]),
        ReadOutcome::Quiescent,
    );
    assert!(backend.connection_registry[Token(main)]
        .parser
        .has_pending_data());
    assert_eq!(
        backend
            .replication_runtime
            .primary_for_connection(ConnectionId(main))
            .unwrap()
            .dual_state(),
        PrimaryDualChannelState::WaitingForMainPsync
    );
    assert!(backend.full_sync.paused_dual_clients.contains(&Token(main)));
}

#[test]
fn detached_replica_owner_keeps_ack_output_from_resuming_reads() {
    use crate::framework::distributed::replication::ReplicaAction;

    for uses_action in [false, true] {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let (main_value, _peer) = add_runtime_connection(&mut backend);
        let main = Token(main_value);
        backend
            .replication_runtime
            .set_replica_link(ReplicaLink::new(
                ConnectionId(main.0),
                MasterInfo {
                    host: "127.0.0.1".to_string(),
                    port: 6379,
                },
            ));
        backend
            .replication_runtime
            .set_replica_main_read_paused(true);
        let link = backend.replication_runtime.take_replica_link().unwrap();
        backend.transport.clear_interest_requests_for_test();
        let frame = protocol::encode_command(&[b"REPLCONF", b"ACK", b"0"]);
        if uses_action {
            backend.queue_replica_actions(main.0, vec![ReplicaAction::Send(frame)]);
        } else {
            backend.queue_client_reply(main, frame);
        }
        assert_eq!(
            backend.transport.last_set_interest_calls_for_test().last(),
            Some(&(main, Interest::Writable))
        );
        backend.replication_runtime.set_replica_link(link);
        backend.dispatch_client_write(main);
        assert_eq!(
            backend.transport.last_set_interest_calls_for_test().last(),
            Some(&(main, Interest::None))
        );
    }
}
