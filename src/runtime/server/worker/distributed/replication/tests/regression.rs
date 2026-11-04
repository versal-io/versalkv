use super::*;
use std::fs::File;
#[cfg(target_os = "linux")]
use std::io::ErrorKind;
use std::io::Write;
use std::net::TcpListener;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;

fn classic_blocked_rdb_receiver() -> (ServerWorkerState, Token, std::net::TcpStream) {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().replication.repl_diskless_load = "flush-before-load".to_string();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind classic RDB peer");
    let peer_addr = listener.local_addr().expect("classic RDB peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect classic RDB peer");
    let (primary_stream, _) = listener.accept().expect("accept classic RDB peer");
    backend.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr,
        },
    );
    let mut link = ReplicaLink::new(
        ConnectionId(peer.token.0),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6379)
        .expect("advance classic link to PSYNC reply");
    backend.replication_runtime.set_replica_link(link);
    (backend, peer.token, primary_stream)
}

fn minimal_classic_rdb_fullresync_frame() -> Vec<u8> {
    let rdb = b"VALKEY080\x00\x01k\x01v\xff\x00\x00\x00\x00\x00\x00\x00\x00";
    let mut frame = b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0\r\n".to_vec();
    frame.extend_from_slice(format!("${}\r\n", rdb.len()).as_bytes());
    frame.extend_from_slice(rdb);
    frame
}

#[test]
fn malformed_streaming_frame_tears_down_the_production_replica_link() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let (main_token, mut primary_stream) = add_runtime_connection(&mut backend);
    let link = ReplicaLink::streaming(
        ConnectionId(main_token),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "0123456789abcdef0123456789abcdef01234567".to_string(),
        offset(0),
    );
    backend.replication_runtime.set_replica_link(link);
    primary_stream
        .write_all(b"*1\r\n!4\r\nPONG\r\n")
        .expect("write malformed replication command");
    backend.transport.wait_readable_for_test(Token(main_token));

    backend.recv_replication_stream(main_token, main_token);

    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(!backend.transport.is_registered(Token(main_token)));
    assert_eq!(backend.ctx.config().replication.master_link_status, "down");
}

fn install_diskless_source_for_test(
    backend: &mut ServerWorkerState,
    read_end: File,
) -> (
    crate::runtime::server::worker::reactor::sources::TransferId,
    crate::runtime::server::worker::reactor::sources::RuntimeSourceHandle,
) {
    backend.full_sync.next_transfer_id =
        backend.full_sync.next_transfer_id.saturating_add(1).max(1);
    let transfer_id = crate::runtime::server::worker::reactor::sources::TransferId(
        backend.full_sync.next_transfer_id,
    );
    let source = backend
        .install_runtime_source(
            crate::foundation::io::transport::OwnedPollSource::readable(read_end),
            crate::runtime::server::worker::reactor::sources::SourceOwner::InternalPipe(
                crate::runtime::server::worker::reactor::sources::InternalPipeOwner::DisklessRdb {
                    transfer_id,
                },
            ),
        )
        .expect("install diskless relay source");
    (transfer_id, source)
}

fn diskless_pipe_source_for_test(payload: &[u8]) -> File {
    let pipe =
        crate::foundation::io::sys::fd::pipe(crate::foundation::io::sys::fd::PipeFlags::NONE)
            .expect("create diskless relay pipe");
    let (read_end, mut write_end) = pipe.into_parts();
    write_end.write_all(payload).expect("write relay payload");
    drop(write_end);
    read_end
}

#[cfg(target_os = "linux")]
fn fill_transport_send_buffer_for_test(backend: &mut ServerWorkerState, token: Token) {
    let payload = vec![b'x'; 256 * 1024];
    for _ in 0..2048 {
        match backend.transport.write(token, &payload) {
            Ok(0) => break,
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::WouldBlock => return,
            Err(error) => panic!("prefill write failed: {error}"),
        }
    }
    panic!("replica peer send buffer did not reach WouldBlock");
}

#[test]
fn replicated_commands_are_visible_to_monitor_clients() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let replication_key = add_connection(&mut backend, 41);
    let monitor_key = add_connection(&mut backend, 42);
    backend.connection_registry[crate::framework::capabilities::io::Token(monitor_key)]
        .monitoring = true;
    let monitor_client_id = backend.connection_registry
        [crate::framework::capabilities::io::Token(monitor_key)]
    .client_id();
    backend.ctx.register_monitor_client(monitor_client_id);
    let mut link = ReplicaLink::new(
        ConnectionId(41),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"HSET"),
        Bytes::from_static(b"hash"),
        Bytes::from_static(b"field"),
        Bytes::from_static(b"value"),
    ]);

    backend
        .apply_replication_command(
            replication_key,
            &mut link,
            ReplicationApply::Command(args),
            CommandOrigin::Replication,
        )
        .expect("replicated HSET should apply");

    let output = _queued(
        &backend.connection_registry[crate::framework::capabilities::io::Token(monitor_key)],
    );
    assert!(
        output
            .windows(b"\"HSET\" \"hash\" \"field\" \"value\"".len())
            .any(|window| window == b"\"HSET\" \"hash\" \"field\" \"value\""),
        "replication-originated commands must be delivered to MONITOR: {}",
        String::from_utf8_lossy(&output)
    );
}

#[test]
fn diskless_relay_yields_before_draining_a_large_ready_source() {
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let (source, writer) =
        crate::foundation::io::sys::fd::pipe_with_background_payload(vec![b'x'; 32 * 1024])
            .expect("create background diskless source");
    let (transfer_id, source) = install_diskless_source_for_test(&mut backend, source);
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id,
        source: Some(source),
        child_release_write_end: None,
        targets: Vec::new(),
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: false,
        marker_queued_targets: std::collections::HashSet::new(),
        marker_sent_targets: std::collections::HashSet::new(),
        completion_announced: false,
        completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
    });

    backend.on_diskless_pipe_readable(transfer_id);

    assert!(
        backend.full_sync.diskless_relay.is_some(),
        "one event-loop turn must not drain an always-ready multi-megabyte pipe to EOF"
    );

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while backend
        .full_sync
        .diskless_relay
        .as_ref()
        .is_some_and(|relay| !relay.eof_reached)
        && std::time::Instant::now() < deadline
    {
        backend.on_diskless_pipe_readable(transfer_id);
        std::thread::yield_now();
    }
    assert!(
        backend
            .full_sync
            .diskless_relay
            .as_ref()
            .is_some_and(|relay| relay.eof_reached && relay.completion_announced),
        "bounded relay turns must still make forward progress to EOF"
    );
    crate::foundation::io::sys::fd::join_background_pipe_writer(writer);
    // This fixture uses a writer thread in place of a registered child.
    // Publish its actual successful exit separately from pipe EOF.
    backend
        .full_sync
        .diskless_relay
        .as_mut()
        .unwrap()
        .completion_state
        .apply_fact(crate::runtime::server::worker::distributed::replication::diskless_child::DisklessFact::ChildExited(
            crate::runtime::server::worker::reactor::sources::ChildExitOutcome::Exited(0),
        ));
    backend.finish_diskless_relay_if_ready();
    assert!(
        backend.full_sync.diskless_relay.is_none(),
        "the completed writer and drained relay must release transfer ownership"
    );
}

#[test]
fn diskless_relay_consumes_only_one_producer_chunk_per_turn() {
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let (source, writer) =
        crate::foundation::io::sys::fd::pipe_with_background_payload(vec![b'x'; 16 * 1024 + 1])
            .expect("create background diskless source");
    let (transfer_id, source) = install_diskless_source_for_test(&mut backend, source);
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id,
        source: Some(source),
        child_release_write_end: None,
        targets: Vec::new(),
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: false,
        marker_queued_targets: std::collections::HashSet::new(),
        marker_sent_targets: std::collections::HashSet::new(),
        completion_announced: false,
        completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
    });

    backend.on_diskless_pipe_readable(transfer_id);

    assert!(
        backend
            .full_sync
            .diskless_relay
            .as_ref()
            .is_some_and(|relay| !relay.eof_reached && relay.source.is_some()),
        "one pipe readiness must consume at most one producer chunk"
    );
    drop(backend);
    crate::foundation::io::sys::fd::join_background_pipe_writer_after_reader_close(writer);
}

#[test]
#[cfg(target_os = "linux")]
fn preexisting_diskless_backpressure_disables_pipe_source_until_target_drains() {
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;
    use crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer;

    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind replica peer");
    let peer_addr = listener.local_addr().expect("replica peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect replica transport");
    let (_replica_stream, _) = listener.accept().expect("accept replica transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(peer.token.0),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: peer_addr.port(),
            capability: vec!["eof".to_string(), "psync2".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    fill_transport_send_buffer_for_test(&mut backend, peer.token);
    backend.connection_registry[peer.token].queue_reply(vec![b'x'; 64 * 1024]);
    let source = diskless_pipe_source_for_test(&vec![b'r'; 1024]);
    let (transfer_id, source) = install_diskless_source_for_test(&mut backend, source);
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id,
        source: Some(source),
        child_release_write_end: None,
        targets: vec![replica_id.into()],
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: false,
        marker_queued_targets: std::collections::HashSet::new(),
        marker_sent_targets: std::collections::HashSet::new(),
        completion_announced: false,
        completion_state:
            crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
    });

    backend.on_diskless_pipe_readable(transfer_id);

    assert!(
        !backend
            .transport
            .source_enabled_for_test(source.token)
            .expect("inspect diskless source enabled state"),
        "preexisting target backpressure must suspend pipe readiness"
    );
}

#[test]
fn diskless_source_registration_failure_closes_partial_targets() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 55;
    add_connection(&mut backend, token_value);
    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(token_value),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["eof".to_string(), "psync2".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let data_pipe =
        crate::foundation::io::sys::fd::pipe(crate::foundation::io::sys::fd::PipeFlags::NONE)
            .expect("create data pipe");
    let release_pipe =
        crate::foundation::io::sys::fd::pipe(crate::foundation::io::sys::fd::PipeFlags::NONE)
            .expect("create release pipe");
    let (read_end, data_write_end) = data_pipe.into_parts();
    let (release_read_end, release_write_end) = release_pipe.into_parts();
    drop(data_write_end);
    drop(release_read_end);
    backend.transport.fail_register_source_for_test();

    backend.store_diskless_relay(
        read_end,
        release_write_end,
        Vec::new(),
        vec![replica_id],
        true,
        0,
    );

    assert!(
        backend.full_sync.diskless_relay.is_none(),
        "failed source registration must not leave an active relay"
    );
    assert!(
        !backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token_value)),
        "targets that already received partial diskless framing must be closed"
    );
    assert!(
        backend
            .replication_runtime
            .primary_link(replica_id)
            .is_none(),
        "replication link must be removed with the closed target"
    );
}

#[test]
fn drained_split_diskless_marker_is_marked_sent_on_target_writable() {
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 56;
    add_connection(&mut backend, token_value);
    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(token_value),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["eof".to_string(), "psync2".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("primary link exists")
        .set_state(PrimaryLinkState::PreparingFullResync);
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id: crate::runtime::server::worker::reactor::sources::TransferId(0),
        source: None,
        child_release_write_end: None,
        targets: vec![replica_id.into()],
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: true,
        marker_queued_targets: [replica_id.into()].into_iter().collect(),
        marker_sent_targets: std::collections::HashSet::new(),
        completion_announced: false,
        completion_state:
            crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
    });

    backend.on_diskless_target_writable(replica_id);

    let relay = backend
        .full_sync
        .diskless_relay
        .as_ref()
        .expect("terminal ACK still pending");
    assert!(
        relay.marker_sent_targets.contains(&replica_id.into()),
        "fully drained queued marker must be promoted to sent"
    );
    assert!(!relay.marker_queued_targets.contains(&replica_id.into()));
    assert_eq!(
        backend
            .replication_runtime
            .primary_link(replica_id)
            .expect("primary link remains registered")
            .state(),
        PrimaryLinkState::WaitingForReplicaAck
    );
}

#[test]
fn dropping_backpressured_diskless_target_reenables_pipe_for_survivors() {
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let slow_token = 57;
    let survivor_token = 58;
    add_connection(&mut backend, slow_token);
    add_connection(&mut backend, survivor_token);
    let slow = backend.replication_runtime.register_primary(
        ConnectionId(slow_token),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["eof".to_string(), "psync2".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let survivor = backend.replication_runtime.register_primary(
        ConnectionId(survivor_token),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6382,
            capability: vec!["eof".to_string(), "psync2".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend.connection_registry[crate::framework::capabilities::io::Token(slow_token)]
        .queue_reply(vec![b'x'; 1024]);
    let source = diskless_pipe_source_for_test(&vec![b'r'; 1024]);
    let (transfer_id, source) = install_diskless_source_for_test(&mut backend, source);
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id,
        source: Some(source),
        child_release_write_end: None,
        targets: vec![slow.into(), survivor.into()],
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: false,
        marker_queued_targets: std::collections::HashSet::new(),
        marker_sent_targets: std::collections::HashSet::new(),
        completion_announced: false,
        completion_state:
            crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
    });
    backend
        .transport
        .set_source_enabled(source.token, false)
        .expect("disable source as backpressure gate");

    backend.drop_diskless_relay_target(slow);

    assert_eq!(
        backend
            .full_sync
            .diskless_relay
            .as_ref()
            .expect("survivor keeps relay alive")
            .targets,
        vec![survivor.into()]
    );
    assert!(
        backend
            .transport
            .source_enabled_for_test(source.token)
            .expect("inspect diskless source enabled state"),
        "dropping the backpressured target must resume pipe readiness for survivors"
    );
}

#[test]
fn diskless_pipe_error_readiness_clears_relay_and_closes_targets() {
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;
    use crate::runtime::server::worker::reactor::ReadyEvent;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 59;
    add_connection(&mut backend, token_value);
    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(token_value),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["eof".to_string(), "psync2".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let source = diskless_pipe_source_for_test(&vec![b'r'; 1024]);
    let (transfer_id, source) = install_diskless_source_for_test(&mut backend, source);
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id,
        source: Some(source),
        child_release_write_end: None,
        targets: vec![replica_id.into()],
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: false,
        marker_queued_targets: std::collections::HashSet::new(),
        marker_sent_targets: std::collections::HashSet::new(),
        completion_announced: false,
        completion_state:
            crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
    });

    backend.dispatch_transport_event(
        ReadyEvent {
            token: source.token,
            readable: false,
            writable: false,
            error: true,
            read_hup: false,
        },
        false,
    );

    assert!(
        backend.full_sync.diskless_relay.is_none(),
        "hard pipe error must clear the diskless relay"
    );
    assert!(
        !backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token_value)),
        "hard pipe error must close partial targets"
    );
}

#[test]
fn classic_diskless_marker_publishes_online_before_terminal_ack() {
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;
    use crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer;

    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind replica peer");
    let peer_addr = listener.local_addr().expect("replica peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect replica transport");
    let (_replica_stream, _) = listener.accept().expect("accept replica transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(peer.token.0),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: peer_addr.port(),
            capability: vec!["eof".to_string(), "psync2".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("primary link exists")
        .set_state(PrimaryLinkState::PreparingFullResync);
    backend.refresh_replication_view();
    assert_eq!(
        backend
            .ctx
            .replication()
            .view
            .replica_info
            .values()
            .next()
            .expect("replica view exists")
            .state,
        "wait_bgsave"
    );
    let (transfer_id, source) =
        install_diskless_source_for_test(&mut backend, diskless_pipe_source_for_test(b""));
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id,
        source: Some(source),
        child_release_write_end: None,
        targets: vec![replica_id.into()],
        marker: b"0123456789012345678901234567890123456789".to_vec(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: false,
        marker_queued_targets: std::collections::HashSet::new(),
        marker_sent_targets: std::collections::HashSet::new(),
        completion_announced: false,
        completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
    });

    backend.on_diskless_pipe_readable(transfer_id);

    assert_eq!(
        backend
            .replication_runtime
            .primary_link(replica_id)
            .expect("primary link remains registered")
            .state(),
        PrimaryLinkState::WaitingForReplicaAck
    );
    assert_eq!(
        backend
            .ctx
            .replication()
            .view
            .replica_info
            .values()
            .next()
            .expect("replica view remains visible")
            .state,
        "online"
    );
}

#[test]
fn diskless_pipe_eof_explicitly_releases_child_with_inherited_writer() {
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    let _fork_guard = crate::framework::commands::server::save::diskless_fork_test_guard();
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    backend.ctx.config_mut().replication.repl_diskless_sync = true;
    let (read_end, marker, release) = backend
        .try_begin_diskless_fork(&[])
        .expect("fork diskless child");
    let inherited_release_write_end = release.try_clone().unwrap();
    let pid = backend
        .ctx
        .persistence()
        .diskless_child_pid()
        .expect("diskless child must be registered");
    let (transfer_id, source) = install_diskless_source_for_test(&mut backend, read_end);
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id,
        source: Some(source),
        child_release_write_end: Some(release),
        targets: Vec::new(),
        marker,
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: false,
        marker_queued_targets: std::collections::HashSet::new(),
        marker_sent_targets: std::collections::HashSet::new(),
        completion_announced: false,
        completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
    });

    let release_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        backend.on_diskless_pipe_readable(transfer_id);
        if backend
            .full_sync
            .diskless_relay
            .as_ref()
            .is_none_or(|relay| relay.child_release_write_end.is_none())
        {
            break;
        }
        assert!(
            Instant::now() < release_deadline,
            "diskless child did not reach the parent-release barrier"
        );
        sleep(Duration::from_millis(1));
    }

    // A fast child may already have exited and been reaped in the EOF
    // callback. The inherited writer stays open throughout this test, so
    // either outcome still requires the parent's explicit release byte.
    assert!(
        backend
            .full_sync
            .diskless_relay
            .as_ref()
            .is_none_or(|relay| relay.child_release_write_end.is_none()),
        "pipe EOF and marker flush must release the child"
    );
    assert!(backend
        .ctx
        .persistence()
        .diskless_child_pid()
        .is_none_or(|current| current == pid));
    let reap_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        backend.collect_idle_diskless_child_exit();
        if backend.full_sync.diskless_relay.is_none() {
            break;
        }
        assert!(
            Instant::now() < reap_deadline,
            "diskless child was not reaped after parent release"
        );
        sleep(Duration::from_millis(1));
    }
    assert_eq!(backend.ctx.persistence().diskless_child_pid(), None);
    assert!(backend.full_sync.diskless_relay.is_none());
    drop(inherited_release_write_end);
}

#[test]
fn dropping_diskless_relay_explicitly_releases_child_when_writer_is_inherited() {
    use crate::foundation::io::sys::fd::{pipe, PipeFlags};
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;
    use std::sync::mpsc;
    use std::time::Duration;

    let (read_end, write_end) = pipe(PipeFlags::CLOEXEC).unwrap().into_parts();
    let inherited_write_end = write_end.try_clone().unwrap();
    let (released_tx, released_rx) = mpsc::channel();
    let child = std::thread::spawn(move || {
        crate::framework::capabilities::process::wait_for_pipe_eof(read_end);
        released_tx.send(()).unwrap();
    });
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id: crate::runtime::server::worker::reactor::sources::TransferId(1),
        source: None,
        child_release_write_end: Some(write_end),
        targets: Vec::new(),
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: false,
        marker_queued_targets: std::collections::HashSet::new(),
        marker_sent_targets: std::collections::HashSet::new(),
        completion_announced: false,
        completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
    });

    drop(backend);
    let release_result = released_rx.recv_timeout(Duration::from_secs(2));
    drop(inherited_write_end);
    child.join().unwrap();

    assert!(
        release_result.is_ok(),
        "dropping the relay must explicitly release the child even when another process inherited the write end"
    );
}

#[test]
#[cfg(target_os = "linux")]
fn successful_diskless_child_reap_preserves_acknowledged_replica() {
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;
    use std::process::Command;
    use std::thread::sleep;
    use std::time::Duration;

    let mut child = Command::new("true").spawn().expect("spawn child");
    let pid = child.id() as i32;
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 48;
    add_connection(&mut backend, token_value);
    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(token_value),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let link = backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("primary link must exist");
    link.set_state(PrimaryLinkState::PreparingFullResync);
    link.finish_full_resync();
    link.acknowledge(offset(0), crate::foundation::io::sys::time::mstime());
    assert!(link.promote_full_resync_on_ack());
    backend
        .ctx
        .persistence_mut()
        .begin_diskless_child(pid)
        .unwrap();
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id: crate::runtime::server::worker::reactor::sources::TransferId(0),
        source: None,
        child_release_write_end: None,
        targets: vec![replica_id.into()],
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: true,
        marker_queued_targets: std::collections::HashSet::new(),
        marker_sent_targets: [replica_id.into()].into_iter().collect(),
        completion_announced: false,
        completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
    });
    backend.finish_diskless_relay_if_ready();
    assert_eq!(
        backend
            .replication_runtime
            .count_good_replicas(1_000, crate::foundation::io::sys::time::mstime()),
        1,
        "an acknowledged marker survivor must already be a good replica"
    );

    for _ in 0..100 {
        backend.collect_idle_diskless_child_exit();
        if backend.full_sync.diskless_relay.is_none() {
            break;
        }
        sleep(Duration::from_millis(10));
    }

    assert_eq!(backend.ctx.persistence().diskless_child_pid(), None);
    assert!(backend.full_sync.diskless_relay.is_none());
    assert_eq!(
        backend
            .replication_runtime
            .primary_link(replica_id)
            .expect("successful target must stay connected")
            .state(),
        PrimaryLinkState::Streaming
    );
    assert_eq!(
        backend
            .replication_runtime
            .count_good_replicas(1_000, crate::foundation::io::sys::time::mstime()),
        1,
        "successful child reap publishes the replica as good"
    );
    let _ = child.wait();
}

#[test]
fn diskless_completion_waits_for_each_classic_target_outcome() {
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;

    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().replication.repl_timeout = 1;
    let now_ms = 10_000;
    let now_sec = 10;

    let fast_token = 51;
    let slow_token = 52;
    add_connection(&mut backend, fast_token);
    add_connection(&mut backend, slow_token);
    let fast = backend.replication_runtime.register_primary(
        ConnectionId(fast_token),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["eof".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let slow = backend.replication_runtime.register_primary(
        ConnectionId(slow_token),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6382,
            capability: vec!["eof".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    {
        let fast_link = backend
            .replication_runtime
            .primary_link_mut(fast)
            .expect("fast primary link must exist");
        fast_link.set_state(PrimaryLinkState::Streaming);
        fast_link.acknowledge(offset(0), now_ms);
        let slow_link = backend
            .replication_runtime
            .primary_link_mut(slow)
            .expect("slow primary link must exist");
        slow_link.set_state(PrimaryLinkState::PreparingFullResync);
        slow_link.finish_full_resync();
        slow_link.acknowledge(offset(0), now_ms - 1_001);
    }
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id: crate::runtime::server::worker::reactor::sources::TransferId(0),
        source: None,
        child_release_write_end: Some(File::open("/dev/null").unwrap()),
        targets: vec![fast.into(), slow.into()],
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: true,
        marker_queued_targets: std::collections::HashSet::new(),
        marker_sent_targets: [fast.into(), slow.into()].into_iter().collect(),
        completion_announced: false,
        completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
    });

    backend.finish_diskless_relay_if_ready();
    let relay = backend
        .full_sync
        .diskless_relay
        .as_ref()
        .expect("unresolved target must keep the relay active");
    assert!(
        relay.completion_announced,
        "physical transport completion is announced independently of target ACK outcomes"
    );
    assert!(
        !relay.completion_state.published(),
        "transport notification must not publish the unresolved command stream"
    );
    assert!(
        relay.child_release_write_end.is_none(),
        "the completed producer is released while replica ACK outcomes remain pending"
    );

    backend.disconnect_timedout_replicas_at(now_ms, now_sec);

    assert!(backend.replication_runtime.primary_link(slow).is_none());
    assert!(backend.replication_runtime.primary_link(fast).is_some());
    let relay = backend
        .full_sync
        .diskless_relay
        .as_ref()
        .expect("surviving target must keep the relay until child reap");
    assert_eq!(relay.targets, vec![fast.into()]);
    assert!(relay.completion_announced);
    assert!(relay.child_release_write_end.is_none());
    backend.finish_diskless_relay_if_ready();
    assert!(
        backend
            .full_sync
            .diskless_relay
            .as_ref()
            .expect("idempotent completion must retain the relay")
            .completion_announced
    );
}

#[test]
fn deferred_dual_reply_overrun_can_retire_the_completed_relay() {
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let (main_token, _peer) = add_runtime_connection(&mut backend);
    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(main_token),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["eof".to_string(), "dual-channel".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .ctx
        .config_mut()
        .replication
        .normal_output_buffer_hard_limit = 1;
    backend
        .full_sync
        .paused_dual_main_replies
        .insert(Token(main_token), vec![b"+OK\r\n".to_vec(); 4096]);
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id: crate::runtime::server::worker::reactor::sources::TransferId(0),
        source: None,
        child_release_write_end: None,
        targets: vec![replica_id.into()],
        marker: Vec::new(),
        complete_full_resync: false,
        not_before_ms: 0,
        eof_reached: true,
        marker_queued_targets: [replica_id.into()].into_iter().collect(),
        marker_sent_targets: [replica_id.into()].into_iter().collect(),
        completion_announced: false,
        completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
    });

    backend.finish_diskless_relay_if_ready();

    assert!(!backend.connection_registry.contains(Token(main_token)));
    assert!(backend.full_sync.diskless_relay.is_none());
    assert!(backend.full_sync.paused_dual_main_replies.is_empty());
    assert_eq!(
        backend
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        1
    );
}

#[test]
fn paused_replica_ack_preserves_read_backpressure_after_writable_drain() {
    use crate::foundation::io::transport::Interest;
    use crate::framework::distributed::replication::ReplicaAction;

    for uses_replica_action in [false, true] {
        let (mut backend, main, _peer) = classic_blocked_rdb_receiver();
        backend
            .replication_runtime
            .set_replica_main_read_paused(true);
        backend
            .transport
            .set_interest(main, Interest::None)
            .unwrap();
        backend.transport.clear_interest_requests_for_test();
        let frame = protocol::encode_command(&[b"REPLCONF", b"ACK", b"0"]);
        if uses_replica_action {
            backend.queue_replica_actions(main.0, vec![ReplicaAction::Send(frame)]);
        } else {
            assert!(backend.queue_client_reply(main, frame));
        }
        assert_eq!(
            backend.transport.last_set_interest_calls_for_test().last(),
            Some(&(main, Interest::Writable)),
            "ACK output must not resume a primary stream paused by the pending buffer"
        );

        backend.handle_writable(main);

        assert!(!backend.connection_registry[main].has_pending_output());
        assert!(backend.replication_runtime.replica_main_read_paused());
        assert_eq!(
            backend.transport.last_set_interest_calls_for_test().last(),
            Some(&(main, Interest::None)),
            "a drained paused stream must not keep polling a writable socket"
        );
    }
}

#[test]
fn paused_dual_relay_disables_ordinary_socket_events_until_abort() {
    use crate::foundation::io::transport::{Interest, ReadOutcome};
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;
    use crate::runtime::server::worker::sessions::read::ClientReadMode;

    for has_eof in [false, true] {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let (ordinary, _peer) = add_runtime_connection(&mut backend);
        backend.ctx.config_mut().server.debug_pause_after_fork = true;
        backend.full_sync.diskless_relay = Some(DisklessRelay {
            transfer_id: crate::runtime::server::worker::reactor::sources::TransferId(0),
            source: None,
            child_release_write_end: None,
            targets: Vec::new(),
            marker: Vec::new(),
            complete_full_resync: false,
            not_before_ms: 0,
            eof_reached: false,
            marker_queued_targets: std::collections::HashSet::new(),
            marker_sent_targets: std::collections::HashSet::new(),
            completion_announced: false,
            completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
        });
        backend.transport.clear_interest_requests_for_test();

        backend.process_client_read_outcome(
            Token(ordinary),
            ClientReadMode::Fair,
            protocol::encode_command(&[b"PING"]),
            if has_eof {
                ReadOutcome::PeerClosed
            } else {
                ReadOutcome::Quiescent
            },
        );

        assert!(backend.connection_registry.contains(Token(ordinary)));
        assert!(backend
            .full_sync
            .paused_dual_clients
            .contains(&Token(ordinary)));
        assert!(backend.connection_registry[Token(ordinary)]
            .parser
            .has_pending_data());
        assert_eq!(
            backend.transport.last_set_interest_calls_for_test().last(),
            Some(&(Token(ordinary), Interest::None))
        );

        backend
            .drop_diskless_relay_target(crate::framework::distributed::replication::ReplicaId(1));

        assert!(backend.full_sync.paused_dual_clients.is_empty());
        assert_eq!(
            backend.transport.last_set_interest_calls_for_test().last(),
            Some(&(Token(ordinary), Interest::Readable))
        );
        assert!(backend.event_batch.contains_runnable(
            crate::runtime::server::worker::reactor::RunnableKey::ClientRead(Token(ordinary))
        ));
    }
}

#[test]
fn classic_diskless_ack_wait_does_not_poll_or_reread_completed_pipe() {
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;
    use std::fs::File;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 53;
    add_connection(&mut backend, token_value);
    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(token_value),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["eof".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let link = backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("primary link must exist");
    link.set_state(PrimaryLinkState::PreparingFullResync);
    link.finish_full_resync();
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id: crate::runtime::server::worker::reactor::sources::TransferId(0),
        source: None,
        child_release_write_end: Some(File::open("/dev/null").unwrap()),
        targets: vec![replica_id.into()],
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: true,
        marker_queued_targets: std::collections::HashSet::new(),
        marker_sent_targets: [replica_id.into()].into_iter().collect(),
        completion_announced: false,
        completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
    });

    backend
        .on_diskless_pipe_readable(crate::runtime::server::worker::reactor::sources::TransferId(0));

    assert!(
        backend.full_sync.diskless_relay.is_some(),
        "waiting for the terminal ACK must ignore stale pipe readiness"
    );
}

#[test]
fn diskless_completion_facts_publish_only_after_all_success_facts() {
    use crate::runtime::server::worker::distributed::replication::diskless_child::{
        DisklessCompletionState, DisklessFact,
    };
    use crate::runtime::server::worker::reactor::sources::ChildExitOutcome;

    let success_facts = [
        DisklessFact::PipeEof,
        DisklessFact::ChildExited(ChildExitOutcome::Exited(0)),
        DisklessFact::MarkersFlushed,
        DisklessFact::TerminalAckSatisfied,
    ];
    for order in permutations(success_facts.clone()) {
        let mut state = DisklessCompletionState::default();
        for (index, fact) in order.iter().cloned().enumerate() {
            let published = state.apply_fact(fact);
            assert_eq!(
                published,
                index == 3,
                "publication must happen exactly after the fourth success fact: {order:?}"
            );
        }
        assert!(!state.apply_fact(DisklessFact::TerminalAckSatisfied));
    }

    for order in permutations(success_facts) {
        let mut state = DisklessCompletionState::default();
        for fact in order.iter().cloned() {
            let fact = match fact {
                DisklessFact::ChildExited(_) => {
                    DisklessFact::ChildExited(ChildExitOutcome::Exited(1))
                }
                other => other,
            };
            assert!(!state.apply_fact(fact));
        }
        assert!(!state.published());
    }
}

#[test]
fn early_diskless_child_success_waits_for_pipe_and_target_facts() {
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;
    use crate::runtime::server::worker::reactor::sources::{ChildExitOutcome, ChildOwner};

    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 54;
    add_connection(&mut backend, token_value);
    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(token_value),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let pid = 123_456;
    backend
        .ctx
        .persistence_mut()
        .begin_diskless_child(pid)
        .unwrap();
    let child = backend.install_test_child(ChildOwner::DisklessReplication, pid);
    backend
        .child_registry
        .record_exit_for_test(child, ChildExitOutcome::Exited(0));
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id: crate::runtime::server::worker::reactor::sources::TransferId(0),
        source: None,
        child_release_write_end: None,
        targets: vec![replica_id.into()],
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: false,
        marker_queued_targets: std::collections::HashSet::new(),
        marker_sent_targets: std::collections::HashSet::new(),
        completion_announced: false,
        completion_state:
            crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
    });

    backend.on_diskless_child_completed(child, ChildExitOutcome::Exited(0));

    let relay = backend
        .full_sync
        .diskless_relay
        .as_ref()
        .expect("early child success must not fail an incomplete relay");
    assert!(!relay.completion_state.published());
    assert!(
        backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token_value)),
        "early child success must not disconnect the target"
    );
}

fn permutations<const N: usize, T: Clone>(items: [T; N]) -> Vec<[T; N]> {
    fn permute<T: Clone>(items: &mut [T], index: usize, output: &mut Vec<Vec<T>>) {
        if index == items.len() {
            output.push(items.to_vec());
            return;
        }
        for swap_index in index..items.len() {
            items.swap(index, swap_index);
            permute(items, index + 1, output);
            items.swap(index, swap_index);
        }
    }

    let mut work = items;
    let mut output = Vec::new();
    permute(&mut work, 0, &mut output);
    output
        .into_iter()
        .map(|values| values.try_into().ok().expect("permutation length"))
        .collect()
}

#[test]
fn blocked_readiness_matrix_classic_rdb_readable_deferred_continuation() {
    let (mut backend, token, mut primary_stream) = classic_blocked_rdb_receiver();
    primary_stream
        .write_all(&minimal_classic_rdb_fullresync_frame())
        .expect("write complete classic RDB frame");
    backend
        .transport
        .wait_pending_bytes_for_test(token, minimal_classic_rdb_fullresync_frame().len());
    backend
        .transport
        .queue_test_transport_event_batch(&[(token, true, false, false, false)]);

    let result = backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(token.0),
    });

    assert!(result.deferred_events >= 1);
    assert!(backend.ctx.db(0).unwrap().get_no_expire(b"k").is_none());
    assert!(backend.transport.is_registered(token));
    assert!(backend.replication_runtime.replica_link().is_some());
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());

    backend.replay_one_deferred_readiness();

    assert!(backend.blocked_events.pop_front_deferred().is_none());
    assert_eq!(
        backend
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"k")
            .map(|value| value.as_bytes()),
        Some(&b"v"[..])
    );
    assert!(backend.replication_runtime.replica_link().is_some());
    assert!(backend.transport.is_registered(token));
    assert!(!backend.ctx.is_loading());
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    assert_eq!(backend.ctx.replication().slave_repl_offset, offset(0));
}

#[test]
fn blocked_readiness_matrix_classic_rdb_readable_hup_drains_tail_then_tears_down() {
    let (mut backend, token, mut primary_stream) = classic_blocked_rdb_receiver();
    primary_stream
        .write_all(&minimal_classic_rdb_fullresync_frame())
        .expect("write complete classic RDB frame");
    backend
        .transport
        .wait_pending_bytes_for_test(token, minimal_classic_rdb_fullresync_frame().len());
    primary_stream
        .shutdown(std::net::Shutdown::Write)
        .expect("half-close classic RDB peer");
    backend
        .transport
        .queue_test_transport_event_batch(&[(token, true, false, true, false)]);

    let result = backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(token.0),
    });
    assert!(result.deferred_events >= 1);
    backend.replay_one_deferred_readiness();

    assert_eq!(
        backend
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"k")
            .map(|value| value.as_bytes()),
        Some(&b"v"[..])
    );
    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(!backend.transport.is_registered(token));
    assert!(!backend.ctx.is_loading());
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    assert_eq!(backend.ctx.replication().slave_repl_offset, offset(0));
}

#[test]
fn blocked_readiness_matrix_classic_rdb_hup_only_tears_down_after_restore() {
    let (mut backend, token, _primary_stream) = classic_blocked_rdb_receiver();
    backend
        .transport
        .queue_test_transport_event_batch(&[(token, false, false, true, false)]);

    let result = backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(token.0),
    });

    assert_eq!(result.dispatched_events, 1);
    assert_eq!(result.deferred_events, 0);
    assert!(backend.blocked_events.pop_front_deferred().is_none());
    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(!backend.transport.is_registered(token));
    assert!(!backend.ctx.is_loading());
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    assert_eq!(backend.ctx.replication().slave_repl_offset, offset(0));
}

#[test]
fn blocked_readiness_matrix_stale_deferred_token_after_removal_is_ignored() {
    let (mut backend, token, _primary_stream) = classic_blocked_rdb_receiver();
    backend
        .transport
        .queue_test_transport_event_batch(&[(token, true, false, false, false)]);
    let result = backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(token.0),
    });
    assert!(result.deferred_events >= 1);
    assert!(backend.replication_runtime.replica_link().is_some());
    backend.remove_connection(token.0);

    backend.replay_one_deferred_readiness();

    assert!(backend.blocked_events.pop_front_deferred().is_none());
    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(!backend.transport.is_registered(token));
    assert!(!backend.ctx.is_loading());
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    assert_eq!(backend.ctx.replication().slave_repl_offset, offset(0));
}

#[test]
fn replicated_function_load_without_replace_preserves_local_library_until_rdb_commit() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.replication_mut().switch_to_replica();
    backend.ctx.config_mut().replication.replica_read_only = false;
    let connection_token = add_connection(&mut backend, 41);
    let local_code =
        "#!lua name=test\nserver.register_function('test', function() return 'hello1' end)";
    let upstream_code =
        "#!lua name=test\nserver.register_function('test', function() return 'hello2' end)";
    backend
        .ctx
        .script_mut()
        .function_registry
        .load_library("test", local_code)
        .expect("load local function library");
    let mut link = ReplicaLink::streaming(
        ConnectionId(41),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        backend.ctx.replication().backlog.replid().to_string(),
        offset(0),
    );
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"FUNCTION"),
        Bytes::from_static(b"LOAD"),
        Bytes::copy_from_slice(upstream_code.as_bytes()),
    ]);

    backend
        .apply_replication_command(
            connection_token,
            &mut link,
            ReplicationApply::Command(args),
            CommandOrigin::Replication,
        )
        .expect("replicated command processing must keep the stream alive");

    let code = &backend
        .ctx
        .script()
        .function_registry
        .get_library("test")
        .expect("local library must remain installed")
        .code;
    assert!(
        code.contains("hello1"),
        "replicated FUNCTION LOAD without REPLACE must not expose the incoming library before a full-sync RDB commit"
    );
}

#[test]
fn replica_output_hard_limit_closes_dual_main_waiting_for_psync() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 43;
    let connection_token = add_connection(&mut backend, token_value);
    backend.ctx.config_mut().server.repl_backlog_size = 1;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = 1_024;

    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(token_value),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["dual-channel".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let link = backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("primary link must exist");
    link.transition_to_waiting_for_rdb_channel().unwrap();
    link.transition_to_waiting_for_main_psync().unwrap();
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .queue_reply(vec![b'x'; 1_025]);

    backend.enforce_replica_output_buffer_limits();

    assert!(
        !backend.connection_registry.contains(crate::framework::capabilities::io::Token(token_value)),
        "a dual main waiting for PSYNC must be closed once its output buffer exceeds the hard limit"
    );
    assert!(
        backend
            .replication_runtime
            .primary_link(replica_id)
            .is_none(),
        "the closed output-buffer-limited main link must leave the replication runtime"
    );
    assert_eq!(
        backend
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        1,
        "the INFO stats counter must record the primary-side replica COB close"
    );
}

#[test]
fn replica_output_hard_limit_aborts_associated_retained_wait_and_closes_main() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = ConnectionId(143);
    let rdb = ConnectionId(144);
    let public_client_id = 30_144;
    add_connection(&mut backend, main.0);
    backend.ctx.config_mut().server.repl_backlog_size = 1;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = 1_024;
    let replica_id = backend.replication_runtime.register_primary(
        main,
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["dual-channel".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .transition_to_waiting_for_rdb_channel()
        .unwrap();
    backend
        .replication_runtime
        .register_rdb_candidate(rdb)
        .unwrap();
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    backend
        .replication_runtime
        .insert_unpaired_rdb_psync_wait_for_test(rdb, boundary);
    backend
        .replication_runtime
        .backfill_waiting_psync_pins(boundary.stream_cursor());
    assert!(backend
        .replication_runtime
        .retain_completed_unpaired_rdb_wait(rdb, public_client_id, i64::MAX,));
    backend.replication_runtime.remove_connection(rdb);
    assert!(backend
        .replication_runtime
        .try_associate_retained_rdb_wait(main, public_client_id, 0)
        .is_ok());
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(&vec![b'x'; 1_025])
        .unwrap();

    backend.enforce_replica_output_buffer_limits();

    assert!(!backend.connection_registry.contains(Token(main.0)));
    assert!(backend
        .replication_runtime
        .primary_link(replica_id)
        .is_none());
    assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 0);
    assert_eq!(
        backend.replication_runtime.min_protected_backlog_cursor(),
        None
    );
    assert_eq!(
        backend
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        1
    );
}

fn paired_output_limit_runtime(
    main: ConnectionId,
    rdb: ConnectionId,
) -> (
    ServerWorkerState,
    crate::framework::distributed::replication::ReplicaId,
) {
    let mut backend = ServerWorkerState::new(1).unwrap();
    add_connection(&mut backend, main.0);
    add_connection(&mut backend, rdb.0);
    backend.ctx.config_mut().server.repl_backlog_size = 1;
    backend
        .ctx
        .config_mut()
        .server
        .debug_delay_rdb_client_free_seconds = 10;
    let replica_id = backend.replication_runtime.register_primary(
        main,
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["dual-channel".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .transition_to_waiting_for_rdb_channel()
        .unwrap();
    backend
        .replication_runtime
        .register_rdb_candidate(rdb)
        .unwrap();
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    backend
        .replication_runtime
        .insert_unpaired_rdb_psync_wait_for_test(rdb, boundary);
    backend
        .replication_runtime
        .backfill_waiting_psync_pins(boundary.stream_cursor());
    backend
        .replication_runtime
        .associate_rdb_channel(main, rdb)
        .unwrap();
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .transition_to_waiting_for_main_psync()
        .unwrap();
    assert_eq!(
        backend.replication_runtime.psync_wait_pin_for_main(main),
        Some(boundary.stream_cursor())
    );
    (backend, replica_id)
}

fn configure_replica_soft_limit(backend: &mut ServerWorkerState, limit: usize) {
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_soft_limit = limit;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_soft_limit_seconds = 0;
}

#[test]
fn paired_replica_main_only_soft_limit_reaches_timeout() {
    let main = ConnectionId(9_210);
    let rdb = ConnectionId(9_211);
    let (mut backend, _) = paired_output_limit_runtime(main, rdb);
    configure_replica_soft_limit(&mut backend, 4);
    backend.connection_registry[Token(main.0)].queue_reply(b"over".to_vec());

    backend.enforce_replica_output_buffer_limits();
    std::thread::sleep(std::time::Duration::from_millis(2));
    backend.enforce_replica_output_buffer_limits();

    assert!(!backend.connection_registry.contains(Token(main.0)));
    assert!(!backend.connection_registry.contains(Token(rdb.0)));
    assert_eq!(
        backend.replication_runtime.psync_wait_owner_count(),
        0,
        "configured soft-limit teardown must not retain a grace owner"
    );
    assert_eq!(
        backend.replication_runtime.psync_wait_pin_for_main(main),
        None
    );
    assert_eq!(
        backend.replication_runtime.min_protected_backlog_cursor(),
        None
    );
    assert_eq!(
        backend
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        1
    );
}

#[test]
fn paired_replica_rdb_only_soft_limit_reaches_timeout() {
    let main = ConnectionId(9_212);
    let rdb = ConnectionId(9_213);
    let (mut backend, _) = paired_output_limit_runtime(main, rdb);
    configure_replica_soft_limit(&mut backend, 4);
    backend.connection_registry[Token(rdb.0)].queue_reply(b"over".to_vec());

    backend.enforce_replica_output_buffer_limits();
    std::thread::sleep(std::time::Duration::from_millis(2));
    backend.enforce_replica_output_buffer_limits();

    assert!(!backend.connection_registry.contains(Token(main.0)));
    assert!(!backend.connection_registry.contains(Token(rdb.0)));
    assert_eq!(
        backend.replication_runtime.psync_wait_owner_count(),
        0,
        "configured soft-limit teardown must not retain a grace owner"
    );
    assert_eq!(
        backend.replication_runtime.psync_wait_pin_for_main(main),
        None
    );
    assert_eq!(
        backend.replication_runtime.min_protected_backlog_cursor(),
        None
    );
    assert_eq!(
        backend
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        1
    );
}

#[test]
fn paired_replica_soft_limit_resets_only_when_both_endpoints_drop_below_limit() {
    let main = ConnectionId(9_214);
    let rdb = ConnectionId(9_215);
    let (mut backend, _) = paired_output_limit_runtime(main, rdb);
    configure_replica_soft_limit(&mut backend, 4);
    backend.connection_registry[Token(main.0)].queue_reply(b"over".to_vec());
    backend.enforce_replica_output_buffer_limits();

    backend.connection_registry[Token(main.0)].clear_output_queue();
    backend.enforce_replica_output_buffer_limits();
    std::thread::sleep(std::time::Duration::from_millis(2));
    backend.connection_registry[Token(rdb.0)].queue_reply(b"over".to_vec());
    backend.enforce_replica_output_buffer_limits();

    assert!(backend.connection_registry.contains(Token(main.0)));
    assert!(backend.connection_registry.contains(Token(rdb.0)));
    assert_eq!(
        backend
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        0,
        "the first over-limit pass after a reset must start a new interval"
    );

    std::thread::sleep(std::time::Duration::from_millis(2));
    backend.enforce_replica_output_buffer_limits();

    assert!(!backend.connection_registry.contains(Token(main.0)));
    assert!(!backend.connection_registry.contains(Token(rdb.0)));
    assert_eq!(
        backend
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        1
    );
}

#[test]
fn paired_replica_both_hard_over_limit_records_one_owner_violation() {
    let main = ConnectionId(9_216);
    let rdb = ConnectionId(9_217);
    let (mut backend, replica_id) = paired_output_limit_runtime(main, rdb);
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = 4;
    backend.connection_registry[Token(main.0)].queue_reply(b"over".to_vec());
    backend.connection_registry[Token(rdb.0)].queue_reply(b"over".to_vec());

    backend.enforce_replica_output_buffer_limits();

    assert!(!backend.connection_registry.contains(Token(main.0)));
    assert!(!backend.connection_registry.contains(Token(rdb.0)));
    assert!(backend
        .replication_runtime
        .primary_link(replica_id)
        .is_none());
    assert_eq!(backend.replication_runtime.channel_pair_for(main), None);
    assert_eq!(backend.replication_runtime.channel_pair_for(rdb), None);
    assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 0);
    assert_eq!(
        backend.replication_runtime.min_protected_backlog_cursor(),
        None
    );
    assert_eq!(
        backend
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        1,
        "one owner violation must emit one log and increment the stat once"
    );
}

#[test]
fn paired_replica_rdb_only_hard_limit_aborts_main_wait_without_grace() {
    let main = ConnectionId(9_218);
    let rdb = ConnectionId(9_219);
    let (mut backend, replica_id) = paired_output_limit_runtime(main, rdb);
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = 4;
    backend.connection_registry[Token(rdb.0)].queue_reply(b"over".to_vec());

    backend.enforce_replica_output_buffer_limits();

    assert!(!backend.connection_registry.contains(Token(main.0)));
    assert!(!backend.connection_registry.contains(Token(rdb.0)));
    assert!(backend
        .replication_runtime
        .primary_link(replica_id)
        .is_none());
    assert_eq!(backend.replication_runtime.channel_pair_for(main), None);
    assert_eq!(backend.replication_runtime.channel_pair_for(rdb), None);
    assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 0);
    assert_eq!(
        backend.replication_runtime.min_protected_backlog_cursor(),
        None,
        "a definitive hard-limit failure must not retain a grace pin"
    );
    assert_eq!(
        backend
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        1
    );
}

#[test]
fn replica_output_hard_limit_uses_backlog_floor_for_dual_main() {
    const CONFIGURED_LIMIT: usize = 10 * 1024;
    const BACKLOG_SIZE: usize = 1024 * 1024;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = ConnectionId(9_190);
    add_connection(&mut backend, main.0);
    backend.ctx.config_mut().server.repl_backlog_size = BACKLOG_SIZE as i64;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = CONFIGURED_LIMIT;
    let replica_id = backend.replication_runtime.register_primary(
        main,
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["dual-channel".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let link = backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("primary link must exist");
    link.transition_to_waiting_for_rdb_channel().unwrap();
    link.transition_to_waiting_for_main_psync().unwrap();
    backend.connection_registry[Token(main.0)].queue_reply(vec![b'x'; CONFIGURED_LIMIT]);

    backend.enforce_replica_output_buffer_limits();

    assert!(
        backend.connection_registry.contains(Token(main.0)),
        "the configured limit below repl-backlog-size must be ignored"
    );

    backend.connection_registry[Token(main.0)]
        .queue_reply(vec![b'x'; BACKLOG_SIZE - CONFIGURED_LIMIT]);
    backend.enforce_replica_output_buffer_limits();

    assert!(
        !backend.connection_registry.contains(Token(main.0)),
        "the main owner must close at the effective backlog-sized limit"
    );
}

#[test]
fn replica_output_hard_limit_uses_backlog_floor_for_unassociated_retained_wait() {
    const CONFIGURED_LIMIT: usize = 10 * 1024;
    const BACKLOG_SIZE: usize = 1024 * 1024;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = ConnectionId(9_191);
    let retained_rdb = ConnectionId(9_192);
    let public_client_id = 90_192;
    backend.ctx.config_mut().server.repl_backlog_size = BACKLOG_SIZE as i64;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = CONFIGURED_LIMIT;
    let replica_id = backend.replication_runtime.register_primary(
        main,
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["dual-channel".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .transition_to_waiting_for_rdb_channel()
        .unwrap();
    backend
        .replication_runtime
        .register_rdb_candidate(retained_rdb)
        .unwrap();
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    backend
        .replication_runtime
        .insert_unpaired_rdb_psync_wait_for_test(retained_rdb, boundary);
    backend
        .replication_runtime
        .backfill_waiting_psync_pins(boundary.stream_cursor());
    assert!(backend
        .replication_runtime
        .retain_completed_unpaired_rdb_wait(retained_rdb, public_client_id, i64::MAX));
    backend.replication_runtime.remove_connection(retained_rdb);
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(&vec![b'x'; CONFIGURED_LIMIT])
        .unwrap();

    backend.enforce_replica_output_buffer_limits();

    assert_eq!(
        backend.replication_runtime.psync_wait_owner_count(),
        1,
        "the configured limit below repl-backlog-size must be ignored"
    );

    backend
        .ctx
        .replication_mut()
        .backlog
        .append(&vec![b'x'; BACKLOG_SIZE - CONFIGURED_LIMIT])
        .unwrap();
    backend.enforce_replica_output_buffer_limits();

    assert_eq!(
        backend.replication_runtime.psync_wait_owner_count(),
        0,
        "the retained owner must close at the effective backlog-sized limit"
    );
}

#[test]
fn replica_output_soft_limit_uses_backlog_floor_for_unassociated_retained_wait() {
    const CONFIGURED_LIMIT: usize = 10 * 1024;
    const BACKLOG_SIZE: usize = 1024 * 1024;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = ConnectionId(9_193);
    let retained_rdb = ConnectionId(9_194);
    let public_client_id = 90_194;
    backend.ctx.config_mut().server.repl_backlog_size = BACKLOG_SIZE as i64;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_soft_limit = CONFIGURED_LIMIT;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_soft_limit_seconds = 0;
    let replica_id = backend.replication_runtime.register_primary(
        main,
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["dual-channel".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .transition_to_waiting_for_rdb_channel()
        .unwrap();
    backend
        .replication_runtime
        .register_rdb_candidate(retained_rdb)
        .unwrap();
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    backend
        .replication_runtime
        .insert_unpaired_rdb_psync_wait_for_test(retained_rdb, boundary);
    backend
        .replication_runtime
        .backfill_waiting_psync_pins(boundary.stream_cursor());
    assert!(backend
        .replication_runtime
        .retain_completed_unpaired_rdb_wait(retained_rdb, public_client_id, i64::MAX));
    backend.replication_runtime.remove_connection(retained_rdb);
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(&vec![b'x'; CONFIGURED_LIMIT])
        .unwrap();

    backend.enforce_replica_output_buffer_limits();
    std::thread::sleep(std::time::Duration::from_millis(2));
    backend.enforce_replica_output_buffer_limits();

    assert_eq!(
        backend.replication_runtime.psync_wait_owner_count(),
        1,
        "the configured soft limit below repl-backlog-size must be ignored"
    );

    backend
        .ctx
        .replication_mut()
        .backlog
        .append(&vec![b'x'; BACKLOG_SIZE - CONFIGURED_LIMIT])
        .unwrap();
    backend.enforce_replica_output_buffer_limits();
    std::thread::sleep(std::time::Duration::from_millis(2));
    backend.enforce_replica_output_buffer_limits();

    assert_eq!(
        backend.replication_runtime.psync_wait_owner_count(),
        0,
        "the retained owner must close after exceeding the effective soft limit duration"
    );
}

#[test]
fn retained_soft_limit_elapsed_time_continues_after_main_association() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = ConnectionId(9_195);
    let retained_rdb = ConnectionId(9_196);
    let public_client_id = 90_196;
    add_connection(&mut backend, main.0);
    backend.ctx.config_mut().server.repl_backlog_size = 1;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_soft_limit = 4;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_soft_limit_seconds = 1;
    let replica_id = backend.replication_runtime.register_primary(
        main,
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["dual-channel".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .transition_to_waiting_for_rdb_channel()
        .unwrap();
    backend
        .replication_runtime
        .register_rdb_candidate(retained_rdb)
        .unwrap();
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    backend
        .replication_runtime
        .insert_unpaired_rdb_psync_wait_for_test(retained_rdb, boundary);
    backend
        .replication_runtime
        .backfill_waiting_psync_pins(boundary.stream_cursor());
    assert!(backend
        .replication_runtime
        .retain_completed_unpaired_rdb_wait(retained_rdb, public_client_id, i64::MAX));
    backend.replication_runtime.remove_connection(retained_rdb);
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"soft")
        .unwrap();
    backend.enforce_replica_output_buffer_limits();

    std::thread::sleep(std::time::Duration::from_millis(900));
    backend
        .replication_runtime
        .try_associate_retained_rdb_wait(
            main,
            public_client_id,
            crate::foundation::io::sys::time::mstime(),
        )
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(150));
    backend.enforce_replica_output_buffer_limits();

    assert!(
        !backend.connection_registry.contains(Token(main.0)),
        "association must not restart an already-running soft-limit interval"
    );
}

#[test]
fn retained_association_preserves_existing_main_soft_limit_deadline() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = ConnectionId(9_220);
    let retained_rdb = ConnectionId(9_221);
    let public_client_id = 90_221;
    add_connection(&mut backend, main.0);
    backend.ctx.config_mut().server.repl_backlog_size = 1;
    backend
        .ctx
        .config_mut()
        .server
        .debug_delay_rdb_client_free_seconds = 10;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_soft_limit = 4;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_soft_limit_seconds = 1;
    let replica_id = backend.replication_runtime.register_primary(
        main,
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["dual-channel".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .transition_to_waiting_for_rdb_channel()
        .unwrap();
    backend
        .replication_runtime
        .register_rdb_candidate(retained_rdb)
        .unwrap();
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    backend
        .replication_runtime
        .insert_unpaired_rdb_psync_wait_for_test(retained_rdb, boundary);
    backend
        .replication_runtime
        .backfill_waiting_psync_pins(boundary.stream_cursor());
    assert!(backend
        .replication_runtime
        .retain_completed_unpaired_rdb_wait(retained_rdb, public_client_id, i64::MAX));
    backend.replication_runtime.remove_connection(retained_rdb);
    backend.connection_registry[Token(main.0)].queue_reply(b"soft".to_vec());
    backend.enforce_replica_output_buffer_limits();

    std::thread::sleep(std::time::Duration::from_millis(900));
    backend
        .replication_runtime
        .try_associate_retained_rdb_wait(
            main,
            public_client_id,
            crate::foundation::io::sys::time::mstime(),
        )
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(150));
    backend.enforce_replica_output_buffer_limits();

    assert!(
        !backend.connection_registry.contains(Token(main.0)),
        "retained association must preserve the main channel's original deadline"
    );
    assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 0);
    assert_eq!(
        backend.replication_runtime.psync_wait_pin_for_main(main),
        None
    );
    assert_eq!(
        backend.replication_runtime.min_protected_backlog_cursor(),
        None
    );
}

#[test]
fn retained_soft_limit_reset_before_association_starts_a_new_main_interval() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = ConnectionId(9_197);
    let retained_rdb = ConnectionId(9_198);
    let public_client_id = 90_198;
    add_connection(&mut backend, main.0);
    backend.ctx.config_mut().server.repl_backlog_size = 1;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_soft_limit = 4;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_soft_limit_seconds = 1;
    let replica_id = backend.replication_runtime.register_primary(
        main,
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["dual-channel".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .transition_to_waiting_for_rdb_channel()
        .unwrap();
    backend
        .replication_runtime
        .register_rdb_candidate(retained_rdb)
        .unwrap();
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    backend
        .replication_runtime
        .insert_unpaired_rdb_psync_wait_for_test(retained_rdb, boundary);
    backend
        .replication_runtime
        .backfill_waiting_psync_pins(boundary.stream_cursor());
    assert!(backend
        .replication_runtime
        .retain_completed_unpaired_rdb_wait(retained_rdb, public_client_id, i64::MAX));
    backend.replication_runtime.remove_connection(retained_rdb);
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"soft")
        .unwrap();
    backend.enforce_replica_output_buffer_limits();

    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_soft_limit = 8;
    backend.enforce_replica_output_buffer_limits();
    std::thread::sleep(std::time::Duration::from_millis(1_050));
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_soft_limit = 4;
    backend
        .replication_runtime
        .try_associate_retained_rdb_wait(
            main,
            public_client_id,
            crate::foundation::io::sys::time::mstime(),
        )
        .unwrap();
    backend.enforce_replica_output_buffer_limits();

    assert!(
        backend.connection_registry.contains(Token(main.0)),
        "dropping below the soft limit before association must clear the retained timer"
    );
    assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 1);
}

#[test]
fn replica_output_hard_limit_removes_unassociated_retained_wait() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = ConnectionId(9_201);
    let retained_rdb = ConnectionId(9_202);
    let next_rdb = ConnectionId(9_203);
    let public_client_id = 90_202;
    backend.ctx.config_mut().server.repl_backlog_size = 1;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = 4;
    let replica_id = backend.replication_runtime.register_primary(
        main,
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["dual-channel".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .transition_to_waiting_for_rdb_channel()
        .unwrap();
    backend
        .replication_runtime
        .register_rdb_candidate(retained_rdb)
        .unwrap();
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    backend
        .replication_runtime
        .insert_unpaired_rdb_psync_wait_for_test(retained_rdb, boundary);
    backend
        .replication_runtime
        .backfill_waiting_psync_pins(boundary.stream_cursor());
    assert!(backend
        .replication_runtime
        .retain_completed_unpaired_rdb_wait(retained_rdb, public_client_id, i64::MAX));
    backend.replication_runtime.remove_connection(retained_rdb);
    backend.refresh_replication_view();
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"over")
        .unwrap();

    backend.enforce_replica_output_buffer_limits();

    assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 0);
    assert_eq!(
        backend.replication_runtime.min_protected_backlog_cursor(),
        None
    );
    assert_eq!(
        backend.ctx.replication().backlog.min_protected_cursor(),
        None
    );
    assert_eq!(backend.ctx.replication().view.replicas_waiting_psync, 0);
    assert_eq!(
        backend
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        1
    );

    backend
        .replication_runtime
        .register_rdb_candidate(next_rdb)
        .unwrap();
    assert!(backend
        .replication_runtime
        .take_endoff_for_rdb_candidate_if_unambiguous(
            next_rdb,
            true,
            &mut backend.ctx.replication_mut().backlog,
        )
        .is_some());
}

#[test]
fn replica_output_cursor_error_removes_unassociated_retained_wait_without_limit_stat() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let retained_rdb = ConnectionId(9_204);
    let public_client_id = 90_204;
    backend
        .replication_runtime
        .register_rdb_candidate(retained_rdb)
        .unwrap();
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    backend
        .replication_runtime
        .insert_unpaired_rdb_psync_wait_for_test(retained_rdb, boundary);
    let invalid_pin = boundary
        .stream_cursor()
        .checked_advance(1)
        .expect("one byte beyond the backlog tail must be representable");
    backend
        .replication_runtime
        .backfill_waiting_psync_pins(invalid_pin);
    assert!(backend
        .replication_runtime
        .retain_completed_unpaired_rdb_wait(retained_rdb, public_client_id, i64::MAX));
    backend.replication_runtime.remove_connection(retained_rdb);

    backend.enforce_replica_output_buffer_limits();

    assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 0);
    assert_eq!(
        backend
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        0,
        "cursor invariant failures fail closed without being counted as configured-limit overruns"
    );
}

#[test]
fn replica_output_soft_limit_for_unassociated_retained_wait_resets_before_timeout() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let retained_rdb = ConnectionId(9_205);
    let public_client_id = 90_205;
    backend.ctx.config_mut().server.repl_backlog_size = 1;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_soft_limit = 4;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_soft_limit_seconds = 0;
    backend
        .replication_runtime
        .register_rdb_candidate(retained_rdb)
        .unwrap();
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    backend
        .replication_runtime
        .insert_unpaired_rdb_psync_wait_for_test(retained_rdb, boundary);
    backend
        .replication_runtime
        .backfill_waiting_psync_pins(boundary.stream_cursor());
    assert!(backend
        .replication_runtime
        .retain_completed_unpaired_rdb_wait(retained_rdb, public_client_id, i64::MAX));
    backend.replication_runtime.remove_connection(retained_rdb);
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"soft")
        .unwrap();

    backend.enforce_replica_output_buffer_limits();
    assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 1);

    std::thread::sleep(std::time::Duration::from_millis(2));
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_soft_limit = 8;
    backend.enforce_replica_output_buffer_limits();
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_soft_limit = 4;
    backend.enforce_replica_output_buffer_limits();
    assert_eq!(
        backend.replication_runtime.psync_wait_owner_count(),
        1,
        "dropping below the soft limit must reset the retained owner's duration window"
    );

    std::thread::sleep(std::time::Duration::from_millis(2));
    backend.enforce_replica_output_buffer_limits();

    assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 0);
    assert_eq!(
        backend
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        1
    );
}

#[test]
fn first_replication_frame_backfills_and_enforces_empty_retained_wait() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let retained_rdb = ConnectionId(9_206);
    let public_client_id = 90_206;
    backend.ctx.config_mut().server.repl_backlog_size = 1;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = 5;
    backend.ctx.replication_mut().backlog.activate();
    backend
        .replication_runtime
        .register_rdb_candidate(retained_rdb)
        .unwrap();
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    backend
        .replication_runtime
        .insert_unpaired_rdb_psync_wait_for_test(retained_rdb, boundary);
    assert!(backend
        .replication_runtime
        .retain_completed_unpaired_rdb_wait(retained_rdb, public_client_id, i64::MAX));
    backend.replication_runtime.remove_connection(retained_rdb);
    assert_eq!(
        backend
            .replication_runtime
            .unassociated_retained_rdb_owners()[0]
            .pin_cursor,
        None
    );

    backend.propagate_replication_frame(b"over");

    assert_eq!(
        backend
            .replication_runtime
            .unassociated_retained_rdb_owners()[0]
            .pin_cursor,
        Some(boundary.stream_cursor())
    );
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = 4;
    backend.enforce_replica_output_buffer_limits();

    assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 0);
}

#[test]
fn retained_output_limit_cleanup_is_owner_local_and_association_safe() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = ConnectionId(9_207);
    let first_rdb = ConnectionId(9_208);
    let second_rdb = ConnectionId(9_209);
    let first_public_id = 90_208;
    let second_public_id = 90_209;
    backend.ctx.config_mut().server.repl_backlog_size = 1;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = 4;
    let replica_id = backend.replication_runtime.register_primary(
        main,
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["dual-channel".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .transition_to_waiting_for_rdb_channel()
        .unwrap();

    backend
        .replication_runtime
        .register_rdb_candidate(first_rdb)
        .unwrap();
    let first_boundary = backend.ctx.replication().backlog.snapshot_boundary();
    backend
        .replication_runtime
        .insert_unpaired_rdb_psync_wait_for_test(first_rdb, first_boundary);
    backend
        .replication_runtime
        .backfill_waiting_psync_pins(first_boundary.stream_cursor());
    assert!(backend
        .replication_runtime
        .retain_completed_unpaired_rdb_wait(first_rdb, first_public_id, i64::MAX));
    backend.replication_runtime.remove_connection(first_rdb);
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"over")
        .unwrap();

    backend
        .replication_runtime
        .register_rdb_candidate(second_rdb)
        .unwrap();
    let second_boundary = backend.ctx.replication().backlog.snapshot_boundary();
    backend
        .replication_runtime
        .insert_unpaired_rdb_psync_wait_for_test(second_rdb, second_boundary);
    backend
        .replication_runtime
        .backfill_waiting_psync_pins(second_boundary.stream_cursor());
    assert!(backend
        .replication_runtime
        .retain_completed_unpaired_rdb_wait(second_rdb, second_public_id, i64::MAX));
    backend.replication_runtime.remove_connection(second_rdb);
    backend.ctx.replication_mut().backlog.append(b"x").unwrap();

    backend.enforce_replica_output_buffer_limits();

    let owners = backend
        .replication_runtime
        .unassociated_retained_rdb_owners();
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].public_client_id, second_public_id);
    assert_eq!(owners[0].pin_cursor, Some(second_boundary.stream_cursor()));
    assert_eq!(
        backend
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        1
    );

    let cleanup_snapshot = owners[0];
    backend
        .replication_runtime
        .try_associate_retained_rdb_wait(main, second_public_id, 0)
        .unwrap();
    let mut cleanup_predicate_called = false;
    assert!(!backend
        .replication_runtime
        .remove_unassociated_retained_rdb_owners_if(|owner, _| {
            cleanup_predicate_called = true;
            owner.public_client_id == cleanup_snapshot.public_client_id
        }));
    assert!(
        !cleanup_predicate_called,
        "cleanup must revalidate association before evaluating the stale owner"
    );
    assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 1);
    assert_eq!(
        backend.replication_runtime.psync_wait_pin_for_main(main),
        Some(second_boundary.stream_cursor())
    );
    assert!(backend
        .replication_runtime
        .unassociated_retained_rdb_owners()
        .is_empty());
}

#[test]
fn replica_output_hard_limit_closes_stalled_full_resync_from_backlog_gap() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 44;
    add_connection(&mut backend, token_value);
    backend.ctx.config_mut().server.repl_backlog_size = 1;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = 1_024;
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();

    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(token_value),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let link = backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("primary link must exist");
    link.set_state(PrimaryLinkState::PreparingFullResync);
    link.set_full_resync_boundary(boundary).unwrap();
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(&vec![b'x'; 1_025])
        .unwrap();

    backend.enforce_replica_output_buffer_limits();

    assert!(
        !backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token_value)),
        "a stalled full-resync replica must be closed based on its shared backlog bytes"
    );
    assert!(
        backend
            .replication_runtime
            .primary_link(replica_id)
            .is_none(),
        "closing the stalled full-resync replica must release its backlog pin"
    );
}

#[test]
fn replica_output_hard_limit_ignores_active_snapshot_queue() {
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 45;
    let rdb_token_value = 46;
    add_connection(&mut backend, token_value);
    add_connection(&mut backend, rdb_token_value);
    backend.ctx.config_mut().server.repl_backlog_size = 1;
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = 1_024;

    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(token_value),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["dual-channel".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("primary link must exist")
        .set_state(PrimaryLinkState::PreparingFullResync);
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("primary link must exist")
        .transition_to_waiting_for_rdb_channel()
        .unwrap();
    backend
        .associate_waiting_rdb_for_test(ConnectionId(token_value), ConnectionId(rdb_token_value));
    backend.connection_registry[crate::framework::capabilities::io::Token(rdb_token_value)]
        .queue_reply(vec![b'x'; 1_025]);
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id: crate::runtime::server::worker::reactor::sources::TransferId(0),
        source: None,
        child_release_write_end: None,
        targets: vec![replica_id.into()],
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: false,
        marker_queued_targets: std::collections::HashSet::new(),
        marker_sent_targets: std::collections::HashSet::new(),
        completion_announced: false,
        completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
    });

    backend.enforce_replica_output_buffer_limits();

    assert!(
        backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token_value)),
        "an active snapshot transfer must not be closed for its transient RDB queue"
    );
}

#[test]
#[cfg(target_os = "linux")]
fn active_diskless_relay_reaps_abnormally_exited_child() {
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;
    use std::process::Command;
    use std::thread::sleep;
    use std::time::Duration;

    let mut child = Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("spawn child");
    let pid = child.id() as i32;
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 49;
    let connection_token = add_connection(&mut backend, token_value);
    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(token_value),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("primary link must exist")
        .set_state(PrimaryLinkState::PreparingFullResync);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .queue_reply(vec![b'x'; 1_025]);
    backend
        .ctx
        .persistence_mut()
        .begin_diskless_child(pid)
        .unwrap();
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id: crate::runtime::server::worker::reactor::sources::TransferId(0),
        source: None,
        child_release_write_end: None,
        targets: vec![replica_id.into()],
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: false,
        marker_queued_targets: std::collections::HashSet::new(),
        marker_sent_targets: std::collections::HashSet::new(),
        completion_announced: false,
        completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
    });

    crate::foundation::io::sys::process::send_signal(
        pid,
        crate::foundation::io::sys::process::ProcessSignal::Kill,
    )
    .expect("kill test child");
    for _ in 0..100 {
        backend.collect_idle_diskless_child_exit();
        if !backend.ctx.persistence().has_active_child() {
            break;
        }
        sleep(Duration::from_millis(10));
    }

    assert!(
        !backend.ctx.persistence().has_active_child(),
        "a signalled diskless child must be reaped even while its relay is active"
    );
    assert!(
        backend.full_sync.diskless_relay.is_none(),
        "an abnormal child exit must drop the active relay"
    );
    assert!(
        !backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token_value)),
        "replica targets of the failed relay must be closed"
    );
    let _ = child.wait();
}

#[test]
fn replicated_spublish_delivers_to_replica_shard_subscribers() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let replication_key = add_connection(&mut backend, 41);
    let subscriber_key = add_connection(&mut backend, 42);
    backend
        .ctx
        .pubsub_mut()
        .ssubscribe(42, Bytes::from_static(b"channel"));
    let mut link = ReplicaLink::new(
        ConnectionId(41),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SPUBLISH"),
        Bytes::from_static(b"channel"),
        Bytes::from_static(b"message"),
    ]);
    backend
        .apply_replication_command(
            replication_key,
            &mut link,
            ReplicationApply::Command(args),
            CommandOrigin::Replication,
        )
        .unwrap();
    let queued: Vec<u8> = backend.connection_registry
        [crate::framework::capabilities::io::Token(subscriber_key)]
    .output
    .queue
    .iter()
    .flat_map(|chunk| chunk.iter().copied())
    .collect();
    assert_eq!(
        queued,
        crate::engine::distributed::pubsub::encode_smessage(b"channel", b"message", false)
    );
}

#[test]
fn replicated_write_records_slot_input_without_client_output() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().server.cluster_enabled = true;
    backend.ctx.config_mut().server.cluster_slot_stats_enabled = true;
    let replication_key = add_connection(&mut backend, 43);
    let mut link = ReplicaLink::new(
        ConnectionId(43),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"key"),
        Bytes::from_static(b"value"),
    ]);
    let frame =
        crate::runtime::server::worker::distributed::propagation::encode_command_args(&args);

    backend
        .apply_replication_command(
            replication_key,
            &mut link,
            ReplicationApply::Command(args),
            CommandOrigin::Replication,
        )
        .unwrap();

    let slot = crate::engine::distributed::cluster::node::key_hash_slot(b"key") as usize;
    let counters = backend.ctx.slot_metrics().counters(slot).unwrap();
    assert!(counters.cpu_usec > 0);
    assert_eq!(counters.network_bytes_in, frame.len() as u64);
    assert_eq!(counters.network_bytes_out, 0);
}

#[test]
fn replicated_xadd_wakes_readonly_replica_xread() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let replication_key = add_connection(&mut backend, 41);
    let reader_key = add_connection(&mut backend, 42);
    backend.connection_registry[crate::framework::capabilities::io::Token(reader_key)].readonly =
        true;
    backend.connection_registry[crate::framework::capabilities::io::Token(reader_key)]
        .set_custom_reply_enabled(false);
    backend.connection_registry[crate::framework::capabilities::io::Token(reader_key)]
        .begin_nested_command_effects();
    let read_args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"XREAD"),
        Bytes::from_static(b"BLOCK"),
        Bytes::from_static(b"0"),
        Bytes::from_static(b"STREAMS"),
        Bytes::from_static(b"k"),
        Bytes::from_static(b"0"),
    ]);
    let reply = crate::framework::commands::stream::xread::XREAD_SPEC.execute(
        &mut backend.connection_registry[crate::framework::capabilities::io::Token(reader_key)],
        &read_args,
        &mut backend.ctx,
    );
    assert_eq!(reply, crate::foundation::io::protocol::Value::Null);
    let reader_session = backend.connection_registry
        [crate::framework::capabilities::io::Token(reader_key)]
    .session_id();
    backend.ctx.blocking_registry_mut().register_client(
        0,
        &[Bytes::from_static(b"k")],
        reader_session,
    );

    let mut link = ReplicaLink::new(
        ConnectionId(41),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let xadd = CommandArgs::from_parts(vec![
        Bytes::from_static(b"XADD"),
        Bytes::from_static(b"k"),
        Bytes::from_static(b"1-0"),
        Bytes::from_static(b"foo"),
        Bytes::from_static(b"bar"),
    ]);
    backend
        .apply_replication_command(
            replication_key,
            &mut link,
            ReplicationApply::Command(xadd),
            CommandOrigin::Replication,
        )
        .unwrap();

    assert!(
        backend.connection_registry[crate::framework::capabilities::io::Token(reader_key)]
            .blocked
            .is_none()
    );
    assert_eq!(
        _queued(
            &backend.connection_registry[crate::framework::capabilities::io::Token(reader_key)]
        ),
        b"*1\r\n*2\r\n$1\r\nk\r\n*1\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    );
}

#[test]
fn replica_incr_preserves_an_expired_key_for_debug_inspection() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_connection(&mut backend, 41);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .selected_db = 0;
    let db = backend.ctx.db_mut(0).expect("database 0 exists");
    db.set(
        b"k".to_vec(),
        crate::foundation::io::store::Robj::string(b"1".as_slice()),
    );
    db.set_expire(
        b"k",
        crate::foundation::io::store::kv_engine::now_ms().saturating_sub(1),
    );
    let mut link = ReplicaLink::new(
        ConnectionId(41),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let args = CommandArgs::from_parts(vec![Bytes::from_static(b"INCR"), Bytes::from_static(b"k")]);
    backend
        .apply_replication_command(
            connection_token,
            &mut link,
            ReplicationApply::Command(args),
            CommandOrigin::Replication,
        )
        .expect("replica INCR should apply");
    assert!(backend.ctx.db(0).unwrap().get_allow_expired(b"k").is_some());
    assert!(backend.ctx.db(0).unwrap().get_expire(b"k").is_some());
}

#[test]
fn expired_replication_backlog_cannot_partially_resync() {
    for restored_offset in [None, Some(offset(41))] {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().replication.repl_backlog_ttl = 1;
        if let Some(restored_offset) = restored_offset {
            backend
                .ctx
                .replication_mut()
                .backlog
                .restore_primary_from_rdb("restored-replid", restored_offset)
                .expect("restored offset must define a live history boundary");
        }
        backend
            .ctx
            .replication_mut()
            .backlog
            .append(b"history")
            .unwrap();
        backend.ctx.replication_mut().backlog.shift_replication_id();
        let current_replid = backend.ctx.replication().backlog.replid().to_string();
        let second_replid = backend
            .ctx
            .replication()
            .backlog
            .second_replid()
            .to_string();
        let request = backend
            .ctx
            .replication()
            .backlog
            .second_replid_request_boundary()
            .expect("shifted history has a second replid boundary");
        for replid in [&current_replid, &second_replid] {
            assert!(backend
                .ctx
                .replication()
                .backlog
                .resolve_continue(replid, request)
                .is_some());
        }

        backend.ctx.replication_mut().backlog_idle_deadline_ms =
            crate::foundation::io::sys::time::mstime().saturating_sub(1);
        backend.expire_idle_replication_backlog();

        let expired_replid = backend.ctx.replication().backlog.replid().to_string();
        assert_ne!(expired_replid, current_replid);
        assert_ne!(expired_replid, second_replid);
        assert!(backend.ctx.replication().backlog.second_replid().is_empty());
        assert_eq!(backend.ctx.replication().backlog.history_len(), 0);
        for (connection_id, replid) in [
            (41, &current_replid),
            (42, &second_replid),
            (43, &expired_replid),
        ] {
            let mut link = PrimaryLink::new(
                ConnectionId(connection_id),
                ReplicaConnectionInfo {
                    ip: "127.0.0.1".to_string(),
                    port: 6380,
                    capability: Vec::new(),
                    rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
                },
            );
            assert!(matches!(
                link.handle_psync(replid, request, &backend.ctx.replication().backlog),
                PrimarySyncPlan::FullResync { .. }
            ));
        }

        backend.ctx.replication_mut().backlog.activate();

        assert!(backend
            .ctx
            .replication()
            .backlog
            .resolve_continue(&expired_replid, request)
            .is_some());
        assert!(backend
            .ctx
            .replication()
            .backlog
            .resolve_continue(&current_replid, request)
            .is_none());
        assert!(backend
            .ctx
            .replication()
            .backlog
            .resolve_continue(&second_replid, request)
            .is_none());
    }
}

#[test]
fn plain_failover_selects_any_caught_up_replica() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = 41usize;
    let paused = 43usize;
    let (_, replica_id) = setup_primary(&mut backend, main, Vec::new(), false, true);
    add_connection(&mut backend, paused);
    backend.ctx.paused_clients_mut().insert(Token(paused));
    backend.ctx.replication_mut().backlog.activate();
    let primary_replid = backend.ctx.replication().backlog.replid().to_string();
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .set_state(PrimaryLinkState::Streaming);
    backend.ctx.replication_mut().pending_failover_target = None;
    backend.ctx.replication_mut().failover_state = "waiting-for-sync".to_string();
    backend.ctx.client_pause_mut().action = crate::framework::context::PauseAction::Write;
    backend.ctx.client_pause_mut().end_time_ms = i64::MAX;

    backend.process_pending_failover();
    let sync_offset = backend
        .ctx
        .replication()
        .failover_sync_offset
        .expect("plain FAILOVER must request a fresh ACK");
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .acknowledge(sync_offset, crate::foundation::io::sys::time::mstime());

    backend.process_pending_failover();

    assert!(backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(paused)));
    assert!(backend.ctx.paused_clients().contains(&Token(paused)));
    let target_key = main;
    assert!(
        backend.connection_registry[crate::framework::capabilities::io::Token(target_key)]
            .output
            .queue
            .iter()
            .all(|frame| !frame
                .windows(b"REPLICAOF".len())
                .any(|part| part == b"REPLICAOF")),
        "C FAILOVER hands off through PSYNC FAILOVER instead of propagating REPLICAOF NO ONE"
    );
    assert_eq!(
        backend.ctx.config().replication.master_host.as_deref(),
        Some("127.0.0.1")
    );
    assert_eq!(backend.ctx.config().replication.master_port, 6380);
    assert_eq!(
        backend.ctx.replication().role,
        crate::engine::distributed::replication::ReplicationRole::Replica
    );
    assert_eq!(
        backend.ctx.replication().failover_state,
        "failover-in-progress"
    );
    assert_eq!(
        backend.ctx.replication().disconnect_clients_until_ms,
        0,
        "clients remain deferred until the PSYNC FAILOVER result is known"
    );
    assert_eq!(
        backend.ctx.client_pause().action,
        crate::framework::context::PauseAction::Write
    );
    let target = MasterInfo {
        host: "127.0.0.1".to_string(),
        port: 6380,
    };
    assert_eq!(
        backend.replication_runtime.replica_resume_for(&target),
        Some((primary_replid.as_str(), sync_offset, 0)),
        "C failover caches the old primary's own history before following the promoted replica"
    );
}

#[test]
fn failover_waits_for_the_replica_to_ack_the_reported_offset() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = 41usize;
    let (_, replica_id) = setup_primary(&mut backend, main, Vec::new(), false, true);
    backend.ctx.replication_mut().backlog.activate();
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .set_state(PrimaryLinkState::Streaming);
    backend.ctx.replication_mut().pending_failover_target = None;
    backend.ctx.replication_mut().failover_state = "waiting-for-sync".to_string();

    backend.process_pending_failover();
    let first_offset = backend.ctx.replication().backlog.reported_offset().get();
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .acknowledge(
            offset(first_offset),
            crate::foundation::io::sys::time::mstime(),
        );
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"later-keepalive")
        .unwrap();

    backend.process_pending_failover();

    assert_eq!(
        backend.ctx.replication().role,
        crate::engine::distributed::replication::ReplicationRole::Primary
    );
    let current_reported_offset = backend.ctx.replication().backlog.reported_offset();
    assert_eq!(
        backend.ctx.replication().failover_sync_offset,
        Some(current_reported_offset)
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .acknowledge(
            current_reported_offset,
            crate::foundation::io::sys::time::mstime(),
        );

    backend.process_pending_failover();

    assert!(backend.ctx.replication().is_replica());
}

#[test]
fn pending_failover_preserves_an_explicit_client_pause() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = 41usize;
    let _ = setup_primary(&mut backend, main, Vec::new(), false, true);
    backend.ctx.replication_mut().failover_state = "waiting-for-sync".to_string();
    let pause_end = crate::foundation::io::sys::time::mstime() + 180_000;
    backend.ctx.client_pause_mut().action = crate::framework::context::PauseAction::Write;
    backend.ctx.client_pause_mut().end_time_ms = pause_end;
    backend.ctx.client_pause_mut().reason = "client_pause";

    backend.process_pending_failover();

    assert_eq!(backend.ctx.client_pause().end_time_ms, pause_end);
    assert_eq!(backend.ctx.client_pause().reason, "client_pause");
}

#[test]
fn primary_to_replica_replaces_stale_resume_with_its_current_history() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"current-history")
        .unwrap();
    let current_replid = backend.ctx.replication().backlog.replid().to_string();
    let current_reported_offset = backend.ctx.replication().backlog.reported_offset().get();
    let target = MasterInfo {
        host: "127.0.0.1".to_string(),
        port: 6380,
    };
    backend.replication_runtime.set_replica_resume(
        target.clone(),
        "stale-replication-id".to_string(),
        offset(7),
        3,
    );
    backend.ctx.config_mut().replication.master_host = Some(target.host.clone());
    backend.ctx.config_mut().replication.master_port = target.port;
    backend.ctx.replication_mut().role =
        crate::engine::distributed::replication::ReplicationRole::Replica;
    backend
        .ctx
        .replication_mut()
        .cache_primary_on_next_reconnect = true;
    backend.ctx.replication_mut().pending_replicaof_request = true;

    backend.apply_pending_replicaof_request();

    assert_eq!(
        backend.replication_runtime.replica_resume_for(&target),
        Some((
            current_replid.as_str(),
            offset(current_reported_offset),
            0,
        )),
        "C replicationSetPrimary discards an old cache and snapshots the node's current primary history"
    );
}

#[test]
fn explicit_replicaof_target_change_overrides_old_link_teardown_backoff() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    add_connection(&mut backend, 41);
    backend
        .replication_runtime
        .set_replica_link(ReplicaLink::new(
            ConnectionId(41),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        ));
    backend.ctx.config_mut().replication.master_host = Some("127.0.0.2".to_string());
    backend.ctx.config_mut().replication.master_port = 6380;
    backend.ctx.replication_mut().role =
        crate::engine::distributed::replication::ReplicationRole::Replica;
    backend.ctx.replication_mut().pending_replicaof_request = true;

    backend.apply_pending_replicaof_request();

    assert!(
        backend
            .replication_runtime
            .master_connect_allowed(crate::foundation::io::sys::time::mstime()),
        "an explicit target change must not inherit teardown backoff"
    );
}

#[test]
fn explicit_replicaof_starts_upstream_connect_immediately() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind primary peer");
    let address = listener.local_addr().expect("primary peer address");
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    backend.ctx.config_mut().replication.master_host = Some(address.ip().to_string());
    backend.ctx.config_mut().replication.master_port = address.port();
    backend.ctx.replication_mut().role =
        crate::engine::distributed::replication::ReplicationRole::Replica;
    backend.ctx.replication_mut().pending_replicaof_request = true;

    backend.apply_pending_replicaof_request();

    let link = backend
        .replication_runtime
        .replica_link()
        .expect("explicit REPLICAOF must start the upstream connection immediately");
    assert_eq!(link.master().host, address.ip().to_string());
    assert_eq!(link.master().port, address.port());
    assert_eq!(
        backend.ctx.config().replication.master_link_status,
        "connect"
    );
}

#[test]
fn primary_to_former_upstream_preserves_resume_from_second_replid() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"upstream-history")
        .unwrap();
    let upstream_replid = backend.ctx.replication().backlog.replid().to_string();
    let upstream_offset = backend.ctx.replication().backlog.reported_offset().get();
    backend.ctx.replication_mut().backlog.shift_replication_id();
    let target = MasterInfo {
        host: "127.0.0.1".to_string(),
        port: 6380,
    };
    backend.replication_runtime.set_replica_resume(
        target.clone(),
        upstream_replid.clone(),
        offset(upstream_offset),
        9,
    );
    backend.ctx.config_mut().replication.master_host = Some(target.host.clone());
    backend.ctx.config_mut().replication.master_port = target.port;
    backend.ctx.replication_mut().role =
        crate::engine::distributed::replication::ReplicationRole::Replica;
    backend
        .ctx
        .replication_mut()
        .cache_primary_on_next_reconnect = true;
    backend.ctx.replication_mut().pending_replicaof_request = true;

    backend.apply_pending_replicaof_request();

    assert_eq!(
        backend.replication_runtime.replica_resume_for(&target),
        Some((upstream_replid.as_str(), offset(upstream_offset), 9)),
        "a cached cursor from the local second replid remains valid for the former upstream"
    );
}

#[test]
fn primary_with_new_writes_replaces_an_old_second_replid_resume() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"upstream-history")
        .unwrap();
    let upstream_replid = backend.ctx.replication().backlog.replid().to_string();
    let detached_offset = backend.ctx.replication().backlog.reported_offset().get();
    backend.ctx.replication_mut().backlog.shift_replication_id();
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"new-primary-history")
        .unwrap();
    let current_replid = backend.ctx.replication().backlog.replid().to_string();
    let current_reported_offset = backend.ctx.replication().backlog.reported_offset().get();
    let target = MasterInfo {
        host: "127.0.0.1".to_string(),
        port: 6380,
    };
    backend.replication_runtime.set_replica_resume(
        target.clone(),
        upstream_replid,
        offset(detached_offset),
        9,
    );
    backend.ctx.config_mut().replication.master_host = Some(target.host.clone());
    backend.ctx.config_mut().replication.master_port = target.port;
    backend.ctx.replication_mut().role =
        crate::engine::distributed::replication::ReplicationRole::Replica;
    backend
        .ctx
        .replication_mut()
        .cache_primary_on_next_reconnect = true;
    backend.ctx.replication_mut().pending_replicaof_request = true;

    backend.apply_pending_replicaof_request();

    assert_eq!(
        backend.replication_runtime.replica_resume_for(&target),
        Some((current_replid.as_str(), offset(current_reported_offset), 0,)),
        "new primary writes make the old replid2 cursor stale for a later target"
    );
}

#[test]
fn primary_demoted_while_throttling_forces_full_sync() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"upstream-history")
        .unwrap();
    let upstream_replid = backend.ctx.replication().backlog.replid().to_string();
    let upstream_offset = backend.ctx.replication().backlog.reported_offset();
    backend.ctx.replication_mut().backlog.shift_replication_id();
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"local-writes-while-replica-output-was-throttled")
        .unwrap();
    let target = MasterInfo {
        host: "127.0.0.1".to_string(),
        port: 6380,
    };
    backend.replication_runtime.set_replica_resume(
        target.clone(),
        upstream_replid,
        upstream_offset,
        0,
    );
    backend
        .ctx
        .replication_mut()
        .throttle
        .adjust(true, 1024, true, &[(1, 8 * 1024 * 1024)]);
    backend.ctx.config_mut().replication.master_host = Some(target.host.clone());
    backend.ctx.config_mut().replication.master_port = target.port;
    backend.ctx.replication_mut().role =
        crate::engine::distributed::replication::ReplicationRole::Replica;
    backend
        .ctx
        .replication_mut()
        .cache_primary_on_next_reconnect = true;
    backend.ctx.replication_mut().pending_replicaof_request = true;

    backend.apply_pending_replicaof_request();

    assert_eq!(
        backend.replication_runtime.replica_resume_for(&target),
        None
    );
}

#[test]
fn accepted_psync_failover_clears_the_old_primary_failover_state() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let paused = 88usize;
    add_connection(&mut backend, paused);
    backend.ctx.paused_clients_mut().insert(Token(paused));
    let replid = "0123456789abcdef0123456789abcdef01234567";
    let mut link = ReplicaLink::new(
        ConnectionId(41),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6380,
        },
    );
    link.set_resume_position(replid.to_string(), offset(42), 0);
    link.set_failover_request(true);
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6379).unwrap();
    link.on_bytes(
        b"+CONTINUE fedcba9876543210fedcba9876543210fedcba98\r\n",
        6379,
    )
    .unwrap();
    backend.replication_runtime.set_replica_link(link);
    // The classic receive path marks a streaming link no-failover before the
    // event-loop cleanup consumes the accepted-handshake edge.
    backend.ctx.replication_mut().failover_state = "no-failover".to_string();
    backend.ctx.replication_mut().failover_end_time_ms = 99;
    backend.ctx.replication_mut().force_failover = true;
    backend.ctx.client_pause_mut().action = crate::framework::context::PauseAction::Write;
    backend.ctx.client_pause_mut().end_time_ms = i64::MAX;
    backend.ctx.client_pause_mut().reason = "failover_in_progress";
    let blocked_session = crate::engine::command::SessionId::from_u64(77);
    backend.ctx.blocking_registry_mut().register_client(
        0,
        &[bytes::Bytes::from_static(b"blocked-key")],
        blocked_session,
    );

    backend.clear_accepted_failover();

    assert_eq!(backend.ctx.replication().failover_state, "no-failover");
    assert_eq!(backend.ctx.replication().failover_end_time_ms, 0);
    assert!(!backend.ctx.replication().force_failover);
    assert_eq!(
        backend.ctx.client_pause().action,
        crate::framework::context::PauseAction::None
    );
    assert_eq!(backend.ctx.pending_blocked_to_reject(), &[blocked_session]);
    assert!(backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(paused)));
    assert!(backend.ctx.paused_clients().contains(&Token(paused)));
    assert!(backend.ctx.pending_unpause());
}

#[test]
fn accepted_psync_failover_preserves_an_explicit_client_pause() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let replid = "0123456789abcdef0123456789abcdef01234567";
    let mut link = ReplicaLink::new(
        ConnectionId(41),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6380,
        },
    );
    link.set_resume_position(replid.to_string(), offset(42), 0);
    link.set_failover_request(true);
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6379).unwrap();
    link.on_bytes(
        b"+CONTINUE fedcba9876543210fedcba9876543210fedcba98\r\n",
        6379,
    )
    .unwrap();
    backend.replication_runtime.set_replica_link(link);
    backend.ctx.replication_mut().failover_state = "failover-in-progress".to_string();
    backend.ctx.client_pause_mut().action = crate::framework::context::PauseAction::Write;
    backend.ctx.client_pause_mut().end_time_ms = i64::MAX - 1;
    backend.ctx.client_pause_mut().reason = "client_pause";

    backend.clear_accepted_failover();

    assert_eq!(
        backend.ctx.client_pause().action,
        crate::framework::context::PauseAction::Write
    );
    assert_eq!(backend.ctx.client_pause().end_time_ms, i64::MAX - 1);
    assert_eq!(backend.ctx.client_pause().reason, "client_pause");
    assert!(!backend.ctx.pending_unpause());
}

#[test]
fn writes_before_first_replica_sync_do_not_create_replication_history() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = 41usize;
    let _ = setup_primary(&mut backend, main, dual_channel_caps(), true, true);

    backend.propagate_to_replicas(b"pre-sync-write");

    let backlog = &backend.ctx.replication().backlog;
    assert_eq!(backlog.reported_offset().get(), 0);
    assert_eq!(backlog.history_len(), 0);
    assert!(!backlog.is_active());
}

#[test]
fn first_dual_rdb_endoff_creates_a_new_replication_history() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = 41usize;
    let rdb = 42usize;
    let (main_slab, _) = setup_primary(&mut backend, main, dual_channel_caps(), true, true);
    let initial_replid = backend.ctx.replication().backlog.replid().to_string();
    let replies = run_psync(&mut backend, main_slab, main, b"PSYNC");
    assert_eq!(
        replies.concat(),
        protocol::DUAL_CHANNEL_SYNC_REPLY,
        "test setup must enter dual-channel full sync"
    );
    add_connection(&mut backend, rdb);
    backend
        .replication_runtime
        .register_rdb_candidate(ConnectionId(rdb))
        .unwrap();

    let endoff = backend
        .replication_runtime
        .take_endoff_for_rdb_candidate_if_unambiguous(
            ConnectionId(rdb),
            backend
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(rdb)),
            &mut backend.ctx.replication_mut().backlog,
        )
        .expect("first RDB candidate must receive ENDOFF");
    let parsed =
        protocol::parse_end_offset(&endoff[..endoff.len() - 2]).expect("ENDOFF frame must parse");
    let backlog = &backend.ctx.replication().backlog;

    assert!(backlog.is_active());
    assert_ne!(backlog.replid(), initial_replid);
    assert_eq!(parsed.replid, backlog.replid());
    assert_eq!(parsed.offset, offset(0));
    assert_eq!(backlog.history_len(), 0);
}

#[test]
fn first_post_endoff_frame_backfills_waiting_psync_pin() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = 41usize;
    let rdb = 42usize;
    let (main_slab, _) = setup_primary(&mut backend, main, dual_channel_caps(), true, true);
    let _ = run_psync(&mut backend, main_slab, main, b"PSYNC");
    add_connection(&mut backend, rdb);
    backend
        .replication_runtime
        .register_rdb_candidate(ConnectionId(rdb))
        .unwrap();
    backend
        .replication_runtime
        .take_endoff_for_rdb_candidate_if_unambiguous(
            ConnectionId(rdb),
            backend
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(rdb)),
            &mut backend.ctx.replication_mut().backlog,
        )
        .expect("first RDB candidate must receive ENDOFF");
    let resume_cursor = backend.ctx.replication().backlog.next_stream_cursor();
    backend.ctx.replication_mut().backlog.set_capacity(1);

    backend.propagate_to_replicas(b"post-endoff-history");

    assert_eq!(
        backend.replication_runtime.min_protected_backlog_cursor(),
        Some(resume_cursor)
    );
    assert_eq!(
        backend.ctx.replication().backlog.read_from(resume_cursor),
        Some(b"post-endoff-history".to_vec())
    );
}

#[test]
fn offset_domain_restored_empty_wait_psync_backfills_stream_cursor_and_trims() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend
        .ctx
        .replication_mut()
        .backlog
        .restore_primary_from_rdb(
            "restored",
            crate::engine::distributed::replication::ReplicationOffset::try_from(41).unwrap(),
        )
        .unwrap();
    let main = 41usize;
    let rdb = 42usize;
    let (main_slab, _) = setup_primary(&mut backend, main, dual_channel_caps(), true, true);
    let _ = run_psync(&mut backend, main_slab, main, b"PSYNC");
    add_connection(&mut backend, rdb);
    backend
        .replication_runtime
        .register_rdb_candidate(ConnectionId(rdb))
        .unwrap();
    backend
        .replication_runtime
        .take_endoff_for_rdb_candidate_if_unambiguous(
            ConnectionId(rdb),
            true,
            &mut backend.ctx.replication_mut().backlog,
        )
        .expect("restored empty history must produce ENDOFF");
    let expected_pin = backend.ctx.replication().backlog.next_stream_cursor();

    backend.propagate_to_replicas(b"first");

    assert_eq!(
        backend.replication_runtime.min_protected_backlog_cursor(),
        Some(expected_pin)
    );
    backend
        .replication_runtime
        .remove_connection(ConnectionId(rdb));
    backend.ctx.config_mut().server.repl_backlog_size = 1;
    backend.refresh_replication_buffer_accounting();
    assert_eq!(backend.ctx.replication().backlog.history_len(), 1);
}

#[test]
fn offset_domain_restored_dual_partial_write_ack_trim_and_drain_stays_ordered() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend
        .ctx
        .replication_mut()
        .backlog
        .restore_primary_from_rdb(
            "restored",
            crate::engine::distributed::replication::ReplicationOffset::try_from(41).unwrap(),
        )
        .unwrap();
    let replid = backend.ctx.replication().backlog.replid().to_string();
    let connection_token = add_connection(&mut backend, 41);
    let connection_id = ConnectionId(41);
    let replica_id = backend.replication_runtime.register_primary(
        connection_id,
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: dual_channel_caps(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    {
        let backlog = &backend.ctx.replication().backlog;
        let link = backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap();
        link.transition_to_waiting_for_rdb_channel().unwrap();
        link.record_dual_channel_endoff_boundary(boundary);
        link.transition_to_waiting_for_main_psync().unwrap();
        assert!(link.complete_dual_channel_main_psync(
            &replid,
            crate::engine::distributed::replication::PsyncRequestOffset::try_from_raw(42).unwrap(),
            backlog,
        ));
    }
    let assert_ordered = |backend: &ServerWorkerState| {
        let link = backend
            .replication_runtime
            .primary_link(replica_id)
            .unwrap();
        assert!(link.sent_cursor() <= link.next_send_cursor());
        assert!(link.next_send_cursor() <= backend.ctx.replication().backlog.next_stream_cursor());
        assert_eq!(link.state(), PrimaryLinkState::Streaming);
        assert_ne!(link.state(), PrimaryLinkState::Closing);
    };
    assert_ordered(&backend);

    backend.propagate_to_replicas(b"AA");
    assert_ordered(&backend);
    let queued_before = backend.connection_registry[Token(connection_token)].output_buffer_bytes();
    backend.connection_registry[Token(connection_token)]
        .output
        .queue
        .front_mut()
        .unwrap()
        .drain(..1);
    let replication_written = backend.connection_registry[Token(connection_token)]
        .consume_written_bytes(1, queued_before)
        .unwrap();
    let backlog_end = backend.ctx.replication().backlog.next_stream_cursor();
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .mark_replication_written(replication_written, backlog_end)
        .unwrap();
    assert_ordered(&backend);

    {
        let link = backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap();
        link.mark_dual_rdb_complete();
        link.acknowledge(offset(43), 1_000);
        assert!(link.promote_dual_replica_on_ack());
        assert_eq!(link.dual_state(), PrimaryDualChannelState::Online);
    }
    assert_ordered(&backend);

    backend.ctx.config_mut().server.repl_backlog_size = 1;
    backend.refresh_replication_buffer_accounting();
    backend.propagate_to_replicas(b"B");
    assert_ordered(&backend);
    let queued_before = backend.connection_registry[Token(connection_token)].output_buffer_bytes();
    let frames = &mut backend.connection_registry[Token(connection_token)]
        .output
        .queue;
    frames.clear();
    let replication_written = backend.connection_registry[Token(connection_token)]
        .consume_written_bytes(queued_before, queued_before)
        .unwrap();
    let backlog_end = backend.ctx.replication().backlog.next_stream_cursor();
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .mark_replication_written(replication_written, backlog_end)
        .unwrap();
    assert_ordered(&backend);
    let link = backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap();
    assert_eq!(
        link.pump(&backend.ctx.replication().backlog),
        Ok(Vec::new())
    );
    assert_eq!(link.dual_state(), PrimaryDualChannelState::Online);
}

#[test]
#[cfg(target_os = "linux")]
fn replica_buffer_memory_excludes_tcp_kernel_send_queue() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let peer = backend.transport.connect_tcp(address).unwrap();
    let (reader, _) = listener.accept().unwrap();
    let token_value = peer.token.0;
    let connection_token = backend
        .connection_registry
        .insert(Connection::new(peer.token, address, false));
    let id = backend.replication_runtime.register_primary(
        ConnectionId(token_value),
        ReplicaConnectionInfo {
            ip: address.ip().to_string(),
            port: address.port(),
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(id)
        .unwrap()
        .set_state(PrimaryLinkState::Streaming);

    let send_buffer = 8 * 1024;
    let receive_buffer = 8 * 1024;
    let writer_fd = backend.transport.raw_fd(peer.token).unwrap();
    crate::foundation::io::sys::socket::set_buffer_size_raw(
        writer_fd,
        crate::foundation::io::sys::socket::SocketBuffer::Send,
        send_buffer,
    )
    .expect("set writer send buffer");
    crate::foundation::io::sys::socket::set_buffer_size_raw(
        reader.as_raw_fd(),
        crate::foundation::io::sys::socket::SocketBuffer::Receive,
        receive_buffer,
    )
    .expect("set reader receive buffer");

    let chunk = [0x5a; 64 * 1024];
    let mut wrote = 0usize;
    loop {
        match backend.transport.write(peer.token, &chunk) {
            Ok(0) => break,
            Ok(count) => wrote += count,
            Err(error) if error.kind() == ErrorKind::WouldBlock => break,
            Err(error) => panic!("write failed before socket backpressure: {error}"),
        }
        assert!(wrote < 16 * 1024 * 1024, "socket never back-pressured");
    }
    assert!(
        crate::foundation::io::sys::os::socket_pending_write_bytes_fd(writer_fd).unwrap() > 0,
        "test setup must leave bytes in the kernel send queue"
    );
    assert_eq!(
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .output
            .replication_bytes,
        0
    );

    backend.refresh_replica_output_buffer_bytes();

    assert_eq!(
        backend.ctx.replication().replica_output_buffer_bytes,
        0,
        "kernel TCP send queue is not server-owned replication-buffer memory"
    );
}
