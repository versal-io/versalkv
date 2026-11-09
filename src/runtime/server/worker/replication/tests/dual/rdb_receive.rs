use super::*;

fn setup_live_ordinary_connection(backend: &mut ServerWorkerState) -> (std::net::TcpStream, Token) {
    use crate::runtime::server::worker::event_loop::transport::RuntimeConnectedPeer;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ordinary peer");
    let peer_addr = listener.local_addr().expect("ordinary peer address");
    let connected = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect ordinary transport");
    let (peer, _) = listener.accept().expect("accept ordinary transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: connected.token,
        peer_addr,
    });
    (peer, connected.token)
}

fn poll_until_readable_tokens(
    backend: &mut ServerWorkerState,
    tokens: &[Token],
) -> Vec<crate::runtime::server::worker::event_loop::transport::RuntimeEvent> {
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for readable tokens: {tokens:?}"
        );
        let events = backend
            .transport
            .poll_events(Some(remaining))
            .expect("poll live readiness");
        if tokens.iter().all(|token| {
            events
                .iter()
                .any(|event| event.token == *token && event.readable)
        }) {
            return events;
        }
        std::thread::yield_now();
    }
}

#[test]
fn streaming_rdb_dispatch_restores_main_link_before_opening_dual_rdb_channel() {
    assert_dual_rdb_channel_opens_after_dispatch(|backend, token_value, connection_token| {
        backend.recv_streaming_rdb_chunk(token_value, connection_token);
    });
}

#[test]
fn replication_stream_dispatch_restores_main_link_before_opening_dual_rdb_channel() {
    assert_dual_rdb_channel_opens_after_dispatch(|backend, token_value, connection_token| {
        backend.recv_replication_stream(token_value, connection_token);
    });
}

#[test]
fn rdb_channel_yields_after_one_io_buffer_and_resumes_on_level_readiness() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().replication.repl_diskless_load = "flush-before-load".to_string();
    let (main_token, _main_slab, rdb_token, _rdb_slab, mut primary_stream) =
        setup_live_dual_rdb_receiver(&mut backend);
    let (mut ordinary_peer, ordinary_token) = setup_live_ordinary_connection(&mut backend);
    let value = vec![b'x'; crate::runtime::server::worker::IO_BUF_SIZE * 2];
    let mut rdb = b"VALKEY080\x00".to_vec();
    crate::foundation::io::persistence::save_string(&mut rdb, b"k");
    crate::foundation::io::persistence::save_string(&mut rdb, &value);
    rdb.push(0xff);
    rdb.extend_from_slice(&[0; 8]);

    let mut bytes = b"+OK\r\n".to_vec();
    bytes.extend_from_slice(&protocol::encode_end_offset(
        offset(123),
        "0123456789abcdef0123456789abcdef01234567",
        0,
        rdb_token as u64,
    ));
    bytes.extend_from_slice(format!("${}\r\n", rdb.len()).as_bytes());
    bytes.extend_from_slice(&rdb);
    primary_stream
        .write_all(&bytes)
        .expect("send RDB larger than one event-loop buffer");
    ordinary_peer
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .expect("send competing PING");

    let first_events =
        poll_until_readable_tokens(&mut backend, &[Token(rdb_token), ordinary_token]);
    for event in first_events {
        backend.dispatch_transport_event(
            crate::runtime::server::worker::event_loop::ReadyEvent {
                token: event.token,
                readable: event.readable,
                writable: event.writable,
                error: event.error,
                read_hup: event.read_hup,
            },
            event.token == ordinary_token,
        );
    }

    let first_payload_bytes = backend
        .replication_runtime
        .replica_link()
        .expect("main link remains while RDB is incomplete")
        .rdb_payload_bytes();
    assert!(first_payload_bytes > 0);
    assert!(first_payload_bytes <= crate::runtime::server::worker::IO_BUF_SIZE as u64);
    assert!(backend
        .replication_runtime
        .replica_link()
        .is_some_and(|link| link.has_pending_rdb_receive()));
    assert_eq!(
        backend
            .replication_runtime
            .replica_link()
            .expect("main link remains while RDB is incomplete")
            .connection_id(),
        ConnectionId(main_token),
        "the fair read must restore the main link before yielding"
    );
    assert_eq!(
        backend.next_poll_timeout(1_000),
        None,
        "TCP level readiness, not a user-space continuation, must resume the RDB read"
    );
    ordinary_peer
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set competing PING reply timeout");
    let mut reply = Vec::new();
    std::io::BufRead::read_until(
        &mut std::io::BufReader::new(&mut ordinary_peer),
        b'\n',
        &mut reply,
    )
    .expect("read competing PING reply");
    assert!(
        reply.starts_with(b"-LOADING ") && reply.ends_with(b"\r\n"),
        "competing PING must receive the loading reply in the first dispatch batch: {reply:?}"
    );

    let second_events = poll_until_readable_tokens(&mut backend, &[Token(rdb_token)]);
    let second_rdb_event = second_events
        .into_iter()
        .find(|event| event.token == Token(rdb_token) && event.readable)
        .expect("LT poll must report the same RDB token readable again");
    backend.dispatch_transport_event(
        crate::runtime::server::worker::event_loop::ReadyEvent {
            token: second_rdb_event.token,
            readable: second_rdb_event.readable,
            writable: second_rdb_event.writable,
            error: second_rdb_event.error,
            read_hup: second_rdb_event.read_hup,
        },
        false,
    );

    let link = backend
        .replication_runtime
        .replica_link()
        .expect("main link remains registered after the next readable turn");
    assert_eq!(link.connection_id(), ConnectionId(main_token));
    assert!(
        !link.has_pending_rdb_receive() || link.rdb_payload_bytes() > first_payload_bytes,
        "the next LT readable turn must continue or complete the RDB receive"
    );
}

#[test]
fn dual_rdb_readable_hup_drains_complete_multichunk_rdb_without_phantom_pair() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_loading(true);
    let (main_token, _main_slab, rdb_token, _primary_stream, mut rdb_primary_stream) =
        setup_dual_blocked_main_receiver_with_rdb_peer(&mut backend);
    let large_value = vec![b'x'; backend.io_buf.len() + 1];
    let mut rdb_tail = b"VALKEY080\x00".to_vec();
    crate::foundation::io::persistence::save_string(&mut rdb_tail, b"large");
    crate::foundation::io::persistence::save_string(&mut rdb_tail, &large_value);
    rdb_tail.push(0xff);
    rdb_tail.extend_from_slice(&[0; 8]);
    rdb_tail.extend_from_slice(b"0123456789abcdef0123456789abcdef01234567");
    assert!(rdb_tail.len() > backend.io_buf.len());
    backend
        .transport
        .reserve_test_receive_buffer(Token(rdb_token), rdb_tail.len())
        .expect("size receive buffer for the unread multi-chunk RDB tail");
    rdb_primary_stream
        .write_all(&rdb_tail)
        .expect("write complete multi-chunk RDB tail");
    rdb_primary_stream
        .shutdown(std::net::Shutdown::Write)
        .expect("half-close complete RDB peer");
    // Do not inject a terminal event before the complete TCP tail has arrived.
    backend
        .transport
        .wait_pending_bytes_for_test(Token(rdb_token), rdb_tail.len());
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
    backend.replay_one_deferred_readiness();

    assert_eq!(
        backend
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"large")
            .map(|value| value.as_bytes()),
        Some(large_value.as_slice())
    );
    let link = backend
        .replication_runtime
        .replica_link()
        .expect("successful RDB load keeps the main link");
    assert_eq!(link.rdb_channel_state(), ReplicaRdbChannelState::None);
    assert_eq!(link.applied_position(), offset(100));
    assert!(backend.transport.is_registered(Token(main_token)));
    assert!(!backend.transport.is_registered(Token(rdb_token)));
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_token)));
    assert!(backend.replication_runtime.replica_rdb_link().is_none());
    assert_eq!(
        backend.replication_runtime.role(ConnectionId(rdb_token)),
        None
    );
    assert_eq!(
        backend
            .replication_runtime
            .channel_pair_for(ConnectionId(main_token)),
        None
    );
    assert!(!backend.ctx.is_loading());
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    assert_eq!(backend.ctx.replication().slave_repl_offset, offset(100));
}

#[test]
fn dual_framing_bytes_do_not_disable_payload_read_budget() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend
        .ctx
        .config_mut()
        .persistence
        .loading_process_events_interval_bytes = 1024;
    backend.ctx.config_mut().replication.repl_diskless_load = "swapdb".to_string();
    let aborted = ReplicaLink::new(
        ConnectionId(701),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    backend
        .classic_rdb_loading_progress
        .begin_session(aborted.session_id());
    assert!(!backend
        .classic_rdb_loading_progress
        .record_payload(1000, 1024));
    let (_main_token, _main_slab, rdb_token, rdb_slab, mut primary_stream) =
        setup_live_dual_rdb_receiver(&mut backend);
    let replid = backend.ctx.replication().backlog.replid().to_string();

    let mut rdb = b"VALKEY080\x00".to_vec();
    crate::foundation::io::persistence::save_string(&mut rdb, b"large");
    crate::foundation::io::persistence::save_string(&mut rdb, &vec![b'x'; 4096]);
    rdb.push(0xff);
    rdb.extend_from_slice(&[0; 8]);
    let mut frame = b"+OK\r\n".to_vec();
    frame.extend_from_slice(&protocol::encode_end_offset(
        offset(123),
        &replid,
        0,
        rdb_token as u64,
    ));
    frame.extend_from_slice(format!("${}\r\n", rdb.len()).as_bytes());
    let framing_bytes = frame.len();
    frame.extend_from_slice(&rdb[..1024 - framing_bytes]);
    assert_eq!(frame.len(), 1024);
    primary_stream
        .write_all(&frame)
        .expect("send one read-budget-sized dual RDB prefix");
    backend
        .transport
        .wait_pending_bytes_for_test(Token(rdb_token), frame.len());

    backend.recv_rdb_channel_streaming_chunk(rdb_token, rdb_slab);

    let payload_bytes = backend
        .replication_runtime
        .replica_link()
        .expect("dual RDB session remains active")
        .rdb_payload_bytes();
    assert_eq!(payload_bytes, (1024 - framing_bytes) as u64);
    assert_eq!(
        backend
            .classic_rdb_loading_progress
            .read_budget(crate::runtime::server::worker::IO_BUF_SIZE, 1024),
        framing_bytes,
        "the next dual RDB socket read must stop at the payload boundary"
    );
}

fn decoder_rejected_rdb_body() -> &'static [u8] {
    // Valid RDB magic/version followed by an unknown top-level object type.
    b"VALKEY080\x80"
}

#[test]
fn coalesced_dual_rdb_body_waits_for_decoder_completion() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let tmp_dir =
        std::env::temp_dir().join(format!("pkv-coalesced-dual-rdb-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).expect("create temp rdb commit dir");
    backend.ctx.config_mut().persistence.dir = tmp_dir.display().to_string();
    let (main_token, main_slab, rdb_token, rdb_slab, mut primary_stream) =
        setup_live_dual_rdb_receiver(&mut backend);
    let mut bytes = b"+OK\r\n".to_vec();
    bytes.extend_from_slice(&protocol::encode_end_offset(
        offset(123),
        "0123456789abcdef0123456789abcdef01234567",
        0,
        rdb_token as u64,
    ));
    let rdb = minimal_valid_rdb_body();
    bytes.extend_from_slice(format!("${}\r\n", rdb.len()).as_bytes());
    bytes.extend_from_slice(rdb);
    primary_stream
        .write_all(&bytes)
        .expect("send coalesced ENDOFF and RDB framing");
    backend
        .transport
        .wait_pending_bytes_for_test(Token(rdb_token), bytes.len());

    backend.dispatch_client_read(
        rdb_token,
        crate::runtime::server::worker::connection::ClientReadMode::Fair,
    );

    let link = backend
        .replication_runtime
        .replica_link()
        .expect("main link must survive successful RDB decode");
    assert_eq!(link.connection_id(), ConnectionId(main_token));
    assert_eq!(
        link.rdb_channel_state(),
        ReplicaRdbChannelState::RdbLoaded,
        "RDB channel may become loaded only after the decoder finishes"
    );
    assert!(
        !link.has_pending_rdb_receive(),
        "finished decoder must hand its receive state to the RDB commit"
    );
    assert_eq!(
        backend
            .ctx
            .dbs()
            .get(0)
            .and_then(|db| db.get_no_expire(b"k"))
            .map(|value| value.as_bytes()),
        Some(&b"v"[..]),
        "the framed RDB body must reach RdbReceive and be decoded before RdbLoaded"
    );
    assert!(
        !backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(rdb_slab)),
        "the dedicated RDB transport must close after its load commit"
    );
    assert!(
        backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(main_slab)),
        "closing the RDB transport must preserve the pending main PSYNC transport"
    );

    let _ = std::fs::remove_dir_all(&tmp_dir);
}

#[test]
fn failed_dual_rdb_commit_keeps_transport_for_outer_teardown() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let missing_dir = std::env::temp_dir().join(format!(
        "pkv-missing-dual-rdb-commit-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&missing_dir);
    backend.ctx.config_mut().persistence.dir = missing_dir.display().to_string();

    let (_main_token, _main_slab, rdb_token, rdb_slab, _primary_stream) =
        setup_live_dual_rdb_receiver(&mut backend);
    let mut link = backend
        .replication_runtime
        .take_replica_link()
        .expect("dual main link must exist");
    link.pin_rdb_load_mode(crate::framework::distributed::replication::RdbLoadMode::DiskBacked);

    let rdb = minimal_valid_rdb_body();
    let mut bytes = b"+OK\r\n".to_vec();
    bytes.extend_from_slice(&protocol::encode_end_offset(
        offset(123),
        "0123456789abcdef0123456789abcdef01234567",
        0,
        rdb_token as u64,
    ));
    bytes.extend_from_slice(format!("${}\r\n", rdb.len()).as_bytes());
    bytes.extend_from_slice(rdb);
    link.on_rdb_bytes(&bytes)
        .expect("valid framed RDB must reach commit preparation");

    assert!(
        backend
            .drive_streaming_decode(rdb_token, rdb_slab, &mut link)
            .is_err(),
        "missing persistence directory must fail the disk-backed commit"
    );
    assert_eq!(
        link.rdb_channel_state(),
        ReplicaRdbChannelState::ReceivingRdb,
        "a failed snapshot commit must not publish RdbLoaded"
    );
    assert!(
        backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(rdb_slab)),
        "decoder must leave the RDB connection for the outer failure teardown"
    );
    assert!(
        backend.transport.is_registered(Token(rdb_token)),
        "RDB transport must not close before the received snapshot commit succeeds"
    );
}

#[test]
fn decoder_rejected_dual_rdb_body_tears_down_paired_transports_and_reconnects() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let (main_token, main_slab, rdb_token, rdb_slab, mut primary_stream) =
        setup_live_dual_rdb_receiver(&mut backend);
    let mut bytes = b"+OK\r\n".to_vec();
    bytes.extend_from_slice(&protocol::encode_end_offset(
        offset(123),
        "0123456789abcdef0123456789abcdef01234567",
        0,
        rdb_token as u64,
    ));
    let rdb = decoder_rejected_rdb_body();
    bytes.extend_from_slice(format!("${}\r\n", rdb.len()).as_bytes());
    bytes.extend_from_slice(rdb);
    primary_stream
        .write_all(&bytes)
        .expect("send decoder-rejected framed RDB body");
    backend.transport.wait_readable_for_test(Token(rdb_token));

    backend.dispatch_client_read(
        rdb_token,
        crate::runtime::server::worker::connection::ClientReadMode::Fair,
    );

    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(backend.replication_runtime.replica_rdb_link().is_none());
    assert_eq!(
        backend.replication_runtime.role(ConnectionId(main_token)),
        None
    );
    assert_eq!(
        backend.replication_runtime.role(ConnectionId(rdb_token)),
        None
    );
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(main_slab)));
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_slab)));
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(main_token)));
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_token)));
    assert!(backend
        .replication_runtime
        .master_connect_allowed(crate::foundation::io::sys::time::mstime()));
}

#[test]
fn dual_diskless_load_mode_decode_on_rdb_channel_reaches_rdb_loaded() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().replication.repl_diskless_load = "on".to_string();

    let main_token = 111usize;
    let rdb_token = 112usize;
    let _main_slab = add_connection(&mut backend, main_token);
    let rdb_slab = add_connection(&mut backend, rdb_token);

    let main_connection_id = ConnectionId(main_token);
    let rdb_connection_id = ConnectionId(rdb_token);

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
    backend.replication_runtime.set_replica_link(link);
    backend
        .replication_runtime
        .set_replica_rdb_link(rdb_connection_id, main_connection_id)
        .unwrap();

    let marker = b"0123456789abcdef0123456789abcdef01234567";
    let rdb = crate::foundation::io::persistence::serialize_all(backend.ctx.dbs(), 0);
    let mut frame = b"+OK\r\n".to_vec();
    frame.extend_from_slice(&protocol::encode_end_offset(
        offset(1000),
        "0123456789abcdef0123456789abcdef01234567",
        0,
        rdb_token as u64,
    ));
    frame.extend_from_slice(format!("$EOF:{}\r\n", String::from_utf8_lossy(marker)).as_bytes());
    frame.extend_from_slice(&rdb);
    frame.extend_from_slice(marker);

    {
        let mut link = backend.replication_runtime.take_replica_link().unwrap();
        let result = link.on_rdb_bytes(&frame);
        assert!(
            result.is_ok(),
            "on_rdb_channel_framing_bytes failed: {:?}",
            result.err()
        );
        backend.replication_runtime.set_replica_link(link);
    }

    {
        let link = backend.replication_runtime.replica_link().unwrap();
        assert!(
            !link.is_receiving_rdb(),
            " ReplicaLinkState  ReceivingRdb* "
        );
    }

    let token_value = rdb_token;
    let connection_token = rdb_slab;
    {
        let mut link = backend.replication_runtime.take_replica_link().unwrap();
        let _drive_result =
            backend.drive_streaming_decode(token_value, connection_token, &mut link);
        backend.replication_runtime.set_replica_link(link);
    }

    let link = backend.replication_runtime.replica_link().unwrap();
    assert_eq!(
        link.rdb_channel_state(),
        ReplicaRdbChannelState::RdbLoaded,
        "diskless mode: rdb_channel_state  RdbLoaded"
    );
    assert!(!link.is_receiving_rdb(), " state  Streaming ReceivingRdb*");
}

#[test]
fn dual_disk_based_load_mode_decode_on_rdb_channel_reaches_rdb_loaded() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().replication.repl_diskless_load = "off".to_string();

    let main_token = 121usize;
    let rdb_token = 122usize;
    let _main_slab = add_connection(&mut backend, main_token);
    let rdb_slab = add_connection(&mut backend, rdb_token);

    let main_connection_id = ConnectionId(main_token);
    let rdb_connection_id = ConnectionId(rdb_token);

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
    backend.replication_runtime.set_replica_link(link);
    backend
        .replication_runtime
        .set_replica_rdb_link(rdb_connection_id, main_connection_id)
        .unwrap();

    let rdb = crate::foundation::io::persistence::serialize_all(backend.ctx.dbs(), 0);
    let mut frame = b"+OK\r\n".to_vec();
    frame.extend_from_slice(&protocol::encode_end_offset(
        offset(1000),
        "0123456789abcdef0123456789abcdef01234567",
        0,
        rdb_token as u64,
    ));
    frame.extend_from_slice(format!("${}\r\n", rdb.len()).as_bytes());
    frame.extend_from_slice(&rdb);

    {
        let mut link = backend.replication_runtime.take_replica_link().unwrap();
        let result = link.on_rdb_bytes(&frame);
        assert!(
            result.is_ok(),
            "on_rdb_channel_framing_bytes failed: {:?}",
            result.err()
        );
        backend.replication_runtime.set_replica_link(link);
    }

    {
        let link = backend.replication_runtime.replica_link().unwrap();
        assert!(!link.is_receiving_rdb(), " ReceivingRdb* ");
    }

    let token_value = rdb_token;
    let connection_token = rdb_slab;
    {
        let mut link = backend.replication_runtime.take_replica_link().unwrap();
        let _drive_result =
            backend.drive_streaming_decode(token_value, connection_token, &mut link);
        backend.replication_runtime.set_replica_link(link);
    }

    let link = backend.replication_runtime.replica_link().unwrap();
    assert_eq!(
        link.rdb_channel_state(),
        ReplicaRdbChannelState::RdbLoaded,
        "disk-based mode: rdb_channel_state  RdbLoaded"
    );
    assert!(!link.is_receiving_rdb(), " state  Streaming ReceivingRdb*");
}
