use super::*;

#[test]
fn blocked_pump_flushes_aof_before_pending_client_output() {
    let dir = std::env::temp_dir().join(format!(
        "versal-blocked-pump-aof-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create AOF test directory");

    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    backend.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    backend
        .ctx
        .config_mut()
        .persistence
        .aof_appenddirname
        .clear();
    backend.ctx.config_mut().persistence.aof_filename = "appendonly.aof".to_string();
    backend.ctx.config_mut().persistence.aof_fsync =
        crate::foundation::io::persistence::AofFsync::Always;
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.flush_aof_if_needed();
    let initial_size = backend
        .ctx
        .aof()
        .journal
        .as_ref()
        .expect("AOF journal")
        .appender
        .current_size();

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind client peer");
    let peer_addr = listener.local_addr().expect("client peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect client transport");
    let (mut client, _) = listener.accept().expect("accept client transport");
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set client timeout");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    backend.writes.ordinary.begin_turn();
    backend.feed_aof(b"aof-frame");
    assert!(backend.queue_client_reply(peer.token, b"reply".to_vec()));
    backend.transport.queue_test_transport_event_batch(&[]);

    backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(usize::MAX),
    });

    assert!(
        backend
            .ctx
            .aof()
            .journal
            .as_ref()
            .expect("AOF journal")
            .appender
            .current_size()
            > initial_size,
        "blocked before-poll maintenance must flush AOF"
    );
    let mut reply = [0_u8; 5];
    client.read_exact(&mut reply).expect("read pending reply");
    assert_eq!(&reply, b"reply");

    drop(backend);
    std::fs::remove_dir_all(dir).expect("remove AOF test directory");
}

#[test]
fn blocked_pump_counts_maintenance_progress_before_polling_again() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let output_listener = TcpListener::bind("127.0.0.1:0").expect("bind output peer");
    let output_addr = output_listener.local_addr().expect("output peer address");
    let output_peer = backend
        .transport
        .connect_tcp(output_addr)
        .expect("connect output transport");
    let (_output_client, _) = output_listener.accept().expect("accept output transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: output_peer.token,
        peer_addr: output_addr,
    });
    backend.writes.ordinary.begin_turn();
    assert!(backend.queue_client_reply(output_peer.token, b"reply".to_vec()));

    let closing_listener = TcpListener::bind("127.0.0.1:0").expect("bind closing peer");
    let closing_addr = closing_listener.local_addr().expect("closing peer address");
    let closing_peer = backend
        .transport
        .connect_tcp(closing_addr)
        .expect("connect closing transport");
    let (_closing_client, _) = closing_listener.accept().expect("accept closing transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: closing_peer.token,
        peer_addr: closing_addr,
    });
    backend.transport.queue_test_transport_event_batch(&[]);
    backend.transport.queue_test_transport_event_batch(&[(
        closing_peer.token,
        false,
        false,
        true,
        false,
    )]);

    let result = backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(usize::MAX),
    });

    assert!(
        result.outcomes.handled + result.outcomes.owner_closed > 0,
        "the unified dispatcher must report the closed transport event"
    );
    assert!(
        (1..=2).contains(&result.poll_rounds),
        "pending-output progress must let the blocked pump observe the close"
    );
    assert!(!backend.transport.is_registered(closing_peer.token));
}

#[test]
fn blocked_pump_processes_transport_pending_data_before_polling() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind client peer");
    let peer_addr = listener.local_addr().expect("client peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect client transport");
    let (mut client, _) = listener.accept().expect("accept client transport");
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set client read timeout");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    client
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .expect("write pending request");
    let ready = backend.transport.wait_readable_for_test(peer.token);
    assert!(ready
        .iter()
        .any(|event| event.token == peer.token && event.readable));
    backend.transport.queue_test_pending_data_event(peer.token);
    backend.transport.queue_test_transport_event_batch(&[]);
    backend.transport.queue_test_transport_event_batch(&[]);
    backend.writes.ordinary.begin_turn();

    let result = backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(usize::MAX),
    });

    assert!(
        result.outcomes.handled > 0,
        "the unified dispatcher must report the pending-data dispatch"
    );
    let mut reply = [0_u8; 7];
    client
        .read_exact(&mut reply)
        .expect("read reply produced by blocked pending-data dispatch");
    assert_eq!(&reply, b"+PONG\r\n");
}

fn run_failing_blocked_pump_child(client_address: SocketAddr, control_address: SocketAddr) {
    let mut backend = ServerWorkerState::new(1).expect("create child runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.ctx.config_mut().persistence.aof_fsync =
        crate::foundation::io::persistence::AofFsync::Always;
    let appender = crate::foundation::io::persistence::AofAppender::open_append_path(
        std::path::Path::new("/dev/null"),
        crate::foundation::io::persistence::AofFsync::Always,
    )
    .expect("open fsync-failure target");
    backend.ctx.aof_mut().journal =
        Some(crate::foundation::io::persistence::journal::AofJournal::new(appender));
    let peer = backend
        .transport
        .connect_tcp(client_address)
        .expect("connect child client");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr: client_address,
    });
    let mut control = TcpStream::connect(control_address).expect("connect child control");
    let mut start = [0_u8; 1];
    control
        .read_exact(&mut start)
        .expect("wait for parent input");
    backend.transport.wait_readable_for_test(peer.token);
    backend
        .transport
        .queue_test_transport_event_batch(&[(peer.token, true, false, false, false)]);

    backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(usize::MAX),
    });
    panic!("appendfsync always unexpectedly survived /dev/null fsync");
}

#[test]
fn blocked_pump_never_exposes_a_reply_before_a_failed_fsync() {
    const CHILD_MARKER: &str = "VERSAL_BLOCKED_PUMP_AOF_FAILURE_CHILD";
    const TEST_NAME: &str = concat!(
        "runtime::server::worker::event_loop::tests::blocked_pump_tests::",
        "blocked_pump_never_exposes_a_reply_before_a_failed_fsync"
    );
    if std::env::var_os(CHILD_MARKER).is_some() {
        let client_address = std::env::var("VERSAL_BLOCKED_PUMP_CLIENT_ADDRESS")
            .expect("child client address")
            .parse()
            .expect("valid child client address");
        let control_address = std::env::var("VERSAL_BLOCKED_PUMP_CONTROL_ADDRESS")
            .expect("child control address")
            .parse()
            .expect("valid child control address");
        run_failing_blocked_pump_child(client_address, control_address);
        return;
    }

    let client_listener = TcpListener::bind("127.0.0.1:0").expect("bind client listener");
    let control_listener = TcpListener::bind("127.0.0.1:0").expect("bind control listener");
    let child = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", TEST_NAME, "--nocapture"])
        .env(CHILD_MARKER, "1")
        .env(
            "VERSAL_BLOCKED_PUMP_CLIENT_ADDRESS",
            client_listener.local_addr().unwrap().to_string(),
        )
        .env(
            "VERSAL_BLOCKED_PUMP_CONTROL_ADDRESS",
            control_listener.local_addr().unwrap().to_string(),
        )
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn blocked-pump failure child");
    let (mut client, _) = client_listener.accept().expect("accept child client");
    let (mut control, _) = control_listener.accept().expect("accept child control");
    client
        .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
        .expect("write SET");
    control.write_all(&[1]).expect("release child");

    let child_status =
        super::wait_child_with_timeout(child, Duration::from_secs(10), "blocked-pump");
    assert_eq!(child_status.code(), Some(1));
    let mut reply = Vec::new();
    client.read_to_end(&mut reply).expect("read client EOF");
    assert!(reply.is_empty(), "reply crossed failed fsync: {reply:?}");
}
