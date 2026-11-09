use super::*;

use super::super::rdb_open::ReplicaRdbOpenStage;
use crate::framework::capabilities::connection::Connection;
use crate::framework::distributed::replication::{ReplicaConnClass, ReplicationConnection};
use std::io::{ErrorKind, Write};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

const ACCEPT_RETRY_COUNT: usize = 50;

fn accept_with_bounded_retry(listener: &TcpListener, peer_name: &str) -> std::net::TcpStream {
    for _ in 0..ACCEPT_RETRY_COUNT {
        match listener.accept() {
            Ok((stream, _)) => return stream,
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("failed to accept {peer_name}: {error}"),
        }
    }
    panic!("timed out accepting {peer_name} after {ACCEPT_RETRY_COUNT} retries");
}

fn setup_dual_blocked_main_receiver(
    backend: &mut ServerWorkerState,
) -> (usize, usize, usize, std::net::TcpStream) {
    let (main_token, main_slab, rdb_token, primary_stream, rdb_primary_stream) =
        setup_dual_blocked_main_receiver_with_rdb_peer(backend);
    drop(rdb_primary_stream);
    (main_token, main_slab, rdb_token, primary_stream)
}

fn setup_dual_blocked_main_receiver_with_rdb_peer(
    backend: &mut ServerWorkerState,
) -> (
    usize,
    usize,
    usize,
    std::net::TcpStream,
    std::net::TcpStream,
) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind main peer");
    let peer_addr = listener.local_addr().expect("main peer address");
    let main_peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect main replica link");
    let (primary_stream, _) = listener.accept().expect("accept main replica link");
    let main_token = main_peer.token.0;
    backend.add_connection(
        crate::runtime::server::worker::event_loop::transport::RuntimeConnectedPeer {
            token: main_peer.token,
            peer_addr,
        },
    );
    let main_slab = main_peer.token.0;

    let rdb_listener = TcpListener::bind("127.0.0.1:0").expect("bind rdb peer");
    let rdb_addr = rdb_listener.local_addr().expect("rdb peer address");
    let rdb_peer = backend
        .transport
        .connect_tcp(rdb_addr)
        .expect("connect rdb replica link");
    let (rdb_primary_stream, _) = rdb_listener.accept().expect("accept rdb replica link");
    let rdb_token = rdb_peer.token.0;
    backend.add_connection(
        crate::runtime::server::worker::event_loop::transport::RuntimeConnectedPeer {
            token: rdb_peer.token,
            peer_addr: rdb_addr,
        },
    );

    let snapshot_end_offset = 100;
    let selected_db = 0;
    let replid = "0123456789abcdef0123456789abcdef01234567".to_string();
    let mut link = ReplicaLink::new(
        ConnectionId(main_token),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.set_resume_position(replid.clone(), offset(snapshot_end_offset), selected_db);
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6379)
        .expect("main link reaches PSYNC reply wait");
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    link.on_rdb_bytes(b"+OK\r\n").unwrap();
    link.on_rdb_bytes(&protocol::encode_end_offset(
        offset(snapshot_end_offset),
        &replid,
        selected_db,
        rdb_token as u64,
    ))
    .unwrap();
    link.on_rdb_bytes(b"$EOF:0123456789abcdef0123456789abcdef01234567\r\n")
        .expect("dual RDB receive begins");
    link.on_bytes(b"+OK\r\n+CONTINUE\r\n", 6379)
        .expect("main link reaches streaming");
    assert!(link.is_streaming());
    assert!(link.has_pending_rdb_receive());
    assert!(link.is_dual_rdb_mode_active());

    backend.replication_runtime.set_replica_link(link);
    backend
        .replication_runtime
        .set_replica_rdb_link(ConnectionId(rdb_token), ConnectionId(main_token))
        .unwrap();

    (
        main_token,
        main_slab,
        rdb_token,
        primary_stream,
        rdb_primary_stream,
    )
}

fn setup_replica_dual_link() -> crate::framework::distributed::replication::ReplicationRuntime {
    let mut runtime = crate::framework::distributed::replication::ReplicationRuntime::new();
    let main_connection_id = ConnectionId(7);
    let rdb_connection_id = ConnectionId(8);
    runtime.set_replica_link(ReplicaLink::new(
        main_connection_id,
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    ));
    runtime
        .set_replica_rdb_link(rdb_connection_id, main_connection_id)
        .unwrap();
    runtime
}

use crate::framework::distributed::replication::{RdbCommit, RdbReceive};

fn assert_dual_rdb_channel_opens_after_dispatch(
    mut receive: impl FnMut(&mut ServerWorkerState, usize, usize),
) {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind successful RDB peer");
    let primary_addr = listener
        .local_addr()
        .expect("read successful RDB peer address");
    let main_peer = backend
        .transport
        .connect_tcp(primary_addr)
        .expect("connect main replica link");
    let (mut primary_stream, _) = listener.accept().expect("accept main replica link");
    listener
        .set_nonblocking(true)
        .expect("set RDB listener nonblocking");

    let main_connection_id = ConnectionId(main_peer.token.0);
    backend.add_connection(
        crate::runtime::server::worker::event_loop::transport::RuntimeConnectedPeer {
            token: main_peer.token,
            peer_addr: primary_addr,
        },
    );
    let main_slab = main_peer.token.0;
    let mut link = ReplicaLink::new(
        main_connection_id,
        MasterInfo {
            host: primary_addr.ip().to_string(),
            port: primary_addr.port(),
        },
    );
    let _ = link.connected(None, None);
    link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", primary_addr.port())
        .expect("advance main link to PSYNC reply");
    backend.replication_runtime.set_replica_link(link);

    primary_stream
        .write_all(b"+DUALCHANNELSYNC\r\n")
        .expect("send dual-channel sync reply");
    backend.transport.wait_readable_for_test(main_peer.token);
    receive(&mut backend, main_peer.token.0, main_slab);
    assert!(
        backend.replication_runtime.replica_rdb_link().is_some(),
        "RDB link must open from decoded action within bounded poll window"
    );

    let rdb_connection_id = backend
        .replication_runtime
        .replica_rdb_link()
        .expect("RDB link must open from decoded action")
        .connection_id();
    let _rdb_peer = accept_with_bounded_retry(&listener, "RDB peer");
    let link = backend
        .replication_runtime
        .replica_link()
        .expect("main link must be restored");
    assert_eq!(link.rdb_channel_state(), ReplicaRdbChannelState::Handshake);
    assert_eq!(
        backend
            .replication_runtime
            .replica_connection_class(main_connection_id),
        ReplicaConnClass::Main
    );
    assert_eq!(
        backend
            .replication_runtime
            .replica_connection_class(rdb_connection_id),
        ReplicaConnClass::Rdb
    );
    assert_eq!(
        backend
            .replication_runtime
            .channel_pair_for(main_connection_id),
        Some(rdb_connection_id)
    );
    assert!(backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(main_slab)));
}

fn setup_live_dual_rdb_receiver(
    backend: &mut ServerWorkerState,
) -> (usize, usize, usize, usize, std::net::TcpStream) {
    let main_token = 701usize;
    let main_slab = add_connection(backend, main_token);
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind RDB peer");
    let rdb_addr = listener.local_addr().expect("read RDB peer address");
    let rdb_peer = backend
        .transport
        .connect_tcp(rdb_addr)
        .expect("connect RDB replica link");
    let (primary_stream, _) = listener.accept().expect("accept RDB replica link");
    let rdb_token = rdb_peer.token.0;
    backend.add_connection(
        crate::runtime::server::worker::event_loop::transport::RuntimeConnectedPeer {
            token: rdb_peer.token,
            peer_addr: rdb_addr,
        },
    );
    let rdb_slab = rdb_peer.token.0;
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

    (main_token, main_slab, rdb_token, rdb_slab, primary_stream)
}

fn minimal_valid_rdb_body() -> &'static [u8] {
    // RDB 80: string key "k" -> value "v", EOF, then a disabled checksum.
    b"VALKEY080\x00\x01k\x01v\xff\x00\x00\x00\x00\x00\x00\x00\x00"
}

fn register_transport_for_slab(backend: &mut ServerWorkerState, connection_token: usize) -> usize {
    use crate::foundation::io::transport::TransportId;
    let raw_tid = connection_token.saturating_add(100000);
    let tid = TransportId::new(raw_tid).unwrap_or_else(|| TransportId::new(1).unwrap());
    let registered = backend.transport.register_test_transport(tid);
    let token_value = backend
        .connection_registry
        .replace_token_for_test(connection_token, registered);
    backend.install_test_connection_source(registered);
    token_value
}

fn register_live_transport_for_slab(
    backend: &mut ServerWorkerState,
    connection_token: usize,
) -> (std::net::TcpStream, usize) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind live test transport");
    let peer_addr = listener.local_addr().expect("live test transport address");
    let connected = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect live test transport");
    let (peer_stream, _) = listener.accept().expect("accept live test transport");
    let token_value = backend
        .connection_registry
        .replace_token_for_test(connection_token, connected.token);
    backend.install_test_connection_source(connected.token);
    (peer_stream, token_value)
}

mod handoff;
mod handshake;
mod rdb_receive;
