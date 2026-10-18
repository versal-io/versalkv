use super::*;
use std::io::Read;

#[test]
fn replica_dual_link_coexist_and_classify_tokens() {
    let runtime = setup_replica_dual_link();
    assert_eq!(
        runtime.replica_link().unwrap().connection_id(),
        ConnectionId(7)
    );
    assert_eq!(
        runtime.replica_rdb_link().unwrap().connection_id(),
        ConnectionId(8)
    );
    assert_eq!(
        runtime.replica_connection_class(ConnectionId(7)),
        ReplicaConnClass::Main
    );
    assert_eq!(
        runtime.replica_connection_class(ConnectionId(8)),
        ReplicaConnClass::Rdb
    );
    assert_eq!(
        runtime.replica_connection_class(ConnectionId(99)),
        ReplicaConnClass::Unknown
    );
}

#[test]
fn replica_dual_inbound_bytes_route_isolated() {
    const MAIN_BYTES: &[u8] = b"MAIN-PAYLOAD-012345";
    const RDB_BYTES: &[u8] = b"RDB--PAYLOAD-6789AB";
    let mut runtime = setup_replica_dual_link();
    let main_conn_id = ConnectionId(7);
    let rdb_conn_id = ConnectionId(8);
    if let Some(main_link) = runtime.replica_link_mut() {
        let _ = main_link.on_bytes_with_announce_ip(MAIN_BYTES, 6379, "");
    }
    if let Some(rdb_link) = runtime.replica_rdb_link_mut() {
        rdb_link.push_bytes(RDB_BYTES);
    }
    assert_eq!(
        runtime.replica_connection_class(main_conn_id),
        ReplicaConnClass::Main
    );
    assert_eq!(
        runtime.replica_connection_class(rdb_conn_id),
        ReplicaConnClass::Rdb
    );
    let queued_main: Vec<u8> = runtime.replica_link().unwrap().input_bytes_flattened();
    let queued_rdb: Vec<u8> = runtime.replica_rdb_link().unwrap().input.clone();
    assert!(
        queued_main
            .windows(MAIN_BYTES.len())
            .any(|w| w == MAIN_BYTES),
        "main queue must contain MAIN_BYTES"
    );
    assert!(
        !queued_main.windows(RDB_BYTES.len()).any(|w| w == RDB_BYTES),
        "main queue must NOT contain RDB_BYTES"
    );
    assert!(
        queued_rdb.windows(RDB_BYTES.len()).any(|w| w == RDB_BYTES),
        "rdb queue must contain RDB_BYTES"
    );
    assert!(
        !queued_rdb
            .windows(MAIN_BYTES.len())
            .any(|w| w == MAIN_BYTES),
        "rdb queue must NOT contain MAIN_BYTES"
    );
    let main_still_exists = runtime
        .replica_link()
        .is_some_and(|l| l.connection_id() == main_conn_id);
    assert!(main_still_exists, "main link must remain independent");
}

#[test]
fn replica_close_main_keeps_rdb() {
    let mut runtime = setup_replica_dual_link();
    runtime.remove_connection(ConnectionId(7));
    assert!(runtime.replica_link().is_none());
    assert!(runtime.replica_rdb_link().is_some());
    assert_eq!(
        runtime.replica_rdb_link().unwrap().connection_id(),
        ConnectionId(8)
    );
    assert!(matches!(
        runtime.role(ConnectionId(8)),
        Some(ReplicationConnection::RdbChannel {
            main: ConnectionId(7)
        })
    ));
}

#[test]
fn replica_close_rdb_keeps_main() {
    let mut runtime = setup_replica_dual_link();
    runtime.remove_connection(ConnectionId(8));
    assert!(runtime.replica_rdb_link().is_none());
    assert!(runtime.replica_link().is_some());
    assert_eq!(
        runtime.replica_link().unwrap().connection_id(),
        ConnectionId(7)
    );
}

#[test]
fn rdb_open_reports_pairing_failure_and_rolls_back_connection() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main_connection_id = ConnectionId(41);
    let failed_rdb_connection_id = ConnectionId(42);
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
    backend.replication_runtime.register_primary(
        failed_rdb_connection_id,
        crate::framework::distributed::replication::ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );

    let error = backend
        .open_replica_rdb_channel_for_test(main_connection_id, failed_rdb_connection_id)
        .expect_err("conflicting role must reject RDB pairing");

    assert_eq!(error.stage(), ReplicaRdbOpenStage::Pair);
    assert!(backend.replication_runtime.replica_rdb_link().is_none());
    assert!(backend
        .replication_runtime
        .channel_pair_for(main_connection_id)
        .is_none());
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(
            failed_rdb_connection_id.0
        )));
    assert!(backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(main_slab)));
    assert_eq!(
        backend.replication_runtime.role(main_connection_id),
        Some(ReplicationConnection::Replica)
    );
}

#[test]
fn live_rdb_reply_then_endoff_routes_main_replconf() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind main peer");
    let primary_addr = listener.local_addr().expect("read main peer address");
    let main_peer = backend
        .transport
        .connect_tcp(primary_addr)
        .expect("connect main replica link");
    let (mut primary_stream, _) = listener.accept().expect("accept main replica link");
    let main_token = main_peer.token.0;
    let main_slab = {
        backend
            .connection_registry
            .insert(Connection::new(main_peer.token, primary_addr, false))
    };
    let rdb_listener = TcpListener::bind("127.0.0.1:0").expect("bind RDB peer");
    let rdb_addr = rdb_listener.local_addr().expect("read RDB peer address");
    let rdb_peer = backend
        .transport
        .connect_tcp(rdb_addr)
        .expect("connect RDB replica link");
    let (mut rdb_primary_stream, _) = rdb_listener.accept().expect("accept RDB replica link");
    let rdb_token = rdb_peer.token.0;
    let rdb_slab = {
        backend
            .connection_registry
            .insert(Connection::new(rdb_peer.token, rdb_addr, false))
    };

    let main_connection_id = ConnectionId(main_token);
    let rdb_connection_id = ConnectionId(rdb_token);

    let mut link = ReplicaLink::new(
        main_connection_id,
        MasterInfo {
            host: primary_addr.ip().to_string(),
            port: primary_addr.port(),
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

    let replid = "aaaabbbbccccddddeeeeffff0000111122223333";
    let endoff_bytes = protocol::encode_end_offset(offset(98765), replid, 2, 42u64);
    let mut rdb_reply_and_endoff = b"+OK\r\n".to_vec();
    rdb_reply_and_endoff.extend_from_slice(&endoff_bytes);
    rdb_primary_stream
        .write_all(&rdb_reply_and_endoff)
        .expect("send RDB REPLCONF reply and ENDOFF");
    backend.transport.wait_readable_for_test(Token(rdb_token));
    backend.handle_readable(
        rdb_token,
        crate::runtime::server::worker::sessions::ClientReadMode::Fair,
    );

    let link = backend.replication_runtime.replica_link().unwrap();
    let pp = link.provisional_primary().unwrap();
    assert_eq!(pp.replid, replid);
    assert_eq!(pp.end_offset, offset(98765));
    assert_eq!(pp.db_id, 2);
    assert_eq!(pp.rdb_client_id, 42);

    let queued_main: Vec<u8> = backend.connection_registry
        [crate::framework::capabilities::io::Token(main_slab)]
    .output
    .queue
    .iter()
    .flat_map(|c| c.iter().copied())
    .collect();
    let replconf_expected = protocol::build_replconf_set_rdb_client_id(42);
    let psync_expected = protocol::build_psync(replid, 98766);
    assert_eq!(
        queued_main, replconf_expected,
        "main channel must queue only the RDB client id REPLCONF frame"
    );
    assert_eq!(
        backend.replication_runtime.role(rdb_connection_id),
        Some(ReplicationConnection::RdbChannel {
            main: main_connection_id
        }),
        "RDB link must remain live after ENDOFF dispatch"
    );
    assert!(backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_slab)));
    assert!(backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_token)));

    let queued_rdb: Vec<u8> = backend.connection_registry
        [crate::framework::capabilities::io::Token(rdb_slab)]
    .output
    .queue
    .iter()
    .flat_map(|c| c.iter().copied())
    .collect();
    assert!(
        !queued_rdb
            .windows(replconf_expected.len())
            .any(|w| w == replconf_expected.as_slice()),
        "rdb channel must NOT contain REPLCONF frame"
    );

    primary_stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .expect("set main peer read timeout");
    backend.handle_writable(Token(main_token));
    let mut flushed_replconf = vec![0; replconf_expected.len()];
    primary_stream
        .read_exact(&mut flushed_replconf)
        .expect("main REPLCONF must be flushed before waiting for its reply");
    assert_eq!(flushed_replconf, replconf_expected);
    primary_stream
        .write_all(b"+OK\r\n")
        .expect("send main REPLCONF reply");
    backend.transport.wait_readable_for_test(Token(main_token));
    backend.handle_master_link_readable(main_token, main_slab);
    backend.handle_writable(Token(main_token));
    let mut flushed_psync = vec![0; psync_expected.len()];
    primary_stream
        .read_exact(&mut flushed_psync)
        .expect("main PSYNC must be flushed after the REPLCONF reply");
    assert_eq!(flushed_psync, psync_expected);
    let queued_main: Vec<u8> = backend.connection_registry
        [crate::framework::capabilities::io::Token(main_slab)]
    .output
    .queue
    .iter()
    .flat_map(|chunk| chunk.iter().copied())
    .collect();
    assert!(
        queued_main.is_empty(),
        "PSYNC must not remain queued after flush"
    );
}

#[test]
fn rejected_dual_rdb_client_id_reply_tears_down_pair_and_allows_immediate_reconnect() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind main peer");
    let primary_addr = listener.local_addr().expect("read main peer address");
    let main_peer = backend
        .transport
        .connect_tcp(primary_addr)
        .expect("connect main replica link");
    let (mut primary_stream, _) = listener.accept().expect("accept main replica link");
    let main_token = main_peer.token.0;
    let rdb_token = 9usize;
    let main_slab = {
        backend
            .connection_registry
            .insert(Connection::new(main_peer.token, primary_addr, false))
    };
    let rdb_slab = add_connection(&mut backend, rdb_token);
    let main_connection_id = ConnectionId(main_token);
    let rdb_connection_id = ConnectionId(rdb_token);

    let mut link = ReplicaLink::new(
        main_connection_id,
        MasterInfo {
            host: primary_addr.ip().to_string(),
            port: primary_addr.port(),
        },
    );
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    let endoff = protocol::encode_end_offset(
        offset(98765),
        "aaaabbbbccccddddeeeeffff0000111122223333",
        2,
        42u64,
    );
    link.on_rdb_bytes(b"+OK\r\n").unwrap();
    link.on_rdb_bytes(&endoff)
        .expect("ENDOFF must put main link in REPLCONF reply wait state");
    backend.replication_runtime.set_replica_link(link);
    backend
        .replication_runtime
        .set_replica_rdb_link(rdb_connection_id, main_connection_id)
        .unwrap();

    primary_stream
        .write_all(b"-ERR rejected dual RDB association\r\n")
        .expect("send rejected REPLCONF reply");
    backend.transport.wait_readable_for_test(Token(main_token));
    backend.handle_master_link_readable(main_token, main_slab);

    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(backend.replication_runtime.replica_rdb_link().is_none());
    assert_eq!(backend.replication_runtime.role(main_connection_id), None);
    assert_eq!(backend.replication_runtime.role(rdb_connection_id), None);
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(main_token)));
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_token)));
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(main_slab)));
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_slab)));

    assert!(backend
        .replication_runtime
        .master_connect_allowed(crate::foundation::io::sys::time::mstime()));
}

#[test]
fn replication_stream_rejected_dual_rdb_client_id_reply_allows_immediate_reconnect() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind main peer");
    let primary_addr = listener.local_addr().expect("read main peer address");
    let main_peer = backend
        .transport
        .connect_tcp(primary_addr)
        .expect("connect main replica link");
    let (mut primary_stream, _) = listener.accept().expect("accept main replica link");
    let main_token = main_peer.token.0;
    let rdb_token = 10usize;
    let main_slab = {
        backend
            .connection_registry
            .insert(Connection::new(main_peer.token, primary_addr, false))
    };
    let rdb_slab = add_connection(&mut backend, rdb_token);
    let main_connection_id = ConnectionId(main_token);
    let rdb_connection_id = ConnectionId(rdb_token);

    let mut link = ReplicaLink::new(
        main_connection_id,
        MasterInfo {
            host: primary_addr.ip().to_string(),
            port: primary_addr.port(),
        },
    );
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
        .unwrap();
    link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
        .unwrap();
    let endoff = protocol::encode_end_offset(
        offset(98765),
        "aaaabbbbccccddddeeeeffff0000111122223333",
        2,
        42u64,
    );
    link.on_rdb_bytes(b"+OK\r\n").unwrap();
    link.on_rdb_bytes(&endoff)
        .expect("ENDOFF must put main link in REPLCONF reply wait state");
    backend.replication_runtime.set_replica_link(link);
    backend
        .replication_runtime
        .set_replica_rdb_link(rdb_connection_id, main_connection_id)
        .unwrap();

    primary_stream
        .write_all(b"-ERR rejected dual RDB association\r\n")
        .expect("send rejected REPLCONF reply");
    backend.transport.wait_readable_for_test(Token(main_token));
    backend.recv_replication_stream(main_token, main_slab);

    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(backend.replication_runtime.replica_rdb_link().is_none());
    assert_eq!(backend.replication_runtime.role(main_connection_id), None);
    assert_eq!(backend.replication_runtime.role(rdb_connection_id), None);
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(main_token)));
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_token)));
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(main_slab)));
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_slab)));

    assert!(backend
        .replication_runtime
        .master_connect_allowed(crate::foundation::io::sys::time::mstime()));
}
