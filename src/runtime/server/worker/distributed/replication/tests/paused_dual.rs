use super::*;
use crate::framework::capabilities::connection::Connection;
use std::io::Write;
use std::net::TcpListener;

#[test]
fn paused_empty_main_stream_finalizes_loaded_dual_handoff_once() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().replication.master_link_status = "sync".to_string();

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
    let rdb_token = 402usize;
    let rdb_slab = add_connection(&mut backend, rdb_token);
    let snapshot_end_offset = 200;

    let mut link = ReplicaLink::streaming(
        ConnectionId(main_token),
        MasterInfo {
            host: primary_addr.ip().to_string(),
            port: primary_addr.port(),
        },
        "abcdef0123456789abcdef0123456789abcdef01".to_string(),
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
    link.transition_rdb_channel_state(ReplicaRdbChannelState::RdbLoaded)
        .unwrap();
    link.provisional_primary = Some(ProvisionalPrimary {
        replid: "abcdef0123456789abcdef0123456789abcdef01".to_string(),
        end_offset: offset(snapshot_end_offset),
        db_id: 0,
        rdb_client_id: rdb_token as u64,
    });
    backend.replication_runtime.set_replica_link(link);
    backend
        .replication_runtime
        .set_replica_rdb_link(ConnectionId(rdb_token), ConnectionId(main_token))
        .unwrap();
    backend.ctx.client_pause_mut().action = crate::framework::context::PauseAction::Write;
    backend.ctx.client_pause_mut().end_time_ms = i64::MAX;
    backend.ctx.client_pause_mut().reason = "client_pause";

    primary_stream
        .write_all(b"*1\r\n")
        .expect("send incomplete replication frame");
    backend.transport.wait_readable_for_test(Token(main_token));
    backend.recv_replication_stream(main_token, main_slab);

    assert!(backend.pending_replication_replay.is_some());
    assert!(backend.replication_runtime.replica_rdb_link().is_some());

    // The direct readiness helper only schedules handoff replay. Mirror the
    // outer event-loop continuation before asserting the terminal state.
    backend.drive_one_pending_replication_replay();

    assert!(backend.replication_runtime.replica_rdb_link().is_none());
    assert_eq!(
        backend
            .replication_runtime
            .channel_pair_for(ConnectionId(main_token)),
        None
    );
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_slab)));
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_token)));
    assert_eq!(
        backend
            .replication_runtime
            .replica_link()
            .expect("main link remains streaming")
            .rdb_channel_state(),
        ReplicaRdbChannelState::None
    );
    assert_eq!(backend.ctx.config().replication.master_link_status, "up");

    let expected_ack = crate::engine::distributed::replication::protocol::build_replconf_ack(
        offset(snapshot_end_offset),
        None,
    );
    let queued_main: Vec<u8> = backend.connection_registry
        [crate::framework::capabilities::io::Token(main_slab)]
    .output
    .queue
    .iter()
    .flat_map(|chunk| chunk.iter().copied())
    .collect();
    assert_eq!(queued_main, expected_ack);

    backend.ctx.client_pause_mut().action = crate::framework::context::PauseAction::None;
    backend.ctx.client_pause_mut().end_time_ms = 0;
    backend.ctx.client_pause_mut().reason = "none";
    backend.apply_deferred_replica_events();

    let queued_after_unpause: Vec<u8> = backend.connection_registry
        [crate::framework::capabilities::io::Token(main_slab)]
    .output
    .queue
    .iter()
    .flat_map(|chunk| chunk.iter().copied())
    .collect();
    assert_eq!(queued_after_unpause, expected_ack);
}

#[test]
fn unpausing_nonempty_deferred_dual_events_replays_before_handoff() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let main_token = 403usize;
    let rdb_token = 404usize;
    let main_slab = add_connection(&mut backend, main_token);
    let rdb_slab = add_connection(&mut backend, rdb_token);
    let snapshot_end_offset = 200;

    let mut link = ReplicaLink::streaming(
        ConnectionId(main_token),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "abcdef0123456789abcdef0123456789abcdef01".to_string(),
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
    link.transition_rdb_channel_state(ReplicaRdbChannelState::RdbLoaded)
        .unwrap();
    link.provisional_primary = Some(ProvisionalPrimary {
        replid: "abcdef0123456789abcdef0123456789abcdef01".to_string(),
        end_offset: offset(snapshot_end_offset),
        db_id: 0,
        rdb_client_id: rdb_token as u64,
    });
    let (events, _actions) = link
        .on_bytes(
            b"*3\r\n$3\r\nSET\r\n$12\r\npaused-order\r\n$5\r\nfirst\r\n*3\r\n$3\r\nSET\r\n$12\r\npaused-order\r\n$6\r\nsecond\r\n",
            6379,
        )
        .expect("decode deferred replica commands");
    link.defer_events(events);
    backend.replication_runtime.set_replica_link(link);
    backend
        .replication_runtime
        .set_replica_rdb_link(ConnectionId(rdb_token), ConnectionId(main_token))
        .unwrap();
    backend.ctx.client_pause_mut().action = crate::framework::context::PauseAction::Write;
    backend.ctx.client_pause_mut().end_time_ms = i64::MAX;
    backend.ctx.client_pause_mut().reason = "client_pause";

    backend.apply_deferred_replica_events();
    assert!(backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"paused-order")
        .is_none());
    assert!(backend.replication_runtime.replica_rdb_link().is_some());

    backend.ctx.client_pause_mut().action = crate::framework::context::PauseAction::None;
    backend.ctx.client_pause_mut().end_time_ms = 0;
    backend.ctx.client_pause_mut().reason = "none";
    backend.apply_deferred_replica_events();

    assert_eq!(
        backend
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"paused-order")
            .map(|value| value.as_bytes()),
        Some(&b"second"[..])
    );
    assert!(backend.replication_runtime.replica_rdb_link().is_none());
    assert_eq!(
        backend
            .replication_runtime
            .channel_pair_for(ConnectionId(main_token)),
        None
    );
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_slab)));
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
            backend.ctx.replication().slave_repl_offset,
            None
        )
    );
}

#[test]
fn rdb_first_streaming_main_with_no_events_runs_dual_join() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.config_mut().replication.master_link_status = "sync".to_string();
    let main_token = 405usize;
    let rdb_token = 406usize;
    let main_slab = add_connection(&mut backend, main_token);
    let rdb_slab = add_connection(&mut backend, rdb_token);
    let snapshot_end_offset = 300;

    let mut link = ReplicaLink::streaming(
        ConnectionId(main_token),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "abcdef0123456789abcdef0123456789abcdef01".to_string(),
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
    link.transition_rdb_channel_state(ReplicaRdbChannelState::RdbLoaded)
        .unwrap();
    link.provisional_primary = Some(ProvisionalPrimary {
        replid: "abcdef0123456789abcdef0123456789abcdef01".to_string(),
        end_offset: offset(snapshot_end_offset),
        db_id: 0,
        rdb_client_id: rdb_token as u64,
    });
    backend.replication_runtime.set_replica_link(link);
    backend
        .replication_runtime
        .set_replica_rdb_link(ConnectionId(rdb_token), ConnectionId(main_token))
        .unwrap();

    backend.apply_deferred_replica_events();

    assert!(backend.replication_runtime.replica_rdb_link().is_none());
    assert_eq!(
        backend
            .replication_runtime
            .channel_pair_for(ConnectionId(main_token)),
        None
    );
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(rdb_slab)));
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
            offset(snapshot_end_offset),
            None
        )
    );
}
