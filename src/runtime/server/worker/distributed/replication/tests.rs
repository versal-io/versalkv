use super::{resolve_master_addr, ServerWorkerState};
use crate::engine::command::CommandArgs;
use crate::engine::distributed::cluster::node::{
    ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PFAIL,
};
use crate::engine::distributed::replication::{protocol, ReplicationOffset};
use crate::framework::capabilities::connection::Connection;
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::{
    CommandOrigin, ConnectionId, MasterInfo, PendingEntry, PrimaryDualChannelState, PrimaryLink,
    PrimaryLinkState, PrimarySyncPlan, ProvisionalPrimary, ReplicaConnectionInfo, ReplicaEvent,
    ReplicaLink, ReplicaRdbChannelState, ReplicationApply,
};
use crate::runtime::server::worker::reactor::blocked_events::BlockedOperation;
use bytes::Bytes;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

mod codec_fix_tests;
mod rdb_cohort;

fn offset(value: i64) -> ReplicationOffset {
    ReplicationOffset::try_from(value).expect("valid test offset")
}

fn _assert_command_transients_cleared(conn: &Connection) {
    assert!(!conn.skip_propagation());
    assert!(conn.custom_propagation().is_empty());
    assert!(conn.script_propagation().is_empty());
    assert!(conn.dirty_keys().is_empty());
    assert!(conn.pending_notifications().is_empty());
    assert!(conn.pending_shard_notifications().is_empty());
    assert!(conn.swapdb_intent().is_none());
}

fn _queued(conn: &Connection) -> Vec<u8> {
    conn.output
        .queue
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect()
}

fn send_endoff_for_test(backend: &mut ServerWorkerState, rdb: ConnectionId) {
    backend.admit_rdb_sync_for_test(rdb);
    let frame = backend
        .replication_runtime
        .take_endoff_for_rdb_candidate_if_unambiguous(
            rdb,
            backend.connection_registry.contains(Token(rdb.0)),
            &mut backend.ctx.replication_mut().backlog,
        );
    backend.connection_registry[Token(rdb.0)]
        .queue_reply(frame.expect("admitted RDB candidate must receive ENDOFF"));
}

impl ServerWorkerState {
    pub(crate) fn admit_rdb_sync_for_test(&mut self, rdb: ConnectionId) {
        assert_eq!(
            self.replication_runtime.role(rdb),
            Some(crate::framework::distributed::replication::ReplicationConnection::RdbChannelCandidate)
        );
        assert!(!self
            .full_sync
            .pending_dual_rdb_snapshots
            .contains_key(&Token(rdb.0)));
        let mut replies = Vec::new();
        let mut unblocks = Vec::new();
        let capa = CommandArgs::from_parts(vec![
            Bytes::from_static(b"REPLCONF"),
            Bytes::from_static(b"capa"),
            Bytes::from_static(b"eof"),
        ]);
        assert_eq!(
            self.try_handle_transport_cmd(
                rdb.0,
                rdb.0,
                &capa,
                "replconf",
                false,
                &mut replies,
                &mut unblocks
            ),
            Some(false)
        );
        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        replies.clear();
        let sync = CommandArgs::from_parts(vec![Bytes::from_static(b"SYNC")]);
        assert_eq!(
            self.try_handle_transport_cmd(
                rdb.0,
                rdb.0,
                &sync,
                "sync",
                false,
                &mut replies,
                &mut unblocks
            ),
            Some(false)
        );
        assert!(
            replies.is_empty(),
            "candidate SYNC must not send FULLRESYNC: {replies:?}"
        );
        assert!(self
            .full_sync
            .pending_dual_rdb_snapshots
            .contains_key(&Token(rdb.0)));
    }

    pub(crate) fn associate_waiting_rdb_for_test(&mut self, main: ConnectionId, rdb: ConnectionId) {
        let link = self
            .replication_runtime
            .primary_for_connection_mut(main)
            .unwrap();
        if link.dual_state() == PrimaryDualChannelState::Disabled {
            link.transition_to_waiting_for_rdb_channel().unwrap();
        }
        self.replication_runtime
            .register_rdb_candidate(rdb)
            .unwrap();
        assert!(self
            .replication_runtime
            .associate_rdb_channel(main, rdb)
            .is_err());
        assert_eq!(self.replication_runtime.channel_pair_for(main), None);
        send_endoff_for_test(self, rdb);
        self.replication_runtime
            .associate_rdb_channel(main, rdb)
            .unwrap();
    }
}

#[test]
fn blocked_service_caps_nonblocking_polls_at_four_rounds() {
    let mut backend = ServerWorkerState::new_with_slot_router(1, 14, |key| {
        crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
    })
    .unwrap();
    let main_token = 41usize;
    add_connection(&mut backend, main_token);
    backend
        .replication_runtime
        .set_replica_link(ReplicaLink::new(
            ConnectionId(main_token),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        ));

    for _ in 0..5 {
        backend.transport.queue_test_transport_event_batch(&[(
            Token(main_token),
            true,
            false,
            false,
            false,
        )]);
    }

    let result = backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(main_token),
    });

    assert_eq!(result.poll_rounds, 4);
    assert_eq!(backend.blocked_events.depth(), 0);
}

#[test]
fn blocked_service_stops_after_a_round_with_no_useful_work() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = ConnectionId(41);
    add_connection(&mut backend, main.0);
    backend
        .replication_runtime
        .set_replica_link(ReplicaLink::new(
            main,
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        ));

    backend.transport.queue_test_transport_event_batch(&[(
        Token(main.0),
        true,
        false,
        false,
        false,
    )]);
    backend.transport.queue_test_transport_event_batch(&[(
        Token(usize::MAX - 1),
        true,
        true,
        false,
        false,
    )]);
    backend.transport.queue_test_transport_event_batch(&[(
        Token(main.0),
        true,
        false,
        true,
        false,
    )]);

    let result = backend.process_events_while_blocked(BlockedOperation::ClassicRdb { main });

    assert_eq!(result.poll_rounds, 2);
    assert_eq!(result.dispatched_events, 0);
    assert_eq!(result.deferred_events, 1);
    assert_eq!(backend.blocked_events.depth(), 0);
    let pending = backend
        .transport
        .poll_events(Some(std::time::Duration::ZERO))
        .expect("poll the unconsumed useful batch");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].token, Token(main.0));
    assert!(pending[0].readable);
    assert!(pending[0].read_hup);
}

#[test]
fn blocked_service_coalesces_protected_readiness_without_dropping_hup() {
    let mut backend = ServerWorkerState::new_with_slot_router(1, 14, |key| {
        crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
    })
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind replica-main listener");
    let peer_addr = listener.local_addr().expect("get replica-main address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect replica main");
    let (_primary, _) = listener.accept().expect("accept replica main");
    backend.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr,
        },
    );
    let main_token = peer.token.0;
    backend
        .replication_runtime
        .set_replica_link(ReplicaLink::new(
            ConnectionId(main_token),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        ));

    backend.transport.queue_test_transport_event_batch(&[(
        Token(main_token),
        true,
        false,
        false,
        false,
    )]);
    backend.transport.queue_test_transport_event_batch(&[(
        Token(main_token),
        false,
        false,
        true,
        false,
    )]);

    let result = backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(main_token),
    });
    let event = backend
        .blocked_events
        .pop_front_deferred()
        .expect("protected readiness must be deferred");

    assert_eq!(result.deferred_events, 2);
    assert!(event.readable);
    assert!(event.read_hup);
    assert!(!event.writable);
    assert!(backend.transport.is_registered(Token(main_token)));
    assert!(backend.replication_runtime.replica_link().is_some());
}

#[test]
fn blocked_service_error_invalidates_an_older_deferred_read() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind replica-main listener");
    let peer_addr = listener.local_addr().expect("get replica-main address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect replica main");
    let (_primary, _) = listener.accept().expect("accept replica main");
    backend.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr,
        },
    );
    let main = ConnectionId(peer.token.0);
    backend
        .replication_runtime
        .set_replica_link(ReplicaLink::new(
            main,
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        ));
    backend
        .transport
        .queue_test_transport_event_batch(&[(peer.token, true, false, false, false)]);
    backend
        .transport
        .queue_test_transport_event_batch(&[(peer.token, false, false, false, true)]);

    let result = backend.process_events_while_blocked(BlockedOperation::ClassicRdb { main });

    assert_eq!(result.deferred_events, 1);
    assert_eq!(result.dispatched_events, 1);
    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(!backend.transport.is_registered(peer.token));
    backend.replay_one_deferred_readiness();
    assert!(backend.blocked_events.pop_front_deferred().is_none());
}

#[test]
fn nested_scopes_protect_the_union_of_tokens() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let outer_main = ConnectionId(41);
    let inner_main = ConnectionId(42);

    backend
        .blocked_events
        .push_scope(BlockedOperation::ClassicRdb { main: outer_main });
    backend
        .blocked_events
        .push_scope(BlockedOperation::PendingReplay { main: inner_main });

    assert!(backend
        .blocked_events
        .is_read_protected(Token(outer_main.0)));
    assert!(backend
        .blocked_events
        .is_read_protected(Token(inner_main.0)));

    backend.blocked_events.pop_scope();
    backend.blocked_events.pop_scope();
}

#[test]
fn blocked_service_uses_existing_loading_command_gate() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_loading(true);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer_addr = listener.local_addr().unwrap();
    let peer = backend.transport.connect_tcp(peer_addr).unwrap();
    let (mut client, _) = listener.accept().unwrap();
    backend.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr,
        },
    );
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .unwrap();
    client
        .write_all(b"*1\r\n$4\r\nINFO\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
        .unwrap();
    backend.transport.wait_pending_bytes_for_test(
        peer.token,
        b"*1\r\n$4\r\nINFO\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n".len(),
    );
    backend
        .transport
        .queue_test_transport_event_batch(&[(peer.token, true, false, false, false)]);

    let result = backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(usize::MAX),
    });

    assert_eq!(result.dispatched_events, 1);
    let mut reply = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        match client.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                reply.extend_from_slice(&chunk[..n]);
                let reply_text = String::from_utf8_lossy(&reply);
                if reply_text.contains("loading:1")
                    && reply_text.contains("LOADING Valkey is loading the dataset in memory")
                {
                    break;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                break;
            }
            Err(error) => panic!("read loading-gate replies: {error}"),
        }
    }
    let reply = String::from_utf8_lossy(&reply);
    assert!(
        reply.contains("loading:1"),
        "INFO must run during loading: {reply}"
    );
    assert!(
        reply.contains("LOADING Valkey is loading the dataset in memory"),
        "GET must be rejected by the existing loading gate: {reply}"
    );
}

#[test]
fn master_hostname_resolves_to_ipv4_when_available() {
    let addr = resolve_master_addr("localhost", 6379).expect("localhost must resolve");
    assert!(addr.ip().is_loopback());
    assert!(addr.is_ipv4());
    assert_eq!(addr.port(), 6379);
}

#[test]
fn refresh_cluster_replication_view_retains_disconnected_offsets_and_relationships() {
    use crate::engine::distributed::cluster::node::{
        CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
    };
    use crate::engine::distributed::cluster::state::ClusterReplicaHealth;
    use crate::engine::distributed::replication::ReplicationRole;

    const PRIMARY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const REMOTE_REPLICA: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut backend = ServerWorkerState::new_with_slot_router(1, 14, |key| {
        crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
    })
    .unwrap();
    let mut state =
        crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6380, 16380);
    let local_id = state.myself.name.clone();
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = PRIMARY.to_string();
    state.myself.replica_priority = 90;
    let mut primary = ClusterNode::new(
        PRIMARY.to_string(),
        "127.0.0.1".to_string(),
        6379,
        16379,
        true,
    );
    primary.flags &= !CLUSTER_NODE_MYSELF;
    let mut remote = ClusterNode::new(
        REMOTE_REPLICA.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16381,
        false,
    );
    remote.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
    remote.flags |= CLUSTER_NODE_REPLICA;
    remote.replicaof = PRIMARY.to_string();
    remote.repl_offset = 41;
    remote.replica_priority = 80;
    state.nodes.insert(PRIMARY.to_string(), primary);
    state.nodes.insert(REMOTE_REPLICA.to_string(), remote);
    *backend.ctx.cluster_state_mut() = Some(Box::new(state));
    backend.ctx.config_mut().replication.replica_priority = 90;
    backend.ctx.replication_mut().role = ReplicationRole::Replica;
    backend.ctx.replication_mut().slave_read_repl_offset = offset(43);
    backend.ctx.replication_mut().slave_repl_offset = offset(42);

    backend.refresh_replication_view();

    let views = &backend.ctx.replication().view.cluster_replica_views;
    assert_eq!(views[&local_id].health, ClusterReplicaHealth::Disconnected);
    assert_eq!(
        views[&local_id].upstream.as_ref().unwrap().as_str(),
        PRIMARY
    );
    assert_eq!(views[&local_id].received_offset, offset(43));
    assert_eq!(views[&local_id].applied_offset, offset(42));
    assert_eq!(views[&local_id].acknowledged_offset, offset(42));
    assert_eq!(views[&local_id].priority, 90);
    assert!(!views.contains_key(REMOTE_REPLICA));
}

#[test]
fn replication_writes_do_not_restart_idle_backlog_ttl() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().replication.repl_backlog_ttl = 1;
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"history")
        .unwrap();
    backend.expire_idle_replication_backlog();
    let deadline = backend.ctx.replication().backlog_idle_deadline_ms;
    assert!(deadline > 0);

    backend.propagate_to_replicas(b"*1\r\n$4\r\nPING\r\n");

    assert_eq!(
        backend.ctx.replication().backlog_idle_deadline_ms,
        deadline,
        "ongoing writes must not extend the backlog TTL once no replicas remain"
    );
}

#[test]
fn unknown_replicated_command_is_ignored_when_configured() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.replication_mut().backlog.activate();
    crate::framework::commands::register_all(&mut backend.registry);
    backend
        .ctx
        .config_mut()
        .replication
        .propagation_error_behavior = "ignore".to_string();
    let connection_token = add_connection(&mut backend, 41);
    let mut link = ReplicaLink::new(
        ConnectionId(41),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let args = CommandArgs::from_parts(vec![Bytes::from_static(b"fake-command-1")]);
    let before = backend.ctx.replication().backlog.next_stream_cursor();

    backend
        .apply_replication_command(
            connection_token,
            &mut link,
            ReplicationApply::Command(args),
            CommandOrigin::Replication,
        )
        .expect("ignored unknown command should not tear down replication");

    let frame = protocol::encode_command(&[b"fake-command-1"]);
    assert_eq!(
        backend.ctx.replication().backlog.read_from(before),
        Some(frame)
    );
}

#[test]
fn propagation_does_not_rebuild_cached_replication_view() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 43;
    add_connection(&mut backend, token_value);
    backend.replication_runtime.register_primary(
        ConnectionId(token_value),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["dual-channel".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend.ctx.replication_mut().view.replica_order = vec![usize::MAX];

    backend.propagate_replication_frame(b"*1\r\n$4\r\nPING\r\n");

    assert_eq!(
        backend.ctx.replication().view.replica_order,
        vec![usize::MAX],
        "the per-frame propagation path must not rebuild INFO replication state"
    );
}

#[test]
fn replicaof_no_one_tears_down_dual_session_before_becoming_primary() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main_token = 41;
    let rdb_token = 42;
    let main_slab = add_connection(&mut backend, main_token);
    let rdb_slab = add_connection(&mut backend, rdb_token);
    let main_connection_id = ConnectionId(main_token);

    backend.ctx.config_mut().replication.master_host = Some("127.0.0.1".to_string());
    backend.ctx.config_mut().replication.master_port = 6379;
    backend.ctx.config_mut().replication.master_link_status = "sync".to_string();
    backend.ctx.set_loading(true);
    backend.ctx.replication_mut().async_loading = true;
    backend.ctx.replication_mut().pending_replicaof_request = true;
    backend
        .replication_runtime
        .set_replica_link(ReplicaLink::new(
            main_connection_id,
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        ));
    backend
        .replication_runtime
        .set_replica_rdb_link(ConnectionId(rdb_token), main_connection_id)
        .expect("pair the RDB connection with the replica main connection");

    backend.apply_pending_replicaof_request();

    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(backend.replication_runtime.replica_rdb_link().is_none());
    assert_eq!(backend.replication_runtime.role(main_connection_id), None);
    assert_eq!(
        backend.replication_runtime.role(ConnectionId(rdb_token)),
        None
    );
    assert!(!backend.connection_registry.contains(Token(main_slab)));
    assert!(!backend.connection_registry.contains(Token(rdb_slab)));
    assert!(!backend.connection_registry.contains(Token(main_token)));
    assert!(!backend.connection_registry.contains(Token(rdb_token)));
    assert!(!backend.ctx.is_loading());
    assert!(!backend.ctx.replication().async_loading);
    assert_eq!(backend.ctx.config().replication.master_link_status, "down");
}

#[test]
fn stalled_receiving_rdb_with_cluster_pfail_disconnects_master_link_and_clears_async_loading() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().replication.master_host = Some("127.0.0.1".to_string());
    backend.ctx.config_mut().replication.master_port = 6379;
    backend.ctx.config_mut().replication.master_link_status = "sync".to_string();
    backend.ctx.replication_mut().async_loading = true;
    backend.ctx.config_mut().server.cluster_node_timeout = 1_000;

    let mut cluster_state =
        crate::engine::distributed::cluster::state::ClusterState::new("127.0.0.1", 6380, 16380);
    cluster_state.myself.flags &= !CLUSTER_NODE_MYSELF;
    cluster_state.myself.flags |= CLUSTER_NODE_MYSELF;
    cluster_state.myself.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_PRIMARY;
    cluster_state.myself.flags |= crate::engine::distributed::cluster::node::CLUSTER_NODE_REPLICA;
    cluster_state.myself.replicaof = "primary".to_string();

    let mut primary = ClusterNode::new(
        "primary".to_string(),
        "127.0.0.1".to_string(),
        6379,
        16379,
        true,
    );
    primary.flags &= !CLUSTER_NODE_MYSELF;
    primary.flags |= CLUSTER_NODE_PFAIL;
    cluster_state.nodes.insert(primary.name.clone(), primary);
    *backend.ctx.cluster_state_mut() = Some(Box::new(cluster_state));

    let token_value = 41;
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(Token(token_value));
    let _connection_token = backend.connection_registry.insert(connection);

    let mut link = ReplicaLink::new(
        ConnectionId(token_value),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let _ = link.connected(None, None);
    let psync = b"+PONG\r\n+OK\r\n+OK\r\n+FULLRESYNC 0123456789012345678901234567890123456789 0\r\n$5\r\nhe";
    let _ = link.on_bytes(psync, 6380).expect("handshake should decode");
    assert!(link.is_receiving_rdb());
    link.set_last_io_time_ms(crate::foundation::io::sys::time::mstime() - 2_000);
    backend.replication_runtime.set_replica_link(link);

    backend.drive_replication();

    assert!(!backend.ctx.replication().async_loading);
    assert_eq!(backend.ctx.config().replication.master_link_status, "down");
    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(!backend.connection_registry.contains(Token(token_value)));
    assert!(
        !backend
            .replication_runtime
            .master_connect_allowed(crate::foundation::io::sys::time::mstime()),
        "a Cluster-unavailable upstream must retain reconnect backoff"
    );
}

#[test]
fn deferred_diskless_resync_advertises_fire_time_offset_and_resumes_from_it() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().replication.repl_diskless_sync = true;
    backend
        .ctx
        .replication_mut()
        .backlog
        .restore_primary_from_rdb("restored", offset(41))
        .unwrap();
    // Exercise real admission and offset alignment without forking the shared
    // test process. Invalid affinity preparation fails before fork and selects
    // the in-process fallback. An active child would correctly keep All queued.
    backend.ctx.config_mut().server.bgsave_cpulist = "not-a-cpu-list".to_string();
    assert!(
        crate::framework::commands::server::save::prepare_bgsave_cpu_affinity(
            &backend.ctx.config().server.bgsave_cpulist,
        )
        .is_err()
    );
    assert!(!backend.ctx.persistence().has_active_child());

    let token_value = 41usize;
    let connection_token = add_connection(&mut backend, token_value);
    let id = backend.replication_runtime.register_primary(
        ConnectionId(token_value),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: vec!["eof".to_string(), "psync2".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let now = crate::foundation::io::sys::time::mstime();
    {
        let backlog = &backend.ctx.replication().backlog;
        let plan = backend
            .replication_runtime
            .primary_link_mut(id)
            .unwrap()
            .plan_full_resync(backlog);
        assert!(matches!(plan, PrimarySyncPlan::FullResync { .. }));
    }
    backend.ctx.replication_mut().backlog.activate_new_history();
    let psync_offset = backend.ctx.replication().backlog.reported_offset().get();
    backend
        .replication_runtime
        .primary_link_mut(id)
        .unwrap()
        .defer_diskless_sync(5_000, now, true);

    for i in 0..64u32 {
        let frame = crate::engine::distributed::replication::protocol::encode_command(&[
            b"SET",
            format!("k{i}").as_bytes(),
            b"v",
        ]);
        backend.propagate_to_replicas(&frame);
    }
    let fire_boundary = backend.ctx.replication().backlog.snapshot_boundary();
    let fire_offset = fire_boundary.reported_offset().get();
    assert!(
        fire_offset > psync_offset,
        "test setup: backlog must advance during the deferral window"
    );

    backend.fire_deferred_full_resyncs(vec![id]);
    assert_eq!(backend.ctx.stats().total_forks, 0);

    let queued: Vec<u8> = backend.connection_registry[Token(connection_token)]
        .output
        .queue
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect();
    let header = String::from_utf8_lossy(&queued);
    assert!(
        header.contains("+FULLRESYNC"),
        "expected FULLRESYNC header, got: {header:?}"
    );
    assert!(
        header.contains(&format!(" {}\r\n", fire_offset)),
        "advertised offset must equal fire-time reported offset {fire_offset}, header: {header:?}"
    );
    assert!(
        !header.contains(&format!(" {}\r\n", psync_offset)),
        "must NOT advertise stale PSYNC-time offset {psync_offset}, header: {header:?}"
    );

    let link = backend
        .replication_runtime
        .primary_link(id)
        .expect("deferred replica link remains registered");
    assert_eq!(
        link.next_send_cursor().get(),
        fire_boundary.stream_cursor().get()
    );
    assert_eq!(
        link.sent_cursor().get(),
        fire_boundary.stream_cursor().get()
    );

    {
        let link = backend
            .replication_runtime
            .primary_link_mut(id)
            .expect("deferred replica link remains registered");
        link.acknowledge(offset(0), now);
        assert!(link.promote_full_resync_on_ack());
    }
    let queued_before_post = backend.connection_registry[Token(connection_token)]
        .output
        .queue
        .len();
    let frame =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SET", b"post", b"v"]);
    backend.propagate_to_replicas(&frame);
    let streamed_after_post: Vec<u8> = backend.connection_registry[Token(connection_token)]
        .output
        .queue
        .iter()
        .skip(queued_before_post)
        .flat_map(|chunk| chunk.iter().copied())
        .collect();
    assert!(
        streamed_after_post.windows(4).any(|w| w == b"post"),
        "the new write must be streamed to the replica"
    );
    assert!(
        !streamed_after_post.windows(2).any(|w| w == b"k0"),
        "must NOT re-stream RDB-baked writes; stale next send cursor detected: {:?}",
        String::from_utf8_lossy(&streamed_after_post)
    );
}

#[test]
fn restored_dual_fallback_header_uses_reported_snapshot_offset() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend
        .ctx
        .replication_mut()
        .backlog
        .restore_primary_from_rdb("restored", offset(41))
        .unwrap();
    let replid = backend.ctx.replication().backlog.replid().to_string();
    let connection_token = add_connection(&mut backend, 0);
    let connection_id = ConnectionId(connection_token);
    let primary_id = backend.replication_runtime.register_primary(
        connection_id,
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    {
        let link = backend
            .replication_runtime
            .primary_link_mut(primary_id)
            .unwrap();
        link.transition_to_waiting_for_rdb_channel().unwrap();
    }
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"during-wait")
        .unwrap();
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();

    backend.execute_dual_channel_full_resync(connection_id, primary_id);

    let queued = _queued(&backend.connection_registry[Token(connection_token)]);
    assert!(queued.starts_with(
        format!(
            "+FULLRESYNC {replid} {}\r\n",
            boundary.reported_offset().get()
        )
        .as_bytes()
    ));
    let link = backend
        .replication_runtime
        .primary_link(primary_id)
        .unwrap();
    assert_eq!(
        link.next_send_cursor().get(),
        boundary.stream_cursor().get()
    );
    assert_eq!(link.sent_cursor().get(), boundary.stream_cursor().get());
    assert_eq!(link.dual_state(), PrimaryDualChannelState::Disabled);
    assert_eq!(link.rdb_channel_wait_deadline_ms, None);
    assert_eq!(link.state(), PrimaryLinkState::WaitingForReplicaAck);
}

#[test]
fn full_resync_rdb_output_does_not_advance_replication_cursor() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend
        .ctx
        .replication_mut()
        .backlog
        .restore_primary_from_rdb("restored", offset(41))
        .unwrap();
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    let connection_token = add_connection(&mut backend, 0);
    let connection_id = ConnectionId(connection_token);
    let primary_id = backend.replication_runtime.register_primary(
        connection_id,
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let rdb = b"$EOF:marker\r\nrdb-bodymarker".to_vec();
    let tail = b"tail".to_vec();
    backend.ctx.replication_mut().backlog.append(&tail).unwrap();
    {
        let link = backend
            .replication_runtime
            .primary_link_mut(primary_id)
            .unwrap();
        link.set_full_resync_boundary(boundary).unwrap();
        link.begin_length_framed_full_resync(rdb.clone(), 1, 0);
    }

    backend.pump_full_resync_transfers();

    let connection = &mut backend.connection_registry[Token(connection_token)];
    assert_eq!(connection.output.queue.pop_front(), Some(rdb.clone()));
    let rdb_written = connection
        .consume_written_bytes(rdb.len(), rdb.len() + tail.len())
        .unwrap();
    assert_eq!(rdb_written, 0);
    let backlog_end = backend.ctx.replication().backlog.next_stream_cursor();
    backend
        .replication_runtime
        .primary_link_mut(primary_id)
        .unwrap()
        .mark_replication_written(rdb_written, backlog_end)
        .unwrap();
    let link = backend
        .replication_runtime
        .primary_link(primary_id)
        .unwrap();
    assert_eq!(link.sent_cursor().get(), boundary.stream_cursor().get());

    let connection = &mut backend.connection_registry[Token(connection_token)];
    assert_eq!(connection.output.queue.pop_front(), Some(tail.clone()));
    let tail_written = connection
        .consume_written_bytes(tail.len(), tail.len())
        .unwrap();
    assert_eq!(tail_written, tail.len());
    let backlog_end = backend.ctx.replication().backlog.next_stream_cursor();
    backend
        .replication_runtime
        .primary_link_mut(primary_id)
        .unwrap()
        .mark_replication_written(tail_written, backlog_end)
        .unwrap();
    let link = backend
        .replication_runtime
        .primary_link(primary_id)
        .unwrap();
    assert_eq!(
        link.sent_cursor().get(),
        boundary.stream_cursor().get() + tail.len() as i64
    );
    assert!(link.sent_cursor().get() <= link.next_send_cursor().get());
}

#[test]
fn in_process_full_resync_pump_error_removes_stale_replica() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_connection(&mut backend, 0);
    let connection_id = ConnectionId(connection_token);
    let primary_id = backend.replication_runtime.register_primary(
        connection_id,
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    {
        let backlog = &backend.ctx.replication().backlog;
        let link = backend
            .replication_runtime
            .primary_link_mut(primary_id)
            .unwrap();
        link.plan_full_resync(backlog);
        link.begin_length_framed_full_resync(b"rdb".to_vec(), 1, 0);
    }
    backend.ctx.replication_mut().backlog.set_capacity(0);
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"evicted-tail")
        .unwrap();

    backend.pump_full_resync_transfers();

    assert!(!backend
        .connection_registry
        .contains(Token(connection_token)));
    assert!(backend
        .replication_runtime
        .primary_link(primary_id)
        .is_none());
}

#[test]
fn replica_select_relays_and_feeds_explicit_frame_then_updates_both_cursors() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.replication_mut().backlog.activate();
    let connection_token = add_connection(&mut backend, 41);
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend
        .ctx
        .persistence_mut()
        .begin_aof_rewrite_child(123, 1_000)
        .unwrap();
    backend.ctx.set_repl_seldb(1);
    backend.ctx.persistence_mut().set_aof_selected_db(2);
    let before = backend.ctx.replication().backlog.next_stream_cursor();
    let mut link = ReplicaLink::new(
        ConnectionId(41),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let select =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SELECT", b"10"]);

    backend
        .apply_replication_command(
            connection_token,
            &mut link,
            ReplicationApply::Select(10),
            CommandOrigin::Replication,
        )
        .unwrap();

    assert_eq!(
        backend.ctx.replication().backlog.read_from(before),
        Some(select.clone())
    );
    assert_eq!(backend.ctx.aof().rewrite.buffer, select);
    assert_eq!(backend.ctx.repl_seldb(), 10);
    assert_eq!(backend.ctx.persistence().aof_selected_db(), 10);
    backend.ctx.persistence_mut().abort_child();
}

#[test]
fn replica_select_updates_aof_cursor_while_aof_is_disabled_without_feeding_it() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.replication_mut().backlog.activate();
    let connection_token = add_connection(&mut backend, 41);
    backend.ctx.config_mut().persistence.aof_enabled = false;
    backend.ctx.set_repl_seldb(1);
    backend.ctx.persistence_mut().set_aof_selected_db(2);
    let before = backend.ctx.replication().backlog.next_stream_cursor();
    let mut link = ReplicaLink::new(
        ConnectionId(41),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let select =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SELECT", b"10"]);

    backend
        .apply_replication_command(
            connection_token,
            &mut link,
            ReplicationApply::Select(10),
            CommandOrigin::Replication,
        )
        .unwrap();

    assert_eq!(
        backend.ctx.replication().backlog.read_from(before),
        Some(select)
    );
    assert!(backend.ctx.aof().rewrite.buffer.is_empty());
    assert_eq!(backend.ctx.repl_seldb(), 10);
    assert_eq!(backend.ctx.persistence().aof_selected_db(), 10);
}

#[test]
fn replica_command_after_rewrite_reset_selects_aof_db_without_changing_relay() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.replication_mut().backlog.activate();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_connection(&mut backend, 41);
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.ctx.persistence_mut().set_aof_selected_db(5);
    let before = backend.ctx.replication().backlog.next_stream_cursor();
    let mut link = ReplicaLink::new(
        ConnectionId(41),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    backend.connection_registry[Token(connection_token)].selected_db = 5;
    link.set_selected_db(5);
    backend.ctx.set_repl_seldb(5);
    backend
        .ctx
        .persistence_mut()
        .begin_aof_rewrite_child(123, 1_000)
        .unwrap();
    assert_eq!(backend.ctx.persistence().aof_selected_db(), -1);
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"replica-key"),
        Bytes::from_static(b"value"),
    ]);
    let frame = crate::engine::distributed::replication::protocol::encode_command(&[
        b"SET",
        b"replica-key",
        b"value",
    ]);
    let select =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SELECT", b"5"]);

    backend
        .apply_replication_command(
            connection_token,
            &mut link,
            ReplicationApply::Command(args),
            CommandOrigin::Replication,
        )
        .unwrap();

    assert_eq!(
        backend.ctx.replication().backlog.read_from(before),
        Some(frame.clone())
    );
    assert_eq!(backend.ctx.aof().rewrite.buffer, [select, frame].concat());
    assert_eq!(backend.ctx.repl_seldb(), 5);
    assert_eq!(backend.ctx.persistence().aof_selected_db(), 5);
    assert_eq!(
        backend.connection_registry[Token(connection_token)].selected_db,
        5
    );
    assert_eq!(link.selected_db(), 5);

    let produced = backend.ctx.aof().rewrite.buffer.clone();
    let mut replay = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut replay.registry);
    let mut player =
        crate::framework::aof_replay::AofReplayPlayer::new(&replay.registry, &mut replay.ctx);
    crate::foundation::io::persistence::load_aof_bytes(&produced, &mut player).unwrap();
    assert_eq!(
        replay
            .ctx
            .db(5)
            .unwrap()
            .get_no_expire(b"replica-key")
            .unwrap()
            .as_bytes(),
        b"value"
    );
    assert!(replay
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"replica-key")
        .is_none());
    backend.ctx.persistence_mut().abort_child();
}

#[test]
fn replica_command_with_matching_aof_cursor_does_not_add_select() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.replication_mut().backlog.activate();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_connection(&mut backend, 41);
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend
        .ctx
        .persistence_mut()
        .begin_aof_rewrite_child(123, 1_000)
        .unwrap();
    backend.connection_registry[Token(connection_token)].selected_db = 5;
    backend.ctx.set_repl_seldb(5);
    backend.ctx.persistence_mut().set_aof_selected_db(5);
    let before = backend.ctx.replication().backlog.next_stream_cursor();
    let mut link = ReplicaLink::new(
        ConnectionId(41),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.set_selected_db(5);
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"replica-key"),
        Bytes::from_static(b"value"),
    ]);
    let frame = crate::engine::distributed::replication::protocol::encode_command(&[
        b"SET",
        b"replica-key",
        b"value",
    ]);

    backend
        .apply_replication_command(
            connection_token,
            &mut link,
            ReplicationApply::Command(args),
            CommandOrigin::Replication,
        )
        .unwrap();

    assert_eq!(
        backend.ctx.replication().backlog.read_from(before),
        Some(frame.clone())
    );
    assert_eq!(backend.ctx.aof().rewrite.buffer, frame);
    assert_eq!(backend.ctx.repl_seldb(), 5);
    assert_eq!(backend.ctx.persistence().aof_selected_db(), 5);
    assert_eq!(
        backend.connection_registry[Token(connection_token)].selected_db,
        5
    );
    assert_eq!(link.selected_db(), 5);
    backend.ctx.persistence_mut().abort_child();
}

#[test]
fn replica_eval_repeatedly_clears_command_transients_and_delivers_notifications() {
    const APPLY_COUNT: u64 = 3;

    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.replication_mut().backlog.activate();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_command_registry_snapshot(&backend.registry);
    let replication_key = add_connection(&mut backend, 41);
    let normal_subscriber_key = add_connection(&mut backend, 42);
    let shard_subscriber_key = add_connection(&mut backend, 43);
    backend
        .ctx
        .pubsub_mut()
        .subscribe(42, Bytes::from_static(b"normal-channel"));
    backend
        .ctx
        .pubsub_mut()
        .ssubscribe(43, Bytes::from_static(b"shard-channel"));
    {
        let conn = &mut backend.connection_registry[Token(replication_key)];
        conn.selected_db = 6;
        conn.authenticated = false;
        conn.ever_authenticated = false;
        conn.username = "replication-link".to_string();
        conn.tot_net_in = 101;
        conn.tot_net_out = 202;
        conn.cmd_count = 303;
    }
    backend.ctx.replication_mut().slave_repl_offset = offset(404);
    let mut link = ReplicaLink::new(
        ConnectionId(41),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.set_selected_db(6);
    let script = Bytes::from_static(
        b"redis.call('set','replica-script-key','value'); \
          redis.call('publish','normal-channel','message'); \
          redis.call('spublish','shard-channel','message'); return 1",
    );
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"EVAL"),
        script.clone(),
        Bytes::from_static(b"0"),
    ]);
    let frame = crate::engine::distributed::replication::protocol::encode_command(&[
        b"EVAL",
        script.as_ref(),
        b"0",
    ]);
    let before = backend.ctx.replication().backlog.next_stream_cursor();

    for _ in 0..APPLY_COUNT {
        backend
            .apply_replication_command(
                replication_key,
                &mut link,
                ReplicationApply::Command(args.clone()),
                CommandOrigin::Replication,
            )
            .unwrap();
        _assert_command_transients_cleared(&backend.connection_registry[Token(replication_key)]);
    }

    assert_eq!(
        backend
            .ctx
            .db(6)
            .unwrap()
            .get_no_expire(b"replica-script-key")
            .unwrap()
            .as_bytes(),
        b"value"
    );
    assert_eq!(
        _queued(&backend.connection_registry[Token(normal_subscriber_key)]),
        crate::engine::distributed::pubsub::encode_message(b"normal-channel", b"message", false,)
            .repeat(APPLY_COUNT as usize)
    );
    assert_eq!(
        _queued(&backend.connection_registry[Token(shard_subscriber_key)]),
        crate::engine::distributed::pubsub::encode_smessage(b"shard-channel", b"message", false,)
            .repeat(APPLY_COUNT as usize)
    );
    assert_eq!(
        backend.ctx.replication().backlog.read_from(before),
        Some(frame.repeat(APPLY_COUNT as usize))
    );
    let conn = &backend.connection_registry[Token(replication_key)];
    assert_eq!(conn.selected_db, 6);
    assert!(!conn.authenticated);
    assert!(!conn.ever_authenticated);
    assert_eq!(conn.username, "replication-link");
    assert_eq!(conn.tot_net_in, 101);
    assert_eq!(conn.tot_net_out, 202);
    assert_eq!(conn.cmd_count, 303 + 3 * APPLY_COUNT);
    assert_eq!(backend.ctx.replication().slave_repl_offset, offset(404));
    assert_eq!(link.applied_position(), offset(0));
}

#[test]
fn replica_erroring_eval_keeps_completed_effects_and_clears_command_transients() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.replication_mut().backlog.activate();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_command_registry_snapshot(&backend.registry);
    let replication_key = add_connection(&mut backend, 51);
    let subscriber_key = add_connection(&mut backend, 52);
    backend
        .ctx
        .pubsub_mut()
        .subscribe(52, Bytes::from_static(b"error-channel"));
    backend.connection_registry[Token(replication_key)].selected_db = 7;
    let mut link = ReplicaLink::new(
        ConnectionId(51),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.set_selected_db(7);
    let script = Bytes::from_static(
        b"redis.call('set','replica-error-key','durable'); \
          redis.call('publish','error-channel','before-error'); \
          return redis.call('missing-command')",
    );
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"EVAL"),
        script.clone(),
        Bytes::from_static(b"0"),
    ]);
    let frame = crate::engine::distributed::replication::protocol::encode_command(&[
        b"EVAL",
        script.as_ref(),
        b"0",
    ]);
    let before = backend.ctx.replication().backlog.next_stream_cursor();

    backend
        .apply_replication_command(
            replication_key,
            &mut link,
            ReplicationApply::Command(args),
            CommandOrigin::Replication,
        )
        .unwrap();

    _assert_command_transients_cleared(&backend.connection_registry[Token(replication_key)]);
    assert_eq!(
        backend
            .ctx
            .db(7)
            .unwrap()
            .get_no_expire(b"replica-error-key")
            .unwrap()
            .as_bytes(),
        b"durable"
    );
    assert_eq!(
        _queued(&backend.connection_registry[Token(subscriber_key)]),
        crate::engine::distributed::pubsub::encode_message(
            b"error-channel",
            b"before-error",
            false,
        )
    );
    assert_eq!(
        backend.ctx.replication().backlog.read_from(before),
        Some(frame)
    );
    assert_eq!(
        backend.connection_registry[Token(replication_key)].selected_db,
        7
    );
    assert_eq!(link.applied_position(), offset(0));
}

fn add_connection(backend: &mut ServerWorkerState, token_value: usize) -> usize {
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(Token(token_value));

    backend.connection_registry.insert(connection)
}

#[test]
fn retained_rdb_wait_expiry_releases_unassociated_owner() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let rdb = ConnectionId(9_101);
    let public_client_id = 90_101;
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    backend
        .replication_runtime
        .register_rdb_candidate(rdb)
        .unwrap();
    backend
        .replication_runtime
        .insert_unpaired_rdb_psync_wait_for_test(rdb, boundary);
    assert!(backend
        .replication_runtime
        .retain_completed_unpaired_rdb_wait(rdb, public_client_id, 0));
    backend.replication_runtime.remove_connection(rdb);

    assert_eq!(backend.expire_grace_psync_waits(), 1);
    assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 0);
}

#[test]
fn retained_rdb_wait_expiry_closes_associated_main() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = ConnectionId(9_102);
    let rdb = ConnectionId(9_103);
    let public_client_id = 90_103;
    add_connection(&mut backend, main.0);
    let replica_id = backend.replication_runtime.register_primary(
        main,
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: dual_channel_caps(),
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
    assert!(backend
        .replication_runtime
        .retain_completed_unpaired_rdb_wait(rdb, public_client_id, 1));
    backend.replication_runtime.remove_connection(rdb);
    backend
        .replication_runtime
        .try_associate_retained_rdb_wait(main, public_client_id, 0)
        .unwrap();

    assert_eq!(backend.expire_grace_psync_waits(), 1);
    assert!(!backend.connection_registry.contains(Token(main.0)));
    assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 0);
    assert_eq!(backend.replication_runtime.role(main), None);
    assert!(backend.replication_runtime.primary_links().is_empty());
    assert_eq!(
        backend.replication_runtime.min_protected_backlog_cursor(),
        None
    );
}

fn add_runtime_connection(backend: &mut ServerWorkerState) -> (usize, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind runtime test listener");
    let peer_addr = listener
        .local_addr()
        .expect("read runtime test listener address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect runtime test peer");
    let (target, _) = listener.accept().expect("accept runtime test peer");
    backend.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr,
        },
    );
    (peer.token.0, target)
}

fn dual_channel_caps() -> Vec<String> {
    vec![
        "eof".to_string(),
        "psync2".to_string(),
        "dual-channel".to_string(),
    ]
}

fn setup_primary(
    backend: &mut ServerWorkerState,
    token_value: usize,
    capability: Vec<String>,
    dual_channel_enabled: bool,
    diskless_enabled: bool,
) -> (usize, crate::framework::distributed::replication::ReplicaId) {
    let connection_token = add_connection(backend, token_value);
    backend
        .ctx
        .config_mut()
        .replication
        .dual_channel_replication = dual_channel_enabled;
    backend.ctx.config_mut().replication.repl_diskless_sync = diskless_enabled;
    // These classic/dual negotiation fixtures assert the immediate wire
    // response. Production's default diskless delay is exercised separately
    // by the transport regression, so disable the cohort window here.
    backend
        .ctx
        .config_mut()
        .replication
        .repl_diskless_sync_delay = 0;
    let id = backend.replication_runtime.register_primary(
        crate::framework::distributed::replication::ConnectionId(token_value),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability,
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    (connection_token, id)
}

fn run_psync(
    backend: &mut ServerWorkerState,
    connection_token: usize,
    token_value: usize,
    command: &[u8],
) -> Vec<Vec<u8>> {
    let args = CommandArgs::from_parts(vec![
        Bytes::copy_from_slice(command),
        Bytes::from_static(b"?"),
        Bytes::from_static(b"-1"),
    ]);
    let name_lower = String::from_utf8_lossy(command).to_lowercase();
    let mut replies: Vec<Vec<u8>> = Vec::new();
    let mut pending_unblock: Vec<(usize, bool)> = Vec::new();
    let _ = backend.try_handle_transport_cmd(
        connection_token,
        token_value,
        &args,
        &name_lower,
        false,
        &mut replies,
        &mut pending_unblock,
    );
    replies
}

fn run_sync(
    backend: &mut ServerWorkerState,
    connection_token: usize,
    token_value: usize,
) -> Vec<Vec<u8>> {
    let args = CommandArgs::from_parts(vec![Bytes::from_static(b"SYNC")]);
    let name_lower = "sync".to_string();
    let mut replies: Vec<Vec<u8>> = Vec::new();
    let mut pending_unblock: Vec<(usize, bool)> = Vec::new();
    let _ = backend.try_handle_transport_cmd(
        connection_token,
        token_value,
        &args,
        &name_lower,
        false,
        &mut replies,
        &mut pending_unblock,
    );
    replies
}

#[test]
fn dual_channel_psync_returns_dualchannelsync_and_transitions_state() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 41usize;
    let (connection_token, id) =
        setup_primary(&mut backend, token_value, dual_channel_caps(), true, true);

    {
        let link = backend.replication_runtime.primary_link(id).unwrap();
        assert_eq!(
            link.dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::Disabled
        );
    }

    let replies = run_psync(&mut backend, connection_token, token_value, b"PSYNC");
    let merged: Vec<u8> = replies.into_iter().flatten().collect();

    assert_eq!(
        merged,
        crate::engine::distributed::replication::protocol::DUAL_CHANNEL_SYNC_REPLY.to_vec(),
        "expected +DUALCHANNELSYNC, got: {:?}",
        String::from_utf8_lossy(&merged)
    );
    assert!(
        !merged
            .windows(b"+FULLRESYNC".len())
            .any(|w| w == b"+FULLRESYNC"),
        "must NOT contain +FULLRESYNC bytes"
    );
    assert!(
        !merged.starts_with(b"$"),
        "must NOT contain RDB bulk header ($<len>)"
    );

    let link = backend.replication_runtime.primary_link(id).unwrap();
    assert_eq!(
        link.dual_state(),
        crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForRdbChannel
    );
    assert!(
        matches!(
            link.state(),
            crate::framework::distributed::replication::PrimaryLinkState::AwaitingPsync
        ),
        "dual-channel negotiation must not enter classic full-resync state"
    );
    assert!(
        link.full_resync_is_none(),
        "link must NOT have FullResyncTransfer attached"
    );
}

#[test]
fn dual_channel_falls_back_when_local_config_disabled() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 41usize;
    let (connection_token, _id) =
        setup_primary(&mut backend, token_value, dual_channel_caps(), false, true);

    let replies = run_psync(&mut backend, connection_token, token_value, b"PSYNC");
    let merged: Vec<u8> = replies.into_iter().flatten().collect();
    let reply_str = String::from_utf8_lossy(&merged);
    let replid = backend.ctx.replication().backlog.replid().to_string();

    assert!(
        reply_str.starts_with("+FULLRESYNC "),
        "expected classic +FULLRESYNC when dual_channel_replication=false, got: {:?}",
        reply_str
    );
    let expected_prefix = format!("+FULLRESYNC {} ", replid);
    assert!(
        reply_str.starts_with(&expected_prefix),
        "expected FULLRESYNC with current replid, prefix: {:?}, got: {:?}",
        expected_prefix,
        reply_str
    );
}

#[test]
fn dual_channel_falls_back_when_peer_missing_eof() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 41usize;
    let caps = vec!["psync2".to_string(), "dual-channel".to_string()];
    let (connection_token, _id) = setup_primary(&mut backend, token_value, caps, true, true);

    // A length-framed classic resync reaches the command stream inline, so
    // finish_psync queues the FULLRESYNC/RDB frames directly on the
    // connection output queue (followed by the bootstrap PING) instead of
    // returning them through the command replies vec.
    let replies = run_psync(&mut backend, connection_token, token_value, b"PSYNC");
    let mut merged: Vec<u8> = replies.into_iter().flatten().collect();
    let queued: Vec<u8> = backend.connection_registry[Token(connection_token)]
        .output
        .queue
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect();
    merged.extend_from_slice(&queued);
    let reply_str = String::from_utf8_lossy(&merged);

    assert!(
        reply_str.starts_with("+FULLRESYNC "),
        "expected classic +FULLRESYNC when peer missing eof, got: {:?}",
        reply_str
    );
}

#[test]
fn dual_channel_falls_back_when_peer_missing_psync2() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 41usize;
    let caps = vec!["eof".to_string(), "dual-channel".to_string()];
    let (connection_token, _id) = setup_primary(&mut backend, token_value, caps, true, true);

    let replies = run_psync(&mut backend, connection_token, token_value, b"PSYNC");
    let merged: Vec<u8> = replies.into_iter().flatten().collect();
    let reply_str = String::from_utf8_lossy(&merged);

    assert!(
        reply_str.starts_with("+FULLRESYNC "),
        "expected classic +FULLRESYNC when peer missing psync2, got: {:?}",
        reply_str
    );
}

#[test]
fn dual_channel_falls_back_when_peer_missing_dual_channel_cap() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 41usize;
    let caps = vec!["eof".to_string(), "psync2".to_string()];
    let (connection_token, _id) = setup_primary(&mut backend, token_value, caps, true, true);

    let replies = run_psync(&mut backend, connection_token, token_value, b"PSYNC");
    let merged: Vec<u8> = replies.into_iter().flatten().collect();
    let reply_str = String::from_utf8_lossy(&merged);

    assert!(
        reply_str.starts_with("+FULLRESYNC "),
        "expected classic +FULLRESYNC when peer missing dual-channel cap, got: {:?}",
        reply_str
    );
}

#[test]
fn legacy_sync_command_uses_classic_bytes() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 41usize;
    let (connection_token, _id) =
        setup_primary(&mut backend, token_value, dual_channel_caps(), true, true);

    let replies = run_sync(&mut backend, connection_token, token_value);
    let merged: Vec<u8> = replies.into_iter().flatten().collect();

    assert!(
        !merged
            .windows(b"+DUALCHANNELSYNC".len())
            .any(|w| w == b"+DUALCHANNELSYNC"),
        "SYNC must NOT use +DUALCHANNELSYNC"
    );
    assert!(
        merged.starts_with(b"$"),
        "SYNC must start with RDB bulk header, got: {:?}",
        String::from_utf8_lossy(&merged)
    );
}

#[test]
fn dual_channel_candidate_sends_endoff_before_association_and_finalization_transitions_state() {
    // Test 1 (happy path): an RDB candidate receives $ENDOFF and pins the
    // backlog while the main remains WaitingForRdbChannel. Main association
    // then transitions it to WaitingForMainPsync without another $ENDOFF.
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main_token = 41usize;
    let (main_slab, id) = setup_primary(&mut backend, main_token, dual_channel_caps(), true, true);

    let replies = run_psync(&mut backend, main_slab, main_token, b"PSYNC");
    let merged: Vec<u8> = replies.into_iter().flatten().collect();
    assert_eq!(
        merged,
        crate::engine::distributed::replication::protocol::DUAL_CHANNEL_SYNC_REPLY.to_vec()
    );
    let snapshot_boundary = backend.ctx.replication().backlog.snapshot_boundary();
    let snapshot_offset = snapshot_boundary.reported_offset().get();
    let snapshot_cursor = snapshot_boundary.stream_cursor();
    {
        let link = backend.replication_runtime.primary_link(id).unwrap();
        assert_eq!(
            link.dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForRdbChannel
        );
        assert_eq!(
            link.state(),
            crate::framework::distributed::replication::PrimaryLinkState::AwaitingPsync
        );
    }

    let rdb_token = 42usize;
    let rdb_slab = add_connection(&mut backend, rdb_token);
    backend
        .replication_runtime
        .register_rdb_candidate(crate::framework::distributed::replication::ConnectionId(
            rdb_token,
        ))
        .unwrap();

    let main_id = crate::framework::distributed::replication::ConnectionId(main_token);
    let rdb_id = crate::framework::distributed::replication::ConnectionId(rdb_token);
    send_endoff_for_test(&mut backend, rdb_id);
    let replid = backend.ctx.replication().backlog.replid().to_string();
    backend
        .replication_runtime
        .associate_rdb_channel(main_id, rdb_id)
        .unwrap();
    {
        let ServerWorkerState {
            ref mut replication_runtime,
            ..
        } = backend;
        replication_runtime
            .finalize_dual_channel_rdb_association(main_id, rdb_id)
            .unwrap();
    }

    // a) $ENDOFF bytes on RDB channel output queue, exact match.
    let queued_rdb: Vec<u8> = backend.connection_registry[Token(rdb_slab)]
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
    assert_eq!(
        queued_rdb,
        expected_endoff,
        "RDB channel output must contain $ENDOFF, got: {:?}",
        String::from_utf8_lossy(&queued_rdb)
    );

    // Main channel output queue must NOT contain $ENDOFF.
    let queued_main: Vec<u8> = backend.connection_registry[Token(main_slab)]
        .output
        .queue
        .iter()
        .flat_map(|c| c.iter().copied())
        .collect();
    assert!(
        !queued_main
            .windows(b"$ENDOFF:".len())
            .any(|w| w == b"$ENDOFF:"),
        "main channel must NOT carry $ENDOFF"
    );

    // b) backlog pinning: force tiny capacity, churn, still readable from pinned offset.
    {
        let backlog = &mut backend.ctx.replication_mut().backlog;
        backlog.set_capacity(1);
    }
    for i in 0..32u32 {
        let frame = crate::engine::distributed::replication::protocol::encode_command(&[
            b"SET",
            format!("churn{i}").as_bytes(),
            b"v",
        ]);
        backend.propagate_to_replicas(&frame);
    }
    let pinned_readable = backend
        .ctx
        .replication()
        .backlog
        .read_from(snapshot_cursor)
        .is_some();
    assert!(
        pinned_readable,
        "bytes at snapshot_offset={snapshot_offset} must remain readable after backlog churn with pin"
    );

    // c) dual_state transitioned to WaitingForMainPsync.
    let link = backend.replication_runtime.primary_link(id).unwrap();
    assert_eq!(
        link.dual_state(),
        crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForMainPsync
    );
    // d) dual negotiation remains before the classic full-resync transition.
    assert_eq!(
        link.state(),
        crate::framework::distributed::replication::PrimaryLinkState::AwaitingPsync
    );
}

#[test]
fn backlog_pin_resists_eviction_under_churn() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main_token = 41usize;
    let (_main_slab, id) = setup_primary(&mut backend, main_token, dual_channel_caps(), true, true);
    let main_slab = _main_slab;

    let _replies = run_psync(&mut backend, main_slab, main_token, b"PSYNC");
    let snapshot_boundary = backend.ctx.replication().backlog.snapshot_boundary();
    let snapshot_offset = snapshot_boundary.reported_offset().get();
    let snapshot_cursor = snapshot_boundary.stream_cursor();
    {
        let link = backend.replication_runtime.primary_link(id).unwrap();
        assert_eq!(
            link.dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForRdbChannel
        );
    }

    let rdb_token = 42usize;
    let _rdb_slab = add_connection(&mut backend, rdb_token);
    backend
        .replication_runtime
        .register_rdb_candidate(crate::framework::distributed::replication::ConnectionId(
            rdb_token,
        ))
        .unwrap();

    let main_id = crate::framework::distributed::replication::ConnectionId(main_token);
    let rdb_id = crate::framework::distributed::replication::ConnectionId(rdb_token);
    send_endoff_for_test(&mut backend, rdb_id);
    backend
        .replication_runtime
        .associate_rdb_channel(main_id, rdb_id)
        .unwrap();
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
        let backlog = &mut backend.ctx.replication_mut().backlog;
        backlog.set_capacity(1);
    }
    for i in 0..32u32 {
        let frame = crate::engine::distributed::replication::protocol::encode_command(&[
            b"SET",
            format!("churn{i}").as_bytes(),
            b"v",
        ]);
        backend.propagate_to_replicas(&frame);
    }
    let pinned_readable = backend
        .ctx
        .replication()
        .backlog
        .read_from(snapshot_cursor)
        .is_some();
    assert!(
        pinned_readable,
        "bytes at snapshot_offset={snapshot_offset} must remain readable after backlog churn with pin"
    );
}

#[test]
fn classic_non_dual_main_link_association_is_noop() {
    // Test 2: classic link with dual_state=Disabled, associating an RDB candidate
    // must NOT queue $ENDOFF, NOT change backlog floor, NOT change dual_state.
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main_token = 51usize;
    let caps = vec!["eof".to_string(), "psync2".to_string()];
    let (main_slab, id) = setup_primary(&mut backend, main_token, caps, false, true);

    let _replies = run_psync(&mut backend, main_slab, main_token, b"PSYNC");
    {
        let link = backend.replication_runtime.primary_link(id).unwrap();
        assert_eq!(
            link.dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::Disabled
        );
    }
    let protected_before = backend.replication_runtime.min_protected_backlog_cursor();

    let rdb_token = 52usize;
    let rdb_slab = add_connection(&mut backend, rdb_token);
    backend
        .replication_runtime
        .register_rdb_candidate(crate::framework::distributed::replication::ConnectionId(
            rdb_token,
        ))
        .unwrap();

    let main_id2 = crate::framework::distributed::replication::ConnectionId(main_token);
    let rdb_id2 = crate::framework::distributed::replication::ConnectionId(rdb_token);
    assert!(backend
        .replication_runtime
        .associate_rdb_channel(main_id2, rdb_id2)
        .is_err());
    {
        let ServerWorkerState {
            ref mut replication_runtime,
            ..
        } = backend;
        let _ = replication_runtime.finalize_dual_channel_rdb_association(main_id2, rdb_id2);
    }

    let queued_rdb: Vec<u8> = backend.connection_registry[Token(rdb_slab)]
        .output
        .queue
        .iter()
        .flat_map(|c| c.iter().copied())
        .collect();
    assert!(
        queued_rdb.is_empty(),
        "classic link: RDB queue must remain empty"
    );

    let protected_after = backend.replication_runtime.min_protected_backlog_cursor();
    assert_eq!(
        protected_before, protected_after,
        "classic link: backlog protected offset must NOT change"
    );

    let link = backend.replication_runtime.primary_link(id).unwrap();
    assert_eq!(
        link.dual_state(),
        crate::framework::distributed::replication::PrimaryDualChannelState::Disabled
    );
}

#[test]
fn stale_rdb_token_on_association_does_not_panic() {
    // Test 3: main link is dual + WaitingForRdbChannel, but RDB channel token
    // is missing from the connection registry. No panic; state preserved.
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main_token = 61usize;
    let (main_slab, id) = setup_primary(&mut backend, main_token, dual_channel_caps(), true, true);

    let _replies = run_psync(&mut backend, main_slab, main_token, b"PSYNC");
    {
        let link = backend.replication_runtime.primary_link(id).unwrap();
        assert_eq!(
            link.dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForRdbChannel
        );
    }

    let rdb_token = 62usize;
    backend
        .replication_runtime
        .register_rdb_candidate(crate::framework::distributed::replication::ConnectionId(
            rdb_token,
        ))
        .unwrap();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        backend.replication_runtime.associate_rdb_channel(
            crate::framework::distributed::replication::ConnectionId(main_token),
            crate::framework::distributed::replication::ConnectionId(rdb_token),
        )
    }));
    assert!(result.is_ok(), "stale RDB token must NOT panic");

    let link = backend.replication_runtime.primary_link(id).unwrap();
    assert_eq!(
        link.dual_state(),
        crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForRdbChannel
    );
}

#[test]
fn unpaired_rdb_candidate_starts_pipe_relay_after_endoff_flush() {
    let _fork_guard = crate::framework::commands::server::save::diskless_fork_test_guard();
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main_token = 63usize;
    let (main_slab, replica_id) =
        setup_primary(&mut backend, main_token, dual_channel_caps(), true, true);

    let _replies = run_psync(&mut backend, main_slab, main_token, b"PSYNC");
    let link = backend
        .replication_runtime
        .primary_link(replica_id)
        .unwrap();
    assert_eq!(
        link.dual_state(),
        PrimaryDualChannelState::WaitingForRdbChannel
    );

    let (rdb_token, mut rdb_target) = add_runtime_connection(&mut backend);
    let rdb_id = ConnectionId(rdb_token);
    let rdb_channel = CommandArgs {
        name: Bytes::from_static(b"REPLCONF"),
        args: vec![Bytes::from_static(b"rdb-channel"), Bytes::from_static(b"1")],
    };
    let mut replies = Vec::new();
    let mut pending_unblock = Vec::new();
    backend.try_handle_transport_cmd(
        rdb_token,
        rdb_token,
        &rdb_channel,
        "replconf",
        false,
        &mut replies,
        &mut pending_unblock,
    );
    assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
    assert!(backend.full_sync.pending_dual_rdb_snapshots.is_empty());
    backend.admit_rdb_sync_for_test(rdb_id);
    assert!(backend
        .full_sync
        .pending_dual_rdb_snapshots
        .contains_key(&Token(rdb_token)));
    assert_eq!(
        backend
            .replication_runtime
            .rebuild_view()
            .replicas_waiting_psync,
        0,
        "REPLCONF handling must not capture an earlier ENDOFF scalar"
    );

    backend.ctx.db_mut(0).unwrap().set(
        b"counter".to_vec(),
        crate::foundation::io::store::Robj::string(b"1".as_slice()),
    );
    backend.ctx.replication_mut().backlog.activate();
    let increment = b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n";
    backend.propagate_to_replicas(increment);
    let expected_boundary = backend.ctx.replication().backlog.snapshot_boundary();

    backend.drive_pending_dual_rdb_snapshots();

    assert!(
        backend.full_sync.diskless_relay.is_some(),
        "once ENDOFF is flushed, the snapshot must start at the same boundary instead of waiting for main association"
    );
    rdb_target
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .expect("set test socket read timeout");
    let mut queued_rdb = [0u8; 256];
    let n = rdb_target
        .read(&mut queued_rdb)
        .expect("read queued ENDOFF from RDB channel");
    assert!(
        queued_rdb[..n]
            .windows(b"$ENDOFF:".len())
            .any(|window| window == b"$ENDOFF:"),
        "candidate must receive ENDOFF so the main channel can send set-rdb-client-id"
    );
    let line_end = queued_rdb[..n]
        .windows(2)
        .position(|window| window == b"\r\n")
        .expect("ENDOFF line must be complete");
    let endoff = protocol::parse_end_offset(&queued_rdb[..line_end])
        .expect("the first frame must be ENDOFF");
    assert_eq!(endoff.offset, expected_boundary.reported_offset());
    let relay = backend
        .full_sync
        .diskless_relay
        .as_ref()
        .expect("pipe relay must be retained while the main association catches up");
    assert!(
        relay.targets.len() == 1,
        "the dedicated RDB session owns transport before main-channel association"
    );

    backend
        .replication_runtime
        .associate_rdb_channel(ConnectionId(main_token), rdb_id)
        .unwrap();
    backend
        .replication_runtime
        .finalize_dual_channel_rdb_association(ConnectionId(main_token), rdb_id)
        .unwrap();
    assert_eq!(
        backend
            .replication_runtime
            .primary_for_connection(ConnectionId(main_token))
            .unwrap()
            .dual_endoff_boundary(),
        Some(expected_boundary)
    );
    assert_eq!(
        backend
            .ctx
            .replication()
            .backlog
            .read_from(expected_boundary.stream_cursor()),
        Some(Vec::new()),
        "the INCR included in the snapshot must not be replayed from the backlog tail"
    );
    assert!(
        backend
            .full_sync
            .diskless_relay
            .as_ref()
            .expect("pipe relay must survive association")
            .targets
            .iter().any(|target| matches!(target,
                super::diskless_child::DisklessTarget::Rdb { connection_id, .. } if *connection_id == rdb_id)),
        "late association must retain the original dedicated RDB transport target"
    );

    backend.abort_diskless_relay_for_dataset_flush();
}

#[test]
fn paired_rdb_channel_waits_for_unpaired_cohort_member_before_fork() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend
        .ctx
        .config_mut()
        .replication
        .repl_diskless_sync_max_replicas = 2;
    let (first_main_slab, _) = setup_primary(&mut backend, 65, dual_channel_caps(), true, true);
    let (second_main_slab, _) = setup_primary(&mut backend, 66, dual_channel_caps(), true, true);

    let _ = run_psync(&mut backend, first_main_slab, 65, b"PSYNC");
    let _ = run_psync(&mut backend, second_main_slab, 66, b"PSYNC");

    let first_rdb = ConnectionId(67);
    let second_rdb = ConnectionId(68);
    add_connection(&mut backend, first_rdb.0);
    backend
        .replication_runtime
        .register_rdb_candidate(first_rdb)
        .unwrap();
    backend
        .replication_runtime
        .register_rdb_candidate(second_rdb)
        .unwrap();
    send_endoff_for_test(&mut backend, first_rdb);
    backend
        .full_sync
        .pending_dual_rdb_snapshots
        .insert(Token(first_rdb.0), (0, vec![1]));
    backend
        .full_sync
        .pending_dual_rdb_snapshots
        .insert(Token(second_rdb.0), (0, Vec::new()));

    backend
        .replication_runtime
        .associate_rdb_channel(ConnectionId(65), first_rdb)
        .unwrap();
    backend.drive_pending_dual_rdb_snapshots();

    assert!(
        backend.full_sync.diskless_relay.is_none(),
        "cohort must not start a relay while another ready RDB candidate has not received ENDOFF"
    );
    assert!(
        backend
            .full_sync
            .pending_dual_rdb_snapshots
            .contains_key(&Token(first_rdb.0)),
        "paired RDB must remain in the pending cohort until every candidate is paired"
    );
    assert!(
        backend
            .full_sync
            .pending_dual_rdb_snapshots
            .contains_key(&Token(second_rdb.0)),
        "unpaired RDB candidate must remain pending until ENDOFF is flushed"
    );
}

const RDB_BYTES: &[u8] = b"FAKE-RDB-BYTES-0123456789ABCDEF";

#[test]
fn dual_happy_path_rdb_over_rdb_channel() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main_token = 71usize;
    let (main_slab, id) = setup_primary(&mut backend, main_token, dual_channel_caps(), true, true);

    let _replies = run_psync(&mut backend, main_slab, main_token, b"PSYNC");

    let rdb_token = 72usize;
    let rdb_slab = add_connection(&mut backend, rdb_token);
    backend
        .replication_runtime
        .register_rdb_candidate(crate::framework::distributed::replication::ConnectionId(
            rdb_token,
        ))
        .unwrap();

    let main_id = crate::framework::distributed::replication::ConnectionId(main_token);
    let rdb_id = crate::framework::distributed::replication::ConnectionId(rdb_token);
    send_endoff_for_test(&mut backend, rdb_id);
    backend
        .replication_runtime
        .associate_rdb_channel(main_id, rdb_id)
        .unwrap();
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
        let link = backend.replication_runtime.primary_link(id).unwrap();
        assert_eq!(
            link.dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForMainPsync
        );
    }

    let target_conn_id = backend
        .replication_runtime
        .resolve_diskless_rdb_target(main_id);
    let target_slab = if backend
        .connection_registry
        .contains(Token(target_conn_id.0))
    {
        target_conn_id.0
    } else {
        main_slab
    };
    if let Some(conn) = backend.connection_registry.get_mut(Token(target_slab)) {
        conn.output.queue.push_back(RDB_BYTES.to_vec());
    }

    let queued_rdb: Vec<u8> = backend.connection_registry[Token(rdb_slab)]
        .output
        .queue
        .iter()
        .flat_map(|c| c.iter().copied())
        .collect();
    let queued_main: Vec<u8> = backend.connection_registry[Token(main_slab)]
        .output
        .queue
        .iter()
        .flat_map(|c| c.iter().copied())
        .collect();

    assert!(
        queued_rdb.windows(RDB_BYTES.len()).any(|w| w == RDB_BYTES),
        "RDB bytes must appear on RDB channel"
    );
    assert!(
        !queued_main.windows(RDB_BYTES.len()).any(|w| w == RDB_BYTES),
        "RDB bytes must NOT appear on main channel"
    );
}

#[test]
fn classic_fallback_still_to_main() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main_token = 81usize;
    let caps = vec!["eof".to_string(), "psync2".to_string()];
    let (main_slab, _id) = setup_primary(&mut backend, main_token, caps, false, true);

    let _replies = run_psync(&mut backend, main_slab, main_token, b"PSYNC");

    let rdb_token = 82usize;
    let _rdb_slab = add_connection(&mut backend, rdb_token);

    let main_id = crate::framework::distributed::replication::ConnectionId(main_token);
    let target_conn_id = backend
        .replication_runtime
        .resolve_diskless_rdb_target(main_id);
    let target_slab = if backend
        .connection_registry
        .contains(Token(target_conn_id.0))
    {
        target_conn_id.0
    } else {
        main_slab
    };
    if let Some(conn) = backend.connection_registry.get_mut(Token(target_slab)) {
        conn.output.queue.push_back(RDB_BYTES.to_vec());
    }

    let queued_main: Vec<u8> = backend.connection_registry[Token(main_slab)]
        .output
        .queue
        .iter()
        .flat_map(|c| c.iter().copied())
        .collect();
    let queued_rdb: Vec<u8> = backend.connection_registry[Token(_rdb_slab)]
        .output
        .queue
        .iter()
        .flat_map(|c| c.iter().copied())
        .collect();

    assert!(
        queued_main.windows(RDB_BYTES.len()).any(|w| w == RDB_BYTES),
        "classic mode: RDB bytes must appear on main channel"
    );
    assert!(
        !queued_rdb.windows(RDB_BYTES.len()).any(|w| w == RDB_BYTES),
        "classic mode: RDB bytes must NOT appear on RDB channel"
    );
}

#[test]
fn stale_rdb_token_fallback_to_main_no_panic() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main_token = 91usize;
    let (main_slab, id) = setup_primary(&mut backend, main_token, dual_channel_caps(), true, true);

    let _replies = run_psync(&mut backend, main_slab, main_token, b"PSYNC");

    let rdb_token = 92usize;
    let main_id = crate::framework::distributed::replication::ConnectionId(main_token);
    let rdb_id = crate::framework::distributed::replication::ConnectionId(rdb_token);

    backend
        .replication_runtime
        .register_rdb_candidate(rdb_id)
        .unwrap();

    add_connection(&mut backend, rdb_token);
    send_endoff_for_test(&mut backend, rdb_id);
    backend
        .replication_runtime
        .associate_rdb_channel(main_id, rdb_id)
        .unwrap();
    backend.connection_registry.remove(Token(rdb_token));

    backend
        .replication_runtime
        .primary_for_connection_mut(main_id)
        .unwrap()
        .transition_to_waiting_for_main_psync()
        .unwrap();

    {
        let link = backend.replication_runtime.primary_link(id).unwrap();
        assert_eq!(
            link.dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForMainPsync
        );
        assert!(
            backend
                .replication_runtime
                .rdb_channel_for_main(main_id)
                .is_some(),
            "rdb_channel_for_main must return Some"
        );
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let target_conn_id = backend
            .replication_runtime
            .resolve_diskless_rdb_target(main_id);
        let main_slab_fallback = backend
            .connection_registry
            .contains(Token(main_id.0))
            .then_some(main_id.0);
        let target_slab = backend
            .connection_registry
            .contains(Token(target_conn_id.0))
            .then_some(target_conn_id.0)
            .or(main_slab_fallback);
        if let Some(slab) = target_slab {
            if let Some(conn) = backend.connection_registry.get_mut(Token(slab)) {
                conn.output.queue.push_back(RDB_BYTES.to_vec());
            }
        }
    }));
    assert!(result.is_ok(), "stale RDB token must NOT panic");

    let queued_main: Vec<u8> = backend.connection_registry[Token(main_slab)]
        .output
        .queue
        .iter()
        .flat_map(|c| c.iter().copied())
        .collect();

    assert!(
        queued_main.windows(RDB_BYTES.len()).any(|w| w == RDB_BYTES),
        "stale RDB token: bytes must fallback to main channel"
    );
}

#[test]
fn eof_marker_only_on_rdb_channel_in_dual_mode() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main_token = 101usize;
    let (main_slab, id) = setup_primary(&mut backend, main_token, dual_channel_caps(), true, true);

    let _replies = run_psync(&mut backend, main_slab, main_token, b"PSYNC");

    let rdb_token = 102usize;
    let rdb_slab = add_connection(&mut backend, rdb_token);
    backend
        .replication_runtime
        .register_rdb_candidate(crate::framework::distributed::replication::ConnectionId(
            rdb_token,
        ))
        .unwrap();

    let main_id = crate::framework::distributed::replication::ConnectionId(main_token);
    let rdb_id = crate::framework::distributed::replication::ConnectionId(rdb_token);
    send_endoff_for_test(&mut backend, rdb_id);
    backend
        .replication_runtime
        .associate_rdb_channel(main_id, rdb_id)
        .unwrap();
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
        let link = backend.replication_runtime.primary_link(id).unwrap();
        assert_eq!(
            link.dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForMainPsync
        );
    }

    let marker = format!(
        "$EOF:{}\r\n",
        crate::engine::distributed::replication::protocol::generate_replid()
    )
    .into_bytes();

    let target_conn_id = backend
        .replication_runtime
        .resolve_diskless_rdb_target(main_id);
    let target_slab = if backend
        .connection_registry
        .contains(Token(target_conn_id.0))
    {
        target_conn_id.0
    } else {
        main_slab
    };
    if let Some(conn) = backend.connection_registry.get_mut(Token(target_slab)) {
        conn.output.queue.push_back(marker.clone());
    }

    let queued_rdb: Vec<u8> = backend.connection_registry[Token(rdb_slab)]
        .output
        .queue
        .iter()
        .flat_map(|c| c.iter().copied())
        .collect();
    let queued_main: Vec<u8> = backend.connection_registry[Token(main_slab)]
        .output
        .queue
        .iter()
        .flat_map(|c| c.iter().copied())
        .collect();

    assert!(
        queued_rdb.ends_with(&marker),
        "EOF marker must appear at end of RDB channel queue"
    );
    assert!(
        !queued_main
            .windows(marker.len())
            .any(|w| w == marker.as_slice()),
        "EOF marker must NOT appear on main channel"
    );
}

mod diskless_completion;
mod dual;
mod dual_pause;
mod full_sync;
mod import_tracking;
mod paused_dual;
mod regression;
mod transaction_relay;

fn boundary_replconf(backend: &mut ServerWorkerState, token: usize, parts: Vec<Bytes>) {
    let args = CommandArgs {
        name: Bytes::from_static(b"REPLCONF"),
        args: parts,
    };
    let mut replies = Vec::new();
    assert_eq!(
        backend.try_handle_transport_cmd(
            token,
            token,
            &args,
            "replconf",
            false,
            &mut replies,
            &mut Vec::new(),
        ),
        Some(false)
    );
    assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
}

fn boundary_waiting_main(backend: &mut ServerWorkerState) -> (usize, TcpStream) {
    let (token, target) = add_runtime_connection(backend);
    boundary_replconf(
        backend,
        token,
        vec![
            Bytes::from_static(b"capa"),
            Bytes::from_static(b"eof"),
            Bytes::from_static(b"capa"),
            Bytes::from_static(b"psync2"),
            Bytes::from_static(b"capa"),
            Bytes::from_static(b"dual-channel"),
        ],
    );
    assert_eq!(
        run_psync(backend, token, token, b"PSYNC"),
        vec![protocol::DUAL_CHANNEL_SYNC_REPLY.to_vec()]
    );
    assert_eq!(
        backend
            .replication_runtime
            .primary_for_connection(ConnectionId(token))
            .unwrap()
            .dual_state(),
        PrimaryDualChannelState::WaitingForRdbChannel
    );
    (token, target)
}

fn boundary_sync_candidate(backend: &mut ServerWorkerState, filter: &[u8]) -> (usize, TcpStream) {
    let (token, target) = add_runtime_connection(backend);
    boundary_replconf(
        backend,
        token,
        vec![
            Bytes::from_static(b"capa"),
            Bytes::from_static(b"eof"),
            Bytes::from_static(b"rdb-only"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"rdb-channel"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"rdb-filter-only"),
            Bytes::copy_from_slice(filter),
        ],
    );
    backend.admit_rdb_sync_for_test(ConnectionId(token));
    (token, target)
}

fn boundary_read_endoff(target: &mut TcpStream) -> protocol::EndOffset {
    target
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .unwrap();
    let mut line = Vec::new();
    for _ in 0..256 {
        let mut byte = [0];
        target.read_exact(&mut byte).expect("read flushed ENDOFF");
        line.push(byte[0]);
        if line.ends_with(b"\r\n") {
            return protocol::parse_end_offset(&line)
                .expect("first frame must be a complete ENDOFF");
        }
    }
    panic!("ENDOFF exceeded the bounded line length");
}

#[test]
fn boundary_dual_candidates_keep_other_filter_deadlines_and_waits() {
    let _fork_guard = crate::framework::commands::server::save::diskless_fork_test_guard();
    let mut observations = Vec::new();
    for associate in [false, true] {
        let mut backend = ServerWorkerState::new(1).unwrap();
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
        backend
            .ctx
            .config_mut()
            .replication
            .repl_diskless_sync_max_replicas = 2;
        let (main, _main_target) = boundary_waiting_main(&mut backend);
        let (_other_main, _other_main_target) = boundary_waiting_main(&mut backend);
        let (first, _first_target) = boundary_sync_candidate(&mut backend, b"functions");
        let first_deadline = backend.full_sync.pending_dual_rdb_snapshots[&Token(first)].0;
        let (other, mut other_target) = boundary_sync_candidate(&mut backend, b"");
        let preserved_after_sync =
            backend.full_sync.pending_dual_rdb_snapshots[&Token(first)].0 == first_deadline;
        let other_deadline = backend.full_sync.pending_dual_rdb_snapshots[&Token(other)].0;
        let other_future = other_deadline > crate::foundation::io::sys::time::mstime();

        if associate {
            // Exercise the real association release with an existing ENDOFF API.
            let frame = backend
                .replication_runtime
                .take_endoff_for_rdb_candidate_if_unambiguous(
                    ConnectionId(first),
                    true,
                    &mut backend.ctx.replication_mut().backlog,
                )
                .unwrap();
            let endoff = protocol::parse_end_offset(&frame).unwrap();
            assert_eq!(
                endoff.offset,
                backend.ctx.replication().backlog.reported_offset()
            );
            let client_id = backend.connection_registry[Token(first)].client_id();
            boundary_replconf(
                &mut backend,
                main,
                vec![
                    Bytes::from_static(b"set-rdb-client-id"),
                    Bytes::from(client_id.to_string()),
                ],
            );
        } else {
            backend.drive_pending_dual_rdb_snapshots();
        }
        let preserved_after_release = backend
            .full_sync
            .pending_dual_rdb_snapshots
            .get(&Token(other))
            .map(|pending| pending.0)
            == Some(other_deadline);
        let no_other_endoff = backend
            .full_sync
            .pending_dual_rdb_snapshots
            .get(&Token(other))
            .is_some_and(|pending| pending.1.is_empty());
        let waits = backend.replication_runtime.psync_wait_owner_count();
        let no_relay = backend.full_sync.diskless_relay.is_none();
        other_target.set_nonblocking(true).unwrap();
        let mut byte = [0];
        let no_other_wire = matches!(other_target.read(&mut byte), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock);
        observations.push((
            associate,
            preserved_after_sync,
            other_future,
            preserved_after_release,
            no_other_endoff,
            waits,
            no_relay,
            no_other_wire,
        ));
    }
    for (associate, sync_deadline, future, release_deadline, no_endoff, waits, no_relay, no_wire) in
        observations
    {
        assert_eq!(
            (
                sync_deadline,
                future,
                release_deadline,
                no_endoff,
                waits,
                no_relay,
                no_wire
            ),
            (true, true, true, true, usize::from(associate), true, true),
            "different-filter candidate must not alter another cohort; association={associate}"
        );
    }
}

#[test]
fn boundary_dual_same_filter_max_starts_before_deadline() {
    let _fork_guard = crate::framework::commands::server::save::diskless_fork_test_guard();
    let mut backend = ServerWorkerState::new(1).unwrap();
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
    backend
        .ctx
        .config_mut()
        .replication
        .repl_diskless_sync_max_replicas = 2;
    let (_first_main, _first_main_target) = boundary_waiting_main(&mut backend);
    let (_second_main, _second_main_target) = boundary_waiting_main(&mut backend);
    let (first, mut first_target) = boundary_sync_candidate(&mut backend, b"functions");
    assert!(
        backend.full_sync.pending_dual_rdb_snapshots[&Token(first)].0
            > crate::foundation::io::sys::time::mstime()
    );
    let (_second, mut second_target) = boundary_sync_candidate(&mut backend, b"functions");
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    backend.drive_pending_dual_rdb_snapshots();
    let relay_started = backend.full_sync.diskless_relay.is_some();
    let waits = backend.replication_runtime.psync_wait_owner_count();
    backend.abort_diskless_relay_for_dataset_flush();

    assert!(
        !relay_started,
        "filtered same-cohort sync must complete without a pipe relay"
    );
    assert_eq!(waits, 2);
    for target in [&mut first_target, &mut second_target] {
        assert_eq!(
            boundary_read_endoff(target).offset,
            boundary.reported_offset()
        );
    }
}

#[test]
fn boundary_dual_expired_group_starts_with_two_waiting_mains() {
    let _fork_guard = crate::framework::commands::server::save::diskless_fork_test_guard();
    let mut backend = ServerWorkerState::new(1).unwrap();
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
    backend
        .ctx
        .config_mut()
        .replication
        .repl_diskless_sync_max_replicas = 2;
    let (_first_main, _first_main_target) = boundary_waiting_main(&mut backend);
    let (_second_main, _second_main_target) = boundary_waiting_main(&mut backend);
    let (rdb, mut target) = boundary_sync_candidate(&mut backend, b"functions");
    assert_eq!(backend.full_sync.pending_dual_rdb_snapshots.len(), 1);
    // Only time is controlled; admission and ENDOFF ownership remain real.
    backend
        .full_sync
        .pending_dual_rdb_snapshots
        .get_mut(&Token(rdb))
        .unwrap()
        .0 = crate::foundation::io::sys::time::mstime().saturating_sub(1);
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    backend.drive_pending_dual_rdb_snapshots();
    let relay_started = backend.full_sync.diskless_relay.is_some();
    let waits = backend.replication_runtime.psync_wait_owner_count();
    backend.abort_diskless_relay_for_dataset_flush();

    assert!(
        !relay_started,
        "filtered expired cohort must complete without a pipe relay"
    );
    assert_eq!(waits, 1);
    assert_eq!(
        boundary_read_endoff(&mut target).offset,
        boundary.reported_offset()
    );
}
