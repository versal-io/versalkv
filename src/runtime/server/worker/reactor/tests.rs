use super::{server_cron_interval, EventLimits, ReadyEvent, ServerWorkerState};
use crate::engine::command::context::{BlockedCmd, BlockedState};
use crate::engine::distributed::replication::ReplicationOffset;
use crate::foundation::io::sys::time::MonotonicInstant;
use crate::framework::capabilities::connection::Connection;
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::{
    ConnectionId, MasterInfo, PrimaryLinkState, RdbFilterMode, ReplicaConnectionInfo, ReplicaLink,
};
use crate::runtime::server::worker::reactor::blocked_events::{
    BlockedOperation, PendingReplayContinuation,
};
use crate::runtime::server::worker::reactor::queue::EventBatch;
use crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer;
use crate::runtime::server::worker::reactor::{BlockedRoundPhase, LoopControl, TurnTracePoint};
use crate::runtime::server::worker::PendingRemovalOwner;
use bytes::Bytes;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn offset(value: i64) -> ReplicationOffset {
    ReplicationOffset::try_from(value).expect("valid test offset")
}

#[path = "tests/event_model_tests.rs"]
mod event_model_tests;
#[path = "tests/lifecycle_diagnostics_tests.rs"]
mod lifecycle_diagnostics_tests;

#[path = "tests/event_barrier_tests.rs"]
mod event_barrier_tests;

#[path = "tests/source_registry_tests.rs"]
mod source_registry_tests;

#[path = "tests/blocked_pump_tests.rs"]
mod blocked_pump_tests;

#[path = "tests/dispatch_policy_tests.rs"]
mod dispatch_policy_tests;

#[path = "tests/qos_tests.rs"]
mod qos_tests;

#[test]
fn server_cron_interval_uses_c_clamped_hz_bounds() {
    assert_eq!(server_cron_interval(0), Duration::from_secs(1));
    assert_eq!(server_cron_interval(1), Duration::from_secs(1));
    assert_eq!(server_cron_interval(500), Duration::from_millis(2));
    assert_eq!(server_cron_interval(i32::MAX), Duration::from_millis(2));
}

fn bind_runtime_listener(backend: &mut ServerWorkerState) -> (Token, SocketAddr) {
    backend
        .bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .expect("bind runtime listener");
    let token = *backend
        .listeners
        .tcp
        .last()
        .expect("runtime listener token");
    let fd = backend.listeners.inherited_fds[&token.0];
    let port = crate::foundation::io::sys::socket::socket_port_raw(fd, false)
        .expect("read runtime listener port");
    (token, SocketAddr::from(([127, 0, 0, 1], port)))
}

fn connected_test_client(backend: &mut ServerWorkerState) -> (TcpStream, Token) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind client peer");
    let peer_addr = listener.local_addr().expect("client peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect client transport");
    let (client, _) = listener.accept().expect("accept client transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    (client, peer.token)
}

fn queue_pending_request(
    backend: &mut ServerWorkerState,
    client: &mut TcpStream,
    token: Token,
    request: &[u8],
) {
    client.write_all(request).expect("write pending request");
    let ready = backend.transport.wait_readable_for_test(token);
    assert!(ready
        .iter()
        .any(|event| event.token == token && event.readable));
    backend.transport.queue_test_pending_data_event(token);
}

#[test]
fn listener_accept_budget_applies_once_per_event_loop_cycle() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    backend
        .ctx
        .config_mut()
        .server
        .max_new_connections_per_cycle = 2;
    let (listener_token, addr) = bind_runtime_listener(&mut backend);
    let _clients: Vec<_> = (0..5)
        .map(|_| TcpStream::connect(addr).expect("connect pending client"))
        .collect();

    backend.dispatch_transport_event(
        ReadyEvent {
            token: listener_token,
            readable: true,
            writable: false,
            error: false,
            read_hup: false,
        },
        false,
    );
    assert_eq!(backend.connected_clients(), 2);

    assert_eq!(
        backend
            .run_before_poll_phase()
            .expect("run before-poll phase"),
        LoopControl::Continue
    );
    assert_eq!(backend.connected_clients(), 2);
}

#[test]
fn before_poll_records_previous_cycle_before_due_time_events() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    backend.start_event_loop_cycle(Instant::now() - Duration::from_micros(10), 0, 0);
    backend.create_test_time_event(|runtime| {
        assert_eq!(runtime.ctx.stats().eventloop_cycles, 1);
        assert!(runtime.ctx.stats().eventloop_duration_sum > 0);
        runtime
            .ctx
            .stats_mut()
            .incr_cmd_call("previous_cycle_timer");
    });

    assert_eq!(
        backend
            .run_before_poll_phase()
            .expect("run before-poll phase"),
        LoopControl::Continue
    );
    backend.collect_due_time_events();
    assert_eq!(
        backend
            .dispatch_time_phase()
            .expect("dispatch previous-cycle timer"),
        LoopControl::Continue
    );
    assert_eq!(
        backend.ctx.stats().cmd_stats["previous_cycle_timer"].calls,
        1
    );
}

#[test]
fn retained_time_work_forces_a_nonblocking_poll_before_its_next_turn_dispatch() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    backend.event_batch = EventBatch::with_limits(EventLimits {
        max_events_per_phase_per_turn: 1,
        max_same_owner_events_per_turn: 64,
    });
    let executed = Arc::new(AtomicUsize::new(0));
    for _ in 0..2 {
        let executed = Arc::clone(&executed);
        backend.create_test_time_event(move |_| {
            executed.fetch_add(1, Ordering::Relaxed);
        });
    }

    backend.transport.queue_test_transport_event_batch(&[]);
    backend.run_one_turn_for_test().expect("run first turn");
    assert_eq!(executed.load(Ordering::Relaxed), 1);

    assert_eq!(
        backend.next_poll_timeout(MonotonicInstant::now()),
        Some(Duration::ZERO),
        "retained due time work must not wait for unrelated readiness"
    );

    backend.transport.queue_test_transport_event_batch(&[]);
    backend.run_one_turn_for_test().expect("run retained turn");
    assert_eq!(
        executed.load(Ordering::Relaxed),
        2,
        "retained work must run after the second turn's empty poll"
    );
}

#[test]
fn listener_readiness_accepts_only_from_fired_listener() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    backend
        .ctx
        .config_mut()
        .server
        .max_new_connections_per_cycle = 1;
    let (listener_a, addr_a) = bind_runtime_listener(&mut backend);
    let (listener_b, addr_b) = bind_runtime_listener(&mut backend);
    let client_a = TcpStream::connect(addr_a).expect("connect pending client A");
    let peer_a = client_a
        .local_addr()
        .expect("read pending client A address");
    let client_b = TcpStream::connect(addr_b).expect("connect pending client B");
    let peer_b = client_b
        .local_addr()
        .expect("read pending client B address");

    backend.transport.wait_readable_for_test(listener_a);
    backend.dispatch_transport_event(
        ReadyEvent {
            token: listener_a,
            readable: true,
            writable: false,
            error: false,
            read_hup: false,
        },
        false,
    );

    assert_eq!(backend.connected_clients(), 1);
    assert!(backend
        .connection_registry
        .iter()
        .any(|(_, connection)| connection.peer_addr == peer_a));
    assert!(!backend
        .connection_registry
        .iter()
        .any(|(_, connection)| connection.peer_addr == peer_b));

    backend.transport.wait_readable_for_test(listener_b);
    backend.dispatch_transport_event(
        ReadyEvent {
            token: listener_b,
            readable: true,
            writable: false,
            error: false,
            read_hup: false,
        },
        false,
    );

    assert_eq!(backend.connected_clients(), 2);
    assert!(backend
        .connection_registry
        .iter()
        .any(|(_, connection)| connection.peer_addr == peer_b));
}

#[test]
fn before_sleep_dispatches_pending_data_before_due_time_events() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    let (mut client, token) = connected_test_client(&mut backend);
    queue_pending_request(
        &mut backend,
        &mut client,
        token,
        b"*3\r\n$3\r\nSET\r\n$11\r\npending-key\r\n$5\r\nvalue\r\n",
    );
    backend.create_test_time_event(|runtime| {
        assert!(
            runtime.ctx.db_mut(0).unwrap().get(b"pending-key").is_some(),
            "pending transport data must run before due time events"
        );
        runtime.ctx.stats_mut().incr_cmd_call("pending_data_timer");
    });

    backend
        .run_before_poll_phase()
        .expect("run before-poll phase");
    backend.collect_due_time_events();
    assert_eq!(
        backend.dispatch_time_phase().expect("dispatch due timer"),
        LoopControl::Continue
    );
    assert_eq!(backend.ctx.stats().cmd_stats["pending_data_timer"].calls, 1);
}

#[test]
fn before_sleep_dispatches_pending_lpush_before_blocked_timeout() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    let key = Bytes::from_static(b"pending-timeout-list");
    let blocked_token = Token(81);
    let args = crate::engine::command::CommandArgs::from_parts(vec![
        Bytes::from_static(b"BLPOP"),
        key.clone(),
        Bytes::from_static(b"0.001"),
    ]);
    let mut blocked = Connection::new_fake();
    blocked.set_token_for_test(blocked_token);
    blocked.set_custom_reply_enabled(false);
    blocked.begin_nested_command_effects();
    blocked.set_request_bytes_for_test(32);
    blocked.blocked = Some(BlockedState {
        cmd: BlockedCmd::Blpop,
        keys: vec![key.clone()],
        timeout_ms: 1,
        start_time_ms: crate::foundation::io::sys::time::mstime().saturating_sub(10),
        orig_args: args,
        req_bytes: 32,
        orig_wrapped: None,
    });
    backend.connection_registry.insert(blocked);
    let blocked_session = backend.connection_registry[blocked_token].session_id();
    backend.ctx.blocking_registry_mut().register_client(
        0,
        std::slice::from_ref(&key),
        blocked_session,
    );

    let (mut client, token) = connected_test_client(&mut backend);
    queue_pending_request(
        &mut backend,
        &mut client,
        token,
        b"*3\r\n$5\r\nLPUSH\r\n$20\r\npending-timeout-list\r\n$5\r\nvalue\r\n",
    );
    backend.writes.ordinary.begin_turn();

    backend
        .run_before_poll_phase()
        .expect("run before-poll phase");

    assert!(
        backend
            .ctx
            .db(0)
            .and_then(|db| db.get_no_expire(&key))
            .is_none(),
        "pending LPUSH must wake BLPOP before its timeout reply wins"
    );
}

#[test]
fn poll_offloads_only_while_worker_io_is_pending() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind IO peer");
    let peer_addr = listener.local_addr().expect("IO peer address");
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    backend.ctx.config_mut().server.io_threads = 2;
    backend.ctx.config_mut().server.io_threads_always_active = true;
    backend.configure_io_threads_from_config().unwrap();
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect IO transport");
    let (mut client, _) = listener.accept().expect("accept IO transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    client.write_all(b"pending worker read").unwrap();
    backend.io_threads_after_poll(1);
    assert!(backend.try_submit_io_read(peer.token, false));

    assert!(backend.poll_ready().expect("offloaded poll").is_some());
    assert_eq!(backend.transport.poll_generation_for_test(), 1);
    assert_eq!(backend.ctx.io_thread_info().polls_processed, 1);
    assert_eq!(backend.ctx.io_thread_info().polls_pending, 0);

    while backend.io_threads.pool().has_pending_io() {
        backend
            .wait_for_io_completion_for_test(Duration::from_secs(1))
            .expect("drain worker read");
    }
    backend.transport.queue_test_transport_event_batch(&[]);
    assert!(backend.poll_ready().expect("main-thread poll").is_some());
    assert_eq!(
        backend.transport.poll_generation_for_test(),
        1,
        "a poll without pending IO responses must stay on the main thread"
    );
}

#[test]
fn before_sleep_recomputes_dirty_cluster_state() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let mut state =
        crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16_379);
    state.set_require_full_coverage(false);
    state.add_slot(7).unwrap();
    assert!(state.topology_state_is_dirty());
    *backend.ctx.cluster_state_mut() = Some(Box::new(state));

    backend.recompute_cluster_state_before_sleep();

    let state = backend.ctx.cluster_state().as_deref().unwrap();
    assert!(!state.topology_state_is_dirty());
    assert_eq!(state.info_str().lines().next(), Some("cluster_state:ok"));
}

fn connected_streaming_slot_export(
    backend: &mut ServerWorkerState,
) -> (
    TcpStream,
    usize,
    crate::engine::distributed::cluster::migration::MigrationJobId,
) {
    use crate::engine::distributed::cluster::migration::{
        ExportPhase, MigrationJob, MigrationJobId, MigrationOperation, MigrationPhase,
        SlotMigrationConnectionRole, SlotRanges,
    };
    use crate::engine::distributed::cluster::{ClusterState, NodeId, SlotRange};

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind export peer");
    let peer_addr = listener.local_addr().expect("export peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect export transport");
    let (target, _) = listener.accept().expect("accept export transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .expect("valid migration id");
    let mut job = MigrationJob::new(
        id.clone(),
        NodeId::parse(state.myself.name.as_bytes()).expect("local node id"),
        NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").expect("target node id"),
        SlotRanges::new(vec![SlotRange::new(1, 1).expect("slot range")]).expect("slot ranges"),
        MigrationOperation::Export,
        1,
    );
    job.phase = MigrationPhase::Export(ExportPhase::Streaming);
    state.migrations.start(job).expect("start migration");
    *backend.ctx.cluster_state_mut() = Some(Box::new(state));
    let connection_token = peer.token.0;
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .slot_migration_role = Some(SlotMigrationConnectionRole::Export(id.clone()));
    backend.slot_migration_runtime.register_export(
        crate::framework::distributed::migration::SlotExportLink::new(id.clone(), peer.token.0),
    );
    backend
        .transport
        .set_interest(
            peer.token,
            crate::foundation::io::transport::Interest::Readable,
        )
        .expect("arm established export reads");
    (target, peer.token.0, id)
}

fn pending_cluster_bus_connection(backend: &mut ServerWorkerState) -> (TcpStream, usize) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind cluster-bus test listener");
    let peer_addr = listener
        .local_addr()
        .expect("get cluster-bus test listener address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect cluster-bus test transport");
    let (server_stream, _) = listener
        .accept()
        .expect("accept cluster-bus test transport");
    backend.cluster_bus.register_incoming(peer.token, peer_addr);
    backend
        .transport
        .deregister_for_test(peer.token)
        .expect("deregister cluster-bus test transport");
    backend.remove_cluster_bus_connection_without_retry(peer.token);

    (server_stream, peer.token.0)
}

#[test]
fn writable_slot_export_event_invalidates_current_turn_continuation() {
    use crate::engine::distributed::cluster::migration::{
        MigrationJobId, SlotMigrationConnectionRole,
    };

    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind export peer");
    let peer_addr = listener.local_addr().expect("export peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect export transport");
    let (_export_target, _) = listener.accept().expect("accept export transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    let id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .expect("valid migration id");
    let connection_token = peer.token.0;
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .slot_migration_role = Some(SlotMigrationConnectionRole::Export(id.clone()));
    backend.slot_migration_runtime.register_export(
        crate::framework::distributed::migration::SlotExportLink::new(id, peer.token.0),
    );
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .queue_replication_reply(vec![b'm'; 3 * 64 * 1024]);
    backend.schedule_runtime_runnable_next_turn(
        crate::runtime::server::worker::reactor::RunnableKey::SlotExportWrite(peer.token),
    );
    backend.event_batch.begin_next_turn();
    backend.writes.begin_turn();

    backend.dispatch_transport_event(
        ReadyEvent {
            token: peer.token,
            readable: false,
            writable: true,
            error: false,
            read_hup: false,
        },
        false,
    );
    backend.drive_pending_slot_export_writes();

    assert_eq!(
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .output
            .replication_bytes,
        2 * 64 * 1024,
        "real writable readiness and its queued continuation share one turn budget"
    );
    backend.event_batch.begin_next_turn();
    backend.writes.begin_turn();
    backend.drive_pending_slot_export_writes();
    assert_eq!(
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .output
            .replication_bytes,
        64 * 1024
    );
}

#[test]
fn pending_export_phase_runs_once_after_all_turn_producers() {
    const WRITE_BUDGET: usize = 64 * 1024;
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let (mut target, token_value, id) = connected_streaming_slot_export(&mut backend);
    let early = vec![b'e'; WRITE_BUDGET / 2];
    let late = vec![b'l'; WRITE_BUDGET];
    let early_id = id.clone();
    backend.create_test_time_event(move |runtime| {
        assert!(runtime.queue_slot_migration_output(&early_id, token_value, early));
    });
    backend.create_test_time_event(move |runtime| {
        assert!(runtime.queue_slot_migration_output(&id, token_value, late));
        runtime.ctx.set_shutdown_requested(true);
    });

    let backend = crate::runtime::server::ServerRuntime::run_owned_for_test(backend)
        .expect("run one event-loop iteration");

    target
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set export target read timeout");
    let mut received = vec![0_u8; WRITE_BUDGET];
    target
        .read_exact(&mut received)
        .expect("final pending phase must write one budget");
    assert!(received[..WRITE_BUDGET / 2]
        .iter()
        .all(|byte| *byte == b'e'));
    assert!(received[WRITE_BUDGET / 2..]
        .iter()
        .all(|byte| *byte == b'l'));
    let connection_token = token_value;
    assert_eq!(
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .output
            .replication_bytes,
        WRITE_BUDGET / 2,
        "one final pending phase must apply one shared turn budget"
    );
}

#[test]
fn blocked_before_sleep_drains_current_export_work_before_next_turn() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let (mut target, token_value, _) = connected_streaming_slot_export(&mut backend);
    backend.writes.slot_exports.begin_turn();
    let reply = b"blocked-before-sleep".to_vec();
    assert!(backend.queue_client_reply(Token(token_value), reply.clone()));

    backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(usize::MAX),
    });

    target
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set export target read timeout");
    let mut received = vec![0_u8; reply.len()];
    target
        .read_exact(&mut received)
        .expect("blocked before-sleep phase must write current export output");
    assert_eq!(received, reply);
    assert!(!backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::SlotExportWrite(Token(token_value))
    ));
    backend.drive_pending_slot_export_writes();
    backend.event_batch.begin_next_turn();
    backend.writes.begin_turn();
    assert!(!backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::SlotExportWrite(Token(token_value))
    ));
}

#[test]
fn blocked_pending_drain_preserves_phase_for_late_unserviced_export() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let (mut target, token_value, _) = connected_streaming_slot_export(&mut backend);
    backend.writes.slot_exports.begin_turn();
    backend.transport.queue_test_transport_event_batch(&[]);

    backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(usize::MAX),
    });

    let reply = b"late-blocked-producer".to_vec();
    assert!(backend.queue_client_reply(Token(token_value), reply.clone()));
    assert!(
        backend.event_batch.contains_runnable(
            crate::runtime::server::worker::reactor::RunnableKey::SlotExportWrite(Token(
                token_value
            ))
        ),
        "an unserviced producer after a blocked drain must remain eligible this turn"
    );
    target
        .set_nonblocking(true)
        .expect("set export target nonblocking");
    let mut byte = [0_u8; 1];
    let error = target
        .read(&mut byte)
        .expect_err("late producer must remain queued before the second blocked boundary");
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    backend.transport.queue_test_transport_event_batch(&[]);

    backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(usize::MAX),
    });

    target
        .set_nonblocking(false)
        .expect("restore blocking export target");
    target
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set export target read timeout");
    let mut received = vec![0_u8; reply.len()];
    target
        .read_exact(&mut received)
        .expect("second blocked boundary must drain the late producer");
    assert_eq!(received, reply);
}

#[test]
fn repeated_blocked_pending_drains_share_one_export_budget() {
    const WRITE_BUDGET: usize = 64 * 1024;
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let (mut target, token_value, id) = connected_streaming_slot_export(&mut backend);
    backend.writes.slot_exports.begin_turn();
    assert!(backend.queue_slot_migration_output(
        &id,
        token_value,
        vec![b'm'; WRITE_BUDGET + WRITE_BUDGET / 2],
    ));
    backend.transport.queue_test_transport_event_batch(&[]);

    backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(usize::MAX),
    });
    backend.transport.queue_test_transport_event_batch(&[]);
    backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(usize::MAX),
    });

    target
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set export target read timeout");
    let mut received = vec![0_u8; WRITE_BUDGET];
    target
        .read_exact(&mut received)
        .expect("first blocked drain must write one budget");
    assert!(received.iter().all(|byte| *byte == b'm'));
    let connection_token = token_value;
    assert_eq!(
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .output
            .replication_bytes,
        WRITE_BUDGET / 2,
        "a repeated blocked boundary must not grant a second budget"
    );
    assert_eq!(
        backend
            .writes
            .slot_exports
            .remaining_budget(Token(token_value), WRITE_BUDGET),
        0
    );
}

#[test]
fn slot_snapshot_source_runs_before_later_shutdown_readiness() {
    use crate::engine::distributed::cluster::migration::{
        ExportPhase, MigrationJob, MigrationJobId, MigrationOperation, MigrationPhase,
        SlotMigrationConnectionRole, SlotRanges,
    };
    use crate::engine::distributed::cluster::{ClusterState, NodeId, SlotRange};

    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);

    let export_listener = TcpListener::bind("127.0.0.1:0").expect("bind export peer");
    let export_addr = export_listener.local_addr().expect("export peer address");
    let export_peer = backend
        .transport
        .connect_tcp(export_addr)
        .expect("connect export transport");
    let (_export_target, _) = export_listener.accept().expect("accept export transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: export_peer.token,
        peer_addr: export_addr,
    });
    let export_slab = export_peer.token.0;

    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .expect("valid migration id");
    let mut job = MigrationJob::new(
        id.clone(),
        NodeId::parse(state.myself.name.as_bytes()).expect("local node id"),
        NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").expect("target node id"),
        SlotRanges::new(vec![SlotRange::new(1, 1).expect("slot range")]).expect("slot ranges"),
        MigrationOperation::Export,
        1,
    );
    job.phase = MigrationPhase::Export(ExportPhase::Snapshotting);
    state.migrations.start(job).expect("start migration");
    *backend.ctx.cluster_state_mut() = Some(Box::new(state));
    backend.connection_registry[crate::framework::capabilities::io::Token(export_slab)]
        .slot_migration_role = Some(SlotMigrationConnectionRole::Export(id.clone()));
    backend.slot_migration_runtime.register_export(
        crate::framework::distributed::migration::SlotExportLink::new(
            id.clone(),
            export_peer.token.0,
        ),
    );

    let (snapshot, writer) =
        crate::foundation::io::sys::fd::pipe_with_background_payload(vec![b'x'; 32 * 1024])
            .expect("create background snapshot source");
    let source = backend
        .install_runtime_source(
            crate::foundation::io::transport::OwnedPollSource::readable(snapshot),
            crate::runtime::server::worker::reactor::sources::SourceOwner::InternalPipe(
                crate::runtime::server::worker::reactor::sources::InternalPipeOwner::SlotSnapshot {
                    job_id: id.clone(),
                },
            ),
        )
        .expect("install snapshot source");
    backend.slot_migration_snapshot =
        Some(crate::runtime::server::worker::distributed::migration::SlotSnapshotRelay {
            job_id: id.clone(),
            pid: i32::MAX,
            source: Some(source),
            completion_state:
                crate::runtime::server::worker::distributed::migration::SlotSnapshotCompletionState::default(),
        });

    let shutdown_listener = TcpListener::bind("127.0.0.1:0").expect("bind shutdown peer");
    let shutdown_addr = shutdown_listener
        .local_addr()
        .expect("shutdown peer address");
    let shutdown_peer = backend
        .transport
        .connect_tcp(shutdown_addr)
        .expect("connect shutdown transport");
    let (mut shutdown_client, _) = shutdown_listener
        .accept()
        .expect("accept shutdown transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: shutdown_peer.token,
        peer_addr: shutdown_addr,
    });
    shutdown_client
        .write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n")
        .expect("send shutdown");
    backend
        .transport
        .wait_readable_for_test(shutdown_peer.token);
    backend.transport.wait_readable_for_test(source.token);
    backend.transport.queue_test_transport_event_batch(&[
        (source.token, true, false, false, false),
        (shutdown_peer.token, true, false, false, false),
    ]);

    let (_cycle_start, events) = backend
        .poll_ready()
        .expect("poll shutdown readiness")
        .expect("shutdown readiness batch");
    backend.dispatch_ready_batch(events);
    assert!(
        backend.connection_registry[crate::framework::capabilities::io::Token(export_slab)]
            .has_pending_output(),
        "slot snapshot source event must run before later shutdown readiness"
    );
    assert!(backend.ctx.is_shutdown_requested());
    drop(backend);
    crate::foundation::io::sys::fd::join_background_pipe_writer_after_reader_close(writer);
}

#[test]
fn terminal_shutdown_from_pending_read_skips_later_continuations() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind shutdown peer");
    let peer_addr = listener.local_addr().expect("shutdown peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect shutdown transport");
    let (mut client, _) = listener.accept().expect("accept shutdown transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    client
        .write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n")
        .expect("send shutdown");
    backend.transport.wait_readable_for_test(peer.token);
    backend.transport.queue_test_transport_event_batch(&[]);
    backend.schedule_runtime_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientRead(peer.token),
    );
    backend.schedule_runtime_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::SlotExportWrite(Token(usize::MAX)),
    );

    let backend = crate::runtime::server::ServerRuntime::run_owned_for_test(backend)
        .expect("terminal shutdown exits event loop");

    assert!(
        backend.event_batch.contains_runnable(
            crate::runtime::server::worker::reactor::RunnableKey::SlotExportWrite(Token(
                usize::MAX
            ))
        ),
        "terminal shutdown must stop before later continuation classes"
    );
}

#[test]
fn blocked_event_pump_stops_after_terminal_shutdown() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);

    let shutdown_listener = TcpListener::bind("127.0.0.1:0").expect("bind shutdown peer");
    let shutdown_addr = shutdown_listener
        .local_addr()
        .expect("shutdown peer address");
    let shutdown_peer = backend
        .transport
        .connect_tcp(shutdown_addr)
        .expect("connect shutdown transport");
    let (mut shutdown_client, _) = shutdown_listener
        .accept()
        .expect("accept shutdown transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: shutdown_peer.token,
        peer_addr: shutdown_addr,
    });
    shutdown_client
        .write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n")
        .expect("send shutdown");
    backend
        .transport
        .wait_readable_for_test(shutdown_peer.token);

    let other_listener = TcpListener::bind("127.0.0.1:0").expect("bind other peer");
    let other_addr = other_listener.local_addr().expect("other peer address");
    let other_peer = backend
        .transport
        .connect_tcp(other_addr)
        .expect("connect other transport");
    let (_other_client, _) = other_listener.accept().expect("accept other transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: other_peer.token,
        peer_addr: other_addr,
    });
    backend.transport.queue_test_transport_event_batch(&[(
        shutdown_peer.token,
        true,
        false,
        false,
        false,
    )]);
    backend.clear_blocked_round_trace_for_test();
    backend.transport.queue_test_transport_event_batch(&[(
        other_peer.token,
        false,
        false,
        false,
        true,
    )]);

    backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(usize::MAX),
    });

    assert!(backend.ctx.is_shutdown_requested());
    assert_eq!(
        backend.blocked_round_trace_for_test().last(),
        Some(&BlockedRoundPhase::FinalCompletions),
        "a polled shutdown must still execute the shared round epilogue",
    );
    assert_eq!(
        backend.blocked_events.depth(),
        0,
        "hosted scope must unwind once"
    );
    assert!(
        backend.transport.is_registered(other_peer.token),
        "terminal shutdown must stop the blocked pump before its next batch"
    );
}

#[test]
fn blocked_event_pump_pending_shutdown_runs_final_completion_epilogue() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind shutdown peer");
    let peer_addr = listener.local_addr().expect("shutdown peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect shutdown transport");
    let (mut client, _) = listener.accept().expect("accept shutdown transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    client
        .write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n")
        .expect("send shutdown");
    backend.transport.wait_readable_for_test(peer.token);
    backend.transport.queue_test_pending_data_event(peer.token);
    backend.transport.queue_test_transport_event_batch(&[]);
    backend.clear_blocked_round_trace_for_test();

    backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(usize::MAX),
    });

    assert!(backend.ctx.is_shutdown_requested());
    assert_eq!(
        backend.blocked_round_trace_for_test().last(),
        Some(&BlockedRoundPhase::FinalCompletions),
        "a pending-data shutdown must still execute the shared round epilogue",
    );
    assert_eq!(
        backend.blocked_events.depth(),
        0,
        "hosted scope must unwind once"
    );
}

fn run_failed_fsync_pending_pump_child(
    client_address: SocketAddr,
    control_address: SocketAddr,
    pump: &str,
) {
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
    let ready = backend.transport.wait_readable_for_test(peer.token);
    assert!(ready
        .iter()
        .any(|event| event.token == peer.token && event.readable));
    backend.transport.queue_test_pending_data_event(peer.token);
    backend.transport.queue_test_transport_event_batch(&[]);
    backend.writes.ordinary.begin_turn();

    match pump {
        "loading" => {
            backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
                main: ConnectionId(usize::MAX),
            });
        }
        "command" => backend.process_events_while_command_blocked(Token(usize::MAX)),
        _ => panic!("unknown blocked pump {pump}"),
    }
    panic!("appendfsync always unexpectedly survived /dev/null fsync");
}

#[test]
fn blocked_pumps_never_expose_pending_reply_before_failed_fsync() {
    const CHILD_MARKER: &str = "VERSAL_PENDING_AOF_FAILURE_CHILD";
    const TEST_NAME: &str = concat!(
        "runtime::server::worker::reactor::tests::",
        "blocked_pumps_never_expose_pending_reply_before_failed_fsync"
    );
    if std::env::var_os(CHILD_MARKER).is_some() {
        let client_address = std::env::var("VERSAL_PENDING_AOF_CLIENT_ADDRESS")
            .expect("child client address")
            .parse()
            .expect("valid child client address");
        let control_address = std::env::var("VERSAL_PENDING_AOF_CONTROL_ADDRESS")
            .expect("child control address")
            .parse()
            .expect("valid child control address");
        let pump = std::env::var("VERSAL_PENDING_AOF_PUMP").expect("blocked pump kind");
        run_failed_fsync_pending_pump_child(client_address, control_address, &pump);
        return;
    }

    for pump in ["loading", "command"] {
        let client_listener = TcpListener::bind("127.0.0.1:0").expect("bind client listener");
        let control_listener = TcpListener::bind("127.0.0.1:0").expect("bind control listener");
        let mut child = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args(["--exact", TEST_NAME, "--nocapture"])
            .env(CHILD_MARKER, "1")
            .env(
                "VERSAL_PENDING_AOF_CLIENT_ADDRESS",
                client_listener.local_addr().unwrap().to_string(),
            )
            .env(
                "VERSAL_PENDING_AOF_CONTROL_ADDRESS",
                control_listener.local_addr().unwrap().to_string(),
            )
            .env("VERSAL_PENDING_AOF_PUMP", pump)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn pending-AOF failure child");
        let (mut client, _) = client_listener.accept().expect("accept child client");
        let (mut control, _) = control_listener.accept().expect("accept child control");
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
            .expect("write pending SET");
        control.write_all(&[1]).expect("release child");

        let child_status = child.wait().expect("wait for pending-AOF child");
        assert_eq!(child_status.code(), Some(1), "pump={pump}");
        let mut reply = Vec::new();
        client.read_to_end(&mut reply).expect("read client EOF");
        assert!(
            reply.is_empty(),
            "pump={pump} reply crossed failed fsync: {reply:?}"
        );
    }
}

#[test]
fn blocked_service_defers_protected_pending_readiness() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind protected peer");
    let peer_addr = listener.local_addr().expect("protected peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect protected transport");
    let (_client, _) = listener.accept().expect("accept protected transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    backend.transport.queue_test_pending_data_event(peer.token);
    backend.transport.queue_test_transport_event_batch(&[]);

    let result = backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(peer.token.0),
    });

    assert_eq!(result.deferred_events, 1);
    let deferred = backend
        .blocked_events
        .pop_front_deferred()
        .expect("protected pending readability must be deferred");
    assert_eq!(deferred.token, peer.token);
    assert!(deferred.readable);
    assert!(backend.blocked_events.pop_front_deferred().is_none());
}

#[test]
fn blocked_service_counts_live_nonprotected_hup_as_useful_work() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let protected_main = ConnectionId(41);
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ordinary-client listener");
    let peer_addr = listener.local_addr().expect("get ordinary-client address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect ordinary client");
    let (_client, _) = listener.accept().expect("accept ordinary client");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    backend
        .transport
        .queue_test_transport_event_batch(&[(peer.token, false, false, true, false)]);
    backend.transport.queue_test_transport_event_batch(&[(
        Token(protected_main.0),
        true,
        false,
        false,
        false,
    )]);

    let result = backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: protected_main,
    });

    assert!(
        (2..=crate::runtime::server::worker::reactor::BLOCKED_EVENT_SERVICE_ROUNDS)
            .contains(&result.poll_rounds)
    );
    assert_eq!(result.dispatched_events, 1);
    assert_eq!(result.deferred_events, 1);
    assert_eq!(backend.blocked_events.depth(), 0);
    assert!(!backend.transport.is_registered(peer.token));
}

#[test]
fn blocked_service_dispatches_writable_only_for_a_read_protected_token() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind replica-main listener");
    let peer_addr = listener.local_addr().expect("get replica-main address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect replica main");
    let (mut primary, _) = listener.accept().expect("accept replica main");
    primary
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set replica-main read timeout");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
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
    assert!(backend.queue_replica_loading_newline(main));
    backend
        .transport
        .queue_test_transport_event_batch(&[(peer.token, false, true, false, false)]);

    let result = backend.process_events_while_blocked(BlockedOperation::ClassicRdb { main });

    assert_eq!(result.dispatched_events, 1);
    assert_eq!(result.deferred_events, 0);
    assert_eq!(backend.blocked_events.depth(), 0);
    assert!(backend.blocked_events.pop_front_deferred().is_none());
    let mut newline = [0_u8; 1];
    primary
        .read_exact(&mut newline)
        .expect("writable readiness must flush the loading newline");
    assert_eq!(&newline, b"\n");
}

#[test]
fn ready_upstream_replication_is_dispatched_before_ordinary_clients() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = ConnectionId(42);
    backend
        .replication_runtime
        .set_replica_link(ReplicaLink::new(
            main,
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        ));
    let mut events = vec![
        (Token(7), true, false, false, false),
        (Token(main.0), true, false, false, false),
    ];

    backend.prioritize_upstream_readiness(&mut events);

    assert_eq!(events[0].0, Token(main.0));
    assert_eq!(events[1].0, Token(7));
}

#[test]
fn ready_dual_rdb_channel_is_dispatched_before_main_replication_stream() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = ConnectionId(42);
    let rdb = ConnectionId(43);
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
        .replication_runtime
        .set_replica_rdb_link(rdb, main)
        .expect("dual RDB link can be registered");
    let mut events = vec![
        (Token(7), true, false, false, false),
        (Token(main.0), true, false, false, false),
        (Token(rdb.0), true, false, false, false),
    ];

    backend.prioritize_upstream_readiness(&mut events);

    assert_eq!(events[0].0, Token(rdb.0));
    assert_eq!(events[1].0, Token(main.0));
    assert_eq!(events[2].0, Token(7));
}

#[test]
fn blocked_service_dispatches_writable_while_deferring_protected_read() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind replica-main listener");
    let peer_addr = listener.local_addr().expect("get replica-main address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect replica main");
    let (mut primary, _) = listener.accept().expect("accept replica main");
    primary
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set replica-main read timeout");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
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
    assert!(backend.queue_replica_loading_newline(main));
    backend
        .transport
        .queue_test_transport_event_batch(&[(peer.token, true, true, false, false)]);

    let result = backend.process_events_while_blocked(BlockedOperation::ClassicRdb { main });

    assert_eq!(result.outcomes.deferred, 1);
    assert_eq!(result.deferred_events, 1);
    let deferred = backend
        .blocked_events
        .pop_front_deferred()
        .expect("protected read must remain deferred");
    assert!(deferred.readable);
    assert!(
        !deferred.writable,
        "writable readiness was already serviced"
    );
    let mut newline = [0_u8; 1];
    primary
        .read_exact(&mut newline)
        .expect("combined writable readiness must flush the loading newline");
    assert_eq!(&newline, b"\n");
}

#[test]
fn normal_dispatch_defers_replica_main_read_while_pending_replay_exists() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind replica-main listener");
    let peer_addr = listener
        .local_addr()
        .expect("get replica-main listener address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect replica main");
    let (mut primary, _) = listener.accept().expect("accept replica main");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    let main = ConnectionId(peer.token.0);
    let link = ReplicaLink::streaming(
        main,
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "0123456789abcdef0123456789abcdef01234567".to_string(),
        offset(100),
    );
    let session_id = link.session_id();
    backend.replication_runtime.set_replica_link(link);
    backend.pending_replication_replay = Some(PendingReplayContinuation::new_for_test(
        main,
        session_id,
        crate::foundation::io::sys::time::mstime(),
    ));
    primary
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .expect("write a live replication frame");

    backend.dispatch_transport_event(
        ReadyEvent {
            token: peer.token,
            readable: true,
            writable: false,
            error: false,
            read_hup: false,
        },
        false,
    );

    let deferred = backend
        .blocked_events
        .pop_front_deferred()
        .expect("a main-channel read already returned by poll must wait for replay");
    assert_eq!(deferred.token, peer.token);
    assert!(deferred.readable);
    assert_eq!(
        backend
            .replication_runtime
            .replica_link()
            .unwrap()
            .received_position(),
        offset(100),
        "live input must not extend the pending buffer ahead of replay"
    );
}

#[test]
fn listener_error_removes_all_owner_bookkeeping() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend
        .bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .expect("bind TCP listener");
    let token = backend.listeners.tcp[0];
    let inherited_fd = backend.ctx.listener_fds()[0];

    backend.dispatch_transport_event(
        ReadyEvent {
            token,
            readable: false,
            writable: false,
            error: true,
            read_hup: false,
        },
        false,
    );

    assert!(!backend.listeners.all.contains(&token));
    assert!(!backend.listeners.tcp.contains(&token));
    assert!(backend.listeners.bind_addrs.is_empty());
    assert!(!backend.ctx.listener_fds().contains(&inherited_fd));
    assert!(!backend.transport.is_registered(token));
    assert!(!backend.pending_transport_removals.contains_key(&token));
}

#[test]
fn listener_readable_hup_defers_deregistration_with_listener_owner() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend
        .bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .expect("bind TCP listener");
    let token = backend.listeners.tcp[0];
    let inherited_fd = backend.ctx.listener_fds()[0];
    backend
        .transport
        .deregister_for_test(token)
        .expect("inject listener deregistration failure");

    backend.dispatch_transport_event(
        ReadyEvent {
            token,
            readable: true,
            writable: false,
            error: false,
            read_hup: true,
        },
        false,
    );

    assert!(!backend.listeners.all.contains(&token));
    assert!(!backend.listeners.tcp.contains(&token));
    assert!(backend.listeners.bind_addrs.is_empty());
    assert!(!backend.ctx.listener_fds().contains(&inherited_fd));
    assert_eq!(
        backend
            .pending_transport_removals
            .get(&token)
            .map(|removal| removal.owner_kind),
        Some(PendingRemovalOwner::Listener)
    );
    assert!(backend.transport.is_registered(token));
}

#[test]
fn loading_newline_interest_failure_tears_down_master_session() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind replica-main listener");
    let peer_addr = listener.local_addr().expect("get replica-main address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect replica main");
    let (_primary, _) = listener.accept().expect("accept replica main");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
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
        .replication_runtime
        .set_replica_main_read_paused(true);
    backend
        .transport
        .deregister_for_test(peer.token)
        .expect("inject loading-newline rearm failure");

    assert!(!backend.queue_replica_loading_newline(main));

    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(peer.token.0)));
    assert!(backend.replication_runtime.replica_link().is_none());
}

#[test]
fn before_sleep_flush_runs_after_due_time_events_produce_replication() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    backend.ctx.replication_mut().backlog.activate();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind replica peer");
    let peer_addr = listener.local_addr().expect("replica peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect replica transport");
    let (mut replica_stream, _) = listener.accept().expect("accept replica transport");
    replica_stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set replica read timeout");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(peer.token.0),
        ReplicaConnectionInfo {
            ip: peer_addr.ip().to_string(),
            port: peer_addr.port(),
            capability: Vec::new(),
            rdb_filter_mode: RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("registered replica link")
        .set_state(PrimaryLinkState::Streaming);

    backend.create_test_time_event(|runtime| {
        runtime.propagate_to_replicas(b"timer-frame");
        runtime.ctx.set_shutdown_requested(true);
    });

    let _server = crate::runtime::server::ServerRuntime::run_owned_for_test(backend)
        .expect("run one event-loop iteration");

    let mut received = [0_u8; 11];
    replica_stream
        .read_exact(&mut received)
        .expect("timer-produced replication must flush before the next poll");
    assert_eq!(&received, b"timer-frame");
}

#[test]
fn one_turn_trace_follows_before_poll_poll_ready_then_time_order() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    backend.transport.queue_test_transport_event_batch(&[]);

    let trace = backend
        .run_one_turn_for_test()
        .expect("run one event-loop turn");

    assert_eq!(
        trace,
        vec![
            TurnTracePoint::BeforePoll,
            TurnTracePoint::Poll,
            TurnTracePoint::AfterPoll,
            TurnTracePoint::ReadyDispatch,
            TurnTracePoint::TimeDispatch,
        ]
    );
}

#[test]
fn one_turn_trace_dispatches_ready_work_before_due_timers() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    let (mut client, token) = connected_test_client(&mut backend);
    client
        .write_all(b"*3\r\n$3\r\nSET\r\n$10\r\nturn-order\r\n$5\r\nvalue\r\n")
        .expect("write SET request");
    backend.transport.wait_readable_for_test(token);
    backend
        .transport
        .queue_test_transport_event_batch(&[(token, true, false, false, false)]);
    backend.create_test_time_event(|runtime| {
        assert!(
            runtime.ctx.db_mut(0).unwrap().get(b"turn-order").is_some(),
            "readiness effect must happen before due timers",
        );
        runtime
            .ctx
            .stats_mut()
            .incr_cmd_call("timer_after_ready_turn_trace");
    });

    let trace = backend
        .run_one_turn_for_test()
        .expect("run one event-loop turn");

    client
        .set_nonblocking(false)
        .expect("restore blocking reads");
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("bound reply wait");
    let mut reply = [0; 5];
    client.read_exact(&mut reply).expect("read SET reply");
    assert_eq!(&reply, b"+OK\r\n");

    let ready_index = trace
        .iter()
        .position(|point| *point == TurnTracePoint::ReadyDispatch)
        .expect("ready dispatch trace");
    let time_index = trace
        .iter()
        .position(|point| *point == TurnTracePoint::TimeDispatch)
        .expect("time dispatch trace");
    assert!(ready_index < time_index, "timers must run after readiness");
    assert_eq!(
        backend.ctx.stats().cmd_stats["timer_after_ready_turn_trace"].calls,
        1
    );
}

#[test]
fn ready_phase_counts_replication_replay_deferral_as_deferred() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind replica-main listener");
    let peer_addr = listener
        .local_addr()
        .expect("get replica-main listener address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect replica main");
    let (_primary, _) = listener.accept().expect("accept replica main");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    let main = ConnectionId(peer.token.0);
    let link = ReplicaLink::streaming(
        main,
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "0123456789abcdef0123456789abcdef01234567".to_string(),
        offset(100),
    );
    let session_id = link.session_id();
    backend.replication_runtime.set_replica_link(link);
    backend.pending_replication_replay = Some(PendingReplayContinuation::new_for_test(
        main,
        session_id,
        crate::foundation::io::sys::time::mstime(),
    ));
    let before = (
        backend.ctx.stats().eventloop_events_handled,
        backend.ctx.stats().eventloop_events_deferred,
        backend.ctx.stats().eventloop_events_owner_closed,
    );
    backend.collect_after_poll_events(Some((
        Instant::now(),
        vec![
            crate::runtime::server::worker::reactor::transport::RuntimeEvent {
                token: peer.token,
                readable: true,
                writable: false,
                read_hup: false,
                error: false,
            },
        ],
    )));

    assert_eq!(
        backend
            .dispatch_ready_phase(crate::runtime::server::worker::reactor::DispatchContext::Normal)
            .expect("dispatch ready phase"),
        LoopControl::Continue
    );

    assert_eq!(
        backend.ctx.stats().eventloop_events_handled,
        before.0,
        "a deferred replay wait must not count as handled"
    );
    assert_eq!(
        backend.ctx.stats().eventloop_events_deferred,
        before.1 + 1,
        "a replay-blocked readiness must count as deferred"
    );
    assert_eq!(
        backend.ctx.stats().eventloop_events_owner_closed,
        before.2,
        "a deferred replay wait must not count as owner-closed"
    );
}

#[test]
fn blocked_dispatch_reports_replication_replay_readiness_as_deferred() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind replica-main listener");
    let peer_addr = listener
        .local_addr()
        .expect("get replica-main listener address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect replica main");
    let (_primary, _) = listener.accept().expect("accept replica main");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    let main = ConnectionId(peer.token.0);
    let link = ReplicaLink::streaming(
        main,
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "0123456789abcdef0123456789abcdef01234567".to_string(),
        offset(100),
    );
    let session_id = link.session_id();
    backend.replication_runtime.set_replica_link(link);
    backend.pending_replication_replay = Some(PendingReplayContinuation::new_for_test(
        main,
        session_id,
        crate::foundation::io::sys::time::mstime(),
    ));

    backend.transport.queue_test_pending_data_event(peer.token);
    backend.transport.queue_test_transport_event_batch(&[]);

    let result = backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(usize::MAX),
    });

    assert_eq!(result.deferred_events, 1);
}

#[test]
fn unified_dispatch_records_ready_and_time_outcomes_once_with_latency() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    let (mut client, token) = connected_test_client(&mut backend);
    client
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .expect("write readiness effect");
    backend.transport.wait_readable_for_test(token);
    backend.collect_after_poll_events(Some((
        Instant::now(),
        vec![
            crate::runtime::server::worker::reactor::transport::RuntimeEvent {
                token,
                readable: true,
                writable: false,
                read_hup: false,
                error: false,
            },
        ],
    )));
    backend.create_test_time_event(|runtime| {
        assert_eq!(runtime.ctx.stats().cmd_stats["ping"].calls, 1);
        std::thread::sleep(Duration::from_millis(2));
        runtime.ctx.stats_mut().incr_cmd_call("accounted_timer");
    });
    assert_eq!(
        backend
            .dispatch_ready_phase(crate::runtime::server::worker::reactor::DispatchContext::Normal)
            .expect("dispatch real readiness"),
        LoopControl::Continue
    );
    backend.collect_due_time_events();
    assert_eq!(
        backend
            .dispatch_time_phase()
            .expect("dispatch actual timer"),
        LoopControl::Continue
    );
    let stats = backend.ctx.stats();
    assert_eq!(stats.eventloop_events_collected, 2);
    assert_eq!(stats.eventloop_events_handled, 2);
    assert_eq!(stats.eventloop_events_deferred, 0);
    assert_eq!(stats.eventloop_events_requeued, 0);
    assert_eq!(stats.eventloop_events_stale, 0);
    assert_eq!(stats.eventloop_events_owner_closed, 0);
    assert!(stats.eventloop_events_dispatch_latency_sum >= 2_000);
    assert_eq!(stats.cmd_stats["accounted_timer"].calls, 1);
}

#[test]
fn ready_phase_counts_io_worker_failure_as_handled_not_owner_closed() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let wake_token = backend.io_threads.wake_token();
    let before = (
        backend.ctx.stats().eventloop_events_collected,
        backend.ctx.stats().eventloop_events_handled,
        backend.ctx.stats().eventloop_events_owner_closed,
    );
    backend.collect_after_poll_events(Some((
        Instant::now(),
        vec![
            crate::runtime::server::worker::reactor::transport::RuntimeEvent {
                token: wake_token,
                readable: false,
                writable: false,
                read_hup: false,
                error: true,
            },
        ],
    )));

    assert_eq!(
        backend
            .dispatch_ready_phase(crate::runtime::server::worker::reactor::DispatchContext::Normal)
            .expect("dispatch ready phase"),
        LoopControl::Continue
    );

    assert_eq!(
        backend.ctx.stats().eventloop_events_collected,
        before.0 + 1,
        "polled terminal IO worker wakes must be collected as readiness"
    );
    assert_eq!(
        backend.ctx.stats().eventloop_events_handled,
        before.1 + 2,
        "terminal IO worker wakes must be handled through readiness and control ingress"
    );
    assert_eq!(
        backend.ctx.stats().eventloop_events_owner_closed,
        before.2,
        "terminal IO worker wakes must not count as owner-closed"
    );
}

#[test]
fn ready_phase_counts_terminal_connection_failure_as_owner_closed() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let (_client, token) = connected_test_client(&mut backend);
    let before = (
        backend.ctx.stats().eventloop_events_handled,
        backend.ctx.stats().eventloop_events_owner_closed,
    );
    backend.collect_after_poll_events(Some((
        Instant::now(),
        vec![
            crate::runtime::server::worker::reactor::transport::RuntimeEvent {
                token,
                readable: false,
                writable: false,
                read_hup: true,
                error: false,
            },
        ],
    )));

    assert_eq!(
        backend
            .dispatch_ready_phase(crate::runtime::server::worker::reactor::DispatchContext::Normal)
            .expect("dispatch ready phase"),
        LoopControl::Continue
    );

    assert_eq!(
        backend.ctx.stats().eventloop_events_handled,
        before.0,
        "terminal connection teardown must not count as handled"
    );
    assert_eq!(
        backend.ctx.stats().eventloop_events_owner_closed,
        before.1 + 1,
        "terminal connection teardown must count as owner-closed"
    );
}

#[test]
fn normal_dispatch_drops_events_for_isolated_pending_removal() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let (_server_stream, token_value) = pending_cluster_bus_connection(&mut backend);
    let token = crate::framework::capabilities::io::Token(token_value);

    for (readable, writable, error, read_hup) in [
        (true, false, false, false),
        (false, true, false, false),
        (false, false, true, false),
        (false, false, false, true),
    ] {
        assert!(!backend.dispatch_transport_event(
            ReadyEvent {
                token,
                readable,
                writable,
                error,
                read_hup,
            },
            false,
        ));
    }

    // Centralized teardown isolated the owner from cluster_bus.connections at
    // teardown time; only the retained retry entry keeps the token reserved.
    assert!(!backend.cluster_bus.connections.contains_key(&token_value));
    assert!(backend.is_pending_transport_removal(token));
}

#[test]
fn client_hup_error_retries_failed_deregistration_on_background_tick() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let listener = backend.transport.bind_tcp(peer_addr).unwrap();
    let token = listener.token;
    backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
    backend
        .transport
        .deregister_for_test(token)
        .expect("deregister test transport");

    backend.handle_transport_failure(token);
    backend.handle_transport_failure(token);

    // Isolate-first teardown: the client leaves every business index at once;
    // only the retained retry entry and its L1 registration survive.
    assert_eq!(backend.connected_clients(), 0);
    assert!(backend.is_pending_transport_removal(token));
    assert!(backend.transport.is_registered(token));

    backend
        .transport
        .reregister_for_test(token)
        .expect("reregister test transport");
    backend.run_background_tasks();

    assert_eq!(backend.connected_clients(), 0);
    assert!(!backend.is_pending_transport_removal(token));
    assert!(!backend.transport.is_registered(token));
}

#[test]
fn readable_hup_drains_replication_tail_before_teardown() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind replication test listener");
    let peer_addr = listener
        .local_addr()
        .expect("get replication listener address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect replication test transport");
    let (mut server_stream, _) = listener
        .accept()
        .expect("accept replication test transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    let link = ReplicaLink::streaming(
        ConnectionId(peer.token.0),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        backend.ctx.replication().backlog.replid().to_string(),
        offset(0),
    );
    backend.replication_runtime.set_replica_link(link);
    server_stream
        .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
        .expect("write replication tail");
    server_stream
        .shutdown(std::net::Shutdown::Write)
        .expect("half-close replication peer");
    backend.transport.wait_readable_for_test(peer.token);

    backend.dispatch_transport_event(
        ReadyEvent {
            token: peer.token,
            readable: true,
            writable: false,
            error: false,
            read_hup: true,
        },
        false,
    );

    let connection_token = backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(peer.token.0))
        .then_some(peer.token.0);
    assert!(
        connection_token.is_none(),
        "HUP must tear down after readable drain"
    );
    let db = backend.ctx.db_mut(0).expect("default db exists");
    assert_eq!(db.get_string(b"key"), Ok(Some(b"value".as_slice())));
}

#[test]
fn readable_hup_yields_large_client_pipeline_and_flushes_before_close() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind client listener");
    let peer_addr = listener.local_addr().expect("get client listener address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect client transport");
    let (mut client, _) = listener.accept().expect("accept client transport");
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set client read timeout");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });

    let command = b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n";
    let command_count =
        crate::runtime::server::worker::HUP_CLIENT_READ_BUDGET.div_ceil(command.len()) + 32;
    let mut request = Vec::with_capacity(command.len() * command_count);
    let mut expected = Vec::new();
    for value in 1..=command_count {
        request.extend_from_slice(command);
        expected.extend_from_slice(format!(":{value}\r\n").as_bytes());
    }
    assert!(request.len() > crate::runtime::server::worker::HUP_CLIENT_READ_BUDGET);
    backend
        .transport
        .reserve_test_receive_buffer(peer.token, request.len())
        .expect("size receive buffer for the unread HUP pipeline");
    client
        .write_all(&request)
        .expect("write pipelined commands");
    client
        .shutdown(std::net::Shutdown::Write)
        .expect("half-close client input");
    backend.transport.wait_readable_for_test(peer.token);
    backend
        .transport
        .wait_pending_bytes_for_test(peer.token, request.len());

    backend.dispatch_transport_event(
        ReadyEvent {
            token: peer.token,
            readable: true,
            writable: false,
            error: false,
            read_hup: true,
        },
        false,
    );

    assert!(
        backend.transport.is_registered(peer.token),
        "a readable HUP must yield after one bounded client read"
    );
    assert!(backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientRead(peer.token)
    ));
    let first_turn_count = backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"counter")
        .and_then(|value| {
            std::str::from_utf8(value.as_bytes())
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
        })
        .expect("first bounded read must process complete commands");
    assert!(
        first_turn_count <= crate::runtime::server::worker::HUP_CLIENT_COMMAND_BUDGET,
        "a readable HUP turn must bound parsed commands, got {first_turn_count}"
    );
    assert!(first_turn_count < command_count);

    for _ in 0..=command_count {
        if !backend.transport.is_registered(peer.token) {
            break;
        }
        backend.event_batch.begin_next_turn();
        backend.writes.begin_turn();
        backend.drive_one_pending_client_read();
        backend.drive_pending_ordinary_writes();
    }

    let mut replies = Vec::new();
    client
        .read_to_end(&mut replies)
        .expect("read every reply before server EOF");
    assert_eq!(replies, expected);
    assert!(!backend.transport.is_registered(peer.token));
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(peer.token.0)));
    assert_eq!(
        backend
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"counter")
            .map(|value| value.as_bytes()),
        Some(command_count.to_string().as_bytes())
    );
}

#[test]
fn readable_hup_preserves_pipeline_while_a_command_is_blocked() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);

    let blocked_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let blocked_addr = blocked_listener.local_addr().unwrap();
    let blocked_peer = backend.transport.connect_tcp(blocked_addr).unwrap();
    let (mut blocked_client, _) = blocked_listener.accept().unwrap();
    blocked_client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    backend.add_connection(RuntimeConnectedPeer {
        token: blocked_peer.token,
        peer_addr: blocked_addr,
    });
    blocked_client
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$8\r\nhup-list\r\n$1\r\n0\r\n*1\r\n$4\r\nPING\r\n")
        .unwrap();
    blocked_client.shutdown(std::net::Shutdown::Write).unwrap();
    backend.transport.wait_readable_for_test(blocked_peer.token);

    backend.dispatch_transport_event(
        ReadyEvent {
            token: blocked_peer.token,
            readable: true,
            writable: false,
            error: false,
            read_hup: true,
        },
        false,
    );

    assert!(backend.transport.is_registered(blocked_peer.token));
    assert!(backend.client_hup_drains.contains(&blocked_peer.token));
    assert!(backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientRead(blocked_peer.token)
    ));

    let writer_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let writer_addr = writer_listener.local_addr().unwrap();
    let writer_peer = backend.transport.connect_tcp(writer_addr).unwrap();
    let (mut writer_client, _) = writer_listener.accept().unwrap();
    backend.add_connection(RuntimeConnectedPeer {
        token: writer_peer.token,
        peer_addr: writer_addr,
    });
    writer_client
        .write_all(b"*3\r\n$5\r\nLPUSH\r\n$8\r\nhup-list\r\n$5\r\nvalue\r\n")
        .unwrap();
    backend.transport.wait_readable_for_test(writer_peer.token);
    backend.dispatch_transport_event(
        ReadyEvent {
            token: writer_peer.token,
            readable: true,
            writable: false,
            error: false,
            read_hup: false,
        },
        true,
    );
    backend.drive_one_pending_client_read();

    let mut replies = Vec::new();
    blocked_client.read_to_end(&mut replies).unwrap();
    assert_eq!(replies, b"*2\r\n$8\r\nhup-list\r\n$5\r\nvalue\r\n+PONG\r\n");
    assert!(!backend.transport.is_registered(blocked_peer.token));
    assert!(!backend.client_hup_drains.contains(&blocked_peer.token));
    assert!(!backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientRead(blocked_peer.token)
    ));
}

#[test]
fn blocked_hup_buffer_growth_refreshes_memory_and_evicts_immediately() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().server.maxmemory_clients = "256kb".to_string();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind client listener");
    let peer_addr = listener.local_addr().expect("get client listener address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect client transport");
    let (mut client, _) = listener.accept().expect("accept client transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    let value_len = crate::runtime::server::worker::HUP_CLIENT_READ_BUDGET * 5;
    let mut request = b"*3\r\n$5\r\nBLPOP\r\n$8\r\nhup-list\r\n$1\r\n0\r\n".to_vec();
    request.extend_from_slice(
        format!("*3\r\n$3\r\nSET\r\n$8\r\nbuffered\r\n${value_len}\r\n").as_bytes(),
    );
    request.extend(std::iter::repeat_n(b'x', value_len));
    request.extend_from_slice(b"\r\n");
    backend
        .transport
        .reserve_test_receive_buffer(peer.token, request.len())
        .expect("size receive buffer for the unread blocked-HUP pipeline");
    client
        .write_all(&request)
        .expect("write blocked HUP pipeline");
    client
        .shutdown(std::net::Shutdown::Write)
        .expect("half-close client input");
    backend.transport.wait_readable_for_test(peer.token);
    backend
        .transport
        .wait_pending_bytes_for_test(peer.token, request.len());

    backend.dispatch_transport_event(
        ReadyEvent {
            token: peer.token,
            readable: true,
            writable: false,
            error: false,
            read_hup: true,
        },
        false,
    );

    assert!(backend.transport.is_registered(peer.token));
    assert!(backend.connection_registry[peer.token].blocked.is_some());
    assert!(backend.client_hup_drains.contains(&peer.token));
    for _ in 0..8 {
        if !backend.transport.is_registered(peer.token) {
            break;
        }
        backend.drive_one_pending_client_read();
    }

    assert!(!backend.transport.is_registered(peer.token));
    assert!(!backend.connection_registry.contains(peer.token));
    assert_eq!(backend.ctx.stats().evicted_clients, 1);
    assert_eq!(backend.ctx.normal_client_memory(), 0);
    assert!(!backend.client_hup_drains.contains(&peer.token));
    assert!(!backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientRead(peer.token)
    ));
}

#[test]
fn readable_hup_removes_a_blocked_client_without_buffered_pipeline() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer_addr = listener.local_addr().unwrap();
    let peer = backend.transport.connect_tcp(peer_addr).unwrap();
    let (mut client, _) = listener.accept().unwrap();
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    client
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$8\r\nhup-list\r\n$1\r\n0\r\n")
        .unwrap();
    client.shutdown(std::net::Shutdown::Write).unwrap();
    backend.transport.wait_readable_for_test(peer.token);

    backend.dispatch_transport_event(
        ReadyEvent {
            token: peer.token,
            readable: true,
            writable: false,
            error: false,
            read_hup: true,
        },
        false,
    );
    assert!(backend.transport.is_registered(peer.token));

    // The first readable event may contain only the command bytes. Wait for
    // the subsequent EOF readiness before driving the blocked HUP continuation.
    backend.transport.wait_readable_for_test(peer.token);
    backend.drive_one_pending_client_read();

    assert!(!backend.transport.is_registered(peer.token));
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(peer.token.0)));
    assert!(!backend.client_hup_drains.contains(&peer.token));
    assert!(!backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientRead(peer.token)
    ));
}
