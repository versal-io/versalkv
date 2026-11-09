use super::*;
use crate::foundation::io::sys::time::MonotonicInstant;
use crate::runtime::server::worker::event_loop::timer::RuntimeTimeEvent;
use crate::runtime::server::worker::event_loop::DispatchContext;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

static MODULE_EVENT_LOOP_PHASES: Mutex<Vec<u64>> = Mutex::new(Vec::new());
static MODULE_EVENT_LOOP_TEST_LOCK: Mutex<()> = Mutex::new(());
static TEST_TEMP_DIR_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

struct TestTempDir {
    path: PathBuf,
}

impl TestTempDir {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "{label}-{}-{}",
            std::process::id(),
            TEST_TEMP_DIR_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).expect("create test temporary directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct TestChild {
    child: Option<Child>,
}

impl TestChild {
    fn spawn(command: &mut Command) -> Self {
        Self {
            child: Some(command.spawn().expect("spawn test child")),
        }
    }

    fn wait_for_exit(mut self) -> ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self
                .child
                .as_mut()
                .expect("test child remains owned until exit")
                .try_wait()
                .expect("poll test child")
            {
                self.child = None;
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "test child did not exit before deadline"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for TestChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn accept_with_deadline(listener: &TcpListener, description: &str) -> TcpStream {
    listener
        .set_nonblocking(true)
        .expect("set test listener nonblocking");
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match listener.accept() {
            Ok((stream, _)) => return stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "{description} did not connect before deadline"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("accept {description}: {error}"),
        }
    }
}

fn wait_for_parent_release(control: &mut TcpStream) {
    control
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set child control read timeout");
    let mut start = [0_u8; 1];
    control
        .read_exact(&mut start)
        .expect("wait for parent input");
}

fn release_child(control: &mut TcpStream) {
    control
        .set_write_timeout(Some(Duration::from_secs(1)))
        .expect("set parent control write timeout");
    control.write_all(&[1]).expect("release child");
}

fn read_to_end_with_timeout(stream: &mut TcpStream, output: &mut Vec<u8>, description: &str) {
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set test reply read timeout");
    stream
        .read_to_end(output)
        .unwrap_or_else(|error| panic!("read {description} EOF: {error}"));
}

extern "C" fn record_module_event_loop_phase(
    _ctx: *mut crate::engine::extension::module::ValkeyModuleCtx,
    _event: crate::engine::extension::module::ValkeyModuleEvent,
    subevent: u64,
    _data: *mut libc::c_void,
) {
    MODULE_EVENT_LOOP_PHASES.lock().unwrap().push(subevent);
}

fn install_module_event_loop_fixture(backend: &mut ServerWorkerState) {
    use crate::engine::extension::module::{
        ModuleRef, ValkeyModuleEvent, VALKEYMODULE_EVENT_EVENTLOOP,
    };

    backend
        .ctx
        .module_registry_mut()
        .register(ModuleRef::new(
            "event-loop-fixture",
            std::ptr::null_mut(),
            1,
            true,
        ))
        .unwrap();
    assert!(backend.ctx.module_registry_mut().subscribe_server_event(
        "event-loop-fixture",
        ValkeyModuleEvent {
            id: VALKEYMODULE_EVENT_EVENTLOOP,
            dataver: 1,
        },
        Some(record_module_event_loop_phase),
    ));
}

#[test]
fn before_sleep_processes_transport_pending_data_before_flushing_replies() {
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
        .expect("write request buffered above poller");
    let ready = backend.transport.wait_readable_for_test(peer.token);
    assert!(ready
        .iter()
        .any(|event| event.token == peer.token && event.readable));
    backend.transport.queue_test_pending_data_event(peer.token);
    backend.writes.ordinary.begin_turn();

    let _ = backend.run_before_poll_phase();

    let mut reply = [0_u8; 7];
    client
        .read_exact(&mut reply)
        .expect("read reply produced by pending-data dispatch");
    assert_eq!(&reply, b"+PONG\r\n");
    assert!(!backend.transport.has_pending_data());
}

#[test]
fn pending_output_drains_before_a_continued_eviction_slice() {
    const ISOLATION_MARKER: &str = "VERSAL_EVENT_BARRIER_EVICTION_CHILD";
    if std::env::var_os(ISOLATION_MARKER).is_none() {
        let mut command = Command::new(std::env::current_exe().expect("test binary"));
        command
            .args([
                "--exact",
                "runtime::server::worker::event_loop::tests::event_barrier_tests::pending_output_drains_before_a_continued_eviction_slice",
            ])
            .env(ISOLATION_MARKER, "1");
        let status = TestChild::spawn(&mut command).wait_for_exit();
        assert_eq!(status.code(), Some(87));
        return;
    }

    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind client peer");
    let peer_addr = listener.local_addr().expect("client peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect client transport");
    let (_client, _) = listener.accept().expect("accept client transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    for index in 0..2 {
        backend.ctx.db_mut(0).unwrap().set(
            format!("key-{index}").into_bytes(),
            crate::foundation::io::store::Robj::string_raw(bytes::Bytes::from(vec![
                b'x';
                96 * 1024
            ])),
        );
    }

    let callback_index = backend.test_time_event_callbacks.len();
    backend
        .test_time_event_callbacks
        .push(Some(Box::new(move |runtime| {
            runtime.connection_registry[peer.token].queue_reply(vec![b'r'; 128 * 1024]);
            runtime.refresh_client_memory_usage(peer.token, true);
            assert!(runtime.schedule_ordinary_write(peer.token.0));
            let with_pending_output = runtime.ctx.maxmemory_state().logical;
            runtime.ctx.config_mut().server.maxmemory =
                with_pending_output.saturating_sub(64 * 1024) as u64;
            runtime.ctx.config_mut().server.maxmemory_policy = "allkeys-random".to_string();
            assert!(runtime.ctx.maxmemory_state().is_over_limit());
            runtime.ctx.config_mut().server.maxmemory_eviction_tenacity = 100;
            runtime.ctx.set_needs_eviction(true);
            let callback_index = runtime.test_time_event_callbacks.len();
            runtime
                .test_time_event_callbacks
                .push(Some(Box::new(|runtime| {
                    runtime.ctx.set_shutdown_requested(true)
                })));
            runtime.time_events.create_time_event(
                MonotonicInstant::now(),
                Duration::from_millis(1),
                None,
                RuntimeTimeEvent::Test(callback_index),
            );
        })));
    backend.time_events.create_time_event(
        MonotonicInstant::now(),
        Duration::from_millis(1),
        None,
        RuntimeTimeEvent::Test(callback_index),
    );

    let backend = crate::runtime::server::ServerRuntime::run_owned_for_test(backend)
        .expect("run through the eviction continuation");

    assert_eq!(backend.ctx.db(0).unwrap().dbsize(), 2);
    std::process::exit(87);
}

#[test]
fn poll_fires_module_event_loop_hooks_around_the_wait() {
    use crate::engine::extension::module::{
        VALKEYMODULE_SUBEVENT_EVENTLOOP_AFTER_SLEEP, VALKEYMODULE_SUBEVENT_EVENTLOOP_BEFORE_SLEEP,
    };

    let _guard = MODULE_EVENT_LOOP_TEST_LOCK.lock().unwrap();
    MODULE_EVENT_LOOP_PHASES.lock().unwrap().clear();
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    install_module_event_loop_fixture(&mut backend);
    backend.transport.queue_test_transport_event_batch(&[]);

    assert!(backend.poll_ready().expect("poll test transport").is_some());

    assert_eq!(
        *MODULE_EVENT_LOOP_PHASES.lock().unwrap(),
        vec![
            VALKEYMODULE_SUBEVENT_EVENTLOOP_BEFORE_SLEEP,
            VALKEYMODULE_SUBEVENT_EVENTLOOP_AFTER_SLEEP,
        ]
    );
}

#[test]
fn blocked_poll_omits_module_event_loop_hooks_like_valkey() {
    let _guard = MODULE_EVENT_LOOP_TEST_LOCK.lock().unwrap();
    MODULE_EVENT_LOOP_PHASES.lock().unwrap().clear();
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    install_module_event_loop_fixture(&mut backend);
    backend.transport.queue_test_transport_event_batch(&[]);

    backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: ConnectionId(usize::MAX),
    });

    assert!(MODULE_EVENT_LOOP_PHASES.lock().unwrap().is_empty());
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
        runtime.create_test_time_event(|runtime| runtime.ctx.set_shutdown_requested(true));
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
fn appendfsync_always_defers_new_reply_until_the_next_durable_boundary() {
    let dir = TestTempDir::new("versal-event-barrier");

    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().persistence.dir = dir.path().to_string_lossy().into_owned();
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

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind client listener");
    let address = listener.local_addr().expect("client listener address");
    let peer = backend
        .transport
        .connect_tcp(address)
        .expect("connect client");
    let mut client = accept_with_deadline(&listener, "AOF barrier client");
    client
        .set_nonblocking(true)
        .expect("set client nonblocking");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr: address,
    });
    client
        .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
        .expect("write SET");
    backend.transport.wait_readable_for_test(peer.token);

    let mut barrier_client = client.try_clone().expect("clone barrier client");
    backend.create_test_time_event(move |runtime| {
        assert_eq!(
            runtime
                .ctx
                .aof()
                .journal
                .as_ref()
                .expect("AOF journal")
                .appender
                .current_size(),
            initial_size,
            "the pre-read barrier must not expose the new reply before durability"
        );
        let mut byte = [0_u8; 1];
        assert_eq!(
            barrier_client.read(&mut byte).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock,
            "AE_BARRIER must not expose the reply during ready dispatch"
        );
    });
    backend
        .transport
        .queue_test_transport_event_batch(&[(peer.token, true, true, false, false)]);
    let trace = backend
        .run_one_turn_for_test()
        .expect("run production-order turn");
    assert_eq!(
        trace,
        vec![
            TurnTracePoint::BeforePoll,
            TurnTracePoint::AofFlush,
            TurnTracePoint::Poll,
            TurnTracePoint::AfterPoll,
            TurnTracePoint::ReadyDispatch,
            TurnTracePoint::TimeDispatch,
            TurnTracePoint::AofFlush,
        ]
    );
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
        "the first completed turn must establish the durable AOF boundary"
    );
    let mut byte = [0_u8; 1];
    assert_eq!(
        client.read(&mut byte).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock,
        "the reply must remain queued until the subsequent production turn"
    );

    backend.transport.queue_test_transport_event_batch(&[]);
    let trace = backend
        .run_one_turn_for_test()
        .expect("run reply delivery turn");
    assert_eq!(
        trace,
        vec![
            TurnTracePoint::BeforePoll,
            TurnTracePoint::AofFlush,
            TurnTracePoint::OrdinaryWrite,
            TurnTracePoint::Poll,
            TurnTracePoint::AfterPoll,
            TurnTracePoint::ReadyDispatch,
            TurnTracePoint::TimeDispatch,
            TurnTracePoint::AofFlush,
        ]
    );

    client
        .set_nonblocking(false)
        .expect("restore client blocking mode");
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set client read timeout");
    let mut reply = [0_u8; 5];
    client
        .read_exact(&mut reply)
        .expect("read durable SET reply");
    assert_eq!(&reply, b"+OK\r\n");
    assert!(
        backend
            .ctx
            .aof()
            .journal
            .as_ref()
            .expect("AOF journal")
            .appender
            .current_size()
            > initial_size
    );
}

fn setup_appendfsync_always_half_close_test(
) -> (TestTempDir, ServerWorkerState, TcpStream, Token, u64) {
    let dir = TestTempDir::new("versal-event-barrier-hup");

    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().persistence.dir = dir.path().to_string_lossy().into_owned();
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

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind client listener");
    let address = listener.local_addr().expect("client listener address");
    let peer = backend
        .transport
        .connect_tcp(address)
        .expect("connect client");
    let client = accept_with_deadline(&listener, "half-close AOF client");
    client
        .set_nonblocking(true)
        .expect("set client nonblocking");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr: address,
    });
    (dir, backend, client, peer.token, initial_size)
}

fn dispatch_readable_half_close_turn(backend: &mut ServerWorkerState, token: Token) {
    backend.transport.wait_readable_for_test(token);
    backend.turn_trace.clear();
    assert_eq!(
        backend.run_before_poll_phase().expect("before-poll phase"),
        LoopControl::Continue
    );
    backend.writes.ordinary.begin_turn();
    backend.writes.slot_exports.begin_turn();
    backend
        .transport
        .queue_test_transport_event_batch(&[(token, true, false, true, false)]);
    let poll_result = backend.poll_ready().expect("poll half-close readiness");
    backend.collect_after_poll_events(poll_result);
    assert_eq!(
        backend
            .dispatch_ready_phase(DispatchContext::Normal)
            .expect("ready phase"),
        LoopControl::Continue
    );
}

fn finish_half_close_turn_at_durable_boundary(backend: &mut ServerWorkerState) {
    backend.collect_due_time_events();
    assert_eq!(
        backend.dispatch_time_phase().expect("time phase"),
        LoopControl::Continue
    );
    backend.finish_turn_pending_outputs();
    backend.event_batch.begin_next_turn();
}

fn read_durable_reply_and_eof(client: &mut TcpStream) -> Vec<u8> {
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    let mut received = Vec::new();
    loop {
        let mut chunk = [0_u8; 32];
        match client.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => {
                received.extend_from_slice(&chunk[..read]);
                if received.len() >= 5 {
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "durable half-close reply never became visible"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("read durable SET reply: {error}"),
        }
    }
    assert_eq!(&received[..5], b"+OK\r\n");

    let eof_deadline = std::time::Instant::now() + Duration::from_secs(1);
    loop {
        let mut chunk = [0_u8; 32];
        match client.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => received.extend_from_slice(&chunk[..read]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < eof_deadline,
                    "half-close connection never reached EOF after durable reply"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("read server EOF after close: {error}"),
        }
    }
    received
}

#[test]
fn appendfsync_always_readable_hup_defers_new_reply_until_next_durable_boundary() {
    let (_dir, mut backend, mut client, token, initial_size) =
        setup_appendfsync_always_half_close_test();
    client
        .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
        .expect("write SET");
    client
        .shutdown(std::net::Shutdown::Write)
        .expect("half-close client write side");
    dispatch_readable_half_close_turn(&mut backend, token);

    assert_eq!(
        backend.turn_trace,
        vec![
            TurnTracePoint::BeforePoll,
            TurnTracePoint::AofFlush,
            TurnTracePoint::Poll,
            TurnTracePoint::AfterPoll,
            TurnTracePoint::ReadyDispatch,
        ]
    );
    assert_eq!(
        backend
            .ctx
            .aof()
            .journal
            .as_ref()
            .expect("AOF journal")
            .appender
            .current_size(),
        initial_size,
        "the readable half-close turn must not flush AOF inline"
    );
    let mut byte = [0_u8; 1];
    assert_eq!(
        client.read(&mut byte).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock,
        "a readable half-close must not expose a new reply during ready dispatch"
    );

    finish_half_close_turn_at_durable_boundary(&mut backend);

    assert_eq!(
        backend.turn_trace,
        vec![
            TurnTracePoint::BeforePoll,
            TurnTracePoint::AofFlush,
            TurnTracePoint::Poll,
            TurnTracePoint::AfterPoll,
            TurnTracePoint::ReadyDispatch,
            TurnTracePoint::TimeDispatch,
            TurnTracePoint::AofFlush,
            TurnTracePoint::OrdinaryWrite,
        ]
    );
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
        "the end-of-turn durable boundary must flush the SET to AOF"
    );
    let received = read_durable_reply_and_eof(&mut client);
    assert_eq!(received, b"+OK\r\n");
    assert!(
        backend
            .ctx
            .aof()
            .journal
            .as_ref()
            .expect("AOF journal")
            .appender
            .current_size()
            > initial_size
    );
}

#[test]
fn pipelined_write_and_unblocked_write_share_the_next_aof_boundary() {
    let dir = TestTempDir::new("versal-unblocked-event-barrier");

    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().persistence.dir = dir.path().to_string_lossy().into_owned();
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

    let blocked_listener = TcpListener::bind("127.0.0.1:0").expect("bind blocked listener");
    let blocked_address = blocked_listener
        .local_addr()
        .expect("blocked listener address");
    let blocked_peer = backend
        .transport
        .connect_tcp(blocked_address)
        .expect("connect blocked client");
    let mut blocked_client = accept_with_deadline(&blocked_listener, "blocked AOF client");
    backend.add_connection(RuntimeConnectedPeer {
        token: blocked_peer.token,
        peer_addr: blocked_address,
    });
    blocked_client
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$3\r\nkey\r\n$1\r\n0\r\n")
        .expect("write BLPOP");
    backend.transport.wait_readable_for_test(blocked_peer.token);
    backend.dispatch_transport_event(
        ReadyEvent {
            token: blocked_peer.token,
            readable: true,
            writable: false,
            error: false,
            read_hup: false,
        },
        false,
    );

    let writer_listener = TcpListener::bind("127.0.0.1:0").expect("bind writer listener");
    let writer_address = writer_listener
        .local_addr()
        .expect("writer listener address");
    let writer_peer = backend
        .transport
        .connect_tcp(writer_address)
        .expect("connect writer client");
    let mut writer_client = accept_with_deadline(&writer_listener, "writer AOF client");
    backend.add_connection(RuntimeConnectedPeer {
        token: writer_peer.token,
        peer_addr: writer_address,
    });
    writer_client
        .write_all(
            b"*3\r\n$5\r\nLPUSH\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*3\r\n$3\r\nSET\r\n$5\r\nother\r\n$1\r\n1\r\n",
        )
        .expect("write pipelined LPUSH and SET");
    backend.transport.wait_readable_for_test(writer_peer.token);
    backend.dispatch_transport_event(
        ReadyEvent {
            token: writer_peer.token,
            readable: true,
            writable: false,
            error: false,
            read_hup: false,
        },
        false,
    );

    backend.process_ready_keys();
    assert_eq!(
        backend
            .ctx
            .aof()
            .journal
            .as_ref()
            .expect("AOF journal")
            .appender
            .current_size(),
        initial_size,
        "pipelined and unblocked command completion must not flush AOF inline"
    );
    assert!(backend.ctx.db(0).unwrap().get_no_expire(b"key").is_none());
    assert_eq!(
        backend
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"other")
            .unwrap()
            .as_bytes(),
        b"1"
    );
}

fn run_failing_aof_barrier_child(
    blocked_address: SocketAddr,
    writer_address: SocketAddr,
    control_address: SocketAddr,
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

    let blocked_peer = backend
        .transport
        .connect_tcp(blocked_address)
        .expect("connect blocked child client");
    backend.add_connection(RuntimeConnectedPeer {
        token: blocked_peer.token,
        peer_addr: blocked_address,
    });
    let writer_peer = backend
        .transport
        .connect_tcp(writer_address)
        .expect("connect writer child client");
    backend.add_connection(RuntimeConnectedPeer {
        token: writer_peer.token,
        peer_addr: writer_address,
    });
    let mut control = TcpStream::connect(control_address).expect("connect child control");
    wait_for_parent_release(&mut control);
    backend.transport.wait_readable_for_test(blocked_peer.token);
    backend.transport.wait_readable_for_test(writer_peer.token);

    backend.writes.ordinary.begin_turn();
    backend.dispatch_transport_event(
        ReadyEvent {
            token: blocked_peer.token,
            readable: true,
            writable: false,
            error: false,
            read_hup: false,
        },
        false,
    );
    backend.dispatch_transport_event(
        ReadyEvent {
            token: writer_peer.token,
            readable: true,
            writable: false,
            error: false,
            read_hup: false,
        },
        false,
    );
    backend.process_ready_keys();
    assert_eq!(backend.ctx.blocking_registry().count(), 0);
    let _ = backend.run_before_poll_phase();
    panic!("appendfsync always unexpectedly survived /dev/null fsync");
}

fn run_failing_aof_half_close_barrier_child(
    client_address: SocketAddr,
    control_address: SocketAddr,
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

    let client_peer = backend
        .transport
        .connect_tcp(client_address)
        .expect("connect child client");
    backend.add_connection(RuntimeConnectedPeer {
        token: client_peer.token,
        peer_addr: client_address,
    });
    let mut control = TcpStream::connect(control_address).expect("connect child control");
    wait_for_parent_release(&mut control);
    backend.transport.wait_readable_for_test(client_peer.token);

    backend.transport.queue_test_transport_event_batch(&[(
        client_peer.token,
        true,
        false,
        true,
        false,
    )]);
    let _ = backend
        .run_one_turn_for_test()
        .expect("run half-close turn");
    panic!("appendfsync always unexpectedly survived /dev/null fsync after readable half-close");
}

#[test]
fn appendfsync_always_failure_exposes_no_pipeline_or_unblocked_reply() {
    const CHILD_MARKER: &str = "VERSAL_AOF_BARRIER_FAILURE_CHILD";
    const TEST_NAME: &str = concat!(
        "runtime::server::worker::event_loop::tests::event_barrier_tests::",
        "appendfsync_always_failure_exposes_no_pipeline_or_unblocked_reply"
    );
    if std::env::var_os(CHILD_MARKER).is_some() {
        let parse_address = |name| {
            std::env::var(name)
                .expect("child address")
                .parse()
                .expect("valid child address")
        };
        run_failing_aof_barrier_child(
            parse_address("VERSAL_AOF_BLOCKED_ADDRESS"),
            parse_address("VERSAL_AOF_WRITER_ADDRESS"),
            parse_address("VERSAL_AOF_CONTROL_ADDRESS"),
        );
        return;
    }

    let blocked_listener = TcpListener::bind("127.0.0.1:0").expect("bind blocked listener");
    let writer_listener = TcpListener::bind("127.0.0.1:0").expect("bind writer listener");
    let control_listener = TcpListener::bind("127.0.0.1:0").expect("bind control listener");
    let mut command = Command::new(std::env::current_exe().expect("test binary"));
    command
        .args(["--exact", TEST_NAME, "--nocapture"])
        .env(CHILD_MARKER, "1")
        .env(
            "VERSAL_AOF_BLOCKED_ADDRESS",
            blocked_listener.local_addr().unwrap().to_string(),
        )
        .env(
            "VERSAL_AOF_WRITER_ADDRESS",
            writer_listener.local_addr().unwrap().to_string(),
        )
        .env(
            "VERSAL_AOF_CONTROL_ADDRESS",
            control_listener.local_addr().unwrap().to_string(),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let child = TestChild::spawn(&mut command);
    let mut blocked_client = accept_with_deadline(&blocked_listener, "blocked child client");
    let mut writer_client = accept_with_deadline(&writer_listener, "writer child client");
    let mut control = accept_with_deadline(&control_listener, "child control");

    blocked_client
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$3\r\nkey\r\n$1\r\n0\r\n")
        .expect("write BLPOP");
    writer_client
        .write_all(
            b"*3\r\n$5\r\nLPUSH\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*3\r\n$3\r\nSET\r\n$5\r\nother\r\n$1\r\n1\r\n",
        )
        .expect("write pipelined LPUSH and SET");
    release_child(&mut control);

    let child_status = child.wait_for_exit();
    assert_eq!(child_status.code(), Some(1));
    let mut blocked_reply = Vec::new();
    let mut writer_reply = Vec::new();
    read_to_end_with_timeout(&mut blocked_client, &mut blocked_reply, "blocked client");
    read_to_end_with_timeout(&mut writer_client, &mut writer_reply, "writer client");
    assert!(
        blocked_reply.is_empty(),
        "blocked reply crossed failed fsync"
    );
    assert!(
        writer_reply.is_empty(),
        "pipeline reply crossed failed fsync"
    );
}

#[test]
fn appendfsync_always_half_close_failure_exposes_no_reply() {
    const CHILD_MARKER: &str = "VERSAL_AOF_HUP_BARRIER_FAILURE_CHILD";
    const TEST_NAME: &str = concat!(
        "runtime::server::worker::event_loop::tests::event_barrier_tests::",
        "appendfsync_always_half_close_failure_exposes_no_reply"
    );
    if std::env::var_os(CHILD_MARKER).is_some() {
        let parse_address = |name| {
            std::env::var(name)
                .expect("child address")
                .parse()
                .expect("valid child address")
        };
        run_failing_aof_half_close_barrier_child(
            parse_address("VERSAL_AOF_HUP_CLIENT_ADDRESS"),
            parse_address("VERSAL_AOF_HUP_CONTROL_ADDRESS"),
        );
        return;
    }

    let client_listener = TcpListener::bind("127.0.0.1:0").expect("bind client listener");
    let control_listener = TcpListener::bind("127.0.0.1:0").expect("bind control listener");
    let mut command = Command::new(std::env::current_exe().expect("test binary"));
    command
        .args(["--exact", TEST_NAME, "--nocapture"])
        .env(CHILD_MARKER, "1")
        .env(
            "VERSAL_AOF_HUP_CLIENT_ADDRESS",
            client_listener.local_addr().unwrap().to_string(),
        )
        .env(
            "VERSAL_AOF_HUP_CONTROL_ADDRESS",
            control_listener.local_addr().unwrap().to_string(),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let child = TestChild::spawn(&mut command);
    let mut client = accept_with_deadline(&client_listener, "half-close child client");
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("bound half-close reply before peer teardown");
    let mut control = accept_with_deadline(&control_listener, "half-close child control");

    client
        .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
        .expect("write SET");
    client
        .shutdown(std::net::Shutdown::Write)
        .expect("half-close client write side");
    release_child(&mut control);

    let child_status = child.wait_for_exit();
    assert_eq!(child_status.code(), Some(1));
    let mut reply = Vec::new();
    client
        .read_to_end(&mut reply)
        .expect("read half-close client EOF");
    assert!(reply.is_empty(), "a half-close reply crossed failed fsync");
}

#[test]
fn appendfsync_always_traces_aof_flush_before_ordinary_write() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.ctx.config_mut().persistence.aof_fsync =
        crate::foundation::io::persistence::AofFsync::Always;
    let dir = TestTempDir::new("versal-aof-trace-order");
    backend.ctx.config_mut().persistence.dir = dir.path().to_string_lossy().into_owned();
    backend.ctx.config_mut().persistence.aof_filename = "appendonly.aof".to_string();
    backend.ctx.config_mut().persistence.aof_appenddirname = "appendonlydir".to_string();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind client peer");
    let peer_addr = listener.local_addr().expect("client peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect client transport");
    let _client = accept_with_deadline(&listener, "AOF trace client");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    backend.connection_registry[peer.token].queue_reply(b"trace-order".to_vec());
    assert!(backend.schedule_ordinary_write(peer.token.0));
    backend.writes.ordinary.begin_turn();
    backend.turn_trace.clear();

    assert_eq!(
        backend.run_before_poll_phase().expect("before-poll phase"),
        LoopControl::Continue
    );

    let aof_index = backend
        .turn_trace
        .iter()
        .position(|point| *point == TurnTracePoint::AofFlush)
        .expect("AOF flush trace");
    let ordinary_index = backend
        .turn_trace
        .iter()
        .position(|point| *point == TurnTracePoint::OrdinaryWrite)
        .expect("ordinary write trace");
    assert!(
        aof_index < ordinary_index,
        "appendfsync=always must flush AOF before draining ordinary writes",
    );
}
