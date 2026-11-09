use super::*;
use std::cell::RefCell;
use std::sync::Once;

thread_local! {
    static CAPTURED_LOGS: RefCell<Option<Vec<String>>> = const { RefCell::new(None) };
}

struct TestLogger;

struct LogCaptureGuard;

impl LogCaptureGuard {
    fn start() -> Self {
        CAPTURED_LOGS.with(|logs| *logs.borrow_mut() = Some(Vec::new()));
        Self
    }

    fn finish(self) -> Vec<String> {
        CAPTURED_LOGS.with(|logs| logs.borrow_mut().take().expect("active log capture"))
    }
}

impl Drop for LogCaptureGuard {
    fn drop(&mut self) {
        CAPTURED_LOGS.with(|logs| {
            logs.borrow_mut().take();
        });
    }
}

impl log::Log for TestLogger {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        CAPTURED_LOGS.with(|logs| {
            if let Some(logs) = logs.borrow_mut().as_mut() {
                logs.push(record.args().to_string());
            }
        });
    }

    fn flush(&self) {}
}

fn capture_logs(action: impl FnOnce()) -> Vec<String> {
    static LOGGER: TestLogger = TestLogger;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        log::set_logger(&LOGGER).expect("install test logger");
        log::set_max_level(log::LevelFilter::Debug);
    });
    let capture = LogCaptureGuard::start();
    action();
    capture.finish()
}

#[test]
fn rdb_receive_reports_disk_installation_once_after_payload_starts() {
    use crate::framework::distributed::replication::RdbLoadMode;

    for mode in [
        RdbLoadMode::DiskBacked,
        RdbLoadMode::Direct,
        RdbLoadMode::Swap,
    ] {
        for is_eof_framed in [true, false] {
            let mut backend = ServerWorkerState::new(1).unwrap();
            backend.ctx.replication_mut().switch_to_replica();
            backend.ctx.db_mut(0).unwrap().set(
                b"old".to_vec(),
                crate::foundation::io::store::Robj::string(Bytes::from_static(b"value")),
            );
            let mut connection = Connection::new_fake();
            connection.set_token_for_test(Token(41));
            let token = backend.connection_registry.insert(connection);
            let mut link = ReplicaLink::new(
                ConnectionId(41),
                MasterInfo {
                    host: "127.0.0.1".to_string(),
                    port: 6379,
                },
            );
            link.pin_rdb_load_mode(mode);
            link.connected(None, None);
            link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6379).unwrap();
            link.on_bytes(
                b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0\r\n",
                6379,
            )
            .unwrap();
            let header = if is_eof_framed {
                b"$EOF:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n".as_slice()
            } else {
                b"$1000\r\n".as_slice()
            };
            link.on_bytes(header, 6379).unwrap();
            let header_logs = capture_logs(|| {
                backend
                    .drive_streaming_decode(41, token, &mut link)
                    .unwrap();
            });
            assert!(!header_logs.iter().any(|log| log.contains("to disk")));

            link.rdb_receive_mut().unwrap().feed(b"VALKEY080");
            let logs = capture_logs(|| {
                backend
                    .drive_streaming_decode(41, token, &mut link)
                    .unwrap();
                backend
                    .drive_streaming_decode(41, token, &mut link)
                    .unwrap();
            });
            let disk_logs: Vec<_> = logs.iter().filter(|log| log.contains("to disk")).collect();
            if mode == RdbLoadMode::DiskBacked {
                assert_eq!(
                    disk_logs.len(),
                    1,
                    "DiskBacked receive must report delayed disk installation"
                );
                assert!(disk_logs[0].contains("receiving streamed RDB from primary"));
                assert!(disk_logs[0].contains("buffering"));
                assert!(disk_logs[0].contains("delayed installation"));
                assert!(backend
                    .ctx
                    .db(0)
                    .unwrap()
                    .get_allow_expired(b"old")
                    .is_some());
            } else {
                assert!(disk_logs.is_empty());
            }
            assert_eq!(
                logs.iter()
                    .filter(|log| log.contains("Loading DB in memory"))
                    .count(),
                usize::from(mode != RdbLoadMode::DiskBacked),
                "disk-backed loading starts only after the full transfer is received"
            );
        }
    }
}

fn refused_master(backend: &mut ServerWorkerState) -> Token {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve refused port");
    let peer_addr = listener.local_addr().expect("reserved address");
    drop(listener);
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("start nonblocking connect");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    backend
        .replication_runtime
        .set_replica_link(ReplicaLink::new(
            ConnectionId(peer.token.0),
            MasterInfo {
                host: peer_addr.ip().to_string(),
                port: peer_addr.port(),
            },
        ));
    backend.ctx.config_mut().replication.master_link_status = "connect".to_string();
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "refused connection must become ready before deadline"
        );
        let events = backend
            .transport
            .poll_events(Some(remaining))
            .expect("wait for refusal");
        if events.iter().any(|event| event.token == peer.token) {
            break;
        }
        std::thread::yield_now();
    }
    peer.token
}

fn assert_diagnostic_before_removal(logs: &[String], token: Token, operation: &str) {
    let prefix = format!(
        "token={} owner=connection operation={operation} error=",
        token.0
    );
    let diagnostic = logs
        .iter()
        .position(|message| message.contains(&prefix))
        .expect("diagnostic includes token, owner, operation and concrete error");
    assert!(
        logs[diagnostic].contains("Connection refused"),
        "{}",
        logs[diagnostic]
    );
    let removal = logs
        .iter()
        .position(|message| message.contains(&format!("remove connection token={}", token.0)))
        .expect("connection removal log");
    assert!(
        diagnostic < removal,
        "failure must be logged before cleanup"
    );
}

#[test]
fn writable_master_connect_logs_concrete_error_before_removal() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let token = refused_master(&mut backend);
    backend.collect_after_poll_events(Some((
        Instant::now(),
        vec![
            crate::runtime::server::worker::event_loop::transport::RuntimeEvent {
                token,
                readable: false,
                writable: true,
                read_hup: false,
                error: false,
            },
        ],
    )));
    let logs = capture_logs(|| {
        assert_eq!(
            backend
                .dispatch_ready_phase(
                    crate::runtime::server::worker::event_loop::DispatchContext::Normal
                )
                .expect("dispatch failed master connect"),
            LoopControl::Continue
        );
    });
    assert_diagnostic_before_removal(&logs, token, "connect");
    assert!(!backend.transport.is_registered(token));
    assert_eq!(backend.ctx.config().replication.master_link_status, "down");
    assert_eq!(backend.ctx.stats().eventloop_events_owner_closed, 1);
    assert_eq!(backend.ctx.stats().eventloop_events_handled, 0);
}

#[test]
fn master_error_readiness_logs_concrete_error_before_removal() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let token = refused_master(&mut backend);
    backend.collect_after_poll_events(Some((
        Instant::now(),
        vec![
            crate::runtime::server::worker::event_loop::transport::RuntimeEvent {
                token,
                readable: false,
                writable: true,
                read_hup: false,
                error: true,
            },
        ],
    )));
    let logs = capture_logs(|| {
        assert_eq!(
            backend
                .dispatch_ready_phase(
                    crate::runtime::server::worker::event_loop::DispatchContext::Normal
                )
                .expect("dispatch master error readiness"),
            LoopControl::Continue
        );
    });
    assert_diagnostic_before_removal(&logs, token, "readiness-failure");
    assert!(!backend.transport.is_registered(token));
}

#[test]
fn client_read_failure_logs_operation_and_error_before_cleanup() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let (_client, token) = connected_test_client(&mut backend);
    let logs = capture_logs(|| {
        backend.process_client_read_outcome(
            token,
            crate::runtime::server::worker::connection::ClientReadMode::Fair,
            Vec::new(),
            crate::foundation::io::transport::ReadOutcome::Failed(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "read failure fixture",
            )),
        );
    });
    let diagnostic = logs
        .iter()
        .position(|message| {
            message.contains(&format!(
                "token={} owner=connection operation=read error=read failure fixture",
                token.0
            ))
        })
        .expect("read failure diagnostic");
    let removal = logs
        .iter()
        .position(|message| message.contains(&format!("remove connection token={}", token.0)))
        .expect("read failure cleanup");
    assert!(diagnostic < removal);
}

#[test]
fn client_write_failure_logs_operation_and_error_before_cleanup() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let (_client, token) = connected_test_client(&mut backend);
    backend.connection_registry[token].queue_reply(b"pending".to_vec());
    backend.writes.ordinary.begin_turn();
    assert!(backend.schedule_ordinary_write(token.0));
    backend
        .transport
        .remove_registered(token)
        .expect("remove write transport fixture");
    let logs = capture_logs(|| backend.dispatch_client_write(token));
    let diagnostic = logs.iter().position(|message| message.contains(&format!(
        "token={} owner=ordinary-client operation=write error=client transport is not registered", token.0)))
        .expect("write failure diagnostic");
    let removal = logs
        .iter()
        .position(|message| message.contains(&format!("remove connection token={}", token.0)))
        .expect("write failure cleanup");
    assert!(diagnostic < removal);
}

#[test]
fn master_connect_probe_error_logs_concrete_error_before_cleanup() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let token = refused_master(&mut backend);
    backend
        .transport
        .remove_registered(token)
        .expect("remove connect probe transport fixture");
    let logs = capture_logs(|| backend.dispatch_client_write(token));
    let diagnostic = logs
        .iter()
        .position(|message| {
            message.contains(&format!(
                "token={} owner=connection operation=connect error=",
                token.0
            ))
        })
        .expect("connect probe failure diagnostic");
    let expected_error = std::io::Error::from(std::io::ErrorKind::NotFound).to_string();
    assert!(
        logs[diagnostic].contains(&expected_error),
        "{}",
        logs[diagnostic]
    );
    let removal = logs
        .iter()
        .position(|message| message.contains(&format!("remove connection token={}", token.0)))
        .expect("connect probe cleanup");
    assert!(diagnostic < removal);
    assert_eq!(backend.ctx.config().replication.master_link_status, "down");
}
