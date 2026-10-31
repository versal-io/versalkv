use super::pool::{read_with_budget, write_with_budget};
use super::{
    IoCompletion, IoOperationId, IoThreadRuntime, ReadCompletion, ReadCompletionAction, ReadJob,
    SharedIoPool, WorkerCompletionEvent, WriteCompletion, WriteCompletionAction, WriteJob,
};
use crate::foundation::io::transport::{
    PollingTransportRuntime, ReadOutcome, ThreadIoHandle, TransportWorkerBackend, WriteOutcome,
};
use crate::framework::capabilities::io::mt_driver::IoDriverMtConfig;
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::reactor::transport::{
    RuntimeEvent, RuntimePollJob, RuntimeTransport,
};
use crate::runtime::server::worker::reactor::{DispatchContext, EventLane, LoopControl};
use std::collections::VecDeque;
use std::io::{Cursor, Read, Write};
use std::net::Shutdown;
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};

fn recv_runtime_read_completion(
    runtime: &mut crate::runtime::server::worker::ServerWorkerState,
    timeout: Duration,
) -> ReadCompletion {
    match runtime
        .recv_worker_completion_for_test(timeout)
        .expect("worker read completion")
    {
        WorkerCompletionEvent::SocketRead(completion) => completion,
        other => panic!("expected worker read completion, got {other:?}"),
    }
}

fn recv_runtime_write_completion(
    runtime: &mut crate::runtime::server::worker::ServerWorkerState,
    timeout: Duration,
) -> WriteCompletion {
    match runtime
        .recv_worker_completion_for_test(timeout)
        .expect("worker write completion")
    {
        WorkerCompletionEvent::SocketWrite(completion) => completion,
        other => panic!("expected worker write completion, got {other:?}"),
    }
}

struct ErrorReader(std::io::ErrorKind);

impl Read for ErrorReader {
    fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
        Err(std::io::Error::new(self.0, "scripted read failure"))
    }
}

struct BytesThenErrorReader {
    bytes: Vec<u8>,
    error_kind: std::io::ErrorKind,
}

impl Read for BytesThenErrorReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if !self.bytes.is_empty() {
            let count = self.bytes.len().min(buffer.len());
            buffer[..count].copy_from_slice(&self.bytes[..count]);
            self.bytes.drain(..count);
            return Ok(count);
        }
        Err(std::io::Error::new(
            self.error_kind,
            "scripted read failure after bytes",
        ))
    }
}

struct ShortWriter;

impl Write for ShortWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        Ok(bytes.len().min(1))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct ErrorWriter(std::io::ErrorKind);

impl Write for ErrorWriter {
    fn write(&mut self, _bytes: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::new(self.0, "scripted write failure"))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn test_waker() -> crate::foundation::io::transport::TransportWakeHandle {
    let mut transport = PollingTransportRuntime::new().expect("test transport");
    transport
        .register_worker_waker()
        .expect("register worker waker")
        .1
}

fn read_job(token: usize, payload: &[u8]) -> (ReadJob, std::os::unix::net::UnixStream) {
    let (worker, mut peer) = std::os::unix::net::UnixStream::pair().expect("socket pair");
    worker.set_nonblocking(true).unwrap();
    peer.write_all(payload).unwrap();
    (
        ReadJob {
            id: IoOperationId {
                token: Token(token),
                generation: 1,
                sequence: 1,
            },
            handle: ThreadIoHandle::Unix(worker),
            max_bytes: 64 * 1024,
            drain_to_budget: false,
        },
        peer,
    )
}

fn write_job(token: usize, payload: &[u8]) -> (WriteJob, std::os::unix::net::UnixStream) {
    let (worker, peer) = std::os::unix::net::UnixStream::pair().expect("socket pair");
    worker.set_nonblocking(true).unwrap();
    peer.set_nonblocking(true).unwrap();
    (
        WriteJob {
            id: IoOperationId {
                token: Token(token),
                generation: 1,
                sequence: 1,
            },
            handle: ThreadIoHandle::Unix(worker),
            frames: VecDeque::from([payload.to_vec()]),
            byte_budget: usize::MAX,
        },
        peer,
    )
}

fn thread_handle_pair() -> (ThreadIoHandle, std::os::unix::net::UnixStream) {
    let (worker, peer) = std::os::unix::net::UnixStream::pair().expect("socket pair");
    worker.set_nonblocking(true).unwrap();
    (ThreadIoHandle::Unix(worker), peer)
}

fn exact_ping_echo_pipeline(total_bytes: usize) -> (Vec<u8>, Vec<u8>) {
    assert_eq!(total_bytes % 2, 0);
    let frame_bytes = total_bytes / 2;
    let payload_len = (0..=frame_bytes)
        .find(|payload_len| {
            let header = format!("*2\r\n$4\r\nECHO\r\n${payload_len}\r\n");
            header.len() + payload_len + 2 == frame_bytes
        })
        .expect("find exact-size ECHO payload");
    let mut request = Vec::with_capacity(total_bytes);
    let mut expected = Vec::new();
    for fill in *b"xy" {
        let payload = vec![fill; payload_len];
        request.extend_from_slice(format!("*2\r\n$4\r\nECHO\r\n${payload_len}\r\n").as_bytes());
        request.extend_from_slice(&payload);
        request.extend_from_slice(b"\r\n");
        expected.extend_from_slice(format!("${payload_len}\r\n").as_bytes());
        expected.extend_from_slice(&payload);
        expected.extend_from_slice(b"\r\n");
    }
    assert_eq!(request.len(), total_bytes);
    (request, expected)
}

fn fill_runtime_send_buffer(
    runtime: &mut crate::runtime::server::worker::ServerWorkerState,
    token: Token,
) -> usize {
    let payload = vec![b'z'; 64 * 1024];
    let mut written = 0usize;
    for _ in 0..2048 {
        let mut stream = runtime
            .transport
            .stream(token)
            .expect("registered client transport");
        match stream.write(&payload) {
            Ok(0) => panic!("client transport closed while filling send buffer"),
            Ok(count) => written += count,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return written,
            Err(error) => panic!("failed to fill client send buffer: {error}"),
        }
    }
    panic!("client send buffer did not reach WouldBlock");
}

fn test_runtime_with_workers() -> crate::runtime::server::worker::ServerWorkerState {
    let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).expect("runtime");
    runtime.ctx.config_mut().server.io_threads = 2;
    runtime.ctx.config_mut().server.io_threads_always_active = true;
    runtime
        .configure_io_threads_from_config()
        .expect("configure workers");
    runtime
}

#[test]
fn lazyfree_and_socket_completions_share_one_control_wake() {
    let mut runtime = test_runtime_with_workers();

    runtime.complete_test_data_lazyfree(3, 1);
    runtime.complete_test_socket_read(Token(8), b"PING\r\n".to_vec());

    let events = runtime.collect_control_events_for_test();
    assert_eq!(events.len(), 2);
    assert!(events.iter().all(|event| event.lane == EventLane::Control));
}

#[test]
fn replication_lazyfree_and_socket_completions_share_one_control_wake() {
    let mut runtime = test_runtime_with_workers();

    runtime.complete_test_replication_lazyfree(5);
    runtime.complete_test_socket_read(Token(9), b"PING\r\n".to_vec());

    let events = runtime.collect_control_events_for_test();
    assert_eq!(events.len(), 2);
    assert!(events.iter().all(|event| event.lane == EventLane::Control));
}

#[test]
fn shutdown_discards_queued_shared_socket_read_completion() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let peer_addr = listener.local_addr().unwrap();
    let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut runtime.registry);
    runtime.ctx.set_command_registry_snapshot(&runtime.registry);
    runtime.ctx.config_mut().server.io_threads = 2;
    runtime.ctx.config_mut().server.io_threads_always_active = true;
    runtime.configure_io_threads_from_config().unwrap();
    let peer = runtime.transport.connect_tcp(peer_addr).unwrap();
    let (_client, _) = listener.accept().unwrap();
    runtime.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr,
        },
    );

    runtime.complete_test_socket_read(peer.token, b"*1\r\n$4\r\nPING\r\n".to_vec());
    runtime.shutdown_io_threads();

    assert!(
        !runtime.connection_registry[peer.token].has_pending_output(),
        "shutdown must discard queued shared socket completions instead of dispatching commands"
    );
    assert_eq!(runtime.ctx.io_thread_info().reads_pending, 0);
    assert_eq!(runtime.ctx.io_thread_info().reads_processed, 1);
}

#[test]
fn configured_count_includes_the_main_thread() {
    let pool = SharedIoPool::new(IoDriverMtConfig::new(5), test_waker()).unwrap();

    assert_eq!(pool.configured_threads(), 5);
    assert_eq!(pool.worker_handles(), 4);
}

#[test]
fn reset_generation_rebases_processed_poll_counters() {
    let mut io = IoThreadRuntime::new(IoDriverMtConfig::new(1), Token(99), test_waker()).unwrap();
    io.pool().stats().record_poll_submitted();
    io.record_poll_completion(false);
    assert_eq!(io.info().polls_processed, 1);

    io.observe_counter_reset(1);

    let reset = io.info();
    assert_eq!(reset.polls_processed, 0);
    assert_eq!(reset.polls_pending, 0);
    assert_eq!(reset.counter_reset_generation, 1);
    io.pool().stats().record_poll_submitted();
    io.record_poll_completion(false);
    assert_eq!(io.info().polls_processed, 1);
}

#[test]
fn main_thread_poll_returned_metric_does_not_wake_control_source() {
    let mut runtime = test_runtime_with_workers();
    runtime.enqueue_poll_returned_metric_for_test();
    let events = runtime.collect_control_events_for_test();

    assert_eq!(events.len(), 1);
    assert!(events.iter().all(|event| event.lane == EventLane::Control));
    assert!(
        runtime
            .transport
            .poll_events(Some(Duration::ZERO))
            .unwrap()
            .is_empty(),
        "main-thread PollReturned accounting must not leave a stale worker wake"
    );
}

#[test]
fn worker_completion_uses_unified_ingress_metrics() {
    let mut runtime = test_runtime_with_workers();
    runtime.enqueue_poll_returned_metric_for_test();
    let collected_before = runtime.ctx.stats().eventloop_events_collected;
    let handled_before = runtime.ctx.stats().eventloop_events_handled;

    runtime.dispatch_worker_completion_events();

    assert_eq!(
        runtime.ctx.stats().eventloop_events_collected,
        collected_before + 1,
    );
    assert_eq!(
        runtime.ctx.stats().eventloop_events_handled,
        handled_before + 1,
    );
}

#[test]
fn terminal_io_worker_wake_uses_control_ingress() {
    for (read_closed, error) in [(false, true), (true, false)] {
        let mut runtime = test_runtime_with_workers();
        runtime.enqueue_poll_returned_metric_for_test();
        let wake_token = runtime.io_threads.wake_token();
        let collected_before = runtime.ctx.stats().eventloop_events_collected;
        let handled_before = runtime.ctx.stats().eventloop_events_handled;

        runtime.collect_after_poll_events(Some((
            Instant::now(),
            vec![RuntimeEvent {
                token: wake_token,
                readable: false,
                writable: false,
                read_hup: read_closed,
                error,
            }],
        )));

        assert_eq!(
            runtime
                .dispatch_ready_phase(DispatchContext::Normal)
                .expect("dispatch ready phase"),
            LoopControl::Continue
        );
        assert_eq!(
            runtime.ctx.stats().eventloop_events_collected,
            collected_before + 2,
        );
        assert_eq!(
            runtime.ctx.stats().eventloop_events_handled,
            handled_before + 3,
        );
    }
}

fn detached_poll(
    timeout: Option<Duration>,
) -> (
    RuntimeTransport<PollingTransportRuntime>,
    crate::foundation::io::transport::TransportWakeHandle,
    RuntimePollJob,
) {
    let mut transport = RuntimeTransport::new(PollingTransportRuntime::new().unwrap());
    let (_token, wake) = transport.register_thread_waker().unwrap();
    let job = transport
        .begin_poll_offload(timeout)
        .expect("detach poll state");
    (transport, wake, job)
}

#[test]
fn poll_executes_on_a_named_io_worker() {
    let mut pool = SharedIoPool::new(IoDriverMtConfig::new(2), test_waker()).unwrap();
    pool.set_active_workers(1);
    let (mut transport, wake, job) = detached_poll(Some(Duration::from_secs(1)));
    let (execution_tx, execution_rx) = std::sync::mpsc::channel();

    pool.try_submit_poll(job.observe_execution_for_test(execution_tx))
        .expect("submit poll job");
    assert_eq!(
        execution_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("poll worker execution"),
        "io_thd_1"
    );
    assert_eq!(pool.worker_handles(), 1);
    assert_eq!(pool.configured_threads(), 2);
    wake.wake().expect("wake poll worker");
    let IoCompletion::Poll(completion) = pool
        .recv_completion_timeout(Duration::from_secs(1))
        .expect("poll completion")
    else {
        panic!("expected poll completion");
    };

    assert_eq!(completion.executor_thread_name_for_test(), Some("io_thd_1"));
    transport
        .finish_poll_offload(completion)
        .expect("restore worker poll state");
}

#[test]
fn poll_worker_returns_after_timeout_without_a_wake() {
    let mut pool = SharedIoPool::new(IoDriverMtConfig::new(2), test_waker()).unwrap();
    pool.set_active_workers(1);
    let (mut transport, _wake, job) = detached_poll(Some(Duration::from_millis(10)));

    pool.try_submit_poll(job).expect("submit timed poll");
    let IoCompletion::Poll(completion) = pool
        .recv_completion_timeout(Duration::from_secs(1))
        .expect("timed poll completion")
    else {
        panic!("expected poll completion");
    };

    transport
        .finish_poll_offload(completion)
        .expect("restore timed poll state");
}

#[test]
fn one_configured_thread_returns_the_poll_job_for_synchronous_execution() {
    let mut pool = SharedIoPool::new(IoDriverMtConfig::new(1), test_waker()).unwrap();
    let (mut transport, _wake, job) = detached_poll(Some(Duration::ZERO));

    let job = pool
        .try_submit_poll(job)
        .expect_err("one configured thread has no worker")
        .into_inner();
    let completion = job.execute();

    assert_eq!(
        completion.executor_thread_name_for_test(),
        std::thread::current().name()
    );
    transport
        .finish_poll_offload(completion)
        .expect("restore synchronous poll state");
    assert_eq!(pool.worker_handles(), 0);
    assert_eq!(pool.stats().polls_processed(), 0);
}

#[test]
fn inactive_pool_returns_the_complete_poll_job() {
    let mut pool = SharedIoPool::new(IoDriverMtConfig::new(2), test_waker()).unwrap();
    let (mut transport, _wake, job) = detached_poll(Some(Duration::ZERO));

    let completion = pool
        .try_submit_poll(job)
        .expect_err("parked workers must reject poll jobs")
        .into_inner()
        .execute();

    transport
        .finish_poll_offload(completion)
        .expect("inactive fallback preserves poll ownership");
    assert_eq!(pool.stats().polls_pending(), 0);
}

#[test]
fn full_queue_returns_the_complete_poll_job_for_main_thread_fallback() {
    let (mut blocked_transport, blocked_wake, blocked_job) =
        detached_poll(Some(Duration::from_secs(30)));
    let mut pool = SharedIoPool::new_with_queue_capacity_for_test(
        IoDriverMtConfig::new(2),
        blocked_wake.clone(),
        1,
    )
    .unwrap();
    pool.set_active_workers(1);
    pool.try_submit_poll(blocked_job)
        .expect("submit blocking poll");
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while pool.stats().total_queue_depth() != 0 {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    let (mut queued_transport, _queued_wake, queued_job) = detached_poll(Some(Duration::ZERO));
    pool.try_submit_poll(queued_job).expect("fill worker queue");
    assert_eq!(pool.queue_depth(), 1, "poll jobs count toward queue depth");
    assert_eq!(pool.stats().polls_pending(), 2);
    let (mut fallback_transport, _fallback_wake, fallback_job) =
        detached_poll(Some(Duration::ZERO));

    let fallback = pool
        .try_submit_poll(fallback_job)
        .expect_err("full queue must return poll ownership")
        .into_inner()
        .execute();

    assert_eq!(
        fallback.executor_thread_name_for_test(),
        std::thread::current().name()
    );
    fallback_transport
        .finish_poll_offload(fallback)
        .expect("restore fallback poll state");
    blocked_wake.wake().expect("release blocking poll");
    let IoCompletion::Poll(completion) = pool
        .recv_completion_timeout(Duration::from_secs(1))
        .expect("blocking poll completion")
    else {
        panic!("expected blocking poll completion");
    };
    blocked_transport
        .finish_poll_offload(completion)
        .expect("restore blocking poll state");
    pool.stats().record_poll_completed();
    let IoCompletion::Poll(completion) = pool
        .recv_completion_timeout(Duration::from_secs(1))
        .expect("queued poll completion")
    else {
        panic!("expected queued poll completion");
    };
    queued_transport
        .finish_poll_offload(completion)
        .expect("restore queued poll state");
    pool.stats().record_poll_completed();
    assert_eq!(pool.stats().polls_pending(), 0);
}

#[test]
fn resize_wakes_poll_and_preserves_completion_and_active_time() {
    let (mut transport, wake, job) = detached_poll(Some(Duration::from_secs(30)));
    let mut pool = SharedIoPool::new(IoDriverMtConfig::new(2), wake).unwrap();
    pool.set_active_workers(1);
    let (execution_tx, execution_rx) = std::sync::mpsc::channel();
    pool.try_submit_poll(job.observe_execution_for_test(execution_tx))
        .expect("submit blocking poll");
    assert_eq!(
        execution_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "io_thd_1"
    );

    pool.resize(1).expect("resize wakes and joins poll worker");

    let IoCompletion::Poll(completion) = pool
        .recv_completion_timeout(Duration::from_secs(1))
        .expect("poll completion survives resize")
    else {
        panic!("expected poll completion");
    };
    transport
        .finish_poll_offload(completion)
        .expect("restore poll state after resize");
    let active_before = pool.stats().active_time_us(1);
    pool.resize(2).expect("restore worker count");
    assert!(active_before > 0);
    assert!(pool.stats().active_time_us(1) >= active_before);
}

#[test]
fn shutdown_wakes_poll_joins_worker_and_returns_poll_state() {
    let (mut transport, wake, job) = detached_poll(Some(Duration::from_secs(30)));
    let mut pool = SharedIoPool::new(IoDriverMtConfig::new(2), wake).unwrap();
    pool.set_active_workers(1);
    let (execution_tx, execution_rx) = std::sync::mpsc::channel();
    pool.try_submit_poll(job.observe_execution_for_test(execution_tx))
        .expect("submit blocking poll");
    assert_eq!(
        execution_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "io_thd_1"
    );

    let completions = pool.shutdown_and_drain();

    let completion = completions
        .into_iter()
        .find_map(|completion| match completion {
            IoCompletion::Poll(completion) => Some(completion),
            IoCompletion::Read(_) | IoCompletion::Write(_) | IoCompletion::WorkerStopped => None,
        })
        .expect("shutdown returns poll state");
    transport
        .finish_poll_offload(completion)
        .expect("restore poll state after shutdown");
    assert_eq!(pool.worker_handles(), 0);
}

#[test]
fn saturated_shutdown_cancels_queued_io_and_returns_all_ownership() {
    let (mut running_transport, running_wake, running_job) =
        detached_poll(Some(Duration::from_secs(30)));
    let (running_tx, running_rx) = std::sync::mpsc::channel();
    let mut pool =
        SharedIoPool::new_with_queue_capacity_for_test(IoDriverMtConfig::new(2), running_wake, 3)
            .unwrap();
    pool.set_active_workers(1);
    pool.try_submit_poll(running_job.observe_execution_for_test(running_tx))
        .unwrap();
    assert_eq!(
        running_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "io_thd_1"
    );
    assert_eq!(pool.worker_handles(), 1);
    assert_eq!(pool.configured_threads(), 2);

    let (queued_write, mut write_peer) = write_job(81, b"must-not-write");
    pool.try_submit_write(queued_write).unwrap();
    let (queued_read, _read_peer) = read_job(82, b"must-not-read");
    pool.try_submit_read(queued_read).unwrap();
    let (mut queued_transport, _queued_wake, queued_poll) =
        detached_poll(Some(Duration::from_secs(30)));
    let (queued_tx, queued_rx) = std::sync::mpsc::channel();
    pool.try_submit_poll(queued_poll.observe_execution_for_test(queued_tx))
        .unwrap();
    assert_eq!(pool.queue_depth(), 3);

    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
    let shutdown = std::thread::spawn(move || {
        let completions = pool.shutdown_and_drain();
        shutdown_tx.send((pool, completions)).unwrap();
    });
    let (pool, completions) = shutdown_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("saturated shutdown must not execute queued IO");
    shutdown.join().unwrap();
    assert_eq!(pool.queue_depth(), 0);
    assert!(queued_rx.try_recv().is_err());
    let mut read_buffer = [0_u8; 32];
    assert_eq!(
        write_peer.read(&mut read_buffer).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );

    let mut polls = Vec::new();
    let mut saw_read = false;
    let mut saw_write = false;
    for completion in completions {
        match completion {
            IoCompletion::Read(mut completion) => {
                saw_read = true;
                assert!(completion.bytes.is_empty());
                assert_eq!(completion.handle.read(&mut read_buffer).unwrap(), 13);
                assert_eq!(&read_buffer[..13], b"must-not-read");
                pool.stats().cancel_read_submission();
            }
            IoCompletion::Write(completion) => {
                saw_write = true;
                assert_eq!(completion.written, 0);
                assert_eq!(
                    completion.frames,
                    VecDeque::from([b"must-not-write".to_vec()])
                );
                pool.stats().cancel_write_submission();
            }
            IoCompletion::Poll(completion) => polls.push(completion),
            IoCompletion::WorkerStopped => {}
        }
    }
    assert!(saw_read && saw_write);
    assert_eq!(polls.len(), 2);
    running_transport
        .finish_poll_offload(polls.remove(0))
        .expect("running poll may complete during stop");
    pool.stats().record_poll_completed();
    assert_eq!(
        queued_transport
            .finish_poll_offload(polls.remove(0))
            .expect_err("queued poll must be cancelled")
            .kind(),
        std::io::ErrorKind::Interrupted
    );
    pool.stats().cancel_poll_submission();
    assert!(queued_transport.poll_events(Some(Duration::ZERO)).is_ok());
    assert_eq!(pool.stats().reads_pending(), 0);
    assert_eq!(pool.stats().writes_pending(), 0);
    assert_eq!(pool.stats().polls_pending(), 0);
}

#[test]
fn saturated_resize_cancels_queue_and_restores_connection_sidecars() {
    let (mut running_transport, running_wake, running_job) =
        detached_poll(Some(Duration::from_secs(30)));
    let (running_tx, running_rx) = std::sync::mpsc::channel();
    let mut io = IoThreadRuntime::new_with_queue_capacity_for_test(
        IoDriverMtConfig::new(2),
        Token(99),
        running_wake,
        3,
    )
    .unwrap();
    io.pool.set_active_workers(1);
    io.try_submit_poll(running_job.observe_execution_for_test(running_tx))
        .unwrap();
    assert_eq!(
        running_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "io_thd_1"
    );

    let token = Token(83);
    let (read_handle, mut read_peer) = thread_handle_pair();
    read_peer.write_all(b"queued-read").unwrap();
    let (write_handle, mut write_peer) = thread_handle_pair();
    write_peer.set_nonblocking(true).unwrap();
    io.register_connection(token, read_handle, write_handle);
    assert!(matches!(
        io.submit_write(
            token,
            VecDeque::from([b"queued-write".to_vec()]),
            usize::MAX
        ),
        super::WriteSubmission::Submitted
    ));
    assert!(io.submit_read(token, 64, false));
    let (mut queued_transport, _queued_wake, queued_poll) =
        detached_poll(Some(Duration::from_secs(30)));
    let (queued_tx, queued_rx) = std::sync::mpsc::channel();
    io.try_submit_poll(queued_poll.observe_execution_for_test(queued_tx))
        .unwrap();
    assert_eq!(io.pool().queue_depth(), 3);

    let (resize_tx, resize_rx) = std::sync::mpsc::channel();
    let resize = std::thread::spawn(move || {
        let result = io.configure(1, false);
        resize_tx.send((io, result)).unwrap();
    });
    let (mut io, resize_result) = resize_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("saturated resize must not execute queued IO");
    resize.join().unwrap();
    resize_result.unwrap();
    assert!(queued_rx.try_recv().is_err());

    let mut polls = Vec::new();
    while let Some(completion) = io.pool().try_recv_completion() {
        match completion {
            IoCompletion::Read(completion) => {
                assert!(matches!(
                    io.complete_read(completion),
                    ReadCompletionAction::Cancelled
                ));
            }
            IoCompletion::Write(completion) => {
                let WriteCompletionAction::Apply {
                    frames,
                    written,
                    outcome,
                    ..
                } = io.complete_write(completion)
                else {
                    panic!("cancelled write must restore its sidecar and frames");
                };
                assert_eq!(written, 0);
                assert!(matches!(outcome, WriteOutcome::Backpressured));
                assert_eq!(frames, VecDeque::from([b"queued-write".to_vec()]));
            }
            IoCompletion::Poll(completion) => polls.push(completion),
            IoCompletion::WorkerStopped => {}
        }
    }
    running_transport
        .finish_poll_offload(polls.remove(0))
        .unwrap();
    io.record_poll_completion(false);
    assert_eq!(
        queued_transport
            .finish_poll_offload(polls.remove(0))
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::Interrupted
    );
    io.pool().stats().cancel_poll_submission();
    assert!(io.connections.get(&token).unwrap().read_handle.is_some());
    assert!(io.connections.get(&token).unwrap().write_handle.is_some());
    let mut buffer = [0_u8; 32];
    assert_eq!(
        write_peer.read(&mut buffer).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(io.pool().queue_depth(), 0);
    assert_eq!(io.pool().stats().reads_pending(), 0);
    assert_eq!(io.pool().stats().writes_pending(), 0);
    assert_eq!(io.pool().stats().polls_pending(), 0);
    assert!(queued_transport.poll_events(Some(Duration::ZERO)).is_ok());
}

#[test]
fn runtime_shutdown_discards_completed_reads_without_dispatching_commands() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let peer_addr = listener.local_addr().unwrap();
    let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut runtime.registry);
    runtime.ctx.config_mut().server.io_threads = 2;
    runtime.ctx.config_mut().server.io_threads_always_active = true;
    runtime.configure_io_threads_from_config().unwrap();
    let peer = runtime.transport.connect_tcp(peer_addr).unwrap();
    let (mut client, _) = listener.accept().unwrap();
    runtime.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr,
        },
    );
    let (poll_tx, poll_rx) = std::sync::mpsc::channel();
    let poll_job = runtime
        .transport
        .begin_poll_offload(Some(Duration::from_secs(30)))
        .unwrap()
        .observe_execution_for_test(poll_tx);
    runtime.io_threads.try_submit_poll(poll_job).unwrap();
    assert_eq!(
        poll_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "io_thd_1"
    );
    client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    runtime
        .transport
        .wait_pending_bytes_for_test(peer.token, 14);
    assert!(runtime.try_submit_io_read(peer.token, false));

    runtime.shutdown_io_threads();

    assert!(!runtime.connection_registry[peer.token].has_pending_output());
    assert_eq!(runtime.ctx.io_thread_info().reads_pending, 0);
    assert!(
        runtime.ctx.io_thread_info().reads_processed <= 1,
        "shutdown may cancel a queued read or discard a read that completed before shutdown"
    );
    assert_eq!(runtime.ctx.io_thread_info().polls_pending, 0);
    assert_eq!(runtime.ctx.io_thread_info().polls_processed, 1);
    assert!(runtime.transport.poll_events(Some(Duration::ZERO)).is_ok());
}

#[test]
fn worker_read_records_active_time_and_returns_the_handle() {
    let mut pool = SharedIoPool::new(IoDriverMtConfig::new(2), test_waker()).unwrap();
    pool.set_active_workers(1);
    let (job, _peer) = read_job(7, b"ping");

    pool.try_submit_read(job).expect("submit read job");
    let completion = pool
        .recv_completion_timeout(Duration::from_secs(1))
        .expect("read completion");

    let IoCompletion::Read(completion) = completion else {
        panic!("expected read completion");
    };
    assert_eq!(completion.id.token, Token(7));
    assert_eq!(completion.bytes, b"ping");
    assert!(matches!(completion.outcome, ReadOutcome::Quiescent));
    drop(completion.handle);
    assert!(pool.try_recv_completion().is_none());
    assert!(pool.stats().active_time_us(1) > 0);
}

#[test]
fn resize_preserves_worker_active_time() {
    let mut pool = SharedIoPool::new(IoDriverMtConfig::new(2), test_waker()).unwrap();
    pool.set_active_workers(1);
    let (job, _peer) = read_job(8, b"work");
    pool.try_submit_read(job).unwrap();
    pool.recv_completion_timeout(Duration::from_secs(1))
        .unwrap();
    let before = pool.stats().active_time_us(1);

    pool.resize(1).unwrap();
    pool.resize(2).unwrap();

    assert!(before > 0);
    assert!(pool.stats().active_time_us(1) >= before);
}

#[test]
fn parked_time_is_not_counted_as_active() {
    let mut pool = SharedIoPool::new(IoDriverMtConfig::new(2), test_waker()).unwrap();
    pool.set_active_workers(1);
    let (job, _peer) = read_job(9, b"work");
    pool.try_submit_read(job).unwrap();
    pool.recv_completion_timeout(Duration::from_secs(1))
        .unwrap();
    let before = pool.stats().active_time_us(1);

    std::thread::sleep(Duration::from_millis(25));

    assert_eq!(pool.stats().active_time_us(1), before);
}

#[test]
fn closing_sidecar_waits_for_inflight_read() {
    let mut io = IoThreadRuntime::new(IoDriverMtConfig::new(1), Token(99), test_waker()).unwrap();
    let token = Token(7);
    let (read, _read_peer) = thread_handle_pair();
    let (write, _write_peer) = thread_handle_pair();
    io.register_connection(token, read, write);
    let job = io
        .take_read_job(token, 64, false)
        .expect("read job should be available");

    assert!(io.begin_close(token));
    let action = io.complete_read(ReadCompletion {
        id: job.id,
        handle: job.handle,
        bytes: Vec::new(),
        outcome: ReadOutcome::PeerClosed,
        drain_to_budget: false,
        cancelled: false,
    });

    assert!(matches!(action, ReadCompletionAction::CloseTransport(closed) if closed == token));
    assert!(!io.has_connection(token));
}

#[test]
fn stale_read_completion_cannot_touch_reused_token() {
    let mut io = IoThreadRuntime::new(IoDriverMtConfig::new(1), Token(99), test_waker()).unwrap();
    let token = Token(8);
    let (old_read, _old_read_peer) = thread_handle_pair();
    let (old_write, _old_write_peer) = thread_handle_pair();
    let old_generation = io.register_connection(token, old_read, old_write);
    assert!(!io.begin_close(token));

    let (new_read, _new_read_peer) = thread_handle_pair();
    let (new_write, _new_write_peer) = thread_handle_pair();
    let new_generation = io.register_connection(token, new_read, new_write);
    assert_ne!(old_generation, new_generation);
    let (stale_handle, _stale_peer) = thread_handle_pair();

    let action = io.complete_read(ReadCompletion {
        id: IoOperationId {
            token,
            generation: old_generation,
            sequence: 1,
        },
        handle: stale_handle,
        bytes: b"stale".to_vec(),
        outcome: ReadOutcome::Quiescent,
        drain_to_budget: false,
        cancelled: false,
    });

    assert!(matches!(action, ReadCompletionAction::Stale));
    assert_eq!(io.connection_generation(token), Some(new_generation));
}

#[test]
fn cluster_bus_full_queue_rolls_back_both_io_handles() {
    let (mut running_transport, wake, running_job) = detached_poll(Some(Duration::from_secs(30)));
    let (running_tx, running_rx) = std::sync::mpsc::channel();
    let mut io = IoThreadRuntime::new_with_queue_capacity_for_test(
        IoDriverMtConfig::new(2),
        Token(99),
        wake,
        1,
    )
    .unwrap();
    io.pool.set_active_workers(1);
    io.try_submit_poll(running_job.observe_execution_for_test(running_tx))
        .unwrap();
    assert_eq!(
        running_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "io_thd_1"
    );
    let (mut queued_transport, _wake, queued_job) = detached_poll(Some(Duration::from_secs(30)));
    io.try_submit_poll(queued_job).unwrap();

    let token = Token(84);
    let (read, _read_peer) = thread_handle_pair();
    let (write, _write_peer) = thread_handle_pair();
    io.register_cluster_bus(token, read, write);
    let super::WriteSubmission::Unavailable(frames) =
        io.submit_write(token, VecDeque::from([b"queued".to_vec()]), 64)
    else {
        panic!("full worker queue must return the write snapshot");
    };
    assert_eq!(frames, VecDeque::from([b"queued".to_vec()]));
    assert!(!io.is_write_inflight(token));
    assert!(io.connections[&token].write_handle.is_some());
    assert!(!io.submit_read(token, 64, false));
    assert!(io.read_inflight_id(token).is_none());
    assert!(io.connections[&token].read_handle.is_some());
    assert!(!io.cluster_read_deferred(token));
    assert_eq!(io.info().reads_pending, 0);
    assert_eq!(io.info().writes_pending, 0);

    io.configure(1, false).unwrap();
    let mut polls = Vec::new();
    while let Some(completion) = io.pool().try_recv_completion() {
        if let IoCompletion::Poll(completion) = completion {
            polls.push(completion);
        }
    }
    running_transport
        .finish_poll_offload(polls.remove(0))
        .unwrap();
    assert_eq!(
        queued_transport
            .finish_poll_offload(polls.remove(0))
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::Interrupted
    );
}

#[test]
fn cluster_bus_stale_completion_cannot_release_new_generation() {
    let mut io = IoThreadRuntime::new(IoDriverMtConfig::new(1), Token(99), test_waker()).unwrap();
    let token = Token(85);
    let (read, _read_peer) = thread_handle_pair();
    let (write, _write_peer) = thread_handle_pair();
    io.register_cluster_bus(token, read, write);
    let old = io.take_read_job(token, 64, false).unwrap();
    let (read, _read_peer) = thread_handle_pair();
    let (write, _write_peer) = thread_handle_pair();
    io.register_cluster_bus(token, read, write);
    let current = io.take_read_job(token, 64, false).unwrap();
    assert_ne!(old.id.generation, current.id.generation);
    assert!(matches!(
        io.complete_read(ReadCompletion {
            id: old.id,
            handle: old.handle,
            bytes: b"stale".to_vec(),
            outcome: ReadOutcome::PeerClosed,
            drain_to_budget: false,
            cancelled: false,
        }),
        ReadCompletionAction::Stale
    ));
    assert!(io.is_cluster_bus(token));
    assert_eq!(io.read_inflight_id(token), Some(current.id));
    assert!(!io.is_close_pending(token));
    assert!(matches!(io.complete_read(ReadCompletion {
        id: current.id, handle: current.handle, bytes: b"current".to_vec(),
        outcome: ReadOutcome::Quiescent, drain_to_budget: false, cancelled: false,
    }), ReadCompletionAction::Apply { bytes, .. } if bytes == b"current"));
    assert!(io.read_inflight_id(token).is_none());
}

#[test]
fn event_loop_owns_a_registered_main_thread_backend() {
    let runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();

    assert_eq!(runtime.io_threads.pool().configured_threads(), 1);
    assert_eq!(runtime.io_threads.pool().worker_handles(), 0);
    assert!(runtime
        .transport
        .is_registered(runtime.io_threads.wake_token()));
}

#[test]
fn adding_an_ordinary_connection_registers_thread_io_sidecar() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
    let peer = runtime
        .transport
        .connect_tcp(listener.local_addr().unwrap())
        .unwrap();
    let (_remote, _) = listener.accept().unwrap();

    runtime.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr: peer.peer_addr,
        },
    );

    assert!(runtime.io_threads.has_connection(peer.token));
    assert!(runtime
        .io_threads
        .connection_generation(peer.token)
        .is_some());
}

#[test]
fn event_loop_defers_transport_removal_until_inflight_read_returns() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
    let peer = runtime
        .transport
        .connect_tcp(listener.local_addr().unwrap())
        .unwrap();
    let (_remote, _) = listener.accept().unwrap();
    runtime.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr: peer.peer_addr,
        },
    );
    let job = runtime
        .io_threads
        .take_read_job(peer.token, 64, false)
        .unwrap();

    runtime.remove_connection(peer.token.0);

    assert_eq!(runtime.connected_clients(), 0);
    assert!(runtime.transport.is_registered(peer.token));
    runtime.complete_io_read(ReadCompletion {
        id: job.id,
        handle: job.handle,
        bytes: Vec::new(),
        outcome: ReadOutcome::PeerClosed,
        drain_to_budget: false,
        cancelled: false,
    });
    assert!(!runtime.transport.is_registered(peer.token));
}

#[test]
fn ordinary_read_runs_on_worker_and_dispatches_on_main() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut runtime.registry);
    runtime.ctx.set_command_registry_snapshot(&runtime.registry);
    runtime.ctx.config_mut().server.io_threads = 2;
    runtime.ctx.config_mut().server.io_threads_always_active = true;
    runtime.configure_io_threads_from_config().unwrap();
    let peer = runtime
        .transport
        .connect_tcp(listener.local_addr().unwrap())
        .unwrap();
    let (mut client, _) = listener.accept().unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    runtime.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr: peer.peer_addr,
        },
    );
    client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    runtime.transport.wait_readable_for_test(peer.token);
    runtime.io_threads_after_poll(1);

    runtime.dispatch_transport_event(
        crate::runtime::server::worker::reactor::ReadyEvent {
            token: peer.token,
            readable: true,
            writable: false,
            error: false,
            read_hup: false,
        },
        false,
    );
    runtime
        .wait_for_io_completion_for_test(Duration::from_secs(1))
        .unwrap();
    runtime.event_batch.begin_next_turn();
    runtime.writes.begin_turn();
    runtime.drive_pending_ordinary_writes();
    runtime
        .wait_for_io_completion_for_test(Duration::from_secs(1))
        .unwrap();

    let mut reply = [0_u8; 7];
    client.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"+PONG\r\n");
    assert!(runtime.io_threads.pool().stats().active_time_us(1) > 0);
    assert_eq!(runtime.ctx.io_thread_info().configured_threads, 2);
    assert_eq!(runtime.ctx.io_thread_info().active_threads, 2);
    assert_eq!(runtime.ctx.io_thread_info().reads_processed, 1);
    assert_eq!(runtime.ctx.io_thread_info().writes_processed, 1);
    assert_eq!(runtime.ctx.io_thread_info().reads_pending, 0);
    assert_eq!(runtime.ctx.io_thread_info().writes_pending, 0);
}

#[test]
fn sync_admission_waits_for_threaded_output_settlement() {
    let pending_output_error = b"-ERR SYNC and PSYNC are invalid with pending output\r\n";
    for settle_before_read in [true, false] {
        for request in [b"PSYNC ? -1\r\n".as_slice(), b"SYNC\r\n".as_slice()] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
            crate::framework::commands::register_all(&mut runtime.registry);
            runtime.ctx.set_command_registry_snapshot(&runtime.registry);
            runtime.ctx.config_mut().replication.repl_diskless_sync = true;
            runtime
                .ctx
                .config_mut()
                .replication
                .repl_diskless_sync_delay = 0;
            runtime.ctx.config_mut().server.io_threads = 2;
            runtime.ctx.config_mut().server.io_threads_always_active = true;
            runtime.configure_io_threads_from_config().unwrap();
            let peer = runtime
                .transport
                .connect_tcp(listener.local_addr().unwrap())
                .unwrap();
            let (mut client, _) = listener.accept().unwrap();
            client
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            runtime.add_connection(
                crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
                    token: peer.token,
                    peer_addr: peer.peer_addr,
                },
            );
            runtime.connection_registry[peer.token].queue_reply(b"+PONG\r\n".to_vec());
            runtime.writes.ordinary.begin_turn();
            assert!(runtime.schedule_ordinary_write(peer.token.0));
            runtime.drive_pending_ordinary_writes();
            let completion = recv_runtime_write_completion(&mut runtime, Duration::from_secs(2));
            assert!(completion.frames.is_empty());
            assert_eq!(completion.written, 7);
            assert!(runtime.io_threads.is_write_inflight(peer.token));
            assert!(!runtime.connection_registry[peer.token].has_pending_output());
            assert_eq!(
                runtime.connection_registry[peer.token].output_buffer_bytes(),
                7
            );
            let mut pong = [0_u8; 7];
            client.read_exact(&mut pong).unwrap();
            assert_eq!(&pong, b"+PONG\r\n");

            // The worker has returned a real write snapshot, but only the main
            // thread can settle its accounting before the next read is applied.
            let pending_completion = if settle_before_read {
                runtime.complete_io_write(completion);
                assert_eq!(
                    runtime.connection_registry[peer.token].output_buffer_bytes(),
                    0
                );
                None
            } else {
                Some(completion)
            };
            client.write_all(request).unwrap();
            runtime
                .transport
                .wait_pending_bytes_for_test(peer.token, request.len());
            assert!(runtime.try_submit_io_read(peer.token, false));
            let read = recv_runtime_read_completion(&mut runtime, Duration::from_secs(2));
            assert_eq!(read.bytes, request);
            runtime.complete_io_read(read);
            if settle_before_read {
                assert_eq!(runtime.ctx.stats().sync_full, 1);
                assert!(runtime.is_priority_connection(peer.token));
                assert!(runtime
                    .replication_runtime
                    .primary_for_connection(
                        crate::framework::distributed::replication::ConnectionId(peer.token.0),
                    )
                    .is_some());
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n") {
                    let mut byte = [0_u8; 1];
                    client.read_exact(&mut byte).unwrap();
                    header.push(byte[0]);
                    assert!(header.len() < 128);
                }
                assert!(
                    if request.starts_with(b"PSYNC") {
                        header.starts_with(b"+FULLRESYNC ")
                    } else {
                        header.starts_with(b"$")
                    },
                    "accepted sync must send its transfer header: {header:?}"
                );
            } else {
                let snapshot =
                    runtime.connection_registry[peer.token].take_threaded_write_snapshot();
                assert_eq!(
                    snapshot.frames(),
                    &VecDeque::from([pending_output_error.to_vec()]),
                    "unsettled write must reject {request:?}"
                );
                assert_eq!(runtime.ctx.stats().sync_full, 0);
                assert_eq!(runtime.ctx.stats().sync_partial_err, 0);
                assert!(!runtime.is_priority_connection(peer.token));
                assert!(runtime
                    .replication_runtime
                    .primary_for_connection(
                        crate::framework::distributed::replication::ConnectionId(peer.token.0),
                    )
                    .is_none());
                runtime.connection_registry[peer.token]
                    .restore_threaded_write_snapshot(snapshot.into_frames());
            }
            if let Some(completion) = pending_completion {
                runtime.complete_io_write(completion);
                assert!(!runtime.io_threads.is_write_inflight(peer.token));
                assert_eq!(
                    runtime.connection_registry[peer.token].output_buffer_bytes(),
                    pending_output_error.len()
                );
                runtime.event_batch.begin_next_turn();
                runtime.writes.begin_turn();
                runtime.schedule_ordinary_write(peer.token.0);
                runtime.drive_pending_ordinary_writes();
                let completion =
                    recv_runtime_write_completion(&mut runtime, Duration::from_secs(2));
                runtime.complete_io_write(completion);
                let mut error = vec![0_u8; pending_output_error.len()];
                client.read_exact(&mut error).unwrap();
                assert_eq!(error, pending_output_error);
                assert_eq!(
                    runtime.connection_registry[peer.token].output_buffer_bytes(),
                    0
                );
            }
        }
    }
}

#[test]
fn closing_inline_error_waits_for_threaded_output_settlement() {
    use crate::foundation::io::transport::Interest;
    use crate::framework::capabilities::connection::ConnectionState;

    let error = b"-ERR Protocol error: embedded NUL byte in inline request\r\n";
    let expected = [b"+PONG\r\n".as_slice(), error.as_slice()].concat();
    for byte_budget in [0, 3, 7, 10, expected.len()] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        runtime.ctx.config_mut().server.io_threads = 2;
        runtime.ctx.config_mut().server.io_threads_always_active = true;
        runtime.configure_io_threads_from_config().unwrap();
        let peer = runtime
            .transport
            .connect_tcp(listener.local_addr().unwrap())
            .unwrap();
        let (mut client, _) = listener.accept().unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        runtime.add_connection(
            crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
                token: peer.token,
                peer_addr: peer.peer_addr,
            },
        );

        let request = b"PING\r\nSET key value\0\r\nSET next value\r\n";
        client.write_all(request).unwrap();
        runtime
            .transport
            .wait_pending_bytes_for_test(peer.token, request.len());
        assert!(runtime.try_submit_io_read(peer.token, false));
        let read = recv_runtime_read_completion(&mut runtime, Duration::from_secs(2));
        assert_eq!(read.bytes, request);
        runtime.complete_io_read(read);
        assert_eq!(
            runtime.connection_registry[peer.token].state,
            ConnectionState::Closing
        );
        let snapshot = runtime.connection_registry[peer.token].take_threaded_write_snapshot();
        assert_eq!(
            snapshot.frames(),
            &VecDeque::from([b"+PONG\r\n".to_vec(), error.to_vec()])
        );
        assert!(matches!(
            runtime
                .io_threads
                .submit_write(peer.token, snapshot.into_frames(), byte_budget),
            super::WriteSubmission::Submitted
        ));
        let id = runtime.io_threads.write_inflight_id(peer.token).unwrap();
        runtime.acquire_io_worker_write_socket_lease(id);
        let completion = recv_runtime_write_completion(&mut runtime, Duration::from_secs(2));
        assert_eq!(completion.written, byte_budget);
        assert_eq!(
            completion
                .frames
                .iter()
                .flatten()
                .copied()
                .collect::<Vec<_>>(),
            expected[byte_budget..]
        );
        assert!(matches!(
            completion.outcome,
            WriteOutcome::Complete | WriteOutcome::BudgetExhausted
        ));
        assert!(runtime.io_threads.is_write_inflight(peer.token));
        assert!(!runtime.connection_registry[peer.token].has_pending_output());
        assert_eq!(
            runtime.connection_registry[peer.token].output_buffer_bytes(),
            expected.len()
        );

        // Hold a real completion until after the cron cleanup, fixing the
        // ordering independently of socket timing or worker scheduling.
        runtime.cleanup_closed();
        assert!(
            runtime.connection_registry.contains(peer.token),
            "cron must retain unsettled output for budget {byte_budget}"
        );
        assert!(!runtime.io_threads.is_close_pending(peer.token));
        runtime.transport.clear_interest_requests_for_test();
        runtime.complete_io_write(completion);
        assert!(!runtime.io_threads.is_write_inflight(peer.token));

        if byte_budget < expected.len() {
            assert!(runtime.connection_registry.contains(peer.token));
            assert_eq!(
                runtime.connection_registry[peer.token].state,
                ConnectionState::Closing
            );
            assert_eq!(
                runtime.connection_registry[peer.token].output_buffer_bytes(),
                expected.len() - byte_budget
            );
            assert_eq!(
                runtime.connection_registry[peer.token]
                    .output
                    .queue
                    .iter()
                    .flatten()
                    .copied()
                    .collect::<Vec<_>>(),
                expected[byte_budget..]
            );
            assert_eq!(
                runtime.transport.last_set_interest_calls_for_test().last(),
                Some(&(peer.token, Interest::Writable))
            );
            runtime.cleanup_closed();
            assert!(runtime.connection_registry.contains(peer.token));
            runtime.event_batch.begin_next_turn();
            runtime.writes.begin_turn();
            assert!(runtime.schedule_ordinary_write(peer.token.0));
            runtime.drive_pending_ordinary_writes();
            let completion = recv_runtime_write_completion(&mut runtime, Duration::from_secs(2));
            assert!(completion.frames.is_empty());
            assert_eq!(completion.written, expected.len() - byte_budget);
            assert!(matches!(completion.outcome, WriteOutcome::Complete));
            runtime.cleanup_closed();
            assert!(runtime.connection_registry.contains(peer.token));
            runtime.complete_io_write(completion);
        }
        assert!(!runtime.connection_registry.contains(peer.token));
        let mut reply = Vec::new();
        client.read_to_end(&mut reply).unwrap();
        assert_eq!(
            reply, expected,
            "all bytes must drain for budget {byte_budget}"
        );
    }
}

#[test]
fn threaded_mixed_write_advances_only_replication_tagged_cursor_bytes() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
    runtime.ctx.config_mut().server.io_threads = 2;
    runtime.ctx.config_mut().server.io_threads_always_active = true;
    runtime.configure_io_threads_from_config().unwrap();
    let peer = runtime
        .transport
        .connect_tcp(listener.local_addr().unwrap())
        .unwrap();
    let (mut client, _) = listener.accept().unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    runtime.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr: peer.peer_addr,
        },
    );
    runtime.connection_registry[peer.token].queue_reply(b"rdb".to_vec());
    runtime.connection_registry[peer.token].queue_reply(b"command".to_vec());
    runtime.connection_registry[peer.token].queue_replication_reply(b"tail".to_vec());
    runtime.writes.ordinary.begin_turn();
    assert!(runtime.schedule_ordinary_write(peer.token.0));
    runtime.drive_pending_ordinary_writes();
    let completion = recv_runtime_write_completion(&mut runtime, Duration::from_secs(1));

    let replica_id = runtime.replication_runtime.register_primary(
        crate::framework::distributed::replication::ConnectionId(peer.token.0),
        crate::framework::distributed::replication::ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let boundary = runtime.ctx.replication().backlog.snapshot_boundary();
    runtime
        .ctx
        .replication_mut()
        .backlog
        .append(b"tail")
        .unwrap();
    {
        let backlog = &runtime.ctx.replication().backlog;
        let link = runtime
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap();
        link.set_full_resync_boundary(boundary).unwrap();
        link.set_state(crate::framework::distributed::replication::PrimaryLinkState::Streaming);
        assert_eq!(link.pump(backlog), Ok(b"tail".to_vec()));
        link.mark_replication_queued(4);
    }

    runtime.complete_io_write(completion);

    let mut received = vec![0; b"rdbcommandtail".len()];
    client.read_exact(&mut received).unwrap();
    assert_eq!(received, b"rdbcommandtail");
    let link = runtime
        .replication_runtime
        .primary_link(replica_id)
        .unwrap();
    assert_eq!(link.sent_cursor().get(), 4);
    assert_eq!(link.sent_cursor(), link.next_send_cursor());
}

#[test]
fn threaded_accounting_error_closes_the_replica_link() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
    runtime.ctx.config_mut().server.io_threads = 2;
    runtime.ctx.config_mut().server.io_threads_always_active = true;
    runtime.configure_io_threads_from_config().unwrap();
    let peer = runtime
        .transport
        .connect_tcp(listener.local_addr().unwrap())
        .unwrap();
    let (_client, _) = listener.accept().unwrap();
    runtime.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr: peer.peer_addr,
        },
    );
    runtime.connection_registry[peer.token].queue_replication_reply(b"tail".to_vec());
    runtime.writes.ordinary.begin_turn();
    assert!(runtime.schedule_ordinary_write(peer.token.0));
    runtime.drive_pending_ordinary_writes();
    let mut completion = recv_runtime_write_completion(&mut runtime, Duration::from_secs(1));
    completion.written += 1;
    let replica_id = runtime.replication_runtime.register_primary(
        crate::framework::distributed::replication::ConnectionId(peer.token.0),
        crate::framework::distributed::replication::ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );

    runtime.complete_io_write(completion);

    assert!(!runtime.connection_registry.contains(peer.token));
    assert!(runtime
        .replication_runtime
        .primary_link(replica_id)
        .is_none());
}

#[test]
fn threaded_live_backlog_regression_closes_the_replica_link() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
    runtime.ctx.config_mut().server.io_threads = 2;
    runtime.ctx.config_mut().server.io_threads_always_active = true;
    runtime.configure_io_threads_from_config().unwrap();
    let peer = runtime
        .transport
        .connect_tcp(listener.local_addr().unwrap())
        .unwrap();
    let (_client, _) = listener.accept().unwrap();
    runtime.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr: peer.peer_addr,
        },
    );
    runtime.connection_registry[peer.token].queue_replication_reply(b"tail".to_vec());
    runtime.writes.ordinary.begin_turn();
    assert!(runtime.schedule_ordinary_write(peer.token.0));
    runtime.drive_pending_ordinary_writes();
    let completion = recv_runtime_write_completion(&mut runtime, Duration::from_secs(1));
    let replica_id = runtime.replication_runtime.register_primary(
        crate::framework::distributed::replication::ConnectionId(peer.token.0),
        crate::framework::distributed::replication::ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let mut future_backlog =
        crate::engine::distributed::replication::backlog::ReplicationBacklog::new(
            "future".to_string(),
            64,
        );
    future_backlog
        .restore_primary_from_rdb(
            "future",
            crate::engine::distributed::replication::ReplicationOffset::try_from(41).unwrap(),
        )
        .unwrap();
    let boundary = future_backlog.snapshot_boundary();
    future_backlog.append(b"tail").unwrap();
    {
        let link = runtime
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap();
        link.set_full_resync_boundary(boundary).unwrap();
        link.set_state(crate::framework::distributed::replication::PrimaryLinkState::Streaming);
        let queued = link.pump(&future_backlog).unwrap();
        assert_eq!(queued, b"tail");
        link.mark_replication_queued(queued.len());
    }

    runtime.complete_io_write(completion);

    assert!(!runtime.connection_registry.contains(peer.token));
    assert!(runtime
        .replication_runtime
        .primary_link(replica_id)
        .is_none());
}

#[test]
fn active_worker_hup_flushes_final_command_before_connection_teardown() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut runtime.registry);
    runtime.ctx.set_command_registry_snapshot(&runtime.registry);
    runtime.ctx.config_mut().server.io_threads = 2;
    runtime.ctx.config_mut().server.io_threads_always_active = true;
    runtime.configure_io_threads_from_config().unwrap();
    let peer = runtime
        .transport
        .connect_tcp(listener.local_addr().unwrap())
        .unwrap();
    let (mut client, _) = listener.accept().unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    runtime.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr: peer.peer_addr,
        },
    );
    client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    runtime.transport.wait_readable_for_test(peer.token);
    runtime.io_threads_after_poll(1);

    runtime.dispatch_transport_event(
        crate::runtime::server::worker::reactor::ReadyEvent {
            token: peer.token,
            readable: true,
            writable: false,
            error: false,
            read_hup: true,
        },
        false,
    );

    assert!(runtime.connection_registry.contains(peer.token));
    assert_eq!(
        runtime.connection_registry[peer.token].state,
        crate::framework::capabilities::connection::ConnectionState::Connected
    );
    assert!(!runtime.io_threads.is_close_pending(peer.token));

    runtime
        .wait_for_io_completion_for_test(Duration::from_secs(1))
        .unwrap();
    runtime.event_batch.begin_next_turn();
    runtime.writes.begin_turn();
    runtime.drive_pending_ordinary_writes();
    runtime
        .wait_for_io_completion_for_test(Duration::from_secs(1))
        .unwrap();

    let mut reply = Vec::new();
    client.read_to_end(&mut reply).unwrap();
    assert_eq!(reply, b"+PONG\r\n");
    assert!(!runtime.connection_registry.contains(peer.token));
    assert!(!runtime.transport.is_registered(peer.token));
    assert!(!runtime.io_threads.has_connection(peer.token));
    assert!(!runtime.client_hup_drains.contains(&peer.token));
    assert!(!runtime.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientRead(peer.token)
    ));
}

#[test]
fn hup_peer_close_waits_for_backpressured_inflight_write_frames() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut runtime.registry);
    runtime.ctx.set_command_registry_snapshot(&runtime.registry);
    runtime.ctx.config_mut().server.io_threads = 2;
    runtime.ctx.config_mut().server.io_threads_always_active = true;
    runtime.configure_io_threads_from_config().unwrap();
    let peer = runtime
        .transport
        .connect_tcp(listener.local_addr().unwrap())
        .unwrap();
    let (mut client, _) = listener.accept().unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    runtime.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr: peer.peer_addr,
        },
    );
    crate::foundation::io::sys::socket::set_buffer_size_raw(
        runtime.transport.raw_fd(peer.token).unwrap(),
        crate::foundation::io::sys::socket::SocketBuffer::Send,
        8 * 1024,
    )
    .unwrap();
    crate::foundation::io::sys::socket::set_buffer_size_raw(
        client.as_raw_fd(),
        crate::foundation::io::sys::socket::SocketBuffer::Receive,
        8 * 1024,
    )
    .unwrap();
    // The 8 KiB reverse-direction buffers stay small to force write
    // backpressure; the request direction must still hold the full unread
    // pipeline so the non-consuming peek barrier can complete on Linux.
    runtime
        .transport
        .reserve_test_receive_buffer(
            peer.token,
            2 * crate::runtime::server::worker::NORMAL_CLIENT_READ_BUDGET,
        )
        .unwrap();
    let prefilled = fill_runtime_send_buffer(&mut runtime, peer.token);
    let (request, expected) =
        exact_ping_echo_pipeline(2 * crate::runtime::server::worker::NORMAL_CLIENT_READ_BUDGET);
    client.write_all(&request).unwrap();
    runtime
        .transport
        .wait_pending_bytes_for_test(peer.token, request.len());
    client.shutdown(Shutdown::Write).unwrap();
    runtime.transport.wait_readable_for_test(peer.token);
    runtime.io_threads_after_poll(1);
    let worker_timeout = Duration::from_secs(10);

    runtime.dispatch_transport_event(
        crate::runtime::server::worker::reactor::ReadyEvent {
            token: peer.token,
            readable: true,
            writable: false,
            error: false,
            read_hup: true,
        },
        false,
    );
    let first_read = recv_runtime_read_completion(&mut runtime, worker_timeout);
    assert!(matches!(first_read.outcome, ReadOutcome::BudgetExhausted));
    runtime.complete_io_read(first_read);

    runtime.event_batch.begin_next_turn();
    runtime.writes.begin_turn();
    runtime.drive_pending_ordinary_writes();
    runtime.drive_one_pending_client_read();
    let first_write = recv_runtime_write_completion(&mut runtime, worker_timeout);
    assert!(matches!(first_write.outcome, WriteOutcome::Backpressured));
    assert!(!first_write.frames.is_empty());
    let second_read = recv_runtime_read_completion(&mut runtime, worker_timeout);
    assert!(matches!(second_read.outcome, ReadOutcome::BudgetExhausted));
    runtime.complete_io_read(second_read);
    runtime.complete_io_write(first_write);

    runtime.event_batch.begin_next_turn();
    runtime.writes.begin_turn();
    runtime.drive_pending_ordinary_writes();
    let final_read_job = runtime
        .io_threads
        .take_read_job(
            peer.token,
            crate::runtime::server::worker::NORMAL_CLIENT_READ_BUDGET,
            true,
        )
        .expect("read handle should be available for the final HUP drain");
    let second_write = recv_runtime_write_completion(&mut runtime, worker_timeout);
    assert!(matches!(second_write.outcome, WriteOutcome::Backpressured));
    assert!(!second_write.frames.is_empty());
    let final_read = ReadCompletion {
        id: final_read_job.id,
        handle: final_read_job.handle,
        bytes: Vec::new(),
        outcome: ReadOutcome::PeerClosed,
        drain_to_budget: final_read_job.drain_to_budget,
        cancelled: false,
    };
    assert!(runtime.io_threads.is_write_inflight(peer.token));
    runtime.writes.ordinary.begin_turn();
    runtime.schedule_runtime_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(peer.token),
    );
    runtime.cancel_runtime_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientRead(peer.token),
    );

    runtime.complete_io_read(final_read);

    assert!(runtime.connection_registry.contains(peer.token));
    assert_eq!(
        runtime.connection_registry[peer.token].state,
        crate::framework::capabilities::connection::ConnectionState::Closing
    );
    assert!(!runtime.io_threads.is_close_pending(peer.token));
    assert!(runtime.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(peer.token)
    ));

    let mut filler = vec![0_u8; prefilled];
    client.read_exact(&mut filler).unwrap();
    assert!(filler.iter().all(|byte| *byte == b'z'));
    crate::foundation::io::sys::socket::set_buffer_size_raw(
        client.as_raw_fd(),
        crate::foundation::io::sys::socket::SocketBuffer::Receive,
        1024 * 1024,
    )
    .unwrap();
    runtime.transport.clear_interest_requests_for_test();
    runtime.complete_io_write(second_write);
    assert_eq!(
        runtime.transport.last_set_interest_calls_for_test().last(),
        Some(&(
            peer.token,
            crate::foundation::io::transport::Interest::Writable
        ))
    );

    while runtime.connection_registry.contains(peer.token) {
        runtime.event_batch.begin_next_turn();
        runtime.writes.begin_turn();
        if !runtime.event_batch.contains_runnable(
            crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(peer.token),
        ) {
            assert!(runtime.schedule_ordinary_write(peer.token.0));
        }
        runtime.drive_pending_ordinary_writes();
        let completion = recv_runtime_write_completion(&mut runtime, Duration::from_secs(2));
        runtime.complete_io_write(completion);
    }

    let mut replies = Vec::new();
    client.read_to_end(&mut replies).unwrap();
    assert_eq!(replies, expected);
    assert!(!runtime.transport.is_registered(peer.token));
    assert!(!runtime.io_threads.has_connection(peer.token));
    assert_eq!(runtime.ctx.normal_client_memory(), 0);
    assert!(!runtime.client_hup_drains.contains(&peer.token));
    assert!(!runtime.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientRead(peer.token)
    ));
}

#[test]
fn worker_write_preserves_unwritten_frames_and_reports_progress() {
    let mut pool = SharedIoPool::new(IoDriverMtConfig::new(2), test_waker()).unwrap();
    pool.set_active_workers(1);
    let (worker, mut peer) = std::os::unix::net::UnixStream::pair().unwrap();
    worker.set_nonblocking(true).unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let first = b"first".to_vec();
    let second = b"second".to_vec();
    let expected_released = crate::foundation::memory::util_alloc::vec_allocation_bytes(&first)
        + crate::foundation::memory::util_alloc::vec_allocation_bytes(&second);
    let frames = std::collections::VecDeque::from([first, second]);
    let job = WriteJob {
        id: IoOperationId {
            token: Token(10),
            generation: 1,
            sequence: 1,
        },
        handle: ThreadIoHandle::Unix(worker),
        frames,
        byte_budget: 11,
    };

    pool.try_submit_write(job).unwrap();
    let completion = pool
        .recv_completion_timeout(Duration::from_secs(1))
        .unwrap();

    let IoCompletion::Write(completion) = completion else {
        panic!("expected write completion");
    };
    assert_eq!(completion.written, 11);
    assert!(matches!(completion.outcome, WriteOutcome::Complete));
    assert!(completion.frames.is_empty());
    assert_eq!(completion.released_allocation_bytes, expected_released);
    drop(completion.handle);
    let mut bytes = [0_u8; 11];
    peer.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"firstsecond");
}

#[test]
fn worker_read_maps_typed_stop_reasons_exactly() {
    let (bytes, budget) = read_with_budget(&mut Cursor::new(b"budget".to_vec()), 3);
    assert_eq!(bytes, b"bud");
    assert!(matches!(budget, ReadOutcome::BudgetExhausted));

    let (bytes, closed) = read_with_budget(&mut Cursor::new(b"tail".to_vec()), 8);
    assert_eq!(bytes, b"tail");
    assert!(matches!(closed, ReadOutcome::PeerClosed));

    let (bytes, quiescent) = read_with_budget(&mut ErrorReader(std::io::ErrorKind::WouldBlock), 8);
    assert!(bytes.is_empty());
    assert!(matches!(quiescent, ReadOutcome::Quiescent));

    let (bytes, failed) =
        read_with_budget(&mut ErrorReader(std::io::ErrorKind::ConnectionReset), 8);
    assert!(bytes.is_empty());
    assert!(matches!(
        failed,
        ReadOutcome::Failed(error)
            if error.kind() == std::io::ErrorKind::ConnectionReset
                && error.to_string() == "scripted read failure"
    ));

    let mut reader = BytesThenErrorReader {
        bytes: b"*1\r\n$4\r\nPING\r\n".to_vec(),
        error_kind: std::io::ErrorKind::ConnectionReset,
    };
    let (bytes, failed_after_bytes) = read_with_budget(&mut reader, 64);
    assert_eq!(bytes, b"*1\r\n$4\r\nPING\r\n");
    assert!(matches!(
        failed_after_bytes,
        ReadOutcome::Failed(error)
            if error.kind() == std::io::ErrorKind::ConnectionReset
                && error.to_string() == "scripted read failure after bytes"
    ));
}

#[test]
fn worker_write_maps_typed_stop_reasons_exactly() {
    let mut complete_frames = VecDeque::from([b"done".to_vec()]);
    let (_, _, complete) = write_with_budget(&mut Vec::new(), &mut complete_frames, 4);
    assert!(matches!(complete, WriteOutcome::Complete));
    assert!(complete_frames.is_empty());

    let mut budget_frames = VecDeque::from([b"first".to_vec(), b"second".to_vec()]);
    let (written, _, budget) = write_with_budget(&mut Vec::new(), &mut budget_frames, 5);
    assert_eq!(written, 5);
    assert!(matches!(budget, WriteOutcome::BudgetExhausted));
    assert_eq!(budget_frames, VecDeque::from([b"second".to_vec()]));

    let mut blocked_frames = VecDeque::from([b"blocked".to_vec()]);
    let (written, _, blocked) =
        write_with_budget(&mut ShortWriter, &mut blocked_frames, usize::MAX);
    assert_eq!(written, 1);
    assert!(matches!(blocked, WriteOutcome::Backpressured));
    assert_eq!(
        blocked_frames.front().map(Vec::as_slice),
        Some(b"locked".as_slice())
    );

    let mut failed_frames = VecDeque::from([b"failed".to_vec()]);
    let (written, _, failed) = write_with_budget(
        &mut ErrorWriter(std::io::ErrorKind::BrokenPipe),
        &mut failed_frames,
        usize::MAX,
    );
    assert_eq!(written, 0);
    assert!(matches!(
        failed,
        WriteOutcome::Failed(error)
            if error.kind() == std::io::ErrorKind::BrokenPipe
                && error.to_string() == "scripted write failure"
    ));
    assert_eq!(failed_frames, VecDeque::from([b"failed".to_vec()]));
}

#[test]
fn pending_runtime_request_resizes_the_pool() {
    let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
    let previous = runtime.ctx.config().clone();
    runtime.ctx.config_mut().server.io_threads = 3;
    runtime.ctx.request_io_threads_reconfigure(previous, 3);

    assert_eq!(
        runtime.resolve_io_threads_reconfigure_request(),
        Some(crate::foundation::io::protocol::Value::ok())
    );
    assert_eq!(runtime.io_threads.pool().configured_threads(), 3);
    assert_eq!(runtime.io_threads.pool().worker_handles(), 2);
}

#[test]
fn config_set_io_threads_resizes_before_command_completion() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut runtime.registry);
    runtime.ctx.set_command_registry_snapshot(&runtime.registry);
    let peer = runtime
        .transport
        .connect_tcp(listener.local_addr().unwrap())
        .unwrap();
    let (mut client, _) = listener.accept().unwrap();
    runtime.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr: peer.peer_addr,
        },
    );
    client
        .write_all(b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$10\r\nio-threads\r\n$1\r\n2\r\n")
        .unwrap();

    runtime.transport.wait_readable_for_test(peer.token);
    runtime.handle_readable(
        peer.token.0,
        crate::runtime::server::worker::sessions::ClientReadMode::Fair,
    );

    assert_eq!(runtime.io_threads.pool().configured_threads(), 2);
    assert_eq!(runtime.ctx.config().server.io_threads, 2);
}

#[test]
fn disabling_always_active_parks_idle_workers() {
    let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
    runtime.ctx.config_mut().server.io_threads = 2;
    runtime.ctx.config_mut().server.io_threads_always_active = true;
    runtime.configure_io_threads_from_config().unwrap();
    runtime.io_threads_after_poll(1);
    assert_eq!(runtime.ctx.io_thread_info().active_threads, 2);

    runtime.ctx.config_mut().server.io_threads_always_active = false;
    runtime.io_threads_before_sleep();

    assert_eq!(runtime.ctx.io_thread_info().active_threads, 1);
}
