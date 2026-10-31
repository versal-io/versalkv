use crate::foundation::io::transport::ReadOutcome;
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::reactor::io_workers::{
    IoCompletion, ReadCompletion, ReadJob, WorkerCompletionEvent,
};
use crate::runtime::server::worker::reactor::transport::{RuntimeConnectedPeer, RuntimeEvent};
use crate::runtime::server::worker::ServerWorkerState;
use std::io::Write;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::time::{Duration, Instant};

const PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";

fn ordinary_client() -> (ServerWorkerState, TcpStream, Token) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut runtime = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut runtime.registry);
    runtime.ctx.set_command_registry_snapshot(&runtime.registry);
    runtime.ctx.config_mut().server.io_threads = 2;
    runtime.ctx.config_mut().server.io_threads_always_active = true;
    runtime.configure_io_threads_from_config().unwrap();
    let peer = runtime
        .transport
        .connect_tcp(listener.local_addr().unwrap())
        .unwrap();
    let (client, _) = listener.accept().unwrap();
    runtime.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr: peer.peer_addr,
    });
    (runtime, client, peer.token)
}

fn lease_read(runtime: &mut ServerWorkerState, token: Token, max_bytes: usize) -> ReadJob {
    let job = runtime
        .io_threads
        .take_read_job(token, max_bytes, false)
        .unwrap();
    runtime.acquire_io_worker_read_socket_lease(job.id);
    job
}

fn execute_read(job: ReadJob) -> ReadCompletion {
    match crate::runtime::server::worker::reactor::io_workers::IoJob::Read(job)
        .execute()
        .unwrap()
    {
        IoCompletion::Read(completion) => completion,
        _ => panic!("expected a read completion"),
    }
}

fn replay_poll_observation(
    runtime: &mut ServerWorkerState,
    observed: &[RuntimeEvent],
) -> Vec<RuntimeEvent> {
    let events: Vec<_> = observed
        .iter()
        .map(|event| {
            (
                event.token,
                event.readable,
                event.writable,
                event.read_hup,
                event.error,
            )
        })
        .collect();
    runtime.transport.queue_test_transport_event_batch(&events);
    runtime
        .poll_ready_with_timeout(Some(Duration::ZERO))
        .unwrap()
        .unwrap()
        .1
}

fn completed_poll_read(
    runtime: &mut ServerWorkerState,
    client: &mut TcpStream,
    token: Token,
    max_bytes: usize,
    payload: &[u8],
) -> (ReadCompletion, Vec<RuntimeEvent>) {
    client.write_all(payload).unwrap();
    runtime
        .transport
        .wait_pending_bytes_for_test(token, payload.len());
    let job = lease_read(runtime, token, max_bytes);
    // Capture real LT readiness before the leased job consumes the socket.
    let observed = runtime.transport.wait_readable_for_test(token);
    let completion = execute_read(job);
    assert_eq!(completion.id.token, token);
    (completion, observed)
}

fn assert_no_readable(events: &[RuntimeEvent], token: Token) {
    assert!(
        !events
            .iter()
            .any(|event| event.token == token && event.readable),
        "a polled observation covered by the existing read must not start another read"
    );
}

fn finish_pending_read(runtime: &mut ServerWorkerState, token: Token) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while runtime.io_threads.read_inflight_id(token).is_some() {
        let completion = runtime
            .recv_worker_completion_for_test(deadline.saturating_duration_since(Instant::now()))
            .unwrap();
        eprintln!("primary-io test completion: {completion:?}");
        runtime.apply_worker_completion(completion);
    }
    runtime.sync_io_thread_stats();
}

#[test]
fn primary_io_completed_read_does_not_resubmit_old_poll_observation() {
    let (mut runtime, mut client, token) = ordinary_client();
    let (completion, observed) = completed_poll_read(&mut runtime, &mut client, token, 65536, PING);
    assert_eq!(completion.bytes, PING);
    assert!(matches!(completion.outcome, ReadOutcome::Quiescent));
    runtime.enqueue_worker_completion(WorkerCompletionEvent::SocketRead(completion));
    let events = replay_poll_observation(&mut runtime, &observed);
    runtime.dispatch_ready_batch(events);
    assert!(
        runtime.io_threads.read_inflight_id(token).is_none(),
        "the old poll observation submitted a second empty read"
    );
    assert_eq!(runtime.ctx.io_thread_info().reads_processed, 1);
    assert_eq!(runtime.ctx.stats().cmd_stats["ping"].calls, 1);
    assert!(runtime.socket_read_owned_by_main_thread(token));
}

#[test]
fn primary_io_unfinished_read_keeps_lease_without_replaying_readable() {
    let (mut runtime, mut client, token) = ordinary_client();
    client.write_all(PING).unwrap();
    runtime
        .transport
        .wait_pending_bytes_for_test(token, PING.len());
    let job = lease_read(&mut runtime, token, 65536);
    let observed = runtime.transport.wait_readable_for_test(token);
    let events = replay_poll_observation(&mut runtime, &observed);
    assert_no_readable(&events, token);
    runtime.dispatch_ready_batch(events);
    assert_eq!(runtime.io_threads.read_inflight_id(token), Some(job.id));
    assert!(!runtime.socket_read_owned_by_main_thread(token));
    assert_eq!(runtime.ctx.io_thread_info().reads_processed, 0);
    runtime.complete_io_read(execute_read(job));
    runtime.sync_io_thread_stats();
    assert_eq!(runtime.ctx.io_thread_info().reads_processed, 1);
}

#[test]
fn primary_io_new_bytes_after_quiescent_completion_return_on_next_lt_poll() {
    let (mut runtime, mut client, token) = ordinary_client();
    let (completion, observed) = completed_poll_read(&mut runtime, &mut client, token, 65536, PING);
    assert!(matches!(completion.outcome, ReadOutcome::Quiescent));
    client.write_all(PING).unwrap();
    runtime
        .transport
        .wait_pending_bytes_for_test(token, PING.len());
    runtime.enqueue_worker_completion(WorkerCompletionEvent::SocketRead(completion));
    let events = replay_poll_observation(&mut runtime, &observed);
    assert_no_readable(&events, token);
    runtime.dispatch_ready_batch(events);
    while runtime.io_threads.is_write_inflight(token) {
        runtime
            .wait_for_io_completion_for_test(Duration::from_secs(1))
            .unwrap();
    }
    let next = runtime.transport.wait_readable_for_test(token);
    assert!(next
        .iter()
        .any(|event| event.token == token && event.readable));
    runtime.event_batch.begin_next_turn();
    runtime.writes.begin_turn();
    runtime.dispatch_ready_batch(next);
    assert!(runtime.io_threads.read_inflight_id(token).is_some());
    finish_pending_read(&mut runtime, token);
    assert_eq!(runtime.ctx.stats().cmd_stats["ping"].calls, 2);
    assert_eq!(runtime.ctx.io_thread_info().reads_processed, 2);
}

#[test]
fn primary_io_cancelled_read_retries_unconsumed_bytes_on_next_lt_poll() {
    let (mut runtime, mut client, token) = ordinary_client();
    client.write_all(PING).unwrap();
    runtime
        .transport
        .wait_pending_bytes_for_test(token, PING.len());
    let job = lease_read(&mut runtime, token, 65536);
    let observed = runtime.transport.wait_readable_for_test(token);
    let completion = match crate::runtime::server::worker::reactor::io_workers::IoJob::Read(job)
        .cancel()
        .unwrap()
    {
        IoCompletion::Read(completion) => completion,
        _ => panic!("expected a cancelled read"),
    };
    runtime.enqueue_worker_completion(WorkerCompletionEvent::SocketRead(completion));
    let events = replay_poll_observation(&mut runtime, &observed);
    assert_no_readable(&events, token);
    assert_eq!(runtime.ctx.io_thread_info().reads_processed, 0);
    assert!(runtime.socket_read_owned_by_main_thread(token));
    let next = runtime.transport.wait_readable_for_test(token);
    runtime.dispatch_ready_batch(next);
    runtime
        .wait_for_io_completion_for_test(Duration::from_secs(1))
        .unwrap();
    assert_eq!(runtime.ctx.stats().cmd_stats["ping"].calls, 1);
    assert_eq!(runtime.ctx.io_thread_info().reads_processed, 1);
}

#[test]
fn primary_io_budget_completion_uses_one_continuation_without_old_poll_read() {
    let (mut runtime, mut client, token) = ordinary_client();
    let payload = [PING, PING].concat();
    let (completion, observed) =
        completed_poll_read(&mut runtime, &mut client, token, PING.len(), &payload);
    assert_eq!(completion.bytes, PING);
    assert!(matches!(completion.outcome, ReadOutcome::BudgetExhausted));
    runtime.enqueue_worker_completion(WorkerCompletionEvent::SocketRead(completion));
    let events = replay_poll_observation(&mut runtime, &observed);
    assert_no_readable(&events, token);
    runtime.dispatch_ready_batch(events);
    assert_eq!(runtime.ctx.stats().cmd_stats["ping"].calls, 1);
    assert!(runtime.drive_one_pending_client_read());
    finish_pending_read(&mut runtime, token);
    assert_eq!(runtime.ctx.stats().cmd_stats["ping"].calls, 2);
    assert_eq!(runtime.ctx.io_thread_info().reads_processed, 2);
    assert!(!runtime.drive_one_pending_client_read());
}

#[test]
fn primary_io_terminal_and_write_bits_are_preserved_while_read_is_leased() {
    for (readable, writable, read_hup, error) in [
        (true, true, false, false),
        (true, false, true, false),
        (false, false, true, false),
        (true, true, false, true),
        (false, true, false, false),
    ] {
        let (mut runtime, _client, token) = ordinary_client();
        let job = lease_read(&mut runtime, token, 65536);
        let observed = [RuntimeEvent {
            token,
            readable,
            writable,
            read_hup,
            error,
        }];
        let events = replay_poll_observation(&mut runtime, &observed);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].writable, writable);
        assert_eq!(events[0].read_hup, read_hup);
        assert_eq!(events[0].error, error);
        assert_eq!(events[0].readable, readable && (read_hup || error));
        assert_eq!(runtime.io_threads.read_inflight_id(token), Some(job.id));
        assert!(!runtime.socket_read_owned_by_main_thread(token));
    }
}

#[test]
fn primary_io_eof_completion_closes_transport_without_new_read() {
    let (mut runtime, client, token) = ordinary_client();
    let job = lease_read(&mut runtime, token, 65536);
    client.shutdown(Shutdown::Write).unwrap();
    let observed = runtime.transport.wait_readable_for_test(token);
    let completion = execute_read(job);
    assert!(completion.bytes.is_empty());
    assert!(matches!(completion.outcome, ReadOutcome::PeerClosed));
    runtime.enqueue_worker_completion(WorkerCompletionEvent::SocketRead(completion));
    let events = replay_poll_observation(&mut runtime, &observed);
    runtime.dispatch_ready_batch(events);
    assert!(!runtime.transport.is_registered(token));
    assert!(!runtime.connection_registry.contains(token));
    assert!(runtime.io_threads.read_inflight_id(token).is_none());
    assert_eq!(runtime.ctx.io_thread_info().reads_processed, 1);
}

#[test]
fn primary_io_unleased_readiness_dispatches_normal_worker_read() {
    let (mut runtime, mut client, token) = ordinary_client();
    client.write_all(PING).unwrap();
    runtime
        .transport
        .wait_pending_bytes_for_test(token, PING.len());
    let observed = runtime.transport.wait_readable_for_test(token);
    let events = replay_poll_observation(&mut runtime, &observed);
    assert!(events
        .iter()
        .any(|event| event.token == token && event.readable));
    runtime.dispatch_ready_batch(events);
    finish_pending_read(&mut runtime, token);
    assert_eq!(runtime.ctx.stats().cmd_stats["ping"].calls, 1);
    assert_eq!(runtime.ctx.io_thread_info().reads_processed, 1);
}

#[test]
fn primary_io_empty_quiescent_completion_still_counts_as_a_read_job() {
    let (mut runtime, _client, token) = ordinary_client();
    let job = lease_read(&mut runtime, token, 65536);
    let completion = execute_read(job);
    assert!(completion.bytes.is_empty());
    assert!(matches!(completion.outcome, ReadOutcome::Quiescent));
    runtime.enqueue_worker_completion(WorkerCompletionEvent::SocketRead(completion));
    let events = replay_poll_observation(&mut runtime, &[]);
    assert!(events.is_empty());
    assert_eq!(runtime.ctx.io_thread_info().reads_processed, 1);
    assert!(runtime.io_threads.read_inflight_id(token).is_none());
    assert!(runtime.socket_read_owned_by_main_thread(token));
}

#[test]
fn primary_io_stale_completion_cannot_release_current_read_generation() {
    let (mut runtime, mut client, token) = ordinary_client();
    let old = lease_read(&mut runtime, token, 65536);
    runtime.release_io_worker_read_socket_lease(old.id);
    runtime.io_threads.register_connection(
        token,
        runtime.transport.duplicate_stream(token).unwrap(),
        runtime.transport.duplicate_stream(token).unwrap(),
    );
    client.write_all(PING).unwrap();
    runtime
        .transport
        .wait_pending_bytes_for_test(token, PING.len());
    let current = lease_read(&mut runtime, token, 65536);
    assert_ne!(old.id.generation, current.id.generation);
    let observed = runtime.transport.wait_readable_for_test(token);
    runtime.enqueue_worker_completion(WorkerCompletionEvent::SocketRead(ReadCompletion {
        id: old.id,
        handle: old.handle,
        bytes: Vec::new(),
        outcome: ReadOutcome::Quiescent,
        drain_to_budget: false,
        cancelled: false,
    }));
    let events = replay_poll_observation(&mut runtime, &observed);
    assert_no_readable(&events, token);
    assert_eq!(runtime.io_threads.read_inflight_id(token), Some(current.id));
    assert!(!runtime.socket_read_owned_by_main_thread(token));
    runtime.complete_io_read(execute_read(current));
    runtime.sync_io_thread_stats();
    assert!(runtime.socket_read_owned_by_main_thread(token));
    assert_eq!(runtime.ctx.stats().cmd_stats["ping"].calls, 1);
    assert_eq!(runtime.ctx.io_thread_info().reads_processed, 2);
}

#[test]
fn primary_io_inflight_read_does_not_delay_due_time_events() {
    let (mut runtime, mut client, token) = ordinary_client();
    client.write_all(PING).unwrap();
    runtime
        .transport
        .wait_pending_bytes_for_test(token, PING.len());
    let job = lease_read(&mut runtime, token, 65536);
    let observed = runtime.transport.wait_readable_for_test(token);
    runtime.create_test_time_event(|worker| {
        worker.ctx.stats_mut().incr_cmd_call("primary_io_due_timer");
    });
    let events = replay_poll_observation(&mut runtime, &observed);
    runtime.dispatch_ready_batch(events);
    runtime.collect_due_time_events();
    runtime.dispatch_time_phase().unwrap();
    assert_eq!(
        runtime.ctx.stats().cmd_stats["primary_io_due_timer"].calls,
        1
    );
    assert_eq!(runtime.io_threads.read_inflight_id(token), Some(job.id));
    assert!(!runtime.socket_read_owned_by_main_thread(token));
    runtime.complete_io_read(execute_read(job));
}

#[test]
fn primary_io_eof_after_quiescent_read_returns_on_next_lt_poll() {
    let (mut runtime, mut client, token) = ordinary_client();
    let (completion, observed) = completed_poll_read(&mut runtime, &mut client, token, 65536, PING);
    assert!(matches!(completion.outcome, ReadOutcome::Quiescent));
    client.shutdown(Shutdown::Write).unwrap();
    runtime.enqueue_worker_completion(WorkerCompletionEvent::SocketRead(completion));
    let events = replay_poll_observation(&mut runtime, &observed);
    assert_no_readable(&events, token);
    runtime.dispatch_ready_batch(events);
    let next = runtime.transport.wait_readable_for_test(token);
    assert!(next
        .iter()
        .any(|event| event.token == token && event.read_hup));
    runtime.event_batch.begin_next_turn();
    runtime.writes.begin_turn();
    runtime.dispatch_ready_batch(next);
    finish_pending_read(&mut runtime, token);
    eprintln!(
        "primary-io EOF: connected={} registered={} owner={:?} write={:?}",
        runtime.connection_registry.contains(token),
        runtime.transport.is_registered(token),
        runtime.socket_owner(token),
        runtime.io_threads.write_inflight_id(token),
    );
    while runtime.io_threads.is_write_inflight(token) {
        runtime
            .wait_for_io_completion_for_test(Duration::from_secs(1))
            .unwrap();
    }
    assert_eq!(runtime.ctx.stats().cmd_stats["ping"].calls, 1);
    assert!(!runtime.transport.is_registered(token));
    assert!(!runtime.connection_registry.contains(token));
}
