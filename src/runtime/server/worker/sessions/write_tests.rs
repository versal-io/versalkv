use super::*;
use crate::engine::distributed::cluster::migration::{
    ExportPhase, MigrationJob, MigrationJobId, MigrationOperation, MigrationPhase,
    SlotMigrationConnectionRole, SlotRanges, TerminalPhase,
};
use crate::engine::distributed::cluster::{ClusterState, NodeId, SlotRange};
use crate::engine::distributed::replication::ReplicationOffset;
use crate::foundation::io::transport::WriteOutcome;
use crate::framework::distributed::replication::{
    ConnectionId, PrimaryLinkState, RdbFilterMode, ReplicaConnectionInfo, ReplicaId,
};
use crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer;
use socket2::SockRef;
use std::io::{IoSlice, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::{Duration, Instant};

fn offset(value: i64) -> ReplicationOffset {
    ReplicationOffset::try_from(value).expect("valid test offset")
}

fn slot_export_key(token_value: usize) -> crate::runtime::server::worker::reactor::RunnableKey {
    crate::runtime::server::worker::reactor::RunnableKey::SlotExportWrite(Token(token_value))
}

struct OneWriteThenBlock {
    accepted: usize,
    calls: usize,
}

struct FailingWriter(std::io::ErrorKind);

impl Write for FailingWriter {
    fn write(&mut self, _bytes: &[u8]) -> std::io::Result<usize> {
        Err(self.0.into())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct WriteZero;

struct ProgressThenBlock {
    calls: usize,
}

enum InterruptedFollowup {
    Success,
    WouldBlock,
}

struct InterruptedThenWriter {
    calls: usize,
    followup: InterruptedFollowup,
    written: Vec<u8>,
}

impl Write for InterruptedThenWriter {
    fn write(&mut self, _bytes: &[u8]) -> std::io::Result<usize> {
        unreachable!("the Interrupted retry tests use vectored writes")
    }

    fn write_vectored(&mut self, buffers: &[IoSlice<'_>]) -> std::io::Result<usize> {
        self.calls += 1;
        if self.calls == 1 {
            return Err(std::io::ErrorKind::Interrupted.into());
        }
        match self.followup {
            InterruptedFollowup::Success => {
                for buffer in buffers {
                    self.written.extend_from_slice(buffer);
                }
                Ok(buffers.iter().map(|buffer| buffer.len()).sum())
            }
            InterruptedFollowup::WouldBlock => Err(std::io::ErrorKind::WouldBlock.into()),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Write for ProgressThenBlock {
    fn write(&mut self, _bytes: &[u8]) -> std::io::Result<usize> {
        unreachable!("the outcome test uses vectored writes")
    }

    fn write_vectored(&mut self, buffers: &[IoSlice<'_>]) -> std::io::Result<usize> {
        self.calls += 1;
        if self.calls == 1 {
            Ok(buffers.iter().map(|buffer| buffer.len()).sum())
        } else {
            Err(std::io::ErrorKind::WouldBlock.into())
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Write for WriteZero {
    fn write(&mut self, _bytes: &[u8]) -> std::io::Result<usize> {
        Ok(0)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Write for OneWriteThenBlock {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.calls += 1;
        if self.calls == 1 {
            Ok(bytes.len().min(self.accepted))
        } else {
            Err(std::io::ErrorKind::WouldBlock.into())
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn connected_streaming_slot_export(mutation_bytes: usize) -> (ServerWorkerState, TcpStream, usize) {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind export peer");
    let peer_addr = listener.local_addr().expect("export peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect export transport");
    let (export_target, _) = listener.accept().expect("accept export transport");
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
    backend.connection_registry[Token(connection_token)].slot_migration_role =
        Some(SlotMigrationConnectionRole::Export(id.clone()));
    backend.slot_migration_runtime.register_export(
        crate::framework::distributed::migration::SlotExportLink::new(id, peer.token.0),
    );
    backend.connection_registry[Token(connection_token)]
        .queue_replication_reply(vec![b'm'; mutation_bytes]);
    backend.track_slot_export_connection_output(connection_token);
    (backend, export_target, peer.token.0)
}

fn connected_ordinary_client() -> (ServerWorkerState, TcpStream, Token) {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ordinary peer");
    let peer_addr = listener.local_addr().expect("ordinary peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect ordinary transport");
    let (target, _) = listener.accept().expect("accept ordinary transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    (backend, target, peer.token)
}

fn fill_transport_send_buffer(backend: &mut ServerWorkerState, token: Token) {
    let payload = vec![b'x'; 256 * 1024];
    for _ in 0..2048 {
        let Some(mut stream) = backend.transport.stream(token) else {
            panic!("registered ordinary stream must be available");
        };
        match stream.write(&payload) {
            Ok(0) => break,
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return,
            Err(error) => panic!("prefill write failed: {error}"),
        }
    }
    panic!("ordinary peer send buffer did not reach WouldBlock");
}

#[test]
fn partial_slot_export_write_retains_unaccepted_replication_bytes() {
    let mut connection = Connection::new_fake();
    connection.queue_replication_reply(b"mutation-frame".to_vec());
    let queued_before = connection.output.queue.iter().map(Vec::len).sum::<usize>();
    let mut queue = std::mem::take(&mut connection.output.queue);
    let mut writer = OneWriteThenBlock {
        accepted: 5,
        calls: 0,
    };

    let allocation_before = connection.output_buffer_allocation_bytes();
    let (result, released_allocation_bytes) = flush_queue_vectored(&mut writer, &mut queue, None);
    let queued_after = queue.iter().map(Vec::len).sum::<usize>();
    connection
        .consume_written_bytes(queued_before - queued_after, queued_before)
        .unwrap();
    connection.release_output_frame_allocations(released_allocation_bytes);
    connection.output.queue = queue;

    assert!(matches!(result, WriteOutcome::Backpressured));
    assert_eq!(writer.calls, 1);
    assert_eq!(released_allocation_bytes, 0);
    assert_eq!(
        connection.output_buffer_allocation_bytes(),
        allocation_before
    );
    assert_eq!(connection.output.queue.front().unwrap(), b"ion-frame");
    assert_eq!(connection.output.replication_bytes, b"ion-frame".len());
}

#[test]
fn would_block_waits_for_real_writable_readiness() {
    let (mut backend, target, token) = connected_ordinary_client();
    SockRef::from(&target)
        .set_recv_buffer_size(4096)
        .expect("shrink ordinary peer receive buffer");
    fill_transport_send_buffer(&mut backend, token);
    backend.transport.clear_interest_requests_for_test();
    backend.writes.ordinary.begin_turn();
    backend.connection_registry[token].queue_reply(vec![b'a'; 1024 * 1024]);
    assert!(backend.schedule_ordinary_write(token.0));

    backend.drain_current_ordinary_writes();

    assert_eq!(
        backend.transport.last_set_interest_calls_for_test().last(),
        Some(&(token, Interest::ReadWritable)),
        "WouldBlock must keep writable interest armed until a real writable event arrives"
    );
    assert!(
        backend.connection_registry[token].has_pending_output(),
        "the reply must stay queued while the peer is not reading"
    );
    assert_eq!(
        backend.next_poll_timeout(1_000),
        None,
        "next-turn write backpressure alone must not spin the poll loop"
    );
}

#[test]
fn backpressured_streaming_replica_waits_for_real_writable_readiness() {
    let (mut backend, mut replica_stream, token_value, replica_id) = connected_primary_link();
    let token = Token(token_value);
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("registered replica link")
        .set_state(PrimaryLinkState::Streaming);
    SockRef::from(&replica_stream)
        .set_recv_buffer_size(4096)
        .expect("shrink replica peer receive buffer");
    fill_transport_send_buffer(&mut backend, token);
    backend.transport.clear_interest_requests_for_test();
    let frame = b"replica-frame";
    backend.ctx.replication_mut().backlog.append(frame).unwrap();
    let queued = {
        let backlog = &backend.ctx.replication().backlog;
        backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .expect("registered replica link")
            .pump(backlog)
            .expect("active backlog frame must be readable")
    };
    assert_eq!(queued, frame);
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("registered replica link")
        .mark_replication_queued(queued.len());
    backend.connection_registry[token].queue_replication_reply(queued);

    backend.schedule_pending_replica_writes_before_sleep();
    backend.drive_pending_slot_export_writes();

    assert!(backend.connection_registry[token].has_pending_output());
    assert_eq!(
        backend.transport.last_set_interest_calls_for_test().last(),
        Some(&(token, Interest::ReadWritable))
    );
    assert!(backend.writes.replica_waiting_for_writable(token));
    assert!(!backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(token)
    ));
    backend.transport.clear_interest_requests_for_test();

    backend.schedule_pending_replica_writes_before_sleep();
    assert!(!backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(token)
    ));
    backend.finish_turn_pending_outputs();
    assert!(backend
        .transport
        .last_set_interest_calls_for_test()
        .is_empty());

    replica_stream
        .set_nonblocking(true)
        .expect("set replica peer nonblocking");
    SockRef::from(&replica_stream)
        .set_recv_buffer_size(1024 * 1024)
        .expect("expand replica peer receive buffer");
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut drain = [0_u8; 64 * 1024];
    let ready = loop {
        loop {
            match replica_stream.read(&mut drain) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("failed to drain replica peer: {error}"),
            }
        }
        if let Some(ready) = backend
            .transport
            .poll_events(Some(Duration::from_millis(10)))
            .expect("poll real replica writable readiness")
            .into_iter()
            .find(|event| event.token == token && event.writable)
        {
            break ready;
        }
        assert!(
            Instant::now() < deadline,
            "replica socket did not become writable after peer drain"
        );
    };
    backend.dispatch_transport_event(
        crate::runtime::server::worker::reactor::ReadyEvent {
            token: ready.token,
            readable: ready.readable,
            writable: ready.writable,
            error: ready.error,
            read_hup: ready.read_hup,
        },
        false,
    );

    assert!(!backend.connection_registry[token].has_pending_output());
    assert!(!backend.writes.replica_waiting_for_writable(token));
}

#[test]
fn downstream_replica_budget_yield_state_transition_enqueues_next_turn_runnable() {
    let (mut backend, _replica_stream, token_value, replica_id) = connected_primary_link();
    let token = Token(token_value);
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("registered replica link")
        .set_state(PrimaryLinkState::Streaming);
    backend.connection_registry[token].queue_replication_reply(b"replica-frame".to_vec());
    let context = WritableContext {
        token,
        target: WritableTarget::DownstreamReplica,
    };

    backend.finish_partial_writable(&context, &WriteOutcome::BudgetExhausted);

    assert!(backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(token)
    ));
    backend.event_batch.begin_next_turn();
    let event = backend
        .event_batch
        .pop_before_poll_slot(
            crate::runtime::server::worker::reactor::BeforePollSlot::DistributedWrites,
        )
        .expect("next-turn downstream replica write");
    assert_eq!(
        event.owner,
        crate::runtime::server::worker::reactor::EventOwner::Runnable(
            crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(token)
        )
    );
}

#[test]
fn replica_writable_wait_clears_on_connection_removal() {
    let (mut backend, _replica_stream, token_value, _) = connected_primary_link();
    let token = Token(token_value);
    backend.writes.mark_replica_waiting_for_writable(token);

    backend.remove_connection(token_value);

    assert!(!backend.writes.replica_waiting_for_writable(token));
}

#[test]
fn replica_writable_wait_clears_when_link_leaves_streaming() {
    let (mut backend, _replica_stream, token_value, replica_id) = connected_primary_link();
    let token = Token(token_value);
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("registered replica link")
        .set_state(PrimaryLinkState::Streaming);
    backend.writes.mark_replica_waiting_for_writable(token);
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("registered replica link")
        .set_state(PrimaryLinkState::Closing);

    backend.schedule_pending_replica_writes_before_sleep();

    assert!(!backend.writes.replica_waiting_for_writable(token));
    assert!(!backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(token)
    ));
}

#[test]
fn c_shaped_export_partial_then_eagain_closes_on_second_limit_check() {
    let (mut backend, _target, token_value) = connected_streaming_slot_export(0);
    let connection_token = token_value;
    let id = backend.connection_registry[Token(connection_token)]
        .slot_migration_role
        .as_ref()
        .expect("connected export has migration role")
        .job_id()
        .clone();
    backend.ctx.config_mut().server.repl_backlog_size = 0;
    backend.event_batch.begin_next_turn();
    backend.writes.begin_turn();
    let mut first = Vec::with_capacity(4096);
    first.extend_from_slice(&[b'a'; 257]);

    assert!(backend.queue_slot_migration_output(&id, token_value, first));
    let owned_before = backend.slot_export_output_memory_usage(&id, token_value);
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = owned_before + 1;
    assert!(!backend.enforce_client_output_buffer_limit_for(token_value));
    assert!(backend
        .event_batch
        .contains_runnable(slot_export_key(connection_token)));
    backend.cancel_runtime_runnable(slot_export_key(token_value));

    let logical_before = backend.slot_export_reply_bytes(&id, token_value);
    let allocator_before = backend.writes.slot_export_allocation_bytes;
    let mut writer = OneWriteThenBlock {
        accepted: 17,
        calls: 0,
    };
    let mut queue = std::mem::take(
        &mut backend.connection_registry[Token(connection_token)]
            .output
            .queue,
    );
    let queued_before = queue.iter().map(Vec::len).sum::<usize>();
    let (partial, released_on_partial) = flush_queue_vectored(&mut writer, &mut queue, None);
    let queued_after = queue.iter().map(Vec::len).sum::<usize>();
    let written = queued_before - queued_after;
    backend.connection_registry[Token(connection_token)]
        .consume_written_bytes(written, queued_before)
        .unwrap();
    backend.connection_registry[Token(connection_token)]
        .release_output_frame_allocations(released_on_partial);
    backend.connection_registry[Token(connection_token)]
        .output
        .queue = queue;
    backend.reconcile_slot_export_output_allocation(token_value);
    backend.schedule_slot_export_write_continuation(token_value, &partial);

    assert!(matches!(partial, WriteOutcome::Backpressured));
    assert!(
        !backend
            .event_batch
            .contains_runnable(slot_export_key(token_value)),
        "a short write below the configured budget must wait for writable readiness"
    );
    assert_eq!(writer.calls, 1);
    assert_eq!(written, 17);
    assert_eq!(released_on_partial, 0);
    assert_eq!(
        backend.slot_export_reply_bytes(&id, token_value),
        logical_before - written
    );
    assert_eq!(
        backend.slot_export_output_memory_usage(&id, token_value),
        owned_before,
        "a partial frame retains its complete client-output allocation"
    );
    assert_eq!(
        backend.writes.slot_export_allocation_bytes,
        allocator_before
    );

    let logical_after_partial = backend.slot_export_reply_bytes(&id, token_value);
    let mut queue = std::mem::take(
        &mut backend.connection_registry[Token(connection_token)]
            .output
            .queue,
    );
    let queued_before_eagain = queue.iter().map(Vec::len).sum::<usize>();
    let (blocked, released_on_eagain) = flush_queue_vectored(&mut writer, &mut queue, None);
    let queued_after_eagain = queue.iter().map(Vec::len).sum::<usize>();
    backend.connection_registry[Token(connection_token)]
        .consume_written_bytes(
            queued_before_eagain - queued_after_eagain,
            queued_before_eagain,
        )
        .unwrap();
    backend.connection_registry[Token(connection_token)]
        .release_output_frame_allocations(released_on_eagain);
    backend.connection_registry[Token(connection_token)]
        .output
        .queue = queue;
    backend.reconcile_slot_export_output_allocation(token_value);
    backend.schedule_slot_export_write_continuation(token_value, &blocked);

    assert!(matches!(blocked, WriteOutcome::Backpressured));
    assert!(
        !backend
            .event_batch
            .contains_runnable(slot_export_key(token_value)),
        "WouldBlock must not schedule a fairness continuation"
    );
    assert_eq!(writer.calls, 2);
    assert_eq!(queued_after_eagain, queued_before_eagain);
    assert_eq!(released_on_eagain, 0);
    assert_eq!(
        backend.slot_export_reply_bytes(&id, token_value),
        logical_after_partial
    );
    assert_eq!(
        backend.slot_export_output_memory_usage(&id, token_value),
        owned_before
    );
    assert_eq!(
        backend.writes.slot_export_allocation_bytes,
        allocator_before
    );

    backend.connection_registry[Token(connection_token)]
        .queue_replication_reply(b"cross-limit".to_vec());
    backend.reconcile_slot_export_output_allocation(token_value);
    assert!(backend.slot_export_output_memory_usage(&id, token_value) > owned_before + 1);
    assert!(backend.enforce_client_output_buffer_limit_for(token_value));

    assert!(!backend.connection_registry.contains(Token(token_value)));
    assert!(!backend
        .event_batch
        .contains_runnable(slot_export_key(connection_token)));
    assert_eq!(
        backend
            .ctx
            .cluster_state()
            .as_deref()
            .expect("cluster state")
            .migrations
            .get(&id)
            .expect("migration job")
            .phase,
        MigrationPhase::Terminal(TerminalPhase::Failed)
    );
}

#[test]
fn slot_export_continuation_waits_for_next_turn() {
    let mut runtime = ServerWorkerState::new(1).expect("create runtime");
    let key = slot_export_key(7);
    runtime.schedule_runtime_runnable_next_turn(key);
    runtime.schedule_runtime_runnable_next_turn(key);
    assert!(runtime.event_batch.contains_runnable(key));
    assert!(runtime
        .event_batch
        .pop_before_poll_slot(
            crate::runtime::server::worker::reactor::BeforePollSlot::DistributedWrites
        )
        .is_none());

    runtime.event_batch.begin_next_turn();
    let event = runtime
        .event_batch
        .pop_before_poll_slot(
            crate::runtime::server::worker::reactor::BeforePollSlot::DistributedWrites,
        )
        .expect("next-turn slot-export runnable");
    assert_eq!(
        event.owner,
        crate::runtime::server::worker::reactor::EventOwner::Runnable(key)
    );
    runtime.schedule_runtime_runnable_next_turn(key);
    runtime.cancel_runtime_runnable(key);
    runtime.event_batch.begin_next_turn();
    assert!(!runtime.event_batch.contains_runnable(key));
}

#[test]
fn first_slot_export_enqueue_registers_pending_write_once() {
    let (mut backend, _target, token_value) = connected_streaming_slot_export(0);
    let connection_token = token_value;
    let id = backend.connection_registry[Token(connection_token)]
        .slot_migration_role
        .as_ref()
        .expect("connected export has migration role")
        .job_id()
        .clone();
    backend.writes.slot_exports.begin_turn();

    assert!(backend.queue_slot_migration_output(&id, token_value, b"first".to_vec()));
    assert!(backend.queue_slot_migration_output(&id, token_value, b"second".to_vec()));

    assert!(backend
        .event_batch
        .contains_runnable(slot_export_key(token_value)));
}

#[test]
fn slot_export_enqueue_after_drained_pending_phase_waits_for_next_turn() {
    let (mut backend, _target, token_value) = connected_streaming_slot_export(0);
    let connection_token = token_value;
    let id = backend.connection_registry[Token(connection_token)]
        .slot_migration_role
        .as_ref()
        .expect("connected export has migration role")
        .job_id()
        .clone();
    backend.writes.slot_exports.begin_turn();
    assert!(backend.queue_slot_migration_output(&id, token_value, b"first".to_vec()));
    backend.drive_pending_slot_export_writes();
    assert_eq!(
        backend.connection_registry[Token(connection_token)]
            .output
            .replication_bytes,
        0
    );

    assert!(backend.queue_slot_migration_output(&id, token_value, b"second".to_vec()));
    assert!(backend.queue_slot_migration_output(&id, token_value, b"third".to_vec()));
    assert!(backend
        .event_batch
        .contains_runnable(slot_export_key(token_value)));
    backend.drive_pending_slot_export_writes();
    assert_eq!(
        backend.connection_registry[Token(connection_token)]
            .output
            .replication_bytes,
        b"second".len() + b"third".len()
    );

    backend.event_batch.begin_next_turn();
    backend.writes.begin_turn();
    backend.drive_pending_slot_export_writes();
    assert_eq!(
        backend.connection_registry[Token(connection_token)]
            .output
            .replication_bytes,
        0
    );
    backend.event_batch.begin_next_turn();
    backend.writes.begin_turn();
    assert!(!backend
        .event_batch
        .contains_runnable(slot_export_key(token_value)));
}

#[test]
fn later_enqueue_does_not_duplicate_a_serviced_export_into_current() {
    let (mut backend, _target, token_value) = connected_streaming_slot_export(0);
    let connection_token = token_value;
    let id = backend.connection_registry[Token(connection_token)]
        .slot_migration_role
        .as_ref()
        .expect("connected export has migration role")
        .job_id()
        .clone();
    backend.writes.slot_exports.begin_turn();
    assert!(backend.queue_slot_migration_output(
        &id,
        token_value,
        vec![b'a'; NET_MAX_WRITES_PER_EVENT + 5],
    ));

    backend.dispatch_transport_event(
        crate::runtime::server::worker::reactor::ReadyEvent {
            token: Token(token_value),
            readable: false,
            writable: true,
            error: false,
            read_hup: false,
        },
        false,
    );

    assert_eq!(
        backend
            .writes
            .slot_exports
            .remaining_budget(Token(token_value), NET_MAX_WRITES_PER_EVENT),
        0
    );
    assert_eq!(
        backend.connection_registry[Token(connection_token)]
            .output
            .replication_bytes,
        5
    );
    assert!(backend.queue_slot_migration_output(&id, token_value, b"later".to_vec()));
    assert!(backend
        .event_batch
        .contains_runnable(slot_export_key(token_value)));

    backend.drive_pending_slot_export_writes();
    assert!(backend.connection_registry.contains(Token(token_value)));
    assert_eq!(
        backend.connection_registry[Token(connection_token)]
            .output
            .replication_bytes,
        10
    );

    backend.event_batch.begin_next_turn();
    backend.writes.begin_turn();
    backend.drive_pending_slot_export_writes();
    assert!(backend.connection_registry.contains(Token(token_value)));
    assert_eq!(
        backend.connection_registry[Token(connection_token)]
            .output
            .replication_bytes,
        0
    );
}

#[test]
fn zero_budget_queue_flush_is_budget_exhausted_without_writing() {
    let mut queue = std::collections::VecDeque::from([b"pending".to_vec()]);
    let mut writer = Vec::new();

    let (result, released_allocation_bytes) =
        flush_queue_vectored(&mut writer, &mut queue, Some(0));

    assert!(matches!(result, WriteOutcome::BudgetExhausted));
    assert_eq!(released_allocation_bytes, 0);
    assert!(writer.is_empty());
    assert_eq!(
        queue,
        std::collections::VecDeque::from([b"pending".to_vec()])
    );
}

#[test]
fn short_write_before_configured_budget_is_backpressured() {
    let mut queue = std::collections::VecDeque::from([b"long-frame".to_vec()]);
    let mut writer = OneWriteThenBlock {
        accepted: 4,
        calls: 0,
    };

    let (result, _) = flush_queue_vectored(&mut writer, &mut queue, Some(8));

    assert!(matches!(result, WriteOutcome::Backpressured));
    assert_eq!(writer.calls, 1);
    assert_eq!(queue.front().map(Vec::as_slice), Some(b"-frame".as_slice()));
}

#[test]
fn progress_then_would_block_is_backpressured_before_budget() {
    let mut queue = std::collections::VecDeque::from(vec![vec![b'x']; MAX_IOV_PER_WRITE + 1]);
    let mut writer = ProgressThenBlock { calls: 0 };

    let (result, _) = flush_queue_vectored(&mut writer, &mut queue, Some(MAX_IOV_PER_WRITE + 1));

    assert!(matches!(result, WriteOutcome::Backpressured));
    assert_eq!(writer.calls, 2);
    assert_eq!(queue, std::collections::VecDeque::from([vec![b'x']]));
}

#[test]
fn interrupted_vectored_write_retries_and_completes() {
    let mut queue = std::collections::VecDeque::from([b"retry".to_vec()]);
    let mut writer = InterruptedThenWriter {
        calls: 0,
        followup: InterruptedFollowup::Success,
        written: Vec::new(),
    };

    let (result, released_allocation_bytes) = flush_queue_vectored(&mut writer, &mut queue, None);

    assert!(matches!(result, WriteOutcome::Complete));
    assert_eq!(writer.calls, 2);
    assert_eq!(writer.written, b"retry");
    assert!(queue.is_empty());
    assert!(released_allocation_bytes > 0);
}

#[test]
fn interrupted_vectored_write_retries_then_reports_backpressure() {
    let mut queue = std::collections::VecDeque::from([b"pending".to_vec()]);
    let mut writer = InterruptedThenWriter {
        calls: 0,
        followup: InterruptedFollowup::WouldBlock,
        written: Vec::new(),
    };

    let (result, released_allocation_bytes) = flush_queue_vectored(&mut writer, &mut queue, None);

    assert!(matches!(result, WriteOutcome::Backpressured));
    assert_eq!(writer.calls, 2);
    assert_eq!(released_allocation_bytes, 0);
    assert_eq!(
        queue.front().map(Vec::as_slice),
        Some(b"pending".as_slice())
    );
}

#[test]
fn drained_queue_flush_reports_complete_exactly() {
    let mut queue = std::collections::VecDeque::from([b"complete".to_vec()]);
    let mut writer = Vec::new();

    let (result, released_allocation_bytes) = flush_queue_vectored(&mut writer, &mut queue, None);

    assert!(matches!(result, WriteOutcome::Complete));
    assert_eq!(writer, b"complete");
    assert!(queue.is_empty());
    assert!(released_allocation_bytes > 0);
}

#[test]
fn write_zero_is_preserved_as_failed_outcome() {
    let mut queue = std::collections::VecDeque::from([b"pending".to_vec()]);

    let (result, released_allocation_bytes) =
        flush_queue_vectored(&mut WriteZero, &mut queue, None);

    assert!(matches!(
        result,
        WriteOutcome::Failed(error) if error.kind() == std::io::ErrorKind::WriteZero
    ));
    assert_eq!(released_allocation_bytes, 0);
    assert_eq!(
        queue.front().map(Vec::as_slice),
        Some(b"pending".as_slice())
    );
}

#[test]
fn hard_write_error_is_preserved_as_failed_outcome() {
    let mut queue = std::collections::VecDeque::from([b"pending".to_vec()]);
    let mut writer = FailingWriter(std::io::ErrorKind::BrokenPipe);

    let (result, released_allocation_bytes) = flush_queue_vectored(&mut writer, &mut queue, None);

    assert!(matches!(
        result,
        WriteOutcome::Failed(error) if error.kind() == std::io::ErrorKind::BrokenPipe
    ));
    assert_eq!(released_allocation_bytes, 0);
    assert_eq!(
        queue.front().map(Vec::as_slice),
        Some(b"pending".as_slice())
    );
}

#[test]
fn writable_failure_contract_preserves_token_owner_and_error() {
    let context = WritableContext {
        token: Token(42),
        target: WritableTarget::ReplicaRdb,
    };
    let error = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "replica transport closed");

    let message = writable_failure_log_message(&context, &error);

    assert_eq!(
        message,
        "write failed: token=42 owner=replica-rdb operation=write error=replica transport closed"
    );
    assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
}

#[test]
fn failed_origin_flush_tears_down_connection_for_traced_and_untraced_replies() {
    for trace_rdb_ok in [false, true] {
        let (mut backend, _target, token) = connected_ordinary_client();
        backend.connection_registry[token].queue_reply(b"reply".to_vec());
        backend
            .transport
            .remove_registered(token)
            .expect("remove transport before failed flush");

        backend.flush_origin_reply(token.0, token.0, trace_rdb_ok);

        assert!(
            !backend.connection_registry.contains(token),
            "Failed must tear down the connection before any writable rearm"
        );
    }
}

#[test]
fn scheduled_slot_export_write_enforces_limit_before_flushing() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = 10;
    backend.ctx.config_mut().server.repl_backlog_size = 32;
    let token = Token(97);
    let id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .expect("valid migration id");
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(token);
    connection.slot_migration_role = Some(SlotMigrationConnectionRole::Export(id.clone()));
    connection.queue_replication_reply(vec![b'm'; 33]);
    let _connection_token = backend.connection_registry.insert(connection);
    backend.slot_migration_runtime.register_export(
        crate::framework::distributed::migration::SlotExportLink::new(id, token.0),
    );
    backend.schedule_runtime_runnable_next_turn(slot_export_key(token.0));
    backend.event_batch.begin_next_turn();
    backend.writes.begin_turn();

    backend.drive_pending_slot_export_writes();

    assert!(!backend.connection_registry.contains(Token(token.0)));
    assert_eq!(
        backend
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        1
    );
    assert!(!backend
        .event_batch
        .contains_runnable(slot_export_key(token.0)));
}

#[test]
fn combined_readable_writable_ack_consumes_one_slot_export_budget() {
    let (mut backend, mut target, token_value) =
        connected_streaming_slot_export(3 * NET_MAX_WRITES_PER_EVENT);
    target
        .write_all(
            &crate::engine::distributed::replication::protocol::encode_command(&[
                b"CLUSTER",
                b"SYNCSLOTS",
                b"ACK",
            ]),
        )
        .expect("send slot migration ACK");

    backend.dispatch_transport_event(
        crate::runtime::server::worker::reactor::ReadyEvent {
            token: Token(token_value),
            readable: true,
            writable: true,
            error: false,
            read_hup: false,
        },
        true,
    );

    let connection_token = token_value;
    assert_eq!(
        backend.connection_registry[Token(connection_token)]
            .output
            .replication_bytes,
        2 * NET_MAX_WRITES_PER_EVENT
    );
    assert_eq!(
        backend.writes.slot_export_allocation_bytes,
        backend.connection_registry[Token(connection_token)].output_buffer_allocation_bytes(),
        "periodic ACK control output must reconcile allocator ownership"
    );
}

#[test]
fn periodic_ack_after_continuation_shares_slot_export_turn_budget() {
    let (mut backend, _target, token_value) =
        connected_streaming_slot_export(3 * NET_MAX_WRITES_PER_EVENT);
    backend.schedule_runtime_runnable_next_turn(slot_export_key(token_value));
    backend.event_batch.begin_next_turn();
    backend.writes.begin_turn();

    backend.drive_pending_slot_export_writes();
    backend.send_slot_migration_periodic_acks();

    let connection_token = token_value;
    assert_eq!(
        backend.connection_registry[Token(connection_token)]
            .output
            .replication_bytes,
        2 * NET_MAX_WRITES_PER_EVENT
    );
}

#[test]
fn periodic_export_ack_waits_for_pending_write_phase() {
    let (mut backend, mut target, token_value) = connected_streaming_slot_export(0);
    backend.event_batch.begin_next_turn();
    backend.writes.begin_turn();

    backend.send_slot_migration_periodic_acks();

    let connection_token = token_value;
    let pending_bytes = backend.connection_registry[Token(connection_token)].pending_reply_bytes();
    assert!(pending_bytes > 0, "periodic ACK must remain queued");
    target
        .set_nonblocking(true)
        .expect("set export target nonblocking");
    let mut byte = [0_u8; 1];
    let error = target
        .read(&mut byte)
        .expect_err("periodic ACK must not bypass the pending phase");
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);

    backend.drive_pending_slot_export_writes();

    target
        .set_nonblocking(false)
        .expect("restore blocking export target");
    target
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set export target read timeout");
    let mut received = vec![0_u8; pending_bytes];
    target
        .read_exact(&mut received)
        .expect("pending phase must write the periodic ACK");
    assert!(received.windows(3).any(|bytes| bytes == b"ACK"));
    assert_eq!(
        backend.connection_registry[Token(connection_token)].pending_reply_bytes(),
        0
    );
}

#[test]
fn slot_export_write_immediately_refreshes_remaining_repl_size() {
    let (mut backend, _target, token_value) = connected_streaming_slot_export(41);
    let connection_token = token_value;
    let id = backend.connection_registry[Token(connection_token)]
        .slot_migration_role
        .as_ref()
        .expect("connected export has migration role")
        .job_id()
        .clone();

    assert_eq!(
        backend
            .ctx
            .cluster_state()
            .as_deref()
            .expect("cluster state")
            .migrations
            .get(&id)
            .expect("migration job")
            .remaining_repl_size,
        41,
        "C exposes the live client reply bytes as soon as output is queued"
    );

    backend.handle_writable(Token(token_value));

    assert_eq!(
        backend.connection_registry[Token(connection_token)].pending_reply_bytes(),
        0
    );
    assert_eq!(
        backend
            .ctx
            .cluster_state()
            .as_deref()
            .expect("cluster state")
            .migrations
            .get(&id)
            .expect("migration job")
            .remaining_repl_size,
        0,
        "a completed write must be visible without waiting for the next cron tick"
    );
}

#[test]
fn bounded_queue_flush_stops_after_budget_on_always_writable_stream() {
    let mut queue =
        std::collections::VecDeque::from([vec![b'a'; 48 * 1024], vec![b'b'; 48 * 1024]]);
    let mut writer = Vec::new();

    let (result, _) = flush_queue_vectored(&mut writer, &mut queue, Some(64 * 1024));

    assert!(matches!(result, WriteOutcome::BudgetExhausted));
    assert_eq!(writer.len(), 64 * 1024);
    assert_eq!(queue.iter().map(Vec::len).sum::<usize>(), 32 * 1024);
}

#[test]
fn bounded_slot_export_write_turn_leaves_followup_bytes_queued() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind export peer");
    let peer_addr = listener.local_addr().expect("export peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect export transport");
    let (_export_stream, _) = listener.accept().expect("accept export transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    let connection_token = peer.token.0;
    let id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .expect("valid migration id");
    backend.connection_registry[Token(connection_token)].slot_migration_role =
        Some(SlotMigrationConnectionRole::Export(id.clone()));
    backend.slot_migration_runtime.register_export(
        crate::framework::distributed::migration::SlotExportLink::new(id, peer.token.0),
    );
    let first = vec![b'a'; 64 * 1024];
    let first_allocation = crate::foundation::memory::util_alloc::vec_allocation_bytes(&first);
    backend.connection_registry[Token(connection_token)].queue_replication_reply(first);
    backend.connection_registry[Token(connection_token)]
        .queue_replication_reply(vec![b'b'; 64 * 1024]);
    backend.track_slot_export_connection_output(connection_token);
    let allocation_before = backend.writes.slot_export_allocation_bytes;

    backend.handle_writable(peer.token);

    assert_eq!(
        backend.connection_registry[Token(connection_token)]
            .output
            .queue
            .iter()
            .map(Vec::len)
            .sum::<usize>(),
        64 * 1024
    );
    assert_eq!(
        backend.connection_registry[Token(connection_token)]
            .output
            .replication_bytes,
        64 * 1024,
        "the unaccepted mutation suffix remains owned by the connection"
    );
    assert_eq!(
        backend.writes.slot_export_allocation_bytes,
        allocation_before - first_allocation
    );
    backend.event_batch.begin_next_turn();
    backend.writes.begin_turn();

    backend.drive_pending_slot_export_writes();

    assert!(backend.connection_registry[Token(connection_token)]
        .output
        .queue
        .is_empty());
    assert!(!backend
        .event_batch
        .contains_runnable(slot_export_key(connection_token)));
    assert_eq!(
        backend.writes.slot_export_allocation_bytes,
        backend.connection_registry[Token(connection_token)].output_buffer_allocation_bytes()
    );
}

#[test]
fn ordinary_write_updates_incremental_client_memory_at_lifecycle_boundaries() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    backend.ctx.config_mut().server.maxmemory_clients = "1gb".to_string();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ordinary peer");
    let peer_addr = listener.local_addr().expect("ordinary peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect ordinary transport");
    let (_ordinary_stream, _) = listener.accept().expect("accept ordinary transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    let connection_token = peer.token.0;
    let initial = backend.connection_registry[Token(connection_token)]
        .client_memory_usage()
        .total;
    assert_eq!(backend.ctx.normal_client_memory(), initial);

    assert!(backend.queue_client_reply(peer.token, vec![b'x'; 32 * 1024]));
    let queued = backend.connection_registry[Token(connection_token)]
        .client_memory_usage()
        .total;
    assert!(queued > initial);
    assert_eq!(backend.ctx.normal_client_memory(), queued);

    backend.handle_writable(peer.token);

    assert!(backend.connection_registry[Token(connection_token)]
        .output
        .queue
        .is_empty());
    let drained = backend.connection_registry[Token(connection_token)]
        .client_memory_usage()
        .total;
    assert!(drained < queued);
    assert_eq!(backend.ctx.normal_client_memory(), drained);

    backend.remove_connection(peer.token.0);
    assert_eq!(backend.ctx.normal_client_memory(), 0);
}

#[test]
fn ordinary_large_write_turn_stops_after_first_frame_crossing_c_budget() {
    let frame_len = 512 * 1024;
    let mut queue =
        std::collections::VecDeque::from([vec![b'a'; frame_len], vec![b'b'; frame_len]]);
    let budget = c_client_write_budget(&queue);
    let mut writer = Vec::new();

    let (result, _) = flush_queue_vectored(&mut writer, &mut queue, Some(budget));

    assert!(matches!(result, WriteOutcome::BudgetExhausted));
    assert_eq!(writer.len(), frame_len);
    assert_eq!(
        queue,
        std::collections::VecDeque::from([vec![b'b'; frame_len]])
    );
}

fn _temp_dir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "valkey-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ))
}

pub(super) fn connected_primary_link() -> (ServerWorkerState, TcpStream, usize, ReplicaId) {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    backend.ctx.replication_mut().backlog.activate();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind replica peer");
    let peer_addr = listener.local_addr().expect("replica peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect replica transport");
    let (replica_stream, _) = listener.accept().expect("accept replica transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr: SocketAddr::from(([127, 0, 0, 1], 6380)),
    });
    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(peer.token.0),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: Vec::new(),
            rdb_filter_mode: RdbFilterMode::All,
        },
    );
    (backend, replica_stream, peer.token.0, replica_id)
}

#[test]
fn mixed_socket_write_advances_only_replication_tagged_cursor_bytes() {
    let (mut backend, mut replica_stream, token_value, replica_id) = connected_primary_link();
    let token = Token(token_value);
    replica_stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .set_state(PrimaryLinkState::Streaming);
    backend.connection_registry[token].queue_reply(b"rdb".to_vec());
    backend.connection_registry[token].queue_reply(b"command".to_vec());

    backend.propagate_to_replicas(b"tail");
    backend.handle_writable(token);

    let mut received = vec![0; b"rdbcommandtail".len()];
    replica_stream.read_exact(&mut received).unwrap();
    assert_eq!(received, b"rdbcommandtail");
    let link = backend
        .replication_runtime
        .primary_link(replica_id)
        .unwrap();
    assert_eq!(link.sent_cursor().get(), 4);
    assert_eq!(link.sent_cursor(), link.next_send_cursor());
}

#[test]
fn synchronous_cursor_accounting_error_closes_only_the_replica() {
    let (mut backend, mut replica_stream, token_value, replica_id) = connected_primary_link();
    let token = Token(token_value);
    replica_stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    {
        let link = backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap();
        link.set_state(PrimaryLinkState::Streaming);
        link.mark_replication_queued(1);
    }
    backend.connection_registry[token].queue_replication_reply(b"xx".to_vec());

    backend.handle_writable(token);

    let mut received = [0; 2];
    replica_stream.read_exact(&mut received).unwrap();
    assert_eq!(&received, b"xx");
    assert!(!backend.connection_registry.contains(token));
    assert!(backend
        .replication_runtime
        .primary_link(replica_id)
        .is_none());
}

#[test]
fn synchronous_live_backlog_regression_closes_the_replica() {
    let (mut backend, mut replica_stream, token_value, replica_id) = connected_primary_link();
    let token = Token(token_value);
    replica_stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let mut future_backlog =
        crate::engine::distributed::replication::backlog::ReplicationBacklog::new(
            "future".to_string(),
            64,
        );
    future_backlog
        .restore_primary_from_rdb("future", offset(41))
        .unwrap();
    let boundary = future_backlog.snapshot_boundary();
    future_backlog.append(b"ab").unwrap();
    let queued = {
        let link = backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap();
        link.set_full_resync_boundary(boundary).unwrap();
        link.set_state(PrimaryLinkState::Streaming);
        link.pump(&future_backlog).unwrap()
    };
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .mark_replication_queued(queued.len());
    backend.connection_registry[token].queue_replication_reply(queued.clone());

    backend.handle_writable(token);

    let mut received = vec![0; queued.len()];
    replica_stream.read_exact(&mut received).unwrap();
    assert_eq!(received, queued);
    assert!(!backend.connection_registry.contains(token));
    assert!(backend
        .replication_runtime
        .primary_link(replica_id)
        .is_none());
}

fn assert_replica_socket_has_no_bytes(stream: &mut TcpStream) {
    stream
        .set_nonblocking(true)
        .expect("set replica stream nonblocking");
    let mut byte = [0_u8; 1];
    let error = stream
        .read(&mut byte)
        .expect_err("before-sleep flush must leave this replica socket untouched");
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
}

#[test]
fn before_sleep_flush_sends_pending_replication_without_a_writable_event() {
    let (mut backend, mut replica_stream, token_value, replica_id) = connected_primary_link();
    replica_stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set replica read timeout");
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("registered replica link")
        .set_state(PrimaryLinkState::Streaming);
    let frame = b"*1\r\n$4\r\nPING\r\n";
    backend.propagate_to_replicas(frame);

    backend.schedule_pending_replica_writes_before_sleep();
    assert!(backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(Token(token_value))
    ));
    backend.drive_pending_slot_export_writes();

    let mut received = vec![0; frame.len()];
    replica_stream
        .read_exact(&mut received)
        .expect("before-sleep flush must write the replication frame");
    assert_eq!(received, frame);
}

#[test]
fn replica_write_flush_refreshes_replication_buffer_memory() {
    let (mut backend, mut replica_stream, token_value, replica_id) = connected_primary_link();
    backend.ctx.config_mut().server.repl_backlog_size = 4;
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("registered replica link")
        .set_state(PrimaryLinkState::Streaming);
    let frame = b"*1\r\n$4\r\nPING\r\n";

    backend.propagate_to_replicas(frame);

    assert_eq!(
        backend.ctx.replication().replica_output_buffer_bytes,
        frame.len()
    );
    backend.handle_writable(Token(token_value));
    let mut received = vec![0; frame.len()];
    replica_stream
        .read_exact(&mut received)
        .expect("replica must receive the queued frame");
    assert_eq!(received, frame);
    let connection_token = token_value;
    assert_eq!(
        backend.connection_registry[Token(connection_token)]
            .output
            .replication_bytes,
        0
    );
    assert!(backend.replication_buffer_accounting_dirty);
    assert_eq!(
        backend.ctx.replication().replica_output_buffer_bytes,
        frame.len(),
        "per-write handling must defer the O(replicas) refresh"
    );

    backend.schedule_pending_replica_writes_before_sleep();

    assert!(!backend.replication_buffer_accounting_dirty);
    assert_eq!(
        backend.ctx.replication().replica_output_buffer_bytes,
        4,
        "INFO memory cache must fall back to the retained backlog after the replica queue drains"
    );
}

#[test]
fn before_sleep_flush_skips_protected_streaming_replica() {
    let (mut backend, mut replica_stream, token_value, replica_id) = connected_primary_link();
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("registered replica link")
        .set_state(PrimaryLinkState::Streaming);
    let frame = b"*1\r\n$4\r\nPING\r\n";
    backend.propagate_to_replicas(frame);
    let connection_token = token_value;
    backend.connection_registry[Token(connection_token)].protected = true;

    backend.schedule_pending_replica_writes_before_sleep();

    assert!(backend.connection_registry[Token(connection_token)].has_pending_output());
    assert_replica_socket_has_no_bytes(&mut replica_stream);
}

#[test]
fn before_sleep_flush_skips_replica_during_full_sync_handshake() {
    let (mut backend, mut replica_stream, token_value, replica_id) = connected_primary_link();
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("registered replica link")
        .set_state(PrimaryLinkState::PreparingFullResync);
    let frame = b"+FULLRESYNC pending\r\n";
    let connection_token = token_value;
    backend.connection_registry[Token(connection_token)].queue_replication_reply(frame.to_vec());

    backend.schedule_pending_replica_writes_before_sleep();

    assert!(backend.connection_registry[Token(connection_token)].has_pending_output());
    assert_replica_socket_has_no_bytes(&mut replica_stream);
}

#[test]
fn before_sleep_flush_skips_replica_marked_close_after_reply() {
    let (mut backend, mut replica_stream, token_value, replica_id) = connected_primary_link();
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("registered replica link")
        .set_state(PrimaryLinkState::Streaming);
    let frame = b"*1\r\n$4\r\nPING\r\n";
    backend.propagate_to_replicas(frame);
    let connection_token = token_value;
    backend.connection_registry[Token(connection_token)].state = ConnectionState::CloseAfterReply;

    backend.schedule_pending_replica_writes_before_sleep();

    assert!(backend.connection_registry[Token(connection_token)].has_pending_output());
    assert_replica_socket_has_no_bytes(&mut replica_stream);
}

#[test]
fn failed_paused_master_rearm_isolates_the_replica_session() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind master peer");
    let peer_addr = listener.local_addr().expect("master peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect master transport");
    let (_master_stream, _) = listener.accept().expect("accept master transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    backend.replication_runtime.set_replica_link(
        crate::framework::distributed::replication::ReplicaLink::streaming(
            ConnectionId(peer.token.0),
            crate::framework::distributed::replication::MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
            "0123456789abcdef0123456789abcdef01234567".to_string(),
            offset(0),
        ),
    );
    backend
        .replication_runtime
        .set_replica_main_read_paused(true);
    backend
        .transport
        .deregister_for_test(peer.token)
        .expect("inject rearm failure");

    backend.handle_writable(peer.token);

    assert!(!backend.connection_registry.contains(Token(peer.token.0)));
    assert!(backend.replication_runtime.replica_link().is_none());
}

#[test]
fn replacement_master_writable_drain_does_not_inherit_paused_interest() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let old_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let old_addr = old_listener.local_addr().unwrap();
    let old_peer = backend.transport.connect_tcp(old_addr).unwrap();
    let (_old_master, _) = old_listener.accept().unwrap();
    backend.add_connection(RuntimeConnectedPeer {
        token: old_peer.token,
        peer_addr: old_addr,
    });
    backend.replication_runtime.set_replica_link(
        crate::framework::distributed::replication::ReplicaLink::streaming(
            ConnectionId(old_peer.token.0),
            crate::framework::distributed::replication::MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
            "0123456789abcdef0123456789abcdef01234567".to_string(),
            offset(0),
        ),
    );
    backend
        .replication_runtime
        .set_replica_main_read_paused(true);
    backend.remove_connection(old_peer.token.0);

    let new_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let new_addr = new_listener.local_addr().unwrap();
    let new_peer = backend.transport.connect_tcp(new_addr).unwrap();
    let (_new_master, _) = new_listener.accept().unwrap();
    backend.add_connection(RuntimeConnectedPeer {
        token: new_peer.token,
        peer_addr: new_addr,
    });
    backend.replication_runtime.set_replica_link(
        crate::framework::distributed::replication::ReplicaLink::streaming(
            ConnectionId(new_peer.token.0),
            crate::framework::distributed::replication::MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
            "fedcba9876543210fedcba9876543210fedcba98".to_string(),
            offset(0),
        ),
    );
    backend.transport.clear_interest_requests_for_test();

    backend.handle_writable(new_peer.token);

    assert_eq!(
        backend.transport.last_set_interest_calls_for_test().last(),
        Some(&(new_peer.token, Interest::Readable))
    );
}

#[test]
fn aof_open_and_close_reset_only_aof_cursor() {
    let dir = _temp_dir("aof-open-close-cursor");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.ctx.set_repl_seldb(9);
    backend.ctx.persistence_mut().set_aof_selected_db(7);

    backend.flush_aof_if_needed();

    assert!(backend.ctx.aof().journal.is_some());
    assert_eq!(backend.ctx.repl_seldb(), 9);
    assert_eq!(backend.ctx.persistence().aof_selected_db(), -1);

    backend.ctx.set_repl_seldb(11);
    backend.ctx.persistence_mut().set_aof_selected_db(10);
    backend.ctx.config_mut().persistence.aof_enabled = false;
    backend.flush_aof_if_needed();

    assert!(backend.ctx.aof().journal.is_none());
    assert_eq!(backend.ctx.repl_seldb(), 11);
    assert_eq!(backend.ctx.persistence().aof_selected_db(), -1);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn loading_does_not_create_live_aof_increment_before_replay_finishes() {
    let dir = _temp_dir("aof-loading-no-live-increment");
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    backend.ctx.config_mut().persistence.aof_filename = "appendonly.aof".to_string();
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.ctx.set_host_task_loading(true);

    backend.flush_aof_if_needed();

    assert!(backend.ctx.aof().journal.is_none());
    assert!(!dir
        .join("appendonlydir")
        .join("appendonly.aof.1.incr.aof")
        .exists());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn no_appendfsync_on_rewrite_pauses_and_restores_effective_fsync_mode() {
    let dir = _temp_dir("aof-fsync-rewrite-gate");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.ctx.config_mut().persistence.aof_fsync =
        crate::foundation::io::persistence::AofFsync::Always;
    backend.ctx.config_mut().persistence.aof_no_fsync_on_rewrite = true;

    backend.flush_aof_if_needed();
    assert_eq!(
        backend
            .ctx
            .aof()
            .journal
            .as_ref()
            .unwrap()
            .appender
            .fsync_mode(),
        crate::foundation::io::persistence::AofFsync::Always
    );

    backend
        .ctx
        .persistence_mut()
        .begin_rdb_child(123, 1)
        .unwrap();
    backend.flush_aof_if_needed();
    assert_eq!(
        backend
            .ctx
            .aof()
            .journal
            .as_ref()
            .unwrap()
            .appender
            .fsync_mode(),
        crate::foundation::io::persistence::AofFsync::No
    );

    backend
        .ctx
        .persistence_mut()
        .complete_rdb_child(true, 2)
        .unwrap();
    backend.flush_aof_if_needed();
    assert_eq!(
        backend
            .ctx
            .aof()
            .journal
            .as_ref()
            .unwrap()
            .appender
            .fsync_mode(),
        crate::foundation::io::persistence::AofFsync::Always
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn aof_open_failure_disables_aof_and_resets_only_aof_cursor() {
    let dir = _temp_dir("aof-open-failure-cursor");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::write(&dir, b"not-a-directory").unwrap();
    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.ctx.aof_mut().last_write_status = crate::engine::persistence::OperationStatus::Err;
    backend.ctx.set_repl_seldb(9);
    backend.ctx.persistence_mut().set_aof_selected_db(7);

    backend.flush_aof_if_needed();

    assert!(!backend.ctx.config().persistence.aof_enabled);
    assert_eq!(
        backend.ctx.aof().last_write_status,
        crate::engine::persistence::OperationStatus::Err
    );
    assert_eq!(backend.ctx.persistence().aof_selected_db(), -1);
    assert_eq!(backend.ctx.repl_seldb(), 9);
    assert!(backend.ctx.aof().journal.is_none());
    std::fs::remove_file(dir).unwrap();
}

#[test]
fn rewrite_completion_preserves_cursor_for_collected_increment() {
    let dir = _temp_dir("aof-rewrite-completion-cursor");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    backend.ctx.config_mut().persistence.aof_enabled = false;
    crate::framework::commands::server::bgrewriteaof::start_aof_rewrite(&mut backend.ctx).unwrap();
    backend.ctx.persistence_mut().set_aof_selected_db(10);
    backend.ctx.persistence_mut().feed_aof(b"increment");

    let deadline = Instant::now() + Duration::from_secs(30);
    while backend.ctx.persistence().aof_rewrite_child_pid().is_some() {
        backend.reap_aof_child();
        assert!(Instant::now() < deadline, "AOF rewrite child did not exit");
        std::thread::yield_now();
    }

    assert_eq!(backend.ctx.persistence().aof_selected_db(), 10);
    std::fs::remove_dir_all(dir).unwrap();
}
