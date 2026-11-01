use crate::foundation::io::transport::{ReadOutcome, WriteOutcome};
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::reactor::io_workers::{
    ReadCompletion, ReadCompletionAction, WriteCompletion, WriteCompletionAction, WriteSubmission,
};
use crate::runtime::server::worker::{PendingRemovalOwner, ServerWorkerState};
use std::collections::VecDeque;
use std::io::Read;

const CLUSTER_IO_BYTE_BUDGET: usize = 64 * 1024;

impl ServerWorkerState {
    fn ensure_cluster_bus_io_handles(&mut self, token: Token) -> bool {
        if self.io_threads.has_connection(token) {
            return self.io_threads.is_cluster_bus(token);
        }
        let handles = self.transport.duplicate_stream(token).and_then(|read| {
            self.transport
                .duplicate_stream(token)
                .map(|write| (read, write))
        });
        match handles {
            Ok((read, write)) => {
                self.io_threads.register_cluster_bus(token, read, write);
                true
            }
            Err(_) => false,
        }
    }

    pub(super) fn try_submit_cluster_bus_read(&mut self, token: Token) -> bool {
        if self.io_threads.is_close_pending(token) {
            return true;
        }
        if !self.transport.is_connection_ready(token) {
            return false;
        }
        if self.ensure_cluster_bus_io_handles(token)
            && self
                .io_threads
                .submit_read(token, CLUSTER_IO_BYTE_BUDGET, false)
        {
            if let Some(id) = self.io_threads.read_inflight_id(token) {
                self.acquire_io_worker_read_socket_lease(id);
            }
            return true;
        }
        self.ctx.stats_mut().cluster_io_main_thread_fallbacks += 1;
        false
    }

    pub(super) fn read_cluster_bus_bytes(
        &mut self,
        token: Token,
    ) -> Option<(Vec<u8>, ReadOutcome)> {
        let mut stream = self.transport.stream(token)?;
        let mut bytes = vec![0; CLUSTER_IO_BYTE_BUDGET];
        let mut used = 0;
        let outcome = loop {
            match stream.read(&mut bytes[used..]) {
                Ok(0) => break ReadOutcome::PeerClosed,
                Ok(count) => {
                    used += count;
                    if used == bytes.len() {
                        break ReadOutcome::BudgetExhausted;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    break ReadOutcome::Quiescent;
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => break ReadOutcome::Failed(error),
            }
        };
        bytes.truncate(used);
        Some((bytes, outcome))
    }

    pub(super) fn try_submit_cluster_bus_write(&mut self, token: Token) -> bool {
        if self.io_threads.is_close_pending(token) {
            return true;
        }
        if !self.transport.is_connection_ready(token) {
            return false;
        }
        if self.io_threads.is_write_inflight(token)
            || self.io_threads.read_inflight_id(token).is_some()
            || self.io_threads.cluster_read_deferred(token)
        {
            return true;
        }
        if let Some(state) = self.ctx.cluster_state_mut().as_deref_mut() {
            self.cluster_bus.queue_initial_handshake(state, token.0);
        }
        let Some(connection) = self.cluster_bus.connections.get(&token.0) else {
            return true;
        };
        if connection.write_buf.is_empty() {
            return true;
        }
        // Keep the canonical queue on the main thread. Only this bounded
        // snapshot is visible to the worker; concurrent appends stay queued.
        let snapshot =
            connection.write_buf[..connection.write_buf.len().min(CLUSTER_IO_BYTE_BUDGET)].to_vec();
        if self.ensure_cluster_bus_io_handles(token) {
            match self.io_threads.submit_write(
                token,
                VecDeque::from([snapshot]),
                CLUSTER_IO_BYTE_BUDGET,
            ) {
                WriteSubmission::Submitted => {
                    if let Some(id) = self.io_threads.write_inflight_id(token) {
                        self.acquire_io_worker_write_socket_lease(id);
                    }
                    return true;
                }
                WriteSubmission::AlreadyPending(_) => return true,
                WriteSubmission::Unavailable(_) => {}
            }
        }
        self.ctx.stats_mut().cluster_io_main_thread_fallbacks += 1;
        false
    }

    pub(crate) fn complete_cluster_bus_read(&mut self, completion: ReadCompletion) {
        let id = completion.id;
        let cancelled = completion.cancelled;
        let action = self.io_threads.complete_read(completion);
        self.release_io_worker_read_socket_lease(id);
        if !cancelled {
            self.ctx.stats_mut().cluster_io_threaded_reads_processed += 1;
        }
        match action {
            ReadCompletionAction::Apply {
                token,
                bytes,
                outcome,
                ..
            } => {
                self.process_cluster_bus_read_result(token, bytes, outcome);
            }
            ReadCompletionAction::CloseTransport(token) => {
                self.deregister_or_defer(token, PendingRemovalOwner::ClusterBus);
            }
            ReadCompletionAction::Cancelled | ReadCompletionAction::Stale => {}
        }
    }

    pub(crate) fn complete_cluster_bus_write(&mut self, completion: WriteCompletion) {
        let id = completion.id;
        let cancelled = completion.cancelled;
        let action = self.io_threads.complete_write(completion);
        self.release_io_worker_write_socket_lease(id);
        if !cancelled {
            self.ctx.stats_mut().cluster_io_threaded_writes_processed += 1;
        }
        match action {
            WriteCompletionAction::Apply {
                token,
                written,
                outcome,
                ..
            } => {
                let mut state = self.ctx.cluster_state_mut().take();
                if let Some(connection) = self.cluster_bus.connections.get_mut(&token.0) {
                    connection.write_buf.drain(..written);
                    if connection.write_buf.is_empty() && connection.write_buf.capacity() > 4096 {
                        connection.write_buf = Vec::with_capacity(4096);
                    }
                    if written > 0 {
                        if let Some(state) = state.as_deref_mut() {
                            if connection.is_outgoing && connection.bytes_sent == 0 {
                                state.record_link_established(
                                    crate::engine::distributed::cluster::ClusterLinkDirection::To,
                                );
                            }
                            state.record_bus_bytes_sent(written);
                        }
                        connection.last_send = crate::foundation::io::sys::time::mstime();
                        connection.bytes_sent += written as u64;
                        connection.bytes_sent_since_read =
                            connection.bytes_sent_since_read.saturating_add(written);
                    }
                }
                if let Some(state) = state.as_deref_mut() {
                    self.record_inbound_cluster_link_if_ready(state, token);
                    self.refresh_cluster_link_views_for_state(state);
                    let limit = self.ctx.config().server.cluster_link_sendbuf_limit;
                    self.enforce_cluster_bus_sendbuf_limit_for_state(state, limit);
                }
                *self.ctx.cluster_state_mut() = state;
                if let WriteOutcome::Failed(error) = outcome {
                    crate::server_log!(warning, "Cluster bus write error: {}", error);
                    self.remove_cluster_bus_connection(token);
                    return;
                }
                self.update_cluster_bus_interest(token);
                if self.io_threads.cluster_read_deferred(token) {
                    self.handle_cluster_bus_readable(token);
                }
            }
            WriteCompletionAction::CloseTransport(token) => {
                self.deregister_or_defer(token, PendingRemovalOwner::ClusterBus);
            }
            WriteCompletionAction::Stale => {}
        }
    }

    pub(super) fn update_cluster_bus_interest(&mut self, token: Token) {
        let Some(connection) = self.cluster_bus.connections.get(&token.0) else {
            return;
        };
        let (interest, events) = if connection.write_buf.is_empty() {
            (
                crate::foundation::io::transport::Interest::Readable,
                crate::engine::distributed::cluster::LinkEvents::READABLE,
            )
        } else {
            (
                crate::foundation::io::transport::Interest::ReadWritable,
                crate::engine::distributed::cluster::LinkEvents::READ_WRITE,
            )
        };
        if self.transport.set_interest(token, interest).is_ok() {
            if let Some(connection) = self.cluster_bus.connections.get_mut(&token.0) {
                connection.events = events;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::distributed::cluster::msg::{
        ClusterBusBody, ClusterBusMessage, ClusterMsgType,
    };
    use crate::engine::distributed::cluster::ClusterState;
    use crate::runtime::server::worker::reactor::io_workers::WorkerCompletionEvent;
    use std::io::Write;
    use std::net::{Shutdown, TcpListener, TcpStream};
    use std::time::Duration;

    fn bus_connection(threads: u16) -> (ServerWorkerState, Token, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().server.io_threads = threads.into();
        backend.ctx.config_mut().server.io_threads_always_active = true;
        backend.configure_io_threads_from_config().unwrap();
        let peer = backend.transport.connect_tcp(addr).unwrap();
        let (socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        *backend.ctx.cluster_state_mut() =
            Some(Box::new(ClusterState::new("127.0.0.1", 6379, 16379)));
        backend.cluster_bus.register_incoming(peer.token, addr);
        (backend, peer.token, socket)
    }

    fn ping_packet() -> Vec<u8> {
        let remote = ClusterState::new("127.0.0.1", 6380, 16380);
        ClusterBusMessage::from_node(
            ClusterMsgType::Ping,
            &remote.myself,
            0,
            ClusterBusBody::Gossip(Vec::new()),
        )
        .encode()
        .unwrap()
    }

    fn write_completion(backend: &mut ServerWorkerState) -> WriteCompletion {
        match backend
            .recv_worker_completion_for_test(Duration::from_secs(2))
            .unwrap()
        {
            WorkerCompletionEvent::SocketWrite(completion) => completion,
            other => panic!("expected cluster write completion, got {other:?}"),
        }
    }

    #[test]
    fn cluster_bus_disabled_pool_counts_real_main_thread_io() {
        let (mut backend, token, mut socket) = bus_connection(1);
        backend
            .cluster_bus
            .connections
            .get_mut(&token.0)
            .unwrap()
            .write_buf = b"queued".to_vec();
        backend.handle_cluster_bus_writable(token);
        let mut bytes = [0; 6];
        socket.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"queued");
        assert_eq!(backend.ctx.stats().cluster_io_main_thread_fallbacks, 1);
        socket.write_all(&ping_packet()).unwrap();
        backend.transport.wait_readable_for_test(token);
        backend.handle_cluster_bus_readable(token);
        assert_eq!(
            backend
                .ctx
                .cluster_state()
                .as_ref()
                .unwrap()
                .total_bus_messages_received(),
            1
        );
        // Processing a PING also flushes its PONG through a main-thread fallback.
        assert_eq!(backend.ctx.stats().cluster_io_main_thread_fallbacks, 3);
        assert_eq!(backend.ctx.stats().cluster_io_threaded_reads_processed, 0);
        assert_eq!(backend.ctx.stats().cluster_io_threaded_writes_processed, 0);
    }

    #[test]
    fn cluster_bus_threaded_write_preserves_bounded_snapshot_and_appended_suffix() {
        let (mut backend, token, mut socket) = bus_connection(2);
        let payload = vec![b'x'; CLUSTER_IO_BYTE_BUDGET + 5];
        backend
            .cluster_bus
            .connections
            .get_mut(&token.0)
            .unwrap()
            .write_buf = payload.clone();
        backend.handle_cluster_bus_writable(token);
        assert!(backend.io_threads.is_write_inflight(token));
        assert_eq!(backend.ctx.stats().cluster_io_threaded_writes_processed, 0);
        backend
            .cluster_bus
            .connections
            .get_mut(&token.0)
            .unwrap()
            .write_buf
            .extend_from_slice(b"tail");
        let completion = write_completion(&mut backend);
        assert_eq!(completion.written, CLUSTER_IO_BYTE_BUDGET);
        backend.complete_io_write(completion);
        let mut received = vec![0; CLUSTER_IO_BYTE_BUDGET];
        socket.read_exact(&mut received).unwrap();
        assert_eq!(received, payload[..CLUSTER_IO_BYTE_BUDGET]);
        assert_eq!(
            backend.cluster_bus.connections[&token.0].write_buf,
            b"xxxxxtail"
        );
        assert_eq!(
            backend.cluster_bus.connections[&token.0].events,
            crate::engine::distributed::cluster::LinkEvents::READ_WRITE
        );
        backend.handle_cluster_bus_writable(token);
        backend
            .wait_for_io_completion_for_test(Duration::from_secs(2))
            .unwrap();
        let mut suffix = [0; 9];
        socket.read_exact(&mut suffix).unwrap();
        assert_eq!(&suffix, b"xxxxxtail");
        let connection = &backend.cluster_bus.connections[&token.0];
        assert!(connection.write_buf.is_empty());
        assert!(connection.write_buf.capacity() <= 4096);
        assert_eq!(connection.bytes_sent, (payload.len() + 4) as u64);
        assert_eq!(
            connection.events,
            crate::engine::distributed::cluster::LinkEvents::READABLE
        );
        assert_eq!(backend.ctx.stats().cluster_io_threaded_writes_processed, 2);
        assert_eq!(backend.ctx.stats().cluster_io_main_thread_fallbacks, 0);
    }

    #[test]
    fn cluster_bus_threaded_partial_write_preserves_canonical_queue() {
        let (mut backend, token, mut socket) = bus_connection(2);
        let filler = vec![b'z'; CLUSTER_IO_BYTE_BUDGET];
        let mut filled = 0;
        loop {
            let mut stream = backend.transport.stream(token).unwrap();
            match stream.write(&filler) {
                Ok(count) => {
                    assert_ne!(count, 0);
                    filled += count;
                    assert!(filled < 128 * 1024 * 1024);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("failed to fill socket: {error}"),
            }
        }
        let payload = vec![b'p'; CLUSTER_IO_BYTE_BUDGET + 13];
        backend
            .cluster_bus
            .connections
            .get_mut(&token.0)
            .unwrap()
            .write_buf = payload.clone();
        backend.handle_cluster_bus_writable(token);
        let completion = write_completion(&mut backend);
        let written = completion.written;
        match completion.outcome {
            WriteOutcome::Complete | WriteOutcome::BudgetExhausted => {
                assert_eq!(written, CLUSTER_IO_BYTE_BUDGET);
            }
            WriteOutcome::Backpressured => assert!(written < CLUSTER_IO_BYTE_BUDGET),
            WriteOutcome::Failed(ref error) => panic!("cluster write failed: {error}"),
        }
        backend.complete_io_write(completion);
        assert_eq!(
            backend.cluster_bus.connections[&token.0].write_buf,
            payload[written..]
        );
        let mut drained = vec![0; filled];
        socket.read_exact(&mut drained).unwrap();
        assert!(drained.iter().all(|byte| *byte == b'z'));
        let mut dispatches = 0;
        while !backend.cluster_bus.connections[&token.0]
            .write_buf
            .is_empty()
        {
            dispatches += 1;
            assert!(dispatches <= 3);
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            loop {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                assert!(
                    !remaining.is_zero(),
                    "timed out waiting for writable cluster socket"
                );
                if backend
                    .transport
                    .poll_events(Some(remaining))
                    .unwrap()
                    .iter()
                    .any(|event| event.token == token && event.writable)
                {
                    break;
                }
            }
            backend.handle_cluster_bus_writable(token);
            backend
                .wait_for_io_completion_for_test(Duration::from_secs(2))
                .unwrap();
        }
        let mut received = vec![0; payload.len()];
        socket.read_exact(&mut received).unwrap();
        assert_eq!(received, payload);
        assert!(backend.cluster_bus.connections[&token.0]
            .write_buf
            .is_empty());
    }

    #[test]
    fn cluster_bus_serializes_read_after_inflight_write() {
        let (mut backend, token, mut socket) = bus_connection(2);
        backend
            .cluster_bus
            .connections
            .get_mut(&token.0)
            .unwrap()
            .write_buf = b"queued".to_vec();
        backend.handle_cluster_bus_writable(token);
        let completion = write_completion(&mut backend);
        socket.write_all(&ping_packet()).unwrap();
        backend.transport.wait_readable_for_test(token);
        backend.handle_cluster_bus_readable(token);
        assert!(backend.io_threads.cluster_read_deferred(token));
        assert!(backend.io_threads.read_inflight_id(token).is_none());
        assert_eq!(backend.ctx.stats().cluster_io_threaded_reads_processed, 0);
        backend.complete_io_write(completion);
        assert!(!backend.io_threads.cluster_read_deferred(token));
        assert!(backend.io_threads.read_inflight_id(token).is_some());
        backend.handle_cluster_bus_writable(token);
        assert!(!backend.io_threads.is_write_inflight(token));
        backend
            .wait_for_io_completion_for_test(Duration::from_secs(2))
            .unwrap();
        assert_eq!(
            backend
                .ctx
                .cluster_state()
                .as_ref()
                .unwrap()
                .total_bus_messages_received(),
            1
        );
        assert_eq!(backend.ctx.stats().cluster_io_threaded_reads_processed, 1);
        assert_eq!(backend.ctx.stats().cluster_io_main_thread_fallbacks, 0);
    }

    #[test]
    fn cluster_bus_threaded_eof_processes_valid_prefix_before_teardown() {
        use crate::runtime::server::worker::reactor::sources::SourceOwner;
        use std::collections::HashSet;
        use std::time::Instant;

        for is_fin_deferred in [false, true] {
            let (mut backend, token, mut socket) = bus_connection(2);
            backend
                .route_runtime_source(token, SourceOwner::ClusterBus)
                .unwrap();
            let packet = ping_packet();
            socket.write_all(&packet).unwrap();
            if !is_fin_deferred {
                socket.shutdown(Shutdown::Write).unwrap();
            }
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut received = Vec::new();
            let mut operation_ids = HashSet::new();
            let mut reads = 0;
            let mut writes = 0;
            let mut has_seen_eof = false;
            let mut has_sent_deferred_fin = false;
            while !has_seen_eof || backend.transport.is_registered(token) {
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(!remaining.is_zero(), "timed out waiting for cluster EOF");
                if backend.io_threads.read_inflight_id(token).is_some()
                    || backend.io_threads.write_inflight_id(token).is_some()
                {
                    let completion = backend.recv_worker_completion_for_test(remaining).unwrap();
                    match &completion {
                        WorkerCompletionEvent::SocketRead(read) => {
                            assert_eq!(read.id.token, token);
                            assert_eq!(backend.io_threads.read_inflight_id(token), Some(read.id));
                            assert!(!read.cancelled);
                            assert!(!read.drain_to_budget);
                            assert!(operation_ids.insert((false, read.id)));
                            reads += 1;
                            received.extend_from_slice(&read.bytes);
                            assert!(packet.starts_with(&received));
                            match &read.outcome {
                                ReadOutcome::PeerClosed => has_seen_eof = true,
                                ReadOutcome::Quiescent => {}
                                other => panic!("unexpected cluster read outcome: {other:?}"),
                            }
                            eprintln!(
                                "deferred_fin={is_fin_deferred} read={:?} bytes={} outcome={:?}",
                                read.id,
                                read.bytes.len(),
                                read.outcome,
                            );
                        }
                        WorkerCompletionEvent::SocketWrite(write) => {
                            assert_eq!(write.id.token, token);
                            assert_eq!(backend.io_threads.write_inflight_id(token), Some(write.id),);
                            assert!(!write.cancelled);
                            assert!(operation_ids.insert((true, write.id)));
                            assert!(
                                !matches!(write.outcome, WriteOutcome::Failed(_)),
                                "cluster write failed: {:?}",
                                write.outcome,
                            );
                            writes += 1;
                        }
                        other => panic!("unexpected cluster completion: {other:?}"),
                    }
                    backend.apply_worker_completion(completion);
                    assert_eq!(
                        backend.ctx.stats().cluster_io_threaded_reads_processed,
                        reads
                    );
                    assert_eq!(
                        backend.ctx.stats().cluster_io_threaded_writes_processed,
                        writes
                    );
                    if is_fin_deferred && !has_sent_deferred_fin && received == packet {
                        assert!(!has_seen_eof);
                        assert!(backend.cluster_bus.is_bus_token(token));
                        assert_eq!(
                            backend
                                .ctx
                                .cluster_state()
                                .as_ref()
                                .unwrap()
                                .total_bus_messages_received(),
                            1,
                        );
                        // The real socket has supplied the complete prefix but no FIN.
                        // Drive the next read only after this observed quiescent state.
                        socket.shutdown(Shutdown::Write).unwrap();
                        has_sent_deferred_fin = true;
                    }
                } else {
                    let events = backend.transport.poll_events(Some(remaining)).unwrap();
                    for event in events.into_iter().filter(|event| event.token == token) {
                        backend.dispatch_transport_event((&event).into(), false);
                    }
                }
            }
            assert!(has_seen_eof);
            assert_eq!(has_sent_deferred_fin, is_fin_deferred);
            assert_eq!(received, packet);
            assert_eq!(
                backend
                    .ctx
                    .cluster_state()
                    .as_ref()
                    .unwrap()
                    .total_bus_messages_received(),
                1,
            );
            assert!(!backend.cluster_bus.is_bus_token(token));
            assert!(!backend.transport.is_registered(token));
            assert!(!backend.io_threads.has_connection(token));
            assert!(backend.io_threads.read_inflight_id(token).is_none());
            assert!(backend.io_threads.write_inflight_id(token).is_none());
            assert!(!backend.is_pending_transport_removal(token));
            assert!(backend.runtime_sources.resolve_token(token).is_none());
            assert!(!backend.socket_owners.contains_key(&token));
            assert_eq!(backend.ctx.stats().cluster_io_main_thread_fallbacks, 0);
            assert_eq!(
                backend.ctx.stats().cluster_io_threaded_reads_processed,
                reads
            );
            assert_eq!(
                backend.ctx.stats().cluster_io_threaded_writes_processed,
                writes
            );
        }
    }

    #[test]
    fn cluster_bus_readiness_hup_preserves_budget_exhausted_publish_tail() {
        use crate::engine::distributed::cluster::msg::ClusterPublish;
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;
        use crate::framework::capabilities::connection::Connection;
        use crate::runtime::server::worker::reactor::sources::SourceOwner;
        use crate::runtime::server::worker::reactor::ReadyEvent;
        use bytes::Bytes;

        for threads in [1, 2] {
            let (mut backend, token, mut socket) = bus_connection(threads);
            backend.ctx.config_mut().server.io_threads_always_active = false;
            backend.configure_io_threads_from_config().unwrap();
            backend
                .route_runtime_source(token, SourceOwner::ClusterBus)
                .unwrap();
            let mut sender = ClusterState::new("127.0.0.1", 6380, 16380).myself;
            sender.flags &= !CLUSTER_NODE_MYSELF;
            backend
                .ctx
                .cluster_state_mut()
                .as_deref_mut()
                .unwrap()
                .insert_node(sender.clone());
            let subscriber_token = Token(900);
            let mut subscriber = Connection::new_fake();
            subscriber.set_token_for_test(subscriber_token);
            let subscriber_key = backend.connection_registry.insert(subscriber);
            let channel = Bytes::from_static(b"channel");
            backend
                .ctx
                .pubsub_mut()
                .subscribe(subscriber_token.0, channel.clone());
            let payload = Bytes::from(vec![b'p'; 70 * 1024]);
            let packet = ClusterBusMessage::from_node(
                ClusterMsgType::Publish,
                &sender,
                0,
                ClusterBusBody::Publish(ClusterPublish::new(channel, payload.clone(), false)),
            )
            .encode()
            .unwrap();
            assert!(packet.len() > CLUSTER_IO_BYTE_BUDGET);
            socket.write_all(&packet).unwrap();
            socket.shutdown(Shutdown::Write).unwrap();
            backend.transport.wait_readable_for_test(token);
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
            assert!(
                backend.cluster_bus.is_bus_token(token),
                "budget exhausted must yield without closing"
            );
            assert_eq!(
                backend.cluster_bus.connections[&token.0].bytes_received,
                CLUSTER_IO_BYTE_BUDGET as u64
            );
            assert_eq!(
                backend
                    .ctx
                    .cluster_state()
                    .as_ref()
                    .unwrap()
                    .total_bus_messages_received(),
                0
            );
            assert!(backend.connection_registry[Token(subscriber_key)]
                .output
                .queue
                .is_empty());
            backend.transport.wait_readable_for_test(token);
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
            assert!(!backend.cluster_bus.is_bus_token(token));
            assert!(!backend.transport.is_registered(token));
            assert_eq!(
                backend
                    .ctx
                    .cluster_state()
                    .as_ref()
                    .unwrap()
                    .total_bus_messages_received(),
                1
            );
            let delivered: Vec<_> = backend.connection_registry[Token(subscriber_key)]
                .output
                .queue
                .iter()
                .flat_map(|frame| frame.iter().copied())
                .collect();
            let mut expected = format!(
                "*3\r\n$7\r\nmessage\r\n$7\r\nchannel\r\n${}\r\n",
                payload.len(),
            )
            .into_bytes();
            expected.extend_from_slice(&payload);
            expected.extend_from_slice(b"\r\n");
            assert_eq!(delivered, expected);
            assert_eq!(backend.ctx.stats().cluster_io_threaded_reads_processed, 0);
        }
    }

    #[test]
    fn cluster_bus_teardown_waits_for_inflight_read_and_write() {
        for is_write in [false, true] {
            let (mut backend, token, mut socket) = bus_connection(2);
            if is_write {
                backend
                    .cluster_bus
                    .connections
                    .get_mut(&token.0)
                    .unwrap()
                    .write_buf = b"queued".to_vec();
                backend.handle_cluster_bus_writable(token);
            } else {
                socket.write_all(&ping_packet()).unwrap();
                backend.transport.wait_readable_for_test(token);
                backend.handle_cluster_bus_readable(token);
            }
            backend.remove_cluster_bus_connection(token);
            assert!(!backend.cluster_bus.is_bus_token(token));
            assert!(backend.io_threads.is_close_pending(token));
            assert!(backend.transport.is_registered(token));
            backend
                .wait_for_io_completion_for_test(Duration::from_secs(2))
                .unwrap();
            assert!(!backend.transport.is_registered(token));
            assert!(!backend.io_threads.has_connection(token));
            assert_eq!(
                backend
                    .ctx
                    .cluster_state()
                    .as_ref()
                    .unwrap()
                    .total_bus_messages_received(),
                0
            );
            assert_eq!(
                backend.ctx.stats().cluster_io_threaded_reads_processed,
                u64::from(!is_write)
            );
            assert_eq!(
                backend.ctx.stats().cluster_io_threaded_writes_processed,
                u64::from(is_write)
            );
        }
    }
}
