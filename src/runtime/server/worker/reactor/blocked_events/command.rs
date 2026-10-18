//! Nested event-loop service while a command owns the outer stack.
//! This is event-loop scheduling, not command semantics. It protects the
//! initiating token while continuing bounded transport and producer work.

use std::cell::Cell;
use std::time::Duration;

use super::super::{
    BlockedRoundPhase, DispatchContext, DispatchOutcome, ReadinessEvent, ReadinessOrigin,
    ServerWorkerState,
};
use crate::framework::capabilities::io::Token;

const BLOCKED_EMPTY_POLL_THRESHOLD: u32 = 64;

pub(super) fn blocked_event_poll_timeout(
    requested: Option<Duration>,
    round: usize,
    rearm: bool,
    has_pending_data: bool,
) -> Option<Duration> {
    if has_pending_data {
        Some(Duration::ZERO)
    } else if round == 0 {
        requested
    } else if rearm {
        Some(Duration::from_millis(1))
    } else {
        Some(Duration::ZERO)
    }
}

fn pending_event_is_dispatchable(
    token: Token,
    protected_token: Token,
    is_read_protected: bool,
) -> bool {
    token != protected_token && !is_read_protected
}

thread_local! {
    static BLOCKED_EMPTY_POLLS: Cell<u32> = const { Cell::new(0) };
}

impl ServerWorkerState {
    pub(crate) fn process_events_while_command_blocked(&mut self, protected_token: Token) {
        self.process_events_while_command_blocked_with_timeout(
            protected_token,
            Some(Duration::ZERO),
        );
    }

    pub(crate) fn process_events_while_command_blocked_with_timeout(
        &mut self,
        protected_token: Token,
        first_poll_timeout: Option<Duration>,
    ) {
        let _command_time = crate::foundation::io::sys::time::enter_fresh_command_time_snapshot();
        self.writes.ordinary.begin_turn();
        for round in 0..4 {
            let _round_time = crate::foundation::io::sys::time::enter_fresh_command_time_snapshot();
            self.prepare_command_blocked_round_write_phase(round);
            // C processEventsWhileBlocked() enters aeProcessEvents with
            // AE_CALL_BEFORE_SLEEP on every nested iteration. The shared
            // blocked-round contract retains only the allowed phases.
            self.drain_blocked_round_initial_completions();
            let rearm = BLOCKED_EMPTY_POLLS.with(|cell| cell.get() >= BLOCKED_EMPTY_POLL_THRESHOLD);
            if rearm {
                self.rearm_client_read_interests(protected_token);
            }
            self.record_blocked_round_phase(BlockedRoundPhase::PendingData);
            let pending_events = self.transport.process_pending_data();
            let mut saw_dispatchable_pending = false;
            let mut stop_after_epilogue = false;
            for event in pending_events {
                let is_dispatchable = pending_event_is_dispatchable(
                    event.token,
                    protected_token,
                    self.blocked_events.is_read_protected(event.token),
                );
                if is_dispatchable {
                    saw_dispatchable_pending = true;
                }
                if self.dispatch_command_readiness(
                    protected_token,
                    ReadinessEvent {
                        token: event.token,
                        origin: ReadinessOrigin::PendingData,
                        readable: event.readable,
                        writable: event.writable,
                        read_closed: event.read_hup,
                        error: event.error,
                    },
                ) {
                    stop_after_epilogue = true;
                    break;
                }
            }
            if !stop_after_epilogue {
                self.flush_blocked_round_aof_barrier();
                self.flush_blocked_round_eligible_writes();
                let poll_timeout = blocked_event_poll_timeout(
                    first_poll_timeout,
                    round,
                    rearm,
                    saw_dispatchable_pending && self.transport.has_pending_data(),
                );
                self.record_blocked_round_phase(BlockedRoundPhase::Poll);
                match self.transport.poll_events(poll_timeout) {
                    Ok(events) => {
                        self.run_blocked_round_restricted_after_sleep(events.len());
                        if events.is_empty() {
                            // A zero-time poll is required to keep blocked scripts
                            // responsive, but spinning without yielding can starve the
                            // client thread that must deliver SCRIPT/FUNCTION KILL.
                            std::thread::yield_now();
                            BLOCKED_EMPTY_POLLS.with(|cell| {
                                if rearm {
                                    cell.set(0);
                                } else {
                                    cell.set(cell.get().saturating_add(1));
                                }
                            });
                            stop_after_epilogue = true;
                        } else {
                            BLOCKED_EMPTY_POLLS.with(|cell| cell.set(0));
                            self.record_blocked_round_phase(BlockedRoundPhase::PolledReadiness);
                            for event in events {
                                if self.dispatch_command_readiness(
                                    protected_token,
                                    ReadinessEvent {
                                        token: event.token,
                                        origin: ReadinessOrigin::Polled,
                                        readable: event.readable,
                                        writable: event.writable,
                                        read_closed: event.read_hup,
                                        error: event.error,
                                    },
                                ) {
                                    stop_after_epilogue = true;
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        crate::server_log!(warning, "process events while blocked failed: {}", e);
                        stop_after_epilogue = true;
                    }
                }
            }
            self.drain_blocked_round_final_completions();
            if stop_after_epilogue {
                self.run_while_blocked_cron();
                return;
            }
        }
        self.run_while_blocked_cron();
    }

    fn rearm_client_read_interests(&mut self, protected_token: Token) {
        let clients: Vec<(Token, bool)> = self
            .connection_registry
            .iter()
            .filter_map(|(_, conn)| {
                (conn.token() != protected_token
                    && !self.blocked_events.is_read_protected(conn.token()))
                .then_some((conn.token(), conn.has_pending_output()))
            })
            .collect();
        for (token, has_output) in clients {
            let interest = if has_output {
                crate::foundation::io::transport::Interest::ReadWritable
            } else {
                crate::foundation::io::transport::Interest::Readable
            };
            let _ = self.transport.set_interest(token, interest);
        }
    }

    fn prepare_command_blocked_round_write_phase(&mut self, round: usize) {
        self.prepare_blocked_round_write_phase(round);
    }

    fn dispatch_command_readiness(
        &mut self,
        protected_token: Token,
        readiness: ReadinessEvent,
    ) -> bool {
        let token = readiness.token;
        let reconnect_streaming_master = self.ctx.is_script_busy()
            && self
                .replication_runtime
                .replica_link()
                .is_some_and(|link| link.connection_id().0 == token.0 && link.is_streaming());
        let outcome = self.dispatch_readiness_in_context(
            DispatchContext::CommandBlocked {
                protected_client: protected_token,
            },
            readiness,
        );
        if reconnect_streaming_master
            && self.replication_runtime.replica_link().is_none()
            && self.ctx.replication().is_replica()
            && self.ctx.config().replication.master_host.is_some()
        {
            // C replicationHandlePrimaryDisconnection() immediately starts one
            // nonblocking reconnect instead of waiting for replicationCron.
            // Limit this to an established upstream lost from the blocked
            // script event pump; handshake failures retain normal backoff.
            self.replication_runtime.reset_master_reconnect_backoff();
            self.drive_replication();
        }
        if matches!(outcome, DispatchOutcome::ShutdownRequested)
            && (self.ctx.is_shutdown_requested() || self.ctx.shutdown_state().asap)
        {
            self.close_listeners_for_shutdown();
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::{blocked_event_poll_timeout, pending_event_is_dispatchable, ServerWorkerState};
    use crate::engine::distributed::replication::ReplicationOffset;
    use crate::framework::capabilities::io::Token;
    use crate::framework::distributed::replication::{
        ConnectionId, MasterInfo, RdbFilterMode, ReplicaConnectionInfo, ReplicaLink,
    };
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;
    use crate::runtime::server::worker::reactor::blocked_events::BlockedOperation;
    use crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer;
    use crate::runtime::server::worker::reactor::{
        BlockedRoundPhase, ReadinessEvent, ReadinessOrigin, ReadyEvent,
    };
    use std::collections::HashSet;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("valid test offset")
    }

    fn ready(
        token: Token,
        readable: bool,
        writable: bool,
        error: bool,
        read_hup: bool,
    ) -> ReadyEvent {
        ReadyEvent {
            token,
            readable,
            writable,
            error,
            read_hup,
        }
    }

    fn polled_ready(ready: ReadyEvent) -> ReadinessEvent {
        ReadinessEvent {
            token: ready.token,
            origin: ReadinessOrigin::Polled,
            readable: ready.readable,
            writable: ready.writable,
            read_closed: ready.read_hup,
            error: ready.error,
        }
    }

    fn pending_cluster_bus_connection(backend: &mut ServerWorkerState) -> (TcpStream, Token) {
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

        (server_stream, peer.token)
    }

    fn connected_client(backend: &mut ServerWorkerState) -> (TcpStream, Token) {
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

    #[test]
    fn blocked_dispatch_drops_events_for_isolated_pending_removal() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let (_server_stream, token) = pending_cluster_bus_connection(&mut backend);

        for (readable, writable, error, read_hup) in [
            (true, false, false, false),
            (false, true, false, false),
            (false, false, true, false),
            (false, false, false, true),
        ] {
            assert!(!backend.dispatch_command_readiness(
                Token(usize::MAX),
                polled_ready(ready(token, readable, writable, error, read_hup)),
            ));
        }

        // Centralized teardown isolated the owner already; the blocked dispatcher
        // drops its stale events via the single centralized predicate.
        assert!(!backend.cluster_bus.connections.contains_key(&token.0));
        assert!(backend.is_pending_transport_removal(token));
    }

    #[test]
    fn blocked_wait_uses_requested_timeout_only_for_first_poll() {
        assert_eq!(
            blocked_event_poll_timeout(Some(std::time::Duration::from_millis(1)), 0, false, false,),
            Some(std::time::Duration::from_millis(1))
        );
        assert_eq!(
            blocked_event_poll_timeout(Some(std::time::Duration::from_millis(1)), 1, false, false,),
            Some(std::time::Duration::ZERO)
        );
        assert_eq!(
            blocked_event_poll_timeout(Some(std::time::Duration::from_millis(1)), 0, false, true,),
            Some(std::time::Duration::ZERO)
        );
    }

    #[test]
    fn pending_dispatchability_excludes_protected_reads() {
        let protected = Token(7);
        let outer_protected = Token(8);
        let unprotected = Token(9);

        assert!(!pending_event_is_dispatchable(protected, protected, false));
        assert!(!pending_event_is_dispatchable(
            outer_protected,
            protected,
            true
        ));
        assert!(pending_event_is_dispatchable(unprotected, protected, false));
    }

    #[test]
    fn empty_command_blocked_poll_settles_dirty_replication_accounting() {
        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        backend.ctx.replication_mut().replica_output_buffer_bytes = 123;
        backend.replication_buffer_accounting_dirty = true;
        backend.transport.queue_test_transport_event_batch(&[]);

        backend.process_events_while_command_blocked(Token(usize::MAX));

        assert!(!backend.replication_buffer_accounting_dirty);
        assert_eq!(backend.ctx.replication().replica_output_buffer_bytes, 0);
    }

    #[test]
    fn successive_blocked_event_polls_give_controller_new_write_turns() {
        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        crate::framework::commands::register_all(&mut backend.registry);
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind controller peer");
        let peer_addr = listener.local_addr().expect("controller peer address");
        let peer = backend
            .transport
            .connect_tcp(peer_addr)
            .expect("connect controller transport");
        let (mut controller, _) = listener.accept().expect("accept controller transport");
        backend.add_connection(RuntimeConnectedPeer {
            token: peer.token,
            peer_addr,
        });
        backend.writes.ordinary.begin_turn();
        controller
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .expect("set controller timeout");

        for _ in 0..2 {
            controller
                .write_all(b"*1\r\n$4\r\nPING\r\n")
                .expect("write controller PING");
            backend.transport.wait_readable_for_test(peer.token);
            backend.process_events_while_command_blocked(Token(usize::MAX));
            let mut reply = [0_u8; 7];
            controller
                .read_exact(&mut reply)
                .expect("each nested event turn must flush its controller reply");
            assert_eq!(&reply, b"+PONG\r\n");
        }
    }

    #[test]
    fn command_blocked_processes_unprotected_pending_without_reading_protected_client() {
        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        crate::framework::commands::register_all(&mut backend.registry);

        let (mut protected_client, protected_token) = connected_client(&mut backend);
        let (mut other_client, other_token) = connected_client(&mut backend);
        other_client
            .set_read_timeout(Some(std::time::Duration::from_millis(100)))
            .expect("set other client read timeout");

        protected_client
            .write_all(b"*3\r\n$3\r\nSET\r\n$9\r\nprotected\r\n$5\r\nvalue\r\n")
            .expect("write protected request");
        other_client
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .expect("write unprotected request");
        backend.transport.wait_readable_for_test(protected_token);
        backend.transport.wait_readable_for_test(other_token);
        backend
            .transport
            .queue_test_pending_data_event(protected_token);
        backend.transport.queue_test_pending_data_event(other_token);
        backend.transport.queue_test_transport_event_batch(&[]);

        backend.process_events_while_command_blocked(protected_token);

        let mut reply = [0_u8; 7];
        other_client
            .read_exact(&mut reply)
            .expect("unprotected pending request must be serviced");
        assert_eq!(&reply, b"+PONG\r\n");
        assert!(backend.ctx.db_mut(0).unwrap().get(b"protected").is_none());
    }

    #[test]
    fn command_blocked_accepts_listener_and_services_new_connection() {
        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        crate::framework::commands::register_all(&mut backend.registry);
        let (listener_token, addr) = bind_runtime_listener(&mut backend);
        let mut client = TcpStream::connect(addr).expect("connect loading-time client");
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .expect("set client timeout");
        backend.transport.wait_readable_for_test(listener_token);
        backend.transport.queue_test_transport_event_batch(&[(
            listener_token,
            true,
            false,
            false,
            false,
        )]);

        backend.process_events_while_command_blocked(Token(usize::MAX));

        let token = backend
            .connection_registry
            .iter()
            .map(|(_, connection)| connection.token())
            .next()
            .expect("listener readiness must accept the client while blocked");
        client
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .expect("write loading-time PING");
        backend.transport.wait_readable_for_test(token);
        backend.transport.queue_test_pending_data_event(token);
        backend.transport.queue_test_transport_event_batch(&[]);

        backend.process_events_while_command_blocked(Token(usize::MAX));

        let mut reply = [0_u8; 7];
        client
            .read_exact(&mut reply)
            .expect("blocked event service must flush the accepted client reply");
        assert_eq!(&reply, b"+PONG\r\n");
    }

    #[test]
    fn command_blocked_shutdown_stops_later_pending_readiness_once() {
        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        crate::framework::commands::register_all(&mut backend.registry);
        let (mut shutdown_client, shutdown_token) = connected_client(&mut backend);
        let (mut later_client, later_token) = connected_client(&mut backend);
        shutdown_client
            .write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n")
            .expect("write shutdown request");
        later_client
            .write_all(b"*3\r\n$3\r\nSET\r\n$5\r\nlater\r\n$5\r\nvalue\r\n")
            .expect("write later request");
        backend.transport.wait_readable_for_test(shutdown_token);
        backend.transport.wait_readable_for_test(later_token);
        backend
            .transport
            .queue_test_pending_data_event(shutdown_token);
        backend.transport.queue_test_pending_data_event(later_token);
        let handled_before = backend.ctx.stats().eventloop_events_handled;
        let cron_before = backend.blocked_cron_calls_for_test();
        backend.clear_blocked_round_trace_for_test();

        backend.process_events_while_command_blocked(Token(usize::MAX));

        assert!(backend.ctx.is_shutdown_requested());
        assert!(backend.ctx.db_mut(0).unwrap().get(b"later").is_none());
        assert_eq!(
            backend.blocked_round_trace_for_test().last(),
            Some(&BlockedRoundPhase::FinalCompletions),
            "a terminal pending command must reach the common round epilogue",
        );
        assert_eq!(
            backend.ctx.stats().eventloop_events_handled,
            handled_before + 1,
            "the shutdown readiness must have one terminal dispatch outcome",
        );
        assert_eq!(backend.blocked_cron_calls_for_test(), cron_before + 1);
    }

    #[test]
    fn command_blocked_polled_shutdown_reaches_the_final_completion_epilogue() {
        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        crate::framework::commands::register_all(&mut backend.registry);
        let (mut shutdown_client, shutdown_token) = connected_client(&mut backend);
        let (mut later_client, later_token) = connected_client(&mut backend);
        shutdown_client
            .write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n")
            .expect("write shutdown request");
        later_client
            .write_all(b"*3\r\n$3\r\nSET\r\n$5\r\nlater\r\n$5\r\nvalue\r\n")
            .expect("write later request");
        backend.transport.wait_readable_for_test(shutdown_token);
        backend.transport.wait_readable_for_test(later_token);
        backend.transport.queue_test_transport_event_batch(&[
            (shutdown_token, true, false, false, false),
            (later_token, true, false, false, false),
        ]);
        backend.clear_blocked_round_trace_for_test();

        backend.process_events_while_command_blocked(Token(usize::MAX));

        assert!(backend.ctx.is_shutdown_requested());
        assert!(backend.ctx.db_mut(0).unwrap().get(b"later").is_none());
        assert_eq!(
            backend.blocked_round_trace_for_test().last(),
            Some(&BlockedRoundPhase::FinalCompletions),
        );
    }

    #[test]
    fn blocked_script_reconnects_immediately_after_streaming_master_disconnect() {
        use crate::engine::distributed::replication::ReplicationRole;

        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        let old_listener = TcpListener::bind("127.0.0.1:0").expect("bind old primary");
        let old_addr = old_listener.local_addr().expect("old primary address");
        let old_peer = backend
            .transport
            .connect_tcp(old_addr)
            .expect("connect old primary transport");
        let (_old_primary, _) = old_listener.accept().expect("accept old primary transport");
        backend.add_connection(RuntimeConnectedPeer {
            token: old_peer.token,
            peer_addr: old_addr,
        });
        backend
            .replication_runtime
            .set_replica_link(ReplicaLink::streaming(
                ConnectionId(old_peer.token.0),
                MasterInfo {
                    host: old_addr.ip().to_string(),
                    port: old_addr.port(),
                },
                "0123456789abcdef0123456789abcdef01234567".to_string(),
                offset(0),
            ));

        let new_listener = TcpListener::bind("127.0.0.1:0").expect("bind replacement primary");
        let new_addr = new_listener
            .local_addr()
            .expect("replacement primary address");
        backend.ctx.replication_mut().role = ReplicationRole::Replica;
        backend.ctx.config_mut().replication.master_host = Some(new_addr.ip().to_string());
        backend.ctx.config_mut().replication.master_port = new_addr.port();
        backend.ctx.config_mut().replication.master_link_status = "up".to_string();
        backend.ctx.set_script_busy(true, false);

        backend.dispatch_command_readiness(
            Token(usize::MAX),
            polled_ready(ready(old_peer.token, false, false, true, false)),
        );

        let replacement = backend
            .replication_runtime
            .replica_link()
            .expect("blocked event handling must reconnect the lost primary");
        assert_ne!(replacement.connection_id(), ConnectionId(old_peer.token.0));
        assert_eq!(replacement.master().port, new_addr.port());
        assert_eq!(
            backend.ctx.config().replication.master_link_status,
            "connect"
        );
    }

    #[test]
    fn blocked_event_loop_services_diskless_relay_without_socket_readiness() {
        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind replica peer");
        let peer_addr = listener.local_addr().expect("replica peer address");
        let peer = backend
            .transport
            .connect_tcp(peer_addr)
            .expect("connect replica transport");
        let (mut replica_stream, _) = listener.accept().expect("accept replica transport");
        replica_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .expect("set replica read timeout");
        backend.add_connection(RuntimeConnectedPeer {
            token: peer.token,
            peer_addr,
        });

        let replica_id = backend.replication_runtime.register_primary(
            ConnectionId(peer.token.0),
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: peer_addr.port(),
                capability: vec!["eof".to_string(), "psync2".to_string()],
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        let pipe =
            crate::foundation::io::sys::fd::pipe(crate::foundation::io::sys::fd::PipeFlags::NONE)
                .expect("create diskless relay pipe");
        let (read_end, mut write_end) = pipe.into_parts();
        let payload = b"";
        write_end.write_all(payload).expect("write relay payload");
        drop(write_end);
        let marker = b"EOF-MARKER".to_vec();
        let transfer_id = crate::runtime::server::worker::reactor::sources::TransferId(1);
        let source = backend
            .install_runtime_source(
                crate::foundation::io::transport::OwnedPollSource::readable(read_end),
                crate::runtime::server::worker::reactor::sources::SourceOwner::InternalPipe(
                    crate::runtime::server::worker::reactor::sources::InternalPipeOwner::DisklessRdb {
                        transfer_id,
                    },
                ),
            )
            .expect("install diskless relay source");
        backend.full_sync.diskless_relay = Some(DisklessRelay {
            transfer_id,
            source: Some(source),
            child_release_write_end: None,
            targets: vec![replica_id],
            marker: marker.clone(),
            complete_full_resync: false,
            not_before_ms: 0,
            eof_reached: false,
            marker_queued_targets: HashSet::new(),
            marker_sent_targets: HashSet::new(),
            completion_announced: false,
            completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
        });
        backend.transport.queue_test_transport_event_batch(&[(
            source.token,
            true,
            false,
            false,
            false,
        )]);

        backend.process_events_while_command_blocked_with_timeout(
            Token(usize::MAX),
            Some(std::time::Duration::ZERO),
        );

        let mut received = vec![0; payload.len() + marker.len()];
        replica_stream
            .read_exact(&mut received)
            .expect("diskless relay bytes should be flushed while blocked");
        assert_eq!(received, [payload.as_slice(), marker.as_slice()].concat());
        assert!(backend.full_sync.diskless_relay.is_none());
    }

    #[test]
    fn command_blocked_poll_defers_outer_rdb_readiness_and_preserves_scope() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let primary_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let primary_addr = primary_listener.local_addr().unwrap();
        let primary_peer = backend.transport.connect_tcp(primary_addr).unwrap();
        let (mut primary, _) = primary_listener.accept().unwrap();
        backend.add_connection(RuntimeConnectedPeer {
            token: primary_peer.token,
            peer_addr: primary_addr,
        });
        let mut link = ReplicaLink::new(
            ConnectionId(primary_peer.token.0),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        link.connected(None, None);
        link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6379)
            .expect("advance replica link to PSYNC reply");
        backend.replication_runtime.set_replica_link(link);

        let local_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let local_addr = local_listener.local_addr().unwrap();
        let local_peer = backend.transport.connect_tcp(local_addr).unwrap();
        let (_local, _) = local_listener.accept().unwrap();
        backend.add_connection(RuntimeConnectedPeer {
            token: local_peer.token,
            peer_addr: local_addr,
        });

        let rdb = b"VALKEY080\x00\x01k\x01v\xff\x00\x00\x00\x00\x00\x00\x00\x00";
        let mut frame = b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0\r\n".to_vec();
        frame.extend_from_slice(format!("${}\r\n", rdb.len()).as_bytes());
        frame.extend_from_slice(rdb);
        primary.write_all(&frame).unwrap();

        let outer = BlockedOperation::ClassicRdb {
            main: ConnectionId(primary_peer.token.0),
        };
        backend.blocked_events.push_scope(outer);
        backend.transport.queue_test_transport_event_batch(&[
            (primary_peer.token, true, true, false, false),
            (local_peer.token, true, true, true, false),
            (primary_peer.token, false, false, true, false),
        ]);

        backend.process_events_while_command_blocked(local_peer.token);

        assert_eq!(backend.blocked_events.current(), Some(outer));
        assert_eq!(backend.blocked_events.depth(), 1);
        let deferred = backend
            .blocked_events
            .pop_front_deferred()
            .expect("outer RDB readiness must be deferred");
        assert_eq!(deferred.token, primary_peer.token);
        assert!(deferred.readable);
        assert!(deferred.read_hup);
        assert!(!deferred.writable, "writable readiness is serviced once");
        let protected = backend
            .blocked_events
            .pop_front_deferred()
            .expect("protected command read must remain deferred");
        assert_eq!(protected.token, local_peer.token);
        assert!(protected.readable);
        assert!(protected.read_hup);
        assert!(!protected.writable, "writable readiness is serviced once");
        assert!(backend.blocked_events.pop_front_deferred().is_none());
        assert!(
            backend
                .replication_runtime
                .replica_link()
                .is_some_and(|link| !link.has_pending_rdb_receive()),
            "nested command polling must not re-enter the outer RDB callback"
        );
        assert!(backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(
                local_peer.token.0
            )));
        assert_eq!(backend.blocked_events.pop_scope(), Some(outer));
    }
}
