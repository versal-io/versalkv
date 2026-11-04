use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::reactor::{BeforePollSlot, DispatchContext, RunnableKey};
use std::collections::HashSet;

#[derive(Default)]
pub(crate) struct OrdinaryWriteScheduler {
    serviced_this_turn: HashSet<Token>,
}

impl OrdinaryWriteScheduler {
    pub(crate) fn begin_turn(&mut self) {
        self.serviced_this_turn.clear();
    }

    pub(crate) fn claim_service(&mut self, token: Token) -> bool {
        self.serviced_this_turn.insert(token)
    }

    pub(crate) fn remove(&mut self, token: Token) {
        self.serviced_this_turn.remove(&token);
    }

    pub(crate) fn was_serviced(&self, token: Token) -> bool {
        self.serviced_this_turn.contains(&token)
    }
}

use super::super::ServerWorkerState;
use crate::framework::distributed::replication::ConnectionId;

impl ServerWorkerState {
    pub(crate) fn is_scheduleable_ordinary_write(&self, token_value: usize) -> bool {
        self.connection_registry
            .get(crate::framework::capabilities::io::Token(token_value))
            .is_some_and(|connection| {
                self.replication_runtime
                    .role(ConnectionId(token_value))
                    .is_none()
                    && !super::write::is_slot_export_connection(connection)
                    && connection.has_pending_output()
            })
    }

    pub(crate) fn schedule_ordinary_write(&mut self, token_value: usize) -> bool {
        if self.pause_dual_relay_client_if_needed(Token(token_value)) {
            return false;
        }
        if !self.is_scheduleable_ordinary_write(token_value)
            || self.enforce_client_output_buffer_limit_for(token_value)
        {
            return false;
        }
        let token = Token(token_value);
        if self.writes.ordinary.was_serviced(token) {
            self.schedule_runtime_runnable_next_turn(RunnableKey::ClientWrite(token));
        } else {
            self.schedule_runtime_runnable(RunnableKey::ClientWrite(token));
        }
        true
    }

    pub(crate) fn drain_current_ordinary_writes(&mut self) {
        let _ =
            self.dispatch_before_poll_slot(BeforePollSlot::OrdinaryWrites, DispatchContext::Normal);
    }

    pub(crate) fn drive_pending_ordinary_writes(&mut self) {
        self.drain_current_ordinary_writes();
    }
}

#[cfg(test)]
mod tests {
    use crate::framework::capabilities::io::Token;
    use crate::framework::distributed::replication::ConnectionId;
    use crate::runtime::server::worker::reactor::blocked_events::BlockedOperation;
    use crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer;
    use crate::runtime::server::worker::reactor::RunnableKey;
    use crate::runtime::server::worker::ServerWorkerState;
    use std::io::Read;
    use std::net::{TcpListener, TcpStream};
    use std::time::Duration;

    fn connected_ordinary(backend: &mut ServerWorkerState) -> (TcpStream, usize, usize) {
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
        let connection_token = peer.token.0;
        (target, peer.token.0, connection_token)
    }

    #[test]
    fn runtime_starts_without_pending_ordinary_writes() {
        let runtime =
            crate::runtime::server::worker::ServerWorkerState::new(1).expect("create runtime");

        assert!(!runtime
            .event_batch
            .contains_runnable(RunnableKey::ClientWrite(Token(7))));
        assert!(!runtime.writes.ordinary.was_serviced(Token(7)));
    }

    #[test]
    fn connection_removal_discards_the_explicit_write_runnable() {
        let mut runtime =
            crate::runtime::server::worker::ServerWorkerState::new(1).expect("create runtime");
        let mut connection = crate::framework::capabilities::connection::Connection::new_fake();
        connection.set_token_for_test(crate::framework::capabilities::io::Token(77));
        let _connection_token = runtime.connection_registry.insert(connection);
        runtime.connection_registry[Token(77)].queue_reply(b"reply".to_vec());
        assert!(runtime.schedule_ordinary_write(77));
        assert!(runtime
            .event_batch
            .contains_runnable(RunnableKey::ClientWrite(Token(77))));

        runtime.remove_connection(77);

        assert!(!runtime
            .event_batch
            .contains_runnable(RunnableKey::ClientWrite(Token(77))));
        assert!(!runtime.writes.ordinary.was_serviced(Token(77)));
    }

    #[test]
    fn pending_ordinary_reply_is_flushed_by_before_sleep() {
        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        let (mut target, token_value, connection_token) = connected_ordinary(&mut backend);
        let reply = b"ordinary-before-sleep".to_vec();
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .queue_reply(reply.clone());
        assert!(backend.schedule_ordinary_write(token_value));
        backend
            .transport
            .set_interest(
                Token(token_value),
                crate::foundation::io::transport::Interest::Readable,
            )
            .expect("suppress writable readiness before the pending phase");
        backend.create_test_time_event(|runtime| runtime.ctx.set_shutdown_requested(true));

        let _server = crate::runtime::server::ServerRuntime::run_owned_for_test(backend)
            .expect("run one event-loop iteration");

        target
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("set ordinary target read timeout");
        let mut received = vec![0_u8; reply.len()];
        target
            .read_exact(&mut received)
            .expect("before-sleep must flush pending ordinary output");
        assert_eq!(received, reply);
    }

    #[test]
    fn run_settles_write_turn_left_open_by_startup_host_task() {
        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        let (mut target, token_value, connection_token) = connected_ordinary(&mut backend);
        let reply = b"startup-host-task-reply".to_vec();
        backend.writes.ordinary.begin_turn();
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .queue_reply(reply.clone());
        assert!(backend.schedule_ordinary_write(token_value));
        backend.create_test_time_event(|runtime| runtime.ctx.set_shutdown_requested(true));

        let _server = crate::runtime::server::ServerRuntime::run_owned_for_test(backend)
            .expect("enter the main event loop");

        target
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("set startup target read timeout");
        let mut received = vec![0_u8; reply.len()];
        target
            .read_exact(&mut received)
            .expect("main-loop entry must settle startup pending output");
        assert_eq!(received, reply);
    }

    #[test]
    fn before_sleep_writes_acl_reply_before_deferred_kill() {
        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        let (mut target, token_value, connection_token) = connected_ordinary(&mut backend);
        let reply = b"+OK\r\n".to_vec();
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .queue_reply(reply.clone());
        assert!(backend.schedule_ordinary_write(token_value));
        backend
            .transport
            .set_interest(
                Token(token_value),
                crate::foundation::io::transport::Interest::Readable,
            )
            .expect("suppress writable readiness before the pending phase");
        backend
            .ctx
            .pending_acl_tokens_to_kill_mut()
            .push(Token(token_value));
        backend.create_test_time_event(|runtime| runtime.ctx.set_shutdown_requested(true));

        let _server = crate::runtime::server::ServerRuntime::run_owned_for_test(backend)
            .expect("run one event-loop iteration");

        target
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("set ordinary target read timeout");
        let mut received = Vec::new();
        target
            .read_to_end(&mut received)
            .expect("deferred ACL kill must close after writing the reply");
        assert_eq!(received, reply);
    }

    #[test]
    fn writable_service_defers_later_ordinary_output_to_next_turn() {
        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        let (_client, token_value, connection_token) = connected_ordinary(&mut backend);
        let first = b"+FIRST\r\n".to_vec();
        let later = b"+LATER\r\n".to_vec();
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .queue_reply(first);
        backend.writes.ordinary.begin_turn();
        assert!(backend.schedule_ordinary_write(token_value));

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
        assert!(
            backend.writes.ordinary.was_serviced(Token(token_value)),
            "real writable readiness consumes the turn's ordinary write service"
        );
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .queue_reply(later.clone());
        assert!(backend.schedule_ordinary_write(token_value));
        assert!(backend
            .event_batch
            .contains_runnable(RunnableKey::ClientWrite(Token(token_value))));
        backend.drive_pending_ordinary_writes();

        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .ordinary_output_bytes(),
            later.len(),
            "a token already serviced by writable readiness cannot write again this turn"
        );

        backend.event_batch.begin_next_turn();
        backend.writes.begin_turn();
        backend.drive_pending_ordinary_writes();
        assert_eq!(
            backend.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .ordinary_output_bytes(),
            0
        );
    }

    #[test]
    fn continuous_soft_limit_survives_one_write_turn_and_disconnects() {
        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        let (_peer, token_value, connection_token) = connected_ordinary(&mut runtime);
        runtime
            .ctx
            .config_mut()
            .replication
            .normal_output_buffer_soft_limit = 150_000;
        runtime
            .ctx
            .config_mut()
            .replication
            .normal_output_buffer_soft_limit_seconds = 2;
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .queue_reply(vec![b'a'; 512 * 1024]);
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .queue_reply(vec![b'b'; 512 * 1024]);

        assert!(!runtime.enforce_client_output_buffer_limit_for(token_value));
        let reached_at = runtime.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .output
        .soft_limit_reached_at_ms
        .expect("first observation starts the soft-limit clock");
        runtime.writes.ordinary.begin_turn();
        assert!(runtime.schedule_ordinary_write(token_value));
        runtime.drain_current_ordinary_writes();

        assert!(
            runtime.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .pending_output_memory_usage()
                >= 150_000
        );
        assert_eq!(
            runtime.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .output
            .soft_limit_reached_at_ms,
            Some(reached_at)
        );
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .output
            .soft_limit_reached_at_ms = Some(crate::foundation::io::sys::time::mstime() - 2_001);

        assert!(runtime.enforce_client_output_buffer_limit_for(token_value));
        assert!(!runtime
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token_value)));
    }

    #[test]
    fn blocked_before_sleep_drains_current_ordinary_work() {
        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        let (mut target, token_value, connection_token) = connected_ordinary(&mut backend);
        backend.writes.ordinary.begin_turn();
        let reply = b"ordinary-blocked-before-sleep".to_vec();
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .queue_reply(reply.clone());
        assert!(backend.schedule_ordinary_write(token_value));
        backend
            .transport
            .set_interest(
                Token(token_value),
                crate::foundation::io::transport::Interest::Readable,
            )
            .expect("suppress writable readiness for the blocked-service test");

        backend.process_events_while_blocked(BlockedOperation::ClassicRdb {
            main: ConnectionId(usize::MAX),
        });

        target
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("set ordinary target read timeout");
        let mut received = vec![0_u8; reply.len()];
        target
            .read_exact(&mut received)
            .expect("blocked before-sleep must flush current ordinary output");
        assert_eq!(received, reply);
    }
}
