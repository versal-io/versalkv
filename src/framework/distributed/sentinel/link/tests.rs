use super::SentinelLinkKind;
use super::{
    auth_for_instance, current_primary_addr, sentinel_announce_addr, RespFrame,
    SentinelLinkManager, SentinelReadState,
};
use crate::engine::command::context::Token;
use crate::engine::distributed::sentinel::hello::SENTINEL_HELLO_CHANNEL;
use crate::engine::distributed::sentinel::{
    DownState, FailoverState, InstanceRole, SentinelInstance, SentinelState,
};
use crate::framework::distributed::sentinel::io::{IoInterest, SentinelIo};
use std::collections::HashMap;

impl SentinelLinkManager {
    fn register_for_test(&mut self, addr: &str, kind: SentinelLinkKind, token: Token) {
        self.by_endpoint.insert((addr.to_string(), kind), token.0);
        self.links.insert(
            token.0,
            super::SentinelLink {
                token,
                addr: addr.to_string(),
                kind,
                connected: false,
                authenticated: false,
                refcount: 1,
                close_pending: false,
                input: Vec::new(),
                output: Vec::new(),
                pending: Vec::new(),
            },
        );
    }

    fn refcount_for_test(&self, token: Token) -> Option<usize> {
        self.links.get(&token.0).map(|link| link.refcount)
    }

    fn retry_after_for_test(&self, addr: &str, kind: SentinelLinkKind) -> Option<i64> {
        self.retry_after.get(&(addr.to_string(), kind)).copied()
    }

    fn link_count_for_test(&self) -> usize {
        self.links.len()
    }

    fn output_count_for_test(&self, token: Token) -> usize {
        self.links
            .get(&token.0)
            .map(|link| link.output.len())
            .unwrap_or(0)
    }

    fn output_for_test(&self, token: Token, index: usize) -> Option<&[u8]> {
        self.links
            .get(&token.0)?
            .output
            .get(index)
            .map(Vec::as_slice)
    }

    fn pending_count_for_test(&self, token: Token) -> usize {
        self.links
            .get(&token.0)
            .map(|link| link.pending.len())
            .unwrap_or(0)
    }

    fn is_authenticated_for_test(&self, token: Token) -> bool {
        self.links
            .get(&token.0)
            .is_some_and(|link| link.authenticated)
    }

    fn set_connected_for_test(&mut self, token: Token, connected: bool) {
        if let Some(link) = self.links.get_mut(&token.0) {
            link.connected = connected;
        }
    }

    fn set_authenticated_for_test(&mut self, token: Token, authenticated: bool) {
        if let Some(link) = self.links.get_mut(&token.0) {
            link.authenticated = authenticated;
        }
    }

    fn pending_request_for_test(
        &self,
        token: Token,
        index: usize,
    ) -> Option<&super::SentinelRequest> {
        self.links.get(&token.0)?.pending.get(index)
    }
}

struct TestSentinelIo {
    next_token: usize,
    writes: Vec<Vec<u8>>,
    max_write_bytes: Option<usize>,
    remove_error: Option<String>,
    input: Vec<u8>,
    input_offset: usize,
    interrupted_reads: usize,
    would_block_after_input: bool,
}

impl TestSentinelIo {
    fn new() -> Self {
        Self {
            next_token: 1,
            writes: Vec::new(),
            max_write_bytes: None,
            remove_error: None,
            input: Vec::new(),
            input_offset: 0,
            interrupted_reads: 0,
            would_block_after_input: false,
        }
    }

    fn fail_next_remove(&mut self) {
        self.remove_error = Some("deregister failed".to_string());
    }
}

impl SentinelIo for TestSentinelIo {
    fn connect_tcp(&mut self, addr: &str) -> Result<Token, String> {
        let _: std::net::SocketAddr = addr.parse().map_err(|error| format!("{error}"))?;
        let token = Token(self.next_token);
        self.next_token += 1;
        Ok(token)
    }

    fn set_interest(&mut self, _token: Token, _interest: IoInterest) -> Result<(), String> {
        Ok(())
    }

    fn remove(&mut self, _token: Token) -> Result<(), String> {
        match self.remove_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn take_connect_error(&mut self, _token: Token) -> Result<Option<String>, String> {
        Ok(None)
    }

    fn read_stream(&mut self, _token: Token, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.interrupted_reads > 0 {
            self.interrupted_reads -= 1;
            return Err(std::io::ErrorKind::Interrupted.into());
        }
        let remaining = &self.input[self.input_offset..];
        if remaining.is_empty() && self.would_block_after_input {
            return Err(std::io::ErrorKind::WouldBlock.into());
        }
        let read = remaining.len().min(buf.len());
        buf[..read].copy_from_slice(&remaining[..read]);
        self.input_offset += read;
        Ok(read)
    }

    fn write_stream(&mut self, _token: Token, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.max_write_bytes.unwrap_or(buf.len()).min(buf.len());
        self.writes.push(buf[..written].to_vec());
        Ok(written)
    }
}

#[test]
fn parses_complete_bulk_reply_without_consuming_following_reply() {
    let mut input = b"$4\r\ninfo\r\n+OK\r\n".to_vec();
    assert_eq!(
        RespFrame::take(&mut input),
        Some(RespFrame::Bulk(b"info".to_vec()))
    );
    assert_eq!(input, b"+OK\r\n");
}

#[test]
fn hostname_announcement_without_overrides_uses_runtime_address() {
    let mut state = SentinelState::new();
    state.announce_addr = "127.0.0.1:26379".to_string();
    state.announce_hostnames = true;
    assert_eq!(sentinel_announce_addr(&state), "127.0.0.1:26379");
}

#[test]
fn parses_pubsub_message_array() {
    let mut input = b"*3\r\n$7\r\nmessage\r\n$18\r\n__sentinel__:hello\r\n$5\r\nhello\r\n".to_vec();
    assert_eq!(
        RespFrame::take(&mut input),
        Some(RespFrame::Array(vec![
            RespFrame::Bulk(b"message".to_vec()),
            RespFrame::Bulk(b"__sentinel__:hello".to_vec()),
            RespFrame::Bulk(b"hello".to_vec()),
        ]))
    );
}

#[test]
fn manager_tracks_owned_tokens() {
    let mut manager = SentinelLinkManager::new();
    manager.register_for_test("127.0.0.1:6379", SentinelLinkKind::Command, Token(42));
    assert!(manager.is_link_token(Token(42)));
    assert!(!manager.is_link_token(Token(43)));
}

#[test]
fn readable_hup_drains_sentinel_tail_before_removal() {
    let mut manager = SentinelLinkManager::new();
    let token = Token(42);
    manager.register_for_test("127.0.0.1:6379", SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);
    let mut driver = TestSentinelIo::new();
    driver.input = b"+PONG\r\n".repeat(10_000);
    driver.interrupted_reads = 1;
    assert!(driver.input.len() > 64 * 1024);
    let mut ctx = crate::framework::context::ServerContext::new(1);

    manager.readable_after_hup(&mut ctx, &mut driver, token, 1_000);

    assert_eq!(driver.input_offset, driver.input.len());
    assert!(!manager.is_link_token(token));
}

#[test]
fn readable_retries_interrupted_then_stops_at_would_block() {
    let mut manager = SentinelLinkManager::new();
    let token = Token(42);
    manager.register_for_test("127.0.0.1:6379", SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);
    let mut driver = TestSentinelIo::new();
    driver.input = b"+PONG\r\n".to_vec();
    driver.interrupted_reads = 1;
    driver.would_block_after_input = true;
    let mut ctx = crate::framework::context::ServerContext::new(1);

    manager.readable(&mut ctx, &mut driver, token, 1_000);

    assert_eq!(driver.input_offset, driver.input.len());
    assert!(manager.is_link_token(token));
}

#[test]
fn bounded_read_reports_that_a_continuation_is_required() {
    let mut manager = SentinelLinkManager::new();
    let token = Token(42);
    manager.register_for_test("127.0.0.1:6379", SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);
    let mut driver = TestSentinelIo::new();
    driver.input = vec![b'x'; 64 * 1024 + 1];
    driver.would_block_after_input = true;
    let mut ctx = crate::framework::context::ServerContext::new(1);

    let outcome = manager.readable(&mut ctx, &mut driver, token, 1_000);

    assert_eq!(outcome.transport, SentinelReadState::BudgetExhausted);
    assert!(!outcome.has_buffered_frames);
    assert!(outcome.needs_continuation());
    assert_eq!(driver.input_offset, 64 * 1024);
}

#[test]
fn buffered_frames_schedule_continuation() {
    let mut manager = SentinelLinkManager::new();
    let token = Token(42);
    manager.register_for_test("127.0.0.1:6379", SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);
    let mut driver = TestSentinelIo::new();
    driver.input = b"+PONG\r\n".repeat(257);
    driver.would_block_after_input = true;
    let mut ctx = crate::framework::context::ServerContext::new(1);

    let outcome = manager.readable(&mut ctx, &mut driver, token, 1_000);

    assert_eq!(outcome.transport, SentinelReadState::Quiescent);
    assert!(outcome.has_buffered_frames);
    assert!(outcome.needs_continuation());
    assert_eq!(manager.links[&token.0].input, b"+PONG\r\n");
}

#[test]
fn incomplete_buffered_frame_does_not_schedule_continuation() {
    let mut manager = SentinelLinkManager::new();
    let token = Token(42);
    manager.register_for_test("127.0.0.1:6379", SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);
    let mut driver = TestSentinelIo::new();
    driver.input = b"+PONG\r\n".repeat(256);
    driver.input.extend_from_slice(b"+partial");
    driver.would_block_after_input = true;
    let mut ctx = crate::framework::context::ServerContext::new(1);

    let outcome = manager.readable(&mut ctx, &mut driver, token, 1_000);

    assert_eq!(outcome.transport, SentinelReadState::Quiescent);
    assert!(!outcome.has_buffered_frames);
    assert!(!outcome.needs_continuation());
    assert_eq!(manager.links[&token.0].input, b"+partial");
}

#[test]
fn connected_command_link_sets_sentinel_client_name() {
    let mut manager = SentinelLinkManager::new();
    let mut driver = TestSentinelIo::new();
    let token = manager
        .connect(
            &mut driver,
            "127.0.0.1:6379",
            SentinelLinkKind::Command,
            1_000,
        )
        .unwrap();
    let ctx = crate::framework::context::ServerContext::new(1);
    manager.writable(&ctx, &mut driver, token, 1_000);
    assert_eq!(
        driver.writes,
        vec![b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$21\r\nsentinel-00000000-cmd\r\n".to_vec()]
    );
    assert_eq!(manager.pending_count_for_test(token), 1);
}

#[test]
fn short_writes_continue_until_sentinel_output_is_drained() {
    let mut manager = SentinelLinkManager::new();
    let token = Token(42);
    manager.register_for_test("127.0.0.1:6379", SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);
    assert!(manager.queue_command(
        "127.0.0.1:6379",
        super::SentinelRequest::Ping(7),
        &["PING"],
        &HashMap::new(),
        "",
        "",
    ));
    let mut driver = TestSentinelIo::new();
    driver.max_write_bytes = Some(3);
    let ctx = crate::framework::context::ServerContext::new(1);

    manager.writable(&ctx, &mut driver, token, 1_000);

    assert_eq!(manager.output_count_for_test(token), 0);
    assert!(driver.writes.len() > 1);
}

#[test]
fn subscription_link_consumes_auth_reply_before_pubsub_frames() {
    let mut manager = SentinelLinkManager::new();
    let token = Token(42);
    manager.register_for_test("127.0.0.1:6379", SentinelLinkKind::HelloSubscription, token);
    manager.set_connected_for_test(token, true);
    super::queue_request(
        manager.links.get_mut(&token.0).unwrap(),
        super::SentinelRequest::Auth,
        &["AUTH", "secret"],
    );
    let mut ctx = crate::framework::context::ServerContext::new(1);
    manager.handle_frame(&mut ctx, token, RespFrame::Simple(b"OK".to_vec()), 1_000);
    assert!(manager.is_authenticated_for_test(token));
    assert_eq!(manager.pending_count_for_test(token), 0);
}

#[test]
fn subscription_confirmation_can_arrive_before_auth_reply() {
    let mut manager = SentinelLinkManager::new();
    let token = Token(42);
    manager.register_for_test("127.0.0.1:6379", SentinelLinkKind::HelloSubscription, token);
    manager.set_connected_for_test(token, true);
    let link = manager.links.get_mut(&token.0).unwrap();
    super::queue_request(link, super::SentinelRequest::Auth, &["AUTH", "secret"]);
    super::queue_request(
        link,
        super::SentinelRequest::SubscribeHello,
        &["SUBSCRIBE", SENTINEL_HELLO_CHANNEL],
    );
    let mut ctx = crate::framework::context::ServerContext::new(1);
    let confirmation = RespFrame::Array(vec![
        RespFrame::Bulk(b"subscribe".to_vec()),
        RespFrame::Bulk(SENTINEL_HELLO_CHANNEL.as_bytes().to_vec()),
        RespFrame::Integer(1),
    ]);
    // Subscribe confirmation must NOT flip authenticated=true when an Auth
    // reply is still pending: that race caused NOAUTH on ACL-protected links.
    manager.handle_frame(&mut ctx, token, confirmation, 1_000);
    assert!(!manager.is_authenticated_for_test(token));
    // Only SubscribeHello is cleared; Auth remains pending so dispatch_reply
    // can do the real authentication handshake.
    assert_eq!(manager.pending_count_for_test(token), 1);

    // After AUTH OK arrives the link is marked authenticated.
    let auth_ok = RespFrame::Simple(b"OK".to_vec());
    manager.handle_frame(&mut ctx, token, auth_ok, 1_000);
    assert!(manager.is_authenticated_for_test(token));
    assert_eq!(manager.pending_count_for_test(token), 0);
}

#[test]
fn remove_schedules_endpoint_retry() {
    let mut manager = SentinelLinkManager::new();
    manager.register_for_test("127.0.0.1:6379", SentinelLinkKind::Command, Token(42));
    let mut driver = TestSentinelIo::new();
    manager.remove(&mut driver, Token(42), 1_000).unwrap();
    assert!(!manager.is_link_token(Token(42)));
    assert_eq!(
        manager.retry_after_for_test("127.0.0.1:6379", SentinelLinkKind::Command),
        Some(2_000)
    );
}

#[test]
fn topology_rebuild_removes_endpoint_without_reconnect_backoff() {
    let mut manager = SentinelLinkManager::new();
    manager.register_for_test("127.0.0.1:6379", SentinelLinkKind::Command, Token(42));
    let mut driver = TestSentinelIo::new();

    manager
        .reset_by_addr(&mut driver, "127.0.0.1:6379", 1_000)
        .unwrap();

    assert!(!manager.is_link_token(Token(42)));
    assert_eq!(
        manager.retry_after_for_test("127.0.0.1:6379", SentinelLinkKind::Command),
        None
    );
}

#[test]
fn connect_invalid_address_sets_retry_without_token() {
    let mut manager = SentinelLinkManager::new();
    let mut driver = TestSentinelIo::new();
    assert_eq!(
        manager.connect(
            &mut driver,
            "not-an-address",
            SentinelLinkKind::Command,
            1_000
        ),
        None
    );
    assert_eq!(manager.link_count_for_test(), 0);
    assert_eq!(
        manager.retry_after_for_test("not-an-address", SentinelLinkKind::Command),
        Some(2_000)
    );
}

#[test]
fn queue_command_records_output_and_pending_request() {
    let mut manager = SentinelLinkManager::new();
    let token = Token(42);
    manager.register_for_test("127.0.0.1:6379", SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);
    assert!(manager.queue_command(
        "127.0.0.1:6379",
        super::SentinelRequest::Ping(7),
        &["PING"],
        &HashMap::new(),
        "",
        "",
    ));
    assert_eq!(manager.output_count_for_test(token), 1);
    assert_eq!(manager.pending_count_for_test(token), 1);
}

#[test]
fn command_requests_wait_for_connection_handshake() {
    let mut manager = SentinelLinkManager::new();
    let token = Token(42);
    manager.register_for_test("127.0.0.1:6379", SentinelLinkKind::Command, token);
    assert!(!manager.queue_command(
        "127.0.0.1:6379",
        super::SentinelRequest::Ping(7),
        &["PING"],
        &HashMap::new(),
        "",
        "",
    ));
    assert_eq!(manager.output_count_for_test(token), 0);
    assert_eq!(manager.pending_count_for_test(token), 0);
}

#[test]
fn info_polling_uses_configured_debug_period() {
    let mut manager = SentinelLinkManager::new();
    let addr = "127.0.0.1:6379";
    let token = Token(42);
    manager.register_for_test(addr, SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);
    let mut ctx = crate::framework::context::ServerContext::new(1);
    ctx.sentinel_state_mut().tunables.info_period_ms = 100;
    let mut instance = SentinelInstance::new_master(1, "primary", addr, 1);
    instance.last_info_sent = 1_000;
    ctx.sentinel_state_mut()
        .registry
        .insert_instance(instance)
        .unwrap();
    manager.maybe_queue_info(&mut ctx, 1, addr, 1_101);
    assert_eq!(manager.pending_count_for_test(token), 1);
}

#[test]
fn replica_info_polling_accelerates_when_primary_is_objectively_down() {
    let mut manager = SentinelLinkManager::new();
    let addr = "127.0.0.1:6380";
    let token = Token(42);
    manager.register_for_test(addr, SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);

    let mut ctx = crate::framework::context::ServerContext::new(1);
    ctx.sentinel_state_mut().tunables.info_period_ms = 10_000;

    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
    master.down_state = DownState::ODown;
    master.replicas.push(2);
    ctx.sentinel_state_mut()
        .registry
        .insert_master(master.name.clone(), master)
        .unwrap();

    let mut replica = SentinelInstance::new_master(2, "replica", addr, 1);
    replica.role = InstanceRole::Replica;
    replica.last_info_sent = 1_000;
    ctx.sentinel_state_mut()
        .registry
        .insert_instance(replica)
        .unwrap();

    manager.maybe_queue_info(&mut ctx, 2, addr, 2_001);

    assert_eq!(manager.pending_count_for_test(token), 1);
}

#[test]
fn hello_publish_uses_configured_debug_period() {
    let mut manager = SentinelLinkManager::new();
    let addr = "127.0.0.1:6379";
    let token = Token(42);
    manager.register_for_test(addr, SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);

    let mut ctx = crate::framework::context::ServerContext::new(1);
    ctx.sentinel_state_mut().sentinel_mode = true;
    ctx.sentinel_state_mut().initialized = true;
    ctx.sentinel_state_mut().announce_addr = "127.0.0.1:26379".to_string();
    ctx.sentinel_state_mut().tunables.publish_period_ms = 1_000;
    let mut master = SentinelInstance::new_master(1, "primary", addr, 1);
    master.last_hello_sent = 1_000;
    ctx.sentinel_state_mut()
        .registry
        .insert_master(master.name.clone(), master)
        .unwrap();

    manager.queue_hello_publishes(&mut ctx, 2_001);

    assert_eq!(manager.pending_count_for_test(token), 1);
    assert_eq!(
        ctx.sentinel_state()
            .registry
            .master("primary")
            .unwrap()
            .last_hello_sent,
        2_001
    );
}

#[test]
fn pending_ping_is_not_queued_again() {
    let mut manager = SentinelLinkManager::new();
    let addr = "127.0.0.1:6379";
    let token = Token(42);
    manager.register_for_test(addr, SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);
    super::queue_request(
        manager.links.get_mut(&token.0).unwrap(),
        super::SentinelRequest::Ping(1),
        &["PING"],
    );
    let mut ctx = crate::framework::context::ServerContext::new(1);
    ctx.sentinel_state_mut().tunables.ping_period_ms = 100;
    let mut instance = SentinelInstance::new_master(1, "primary", addr, 1);
    instance.last_ping_sent = 1_000;
    instance.act_ping_time = 1_000;
    ctx.sentinel_state_mut()
        .registry
        .insert_instance(instance)
        .unwrap();
    manager.maybe_queue_ping(&mut ctx, 1, addr, 1_101);
    assert_eq!(manager.pending_count_for_test(token), 1);
}

#[test]
fn ping_retries_after_invalid_reply_without_resetting_first_failure_time() {
    let mut manager = SentinelLinkManager::new();
    let addr = "127.0.0.1:6379";
    let token = Token(42);
    manager.register_for_test(addr, SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);
    let mut ctx = crate::framework::context::ServerContext::new(1);
    ctx.sentinel_state_mut().tunables.ping_period_ms = 100;
    let mut instance = SentinelInstance::new_master(1, "primary", addr, 1);
    instance.last_ping_sent = 1_000;
    instance.act_ping_time = 900;
    ctx.sentinel_state_mut()
        .registry
        .insert_instance(instance)
        .unwrap();
    manager.maybe_queue_ping(&mut ctx, 1, addr, 1_101);
    assert_eq!(manager.pending_count_for_test(token), 1);
    assert_eq!(
        ctx.sentinel_state().registry.instances()[&1].act_ping_time,
        900
    );
}

fn context_with_instance(id: u64, addr: &str) -> crate::framework::context::ServerContext {
    let mut ctx = crate::framework::context::ServerContext::new(1);
    ctx.sentinel_state_mut().sentinel_mode = true;
    ctx.sentinel_state_mut().initialized = true;
    ctx.sentinel_state_mut()
        .registry
        .insert_instance(SentinelInstance::new_master(id, "primary", addr, 1))
        .unwrap();
    ctx
}

#[test]
fn unestablished_command_link_marks_instance_disconnected() {
    let mut manager = SentinelLinkManager::new();
    let mut driver = TestSentinelIo::new();
    let addr = "127.0.0.1:6379";
    manager.register_for_test(addr, SentinelLinkKind::Command, Token(42));
    let mut ctx = context_with_instance(1, addr);
    manager.cron(&mut ctx, &mut driver, 1_000);
    assert_eq!(ctx.sentinel_state().registry.instances()[&1].link_state, 1);
    assert_eq!(
        ctx.sentinel_state().registry.instances()[&1].act_ping_time,
        0
    );
}

#[test]
fn newly_connected_command_link_does_not_imply_instance_health() {
    let mut manager = SentinelLinkManager::new();
    let mut driver = TestSentinelIo::new();
    let addr = "127.0.0.1:6379";
    let token = Token(42);
    manager.register_for_test(addr, SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);
    let mut ctx = context_with_instance(1, addr);
    let instance = ctx.sentinel_state_mut().registry.instance_mut(1).unwrap();
    instance.link_state = 1;
    instance.act_ping_time = 1_000;
    instance.last_ping_sent = 1_100;
    instance.link_down_since_time = 900;
    instance.down_state = crate::engine::distributed::sentinel::DownState::SDown;
    manager.cron(&mut ctx, &mut driver, 2_000);
    let instance = &ctx.sentinel_state().registry.instances()[&1];
    assert_eq!(instance.link_state, 0);
    assert_eq!(instance.act_ping_time, 1_000);
    assert_eq!(instance.last_ping_sent, 1_100);
    assert_eq!(instance.link_down_since_time, 900);
    assert_eq!(
        instance.down_state,
        crate::engine::distributed::sentinel::DownState::SDown
    );
}

#[test]
fn stale_close_clears_ping_timer_without_marking_instance_up() {
    let mut manager = SentinelLinkManager::new();
    let mut driver = TestSentinelIo::new();
    let addr = "127.0.0.1:6379";
    manager.register_for_test(addr, SentinelLinkKind::Command, Token(42));
    manager.set_connected_for_test(Token(42), true);
    let mut ctx = context_with_instance(1, addr);
    ctx.sentinel_state_mut()
        .pending_link_close
        .push(addr.to_string());
    let instance = ctx.sentinel_state_mut().registry.instance_mut(1).unwrap();
    instance.act_ping_time = 1_000;
    instance.last_ping_sent = 1_900;
    instance.down_state = crate::engine::distributed::sentinel::DownState::SDown;
    manager.cron(&mut ctx, &mut driver, 2_000);
    let instance = &ctx.sentinel_state().registry.instances()[&1];
    assert_eq!(instance.act_ping_time, 0);
    assert_eq!(instance.last_ping_sent, 1_900);
    assert_eq!(
        instance.down_state,
        crate::engine::distributed::sentinel::DownState::SDown
    );
}

#[test]
fn failed_connection_starts_down_timer() {
    let mut manager = SentinelLinkManager::new();
    let mut driver = TestSentinelIo::new();
    let mut ctx = context_with_instance(1, "not-an-address");
    manager.cron(&mut ctx, &mut driver, 1_000);
    assert_eq!(ctx.sentinel_state().registry.instances()[&1].link_state, 1);
    assert_eq!(
        ctx.sentinel_state().registry.instances()[&1].link_down_since_time,
        1_000
    );
}

#[test]
fn failed_auth_marks_command_link_disconnected() {
    let mut manager = SentinelLinkManager::new();
    let mut driver = TestSentinelIo::new();
    let addr = "127.0.0.1:6379";
    let token = Token(42);
    manager.register_for_test(addr, SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);
    manager.set_authenticated_for_test(token, true);
    let mut ctx = context_with_instance(1, addr);
    let mut master = ctx.sentinel_state().registry.instances()[&1].clone();
    master.auth_pass = "wrong-password".to_string();
    ctx.sentinel_state_mut().registry.remove_instance(master.id);
    ctx.sentinel_state_mut()
        .registry
        .insert_master(master.name.clone(), master)
        .unwrap();
    assert!(manager.queue_command(
        addr,
        super::SentinelRequest::Ping(1),
        &["PING"],
        ctx.sentinel_state().registry.instances(),
        "",
        "wrong-password",
    ));
    manager.handle_frame(
        &mut ctx,
        token,
        RespFrame::Error(b"WRONGPASS invalid username-password pair".to_vec()),
        1_000,
    );
    manager.cron(&mut ctx, &mut driver, 1_001);
    assert_eq!(ctx.sentinel_state().registry.instances()[&1].link_state, 1);
}

#[test]
fn generic_auth_error_keeps_link_usable_when_auth_is_not_required() {
    let mut manager = SentinelLinkManager::new();
    let addr = "127.0.0.1:6379";
    let token = Token(42);
    manager.register_for_test(addr, SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);
    super::queue_request(
        manager.links.get_mut(&token.0).unwrap(),
        super::SentinelRequest::Auth,
        &["AUTH", "secret"],
    );
    let mut ctx = context_with_instance(1, addr);

    manager.handle_frame(
        &mut ctx,
        token,
        RespFrame::Error(b"ERR AUTH called without any password configured".to_vec()),
        1_000,
    );

    assert!(ctx.sentinel_state().pending_link_close.is_empty());
    assert!(manager.is_authenticated_for_test(token));
}

#[test]
fn wrongpass_auth_reply_is_deferred_to_the_following_command_reply() {
    let mut manager = SentinelLinkManager::new();
    let addr = "127.0.0.1:6379";
    let token = Token(42);
    manager.register_for_test(addr, SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);
    let link = manager.links.get_mut(&token.0).unwrap();
    super::queue_request(
        link,
        super::SentinelRequest::Auth,
        &["AUTH", "stale-password"],
    );
    super::queue_request(
        link,
        super::SentinelRequest::SetName,
        &["CLIENT", "SETNAME", "sentinel"],
    );
    let mut ctx = context_with_instance(1, addr);

    manager.handle_frame(
        &mut ctx,
        token,
        RespFrame::Error(b"WRONGPASS invalid username-password pair".to_vec()),
        1_000,
    );

    assert!(ctx.sentinel_state().pending_link_close.is_empty());
    assert!(manager.is_authenticated_for_test(token));
    assert_eq!(manager.pending_count_for_test(token), 1);

    manager.handle_frame(
        &mut ctx,
        token,
        RespFrame::Error(b"NOAUTH Authentication required".to_vec()),
        1_001,
    );
    assert_eq!(
        ctx.sentinel_state().pending_link_close,
        vec![addr.to_string()]
    );
}

#[test]
fn auth_failure_link_close_restarts_ping_but_preserves_down_time() {
    let mut manager = SentinelLinkManager::new();
    let mut driver = TestSentinelIo::new();
    let addr = "127.0.0.1:6379";
    manager.register_for_test(addr, SentinelLinkKind::Command, Token(42));
    let mut ctx = context_with_instance(1, addr);
    let instance = ctx.sentinel_state_mut().registry.instance_mut(1).unwrap();
    instance.link_state = 1;
    instance.act_ping_time = 900;
    instance.link_down_since_time = 800;
    ctx.sentinel_state_mut()
        .pending_link_close
        .push(addr.to_string());
    manager.cron(&mut ctx, &mut driver, 1_000);
    assert_eq!(
        ctx.sentinel_state().registry.instances()[&1].act_ping_time,
        0
    );
    assert_eq!(
        ctx.sentinel_state().registry.instances()[&1].link_down_since_time,
        800
    );
}

#[test]
fn noauth_ping_reply_marks_command_link_disconnected() {
    let mut manager = SentinelLinkManager::new();
    let addr = "127.0.0.1:6379";
    let token = Token(42);
    manager.register_for_test(addr, SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);
    let mut ctx = context_with_instance(1, addr);
    assert!(manager.queue_command(
        addr,
        super::SentinelRequest::Ping(1),
        &["PING"],
        ctx.sentinel_state().registry.instances(),
        "",
        "",
    ));
    manager.handle_frame(
        &mut ctx,
        token,
        RespFrame::Error(b"NOAUTH Authentication required".to_vec()),
        1_000,
    );
    assert_eq!(ctx.sentinel_state().registry.instances()[&1].link_state, 1);
    assert_eq!(
        ctx.sentinel_state().pending_link_close,
        vec![addr.to_string()]
    );
}

#[test]
fn monitored_instances_use_per_master_auth_and_peers_use_sentinel_auth() {
    let mut state = SentinelState::new();
    state.sentinel_auth_user = "sentinel-user".to_string();
    state.sentinel_auth_pass = "sentinel-pass".to_string();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
    master.auth_user = "primary-user".to_string();
    master.auth_pass = "primary-pass".to_string();
    master.replicas.push(2);
    state
        .registry
        .insert_master("primary".to_string(), master)
        .unwrap();
    let mut replica = SentinelInstance::new_master(2, "127.0.0.1:6380", "127.0.0.1:6380", 2);
    replica.role = InstanceRole::Replica;
    state.registry.insert_instance(replica).unwrap();
    let mut peer = SentinelInstance::new_master(3, "peer", "127.0.0.1:26380", 2);
    peer.role = InstanceRole::Sentinel;
    state.registry.insert_instance(peer).unwrap();
    assert_eq!(
        auth_for_instance(&state, 1),
        ("primary-user".to_string(), "primary-pass".to_string())
    );
    assert_eq!(
        auth_for_instance(&state, 2),
        ("primary-user".to_string(), "primary-pass".to_string())
    );
    assert_eq!(
        auth_for_instance(&state, 3),
        ("sentinel-user".to_string(), "sentinel-pass".to_string())
    );
    assert_eq!(
        super::auth_for_addr(&state, "127.0.0.1:26380"),
        ("sentinel-user".to_string(), "sentinel-pass".to_string())
    );
}

#[test]
fn endpoint_auth_ignores_stale_duplicate_address_entries() {
    let mut state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
    master.auth_user = "sentinel-user".to_string();
    master.auth_pass = "sentinel-pass".to_string();
    master.replicas.push(2);
    state
        .registry
        .insert_master(master.name.clone(), master)
        .unwrap();
    let mut replica = SentinelInstance::new_master(2, "replica", "127.0.0.1:6380", 1);
    replica.role = InstanceRole::Replica;
    state.registry.insert_instance(replica).unwrap();
    let stale = SentinelInstance::new_master(3, "stale", "127.0.0.1:6380", 1);
    state.registry.insert_instance(stale).unwrap();
    assert_eq!(
        super::auth_for_addr(&state, "127.0.0.1:6380"),
        ("sentinel-user".to_string(), "sentinel-pass".to_string())
    );
}

#[test]
fn hello_advertises_promoted_replica_after_promotion_is_confirmed() {
    let mut state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
    master.flags |= crate::engine::distributed::sentinel::SRI_FAILOVER_IN_PROGRESS;
    master.promoted_replica = Some(2);
    let mut promoted = SentinelInstance::new_master(2, "promoted", "127.0.0.1:6380", 2);
    promoted.role = InstanceRole::Master;
    state.registry.insert_instance(promoted).unwrap();
    master.failover_state = FailoverState::WaitPromotion;
    assert_eq!(current_primary_addr(&state, &master), "127.0.0.1:6379");
    master.failover_state = FailoverState::ReconfigureReplicas;
    assert_eq!(current_primary_addr(&state, &master), "127.0.0.1:6380");
}

#[test]
fn sentinel_announce_override_formats_ipv6_endpoint() {
    let mut state = SentinelState::new();
    state.announce_hostnames = true;
    state.announce_addr = "127.0.0.1:26379".to_string();
    state.announce_ip = "::1".to_string();

    assert_eq!(sentinel_announce_addr(&state), "[::1]:26379");
}

#[test]
fn vote_request_uses_bare_ipv6_host_and_separate_port() {
    let mut manager = SentinelLinkManager::new();
    let token = Token(42);
    manager.register_for_test("127.0.0.1:26380", SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);
    let mut ctx = crate::framework::context::ServerContext::new(1);
    let mut master = SentinelInstance::new_master(1, "primary", "[::1]:6379", 1);
    master.down_state = DownState::SDown;
    master.sentinels.push(2);
    ctx.sentinel_state_mut()
        .registry
        .insert_master("primary".to_string(), master)
        .unwrap();
    let mut peer = SentinelInstance::new_master(2, "peer", "127.0.0.1:26380", 1);
    peer.role = InstanceRole::Sentinel;
    ctx.sentinel_state_mut()
        .registry
        .insert_instance(peer)
        .unwrap();

    manager.queue_vote_requests(&mut ctx, 2_000);

    let output = manager
        .output_for_test(token, 0)
        .expect("vote request output");
    assert!(output
        .windows(b"$3\r\n::1\r\n".len())
        .any(|window| { window == b"$3\r\n::1\r\n" }));
    assert!(!output
        .windows(b"[::1]".len())
        .any(|window| window == b"[::1]"));
}

#[test]
fn post_failover_hello_uses_ping_period_temporarily() {
    let mut manager = SentinelLinkManager::new();
    let token = Token(42);
    manager.register_for_test("127.0.0.1:26380", SentinelLinkKind::Command, token);
    manager.set_connected_for_test(token, true);

    let mut ctx = crate::framework::context::ServerContext::new(1);
    {
        let state = ctx.sentinel_state_mut();
        state.sentinel_mode = true;
        state.initialized = true;
        state.announce_addr = "127.0.0.1:26379".to_string();
        state.tunables.ping_period_ms = 500;
        state.tunables.publish_period_ms = 10_000;
        let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
        master.config_epoch = 2;
        master.failover_state_change_time = 10_000;
        master.last_hello_sent = 10_000;
        master.sentinels.push(2);
        state
            .registry
            .insert_master("primary".to_string(), master)
            .unwrap();
        let mut peer = SentinelInstance::new_master(2, "peer", "127.0.0.1:26380", 1);
        peer.role = InstanceRole::Sentinel;
        state.registry.insert_instance(peer).unwrap();
    }

    let master = ctx
        .sentinel_state()
        .registry
        .master("primary")
        .unwrap()
        .clone();
    assert!(!manager.queue_master_hello(&mut ctx, &master, 10_499));
    assert!(manager.queue_master_hello(&mut ctx, &master, 10_500));
    assert!(matches!(
        manager.pending_request_for_test(token, 0),
        Some(super::SentinelRequest::PublishHello)
    ));
    ctx.sentinel_state_mut()
        .registry
        .master_mut("primary")
        .unwrap()
        .last_hello_sent = 15_500;
    let master = ctx
        .sentinel_state()
        .registry
        .master("primary")
        .unwrap()
        .clone();
    assert!(!manager.queue_master_hello(&mut ctx, &master, 15_999));
    assert!(manager.queue_master_hello(&mut ctx, &master, 16_000));
    ctx.sentinel_state_mut()
        .registry
        .master_mut("primary")
        .unwrap()
        .last_hello_sent = 39_000;
    let master = ctx
        .sentinel_state()
        .registry
        .master("primary")
        .unwrap()
        .clone();
    assert!(!manager.queue_master_hello(&mut ctx, &master, 40_001));
    assert!(manager.queue_master_hello(&mut ctx, &master, 49_000));
}

#[test]
fn connect_shared_increments_refcount() {
    let mut manager = SentinelLinkManager::new();
    let mut driver = TestSentinelIo::new();
    let addr = "127.0.0.1:9999";
    let token = manager
        .connect(&mut driver, addr, SentinelLinkKind::Command, 1_000)
        .expect("first connect should succeed");
    assert_eq!(manager.refcount_for_test(token), Some(1));
    let token2 = manager
        .connect(&mut driver, addr, SentinelLinkKind::Command, 1_000)
        .expect("second connect should succeed");
    assert_eq!(token, token2);
    assert_eq!(manager.refcount_for_test(token), Some(2));
}

#[test]
fn periodic_connection_ensure_does_not_increment_refcount() {
    let mut manager = SentinelLinkManager::new();
    let mut driver = TestSentinelIo::new();
    let addr = "127.0.0.1:9997";
    let token = manager
        .ensure_connection(&mut driver, addr, SentinelLinkKind::Command, 1_000)
        .expect("first ensure should connect");
    let same = manager
        .ensure_connection(&mut driver, addr, SentinelLinkKind::Command, 1_100)
        .expect("second ensure should reuse");
    assert_eq!(same, token);
    assert_eq!(manager.refcount_for_test(token), Some(1));
}

#[test]
fn remove_shared_decrements_refcount_and_closes_on_last() {
    let mut manager = SentinelLinkManager::new();
    let mut driver = TestSentinelIo::new();
    let addr = "127.0.0.1:9998";
    let token = manager
        .connect(&mut driver, addr, SentinelLinkKind::Command, 1_000)
        .expect("first connect");
    manager
        .connect(&mut driver, addr, SentinelLinkKind::Command, 1_000)
        .expect("second connect");
    assert_eq!(manager.refcount_for_test(token), Some(2));
    manager.remove(&mut driver, token, 2_000).unwrap();
    assert!(manager.is_link_token(token));
    assert_eq!(manager.refcount_for_test(token), Some(1));
    manager.remove(&mut driver, token, 3_000).unwrap();
    assert!(!manager.is_link_token(token));
}

#[test]
fn failed_remove_retains_sentinel_link_for_retry() {
    let mut manager = SentinelLinkManager::new();
    let mut driver = TestSentinelIo::new();
    let mut ctx = crate::framework::context::ServerContext::new(1);
    ctx.sentinel_state_mut().sentinel_mode = true;
    ctx.sentinel_state_mut().initialized = true;
    let token = manager
        .connect(
            &mut driver,
            "127.0.0.1:9998",
            SentinelLinkKind::Command,
            1_000,
        )
        .expect("connect");
    driver.fail_next_remove();

    assert_eq!(
        manager.remove(&mut driver, token, 2_000),
        Err("deregister failed".to_string())
    );
    assert!(manager.is_link_token(token));
    assert_eq!(manager.link_count_for_test(), 1);

    manager.cron(&mut ctx, &mut driver, 3_000);
    assert!(!manager.is_link_token(token));
}
