use super::SentinelLinkKind;
use super::SentinelLinkManager;
use crate::engine::command::context::Token;
use crate::framework::distributed::sentinel::io::{IoInterest, SentinelIo};

struct TestSentinelIo {
    next_token: usize,
    remove_error: Option<String>,
    remove_attempts: usize,
    interest_attempts: usize,
    read_attempts: usize,
    write_attempts: usize,
}

impl TestSentinelIo {
    fn new() -> Self {
        Self {
            next_token: 1,
            remove_error: None,
            remove_attempts: 0,
            interest_attempts: 0,
            read_attempts: 0,
            write_attempts: 0,
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
        self.interest_attempts += 1;
        Ok(())
    }

    fn remove(&mut self, _token: Token) -> Result<(), String> {
        self.remove_attempts += 1;
        match self.remove_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn take_connect_error(&mut self, _token: Token) -> Result<Option<String>, String> {
        Ok(None)
    }

    fn read_stream(&mut self, _token: Token, _buf: &mut [u8]) -> std::io::Result<usize> {
        self.read_attempts += 1;
        Ok(0)
    }

    fn write_stream(&mut self, _token: Token, _buf: &[u8]) -> std::io::Result<usize> {
        self.write_attempts += 1;
        Ok(0)
    }
}

#[test]
fn failed_sentinel_remove_retries_on_cron_tick() {
    let mut manager = SentinelLinkManager::new();
    let mut driver = TestSentinelIo::new();
    let mut ctx = crate::framework::context::ServerContext::new(1);
    ctx.sentinel_state_mut().sentinel_mode = true;
    ctx.sentinel_state_mut().initialized = true;
    let token = manager
        .connect(
            &mut driver,
            "127.0.0.1:9997",
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

    manager.remove(&mut driver, token, 2_500).unwrap();
    assert_eq!(driver.remove_attempts, 1);

    manager.cron(&mut ctx, &mut driver, 3_000);

    assert!(!manager.is_link_token(token));
    assert_eq!(driver.remove_attempts, 2);
}

#[test]
fn stale_address_removal_marks_every_link_when_one_deregistration_fails() {
    let mut manager = SentinelLinkManager::new();
    let mut driver = TestSentinelIo::new();
    let mut ctx = crate::framework::context::ServerContext::new(1);
    let command = manager
        .connect(
            &mut driver,
            "127.0.0.1:9996",
            SentinelLinkKind::Command,
            1_000,
        )
        .expect("connect command");
    let subscription = manager
        .connect(
            &mut driver,
            "127.0.0.1:9996",
            SentinelLinkKind::HelloSubscription,
            1_000,
        )
        .expect("connect subscription");
    ctx.sentinel_state_mut()
        .pending_link_close
        .push("127.0.0.1:9996".to_string());
    driver.fail_next_remove();

    manager.close_stale_links(&mut ctx, &mut driver, 2_000);

    assert!(manager.is_link_token(command));
    assert!(!manager.is_link_token(subscription));
}

#[test]
fn close_pending_link_is_inert_while_cron_removal_retries_fail() {
    let mut manager = SentinelLinkManager::new();
    let mut driver = TestSentinelIo::new();
    let addr = "127.0.0.1:9995";
    let token = manager
        .connect(&mut driver, addr, SentinelLinkKind::Command, 1_000)
        .expect("connect");
    let mut ctx = crate::framework::context::ServerContext::new(1);
    {
        let state = ctx.sentinel_state_mut();
        state.sentinel_mode = true;
        state.initialized = true;
        state.tunables.ping_period_ms = 100;
        state.tunables.info_period_ms = 100;
        let mut instance = crate::engine::distributed::sentinel::SentinelInstance::new_master(
            1, "primary", addr, 1,
        );
        instance.link_state = 1;
        instance.link_down_since_time = 500;
        instance.last_ping_sent = 1_000;
        instance.last_info_sent = 1_000;
        instance.act_ping_time = 500;
        state.registry.insert_instance(instance).unwrap();
    }
    {
        let link = manager.links.get_mut(&token.0).expect("link");
        link.connected = true;
        link.output.push(b"queued before close".to_vec());
    }

    driver.fail_next_remove();
    assert_eq!(
        manager.remove(&mut driver, token, 1_500),
        Err("deregister failed".to_string())
    );
    assert!(manager.links[&token.0].close_pending);

    manager.readable(&mut ctx, &mut driver, token, 1_600);
    manager.writable(&ctx, &mut driver, token, 1_600);
    assert_eq!(driver.read_attempts, 0);
    assert_eq!(driver.write_attempts, 0);
    assert_eq!(driver.interest_attempts, 0);

    driver.fail_next_remove();
    manager.cron(&mut ctx, &mut driver, 2_000);
    assert_eq!(driver.remove_attempts, 2);
    assert!(manager.is_link_token(token));
    assert_eq!(
        manager.by_endpoint[&(addr.to_string(), SentinelLinkKind::Command)],
        token.0
    );
    assert_eq!(manager.links[&token.0].output.len(), 1);
    assert_eq!(driver.interest_attempts, 0);
    assert_eq!(ctx.sentinel_state().registry.instances()[&1].link_state, 1);
    assert_eq!(
        ctx.sentinel_state().registry.instances()[&1].link_down_since_time,
        500
    );
    assert_eq!(
        ctx.sentinel_state().registry.instances()[&1].last_ping_sent,
        1_000
    );
    assert_eq!(
        ctx.sentinel_state().registry.instances()[&1].last_info_sent,
        1_000
    );
    assert_eq!(
        ctx.sentinel_state().registry.instances()[&1].act_ping_time,
        500
    );

    driver.fail_next_remove();
    manager.cron(&mut ctx, &mut driver, 3_000);
    assert_eq!(driver.remove_attempts, 3);
    assert!(manager.is_link_token(token));
    assert_eq!(manager.links[&token.0].output.len(), 1);
    assert_eq!(driver.interest_attempts, 0);
    assert_eq!(ctx.sentinel_state().registry.instances()[&1].link_state, 1);
    assert_eq!(
        ctx.sentinel_state().registry.instances()[&1].last_ping_sent,
        1_000
    );
    assert_eq!(
        ctx.sentinel_state().registry.instances()[&1].last_info_sent,
        1_000
    );
}

#[test]
fn failed_stale_close_clears_ping_deadline_before_successful_retry() {
    let mut manager = SentinelLinkManager::new();
    let mut driver = TestSentinelIo::new();
    let addr = "127.0.0.1:9994";
    manager
        .connect(&mut driver, addr, SentinelLinkKind::Command, 1_000)
        .expect("connect");
    let mut ctx = crate::framework::context::ServerContext::new(1);
    {
        let state = ctx.sentinel_state_mut();
        state.sentinel_mode = true;
        state.initialized = true;
        let mut instance = crate::engine::distributed::sentinel::SentinelInstance::new_master(
            1, "primary", addr, 1,
        );
        instance.down_after_ms = 1_000;
        instance.act_ping_time = 500;
        instance.link_down_since_time = 1_800;
        state
            .registry
            .insert_master(instance.name.clone(), instance)
            .unwrap();
        state.pending_link_close.push(addr.to_string());
    }

    driver.fail_next_remove();
    manager.cron(&mut ctx, &mut driver, 1_900);

    let instance = &ctx.sentinel_state().registry.instances()[&1];
    assert_eq!(instance.act_ping_time, 0);
    assert_eq!(instance.link_down_since_time, 1_800);
    crate::framework::distributed::sentinel::timer::check_sdown(&mut ctx, 1, 2_001);
    assert_eq!(
        ctx.sentinel_state().registry.instances()[&1].down_state,
        crate::engine::distributed::sentinel::DownState::Up
    );
    assert_eq!(
        ctx.sentinel_state()
            .registry
            .master("primary")
            .unwrap()
            .down_state,
        crate::engine::distributed::sentinel::DownState::Up
    );
    assert_eq!(
        ctx.sentinel_state()
            .registry
            .master("primary")
            .unwrap()
            .failover_state,
        crate::engine::distributed::sentinel::FailoverState::None
    );

    manager.cron(&mut ctx, &mut driver, 2_100);

    assert_eq!(driver.remove_attempts, 2);
    assert_eq!(
        ctx.sentinel_state().registry.instances()[&1].act_ping_time,
        0
    );
    assert_eq!(
        ctx.sentinel_state().registry.instances()[&1].link_down_since_time,
        1_800
    );
    crate::framework::distributed::sentinel::timer::check_sdown(&mut ctx, 1, 2_500);
    assert_eq!(
        ctx.sentinel_state().registry.instances()[&1].down_state,
        crate::engine::distributed::sentinel::DownState::Up
    );
    assert_eq!(
        ctx.sentinel_state()
            .registry
            .master("primary")
            .unwrap()
            .failover_state,
        crate::engine::distributed::sentinel::FailoverState::None
    );
}
