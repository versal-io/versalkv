use super::endpoint::{
    auth_for_addr, auth_for_instance, current_primary_addr, hello_sentinel_targets, hello_targets,
    queue_link_auth, sentinel_announce_addr, sentinel_command_client_name,
};
use super::proto::*;
use crate::engine::command::context::Token;
use crate::engine::distributed::sentinel::hello;
use crate::engine::distributed::sentinel::{
    split_sentinel_endpoint, DownState, FailoverState, InstanceRole, SentinelInstance,
    SentinelInstanceId, SentinelState,
};
use crate::framework::context::ServerContext;
use crate::framework::distributed::sentinel::io::{IoInterest, SentinelIo};
use std::collections::HashMap;

const SENTINEL_LINK_RETRY_MS: i64 = 1_000;
const POST_FAILOVER_HELLO_BURST_MS: i64 = 30_000;

fn info_period_for_instance(ctx: &ServerContext, id: SentinelInstanceId) -> i64 {
    let state = ctx.sentinel_state();
    let is_replica_of_failing_master = state
        .registry
        .instances()
        .get(&id)
        .is_some_and(|instance| instance.role == InstanceRole::Replica)
        && state
            .registry
            .masters()
            .map(|(_, master)| master)
            .any(|master| {
                master.replicas.contains(&id)
                    && (master.down_state == DownState::ODown
                        || master.failover_state != FailoverState::None)
            });
    if is_replica_of_failing_master {
        state.tunables.ping_period_ms
    } else {
        state.tunables.info_period_ms
    }
}

#[derive(Clone, Copy)]
struct VoteParams<'a> {
    host: &'a str,
    port: &'a str,
    epoch: &'a str,
    leader: &'a str,
    now: i64,
    ask_period: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SentinelReadState {
    Quiescent,
    BudgetExhausted,
    Closed,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SentinelReadOutcome {
    pub(crate) transport: SentinelReadState,
    pub(crate) has_buffered_frames: bool,
}

impl SentinelReadOutcome {
    #[cfg(test)]
    pub(crate) fn needs_continuation(self) -> bool {
        self.transport == SentinelReadState::BudgetExhausted || self.has_buffered_frames
    }
}

impl Default for SentinelReadOutcome {
    fn default() -> Self {
        Self {
            transport: SentinelReadState::Quiescent,
            has_buffered_frames: false,
        }
    }
}

pub struct SentinelLinkManager {
    pub(super) links: HashMap<usize, SentinelLink>,
    pub(super) by_endpoint: HashMap<(String, SentinelLinkKind), usize>,
    pub(super) retry_after: HashMap<(String, SentinelLinkKind), i64>,
}

impl SentinelLinkManager {
    pub fn connect(
        &mut self,
        driver: &mut impl SentinelIo,
        addr: &str,
        kind: SentinelLinkKind,
        now: i64,
    ) -> Option<Token> {
        let key = (addr.to_string(), kind);
        // Check if a connection for this (addr, kind) already exists
        if let Some(token) = self.by_endpoint.get(&key) {
            if let Some(link) = self.links.get_mut(token) {
                if link.close_pending {
                    return None;
                }
                link.refcount += 1;
                return Some(Token(*token));
            }
        }
        if self
            .retry_after
            .get(&key)
            .map(|retry_after| now < *retry_after)
            .unwrap_or(false)
        {
            return None;
        }
        let token = match driver.connect_tcp(addr) {
            Ok(token) => token,
            Err(_) => {
                self.retry_after.insert(key, now + SENTINEL_LINK_RETRY_MS);
                return None;
            }
        };
        self.by_endpoint.insert(key.clone(), token.0);
        self.retry_after.remove(&key);
        self.links.insert(
            token.0,
            SentinelLink {
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
        Some(token)
    }

    pub(super) fn ensure_connection(
        &mut self,
        driver: &mut impl SentinelIo,
        addr: &str,
        kind: SentinelLinkKind,
        now: i64,
    ) -> Option<Token> {
        let key = (addr.to_string(), kind);
        if let Some(token) = self.by_endpoint.get(&key).copied() {
            return self
                .links
                .get(&token)
                .filter(|link| !link.close_pending)
                .map(|_| Token(token));
        }
        self.connect(driver, addr, kind, now)
    }

    pub fn remove(
        &mut self,
        driver: &mut impl SentinelIo,
        token: Token,
        now: i64,
    ) -> Result<(), String> {
        // Decrement refcount; only close when refcount reaches 0
        if let Some(link) = self.links.get_mut(&token.0) {
            if link.refcount > 1 {
                link.refcount -= 1;
                return Ok(());
            }
            if link.close_pending {
                return Ok(());
            }
        }
        self.remove_link(driver, token, now)
    }

    pub(super) fn retry_pending_removals(&mut self, driver: &mut impl SentinelIo, now: i64) {
        let tokens: Vec<Token> = self
            .links
            .values()
            .filter(|link| link.close_pending)
            .map(|link| link.token)
            .collect();
        for token in tokens {
            if let Err(error) = self.remove_link(driver, token, now) {
                crate::server_log!(
                    warning,
                    "SENTINEL_CRON: retaining link {} after deregistration retry failure: {}",
                    token.0,
                    error
                );
            }
        }
    }

    fn remove_link(
        &mut self,
        driver: &mut impl SentinelIo,
        token: Token,
        now: i64,
    ) -> Result<(), String> {
        let Some(link) = self.links.get(&token.0) else {
            return Ok(());
        };
        let key = (link.addr.clone(), link.kind);
        if let Some(link) = self.links.get_mut(&token.0) {
            link.close_pending = true;
        }
        driver.remove(token)?;
        self.links.remove(&token.0);
        self.by_endpoint.remove(&key);
        self.retry_after.insert(key, now + SENTINEL_LINK_RETRY_MS);
        Ok(())
    }

    pub(super) fn remove_by_addr(
        &mut self,
        driver: &mut impl SentinelIo,
        addr: &str,
        now: i64,
    ) -> Result<(), String> {
        let mut removal_error = None;
        for kind in [
            SentinelLinkKind::Command,
            SentinelLinkKind::HelloSubscription,
        ] {
            let key = (addr.to_string(), kind);
            if let Some(token) = self.by_endpoint.get(&key).copied() {
                if self
                    .links
                    .get(&token)
                    .is_some_and(|link| link.close_pending)
                {
                    continue;
                }
                if let Err(error) = self.remove_link(driver, Token(token), now) {
                    removal_error.get_or_insert(error);
                }
            }
        }
        removal_error.map_or(Ok(()), Err)
    }

    pub(super) fn reset_by_addr(
        &mut self,
        driver: &mut impl SentinelIo,
        addr: &str,
        now: i64,
    ) -> Result<(), String> {
        self.remove_by_addr(driver, addr, now)?;
        for kind in [
            SentinelLinkKind::Command,
            SentinelLinkKind::HelloSubscription,
        ] {
            self.retry_after.remove(&(addr.to_string(), kind));
        }
        Ok(())
    }

    pub fn writable(
        &mut self,
        ctx: &ServerContext,
        driver: &mut impl SentinelIo,
        token: Token,
        now: i64,
    ) {
        let Some(connected) = self
            .links
            .get(&token.0)
            .filter(|link| !link.close_pending)
            .map(|link| link.connected)
        else {
            return;
        };
        if !connected && !self.finish_connect(ctx, driver, token) {
            if let Err(error) = self.remove(driver, token, now) {
                crate::server_log!(
                    warning,
                    "SENTINEL: retaining link {} after deregistration failure: {}",
                    token.0,
                    error
                );
            }
            return;
        }
        crate::server_log!(debug, "WRITABLE: token={} connected={}", token.0, connected);
        if !self.flush_output(driver, token) {
            if let Err(error) = self.remove(driver, token, now) {
                crate::server_log!(
                    warning,
                    "SENTINEL: retaining link {} after deregistration failure: {}",
                    token.0,
                    error
                );
            }
        }
    }

    fn finish_connect(
        &mut self,
        ctx: &ServerContext,
        driver: &mut impl SentinelIo,
        token: Token,
    ) -> bool {
        if !matches!(driver.take_connect_error(token), Ok(None)) {
            return false;
        }
        let auth = self
            .links
            .get(&token.0)
            .map(|link| auth_for_addr(ctx.sentinel_state(), &link.addr))
            .unwrap_or_default();
        let client_name = sentinel_command_client_name(ctx.sentinel_state());
        if let Some(link) = self.links.get_mut(&token.0) {
            link.connected = true;
            if !auth.0.is_empty() || !auth.1.is_empty() {
                let auth_args = if auth.0.is_empty() {
                    vec!["AUTH", auth.1.as_str()]
                } else if auth.1.is_empty() {
                    vec!["AUTH", auth.0.as_str(), ""]
                } else {
                    vec!["AUTH", auth.0.as_str(), auth.1.as_str()]
                };
                queue_request(link, SentinelRequest::Auth, &auth_args);
            }
            if link.kind == SentinelLinkKind::HelloSubscription {
                queue_request(
                    link,
                    SentinelRequest::SubscribeHello,
                    &["SUBSCRIBE", hello::SENTINEL_HELLO_CHANNEL],
                );
            } else {
                queue_request(
                    link,
                    SentinelRequest::SetName,
                    &["CLIENT", "SETNAME", client_name.as_str()],
                );
            }
        }
        true
    }

    fn flush_output(&mut self, driver: &mut impl SentinelIo, token: Token) -> bool {
        let mut succeeded = true;
        if let Some(link) = self.links.get_mut(&token.0) {
            while let Some(frame) = link.output.first_mut() {
                match driver.write_stream(token, frame) {
                    Ok(0) => {
                        succeeded = false;
                        break;
                    }
                    Ok(n) if n >= frame.len() => {
                        link.output.remove(0);
                    }
                    Ok(n) => {
                        frame.drain(..n);
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => {
                        succeeded = false;
                        break;
                    }
                }
            }
            let interest = if link.output.is_empty() {
                IoInterest::Readable
            } else {
                IoInterest::ReadWritable
            };
            let _ = driver.set_interest(token, interest);
        }
        succeeded
    }

    pub fn readable(
        &mut self,
        ctx: &mut ServerContext,
        driver: &mut impl SentinelIo,
        token: Token,
        now: i64,
    ) -> SentinelReadOutcome {
        if self
            .links
            .get(&token.0)
            .map(|link| link.close_pending)
            .unwrap_or(true)
        {
            return SentinelReadOutcome::default();
        }
        let read_state = self.read_link_input(driver, token, Some(64 * 1024));
        if matches!(
            read_state,
            SentinelReadState::Closed | SentinelReadState::Failed
        ) {
            self.remove_after_read(driver, token, now);
            return SentinelReadOutcome {
                transport: read_state,
                has_buffered_frames: false,
            };
        }
        let has_buffered_frames = self.process_input_frames(ctx, token, now, 256);
        SentinelReadOutcome {
            transport: read_state,
            has_buffered_frames,
        }
    }

    pub fn readable_after_hup(
        &mut self,
        ctx: &mut ServerContext,
        driver: &mut impl SentinelIo,
        token: Token,
        now: i64,
    ) {
        if self
            .links
            .get(&token.0)
            .map(|link| link.close_pending)
            .unwrap_or(true)
        {
            return;
        }
        let state = self.read_link_input(driver, token, None);
        self.process_input_frames(ctx, token, now, usize::MAX);
        if matches!(state, SentinelReadState::Closed | SentinelReadState::Failed) {
            self.remove_after_read(driver, token, now);
        }
    }

    fn read_link_input(
        &mut self,
        driver: &mut impl SentinelIo,
        token: Token,
        limit: Option<usize>,
    ) -> SentinelReadState {
        let mut buf = [0u8; 4096];
        let mut bytes_read = 0usize;
        while limit.is_none_or(|limit| bytes_read < limit) {
            match crate::foundation::io::sys::os::retry_on_interrupt(|| {
                driver.read_stream(token, &mut buf)
            }) {
                Ok(0) => return SentinelReadState::Closed,
                Ok(n) => {
                    bytes_read += n;
                    if let Some(link) = self.links.get_mut(&token.0) {
                        link.input.extend_from_slice(&buf[..n]);
                    }
                }
                Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return SentinelReadState::Quiescent;
                }
                Err(_) => return SentinelReadState::Failed,
            }
        }
        SentinelReadState::BudgetExhausted
    }

    fn process_input_frames(
        &mut self,
        ctx: &mut ServerContext,
        token: Token,
        now: i64,
        limit: usize,
    ) -> bool {
        let mut processed = 0;
        for _ in 0..limit {
            let Some(frame) = self
                .links
                .get_mut(&token.0)
                .and_then(|link| RespFrame::take(&mut link.input))
            else {
                break;
            };
            self.handle_frame(ctx, token, frame, now);
            processed += 1;
        }
        processed == limit
            && self
                .links
                .get(&token.0)
                .is_some_and(|link| RespFrame::is_complete(&link.input))
    }

    fn remove_after_read(&mut self, driver: &mut impl SentinelIo, token: Token, now: i64) {
        if let Err(error) = self.remove(driver, token, now) {
            crate::server_log!(
                warning,
                "SENTINEL: retaining link {} after deregistration failure: {}",
                token.0,
                error
            );
        }
    }

    pub(super) fn maybe_queue_ping(
        &mut self,
        ctx: &mut ServerContext,
        id: SentinelInstanceId,
        addr: &str,
        now: i64,
    ) {
        let ping_period = ctx.sentinel_state().tunables.ping_period_ms;
        let due = ctx
            .sentinel_state_mut()
            .registry
            .instances()
            .get(&id)
            .map(|instance| now - instance.last_ping_sent >= ping_period)
            .unwrap_or(false);
        let has_pending_ping = self
            .by_endpoint
            .get(&(addr.to_string(), SentinelLinkKind::Command))
            .and_then(|token| self.links.get(token))
            .is_some_and(|link| {
                link.pending
                    .iter()
                    .any(|request| matches!(request, SentinelRequest::Ping(pending_id) if *pending_id == id))
            });
        if !due || has_pending_ping {
            return;
        }
        let (auth_user, auth_pass) = auth_for_instance(ctx.sentinel_state(), id);
        if self.queue_command(
            addr,
            SentinelRequest::Ping(id),
            &["PING"],
            ctx.sentinel_state().registry.instances(),
            &auth_user,
            &auth_pass,
        ) {
            if let Some(instance) = ctx.sentinel_state_mut().registry.instance_mut(id) {
                instance.last_ping_sent = now;
                if instance.act_ping_time == 0 {
                    instance.act_ping_time = now;
                }
            }
        }
    }

    pub(super) fn maybe_queue_info(
        &mut self,
        ctx: &mut ServerContext,
        id: SentinelInstanceId,
        addr: &str,
        now: i64,
    ) {
        let info_period = info_period_for_instance(ctx, id);
        let due = ctx
            .sentinel_state_mut()
            .registry
            .instances()
            .get(&id)
            .map(|instance| now - instance.last_info_sent >= info_period)
            .unwrap_or(false);
        if !due {
            return;
        }
        let (auth_user, auth_pass) = auth_for_instance(ctx.sentinel_state(), id);
        if self.queue_command(
            addr,
            SentinelRequest::Info(id),
            &["INFO"],
            ctx.sentinel_state().registry.instances(),
            &auth_user,
            &auth_pass,
        ) {
            if let Some(instance) = ctx.sentinel_state_mut().registry.instance_mut(id) {
                instance.last_info_sent = now;
            }
        }
    }

    pub(super) fn queue_hello_publishes(&mut self, ctx: &mut ServerContext, now: i64) {
        if ctx.sentinel_state_mut().announce_addr.is_empty() {
            return;
        }
        let masters: Vec<_> = ctx
            .sentinel_state_mut()
            .registry
            .masters()
            .map(|(_, master)| master)
            .cloned()
            .collect();
        for master in masters {
            if self.queue_master_hello(ctx, &master, now) {
                ctx.sentinel_state_mut()
                    .registry
                    .master_mut(&master.name)
                    .unwrap()
                    .last_hello_sent = now;
            }
        }
    }

    fn queue_master_hello(
        &mut self,
        ctx: &mut ServerContext,
        master: &SentinelInstance,
        now: i64,
    ) -> bool {
        let publish_period = if master.config_epoch > 0
            && master.failover_state_change_time > 0
            && now - master.failover_state_change_time <= POST_FAILOVER_HELLO_BURST_MS
        {
            ctx.sentinel_state().tunables.ping_period_ms
        } else {
            ctx.sentinel_state().tunables.publish_period_ms
        };
        if now - master.last_hello_sent < publish_period {
            return false;
        }
        let state = ctx.sentinel_state();
        let payload = hello::HelloMessage {
            sentinel_addr: sentinel_announce_addr(state),
            runid: state.myid.clone(),
            current_epoch: state.current_epoch,
            master_name: master.name.clone(),
            master_addr: current_primary_addr(state, master),
            master_config_epoch: master.config_epoch,
        }
        .encode(state.announce_hostnames);
        let targets = hello_targets(state, master);
        let sentinel_targets = hello_sentinel_targets(state, master);
        let mut queued_sentinels = 0;
        for addr in targets {
            let (user, pass) = auth_for_addr(state, &addr);
            let queued = self.queue_command(
                &addr,
                SentinelRequest::PublishHello,
                &["PUBLISH", hello::SENTINEL_HELLO_CHANNEL, &payload],
                state.registry.instances(),
                &user,
                &pass,
            );
            queued_sentinels +=
                usize::from(queued && sentinel_targets.iter().any(|target| target == &addr));
        }
        sentinel_targets.is_empty() || queued_sentinels == sentinel_targets.len()
    }

    pub(super) fn queue_vote_requests(&mut self, ctx: &mut ServerContext, now: i64) {
        let ask_period = ctx.sentinel_state().tunables.ask_period_ms;
        let masters: Vec<_> = ctx
            .sentinel_state_mut()
            .registry
            .masters()
            .map(|(_, master)| master)
            .cloned()
            .collect();
        let state = ctx.sentinel_state_mut();
        for master in masters {
            self.queue_master_votes(state, &master, now, ask_period);
        }
    }

    fn queue_master_votes(
        &mut self,
        state: &SentinelState,
        master: &SentinelInstance,
        now: i64,
        ask_period: i64,
    ) {
        if master.down_state == DownState::Up && master.failover_state == FailoverState::None {
            return;
        }
        let Some(endpoint) = split_sentinel_endpoint(&master.addr) else {
            return;
        };
        let leader = if master.failover_state == FailoverState::None {
            "*"
        } else {
            &state.myid
        };
        let epoch = state.current_epoch.to_string();
        let port = endpoint.port.to_string();
        let params = VoteParams {
            host: endpoint.host,
            port: &port,
            epoch: &epoch,
            leader,
            now,
            ask_period,
        };
        for peer_id in &master.sentinels {
            self.queue_peer_vote(state, master, *peer_id, params);
        }
    }

    fn queue_peer_vote(
        &mut self,
        state: &SentinelState,
        master: &SentinelInstance,
        peer_id: SentinelInstanceId,
        params: VoteParams<'_>,
    ) {
        let Some(peer) = state.registry.instances().get(&peer_id) else {
            return;
        };
        let last_reply = master
            .peer_states
            .get(&peer_id)
            .map(|peer_state| peer_state.last_master_down_reply)
            .unwrap_or(0);
        if self.pending_vote_exists(&peer.addr, master.id, peer_id)
            || params.now - last_reply < params.ask_period
        {
            return;
        }
        self.queue_command(
            &peer.addr,
            SentinelRequest::Vote(master.id, peer_id),
            &[
                "SENTINEL",
                "IS-MASTER-DOWN-BY-ADDR",
                params.host,
                params.port,
                params.epoch,
                params.leader,
            ],
            state.registry.instances(),
            &state.sentinel_auth_user,
            &state.sentinel_auth_pass,
        );
    }

    fn pending_vote_exists(
        &self,
        addr: &str,
        master_id: SentinelInstanceId,
        peer_id: SentinelInstanceId,
    ) -> bool {
        self.by_endpoint
            .get(&(addr.to_string(), SentinelLinkKind::Command))
            .and_then(|token| self.links.get(token))
            .is_some_and(|link| {
                link.pending.iter().any(|request| {
                    matches!(
                        request,
                        SentinelRequest::Vote(pending_master, pending_peer)
                            if *pending_master == master_id && *pending_peer == peer_id
                    )
                })
            })
    }

    /// Look up renamed command for a given instance address.
    /// Returns `Some(renamed)` if a rename mapping exists, `None` otherwise.
    fn get_renamed_command(
        &self,
        instances: &HashMap<SentinelInstanceId, SentinelInstance>,
        addr: &str,
        cmd: &str,
    ) -> Option<String> {
        for inst in instances.values() {
            if inst.addr == addr {
                return inst.renamed_commands.get(cmd).cloned();
            }
        }
        None
    }

    fn queue_command(
        &mut self,
        addr: &str,
        request: SentinelRequest,
        args: &[&str],
        instances: &HashMap<SentinelInstanceId, SentinelInstance>,
        sentinel_auth_user: &str,
        sentinel_auth_pass: &str,
    ) -> bool {
        let token = match self
            .by_endpoint
            .get(&(addr.to_string(), SentinelLinkKind::Command))
            .copied()
        {
            Some(token) => token,
            None => return false,
        };
        let is_ready = self.links.get(&token).is_some_and(|link| {
            !link.close_pending
                && link.connected
                && ((sentinel_auth_user.is_empty() && sentinel_auth_pass.is_empty())
                    || link.authenticated)
        });
        if !is_ready {
            return false;
        }
        let renamed_args: Option<Vec<String>> = self
            .get_renamed_command(instances, addr, args[0])
            .map(|renamed| {
                let mut renamed_args = Vec::with_capacity(args.len());
                renamed_args.push(renamed);
                renamed_args.extend(args[1..].iter().map(|s| s.to_string()));
                renamed_args
            });
        if let Some(link) = self.links.get_mut(&token) {
            queue_link_auth(link, sentinel_auth_user, sentinel_auth_pass);
            queue_link_command(link, request, args, renamed_args.as_deref());
            return true;
        }
        false
    }
}

fn queue_link_command(
    link: &mut SentinelLink,
    request: SentinelRequest,
    args: &[&str],
    renamed_args: Option<&[String]>,
) {
    if let Some(renamed_args) = renamed_args {
        let refs: Vec<&str> = renamed_args.iter().map(String::as_str).collect();
        queue_request(link, request, &refs);
    } else {
        queue_request(link, request, args);
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

#[cfg(test)]
#[path = "removal_tests.rs"]
mod removal_tests;
