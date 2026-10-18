use super::proto::SentinelLinkKind;
use super::SentinelLinkManager;
use crate::engine::distributed::sentinel::{InstanceRole, SentinelInstanceId};
use crate::framework::context::ServerContext;
use crate::framework::distributed::sentinel::io::{IoInterest, SentinelIo};

impl SentinelLinkManager {
    pub fn cron(&mut self, ctx: &mut ServerContext, driver: &mut impl SentinelIo, now: i64) {
        if !ctx.sentinel_state_mut().sentinel_mode || !ctx.sentinel_state_mut().initialized {
            return;
        }
        self.retry_pending_removals(driver, now);
        self.close_stale_links(ctx, driver, now);
        self.rebuild_links_if_needed(ctx, driver, now);
        let targets: Vec<(SentinelInstanceId, String, InstanceRole)> = ctx
            .sentinel_state()
            .registry
            .instances()
            .values()
            .map(|instance| (instance.id, instance.addr.clone(), instance.role))
            .collect();
        for (id, addr, role) in targets {
            self.poll_target(ctx, driver, id, &addr, role, now);
        }
        self.queue_hello_publishes(ctx, now);
        self.queue_vote_requests(ctx, now);
        self.ensure_write_interest(driver);
    }

    pub(super) fn close_stale_links(
        &mut self,
        ctx: &mut ServerContext,
        driver: &mut impl SentinelIo,
        now: i64,
    ) {
        let pending_close = std::mem::take(&mut ctx.sentinel_state_mut().pending_link_close);
        for addr in &pending_close {
            crate::server_log!(
                debug,
                "SENTINEL_CRON: closing stale command link for {}",
                addr
            );
            for instance in ctx
                .sentinel_state_mut()
                .registry
                .values_mut()
                .filter(|instance| instance.addr == *addr)
            {
                // This PING is being abandoned with the command link.  Reset its
                // deadline even when deregistration must be retried later.
                instance.act_ping_time = 0;
            }
            match self.remove_by_addr(driver, addr, now) {
                Err(error) => {
                    crate::server_log!(
                        warning,
                        "SENTINEL_CRON: retaining stale link for {} after deregistration failure: {}",
                        addr,
                        error
                    );
                    continue;
                }
                Ok(()) if self.has_pending_close_for_addr(addr) => continue,
                Ok(()) => {}
            }
        }
    }

    fn rebuild_links_if_needed(
        &mut self,
        ctx: &mut ServerContext,
        driver: &mut impl SentinelIo,
        now: i64,
    ) {
        if !ctx.sentinel_state().link_rebuild_needed {
            return;
        }
        ctx.sentinel_state_mut().link_rebuild_needed = false;
        let mut addresses = Vec::new();
        for link in self.links.values() {
            if !addresses.contains(&link.addr) {
                addresses.push(link.addr.clone());
            }
        }
        for addr in addresses {
            if let Err(error) = self.reset_by_addr(driver, &addr, now) {
                crate::server_log!(
                    warning,
                    "SENTINEL_CRON: retaining links for {} after deregistration failure: {}",
                    addr,
                    error
                );
            }
        }
    }

    fn poll_target(
        &mut self,
        ctx: &mut ServerContext,
        driver: &mut impl SentinelIo,
        id: SentinelInstanceId,
        addr: &str,
        role: InstanceRole,
        now: i64,
    ) {
        if self.is_close_pending(addr, SentinelLinkKind::Command) {
            return;
        }
        let connected = self
            .ensure_connection(driver, addr, SentinelLinkKind::Command, now)
            .and_then(|token| self.links.get(&token.0))
            .is_some_and(|link| link.connected);
        if let Some(instance) = ctx.sentinel_state_mut().registry.instance_mut(id) {
            instance.link_state = i64::from(!connected);
            if !connected && instance.link_down_since_time == 0 {
                instance.link_down_since_time = now;
            }
        }
        if role != InstanceRole::Sentinel {
            self.ensure_connection(driver, addr, SentinelLinkKind::HelloSubscription, now);
        }
        self.maybe_queue_ping(ctx, id, addr, now);
        if role != InstanceRole::Sentinel {
            self.maybe_queue_info(ctx, id, addr, now);
        }
    }

    fn ensure_write_interest(&mut self, driver: &mut impl SentinelIo) {
        for link in self.links.values() {
            if !link.close_pending && !link.output.is_empty() {
                let _ = driver.set_interest(link.token, IoInterest::ReadWritable);
            }
        }
    }

    fn is_close_pending(&self, addr: &str, kind: SentinelLinkKind) -> bool {
        self.by_endpoint
            .get(&(addr.to_string(), kind))
            .and_then(|token| self.links.get(token))
            .is_some_and(|link| link.close_pending)
    }

    fn has_pending_close_for_addr(&self, addr: &str) -> bool {
        self.links
            .values()
            .any(|link| link.addr == addr && link.close_pending)
    }
}
