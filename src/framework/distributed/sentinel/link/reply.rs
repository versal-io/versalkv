use super::endpoint::{is_auth_error, is_subscription_confirmation};
use super::proto::{
    bulk_as_str, pop_pending_request, RespFrame, SentinelLinkKind, SentinelRequest,
};
use super::SentinelLinkManager;

use crate::engine::command::context::Token;
use crate::engine::distributed::sentinel::hello::{self, HelloMessage};
use crate::framework::context::ServerContext;
use crate::framework::distributed::sentinel::apply_hello_and_flush_config;
use crate::framework::distributed::sentinel::timer::{
    apply_info_reply, apply_ping_reply, apply_vote_reply,
};

impl SentinelLinkManager {
    pub(super) fn handle_frame(
        &mut self,
        ctx: &mut ServerContext,
        token: Token,
        frame: RespFrame,
        now: i64,
    ) {
        let kind = match self.links.get(&token.0) {
            Some(link) if !link.close_pending => link.kind,
            None => return,
            Some(_) => return,
        };
        if kind == SentinelLinkKind::HelloSubscription && is_subscription_confirmation(&frame) {
            if let Some(link) = self.links.get_mut(&token.0) {
                // SUBSCRIBE confirmations are interleaved with AUTH replies.
                // Only set authenticated=true if there is no AUTH in flight;
                // otherwise the AUTH reply path (dispatch_reply for Auth) is the
                // single place that flips authenticated=true. This avoids the
                // race where a sub-confirm wins and drops the pending Auth,
                // leaving the link spuriously marked "authed" for NOAUTH peers.
                let has_pending_auth = link
                    .pending
                    .iter()
                    .any(|r| matches!(r, SentinelRequest::Auth));
                if !has_pending_auth {
                    link.authenticated = true;
                }
                link.pending
                    .retain(|request| !matches!(request, SentinelRequest::SubscribeHello));
            }
            return;
        }
        let has_pending_request = self
            .links
            .get(&token.0)
            .is_some_and(|link| !link.pending.is_empty());
        if kind == SentinelLinkKind::HelloSubscription && !has_pending_request {
            self.handle_subscription_frame(ctx, frame, now);
            return;
        }
        let is_auth_reply = self
            .links
            .get(&token.0)
            .and_then(|link| link.pending.first())
            .is_some_and(|request| matches!(request, SentinelRequest::Auth));
        if is_auth_error(&frame) && !is_auth_reply {
            self.mark_auth_failed(ctx, token, true);
            return;
        }
        let request = self.links.get_mut(&token.0).and_then(pop_pending_request);
        self.dispatch_reply(ctx, token, request, frame, now);
    }

    pub(super) fn mark_auth_failed(&mut self, ctx: &mut ServerContext, token: Token, close: bool) {
        let addr = self.links.get(&token.0).map(|link| link.addr.clone());
        if let Some(link) = self.links.get_mut(&token.0) {
            link.connected = false;
            link.authenticated = false;
        }
        let Some(addr) = addr else {
            return;
        };
        if close {
            ctx.sentinel_state_mut()
                .pending_link_close
                .push(addr.clone());
        }
        for instance in ctx
            .sentinel_state_mut()
            .registry
            .values_mut()
            .filter(|instance| instance.addr == addr)
        {
            instance.link_state = 1;
        }
    }

    fn dispatch_reply(
        &mut self,
        ctx: &mut ServerContext,
        token: Token,
        request: Option<SentinelRequest>,
        frame: RespFrame,
        now: i64,
    ) {
        match request {
            Some(SentinelRequest::Auth) => {
                // NOAUTH and WRONGPASS were handled above. Other AUTH replies
                // are deliberately discarded like C Sentinel: notably, a
                // server that just removed requirepass returns an ERR saying
                // no password is configured, but the connection is usable.
                if let Some(link) = self.links.get_mut(&token.0) {
                    link.authenticated = true;
                }
            }
            Some(SentinelRequest::SetName) => {}
            Some(SentinelRequest::Ping(id)) => {
                let reply = match &frame {
                    RespFrame::Simple(data) | RespFrame::Error(data) => data.as_slice(),
                    _ => &[],
                };
                if !reply.is_empty() {
                    apply_ping_reply(ctx, id, reply, now);
                }
            }
            Some(SentinelRequest::Info(id)) => {
                if let RespFrame::Bulk(data) = frame {
                    if let Ok(info) = String::from_utf8(data) {
                        apply_info_reply(ctx, id, &info, now);
                    }
                }
            }
            Some(SentinelRequest::Vote(master_id, peer_id)) => {
                if let RespFrame::Array(parts) = frame {
                    apply_vote_reply(ctx, master_id, peer_id, &parts, now);
                }
            }
            Some(SentinelRequest::PublishHello) | Some(SentinelRequest::SubscribeHello) | None => {}
        }
    }

    fn handle_subscription_frame(&mut self, ctx: &mut ServerContext, frame: RespFrame, now: i64) {
        let parts = match frame {
            RespFrame::Array(parts) => parts,
            _ => return,
        };
        if parts.len() != 3 {
            return;
        }
        let kind = bulk_as_str(&parts[0]);
        let channel = bulk_as_str(&parts[1]);
        let payload = bulk_as_str(&parts[2]);
        if kind == Some("message") && channel == Some(hello::SENTINEL_HELLO_CHANNEL) {
            if let Some(payload) = payload {
                if let Some(hello_msg) = HelloMessage::parse(payload) {
                    apply_hello_and_flush_config(ctx, &hello_msg, now);
                }
            }
        }
    }
}
