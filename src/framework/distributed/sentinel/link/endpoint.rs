use crate::engine::distributed::sentinel::hello::SENTINEL_HELLO_CHANNEL;
use crate::engine::distributed::sentinel::{
    format_sentinel_endpoint, split_sentinel_endpoint, FailoverState, InstanceRole,
    SentinelInstance, SentinelInstanceId, SentinelState,
};

use super::{
    bulk_as_str, queue_request, RespFrame, SentinelLink, SentinelLinkManager, SentinelRequest,
};
#[cfg(test)]
use crate::engine::command::context::Token;

impl SentinelLinkManager {
    pub fn new() -> Self {
        Self {
            links: std::collections::HashMap::new(),
            by_endpoint: std::collections::HashMap::new(),
            retry_after: std::collections::HashMap::new(),
        }
    }

    #[cfg(test)]
    pub fn is_link_token(&self, token: Token) -> bool {
        self.links.contains_key(&token.0)
    }
}

pub(super) fn auth_for_instance(state: &SentinelState, id: SentinelInstanceId) -> (String, String) {
    let instance = match state.registry.instances().get(&id) {
        Some(instance) => instance,
        None => return (String::new(), String::new()),
    };
    if instance.role == InstanceRole::Sentinel {
        return (
            state.sentinel_auth_user.clone(),
            state.sentinel_auth_pass.clone(),
        );
    }
    state
        .registry
        .masters()
        .map(|(_, master)| master)
        .find(|master| master.id == id || master.replicas.contains(&id))
        .map(|master| (master.auth_user.clone(), master.auth_pass.clone()))
        .unwrap_or_else(|| (instance.auth_user.clone(), instance.auth_pass.clone()))
}

pub(super) fn auth_for_addr(state: &SentinelState, addr: &str) -> (String, String) {
    if state
        .registry
        .instances()
        .values()
        .any(|instance| instance.addr == addr && instance.role == InstanceRole::Sentinel)
    {
        return (
            state.sentinel_auth_user.clone(),
            state.sentinel_auth_pass.clone(),
        );
    }
    state
        .registry
        .masters()
        .map(|(_, master)| master)
        .find(|master| {
            master.addr == addr
                || master.replicas.iter().any(|replica_id| {
                    state
                        .registry
                        .instances()
                        .get(replica_id)
                        .is_some_and(|replica| replica.addr == addr)
                })
        })
        .map(|master| (master.auth_user.clone(), master.auth_pass.clone()))
        .unwrap_or_default()
}

pub(super) fn current_primary_addr(state: &SentinelState, master: &SentinelInstance) -> String {
    if master.flags & crate::engine::distributed::sentinel::SRI_FAILOVER_IN_PROGRESS != 0
        && matches!(
            master.failover_state,
            FailoverState::ReconfigureReplicas | FailoverState::UpdateConfig
        )
    {
        if let Some(addr) = master
            .promoted_replica
            .and_then(|id| state.registry.instances().get(&id))
            .map(|instance| instance.addr.clone())
        {
            return addr;
        }
    }
    master.addr.clone()
}

pub(super) fn sentinel_announce_addr(state: &SentinelState) -> String {
    if !state.announce_hostnames {
        return state.announce_addr.clone();
    }
    let default = split_sentinel_endpoint(&state.announce_addr);
    let default_host = default.map_or(state.announce_addr.as_str(), |endpoint| endpoint.host);
    let default_port = default.map_or(0, |endpoint| endpoint.port);
    let host = if state.announce_ip.is_empty() {
        default_host
    } else {
        state.announce_ip.as_str()
    };
    let port = if state.announce_port == 0 {
        default_port
    } else {
        u16::try_from(state.announce_port).unwrap_or(default_port)
    };
    format_sentinel_endpoint(host, port)
}

pub(super) fn sentinel_command_client_name(state: &SentinelState) -> String {
    let id_prefix = state.myid.get(..8).unwrap_or(state.myid.as_str());
    format!("sentinel-{}-cmd", id_prefix)
}

pub(super) fn hello_targets(state: &SentinelState, master: &SentinelInstance) -> Vec<String> {
    let mut targets = vec![master.addr.clone()];
    for id in master.replicas.iter().chain(&master.sentinels) {
        if let Some(instance) = state.registry.instances().get(id) {
            targets.push(instance.addr.clone());
        }
    }
    targets
}

pub(super) fn hello_sentinel_targets(
    state: &SentinelState,
    master: &SentinelInstance,
) -> Vec<String> {
    master
        .sentinels
        .iter()
        .filter_map(|id| {
            state
                .registry
                .instances()
                .get(id)
                .map(|instance| instance.addr.clone())
        })
        .collect()
}

pub(super) fn is_subscription_confirmation(frame: &RespFrame) -> bool {
    matches!(
        frame,
        RespFrame::Array(parts)
            if parts.first().and_then(bulk_as_str) == Some("subscribe")
                && parts.get(1).and_then(bulk_as_str) == Some(SENTINEL_HELLO_CHANNEL)
    )
}

pub(super) fn is_auth_error(frame: &RespFrame) -> bool {
    matches!(
        frame,
        RespFrame::Error(data)
            if data.starts_with(b"NOAUTH") || data.starts_with(b"WRONGPASS")
    )
}

pub(super) fn queue_link_auth(link: &mut SentinelLink, user: &str, pass: &str) {
    if link.authenticated || (user.is_empty() && pass.is_empty()) {
        return;
    }
    let auth_args = if user.is_empty() {
        vec!["AUTH", pass]
    } else if pass.is_empty() {
        vec!["AUTH", user, ""]
    } else {
        vec!["AUTH", user, pass]
    };
    queue_request(link, SentinelRequest::Auth, &auth_args);
}
