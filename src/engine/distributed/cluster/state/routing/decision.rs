use crate::engine::command::{CMD_ADMIN, CMD_ASKING, CMD_PUBSUB, CMD_WRITE};
use crate::engine::distributed::cluster::node::key_hash_slot;
use crate::engine::distributed::cluster::{
    AdvertisedEndpoint, ClientTransport, ClusterNodeHealth, ClusterNodeRole,
    ClusterTopologySnapshot, NodeId, PreferredEndpoint,
};

use super::super::{ClusterHealth, ClusterState};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterRouteDecision {
    ExecuteLocal,
    Moved {
        slot: u16,
        endpoint: AdvertisedEndpoint,
    },
    Ask {
        slot: u16,
        endpoint: AdvertisedEndpoint,
    },
    Redirect {
        endpoint: AdvertisedEndpoint,
    },
    TryAgain,
    CrossSlot,
    ClusterDown,
    ReadOnly,
}

impl ClusterRouteDecision {
    pub fn error_message(&self) -> Option<String> {
        match self {
            Self::ExecuteLocal => None,
            Self::Moved { slot, endpoint } => {
                Some(format!("MOVED {slot} {}:{}", endpoint.host, endpoint.port))
            }
            Self::Ask { slot, endpoint } => {
                Some(format!("ASK {slot} {}:{}", endpoint.host, endpoint.port))
            }
            Self::Redirect { endpoint } => {
                Some(format!("REDIRECT {}:{}", endpoint.host, endpoint.port))
            }
            Self::TryAgain => {
                Some("TRYAGAIN Multiple keys request during rehashing of slot".to_string())
            }
            Self::CrossSlot => {
                Some("CROSSSLOT Keys in request don't hash to the same slot".to_string())
            }
            Self::ClusterDown => Some("CLUSTERDOWN The cluster is down".to_string()),
            Self::ReadOnly => {
                Some("READONLY You can't write against a read only replica.".to_string())
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RouteRequest<'a> {
    pub keys: &'a [&'a [u8]],
    pub command_flags: u64,
    pub existing_keys: Option<&'a [bool]>,
    pub allow_replica_read: bool,
    pub preferred_endpoint: PreferredEndpoint,
    pub transport: ClientTransport,
    pub prefer_ipv6: bool,
    pub require_cluster_ok: bool,
    pub route_sharded_pubsub: bool,
    pub redirect_keyless: bool,
    pub client_readonly: bool,
    pub client_asking: bool,
    pub open_slot_policy: OpenSlotPolicy,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OpenSlotPolicy {
    #[default]
    Standard,
    ExecuteLocal,
    RejectUnstable,
}

impl Default for RouteRequest<'_> {
    fn default() -> Self {
        Self {
            keys: &[],
            command_flags: 0,
            existing_keys: None,
            allow_replica_read: false,
            preferred_endpoint: PreferredEndpoint::Ip,
            transport: ClientTransport::Tcp,
            prefer_ipv6: false,
            require_cluster_ok: false,
            route_sharded_pubsub: false,
            redirect_keyless: false,
            client_readonly: false,
            client_asking: false,
            open_slot_policy: OpenSlotPolicy::Standard,
        }
    }
}

impl ClusterState {
    pub fn decide_route(&self, request: RouteRequest<'_>) -> ClusterRouteDecision {
        if request.require_cluster_ok && self.state != super::super::CLUSTER_OK {
            return ClusterRouteDecision::ClusterDown;
        }
        let bypass = (request.command_flags & CMD_ADMIN) != 0
            || ((request.command_flags & CMD_PUBSUB) != 0 && !request.route_sharded_pubsub);
        if bypass {
            return ClusterRouteDecision::ExecuteLocal;
        }
        if request.keys.is_empty() {
            if request.redirect_keyless
                && self.myself.is_replica()
                && !request.client_readonly
                && (request.command_flags & (CMD_WRITE | crate::engine::command::CMD_READONLY)) != 0
            {
                if let Some(primary) = self.nodes.get(&self.myself.replicaof) {
                    return ClusterRouteDecision::Redirect {
                        endpoint: primary.endpoints.redirect_endpoint_for_client(
                            request.preferred_endpoint,
                            request.transport,
                            request.prefer_ipv6,
                        ),
                    };
                }
            }
            if self.myself.is_replica() && (request.command_flags & CMD_WRITE) != 0 {
                return ClusterRouteDecision::ReadOnly;
            }
            return ClusterRouteDecision::ExecuteLocal;
        }

        let slot = key_hash_slot(request.keys[0]);
        if request
            .keys
            .iter()
            .skip(1)
            .any(|key| key_hash_slot(key) != slot)
        {
            return ClusterRouteDecision::CrossSlot;
        }
        let slot_index = slot as usize;
        let migrating_to = self.slot_ownership.migrating_to.get(&slot_index);
        let importing_from = self.slot_ownership.importing_from.get(&slot_index);
        let owner = self.slot_owner_ref(slot_index).or_else(|| {
            (!self.require_full_coverage)
                .then(|| self.declared_node_by_slot(slot_index))
                .flatten()
        });
        if migrating_to.is_some() || importing_from.is_some() {
            match request.open_slot_policy {
                OpenSlotPolicy::ExecuteLocal if self.myself.is_primary() => {
                    return ClusterRouteDecision::ExecuteLocal;
                }
                OpenSlotPolicy::RejectUnstable => return ClusterRouteDecision::TryAgain,
                OpenSlotPolicy::Standard | OpenSlotPolicy::ExecuteLocal => {}
            }
            let owner_is_local = owner.is_some_and(|owner| {
                owner.is_myself()
                    || (request.client_readonly
                        && self.myself.is_replica()
                        && self.myself.replicaof == owner.name)
            });
            let (missing, existing) =
                request
                    .existing_keys
                    .map_or((0, request.keys.len()), |flags| {
                        let existing = flags.iter().filter(|&&exists| exists).count();
                        (flags.len().saturating_sub(existing), existing)
                    });
            if owner_is_local && missing > 0 {
                if existing > 0 {
                    return ClusterRouteDecision::TryAgain;
                }
                if let Some(target) = migrating_to.and_then(|id| self.nodes.get(id)) {
                    return ClusterRouteDecision::Ask {
                        slot,
                        endpoint: target.endpoints.redirect_endpoint_for_client(
                            request.preferred_endpoint,
                            request.transport,
                            request.prefer_ipv6,
                        ),
                    };
                }
            }
            if importing_from.is_some()
                && (request.client_asking || (request.command_flags & CMD_ASKING) != 0)
            {
                if request.keys.len() > 1 && missing > 0 {
                    return ClusterRouteDecision::TryAgain;
                }
                return ClusterRouteDecision::ExecuteLocal;
            }
        }

        let Some(owner) = owner else {
            return ClusterRouteDecision::ClusterDown;
        };
        if owner.is_myself() {
            return ClusterRouteDecision::ExecuteLocal;
        }
        let is_read = (request.command_flags & CMD_WRITE) == 0;
        if request.allow_replica_read
            && is_read
            && self.myself.is_replica()
            && (self.myself.replicaof == owner.name || self.replica_primary_is_stale())
        {
            return ClusterRouteDecision::ExecuteLocal;
        }
        ClusterRouteDecision::Moved {
            slot,
            endpoint: owner.endpoints.redirect_endpoint_for_client(
                request.preferred_endpoint,
                request.transport,
                request.prefer_ipv6,
            ),
        }
    }

    fn replica_primary_is_stale(&self) -> bool {
        use crate::engine::distributed::cluster::node::{CLUSTER_NODE_FAIL, CLUSTER_NODE_PFAIL};

        self.myself.is_replica()
            && self
                .nodes
                .get(&self.myself.replicaof)
                .is_some_and(|primary| {
                    !primary.is_primary()
                        || primary.numslots == 0
                        || primary.has_flag(CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)
                })
    }
}

impl ClusterTopologySnapshot {
    pub fn is_slot_open(&self, slot: u16) -> bool {
        transition_target(&self.migrating_slots_to, slot).is_some()
            || transition_target(&self.importing_slots_from, slot).is_some()
    }

    pub fn decide_route(&self, request: RouteRequest<'_>) -> ClusterRouteDecision {
        if request.require_cluster_ok && self.health != ClusterHealth::Ok {
            return ClusterRouteDecision::ClusterDown;
        }
        let bypass = (request.command_flags & CMD_ADMIN) != 0
            || ((request.command_flags & CMD_PUBSUB) != 0 && !request.route_sharded_pubsub);
        if bypass {
            return ClusterRouteDecision::ExecuteLocal;
        }
        if request.keys.is_empty() {
            if request.redirect_keyless
                && self.local_node().role == ClusterNodeRole::Replica
                && !request.client_readonly
                && (request.command_flags & (CMD_WRITE | crate::engine::command::CMD_READONLY)) != 0
            {
                let endpoint = self
                    .local_node()
                    .replica_of
                    .as_ref()
                    .and_then(|primary| self.node(primary))
                    .map(|primary| {
                        primary.endpoints.redirect_endpoint_for_client(
                            request.preferred_endpoint,
                            request.transport,
                            request.prefer_ipv6,
                        )
                    });
                if let Some(endpoint) = endpoint {
                    return ClusterRouteDecision::Redirect { endpoint };
                }
            }
            if self.local_node().role == ClusterNodeRole::Replica
                && (request.command_flags & CMD_WRITE) != 0
            {
                return ClusterRouteDecision::ReadOnly;
            }
            return ClusterRouteDecision::ExecuteLocal;
        }

        let slot = key_hash_slot(request.keys[0]);
        if request
            .keys
            .iter()
            .skip(1)
            .any(|key| key_hash_slot(key) != slot)
        {
            return ClusterRouteDecision::CrossSlot;
        }

        let migrating_to = transition_target(&self.migrating_slots_to, slot);
        let importing_from = transition_target(&self.importing_slots_from, slot);
        if migrating_to.is_some() || importing_from.is_some() {
            match request.open_slot_policy {
                OpenSlotPolicy::ExecuteLocal
                    if self.local_node().role == ClusterNodeRole::Primary =>
                {
                    return ClusterRouteDecision::ExecuteLocal;
                }
                OpenSlotPolicy::RejectUnstable => return ClusterRouteDecision::TryAgain,
                OpenSlotPolicy::Standard | OpenSlotPolicy::ExecuteLocal => {}
            }
            let local = self.local_node();
            let owner_is_local = self.slot_owners[slot as usize]
                .as_ref()
                .is_some_and(|owner| {
                    owner == self.local_node_id()
                        || (request.client_readonly
                            && local.role == ClusterNodeRole::Replica
                            && local.replica_of.as_ref() == Some(owner))
                });
            let (missing, existing) =
                request
                    .existing_keys
                    .map_or((0, request.keys.len()), |flags| {
                        let existing = flags.iter().filter(|&&exists| exists).count();
                        (flags.len().saturating_sub(existing), existing)
                    });
            if owner_is_local && missing > 0 {
                if existing > 0 {
                    return ClusterRouteDecision::TryAgain;
                }
                if let Some(target) = migrating_to.and_then(|id| self.node(id)) {
                    return ClusterRouteDecision::Ask {
                        slot,
                        endpoint: target.endpoints.redirect_endpoint_for_client(
                            request.preferred_endpoint,
                            request.transport,
                            request.prefer_ipv6,
                        ),
                    };
                }
            }
            if importing_from.is_some()
                && (request.client_asking || (request.command_flags & CMD_ASKING) != 0)
            {
                if request.keys.len() > 1 && missing > 0 {
                    return ClusterRouteDecision::TryAgain;
                }
                return ClusterRouteDecision::ExecuteLocal;
            }
        }

        let Some(owner_id) = self.slot_owners[slot as usize].as_ref() else {
            return ClusterRouteDecision::ClusterDown;
        };
        if owner_id == self.local_node_id() {
            return ClusterRouteDecision::ExecuteLocal;
        }
        let local = self.local_node();
        let is_read = (request.command_flags & CMD_WRITE) == 0;
        if request.allow_replica_read
            && is_read
            && local.role == ClusterNodeRole::Replica
            && (local.replica_of.as_ref() == Some(owner_id) || self.replica_primary_is_stale(local))
        {
            return ClusterRouteDecision::ExecuteLocal;
        }
        let Some(owner) = self.node(owner_id) else {
            return ClusterRouteDecision::ClusterDown;
        };
        ClusterRouteDecision::Moved {
            slot,
            endpoint: owner.endpoints.redirect_endpoint_for_client(
                request.preferred_endpoint,
                request.transport,
                request.prefer_ipv6,
            ),
        }
    }

    fn local_node(&self) -> &crate::engine::distributed::cluster::NodeView {
        self.nodes
            .iter()
            .find(|node| {
                node.flags & crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF != 0
            })
            .expect("topology snapshot always contains myself")
    }

    fn local_node_id(&self) -> &NodeId {
        &self.local_node().id
    }

    fn node(&self, id: &NodeId) -> Option<&crate::engine::distributed::cluster::NodeView> {
        self.nodes.iter().find(|node| node.id == *id)
    }

    fn replica_primary_is_stale(
        &self,
        local: &crate::engine::distributed::cluster::NodeView,
    ) -> bool {
        local.role == ClusterNodeRole::Replica
            && local.replica_of.as_ref().is_some_and(|primary_id| {
                self.node(primary_id).is_some_and(|primary| {
                    primary.role != ClusterNodeRole::Primary
                        || !primary.slots.iter().any(|owned| *owned)
                        || matches!(
                            primary.health,
                            ClusterNodeHealth::Fail | ClusterNodeHealth::PFail
                        )
                })
            })
    }
}

fn transition_target(transitions: &[(u16, NodeId)], slot: u16) -> Option<&NodeId> {
    transitions
        .iter()
        .find_map(|(candidate, node_id)| (*candidate == slot).then_some(node_id))
}
