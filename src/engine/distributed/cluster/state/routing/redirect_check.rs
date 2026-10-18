use crate::engine::distributed::cluster::node::ClusterNode;
#[cfg(test)]
use crate::engine::distributed::cluster::{ClientTransport, PreferredEndpoint};

use crate::engine::distributed::cluster::state::types::{ClusterState, CLUSTER_SLOTS};

#[cfg(test)]
use super::{ClusterRouteDecision, RouteRequest};

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub struct RedirectOptions {
    pub allow_replica_read: bool,
    pub preferred_endpoint: PreferredEndpoint,
    pub transport: ClientTransport,
    pub prefer_ipv6: bool,
    pub require_cluster_ok: bool,
}

#[cfg(test)]
impl Default for RedirectOptions {
    fn default() -> Self {
        Self {
            allow_replica_read: false,
            preferred_endpoint: PreferredEndpoint::Ip,
            transport: ClientTransport::Tcp,
            prefer_ipv6: false,
            require_cluster_ok: false,
        }
    }
}

impl ClusterState {
    /// Get the node that owns a given slot (None if unassigned).
    #[cfg(test)]
    pub fn get_node_by_slot(&self, slot: usize) -> Option<&ClusterNode> {
        if slot >= CLUSTER_SLOTS {
            return None;
        }
        self.slot_owners_ref()[slot].or_else(|| {
            (!self.require_full_coverage)
                .then(|| self.declared_node_by_slot(slot))
                .flatten()
        })
    }

    pub fn declared_node_by_slot(&self, slot: usize) -> Option<&ClusterNode> {
        self.node_bound_to_slot(slot, false)
    }

    /// Return the locally recorded owner binding, including a node that has
    /// stopped advertising the slot. C keeps that binding in `cluster->slots`
    /// until a later ownership transition replaces it.
    pub(crate) fn bound_node_by_slot(&self, slot: usize) -> Option<&ClusterNode> {
        self.node_bound_to_slot(slot, true)
    }

    fn node_bound_to_slot(&self, slot: usize, include_withdrawn: bool) -> Option<&ClusterNode> {
        if slot >= CLUSTER_SLOTS {
            return None;
        }
        let owner = self.canonical_slot_owner_ref(slot);
        if owner.is_some() {
            return owner.filter(|node| {
                include_withdrawn
                    || self.slot_ownership.withdrawn_by[slot].as_deref() != Some(node.name.as_str())
            });
        }

        // Unit fixtures historically seeded crate-visible node bitmaps
        // directly. Keep that compatibility out of production builds; all
        // runtime ownership mutations must update the canonical table.
        #[cfg(test)]
        return self.bitmap_owner_ref_for_test(slot, include_withdrawn);
        #[cfg(not(test))]
        None
    }

    #[cfg(test)]
    fn bitmap_owner_ref_for_test(
        &self,
        slot: usize,
        include_withdrawn: bool,
    ) -> Option<&ClusterNode> {
        std::iter::once(&self.myself)
            .chain(self.nodes.values())
            .filter(|node| {
                node.slots.get(slot).copied().unwrap_or(false)
                    && (include_withdrawn
                        || self.slot_ownership.withdrawn_by[slot].as_deref()
                            != Some(node.name.as_str()))
            })
            .max_by(|left, right| {
                left.config_epoch
                    .cmp(&right.config_epoch)
                    .then_with(|| right.name.cmp(&left.name))
            })
    }

    /// Return the inclusive end of the contiguous range owned locally at
    /// `start_slot`. Ownership is resolved once for the whole range so callers
    /// do not rebuild the epoch-aware 16K-slot pipeline for every slot.
    pub fn local_slot_range_end(&self, start_slot: usize) -> Option<usize> {
        if start_slot >= CLUSTER_SLOTS {
            return None;
        }
        let is_local = |slot: usize| {
            self.slot_owner_ref(slot)
                .is_some_and(|owner| std::ptr::eq(owner, &self.myself))
        };
        if !is_local(start_slot) {
            return None;
        }
        let mut final_slot = start_slot;
        while final_slot + 1 < CLUSTER_SLOTS && is_local(final_slot + 1) {
            final_slot += 1;
        }
        Some(final_slot)
    }

    /// Check if a command with given key positions can be executed locally.
    /// Returns Ok(()) if the command may proceed, or a RESP error string for
    /// MOVED / CLUSTERDOWN / CROSSSLOT.
    /// `keys` are the raw key bytes extracted from the command.
    /// `cmd_flags` are the command flags (CMD_* from engine::command).
    /// Uses get_node_by_slot() for the "am I the owner" check so the same
    /// epoch-aware, FAIL-skipping tie-break that produces MOVED targets is
    /// also used to accept local writes — no fixpoint divergence between
    /// MOVED target and CLUSTER NODES / local accept decision.
    /// Whether the given slot is currently open (being migrated away or
    /// imported), meaning the ASK/TRYAGAIN protocol may apply. Callers use this
    /// to decide whether local key-existence needs to be computed before
    /// calling `check_redirect_with_keys`.
    /// Redirect check that can account for open-slot (MIGRATING/IMPORTING) state.
    /// `existing_keys`, when provided, is a per-key slice telling whether each
    /// key currently exists in the local dataset. It is only consulted when the
    /// target slot is open (being migrated or imported); callers should pass
    /// `None` (or an empty slice) for the common stable-slot path, keeping that
    /// path free of any dataset lookups. Aligns with C `getNodeByQuery`.
    #[cfg(test)]
    pub fn check_redirect_with_keys(
        &self,
        keys: &[&[u8]],
        cmd_flags: u64,
        existing_keys: Option<&[bool]>,
        options: RedirectOptions,
    ) -> Result<(), String> {
        let decision = self.decide_route(RouteRequest {
            keys,
            command_flags: cmd_flags,
            existing_keys,
            allow_replica_read: options.allow_replica_read,
            preferred_endpoint: options.preferred_endpoint,
            transport: options.transport,
            prefer_ipv6: options.prefer_ipv6,
            require_cluster_ok: options.require_cluster_ok,
            route_sharded_pubsub: false,
            redirect_keyless: false,
            client_readonly: options.allow_replica_read,
            client_asking: false,
            open_slot_policy: super::OpenSlotPolicy::Standard,
        });
        match decision {
            ClusterRouteDecision::ExecuteLocal => Ok(()),
            other => Err(other.error_message().expect("non-local route has an error")),
        }
    }
}
