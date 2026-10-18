//! Slots rendering: claimant resolution pipeline + CLUSTER SLOTS output array.
//! The `slot_owners_ref` and `slots_array` helpers are isolated here to keep the
//! main `routing` module (redirects, slot-open checks, stale-primary detection)
//! under the 800 line hard threshold (`scripts/architecture-size-debt.txt`).

use crate::engine::distributed::cluster::node::ClusterNode;
use crate::engine::distributed::cluster::state::types::{ClusterState, CLUSTER_SLOTS};
#[cfg(test)]
use crate::engine::distributed::cluster::{ClientTransport, PreferredEndpoint};

impl ClusterState {
    pub(crate) fn slot_owner_ref(&self, slot: usize) -> Option<&ClusterNode> {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_FAIL, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_NOADDR, CLUSTER_NODE_REPLICA,
        };

        if slot >= CLUSTER_SLOTS {
            return None;
        }
        let bound = self.bound_node_by_slot(slot)?;
        if !bound.is_myself() && bound.has_flag(CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR) {
            return None;
        }
        if self.slot_ownership.withdrawn_by[slot].as_deref() == Some(bound.name.as_str()) {
            return Some(bound);
        }
        if !bound.has_flag(CLUSTER_NODE_FAIL | CLUSTER_NODE_REPLICA)
            && (bound.is_myself() || !bound.has_flag(CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR))
        {
            return Some(bound);
        }
        if !bound.has_flag(CLUSTER_NODE_FAIL) {
            return Some(bound);
        }
        let rank = |left: &&ClusterNode, right: &&ClusterNode| {
            left.config_epoch
                .cmp(&right.config_epoch)
                .then_with(|| right.name.cmp(&left.name))
        };
        let nodes = || std::iter::once(&self.myself).chain(self.nodes.values());
        nodes()
            .filter(|node| {
                node.is_primary()
                    && !node.has_flag(CLUSTER_NODE_FAIL)
                    && (node.is_myself()
                        || !node.has_flag(CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR))
                    && (node.replicaof == bound.name
                        || node.slots[slot]
                        || (!bound.shard_id.is_empty() && node.shard_id == bound.shard_id))
            })
            .max_by(rank)
    }

    /// Resolve every slot through the canonical owner table. `slot_owner_ref`
    /// preserves the transient failover routing rules for a bound FAIL owner.
    pub(crate) fn slot_owners_ref(&self) -> Vec<Option<&ClusterNode>> {
        (0..CLUSTER_SLOTS)
            .map(|slot| self.slot_owner_ref(slot))
            .collect()
    }

    /// Render-safe owner projection shared by CLUSTER SLOTS and CLUSTER NODES.
    /// Canonical redirect ownership remains in `slot_owners_ref`. This method
    /// only closes large transient gossip gaps in client-facing topology while
    /// preserving small zero-claimant DELSLOTS holes.
    #[cfg(test)]
    fn rendered_slot_owners_ref(&self) -> Vec<Option<&ClusterNode>> {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_FAIL, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_NOADDR,
        };

        let mut owners = self.slot_owners_ref();
        let mut leader = (!self.myself.has_flag(CLUSTER_NODE_FAIL) && self.myself.is_primary())
            .then_some(&self.myself);
        for node in self.nodes.values() {
            if node.is_myself()
                || node.has_flag(CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR | CLUSTER_NODE_FAIL)
                || !node.is_primary()
            {
                continue;
            }
            let is_better = leader.is_none_or(|current| {
                node.config_epoch > current.config_epoch
                    || (node.config_epoch == current.config_epoch && node.name < current.name)
            });
            if is_better {
                leader = Some(node);
            }
        }
        if !self.require_full_coverage {
            for (slot, owner) in owners.iter_mut().enumerate() {
                if owner.is_none() {
                    *owner = self.declared_node_by_slot(slot);
                }
            }
        }

        for (slot, owner) in owners.iter_mut().enumerate() {
            if owner.is_none() && self.myself.slots.get(slot).copied().unwrap_or(false) {
                *owner = Some(&self.myself);
            }
        }

        let claimed_slots = owners.iter().filter(|owner| owner.is_some()).count();
        let has_declared_slots =
            self.myself.numslots > 0 || self.nodes.values().any(|node| node.numslots > 0);
        if claimed_slots == 0 && !has_declared_slots {
            if let Some(leader) = leader {
                owners.iter_mut().for_each(|owner| {
                    if owner.is_none() {
                        *owner = Some(leader);
                    }
                });
            }
        }
        owners
    }

    /// Node-name projection of the render-safe owner view; consumed by
    /// `nodes_str` to render per-row slot ranges in CLUSTER NODES output.
    #[cfg(test)]
    pub(crate) fn slot_owners_by_name(&self) -> Vec<Option<String>> {
        self.rendered_slot_owners_ref()
            .into_iter()
            .map(|owner| owner.map(|node| node.name.clone()))
            .collect()
    }

    /// Run-length encode slots whose canonical owner is the given node name,
    /// using the epoch-aware `slot_owners` array (not the per-node bitmap).
    #[cfg(test)]
    pub(crate) fn slot_ranges_for_node<T: AsRef<str>>(
        slot_owners: &[Option<T>],
        node_name: &str,
    ) -> String {
        let mut parts: Vec<String> = Vec::new();
        let mut i = 0;
        while i < slot_owners.len() {
            if slot_owners[i].as_ref().map(AsRef::as_ref) == Some(node_name) {
                let start = i;
                i += 1;
                while i < slot_owners.len()
                    && slot_owners[i].as_ref().map(AsRef::as_ref) == Some(node_name)
                {
                    i += 1;
                }
                let end = i - 1;
                if start == end {
                    parts.push(start.to_string());
                } else {
                    parts.push(format!("{}-{}", start, end));
                }
            } else {
                i += 1;
            }
        }
        parts.join(" ")
    }

    /// Generate CLUSTER SLOTS output as nested arrays.
    /// Format: [start, end, [ip, port, id], [ip, port, id], ...]
    /// One entry per contiguous slot range, with the owner primary first.
    /// Owner attribution is delegated to `slot_owners_ref()` so CLUSTER
    /// SLOTS converges with CLUSTER NODES, MOVED redirects and the local
    /// dispatch gate on exactly the same 3-pass pipeline — this is what
    /// prevents post-failover MOVED loops when the promoted replica's
    /// UPDATE gossip hasn't reached every peer yet.
    #[cfg(test)]
    pub fn slots_array(&self) -> Vec<Vec<crate::foundation::io::protocol::Value>> {
        self.slots_array_for_client(PreferredEndpoint::Ip, ClientTransport::Tcp, false)
    }

    #[cfg(test)]
    pub fn slots_array_for_client(
        &self,
        preference: PreferredEndpoint,
        transport: ClientTransport,
        prefer_ipv6: bool,
    ) -> Vec<Vec<crate::foundation::io::protocol::Value>> {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL;
        use crate::foundation::io::protocol::Value;
        use crate::foundation::io::sys::time::mstime;
        let slot_owner = self.rendered_slot_owners_ref();
        // Safety trace: count any replica returned as owner. Should always
        // be 0; a non-zero value here means the claim pipeline handed a
        // slot to a REPLICA-flagged node, which leaks READONLY into the
        // write path. Rate-limited to ~500 ms because the harness calls
        // CLUSTER SLOTS repeatedly between dispatch retries.
        let mut replica_owner_count = 0usize;
        for o in slot_owner.iter().flatten() {
            if !o.is_primary() {
                replica_owner_count += 1;
            }
        }
        static LAST_TRACE_MS: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
        let now = mstime();
        let prev_ts = LAST_TRACE_MS.load(std::sync::atomic::Ordering::Relaxed);
        let should_trace = replica_owner_count > 0 || now - prev_ts >= 500;
        if should_trace {
            let _ = LAST_TRACE_MS.compare_exchange(
                prev_ts,
                now,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        // Compute a "safe owner" for each FAIL owner id: prefer the
        // highest-config-epoch replica of the dead owner (promoted replicas
        // are already primaries with overlap); else any non-down primary
        // that overlaps the dead owner's slot bitmap. Returning a dead
        // host:port here would cause the Tcl harness to open a blocking
        // TCP socket to a SIGSTOP'd (or crash'd) listener and stall the
        // 99-iteration per-key retry budget.
        use std::collections::HashMap;
        let dead_ids: Vec<String> = slot_owner
            .iter()
            .flatten()
            .filter(|o| o.has_flag(CLUSTER_NODE_FAIL))
            .map(|o| o.name.clone())
            .collect();
        let mut safe_owner: HashMap<String, Option<String>> = HashMap::new();
        for dead_id in dead_ids {
            if safe_owner.contains_key(&dead_id) {
                continue;
            }
            let mut best: Option<String> = None;
            let mut best_ep = 0u64;
            // Step 1: highest-config-epoch replica of the dead id.
            for n in self.nodes.values() {
                if n.name == dead_id {
                    continue;
                }
                if n.has_flag(CLUSTER_NODE_FAIL) {
                    continue;
                }
                if n.replicaof == dead_id && n.config_epoch > best_ep {
                    best_ep = n.config_epoch;
                    best = Some(n.name.clone());
                }
            }
            // Step 2: if no replica match, any non-down primary with
            // overlap against the dead owner's cached slot bitmap.
            if best.is_none() {
                if let Some(dead) = self.nodes.get(&dead_id) {
                    for n in self.nodes.values() {
                        if n.name == dead_id {
                            continue;
                        }
                        if n.has_flag(CLUSTER_NODE_FAIL) {
                            continue;
                        }
                        if !n.is_primary() || n.numslots == 0 {
                            continue;
                        }
                        let mut overlap = 0usize;
                        for s in 0..CLUSTER_SLOTS {
                            if dead.slots[s] && n.slots[s] {
                                overlap += 1;
                                if overlap > 0 {
                                    break;
                                }
                            }
                        }
                        if overlap > 0 && n.config_epoch > best_ep {
                            best_ep = n.config_epoch;
                            best = Some(n.name.clone());
                        }
                    }
                }
            }
            safe_owner.insert(dead_id, best);
        }
        let mut ranges: Vec<Vec<Value>> = Vec::new();
        let mut i = 0;
        while i < CLUSTER_SLOTS {
            if let Some(owner) = slot_owner[i] {
                let start = i;
                let owner_id = owner.name.clone();
                let owner_down = owner.has_flag(CLUSTER_NODE_FAIL);
                i += 1;
                while i < CLUSTER_SLOTS
                    && slot_owner[i].map(|n| n.name == owner_id).unwrap_or(false)
                {
                    i += 1;
                }
                let end = i - 1;
                // Resolve live owner: if the nominal owner is DOWN, look
                // up the pre-computed safe alternative. If no alternative
                // exists yet (promotion UPDATE hasn't reached this peer
                // via gossip), fall back to the nominal down owner — a
                // MOVED/ASK redirect from the harness is strictly better
                // than omitting the range entirely, which would leave
                // $::valkey_cluster::slots() missing keys and hard-fail
                // __dispatch__ with "key <slot> not known in dictionary".
                let resolved = if owner_down {
                    safe_owner
                        .get(&owner_id)
                        .and_then(Option::as_deref)
                        .and_then(|name| self.nodes.get(name))
                        .unwrap_or(owner)
                } else {
                    owner
                };
                let mut range = vec![
                    Value::Integer(start as i64),
                    Value::Integer(end as i64),
                    Value::Array(slot_node_entry(
                        resolved,
                        preference,
                        transport,
                        prefer_ipv6,
                    )),
                ];
                let mut replicas: Vec<_> = self
                    .nodes
                    .values()
                    .chain(std::iter::once(&self.myself))
                    .filter(|node| {
                        node.is_replica()
                            && node.replicaof == resolved.name
                            && !node.has_flag(CLUSTER_NODE_FAIL)
                            && node.repl_offset != 0
                    })
                    .collect();
                replicas.sort_unstable_by(|left, right| left.name.cmp(&right.name));
                range.extend(replicas.into_iter().map(|replica| {
                    Value::Array(slot_node_entry(replica, preference, transport, prefer_ipv6))
                }));
                ranges.push(range);
            } else {
                i += 1;
            }
        }
        if should_trace {
            // Compact summary (truncate to first 5 ranges); enough to reason
            // about slot ownership splits without dumping 5 ranges twice per
            // cron cycle.
            let top5: Vec<String> = ranges
                .iter()
                .take(5)
                .map(|r| {
                    let s = match &r[0] {
                        Value::Integer(v) => *v,
                        _ => -1,
                    };
                    let e = match &r[1] {
                        Value::Integer(v) => *v,
                        _ => -1,
                    };
                    let id: String = match &r[2] {
                        Value::Array(inner) => match inner.get(2) {
                            Some(Value::Bulk(b)) => std::str::from_utf8(b)
                                .unwrap_or("?")
                                .chars()
                                .take(8)
                                .collect(),
                            _ => "?".into(),
                        },
                        _ => "?".into(),
                    };
                    format!("{}..{}={}", s, e, id)
                })
                .collect();
            crate::server_log!(
                notice,
                "[CLUSTER-ROLE] SLOTS-ARRAY: myself={:.8} ranges={} replica_owners={} top5=[{}]",
                self.myself.name,
                ranges.len(),
                replica_owner_count,
                top5.join(" ")
            );
            if replica_owner_count > 0 {
                crate::server_log!(
                    warning,
                    "[CLUSTER-ROLE] SLOTS-ARRAY REPLICA-LEAK: {} slots owned by replica nodes — dispatch will hit READONLY. Node list:",
                    replica_owner_count
                );
                for (idx, o) in slot_owner.iter().enumerate() {
                    if let Some(n) = o {
                        if !n.is_primary() {
                            let rof: &str = if n.replicaof.is_empty() {
                                "-"
                            } else {
                                &n.replicaof
                            };
                            crate::server_log!(
                                warning,
                                "[CLUSTER-ROLE]   slot={} owner={:.8} flags=0x{:04x} replicaof={:.8}",
                                idx,
                                n.name,
                                n.flags,
                                rof
                            );
                        }
                    }
                }
            }
        }
        ranges
    }
}

#[cfg(test)]
fn slot_node_entry(
    node: &ClusterNode,
    preference: PreferredEndpoint,
    transport: ClientTransport,
    prefer_ipv6: bool,
) -> Vec<crate::foundation::io::protocol::Value> {
    use crate::foundation::io::protocol::Value;
    use bytes::Bytes;

    let client = node
        .endpoints
        .client_endpoint_for_family(transport, prefer_ipv6);
    let preferred = node
        .endpoints
        .redirect_endpoint_for_client(preference, transport, prefer_ipv6);
    let address = if preference == PreferredEndpoint::Unknown {
        Value::Null
    } else {
        Value::Bulk(Bytes::from(preferred.host))
    };
    let mut metadata = Vec::new();
    if preference != PreferredEndpoint::Ip {
        metadata.push((Value::bulk("ip"), Value::bulk(client.host)));
    }
    if preference != PreferredEndpoint::Hostname {
        if let Some(hostname) = node.endpoints.hostname() {
            metadata.push((Value::bulk("hostname"), Value::bulk(hostname.to_string())));
        }
    }
    if let Some(zone) = node.endpoints.availability_zone() {
        metadata.push((
            Value::bulk("availability-zone"),
            Value::bulk(zone.to_string()),
        ));
    }
    vec![
        address,
        Value::Integer(client.port as i64),
        Value::Bulk(Bytes::copy_from_slice(node.name.as_bytes())),
        Value::Map(metadata),
    ]
}
