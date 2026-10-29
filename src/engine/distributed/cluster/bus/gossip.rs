use crate::engine::distributed::cluster::msg::{
    self, bytes_to_slots, ClusterExtension, ClusterMessageFlags, ClusterMsg, GossipEntry,
};
use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_HANDSHAKE};
use crate::engine::distributed::cluster::state::ClusterState;
use std::borrow::Cow;

use super::types::ClusterBusManager;

struct PrimaryDemotion {
    old_primary_id: String,
    new_primary_id: String,
    shard_id: String,
    old_primary_name: String,
    new_primary_name: String,
    slots: Vec<bool>,
}

struct PrimaryDemotionCounts {
    moved: usize,
    importing: usize,
    migrating: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ObservedRole {
    Primary,
    Replica,
    Unknown,
}

pub(super) struct SenderObservation<'a> {
    message: &'a ClusterMsg,
    sender_id: &'a str,
    pub(super) sender_ip: Cow<'a, str>,
    pub(super) replicaof: &'a str,
    pub(super) role: ObservedRole,
}

impl<'a> SenderObservation<'a> {
    pub(super) fn normalize(message: &'a ClusterMsg) -> Option<Self> {
        if message.sender_id.is_empty() {
            return None;
        }
        let sender_ip = if message.sender_ip.is_empty() {
            Cow::Borrowed("127.0.0.1")
        } else {
            Cow::Borrowed(message.sender_ip.as_str())
        };
        let role = if !message.replicaof.is_empty()
            || message.flags & crate::engine::distributed::cluster::node::CLUSTER_NODE_REPLICA != 0
        {
            ObservedRole::Replica
        } else if message.flags & crate::engine::distributed::cluster::node::CLUSTER_NODE_PRIMARY
            != 0
        {
            ObservedRole::Primary
        } else {
            ObservedRole::Unknown
        };
        Some(Self {
            message,
            sender_id: message.sender_id.as_str(),
            sender_ip,
            replicaof: message.replicaof.as_str(),
            role,
        })
    }
}

#[derive(Clone, Copy)]
enum SlotReconciliation {
    Preserve,
    Apply,
}

impl SlotReconciliation {
    fn from_apply(apply: bool) -> Self {
        if apply {
            Self::Apply
        } else {
            Self::Preserve
        }
    }
}

struct NodeUpdatePlan<'a> {
    observation: SenderObservation<'a>,
    clear_handshake: bool,
    slot_reconciliation: SlotReconciliation,
    replicated_shard_id: Option<String>,
    previous_replicaof: String,
    sender_owned_slots: bool,
    local_follows_sender: bool,
    primary_demotion: Option<PrimaryDemotion>,
    stale_sender_role: bool,
}

#[derive(Default)]
struct DirectSenderOutcome {
    topology_changed: bool,
    competing_primary_claim: Option<u64>,
    lost_primary_slots: Vec<usize>,
}

struct SenderExtensions<'a> {
    hostname: &'a str,
    human_name: &'a str,
    client_ipv4: &'a str,
    client_ipv6: &'a str,
    availability_zone: &'a str,
    client_port: u16,
    client_tls_port: u16,
    shard_id: Option<String>,
    cluster_replica_priority: u32,
}

impl<'a> SenderExtensions<'a> {
    fn parse(extensions: &'a [ClusterExtension]) -> Self {
        Self {
            hostname: extension_string(extensions, |item| match item {
                ClusterExtension::Hostname(value) => Some(value.as_str()),
                _ => None,
            }),
            human_name: extension_string(extensions, |item| match item {
                ClusterExtension::HumanNodeName(value) => Some(value.as_str()),
                _ => None,
            }),
            client_ipv4: extension_string(extensions, |item| match item {
                ClusterExtension::ClientIpv4(value) => Some(value.as_str()),
                _ => None,
            }),
            client_ipv6: extension_string(extensions, |item| match item {
                ClusterExtension::ClientIpv6(value) => Some(value.as_str()),
                _ => None,
            }),
            availability_zone: extension_string(extensions, |item| match item {
                ClusterExtension::AvailabilityZone(value) => Some(value.as_str()),
                _ => None,
            }),
            client_port: extension_port(extensions, |item| match item {
                ClusterExtension::ClientPort(port) => Some(*port),
                _ => None,
            }),
            client_tls_port: extension_port(extensions, |item| match item {
                ClusterExtension::ClientTlsPort(port) => Some(*port),
                _ => None,
            }),
            shard_id: extensions.iter().find_map(|item| match item {
                ClusterExtension::ShardId(node_id) => Some(node_id.to_string()),
                _ => None,
            }),
            cluster_replica_priority: extensions
                .iter()
                .rev()
                .find_map(|item| match item {
                    ClusterExtension::ReplicaPriority(priority) => Some(*priority),
                    _ => None,
                })
                .unwrap_or_default(),
        }
    }
}

impl ClusterBusManager {
    /// Add or update a node in the cluster state from a message.
    /// Also processes gossip entries and applies slot bitmap from sender.
    pub(super) fn add_or_update_node(
        state: &mut ClusterState,
        msg: &ClusterMsg,
        clear_handshake: bool,
    ) {
        Self::add_or_update_node_with_slot_bitmap(state, msg, clear_handshake, true);
    }

    /// Refresh sender metadata without rebuilding its known slot bitmap. The
    /// PING/PONG receive path uses this after it has compared the wire bitmap
    /// to the cached primary: C only enters `clusterUpdateSlotsConfigWith`
    /// when that claim changed.
    #[cfg(test)]
    pub(super) fn add_or_update_node_without_slot_bitmap(
        state: &mut ClusterState,
        msg: &ClusterMsg,
        clear_handshake: bool,
    ) {
        Self::add_or_update_node_with_slot_bitmap(state, msg, clear_handshake, false);
    }

    pub(super) fn add_or_update_node_with_slot_bitmap(
        state: &mut ClusterState,
        msg: &ClusterMsg,
        clear_handshake: bool,
        apply_slot_bitmap: bool,
    ) {
        let Some(plan) = Self::plan_node_update(state, msg, clear_handshake, apply_slot_bitmap)
        else {
            return;
        };
        Self::apply_node_update(state, plan);
    }

    fn plan_node_update<'a>(
        state: &ClusterState,
        msg: &'a ClusterMsg,
        clear_handshake: bool,
        apply_slot_bitmap: bool,
    ) -> Option<NodeUpdatePlan<'a>> {
        let observation = SenderObservation::normalize(msg)?;
        if observation.sender_id == state.myself.name
            || state.nodes_black_list.contains_key(observation.sender_id)
        {
            return None;
        }
        let previous = state.nodes.get(observation.sender_id);
        let primary_demotion = Self::prepare_primary_demotion(state, msg);
        if primary_demotion.is_none() && Self::reject_stale_replica_transition(state, msg) {
            return None;
        }
        Some(NodeUpdatePlan {
            replicated_shard_id: Self::replicated_shard_id(state, observation.replicaof),
            previous_replicaof: previous
                .map(|node| node.replicaof.clone())
                .unwrap_or_default(),
            sender_owned_slots: previous.is_some_and(|node| node.is_primary() && node.numslots > 0),
            local_follows_sender: state.myself.is_replica()
                && state.myself.replicaof == observation.sender_id,
            stale_sender_role: previous.is_some_and(|sender| {
                sender.is_primary()
                    && sender.numslots > 0
                    && observation.role == ObservedRole::Replica
                    && sender.config_epoch > msg.config_epoch
            }),
            observation,
            clear_handshake,
            slot_reconciliation: SlotReconciliation::from_apply(apply_slot_bitmap),
            primary_demotion,
        })
    }

    fn apply_node_update(state: &mut ClusterState, mut plan: NodeUpdatePlan<'_>) {
        let msg = plan.observation.message;
        let sender_ip = plan.observation.sender_ip.as_ref().to_string();
        let apply_slot_bitmap = matches!(plan.slot_reconciliation, SlotReconciliation::Apply);
        let sender_primary_changed = plan.previous_replicaof != msg.replicaof;
        state.clear_handshake_cooldown(&sender_ip, msg.port, msg.bus_port);
        let update = if state.nodes.contains_key(&msg.sender_id) {
            Self::apply_known_sender(state, &plan, &sender_ip, apply_slot_bitmap)
        } else {
            Self::insert_new_sender(state, &plan, &sender_ip)
        };
        let mut topology_changed = update.topology_changed;
        let lost_primary_slots = update.lost_primary_slots;
        Self::handle_competing_primary_claim(state, msg, update.competing_primary_claim);
        Self::update_sender_lineage(state, &plan);
        if let Some(demotion) = plan.primary_demotion.take() {
            topology_changed |= Self::apply_primary_demotion(state, demotion, msg.config_epoch);
        }
        Self::schedule_subreplica_switch(state, &plan, sender_primary_changed);
        Self::apply_sender_extensions(state, msg);
        for slot in lost_primary_slots {
            state.request_shard_slot_cleanup(slot);
        }

        topology_changed |= Self::apply_gossip_observations(state, msg);
        state.advance_current_epoch(msg.current_epoch);
        Self::resolve_gossip_epoch_collision(state, msg);
        if topology_changed {
            state.mark_topology_changed();
            state.request_config_save();
        }
    }

    fn apply_gossip_observations(state: &mut ClusterState, msg: &ClusterMsg) -> bool {
        let now_ms = crate::foundation::io::sys::time::mstime();
        let mut topology_changed = false;
        for entry in msg.gossip() {
            if entry.node_id.is_empty()
                || entry.node_id == state.myself.name
                || state.nodes_black_list.contains_key(&entry.node_id)
            {
                continue;
            }
            if state.nodes.contains_key(&entry.node_id) {
                topology_changed |= Self::update_gossip_node(state, entry, now_ms);
            } else {
                Self::insert_gossip_node(state, entry, now_ms);
                topology_changed = true;
            }
            Self::apply_gossip_failure_report(state, msg, entry, now_ms);
        }
        topology_changed
    }

    fn apply_known_sender(
        state: &mut ClusterState,
        plan: &NodeUpdatePlan<'_>,
        sender_ip: &str,
        apply_slot_bitmap: bool,
    ) -> DirectSenderOutcome {
        let msg = plan.observation.message;
        let mut node = state.nodes[&msg.sender_id].clone();
        let mut outcome = DirectSenderOutcome::default();
        outcome.topology_changed |=
            Self::apply_sender_metadata(&mut node, msg, sender_ip, plan.clear_handshake);
        let (role_changed, lost_slots) =
            Self::apply_sender_role(&mut node, plan, apply_slot_bitmap);
        outcome.topology_changed |= role_changed;
        outcome.lost_primary_slots = lost_slots;
        outcome.topology_changed |= Self::apply_sender_replication(&mut node, plan);
        if msg.replicaof.is_empty() && msg.config_epoch > node.config_epoch {
            node.config_epoch = msg.config_epoch;
            outcome.topology_changed = true;
            if plan.observation.role == ObservedRole::Primary
                && !node.has_flag(CLUSTER_NODE_HANDSHAKE)
            {
                outcome.competing_primary_claim = Some(node.config_epoch);
            }
        }
        state.commit_direct_sender_observation(node);
        outcome
    }

    fn insert_gossip_node(state: &mut ClusterState, entry: &GossipEntry, now_ms: i64) {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_FAIL, CLUSTER_NODE_MIGRATE_TO, CLUSTER_NODE_MYSELF,
            CLUSTER_NODE_MY_PRIMARY_FAIL, CLUSTER_NODE_NOADDR, CLUSTER_NODE_NOFAILOVER,
            CLUSTER_NODE_PFAIL, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };
        let mut node = ClusterNode::new(
            entry.node_id.clone(),
            entry.ip.clone(),
            entry.port,
            entry.bus_port,
            entry.flags & CLUSTER_NODE_PRIMARY != 0,
        );
        let observed_flags = CLUSTER_NODE_PRIMARY
            | CLUSTER_NODE_REPLICA
            | CLUSTER_NODE_PFAIL
            | CLUSTER_NODE_FAIL
            | CLUSTER_NODE_NOADDR
            | CLUSTER_NODE_MIGRATE_TO
            | CLUSTER_NODE_NOFAILOVER
            | CLUSTER_NODE_MY_PRIMARY_FAIL;
        node.flags = (node.flags
            & !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY | CLUSTER_NODE_REPLICA))
            | (entry.flags & observed_flags);
        node.ping_sent = 0;
        node.pong_received = 0;
        node.config_epoch = entry.config_epoch as u64;
        node.asymmetric_meet_started_at = now_ms;
        state.insert_node(node);
        state.queue_connect((entry.ip.clone(), entry.bus_port, false));
    }

    fn update_gossip_node(state: &mut ClusterState, entry: &GossipEntry, now_ms: i64) -> bool {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_NOADDR;
        let mut node = state.nodes[&entry.node_id].clone();
        let previous_epoch = node.config_epoch;
        let previous_endpoints = node.endpoints.clone();
        node.config_epoch = node.config_epoch.max(entry.config_epoch as u64);
        if !node.has_flag(CLUSTER_NODE_NOADDR) {
            let ip = if entry.ip.is_empty() {
                node.ip().to_string()
            } else {
                entry.ip.clone()
            };
            node.endpoints
                .update_bus_address(ip, entry.port, entry.bus_port);
            node.endpoints.set_tls_port(entry.tls_port);
        }
        let changed = node.config_epoch != previous_epoch || node.endpoints != previous_endpoints;
        state.commit_third_party_gossip_observation(node);
        Self::refresh_gossip_pong(state, entry, now_ms);
        changed
    }

    fn refresh_gossip_pong(state: &mut ClusterState, entry: &GossipEntry, now_ms: i64) {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_FAIL, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_PFAIL,
        };
        let can_refresh = state.nodes.get(&entry.node_id).is_some_and(|node| {
            !node.has_flag(CLUSTER_NODE_HANDSHAKE)
                && entry.flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL) == 0
                && node.ping_sent == 0
                && (entry.pong_received as i64) <= now_ms + 500
                && (entry.pong_received as i64) > node.pong_received
        });
        let timeout_ms = state.failure_report_node_timeout_ms();
        if !can_refresh
            || state
                .count_fail_reports(&entry.node_id, now_ms, timeout_ms)
                .0
                != 0
        {
            return;
        }
        if let Some(mut node) = state.nodes.get(&entry.node_id).cloned() {
            node.pong_received = entry.pong_received as i64;
            state.commit_third_party_gossip_observation(node);
        }
    }

    fn apply_gossip_failure_report(
        state: &mut ClusterState,
        msg: &ClusterMsg,
        entry: &GossipEntry,
        now_ms: i64,
    ) {
        use crate::engine::distributed::cluster::node::{CLUSTER_NODE_FAIL, CLUSTER_NODE_PFAIL};
        let reports_failure = entry.flags & (CLUSTER_NODE_PFAIL | CLUSTER_NODE_FAIL) != 0;
        if !reports_failure {
            if state.remove_fail_report(&entry.node_id, &msg.sender_id) {
                crate::server_log!(
                    verbose,
                    "Node {} reported node {} is back online.",
                    msg.sender_id,
                    entry.node_id
                );
            }
            return;
        }
        let timeout_ms = state.failure_report_node_timeout_ms();
        let was_reported = state
            .fail_reports
            .get(&entry.node_id)
            .is_some_and(|reports| reports.contains_key(&msg.sender_id));
        state.add_fail_report(&entry.node_id, &msg.sender_id, now_ms, timeout_ms);
        if !was_reported {
            Self::log_new_failure_report(state, &msg.sender_id, &entry.node_id);
        }
    }

    fn log_new_failure_report(state: &ClusterState, sender_id: &str, target_id: &str) {
        let is_recorded = state
            .fail_reports
            .get(target_id)
            .is_some_and(|reports| reports.contains_key(sender_id));
        if !is_recorded {
            return;
        }
        if let (Some(sender), Some(target)) =
            (state.nodes.get(sender_id), state.nodes.get(target_id))
        {
            crate::server_log!(
                verbose,
                "Node {} ({}) reported node {} ({}) as not reachable.",
                sender.name,
                sender.display_name(),
                target.name,
                target.display_name()
            );
        }
    }

    fn resolve_gossip_epoch_collision(state: &mut ClusterState, msg: &ClusterMsg) {
        let conflict = msg.gossip().iter().find(|entry| {
            !entry.node_id.is_empty()
                && entry.node_id != state.myself.name
                && entry.config_epoch as u64 == state.myself.config_epoch
                && entry.node_id > state.myself.name
                && entry.flags & crate::engine::distributed::cluster::node::CLUSTER_NODE_REPLICA
                    == 0
                && state.myself.is_primary()
        });
        let Some(entry) = conflict else {
            return;
        };
        state.bump_epoch();
        crate::server_log!(
            notice,
            "Cluster config epoch conflict (gossip) with {}: bumped to {}",
            entry.node_id,
            state.myself.config_epoch
        );
    }

    fn handle_competing_primary_claim(
        state: &mut ClusterState,
        msg: &ClusterMsg,
        claimed_epoch: Option<u64>,
    ) {
        let Some(claimed_epoch) = claimed_epoch else {
            return;
        };
        let election_epoch = state.automatic_failover.election_epoch();
        if state.automatic_failover_on_primary_epoch_claim(claimed_epoch) {
            crate::server_log!(
                warning,
                "Failover election in progress for epoch {}, but received a claim from node {} with an equal or higher epoch {}. Resetting the election since we cannot win an election in the past.",
                election_epoch,
                msg.sender_id,
                claimed_epoch,
            );
        }
    }

    pub(super) fn observe_known_sender_header(
        state: &mut ClusterState,
        msg: &ClusterMsg,
        now_ms: i64,
    ) {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_MY_PRIMARY_FAIL;

        let Some(sender) = state.nodes.get_mut(&msg.sender_id) else {
            return;
        };
        // C updates these fields before dispatching every full-header packet,
        // including FAIL, auth ACK, and publish messages. They describe live
        // observations, not a topology or role transition.
        sender.flags = (sender.flags & !CLUSTER_NODE_MY_PRIMARY_FAIL)
            | (msg.flags & CLUSTER_NODE_MY_PRIMARY_FAIL);
        sender.data_received = now_ms;
        if sender.has_flag(CLUSTER_NODE_HANDSHAKE) {
            return;
        }
        sender.repl_offset = msg.repl_offset;
        let claimed_epoch = if msg.replicaof.is_empty() && msg.config_epoch > sender.config_epoch {
            sender.config_epoch = msg.config_epoch;
            Some(msg.config_epoch)
        } else {
            None
        };
        state.advance_current_epoch(msg.current_epoch);
        if claimed_epoch.is_some() {
            state.request_config_fsync();
            Self::handle_competing_primary_claim(state, msg, claimed_epoch);
        }
    }

    fn update_sender_lineage(state: &mut ClusterState, plan: &NodeUpdatePlan<'_>) {
        let msg = plan.observation.message;
        if !msg.replicaof.is_empty() {
            state.mark_primary_migration_target(&msg.replicaof);
        } else if !plan.previous_replicaof.is_empty() {
            if let Some(mut promoted) = state.nodes.get(&msg.sender_id).cloned() {
                promoted.flags |=
                    crate::engine::distributed::cluster::node::CLUSTER_NODE_MIGRATE_TO;
                state.commit_direct_sender_observation(promoted);
            }
        }
        if !plan.previous_replicaof.is_empty() && plan.previous_replicaof != msg.replicaof {
            state.clear_primary_migration_target_if_orphaned(&plan.previous_replicaof);
        }
    }

    fn schedule_subreplica_switch(
        state: &mut ClusterState,
        plan: &NodeUpdatePlan<'_>,
        sender_primary_changed: bool,
    ) {
        let msg = plan.observation.message;
        let target = state.nodes.get(&msg.replicaof);
        let should_switch = plan.local_follows_sender
            && sender_primary_changed
            && (plan.sender_owned_slots || !state.is_replica_migration_allowed())
            && !msg.replicaof.is_empty()
            && msg.replicaof != state.myself.name
            && target.is_some_and(ClusterNode::is_primary);
        if !should_switch {
            return;
        }
        let mode = if target.unwrap().shard_id == state.myself.shard_id {
            crate::engine::distributed::cluster::action::ReplicationSwitchMode::TryPartial
        } else {
            crate::engine::distributed::cluster::action::ReplicationSwitchMode::ForceFull
        };
        crate::server_log!(
            notice,
            "I'm a sub-replica! Reconfiguring myself as a replica of {} from {}",
            msg.replicaof,
            msg.sender_id,
        );
        state.set_pending_replication_switch(
            crate::engine::distributed::cluster::state::PendingReplicationSwitch {
                node_id: msg.replicaof.clone(),
                mode,
            },
        );
    }

    fn apply_sender_metadata(
        node: &mut ClusterNode,
        msg: &ClusterMsg,
        sender_ip: &str,
        clear_handshake: bool,
    ) -> bool {
        let previous_flags = node.flags;
        let previous_endpoints = node.endpoints.clone();
        if clear_handshake {
            node.flags &= !CLUSTER_NODE_HANDSHAKE;
        }
        node.endpoints
            .update_bus_address(sender_ip.to_string(), msg.port, msg.bus_port);
        node.endpoints.set_tls_port(msg.tls_port);
        node.repl_offset = msg.repl_offset;
        let dynamic_mask = crate::engine::distributed::cluster::node::CLUSTER_NODE_NOFAILOVER
            | crate::engine::distributed::cluster::node::CLUSTER_NODE_MIGRATE_TO
            | crate::engine::distributed::cluster::node::CLUSTER_NODE_MY_PRIMARY_FAIL;
        node.flags = (node.flags & !dynamic_mask) | (msg.flags & dynamic_mask);
        node.flags != previous_flags || node.endpoints != previous_endpoints
    }

    fn apply_sender_role(
        node: &mut ClusterNode,
        plan: &NodeUpdatePlan<'_>,
        apply_slot_bitmap: bool,
    ) -> (bool, Vec<usize>) {
        let msg = plan.observation.message;
        if !apply_slot_bitmap || msg.myslots.len() != msg::SLOT_BYTES {
            return (false, Vec::new());
        }
        node.advertised_slots.clone_from(&msg.myslots);
        node.has_advertised_slots = true;
        let old_flags = node.flags;
        let old_numslots = node.numslots;
        let lost_slots = Self::clear_demoted_sender_slots(node, plan);
        Self::replace_sender_role_flags(node, plan);
        (
            node.flags != old_flags || node.numslots != old_numslots,
            lost_slots,
        )
    }

    fn clear_demoted_sender_slots(node: &mut ClusterNode, plan: &NodeUpdatePlan<'_>) -> Vec<usize> {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_MIGRATE_TO;
        if plan.observation.role == ObservedRole::Replica && !plan.stale_sender_role {
            let lost_slots = if plan.local_follows_sender {
                node.slots
                    .iter()
                    .enumerate()
                    .filter_map(|(slot, served)| served.then_some(slot))
                    .collect()
            } else {
                Vec::new()
            };
            node.slots.fill(false);
            node.numslots = 0;
            node.flags &= !CLUSTER_NODE_MIGRATE_TO;
            lost_slots
        } else {
            Vec::new()
        }
    }

    fn replace_sender_role_flags(node: &mut ClusterNode, plan: &NodeUpdatePlan<'_>) {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };
        let msg = plan.observation.message;
        if !plan.stale_sender_role {
            let handshake = if plan.clear_handshake {
                CLUSTER_NODE_HANDSHAKE
            } else {
                0
            };
            node.flags = (node.flags & !(CLUSTER_NODE_PRIMARY | CLUSTER_NODE_REPLICA | handshake))
                | (msg.flags & (CLUSTER_NODE_PRIMARY | CLUSTER_NODE_REPLICA));
        }
    }

    fn apply_sender_replication(node: &mut ClusterNode, plan: &NodeUpdatePlan<'_>) -> bool {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };
        let msg = plan.observation.message;
        let before = (node.replicaof.clone(), node.shard_id.clone(), node.flags);
        if plan.observation.role == ObservedRole::Replica && !plan.stale_sender_role {
            node.replicaof = msg.replicaof.clone();
            node.flags &= !CLUSTER_NODE_PRIMARY;
            node.flags |= CLUSTER_NODE_REPLICA;
            if let Some(shard_id) = plan
                .replicated_shard_id
                .as_ref()
                .filter(|shard_id| !shard_id.is_empty())
            {
                node.shard_id = shard_id.clone();
            }
        } else if plan.observation.role == ObservedRole::Primary && !node.replicaof.is_empty() {
            node.replicaof.clear();
        }
        before != (node.replicaof.clone(), node.shard_id.clone(), node.flags)
    }

    fn insert_new_sender(
        state: &mut ClusterState,
        plan: &NodeUpdatePlan<'_>,
        sender_ip: &str,
    ) -> DirectSenderOutcome {
        let msg = plan.observation.message;
        let mut node = ClusterNode::new(
            msg.sender_id.clone(),
            sender_ip.to_string(),
            msg.port,
            msg.bus_port,
            true,
        );
        node.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;
        if !plan.clear_handshake {
            node.flags |= CLUSTER_NODE_HANDSHAKE;
        }
        let now = crate::foundation::io::sys::time::mstime();
        node.pong_received = now;
        node.created_at = now;
        node.config_epoch = msg.config_epoch;
        node.repl_offset = msg.repl_offset;
        Self::initialize_new_sender(state, plan, &mut node, sender_ip);
        state.insert_node(node);
        crate::server_log!(
            notice,
            "Cluster new node discovered: {} ({}:{})",
            msg.sender_id,
            msg.port,
            msg.bus_port
        );
        DirectSenderOutcome {
            topology_changed: true,
            ..DirectSenderOutcome::default()
        }
    }

    fn initialize_new_sender(
        state: &ClusterState,
        plan: &NodeUpdatePlan<'_>,
        node: &mut ClusterNode,
        sender_ip: &str,
    ) {
        use crate::engine::distributed::cluster::node::{CLUSTER_NODE_FAIL, CLUSTER_NODE_NOADDR};
        let msg = plan.observation.message;
        node.flags |= msg.flags
            & (crate::engine::distributed::cluster::node::CLUSTER_NODE_MY_PRIMARY_FAIL
                | crate::engine::distributed::cluster::node::CLUSTER_NODE_NOFAILOVER);
        node.endpoints.set_tls_port(msg.tls_port);
        if let Some(shard) = state
            .nodes
            .values()
            .find(|candidate| {
                candidate.ip() == sender_ip
                    && candidate.port() == msg.port
                    && candidate.has_flag(CLUSTER_NODE_FAIL | CLUSTER_NODE_NOADDR)
            })
            .map(|candidate| candidate.shard_id.clone())
        {
            node.shard_id = shard;
        }
        if msg.myslots.len() == msg::SLOT_BYTES && plan.observation.role == ObservedRole::Primary {
            node.slots = bytes_to_slots(&msg.myslots);
            node.numslots = node.count_slots();
            node.advertised_slots.clone_from(&msg.myslots);
            node.has_advertised_slots = true;
        }
        if plan.observation.role == ObservedRole::Replica {
            node.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_PRIMARY;
            node.flags |= crate::engine::distributed::cluster::node::CLUSTER_NODE_REPLICA;
            node.replicaof = msg.replicaof.clone();
            if let Some(shard_id) = &plan.replicated_shard_id {
                node.shard_id = shard_id.clone();
            }
        }
    }

    fn replicated_shard_id(state: &ClusterState, replicaof: &str) -> Option<String> {
        if replicaof.is_empty() {
            return None;
        }
        if replicaof == state.myself.name {
            Some(state.myself.shard_id.clone())
        } else {
            state
                .nodes
                .get(replicaof)
                .map(|primary| primary.shard_id.clone())
        }
    }

    fn reject_stale_replica_transition(state: &ClusterState, msg: &ClusterMsg) -> bool {
        state.nodes.get(&msg.sender_id).is_some_and(|sender| {
            sender.is_primary()
                && !msg.replicaof.is_empty()
                && state.nodes.get(&msg.replicaof).is_some_and(|new_primary| {
                    new_primary.shard_id == sender.shard_id
                        && (sender.config_epoch > msg.config_epoch
                            || new_primary.config_epoch > msg.config_epoch)
                })
        })
    }

    /// Apply the direct-sender config-epoch collision rule after PING/PONG
    /// slot reconciliation, matching C's `clusterUpdateSlotsConfigWith` →
    /// `clusterHandleConfigEpochCollision` order.
    pub(super) fn resolve_config_epoch_collisions(state: &mut ClusterState, msg: &ClusterMsg) {
        let reparenting_to_sender = state
            .pending_replication_switch
            .as_ref()
            .is_some_and(|pending| pending.node_id == msg.sender_id);
        if reparenting_to_sender
            || msg.config_epoch != state.myself.config_epoch
            || msg.sender_id <= state.myself.name
            || msg.sender_id.is_empty()
            || (msg.flags & crate::engine::distributed::cluster::node::CLUSTER_NODE_REPLICA) != 0
            || !state.myself.is_primary()
        {
            return;
        }
        state.bump_epoch();
        crate::server_log!(
            notice,
            "Cluster config epoch collision with {} (same epoch {}): bumped to {}",
            msg.sender_id,
            msg.config_epoch,
            state.myself.config_epoch
        );
    }

    fn prepare_primary_demotion(state: &ClusterState, msg: &ClusterMsg) -> Option<PrimaryDemotion> {
        let sender = state.nodes.get(&msg.sender_id)?;
        let new_primary = state.nodes.get(&msg.replicaof)?;
        if !sender.is_primary()
            || msg.replicaof.is_empty()
            || sender.shard_id != new_primary.shard_id
            || sender.config_epoch > msg.config_epoch
            || new_primary.config_epoch > msg.config_epoch
        {
            return None;
        }
        Some(PrimaryDemotion {
            old_primary_id: sender.name.clone(),
            new_primary_id: new_primary.name.clone(),
            shard_id: sender.shard_id.clone(),
            old_primary_name: sender.display_name(),
            new_primary_name: new_primary.display_name(),
            slots: sender.slots.clone(),
        })
    }

    fn apply_primary_demotion(
        state: &mut ClusterState,
        demotion: PrimaryDemotion,
        config_epoch: u64,
    ) -> bool {
        let moved_slots = Self::transfer_primary_slots(state, &demotion, config_epoch);
        let new_primary_id =
            crate::engine::distributed::cluster::NodeId::parse(demotion.new_primary_id.as_bytes())
                .expect("known Cluster node ID must remain valid");
        let (importing_slots, migrating_slots) = state
            .retarget_open_slots_after_primary_demotion(&demotion.old_primary_id, &new_primary_id);
        Self::log_retargeted_open_slots(&demotion, &importing_slots, &migrating_slots);
        Self::log_primary_demotion_summary(
            &demotion,
            config_epoch,
            PrimaryDemotionCounts {
                moved: moved_slots,
                importing: importing_slots.len(),
                migrating: migrating_slots.len(),
            },
        );
        moved_slots > 0 || !importing_slots.is_empty() || !migrating_slots.is_empty()
    }

    fn transfer_primary_slots(
        state: &mut ClusterState,
        demotion: &PrimaryDemotion,
        config_epoch: u64,
    ) -> usize {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_MIGRATE_TO, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };
        let moved_slots = demotion.slots.iter().filter(|&&owned| owned).count();
        if let Some(mut new_primary) = state.nodes.get(&demotion.new_primary_id).cloned() {
            new_primary.flags &= !CLUSTER_NODE_REPLICA;
            new_primary.flags |= CLUSTER_NODE_PRIMARY | CLUSTER_NODE_MIGRATE_TO;
            new_primary.replicaof.clear();
            new_primary.config_epoch = config_epoch;
            for (target, owned) in new_primary.slots.iter_mut().zip(&demotion.slots) {
                *target |= *owned;
            }
            new_primary.numslots = new_primary.count_slots();
            state.commit_primary_demotion_target(new_primary);
            for (slot, owned) in demotion.slots.iter().copied().enumerate() {
                if owned {
                    state
                        .assign_slot_to_remote(slot, &demotion.new_primary_id)
                        .expect("promoted Cluster Bus owner must remain known");
                }
            }
        }
        for (slot, owned) in demotion.slots.iter().copied().enumerate() {
            if owned {
                state.clear_slot_claim_withdrawn_for(
                    slot,
                    &[&demotion.old_primary_id, &demotion.new_primary_id],
                );
            }
        }
        moved_slots
    }

    fn log_primary_demotion_summary(
        demotion: &PrimaryDemotion,
        config_epoch: u64,
        counts: PrimaryDemotionCounts,
    ) {
        if counts.moved > 0 {
            crate::server_log!(
                notice,
                "A failover occurred in shard {}; node {} ({}) lost {} slot(s) and failed over to node {} ({}) with a config epoch of {}",
                demotion.shard_id,
                demotion.old_primary_id,
                demotion.old_primary_name,
                counts.moved,
                demotion.new_primary_id,
                demotion.new_primary_name,
                config_epoch,
            );
        }
        if counts.importing > 0 {
            crate::server_log!(
                notice,
                "A failover occurred in migration source. Update importing source of {} slot(s) to node {} ({}) in shard {}.",
                counts.importing,
                demotion.new_primary_id,
                demotion.new_primary_name,
                demotion.shard_id,
            );
        }
        if counts.migrating > 0 {
            crate::server_log!(
                notice,
                "A failover occurred in migration target. Update migrating target of {} slot(s) to node {} ({}) in shard {}.",
                counts.migrating,
                demotion.new_primary_id,
                demotion.new_primary_name,
                demotion.shard_id,
            );
        }
    }

    fn log_retargeted_open_slots(
        demotion: &PrimaryDemotion,
        importing_slots: &[usize],
        migrating_slots: &[usize],
    ) {
        for slot in importing_slots {
            crate::server_log!(
                verbose,
                "Failover occurred in migration source. Update importing source for slot {} to node {} ({}) in shard {}.",
                slot,
                demotion.new_primary_id,
                demotion.new_primary_name,
                demotion.shard_id,
            );
        }
        for slot in migrating_slots {
            crate::server_log!(
                verbose,
                "Failover occurred in migration target. Slot {} is now being migrated to node {} ({}) in shard {}.",
                slot,
                demotion.new_primary_id,
                demotion.new_primary_name,
                demotion.shard_id,
            );
        }
    }

    fn apply_sender_extensions(state: &mut ClusterState, msg: &ClusterMsg) {
        if !matches!(
            msg.msg_type,
            msg::ClusterMsgType::Ping | msg::ClusterMsgType::Pong | msg::ClusterMsgType::Meet
        ) {
            return;
        }
        if !msg.has_message_flag(ClusterMessageFlags::EXT_DATA) {
            if let Some(mut node) = state
                .nodes
                .get(&msg.sender_id)
                .filter(|node| node.cluster_replica_priority != 0)
                .cloned()
            {
                node.cluster_replica_priority = 0;
                state.commit_sender_extension_observation(node);
                state.mark_topology_changed();
            }
            return;
        }
        let extensions = SenderExtensions::parse(&msg.extensions);
        let mut topology_changed = Self::apply_sender_extension_metadata(state, msg, extensions);
        topology_changed |= Self::apply_forgotten_node_extensions(state, msg);
        if topology_changed {
            state.mark_topology_changed();
        }
    }

    fn apply_sender_extension_metadata(
        state: &mut ClusterState,
        msg: &ClusterMsg,
        extensions: SenderExtensions<'_>,
    ) -> bool {
        if let Some(mut node) = state.nodes.get(&msg.sender_id).cloned() {
            let previous_flags = node.flags;
            let previous_endpoints = node.endpoints.clone();
            let previous_shard_id = node.shard_id.clone();
            let previous_priority = node.cluster_replica_priority;
            node.flags |=
                crate::engine::distributed::cluster::node::CLUSTER_NODE_EXTENSIONS_SUPPORTED;
            let _ = node.endpoints.set_hostname(extensions.hostname.as_bytes());
            let _ = node
                .endpoints
                .set_human_name(extensions.human_name.as_bytes());
            let _ = node
                .endpoints
                .set_client_ipv4(extensions.client_ipv4.as_bytes());
            let _ = node
                .endpoints
                .set_client_ipv6(extensions.client_ipv6.as_bytes());
            let _ = node
                .endpoints
                .set_availability_zone(extensions.availability_zone.as_bytes());
            node.endpoints.set_client_port(extensions.client_port);
            node.endpoints
                .set_client_tls_port(extensions.client_tls_port);
            if let Some(shard_id) = extensions.shard_id {
                node.shard_id = shard_id;
            }
            node.cluster_replica_priority = extensions.cluster_replica_priority;
            let changed = node.flags != previous_flags
                || node.endpoints != previous_endpoints
                || node.shard_id != previous_shard_id
                || node.cluster_replica_priority != previous_priority;
            state.commit_sender_extension_observation(node);
            changed
        } else {
            false
        }
    }

    fn apply_forgotten_node_extensions(state: &mut ClusterState, msg: &ClusterMsg) -> bool {
        let mut topology_changed = false;
        for extension in &msg.extensions {
            let ClusterExtension::ForgottenNode {
                node_id,
                ttl_seconds,
            } = extension
            else {
                continue;
            };
            let node_id = node_id.to_string();
            if node_id == state.myself.name || state.myself.replicaof == node_id {
                continue;
            }
            topology_changed |= state.remove_node(&node_id).is_some();
            state.blacklist_node_until(
                node_id,
                crate::foundation::io::sys::time::mstime()
                    .saturating_add(ttl_seconds.saturating_mul(1_000).min(i64::MAX as u64) as i64),
            );
        }
        topology_changed
    }
}

fn extension_string<'a>(
    extensions: &'a [ClusterExtension],
    select: impl Fn(&'a ClusterExtension) -> Option<&'a str>,
) -> &'a str {
    extensions.iter().find_map(select).unwrap_or_default()
}

fn extension_port(
    extensions: &[ClusterExtension],
    select: impl Fn(&ClusterExtension) -> Option<u16>,
) -> u16 {
    extensions.iter().find_map(select).unwrap_or_default()
}
