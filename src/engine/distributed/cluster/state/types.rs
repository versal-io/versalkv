use crate::engine::distributed::cluster::action::ReplicationSwitchMode;
use crate::engine::distributed::cluster::link::ClusterLinkView;
use crate::engine::distributed::cluster::migration::{
    MigrationAction, MigrationError, MigrationEvent, MigrationJob, MigrationJobId,
    MigrationRegistry, TerminalPhase,
};
use crate::engine::distributed::cluster::msg::ClusterMsgType;
use crate::engine::distributed::cluster::node::{generate_node_id, ClusterNode};
use crate::engine::distributed::cluster::state::failover::AutomaticFailover;
use crate::engine::distributed::cluster::state::failover::ManualFailover;
use crate::engine::distributed::cluster::NodeId;
use std::collections::{BTreeSet, HashMap};

use super::debug::{ClusterActiveOperations, ClusterDebugSnapshot, ClusterPendingWork};
use super::snapshot::ClusterHealth;

/// Cluster state constants.
pub const CLUSTER_SLOTS: usize = 16384;

/// Stable identity for the node bound to a hash slot.
/// Node bitmaps remain the wire/config representation, while this table is
/// the authoritative equivalent of C Valkey's `cluster->slots[slot]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SlotOwner {
    Unassigned,
    Myself,
    Remote(NodeId),
}

pub(super) struct SlotOwnershipTable {
    pub(super) owners: Vec<SlotOwner>,
    pub(super) delete_epochs: Vec<u64>,
    pub(super) withdrawn_by: Vec<Option<String>>,
    pub(super) migrating_to: HashMap<usize, String>,
    pub(super) importing_from: HashMap<usize, String>,
}

impl SlotOwnershipTable {
    fn new() -> Self {
        Self {
            owners: vec![SlotOwner::Unassigned; CLUSTER_SLOTS],
            delete_epochs: vec![0; CLUSTER_SLOTS],
            withdrawn_by: vec![None; CLUSTER_SLOTS],
            migrating_to: HashMap::new(),
            importing_from: HashMap::new(),
        }
    }
}

/// A pending MEET request (ip, port, bus_port).
pub type PendingMeet = (String, u16, u16);
/// A pending outgoing connection (ip, bus_port, explicit_or_temporary_meet).
pub type PendingConnect = (String, u16, bool);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ClusterLinkDirection {
    To,
    From,
    All,
}

pub const CLUSTER_OK: i32 = 0;
pub const CLUSTER_FAIL: i32 = 1;

#[derive(Default)]
pub(super) struct ClusterBusStats {
    pub(super) messages_sent: [u64; 12],
    pub(super) messages_received: [u64; 12],
    pub(super) total_bytes_sent: u64,
    pub(super) total_bytes_received: u64,
    pub(super) pubsub_bytes_sent: u64,
    pub(super) pubsub_bytes_received: u64,
    pub(super) module_bytes_sent: u64,
    pub(super) module_bytes_received: u64,
    pub(super) link_memory: usize,
    pub(super) links_buffer_limit_exceeded: u64,
    pub(super) links_established_inbound: u64,
    pub(super) links_established_outbound: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingReplicationSwitch {
    pub node_id: String,
    pub mode: ReplicationSwitchMode,
}

/// The cluster state: holds all nodes, slots, and configuration.
pub struct ClusterState {
    /// The local node (myself).
    pub(crate) myself: ClusterNode,
    /// All known nodes (name -> node).
    pub(crate) nodes: HashMap<String, ClusterNode>,
    /// Current config epoch.
    pub(crate) current_epoch: u64,
    /// Last epoch at which this node cast a vote (prevents double-voting).
    pub(crate) last_vote_epoch: u64,
    /// Cluster state: CLUSTER_OK or CLUSTER_FAIL.
    pub(crate) state: i32,
    /// Number of primary nodes with at least one slot.
    pub(crate) size: usize,
    pub(crate) config_save_succeeded: bool,
    pub(crate) config_last_save_time_sec: i64,
    /// Assigned slots bitmask.
    pub(crate) slots: Vec<bool>,
    /// Canonical ownership, deletion barriers, withdrawals and open-slot links.
    /// Mutations are owned by `state::slots`; callers submit typed transitions.
    pub(super) slot_ownership: SlotOwnershipTable,
    /// Atomic slot migration jobs and bounded terminal history.
    pub(crate) migrations: MigrationRegistry,
    pending_migration_actions: Vec<MigrationAction>,
    /// Node blacklist (node name -> timestamp).
    pub(crate) nodes_black_list: HashMap<String, i64>,
    /// Failure reports for PFail→Fail upgrade: (reported_node_id -> reporter_node_id -> report_time).
    pub(crate) fail_reports: HashMap<String, HashMap<String, i64>>,
    /// Runtime-owned Cluster Bus connections projected by node and direction.
    pub(crate) link_views: HashMap<(String, ClusterLinkDirection), ClusterLinkView>,
    /// Pending MEET requests (processed by event loop).
    pub(crate) pending_meets: Vec<PendingMeet>,
    /// Pending FORGET requests (node names, processed by event loop).
    pub(crate) pending_forgets: Vec<String>,
    /// Pending outgoing connections (auto-discovery + MEET).
    pub(crate) pending_connects: Vec<PendingConnect>,
    /// Endpoints whose temporary handshake just expired. Stale gossip must
    /// not recreate these handshakes before the reciprocal MEET retry runs.
    handshake_cooldowns: HashMap<(String, u16, u16), i64>,
    /// Cluster links requested for asynchronous close by DEBUG CLUSTERLINK.
    pub(crate) pending_link_closes: Vec<(String, ClusterLinkDirection)>,
    pending_topology_broadcast: bool,
    pending_config_save: bool,
    pending_config_fsync: bool,
    topology_generation: u64,
    pub(super) topology_state_dirty: bool,
    pending_dirty_slot_cleanup: Vec<usize>,
    pub(super) pending_shard_slot_cleanup: BTreeSet<u16>,
    ever_owned_slots: bool,
    replica_migration_allowed: bool,
    pub(super) require_full_coverage: bool,
    gossip_percentage: usize,
    pub(super) bus_stats: ClusterBusStats,
    replica_migration_orphaned_since: HashMap<String, i64>,
    failure_report_node_timeout_ms: u64,
    pub(super) last_observed_master_down: bool,
    pub(super) last_observed_minority: bool,
    pub(super) last_observed_reachability_minority: bool,
    first_topology_evaluation_ms: Option<i64>,
    /// Deterministic automatic election state for this server instance.
    pub(crate) automatic_failover: AutomaticFailover,
    /// Deterministic coordinated/FORCE/TAKEOVER failover state.
    pub(crate) manual_failover: ManualFailover,
    /// Runtime projection of cluster-manual-failover-timeout for bus callbacks.
    manual_failover_timeout_ms: i64,
    /// Actions emitted outside the bus cron (commands and receive callbacks).
    pending_cluster_actions: Vec<crate::engine::distributed::cluster::action::ClusterAction>,
    /// If set, the UPDATE slot-reassignment logic decided the local node
    /// (primary or replica whose primary just lost all slots) must turn into
    /// a replica of the given address.  Consumed by the event-loop cron
    /// immediately after the bus read pass, which sets up the replication
    /// handshake (same flow as the orphaned-primary self-heal path).
    pub(crate) pending_replication_switch: Option<PendingReplicationSwitch>,
    debug_disable_random_ping: bool,
    debug_disable_reconnection: bool,
    debug_prevent_slot_migration_pause: bool,
    debug_prevent_slot_migration_failover: bool,
    debug_drop_packet_filter: i64,
    debug_close_link_on_packet_drop: bool,
}

impl ClusterState {
    /// Initialize a new cluster state with the local node.
    pub fn new(ip: &str, port: u16, cport: u16) -> Self {
        let node_id = generate_node_id();
        let mut myself = ClusterNode::new(node_id.clone(), ip.to_string(), port, cport, true);
        myself.shard_id = node_id;

        let slots = vec![false; CLUSTER_SLOTS];
        let nodes = HashMap::new();

        Self {
            myself,
            nodes,
            current_epoch: 0,
            last_vote_epoch: 0,
            state: CLUSTER_FAIL,
            size: 0,
            config_save_succeeded: true,
            config_last_save_time_sec: crate::foundation::io::sys::time::mstime() / 1_000,
            slots,
            slot_ownership: SlotOwnershipTable::new(),
            migrations: MigrationRegistry::default(),
            pending_migration_actions: Vec::new(),
            nodes_black_list: HashMap::new(),
            fail_reports: HashMap::new(),
            link_views: HashMap::new(),
            pending_meets: Vec::new(),
            pending_forgets: Vec::new(),
            pending_connects: Vec::new(),
            handshake_cooldowns: HashMap::new(),
            pending_link_closes: Vec::new(),
            pending_topology_broadcast: false,
            pending_config_save: false,
            pending_config_fsync: false,
            topology_generation: 0,
            topology_state_dirty: false,
            pending_dirty_slot_cleanup: Vec::new(),
            pending_shard_slot_cleanup: BTreeSet::new(),
            ever_owned_slots: false,
            replica_migration_allowed: true,
            require_full_coverage: true,
            gossip_percentage: 10,
            bus_stats: ClusterBusStats::default(),
            replica_migration_orphaned_since: HashMap::new(),
            failure_report_node_timeout_ms: 15_000,
            last_observed_master_down: false,
            last_observed_minority: false,
            last_observed_reachability_minority: false,
            first_topology_evaluation_ms: None,
            automatic_failover: AutomaticFailover::default(),
            manual_failover: ManualFailover::default(),
            manual_failover_timeout_ms: 5_000,
            pending_cluster_actions: Vec::new(),
            pending_replication_switch: None,
            debug_disable_random_ping: false,
            debug_disable_reconnection: false,
            debug_prevent_slot_migration_pause: false,
            debug_prevent_slot_migration_failover: false,
            debug_drop_packet_filter: -1,
            debug_close_link_on_packet_drop: false,
        }
    }

    pub fn set_debug_packet_filter(&mut self, packet_type: i64) {
        self.debug_drop_packet_filter = packet_type;
    }

    pub fn should_drop_packet(&self, message_type: ClusterMsgType) -> bool {
        self.debug_drop_packet_filter == -2
            || self.debug_drop_packet_filter == debug_packet_type(message_type)
    }

    pub fn set_debug_close_link_on_packet_drop(&mut self, should_close: bool) {
        self.debug_close_link_on_packet_drop = should_close;
    }

    pub fn should_close_link_on_packet_drop(&self) -> bool {
        self.debug_close_link_on_packet_drop
    }

    pub fn record_bus_message_sent(&mut self, message_type: ClusterMsgType) {
        self.bus_stats.messages_sent[debug_packet_type(message_type) as usize] += 1;
    }

    pub fn record_bus_message_received(&mut self, message_type: ClusterMsgType) {
        self.bus_stats.messages_received[debug_packet_type(message_type) as usize] += 1;
    }

    pub(super) fn bus_messages_sent(&self, message_type: ClusterMsgType) -> u64 {
        self.bus_stats.messages_sent[debug_packet_type(message_type) as usize]
    }

    pub(super) fn bus_messages_received(&self, message_type: ClusterMsgType) -> u64 {
        self.bus_stats.messages_received[debug_packet_type(message_type) as usize]
    }

    pub(super) fn total_bus_messages_sent(&self) -> u64 {
        self.bus_stats.messages_sent.iter().sum()
    }

    pub(crate) fn total_bus_messages_received(&self) -> u64 {
        self.bus_stats.messages_received.iter().sum()
    }

    pub fn record_bus_bytes_sent(&mut self, n: usize) {
        self.bus_stats.total_bytes_sent = self.bus_stats.total_bytes_sent.saturating_add(n as u64);
    }

    pub fn record_bus_bytes_received(&mut self, n: usize) {
        self.bus_stats.total_bytes_received =
            self.bus_stats.total_bytes_received.saturating_add(n as u64);
    }

    pub fn record_bus_frame_sent(&mut self, message_type: ClusterMsgType, n: usize) {
        if matches!(
            message_type,
            ClusterMsgType::Publish | ClusterMsgType::PublishShard
        ) {
            self.bus_stats.pubsub_bytes_sent =
                self.bus_stats.pubsub_bytes_sent.saturating_add(n as u64);
        } else if message_type == ClusterMsgType::Module {
            self.bus_stats.module_bytes_sent =
                self.bus_stats.module_bytes_sent.saturating_add(n as u64);
        }
    }

    pub fn record_bus_frame_received(&mut self, message_type: ClusterMsgType, n: usize) {
        self.record_bus_bytes_received(n);
        if matches!(
            message_type,
            ClusterMsgType::Publish | ClusterMsgType::PublishShard
        ) {
            self.bus_stats.pubsub_bytes_received = self
                .bus_stats
                .pubsub_bytes_received
                .saturating_add(n as u64);
        } else if message_type == ClusterMsgType::Module {
            self.bus_stats.module_bytes_received = self
                .bus_stats
                .module_bytes_received
                .saturating_add(n as u64);
        }
    }

    pub(crate) fn record_bus_message_received_from_header(&mut self, frame: &[u8]) {
        let Some(raw_type) = frame
            .get(12..14)
            .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]))
        else {
            return;
        };
        let Some(message_type) = ClusterMsgType::from_u16(raw_type) else {
            return;
        };
        self.record_bus_message_received(message_type);
        if let Some(raw_len) = frame.get(4..8) {
            let declared = u32::from_be_bytes(raw_len.try_into().expect("four-byte slice"));
            self.record_bus_frame_received(message_type, declared as usize);
        }
    }

    pub fn set_gossip_percentage(&mut self, percentage: usize) {
        self.gossip_percentage = percentage.clamp(1, 100);
    }

    pub(crate) fn gossip_target_count(&self) -> usize {
        let known_nodes = self.nodes.len().saturating_add(1);
        let eligible = known_nodes.saturating_sub(1);
        known_nodes
            .saturating_mul(self.gossip_percentage)
            .checked_div(100)
            .unwrap_or(0)
            .max(3)
            .min(eligible)
    }

    pub fn set_cluster_link_memory(&mut self, bytes: usize) {
        self.bus_stats.link_memory = bytes;
    }

    /// Record runtime replication facts advertised by the local node.
    pub(crate) fn observe_local_replication(&mut self, offset: i64, priority: i64) {
        self.myself.repl_offset = offset;
        self.myself.replica_priority = priority;
    }

    pub(crate) fn set_local_replication_offset(
        &mut self,
        offset: crate::engine::distributed::replication::ReplicationOffset,
    ) {
        self.myself.repl_offset = offset.get();
    }

    pub(crate) fn increment_link_buffer_limit_exceeded(&mut self, count: usize) {
        self.bus_stats.links_buffer_limit_exceeded = self
            .bus_stats
            .links_buffer_limit_exceeded
            .saturating_add(count as u64);
    }

    pub(crate) fn record_link_established(&mut self, direction: ClusterLinkDirection) {
        let counter = match direction {
            ClusterLinkDirection::From => &mut self.bus_stats.links_established_inbound,
            ClusterLinkDirection::To => &mut self.bus_stats.links_established_outbound,
            ClusterLinkDirection::All => return,
        };
        *counter = counter.saturating_add(1);
    }

    pub(crate) fn insert_node(&mut self, node: ClusterNode) -> Option<ClusterNode> {
        let node_id = node.name.clone();
        let previous = self.nodes.insert(node_id.clone(), node);
        if previous.is_none() {
            self.index_inserted_node_slots(&node_id);
        }
        previous
    }

    fn replace_known_node(&mut self, node: ClusterNode) {
        let removed_owned_slots = self
            .nodes
            .get(&node.name)
            .filter(|existing| existing.slots != node.slots)
            .map(|existing| {
                existing
                    .slots
                    .iter()
                    .zip(&node.slots)
                    .enumerate()
                    .filter_map(|(slot, (before, after))| {
                        (*before
                            && !*after
                            && self.canonical_slot_owner_id(slot) == Some(node.name.as_str()))
                        .then_some(slot)
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let node_id = node.name.clone();
        if let Some(existing) = self.nodes.get_mut(&node_id) {
            *existing = node;
        }
        self.unassign_observation_removed_slots(&node_id, &removed_owned_slots);
    }

    pub(crate) fn commit_direct_sender_observation(&mut self, node: ClusterNode) {
        if self.myself.is_replica()
            && self.myself.replicaof == node.name
            && !node.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL)
        {
            self.clear_local_primary_failure_view();
        }
        self.replace_known_node(node);
    }

    pub(crate) fn commit_third_party_gossip_observation(&mut self, node: ClusterNode) {
        self.replace_known_node(node);
    }

    pub(crate) fn commit_primary_demotion_target(&mut self, node: ClusterNode) {
        self.replace_known_node(node);
    }

    pub(crate) fn commit_sender_extension_observation(&mut self, node: ClusterNode) {
        self.replace_known_node(node);
    }

    pub(crate) fn set_remote_slot_claim(&mut self, node_id: &str, slot: usize, claimed: bool) {
        if !claimed && self.canonical_slot_owner_id(slot) == Some(node_id) {
            self.unassign_slot_if_owned_by(slot, node_id);
            return;
        }
        let Some(node) = self.nodes.get_mut(node_id) else {
            return;
        };
        if node.slots[slot] == claimed {
            return;
        }
        node.slots[slot] = claimed;
        node.numslots = if claimed {
            node.numslots.saturating_add(1)
        } else {
            node.numslots.saturating_sub(1)
        };
    }

    pub(crate) fn clear_local_primary_failure_view(&mut self) {
        self.myself.flags &=
            !crate::engine::distributed::cluster::node::CLUSTER_NODE_MY_PRIMARY_FAIL;
    }

    pub(crate) fn clear_node_failure_flags(&mut self, node_id: &str) {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_FAIL, CLUSTER_NODE_NOADDR, CLUSTER_NODE_PFAIL,
        };
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.flags &= !(CLUSTER_NODE_NOADDR | CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL);
            node.fail_time = 0;
        }
        if self.myself.is_replica() && self.myself.replicaof == node_id {
            self.clear_local_primary_failure_view();
        }
    }

    pub(crate) fn mark_node_noaddr_failed(&mut self, node_id: &str) {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_FAIL, CLUSTER_NODE_NOADDR, CLUSTER_NODE_PFAIL,
        };
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.flags &= !CLUSTER_NODE_PFAIL;
            node.flags |= CLUSTER_NODE_NOADDR | CLUSTER_NODE_FAIL;
            node.fail_time = crate::foundation::io::sys::time::mstime();
            node.endpoints.update_bus_address(String::new(), 0, 0);
            node.endpoints.set_tls_port(0);
            node.endpoints.set_client_port(0);
            node.endpoints.set_client_tls_port(0);
        }
    }

    pub(crate) fn promote_remote_update_owner(&mut self, node_id: &str, epoch: u64) -> bool {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_MIGRATE_TO, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };
        let Some(node) = self.nodes.get_mut(node_id) else {
            return false;
        };
        let was_replica = node.is_replica();
        node.config_epoch = epoch;
        node.flags &= !CLUSTER_NODE_REPLICA;
        node.flags |= CLUSTER_NODE_PRIMARY;
        if was_replica {
            node.flags |= CLUSTER_NODE_MIGRATE_TO;
        }
        node.replicaof.clear();
        true
    }

    pub(crate) fn confirm_gossip_handshake(&mut self, node_id: &str, now_ms: i64) {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE;
        if let Some(node) = self.nodes.get_mut(node_id) {
            if node.has_flag(CLUSTER_NODE_HANDSHAKE) {
                node.flags &= !CLUSTER_NODE_HANDSHAKE;
                node.asymmetric_meet_started_at = now_ms;
            }
        }
    }

    pub(crate) fn promote_local_update_owner(&mut self, epoch: u64) {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_MIGRATE_TO, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };
        let was_replica = self.myself.is_replica();
        self.myself.config_epoch = epoch;
        self.myself.flags &= !CLUSTER_NODE_REPLICA;
        self.myself.flags |= CLUSTER_NODE_PRIMARY;
        if was_replica {
            self.myself.flags |= CLUSTER_NODE_MIGRATE_TO;
        }
        self.myself.replicaof.clear();
    }

    pub(crate) fn remove_node(&mut self, node_id: &str) -> Option<ClusterNode> {
        if !self.nodes.contains_key(node_id) {
            return None;
        }
        for node in self.nodes.values_mut() {
            if node.replicaof == node_id {
                node.replicaof.clear();
            }
        }
        let changed = self.unassign_slots_owned_by(node_id) > 0;
        let removed = self
            .nodes
            .remove(node_id)
            .expect("known node remains present until removal");
        if changed {
            self.mark_topology_changed();
        }
        Some(removed)
    }

    pub(crate) fn blacklist_node_until(&mut self, node_id: String, expires_at: i64) {
        self.nodes_black_list.insert(node_id, expires_at);
    }

    pub(crate) fn set_pending_replication_switch(&mut self, pending: PendingReplicationSwitch) {
        self.pending_replication_switch = Some(pending);
    }

    pub(crate) fn clear_pending_replication_switch(&mut self) {
        self.pending_replication_switch = None;
    }

    pub(crate) fn track_imports_after_demotion(&mut self, now: i64) -> Vec<MigrationJobId> {
        let ids = self.migrations.track_imports_after_demotion(now);
        self.debug_assert_migration_invariants();
        ids
    }

    pub(crate) fn set_local_config_epoch(&mut self, epoch: u64) {
        self.myself.config_epoch = epoch;
    }

    pub(crate) fn apply_local_cluster_config(
        &mut self,
        endpoints: crate::engine::distributed::cluster::ClusterEndpoints,
        nofailover: bool,
    ) {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_NOFAILOVER;
        self.myself.endpoints = endpoints;
        if nofailover {
            self.myself.flags |= CLUSTER_NODE_NOFAILOVER;
        } else {
            self.myself.flags &= !CLUSTER_NODE_NOFAILOVER;
        }
    }

    pub(crate) fn manual_failover_tick(
        &mut self,
        now_ms: i64,
        local_offset: crate::engine::distributed::replication::ReplicationOffset,
    ) -> Vec<crate::engine::distributed::cluster::action::ClusterAction> {
        let mut actions = self.manual_failover.on_timeout(now_ms);
        actions.extend(self.manual_failover.on_replica_caught_up(local_offset));
        actions
    }

    pub(crate) fn accept_manual_failover_on_primary(
        &mut self,
        requester: crate::engine::distributed::cluster::NodeId,
        now_ms: i64,
        timeout_ms: i64,
    ) -> Vec<crate::engine::distributed::cluster::action::ClusterAction> {
        self.manual_failover
            .accept_on_primary(requester, now_ms, timeout_ms)
    }

    pub(crate) fn observe_manual_primary_paused(
        &mut self,
        primary: &crate::engine::distributed::cluster::NodeId,
        offset: crate::engine::distributed::replication::ReplicationOffset,
    ) -> Vec<crate::engine::distributed::cluster::action::ClusterAction> {
        self.manual_failover.on_primary_paused(primary, offset)
    }

    pub(crate) fn commit_manual_primary_pause(&mut self) {
        self.manual_failover.on_primary_pause_committed();
    }

    pub(crate) fn commit_manual_role_change(
        &mut self,
    ) -> Vec<crate::engine::distributed::cluster::action::ClusterAction> {
        self.manual_failover.on_role_change_committed()
    }

    pub(crate) fn automatic_failover_on_tick(
        &mut self,
        facts: &super::failover::AutomaticFailoverFacts,
    ) -> Vec<crate::engine::distributed::cluster::action::ClusterAction> {
        self.automatic_failover.on_tick(facts)
    }

    pub(crate) fn automatic_failover_on_vote_request(
        &mut self,
        facts: &super::failover::VoteRequestFacts,
    ) -> Vec<crate::engine::distributed::cluster::action::ClusterAction> {
        self.automatic_failover.on_vote_request(facts)
    }

    pub(crate) fn defer_for_better_replica_election(
        &mut self,
        requester: &crate::engine::distributed::cluster::NodeId,
        request_epoch: u64,
        now_ms: i64,
    ) -> bool {
        if request_epoch < self.current_epoch || !self.myself.is_replica() {
            return false;
        }
        let Some(requester_node) = self.nodes.get(requester.as_str()) else {
            return false;
        };
        if !requester_node.is_replica()
            || requester_node.replicaof != self.myself.replicaof
            || requester_node
                .has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_NOFAILOVER)
        {
            return false;
        }
        let Some(local_id) =
            crate::engine::distributed::cluster::NodeId::parse(self.myself.name.as_bytes()).ok()
        else {
            return false;
        };
        let requester_rank = super::failover::ReplicaRankKey {
            replication_offset: requester_node.repl_offset,
            cluster_replica_priority: requester_node.cluster_replica_priority,
            node_id: requester.clone(),
        };
        let local_rank = super::failover::ReplicaRankKey {
            replication_offset: self.myself.repl_offset,
            cluster_replica_priority: self.myself.cluster_replica_priority,
            node_id: local_id,
        };
        if requester_rank >= local_rank {
            return false;
        }
        self.automatic_failover.defer_for_better_replica_election(
            request_epoch,
            now_ms,
            self.failure_report_node_timeout_ms,
        )
    }

    pub(crate) fn automatic_failover_on_vote_ack(
        &mut self,
        voter: crate::engine::distributed::cluster::NodeId,
        epoch: u64,
        is_voting_primary: bool,
    ) -> Vec<crate::engine::distributed::cluster::action::ClusterAction> {
        self.automatic_failover
            .on_vote_ack(voter, epoch, is_voting_primary)
    }

    pub(crate) fn automatic_failover_on_primary_epoch_claim(&mut self, claimed_epoch: u64) -> bool {
        self.automatic_failover
            .reset_on_competing_primary_claim(claimed_epoch)
    }

    pub(crate) fn automatic_failover_on_topology_change(
        &mut self,
        local_is_primary: bool,
        primary_failed: bool,
    ) -> Vec<crate::engine::distributed::cluster::action::ClusterAction> {
        self.automatic_failover
            .on_topology_change(local_is_primary, primary_failed)
    }

    pub(crate) fn mark_node_meet_sent(&mut self, node_id: &str, sent_at: i64) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.meet_sent_at = sent_at;
        }
    }

    pub(crate) fn start_node_probe_if_idle(&mut self, node_id: &str, now_ms: i64) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            if node.ping_sent == 0 {
                node.ping_sent = now_ms;
            }
        }
    }

    pub(crate) fn refresh_manual_failover_probe(&mut self, node_id: &str, now_ms: i64) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.ping_sent = now_ms;
        }
    }

    pub(crate) fn observe_node_failure_timeout(
        &mut self,
        node_id: &str,
        now_ms: i64,
        data_received_ms: i64,
        timeout_ms: i64,
    ) -> Option<(bool, bool, bool)> {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_FAIL, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_NOADDR, CLUSTER_NODE_PFAIL,
        };
        let node = self.nodes.get_mut(node_id)?;
        if node.is_myself()
            || node.has_flag(CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR)
            || node.has_flag(CLUSTER_NODE_FAIL)
        {
            return Some((false, false, false));
        }
        let was_pfail = node.has_flag(CLUSTER_NODE_PFAIL);
        let ping_delay = now_ms.saturating_sub(node.ping_sent);
        let data_delay = now_ms.saturating_sub(data_received_ms);
        if node.ping_sent != 0 && ping_delay.min(data_delay) > timeout_ms {
            node.flags |= CLUSTER_NODE_PFAIL;
        }
        let is_pfail = node.has_flag(CLUSTER_NODE_PFAIL);
        Some((!was_pfail && is_pfail, is_pfail, was_pfail != is_pfail))
    }

    pub(crate) fn arm_node_meet(&mut self, node_id: &str) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.meet_pending = true;
        }
    }

    pub(crate) fn complete_node_meet(&mut self, node_id: &str) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.meet_pending = false;
        }
    }

    pub(crate) fn mark_handshake_retry(&mut self, node_id: &str) {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE;
        if let Some(node) = self.nodes.get_mut(node_id) {
            if node.has_flag(CLUSTER_NODE_HANDSHAKE) {
                node.handshake_retry_on_timeout = true;
            }
        }
    }

    pub fn cluster_link_memory(&self) -> usize {
        self.bus_stats.link_memory
    }

    pub(super) fn total_bus_bytes_sent(&self) -> u64 {
        self.bus_stats.total_bytes_sent
    }

    pub(super) fn total_bus_bytes_received(&self) -> u64 {
        self.bus_stats.total_bytes_received
    }

    pub(crate) fn links_buffer_limit_exceeded(&self) -> u64 {
        self.bus_stats.links_buffer_limit_exceeded
    }

    /// Reset all cluster statistics counters (for CONFIG RESETSTAT).
    pub fn reset_cluster_stats(&mut self) {
        self.bus_stats = ClusterBusStats::default();
    }

    /// Reset the cluster state while preserving the node ID and epochs.
    pub fn reset_soft(&mut self) {
        let previously_served = self.served_shard_slots();
        let previous_nodes = self.nodes.keys().cloned().collect::<Vec<_>>();
        // A reset node becomes an independent empty primary. Its previous shard
        // identity may belong to the primary it replicated, so it must move to
        // a fresh shard even though its node identity remains unchanged.
        self.myself.shard_id = generate_node_id();
        self.myself.slots = vec![false; CLUSTER_SLOTS];
        self.myself.numslots = 0;
        self.myself.replicaof.clear();
        self.myself.flags = crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF
            | crate::engine::distributed::cluster::node::CLUSTER_NODE_PRIMARY;
        self.slots = vec![false; CLUSTER_SLOTS];
        self.slot_ownership = SlotOwnershipTable::new();
        self.state = CLUSTER_FAIL;
        self.size = 0;
        self.migrations = MigrationRegistry::default();
        self.pending_migration_actions.clear();
        self.nodes_black_list.clear();
        self.fail_reports.clear();
        self.pending_meets.clear();
        self.pending_forgets.clear();
        self.pending_connects.clear();
        self.pending_link_closes = previous_nodes
            .into_iter()
            .map(|node_id| (node_id, ClusterLinkDirection::All))
            .collect();
        self.pending_topology_broadcast = false;
        self.pending_config_save = false;
        self.pending_config_fsync = false;
        self.topology_state_dirty = false;
        self.pending_dirty_slot_cleanup.clear();
        self.pending_shard_slot_cleanup.clear();
        self.ever_owned_slots = false;
        self.replica_migration_allowed = true;
        self.replica_migration_orphaned_since.clear();
        self.failure_report_node_timeout_ms = 15_000;
        self.last_observed_master_down = false;
        self.last_observed_minority = false;
        self.last_observed_reachability_minority = false;
        self.first_topology_evaluation_ms = None;
        self.automatic_failover.reset();
        self.nodes.clear();
        self.link_views.clear();
        self.manual_failover.reset();
        self.manual_failover_timeout_ms = 5_000;
        self.pending_cluster_actions.clear();
        self.pending_replication_switch = None;
        self.debug_disable_random_ping = false;
        self.debug_disable_reconnection = false;
        self.debug_prevent_slot_migration_pause = false;
        self.debug_prevent_slot_migration_failover = false;
        self.mark_topology_changed();
        self.request_topology_broadcast();
        self.request_lost_shard_slots(&previously_served);
        self.debug_assert_invariants();
    }

    /// Reset the cluster state (hard reset: entirely new state).
    pub fn reset_hard(&mut self, ip: &str, port: u16, cport: u16) {
        let previously_served = self.served_shard_slots();
        let previous_nodes = self.nodes.keys().cloned().collect::<Vec<_>>();
        let next_generation = self.topology_generation().wrapping_add(1);
        let mut new_self = Self::new(ip, port, cport);
        new_self.myself.cluster_replica_priority = self.myself.cluster_replica_priority;
        new_self.gossip_percentage = self.gossip_percentage;
        new_self.bus_stats.link_memory = self.bus_stats.link_memory;
        new_self.config_save_succeeded = self.config_save_succeeded;
        new_self.config_last_save_time_sec = self.config_last_save_time_sec;
        new_self.automatic_failover = self.automatic_failover.create_reset_state();
        new_self.topology_generation = next_generation;
        *self = new_self;
        self.pending_link_closes = previous_nodes
            .into_iter()
            .map(|node_id| (node_id, ClusterLinkDirection::All))
            .collect();
        self.request_topology_broadcast();
        self.request_lost_shard_slots(&previously_served);
        self.debug_assert_invariants();
    }

    pub fn request_topology_broadcast(&mut self) {
        self.pending_topology_broadcast = true;
        self.request_config_save();
    }

    pub fn request_config_save(&mut self) {
        self.pending_config_save = true;
    }

    pub fn request_config_fsync(&mut self) {
        self.pending_config_save = true;
        self.pending_config_fsync = true;
    }

    pub fn queue_cluster_actions(
        &mut self,
        actions: impl IntoIterator<Item = crate::engine::distributed::cluster::action::ClusterAction>,
    ) {
        self.pending_cluster_actions.extend(actions);
    }

    pub fn queue_migration_actions(&mut self, actions: impl IntoIterator<Item = MigrationAction>) {
        self.pending_migration_actions.extend(actions);
    }

    pub(crate) fn queue_meet(&mut self, meet: PendingMeet) {
        self.pending_meets.push(meet);
    }

    pub(crate) fn queue_forget(&mut self, node_id: String) {
        self.pending_forgets.push(node_id);
    }

    pub(crate) fn queue_connect(&mut self, connect: PendingConnect) {
        self.pending_connects.push(connect);
    }

    pub(crate) fn queue_connect_once(&mut self, connect: PendingConnect) {
        if !self.pending_connects_contains(&connect) {
            self.pending_connects.push(connect);
        }
    }

    pub(crate) fn queue_link_close(&mut self, close: (String, ClusterLinkDirection)) {
        self.pending_link_closes.push(close);
    }

    pub(crate) fn queue_link_close_once(&mut self, close: (String, ClusterLinkDirection)) {
        if !self.pending_link_closes_contains(&close) {
            self.pending_link_closes.push(close);
        }
    }

    pub(crate) fn pending_meets_len(&self) -> usize {
        self.pending_meets.len()
    }

    pub(crate) fn retain_pending_meets(&mut self, mut keep: impl FnMut(&PendingMeet) -> bool) {
        self.pending_meets.retain(|meet| keep(meet));
    }

    pub(crate) fn pending_forgets_len(&self) -> usize {
        self.pending_forgets.len()
    }

    pub(crate) fn pending_connects_len(&self) -> usize {
        self.pending_connects.len()
    }

    pub(crate) fn pending_connects(&self) -> &[PendingConnect] {
        &self.pending_connects
    }

    pub(crate) fn pending_connects_contains(&self, connect: &PendingConnect) -> bool {
        self.pending_connects.contains(connect)
    }

    pub(crate) fn retain_pending_connects(
        &mut self,
        mut keep: impl FnMut(&PendingConnect) -> bool,
    ) {
        self.pending_connects.retain(|connect| keep(connect));
    }

    pub(crate) fn pending_link_closes_len(&self) -> usize {
        self.pending_link_closes.len()
    }

    pub(crate) fn pending_link_closes(&self) -> &[(String, ClusterLinkDirection)] {
        &self.pending_link_closes
    }

    pub(crate) fn pending_link_closes_contains(
        &self,
        close: &(String, ClusterLinkDirection),
    ) -> bool {
        self.pending_link_closes.contains(close)
    }

    pub(crate) fn take_pending_meets(&mut self) -> Vec<PendingMeet> {
        std::mem::take(&mut self.pending_meets)
    }

    pub(crate) fn take_pending_forgets(&mut self) -> Vec<String> {
        std::mem::take(&mut self.pending_forgets)
    }

    pub(crate) fn take_pending_connects(&mut self) -> Vec<PendingConnect> {
        std::mem::take(&mut self.pending_connects)
    }

    pub(crate) fn take_pending_link_closes(&mut self) -> Vec<(String, ClusterLinkDirection)> {
        std::mem::take(&mut self.pending_link_closes)
    }

    pub(crate) fn take_migration_actions(&mut self) -> Vec<MigrationAction> {
        std::mem::take(&mut self.pending_migration_actions)
    }

    pub fn clear_migrations_for_load(&mut self) {
        self.migrations = MigrationRegistry::default();
        self.pending_migration_actions.clear();
    }

    pub fn start_migration(
        &mut self,
        job: MigrationJob,
    ) -> Result<Vec<MigrationAction>, MigrationError> {
        let actions = self.migrations.start(job)?;
        self.debug_assert_migration_invariants();
        Ok(actions)
    }

    pub fn start_migrations(
        &mut self,
        jobs: Vec<MigrationJob>,
    ) -> Result<Vec<MigrationAction>, MigrationError> {
        let actions = self.migrations.start_all(jobs)?;
        self.debug_assert_migration_invariants();
        Ok(actions)
    }

    pub fn transition_migration(
        &mut self,
        id: &MigrationJobId,
        event: MigrationEvent,
        now: i64,
    ) -> Result<Vec<MigrationAction>, MigrationError> {
        let actions = self.migrations.transition(id, event, now)?;
        self.debug_assert_migration_invariants();
        Ok(actions)
    }

    pub fn finish_migration(
        &mut self,
        id: &MigrationJobId,
        terminal: TerminalPhase,
        message: Option<String>,
        now: i64,
    ) -> Result<Vec<MigrationAction>, MigrationError> {
        let actions = self.migrations.finish(id, terminal, message, now)?;
        self.debug_assert_migration_invariants();
        Ok(actions)
    }

    pub fn cancel_export_migrations(&mut self, now: i64) -> Vec<MigrationAction> {
        let actions = self.migrations.cancel_exports(now);
        self.debug_assert_migration_invariants();
        actions
    }

    pub fn fail_all_migrations(&mut self, message: &str, now: i64) -> Vec<MigrationAction> {
        let actions = self.migrations.fail_all(message, now);
        self.debug_assert_migration_invariants();
        actions
    }

    pub fn fail_import_migrations(&mut self, message: &str, now: i64) -> Vec<MigrationAction> {
        let actions = self.migrations.fail_imports(message, now);
        self.debug_assert_migration_invariants();
        actions
    }

    pub fn set_migration_remaining_repl_size(&mut self, id: &MigrationJobId, bytes: usize) {
        self.migrations.set_remaining_repl_size(id, bytes);
        self.debug_assert_migration_invariants();
    }

    pub fn trim_migration_history(&mut self, max_len: usize) {
        self.migrations.trim_history(max_len);
        self.debug_assert_migration_invariants();
    }

    pub fn start_manual_failover(
        &mut self,
        mode: crate::engine::distributed::cluster::state::failover::ManualFailoverMode,
        now_ms: i64,
        timeout_ms: i64,
    ) -> Result<(), String> {
        use crate::engine::distributed::cluster::action::Promotion;

        if !self.myself.is_replica() {
            return Err("You should send CLUSTER FAILOVER to a replica".to_string());
        }
        let primary_id =
            crate::engine::distributed::cluster::NodeId::parse(self.myself.replicaof.as_bytes())
                .map_err(|_| "I'm a replica but my master is unknown to me".to_string())?;
        let primary = self
            .nodes
            .get(primary_id.as_str())
            .ok_or_else(|| "I'm a replica but my master is unknown to me".to_string())?;
        if mode
            == crate::engine::distributed::cluster::state::failover::ManualFailoverMode::Coordinated
            && (primary.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL)
                || !self.has_connected_link(
                    primary_id.as_str(),
                    crate::engine::distributed::cluster::ClusterLinkDirection::To,
                ))
        {
            return Err("Master is down or failed, please use CLUSTER FAILOVER FORCE".to_string());
        }
        let epoch = if mode
            == crate::engine::distributed::cluster::state::failover::ManualFailoverMode::Takeover
        {
            self.next_config_epoch()?
        } else {
            self.current_epoch.saturating_add(1)
        };
        let promotion = Promotion {
            old_primary: primary_id.clone(),
            shard_id: crate::engine::distributed::cluster::NodeId::parse(
                primary.shard_id.as_bytes(),
            )
            .map_err(|_| "primary shard ID is invalid".to_string())?,
            slots: primary.slots.clone(),
            config_epoch: epoch,
        };
        if self.automatic_failover.phase()
            != crate::engine::distributed::cluster::state::failover::AutomaticFailoverPhase::Idle
        {
            crate::server_log!(
                warning,
                "Failover election in progress for epoch {}, but received a new manual failover. Resetting the election.",
                self.automatic_failover.election_epoch()
            );
            self.automatic_failover.reset();
        }
        let mut manual = std::mem::take(&mut self.manual_failover);
        let actions = manual.start_replica(mode, primary_id, promotion, now_ms, timeout_ms);
        self.manual_failover = manual;
        self.queue_cluster_actions(actions);
        Ok(())
    }

    pub fn next_config_epoch(&self) -> Result<u64, String> {
        self.nodes
            .values()
            .map(|node| node.config_epoch)
            .fold(self.current_epoch, u64::max)
            .checked_add(1)
            .ok_or_else(|| "config epoch exhausted".to_string())
    }

    pub(crate) fn take_cluster_actions(
        &mut self,
    ) -> Vec<crate::engine::distributed::cluster::action::ClusterAction> {
        std::mem::take(&mut self.pending_cluster_actions)
    }

    pub fn set_debug_disable_random_ping(&mut self, disabled: bool) {
        self.debug_disable_random_ping = disabled;
    }

    pub fn set_manual_failover_timeout_ms(&mut self, timeout_ms: i64) {
        self.manual_failover_timeout_ms = timeout_ms.max(1);
    }

    pub(crate) fn manual_failover_timeout_ms(&self) -> i64 {
        self.manual_failover_timeout_ms
    }

    pub(crate) fn debug_disable_random_ping(&self) -> bool {
        self.debug_disable_random_ping
    }

    pub fn set_debug_disable_reconnection(&mut self, disabled: bool) {
        self.debug_disable_reconnection = disabled;
    }

    pub(crate) fn debug_disable_reconnection(&self) -> bool {
        self.debug_disable_reconnection
    }

    pub fn set_debug_prevent_slot_migration_pause(&mut self, prevent: bool) {
        self.debug_prevent_slot_migration_pause = prevent;
    }

    pub fn debug_prevent_slot_migration_pause(&self) -> bool {
        self.debug_prevent_slot_migration_pause
    }

    pub fn set_debug_prevent_slot_migration_failover(&mut self, prevent: bool) {
        self.debug_prevent_slot_migration_failover = prevent;
    }

    pub fn debug_prevent_slot_migration_failover(&self) -> bool {
        self.debug_prevent_slot_migration_failover
    }

    pub fn take_manual_failover_actions(
        &mut self,
    ) -> Vec<crate::engine::distributed::cluster::action::ClusterAction> {
        self.take_cluster_actions()
    }

    pub(crate) fn take_config_save_request(&mut self) -> bool {
        std::mem::take(&mut self.pending_config_save)
    }

    pub(crate) fn take_config_fsync_request(&mut self) -> bool {
        std::mem::take(&mut self.pending_config_fsync)
    }

    pub fn topology_generation(&self) -> u64 {
        self.topology_generation
    }

    pub fn debug_snapshot(&self) -> ClusterDebugSnapshot {
        self.projection().debug_snapshot()
    }

    pub(crate) fn build_debug_snapshot(&self) -> ClusterDebugSnapshot {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_FAIL, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_NOADDR,
        };

        let mut links = self.link_views.values().cloned().collect::<Vec<_>>();
        links.sort_unstable_by(|left, right| {
            left.node_id
                .as_str()
                .cmp(right.node_id.as_str())
                .then(left.direction.cmp(&right.direction))
        });
        let nodes = std::iter::once(&self.myself)
            .chain(self.nodes.values().filter(|node| !node.is_myself()));
        // Resolve the same owners as management topology without allocating
        // node IDs, bitmaps, or shard views for a diagnostic count.
        let mut previous_owner = None;
        let mut has_reusable_owner = false;
        let assigned_slot_count = (0..CLUSTER_SLOTS)
            .filter(|&slot| {
                let owner = &self.slot_ownership.owners[slot];
                if previous_owner != Some(owner) || !has_reusable_owner {
                    previous_owner = Some(owner);
                    // A healthy canonical binding is assigned regardless of
                    // withdrawal. Reuse that fact within a contiguous range;
                    // failed bindings still need slot-specific resolution.
                    has_reusable_owner = self.canonical_slot_owner_ref(slot).is_some_and(|node| {
                        !node.has_flag(CLUSTER_NODE_FAIL)
                            && (node.is_myself()
                                || !node.has_flag(CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR))
                    });
                }
                has_reusable_owner
                    || self.slot_owner_ref(slot).is_some()
                    || (!self.require_full_coverage && self.declared_node_by_slot(slot).is_some())
            })
            .count();
        ClusterDebugSnapshot {
            topology_generation: self.topology_generation,
            health: if self.state == CLUSTER_OK {
                ClusterHealth::Ok
            } else {
                ClusterHealth::Fail
            },
            current_epoch: self.current_epoch,
            node_count: nodes.clone().count(),
            primary_count: nodes.filter(|node| !node.is_replica()).count(),
            assigned_slot_count,
            link_count: links.len(),
            links,
            pending: ClusterPendingWork {
                meets: self.pending_meets_len(),
                forgets: self.pending_forgets_len(),
                connects: self.pending_connects_len(),
                link_closes: self.pending_link_closes_len(),
                actions: self.pending_cluster_actions.len(),
                migration_actions: self.pending_migration_actions.len(),
                topology_broadcast: self.pending_topology_broadcast,
                config_save: self.pending_config_save,
                config_fsync: self.pending_config_fsync,
                replication_switch: self.pending_replication_switch.is_some(),
            },
            active: ClusterActiveOperations {
                migrations: self
                    .migrations
                    .views()
                    .into_iter()
                    .filter(|job| !job.phase.is_terminal())
                    .count(),
                automatic_failover: self.automatic_failover.phase()
                    != super::failover::AutomaticFailoverPhase::Idle
                    && !self.automatic_failover.is_manual(),
                manual_failover: self.manual_failover.is_active()
                    || (self.automatic_failover.phase()
                        != super::failover::AutomaticFailoverPhase::Idle
                        && self.automatic_failover.is_manual()),
            },
        }
    }

    pub fn mark_topology_changed(&mut self) {
        self.topology_generation = self.topology_generation().saturating_add(1);
        self.topology_state_dirty = true;
        self.request_config_save();
    }

    #[cfg(test)]
    pub(crate) fn topology_state_is_dirty(&self) -> bool {
        self.topology_state_dirty
    }

    pub(crate) fn observe_topology_evaluation(&mut self, now_ms: i64) {
        self.first_topology_evaluation_ms.get_or_insert(now_ms);
    }

    pub(crate) fn safe_to_vote(&self, now_ms: i64) -> bool {
        // C clusterUpdateState only keeps `safe_to_join` false while a local
        // primary is still in CLUSTER_FAIL during the initial writable-delay
        // window. Once topology is healthy (or this node is a replica), it may
        // vote immediately.
        if !self.myself.is_primary() || self.state != CLUSTER_FAIL {
            return true;
        }
        let Some(first) = self.first_topology_evaluation_ms else {
            return false;
        };
        now_ms.saturating_sub(first) >= 2_000
    }

    pub fn bump_topology_generation(&mut self) {
        self.mark_topology_changed();
        self.request_config_save();
    }

    pub(crate) fn take_topology_broadcast_request(&mut self) -> bool {
        std::mem::take(&mut self.pending_topology_broadcast)
    }

    pub(crate) fn request_dirty_slot_cleanup(&mut self, slot: usize) {
        if !self.pending_dirty_slot_cleanup.contains(&slot) {
            self.pending_dirty_slot_cleanup.push(slot);
        }
    }

    pub(crate) fn take_dirty_slot_cleanup_requests(&mut self) -> Vec<usize> {
        std::mem::take(&mut self.pending_dirty_slot_cleanup)
    }

    pub(crate) fn mark_slot_owned(&mut self) {
        self.ever_owned_slots = true;
    }

    pub(crate) fn has_ever_owned_slots(&self) -> bool {
        self.ever_owned_slots
    }

    pub(crate) fn set_replica_migration_allowed(&mut self, is_allowed: bool) {
        self.replica_migration_allowed = is_allowed;
    }

    pub(crate) fn is_replica_migration_allowed(&self) -> bool {
        self.replica_migration_allowed
    }

    pub(crate) fn orphaned_primary_duration_ms(&mut self, node_id: &str, now: i64) -> i64 {
        let first_seen = self
            .replica_migration_orphaned_since
            .entry(node_id.to_string())
            .or_insert(now);
        now.saturating_sub(*first_seen)
    }

    pub(crate) fn retain_orphaned_primary(&mut self, node_id: Option<&str>) {
        self.replica_migration_orphaned_since
            .retain(|known_id, _| Some(known_id.as_str()) == node_id);
    }

    pub(crate) fn retain_orphaned_primaries(&mut self, node_ids: &BTreeSet<String>) {
        self.replica_migration_orphaned_since
            .retain(|known_id, _| node_ids.contains(known_id));
    }

    pub(crate) fn set_failure_report_node_timeout_ms(&mut self, node_timeout_ms: u64) {
        self.failure_report_node_timeout_ms = node_timeout_ms;
    }

    pub(crate) fn failure_report_node_timeout_ms(&self) -> u64 {
        self.failure_report_node_timeout_ms
    }

    pub(crate) fn expire_handshake_nodes(
        &mut self,
        now_ms: i64,
        node_timeout_ms: u64,
    ) -> Vec<String> {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE;

        // Match C getHandshakeTimeout without changing normal failure detection.
        let timeout_ms = node_timeout_ms.max(1_000).min(i64::MAX as u64) as i64;
        self.handshake_cooldowns.retain(|_, until| *until > now_ms);
        let expired: Vec<String> = self
            .nodes
            .values()
            .filter(|node| {
                let handshake = node.has_flag(CLUSTER_NODE_HANDSHAKE);
                let age = now_ms.saturating_sub(node.created_at);
                handshake && node.created_at > 0 && age > timeout_ms
            })
            .map(|node| node.name.clone())
            .collect();
        for node_id in &expired {
            if let Some(node) = self.nodes.remove(node_id) {
                let endpoint = (node.ip().to_string(), node.port(), node.cport());
                self.handshake_cooldowns
                    .insert(endpoint, now_ms.saturating_add(timeout_ms));
                // Only a retry-marked outbound MEET may restart anonymously.
                // Inbound placeholders expire without creating reciprocal MEETs.
                if node.handshake_retry_on_timeout {
                    // Keep the full endpoint on a fresh temporary identity so
                    // pending connects can register a MEET handshake.
                    let handshake_id =
                        self.ensure_handshake_node(node.ip(), node.port(), node.cport(), now_ms);
                    self.mark_handshake_retry(&handshake_id);
                    let retry = (node.ip().to_string(), node.cport(), true);
                    self.queue_connect_once(retry);
                }
            }
            self.observe_link_close(node_id, ClusterLinkDirection::All);
        }
        if !expired.is_empty() {
            self.mark_topology_changed();
        }
        expired
    }

    pub(crate) fn ensure_handshake_node(
        &mut self,
        ip: &str,
        port: u16,
        bus_port: u16,
        now_ms: i64,
    ) -> String {
        use crate::engine::distributed::cluster::node::{
            generate_node_id, ClusterNode, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_MYSELF,
            CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };

        if let Some(node) = self.nodes.values().find(|node| {
            node.has_flag(CLUSTER_NODE_HANDSHAKE)
                && node.ip() == ip
                && node.port() == port
                && node.cport() == bus_port
        }) {
            return node.name.clone();
        }
        let node_id = generate_node_id();
        self.handshake_cooldowns
            .remove(&(ip.to_string(), port, bus_port));
        let mut node = ClusterNode::new(node_id.clone(), ip.to_string(), port, bus_port, true);
        node.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY | CLUSTER_NODE_REPLICA);
        node.flags |= CLUSTER_NODE_HANDSHAKE;
        node.created_at = now_ms;
        self.nodes.insert(node_id.clone(), node);
        self.mark_topology_changed();
        node_id
    }

    pub(crate) fn clear_handshake_cooldown(&mut self, ip: &str, port: u16, bus_port: u16) {
        self.handshake_cooldowns
            .remove(&(ip.to_string(), port, bus_port));
    }

    pub(crate) fn remove_handshake_node(&mut self, node_id: &str) {
        let is_handshake = self.nodes.get(node_id).is_some_and(|node| {
            node.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE)
        });
        if is_handshake {
            self.nodes.remove(node_id);
            self.observe_link_close(node_id, ClusterLinkDirection::All);
            self.mark_topology_changed();
        }
    }
}

fn debug_packet_type(message_type: ClusterMsgType) -> i64 {
    match message_type {
        ClusterMsgType::Ping => 0,
        ClusterMsgType::Pong => 1,
        ClusterMsgType::Meet => 2,
        ClusterMsgType::Fail => 3,
        ClusterMsgType::Publish => 4,
        ClusterMsgType::FailoverAuthRequest => 5,
        ClusterMsgType::FailoverAuthAck => 6,
        ClusterMsgType::Update => 7,
        ClusterMsgType::MfStart => 8,
        ClusterMsgType::Module => 9,
        ClusterMsgType::PublishShard => 10,
        ClusterMsgType::FailoverAuthNack => 11,
    }
}

pub(super) fn short_name(name: &str) -> String {
    if name.len() <= 8 {
        name.to_string()
    } else {
        name[..8].to_string()
    }
}

pub(super) fn role_str(
    node: &crate::engine::distributed::cluster::node::ClusterNode,
) -> &'static str {
    if node.is_primary() {
        "primary"
    } else if node.is_replica() {
        "replica"
    } else {
        "norole"
    }
}

#[cfg(test)]
mod tests {
    use super::ClusterState;
    use crate::engine::distributed::cluster::msg::ClusterMsgType;
    use crate::engine::distributed::cluster::node::ClusterNode;
    use crate::engine::distributed::cluster::ClusterLinkDirection;

    #[test]
    fn named_failover_events_keep_machine_state_owned() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

        state.commit_manual_primary_pause();
        let facts = crate::engine::distributed::cluster::state::failover::AutomaticFailoverFacts {
            now_ms: 1,
            node_timeout_ms: 1_000,
            current_epoch: 0,
            voting_primaries: 0,
            failed_primary_rank: 0,
            all_replicas_primary_failed: false,
            local: None,
            primary: None,
            siblings: Vec::new(),
            manual: false,
        };
        state.automatic_failover_on_tick(&facts);

        assert!(!state.manual_failover.is_active());
        assert_eq!(
            state.automatic_failover.phase(),
            crate::engine::distributed::cluster::state::failover::AutomaticFailoverPhase::Idle
        );
    }

    #[test]
    fn runtime_local_observation_does_not_change_topology_generation() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let generation = state.topology_generation();

        state.observe_local_replication(42, 90);

        assert_eq!(state.myself.repl_offset, 42);
        assert_eq!(state.myself.replica_priority, 90);
        assert_eq!(state.topology_generation(), generation);
    }

    #[test]
    fn pending_outbox_api_preserves_duplicate_fifo_and_supports_explicit_deduplication() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let meet = ("node.example".to_string(), 6380, 16380);
        let forget = "node-id".to_string();
        let connect = ("node.example".to_string(), 16380, true);
        let close = ("node-id".to_string(), ClusterLinkDirection::All);

        state.queue_meet(meet.clone());
        state.queue_meet(meet.clone());
        state.queue_forget(forget.clone());
        state.queue_forget(forget.clone());
        state.queue_connect(connect.clone());
        state.queue_connect(connect.clone());
        state.queue_link_close(close.clone());
        state.queue_link_close(close.clone());

        assert_eq!(state.take_pending_meets(), vec![meet.clone(), meet]);
        assert_eq!(state.take_pending_forgets(), vec![forget.clone(), forget]);
        assert!(state.pending_connects_contains(&connect));
        assert_eq!(
            state.take_pending_connects(),
            vec![connect.clone(), connect.clone()]
        );
        assert!(state.pending_link_closes_contains(&close));
        assert_eq!(
            state.take_pending_link_closes(),
            vec![close.clone(), close.clone()]
        );

        state.queue_connect_once(connect.clone());
        state.queue_connect_once(connect.clone());
        state.queue_link_close_once(close.clone());
        state.queue_link_close_once(close.clone());

        assert_eq!(state.take_pending_connects(), vec![connect]);
        assert_eq!(state.take_pending_link_closes(), vec![close]);
    }

    #[test]
    fn soft_reset_preserves_node_id_but_moves_to_a_new_shard() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let original_name = state.myself.name.clone();
        let original_shard_id = state.myself.shard_id.clone();

        state.reset_soft();

        assert_eq!(state.myself.name, original_name);
        assert_ne!(state.myself.shard_id, original_shard_id);
        assert_eq!(state.myself.shard_id.len(), 40);
    }

    #[test]
    fn soft_reset_requests_persistence_and_immediate_topology_broadcast() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

        state.reset_soft();

        assert!(state.take_config_save_request());
        assert!(state.take_topology_broadcast_request());
    }

    #[test]
    fn soft_reset_queues_all_previous_peer_links_for_close() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut peer = ClusterNode::new(
            "peer".to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        peer.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;
        state.nodes.insert(peer.name.clone(), peer);

        state.reset_soft();

        assert_eq!(
            state.pending_link_closes,
            vec![("peer".to_string(), super::ClusterLinkDirection::All)]
        );
    }

    #[test]
    fn hard_reset_requests_persistence_and_immediate_topology_broadcast() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

        state.reset_hard("127.0.0.1", 6379, 16379);

        assert!(state.take_config_save_request());
        assert!(state.take_topology_broadcast_request());
    }

    #[test]
    fn topology_changes_request_config_save_once() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

        assert!(!state.take_config_save_request());
        state.mark_topology_changed();
        assert!(state.take_config_save_request());
        assert!(!state.take_config_save_request());

        state.request_topology_broadcast();
        assert!(state.take_config_save_request());
    }

    #[test]
    fn removing_primary_orphans_its_known_replicas() {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };

        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let replica_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        let mut primary = ClusterNode::new(
            primary_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        primary.flags &= !CLUSTER_NODE_MYSELF;
        let mut replica = ClusterNode::new(
            replica_id.to_string(),
            "127.0.0.1".to_string(),
            6381,
            16381,
            false,
        );
        replica.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
        replica.flags |= CLUSTER_NODE_REPLICA;
        replica.replicaof = primary_id.to_string();
        state.nodes.insert(primary_id.to_string(), primary);
        state.nodes.insert(replica_id.to_string(), replica);

        assert!(state.remove_node(primary_id).is_some());

        assert!(state.nodes[replica_id].replicaof.is_empty());
    }

    #[test]
    fn debug_packet_filter_uses_c_message_type_numbers() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

        state.set_debug_packet_filter(0);
        assert!(state.should_drop_packet(ClusterMsgType::Ping));
        assert!(!state.should_drop_packet(ClusterMsgType::Pong));

        state.set_debug_packet_filter(1);
        assert!(state.should_drop_packet(ClusterMsgType::Pong));
        assert!(!state.should_drop_packet(ClusterMsgType::Meet));

        state.set_debug_packet_filter(2);
        assert!(state.should_drop_packet(ClusterMsgType::Meet));
        assert!(!state.should_drop_packet(ClusterMsgType::Ping));
    }

    #[test]
    fn debug_packet_filter_supports_none_all_and_close_link() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

        state.set_debug_packet_filter(-1);
        assert!(!state.should_drop_packet(ClusterMsgType::Ping));

        state.set_debug_packet_filter(-2);
        assert!(state.should_drop_packet(ClusterMsgType::Ping));
        assert!(state.should_drop_packet(ClusterMsgType::Pong));

        state.set_debug_close_link_on_packet_drop(true);
        assert!(state.should_close_link_on_packet_drop());
    }

    #[test]
    fn cluster_info_reports_bus_message_counters() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

        state.record_bus_message_sent(ClusterMsgType::Meet);
        state.record_bus_message_received(ClusterMsgType::Meet);
        state.record_bus_message_received(ClusterMsgType::Ping);
        state.record_bus_message_sent(ClusterMsgType::Publish);
        state.record_bus_message_received(ClusterMsgType::Publish);
        state.record_bus_frame_sent(ClusterMsgType::Publish, 123);
        state.record_bus_frame_received(ClusterMsgType::Publish, 123);

        let info = state.info_str();
        assert!(info.contains("cluster_stats_messages_meet_sent:1\r\n"));
        assert!(info.contains("cluster_stats_messages_meet_received:1\r\n"));
        assert!(info.contains("cluster_stats_messages_ping_received:1\r\n"));
        assert!(info.contains("cluster_stats_messages_publish_sent:1\r\n"));
        assert!(info.contains("cluster_stats_messages_publish_received:1\r\n"));
        assert!(info.contains("cluster_stats_messages_sent:2\r\n"));
        assert!(info.contains("cluster_stats_messages_received:3\r\n"));
        assert!(info.contains("cluster_stats_pubsub_bytes_sent:123\r\n"));
        assert!(info.contains("cluster_stats_pubsub_bytes_received:123\r\n"));
        assert_eq!(state.total_bus_bytes_sent(), 0);
        assert_eq!(state.total_bus_bytes_received(), 123);
    }

    #[test]
    fn slot_ownership_history_survives_becoming_empty() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        assert!(!state.has_ever_owned_slots());

        state.add_slot(0).unwrap();
        state.del_slot(0).unwrap();

        assert!(state.has_ever_owned_slots());
    }

    #[test]
    fn dirty_slot_cleanup_requests_are_deduplicated_and_drained() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

        state.request_dirty_slot_cleanup(7);
        state.request_dirty_slot_cleanup(7);

        assert_eq!(state.take_dirty_slot_cleanup_requests(), vec![7]);
        assert!(state.take_dirty_slot_cleanup_requests().is_empty());
    }

    #[test]
    fn orphaned_primary_duration_requires_continuous_observation() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

        assert_eq!(state.orphaned_primary_duration_ms("target", 1_000), 0);
        assert_eq!(state.orphaned_primary_duration_ms("target", 6_001), 5_001);
        state.retain_orphaned_primary(None);
        assert_eq!(state.orphaned_primary_duration_ms("target", 7_000), 0);
    }

    #[test]
    fn expired_handshake_nodes_use_at_least_one_second() {
        use crate::engine::distributed::cluster::link::{ClusterLinkView, LinkEvents};
        use crate::engine::distributed::cluster::NodeId;

        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let handshake_id = state.ensure_handshake_node("127.0.0.1", 6380, 16380, 1_000);
        for direction in [ClusterLinkDirection::To, ClusterLinkDirection::From] {
            state.observe_link_open(
                ClusterLinkView::new(
                    NodeId::parse(handshake_id.as_bytes()).unwrap(),
                    direction,
                    1_000,
                    LinkEvents::READABLE,
                )
                .with_temporary_handshake(true),
            );
        }
        assert_eq!(state.link_views().count(), 2);

        assert_eq!(
            state.expire_handshake_nodes(1_500, 500),
            Vec::<String>::new()
        );
        assert_eq!(
            state.expire_handshake_nodes(1_501, 500),
            Vec::<String>::new()
        );
        assert_eq!(
            state.expire_handshake_nodes(2_000, 500),
            Vec::<String>::new()
        );
        assert_eq!(state.link_views().count(), 2);
        assert_eq!(
            state.expire_handshake_nodes(2_001, 500),
            vec![handshake_id.clone()]
        );
        assert!(!state.nodes.contains_key(&handshake_id));
        assert!(state.pending_connects.is_empty());
        assert_eq!(state.link_views().count(), 0);
    }

    #[test]
    fn expired_retry_marked_handshake_schedules_reciprocal_meet() {
        use crate::engine::distributed::cluster::node::{
            ClusterNode, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_MYSELF,
        };

        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut handshake = ClusterNode::new(
            "handshake".to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        handshake.flags &= !CLUSTER_NODE_MYSELF;
        handshake.flags |= CLUSTER_NODE_HANDSHAKE;
        handshake.created_at = 1_000;
        handshake.handshake_retry_on_timeout = true;
        state.nodes.insert(handshake.name.clone(), handshake);

        assert_eq!(
            state.expire_handshake_nodes(2_001, 500),
            vec!["handshake".to_string()]
        );
        assert_eq!(
            state.pending_connects,
            vec![("127.0.0.1".to_string(), 16380, true)]
        );
    }

    #[test]
    fn retry_marked_handshake_uses_configured_timeout_above_one_second() {
        use crate::engine::distributed::cluster::node::{
            ClusterNode, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_MYSELF,
        };

        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut handshake = ClusterNode::new(
            "gossip-handshake".to_string(),
            "127.0.0.1".to_string(),
            6381,
            16381,
            true,
        );
        handshake.flags &= !CLUSTER_NODE_MYSELF;
        handshake.flags |= CLUSTER_NODE_HANDSHAKE;
        handshake.created_at = 2_000;
        handshake.handshake_retry_on_timeout = true;
        state.nodes.insert(handshake.name.clone(), handshake);

        assert_eq!(
            state.expire_handshake_nodes(6_000, 4_000),
            Vec::<String>::new()
        );
        assert_eq!(
            state.expire_handshake_nodes(6_001, 4_000),
            vec!["gossip-handshake".to_string()]
        );
        assert_eq!(
            state.pending_connects,
            vec![("127.0.0.1".to_string(), 16381, true)]
        );
    }

    #[test]
    fn zero_and_one_ms_timeouts_keep_handshake_for_one_second() {
        for timeout_ms in [0, 1] {
            let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
            let handshake_id = state.ensure_handshake_node("127.0.0.1", 6380, 16380, 1_000);

            for now_ms in [1_001, 1_002, 1_999, 2_000] {
                assert!(
                    state.expire_handshake_nodes(now_ms, timeout_ms).is_empty(),
                    "timeout={timeout_ms}, now={now_ms}"
                );
                assert!(state.nodes.contains_key(&handshake_id));
            }
            assert_eq!(
                state.expire_handshake_nodes(2_001, timeout_ms),
                vec![handshake_id]
            );
            assert!(state.pending_connects.is_empty());
        }
    }

    #[test]
    fn anonymous_handshake_expiry_does_not_queue_meet() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let handshake_id = state.ensure_handshake_node("127.0.0.1", 6380, 16380, 1_000);
        assert!(!state.nodes[&handshake_id].handshake_retry_on_timeout);

        assert_eq!(
            state.expire_handshake_nodes(2_001, 1_000),
            vec![handshake_id]
        );
        assert!(state.pending_connects.is_empty());
        assert!(state.expire_handshake_nodes(3_002, 1_000).is_empty());
        assert!(state.pending_connects.is_empty());
    }

    #[test]
    fn hard_reset_preserves_config_save_status_until_an_actual_save_completes() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.config_save_succeeded = false;
        state.config_last_save_time_sec = 123456;

        state.reset_hard("127.0.0.1", 6379, 16379);

        assert!(!state.config_save_succeeded);
        assert_eq!(state.config_last_save_time_sec, 123456);
    }

    #[test]
    fn handshake_window_does_not_delay_normal_node_failure_detection() {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;

        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut peer = ClusterNode::new(
            "peer".to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        peer.flags &= !CLUSTER_NODE_MYSELF;
        peer.ping_sent = 1_000;
        state.nodes.insert(peer.name.clone(), peer);

        assert_eq!(
            state.observe_node_failure_timeout("peer", 1_001, 1_000, 1),
            Some((false, false, false))
        );
        assert_eq!(
            state.observe_node_failure_timeout("peer", 1_002, 1_000, 1),
            Some((true, true, true))
        );
    }
}
