use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::SocketAddr;

use crate::engine::distributed::cluster::action::{ClusterAction, ClusterActionBatch};
use crate::engine::distributed::cluster::link::LinkEvents;
use crate::engine::distributed::cluster::msg::SLOT_BYTES;
use crate::foundation::io::sys::time::mstime;
use bytes::Bytes;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceivedClusterPublish {
    pub channel: Bytes,
    pub payload: Bytes,
    pub shard: bool,
}

/// Cluster bus connection state.
pub struct BusConnection {
    /// The runtime token for the transport driver.
    #[allow(dead_code)]
    pub token: crate::engine::command::context::Token,
    /// Remote address.
    pub addr: SocketAddr,
    /// Read buffer (accumulated data).
    pub read_buf: Vec<u8>,
    /// Write buffer (pending data to send).
    pub write_buf: Vec<u8>,
    /// Whether this is an outgoing connection (initiated by MEET or auto-discovery).
    pub is_outgoing: bool,
    /// If true, send MEET instead of PING on first write.
    pub send_meet: bool,
    /// Confirmed remote node identity. Temporary handshake IDs never belong
    /// here because this field drives normal link projection and reconnects.
    pub node_name: String,
    /// Temporary random handshake identity that owns this socket until
    /// directional PING/PONG confirmation or expiry.
    pub handshake_node_name: String,
    /// Last node identity projected into ClusterState.
    pub projected_node_name: String,
    /// Last time we received data (ms).
    pub last_recv: i64,
    /// Connection creation time (ms).
    pub created_at: i64,
    /// Last time a MEET frame was queued on this outgoing link (ms).
    pub meet_sent_at: i64,
    /// Last time bytes were written (ms).
    pub last_send: i64,
    /// Monotonic bytes read from this socket.
    pub bytes_received: u64,
    /// Monotonic bytes written to this socket.
    pub bytes_sent: u64,
    /// Application bytes sent since the peer last produced any bus traffic.
    /// This closes the gap where a stopped process is still ACKed by its kernel
    /// and therefore has no visible userspace/TIOCOUTQ backlog yet.
    pub bytes_sent_since_read: usize,
    /// Current runtime readiness registration.
    pub events: LinkEvents,
    /// Whether we've sent a message (for handshake).
    pub handshake_sent: bool,
    /// Whether the connection is established (TCP connected).
    pub connected: bool,
}

impl BusConnection {
    pub fn new(
        token: crate::engine::command::context::Token,
        addr: SocketAddr,
        is_outgoing: bool,
    ) -> Self {
        Self {
            token,
            addr,
            read_buf: Vec::with_capacity(4096),
            write_buf: Vec::with_capacity(4096),
            is_outgoing,
            send_meet: false,
            node_name: String::new(),
            handshake_node_name: String::new(),
            projected_node_name: String::new(),
            last_recv: 0,
            created_at: mstime(),
            meet_sent_at: 0,
            last_send: 0,
            bytes_received: 0,
            bytes_sent: 0,
            bytes_sent_since_read: 0,
            events: LinkEvents::READABLE,
            handshake_sent: false,
            connected: false,
        }
    }

    pub fn direction(&self) -> crate::engine::distributed::cluster::state::ClusterLinkDirection {
        if self.is_outgoing {
            crate::engine::distributed::cluster::state::ClusterLinkDirection::To
        } else {
            crate::engine::distributed::cluster::state::ClusterLinkDirection::From
        }
    }
}

/// Cluster bus manager.
pub struct ClusterBusManager {
    /// Bus connections (token_value -> connection).
    pub connections: HashMap<usize, BusConnection>,
    /// Tokens of the Cluster Bus listeners, one per successfully bound address family.
    pub listener_tokens: Vec<crate::engine::command::context::Token>,
    /// Last time the once-per-second random probe was considered (ms).
    pub last_random_ping_time: i64,
    /// Last display identity traced for each peer. Rust keeps directional
    /// links, so a short-lived outbound link may never be selected by the
    /// random probe; targeted probes log only a new identity, never each ping.
    pub(super) ping_trace_displays: HashMap<String, String>,
    /// Maximum idle PONG interval before starting a targeted probe (ms).
    pub ping_interval: i64,
    /// Validated Cluster Bus publish events waiting for runtime delivery.
    pub pending_publishes: Vec<ReceivedClusterPublish>,
    /// Offset used to rotate the regular gossip sample across all nodes.
    pub gossip_cursor: usize,
    /// Failover actions produced by receive-path state-machine events. L5
    /// drains these in the same readable turn; cron is the fallback owner if a
    /// lower-level caller invokes `readable` without the event-loop wrapper.
    pub(super) pending_failover_actions: Vec<ClusterAction>,
    /// Cached local primary bitmap for PING/PONG/MEET construction. C keeps
    /// this bitmap in the node object; rebuilding it from `Vec<bool>` for
    /// every heartbeat is prohibitively expensive in large clusters.
    pub(super) cached_advertised_slots_generation: Cell<Option<u64>>,
    pub(super) cached_advertised_slots: RefCell<Vec<u8>>,
}

impl ClusterBusManager {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            listener_tokens: Vec::new(),
            last_random_ping_time: 0,
            ping_trace_displays: HashMap::new(),
            ping_interval: 1000,
            pending_publishes: Vec::new(),
            gossip_cursor: 0,
            pending_failover_actions: Vec::new(),
            cached_advertised_slots_generation: Cell::new(None),
            cached_advertised_slots: RefCell::new(vec![0; SLOT_BYTES]),
        }
    }

    pub(crate) fn drain_receive_failover_actions(
        &mut self,
        state: &mut crate::engine::distributed::cluster::state::ClusterState,
        now_ms: i64,
    ) -> ClusterActionBatch {
        let actions = std::mem::take(&mut self.pending_failover_actions);
        self.execute_failover_actions(state, actions, now_ms, None)
    }

    /// Check if a token belongs to a cluster bus connection.
    pub fn is_bus_token(&self, token: crate::engine::command::context::Token) -> bool {
        self.connections.contains_key(&token.0) || self.is_listener_token(token)
    }

    pub fn is_listener_token(&self, token: crate::engine::command::context::Token) -> bool {
        self.listener_tokens.contains(&token)
    }

    /// Register a new incoming bus connection (accepted from listener).
    pub fn register_incoming(
        &mut self,
        token: crate::engine::command::context::Token,
        addr: SocketAddr,
    ) {
        let mut conn = BusConnection::new(token, addr, false);
        conn.connected = true;
        self.connections.insert(token.0, conn);
    }

    pub fn register_outgoing_for_node(
        &mut self,
        token: crate::engine::command::context::Token,
        addr: SocketAddr,
        send_meet: bool,
        node_name: String,
    ) {
        let mut conn = BusConnection::new(token, addr, true);
        conn.connected = true;
        conn.events = LinkEvents::READ_WRITE;
        conn.send_meet = send_meet;
        conn.node_name = node_name;
        self.connections.insert(token.0, conn);
    }

    pub fn register_outgoing_for_handshake(
        &mut self,
        token: crate::engine::command::context::Token,
        addr: SocketAddr,
        handshake_id: String,
    ) {
        let mut conn = BusConnection::new(token, addr, true);
        conn.connected = true;
        conn.events = LinkEvents::READ_WRITE;
        conn.send_meet = true;
        conn.handshake_node_name = handshake_id;
        self.connections.insert(token.0, conn);
    }

    pub fn arm_meet_for_addr(&mut self, addr: SocketAddr) -> Option<usize> {
        let (token, connection) = self
            .connections
            .iter_mut()
            .find(|(_, connection)| connection.is_outgoing && connection.addr == addr)?;
        connection.send_meet = true;
        connection.handshake_sent = false;
        connection.events = LinkEvents::READ_WRITE;
        Some(*token)
    }

    /// Requeue a normal outbound connection so the node-level MEET state can
    /// choose its next initial frame without turning that state into a
    /// connection-local flag.
    pub fn rearm_outgoing_for_node(&mut self, addr: SocketAddr, node_name: &str) -> Option<usize> {
        let (token, connection) = self.connections.iter_mut().find(|(_, connection)| {
            connection.is_outgoing && connection.addr == addr && connection.node_name == node_name
        })?;
        connection.handshake_sent = false;
        connection.events = LinkEvents::READ_WRITE;
        Some(*token)
    }

    /// Remove a bus connection.
    pub fn remove(&mut self, token_value: usize) {
        self.connections.remove(&token_value);
    }

    pub fn remove_and_schedule_retry(
        &mut self,
        state: &mut crate::engine::distributed::cluster::state::ClusterState,
        token_value: usize,
    ) {
        let Some(connection) = self.connections.remove(&token_value) else {
            return;
        };
        if !connection.is_outgoing || !connection.send_meet {
            return;
        }
        let handshake_incomplete =
            !connection.handshake_node_name.is_empty() || connection.node_name.is_empty();
        if !handshake_incomplete {
            return;
        }
        let retry = (
            connection.addr.ip().to_string(),
            connection.addr.port(),
            true,
        );
        state.queue_connect_once(retry);
    }

    pub fn schedule_asymmetric_meets(
        &mut self,
        state: &mut crate::engine::distributed::cluster::state::ClusterState,
        now_ms: i64,
        node_timeout_ms: u64,
    ) {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_NOADDR,
        };

        // Use the same minimum window as C getHandshakeTimeout.
        let timeout_ms = node_timeout_ms.max(1_000).min(i64::MAX as u64) as i64;
        for connection in self.connections.values_mut().filter(|connection| {
            connection.is_outgoing
                && connection.send_meet
                && connection.handshake_sent
                && connection.meet_sent_at > 0
                && now_ms.saturating_sub(connection.meet_sent_at) > timeout_ms
                && (!connection.handshake_node_name.is_empty() || connection.node_name.is_empty())
        }) {
            connection.handshake_sent = false;
        }
        let node_ids: Vec<String> = state
            .nodes
            .values()
            .filter(|node| {
                let no_inbound_since = if node.inbound_link_missing_since > 0 {
                    node.inbound_link_missing_since
                } else {
                    // Existing trusted-gossip repair records were created
                    // before the node-level inbound-link timestamp existed.
                    // Retain their discovery time as the initial absence
                    // clock; subsequent cron passes keep the dedicated field.
                    node.asymmetric_meet_started_at
                };
                !node.has_flag(CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR)
                    && no_inbound_since > 0
                    && now_ms.saturating_sub(no_inbound_since) > timeout_ms
                    && (node.meet_sent_at == 0
                        || now_ms.saturating_sub(node.meet_sent_at) > timeout_ms)
                    && (node.asymmetric_meet_started_at > 0
                        || self.connections.values().any(|connection| {
                            connection.is_outgoing
                                && connection.connected
                                && connection.node_name == node.name
                        }))
                    && !self.connections.values().any(|connection| {
                        !connection.is_outgoing
                            && connection.connected
                            && connection.node_name == node.name
                    })
            })
            .map(|node| node.name.clone())
            .collect();
        for node_id in node_ids {
            state.arm_node_meet(&node_id);
            let closing_outbound = state.pending_link_closes_contains(&(
                node_id.clone(),
                crate::engine::distributed::cluster::ClusterLinkDirection::To,
            ));
            let Some(connection) = self.connections.values_mut().find(|connection| {
                connection.is_outgoing && connection.node_name == node_id && !closing_outbound
            }) else {
                if let Some(node) = state.nodes.get(&node_id) {
                    let retry = (node.ip.clone(), node.cport, true);
                    state.queue_connect_once(retry);
                }
                // C starts an active probe when reconnect fails before a PING
                // can be sent. An asymmetric MEET retry uses the same endpoint.
                state.start_node_probe_if_idle(&node_id, now_ms);
                continue;
            };
            connection.handshake_sent = false;
            let retry = (
                connection.addr.ip().to_string(),
                connection.addr.port(),
                true,
            );
            state.queue_connect_once(retry);
        }

        for node in state.nodes.values_mut() {
            if node.has_flag(CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR) {
                continue;
            }
            let has_inbound = self.connections.values().any(|connection| {
                !connection.is_outgoing && connection.connected && connection.node_name == node.name
            });
            if has_inbound {
                node.has_seen_inbound_link = true;
                node.inbound_link_missing_since = 0;
            } else if node.has_seen_inbound_link && node.inbound_link_missing_since == 0 {
                node.inbound_link_missing_since = now_ms;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ClusterBusManager;
    use crate::engine::command::context::Token;
    use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};
    use crate::engine::distributed::cluster::state::ClusterState;

    #[test]
    fn inbound_link_suppresses_asymmetric_repair() {
        let mut manager = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = "known".to_string();
        let mut node =
            ClusterNode::new(node_id.clone(), "127.0.0.1".to_string(), 6380, 16380, true);
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.asymmetric_meet_started_at = 1_000;
        node.inbound_link_missing_since = 1_000;
        state.nodes.insert(node_id.clone(), node);
        manager.register_outgoing_for_node(
            Token(7),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id.clone(),
        );
        manager.register_incoming(Token(8), "127.0.0.1:26380".parse().unwrap());
        manager.connections.get_mut(&8).unwrap().node_name = node_id.clone();

        manager.schedule_asymmetric_meets(&mut state, 2_001, 1_000);

        let connection = manager.connections.get_mut(&7).unwrap();
        assert!(!connection.send_meet);
        assert!(!connection.handshake_sent);
        assert_eq!(
            state
                .nodes
                .get(&node_id)
                .unwrap()
                .asymmetric_meet_started_at,
            1_000
        );
    }

    #[test]
    fn asymmetric_meet_retry_uses_link_time_without_retiming_discovery() {
        let mut manager = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = "known".to_string();
        let mut node =
            ClusterNode::new(node_id.clone(), "127.0.0.1".to_string(), 6380, 16380, true);
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.asymmetric_meet_started_at = 1_000;
        node.inbound_link_missing_since = 1_000;
        state.nodes.insert(node_id.clone(), node);
        manager.register_outgoing_for_node(
            Token(7),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id.clone(),
        );

        manager.schedule_asymmetric_meets(&mut state, 5_001, 4_000);
        state.nodes.get_mut(&node_id).unwrap().meet_sent_at = 5_001;
        let connection = manager.connections.get_mut(&7).unwrap();
        connection.handshake_sent = true;
        connection.meet_sent_at = 5_001;
        manager.schedule_asymmetric_meets(&mut state, 6_002, 4_000);

        assert!(manager.connections.get(&7).unwrap().handshake_sent);
        assert_eq!(state.nodes[&node_id].asymmetric_meet_started_at, 1_000);

        manager.schedule_asymmetric_meets(&mut state, 9_002, 4_000);
        assert!(!manager.connections.get(&7).unwrap().handshake_sent);
        assert_eq!(state.nodes[&node_id].asymmetric_meet_started_at, 1_000);
    }

    #[test]
    fn asymmetric_repair_requeues_existing_outbound_link_for_runtime_wakeup() {
        let mut manager = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = "known".to_string();
        let mut node =
            ClusterNode::new(node_id.clone(), "127.0.0.1".to_string(), 6380, 16380, true);
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.asymmetric_meet_started_at = 1_000;
        state.nodes.insert(node_id.clone(), node);
        manager.register_outgoing_for_node(
            Token(7),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id,
        );
        let connection = manager.connections.get_mut(&7).unwrap();
        connection.handshake_sent = true;
        connection.meet_sent_at = 1_000;

        manager.schedule_asymmetric_meets(&mut state, 5_001, 4_000);

        assert_eq!(
            state.pending_connects,
            vec![("127.0.0.1".to_string(), 16380, true)],
            "the runtime owns readiness interest, so a rearmed existing link must
             return through pending_connects rather than waiting for a read event"
        );
    }

    #[test]
    fn normal_node_without_an_inbound_link_rearms_a_meet_after_handshake_timeout() {
        let mut manager = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = "known".to_string();
        let mut node =
            ClusterNode::new(node_id.clone(), "127.0.0.1".to_string(), 6380, 16380, true);
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.inbound_link_missing_since = 1_000;
        node.has_seen_inbound_link = true;
        state.nodes.insert(node_id.clone(), node);
        manager.register_outgoing_for_node(
            Token(7),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id.clone(),
        );
        manager.connections.get_mut(&7).unwrap().handshake_sent = true;

        manager.schedule_asymmetric_meets(&mut state, 5_001, 4_000);

        assert!(
            state.nodes[&node_id].meet_pending,
            "a normal peer with only an outbound link needs a reciprocal MEET"
        );
        assert_eq!(
            state.pending_connects,
            vec![("127.0.0.1".to_string(), 16380, true)]
        );
    }

    #[test]
    fn gossip_discovered_peer_without_prior_inbound_rearms_reciprocal_meet() {
        let mut manager = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = "known".to_string();
        let mut node =
            ClusterNode::new(node_id.clone(), "127.0.0.1".to_string(), 6380, 16380, true);
        node.flags &= !CLUSTER_NODE_MYSELF;
        // Trusted gossip creates a normal node and arms asymmetric repair, but
        // the peer has never opened an inbound link to us.
        node.asymmetric_meet_started_at = 1_000;
        state.nodes.insert(node_id.clone(), node);
        manager.register_outgoing_for_node(
            Token(7),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id.clone(),
        );
        manager.connections.get_mut(&7).unwrap().handshake_sent = true;

        manager.schedule_asymmetric_meets(&mut state, 5_001, 4_000);

        assert!(state.nodes[&node_id].meet_pending);
        assert_eq!(
            state.pending_connects,
            vec![("127.0.0.1".to_string(), 16380, true)]
        );
    }

    #[test]
    fn asymmetric_repair_is_owned_by_the_known_node_across_link_replacement() {
        let mut manager = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = "known".to_string();
        let mut node =
            ClusterNode::new(node_id.clone(), "127.0.0.1".to_string(), 6380, 16380, true);
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.asymmetric_meet_started_at = 1_000;
        state.nodes.insert(node_id.clone(), node);
        manager.register_outgoing_for_node(
            Token(7),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id.clone(),
        );

        manager.schedule_asymmetric_meets(&mut state, 2_001, 1_000);

        assert!(state.nodes[&node_id].meet_pending);
        assert_eq!(state.nodes[&node_id].meet_sent_at, 0);

        manager.remove(7);
        manager.schedule_asymmetric_meets(&mut state, 2_002, 1_000);

        assert_eq!(
            state.pending_connects,
            vec![("127.0.0.1".to_string(), 16380, true)],
            "a replacement outbound connection must inherit the node-level MEET intent"
        );
    }

    #[test]
    fn anonymous_meet_link_retries_after_handshake_timeout() {
        let mut manager = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        manager.register_outgoing_for_node(
            Token(7),
            "127.0.0.1:16380".parse().unwrap(),
            true,
            String::new(),
        );
        let connection = manager.connections.get_mut(&7).unwrap();
        connection.handshake_sent = true;
        connection.meet_sent_at = 1_000;

        manager.schedule_asymmetric_meets(&mut state, 5_001, 4_000);

        assert!(!manager.connections[&7].handshake_sent);
    }

    #[test]
    fn zero_timeout_waits_for_handshake_window_before_asymmetric_meet() {
        let mut manager = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = "known".to_string();
        let mut node =
            ClusterNode::new(node_id.clone(), "127.0.0.1".to_string(), 6380, 16380, true);
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.asymmetric_meet_started_at = 1_000;
        node.inbound_link_missing_since = 1_000;
        state.nodes.insert(node_id.clone(), node);
        manager.register_outgoing_for_node(
            Token(7),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id,
        );
        manager.connections.get_mut(&7).unwrap().handshake_sent = true;

        manager.schedule_asymmetric_meets(&mut state, 1_001, 0);
        assert!(manager.connections.get(&7).unwrap().handshake_sent);

        manager.schedule_asymmetric_meets(&mut state, 1_002, 0);
        assert!(manager.connections.get(&7).unwrap().handshake_sent);
        assert!(state.pending_connects.is_empty());

        manager.schedule_asymmetric_meets(&mut state, 2_000, 0);
        assert!(manager.connections.get(&7).unwrap().handshake_sent);
        assert!(state.pending_connects.is_empty());

        manager.schedule_asymmetric_meets(&mut state, 2_001, 0);
        assert!(!manager.connections.get(&7).unwrap().handshake_sent);
    }

    #[test]
    fn anonymous_meet_retry_waits_for_minimum_handshake_window() {
        for timeout_ms in [0, 1, 500] {
            let mut manager = ClusterBusManager::new();
            let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
            manager.register_outgoing_for_node(
                Token(7),
                "127.0.0.1:16380".parse().unwrap(),
                true,
                String::new(),
            );
            let connection = manager.connections.get_mut(&7).unwrap();
            connection.handshake_sent = true;
            connection.meet_sent_at = 1_000;

            for now_ms in [1_002, 1_501, 1_999, 2_000] {
                manager.schedule_asymmetric_meets(&mut state, now_ms, timeout_ms);
                assert!(
                    manager.connections[&7].handshake_sent,
                    "timeout={timeout_ms}, now={now_ms}"
                );
            }
            manager.schedule_asymmetric_meets(&mut state, 2_001, timeout_ms);
            assert!(!manager.connections[&7].handshake_sent);
        }
    }

    #[test]
    fn known_node_asymmetric_meet_obeys_minimum_handshake_window_and_cooldown() {
        let mut manager = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = "known".to_string();
        let mut node =
            ClusterNode::new(node_id.clone(), "127.0.0.1".to_string(), 6380, 16380, true);
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.inbound_link_missing_since = 1_000;
        node.has_seen_inbound_link = true;
        node.meet_sent_at = 1_500;
        state.nodes.insert(node_id.clone(), node);
        manager.register_outgoing_for_node(
            Token(7),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id.clone(),
        );
        manager.connections.get_mut(&7).unwrap().handshake_sent = true;

        for now_ms in [1_002, 2_000, 2_001, 2_500] {
            manager.schedule_asymmetric_meets(&mut state, now_ms, 1);
            assert!(manager.connections[&7].handshake_sent, "now={now_ms}");
            assert!(!state.nodes[&node_id].meet_pending, "now={now_ms}");
            assert!(state.pending_connects.is_empty(), "now={now_ms}");
        }
        manager.schedule_asymmetric_meets(&mut state, 2_501, 1);

        assert!(!manager.connections[&7].handshake_sent);
        assert!(state.nodes[&node_id].meet_pending);
        assert_eq!(
            state.pending_connects,
            vec![("127.0.0.1".to_string(), 16380, true)]
        );
    }

    #[test]
    fn pending_meet_upgrades_existing_outgoing_link() {
        let mut manager = ClusterBusManager::new();
        let addr = "127.0.0.1:16380".parse().unwrap();
        manager.register_outgoing_for_node(Token(7), addr, false, "known".to_string());
        manager.connections.get_mut(&7).unwrap().handshake_sent = true;

        assert_eq!(manager.arm_meet_for_addr(addr), Some(7));

        let connection = manager.connections.get(&7).unwrap();
        assert!(connection.send_meet);
        assert!(!connection.handshake_sent);
    }

    #[test]
    fn arming_meet_returns_the_same_outbound_link_it_updates() {
        let mut manager = ClusterBusManager::new();
        let addr = "127.0.0.1:16380".parse().unwrap();
        manager.register_outgoing_for_node(Token(7), addr, false, "first".to_string());
        manager.register_outgoing_for_node(Token(8), addr, false, "second".to_string());

        let token = manager.arm_meet_for_addr(addr).unwrap();

        assert!(manager.connections[&token].send_meet);
        assert!(!manager.connections[&token].handshake_sent);
    }

    #[test]
    fn manager_recognizes_each_registered_listener() {
        let mut manager = ClusterBusManager::new();
        manager.listener_tokens = vec![Token(1), Token(2)];

        assert!(manager.is_listener_token(Token(1)));
        assert!(manager.is_listener_token(Token(2)));
        assert!(!manager.is_listener_token(Token(3)));
    }

    #[test]
    fn asymmetric_repair_recreates_missing_outgoing_meet() {
        let mut manager = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = "known".to_string();
        let mut node =
            ClusterNode::new(node_id.clone(), "127.0.0.1".to_string(), 6380, 16380, true);
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.asymmetric_meet_started_at = 1_000;
        node.inbound_link_missing_since = 1_000;
        state.nodes.insert(node_id.clone(), node);

        manager.schedule_asymmetric_meets(&mut state, 2_001, 500);

        assert_eq!(
            state.pending_connects,
            vec![("127.0.0.1".to_string(), 16380, true)]
        );
        assert_eq!(state.nodes[&node_id].ping_sent, 2_001);
        assert_eq!(
            state
                .nodes
                .get(&node_id)
                .unwrap()
                .asymmetric_meet_started_at,
            1_000
        );

        manager.schedule_asymmetric_meets(&mut state, 2_101, 500);
        assert_eq!(state.nodes[&node_id].ping_sent, 2_001);
    }

    #[test]
    fn asymmetric_repair_replaces_an_outbound_link_pending_close() {
        use crate::engine::distributed::cluster::ClusterLinkDirection;

        let mut manager = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = "known".to_string();
        let mut node =
            ClusterNode::new(node_id.clone(), "127.0.0.1".to_string(), 6380, 16380, true);
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.asymmetric_meet_started_at = 1_000;
        state.nodes.insert(node_id.clone(), node);
        manager.register_outgoing_for_node(
            Token(7),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id.clone(),
        );
        state
            .pending_link_closes
            .push((node_id, ClusterLinkDirection::To));

        manager.schedule_asymmetric_meets(&mut state, 2_001, 500);

        assert_eq!(
            state.pending_connects,
            vec![("127.0.0.1".to_string(), 16380, true)]
        );
    }
}
