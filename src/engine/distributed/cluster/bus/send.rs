use std::io::Write;

use crate::engine::distributed::cluster::endpoint::NodeId;
use crate::engine::distributed::cluster::msg::{
    build_rotating_gossip_entries, slots_to_bytes, ClusterBusBody, ClusterBusMessage,
    ClusterMessageFlags, ClusterMsgType, SlotOwnershipUpdate,
};
use crate::engine::distributed::cluster::node::ClusterNode;
use crate::engine::distributed::cluster::state::ClusterState;

use super::types::ClusterBusManager;

impl ClusterBusManager {
    fn build_message(
        &self,
        state: &ClusterState,
        message_type: ClusterMsgType,
        body: ClusterBusBody,
    ) -> ClusterBusMessage {
        let heartbeat = matches!(
            message_type,
            ClusterMsgType::Ping | ClusterMsgType::Pong | ClusterMsgType::Meet
        );
        let mut message = if heartbeat {
            self.refresh_advertised_slots(state);
            ClusterBusMessage::from_node_with_slots(
                message_type,
                &state.myself,
                state.current_epoch,
                body,
                self.cached_advertised_slots.borrow().clone(),
            )
        } else {
            ClusterBusMessage::from_node(message_type, &state.myself, state.current_epoch, body)
        };
        if heartbeat {
            let now = crate::foundation::io::sys::time::mstime();
            message.extensions.extend(
                state
                    .nodes_black_list
                    .iter()
                    .filter_map(|(node_id, expires_at_ms)| {
                        let remaining_ms = expires_at_ms.saturating_sub(now);
                        if remaining_ms <= 0 {
                            return None;
                        }
                        let node_id = NodeId::parse(node_id.as_bytes()).ok()?;
                        Some(crate::engine::distributed::cluster::msg::ClusterExtension::ForgottenNode {
                            node_id,
                            ttl_seconds: (remaining_ms as u64).div_ceil(1_000),
                        })
                    }),
            );
        }
        if state.myself.is_replica() {
            if let Some(primary) = state.nodes.get(&state.myself.replicaof) {
                message.header.config_epoch = primary.config_epoch;
                message.header.myslots = if heartbeat {
                    self.refresh_advertised_slots(state);
                    self.cached_advertised_slots.borrow().clone()
                } else {
                    slots_to_bytes(&primary.slots)
                };
            }
        }
        if state.myself.is_primary() && state.manual_failover.is_pausing_primary() {
            message.set_message_flag(ClusterMessageFlags::PAUSED);
        }
        message
    }

    fn refresh_advertised_slots(&self, state: &ClusterState) {
        if self.cached_advertised_slots_generation.get() == Some(state.topology_generation()) {
            return;
        }
        let advertised = if state.myself.is_replica() {
            state
                .nodes
                .get(&state.myself.replicaof)
                .map(|primary| &primary.slots)
                .unwrap_or(&state.myself.slots)
        } else {
            &state.myself.slots
        };
        *self.cached_advertised_slots.borrow_mut() = slots_to_bytes(advertised);
        self.cached_advertised_slots_generation
            .set(Some(state.topology_generation()));
    }

    fn encode_message(&self, message: ClusterBusMessage) -> Vec<u8> {
        message
            .encode()
            .expect("ClusterState must contain valid bus metadata")
    }

    pub(crate) fn queue_initial_handshake(&mut self, state: &mut ClusterState, token_value: usize) {
        let Some(connection) = self.connections.get(&token_value) else {
            return;
        };
        if !connection.is_outgoing || connection.handshake_sent {
            return;
        }
        let node_requests_meet = state
            .nodes
            .get(&connection.node_name)
            .is_some_and(|node| node.meet_pending);
        let (message_type, message) = if connection.send_meet || node_requests_meet {
            (ClusterMsgType::Meet, self.build_meet(state))
        } else {
            (ClusterMsgType::Ping, self.build_ping(state))
        };
        if let Some(connection) = self.connections.get_mut(&token_value) {
            connection.write_buf.extend_from_slice(&message);
            connection.handshake_sent = true;
            if message_type == ClusterMsgType::Meet {
                connection.meet_sent_at = crate::foundation::io::sys::time::mstime();
                state.mark_node_meet_sent(&connection.node_name, connection.meet_sent_at);
            }
        }
        if message_type == ClusterMsgType::Ping {
            let node_name = self.connections[&token_value].node_name.clone();
            state.start_node_probe_if_idle(&node_name, crate::foundation::io::sys::time::mstime());
        }
        state.record_bus_message_sent(message_type);
    }

    pub(super) fn queue_ping(
        &mut self,
        state: &mut ClusterState,
        token_value: usize,
        now: i64,
    ) -> bool {
        let Some(node_name) = self.connections.get(&token_value).and_then(|connection| {
            (connection.is_outgoing
                && connection.connected
                && connection.handshake_sent
                && !connection.node_name.is_empty())
            .then(|| connection.node_name.clone())
        }) else {
            return false;
        };
        if state
            .nodes
            .get(&node_name)
            .is_none_or(|node| node.ping_sent != 0)
        {
            return false;
        }
        let ping = self.build_ping(state);
        let Some(connection) = self.connections.get_mut(&token_value) else {
            return false;
        };
        connection.write_buf.extend_from_slice(&ping);
        state
            .nodes
            .get_mut(&node_name)
            .expect("queued PING target must remain in ClusterState")
            .ping_sent = now;
        state.record_bus_message_sent(ClusterMsgType::Ping);
        true
    }

    /// Build a PONG message.
    pub(super) fn build_pong(&mut self, state: &ClusterState) -> Vec<u8> {
        let nodes: Vec<&ClusterNode> = state.nodes.values().collect();
        let gossip = self.next_gossip_entries(&nodes, state);
        self.encode_message(self.build_message(
            state,
            ClusterMsgType::Pong,
            ClusterBusBody::Gossip(gossip),
        ))
    }

    /// Build a PING message.
    pub(super) fn build_ping(&mut self, state: &ClusterState) -> Vec<u8> {
        let nodes: Vec<&ClusterNode> = state.nodes.values().collect();
        let gossip = self.next_gossip_entries(&nodes, state);
        self.encode_message(self.build_message(
            state,
            ClusterMsgType::Ping,
            ClusterBusBody::Gossip(gossip),
        ))
    }

    /// Build a MEET message.
    pub fn build_meet(&mut self, state: &ClusterState) -> Vec<u8> {
        let nodes: Vec<&ClusterNode> = state.nodes.values().collect();
        let gossip = self.next_gossip_entries(&nodes, state);
        self.encode_message(self.build_message(
            state,
            ClusterMsgType::Meet,
            ClusterBusBody::Gossip(gossip),
        ))
    }

    /// Build an UPDATE message.
    #[cfg(test)]
    pub fn build_update(&self, state: &ClusterState) -> Vec<u8> {
        self.build_update_for_node(state, &state.myself)
    }

    pub(super) fn build_update_for_node(
        &self,
        state: &ClusterState,
        node: &ClusterNode,
    ) -> Vec<u8> {
        let node_id =
            NodeId::parse(node.name.as_bytes()).expect("local cluster node ID must be valid");
        let mut slots = node.slots.clone();
        for (slot, claimed) in slots.iter_mut().enumerate() {
            if state.slot_claim_withdrawn_by(slot) == Some(node.name.as_str()) {
                *claimed = false;
            }
        }
        self.encode_message(self.build_message(
            state,
            ClusterMsgType::Update,
            ClusterBusBody::Update(SlotOwnershipUpdate {
                config_epoch: node.config_epoch,
                node_id,
                slots: slots_to_bytes(&slots),
            }),
        ))
    }

    /// Build the PONG carrying the primary-paused manual failover acknowledgement.
    pub(super) fn build_paused_pong(&mut self, state: &ClusterState) -> Vec<u8> {
        let nodes: Vec<&ClusterNode> = state.nodes.values().collect();
        let gossip = self.next_gossip_entries(&nodes, state);
        let mut message =
            self.build_message(state, ClusterMsgType::Pong, ClusterBusBody::Gossip(gossip));
        message.set_message_flag(ClusterMessageFlags::PAUSED);
        self.encode_message(message)
    }

    pub(crate) fn queue_paused_pong(&mut self, state: &mut ClusterState, recipient: &NodeId) {
        let pong = self.build_paused_pong(state);
        if let Some(connection) = self
            .connections
            .values_mut()
            .find(|connection| connection.connected && connection.node_name == recipient.as_str())
        {
            connection.write_buf.extend_from_slice(&pong);
            state.record_bus_message_sent(ClusterMsgType::Pong);
        }
    }

    pub(crate) fn queue_topology_broadcast(&mut self, state: &mut ClusterState) {
        let pong = self.build_pong(state);
        for connection in self.connections.values_mut() {
            if connection.connected && !connection.node_name.is_empty() && connection.handshake_sent
            {
                connection.write_buf.extend_from_slice(&pong);
                state.record_bus_message_sent(ClusterMsgType::Pong);
            }
        }
    }

    pub(super) fn queue_local_replica_status(&mut self, state: &mut ClusterState) {
        let pong = self.build_pong(state);
        for connection in self.connections.values_mut() {
            let is_local_replica = state.nodes.get(&connection.node_name).is_some_and(|node| {
                node.is_replica()
                    && !node.replicaof.is_empty()
                    && (node.replicaof == state.myself.name
                        || node.replicaof == state.myself.replicaof)
            });
            if connection.is_outgoing
                && connection.connected
                && connection.handshake_sent
                && is_local_replica
            {
                connection.write_buf.extend_from_slice(&pong);
                state.record_bus_message_sent(ClusterMsgType::Pong);
            }
        }
    }

    fn next_gossip_entries(
        &mut self,
        nodes: &[&ClusterNode],
        state: &ClusterState,
    ) -> Vec<crate::engine::distributed::cluster::msg::GossipEntry> {
        let entries = build_rotating_gossip_entries(
            nodes,
            &state.myself,
            state.gossip_target_count(),
            self.gossip_cursor,
        );
        self.gossip_cursor = self.gossip_cursor.wrapping_add(entries.len().max(1));
        entries
    }

    pub(super) fn build_mfstart(&self, state: &ClusterState) -> Vec<u8> {
        self.encode_message(self.build_message(
            state,
            ClusterMsgType::MfStart,
            ClusterBusBody::Empty,
        ))
    }

    pub(crate) fn queue_manual_failover_start(
        &mut self,
        state: &mut ClusterState,
        primary: &NodeId,
    ) {
        let packet = self.build_mfstart(state);
        if let Some(connection) = self
            .connections
            .values_mut()
            .find(|connection| connection.connected && connection.node_name == primary.as_str())
        {
            connection.write_buf.extend_from_slice(&packet);
            state.record_bus_message_sent(ClusterMsgType::MfStart);
        }
    }

    /// Build a FAILOVER_AUTH_REQUEST from a replica to all primaries.
    pub fn build_failover_auth_request(
        &self,
        state: &ClusterState,
        epoch: u64,
        force_ack: bool,
    ) -> Vec<u8> {
        let mut message = self.build_message(
            state,
            ClusterMsgType::FailoverAuthRequest,
            ClusterBusBody::Empty,
        );
        message.header.current_epoch = epoch;
        if force_ack {
            message.set_message_flag(ClusterMessageFlags::FORCE_ACK);
        }
        self.encode_message(message)
    }

    /// Build a FAILOVER_AUTH_ACK from a primary back to the voting replica.
    /// The `election_epoch` is the election epoch echoed back from the
    /// replica's FAILOVER_AUTH_REQUEST — the ack only counts if the epoch
    /// in the ACK header matches the replica's in-progress election epoch.
    pub(super) fn build_failover_auth_ack(state: &ClusterState, election_epoch: u64) -> Vec<u8> {
        let bus = Self::new();
        let mut message = bus.build_message(
            state,
            ClusterMsgType::FailoverAuthAck,
            ClusterBusBody::Empty,
        );
        message.header.current_epoch = election_epoch;
        bus.encode_message(message)
    }

    /// Build a FAIL message about `about a node.
    pub fn build_fail(&self, state: &ClusterState, failing_node: &ClusterNode) -> Vec<u8> {
        let node_id = NodeId::parse(failing_node.name.as_bytes())
            .expect("failed cluster node ID must be valid");
        self.encode_message(self.build_message(
            state,
            ClusterMsgType::Fail,
            ClusterBusBody::Fail { node_id },
        ))
    }

    /// Handle outgoing data on a cluster bus connection.
    pub fn writable(
        &mut self,
        stream: &mut dyn Write,
        token_value: usize,
        state: &mut ClusterState,
    ) -> std::io::Result<usize> {
        // On first writable for an outgoing connection, build the initial message
        // before borrowing the connection, to satisfy the borrow checker.
        let mut total_written = 0usize;
        self.queue_initial_handshake(state, token_value);

        let conn = match self.connections.get_mut(&token_value) {
            Some(c) => c,
            None => return Ok(0),
        };

        if conn.write_buf.is_empty() {
            return Ok(0);
        }

        while !conn.write_buf.is_empty() {
            let written = match stream.write(&conn.write_buf) {
                Ok(0) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "cluster bus write made no progress",
                    ));
                }
                Ok(written) => written,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                // Darwin can report ENOTCONN before a nonblocking outgoing
                // connect completes. Preserve the initial queued frame and let
                // readiness/connection-error dispatch resolve the socket.
                Err(error)
                    if error.kind() == std::io::ErrorKind::NotConnected
                        && conn.is_outgoing
                        && conn.bytes_sent == 0 =>
                {
                    break
                }
                Err(error) => return Err(error),
            };
            conn.write_buf.drain(..written);
            if conn.write_buf.is_empty() && conn.write_buf.capacity() > 4096 {
                conn.write_buf = Vec::with_capacity(4096);
            }
            total_written = total_written.saturating_add(written);
            conn.last_send = crate::foundation::io::sys::time::mstime();
            conn.bytes_sent = conn.bytes_sent.saturating_add(written as u64);
            conn.bytes_sent_since_read = conn.bytes_sent_since_read.saturating_add(written);
            state.record_bus_bytes_sent(written);
        }
        Ok(total_written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::distributed::cluster::msg::{
        bytes_to_slots, ClusterBusBody, ClusterMessageFlags,
    };
    use crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;

    struct WouldBlockWriter;

    impl std::io::Write for WouldBlockWriter {
        fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    struct ShortWriter {
        bytes: Vec<u8>,
        max_write_bytes: usize,
    }

    impl std::io::Write for ShortWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let written = self.max_write_bytes.min(buf.len());
            self.bytes.extend_from_slice(&buf[..written]);
            Ok(written)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn fail_update_and_paused_pong_builders_use_c_bodies() {
        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.myself.slots[7] = true;
        state.myself.numslots = 1;
        state.myself.config_epoch = 9;
        let mut failed = ClusterNode::new(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        failed.flags &= !CLUSTER_NODE_MYSELF;

        let (fail, _) = ClusterBusMessage::decode(&bus.build_fail(&state, &failed)).unwrap();
        assert!(matches!(
            fail.body,
            ClusterBusBody::Fail { ref node_id } if node_id.as_str() == failed.name
        ));

        let (update, _) = ClusterBusMessage::decode(&bus.build_update(&state)).unwrap();
        assert!(matches!(
            update.body,
            ClusterBusBody::Update(ref body)
                if body.node_id.as_str() == state.myself.name
                    && body.config_epoch == 9
                    && bytes_to_slots(&body.slots)[7]
        ));

        let (pong, _) = ClusterBusMessage::decode(&bus.build_paused_pong(&state)).unwrap();
        assert_eq!(pong.msg_type, ClusterMsgType::Pong);
        assert!(pong.has_message_flag(ClusterMessageFlags::PAUSED));
    }

    #[test]
    fn update_omits_slots_withdrawn_by_the_reported_owner() {
        let bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let mut owner = ClusterNode::new(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        owner.flags &= !CLUSTER_NODE_MYSELF;
        owner.slots[7] = true;
        owner.numslots = 1;
        state.set_slot_claim_withdrawn(7, Some(owner.name.clone()));

        let (update, _) =
            ClusterBusMessage::decode(&bus.build_update_for_node(&state, &owner)).unwrap();
        let ClusterBusBody::Update(body) = update.body else {
            panic!("expected UPDATE body");
        };
        assert!(!bytes_to_slots(&body.slots)[7]);
    }

    #[test]
    fn gossip_count_scales_with_configured_percentage_and_c_minimum() {
        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        for index in 0..9 {
            let node_id = format!("{index:040}");
            let mut node = ClusterNode::new(
                node_id.clone(),
                "127.0.0.1".to_string(),
                6380 + index,
                16380 + index,
                true,
            );
            node.flags &= !CLUSTER_NODE_MYSELF;
            state.nodes.insert(node_id, node);
        }

        state.set_gossip_percentage(80);
        let (message, _) = ClusterBusMessage::decode(&bus.build_pong(&state)).unwrap();
        assert_eq!(message.gossip().len(), 8);

        state.set_gossip_percentage(1);
        let (message, _) = ClusterBusMessage::decode(&bus.build_pong(&state)).unwrap();
        assert_eq!(message.gossip().len(), 3);
    }

    #[test]
    fn successive_gossip_packets_rotate_across_all_known_nodes() {
        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        for index in 0..9 {
            let node_id = format!("{index:040}");
            let mut node = ClusterNode::new(
                node_id.clone(),
                "127.0.0.1".to_string(),
                6380 + index,
                16380 + index,
                true,
            );
            node.flags &= !CLUSTER_NODE_MYSELF;
            state.nodes.insert(node_id, node);
        }
        state.set_gossip_percentage(10);
        let mut seen = std::collections::HashSet::new();

        for _ in 0..3 {
            let (message, _) = ClusterBusMessage::decode(&bus.build_ping(&state)).unwrap();
            seen.extend(message.gossip().iter().map(|entry| entry.node_id.clone()));
        }

        assert_eq!(seen.len(), 9);
    }

    #[test]
    fn topology_broadcast_reaches_every_connected_identified_link() {
        use crate::engine::command::context::Token;

        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let peer_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut peer = ClusterNode::new(
            peer_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        peer.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(peer_id.to_string(), peer);
        bus.register_outgoing_for_node(
            Token(41),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            peer_id.to_string(),
        );
        bus.register_incoming(Token(42), "127.0.0.1:26380".parse().unwrap());
        for token in [41, 42] {
            let connection = bus.connections.get_mut(&token).unwrap();
            connection.node_name = peer_id.to_string();
            connection.handshake_sent = true;
        }

        bus.queue_topology_broadcast(&mut state);

        assert!(!bus.connections[&41].write_buf.is_empty());
        assert!(!bus.connections[&42].write_buf.is_empty());
    }

    #[test]
    fn initial_outbound_ping_starts_a_probe() {
        use crate::engine::command::context::Token;

        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let peer_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut peer = ClusterNode::new(
            peer_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        peer.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(peer_id.to_string(), peer);
        bus.register_outgoing_for_node(
            Token(43),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            peer_id.to_string(),
        );

        bus.queue_initial_handshake(&mut state, 43);

        assert!(state.nodes[peer_id].ping_sent > 0);
        let (message, _) = ClusterBusMessage::decode(&bus.connections[&43].write_buf).unwrap();
        assert_eq!(message.msg_type, ClusterMsgType::Ping);
    }

    #[test]
    fn ping_carries_unexpired_forgotten_node_extensions() {
        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let forgotten = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        state.nodes_black_list.insert(
            forgotten.to_string(),
            crate::foundation::io::sys::time::mstime() + 60_000,
        );

        let (message, _) = ClusterBusMessage::decode(&bus.build_ping(&state)).unwrap();

        assert!(message.extensions.iter().any(|extension| matches!(
            extension,
            crate::engine::distributed::cluster::msg::ClusterExtension::ForgottenNode {
                node_id,
                ttl_seconds
            } if node_id.as_str() == forgotten && (1..=60).contains(ttl_seconds)
        )));
    }

    #[test]
    fn would_block_keeps_the_link_and_queued_bytes() {
        use crate::engine::command::context::Token;

        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let token = Token(9);
        bus.register_outgoing_for_node(
            token,
            "127.0.0.1:16380".parse().unwrap(),
            false,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        );
        let connection = bus.connections.get_mut(&token.0).unwrap();
        connection.handshake_sent = true;
        connection.write_buf.extend_from_slice(b"queued");

        assert_eq!(
            bus.writable(&mut WouldBlockWriter, token.0, &mut state)
                .unwrap(),
            0
        );
        assert_eq!(bus.connections[&token.0].write_buf, b"queued");
    }

    #[test]
    fn connecting_outgoing_meet_retains_queued_frame_on_not_connected() {
        struct ConnectingWriter;
        impl Write for ConnectingWriter {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::from_raw_os_error(libc::ENOTCONN))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        bus.register_outgoing_for_handshake(
            crate::engine::command::context::Token(9),
            "127.0.0.1:16380".parse().unwrap(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        );
        bus.queue_initial_handshake(&mut state, 9);
        let queued = bus.connections[&9].write_buf.clone();
        assert_eq!(
            bus.writable(&mut ConnectingWriter, 9, &mut state).unwrap(),
            0
        );
        assert_eq!(bus.connections[&9].write_buf, queued);
        let mut output = Vec::new();
        bus.writable(&mut output, 9, &mut state).unwrap();
        assert_eq!(output, queued);
        assert!(state
            .info_str()
            .contains("cluster_stats_messages_meet_sent:1\r\n"));
    }

    #[test]
    fn short_writes_continue_until_cluster_output_is_drained() {
        use crate::engine::command::context::Token;

        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let token = Token(9);
        bus.register_outgoing_for_node(
            token,
            "127.0.0.1:16380".parse().unwrap(),
            false,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        );
        let connection = bus.connections.get_mut(&token.0).unwrap();
        connection.handshake_sent = true;
        connection.write_buf.extend_from_slice(b"queued");
        let mut writer = ShortWriter {
            bytes: Vec::new(),
            max_write_bytes: 2,
        };

        assert_eq!(bus.writable(&mut writer, token.0, &mut state).unwrap(), 6);
        assert_eq!(writer.bytes, b"queued");
        assert!(bus.connections[&token.0].write_buf.is_empty());
    }

    #[test]
    fn successful_writes_accumulate_until_peer_data_arrives() {
        use crate::engine::command::context::Token;

        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let token = Token(10);
        bus.register_outgoing_for_node(
            token,
            "127.0.0.1:16380".parse().unwrap(),
            false,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        );
        let connection = bus.connections.get_mut(&token.0).unwrap();
        connection.handshake_sent = true;
        connection.write_buf.extend_from_slice(b"queued");
        let mut output = Vec::new();

        assert_eq!(bus.writable(&mut output, token.0, &mut state).unwrap(), 6);
        assert_eq!(bus.connections[&token.0].bytes_sent_since_read, 6);
    }

    #[test]
    fn advertised_slot_bitmap_cache_tracks_topology_generation() {
        let mut bus = ClusterBusManager::new();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.add_slot(0).unwrap();

        bus.build_ping(&state);

        assert_eq!(
            bus.cached_advertised_slots_generation.get(),
            Some(state.topology_generation())
        );
        assert!(bus.cached_advertised_slots.borrow()[0] & 0x80 != 0);

        state.add_slot(1).unwrap();
        bus.build_ping(&state);

        assert_eq!(
            bus.cached_advertised_slots_generation.get(),
            Some(state.topology_generation())
        );
        assert!(bus.cached_advertised_slots.borrow()[0] & 0x40 != 0);
    }
}
