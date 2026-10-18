use super::super::super::ServerWorkerState;
use crate::engine::distributed::cluster::msg::{
    ClusterBusBody, ClusterBusMessage, ClusterMsgType, ClusterPublish,
};
use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_HANDSHAKE};
use crate::engine::distributed::cluster::{ClusterLinkDirection, ClusterState};
use bytes::Bytes;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};

#[derive(Clone, Copy)]
struct ClusterPublishConnectionView<'a> {
    addr: SocketAddr,
    is_outgoing: bool,
    connected: bool,
    node_name: &'a str,
    handshake_node_name: &'a str,
}

impl ServerWorkerState {
    fn cluster_publish_node_is_eligible(
        node: &ClusterNode,
        local_shard_id: &str,
        shard: bool,
    ) -> bool {
        !node.is_myself()
            && !node.has_flag(CLUSTER_NODE_HANDSHAKE)
            && (!shard || (!local_shard_id.is_empty() && node.shard_id == local_shard_id))
    }

    fn cluster_publish_link_is_closing(
        state: &ClusterState,
        node_id: &str,
        direction: ClusterLinkDirection,
    ) -> bool {
        state
            .pending_link_closes()
            .iter()
            .any(|(pending_node_id, pending_direction)| {
                pending_node_id == node_id
                    && (*pending_direction == ClusterLinkDirection::All
                        || *pending_direction == direction)
            })
    }

    fn cluster_publish_endpoint_owners(
        state: &ClusterState,
    ) -> HashMap<(IpAddr, u16), Option<&str>> {
        let mut owners = HashMap::new();
        for node in state
            .nodes
            .values()
            .filter(|node| !node.is_myself() && !node.has_flag(CLUSTER_NODE_HANDSHAKE))
        {
            let Ok(ip) = node.ip().parse::<IpAddr>() else {
                continue;
            };
            owners
                .entry((ip, node.cport()))
                .and_modify(|owner| *owner = None)
                .or_insert(Some(node.name.as_str()));
        }
        owners
    }

    fn cluster_publish_connection_node<'a>(
        state: &'a ClusterState,
        endpoint_owners: &HashMap<(IpAddr, u16), Option<&'a str>>,
        connection: ClusterPublishConnectionView<'_>,
    ) -> Option<(&'a ClusterNode, u8)> {
        if let Some(node) = state
            .nodes
            .get(connection.node_name)
            .filter(|node| !node.is_myself() && !node.has_flag(CLUSTER_NODE_HANDSHAKE))
        {
            return Some((node, if connection.is_outgoing { 0 } else { 2 }));
        }
        if !connection.is_outgoing
            || state
                .nodes
                .get(connection.node_name)
                .is_some_and(|node| !node.has_flag(CLUSTER_NODE_HANDSHAKE))
        {
            return None;
        }
        let owner = endpoint_owners
            .get(&(connection.addr.ip(), connection.addr.port()))
            .copied()
            .flatten()?;
        state.nodes.get(owner).map(|node| (node, 1))
    }

    fn cluster_publish_connection_is_closing(
        state: &ClusterState,
        node_id: &str,
        connection: ClusterPublishConnectionView<'_>,
    ) -> bool {
        [
            node_id,
            connection.node_name,
            connection.handshake_node_name,
        ]
        .into_iter()
        .filter(|identity| !identity.is_empty())
        .any(|identity| {
            Self::cluster_publish_link_is_closing(
                state,
                identity,
                if connection.is_outgoing {
                    ClusterLinkDirection::To
                } else {
                    ClusterLinkDirection::From
                },
            )
        })
    }

    fn cluster_publish_candidate<'a>(
        state: &'a ClusterState,
        endpoint_owners: &HashMap<(IpAddr, u16), Option<&'a str>>,
        connection: ClusterPublishConnectionView<'_>,
        shard: bool,
    ) -> Option<(&'a ClusterNode, u8)> {
        if !connection.connected {
            return None;
        }
        let (node, rank) =
            Self::cluster_publish_connection_node(state, endpoint_owners, connection)?;
        let outbound_close =
            Self::cluster_publish_link_is_closing(state, &node.name, ClusterLinkDirection::To);
        // Reconnect intent does not retire a live inbound link. Prefer an
        // outbound candidate when available, but keep the identified fallback
        // until then; cluster_legacy.c::clusterPropagatePublish queues once per
        // eligible peer without consulting reconnect scheduling.
        let connection_close =
            Self::cluster_publish_connection_is_closing(state, &node.name, connection);
        (Self::cluster_publish_node_is_eligible(node, &state.myself.shard_id, shard)
            && !outbound_close
            && !connection_close)
            .then_some((node, rank))
    }

    fn cluster_publish_target_tokens(&self, state: &ClusterState, shard: bool) -> Vec<usize> {
        let endpoint_owners = Self::cluster_publish_endpoint_owners(state);
        let mut selected = HashMap::new();
        for (token, connection) in &self.cluster_bus.connections {
            let connection = ClusterPublishConnectionView {
                addr: connection.addr,
                is_outgoing: connection.is_outgoing,
                connected: connection.connected,
                node_name: &connection.node_name,
                handshake_node_name: &connection.handshake_node_name,
            };
            let Some((node, rank)) =
                Self::cluster_publish_candidate(state, &endpoint_owners, connection, shard)
            else {
                continue;
            };
            let preference = (rank, *token);
            selected
                .entry(node.name.as_str())
                .and_modify(|current| {
                    if preference < *current {
                        *current = preference;
                    }
                })
                .or_insert(preference);
        }
        let mut targets = selected
            .into_values()
            .map(|(_, token)| token)
            .collect::<Vec<_>>();
        targets.sort_unstable();
        targets
    }

    pub(crate) fn propagate_cluster_publishes(
        &mut self,
        pending: Vec<(Bytes, Bytes)>,
        pending_shard: Vec<(Bytes, Bytes)>,
    ) {
        self.propagate_cluster_publishes_excluding(pending, pending_shard, &Default::default());
    }

    pub(crate) fn propagate_cluster_publishes_excluding(
        &mut self,
        pending: Vec<(Bytes, Bytes)>,
        pending_shard: Vec<(Bytes, Bytes)>,
        closed_peers: &std::collections::HashSet<String>,
    ) {
        if !self.ctx.config().server.cluster_enabled {
            return;
        }
        for (channel, payload) in pending {
            self.queue_cluster_publish_excluding(channel, payload, false, closed_peers);
        }
        for (channel, payload) in pending_shard {
            self.queue_cluster_publish_excluding(channel, payload, true, closed_peers);
        }
    }

    pub(super) fn queue_cluster_publish(&mut self, channel: Bytes, payload: Bytes, shard: bool) {
        self.queue_cluster_publish_excluding(channel, payload, shard, &Default::default());
    }

    fn queue_cluster_publish_excluding(
        &mut self,
        channel: Bytes,
        payload: Bytes,
        shard: bool,
        closed_peers: &std::collections::HashSet<String>,
    ) {
        let Some(state) = self.ctx.cluster_state().as_deref() else {
            return;
        };
        let message_type = if shard {
            ClusterMsgType::PublishShard
        } else {
            ClusterMsgType::Publish
        };
        let message = ClusterBusMessage::from_node(
            message_type,
            &state.myself,
            state.current_epoch,
            ClusterBusBody::Publish(ClusterPublish::new(channel, payload, shard)),
        );
        let Ok(frame) = message.encode() else {
            return;
        };
        let endpoint_owners = Self::cluster_publish_endpoint_owners(state);
        let targets = self
            .cluster_publish_target_tokens(state, shard)
            .into_iter()
            .filter(|token| {
                let connection = &self.cluster_bus.connections[token];
                let view = ClusterPublishConnectionView {
                    addr: connection.addr,
                    is_outgoing: connection.is_outgoing,
                    connected: connection.connected,
                    node_name: &connection.node_name,
                    handshake_node_name: &connection.handshake_node_name,
                };
                Self::cluster_publish_connection_node(state, &endpoint_owners, view)
                    .is_some_and(|(node, _)| !closed_peers.contains(&node.name))
            })
            .collect::<Vec<_>>();
        let mut sent = 0u64;
        for token in targets {
            if let Some(mut state) = self.ctx.cluster_state_mut().take() {
                self.cluster_bus.queue_initial_handshake(&mut state, token);
                *self.ctx.cluster_state_mut() = Some(state);
            }
            if let Some(connection) = self.cluster_bus.connections.get_mut(&token) {
                connection.write_buf.extend_from_slice(&frame);
                sent += 1;
            }
        }
        if sent > 0 {
            let cluster_link_memory = self
                .cluster_bus
                .connections
                .values()
                .map(|connection| {
                    connection
                        .read_buf
                        .capacity()
                        .saturating_add(connection.write_buf.capacity())
                })
                .sum();
            if let Some(state) = self.ctx.cluster_state_mut().as_deref_mut() {
                for _ in 0..sent {
                    state.record_bus_message_sent(message_type);
                    state.record_bus_frame_sent(message_type, frame.len());
                }
                state.set_cluster_link_memory(cluster_link_memory);
            }
        }
    }

    pub(crate) fn drain_cluster_bus_publishes(&mut self) {
        let events = std::mem::take(&mut self.cluster_bus.pending_publishes);
        for event in events {
            if event.shard {
                self.deliver_published_locally(Vec::new(), vec![(event.channel, event.payload)]);
            } else {
                self.deliver_published_locally(vec![(event.channel, event.payload)], Vec::new());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ServerWorkerState;
    use crate::engine::distributed::cluster::node::{
        ClusterNode, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_MYSELF,
    };
    use crate::engine::distributed::cluster::ClusterState;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use bytes::Bytes;

    fn add_bus_link(backend: &mut ServerWorkerState, token: usize, node_id: &str) {
        backend.cluster_bus.register_outgoing_for_node(
            Token(token),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id.to_string(),
        );
        backend
            .cluster_bus
            .connections
            .get_mut(&token)
            .unwrap()
            .handshake_sent = true;
    }

    fn add_inbound_bus_link(backend: &mut ServerWorkerState, token: usize, node_id: &str) {
        backend
            .cluster_bus
            .register_incoming(Token(token), "127.0.0.1:26380".parse().unwrap());
        backend
            .cluster_bus
            .connections
            .get_mut(&token)
            .unwrap()
            .node_name = node_id.to_string();
    }

    #[test]
    fn local_publish_fans_out_once_per_peer_and_prefers_outbound() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        for (node_id, port) in [
            ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 6380),
            ("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 6381),
        ] {
            let mut node = ClusterNode::new(
                node_id.to_string(),
                "127.0.0.1".to_string(),
                port,
                port + 10_000,
                true,
            );
            node.flags &= !CLUSTER_NODE_MYSELF;
            node.shard_id = state.myself.shard_id.clone();
            state.nodes.insert(node_id.to_string(), node);
        }
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        add_bus_link(&mut backend, 41, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        add_bus_link(&mut backend, 42, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        add_inbound_bus_link(&mut backend, 43, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        backend
            .cluster_bus
            .register_incoming(Token(44), "127.0.0.1:26381".parse().unwrap());
        backend.refresh_cluster_link_views();
        let memory_before = backend
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .cluster_link_memory();

        backend.queue_cluster_publish(
            Bytes::from_static(b"channel"),
            Bytes::from(vec![b'x'; 10_000]),
            false,
        );

        assert!(!backend.cluster_bus.connections[&41].write_buf.is_empty());
        assert!(!backend.cluster_bus.connections[&42].write_buf.is_empty());
        assert!(backend.cluster_bus.connections[&43].write_buf.is_empty());
        assert!(backend.cluster_bus.connections[&44].write_buf.is_empty());
        assert!(backend
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .info_str()
            .contains("cluster_stats_messages_publish_sent:2\r\n"));
        assert!(
            backend
                .ctx
                .cluster_state()
                .as_ref()
                .unwrap()
                .cluster_link_memory()
                > memory_before
        );
    }

    #[test]
    fn local_publish_falls_back_to_an_identified_inbound_peer_link() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let outbound_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let inbound_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        for (node_id, port) in [(outbound_id, 6380), (inbound_id, 6381)] {
            let mut node = ClusterNode::new(
                node_id.to_string(),
                "127.0.0.1".to_string(),
                port,
                port + 10_000,
                true,
            );
            node.flags &= !CLUSTER_NODE_MYSELF;
            state.nodes.insert(node_id.to_string(), node);
        }
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        add_bus_link(&mut backend, 41, outbound_id);
        add_inbound_bus_link(&mut backend, 42, inbound_id);

        backend.queue_cluster_publish(
            Bytes::from_static(b"channel"),
            Bytes::from_static(b"payload"),
            false,
        );

        assert!(!backend.cluster_bus.connections[&41].write_buf.is_empty());
        assert!(
            !backend.cluster_bus.connections[&42].write_buf.is_empty(),
            "an identified inbound link must carry the publish while no outbound link exists"
        );
        assert!(backend
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .info_str()
            .contains("cluster_stats_messages_publish_sent:2\r\n"));
    }

    #[test]
    fn local_publish_uses_unique_endpoint_owned_transitional_outbound() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let peer_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let handshake_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mut peer = ClusterNode::new(
            peer_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        peer.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(peer_id.to_string(), peer);
        let mut placeholder = ClusterNode::new(
            handshake_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        placeholder.flags &= !CLUSTER_NODE_MYSELF;
        placeholder.flags |= CLUSTER_NODE_HANDSHAKE;
        state.nodes.insert(handshake_id.to_string(), placeholder);
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend.cluster_bus.register_outgoing_for_handshake(
            Token(41),
            "127.0.0.1:16380".parse().unwrap(),
            handshake_id.to_string(),
        );

        backend.queue_cluster_publish(
            Bytes::from_static(b"channel"),
            Bytes::from_static(b"payload"),
            false,
        );

        let queued = &backend.cluster_bus.connections[&41].write_buf;
        let (first, consumed) =
            crate::engine::distributed::cluster::msg::ClusterBusMessage::decode(queued).unwrap();
        assert_eq!(
            first.msg_type,
            crate::engine::distributed::cluster::msg::ClusterMsgType::Meet
        );
        let (second, _) = crate::engine::distributed::cluster::msg::ClusterBusMessage::decode(
            &queued[consumed..],
        )
        .unwrap();
        assert_eq!(
            second.msg_type,
            crate::engine::distributed::cluster::msg::ClusterMsgType::Publish
        );
        assert!(backend
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .info_str()
            .contains("cluster_stats_messages_publish_sent:1\r\n"));
    }

    #[test]
    fn local_publish_prefers_confirmed_outbound_over_transitional_outbound() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
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
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend.cluster_bus.register_outgoing_for_handshake(
            Token(41),
            "127.0.0.1:16380".parse().unwrap(),
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
        );
        add_bus_link(&mut backend, 42, peer_id);

        backend.queue_cluster_publish(
            Bytes::from_static(b"channel"),
            Bytes::from_static(b"payload"),
            false,
        );

        assert!(backend.cluster_bus.connections[&41].write_buf.is_empty());
        assert!(!backend.cluster_bus.connections[&42].write_buf.is_empty());
        assert!(backend
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .info_str()
            .contains("cluster_stats_messages_publish_sent:1\r\n"));
    }

    #[test]
    fn local_publish_skips_transitional_outbound_pending_handshake_close() {
        use crate::engine::distributed::cluster::ClusterLinkDirection;

        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let peer_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let handshake_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mut peer = ClusterNode::new(
            peer_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        peer.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(peer_id.to_string(), peer);
        state
            .pending_link_closes
            .push((handshake_id.to_string(), ClusterLinkDirection::All));
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend.cluster_bus.register_outgoing_for_handshake(
            Token(41),
            "127.0.0.1:16380".parse().unwrap(),
            handshake_id.to_string(),
        );

        backend.queue_cluster_publish(
            Bytes::from_static(b"channel"),
            Bytes::from_static(b"payload"),
            false,
        );

        assert!(backend.cluster_bus.connections[&41].write_buf.is_empty());
        assert!(backend
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .info_str()
            .contains("cluster_stats_messages_publish_sent:0\r\n"));
    }

    #[test]
    fn local_publish_rejects_ambiguous_transitional_outbound_endpoint() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        for peer_id in [
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ] {
            let mut peer = ClusterNode::new(
                peer_id.to_string(),
                "127.0.0.1".to_string(),
                6380,
                16_380,
                true,
            );
            peer.flags &= !CLUSTER_NODE_MYSELF;
            state.nodes.insert(peer_id.to_string(), peer);
        }
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend.cluster_bus.register_outgoing_for_handshake(
            Token(41),
            "127.0.0.1:16380".parse().unwrap(),
            "cccccccccccccccccccccccccccccccccccccccc".to_string(),
        );

        backend.queue_cluster_publish(
            Bytes::from_static(b"channel"),
            Bytes::from_static(b"payload"),
            false,
        );

        assert!(backend.cluster_bus.connections[&41].write_buf.is_empty());
        assert!(backend
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .info_str()
            .contains("cluster_stats_messages_publish_sent:0\r\n"));
    }

    #[test]
    fn outbound_reconnect_pending_preserves_inbound_publish_fallback() {
        use crate::engine::distributed::cluster::msg::{ClusterBusMessage, ClusterMsgType};

        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let outbound_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let inbound_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        for (node_id, port) in [(outbound_id, 6380), (inbound_id, 6381)] {
            let mut node = ClusterNode::new(
                node_id.to_string(),
                "127.0.0.1".to_string(),
                port,
                port + 10_000,
                true,
            );
            node.flags &= !CLUSTER_NODE_MYSELF;
            state.nodes.insert(node_id.to_string(), node);
        }
        state
            .pending_connects
            .push(("127.0.0.1".to_string(), 16_381, false));
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        add_bus_link(&mut backend, 41, outbound_id);
        add_inbound_bus_link(&mut backend, 42, inbound_id);
        add_inbound_bus_link(&mut backend, 43, inbound_id);

        backend.queue_cluster_publish(
            Bytes::from_static(b"channel"),
            Bytes::from_static(b"payload"),
            false,
        );

        let state = backend.ctx.cluster_state().as_deref().unwrap();
        assert_eq!(
            backend.cluster_publish_target_tokens(state, false),
            vec![41, 42]
        );
        for token in [41, 42] {
            let queued = &backend.cluster_bus.connections[&token].write_buf;
            let (message, consumed) = ClusterBusMessage::decode(queued).unwrap();
            assert_eq!(message.msg_type, ClusterMsgType::Publish);
            assert_eq!(consumed, queued.len());
        }
        assert!(backend.cluster_bus.connections[&43].write_buf.is_empty());
        assert!(state
            .info_str()
            .contains("cluster_stats_messages_publish_sent:2\r\n"));
        assert_eq!(state.pending_connects().len(), 1);
    }

    #[test]
    fn disconnected_outbound_publish_link_falls_back_to_identified_inbound() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let peer_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut node = ClusterNode::new(
            peer_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(peer_id.to_string(), node);
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        add_bus_link(&mut backend, 41, peer_id);
        backend
            .cluster_bus
            .connections
            .get_mut(&41)
            .unwrap()
            .connected = false;
        add_inbound_bus_link(&mut backend, 42, peer_id);

        backend.queue_cluster_publish(
            Bytes::from_static(b"channel"),
            Bytes::from_static(b"payload"),
            false,
        );

        assert!(backend.cluster_bus.connections[&41].write_buf.is_empty());
        assert!(!backend.cluster_bus.connections[&42].write_buf.is_empty());
    }

    #[test]
    fn inbound_publish_link_respects_from_and_all_pending_closes() {
        use crate::engine::distributed::cluster::ClusterLinkDirection;

        for direction in [ClusterLinkDirection::From, ClusterLinkDirection::All] {
            let mut backend = ServerWorkerState::new(1).unwrap();
            let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
            let peer_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
            let mut node = ClusterNode::new(
                peer_id.to_string(),
                "127.0.0.1".to_string(),
                6380,
                16_380,
                true,
            );
            node.flags &= !CLUSTER_NODE_MYSELF;
            state.nodes.insert(peer_id.to_string(), node);
            state
                .pending_link_closes
                .push((peer_id.to_string(), direction));
            *backend.ctx.cluster_state_mut() = Some(Box::new(state));
            add_inbound_bus_link(&mut backend, 41, peer_id);

            backend.queue_cluster_publish(
                Bytes::from_static(b"channel"),
                Bytes::from_static(b"payload"),
                false,
            );

            assert!(backend.cluster_bus.connections[&41].write_buf.is_empty());
            assert!(backend
                .ctx
                .cluster_state()
                .as_ref()
                .unwrap()
                .info_str()
                .contains("cluster_stats_messages_publish_sent:0\r\n"));
        }
    }

    #[test]
    fn sharded_publish_excludes_other_shards_and_handshake_nodes() {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE;

        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let local_shard = state.myself.shard_id.clone();
        for (node_id, token, shard_id, flags) in [
            (
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                41,
                local_shard.as_str(),
                0,
            ),
            (
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                42,
                "cccccccccccccccccccccccccccccccccccccccc",
                0,
            ),
            (
                "dddddddddddddddddddddddddddddddddddddddd",
                43,
                local_shard.as_str(),
                CLUSTER_NODE_HANDSHAKE,
            ),
        ] {
            let mut node = ClusterNode::new(
                node_id.to_string(),
                "127.0.0.1".to_string(),
                6380,
                16_380,
                true,
            );
            node.flags &= !CLUSTER_NODE_MYSELF;
            node.flags |= flags;
            node.shard_id = shard_id.to_string();
            state.nodes.insert(node_id.to_string(), node);
            add_bus_link(&mut backend, token, node_id);
        }
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));

        backend.queue_cluster_publish(
            Bytes::from_static(b"channel"),
            Bytes::from_static(b"payload"),
            true,
        );

        assert!(!backend.cluster_bus.connections[&41].write_buf.is_empty());
        assert!(backend.cluster_bus.connections[&42].write_buf.is_empty());
        assert!(backend.cluster_bus.connections[&43].write_buf.is_empty());
    }

    #[test]
    fn publish_skips_an_outbound_link_pending_debug_close() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        for (node_id, token) in [
            ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 41),
            ("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 42),
        ] {
            let mut node = ClusterNode::new(
                node_id.to_string(),
                "127.0.0.1".to_string(),
                6380,
                16380,
                true,
            );
            node.flags &= !CLUSTER_NODE_MYSELF;
            node.shard_id = state.myself.shard_id.clone();
            state.nodes.insert(node_id.to_string(), node);
            add_bus_link(&mut backend, token, node_id);
        }
        state.pending_link_closes.push((
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            crate::engine::distributed::cluster::ClusterLinkDirection::To,
        ));
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));

        backend.queue_cluster_publish(
            Bytes::from_static(b"channel"),
            Bytes::from_static(b"payload"),
            true,
        );

        assert!(backend.cluster_bus.connections[&41].write_buf.is_empty());
        assert!(!backend.cluster_bus.connections[&42].write_buf.is_empty());
    }

    #[test]
    fn publish_to_new_outbound_queues_handshake_first() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let peer_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut peer = ClusterNode::new(
            peer_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        peer.flags &= !CLUSTER_NODE_MYSELF;
        peer.shard_id = state.myself.shard_id.clone();
        state.nodes.insert(peer_id.to_string(), peer);
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend.cluster_bus.register_outgoing_for_node(
            Token(41),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            peer_id.to_string(),
        );

        backend.queue_cluster_publish(
            Bytes::from_static(b"channel"),
            Bytes::from_static(b"payload"),
            true,
        );

        let queued = &backend.cluster_bus.connections[&41].write_buf;
        let (first, consumed) =
            crate::engine::distributed::cluster::msg::ClusterBusMessage::decode(queued).unwrap();
        assert_eq!(
            first.msg_type,
            crate::engine::distributed::cluster::msg::ClusterMsgType::Ping
        );
        let (second, _) = crate::engine::distributed::cluster::msg::ClusterBusMessage::decode(
            &queued[consumed..],
        )
        .unwrap();
        assert_eq!(
            second.msg_type,
            crate::engine::distributed::cluster::msg::ClusterMsgType::PublishShard
        );
    }

    #[test]
    fn received_shard_publish_is_delivered_locally_without_rebroadcast() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let token = Token(51);
        let mut subscriber = Connection::new_fake();
        subscriber.set_token_for_test(token);
        let connection_token = backend.connection_registry.insert(subscriber);
        let channel = Bytes::from_static(b"shard-channel");
        backend
            .ctx
            .pubsub_mut()
            .ssubscribe(token.0, channel.clone());
        backend.cluster_bus.pending_publishes.push(
            crate::engine::distributed::cluster::bus::ReceivedClusterPublish {
                channel,
                payload: Bytes::from_static(b"payload"),
                shard: true,
            },
        );
        add_bus_link(&mut backend, 41, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        backend.drain_cluster_bus_publishes();

        let delivered: Vec<_> = backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .output
        .queue
        .iter()
        .flat_map(|frame| frame.iter().copied())
        .collect();
        assert!(delivered.windows(7).any(|window| window == b"payload"));
        assert!(backend.cluster_bus.connections[&41].write_buf.is_empty());
    }

    #[test]
    fn local_only_delivery_does_not_enqueue_a_cluster_frame() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().server.cluster_enabled = true;
        let state = ClusterState::new("127.0.0.1", 6379, 16379);
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        add_bus_link(&mut backend, 41, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        backend.deliver_published(
            vec![(
                Bytes::from_static(b"__keyevent@0__:set"),
                Bytes::from_static(b"key"),
            )],
            Vec::new(),
        );

        assert!(backend.cluster_bus.connections[&41].write_buf.is_empty());
    }
}
