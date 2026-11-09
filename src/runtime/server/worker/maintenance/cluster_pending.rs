use crate::engine::distributed::cluster::action::{ClusterAction, ClusterActionBatch};
use crate::engine::distributed::cluster::event::ClusterEvent;
use crate::engine::distributed::cluster::reducer::reduce;
use crate::engine::distributed::cluster::{ClusterLinkDirection, ClusterState};
use crate::foundation::io::transport::Interest;
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::event_loop::sources::SourceOwner;
use crate::runtime::server::worker::ServerWorkerState;
use std::net::{SocketAddr, ToSocketAddrs};

fn resolve_cluster_bus_addr(host: &str, port: u16) -> Option<SocketAddr> {
    let authority = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    authority.to_socket_addrs().ok()?.next()
}

impl ServerWorkerState {
    pub(in crate::runtime) fn execute_cluster_forget_actions(
        &mut self,
        state: &mut ClusterState,
        actions: ClusterActionBatch,
    ) {
        for action in actions.actions {
            let ClusterAction::ForgetLinks { link_id } = action else {
                continue;
            };
            let tokens: Vec<_> = self
                .cluster_bus
                .connections
                .iter()
                .filter_map(|(token_value, connection)| {
                    (connection.node_name == link_id || connection.handshake_node_name == link_id)
                        .then_some(*token_value)
                })
                .collect();
            let mut n_removed = 0;
            for token_value in tokens {
                let existed = self.cluster_bus.connections.contains_key(&token_value);
                self.teardown_cluster_bus_connection_with_state(state, Token(token_value), false);
                n_removed += usize::from(
                    existed && !self.cluster_bus.connections.contains_key(&token_value),
                );
            }
            crate::server_log!(
                notice,
                "Cluster FORGET processed: removed {} bus connections for {}",
                n_removed,
                link_id
            );
        }
    }

    pub(in crate::runtime) fn execute_cluster_link_actions(
        &mut self,
        state: &mut ClusterState,
        actions: ClusterActionBatch,
    ) {
        for action in actions.actions {
            let ClusterAction::CloseLinks {
                link_id,
                direction,
                reconnect,
            } = action
            else {
                continue;
            };
            let tokens: Vec<_> = self
                .cluster_bus
                .connections
                .iter()
                .filter_map(|(token_value, connection)| {
                    let direction_matches = match direction {
                        ClusterLinkDirection::To => connection.is_outgoing,
                        ClusterLinkDirection::From => !connection.is_outgoing,
                        ClusterLinkDirection::All => true,
                    };
                    ((connection.node_name == link_id || connection.handshake_node_name == link_id)
                        && direction_matches)
                        .then_some(*token_value)
                })
                .collect();
            for token_value in tokens {
                self.teardown_cluster_bus_connection_with_state(state, Token(token_value), false);
            }
            if let Some(reconnect) = reconnect {
                let reconnect = (reconnect.host, reconnect.bus_port, reconnect.temporary_meet);
                state.queue_connect_once(reconnect);
            }
        }
    }

    pub(in crate::runtime) fn execute_cluster_meet_actions(
        &mut self,
        state: &mut ClusterState,
        actions: ClusterActionBatch,
    ) {
        for action in actions.actions {
            let ClusterAction::OpenMeet {
                host,
                port,
                bus_port,
            } = action
            else {
                continue;
            };
            let Some(addr) = resolve_cluster_bus_addr(&host, bus_port) else {
                continue;
            };
            let handshake_id = state.ensure_handshake_node(
                &host,
                port,
                bus_port,
                crate::foundation::io::sys::time::mstime(),
            );
            let connect_result = if self.ctx.config().server.tls_cluster {
                self.configure_tls_client_from_config()
                    .and_then(|_| self.transport.connect_tls_bound(addr, None, host.as_str()))
            } else {
                self.transport.connect_tcp(addr)
            }
            .and_then(|handle| {
                self.route_runtime_source(handle.token, SourceOwner::ClusterBus)?;
                Ok(handle)
            });
            match connect_result {
                Ok(handle) => {
                    self.cluster_bus.register_outgoing_for_handshake(
                        handle.token,
                        addr,
                        handshake_id,
                    );
                    self.cluster_bus
                        .queue_initial_handshake(state, handle.token.0);
                    let write_result = self.transport.stream(handle.token).map(|mut stream| {
                        self.cluster_bus
                            .writable(&mut stream, handle.token.0, state)
                    });
                    match write_result {
                        Some(Ok(written)) => crate::server_log!(
                            notice,
                            "Cluster MEET initial write token={} bytes={}",
                            handle.token.0,
                            written
                        ),
                        Some(Err(error)) => crate::server_log!(
                            warning,
                            "Cluster MEET initial write to {}:{} failed: {}",
                            host,
                            bus_port,
                            error
                        ),
                        None => crate::server_log!(
                            warning,
                            "Cluster MEET initial write stream missing token={}",
                            handle.token.0
                        ),
                    }
                    let interest = self
                        .cluster_bus
                        .connections
                        .get(&handle.token.0)
                        .map(|connection| {
                            if connection.write_buf.is_empty() {
                                Interest::Readable
                            } else {
                                Interest::ReadWritable
                            }
                        })
                        .unwrap_or(Interest::ReadWritable);
                    let _ = self.transport.set_interest(handle.token, interest);
                    crate::server_log!(
                        notice,
                        "Cluster MEET connecting to {}:{} (bus port {})",
                        host,
                        port,
                        bus_port
                    );
                }
                Err(e) => {
                    crate::server_log!(
                        warning,
                        "Cluster MEET connect to {}:{} failed: {}",
                        host,
                        bus_port,
                        e
                    );
                }
            }
        }
    }

    pub(crate) fn cluster_process_pending_dirty_slots(
        &mut self,
        state: &mut crate::engine::distributed::cluster::state::ClusterState,
    ) {
        for slot in state.take_dirty_slot_cleanup_requests() {
            self.ctx.slot_metrics_mut().reset_slot(slot);
            let deleted_keys = self.ctx.take_slot_entries(slot);
            if !deleted_keys.is_empty() {
                crate::server_log!(notice, "Deleting keys in dirty slot {}", slot);
                self.ctx
                    .persistence_mut()
                    .add_dirty(deleted_keys.len() as i64);
                for entry in deleted_keys {
                    self._propagate_background_unlink(entry.db_id, &entry.key);
                }
            }
        }
    }

    pub(crate) fn cluster_process_pending_forgets(&mut self, state: &mut ClusterState) {
        let actions = reduce(state, ClusterEvent::DrainPendingForgets);
        self.execute_cluster_forget_actions(state, actions);
    }

    pub(crate) fn cluster_process_pending_meets(&mut self, state: &mut ClusterState) {
        let actions = reduce(state, ClusterEvent::DrainPendingMeets);
        self.execute_cluster_meet_actions(state, actions);
    }

    pub(in crate::runtime) fn execute_cluster_connect_actions(
        &mut self,
        state: &mut ClusterState,
        actions: ClusterActionBatch,
    ) {
        for action in actions.actions {
            let ClusterAction::OpenConnect {
                host,
                bus_port,
                known_node_name,
                handshake_mode,
                target_is_temporary_handshake,
                replace_handshakes,
            } = action
            else {
                continue;
            };
            let Some(addr) = resolve_cluster_bus_addr(&host, bus_port) else {
                continue;
            };
            let send_meet = handshake_mode
                != crate::engine::distributed::cluster::action::ConnectHandshakeMode::Ping;
            if !send_meet {
                for temporary_id in replace_handshakes {
                    let close = (temporary_id, ClusterLinkDirection::All);
                    state.queue_link_close_once(close);
                }
            }

            if send_meet {
                let rearmed = if handshake_mode
                    == crate::engine::distributed::cluster::action::ConnectHandshakeMode::MeetByNode
                {
                    self.cluster_bus
                        .rearm_outgoing_for_node(addr, &known_node_name)
                } else {
                    self.cluster_bus.arm_meet_for_addr(addr)
                };
                if let Some(token_value) = rearmed {
                    let token = Token(token_value);
                    self.cluster_bus.queue_initial_handshake(state, token_value);
                    let _ = self.transport.set_interest(token, Interest::ReadWritable);
                    continue;
                }
            }

            let already_connected = self.cluster_bus.connections.values().any(|connection| {
                connection.is_outgoing
                    && connection.connected
                    && connection.node_name == known_node_name
            });
            if already_connected {
                continue;
            }
            let connect_result = if self.ctx.config().server.tls_cluster {
                self.configure_tls_client_from_config()
                    .and_then(|_| self.transport.connect_tls_bound(addr, None, host.as_str()))
            } else {
                self.transport.connect_tcp(addr)
            }
            .and_then(|handle| {
                self.route_runtime_source(handle.token, SourceOwner::ClusterBus)?;
                Ok(handle)
            });
            match connect_result {
                Ok(handle) => {
                    if send_meet && target_is_temporary_handshake {
                        self.cluster_bus.register_outgoing_for_handshake(
                            handle.token,
                            addr,
                            known_node_name,
                        );
                    } else {
                        self.cluster_bus.register_outgoing_for_node(
                            handle.token,
                            addr,
                            false,
                            known_node_name,
                        );
                    }
                    self.cluster_bus
                        .queue_initial_handshake(state, handle.token.0);
                    let _ = self
                        .transport
                        .set_interest(handle.token, Interest::ReadWritable);
                    crate::server_log!(
                        debug,
                        "Cluster auto-connect to {}:{} (explicit-meet={})",
                        host,
                        bus_port,
                        send_meet
                    );
                }
                Err(error) => {
                    crate::server_log!(
                        warning,
                        "Cluster auto-connect to {}:{} failed: {}",
                        host,
                        bus_port,
                        error
                    );
                }
            }
        }
    }

    pub(crate) fn cluster_process_pending_connects(&mut self, state: &mut ClusterState) {
        self.refresh_cluster_link_views_for_state(state);
        let resolved_endpoints = state
            .pending_connects()
            .iter()
            .filter_map(|(host, bus_port, _)| {
                resolve_cluster_bus_addr(host, *bus_port).map(|addr| {
                    crate::engine::distributed::cluster::event::ResolvedConnectEndpoint {
                        host: host.clone(),
                        bus_port: *bus_port,
                        resolved_ip: addr.ip().to_string(),
                    }
                })
            })
            .collect();
        let actions = reduce(
            state,
            ClusterEvent::DrainPendingConnects { resolved_endpoints },
        );
        self.execute_cluster_connect_actions(state, actions);
    }

    /// Drain pending MEET / FORGET / CONNECT requests and drive bus connections.
    pub(crate) fn cluster_process_pending_ops(
        &mut self,
        state: &mut crate::engine::distributed::cluster::state::ClusterState,
    ) {
        self.cluster_process_pending_dirty_slots(state);
        let actions = reduce(state, ClusterEvent::DrainLinkCloses);
        self.execute_cluster_link_actions(state, actions);

        self.cluster_process_pending_meets(state);
        self.cluster_process_pending_forgets(state);

        self.cluster_process_pending_connects(state);
    }
}

#[cfg(test)]
mod tests {
    use super::{resolve_cluster_bus_addr, ServerWorkerState};
    use crate::engine::distributed::cluster::action::{
        ClusterAction, ClusterActionBatch, ClusterBusReconnect,
    };
    use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};
    use crate::engine::distributed::cluster::LinkEvents;
    use crate::engine::distributed::cluster::{ClusterLinkDirection, ClusterState};
    use crate::foundation::io::transport::Interest;
    use crate::framework::capabilities::io::Token;
    use std::io::{self, Read};
    use std::net::TcpListener;
    use std::time::Duration;

    fn queued_or_sent_bus_bytes(listener: &TcpListener, queued: &[u8]) -> Vec<u8> {
        if !queued.is_empty() {
            return queued.to_vec();
        }
        let (mut stream, _) = listener.accept().expect("accept outbound bus connection");
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("configure bus read timeout");
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let read = stream.read(&mut chunk).expect("read sent bus frame");
            assert!(
                read != 0,
                "outbound bus connection closed before its frame arrived"
            );
            bytes.extend_from_slice(&chunk[..read]);
            if crate::engine::distributed::cluster::msg::ClusterBusMessage::decode(&bytes).is_ok() {
                return bytes;
            }
        }
    }

    #[test]
    fn dirty_slot_cleanup_propagates_deleted_keys_to_replicas() {
        let mut backend = ServerWorkerState::new_with_slot_router(1, 14, |key| {
            crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
        })
        .unwrap();
        backend.ctx.replication_mut().backlog.activate();
        let key = b"FOO";
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(key) as usize;
        backend.ctx.db_mut(0).unwrap().set(
            key.to_vec(),
            crate::foundation::io::store::Robj::string(b"value".as_slice()),
        );
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.request_dirty_slot_cleanup(slot);
        let before = backend.ctx.replication().backlog.next_stream_cursor();

        backend.cluster_process_pending_ops(&mut state);

        assert!(backend.ctx.db(0).unwrap().get_allow_expired(key).is_none());
        let replicated = backend.ctx.replication().backlog.read_from(before).unwrap();
        let unlink =
            crate::engine::distributed::replication::protocol::encode_command(&[b"UNLINK", key]);
        assert!(
            replicated
                .windows(unlink.len())
                .any(|window| window == unlink),
            "dirty-slot cleanup must replicate UNLINK: {:?}",
            String::from_utf8_lossy(&replicated)
        );
    }

    #[test]
    fn cluster_bus_address_parser_accepts_ipv4_and_ipv6_literals() {
        assert_eq!(
            resolve_cluster_bus_addr("127.0.0.1", 16379),
            Some("127.0.0.1:16379".parse().unwrap())
        );
        assert_eq!(
            resolve_cluster_bus_addr("::1", 16379),
            Some("[::1]:16379".parse().unwrap())
        );
    }

    #[test]
    fn failed_cluster_teardown_isolates_owner_and_retries_l1_only() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().server.cluster_enabled = true;
        backend.ctx.config_mut().server.cluster_ping_interval = 1;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind cluster-bus test listener");
        let peer_addr = listener
            .local_addr()
            .expect("get cluster-bus test listener address");
        let peer = backend
            .transport
            .connect_tcp(peer_addr)
            .expect("connect cluster-bus test transport");
        let (_server_stream, _) = listener
            .accept()
            .expect("accept cluster-bus test transport");
        let node_name = "pending-teardown-peer".to_string();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut node = ClusterNode::new(
            node_name.clone(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.ping_sent = 41;
        node.pong_received = 1;
        state.nodes.insert(node_name.clone(), node);
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));

        backend.cluster_bus.register_outgoing_for_node(
            peer.token,
            peer_addr,
            false,
            node_name.clone(),
        );
        let connection = backend
            .cluster_bus
            .connections
            .get_mut(&peer.token.0)
            .expect("registered cluster-bus connection");
        connection.handshake_sent = true;
        connection.write_buf.extend_from_slice(b"pending-removal");
        backend
            .transport
            .deregister_for_test(peer.token)
            .expect("deregister cluster-bus test transport");
        backend.remove_cluster_bus_connection_without_retry(peer.token);

        // Isolate-first teardown drops the bus connection immediately; only the
        // minimal retry entry keeps the L1 registration reserved (ABA-safe).
        assert!(!backend.cluster_bus.connections.contains_key(&peer.token.0));
        assert!(backend.is_pending_transport_removal(peer.token));

        // Cron runs against an already-isolated owner: it never resurrects the
        // torn-down connection nor inspects any pending predicate.
        backend.run_cluster_cron();
        assert!(!backend.cluster_bus.connections.contains_key(&peer.token.0));
        assert!(backend.is_pending_transport_removal(peer.token));

        backend
            .transport
            .reregister_for_test(peer.token)
            .expect("reregister cluster-bus test transport");
        backend.retry_pending_transport_removals();

        assert!(!backend.cluster_bus.connections.contains_key(&peer.token.0));
        assert!(!backend.is_pending_transport_removal(peer.token));
    }

    #[test]
    fn debug_reconnection_gate_preserves_pending_connects_without_opening_links() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind cluster peer");
        let peer_addr = listener.local_addr().expect("cluster peer address");
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.set_debug_disable_reconnection(true);
        state
            .pending_connects
            .push((peer_addr.ip().to_string(), peer_addr.port(), false));

        backend.cluster_process_pending_ops(&mut state);

        assert_eq!(state.pending_connects.len(), 1);
        assert!(backend.cluster_bus.connections.is_empty());
    }

    #[test]
    fn asymmetric_meet_rearms_an_existing_outbound_link_for_writing() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let addr = "127.0.0.1:16380".parse().unwrap();
        let token = Token(91);
        backend.cluster_bus.register_outgoing_for_node(
            token,
            addr,
            false,
            "known-peer".to_string(),
        );
        backend
            .cluster_bus
            .connections
            .get_mut(&token.0)
            .unwrap()
            .events = LinkEvents::READABLE;
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state
            .pending_connects
            .push(("127.0.0.1".to_string(), 16380, true));

        backend.cluster_process_pending_ops(&mut state);

        let connection = &backend.cluster_bus.connections[&token.0];
        assert!(connection.send_meet);
        assert!(connection.handshake_sent);
        assert!(!connection.write_buf.is_empty());
        assert_eq!(connection.events, LinkEvents::READ_WRITE);
    }

    #[test]
    fn node_level_meet_rearm_does_not_stick_to_the_outbound_connection() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let addr = "127.0.0.1:16380".parse().unwrap();
        let token = Token(95);
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        backend
            .cluster_bus
            .register_outgoing_for_node(token, addr, false, node_id.to_string());
        let connection = backend.cluster_bus.connections.get_mut(&token.0).unwrap();
        connection.handshake_sent = true;
        connection.events = LinkEvents::READABLE;

        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut node = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.meet_pending = true;
        state.nodes.insert(node_id.to_string(), node);
        state
            .pending_connects
            .push(("127.0.0.1".to_string(), 16380, true));

        backend.cluster_process_pending_ops(&mut state);

        let connection = &backend.cluster_bus.connections[&token.0];
        assert!(!connection.send_meet);
        assert!(connection.handshake_sent);
        assert_eq!(connection.events, LinkEvents::READ_WRITE);
        let (message, _) = crate::engine::distributed::cluster::msg::ClusterBusMessage::decode(
            &connection.write_buf,
        )
        .unwrap();
        assert_eq!(
            message.msg_type,
            crate::engine::distributed::cluster::msg::ClusterMsgType::Meet
        );
    }

    #[test]
    fn explicit_handshake_connect_keeps_meet_on_the_temporary_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let placeholder_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mut placeholder = ClusterNode::new(
            placeholder_id.to_string(),
            addr.ip().to_string(),
            addr.port(),
            addr.port(),
            true,
        );
        placeholder.flags &= !CLUSTER_NODE_MYSELF;
        placeholder.flags |= crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE;
        state.nodes.insert(placeholder_id.to_string(), placeholder);
        state
            .pending_connects
            .push((addr.ip().to_string(), addr.port(), true));

        backend.cluster_process_pending_ops(&mut state);

        let connection = backend
            .cluster_bus
            .connections
            .values()
            .find(|connection| connection.handshake_node_name == placeholder_id)
            .expect("temporary handshake connection");
        assert!(
            connection.send_meet,
            "explicit temporary handshakes must send MEET rather than PING"
        );
        assert!(connection.handshake_sent);
        let (message, _) = crate::engine::distributed::cluster::msg::ClusterBusMessage::decode(
            &connection.write_buf,
        )
        .unwrap();
        assert_eq!(
            message.msg_type,
            crate::engine::distributed::cluster::msg::ClusterMsgType::Meet
        );
    }

    #[test]
    fn expired_retry_marked_handshake_replaces_identity_and_sends_meet() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().server.cluster_enabled = true;
        backend.ctx.config_mut().server.cluster_node_timeout = 1;
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let now_ms = crate::foundation::io::sys::time::mstime();
        let old_id = state.ensure_handshake_node(
            &addr.ip().to_string(),
            6381,
            addr.port(),
            now_ms.saturating_sub(1_001),
        );
        state.mark_handshake_retry(&old_id);
        let handle = backend.transport.connect_tcp(addr).unwrap();
        let (mut old_peer, _) = listener.accept().unwrap();
        old_peer
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        backend
            .cluster_bus
            .register_outgoing_for_handshake(handle.token, addr, old_id.clone());
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));

        backend.run_cluster_cron();

        let state = backend.ctx.cluster_state().as_ref().unwrap();
        assert!(!state.nodes.contains_key(&old_id));
        assert_eq!(old_peer.read(&mut [0u8; 1]).unwrap(), 0);
        assert!(state.pending_connects.is_empty());
        assert_eq!(backend.cluster_bus.connections.len(), 1);
        let connection = backend.cluster_bus.connections.values().next().unwrap();
        let bytes = queued_or_sent_bus_bytes(&listener, &connection.write_buf);
        let (message, _) =
            crate::engine::distributed::cluster::msg::ClusterBusMessage::decode(&bytes).unwrap();
        assert_eq!(
            message.msg_type,
            crate::engine::distributed::cluster::msg::ClusterMsgType::Meet
        );
        assert!(connection.node_name.is_empty());
        assert!(!connection.handshake_node_name.is_empty());
        assert_ne!(connection.handshake_node_name, old_id);
        assert!(connection.send_meet);
        assert!(connection.meet_sent_at >= now_ms);
        assert_eq!(connection.addr, addr);
        let replacement = &state.nodes[&connection.handshake_node_name];
        assert!(
            replacement.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE)
        );
        assert!(replacement.handshake_retry_on_timeout);
        assert_eq!(replacement.ip(), addr.ip().to_string());
        assert_eq!(replacement.port(), 6381);
        assert_eq!(replacement.cport(), addr.port());
        assert!(replacement.created_at >= now_ms);
    }

    #[test]
    fn explicit_meet_queues_its_first_bus_frame_without_waiting_for_writable_edge() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state
            .pending_meets
            .push((addr.ip().to_string(), addr.port(), addr.port()));

        backend.cluster_process_pending_ops(&mut state);

        let connection = backend
            .cluster_bus
            .connections
            .values()
            .find(|connection| !connection.handshake_node_name.is_empty())
            .expect("explicit MEET connection");
        assert!(connection.handshake_sent);
        let bytes = queued_or_sent_bus_bytes(&listener, &connection.write_buf);
        let (message, _) =
            crate::engine::distributed::cluster::msg::ClusterBusMessage::decode(&bytes).unwrap();
        assert_eq!(
            message.msg_type,
            crate::engine::distributed::cluster::msg::ClusterMsgType::Meet
        );
    }

    #[test]
    fn open_meet_action_opens_handshake_connection_with_queued_meet_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        backend.transport.clear_interest_requests_for_test();

        backend.execute_cluster_meet_actions(
            &mut state,
            ClusterActionBatch {
                actions: vec![ClusterAction::OpenMeet {
                    host: addr.ip().to_string(),
                    port: addr.port(),
                    bus_port: addr.port(),
                }],
            },
        );

        let connection = backend
            .cluster_bus
            .connections
            .values()
            .find(|connection| !connection.handshake_node_name.is_empty())
            .expect("outgoing handshake connection");
        let token = connection.token;
        assert!(connection.handshake_sent);
        // The first write may race a nonblocking connect; finish it on real writable readiness.
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !backend.cluster_bus.connections[&token.0]
            .write_buf
            .is_empty()
        {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "timed out flushing MEET frame for {token:?}"
            );
            let events = backend.transport.poll_events(Some(remaining)).unwrap();
            if events
                .iter()
                .any(|event| event.token == token && event.writable)
            {
                backend.handle_cluster_bus_writable(token);
            }
            std::thread::yield_now();
        }
        let bytes = queued_or_sent_bus_bytes(
            &listener,
            &backend.cluster_bus.connections[&token.0].write_buf,
        );
        let (message, _) =
            crate::engine::distributed::cluster::msg::ClusterBusMessage::decode(&bytes).unwrap();
        assert_eq!(
            message.msg_type,
            crate::engine::distributed::cluster::msg::ClusterMsgType::Meet
        );
        assert!(
            backend
                .transport
                .last_set_interest_calls_for_test()
                .contains(&(token, Interest::Readable)),
            "opening a MEET connection must leave only readable interest after the immediate flush"
        );
        assert!(state.pending_connects.is_empty());
    }

    #[test]
    fn open_meet_action_with_unresolvable_host_consumes_action_without_connection() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

        backend.execute_cluster_meet_actions(
            &mut state,
            ClusterActionBatch {
                actions: vec![ClusterAction::OpenMeet {
                    host: "invalid host".to_string(),
                    port: 6379,
                    bus_port: 16379,
                }],
            },
        );

        assert!(backend.cluster_bus.connections.is_empty());
        assert!(state.pending_connects.is_empty());
    }

    #[test]
    fn open_meet_action_with_connect_failure_consumes_action_without_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        backend
            .transport
            .set_next_connect_error_for_test(io::Error::from(io::ErrorKind::ConnectionRefused));

        backend.execute_cluster_meet_actions(
            &mut state,
            ClusterActionBatch {
                actions: vec![ClusterAction::OpenMeet {
                    host: addr.ip().to_string(),
                    port: addr.port(),
                    bus_port: addr.port(),
                }],
            },
        );

        assert!(backend.cluster_bus.connections.is_empty());
        assert!(state.pending_connects.is_empty());
    }

    #[test]
    fn normal_reconnect_replaces_an_endpoint_matched_handshake_link_for_the_known_node() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let token = Token(92);
        let handshake_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        backend
            .cluster_bus
            .register_outgoing_for_handshake(token, addr, handshake_id.to_string());
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut node = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            addr.port(),
            addr.port(),
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(node_id.to_string(), node);
        state
            .pending_connects
            .push((addr.ip().to_string(), addr.port(), false));

        backend.cluster_process_pending_ops(&mut state);

        assert!(state.pending_link_closes.contains(&(
            handshake_id.to_string(),
            crate::engine::distributed::cluster::ClusterLinkDirection::All,
        )));
        assert!(backend
            .cluster_bus
            .connections
            .values()
            .any(|connection| connection.node_name == node_id));
    }

    #[test]
    fn stale_meet_recovery_replaces_handshake_link_when_the_peer_is_already_confirmed_inbound() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let addr = "127.0.0.1:16380".parse().unwrap();
        let handshake_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        backend.cluster_bus.register_outgoing_for_handshake(
            Token(93),
            addr,
            handshake_id.to_string(),
        );
        backend
            .cluster_bus
            .register_incoming(Token(94), "127.0.0.1:26380".parse().unwrap());
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        backend
            .cluster_bus
            .connections
            .get_mut(&94)
            .unwrap()
            .node_name = node_id.to_string();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut node = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(node_id.to_string(), node);
        state
            .pending_connects
            .push(("127.0.0.1".to_string(), 16380, true));

        backend.cluster_process_pending_ops(&mut state);

        assert!(state.pending_link_closes.contains(&(
            handshake_id.to_string(),
            crate::engine::distributed::cluster::ClusterLinkDirection::All,
        )));
    }

    #[test]
    fn reconnect_prefers_a_known_node_over_an_endpoint_matched_handshake_placeholder() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let real_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let placeholder_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mut real = ClusterNode::new(
            real_id.to_string(),
            addr.ip().to_string(),
            addr.port(),
            addr.port(),
            true,
        );
        real.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(real_id.to_string(), real);
        let mut placeholder = ClusterNode::new(
            placeholder_id.to_string(),
            addr.ip().to_string(),
            addr.port(),
            addr.port(),
            true,
        );
        placeholder.flags &= !CLUSTER_NODE_MYSELF;
        placeholder.flags |= crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE;
        state.nodes.insert(placeholder_id.to_string(), placeholder);
        state
            .pending_connects
            .push((addr.ip().to_string(), addr.port(), false));

        backend.cluster_process_pending_ops(&mut state);

        assert!(backend
            .cluster_bus
            .connections
            .values()
            .any(|connection| connection.node_name == real_id));
        assert!(!backend
            .cluster_bus
            .connections
            .values()
            .any(|connection| connection.node_name == placeholder_id));
    }

    #[test]
    fn execute_link_close_action_closes_addressable_outbound_link_and_queues_reconnect() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = backend.transport.connect_tcp(addr).unwrap();
        let _server = listener.accept().unwrap();
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut node = ClusterNode::new(
            node_id.to_string(),
            addr.ip().to_string(),
            addr.port(),
            addr.port(),
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(node_id.to_string(), node);
        backend.cluster_bus.register_outgoing_for_node(
            peer.token,
            addr,
            false,
            node_id.to_string(),
        );

        backend.execute_cluster_link_actions(
            &mut state,
            ClusterActionBatch {
                actions: vec![ClusterAction::CloseLinks {
                    link_id: node_id.to_string(),
                    direction: ClusterLinkDirection::To,
                    reconnect: Some(ClusterBusReconnect {
                        host: addr.ip().to_string(),
                        bus_port: addr.port(),
                        temporary_meet: false,
                    }),
                }],
            },
        );

        assert!(!backend.cluster_bus.connections.contains_key(&peer.token.0));
        assert_eq!(
            state.pending_connects,
            vec![(addr.ip().to_string(), addr.port(), false)]
        );
    }

    #[test]
    fn execute_link_close_action_from_closes_only_the_incoming_link() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        backend.cluster_bus.register_outgoing_for_node(
            Token(101),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id.to_string(),
        );
        backend
            .cluster_bus
            .register_incoming(Token(102), "127.0.0.1:26380".parse().unwrap());
        backend
            .cluster_bus
            .connections
            .get_mut(&102)
            .unwrap()
            .node_name = node_id.to_string();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

        backend.execute_cluster_link_actions(
            &mut state,
            ClusterActionBatch {
                actions: vec![ClusterAction::CloseLinks {
                    link_id: node_id.to_string(),
                    direction: ClusterLinkDirection::From,
                    reconnect: None,
                }],
            },
        );

        assert!(backend.cluster_bus.connections.contains_key(&101));
        assert!(!backend.cluster_bus.connections.contains_key(&102));
        assert!(state.pending_connects.is_empty());
    }

    #[test]
    fn execute_link_close_action_queues_and_deduplicates_explicit_reconnect_without_token() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let reconnect = ClusterBusReconnect {
            host: "192.0.2.1".to_string(),
            bus_port: 16380,
            temporary_meet: true,
        };

        backend.execute_cluster_link_actions(
            &mut state,
            ClusterActionBatch {
                actions: vec![
                    ClusterAction::CloseLinks {
                        link_id: "unconnected-peer".to_string(),
                        direction: ClusterLinkDirection::All,
                        reconnect: Some(reconnect.clone()),
                    },
                    ClusterAction::CloseLinks {
                        link_id: "unconnected-peer".to_string(),
                        direction: ClusterLinkDirection::All,
                        reconnect: Some(reconnect),
                    },
                ],
            },
        );

        assert_eq!(
            state.pending_connects,
            vec![("192.0.2.1".to_string(), 16380, true)]
        );
    }

    #[test]
    fn forget_links_action_removes_matching_node_links_without_reconnect() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        backend.cluster_bus.register_outgoing_for_node(
            Token(201),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id.to_string(),
        );
        backend
            .cluster_bus
            .register_incoming(Token(202), "127.0.0.1:26380".parse().unwrap());
        backend
            .cluster_bus
            .connections
            .get_mut(&202)
            .unwrap()
            .node_name = node_id.to_string();
        backend.cluster_bus.register_outgoing_for_node(
            Token(203),
            "127.0.0.1:16381".parse().unwrap(),
            false,
            "other-node".to_string(),
        );
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

        backend.execute_cluster_forget_actions(
            &mut state,
            ClusterActionBatch {
                actions: vec![ClusterAction::ForgetLinks {
                    link_id: node_id.to_string(),
                }],
            },
        );

        assert!(!backend.cluster_bus.connections.contains_key(&201));
        assert!(!backend.cluster_bus.connections.contains_key(&202));
        assert!(backend.cluster_bus.connections.contains_key(&203));
        assert!(state.pending_connects.is_empty());
    }

    #[test]
    fn forget_links_action_matches_handshake_identity_and_allows_no_match() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.cluster_bus.register_outgoing_for_handshake(
            Token(204),
            "127.0.0.1:16380".parse().unwrap(),
            "temporary-handshake".to_string(),
        );
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

        backend.execute_cluster_forget_actions(
            &mut state,
            ClusterActionBatch {
                actions: vec![
                    ClusterAction::ForgetLinks {
                        link_id: "temporary-handshake".to_string(),
                    },
                    ClusterAction::ForgetLinks {
                        link_id: "already-gone".to_string(),
                    },
                ],
            },
        );

        assert!(!backend.cluster_bus.connections.contains_key(&204));
        assert!(state.pending_connects.is_empty());
    }

    #[test]
    fn pending_forgets_reducer_drain_removes_links_and_preserves_connect_queue() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        backend.cluster_bus.register_outgoing_for_node(
            Token(205),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id.to_string(),
        );
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.pending_forgets.push(node_id.to_string());
        state
            .pending_connects
            .push(("127.0.0.1".to_string(), 16380, false));

        backend.cluster_process_pending_forgets(&mut state);

        assert!(!backend.cluster_bus.connections.contains_key(&205));
        assert!(state.pending_forgets.is_empty());
        assert_eq!(
            state.pending_connects,
            vec![("127.0.0.1".to_string(), 16380, false)]
        );
    }

    #[test]
    fn normal_reconnect_queues_its_first_ping_without_waiting_for_writable_edge() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut node = ClusterNode::new(
            node_id.to_string(),
            addr.ip().to_string(),
            addr.port(),
            addr.port(),
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(node_id.to_string(), node);
        state
            .pending_connects
            .push((addr.ip().to_string(), addr.port(), false));

        backend.cluster_process_pending_ops(&mut state);

        let connection = backend
            .cluster_bus
            .connections
            .values()
            .find(|connection| connection.node_name == node_id)
            .expect("normal reconnect");
        assert!(connection.handshake_sent);
        let (message, _) = crate::engine::distributed::cluster::msg::ClusterBusMessage::decode(
            &connection.write_buf,
        )
        .unwrap();
        assert_eq!(
            message.msg_type,
            crate::engine::distributed::cluster::msg::ClusterMsgType::Ping
        );
    }
}
