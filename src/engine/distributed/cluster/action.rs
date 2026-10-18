use super::{ClusterLinkDirection, NodeId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicationSwitchMode {
    TryPartial,
    ForceFull,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationTarget {
    pub node_id: NodeId,
    pub host: String,
    pub port: u16,
    pub mode: ReplicationSwitchMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Promotion {
    pub old_primary: NodeId,
    pub shard_id: NodeId,
    pub slots: Vec<bool>,
    pub config_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterBusReconnect {
    pub host: String,
    pub bus_port: u16,
    pub temporary_meet: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectHandshakeMode {
    Ping,
    MeetByEndpoint,
    MeetByNode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterAction {
    BroadcastFailoverAuthRequest {
        election_epoch: u64,
        force_ack: bool,
    },
    SendFailoverAuthAck {
        recipient: NodeId,
        election_epoch: u64,
    },
    RecordVote {
        epoch: u64,
    },
    SendManualFailoverStart {
        primary: NodeId,
    },
    BeginManualElection,
    PauseWrites {
        requester: NodeId,
        until_ms: i64,
    },
    ResumeWrites {
        timed_out: bool,
    },
    PromoteSelf(Promotion),
    DetachReplica,
    FollowPrimary(ReplicationTarget),
    PersistClusterConfig,
    BroadcastTopology,
    OpenMeet {
        host: String,
        port: u16,
        bus_port: u16,
    },
    OpenConnect {
        host: String,
        bus_port: u16,
        known_node_name: String,
        handshake_mode: ConnectHandshakeMode,
        target_is_temporary_handshake: bool,
        replace_handshakes: Vec<String>,
    },
    CloseLinks {
        link_id: String,
        direction: ClusterLinkDirection,
        reconnect: Option<ClusterBusReconnect>,
    },
    ForgetLinks {
        link_id: String,
    },
}

/// Ordered domain actions emitted by Cluster state-machine transitions.
/// The engine owns action creation; L5 consumes the batch and performs the
/// corresponding transport, persistence, and replication side effects.
#[derive(Default)]
pub struct ClusterActionBatch {
    pub actions: Vec<ClusterAction>,
}

#[cfg(test)]
mod tests {
    use super::{ClusterAction, ClusterBusReconnect};
    use crate::engine::distributed::cluster::event::ClusterEvent;
    use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_NOADDR};
    use crate::engine::distributed::cluster::reducer::reduce;
    use crate::engine::distributed::cluster::{ClusterLinkDirection, ClusterState};

    #[test]
    fn topology_broadcast_event_drains_request_into_action() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.request_topology_broadcast();

        let actions = reduce(&mut state, ClusterEvent::BroadcastTopology);

        assert_eq!(actions.actions, vec![ClusterAction::BroadcastTopology]);
        assert!(!state.take_topology_broadcast_request());
    }

    #[test]
    fn link_close_event_emits_reconnect_only_for_addressable_outbound_node() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.nodes.insert(
            "peer".into(),
            ClusterNode::new("peer".into(), "192.0.2.1".into(), 6380, 16380, true),
        );
        state
            .pending_link_closes
            .push(("peer".into(), ClusterLinkDirection::To));

        let actions = reduce(&mut state, ClusterEvent::DrainLinkCloses);

        assert_eq!(
            actions.actions,
            vec![ClusterAction::CloseLinks {
                link_id: "peer".into(),
                direction: ClusterLinkDirection::To,
                reconnect: Some(ClusterBusReconnect {
                    host: "192.0.2.1".into(),
                    bus_port: 16380,
                    temporary_meet: false,
                }),
            }]
        );
        assert!(state.pending_link_closes.is_empty());
    }

    #[test]
    fn link_close_event_omits_reconnect_for_non_addressable_or_non_outbound_links() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut noaddr = ClusterNode::new("noaddr".into(), "192.0.2.2".into(), 6381, 16381, true);
        noaddr.flags |= CLUSTER_NODE_NOADDR;
        state.nodes.insert("noaddr".into(), noaddr);
        state.nodes.insert(
            "zero-port".into(),
            ClusterNode::new("zero-port".into(), "192.0.2.3".into(), 6382, 0, true),
        );
        state.pending_link_closes.extend([
            ("inbound".into(), ClusterLinkDirection::From),
            ("all".into(), ClusterLinkDirection::All),
            ("unknown".into(), ClusterLinkDirection::To),
            ("noaddr".into(), ClusterLinkDirection::To),
            ("zero-port".into(), ClusterLinkDirection::To),
        ]);

        let actions = reduce(&mut state, ClusterEvent::DrainLinkCloses);

        assert_eq!(
            actions.actions,
            vec![
                ClusterAction::CloseLinks {
                    link_id: "inbound".into(),
                    direction: ClusterLinkDirection::From,
                    reconnect: None,
                },
                ClusterAction::CloseLinks {
                    link_id: "all".into(),
                    direction: ClusterLinkDirection::All,
                    reconnect: None,
                },
                ClusterAction::CloseLinks {
                    link_id: "unknown".into(),
                    direction: ClusterLinkDirection::To,
                    reconnect: None,
                },
                ClusterAction::CloseLinks {
                    link_id: "noaddr".into(),
                    direction: ClusterLinkDirection::To,
                    reconnect: None,
                },
                ClusterAction::CloseLinks {
                    link_id: "zero-port".into(),
                    direction: ClusterLinkDirection::To,
                    reconnect: None,
                },
            ]
        );
        assert!(state.pending_link_closes.is_empty());
    }

    #[test]
    fn pending_forgets_event_emits_ordered_forget_link_actions() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.pending_forgets.extend([
            "known-node".to_string(),
            "temporary-handshake".to_string(),
            "known-node".to_string(),
        ]);

        let actions = reduce(&mut state, ClusterEvent::DrainPendingForgets);

        assert_eq!(
            actions.actions,
            vec![
                ClusterAction::ForgetLinks {
                    link_id: "known-node".to_string(),
                },
                ClusterAction::ForgetLinks {
                    link_id: "temporary-handshake".to_string(),
                },
                ClusterAction::ForgetLinks {
                    link_id: "known-node".to_string(),
                },
            ]
        );
        assert!(state.pending_forgets.is_empty());
    }

    #[test]
    fn pending_meets_event_emits_ordered_open_meet_actions() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.pending_meets.extend([
            ("192.0.2.1".to_string(), 7000, 17000),
            ("2001:db8::1".to_string(), 7001, 17001),
            ("192.0.2.1".to_string(), 7000, 17000),
        ]);

        let actions = reduce(&mut state, ClusterEvent::DrainPendingMeets);

        assert_eq!(
            actions.actions,
            vec![
                ClusterAction::OpenMeet {
                    host: "192.0.2.1".to_string(),
                    port: 7000,
                    bus_port: 17000,
                },
                ClusterAction::OpenMeet {
                    host: "2001:db8::1".to_string(),
                    port: 7001,
                    bus_port: 17001,
                },
                ClusterAction::OpenMeet {
                    host: "192.0.2.1".to_string(),
                    port: 7000,
                    bus_port: 17000,
                },
            ]
        );
        assert!(state.pending_meets.is_empty());
    }

    #[test]
    fn pending_connects_event_emits_ordered_socket_free_open_connect_actions() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let known_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut known = ClusterNode::new(known_id.into(), "192.0.2.1".into(), 7000, 17000, true);
        known.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;
        known.meet_pending = true;
        state.nodes.insert(known_id.into(), known);
        let handshake_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mut handshake =
            ClusterNode::new(handshake_id.into(), "192.0.2.1".into(), 7000, 17000, true);
        handshake.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;
        handshake.flags |= crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE;
        state.nodes.insert(handshake_id.into(), handshake);
        state.observe_link_open(crate::engine::distributed::cluster::ClusterLinkView::new(
            crate::engine::distributed::cluster::NodeId::parse(known_id.as_bytes()).unwrap(),
            ClusterLinkDirection::From,
            1,
            crate::engine::distributed::cluster::LinkEvents::READABLE,
        ));
        state.observe_link_open(
            crate::engine::distributed::cluster::ClusterLinkView::new(
                crate::engine::distributed::cluster::NodeId::parse(handshake_id.as_bytes())
                    .unwrap(),
                ClusterLinkDirection::To,
                1,
                crate::engine::distributed::cluster::LinkEvents::READABLE,
            )
            .with_endpoint("192.0.2.1".into(), 17000)
            .with_temporary_handshake(true),
        );
        state.observe_link_open(
            crate::engine::distributed::cluster::ClusterLinkView::new(
                crate::engine::distributed::cluster::NodeId::parse(known_id.as_bytes()).unwrap(),
                ClusterLinkDirection::To,
                1,
                crate::engine::distributed::cluster::LinkEvents::READABLE,
            )
            .with_endpoint("192.0.2.1".into(), 17000),
        );
        state.pending_connects.extend([
            ("192.0.2.1".into(), 17000, true),
            ("2001:db8::1".into(), 17001, false),
        ]);

        let actions = reduce(
            &mut state,
            ClusterEvent::DrainPendingConnects {
                resolved_endpoints: vec![
                    crate::engine::distributed::cluster::event::ResolvedConnectEndpoint {
                        host: "192.0.2.1".into(),
                        bus_port: 17000,
                        resolved_ip: "192.0.2.1".into(),
                    },
                ],
            },
        );

        assert_eq!(
            actions.actions,
            vec![
                ClusterAction::OpenConnect {
                    host: "192.0.2.1".into(),
                    bus_port: 17000,
                    known_node_name: known_id.into(),
                    handshake_mode: super::ConnectHandshakeMode::Ping,
                    target_is_temporary_handshake: false,
                    replace_handshakes: vec![handshake_id.into()],
                },
                ClusterAction::OpenConnect {
                    host: "2001:db8::1".into(),
                    bus_port: 17001,
                    known_node_name: String::new(),
                    handshake_mode: super::ConnectHandshakeMode::Ping,
                    target_is_temporary_handshake: false,
                    replace_handshakes: Vec::new(),
                },
            ]
        );
        assert!(state.pending_connects.is_empty());
    }

    #[test]
    fn pending_connects_event_preserves_queue_while_reconnection_is_disabled() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let pending = ("192.0.2.1".to_string(), 17000, true);
        state.pending_connects.push(pending.clone());
        state.set_debug_disable_reconnection(true);

        let actions = reduce(
            &mut state,
            ClusterEvent::DrainPendingConnects {
                resolved_endpoints: Vec::new(),
            },
        );

        assert!(actions.actions.is_empty());
        assert_eq!(state.pending_connects, vec![pending]);
    }

    #[test]
    fn pending_connects_event_replaces_only_temporary_handshake_links() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let known_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut known = ClusterNode::new(known_id.into(), "192.0.2.1".into(), 7000, 17000, true);
        known.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;
        state.nodes.insert(known_id.into(), known);
        state.observe_link_open(
            crate::engine::distributed::cluster::ClusterLinkView::new(
                crate::engine::distributed::cluster::NodeId::parse(known_id.as_bytes()).unwrap(),
                ClusterLinkDirection::To,
                1,
                crate::engine::distributed::cluster::LinkEvents::READABLE,
            )
            .with_endpoint("192.0.2.1".into(), 17000),
        );
        state
            .pending_connects
            .push(("192.0.2.1".into(), 17000, false));

        let actions = reduce(
            &mut state,
            ClusterEvent::DrainPendingConnects {
                resolved_endpoints: vec![
                    crate::engine::distributed::cluster::event::ResolvedConnectEndpoint {
                        host: "192.0.2.1".into(),
                        bus_port: 17000,
                        resolved_ip: "192.0.2.1".into(),
                    },
                ],
            },
        );

        assert!(matches!(
            actions.actions.as_slice(),
            [ClusterAction::OpenConnect {
                replace_handshakes,
                ..
            }] if replace_handshakes.is_empty()
        ));
    }

    #[test]
    fn pending_connects_matches_normalized_ipv6_handshake_endpoint() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let handshake_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        state.observe_link_open(
            crate::engine::distributed::cluster::ClusterLinkView::new(
                crate::engine::distributed::cluster::NodeId::parse(handshake_id.as_bytes())
                    .unwrap(),
                ClusterLinkDirection::To,
                1,
                crate::engine::distributed::cluster::LinkEvents::READABLE,
            )
            .with_endpoint("2001:db8::1".into(), 17000)
            .with_temporary_handshake(true),
        );
        state
            .pending_connects
            .push(("peer.example".into(), 17000, false));

        let actions = reduce(
            &mut state,
            ClusterEvent::DrainPendingConnects {
                resolved_endpoints: vec![
                    crate::engine::distributed::cluster::event::ResolvedConnectEndpoint {
                        host: "peer.example".into(),
                        bus_port: 17000,
                        resolved_ip: "2001:0db8:0:0:0:0:0:1".into(),
                    },
                ],
            },
        );

        assert!(matches!(
            actions.actions.as_slice(),
            [ClusterAction::OpenConnect { replace_handshakes, .. }]
                if replace_handshakes == &[handshake_id.to_string()]
        ));
    }

    #[test]
    fn pending_connects_resolves_node_level_meet_rearm_in_reducer() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let known_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut known = ClusterNode::new(known_id.into(), "127.0.0.1".into(), 7000, 17000, true);
        known.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;
        known.meet_pending = true;
        state.nodes.insert(known_id.into(), known);
        state
            .pending_connects
            .push(("localhost".into(), 17000, true));

        let actions = reduce(
            &mut state,
            ClusterEvent::DrainPendingConnects {
                resolved_endpoints: vec![
                    crate::engine::distributed::cluster::event::ResolvedConnectEndpoint {
                        host: "localhost".into(),
                        bus_port: 17000,
                        resolved_ip: "127.0.0.1".into(),
                    },
                ],
            },
        );

        assert!(matches!(
            actions.actions.as_slice(),
            [ClusterAction::OpenConnect {
                handshake_mode: super::ConnectHandshakeMode::MeetByNode,
                ..
            }]
        ));
    }
}
