use super::action::{ClusterAction, ClusterActionBatch, ClusterBusReconnect, ConnectHandshakeMode};
use super::event::{ClusterEvent, ResolvedConnectEndpoint};
use super::node::{CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_NOADDR};
use super::state::ClusterState;
use super::ClusterLinkDirection;

/// Applies a Cluster domain event and returns the ordered L5 side effects.
pub fn reduce(state: &mut ClusterState, event: ClusterEvent) -> ClusterActionBatch {
    match event {
        ClusterEvent::BroadcastTopology => {
            let mut actions = ClusterActionBatch::default();
            if state.take_topology_broadcast_request() {
                actions.actions.push(ClusterAction::BroadcastTopology);
            }
            actions
        }
        ClusterEvent::DrainLinkCloses => ClusterActionBatch {
            actions: state
                .take_pending_link_closes()
                .into_iter()
                .map(|(link_id, direction)| {
                    let reconnect = (direction == ClusterLinkDirection::To)
                        .then(|| state.nodes.get(&link_id))
                        .flatten()
                        .filter(|node| !node.has_flag(CLUSTER_NODE_NOADDR) && node.cport() != 0)
                        .map(|node| ClusterBusReconnect {
                            host: node.ip().to_string(),
                            bus_port: node.cport(),
                            temporary_meet: false,
                        });
                    ClusterAction::CloseLinks {
                        link_id,
                        direction,
                        reconnect,
                    }
                })
                .collect(),
        },
        ClusterEvent::DrainPendingForgets => ClusterActionBatch {
            actions: state
                .take_pending_forgets()
                .into_iter()
                .map(|link_id| ClusterAction::ForgetLinks { link_id })
                .collect(),
        },
        ClusterEvent::DrainPendingMeets => ClusterActionBatch {
            actions: state
                .take_pending_meets()
                .into_iter()
                .map(|(host, port, bus_port)| ClusterAction::OpenMeet {
                    host,
                    port,
                    bus_port,
                })
                .collect(),
        },
        ClusterEvent::DrainPendingConnects { resolved_endpoints } => {
            drain_pending_connects(state, &resolved_endpoints)
        }
    }
}

fn drain_pending_connects(
    state: &mut ClusterState,
    resolved_endpoints: &[ResolvedConnectEndpoint],
) -> ClusterActionBatch {
    if state.debug_disable_reconnection() {
        return ClusterActionBatch::default();
    }

    ClusterActionBatch {
        actions: state
            .take_pending_connects()
            .into_iter()
            .map(|(host, bus_port, explicit_or_temporary_meet)| {
                let resolved_ips = resolved_endpoints
                    .iter()
                    .filter(|endpoint| endpoint.host == host && endpoint.bus_port == bus_port)
                    .filter_map(|endpoint| endpoint.resolved_ip.parse::<std::net::IpAddr>().ok())
                    .collect::<Vec<_>>();
                let endpoint_matches = |node_host: &str, node_bus_port: u16| {
                    node_bus_port == bus_port
                        && node_host
                            .parse::<std::net::IpAddr>()
                            .is_ok_and(|ip| resolved_ips.contains(&ip))
                };
                let known_node = state
                    .nodes
                    .values()
                    .find(|node| {
                        endpoint_matches(node.ip(), node.cport())
                            && !node.has_flag(CLUSTER_NODE_HANDSHAKE)
                    })
                    .or_else(|| {
                        state
                            .nodes
                            .values()
                            .find(|node| endpoint_matches(node.ip(), node.cport()))
                    });
                let known_node_name = known_node.map(|node| node.name.clone()).unwrap_or_default();
                let peer_has_confirmed_inbound = !known_node_name.is_empty()
                    && state.has_connected_link(&known_node_name, ClusterLinkDirection::From);
                let send_meet = explicit_or_temporary_meet && !peer_has_confirmed_inbound;
                let rearm_by_node = known_node.is_some_and(|node| node.meet_pending);
                let handshake_mode = match (send_meet, rearm_by_node) {
                    (false, _) => ConnectHandshakeMode::Ping,
                    (true, true) => ConnectHandshakeMode::MeetByNode,
                    (true, false) => ConnectHandshakeMode::MeetByEndpoint,
                };
                let target_is_temporary_handshake =
                    known_node.is_some_and(|node| node.has_flag(CLUSTER_NODE_HANDSHAKE));
                let mut replace_handshakes = if send_meet {
                    Vec::new()
                } else {
                    state
                        .link_views()
                        .filter(|view| {
                            view.direction == ClusterLinkDirection::To
                                && view.temporary_handshake
                                && view
                                    .endpoint
                                    .as_ref()
                                    .and_then(|(host, port)| {
                                        host.parse::<std::net::IpAddr>().ok().map(|ip| (ip, *port))
                                    })
                                    .is_some_and(|(ip, port)| {
                                        port == bus_port && resolved_ips.contains(&ip)
                                    })
                        })
                        .map(|view| view.node_id.to_string())
                        .collect::<Vec<_>>()
                };
                replace_handshakes.sort();
                replace_handshakes.dedup();
                ClusterAction::OpenConnect {
                    host,
                    bus_port,
                    known_node_name,
                    handshake_mode,
                    target_is_temporary_handshake,
                    replace_handshakes,
                }
            })
            .collect(),
    }
}
