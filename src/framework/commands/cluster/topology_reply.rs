use crate::engine::distributed::cluster::node::{
    CLUSTER_NODE_FAIL, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_MYSELF, CLUSTER_NODE_NOADDR,
    CLUSTER_NODE_NOFAILOVER, CLUSTER_NODE_PFAIL,
};
use crate::engine::distributed::cluster::{
    ClientTransport, ClusterNodeHealth, ClusterNodeRole, ClusterTopologySnapshot, NodeId, NodeView,
    PreferredEndpoint,
};
use crate::foundation::io::protocol::Value;

pub(super) fn render_nodes(
    snapshot: &ClusterTopologySnapshot,
    transport: ClientTransport,
    prefer_ipv6: bool,
) -> String {
    let fallback_owner = fallback_owner(snapshot);
    let mut lines = Vec::with_capacity(snapshot.nodes.len());
    let ordered_nodes = snapshot
        .nodes
        .iter()
        .filter(|node| node.flags & CLUSTER_NODE_MYSELF != 0)
        .chain(
            snapshot
                .nodes
                .iter()
                .filter(|node| node.flags & CLUSTER_NODE_MYSELF == 0),
        );
    for node in ordered_nodes {
        let endpoint = node
            .endpoints
            .client_endpoint_for_family(transport, prefer_ipv6);
        let hostname = node
            .endpoints
            .hostname()
            .map(|hostname| format!(",{hostname}"))
            .unwrap_or_default();
        let primary = node.replica_of.as_ref().map(NodeId::as_str).unwrap_or("-");
        let slots =
            if node.health == ClusterNodeHealth::Fail && node.flags & CLUSTER_NODE_MYSELF == 0 {
                String::new()
            } else {
                slot_ranges_for_node(&snapshot.slot_owners, &node.id, fallback_owner.as_ref())
            };
        lines.push(
            format!(
                "{} {}:{}@{}{} {} {} {} {} {} {} {}",
                node.id,
                endpoint.host,
                endpoint.port,
                node.endpoints.bus_port(),
                hostname,
                node_flags(node.flags),
                primary,
                node.ping_sent,
                node.pong_received,
                node.config_epoch,
                if node.outbound_connected {
                    "connected"
                } else {
                    "disconnected"
                },
                slots
            )
            .trim()
            .to_string(),
        );
    }
    if let Some(myself) = snapshot
        .nodes
        .iter()
        .find(|node| node.flags & CLUSTER_NODE_MYSELF != 0)
    {
        let mut transitions = snapshot
            .migrating_slots_to
            .iter()
            .map(|(slot, target)| (*slot, format!("[{slot}->-{target}]")))
            .chain(
                snapshot
                    .importing_slots_from
                    .iter()
                    .map(|(slot, source)| (*slot, format!("[{slot}-<-{source}]"))),
            )
            .collect::<Vec<_>>();
        transitions.sort_unstable_by_key(|(slot, _)| *slot);
        for (_, transition) in transitions {
            append_slot_transition(&mut lines, myself.id.as_str(), &transition);
        }
    }
    lines.join("\n") + "\n"
}

pub(super) fn render_slots(
    snapshot: &ClusterTopologySnapshot,
    preference: PreferredEndpoint,
    transport: ClientTransport,
    prefer_ipv6: bool,
) -> Vec<Vec<Value>> {
    let fallback_owner = fallback_owner(snapshot);
    let owner_at = |slot: usize| {
        snapshot.slot_owners[slot]
            .as_ref()
            .or(fallback_owner.as_ref())
    };
    let mut ranges = Vec::new();
    let mut slot = 0usize;
    while slot < snapshot.slot_owners.len() {
        let Some(owner_id) = owner_at(slot) else {
            slot += 1;
            continue;
        };
        let start = slot;
        while slot + 1 < snapshot.slot_owners.len() && owner_at(slot + 1) == Some(owner_id) {
            slot += 1;
        }
        let end = slot;
        if let Some(owner) = find_node(snapshot, owner_id) {
            let mut range = vec![
                Value::integer(start as i64),
                Value::integer(end as i64),
                Value::Array(slot_node_entry(owner, preference, transport, prefer_ipv6)),
            ];
            range.extend(
                snapshot
                    .nodes
                    .iter()
                    .filter(|node| {
                        node.role == ClusterNodeRole::Replica
                            && node.replica_of.as_ref() == Some(owner_id)
                            && is_node_available_for_slots(node)
                    })
                    .map(|replica| {
                        Value::Array(slot_node_entry(replica, preference, transport, prefer_ipv6))
                    }),
            );
            ranges.push(range);
        }
        slot += 1;
    }
    ranges
}

fn is_node_available_for_slots(node: &NodeView) -> bool {
    node.health != ClusterNodeHealth::Fail && node.replication_offset != 0
}

pub(super) fn render_shards(
    snapshot: &ClusterTopologySnapshot,
    preference: PreferredEndpoint,
    transport: ClientTransport,
    prefer_ipv6: bool,
) -> Vec<Value> {
    snapshot
        .shards
        .iter()
        .map(|shard| {
            let slots = shard
                .slot_ranges
                .iter()
                .flat_map(|range| {
                    [
                        Value::integer(range.start as i64),
                        Value::integer(range.end as i64),
                    ]
                })
                .collect();
            let nodes = shard
                .nodes
                .iter()
                .map(|node| node_map(node, preference, transport, prefer_ipv6))
                .collect();
            Value::Map(vec![
                (Value::bulk("slots"), Value::Array(slots)),
                (Value::bulk("nodes"), Value::Array(nodes)),
                (Value::bulk("id"), Value::bulk(shard.id.to_string())),
            ])
        })
        .collect()
}

fn node_map(
    node: &NodeView,
    preference: PreferredEndpoint,
    transport: ClientTransport,
    prefer_ipv6: bool,
) -> Value {
    let tcp = node
        .endpoints
        .client_endpoint_for_family(ClientTransport::Tcp, prefer_ipv6);
    let tls = node
        .endpoints
        .client_endpoint_for_family(ClientTransport::Tls, prefer_ipv6);
    let endpoint = node
        .endpoints
        .redirect_endpoint_for_client(preference, transport, prefer_ipv6);
    let role = match node.role {
        ClusterNodeRole::Primary => "master",
        ClusterNodeRole::Replica => "replica",
    };
    let health = match node.health {
        ClusterNodeHealth::Fail => "fail",
        ClusterNodeHealth::Loading => "loading",
        ClusterNodeHealth::Online
        | ClusterNodeHealth::PFail
        | ClusterNodeHealth::NoAddr
        | ClusterNodeHealth::Handshake => "online",
    };
    let mut fields = vec![
        (Value::bulk("id"), Value::bulk(node.id.to_string())),
        (Value::bulk("port"), Value::integer(tcp.port as i64)),
    ];
    if tls.port != 0 {
        fields.push((Value::bulk("tls-port"), Value::integer(tls.port as i64)));
    }
    fields.extend([
        (Value::bulk("ip"), Value::bulk(tcp.host)),
        (Value::bulk("endpoint"), Value::bulk(endpoint.host)),
        (Value::bulk("role"), Value::bulk(role)),
        (
            Value::bulk("replication-offset"),
            Value::integer(node.replication_offset),
        ),
        (Value::bulk("health"), Value::bulk(health)),
    ]);
    if let Some(hostname) = node.endpoints.hostname() {
        fields.push((Value::bulk("hostname"), Value::bulk(hostname.to_string())));
    }
    if let Some(zone) = node.endpoints.availability_zone() {
        fields.push((
            Value::bulk("availability-zone"),
            Value::bulk(zone.to_string()),
        ));
    }
    Value::Map(fields)
}

fn slot_node_entry(
    node: &NodeView,
    preference: PreferredEndpoint,
    transport: ClientTransport,
    prefer_ipv6: bool,
) -> Vec<Value> {
    let client = node
        .endpoints
        .client_endpoint_for_family(transport, prefer_ipv6);
    let preferred = node
        .endpoints
        .redirect_endpoint_for_client(preference, transport, prefer_ipv6);
    let address = if preference == PreferredEndpoint::Unknown {
        Value::Null
    } else {
        Value::bulk(preferred.host)
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
        Value::integer(client.port as i64),
        Value::bulk(node.id.to_string()),
        Value::Map(metadata),
    ]
}

fn find_node<'a>(snapshot: &'a ClusterTopologySnapshot, id: &NodeId) -> Option<&'a NodeView> {
    snapshot.nodes.iter().find(|node| node.id == *id)
}

fn slot_ranges_for_node(
    owners: &[Option<NodeId>],
    node_id: &NodeId,
    fallback_owner: Option<&NodeId>,
) -> String {
    let owner_at = |slot: usize| owners[slot].as_ref().or(fallback_owner);
    let mut parts = Vec::new();
    let mut slot = 0usize;
    while slot < owners.len() {
        if owner_at(slot) != Some(node_id) {
            slot += 1;
            continue;
        }
        let start = slot;
        while slot + 1 < owners.len() && owner_at(slot + 1) == Some(node_id) {
            slot += 1;
        }
        if start == slot {
            parts.push(start.to_string());
        } else {
            parts.push(format!("{start}-{slot}"));
        }
        slot += 1;
    }
    parts.join(" ")
}

fn fallback_owner(snapshot: &ClusterTopologySnapshot) -> Option<NodeId> {
    if snapshot.slot_owners.iter().any(Option::is_some) {
        return None;
    }
    snapshot
        .nodes
        .iter()
        .filter(|node| {
            node.role == ClusterNodeRole::Primary
                && !matches!(
                    node.health,
                    ClusterNodeHealth::Fail
                        | ClusterNodeHealth::NoAddr
                        | ClusterNodeHealth::Handshake
                )
        })
        .max_by(|left, right| {
            left.config_epoch
                .cmp(&right.config_epoch)
                .then_with(|| right.id.as_str().cmp(left.id.as_str()))
        })
        .map(|node| node.id.clone())
}

fn append_slot_transition(lines: &mut [String], node_id: &str, transition: &str) {
    if let Some(line) = lines
        .iter_mut()
        .find(|line| line.starts_with(node_id) && line.as_bytes().get(node_id.len()) == Some(&b' '))
    {
        line.push(' ');
        line.push_str(transition);
    }
}

fn node_flags(flags: u16) -> String {
    let mut parts = Vec::new();
    for (flag, name) in [
        (CLUSTER_NODE_MYSELF, "myself"),
        (
            crate::engine::distributed::cluster::node::CLUSTER_NODE_PRIMARY,
            "master",
        ),
        (
            crate::engine::distributed::cluster::node::CLUSTER_NODE_REPLICA,
            "slave",
        ),
        (CLUSTER_NODE_PFAIL, "fail?"),
        (CLUSTER_NODE_FAIL, "fail"),
        (CLUSTER_NODE_HANDSHAKE, "handshake"),
        (CLUSTER_NODE_NOADDR, "noaddr"),
        (CLUSTER_NODE_NOFAILOVER, "nofailover"),
    ] {
        if flags & flag != 0 {
            parts.push(name);
        }
    }
    if parts.is_empty() {
        parts.push("noflags");
    }
    parts.join(",")
}

#[cfg(test)]
mod tests {
    use super::{render_nodes, render_slots};
    use crate::engine::distributed::cluster::node::{
        ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
    };
    use crate::engine::distributed::cluster::state::{
        ClusterHealth, ClusterReplicaHealth, ClusterReplicationView,
    };
    use crate::engine::distributed::cluster::{
        ClientTransport, ClusterEndpoints, ClusterNodeHealth, ClusterNodeRole,
        ClusterTopologySnapshot, NodeId, NodeView, PreferredEndpoint,
    };
    use crate::engine::distributed::cluster::{ClusterState, ShardView};

    fn offset(value: i64) -> crate::engine::distributed::replication::ReplicationOffset {
        crate::engine::distributed::replication::ReplicationOffset::try_from(value)
            .expect("valid test offset")
    }

    #[test]
    fn nodes_renders_myself_before_lexicographically_earlier_peers() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.myself.name = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz".to_string();
        let mut peer = ClusterNode::new(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        peer.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(peer.name.clone(), peer);

        let rendered = render_nodes(&state.topology_snapshot(), ClientTransport::Tcp, false);

        assert!(rendered.lines().next().unwrap().contains("myself"));
    }

    #[test]
    fn open_slot_markers_are_rendered_in_slot_order() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let first = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let second = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        state.seed_migrating_slot(5_462, second);
        state.seed_importing_slot(0, first);

        let rendered = render_nodes(&state.topology_snapshot(), ClientTransport::Tcp, false);
        let myself = rendered
            .lines()
            .find(|line| line.contains("myself"))
            .unwrap();

        assert!(
            myself.find(&format!("[0-<-{first}]")).unwrap()
                < myself.find(&format!("[5462->-{second}]")).unwrap()
        );
    }

    #[test]
    fn slots_keeps_disconnected_synced_replica_visible() {
        const REPLICA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.add_slot(0).unwrap();
        let mut replica = ClusterNode::new(
            REPLICA.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            false,
        );
        replica.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
        replica.flags |= CLUSTER_NODE_REPLICA;
        replica.replicaof = state.myself.name.clone();
        replica.shard_id = state.myself.shard_id.clone();
        replica.repl_offset = 42;
        state.nodes.insert(REPLICA.to_string(), replica);
        let views = std::collections::BTreeMap::from([(
            REPLICA.to_string(),
            ClusterReplicationView {
                upstream: None,
                received_offset: offset(42),
                applied_offset: offset(42),
                acknowledged_offset: offset(42),
                priority: 100,
                health: ClusterReplicaHealth::Disconnected,
            },
        )]);
        let snapshot = state.topology_snapshot_with_replication(&views);

        let ranges = render_slots(
            &snapshot,
            PreferredEndpoint::Ip,
            ClientTransport::Tcp,
            false,
        );

        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].len(), 4);
    }

    #[test]
    fn slots_uses_replication_offset_not_health_for_replica_availability() {
        let primary = NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
            .expect("valid primary id");
        let replica = NodeId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .expect("valid replica id");
        let mut slot_owners = vec![None; crate::engine::distributed::cluster::state::CLUSTER_SLOTS];
        slot_owners[0] = Some(primary.clone());
        let owner = NodeView {
            id: primary.clone(),
            shard_id: primary.clone(),
            endpoints: ClusterEndpoints::new("127.0.0.1".to_string(), 6379, 16_379),
            role: ClusterNodeRole::Primary,
            health: ClusterNodeHealth::Online,
            flags: CLUSTER_NODE_PRIMARY,
            replica_of: None,
            config_epoch: 1,
            replication_offset: 42,
            ping_sent: 0,
            pong_received: 0,
            outbound_connected: true,
            slots: vec![true; crate::engine::distributed::cluster::state::CLUSTER_SLOTS],
        };
        let loading_replica = NodeView {
            id: replica.clone(),
            shard_id: primary.clone(),
            endpoints: ClusterEndpoints::new("127.0.0.1".to_string(), 6380, 16_380),
            role: ClusterNodeRole::Replica,
            health: ClusterNodeHealth::Loading,
            flags: CLUSTER_NODE_REPLICA,
            replica_of: Some(primary.clone()),
            config_epoch: 0,
            replication_offset: 42,
            ping_sent: 0,
            pong_received: 0,
            outbound_connected: false,
            slots: vec![false; crate::engine::distributed::cluster::state::CLUSTER_SLOTS],
        };
        let snapshot = ClusterTopologySnapshot {
            generation: 0,
            health: ClusterHealth::Ok,
            slot_owners,
            nodes: vec![loading_replica.clone(), owner.clone()],
            shards: vec![ShardView {
                id: primary.clone(),
                slot_ranges: vec![],
                nodes: vec![owner, loading_replica],
            }],
            migrating_slots_to: Vec::new(),
            importing_slots_from: Vec::new(),
        };

        let ranges = render_slots(
            &snapshot,
            PreferredEndpoint::Ip,
            ClientTransport::Tcp,
            false,
        );

        assert_eq!(ranges.len(), 1);
        assert_eq!(
            ranges[0].len(),
            4,
            "CLUSTER SLOTS mirrors C isNodeAvailable and keeps nonzero-offset replicas visible"
        );
    }
}
