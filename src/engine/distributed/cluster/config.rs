use super::node::{
    ClusterNode, CLUSTER_NODE_FAIL, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_MYSELF,
    CLUSTER_NODE_NOADDR, CLUSTER_NODE_NOFAILOVER, CLUSTER_NODE_PFAIL, CLUSTER_NODE_PRIMARY,
    CLUSTER_NODE_REPLICA,
};
use super::state::CLUSTER_SLOTS;
use super::{ClusterEndpoints, ClusterState, NodeId};
use std::collections::HashMap;

struct ParsedNode {
    node: ClusterNode,
    migrating_slots: Vec<(usize, String)>,
    importing_slots: Vec<(usize, String)>,
}

pub fn parse_cluster_config(
    content: &str,
    runtime_ip: &str,
    runtime_port: u16,
    runtime_bus_port: u16,
) -> Result<ClusterState, String> {
    let mut parsed_nodes = Vec::new();
    let mut current_epoch = 0;
    let mut last_vote_epoch = 0;

    for line in content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.first() == Some(&"vars") {
            for pair in fields[1..].as_chunks::<2>().0 {
                match pair[0] {
                    "currentEpoch" => current_epoch = parse_u64(pair[1], "currentEpoch")?,
                    "lastVoteEpoch" => last_vote_epoch = parse_u64(pair[1], "lastVoteEpoch")?,
                    _ => {}
                }
            }
            continue;
        }
        parsed_nodes.push(parse_node(&fields)?);
    }
    retain_first_slot_claims(&mut parsed_nodes);

    let myself_index = parsed_nodes
        .iter()
        .position(|parsed| parsed.node.is_myself())
        .ok_or_else(|| "cluster config has no myself node".to_string())?;
    let mut myself = parsed_nodes.remove(myself_index);
    let migrating_slots_to = std::mem::take(&mut myself.migrating_slots)
        .into_iter()
        .collect();
    let importing_slots_from = std::mem::take(&mut myself.importing_slots)
        .into_iter()
        .collect();
    myself.node.endpoints.restore_local_runtime_address(
        runtime_ip.to_string(),
        runtime_port,
        runtime_bus_port,
    );
    let mut restored_nodes = HashMap::new();
    for parsed in parsed_nodes {
        let node_id = parsed.node.name.clone();
        if restored_nodes
            .insert(node_id.clone(), parsed.node)
            .is_some()
        {
            return Err(format!("duplicate cluster node ID: {node_id}"));
        }
    }
    ClusterState::restore_from_cluster_config(
        myself.node,
        restored_nodes,
        migrating_slots_to,
        importing_slots_from,
        current_epoch,
        last_vote_epoch,
    )
}

fn retain_first_slot_claims(parsed_nodes: &mut [ParsedNode]) {
    let mut claimed = vec![false; CLUSTER_SLOTS];
    for parsed in parsed_nodes {
        for (slot, owns) in parsed.node.slots.iter_mut().enumerate() {
            if !*owns {
                continue;
            }
            if claimed[slot] {
                *owns = false;
            } else {
                claimed[slot] = true;
            }
        }
        parsed.node.numslots = parsed.node.count_slots();
    }
}

impl ClusterState {
    fn restore_from_cluster_config(
        myself: ClusterNode,
        nodes: HashMap<String, ClusterNode>,
        migrating_slots_to: HashMap<usize, String>,
        importing_slots_from: HashMap<usize, String>,
        current_epoch: u64,
        last_vote_epoch: u64,
    ) -> Result<Self, String> {
        let mut state = Self::new(myself.ip(), myself.port(), myself.cport());
        state.myself = myself;
        state.nodes = nodes;
        state.restore_open_slot_maps(migrating_slots_to, importing_slots_from);
        restore_referenced_primaries(&mut state);
        let maximum_node_epoch = std::iter::once(state.myself.config_epoch)
            .chain(state.nodes.values().map(|node| node.config_epoch))
            .max()
            .unwrap_or_default();
        state.advance_current_epoch(current_epoch.max(maximum_node_epoch));
        if !state.record_vote_epoch(last_vote_epoch) && last_vote_epoch != 0 {
            return Err("lastVoteEpoch exceeds currentEpoch".to_string());
        }
        if state.myself.numslots > 0 {
            state.mark_slot_owned();
        }
        repair_replica_shard_ids(&mut state);
        mark_replica_primaries_for_migration(&mut state);
        state.rebuild_slot_owners_from_bitmaps()?;
        state.recompute_state();
        Ok(state)
    }
}

pub fn render_cluster_config(state: &ClusterState) -> String {
    render_cluster_config_inner(state, false)
}

fn render_cluster_config_inner(state: &ClusterState, fingerprint: bool) -> String {
    let mut lines = Vec::with_capacity(state.nodes.len() + 2);
    lines.push(render_node(&state.myself, Some(state), fingerprint));
    lines.extend(
        state
            .nodes
            .values()
            .filter(|node| !node.has_flag(CLUSTER_NODE_HANDSHAKE))
            .map(|node| render_node(node, None, fingerprint)),
    );
    lines.push(format!(
        "vars currentEpoch {} lastVoteEpoch {}",
        state.current_epoch, state.last_vote_epoch
    ));
    lines.push(String::new());
    lines.join("\n")
}

fn render_node(node: &ClusterNode, state: Option<&ClusterState>, fingerprint: bool) -> String {
    let master = if node.replicaof.is_empty() {
        "-"
    } else {
        node.replicaof.as_str()
    };
    let mut slots = render_slots(&node.slots);
    if let Some(state) = state {
        for (slot, target) in state.migrating_slots() {
            slots.push_str(&format!(" [{slot}->-{target}]"));
        }
        for (slot, source) in state.importing_slots() {
            slots.push_str(&format!(" [{slot}-<-{source}]"));
        }
    }
    // The change-detection fingerprint zeroes the heartbeat timestamps so a
    // ping/pong-only update does not force a nodes.conf rewrite. The values
    // written to disk (fingerprint = false) still carry the real timestamps
    // for format parity with C.
    let (ping_sent, pong_received) = if fingerprint {
        (0, 0)
    } else {
        (node.ping_sent, node.pong_received)
    };
    format!(
        "{} {} {} {} {} {} {} connected {}",
        node.name,
        render_address(node),
        render_flags(node),
        master,
        ping_sent,
        pong_received,
        node.config_epoch,
        slots,
    )
    .trim()
    .to_string()
}

fn render_address(node: &ClusterNode) -> String {
    let mut address = format!("{}:{}@{}", node.ip(), node.port(), node.cport());
    address.push(',');
    if let Some(hostname) = node.endpoints.hostname() {
        address.push_str(hostname);
    }
    append_aux(
        &mut address,
        "shard-id",
        (!node.shard_id.is_empty()).then_some(&node.shard_id),
    );
    append_aux(&mut address, "nodename", node.endpoints.human_name());
    append_aux(
        &mut address,
        "replica-priority",
        (node.cluster_replica_priority != 0).then(|| node.cluster_replica_priority.to_string()),
    );
    append_aux(
        &mut address,
        "tcp-port",
        Some(&node.endpoints.port().to_string()),
    );
    append_aux(
        &mut address,
        "tls-port",
        (node.endpoints.tls_port() != 0).then(|| node.endpoints.tls_port().to_string()),
    );
    append_aux(&mut address, "client-ipv4", node.endpoints.client_ipv4());
    append_aux(&mut address, "client-ipv6", node.endpoints.client_ipv6());
    append_aux(
        &mut address,
        "client-tcp-port",
        (node.endpoints.client_port() != 0).then(|| node.endpoints.client_port().to_string()),
    );
    append_aux(
        &mut address,
        "client-tls-port",
        (node.endpoints.client_tls_port() != 0)
            .then(|| node.endpoints.client_tls_port().to_string()),
    );
    append_aux(
        &mut address,
        "availability-zone",
        node.endpoints.availability_zone(),
    );
    address
}

fn append_aux(output: &mut String, name: &str, value: Option<impl AsRef<str>>) {
    if let Some(value) = value {
        use std::fmt::Write;
        let _ = write!(output, ",{name}={}", value.as_ref());
    }
}

fn render_flags(node: &ClusterNode) -> String {
    let mut flags = Vec::new();
    if node.is_myself() {
        flags.push("myself");
    }
    if node.is_primary() {
        flags.push("master");
    }
    if node.is_replica() {
        flags.push("slave");
    }
    if node.has_flag(CLUSTER_NODE_PFAIL) {
        flags.push("fail?");
    }
    if node.has_flag(CLUSTER_NODE_FAIL) {
        flags.push("fail");
    }
    if node.has_flag(CLUSTER_NODE_HANDSHAKE) {
        flags.push("handshake");
    }
    if node.has_flag(CLUSTER_NODE_NOADDR) {
        flags.push("noaddr");
    }
    if node.has_flag(CLUSTER_NODE_NOFAILOVER) {
        flags.push("nofailover");
    }
    if flags.is_empty() {
        flags.push("noflags");
    }
    flags.join(",")
}

fn render_slots(slots: &[bool]) -> String {
    let mut ranges = Vec::new();
    let mut slot = 0;
    while slot < slots.len() {
        if !slots[slot] {
            slot += 1;
            continue;
        }
        let start = slot;
        while slot + 1 < slots.len() && slots[slot + 1] {
            slot += 1;
        }
        if start == slot {
            ranges.push(start.to_string());
        } else {
            ranges.push(format!("{}-{}", start, slot));
        }
        slot += 1;
    }
    ranges.join(" ")
}

fn parse_node(fields: &[&str]) -> Result<ParsedNode, String> {
    if fields.len() < 8 {
        return Err("cluster node line has too few fields".to_string());
    }
    let node_id = NodeId::parse(fields[0].as_bytes()).map_err(|error| error.to_string())?;
    let (ip, port, bus_port, hostname, aux_fields) = parse_address(fields[1])?;
    let flags = parse_flags(fields[2])?;
    let is_primary = flags & CLUSTER_NODE_REPLICA == 0;
    let mut node = ClusterNode::new(node_id.to_string(), ip, port, bus_port, is_primary);
    node.flags = flags;
    if flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_NOADDR) != 0 {
        node.fail_time = crate::foundation::io::sys::time::mstime();
    }
    if let Some(hostname) = hostname {
        node.endpoints
            .set_hostname(hostname.as_bytes())
            .map_err(|error| error.to_string())?;
    }
    for (name, value) in aux_fields {
        apply_aux_field(&mut node, name, value)?;
    }
    if node.shard_id.is_empty() && node.is_primary() {
        node.shard_id = node.name.clone();
    }
    node.replicaof = match fields[3] {
        "-" => String::new(),
        master_id => NodeId::parse(master_id.as_bytes())
            .map_err(|error| error.to_string())?
            .to_string(),
    };
    node.ping_sent = parse_i64(fields[4], "ping-sent")?;
    node.pong_received = parse_i64(fields[5], "pong-received")?;
    node.config_epoch = parse_u64(fields[6], "config-epoch")?;
    let mut migrating_slots = Vec::new();
    let mut importing_slots = Vec::new();
    for token in &fields[8..] {
        parse_slot_token(
            token,
            &mut node.slots,
            &mut migrating_slots,
            &mut importing_slots,
        )?;
    }
    node.numslots = node.count_slots();
    Ok(ParsedNode {
        node,
        migrating_slots,
        importing_slots,
    })
}

type ParsedAddress<'a> = (String, u16, u16, Option<&'a str>, Vec<(&'a str, &'a str)>);

fn parse_address(raw: &str) -> Result<ParsedAddress<'_>, String> {
    let mut sections = raw.split(',');
    let endpoint = sections
        .next()
        .ok_or_else(|| "missing cluster endpoint".to_string())?;
    let (client, bus_port) = endpoint
        .split_once('@')
        .ok_or_else(|| "missing cluster bus port".to_string())?;
    let (ip, port) = client
        .rsplit_once(':')
        .ok_or_else(|| "missing cluster client port".to_string())?;
    let hostname = sections.next().filter(|value| !value.is_empty());
    let mut aux_fields = Vec::new();
    for field in sections {
        let (name, value) = field
            .split_once('=')
            .ok_or_else(|| format!("invalid cluster aux field: {field}"))?;
        if value.contains('=') {
            return Err(format!("invalid cluster aux field: {field}"));
        }
        ClusterEndpoints::validate_aux_value(name.as_bytes()).map_err(|error| error.to_string())?;
        aux_fields.push((name, value));
    }
    Ok((
        ip.to_string(),
        parse_u16(port, "client port")?,
        parse_u16(bus_port, "bus port")?,
        hostname,
        aux_fields,
    ))
}

fn apply_aux_field(node: &mut ClusterNode, name: &str, value: &str) -> Result<(), String> {
    let endpoints = &mut node.endpoints;
    match name {
        "replica-priority" => {
            node.cluster_replica_priority = value
                .parse::<u32>()
                .map_err(|_| format!("invalid replica-priority: {value}"))?;
        }
        "shard-id" => {
            node.shard_id = NodeId::parse(value.as_bytes())
                .map_err(|error| error.to_string())?
                .to_string();
        }
        "nodename" => endpoints
            .set_human_name(value.as_bytes())
            .map_err(|error| error.to_string())?,
        "tcp-port" => endpoints.set_port(parse_u16(value, "tcp-port")?),
        "tls-port" => endpoints.set_tls_port(parse_u16(value, "tls-port")?),
        "client-ipv4" => endpoints
            .set_client_ipv4(value.as_bytes())
            .map_err(|error| error.to_string())?,
        "client-ipv6" => endpoints
            .set_client_ipv6(value.as_bytes())
            .map_err(|error| error.to_string())?,
        "client-tcp-port" => endpoints.set_client_port(parse_u16(value, "client-tcp-port")?),
        "client-tls-port" => endpoints.set_client_tls_port(parse_u16(value, "client-tls-port")?),
        "availability-zone" => endpoints
            .set_availability_zone(value.as_bytes())
            .map_err(|error| error.to_string())?,
        _ => return Err(format!("unknown cluster aux field: {name}")),
    }
    Ok(())
}

fn parse_flags(raw: &str) -> Result<u16, String> {
    let mut flags = 0;
    for flag in raw.split(',') {
        flags |= match flag {
            "myself" => CLUSTER_NODE_MYSELF,
            "master" => CLUSTER_NODE_PRIMARY,
            "slave" => CLUSTER_NODE_REPLICA,
            "fail?" => CLUSTER_NODE_PFAIL,
            "fail" => CLUSTER_NODE_FAIL,
            "handshake" => CLUSTER_NODE_HANDSHAKE,
            "noaddr" => CLUSTER_NODE_NOADDR,
            "nofailover" => CLUSTER_NODE_NOFAILOVER,
            "noflags" => 0,
            unknown => return Err(format!("unknown cluster node flag: {}", unknown)),
        };
    }
    Ok(flags)
}

fn parse_slot_token(
    raw: &str,
    slots: &mut [bool],
    migrating_slots: &mut Vec<(usize, String)>,
    importing_slots: &mut Vec<(usize, String)>,
) -> Result<(), String> {
    if raw.starts_with('[') {
        let inner = raw
            .strip_prefix('[')
            .and_then(|value| value.strip_suffix(']'))
            .ok_or_else(|| format!("invalid open slot: {raw}"))?;
        let (slot, node_id, is_migrating) = if let Some((slot, node_id)) = inner.split_once("->-") {
            (slot, node_id, true)
        } else if let Some((slot, node_id)) = inner.split_once("-<-") {
            (slot, node_id, false)
        } else {
            return Err(format!("invalid open slot: {raw}"));
        };
        let slot = parse_slot(slot, "open slot")?;
        let node_id = NodeId::parse(node_id.as_bytes())
            .map_err(|error| error.to_string())?
            .to_string();
        if is_migrating {
            migrating_slots.push((slot, node_id));
        } else {
            importing_slots.push((slot, node_id));
        }
        return Ok(());
    }
    let (start, end) = match raw.split_once('-') {
        Some((start, end)) => (
            parse_slot(start, "slot range start")?,
            parse_slot(end, "slot range end")?,
        ),
        None => {
            let slot = parse_slot(raw, "slot")?;
            (slot, slot)
        }
    };
    if start > end {
        return Err("cluster slot range is reversed".to_string());
    }
    slots[start..=end].fill(true);
    Ok(())
}

fn restore_referenced_primaries(state: &mut ClusterState) {
    let primary_ids = std::iter::once(&state.myself)
        .chain(state.nodes.values())
        .filter(|node| !node.replicaof.is_empty())
        .map(|node| node.replicaof.clone())
        .collect::<Vec<_>>();
    for primary_id in primary_ids {
        if primary_id == state.myself.name {
            continue;
        }
        state.nodes.entry(primary_id.clone()).or_insert_with(|| {
            // C clusterLoadConfig preserves references to missing nodes with
            // no role flags until a heartbeat supplies their actual identity.
            let mut node = ClusterNode::new(primary_id, String::new(), 0, 0, false);
            node.flags = 0;
            node
        });
    }
}

fn repair_replica_shard_ids(state: &mut ClusterState) {
    let mut primary_shards = std::collections::HashMap::new();
    primary_shards.insert(state.myself.name.clone(), state.myself.shard_id.clone());
    primary_shards.extend(
        state
            .nodes
            .values()
            .filter(|node| node.is_primary())
            .map(|node| (node.name.clone(), node.shard_id.clone())),
    );
    if state.myself.is_replica() {
        if let Some(shard_id) = primary_shards.get(&state.myself.replicaof) {
            state.myself.shard_id.clone_from(shard_id);
        }
    }
    for node in state.nodes.values_mut().filter(|node| node.is_replica()) {
        if let Some(shard_id) = primary_shards.get(&node.replicaof) {
            node.shard_id.clone_from(shard_id);
        }
    }
}

fn mark_replica_primaries_for_migration(state: &mut ClusterState) {
    let primary_ids = std::iter::once(&state.myself)
        .chain(state.nodes.values())
        .filter(|node| node.is_replica() && !node.replicaof.is_empty())
        .map(|node| node.replicaof.clone())
        .collect::<Vec<_>>();
    for primary_id in primary_ids {
        state.mark_primary_migration_target(&primary_id);
    }
}

fn parse_slot(raw: &str, field: &str) -> Result<usize, String> {
    let slot = raw
        .parse::<usize>()
        .map_err(|_| format!("invalid {}: {}", field, raw))?;
    if slot >= CLUSTER_SLOTS {
        return Err(format!("invalid {}: {}", field, raw));
    }
    Ok(slot)
}

fn parse_u16(raw: &str, field: &str) -> Result<u16, String> {
    raw.parse::<u16>()
        .map_err(|_| format!("invalid {}: {}", field, raw))
}

fn parse_u64(raw: &str, field: &str) -> Result<u64, String> {
    raw.parse::<u64>()
        .map_err(|_| format!("invalid {}: {}", field, raw))
}

fn parse_i64(raw: &str, field: &str) -> Result<i64, String> {
    raw.parse::<i64>()
        .map_err(|_| format!("invalid {}: {}", field, raw))
}

#[cfg(test)]
mod tests {
    use super::{parse_cluster_config, render_cluster_config};
    use crate::engine::distributed::cluster::node::{
        ClusterNode, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_MIGRATE_TO, CLUSTER_NODE_MYSELF,
    };
    use crate::engine::distributed::cluster::ClusterState;

    const DIVERGENT_PRIMARY: &str =
        include_str!("../../../../thirdparty/valkey/tests/assets/divergent-shard-1.conf");
    const DIVERGENT_REPLICA: &str =
        include_str!("../../../../thirdparty/valkey/tests/assets/divergent-shard-3.conf");

    #[test]
    fn renderer_omits_unconfirmed_handshake_nodes() {
        let mut state = ClusterState::new("127.0.0.1", 7000, 17000);
        let handshake_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mut handshake = ClusterNode::new(
            handshake_id.to_string(),
            "127.0.0.1".to_string(),
            7001,
            17001,
            true,
        );
        handshake.flags &= !CLUSTER_NODE_MYSELF;
        handshake.flags |= CLUSTER_NODE_HANDSHAKE;
        state.nodes.insert(handshake_id.to_string(), handshake);

        let rendered = render_cluster_config(&state);

        assert!(!rendered.contains(handshake_id));
        parse_cluster_config(&rendered, "127.0.0.1", 7000, 17000)
            .expect("rendered cluster configuration should round trip");
    }

    #[test]
    fn renderer_writes_dash_master_for_replica_without_primary() {
        let mut state = ClusterState::new("127.0.0.1", 7000, 17000);
        let orphan_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mut orphan = ClusterNode::new(
            orphan_id.to_string(),
            "127.0.0.1".to_string(),
            7001,
            17001,
            false,
        );
        orphan.flags &= !CLUSTER_NODE_MYSELF;
        assert!(orphan.replicaof.is_empty());
        state.nodes.insert(orphan_id.to_string(), orphan);

        let rendered = render_cluster_config(&state);

        let orphan_line = rendered
            .lines()
            .find(|line| line.starts_with(orphan_id))
            .expect("orphan replica rendered");
        let master = orphan_line.split_whitespace().nth(3).expect("master field");
        assert_eq!(master, "-");
        parse_cluster_config(&rendered, "127.0.0.1", 7000, 17000)
            .expect("masterless replica line should round trip");
    }

    #[test]
    fn parser_restores_primary_identity_slots_and_epochs() {
        let state = parse_cluster_config(DIVERGENT_PRIMARY, "127.0.0.1", 7000, 17000).unwrap();

        assert_eq!(
            state.myself.name,
            "43ee1cacd6948ee96bb367eb8795e62e8d153f05"
        );
        assert!(state.myself.is_primary());
        assert_eq!(state.myself.port(), 7000);
        assert_eq!(state.myself.cport(), 17000);
        assert_eq!(state.myself.numslots, 16384);
        assert_eq!(state.current_epoch, 13);
        assert_eq!(state.last_vote_epoch, 9);
    }

    #[test]
    fn parser_preserves_missing_primary_referenced_by_a_replica() {
        let local_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let primary_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let replica_id = "cccccccccccccccccccccccccccccccccccccccc";
        for is_local_replica in [false, true] {
            let local_role = if is_local_replica {
                format!("myself,slave {primary_id}")
            } else {
                "myself,master -".to_string()
            };
            let content = format!(
                "{local_id} 127.0.0.1:7000@17000 {local_role} 0 0 1 connected\n\
                 {replica_id} 127.0.0.1:7002@17002 slave {primary_id} 0 0 2 connected\n\
                 vars currentEpoch 2 lastVoteEpoch 0\n"
            );

            let state = parse_cluster_config(&content, "127.0.0.1", 7000, 17000).unwrap();
            let primary = state
                .nodes
                .get(primary_id)
                .expect("a replica reference must preserve its missing primary");
            assert!(!primary.is_primary());
            assert!(!primary.is_replica());
            assert!(!primary.is_myself());
            assert!(primary.ip().is_empty());
            assert_eq!(primary.port(), 0);
            assert_eq!(primary.cport(), 0);
            assert_eq!(primary.numslots, 0);
            assert_eq!(state.nodes.len(), 2);
            assert_eq!(state.nodes[replica_id].replicaof, primary_id);
            if is_local_replica {
                assert_eq!(state.myself.replicaof, primary_id);
            }

            let rendered = render_cluster_config(&state);
            let restored = parse_cluster_config(&rendered, "127.0.0.1", 7000, 17000).unwrap();
            assert!(restored.nodes.contains_key(primary_id));
            assert!(!restored.nodes[primary_id].is_primary());
            assert!(!restored.nodes[primary_id].is_replica());
        }
    }

    #[test]
    fn parser_raises_current_epoch_to_failed_nodes_maximum() {
        let local_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let dead_id = "0000000000000000000000000000000000000000";
        let content = format!(
            "{local_id} 127.0.0.1:7000@17000 myself,master - 0 0 1 connected\n\
             {dead_id} 127.0.0.1:7001@17001 fail,noaddr,master - 0 0 999999 connected\n\
             vars currentEpoch 1 lastVoteEpoch 0\n"
        );

        let state = parse_cluster_config(&content, "127.0.0.1", 7000, 17000).unwrap();

        assert_eq!(state.current_epoch, 999_999);
        assert_eq!(state.nodes[dead_id].config_epoch, 999_999);
    }

    #[test]
    fn parser_keeps_first_slot_owner_when_a_later_node_duplicates_the_claim() {
        let local_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let peer_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let content = format!(
            "{local_id} 127.0.0.1:7000@17000 myself,master - 0 0 1 connected 7\n\
             {peer_id} 127.0.0.1:7001@17001 master - 0 0 2 connected 7\n\
             vars currentEpoch 2 lastVoteEpoch 0\n"
        );

        let state = parse_cluster_config(&content, "127.0.0.1", 7000, 17_000).unwrap();

        assert_eq!(state.canonical_slot_owner_id(7), Some(local_id));
        assert_eq!(state.nodes[peer_id].numslots, 0);
        assert!(!state.nodes[peer_id].slots[7]);
        assert!(state.validate_invariants().is_ok());
    }

    #[test]
    fn parser_repairs_divergent_replica_shard_id_and_renderer_persists_it_twice() {
        let state = parse_cluster_config(DIVERGENT_REPLICA, "127.0.0.1", 7000, 17000).unwrap();
        let primary = state
            .nodes
            .get("43ee1cacd6948ee96bb367eb8795e62e8d153f05")
            .unwrap();

        assert!(state.myself.is_replica());
        assert_eq!(state.myself.shard_id, primary.shard_id);
        assert!(primary.has_flag(CLUSTER_NODE_MIGRATE_TO));

        let rendered = render_cluster_config(&state);
        assert_eq!(rendered.matches(&state.myself.shard_id).count(), 2);
        assert!(rendered.contains("vars currentEpoch 13 lastVoteEpoch 9"));
    }

    #[test]
    fn config_round_trip_preserves_hostname_aux_fields_and_open_slots() {
        let local_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let remote_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let shard_id = "cccccccccccccccccccccccccccccccccccccccc";
        let content = format!(
            concat!(
                "{} 127.0.0.1:7000@17000,,shard-id={} ",
                "myself,master - 0 0 7 connected 0-41 ",
                "[42->-{}] [43-<-{}]\n",
                "{} 198.51.100.8:7001@17001,cache.example.com,",
                "shard-id={},nodename=cache-east-1,tcp-port=7001,",
                "tls-port=7443,client-ipv4=192.0.2.8,client-ipv6=2001:db8::8,",
                "client-tcp-port=7101,client-tls-port=7543,availability-zone=zone-a ",
                "master - 0 0 8 connected 44-16383\n",
                "vars currentEpoch 8 lastVoteEpoch 6\n",
            ),
            local_id, local_id, remote_id, remote_id, remote_id, shard_id,
        );

        let state = parse_cluster_config(&content, "127.0.0.1", 7000, 17000).unwrap();
        let remote = state.nodes.get(remote_id).expect("remote node restored");
        assert_eq!(remote.endpoints.hostname(), Some("cache.example.com"));
        assert_eq!(remote.endpoints.human_name(), Some("cache-east-1"));
        assert_eq!(remote.endpoints.port(), 7001);
        assert_eq!(remote.endpoints.tls_port(), 7443);
        assert_eq!(remote.endpoints.client_ipv4(), Some("192.0.2.8"));
        assert_eq!(remote.endpoints.client_ipv6(), Some("2001:db8::8"));
        assert_eq!(remote.endpoints.client_port(), 7101);
        assert_eq!(remote.endpoints.client_tls_port(), 7543);
        assert_eq!(remote.endpoints.availability_zone(), Some("zone-a"));
        assert_eq!(state.migrating_slot_target(42), Some(remote_id));
        assert_eq!(state.importing_slot_source(43), Some(remote_id));

        let rendered = render_cluster_config(&state);
        let shard_field = format!("shard-id={shard_id}");
        let migrating = format!("[42->-{remote_id}]");
        let importing = format!("[43-<-{remote_id}]");
        for expected in [
            "cache.example.com",
            shard_field.as_str(),
            "nodename=cache-east-1",
            "tls-port=7443",
            "client-ipv4=192.0.2.8",
            "client-ipv6=2001:db8::8",
            "client-tcp-port=7101",
            "client-tls-port=7543",
            "availability-zone=zone-a",
            migrating.as_str(),
            importing.as_str(),
        ] {
            assert!(
                rendered.contains(expected),
                "missing {expected:?}: {rendered}"
            );
        }

        let reparsed = parse_cluster_config(&rendered, "127.0.0.1", 7000, 17000).unwrap();
        let remote = reparsed.nodes.get(remote_id).expect("remote node reparsed");
        assert_eq!(remote.endpoints.hostname(), Some("cache.example.com"));
        assert_eq!(remote.endpoints.human_name(), Some("cache-east-1"));
        assert_eq!(remote.endpoints.client_ipv4(), Some("192.0.2.8"));
        assert_eq!(remote.endpoints.client_ipv6(), Some("2001:db8::8"));
        assert_eq!(remote.endpoints.client_port(), 7101);
        assert_eq!(remote.endpoints.client_tls_port(), 7543);
        assert_eq!(remote.endpoints.availability_zone(), Some("zone-a"));
        assert_eq!(reparsed.migrating_slot_target(42), Some(remote_id));
        assert_eq!(reparsed.importing_slot_source(43), Some(remote_id));
    }

    #[test]
    fn parser_rejects_invalid_node_shard_and_aux_fields() {
        let valid_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let valid_line = |node_id: &str, address: &str, replicaof: &str| {
            format!("{node_id} {address} myself,master {replicaof} 0 0 0 connected\n")
        };

        for content in [
            valid_line("short", "127.0.0.1:7000@17000", "-"),
            valid_line(
                valid_id,
                "127.0.0.1:7000@17000,,shard-id=BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
                "-",
            ),
            valid_line(valid_id, "127.0.0.1:7000@17000,,nodename", "-"),
            valid_line(
                valid_id,
                "127.0.0.1:7000@17000,,unknown-field=value",
                "-",
            ),
            valid_line(
                valid_id,
                "127.0.0.1:7000@17000,bad_hostname,shard-id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "-",
            ),
            valid_line(valid_id, "127.0.0.1:7000@17000", "short"),
        ] {
            assert!(
                parse_cluster_config(&content, "127.0.0.1", 7000, 17000).is_err(),
                "invalid nodes.conf was accepted: {content}"
            );
        }
    }
}
