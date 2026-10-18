use super::super::{Connection, RespValue};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct TopologyEntry {
    id: String,
    is_primary: bool,
    slots: Vec<u16>,
    migrating: Vec<(u16, String)>,
    importing: Vec<(u16, String)>,
}

pub(super) fn execute(args: &[String], command_index: usize) -> Result<(), String> {
    let seed = args
        .get(command_index + 1)
        .ok_or_else(|| "missing cluster seed address".to_string())?;
    let (host, port) = super::parse_address(seed)?;
    let mut connection = Connection::connect_with_defaults(&host, port)?;
    let response = connection.send_command(&[b"CLUSTER", b"NODES"])?;
    let nodes_text = match response {
        RespValue::Bulk(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        RespValue::Error(error) => return Err(error),
        other => return Err(format!("unexpected CLUSTER NODES response: {:?}", other)),
    };
    let nodes = super::parse_cluster_nodes(&nodes_text)?;
    validate_slot_coverage(&nodes)?;
    validate_topology_consistency(&nodes)
}

fn validate_topology_consistency(nodes: &[super::ClusterNode]) -> Result<(), String> {
    let expected = topology_signature(nodes);
    for node in nodes {
        let mut connection = Connection::connect_with_defaults(&node.host, node.port)?;
        let response = connection.send_command(&[b"CLUSTER", b"NODES"])?;
        let text = match response {
            RespValue::Bulk(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            RespValue::Error(error) => return Err(error),
            other => return Err(format!("unexpected CLUSTER NODES response: {:?}", other)),
        };
        let observed = super::parse_cluster_nodes(&text)?;
        if topology_signature(&observed) != expected {
            return Err(format!(
                "cluster configuration is inconsistent at {}:{}",
                node.host, node.port
            ));
        }
    }
    Ok(())
}

fn topology_signature(nodes: &[super::ClusterNode]) -> Vec<TopologyEntry> {
    let mut entries = nodes
        .iter()
        .map(|node| {
            let mut slots = node.slots.clone();
            let mut migrating = node.migrating.clone();
            let mut importing = node.importing.clone();
            slots.sort_unstable();
            migrating.sort_unstable();
            importing.sort_unstable();
            TopologyEntry {
                id: node.id.clone(),
                is_primary: node.is_primary,
                slots,
                migrating,
                importing,
            }
        })
        .collect::<Vec<_>>();
    entries.sort_unstable();
    entries
}

fn validate_slot_coverage(nodes: &[super::ClusterNode]) -> Result<(), String> {
    if let Some((slot, _)) = nodes
        .iter()
        .flat_map(|node| node.migrating.iter().chain(&node.importing))
        .next()
    {
        return Err(format!("slot {slot} is open"));
    }
    let mut owners = vec![0u8; 16_384];
    for node in nodes.iter().filter(|node| node.is_primary) {
        for slot in &node.slots {
            let owner_count = &mut owners[*slot as usize];
            *owner_count = owner_count.saturating_add(1);
            if *owner_count > 1 {
                return Err(format!("slot {} has multiple owners", slot));
            }
        }
    }
    if let Some(slot) = owners.iter().position(|owner_count| *owner_count == 0) {
        return Err(format!(
            "not all slots are covered by nodes; slot {} is missing",
            slot
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_manager::ClusterNode;

    #[test]
    fn validates_complete_non_overlapping_slot_coverage() {
        let nodes = vec![
            ClusterNode {
                id: "a".to_string(),
                host: "127.0.0.1".to_string(),
                port: 7000,
                is_primary: true,
                is_myself: false,
                slots: (0..8192).collect(),
                migrating: Vec::new(),
                importing: Vec::new(),
            },
            ClusterNode {
                id: "b".to_string(),
                host: "127.0.0.1".to_string(),
                port: 7001,
                is_primary: true,
                is_myself: false,
                slots: (8192..16384).collect(),
                migrating: Vec::new(),
                importing: Vec::new(),
            },
        ];

        assert_eq!(validate_slot_coverage(&nodes), Ok(()));
    }

    #[test]
    fn rejects_missing_slot_coverage() {
        let nodes = vec![ClusterNode {
            id: "a".to_string(),
            host: "127.0.0.1".to_string(),
            port: 7000,
            is_primary: true,
            is_myself: false,
            slots: (0..16383).collect(),
            migrating: Vec::new(),
            importing: Vec::new(),
        }];

        assert!(validate_slot_coverage(&nodes)
            .unwrap_err()
            .contains("not all slots are covered"));
    }

    #[test]
    fn topology_signature_detects_a_missing_joined_node() {
        let node = |id: &str, slots: std::ops::Range<u16>| ClusterNode {
            id: id.to_string(),
            host: "127.0.0.1".to_string(),
            port: 7000,
            is_primary: true,
            is_myself: false,
            slots: slots.collect(),
            migrating: Vec::new(),
            importing: Vec::new(),
        };
        let converged = vec![node("a", 0..8192), node("b", 8192..16384), node("c", 0..0)];
        let stale = vec![node("a", 0..8192), node("b", 8192..16384)];

        assert_ne!(topology_signature(&converged), topology_signature(&stale));
    }
}
