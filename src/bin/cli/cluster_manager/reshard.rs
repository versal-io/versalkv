use super::super::{Connection, RespValue};

#[derive(Debug, PartialEq, Eq)]
pub(super) struct ReshardCommand {
    pub seed_host: String,
    pub seed_port: u16,
    pub source_id: String,
    pub target_id: String,
    pub slot_count: usize,
}

pub(super) fn parse(args: &[String], command_index: usize) -> Result<ReshardCommand, String> {
    let seed = args
        .get(command_index + 1)
        .ok_or_else(|| "missing cluster seed address".to_string())?;
    let (seed_host, seed_port) = super::parse_address(seed)?;
    let source_id = option_value(args, "--cluster-from")?;
    let target_id = option_value(args, "--cluster-to")?;
    let slot_count = option_value(args, "--cluster-slots")?
        .parse()
        .map_err(|_| "invalid --cluster-slots value".to_string())?;
    Ok(ReshardCommand {
        seed_host,
        seed_port,
        source_id,
        target_id,
        slot_count,
    })
}

pub(super) fn execute(command: ReshardCommand) -> Result<(), String> {
    let mut seed = Connection::connect_with_defaults(&command.seed_host, command.seed_port)?;
    let response = seed.send_command(&[b"CLUSTER", b"NODES"])?;
    let nodes_text = match response {
        RespValue::Bulk(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        RespValue::Error(error) => return Err(error),
        other => return Err(format!("unexpected CLUSTER NODES response: {:?}", other)),
    };
    let nodes = super::parse_cluster_nodes(&nodes_text)?;
    let plans = plan_reshard(
        &nodes,
        &command.source_id,
        &command.target_id,
        command.slot_count,
    )?;
    let mut session = super::ClusterManagerSession::default();
    for plan in plans {
        for slot in plan.slots.clone() {
            super::move_slot_with_session(&nodes, &plan, slot, &mut session)?;
        }
    }
    Ok(())
}

fn plan_reshard(
    nodes: &[super::ClusterNode],
    source_ids: &str,
    target_id: &str,
    slot_count: usize,
) -> Result<Vec<super::RebalancePlan>, String> {
    let target = nodes
        .iter()
        .find(|node| node.id == target_id && node.is_primary)
        .ok_or_else(|| format!("unknown target node '{target_id}'"))?;
    let mut sources = if source_ids.eq_ignore_ascii_case("all") {
        nodes
            .iter()
            .filter(|node| node.is_primary && node.id != target_id)
            .collect::<Vec<_>>()
    } else {
        source_ids
            .split(',')
            .map(|source_id| {
                nodes
                    .iter()
                    .find(|node| node.id == source_id && node.is_primary)
                    .ok_or_else(|| format!("unknown source node '{source_id}'"))
                    .and_then(|source| {
                        (source.id != target_id)
                            .then_some(source)
                            .ok_or_else(|| "target node cannot be a source node".to_string())
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    if sources.is_empty() {
        return Err("no source primary available".to_string());
    }
    sources.sort_unstable_by(|left, right| {
        right
            .slots
            .len()
            .cmp(&left.slots.len())
            .then_with(|| left.id.cmp(&right.id))
    });
    let available = sources
        .iter()
        .map(|source| source.slots.len())
        .sum::<usize>();
    if slot_count > available {
        return Err(format!(
            "source nodes have only {available} slots, requested {slot_count}"
        ));
    }

    let mut remaining = slot_count;
    let mut plans = Vec::new();
    for (index, source) in sources.iter().enumerate() {
        let share = if index + 1 == sources.len() {
            remaining
        } else if index == 0 {
            slot_count
                .saturating_mul(source.slots.len())
                .div_ceil(available)
                .min(remaining)
        } else {
            slot_count
                .saturating_mul(source.slots.len())
                .checked_div(available)
                .unwrap_or_default()
                .min(remaining)
        }
        .min(source.slots.len());
        if share == 0 {
            continue;
        }
        plans.push(super::RebalancePlan {
            source_id: source.id.clone(),
            source_host: source.host.clone(),
            source_port: source.port,
            target_id: target.id.clone(),
            target_host: target.host.clone(),
            target_port: target.port,
            slots: source.slots.iter().copied().take(share).collect(),
        });
        remaining -= share;
    }
    if remaining != 0 {
        return Err(format!(
            "source nodes have only {} allocatable slots, requested {slot_count}",
            slot_count - remaining
        ));
    }
    Ok(plans)
}

fn option_value(args: &[String], option: &str) -> Result<String, String> {
    let index = args
        .iter()
        .position(|arg| arg == option)
        .ok_or_else(|| format!("missing {}", option))?;
    args.get(index + 1)
        .cloned()
        .ok_or_else(|| format!("missing {} value", option))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, is_primary: bool, slots: &[u16]) -> super::super::ClusterNode {
        super::super::ClusterNode {
            id: id.to_string(),
            host: "127.0.0.1".to_string(),
            port: 7000,
            is_primary,
            is_myself: false,
            slots: slots.to_vec(),
            migrating: Vec::new(),
            importing: Vec::new(),
        }
    }

    #[test]
    fn parses_explicit_reshard_endpoints_and_slot_count() {
        let args = vec![
            "--cluster-yes".to_string(),
            "--cluster".to_string(),
            "reshard".to_string(),
            "127.0.0.1:7000".to_string(),
            "--cluster-to".to_string(),
            "target".to_string(),
            "--cluster-from".to_string(),
            "source".to_string(),
            "--cluster-slots".to_string(),
            "1".to_string(),
            "--cluster-pipeline".to_string(),
            "25".to_string(),
            "--cluster-use-atomic-slot-migration".to_string(),
        ];

        assert_eq!(
            parse(&args, 2).unwrap(),
            ReshardCommand {
                seed_host: "127.0.0.1".to_string(),
                seed_port: 7000,
                source_id: "source".to_string(),
                target_id: "target".to_string(),
                slot_count: 1,
            }
        );
    }

    #[test]
    fn all_sources_are_planned_proportionally_without_target_or_replicas() {
        let nodes = vec![
            node("source-a", true, &[0, 1, 2, 3, 4, 5]),
            node("source-b", true, &[6, 7, 8]),
            node("target", true, &[9]),
            node("replica", false, &[10, 11, 12]),
        ];

        let plans = plan_reshard(&nodes, "all", "target", 4).unwrap();

        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].source_id, "source-a");
        assert_eq!(plans[0].slots, vec![0, 1, 2]);
        assert_eq!(plans[1].source_id, "source-b");
        assert_eq!(plans[1].slots, vec![6]);
        assert_eq!(plans.iter().map(|plan| plan.slots.len()).sum::<usize>(), 4);
    }
}
