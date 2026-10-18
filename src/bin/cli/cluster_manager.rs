use super::{Connection, RespValue};
use std::collections::HashMap;
use std::thread;
use std::time::{Duration, Instant};

#[path = "cluster_manager/add_node.rs"]
mod add_node;
#[path = "cluster_manager/backup.rs"]
mod backup;
#[path = "cluster_manager/call.rs"]
mod call;
#[path = "cluster_manager/check.rs"]
mod check;
#[path = "cluster_manager/create.rs"]
mod create;
#[path = "cluster_manager/del_node.rs"]
mod del_node;
#[path = "cluster_manager/fix.rs"]
mod fix;
#[path = "cluster_manager/help.rs"]
mod help;
#[path = "cluster_manager/info.rs"]
mod info;
#[path = "cluster_manager/reshard.rs"]
mod reshard;
#[path = "cluster_manager/set_timeout.rs"]
mod set_timeout;
#[derive(Debug, PartialEq)]
pub(super) struct RebalanceCommand {
    pub seed_host: String,
    pub seed_port: u16,
    pub weight_node: String,
    pub weight: f64,
    pub use_empty_primaries: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct ClusterNode {
    id: String,
    host: String,
    port: u16,
    is_primary: bool,
    is_myself: bool,
    slots: Vec<u16>,
    migrating: Vec<(u16, String)>,
    importing: Vec<(u16, String)>,
}

struct RebalancePlan {
    source_id: String,
    source_host: String,
    source_port: u16,
    target_id: String,
    target_host: String,
    target_port: u16,
    slots: Vec<u16>,
}

#[derive(Default)]
struct ClusterManagerSession {
    connections: HashMap<(String, u16), SessionConnection>,
}

struct SessionConnection {
    connection: Connection,
    selected_db: u8,
}

impl ClusterManagerSession {
    fn connection(&mut self, host: &str, port: u16) -> Result<&mut Connection, String> {
        let key = (host.to_string(), port);
        if !self.connections.contains_key(&key) {
            let connection = Connection::connect_with_defaults(host, port)?;
            self.connections.insert(
                key.clone(),
                SessionConnection {
                    connection,
                    selected_db: 0,
                },
            );
        }
        Ok(&mut self
            .connections
            .get_mut(&key)
            .expect("connection cache entry was inserted")
            .connection)
    }

    fn send_command(
        &mut self,
        host: &str,
        port: u16,
        command: &[&[u8]],
    ) -> Result<RespValue, String> {
        self.connection(host, port)?.send_command(command)
    }

    fn send_ok(&mut self, host: &str, port: u16, command: &[&[u8]]) -> Result<(), String> {
        expect_ok(self.send_command(host, port, command)?)
            .map_err(|error| format!("{}:{} {}", host, port, error))
    }

    fn select_db(&mut self, host: &str, port: u16, db: u8) -> Result<(), String> {
        let key = (host.to_string(), port);
        if !self.connections.contains_key(&key) {
            let connection = Connection::connect_with_defaults(host, port)?;
            self.connections.insert(
                key.clone(),
                SessionConnection {
                    connection,
                    selected_db: 0,
                },
            );
        }
        let entry = self
            .connections
            .get_mut(&key)
            .expect("connection cache entry was inserted");
        if entry.selected_db != db {
            entry.connection.select_db(db)?;
            entry.selected_db = db;
        }
        Ok(())
    }
}

pub(super) fn parse_cluster_command(args: &[String]) -> Option<Result<RebalanceCommand, String>> {
    let cluster_index = args.iter().position(|arg| arg == "--cluster")?;
    let command = args.get(cluster_index + 1)?;
    if command != "rebalance" {
        return Some(Err(format!(
            "unsupported cluster manager command '{}'",
            command
        )));
    }
    let seed = match args.get(cluster_index + 2) {
        Some(seed) => seed,
        None => return Some(Err("missing cluster seed address".to_string())),
    };
    let (seed_host, seed_port) = match parse_address(seed) {
        Ok(address) => address,
        Err(error) => return Some(Err(error)),
    };
    let weight_index = match args.iter().position(|arg| arg == "--cluster-weight") {
        Some(index) => index,
        None => return Some(Err("missing --cluster-weight".to_string())),
    };
    let weight = match args.get(weight_index + 1) {
        Some(weight) => weight,
        None => return Some(Err("missing --cluster-weight value".to_string())),
    };
    let (node_id, value) = match weight.split_once('=') {
        Some(pair) => pair,
        None => return Some(Err("invalid --cluster-weight value".to_string())),
    };
    let weight = match value.parse::<f64>() {
        Ok(weight) if weight.is_finite() && weight >= 0.0 => weight,
        _ => return Some(Err("invalid --cluster-weight value".to_string())),
    };
    Some(Ok(RebalanceCommand {
        seed_host,
        seed_port,
        weight_node: node_id.to_string(),
        weight,
        use_empty_primaries: args.iter().any(|arg| {
            matches!(
                arg.as_str(),
                "--cluster-use-empty-masters" | "--cluster-use-empty-primaries"
            )
        }),
    }))
}

fn parse_address(address: &str) -> Result<(String, u16), String> {
    let (host, port) = address
        .rsplit_once(':')
        .ok_or_else(|| format!("invalid cluster address '{}'", address))?;
    let port = port
        .parse()
        .map_err(|_| format!("invalid cluster port '{}'", port))?;
    Ok((host.to_string(), port))
}

fn parse_cluster_nodes(response: &str) -> Result<Vec<ClusterNode>, String> {
    let mut nodes = Vec::new();
    for line in response
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 8 {
            return Err(format!("invalid CLUSTER NODES line '{}'", line));
        }
        let endpoint = fields[1].split('@').next().unwrap_or(fields[1]);
        let (host, port) = parse_address(endpoint)?;
        let flags: Vec<&str> = fields[2].split(',').collect();
        let mut slots = Vec::new();
        let mut migrating = Vec::new();
        let mut importing = Vec::new();
        for token in &fields[8..] {
            if token.starts_with('[') {
                let state = token.trim_matches(['[', ']']);
                if let Some((slot, node_id)) = state.split_once("->-") {
                    migrating.push((
                        slot.parse()
                            .map_err(|_| format!("invalid slot '{}'", slot))?,
                        node_id.to_string(),
                    ));
                } else if let Some((slot, node_id)) = state.split_once("-<-") {
                    importing.push((
                        slot.parse()
                            .map_err(|_| format!("invalid slot '{}'", slot))?,
                        node_id.to_string(),
                    ));
                }
                continue;
            }
            if let Some((start, end)) = token.split_once('-') {
                let start: u16 = start
                    .parse()
                    .map_err(|_| format!("invalid slot '{}'", start))?;
                let end: u16 = end.parse().map_err(|_| format!("invalid slot '{}'", end))?;
                slots.extend(start..=end);
            } else {
                slots.push(
                    token
                        .parse()
                        .map_err(|_| format!("invalid slot '{}'", token))?,
                );
            }
        }
        nodes.push(ClusterNode {
            id: fields[0].to_string(),
            host,
            port,
            is_primary: flags.contains(&"master"),
            is_myself: flags.contains(&"myself"),
            slots,
            migrating,
            importing,
        });
    }
    Ok(nodes)
}

fn plan_rebalance(nodes: &[ClusterNode], source_id: &str) -> Result<RebalancePlan, String> {
    let source = nodes
        .iter()
        .find(|node| node.id == source_id && node.is_primary)
        .ok_or_else(|| format!("unknown primary node '{}'", source_id))?;
    let mut targets = nodes
        .iter()
        .filter(|node| node.is_primary && node.id != source_id);
    let mut target = targets
        .next()
        .ok_or_else(|| "no target primary available".to_string())?;
    for candidate in targets {
        if candidate.slots.len() < target.slots.len() {
            target = candidate;
        }
    }
    Ok(RebalancePlan {
        source_id: source.id.clone(),
        source_host: source.host.clone(),
        source_port: source.port,
        target_id: target.id.clone(),
        target_host: target.host.clone(),
        target_port: target.port,
        slots: source.slots.clone(),
    })
}

pub(super) fn try_execute(args: &[String]) -> Option<Result<(), String>> {
    let cluster_index = args.iter().position(|arg| arg == "--cluster")?;
    let command_index = cluster_index + 1;
    let command = match args.get(command_index).map(String::as_str) {
        Some("add-node") => add_node::execute(args, command_index),
        Some("backup") => backup::execute(args, command_index),
        Some("call") => call::execute(args, command_index),
        Some("create") => create::parse(args, command_index).and_then(create::execute),
        Some("check") => check::execute(args, command_index),
        Some("fix") => fix::execute(args, command_index),
        Some("help") => help::execute(),
        Some("info") => info::execute(args, command_index),
        Some("reshard") => reshard::parse(args, command_index).and_then(reshard::execute),
        Some("set-timeout") => set_timeout::execute(args, command_index),
        Some("del-node") => del_node::execute(args, command_index),
        Some("rebalance") => parse_cluster_command(args)
            .expect("cluster command presence was checked")
            .and_then(execute_rebalance),
        Some(command) => Err(format!("unsupported cluster manager command '{}'", command)),
        None => Err("missing cluster manager command".to_string()),
    };
    Some(command)
}

pub(super) fn execute_if_requested(args: &[String]) -> bool {
    let Some(result) = try_execute(args) else {
        return false;
    };
    if let Err(error) = result {
        eprintln!("Error: {}", error);
        std::process::exit(1);
    }
    true
}

fn execute_rebalance(command: RebalanceCommand) -> Result<(), String> {
    let mut seed = Connection::connect_with_defaults(&command.seed_host, command.seed_port)?;
    let response = seed.send_command(&[b"CLUSTER", b"NODES"])?;
    let nodes_text = match response {
        RespValue::Bulk(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        RespValue::Error(error) => return Err(error),
        other => return Err(format!("unexpected CLUSTER NODES response: {:?}", other)),
    };
    let mut nodes = parse_cluster_nodes(&nodes_text)?;
    for node in &mut nodes {
        node.is_primary = is_live_primary(&node.host, node.port)?;
    }
    if command.weight > 0.0 {
        let target = nodes
            .iter()
            .find(|node| node.id == command.weight_node)
            .ok_or_else(|| format!("unknown node '{}'", command.weight_node))?;
        let target_host = target.host.clone();
        let target_port = target.port;
        let target_id = target.id.clone();
        let restore_replica_migration = if !target.is_primary {
            if !command.use_empty_primaries || !target.slots.is_empty() {
                return Err(format!("node '{}' is not a primary", target_id));
            }
            let was_enabled = cluster_replica_migration_enabled(&target_host, target_port)?;
            send_ok(
                &target_host,
                target_port,
                &[b"CONFIG", b"SET", b"cluster-allow-replica-migration", b"no"],
            )?;
            let detach = send_ok(
                &target_host,
                target_port,
                &[b"CLUSTER", b"REPLICATE", b"NO", b"ONE"],
            )
            .and_then(|_| wait_for_primary_visibility(&nodes, &target_id));
            if let Err(error) = detach {
                let _ = restore_replica_migration_config(&target_host, target_port, was_enabled);
                return Err(error);
            }
            Some(was_enabled)
        } else {
            None
        };
        let result: Result<(), String> = (|| {
            let mut session = ClusterManagerSession::default();
            for plan in
                plan_positive_weight_rebalance(&nodes, &command.weight_node, command.weight)?
            {
                for slot in plan.slots.clone() {
                    move_slot_with_session(&nodes, &plan, slot, &mut session)?;
                }
            }
            Ok(())
        })();
        if let Some(was_enabled) = restore_replica_migration {
            let restore = restore_replica_migration_config(&target_host, target_port, was_enabled);
            result?;
            restore?;
        } else {
            result?;
        }
    } else {
        let plan = plan_rebalance(&nodes, &command.weight_node)?;
        let mut session = ClusterManagerSession::default();
        for slot in plan.slots.clone() {
            move_slot_with_session(&nodes, &plan, slot, &mut session)?;
        }
    }
    Ok(())
}

fn cluster_replica_migration_enabled(host: &str, port: u16) -> Result<bool, String> {
    let mut connection = Connection::connect_with_defaults(host, port)?;
    match connection.send_command(&[b"CONFIG", b"GET", b"cluster-allow-replica-migration"])? {
        RespValue::Array(items) => match items.get(1) {
            Some(RespValue::Bulk(value)) if value.eq_ignore_ascii_case(b"yes") => Ok(true),
            Some(RespValue::Bulk(value)) if value.eq_ignore_ascii_case(b"no") => Ok(false),
            other => Err(format!(
                "unexpected cluster-allow-replica-migration value: {:?}",
                other
            )),
        },
        RespValue::Error(error) => Err(error),
        other => Err(format!("unexpected CONFIG GET response: {:?}", other)),
    }
}

fn restore_replica_migration_config(host: &str, port: u16, enabled: bool) -> Result<(), String> {
    let value = if enabled {
        b"yes".as_slice()
    } else {
        b"no".as_slice()
    };
    send_ok(
        host,
        port,
        &[b"CONFIG", b"SET", b"cluster-allow-replica-migration", value],
    )
}

fn wait_for_primary_visibility(nodes: &[ClusterNode], node_id: &str) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let all_visible = nodes.iter().all(|node| {
            let Ok(mut connection) = Connection::connect_with_defaults(&node.host, node.port)
            else {
                return false;
            };
            let Ok(RespValue::Bulk(bytes)) = connection.send_command(&[b"CLUSTER", b"NODES"])
            else {
                return false;
            };
            let text = String::from_utf8_lossy(&bytes);
            parse_cluster_nodes(&text)
                .ok()
                .and_then(|view| view.into_iter().find(|entry| entry.id == node_id))
                .is_some_and(|entry| entry.is_primary)
        });
        if all_visible {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "node '{}' did not become a primary across the cluster",
                node_id
            ));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn plan_positive_weight_rebalance(
    nodes: &[ClusterNode],
    target_id: &str,
    target_weight: f64,
) -> Result<Vec<RebalancePlan>, String> {
    let target = nodes
        .iter()
        .find(|node| node.id == target_id && node.slots.is_empty())
        .ok_or_else(|| format!("unknown empty node '{}'", target_id))?;
    let mut sources = nodes
        .iter()
        .filter(|node| node.is_primary && node.id != target_id && !node.slots.is_empty())
        .collect::<Vec<_>>();
    sources.sort_unstable_by(|left, right| {
        right
            .slots
            .len()
            .cmp(&left.slots.len())
            .then_with(|| left.id.cmp(&right.id))
    });
    let total_weight = sources.len() as f64 + target_weight;
    if total_weight <= 0.0 {
        return Err("total cluster weight must be positive".to_string());
    }
    let desired = ((16_384.0 / total_weight) * target_weight) as usize;
    let mut remaining = desired.saturating_sub(target.slots.len());
    let mut plans = Vec::new();
    for source in sources {
        let count = remaining.min(source.slots.len());
        if count == 0 {
            continue;
        }
        plans.push(RebalancePlan {
            source_id: source.id.clone(),
            source_host: source.host.clone(),
            source_port: source.port,
            target_id: target.id.clone(),
            target_host: target.host.clone(),
            target_port: target.port,
            slots: source.slots.iter().copied().take(count).collect(),
        });
        remaining -= count;
    }
    if remaining != 0 {
        return Err(format!(
            "not enough slots to assign weight to '{}'",
            target_id
        ));
    }
    Ok(plans)
}

fn move_slot_with_session(
    nodes: &[ClusterNode],
    plan: &RebalancePlan,
    slot: u16,
    session: &mut ClusterManagerSession,
) -> Result<(), String> {
    let slot_text = slot.to_string();
    session
        .send_ok(
            &plan.target_host,
            plan.target_port,
            &[
                b"CLUSTER",
                b"SETSLOT",
                slot_text.as_bytes(),
                b"IMPORTING",
                plan.source_id.as_bytes(),
            ],
        )
        .map_err(|error| format!("target importing: {}", error))?;
    session
        .send_ok(
            &plan.source_host,
            plan.source_port,
            &[
                b"CLUSTER",
                b"SETSLOT",
                slot_text.as_bytes(),
                b"MIGRATING",
                plan.target_id.as_bytes(),
            ],
        )
        .map_err(|error| format!("source migrating: {}", error))?;
    migrate_slot_keys(session, plan, slot)?;

    send_slot_owner_with_session(
        session,
        &plan.target_host,
        plan.target_port,
        slot,
        &plan.target_id,
    )
    .map_err(|error| format!("target owner: {}", error))?;
    accept_source_owner_result(send_slot_owner_with_session(
        session,
        &plan.source_host,
        plan.source_port,
        slot,
        &plan.target_id,
    ))
    .map_err(|error| format!("source owner: {}", error))?;
    for node in nodes
        .iter()
        .filter(|node| node.is_primary && node.id != plan.source_id && node.id != plan.target_id)
    {
        if let Err(error) =
            send_slot_owner_with_session(session, &node.host, node.port, slot, &plan.target_id)
        {
            if !is_replica_setslot_error(&error) {
                return Err(format!("peer owner: {}", error));
            }
        }
    }
    Ok(())
}

fn migrate_slot_keys(
    session: &mut ClusterManagerSession,
    plan: &RebalancePlan,
    slot: u16,
) -> Result<(), String> {
    let database_count = cluster_database_count(session, &plan.source_host, plan.source_port)?;
    for database in 0..database_count {
        loop {
            session.select_db(&plan.source_host, plan.source_port, database as u8)?;
            let source = session.connection(&plan.source_host, plan.source_port)?;
            let slot_text = slot.to_string();
            let response = source.send_command(&[
                b"CLUSTER",
                b"GETKEYSINSLOT",
                slot_text.as_bytes(),
                b"10",
            ])?;
            let keys = bulk_items(response)?;
            if keys.is_empty() {
                break;
            }
            let target_port = plan.target_port.to_string();
            let database_text = database.to_string();
            let mut command = vec![
                b"MIGRATE".as_slice(),
                plan.target_host.as_bytes(),
                target_port.as_bytes(),
                b"",
                database_text.as_bytes(),
                b"5000",
                b"KEYS",
            ];
            command.extend(keys.iter().map(Vec::as_slice));
            expect_ok(source.send_command(&command)?)?;
        }
    }
    Ok(())
}

fn cluster_database_count(
    session: &mut ClusterManagerSession,
    host: &str,
    port: u16,
) -> Result<usize, String> {
    match session.send_command(host, port, &[b"CONFIG", b"GET", b"cluster-databases"])? {
        RespValue::Array(items) => match items.get(1) {
            Some(RespValue::Bulk(value)) => String::from_utf8_lossy(value)
                .parse()
                .map_err(|_| "invalid cluster-databases value".to_string()),
            other => Err(format!("unexpected cluster-databases value: {:?}", other)),
        },
        RespValue::Error(_) => Ok(1),
        other => Err(format!(
            "unexpected CONFIG GET cluster-databases response: {:?}",
            other
        )),
    }
}

fn bulk_items(response: RespValue) -> Result<Vec<Vec<u8>>, String> {
    match response {
        RespValue::Array(items) => items
            .into_iter()
            .map(|item| match item {
                RespValue::Bulk(bytes) => Ok(bytes),
                other => Err(format!("unexpected key response: {:?}", other)),
            })
            .collect(),
        RespValue::Error(error) => Err(error),
        other => Err(format!(
            "unexpected CLUSTER GETKEYSINSLOT response: {:?}",
            other
        )),
    }
}

fn send_slot_owner_with_session(
    session: &mut ClusterManagerSession,
    host: &str,
    port: u16,
    slot: u16,
    target_id: &str,
) -> Result<(), String> {
    let slot_text = slot.to_string();
    session.send_ok(
        host,
        port,
        &[
            b"CLUSTER",
            b"SETSLOT",
            slot_text.as_bytes(),
            b"NODE",
            target_id.as_bytes(),
        ],
    )
}

fn send_ok(host: &str, port: u16, command: &[&[u8]]) -> Result<(), String> {
    let mut connection = Connection::connect_with_defaults(host, port)?;
    expect_ok(connection.send_command(command)?)
        .map_err(|error| format!("{}:{} {}", host, port, error))
}

fn expect_ok(response: RespValue) -> Result<(), String> {
    match response {
        RespValue::SimpleString(value) if value == "OK" => Ok(()),
        RespValue::Error(error) => Err(error),
        other => Err(format!("unexpected command response: {:?}", other)),
    }
}

fn is_live_primary_with_session(
    session: &mut ClusterManagerSession,
    host: &str,
    port: u16,
) -> Result<bool, String> {
    role_is_primary(session.send_command(host, port, &[b"ROLE"])?)
}

fn is_live_primary(host: &str, port: u16) -> Result<bool, String> {
    let mut session = ClusterManagerSession::default();
    is_live_primary_with_session(&mut session, host, port)
}

fn role_is_primary(response: RespValue) -> Result<bool, String> {
    match response {
        RespValue::Array(items) => match items.first() {
            Some(RespValue::Bulk(role)) => Ok(role.eq_ignore_ascii_case(b"master")),
            other => Err(format!("unexpected ROLE entry: {:?}", other)),
        },
        RespValue::Error(error) => Err(error),
        other => Err(format!("unexpected ROLE response: {:?}", other)),
    }
}

fn is_replica_setslot_error(error: &str) -> bool {
    error.contains("Please use SETSLOT only with masters")
        || error.contains("Please use SETSLOT only with primaries")
        || error.starts_with("UNBLOCKED")
        || error.starts_with("NOREPLICAS")
        || error.contains(" NOREPLICAS ")
}

fn accept_source_owner_result(result: Result<(), String>) -> Result<(), String> {
    match result {
        Err(error) if is_replica_setslot_error(&error) => Ok(()),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;

    #[test]
    fn parses_rebalance_seed_and_zero_weight_node() {
        let args = vec![
            "--cluster".to_string(),
            "rebalance".to_string(),
            "127.0.0.1:7000".to_string(),
            "--cluster-weight".to_string(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa=0".to_string(),
        ];

        let command = parse_cluster_command(&args).unwrap().unwrap();

        assert_eq!(command.seed_host, "127.0.0.1");
        assert_eq!(command.seed_port, 7000);
        assert_eq!(
            command.weight_node,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(command.weight, 0.0);
        assert!(!command.use_empty_primaries);
    }

    #[test]
    fn parses_positive_weight_for_empty_primary_rebalance() {
        let args = vec![
            "--cluster".to_string(),
            "rebalance".to_string(),
            "127.0.0.1:7000".to_string(),
            "--cluster-weight".to_string(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa=.01".to_string(),
            "--cluster-use-empty-masters".to_string(),
        ];

        let command = parse_cluster_command(&args).unwrap().unwrap();

        assert_eq!(command.weight, 0.01);
        assert!(command.use_empty_primaries);
    }

    #[test]
    fn parses_primary_nodes_and_slot_ranges() {
        let nodes = concat!(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 127.0.0.1:7000@17000 master - 0 0 1 connected 0 10-12\n",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 127.0.0.1:7001@17001 master - 0 0 2 connected 1-9 13-16383\n",
            "cccccccccccccccccccccccccccccccccccccccc 127.0.0.1:7002@17002 slave aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 0 0 3 connected\n",
        );

        let parsed = parse_cluster_nodes(nodes).unwrap();

        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].slots, vec![0, 10, 11, 12]);
        assert_eq!(parsed[1].host, "127.0.0.1");
        assert_eq!(parsed[1].port, 7001);
        assert!(!parsed[2].is_primary);
    }

    #[test]
    fn parses_migrating_and_importing_slot_markers() {
        let nodes = concat!(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 127.0.0.1:7000@17000 master - 0 0 1 connected 609 [609->-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb]\n",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 127.0.0.1:7001@17001 master - 0 0 2 connected [609-<-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa]\n",
        );

        let parsed = parse_cluster_nodes(nodes).unwrap();

        assert_eq!(parsed[0].migrating, vec![(609, parsed[1].id.clone())]);
        assert_eq!(parsed[1].importing, vec![(609, parsed[0].id.clone())]);
    }

    #[test]
    fn plans_all_source_slots_to_least_loaded_primary() {
        let nodes = vec![
            ClusterNode {
                id: "source".to_string(),
                host: "127.0.0.1".to_string(),
                port: 7000,
                is_primary: true,
                is_myself: false,
                slots: vec![0, 1],
                migrating: Vec::new(),
                importing: Vec::new(),
            },
            ClusterNode {
                id: "target".to_string(),
                host: "127.0.0.1".to_string(),
                port: 7001,
                is_primary: true,
                is_myself: false,
                slots: vec![2],
                migrating: Vec::new(),
                importing: Vec::new(),
            },
            ClusterNode {
                id: "other".to_string(),
                host: "127.0.0.1".to_string(),
                port: 7002,
                is_primary: true,
                is_myself: false,
                slots: vec![3, 4, 5],
                migrating: Vec::new(),
                importing: Vec::new(),
            },
        ];

        let plan = plan_rebalance(&nodes, "source").unwrap();

        assert_eq!(plan.source_id, "source");
        assert_eq!(plan.target_id, "target");
        assert_eq!(plan.slots, vec![0, 1]);
    }

    #[test]
    fn positive_weight_assigns_slots_to_an_empty_replica_target() {
        let target = ClusterNode {
            id: "target".to_string(),
            host: "127.0.0.1".to_string(),
            port: 7000,
            is_primary: false,
            is_myself: false,
            slots: Vec::new(),
            migrating: Vec::new(),
            importing: Vec::new(),
        };
        let source = |id: &str, port: u16, slots: std::ops::Range<u16>| ClusterNode {
            id: id.to_string(),
            host: "127.0.0.1".to_string(),
            port,
            is_primary: true,
            is_myself: false,
            slots: slots.collect(),
            migrating: Vec::new(),
            importing: Vec::new(),
        };
        let nodes = vec![
            target,
            source("source-a", 7001, 0..5_000),
            source("source-b", 7002, 5_000..9_000),
            source("source-c", 7003, 9_000..13_000),
            source("source-d", 7004, 13_000..16_384),
        ];

        let plans = plan_positive_weight_rebalance(&nodes, "target", 0.01).unwrap();

        assert_eq!(plans.iter().map(|plan| plan.slots.len()).sum::<usize>(), 40);
        assert!(plans.iter().all(|plan| plan.target_id == "target"));
        assert_eq!(plans[0].source_id, "source-a");
    }

    #[test]
    fn role_reply_distinguishes_primary_and_replica() {
        assert!(
            role_is_primary(RespValue::Array(vec![RespValue::Bulk(b"master".to_vec())])).unwrap()
        );
        assert!(
            !role_is_primary(RespValue::Array(vec![RespValue::Bulk(b"slave".to_vec())])).unwrap()
        );
    }

    #[test]
    fn classifies_replica_setslot_race() {
        assert!(is_replica_setslot_error(
            "127.0.0.1:7000 ERR Please use SETSLOT only with masters."
        ));
        assert!(is_replica_setslot_error(
            "127.0.0.1:7000 ERR Please use SETSLOT only with primaries."
        ));
        assert!(is_replica_setslot_error(
            "127.0.0.1:7000 NOREPLICAS Not enough good replicas to write."
        ));
        assert!(is_replica_setslot_error(
            "UNBLOCKED force unblock from blocking operation"
        ));
        assert!(!is_replica_setslot_error("ERR Unknown node"));
    }

    #[test]
    fn accepts_source_demotion_during_final_slot_owner_update() {
        let result = accept_source_owner_result(Err(
            "127.0.0.1:7000 ERR Please use SETSLOT only with masters.".to_string(),
        ));

        assert_eq!(result, Ok(()));
        assert_eq!(
            accept_source_owner_result(Err("Connection closed by server".to_string())),
            Err("Connection closed by server".to_string())
        );
    }

    #[test]
    fn reshard_session_reuses_one_connection_per_primary() {
        let (source_port, source) = responding_server();
        let (target_port, target) = responding_server();
        let nodes = vec![
            ClusterNode {
                id: "source".to_string(),
                host: "127.0.0.1".to_string(),
                port: source_port,
                is_primary: true,
                is_myself: false,
                slots: vec![42, 43],
                migrating: Vec::new(),
                importing: Vec::new(),
            },
            ClusterNode {
                id: "target".to_string(),
                host: "127.0.0.1".to_string(),
                port: target_port,
                is_primary: true,
                is_myself: false,
                slots: Vec::new(),
                migrating: Vec::new(),
                importing: Vec::new(),
            },
        ];
        let plan = RebalancePlan {
            source_id: "source".to_string(),
            source_host: "127.0.0.1".to_string(),
            source_port,
            target_id: "target".to_string(),
            target_host: "127.0.0.1".to_string(),
            target_port,
            slots: vec![42, 43],
        };

        let mut session = ClusterManagerSession::default();
        move_slot_with_session(&nodes, &plan, 42, &mut session).unwrap();
        move_slot_with_session(&nodes, &plan, 43, &mut session).unwrap();

        assert_eq!(source.join().unwrap(), 1);
        assert_eq!(target.join().unwrap(), 1);
    }

    #[test]
    fn move_slot_updates_source_before_other_primaries() {
        let (events_tx, events_rx) = mpsc::channel();
        let (source_port, source) = recording_server("source", events_tx.clone());
        let (target_port, target) = recording_server("target", events_tx.clone());
        let (peer_port, peer) = recording_server("peer", events_tx);
        let nodes = vec![
            cluster_node("source", source_port, &[42]),
            cluster_node("target", target_port, &[]),
            cluster_node("peer", peer_port, &[]),
        ];
        let plan = RebalancePlan {
            source_id: "source".to_string(),
            source_host: "127.0.0.1".to_string(),
            source_port,
            target_id: "target".to_string(),
            target_host: "127.0.0.1".to_string(),
            target_port,
            slots: vec![42],
        };

        move_slot_with_session(&nodes, &plan, 42, &mut ClusterManagerSession::default()).unwrap();

        source.join().unwrap();
        target.join().unwrap();
        peer.join().unwrap();
        let events: Vec<_> = events_rx.try_iter().collect();
        let target_owner = events
            .iter()
            .position(|event| {
                event
                    == &(
                        "target".to_string(),
                        "CLUSTER SETSLOT 42 NODE target".to_string(),
                    )
            })
            .unwrap();
        let source_owner = events
            .iter()
            .position(|event| {
                event
                    == &(
                        "source".to_string(),
                        "CLUSTER SETSLOT 42 NODE target".to_string(),
                    )
            })
            .unwrap();
        let peer_owner = events
            .iter()
            .position(|event| {
                event
                    == &(
                        "peer".to_string(),
                        "CLUSTER SETSLOT 42 NODE target".to_string(),
                    )
            })
            .unwrap();

        assert!(target_owner < source_owner);
        assert!(source_owner < peer_owner);
        assert!(
            events
                .iter()
                .all(|(_, command)| command != "ROLE" && command != "SELECT 0"),
            "rebalance already has a primary topology snapshot; it must not add per-slot ROLE or SELECT 0 round trips"
        );
    }

    #[test]
    fn session_restores_db_zero_after_reusing_a_source_connection() {
        let (events_tx, events_rx) = mpsc::channel();
        let (port, server) = recording_server("source", events_tx);
        let mut session = ClusterManagerSession::default();

        session.select_db("127.0.0.1", port, 0).unwrap();
        session.select_db("127.0.0.1", port, 1).unwrap();
        session.select_db("127.0.0.1", port, 1).unwrap();
        session.select_db("127.0.0.1", port, 0).unwrap();

        drop(session);
        server.join().unwrap();
        let events: Vec<_> = events_rx.try_iter().collect();
        let commands: Vec<_> = events.into_iter().map(|(_, command)| command).collect();
        assert_eq!(commands, vec!["SELECT 1", "SELECT 0"]);
    }

    fn responding_server() -> (u16, std::thread::JoinHandle<usize>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let mut accepts = 0;
            let mut last_activity = Instant::now();
            while last_activity.elapsed() < Duration::from_millis(250) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        accepts += 1;
                        stream.set_nonblocking(false).unwrap();
                        stream
                            .set_read_timeout(Some(Duration::from_millis(50)))
                            .unwrap();
                        let mut request = [0; 1024];
                        loop {
                            match stream.read(&mut request) {
                                Ok(0) => break,
                                Ok(length) => {
                                    let reply = if request[..length]
                                        .windows(b"cluster-databases".len())
                                        .any(|window| window == b"cluster-databases")
                                    {
                                        b"*2\r\n$17\r\ncluster-databases\r\n$1\r\n1\r\n".as_slice()
                                    } else if request[..length]
                                        .windows(b"GETKEYSINSLOT".len())
                                        .any(|window| window == b"GETKEYSINSLOT")
                                    {
                                        b"*0\r\n".as_slice()
                                    } else {
                                        b"+OK\r\n".as_slice()
                                    };
                                    stream.write_all(reply).unwrap();
                                    last_activity = Instant::now();
                                }
                                Err(error)
                                    if matches!(
                                        error.kind(),
                                        std::io::ErrorKind::WouldBlock
                                            | std::io::ErrorKind::TimedOut
                                    ) =>
                                {
                                    break;
                                }
                                Err(error) => panic!("test server read failed: {error}"),
                            }
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("test server accept failed: {error}"),
                }
            }
            accepts
        });
        (port, server)
    }

    fn cluster_node(id: &str, port: u16, slots: &[u16]) -> ClusterNode {
        ClusterNode {
            id: id.to_string(),
            host: "127.0.0.1".to_string(),
            port,
            is_primary: true,
            is_myself: false,
            slots: slots.to_vec(),
            migrating: Vec::new(),
            importing: Vec::new(),
        }
    }

    fn recording_server(
        name: &'static str,
        events: mpsc::Sender<(String, String)>,
    ) -> (u16, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let mut last_activity = Instant::now();
            while last_activity.elapsed() < Duration::from_millis(250) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream.set_nonblocking(false).unwrap();
                        stream
                            .set_read_timeout(Some(Duration::from_millis(50)))
                            .unwrap();
                        let mut request = [0; 1024];
                        loop {
                            match stream.read(&mut request) {
                                Ok(0) => break,
                                Ok(length) => {
                                    let command = resp_command(&request[..length]);
                                    let reply = if command == "CONFIG GET cluster-databases" {
                                        b"*2\r\n$17\r\ncluster-databases\r\n$1\r\n1\r\n".as_slice()
                                    } else if command.starts_with("CLUSTER GETKEYSINSLOT") {
                                        b"*0\r\n".as_slice()
                                    } else if command == "ROLE" {
                                        b"*1\r\n$6\r\nmaster\r\n".as_slice()
                                    } else {
                                        b"+OK\r\n".as_slice()
                                    };
                                    events.send((name.to_string(), command)).unwrap();
                                    stream.write_all(reply).unwrap();
                                    last_activity = Instant::now();
                                }
                                Err(error)
                                    if matches!(
                                        error.kind(),
                                        std::io::ErrorKind::WouldBlock
                                            | std::io::ErrorKind::TimedOut
                                    ) =>
                                {
                                    break;
                                }
                                Err(error) => panic!("test server read failed: {error}"),
                            }
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("test server accept failed: {error}"),
                }
            }
        });
        (port, server)
    }

    fn resp_command(request: &[u8]) -> String {
        let mut parts = request.split(|byte| *byte == b'\n');
        let _ = parts.next();
        let mut command = Vec::new();
        while let Some(length) = parts.next() {
            if !length.starts_with(b"$") {
                continue;
            }
            if let Some(value) = parts.next() {
                command.push(String::from_utf8_lossy(value).trim().to_string());
            }
        }
        command.join(" ")
    }
}
