use super::super::{Connection, RespValue};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, PartialEq, Eq)]
pub(super) struct CreateCommand {
    pub nodes: Vec<(String, u16)>,
    pub replicas: usize,
}

pub(super) struct ClusterEndpoint {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub bus_port: u16,
}

#[derive(Debug, PartialEq, Eq)]
struct NodeLayout {
    primary_index: usize,
    slot_range: Option<(u16, u16)>,
}

fn plan_layout(node_count: usize, replicas: usize) -> Result<Vec<NodeLayout>, String> {
    let group_size = replicas
        .checked_add(1)
        .ok_or_else(|| "replica count is too large".to_string())?;
    if node_count == 0 || !node_count.is_multiple_of(group_size) {
        return Err(format!(
            "{} nodes cannot form groups with {} replicas",
            node_count, replicas
        ));
    }
    let primary_count = node_count / group_size;
    let mut layout = Vec::with_capacity(node_count);
    for primary_index in 0..primary_count {
        let start = ((primary_index * 16_384 + primary_count / 2) / primary_count) as u16;
        let next_start =
            (((primary_index + 1) * 16_384 + primary_count / 2) / primary_count) as u16;
        layout.push(NodeLayout {
            primary_index,
            slot_range: Some((start, next_start - 1)),
        });
    }
    for replica_index in 0..(node_count - primary_count) {
        layout.push(NodeLayout {
            primary_index: replica_index % primary_count,
            slot_range: None,
        });
    }
    Ok(layout)
}

pub(super) fn parse(args: &[String], command_index: usize) -> Result<CreateCommand, String> {
    let mut nodes = Vec::new();
    let mut replicas = 0;
    let mut index = command_index + 1;
    while index < args.len() {
        match args[index].as_str() {
            "--cluster-replicas" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "missing --cluster-replicas value".to_string())?;
                replicas = value
                    .parse()
                    .map_err(|_| format!("invalid replica count '{}'", value))?;
                index += 2;
            }
            option if option.starts_with('-') => {
                index += 1;
            }
            address => {
                nodes.push(super::parse_address(address)?);
                index += 1;
            }
        }
    }
    if nodes.is_empty() {
        return Err("cluster create requires at least one node".to_string());
    }
    Ok(CreateCommand { nodes, replicas })
}

pub(super) fn execute(command: CreateCommand) -> Result<(), String> {
    let layout = plan_layout(command.nodes.len(), command.replicas)?;
    let mut endpoints = Vec::with_capacity(command.nodes.len());
    for (host, port) in &command.nodes {
        endpoints.push(read_self_endpoint(host, *port)?);
    }
    assign_primary_slots(&endpoints, &layout)?;
    assign_config_epochs(&endpoints)?;
    join_nodes(&endpoints)?;
    wait_for_known_nodes(&endpoints, endpoints.len())?;
    assign_replicas(&endpoints, &layout)?;
    wait_for_cluster_ok(&endpoints)?;
    Ok(())
}

pub(super) fn read_self_endpoint(host: &str, port: u16) -> Result<ClusterEndpoint, String> {
    let mut connection = Connection::connect_with_defaults(host, port)?;
    let response = connection.send_command(&[b"CLUSTER", b"NODES"])?;
    let nodes = bulk_string(response, "CLUSTER NODES")?;
    let line = nodes
        .lines()
        .find(|line| {
            line.split_whitespace()
                .nth(2)
                .is_some_and(|flags| flags.split(',').any(|flag| flag == "myself"))
        })
        .ok_or_else(|| format!("{}:{} CLUSTER NODES has no myself entry", host, port))?;
    let fields: Vec<&str> = line.split_whitespace().collect();
    let endpoint = fields
        .get(1)
        .ok_or_else(|| format!("invalid CLUSTER NODES line '{}'", line))?;
    let (client_endpoint, bus_endpoint) = endpoint
        .split_once('@')
        .ok_or_else(|| format!("missing cluster bus port in '{}'", endpoint))?;
    let (advertised_host, advertised_port) = super::parse_address(client_endpoint)?;
    let bus_port = bus_endpoint
        .split(',')
        .next()
        .ok_or_else(|| format!("missing cluster bus port in '{}'", endpoint))?
        .parse()
        .map_err(|_| format!("invalid cluster bus port in '{}'", endpoint))?;
    Ok(ClusterEndpoint {
        id: fields[0].to_string(),
        host: advertised_host,
        port: advertised_port,
        bus_port,
    })
}

fn join_nodes(endpoints: &[ClusterEndpoint]) -> Result<(), String> {
    let Some(seed) = endpoints.first() else {
        return Ok(());
    };
    for endpoint in &endpoints[1..] {
        let port = seed.port.to_string();
        let bus_port = seed.bus_port.to_string();
        send_ok(
            &endpoint.host,
            endpoint.port,
            &[
                b"CLUSTER",
                b"MEET",
                seed.host.as_bytes(),
                port.as_bytes(),
                bus_port.as_bytes(),
            ],
        )?;
    }
    Ok(())
}

fn assign_primary_slots(
    endpoints: &[ClusterEndpoint],
    layout: &[NodeLayout],
) -> Result<(), String> {
    for (endpoint, node_layout) in endpoints.iter().zip(layout) {
        let Some((start, end)) = node_layout.slot_range else {
            continue;
        };
        let command = build_addslots_command(start, end);
        let command_refs = command.iter().map(Vec::as_slice).collect::<Vec<&[u8]>>();
        send_ok(&endpoint.host, endpoint.port, &command_refs)?;
    }
    Ok(())
}

fn build_addslots_command(start: u16, end: u16) -> Vec<Vec<u8>> {
    let mut command = Vec::with_capacity(usize::from(end - start) + 3);
    command.push(b"CLUSTER".to_vec());
    command.push(b"ADDSLOTS".to_vec());
    for slot in start..=end {
        command.push(slot.to_string().into_bytes());
    }
    command
}

fn assign_config_epochs(endpoints: &[ClusterEndpoint]) -> Result<(), String> {
    for (index, endpoint) in endpoints.iter().enumerate() {
        let epoch = (index + 1).to_string();
        send_ok(
            &endpoint.host,
            endpoint.port,
            &[b"CLUSTER", b"SET-CONFIG-EPOCH", epoch.as_bytes()],
        )?;
    }
    Ok(())
}

fn assign_replicas(endpoints: &[ClusterEndpoint], layout: &[NodeLayout]) -> Result<(), String> {
    for (endpoint, node_layout) in endpoints.iter().zip(layout) {
        if node_layout.slot_range.is_some() {
            continue;
        }
        let primary = &endpoints[node_layout.primary_index];
        send_ok(
            &endpoint.host,
            endpoint.port,
            &[b"CLUSTER", b"REPLICATE", primary.id.as_bytes()],
        )?;
    }
    Ok(())
}

fn wait_for_known_nodes(
    endpoints: &[ClusterEndpoint],
    expected_count: usize,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut all_known = true;
        for endpoint in endpoints {
            let mut connection = Connection::connect_with_defaults(&endpoint.host, endpoint.port)?;
            let response = connection.send_command(&[b"CLUSTER", b"NODES"])?;
            let nodes = bulk_string(response, "CLUSTER NODES")?;
            if nodes.lines().filter(|line| !line.trim().is_empty()).count() != expected_count {
                all_known = false;
                break;
            }
        }
        if all_known {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("cluster nodes did not converge".to_string());
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_cluster_ok(endpoints: &[ClusterEndpoint]) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut all_ok = true;
        for endpoint in endpoints {
            let mut connection = Connection::connect_with_defaults(&endpoint.host, endpoint.port)?;
            let response = connection.send_command(&[b"CLUSTER", b"INFO"])?;
            let info = bulk_string(response, "CLUSTER INFO")?;
            if !info.lines().any(|line| line == "cluster_state:ok") {
                all_ok = false;
                break;
            }
        }
        if all_ok {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("cluster did not reach ok state".to_string());
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn send_ok(host: &str, port: u16, command: &[&[u8]]) -> Result<(), String> {
    let mut connection = Connection::connect_with_defaults(host, port)?;
    super::expect_ok(connection.send_command(command)?)
        .map_err(|error| format!("{}:{} {}", host, port, error))
}

fn bulk_string(response: RespValue, command: &str) -> Result<String, String> {
    match response {
        RespValue::Bulk(bytes) => Ok(String::from_utf8_lossy(&bytes).into_owned()),
        RespValue::Error(error) => Err(error),
        other => Err(format!("unexpected {} response: {:?}", command, other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create_nodes_and_replica_count() {
        let args = vec![
            "--cluster-yes".to_string(),
            "--cluster".to_string(),
            "create".to_string(),
            "127.0.0.1:7000".to_string(),
            "localhost:7001".to_string(),
            "127.0.0.1:7002".to_string(),
            "--cluster-replicas".to_string(),
            "2".to_string(),
        ];

        let command = parse(&args, 2).unwrap();

        assert_eq!(
            command.nodes,
            vec![
                ("127.0.0.1".to_string(), 7000),
                ("localhost".to_string(), 7001),
                ("127.0.0.1".to_string(), 7002),
            ]
        );
        assert_eq!(command.replicas, 2);
    }

    #[test]
    fn plans_even_primary_slots_and_replica_owners() {
        assert_eq!(
            plan_layout(3, 0).unwrap(),
            vec![
                NodeLayout {
                    primary_index: 0,
                    slot_range: Some((0, 5460)),
                },
                NodeLayout {
                    primary_index: 1,
                    slot_range: Some((5461, 10922)),
                },
                NodeLayout {
                    primary_index: 2,
                    slot_range: Some((10923, 16383)),
                },
            ]
        );
        assert_eq!(
            plan_layout(3, 2).unwrap(),
            vec![
                NodeLayout {
                    primary_index: 0,
                    slot_range: Some((0, 16383)),
                },
                NodeLayout {
                    primary_index: 0,
                    slot_range: None,
                },
                NodeLayout {
                    primary_index: 0,
                    slot_range: None,
                },
            ]
        );
    }

    #[test]
    fn builds_addslots_command_with_expanded_slots() {
        let command = build_addslots_command(2, 4);

        assert_eq!(
            command,
            vec![
                b"CLUSTER".to_vec(),
                b"ADDSLOTS".to_vec(),
                b"2".to_vec(),
                b"3".to_vec(),
                b"4".to_vec(),
            ]
        );
    }
}
