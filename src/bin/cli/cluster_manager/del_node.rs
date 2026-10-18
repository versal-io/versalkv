use super::super::{Connection, RespValue};

#[derive(Debug, PartialEq, Eq)]
struct DelNodeCommand {
    seed_host: String,
    seed_port: u16,
    node_id: String,
}

fn parse(args: &[String], command_index: usize) -> Result<DelNodeCommand, String> {
    let seed = args
        .get(command_index + 1)
        .ok_or_else(|| "missing cluster seed address".to_string())?;
    let node_id = args
        .get(command_index + 2)
        .ok_or_else(|| "missing node id".to_string())?;
    let (seed_host, seed_port) = super::parse_address(seed)?;
    Ok(DelNodeCommand {
        seed_host,
        seed_port,
        node_id: node_id.clone(),
    })
}

pub(super) fn execute(args: &[String], command_index: usize) -> Result<(), String> {
    let command = parse(args, command_index)?;
    let mut seed = Connection::connect_with_defaults(&command.seed_host, command.seed_port)?;
    let response = seed.send_command(&[b"CLUSTER", b"NODES"])?;
    let nodes_text = match response {
        RespValue::Bulk(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        RespValue::Error(error) => return Err(error),
        other => return Err(format!("unexpected CLUSTER NODES response: {:?}", other)),
    };
    let nodes = super::parse_cluster_nodes(&nodes_text)?;
    let target = nodes
        .iter()
        .find(|node| node.id == command.node_id)
        .ok_or_else(|| format!("unknown node '{}'", command.node_id))?;
    if !target.slots.is_empty() {
        return Err("node is not empty".to_string());
    }
    let target_host = target.host.clone();
    let target_port = target.port;
    let target_is_primary = target.is_primary;
    let target_connection = Connection::connect_with_defaults(&target_host, target_port);
    let visible_slot_count = nodes
        .iter()
        .filter(|node| node.id != command.node_id)
        .flat_map(|node| node.slots.iter().copied())
        .collect::<std::collections::HashSet<_>>()
        .len();
    if target_is_primary && target_connection.is_err() && visible_slot_count < 16_384 {
        return Err("node is not empty".to_string());
    }

    for node in nodes.iter().filter(|node| node.id != command.node_id) {
        let Ok(mut connection) = Connection::connect_with_defaults(&node.host, node.port) else {
            continue;
        };
        match connection.send_command(&[b"CLUSTER", b"FORGET", command.node_id.as_bytes()]) {
            Ok(RespValue::SimpleString(value)) if value == "OK" => {}
            Ok(RespValue::Error(error)) if error.contains("Unknown node") => {}
            Ok(response) => {
                return Err(format!(
                    "{}:{} unexpected CLUSTER FORGET response: {:?}",
                    node.host, node.port, response
                ))
            }
            Err(error) => return Err(error),
        }
    }

    let reset_result = target_connection.and_then(|mut connection| {
        super::expect_ok(connection.send_command(&[b"CLUSTER", b"RESET", b"SOFT"])?)
    });
    let success = format!("[OK] Node {} removed from the cluster", command.node_id);
    match reset_result {
        Ok(()) => {
            println!(
                "Sending CLUSTER RESET SOFT to {}:{}",
                target_host, target_port
            );
            println!("{}", success);
            Ok(())
        }
        Err(error) => Err(format!(
            "unable to send CLUSTER RESET SOFT to {}:{}: {}\n{}",
            target_host, target_port, error, success
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_seed_and_deleted_node_id() {
        let args = vec![
            "--cluster".to_string(),
            "del-node".to_string(),
            "127.0.0.1:7000".to_string(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        ];

        assert_eq!(
            parse(&args, 1).unwrap(),
            DelNodeCommand {
                seed_host: "127.0.0.1".to_string(),
                seed_port: 7000,
                node_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            }
        );
    }
}
