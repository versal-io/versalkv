use super::super::{Connection, RespValue};

#[derive(Debug, PartialEq, Eq)]
struct CallCommand {
    seed_host: String,
    seed_port: u16,
    command: Vec<String>,
}

fn parse(args: &[String], command_index: usize) -> Result<CallCommand, String> {
    let seed = args
        .get(command_index + 1)
        .ok_or_else(|| "missing cluster seed address".to_string())?;
    let (seed_host, seed_port) = super::parse_address(seed)?;
    let command = args[(command_index + 2)..].to_vec();
    if command.is_empty() {
        return Err("cluster call requires a command".to_string());
    }
    Ok(CallCommand {
        seed_host,
        seed_port,
        command,
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
    let command_parts: Vec<&[u8]> = command.command.iter().map(|part| part.as_bytes()).collect();
    for node in nodes {
        let mut connection = Connection::connect_with_defaults(&node.host, node.port)?;
        if let RespValue::Error(error) = connection.send_command(&command_parts)? {
            return Err(error);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_call_seed_and_command_arguments() {
        let args = vec![
            "--cluster".to_string(),
            "call".to_string(),
            "127.0.0.1:7000".to_string(),
            "FUNCTION".to_string(),
            "LOAD".to_string(),
            "#!lua name=TEST\nreturn 1".to_string(),
        ];

        assert_eq!(
            parse(&args, 1).unwrap(),
            CallCommand {
                seed_host: "127.0.0.1".to_string(),
                seed_port: 7000,
                command: vec![
                    "FUNCTION".to_string(),
                    "LOAD".to_string(),
                    "#!lua name=TEST\nreturn 1".to_string(),
                ],
            }
        );
    }
}
