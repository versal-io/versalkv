use super::super::{Connection, RespValue};

#[derive(Debug, PartialEq, Eq)]
struct AddNodeCommand {
    new_host: String,
    new_port: u16,
    seed_host: String,
    seed_port: u16,
}

fn parse(args: &[String], command_index: usize) -> Result<AddNodeCommand, String> {
    let new_node = args
        .get(command_index + 1)
        .ok_or_else(|| "missing new node address".to_string())?;
    let seed = args
        .get(command_index + 2)
        .ok_or_else(|| "missing cluster seed address".to_string())?;
    let (new_host, new_port) = super::parse_address(new_node)?;
    let (seed_host, seed_port) = super::parse_address(seed)?;
    Ok(AddNodeCommand {
        new_host,
        new_port,
        seed_host,
        seed_port,
    })
}

pub(super) fn execute(args: &[String], command_index: usize) -> Result<(), String> {
    let command = parse(args, command_index)?;
    let new_endpoint = super::create::read_self_endpoint(&command.new_host, command.new_port)?;
    let seed_endpoint = super::create::read_self_endpoint(&command.seed_host, command.seed_port)?;

    let mut new_node = Connection::connect_with_defaults(&command.new_host, command.new_port)?;
    match new_node.send_command(&[b"FUNCTION", b"LIST"])? {
        RespValue::Array(items) if items.is_empty() => {}
        RespValue::Array(_) => return Err("node already contains functions".to_string()),
        RespValue::Error(error) => return Err(error),
        other => return Err(format!("unexpected FUNCTION LIST response: {:?}", other)),
    }

    let mut seed = Connection::connect_with_defaults(&command.seed_host, command.seed_port)?;
    let function_dump = match seed.send_command(&[b"FUNCTION", b"DUMP"])? {
        RespValue::Bulk(bytes) => bytes,
        RespValue::Error(error) => return Err(error),
        other => return Err(format!("unexpected FUNCTION DUMP response: {:?}", other)),
    };
    super::expect_ok(new_node.send_command(&[
        b"FUNCTION",
        b"RESTORE",
        function_dump.as_slice(),
    ])?)?;

    let seed_port = seed_endpoint.port.to_string();
    let seed_bus_port = seed_endpoint.bus_port.to_string();
    super::expect_ok(new_node.send_command(&[
        b"CLUSTER",
        b"MEET",
        seed_endpoint.host.as_bytes(),
        seed_port.as_bytes(),
        seed_bus_port.as_bytes(),
    ])?)?;
    let _ = new_endpoint;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_new_node_and_seed_addresses() {
        let args = vec![
            "--cluster".to_string(),
            "add-node".to_string(),
            "127.0.0.1:7003".to_string(),
            "localhost:7000".to_string(),
        ];

        assert_eq!(
            parse(&args, 1).unwrap(),
            AddNodeCommand {
                new_host: "127.0.0.1".to_string(),
                new_port: 7003,
                seed_host: "localhost".to_string(),
                seed_port: 7000,
            }
        );
    }
}
