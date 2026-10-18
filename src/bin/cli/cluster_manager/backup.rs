use super::super::{Connection, ReplicationRdbTransfer, RespValue};
use std::fs::File;
use std::io::Write;
use std::path::Path;

/// Implement `--cluster backup <host:port> <dir>` by saving each primary's
/// RDB to `<dir>` and writing a nodes.json summary. Mirrors
/// `clusterManagerCommandBackup` in `thirdparty/valkey/src/valkey-cli.c`.
pub(super) fn execute(args: &[String], command_index: usize) -> Result<(), String> {
    let seed = args
        .get(command_index + 1)
        .ok_or_else(|| "missing cluster seed address".to_string())?;
    let backup_dir = args
        .get(command_index + 2)
        .ok_or_else(|| "missing backup directory".to_string())?;
    let dir = Path::new(backup_dir);
    if !dir.is_dir() {
        return Err(format!("[ERR] {} is not a valid directory", backup_dir));
    }
    let (host, port) = super::parse_address(seed)?;
    let mut connection = Connection::connect_with_defaults(&host, port)?;
    let response = connection.send_command(&[b"CLUSTER", b"NODES"])?;
    let nodes_text = match response {
        RespValue::Bulk(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        RespValue::Error(error) => return Err(error),
        other => return Err(format!("unexpected CLUSTER NODES response: {:?}", other)),
    };
    let nodes = super::parse_cluster_nodes(&nodes_text)?;

    let mut json = String::from("[\n");
    let mut first = true;
    for node in &nodes {
        if !first {
            json.push_str(",\n");
        }
        first = false;
        json.push_str(&node_json(node));
        if !node.is_primary {
            continue;
        }
        eprintln!(">>> Node {}:{} -> Saving RDB...", node.host, node.port);
        let rdb_path = dir.join(format!("redis-node-{}-{}.rdb", node.host, node.port));
        save_rdb(&node.host, node.port, &rdb_path)?;
    }
    json.push_str("\n]");
    let json_path = dir.join("nodes.json");
    eprintln!("Saving cluster configuration to: {}", json_path.display());
    let mut file =
        File::create(&json_path).map_err(|error| format!("Could not save nodes: {}", error))?;
    file.write_all(json.as_bytes())
        .map_err(|error| format!("Could not save nodes: {}", error))?;
    eprintln!("[OK] Backup created into: {}", backup_dir);
    Ok(())
}

fn node_json(node: &super::ClusterNode) -> String {
    let role = if node.is_primary { "master" } else { "slave" };
    format!(
        "{{\"name\":\"{}\",\"host\":\"{}\",\"port\":{},\"role\":\"{}\",\"slots\":{}}}",
        node.id,
        node.host,
        node.port,
        role,
        node.slots.len(),
    )
}

fn save_rdb(host: &str, port: u16, out_path: &Path) -> Result<(), String> {
    let mut connection = Connection::connect_with_defaults(host, port)?;
    let _ = connection.send_command(&[b"REPLCONF", b"listening-port", b"0"]);
    let _ = connection.send_command(&[b"REPLCONF", b"capa", b"eof"]);
    if let Err(error) = connection.send_command_without_reply(&[b"SYNC"]) {
        return Err(format!("SYNC {}:{} failed: {}", host, port, error));
    }
    let mut file = File::create(out_path)
        .map_err(|error| format!("Could not open {:?}: {}", out_path, error))?;
    match connection.read_replication_rdb(&mut file) {
        Ok(ReplicationRdbTransfer::FullEof) | Ok(ReplicationRdbTransfer::FullLength) => Ok(()),
        Ok(ReplicationRdbTransfer::Partial) => Err(format!(
            "primary {}:{} responded with partial resync",
            host, port
        )),
        Err(error) => Err(format!(
            "RDB transfer from {}:{} failed: {}",
            host, port, error
        )),
    }
}
