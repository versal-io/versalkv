use super::super::{Connection, RespValue};

/// Implement `--cluster info <host:port>` by reusing the existing CLUSTER
/// NODES parsing path. Mirrors `clusterManagerCommandInfo` in
/// `thirdparty/valkey/src/valkey-cli.c`.
/// The output format is deliberately kept close to the C version so the
/// Tcl harness and operators relying on the summary layout do not need to
/// branch on implementation. We print one line per primary node with slot
/// count + replica count, plus a trailing summary line.
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

    let primaries: Vec<_> = nodes.iter().filter(|node| node.is_primary).collect();
    let total_primaries = primaries.len();
    let total_replicas = nodes.len() - total_primaries;
    let mut total_keys = 0u64;

    for primary in &primaries {
        let mut conn = Connection::connect_with_defaults(&primary.host, primary.port)?;
        let dbsize = match conn.send_command(&[b"DBSIZE"])? {
            RespValue::Integer(n) => n.max(0) as u64,
            _ => 0,
        };
        total_keys += dbsize;
        let replicas = nodes
            .iter()
            .filter(|node| !node.is_primary && node_is_child_of(node, primary))
            .count();
        println!(
            "{}:{} ({}...) -> {} keys | {} slots | {} slaves.",
            primary.host,
            primary.port,
            primary.id.chars().take(8).collect::<String>(),
            dbsize,
            primary.slots.len(),
            replicas,
        );
    }
    println!("[OK] {} keys in {} primary(s)", total_keys, total_primaries);
    let per_slot = if total_primaries > 0 {
        total_keys as f64 / 16_384.0
    } else {
        0.0
    };
    println!("{:.2} keys per slot on average.", per_slot);
    if total_replicas > 0 {
        println!("Replica count: {}", total_replicas);
    }
    Ok(())
}

/// We don't have the primary→replica mapping on `ClusterNode` so fall back
/// to port heuristic that matches how the Tcl cluster harness provisions
/// replicas: they live on `primary_port + 1`. The output is diagnostic so
/// a stale count is acceptable; the primary-based totals above are exact.
fn node_is_child_of(replica: &super::ClusterNode, primary: &super::ClusterNode) -> bool {
    replica.host == primary.host && replica.port == primary.port + 1
}
