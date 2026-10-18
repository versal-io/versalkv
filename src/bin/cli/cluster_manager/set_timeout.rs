use super::super::{Connection, RespValue};

/// Implement `--cluster set-timeout <host:port> <ms>` by writing
/// `cluster-node-timeout` on every reachable node. Mirrors
/// `clusterManagerCommandSetTimeout` in `thirdparty/valkey/src/valkey-cli.c`.
pub(super) fn execute(args: &[String], command_index: usize) -> Result<(), String> {
    let seed = args
        .get(command_index + 1)
        .ok_or_else(|| "missing cluster seed address".to_string())?;
    let timeout_str = args
        .get(command_index + 2)
        .ok_or_else(|| "missing cluster-node-timeout value".to_string())?;
    let timeout: i64 = timeout_str
        .parse()
        .map_err(|_| format!("invalid cluster-node-timeout value '{}'", timeout_str))?;
    if timeout < 100 {
        return Err(
            "Setting a node timeout of less than 100 milliseconds is a bad idea.".to_string(),
        );
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

    eprintln!(">>> Reconfiguring node timeout in every cluster node...");
    let timeout_str = timeout.to_string();
    let mut ok_count = 0usize;
    let mut err_count = 0usize;
    for node in &nodes {
        let mut conn = Connection::connect_with_defaults(&node.host, node.port)?;
        let set_reply = conn.send_command(&[
            b"CONFIG",
            b"SET",
            b"cluster-node-timeout",
            timeout_str.as_bytes(),
        ])?;
        if let RespValue::Error(err) = &set_reply {
            eprintln!(
                "ERR setting node-timeout for {}:{}: {}",
                node.host, node.port, err
            );
            err_count += 1;
            continue;
        }
        // CONFIG REWRITE is best-effort - some operators run without a
        // writable config file; the C version logs the failure but keeps
        // the SET as a success. Mirror that behaviour.
        let _ = conn.send_command(&[b"CONFIG", b"REWRITE"]);
        eprintln!("*** New timeout set for {}:{}", node.host, node.port);
        ok_count += 1;
    }
    eprintln!(
        ">>> New node timeout set. {} OK, {} ERR.",
        ok_count, err_count
    );
    Ok(())
}
