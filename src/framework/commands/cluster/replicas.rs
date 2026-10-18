use crate::engine::distributed::cluster::node::ClusterNode;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::CommandError;
use bytes::Bytes;

fn render_replica_line(replica: &ClusterNode) -> String {
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_FAIL, CLUSTER_NODE_PFAIL};

    let mut flags = vec!["slave"];
    if replica.has_flag(CLUSTER_NODE_PFAIL) {
        flags.push("fail?");
    }
    if replica.has_flag(CLUSTER_NODE_FAIL) {
        flags.push("fail");
    }
    format!(
        "{} {}:{}@{} {} {} {} {} {} connected",
        replica.name,
        replica.ip,
        replica.port,
        replica.cport,
        flags.join(","),
        replica.replicaof,
        replica.ping_sent,
        replica.pong_received,
        replica.config_epoch,
    )
}

pub(super) fn cluster_replicas(args: &[Bytes], ctx: &ServerContext) -> Value {
    if args.len() != 1 {
        return CommandError::wrong_arity("cluster");
    }
    let node_id = match std::str::from_utf8(&args[0]) {
        Ok(node_id) => node_id,
        Err(_) => return Value::error("ERR Invalid node id"),
    };
    let state = match ctx.cluster_state().as_deref() {
        Some(state) => state,
        None => return Value::error("ERR This instance has cluster support disabled"),
    };
    match state.get_replicas(node_id) {
        Some(replicas) => Value::Array(
            replicas
                .into_iter()
                .map(|replica| Value::bulk(render_replica_line(replica)))
                .collect(),
        ),
        None => Value::error(format!("ERR Unknown node {}", node_id)),
    }
}

#[cfg(test)]
mod tests {
    use super::render_replica_line;
    use crate::engine::distributed::cluster::node::ClusterNode;

    #[test]
    fn replica_description_has_slave_as_third_field() {
        let mut replica = ClusterNode::new(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            false,
        );
        replica.replicaof = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string();

        let line = render_replica_line(&replica);
        let fields: Vec<&str> = line.split_whitespace().collect();

        assert_eq!(fields[2], "slave");
        assert_eq!(fields[3], replica.replicaof);
    }
}
