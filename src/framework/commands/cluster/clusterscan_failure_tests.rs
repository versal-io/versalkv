use super::*;
use crate::engine::distributed::cluster::node::{
    ClusterNode, CLUSTER_NODE_FAIL, CLUSTER_NODE_MYSELF,
};
use crate::engine::distributed::cluster::ClusterState;
use crate::engine::command::CommandArgs;
use crate::framework::capabilities::connection::Connection;
use crate::framework::capabilities::io::Token;
use bytes::Bytes;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn args(parts: &[&'static [u8]]) -> CommandArgs {
    CommandArgs::from_parts(parts.iter().map(|p| Bytes::from_static(p)).collect())
}

fn conn() -> Connection {
    Connection::new(
        Token(1),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 12345),
        false,
    )
}

#[test]
fn clusterscan_failed_declared_owner_redirects_without_full_coverage() {
    const FAILED_OWNER: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut ctx = ServerContext::new_with_slot_router(1, 14, |key| {
        crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
    });
    ctx.config_mut().server.cluster_enabled = true;
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let mut failed_owner = ClusterNode::new(
        FAILED_OWNER.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    failed_owner.flags &= !CLUSTER_NODE_MYSELF;
    failed_owner.flags |= CLUSTER_NODE_FAIL;
    failed_owner.slots[0] = true;
    failed_owner.numslots = 1;
    state.nodes.insert(failed_owner.name.clone(), failed_owner);

    state.set_require_full_coverage(false);
    *ctx.cluster_state_mut() = Some(Box::new(state));

    let mut conn = conn();
    let unserved = clusterscan(&mut conn, &args(&[b"CLUSTERSCAN", b"0-{06S}-0"]), &mut ctx);
    assert_eq!(unserved, Value::error("MOVED 0 127.0.0.1:6380"));
}

#[test]
fn clusterscan_failed_declared_owner_returns_clusterdown_with_full_coverage() {
    const FAILED_OWNER: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut ctx = ServerContext::new_with_slot_router(1, 14, |key| {
        crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
    });
    ctx.config_mut().server.cluster_enabled = true;
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let mut failed_owner = ClusterNode::new(
        FAILED_OWNER.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    failed_owner.flags &= !CLUSTER_NODE_MYSELF;
    failed_owner.flags |= CLUSTER_NODE_FAIL;
    failed_owner.slots[0] = true;
    failed_owner.numslots = 1;
    state.nodes.insert(failed_owner.name.clone(), failed_owner);
    *ctx.cluster_state_mut() = Some(Box::new(state));

    let mut conn = conn();
    let reply = clusterscan(&mut conn, &args(&[b"CLUSTERSCAN", b"0-{06S}-0"]), &mut ctx);
    assert_eq!(reply, Value::error("CLUSTERDOWN The cluster is down"));
}
