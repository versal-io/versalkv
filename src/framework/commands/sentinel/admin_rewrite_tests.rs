use super::{config_repr, rewrite_config};
use crate::engine::distributed::sentinel::{InstanceRole, SentinelInstance, SentinelState};
use std::fs;

#[test]
fn config_repr_matches_sdscatrepr_escaping() {
    assert_eq!(
        config_repr("my\"pass\\word with spaces"),
        "\"my\\\"pass\\\\word with spaces\""
    );
    assert_eq!(config_repr("line\nbreak"), "\"line\\nbreak\"");
}

#[test]
fn rewrite_removes_cleared_credentials_and_deduplicates_known_instances() {
    let path = std::env::temp_dir().join(format!(
        "sentinel-rewrite-{}-{}.conf",
        std::process::id(),
        crate::foundation::io::sys::time::mstime()
    ));
    fs::write(
        &path,
        "sentinel monitor cache 127.0.0.1 6379 2\n\
         sentinel auth-user cache \"old-user\"\n\
         sentinel auth-pass cache \"old-pass\"\n\
         sentinel known-replica cache 127.0.0.1 6380\n\
         sentinel known-replica cache 127.0.0.1 6380\n\
         sentinel known-sentinel cache 127.0.0.1 26380 peer-a\n\
         sentinel known-sentinel cache 127.0.0.1 26380 peer-a\n",
    )
    .unwrap();

    let mut state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "cache", "127.0.0.1:6379", 2);
    master.replicas.push(2);
    master.sentinels.push(3);
    state
        .registry
        .insert_master("cache".to_string(), master)
        .unwrap();
    let mut replica = SentinelInstance::new_master(2, "replica", "127.0.0.1:6380", 2);
    replica.role = InstanceRole::Replica;
    state.registry.insert_instance(replica).unwrap();
    let mut peer = SentinelInstance::new_master(3, "peer", "127.0.0.1:26380", 2);
    peer.role = InstanceRole::Sentinel;
    peer.runid = "peer-a".to_string();
    state.registry.insert_instance(peer).unwrap();

    let rewritten = rewrite_config(&state, path.to_str().unwrap()).unwrap();
    fs::remove_file(path).unwrap();

    assert!(!rewritten.contains("sentinel auth-user cache"));
    assert!(!rewritten.contains("sentinel auth-pass cache"));
    assert_eq!(
        rewritten
            .matches("sentinel known-replica cache 127.0.0.1 6380")
            .count(),
        1
    );
    assert_eq!(
        rewritten
            .matches("sentinel known-sentinel cache 127.0.0.1 26380 peer-a")
            .count(),
        1
    );
}

#[test]
fn rewrite_removes_legacy_leader_directive_without_persisting_runtime_vote() {
    let path = std::env::temp_dir().join(format!(
        "sentinel-leader-rewrite-{}-{}.conf",
        std::process::id(),
        crate::foundation::io::sys::time::mstime()
    ));
    fs::write(
        &path,
        "sentinel monitor cache 127.0.0.1 6379 1\nsentinel leader cache stale-peer\n",
    )
    .unwrap();
    let mut state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "cache", "127.0.0.1:6379", 1);
    master.leader = "runtime-peer".to_string();
    state
        .registry
        .insert_master("cache".to_string(), master)
        .unwrap();

    let rewritten = rewrite_config(&state, path.to_str().unwrap()).unwrap();
    fs::remove_file(path).unwrap();

    assert!(!rewritten.contains("sentinel leader cache"));
    assert!(rewritten.contains("sentinel leader-epoch cache"));
}

#[test]
fn rewrite_renders_bracketed_runtime_ipv6_as_config_host_and_port() {
    let path = std::env::temp_dir().join(format!(
        "sentinel-ipv6-rewrite-{}-{}.conf",
        std::process::id(),
        crate::foundation::io::sys::time::mstime()
    ));
    fs::write(&path, "").unwrap();
    let mut state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "cache", "[::1]:6379", 1);
    master.replicas.push(2);
    state
        .registry
        .insert_master("cache".to_string(), master)
        .unwrap();
    let mut replica = SentinelInstance::new_master(2, "replica", "[::1]:6380", 1);
    replica.role = InstanceRole::Replica;
    state.registry.insert_instance(replica).unwrap();

    let rewritten = rewrite_config(&state, path.to_str().unwrap()).unwrap();
    fs::remove_file(path).unwrap();

    assert!(rewritten.contains("sentinel monitor cache ::1 6379 1"));
    assert!(rewritten.contains("sentinel known-replica cache ::1 6380"));
}
