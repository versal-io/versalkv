use super::control_set::{
    invalid_set_argument, reset_instance_health_after_auth_change, set_down_after_ms,
    sync_master_config,
};
use super::{
    ensure_failover_candidate, failover_in_progress_error, is_primary_down, parse_failover_request,
    master_name_for_addr, parse_down_request, remove_monitored_master, validate_no_control_chars,
};
use crate::engine::command::CommandArgs;
use crate::engine::distributed::sentinel::{
    DownState, InstanceRole, SentinelInstance, SentinelState,
};
use crate::foundation::io::protocol::Value;
use bytes::Bytes;

#[test]
fn repeated_failover_uses_in_progress_error_code() {
    assert_eq!(
        failover_in_progress_error(),
        Value::error("INPROG Failover already in progress")
    );
}

#[test]
fn primary_down_reply_uses_master_state_when_instance_mirror_is_stale() {
    assert!(is_primary_down(
        DownState::SDown,
        DownState::Up,
        1_000,
        0,
        10_000,
        0,
        10_100,
    ));
    assert!(is_primary_down(
        DownState::Up,
        DownState::Up,
        1_000,
        10_000,
        9_900,
        0,
        11_001,
    ));
    assert!(!is_primary_down(
        DownState::Up,
        DownState::Up,
        1_000,
        0,
        10_000,
        0,
        11_001,
    ));
}

#[test]
fn coordinated_failover_option_follows_primary_name() {
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SENTINEL"),
        Bytes::from_static(b"FAILOVER"),
        Bytes::from_static(b"mymaster"),
        Bytes::from_static(b"COORDINATED"),
    ]);
    assert_eq!(
        parse_failover_request(&args),
        Ok(("mymaster".to_string(), true))
    );
}

#[test]
fn ipv6_vote_request_is_normalized_to_internal_endpoint() {
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SENTINEL"),
        Bytes::from_static(b"IS-MASTER-DOWN-BY-ADDR"),
        Bytes::from_static(b"::1"),
        Bytes::from_static(b"6379"),
        Bytes::from_static(b"4"),
        Bytes::from_static(b"peer-a"),
    ]);

    let request = parse_down_request(&args).unwrap();
    assert_eq!(request.addr, "[::1]:6379");
}

#[test]
fn down_by_addr_matches_wildcard_primary_address_by_resolved_endpoint() {
    let mut ctx = crate::framework::context::ServerContext::new(1);
    let master = SentinelInstance::new_master(1, "primary", "0.0.0.0:6379", 1);
    ctx.sentinel_state_mut()
        .registry
        .insert_master(master.name.clone(), master)
        .unwrap();

    assert_eq!(
        master_name_for_addr(&ctx, "127.0.0.1:6379").as_deref(),
        Some("primary")
    );
}

#[test]
fn manual_failover_requires_a_healthy_replica_before_starting() {
    let master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
    let mut instances = std::collections::HashMap::new();
    assert_eq!(
        ensure_failover_candidate(&master, &instances, false, 10_000),
        Err(Value::error("NOGOODSLAVE No suitable replica to promote"))
    );

    let mut master = master;
    master.replicas.push(2);
    let mut replica = SentinelInstance::new_master(2, "replica", "127.0.0.1:6380", 1);
    replica.role = InstanceRole::Replica;
    replica.role_reported = InstanceRole::Replica;
    replica.last_ping_ok = 10_000;
    replica.last_info_time = 4_999;
    instances.insert(replica.id, replica);

    assert_eq!(
        ensure_failover_candidate(&master, &instances, false, 10_000),
        Err(Value::error("NOGOODSLAVE No suitable replica to promote"))
    );

    instances.get_mut(&2).unwrap().last_info_time = 10_000;
    assert_eq!(
        ensure_failover_candidate(&master, &instances, true, 10_000),
        Err(Value::error("NOGOODSLAVE No suitable replica to promote"))
    );
    assert_eq!(
        ensure_failover_candidate(&master, &instances, false, 10_000),
        Ok(())
    );
}

#[test]
fn removing_monitored_master_removes_its_runtime_instance() {
    let mut state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
    master.replicas.push(2);
    let mut replica = SentinelInstance::new_master(2, "replica", "127.0.0.1:6380", 1);
    replica.role = InstanceRole::Replica;
    state.registry.insert_instance(replica).unwrap();
    state
        .registry
        .insert_master(master.name.clone(), master)
        .unwrap();

    assert_eq!(remove_monitored_master(&mut state, "primary"), Ok(()));
    assert!(state.registry.instances().is_empty());
}

#[test]
fn sentinel_arguments_reject_all_protocol_control_characters() {
    for value in [
        "has\rcarriage",
        "has\nnewline",
        "has\0null",
        "has\ttab",
        "has\u{7f}del",
    ] {
        assert!(
            !validate_no_control_chars(value),
            "control character must be rejected in {value:?}"
        );
    }
}

#[test]
fn invalid_control_character_argument_is_not_echoed_in_error() {
    assert_eq!(
        invalid_set_argument("user\ninjected", "auth-user"),
        Value::error("ERR Invalid argument: control characters not allowed")
    );
}

#[test]
fn invalid_set_arguments_use_valkey_compatible_error_format() {
    assert_eq!(
        invalid_set_argument("-20", "down-after-milliseconds"),
        Value::error("ERR Invalid argument '-20' for SENTINEL SET 'down-after-milliseconds'")
    );
    assert_eq!(
        invalid_set_argument("abc", "down-after-milliseconds"),
        Value::error("ERR Invalid argument 'abc' for SENTINEL SET 'down-after-milliseconds'")
    );
}

#[test]
fn syncing_master_config_preserves_runtime_health_state() {
    let mut configured = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
    configured.down_after_ms = 1_000;
    configured.auth_user = "sentinel-user".to_string();
    configured.auth_pass = "sentinel-pass".to_string();

    let mut runtime = configured.clone();
    runtime.down_after_ms = 30_000;
    runtime.auth_user.clear();
    runtime.auth_pass.clear();
    runtime.last_ping_ok = 10_000;
    runtime.last_ping_sent = 10_100;
    runtime.act_ping_time = 10_100;
    runtime.last_info_time = 9_900;
    runtime.info_cache = "# Replication\r\nrole:master\r\n".to_string();

    sync_master_config(&configured, &mut runtime);

    assert_eq!(runtime.down_after_ms, 1_000);
    assert_eq!(runtime.auth_user, "sentinel-user");
    assert_eq!(runtime.auth_pass, "sentinel-pass");
    assert_eq!(runtime.last_ping_ok, 10_000);
    assert_eq!(runtime.last_ping_sent, 10_100);
    assert_eq!(runtime.act_ping_time, 10_100);
    assert_eq!(runtime.last_info_time, 9_900);
    assert_eq!(runtime.info_cache, "# Replication\r\nrole:master\r\n");
}

#[test]
fn down_after_set_propagates_to_existing_replicas() {
    let mut state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
    master.replicas.push(2);
    state
        .registry
        .insert_master(master.name.clone(), master)
        .unwrap();
    let mut replica = SentinelInstance::new_master(2, "replica", "127.0.0.1:6380", 2);
    replica.role = InstanceRole::Replica;
    state.registry.insert_instance(replica).unwrap();

    set_down_after_ms(&mut state, "primary", 2_000);

    assert_eq!(state.registry.master("primary").unwrap().down_after_ms, 2_000);
    assert_eq!(state.registry.instances()[&1].down_after_ms, 2_000);
    assert_eq!(state.registry.instances()[&2].down_after_ms, 2_000);
}

#[test]
fn auth_change_clears_stale_health_before_link_rebuild() {
    let mut state = SentinelState::new();
    let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 2);
    master.replicas.push(2);
    state
        .registry
        .insert_master(master.name.clone(), master)
        .unwrap();
    let mut replica = SentinelInstance::new_master(2, "replica", "127.0.0.1:6380", 2);
    replica.role = InstanceRole::Replica;
    state.registry.insert_instance(replica).unwrap();
    for instance in state.registry.values_mut() {
        instance.act_ping_time = 1_000;
        instance.link_down_since_time = 1_000;
        instance.down_state = DownState::SDown;
        instance.sdown_since_time = 1_000;
        instance.sdown_time = 1_000;
        state.pending_link_close.push(instance.addr.clone());
    }

    reset_instance_health_after_auth_change(&mut state, "primary");

    assert!(state.pending_link_close.is_empty());
    for instance in state.registry.instances().values() {
        assert_eq!(instance.act_ping_time, 0);
        assert_eq!(instance.link_down_since_time, 0);
        assert_eq!(instance.down_state, DownState::Up);
        assert_eq!(instance.sdown_since_time, 0);
        assert_eq!(instance.sdown_time, 0);
    }
}
