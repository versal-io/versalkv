use super::*;
use crate::engine::distributed::sentinel::config::{
    SentinelConfigScriptKind, SentinelConfigScriptValidator,
};

struct AcceptScripts;

impl SentinelConfigScriptValidator for AcceptScripts {
    fn validate_script(&self, _kind: SentinelConfigScriptKind, _path: &str) -> Result<(), String> {
        Ok(())
    }
}

struct RejectScripts;

impl SentinelConfigScriptValidator for RejectScripts {
    fn validate_script(&self, kind: SentinelConfigScriptKind, _path: &str) -> Result<(), String> {
        Err(format!("rejected {kind:?}"))
    }
}

trait TestConfigLoader {
    fn load_test_config(&mut self, directives: &[String]) -> Result<(), String>;
}

impl TestConfigLoader for SentinelState {
    fn load_test_config(&mut self, directives: &[String]) -> Result<(), String> {
        self.load_config_directives(directives, &AcceptScripts)
    }
}

fn config_fingerprint(state: &SentinelState) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{:?}",
        state.myid,
        state.current_epoch,
        state.deny_scripts_reconfig,
        state.sentinel_auth_user,
        state.sentinel_auth_pass,
        state.announce_ip,
        state.announce_port,
        state.resolve_hostnames,
        state.announce_hostnames,
        state.next_instance_id,
        state.registry,
    )
}

fn assert_batch_rolls_back(state: &mut SentinelState, directives: &[&str], expected: &str) {
    assert_batch_rolls_back_with(state, directives, expected, &AcceptScripts);
}

fn assert_batch_rolls_back_with(
    state: &mut SentinelState,
    directives: &[&str],
    expected: &str,
    validator: &dyn SentinelConfigScriptValidator,
) {
    let before = config_fingerprint(state);
    let directives: Vec<String> = directives
        .iter()
        .map(|directive| directive.to_string())
        .collect();

    assert_eq!(
        state.load_config_directives(&directives, validator),
        Err(expected.into())
    );
    assert_eq!(config_fingerprint(state), before);
}

#[test]
fn rejected_script_capability_preserves_error_order_and_rolls_back() {
    let mut state = SentinelState::new();
    assert_batch_rolls_back_with(
        &mut state,
        &["notification-script missing /bin/true"],
        "No such master with specified name.",
        &RejectScripts,
    );
    assert_batch_rolls_back_with(
        &mut state,
        &[
            "sentinel-user changed",
            "monitor cache 127.0.0.1 6379 1",
            "notification-script cache /bin/true",
        ],
        "rejected Notification",
        &RejectScripts,
    );
}

#[test]
fn config_loader_decodes_rewritten_credentials() {
    let mut state = SentinelState::new();
    state
        .load_test_config(&[
            "monitor mymaster 127.0.0.1 6379 1".to_string(),
            "auth-user mymaster \"sentinel-user\"".to_string(),
            "auth-pass mymaster \"my\\\"pass\\\\word\"".to_string(),
        ])
        .unwrap();

    let master = &state.registry.master("mymaster").unwrap();
    assert_eq!(master.auth_user, "sentinel-user");
    assert_eq!(master.auth_pass, "my\"pass\\word");
}

#[test]
fn config_loader_applies_sentinel_peer_auth() {
    let mut state = SentinelState::new();
    state
        .load_test_config(&[
            "monitor mymaster 127.0.0.1 6379 1".to_string(),
            "sentinel-user sentpeer".to_string(),
            "sentinel-pass secr\\\"et".to_string(),
        ])
        .unwrap();
    assert_eq!(state.sentinel_auth_user, "sentpeer");
    assert_eq!(state.sentinel_auth_pass, "secr\"et");
}

#[test]
fn configured_master_has_one_shared_registry_entity() {
    let mut state = SentinelState::new();
    state
        .load_test_config(&["monitor cache 127.0.0.1 6379 1".to_string()])
        .unwrap();

    let master_id = state.registry.master("cache").unwrap().id;
    state.registry.master_mut("cache").unwrap().down_after_ms = 12_345;

    assert_eq!(state.registry.instances()[&master_id].down_after_ms, 12_345);
}

#[test]
fn invalid_config_batch_does_not_publish_pre_monitor_changes() {
    let mut state = SentinelState::new();
    let original_id = state.myid.clone();

    let error = state
        .load_test_config(&[
            format!("myid {}", "a".repeat(40)),
            "monitor cache 127.0.0.1 -1 1".to_string(),
        ])
        .unwrap_err();

    assert_eq!(error, "Invalid port number.");
    assert_eq!(state.myid, original_id);
    assert_eq!(state.registry.master_count(), 0);
}

#[test]
fn duplicate_monitors_in_one_batch_are_rejected_atomically() {
    let mut state = SentinelState::new();
    assert_batch_rolls_back(
        &mut state,
        &[
            "sentinel-user changed",
            "monitor cache 127.0.0.1 6379 1",
            "monitor cache 127.0.0.1 6380 2",
        ],
        "Duplicate master name.",
    );
}

#[test]
fn monitor_duplicate_of_existing_master_is_rejected_atomically() {
    let mut state = SentinelState::new();
    state
        .load_test_config(&["monitor cache 127.0.0.1 6379 1".to_string()])
        .unwrap();

    assert_batch_rolls_back(
        &mut state,
        &["announce-ip 192.0.2.1", "monitor cache 127.0.0.1 6380 2"],
        "Duplicate master name.",
    );
}

#[test]
fn every_master_scoped_directive_rejects_an_unknown_master_atomically() {
    let directives = [
        "down-after-milliseconds missing 1000",
        "failover-timeout missing 1000",
        "parallel-syncs missing 1",
        "primary-reboot-down-after-period missing 0",
        "master-reboot-down-after-period missing 0",
        "config-epoch missing 1",
        "leader-epoch missing 1",
        "notification-script missing /bin/true",
        "client-reconfig-script missing /bin/true",
        "auth-pass missing secret",
        "auth-user missing sentinel",
        "rename-command missing CONFIG CONFIG2",
        "known-replica missing 127.0.0.1 6380",
        "known-slave missing 127.0.0.1 6380",
        "known-sentinel missing 127.0.0.1 26380 peer-a",
    ];

    for directive in directives {
        let mut state = SentinelState::new();
        state
            .load_test_config(&["monitor cache 127.0.0.1 6379 1".to_string()])
            .unwrap();
        assert_batch_rolls_back(
            &mut state,
            &["sentinel-pass changed", directive],
            "No such master with specified name.",
        );
    }
}

#[test]
fn unknown_master_error_precedes_directive_specific_constraint_errors() {
    for directive in [
        "down-after-milliseconds missing 0",
        "failover-timeout missing -1",
        "primary-reboot-down-after-period missing -1",
        "known-replica missing 127.0.0.1 -1",
        "known-sentinel missing 127.0.0.1 65536 peer-a",
    ] {
        let mut state = SentinelState::new();
        assert_batch_rolls_back(
            &mut state,
            &[directive],
            "No such master with specified name.",
        );
    }
}

#[test]
fn invalid_monitor_and_post_monitor_constraints_roll_back_the_batch() {
    let cases: &[(&str, &str)] = &[
        ("monitor cache 127.0.0.1 -1 1", "Invalid port number."),
        ("monitor cache 127.0.0.1 65536 1", "Invalid port number."),
        (
            "monitor cache 127.0.0.1 6379 0",
            "Quorum must be 1 or greater.",
        ),
        (
            "down-after-milliseconds existing 0",
            "negative or zero time parameter.",
        ),
        (
            "failover-timeout existing -1",
            "negative or zero time parameter.",
        ),
        (
            "primary-reboot-down-after-period existing -1",
            "negative time parameter.",
        ),
        (
            "known-replica existing 127.0.0.1 -1",
            "Invalid port number.",
        ),
        (
            "known-sentinel existing 127.0.0.1 65536 peer-a",
            "Invalid port number.",
        ),
        (
            "deny-scripts-reconfig maybe",
            "Please specify yes or no for the deny-scripts-reconfig options.",
        ),
        (
            "resolve-hostnames maybe",
            "Please specify yes or no for the resolve-hostnames option.",
        ),
        (
            "announce-hostnames maybe",
            "Please specify yes or no for the announce-hostnames option.",
        ),
    ];

    for (directive, expected) in cases {
        let mut state = SentinelState::new();
        assert_batch_rolls_back(
            &mut state,
            &[
                "sentinel-user changed",
                "monitor existing 127.0.0.1 6379 1",
                directive,
            ],
            expected,
        );
    }
}

#[test]
fn duplicate_rename_command_rolls_back_the_batch() {
    let mut state = SentinelState::new();
    assert_batch_rolls_back(
        &mut state,
        &[
            "monitor cache 127.0.0.1 6379 1",
            "rename-command cache CONFIG CONFIG2",
            "rename-command cache config CONFIG3",
        ],
        "Same command renamed multiple times with rename-command.",
    );
}

#[test]
fn duplicate_related_instances_roll_back_the_batch() {
    let cases = [
        (
            vec![
                "monitor cache 127.0.0.1 6379 1",
                "known-replica cache 127.0.0.1 6380",
                "known-replica cache 127.0.0.1 6380",
            ],
            "Duplicate hostname and port for replica.",
        ),
        (
            vec![
                "monitor cache 127.0.0.1 6379 1",
                "known-sentinel cache 127.0.0.1 26380 peer-a",
                "known-sentinel cache 127.0.0.2 26381 peer-a",
            ],
            "Duplicate runid for sentinel.",
        ),
    ];

    for (directives, expected) in cases {
        let mut state = SentinelState::new();
        assert_batch_rolls_back(&mut state, &directives, expected);
    }
}

#[test]
fn c_atoi_compatible_values_are_not_rejected_more_strictly() {
    let mut state = SentinelState::new();
    state
        .load_test_config(&[
            "monitor cache 127.0.0.1 not-a-number 1".to_string(),
            "parallel-syncs cache -2".to_string(),
            "known-replica cache 127.0.0.1 not-a-number".to_string(),
        ])
        .unwrap();

    let master = state.registry.master("cache").unwrap();
    assert_eq!(master.addr, "127.0.0.1:0");
    assert_eq!(master.parallel_syncs, -2);
    assert_eq!(master.replicas.len(), 1);
    assert_eq!(
        state.registry.instance(master.replicas[0]).unwrap().addr,
        "127.0.0.1:0"
    );
}

#[test]
fn unexpected_draft_insert_failure_does_not_publish_any_change() {
    let mut state = SentinelState::new();
    let occupied = SentinelInstance::new_master(1, "peer", "127.0.0.1:26380", 1);
    state.registry.insert_instance(occupied).unwrap();

    assert_batch_rolls_back(
        &mut state,
        &["sentinel-user changed", "monitor cache 127.0.0.1 6379 1"],
        "Duplicate instance id.",
    );
}

#[test]
fn monitor_stage_reports_earlier_duplicate_before_later_malformed_monitor() {
    let mut state = SentinelState::new();
    assert_batch_rolls_back(
        &mut state,
        &[
            "monitor cache 127.0.0.1 6379 1",
            "monitor cache 127.0.0.1 6380 1",
            "monitor malformed",
        ],
        "Duplicate master name.",
    );
}

#[test]
fn pre_monitor_error_precedes_earlier_unknown_post_directive() {
    let mut state = SentinelState::new();
    assert_batch_rolls_back(
        &mut state,
        &["not-a-sentinel-directive value", "resolve-hostnames maybe"],
        "Please specify yes or no for the resolve-hostnames option.",
    );
}

#[test]
fn invalid_monitor_address_precedes_duplicate_master_name() {
    let mut state = SentinelState::new();
    state
        .load_test_config(&["monitor cache 127.0.0.1 6379 1".to_string()])
        .unwrap();

    assert_batch_rolls_back(
        &mut state,
        &["monitor cache invalid-host-name 6380 1"],
        "Can't resolve instance hostname.",
    );
}

#[test]
fn non_c_config_directives_are_rejected_atomically() {
    for directive in ["down-after-millis cache 1000", "leader cache peer-a"] {
        let mut state = SentinelState::new();
        assert_batch_rolls_back(
            &mut state,
            &["monitor cache 127.0.0.1 6379 1", directive],
            "Unrecognized sentinel configuration statement.",
        );
    }
}

#[test]
fn ipv6_literals_are_normalized_for_storage_and_duplicate_detection() {
    let mut state = SentinelState::new();
    assert_batch_rolls_back(
        &mut state,
        &[
            "monitor cache 0:0:0:0:0:0:0:1 6379 1",
            "known-replica cache 0:0:0:0:0:0:0:1 6380",
            "known-replica cache ::1 6380",
        ],
        "Duplicate hostname and port for replica.",
    );

    state
        .load_test_config(&[
            "monitor cache 0:0:0:0:0:0:0:1 6379 1".to_string(),
            "known-replica cache ::1 6380".to_string(),
        ])
        .unwrap();
    let master = state.registry.master("cache").unwrap();
    assert_eq!(master.addr, "[::1]:6379");
    assert_eq!(
        state.registry.instance(master.replicas[0]).unwrap().addr,
        "[::1]:6380"
    );
}

#[test]
fn resolve_hostnames_preserves_unresolved_hostname_addresses() {
    let mut state = SentinelState::new();
    state
        .load_test_config(&[
            "resolve-hostnames yes".to_string(),
            "monitor cache redis.internal 6379 1".to_string(),
            "known-replica cache replica.internal 6380".to_string(),
        ])
        .unwrap();

    let master = state.registry.master("cache").unwrap();
    assert_eq!(master.addr, "redis.internal:6379");
    assert_eq!(
        state.registry.instance(master.replicas[0]).unwrap().addr,
        "replica.internal:6380"
    );
}

#[test]
fn insert_master_rejects_duplicate_name_without_orphaning_either_instance() {
    let mut registry = SentinelRegistry::default();
    let original = SentinelInstance::new_master(1, "cache", "127.0.0.1:6379", 1);
    let duplicate = SentinelInstance::new_master(2, "cache", "127.0.0.1:6380", 1);
    registry
        .insert_master("cache".to_string(), original)
        .unwrap();

    assert_eq!(
        registry
            .insert_master("cache".to_string(), duplicate)
            .unwrap_err()
            .to_string(),
        "Duplicate master name."
    );
    assert_eq!(registry.master("cache").unwrap().id, 1);
    assert!(registry.instances().get(&2).is_none());
}

#[test]
fn insert_master_rejects_duplicate_id_without_creating_a_name_index() {
    let mut registry = SentinelRegistry::default();
    let original = SentinelInstance::new_master(1, "cache", "127.0.0.1:6379", 1);
    let duplicate_id = SentinelInstance::new_master(1, "other", "127.0.0.1:6380", 1);
    registry
        .insert_master("cache".to_string(), original)
        .unwrap();

    assert_eq!(
        registry
            .insert_master("other".to_string(), duplicate_id)
            .unwrap_err()
            .to_string(),
        "Duplicate instance id."
    );
    assert!(!registry.contains_master("other"));
    assert_eq!(registry.instances()[&1].name, "cache");
}

#[test]
fn tilt_enters_on_large_delta() {
    let mut state = SentinelState::new();
    state.previous_time = 1_000;
    state.check_tilt_condition(5_000);
    assert!(state.tilt);
    assert!(state.tilt_start_time > 0);
}

#[test]
fn tilt_does_not_enter_on_normal_delta() {
    let mut state = SentinelState::new();
    state.previous_time = 1_000;
    state.check_tilt_condition(1_100);
    assert!(!state.tilt);
}

#[test]
fn tilt_enters_on_negative_delta() {
    let mut state = SentinelState::new();
    state.previous_time = 5_000;
    state.check_tilt_condition(1_000);
    assert!(state.tilt);
}

#[test]
fn tilt_exits_after_period() {
    let mut state = SentinelState::new();
    state.tunables.tilt_period_ms = 1_000;
    state.previous_time = 1_000;
    state.check_tilt_condition(5_000);
    assert!(state.tilt, "should be in tilt after large delta");
    state.previous_time = 5_000;
    state.check_tilt_condition(5_500);
    assert!(state.tilt, "should still be in tilt");
    state.previous_time = 5_500;
    state.check_tilt_condition(10_500);
    assert!(!state.tilt, "should exit tilt after period");
}
