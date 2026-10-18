use super::{
    debug_allowed_action, debug_cluster_link, debug_cluster_packet_filter,
    debug_set_disable_deny_scripts, debug_slot_migration,
};
use super::introspection::debug_delay_rdb_client_free_seconds;
use super::load_aof::prepare_debug_loadaof;
use crate::engine::distributed::cluster::msg::ClusterMsgType;
use crate::engine::distributed::cluster::node::ClusterNode;
use crate::engine::distributed::cluster::{ClusterLinkDirection, ClusterState};
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::CommandArgs;
use bytes::Bytes;
use crate::framework::capabilities::connection::Connection;

fn args(values: &[&str]) -> CommandArgs {
    CommandArgs {
        name: Bytes::from_static(b"debug"),
        args: values
            .iter()
            .map(|value| Bytes::copy_from_slice(value.as_bytes()))
            .collect(),
    }
}

#[test]
fn debug_gate_honors_no_yes_and_local_modes() {
    use crate::engine::extension::config::server::ProtectedAction;

    assert!(!debug_allowed_action(ProtectedAction::No, true));
    assert!(debug_allowed_action(ProtectedAction::Yes, false));
    assert!(debug_allowed_action(ProtectedAction::Local, true));
    assert!(!debug_allowed_action(ProtectedAction::Local, false));
}

#[test]
fn debug_loadaof_clears_preexisting_function_libraries() {
    let mut ctx = ServerContext::new(1);
    ctx.config_mut().persistence.dir = std::env::temp_dir()
        .join(format!("pkv-debug-loadaof-{}", std::process::id()))
        .to_string_lossy()
        .into_owned();
    ctx.config_mut().persistence.aof_appenddirname.clear();
    ctx.config_mut().persistence.aof_filename = "missing.aof".to_string();
    ctx.script_mut()
        .function_registry
        .load_library(
            "before_loadaof",
            "#!lua name=before_loadaof\nserver.register_function('before_loadaof_fn', function() return 1 end)",
        )
        .expect("test function library must compile");

    assert_eq!(ctx.script_mut().function_registry.library_count(), 1);
    assert!(matches!(
        prepare_debug_loadaof(&mut ctx),
        Ok(crate::framework::host_tasks::PreparedHostTask::DebugAof(_))
    ));
    assert_eq!(ctx.script_mut().function_registry.library_count(), 0);
}

#[test]
fn debug_disable_deny_scripts_defaults_false_and_toggles() {
    let mut ctx = ServerContext::new(1);

    assert!(!ctx.script_disable_deny_scripts());
    assert_eq!(
        debug_set_disable_deny_scripts(
            &args(&["set-disable-deny-scripts", "1"]),
            &mut ctx
        ),
        Value::ok()
    );
    assert!(ctx.script_disable_deny_scripts());
    assert_eq!(
        debug_set_disable_deny_scripts(
            &args(&["set-disable-deny-scripts", "0"]),
            &mut ctx
        ),
        Value::ok()
    );
    assert!(!ctx.script_disable_deny_scripts());
}

#[test]
fn debug_replicate_queues_a_raw_command_frame() {
    let mut conn = Connection::new_fake();
    let reply = super::debug_replicate(
        &mut conn,
        &args(&["replicate", "fake-command-1"]),
    );

    assert_eq!(reply, Value::ok());
    assert_eq!(
        conn.custom_propagation(),
        vec![crate::engine::distributed::replication::protocol::encode_command(&[
            b"fake-command-1",
        ])]
    );
}

#[test]
fn debug_replicate_requires_exactly_one_command() {
    let mut conn = Connection::new_fake();

    assert_eq!(
        super::debug_replicate(&mut conn, &args(&["replicate"])),
        crate::framework::CommandError::syntax_error()
    );
    assert_eq!(
        super::debug_replicate(&mut conn, &args(&["replicate", "", "extra"])),
        crate::framework::CommandError::syntax_error()
    );
}

#[test]
fn debug_disable_deny_scripts_validates_arity_and_integer() {
    let mut ctx = ServerContext::new(1);

    assert_eq!(
        debug_set_disable_deny_scripts(&args(&["set-disable-deny-scripts"]), &mut ctx),
        crate::framework::CommandError::syntax_error()
    );
    for value in ["yes", "-1", "2"] {
        assert_eq!(
            debug_set_disable_deny_scripts(
                &args(&["set-disable-deny-scripts", value]),
                &mut ctx
            ),
            Value::error("ERR value is not an integer or out of range")
        );
    }
    assert!(!ctx.script_disable_deny_scripts());
}

#[test]
fn debug_delay_rdb_client_free_seconds_updates_server_config() {
    let mut ctx = ServerContext::new(1);

    assert_eq!(
        debug_delay_rdb_client_free_seconds(
            &args(&["delay-rdb-client-free-seconds", "2"]),
            &mut ctx,
        ),
        Value::ok()
    );
    assert_eq!(ctx.config().server.debug_delay_rdb_client_free_seconds, 2);
}

#[test]
fn debug_change_repl_id_replaces_primary_id_and_clears_secondary_id() {
    let mut ctx = ServerContext::new(1);
    ctx.config_mut().server.enable_debug_command =
        crate::engine::extension::config::server::ProtectedAction::Yes;
    let original = ctx.replication().backlog.replid().to_string();
    ctx.replication_mut().backlog.shift_replication_id();
    assert!(!ctx.replication().backlog.second_replid().is_empty());
    let shifted = ctx.replication().backlog.replid().to_string();
    let mut conn = Connection::new_fake();

    assert_eq!(
        super::debug_cmd(&mut conn, &args(&["change-repl-id"]), &mut ctx),
        Value::ok()
    );
    assert_ne!(ctx.replication().backlog.replid(), original);
    assert_ne!(ctx.replication().backlog.replid(), shifted);
    assert!(ctx.replication().backlog.second_replid().is_empty());
}

#[test]
fn debug_restart_defers_process_replacement_without_replying() {
    use crate::engine::command::context::CommandConn;

    let mut ctx = ServerContext::new(1);
    let mut conn = Connection::new_fake();

    assert_eq!(
        super::danger::debug_restart(
            &mut conn,
            &args(&["restart", "25"]),
            &mut ctx,
            true,
        ),
        Value::Null
    );
    assert!(conn.custom_reply());
    assert_eq!(
        ctx.take_server_restart_request(),
        Some(crate::framework::context::RestartIntent {
            delay_ms: 25,
            graceful: true,
            config_rewrite: true,
        })
    );
}

#[test]
fn debug_drop_cluster_packet_filter_updates_cluster_state() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

    let reply =
        debug_cluster_packet_filter(&args(&["drop-cluster-packet-filter", "1"]), &mut state);

    assert_eq!(reply, Value::ok());
    assert!(state.should_drop_packet(ClusterMsgType::Pong));
}

#[test]
fn debug_cluster_packet_filter_accepts_negative_values_and_rejects_non_integer() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

    assert_eq!(
        debug_cluster_packet_filter(&args(&["drop-cluster-packet-filter", "-2"]), &mut state,),
        Value::ok()
    );
    assert!(state.should_drop_packet(ClusterMsgType::Meet));

    let reply =
        debug_cluster_packet_filter(&args(&["drop-cluster-packet-filter", "nope"]), &mut state);
    assert_eq!(
        reply,
        Value::error("ERR value is not an integer or out of range")
    );
}

#[test]
fn debug_slot_migration_toggles_instance_local_guards() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);

    assert_eq!(
        debug_slot_migration(&args(&["slotmigration", "prevent-pause", "1"]), &mut state),
        Value::ok()
    );
    assert!(state.debug_prevent_slot_migration_pause());
    assert!(!state.debug_prevent_slot_migration_failover());

    assert_eq!(
        debug_slot_migration(
            &args(&["slotmigration", "prevent-failover", "1"]),
            &mut state,
        ),
        Value::ok()
    );
    assert!(state.debug_prevent_slot_migration_failover());

    assert_eq!(
        debug_slot_migration(&args(&["slotmigration", "prevent-pause", "0"]), &mut state),
        Value::ok()
    );
    assert!(!state.debug_prevent_slot_migration_pause());
}

#[test]
fn debug_slot_migration_rejects_unknown_mode_and_non_boolean_value() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    assert_eq!(
        debug_slot_migration(&args(&["slotmigration", "unknown", "1"]), &mut state),
        crate::framework::CommandError::syntax_error()
    );
    assert_eq!(
        debug_slot_migration(&args(&["slotmigration", "prevent-pause", "yes"]), &mut state),
        Value::error("ERR value is not an integer or out of range")
    );
}

#[test]
fn debug_clusterlink_kill_queues_requested_direction() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    state.nodes.insert(
        node_id.to_string(),
        ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        ),
    );
    state.observe_link_open(crate::engine::distributed::cluster::ClusterLinkView::new(
        crate::engine::distributed::cluster::NodeId::parse(node_id.as_bytes()).unwrap(),
        ClusterLinkDirection::To,
        1,
        crate::engine::distributed::cluster::LinkEvents::READABLE,
    ));

    let reply = debug_cluster_link(&args(&["clusterlink", "kill", "all", node_id]), &mut state);

    assert_eq!(reply, Value::ok());
    assert_eq!(
        state.pending_link_closes,
        vec![(node_id.to_string(), ClusterLinkDirection::All)]
    );
    assert!(state.link_views().next().is_none());
}

#[test]
fn debug_clusterlink_kill_rejects_unknown_node_and_myself() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let myself = state.myself.name.clone();

    assert_eq!(
        debug_cluster_link(
            &args(&[
                "clusterlink",
                "kill",
                "to",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ]),
            &mut state,
        ),
        Value::error("ERR Unknown node aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    );
    assert_eq!(
        debug_cluster_link(&args(&["clusterlink", "kill", "from", &myself]), &mut state,),
        Value::error("ERR Cannot free cluster link(s) to myself")
    );
}
