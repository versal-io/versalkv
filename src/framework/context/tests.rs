use super::memory::MaxmemoryState;
use super::*;
use crate::engine::command::{CMD_SKIP_COMMANDLOG, CMD_SKIP_SLOWLOG};
use crate::engine::distributed::replication::ReplicationOffset;
use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Mutex;

static LATENCY_TEST_LOCK: Mutex<()> = Mutex::new(());

fn offset(value: i64) -> ReplicationOffset {
    ReplicationOffset::try_from(value).expect("valid test offset")
}

#[test]
fn maxmemory_state_uses_logical_memory_after_exclusions() {
    let state = MaxmemoryState::from_parts(1_000, 600, 250);

    assert_eq!(state.reported, 1_000);
    assert_eq!(state.not_counted, 250);
    assert_eq!(state.logical, 750);
    assert_eq!(state.to_free, 150);
    assert!(state.is_over_limit());
    assert_eq!(state.headroom(), 0);
    assert!(state.would_exceed(1));
}

#[test]
fn maxmemory_state_saturates_exclusions_and_disables_zero_limit() {
    let saturated = MaxmemoryState::from_parts(100, 50, 150);
    assert_eq!(saturated.logical, 0);
    assert_eq!(saturated.to_free, 0);
    assert!(!saturated.is_over_limit());
    assert_eq!(saturated.headroom(), 50);
    assert!(!saturated.would_exceed(50));
    assert!(saturated.would_exceed(51));

    let unlimited = MaxmemoryState::from_parts(1_000, 0, 0);
    assert!(!unlimited.is_over_limit());
    assert_eq!(unlimited.to_free, 0);
    assert_eq!(unlimited.headroom(), usize::MAX);
    assert!(!unlimited.would_exceed(usize::MAX));
}

#[test]
fn normal_client_memory_delta_handles_growth_shrink_and_removal() {
    let mut ctx = ServerContext::new(1);

    ctx.adjust_normal_client_memory(0, 4096);
    assert_eq!(ctx.normal_client_memory(), 4096);
    ctx.adjust_normal_client_memory(4096, 1024);
    assert_eq!(ctx.normal_client_memory(), 1024);
    ctx.adjust_normal_client_memory(1024, 0);
    assert_eq!(ctx.normal_client_memory(), 0);
}

#[test]
fn server_context_excludes_replication_and_slot_export_buffers() {
    let mut ctx = ServerContext::new(1);
    ctx.config_mut().server.maxmemory = 1;
    ctx.config_mut().server.repl_backlog_size = 1_024;
    ctx.replication_mut().backlog.set_capacity(8 * 1_024);
    ctx.replication_mut()
        .backlog
        .append(&vec![b'x'; 4_096])
        .unwrap();
    ctx.replication_mut().replica_output_buffer_bytes = 4_096;
    ctx.replication_mut().replica_output_buffer_allocation_bytes = 8_192;
    ctx.set_slot_export_output_memory(512);
    let backlog_allocation = ctx.replication().backlog.allocation_bytes();

    let state = ctx.maxmemory_state();

    assert_eq!(state.not_counted, backlog_allocation + 8_192 - 1_024 + 512);
    assert_eq!(
        state.logical,
        state.reported.saturating_sub(state.not_counted)
    );
}

#[test]
fn server_context_excludes_active_aof_buffer_allocation() {
    let path = std::env::temp_dir().join(format!(
        "versal-maxmemory-aof-{}-{}.aof",
        std::process::id(),
        crate::foundation::io::sys::time::ustime()
    ));
    let appender = crate::foundation::io::persistence::AofAppender::open_append_path(
        &path,
        crate::foundation::io::persistence::AofFsync::No,
    )
    .unwrap();
    let mut ctx = ServerContext::new(1);
    ctx.config_mut().persistence.aof_enabled = true;
    ctx.aof_mut().journal =
        Some(crate::foundation::io::persistence::journal::AofJournal::new(appender));

    let state = ctx.maxmemory_state();

    assert!(state.not_counted >= 8 * 1024);
    let _ = std::fs::remove_file(path);
}

#[test]
fn server_context_synchronizes_lfu_policy_into_databases() {
    let mut ctx = ServerContext::new(2);
    ctx.config_mut().server.maxmemory_policy = "allkeys-lfu".to_string();

    ctx.sync_eviction_policy();
    ctx.db_mut(1).unwrap().set(
        b"lfu".to_vec(),
        crate::foundation::io::store::Robj::string("v"),
    );

    assert_eq!(ctx.db(1).unwrap().get_lfu_freq(b"lfu"), 5);
}

#[test]
fn latency_history_is_bounded_under_repeated_samples() {
    let mut lat = LatencyState::new();
    lat.set_threshold(1);

    for _ in 0..65_536 {
        lat.add_sample_at("command", 1, 1_000);
    }

    let history = lat.history("command");
    assert!(
        history.len() <= 160,
        "latency history must retain at most 160 slots, got {}",
        history.len()
    );
    let latest = lat.latest();
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].5, 65_536);
}

#[test]
fn latency_samples_in_the_same_second_keep_maximum_and_lifetime_totals() {
    let mut lat = LatencyState::new();
    lat.set_threshold(1);

    lat.add_sample_at("command", 12, 1_000);
    lat.add_sample_at("command", 20, 1_000);
    lat.add_sample_at("command", 15, 1_000);

    assert_eq!(lat.history("command"), vec![(1_000, 20)]);
    assert_eq!(
        lat.latest(),
        vec![("command".to_string(), 1_000, 20, 20, 47, 3)]
    );
    assert!(lat
        .graph("command")
        .contains("command - high 20 ms, low 20 ms (all time high 20 ms)"));
}

#[test]
fn latency_history_overwrites_oldest_slot_without_losing_lifetime_maximum() {
    let mut lat = LatencyState::new();
    lat.set_threshold(1);
    lat.add_sample_at("command", 200, 1_000);
    for latency_ms in 1..=160 {
        lat.add_sample_at("command", latency_ms, 1_000 + latency_ms);
    }

    let expected: Vec<_> = (1..=160)
        .map(|latency_ms| (1_000 + latency_ms, latency_ms))
        .collect();
    assert_eq!(lat.history("command"), expected);
    assert_eq!(
        lat.latest(),
        vec![("command".to_string(), 1_160, 160, 200, 13_080, 161)]
    );
    assert!(lat
        .graph("command")
        .contains("command - high 160 ms, low 1 ms (all time high 200 ms)"));
}

#[test]
fn latency_same_second_merge_at_full_capacity_does_not_evict_a_slot() {
    let mut lat = LatencyState::new();
    lat.set_threshold(1);
    for timestamp_sec in 1_000..1_160 {
        lat.add_sample_at("command", 1, timestamp_sec);
    }
    lat.add_sample_at("command", 12, 1_159);

    let history = lat.history("command");
    assert_eq!(history.len(), 160);
    assert_eq!(history.first(), Some(&(1_000, 1)));
    assert_eq!(history.last(), Some(&(1_159, 12)));
    assert_eq!(
        lat.latest(),
        vec![("command".to_string(), 1_159, 12, 12, 172, 161)]
    );

    lat.add_sample_at("command", 2, 1_160);
    let history = lat.history("command");
    assert_eq!(history.len(), 160);
    assert_eq!(history.first(), Some(&(1_001, 1)));
    assert_eq!(history.last(), Some(&(1_160, 2)));
    assert_eq!(
        lat.latest(),
        vec![("command".to_string(), 1_160, 2, 12, 174, 162)]
    );
}

#[test]
fn latency_history_preserves_insertion_order_when_time_moves_backwards() {
    let mut lat = LatencyState::new();
    lat.set_threshold(1);
    lat.add_sample_at("command", 12, 1_002);
    lat.add_sample_at("command", 20, 1_000);
    lat.add_sample_at("command", 15, 1_002);

    assert_eq!(
        lat.history("command"),
        vec![(1_002, 12), (1_000, 20), (1_002, 15)]
    );
    assert_eq!(
        lat.latest(),
        vec![("command".to_string(), 1_002, 15, 20, 47, 3)]
    );
}

#[test]
fn latency_threshold_filters_samples_without_discarding_existing_history() {
    let mut lat = LatencyState::new();
    lat.add_sample_at("command", 12, 1_000);
    assert!(lat.latest().is_empty());

    lat.set_threshold(10);
    lat.add_sample_at("command", 9, 1_000);
    assert!(lat.latest().is_empty());
    lat.add_sample_at("command", 10, 1_000);
    lat.set_threshold(0);
    lat.add_sample_at("command", 30, 1_001);
    lat.set_threshold(-1);
    lat.add_sample_at("command", 40, 1_001);
    assert_eq!(lat.history("command"), vec![(1_000, 10)]);

    lat.set_threshold(10);
    lat.add_sample_at("command", 20, 1_001);
    assert_eq!(lat.history("command"), vec![(1_000, 10), (1_001, 20)]);
    assert_eq!(
        lat.latest(),
        vec![("command".to_string(), 1_001, 20, 20, 30, 2)]
    );
}

#[test]
fn latency_reset_discards_history_and_lifetime_totals_but_keeps_threshold() {
    let mut lat = LatencyState::new();
    lat.set_threshold(1);
    lat.add_sample_at("command", 200, 1_000);
    for timestamp_sec in 1_001..=1_160 {
        lat.add_sample_at("command", 1, timestamp_sec);
    }
    lat.add_sample_at("other", 3, 1_160);

    assert_eq!(lat.reset(Some("missing")), 0);
    assert_eq!(lat.reset(Some("command")), 1);
    assert_eq!(lat.reset(Some("command")), 0);
    assert!(lat.history("command").is_empty());
    assert_eq!(lat.graph("command"), "No latency data for this event");
    assert_eq!(lat.history("other"), vec![(1_160, 3)]);
    assert_eq!(lat.latest(), vec![("other".to_string(), 1_160, 3, 3, 3, 1)]);

    lat.add_sample_at("command", 2, 2_000);
    let latest = lat.latest();
    let command = latest.iter().find(|sample| sample.0 == "command").unwrap();
    assert_eq!(command, &("command".to_string(), 2_000, 2, 2, 2, 1));
    assert_eq!(lat.reset(None), 2);
    assert_eq!(lat.reset(None), 0);
    assert!(lat.latest().is_empty());
    assert!(lat.history("other").is_empty());

    lat.add_sample_at("command", 1, 3_000);
    assert_eq!(
        lat.latest(),
        vec![("command".to_string(), 3_000, 1, 1, 1, 1)]
    );
}

#[test]
fn latency_threshold_latest_history_graph_and_reset_work_together() {
    let _guard = LATENCY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mut lat = LatencyState::new();

    lat.set_threshold(10);

    lat.add_sample("fast", 5);
    assert!(lat.latest().is_empty());
    lat.add_sample_at("slow", 12, 1_000);
    lat.add_sample_at("slow", 20, 1_001);

    let latest = lat.latest();
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].0, "slow");
    assert_eq!(latest[0].2, 20);
    assert_eq!(latest[0].3, 20);
    assert_eq!(latest[0].4, 32);
    assert_eq!(latest[0].5, 2);
    assert_eq!(lat.history("slow"), vec![(1_000, 12), (1_001, 20)]);
    assert!(lat.history("missing").is_empty());
    let graph = lat.graph("slow");
    assert!(graph.contains("slow - high 20 ms, low 12 ms"));
    assert_eq!(lat.graph("missing"), "No latency data for this event");

    assert_eq!(lat.reset(Some("slow")), 1);
    assert_eq!(lat.reset(Some("slow")), 0);
    lat.add_sample("a", 11);
    lat.add_sample("b", 12);
    assert_eq!(lat.reset(None), 2);
    assert!(lat.latest().is_empty());
    assert!(lat.doctor().contains("No latency data"));
    lat.set_threshold(0);
}

#[test]
fn client_pause_state_reports_active_action_only_before_deadline() {
    let paused = ClientPauseState {
        action: PauseAction::Write,
        end_time_ms: 100,
        reason: "client_pause",
    };
    let indefinite = ClientPauseState {
        action: PauseAction::All,
        end_time_ms: 0,
        reason: "none",
    };

    assert_eq!(paused.is_paused(50), PauseAction::Write);
    assert_eq!(paused.is_paused(100), PauseAction::None);
    assert_eq!(paused.is_paused(150), PauseAction::None);
    assert_eq!(indefinite.is_paused(0), PauseAction::None);
}

#[test]
fn paused_clients_preserve_postponement_order_after_removal() {
    let mut ctx = ServerContext::new(1);

    ctx.paused_clients_mut().insert(Token(30));
    ctx.paused_clients_mut().insert(Token(10));
    ctx.paused_clients_mut().insert(Token(20));
    ctx.paused_clients_mut().shift_remove(&Token(10));
    ctx.paused_clients_mut().insert(Token(40));

    assert_eq!(
        ctx.paused_clients().iter().copied().collect::<Vec<_>>(),
        vec![Token(30), Token(20), Token(40)]
    );
}

#[test]
fn server_context_initializes_domain_services() {
    let mut ctx = ServerContext::new(3);

    assert_eq!(ctx.num_dbs(), 3);
    assert!(ctx.db(0).is_some());
    assert!(ctx.db_mut(1).is_some());
    assert_eq!(ctx.config().server.maxclients, 10000);
    assert!(ctx.dbs().get(2).is_some());
    ctx.dbs_mut().get_mut(0).unwrap().set(
        b"k".to_vec(),
        crate::foundation::io::store::Robj::string("v"),
    );
    assert_eq!(
        ctx.db(0).unwrap().get_no_expire(b"k").unwrap().as_bytes(),
        b"v"
    );
    assert_eq!(ctx.script.command_registry.len(), 0);
    assert!(ctx.cluster_state.is_none());
    assert_eq!(ctx.client_pause.is_paused(0), PauseAction::None);
}

#[test]
fn rdb_replication_info_persists_inclusive_last_applied_offset() {
    let mut ctx = ServerContext::new(1);
    ctx.replication_mut().role = crate::engine::distributed::replication::ReplicationRole::Replica;
    ctx.replication_mut().slave_repl_offset = offset(405);
    let info = ctx.rdb_replication_info();
    assert_eq!(info.repl_offset, 405);
    assert!(info.aux_fields.is_empty());
}

#[test]
fn full_sync_replication_info_includes_cluster_open_slots_only_when_present() {
    const TARGET: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut ctx = ServerContext::new(1);
    let mut cluster =
        crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16_379);
    cluster.seed_migrating_slot(7, TARGET);
    *ctx.cluster_state_mut() = Some(Box::new(cluster));

    let info = ctx.rdb_full_sync_replication_info();

    assert_eq!(
        info.aux_fields,
        vec![(
            b"cluster-slot-states".to_vec(),
            b"7>bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,".to_vec(),
        )]
    );
}

#[test]
fn rdb_slot_import_aux_replaces_stale_tracking_import_migrations() {
    use crate::engine::distributed::cluster::migration::{
        MigrationJob, MigrationJobId, SlotRanges,
    };
    use crate::engine::distributed::cluster::{NodeId, SlotRange};
    use crate::framework::distributed::migration::{SlotImportMetadata, SLOT_IMPORT_AUX_KEY};

    const OLD_JOB_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const RESTORED_JOB_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const SOURCE_ID: &str = "cccccccccccccccccccccccccccccccccccccccc";
    const TARGET_ID: &str = "dddddddddddddddddddddddddddddddddddddddd";

    let ranges = SlotRanges::new(vec![SlotRange::new(16_300, 16_300).unwrap()]).unwrap();
    let source = NodeId::parse(SOURCE_ID.as_bytes()).unwrap();
    let target = NodeId::parse(TARGET_ID.as_bytes()).unwrap();
    let mut ctx = ServerContext::new(1);
    let mut cluster =
        crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6_379, 16_379);
    let old_id = MigrationJobId::parse(OLD_JOB_ID.as_bytes()).unwrap();
    cluster
        .migrations
        .start(MigrationJob::new_tracking_import(
            old_id.clone(),
            source.clone(),
            target.clone(),
            ranges.clone(),
            1,
        ))
        .unwrap();
    *ctx.cluster_state_mut() = Some(Box::new(cluster));

    let restored_id = MigrationJobId::parse(RESTORED_JOB_ID.as_bytes()).unwrap();
    let metadata = SlotImportMetadata {
        id: restored_id.clone(),
        source,
        target,
        ranges,
    };
    ctx.replace_slot_imports_from_rdb_aux(&[(SLOT_IMPORT_AUX_KEY.to_vec(), metadata.encode())])
        .unwrap();

    let migrations = &ctx.cluster_state.as_ref().unwrap().migrations;
    assert!(migrations.get(&old_id).is_none());
    assert!(migrations.get(&restored_id).is_some());
    assert_eq!(migrations.views().len(), 1);
}

#[test]
fn flush_all_dbs_resets_replication_selected_database() {
    let mut ctx = ServerContext::new(10);
    ctx.set_repl_seldb(9);

    ctx.flush_all_dbs();

    assert_eq!(ctx.repl_seldb(), -1);
}

#[test]
fn push_cmd_logs_obeys_skip_flags_and_slow_only_path() {
    let mut ctx = ServerContext::new(1);
    ctx.slowlog.reconfigure(0, 10);
    ctx.commandlog.reconfigure_all(0, 10, 0, 10, 0, 10);
    let addr: SocketAddr = ([127, 0, 0, 1], 6379).into();
    let argv = vec![Bytes::from_static(b"SET"), Bytes::from_static(b"k")];

    ctx.push_cmd_logs(
        argv.clone(),
        10,
        &addr,
        Some(Bytes::from_static(b"client")),
        20,
        30,
        0,
    );
    assert_eq!(ctx.slowlog.len(), 1);
    assert_eq!(ctx.commandlog.slow.len(), 1);
    assert_eq!(ctx.commandlog.large_request.len(), 1);
    assert_eq!(ctx.commandlog.large_reply.len(), 1);

    ctx.push_cmd_logs(
        argv.clone(),
        10,
        &addr,
        None,
        20,
        30,
        CMD_SKIP_SLOWLOG | CMD_SKIP_COMMANDLOG,
    );
    assert_eq!(ctx.slowlog.len(), 1);
    assert_eq!(ctx.commandlog.slow.len(), 1);

    ctx.push_cmd_log_slow_only(argv, 11, &addr, None);
    assert_eq!(ctx.slowlog.len(), 2);
    assert_eq!(ctx.commandlog.slow.len(), 2);
    assert_eq!(ctx.commandlog.large_request.len(), 1);
}
