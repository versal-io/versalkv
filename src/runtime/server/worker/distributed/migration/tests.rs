use crate::engine::distributed::cluster::migration::{
    ExportPhase, MigrationAction, MigrationEvent, MigrationJob, MigrationJobId, MigrationOperation,
    MigrationPhase, SlotMigrationConnectionRole, SlotRanges,
};
use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};
use crate::engine::distributed::cluster::{ClusterState, NodeId, SlotRange};
use crate::framework::capabilities::connection::{Connection, ConnectionState};
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::{ConnectionId, MasterInfo, ReplicaLink};
use crate::runtime::server::worker::ServerWorkerState;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener};

fn slot_export_key(token_value: usize) -> crate::runtime::server::worker::reactor::RunnableKey {
    crate::runtime::server::worker::reactor::RunnableKey::SlotExportWrite(Token(token_value))
}

fn fixture_cluster_bus_port(client_port: u16) -> u16 {
    if client_port <= u16::MAX - 10_000 {
        client_port + 10_000
    } else {
        client_port - 10_000
    }
}

fn setup_export_runtime() -> (ServerWorkerState, std::net::TcpStream, MigrationJobId) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let target_addr = listener.local_addr().unwrap();
    let mut runtime = ServerWorkerState::new_with_slot_router(1, 14, |key| {
        crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
    })
    .unwrap();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let source = NodeId::parse(state.myself.name.as_bytes()).unwrap();
    let target_id = NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
    let mut target = ClusterNode::new(
        target_id.as_str().to_string(),
        target_addr.ip().to_string(),
        target_addr.port(),
        fixture_cluster_bus_port(target_addr.port()),
        true,
    );
    target.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(target.name.clone(), target);
    state.add_slot(1).unwrap();
    let id = MigrationJobId::parse(b"cccccccccccccccccccccccccccccccccccccccc").unwrap();
    let job = MigrationJob::new(
        id.clone(),
        source,
        target_id,
        SlotRanges::new(vec![SlotRange::new(1, 1).unwrap()]).unwrap(),
        MigrationOperation::Export,
        1,
    );
    let actions = state.migrations.start(job).unwrap();
    state.queue_migration_actions(actions);
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));
    runtime.drive_slot_migration_actions();
    let (peer, _) = listener.accept().unwrap();
    (runtime, peer, id)
}

#[test]
fn fixture_cluster_bus_port_is_valid_and_distinct_at_both_boundaries() {
    assert_eq!(fixture_cluster_bus_port(0), 10_000);
    assert_eq!(fixture_cluster_bus_port(u16::MAX - 10_000), u16::MAX);
    assert_eq!(fixture_cluster_bus_port(u16::MAX), u16::MAX - 10_000);
}

fn setup_streaming_export_runtime() -> (ServerWorkerState, std::net::TcpStream, MigrationJobId) {
    let (mut runtime, peer, id) = setup_export_runtime();
    let now = crate::foundation::io::sys::time::unixtime();
    let state = runtime.ctx.cluster_state_mut().as_deref_mut().unwrap();
    for event in [
        MigrationEvent::Connected {
            auth_required: false,
        },
        MigrationEvent::EstablishCommandSent,
        MigrationEvent::Established,
        MigrationEvent::SnapshotStarted,
        MigrationEvent::SnapshotFinished,
    ] {
        state
            .migrations
            .transition(&id, event, now)
            .expect("advance export fixture to streaming");
    }
    (runtime, peer, id)
}

fn setup_registered_import_runtime() -> (ServerWorkerState, std::net::TcpStream, MigrationJobId) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let source_addr = listener.local_addr().unwrap();
    let mut runtime = ServerWorkerState::new(1).unwrap();
    let source = runtime
        .transport
        .connect_tcp(source_addr)
        .expect("connect import transport");
    let (peer, _) = listener.accept().expect("accept import transport");
    runtime.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: source.token,
            peer_addr: source_addr,
        },
    );

    let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
    let id = MigrationJobId::parse(b"dddddddddddddddddddddddddddddddddddddddd").unwrap();
    let job = MigrationJob::new(
        id.clone(),
        NodeId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
        NodeId::parse(state.myself.name.as_bytes()).unwrap(),
        SlotRanges::new(vec![SlotRange::new(1, 1).unwrap()]).unwrap(),
        MigrationOperation::Import,
        1,
    );
    state.migrations.start(job).unwrap();
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));
    let connection_token = source.token.0;
    runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .slot_migration_role = Some(SlotMigrationConnectionRole::Import(id.clone()));
    (runtime, peer, id)
}

fn queue_owned_export_output(
    runtime: &mut ServerWorkerState,
    id: &MigrationJobId,
    token_value: usize,
) {
    let connection_token = token_value;
    let mut ready = Vec::with_capacity(4096);
    ready.extend_from_slice(&[b'r'; 257]);
    runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .queue_replication_reply(ready);
    let queued_before = runtime.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)]
    .output
    .queue[0]
        .len();
    runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .output
        .queue[0]
        .drain(..17);
    assert_eq!(
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .consume_written_bytes(17, queued_before),
        Ok(17)
    );

    let mut deferred = Vec::with_capacity(8192);
    deferred.extend_from_slice(&[b'd'; 129]);
    assert!(runtime
        .queue_slot_export_mutation(id, 0, deferred, true)
        .is_empty());
    runtime.reconcile_slot_export_output_allocation(token_value);
}

fn install_slot_snapshot_source_for_test(
    runtime: &mut ServerWorkerState,
    id: &MigrationJobId,
    read_end: std::fs::File,
) -> crate::runtime::server::worker::reactor::sources::RuntimeSourceHandle {
    runtime
        .install_runtime_source(
            crate::foundation::io::transport::OwnedPollSource::readable(read_end),
            crate::runtime::server::worker::reactor::sources::SourceOwner::InternalPipe(
                crate::runtime::server::worker::reactor::sources::InternalPipeOwner::SlotSnapshot {
                    job_id: id.clone(),
                },
            ),
        )
        .expect("install slot snapshot source")
}

fn trigger_export_output_limit(
    runtime: &mut ServerWorkerState,
    id: &MigrationJobId,
    token_value: usize,
) {
    queue_owned_export_output(runtime, id, token_value);
    runtime
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = 1;
    runtime.ctx.config_mut().server.repl_backlog_size = 0;
    assert!(runtime.enforce_client_output_buffer_limit_for(token_value));
}

#[test]
fn slot_export_limit_cleanup_removes_all_pending_write_state() {
    let (mut runtime, _peer, id) = setup_streaming_export_runtime();
    let token_value = runtime
        .slot_migration_runtime
        .export_connection(&id)
        .expect("streaming export has a registered connection");
    runtime.writes.slot_exports.begin_turn();
    assert!(runtime.queue_slot_migration_output(&id, token_value, b"scheduled-current".to_vec(),));
    runtime.schedule_runtime_runnable_next_turn(slot_export_key(token_value));
    assert_eq!(
        runtime
            .writes
            .slot_exports
            .remaining_budget(Token(token_value), 64 * 1024),
        64 * 1024
    );
    runtime
        .writes
        .slot_exports
        .consume_budget(Token(token_value), 17);
    assert!(runtime
        .event_batch
        .contains_runnable(slot_export_key(token_value)));
    assert!(runtime.writes.slot_exports.was_serviced(Token(token_value)));
    runtime
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = 1;
    runtime.ctx.config_mut().server.repl_backlog_size = 0;

    assert!(runtime.enforce_client_output_buffer_limit_for(token_value));

    assert!(!runtime
        .event_batch
        .contains_runnable(slot_export_key(token_value)));
    assert!(!runtime.writes.slot_exports.was_serviced(Token(token_value)));
    runtime.drain_current_slot_export_writes();
    assert!(!runtime
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(token_value)));
}

#[test]
fn slot_export_limit_cleanup_releases_output_once() {
    let (mut runtime, _peer, id) = setup_streaming_export_runtime();
    let token_value = runtime
        .slot_migration_runtime
        .export_connection(&id)
        .expect("streaming export has a registered connection");
    queue_owned_export_output(&mut runtime, &id, token_value);
    assert!(runtime.writes.slot_export_allocation_bytes > 0);
    assert!(runtime
        .writes
        .slot_export_allocations
        .contains_key(&Token(token_value)));
    assert!(runtime.slot_migration_runtime.deferred_reply_bytes(&id) > 0);
    runtime
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = 1;
    runtime.ctx.config_mut().server.repl_backlog_size = 0;

    assert!(runtime.enforce_client_output_buffer_limit_for(token_value));

    assert_eq!(runtime.writes.slot_export_allocation_bytes, 0);
    assert!(!runtime
        .writes
        .slot_export_allocations
        .contains_key(&Token(token_value)));
    assert!(!runtime
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(token_value)));
    assert!(!runtime.transport.is_registered(Token(token_value)));
    assert_eq!(runtime.slot_migration_runtime.deferred_reply_bytes(&id), 0);
    assert!(!runtime
        .slot_migration_runtime
        .is_export_connection(token_value));
    assert_eq!(
        runtime
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        1
    );

    assert!(!runtime.enforce_client_output_buffer_limit_for(token_value));
    runtime.remove_connection(token_value);
    runtime.remove_connection(token_value);
    assert_eq!(runtime.writes.slot_export_allocation_bytes, 0);
    assert_eq!(
        runtime
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        1
    );
}

#[test]
fn slot_export_owned_output_equal_to_effective_hard_limit_closes() {
    let (mut runtime, _peer, id) = setup_streaming_export_runtime();
    let token_value = runtime
        .slot_migration_runtime
        .export_connection(&id)
        .expect("streaming export has a registered connection");
    let mut frame = Vec::with_capacity(4096);
    frame.extend_from_slice(b"owned-export-output");
    assert!(runtime.queue_slot_migration_output(&id, token_value, frame));
    let owned_output = runtime.slot_export_output_memory_usage(&id, token_value);
    runtime.ctx.config_mut().server.repl_backlog_size = 0;
    runtime
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = owned_output;
    let effective_hard_limit = runtime
        .ctx
        .config()
        .replication
        .replica_output_buffer_hard_limit
        .max(runtime.ctx.config().server.repl_backlog_size.max(0) as usize);

    assert_eq!(owned_output, effective_hard_limit);
    assert!(runtime.enforce_client_output_buffer_limit_for(token_value));

    assert!(!runtime
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(token_value)));
    assert_eq!(
        runtime
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        1
    );
    assert_eq!(
        runtime
            .ctx
            .cluster_state()
            .as_deref()
            .expect("cluster state")
            .migrations
            .get(&id)
            .expect("migration job")
            .phase,
        MigrationPhase::Terminal(
            crate::engine::distributed::cluster::migration::TerminalPhase::Failed
        )
    );
}

#[test]
fn slot_export_limit_cleanup_cannot_reactivate_from_stale_writable() {
    let (mut runtime, _peer, id) = setup_streaming_export_runtime();
    let token_value = runtime
        .slot_migration_runtime
        .export_connection(&id)
        .expect("streaming export has a registered connection");
    runtime.schedule_runtime_runnable_next_turn(slot_export_key(token_value));
    trigger_export_output_limit(&mut runtime, &id, token_value);

    runtime.handle_writable(Token(token_value));
    runtime.dispatch_transport_event(
        crate::runtime::server::worker::reactor::ReadyEvent {
            token: Token(token_value),
            readable: false,
            writable: true,
            error: false,
            read_hup: false,
        },
        false,
    );
    runtime.writes.slot_exports.begin_turn();
    runtime.drive_pending_slot_export_writes();

    assert!(!runtime.queue_slot_migration_output(&id, token_value, b"stale enqueue".to_vec(),));
    assert!(!runtime.arm_slot_export_output(token_value));
    assert!(!runtime
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(token_value)));
    assert!(!runtime
        .writes
        .slot_export_allocations
        .contains_key(&Token(token_value)));
    assert!(!runtime
        .slot_migration_runtime
        .is_export_connection(token_value));
    assert!(!runtime.transport.is_registered(Token(token_value)));
}

#[test]
fn slot_export_limit_counts_disconnect_once_and_preserves_peer_connection_loss_semantics() {
    let (mut runtime, _peer, id) = setup_streaming_export_runtime();
    let token_value = runtime
        .slot_migration_runtime
        .export_connection(&id)
        .expect("streaming export has a registered connection");

    trigger_export_output_limit(&mut runtime, &id, token_value);
    assert!(!runtime.enforce_client_output_buffer_limit_for(token_value));

    assert_eq!(
        runtime
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        1,
        "the local COB-limit path must record its specific diagnostic counter once"
    );
    let job = runtime
        .ctx
        .cluster_state()
        .as_deref()
        .unwrap()
        .migrations
        .get(&id)
        .unwrap();
    assert_eq!(
        job.message.as_deref(),
        Some(
            "Connection lost to target. Check CLUSTER GETSLOTMIGRATIONS on the target node for more information."
        )
    );

    let (mut target_runtime, _source_peer, import_id) = setup_registered_import_runtime();
    let import_token = target_runtime
        .connection_registry
        .iter()
        .find_map(|(_, connection)| {
            matches!(
                connection.slot_migration_role,
                Some(SlotMigrationConnectionRole::Import(_))
            )
            .then_some(connection.token().0)
        })
        .expect("registered import connection");
    target_runtime.remove_connection(import_token);
    assert_eq!(
        target_runtime
            .ctx
            .cluster_state()
            .as_deref()
            .unwrap()
            .migrations
            .get(&import_id)
            .unwrap()
            .message
            .as_deref(),
        Some(
            "Connection lost to source. Check CLUSTER GETSLOTMIGRATIONS on the source node for more information."
        )
    );
}

#[test]
fn arm_slot_export_output_reports_interest_failure() {
    let (mut runtime, _peer, id) = setup_export_runtime();
    let connection_id = runtime
        .slot_migration_runtime
        .export_connection(&id)
        .expect("connected export has a runtime connection");
    let connection_token = connection_id;
    runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .queue_reply(b"control".to_vec());
    runtime.reconcile_slot_export_output_allocation(connection_id);
    runtime
        .transport
        .deregister_for_test(Token(connection_id))
        .expect("inject writable-interest failure");
    runtime.writes.slot_exports.begin_turn();

    assert!(!runtime.arm_slot_export_output(connection_id));
    assert!(!runtime
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(connection_id)));
    assert!(!runtime
        .event_batch
        .contains_runnable(slot_export_key(connection_id)));
    assert!(runtime
        .ctx
        .cluster_state()
        .as_deref()
        .expect("cluster state")
        .migrations
        .get(&id)
        .expect("export job")
        .phase
        .is_terminal());
}

#[test]
fn establish_does_not_advance_when_pending_write_interest_fails() {
    let (mut runtime, mut peer, id) = setup_export_runtime();
    let connection_id = runtime
        .slot_migration_runtime
        .export_connection(&id)
        .expect("connected export has a runtime connection");
    let actions = runtime
        .ctx
        .cluster_state_mut()
        .as_deref_mut()
        .expect("cluster state")
        .migrations
        .transition(
            &id,
            MigrationEvent::Connected {
                auth_required: false,
            },
            crate::foundation::io::sys::time::unixtime(),
        )
        .expect("connected export enters establish send phase");
    runtime
        .ctx
        .cluster_state_mut()
        .as_deref_mut()
        .expect("cluster state")
        .queue_migration_actions(actions);
    runtime
        .transport
        .deregister_for_test(Token(connection_id))
        .expect("inject writable-interest failure");

    runtime.drive_slot_migration_actions();

    assert!(!runtime
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(connection_id)));
    assert!(!runtime
        .event_batch
        .contains_runnable(slot_export_key(connection_id)));
    let job = runtime
        .ctx
        .cluster_state()
        .as_deref()
        .expect("cluster state")
        .migrations
        .get(&id)
        .expect("export job");
    assert_eq!(
        job.phase,
        MigrationPhase::Terminal(
            crate::engine::distributed::cluster::migration::TerminalPhase::Failed
        )
    );
    assert_eq!(
        job.message.as_deref(),
        Some(
            "Connection lost to target. Check CLUSTER GETSLOTMIGRATIONS on the target node for more information."
        )
    );
    peer.set_nonblocking(true)
        .expect("make export peer nonblocking");
    let mut bytes = [0; 4096];
    match peer.read(&mut bytes) {
        Ok(0) => {}
        Ok(count) => panic!(
            "ESTABLISH frame reached the peer after interest failure: {:?}",
            &bytes[..count]
        ),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
        Err(error) => panic!("unexpected export peer read error: {error}"),
    }
}

#[test]
fn remaining_repl_size_includes_live_userspace_mutation_bytes() {
    let mut runtime = ServerWorkerState::new(1).expect("create runtime");
    let id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .expect("valid migration id");
    let token = Token(96);
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(token);
    connection.slot_migration_role = Some(SlotMigrationConnectionRole::Export(id.clone()));
    connection.queue_replication_reply(vec![b'm'; 37]);
    let _connection_token = runtime.connection_registry.insert(connection);
    runtime.slot_migration_runtime.register_export(
        crate::framework::distributed::migration::SlotExportLink::new(id.clone(), token.0),
    );

    assert_eq!(
        runtime.slot_export_reply_bytes(&id, token.0),
        37,
        "slot-export ownership is the live mutation suffix still queued in userspace"
    );
}

#[test]
fn failover_granted_export_does_not_queue_periodic_ack() {
    let (mut runtime, _peer, id) = setup_export_runtime();
    let now = crate::foundation::io::sys::time::unixtime();
    let state = runtime.ctx.cluster_state_mut().as_deref_mut().unwrap();
    for event in [
        MigrationEvent::Connected {
            auth_required: false,
        },
        MigrationEvent::EstablishCommandSent,
        MigrationEvent::Established,
        MigrationEvent::SnapshotStarted,
        MigrationEvent::SnapshotFinished,
        MigrationEvent::PauseRequested,
        MigrationEvent::OutputDrained,
        MigrationEvent::FailoverGranted,
    ] {
        state
            .migrations
            .transition(&id, event, now)
            .expect("advance export fixture to failover granted");
    }
    let token = runtime
        .slot_migration_runtime
        .export_connection(&id)
        .expect("registered export connection");
    let connection_token = token;

    runtime.send_slot_migration_periodic_acks();

    assert_eq!(
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .pending_reply_bytes(),
        0
    );
}

#[test]
fn slot_snapshot_relay_stops_after_one_pending_chunk() {
    let (source, writer) =
        crate::foundation::io::sys::fd::pipe_with_background_payload(vec![b'x'; 32 * 1024])
            .expect("create background slot snapshot source");

    let mut runtime = ServerWorkerState::new(1).unwrap();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let id = MigrationJobId::parse(b"dddddddddddddddddddddddddddddddddddddddd").unwrap();
    let mut job = MigrationJob::new(
        id.clone(),
        NodeId::parse(state.myself.name.as_bytes()).unwrap(),
        NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
        SlotRanges::new(vec![SlotRange::new(1, 1).unwrap()]).unwrap(),
        MigrationOperation::Export,
        1,
    );
    job.phase = MigrationPhase::Export(ExportPhase::Snapshotting);
    state.migrations.start(job).unwrap();
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));

    let token = Token(94);
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(token);
    connection.slot_migration_role = Some(SlotMigrationConnectionRole::Export(id.clone()));
    let connection_token = runtime.connection_registry.insert(connection);
    runtime.slot_migration_runtime.register_export(
        crate::framework::distributed::migration::SlotExportLink::new(id.clone(), token.0),
    );
    let source = install_slot_snapshot_source_for_test(&mut runtime, &id, source);
    runtime.slot_migration_snapshot = Some(super::SlotSnapshotRelay {
        job_id: id.clone(),
        pid: i32::MAX,
        source: Some(source),
        completion_state: super::SlotSnapshotCompletionState::default(),
    });

    runtime.transport.wait_readable_for_test(source.token);
    runtime.on_slot_snapshot_pipe_readable(id.clone());

    assert!(runtime.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)]
    .has_pending_output());
    assert_eq!(
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .output
            .replication_bytes,
        0
    );
    assert_eq!(runtime.slot_export_reply_bytes(&id, token.0), 0);

    runtime.on_slot_snapshot_pipe_readable(id.clone());

    assert!(runtime.slot_migration_snapshot.is_some());
    assert!(
        !runtime
            .transport
            .source_enabled_for_test(source.token)
            .expect("inspect source state"),
        "pending target output must pause slot snapshot source"
    );
    drop(runtime);
    crate::foundation::io::sys::fd::join_background_pipe_writer_after_reader_close(writer);
}

#[test]
fn waiting_slot_snapshot_retries_after_active_child_finishes() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let id = MigrationJobId::parse(b"dddddddddddddddddddddddddddddddddddddddd").unwrap();
    let mut job = MigrationJob::new(
        id.clone(),
        NodeId::parse(state.myself.name.as_bytes()).unwrap(),
        NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
        SlotRanges::new(vec![SlotRange::new(1, 1).unwrap()]).unwrap(),
        MigrationOperation::Export,
        1,
    );
    job.phase = MigrationPhase::Export(ExportPhase::WaitingToSnapshot);
    state.migrations.start(job).unwrap();
    state.queue_migration_actions([MigrationAction::StartSnapshot(id.clone())]);
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));

    runtime
        .ctx
        .persistence_mut()
        .begin_diskless_child(123_456)
        .unwrap();
    runtime.drive_slot_migration_actions();
    assert!(
        runtime.slot_migration_snapshot.is_none(),
        "active persistence child must delay snapshot start"
    );

    runtime.ctx.persistence_mut().complete_diskless_child();
    runtime.drive_slot_migration_actions();
    assert!(
        runtime.slot_migration_snapshot.is_some(),
        "waiting snapshot must retry once the active child slot clears"
    );
    runtime.abort_slot_snapshot(&id);
}

#[test]
fn delayed_slot_snapshot_preserves_later_migration_actions() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let id = MigrationJobId::parse(b"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee").unwrap();
    let mut job = MigrationJob::new(
        id.clone(),
        NodeId::parse(state.myself.name.as_bytes()).unwrap(),
        NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
        SlotRanges::new(vec![SlotRange::new(1, 1).unwrap()]).unwrap(),
        MigrationOperation::Export,
        1,
    );
    job.phase = MigrationPhase::Export(ExportPhase::WaitingToSnapshot);
    state.migrations.start(job).unwrap();
    state.queue_migration_actions([
        MigrationAction::StartSnapshot(id.clone()),
        MigrationAction::Cleanup(id.clone()),
    ]);
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));

    runtime
        .ctx
        .persistence_mut()
        .begin_diskless_child(123_457)
        .unwrap();
    runtime.drive_slot_migration_actions();
    runtime.ctx.persistence_mut().complete_diskless_child();
    runtime.drive_slot_migration_actions();

    assert!(
        runtime.slot_migration_snapshot.is_none(),
        "cleanup queued behind a delayed snapshot must still abort the started snapshot"
    );
    assert!(
        !runtime.ctx.persistence().has_active_child(),
        "cleanup must also clear the snapshot child slot"
    );
}

#[test]
fn active_slot_snapshot_delays_later_snapshot_actions_without_dropping_suffix() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let active_id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    let delayed_id = MigrationJobId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
    for (id, slot) in [(&active_id, 1), (&delayed_id, 2)] {
        let mut job = MigrationJob::new(
            id.clone(),
            NodeId::parse(state.myself.name.as_bytes()).unwrap(),
            NodeId::parse(b"cccccccccccccccccccccccccccccccccccccccc").unwrap(),
            SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap(),
            MigrationOperation::Export,
            1,
        );
        job.phase = MigrationPhase::Export(ExportPhase::WaitingToSnapshot);
        state.migrations.start(job).unwrap();
    }
    state.queue_migration_actions([
        MigrationAction::StartSnapshot(delayed_id.clone()),
        MigrationAction::Cleanup(delayed_id.clone()),
    ]);
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));
    runtime.slot_migration_snapshot = Some(super::SlotSnapshotRelay {
        job_id: active_id,
        pid: i32::MAX,
        source: None,
        completion_state: super::SlotSnapshotCompletionState::default(),
    });

    runtime.drive_slot_migration_actions();
    assert!(
        runtime
            .ctx
            .cluster_state_mut()
            .as_deref_mut()
            .expect("cluster state")
            .take_migration_actions()
            .len()
            == 2,
        "delayed StartSnapshot must preserve the queued suffix"
    );
}

#[test]
fn slot_snapshot_source_registration_failure_clears_child_state() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let id = MigrationJobId::parse(b"ffffffffffffffffffffffffffffffffffffffff").unwrap();
    let mut job = MigrationJob::new(
        id.clone(),
        NodeId::parse(state.myself.name.as_bytes()).unwrap(),
        NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
        SlotRanges::new(vec![SlotRange::new(1, 1).unwrap()]).unwrap(),
        MigrationOperation::Export,
        1,
    );
    job.phase = MigrationPhase::Export(ExportPhase::WaitingToSnapshot);
    state.migrations.start(job).unwrap();
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));
    runtime.transport.fail_register_source_for_test();

    runtime.try_start_slot_snapshot(&id);

    assert!(runtime.slot_migration_snapshot.is_none());
    assert!(!runtime.ctx.persistence().has_active_child());
    assert_eq!(runtime.collect_child_completion_events(), 0);
}

#[test]
fn slot_snapshot_completion_facts_require_pipe_eof_and_child_success() {
    use crate::runtime::server::worker::reactor::sources::ChildExitOutcome;

    let mut pipe_first = super::SlotSnapshotCompletionState::default();
    pipe_first.apply_fact(super::SnapshotFact::PipeEof);
    assert!(!pipe_first.can_finish());
    pipe_first.apply_fact(super::SnapshotFact::ChildExited(ChildExitOutcome::Exited(
        0,
    )));
    assert!(pipe_first.can_finish());

    let mut child_first = super::SlotSnapshotCompletionState::default();
    child_first.apply_fact(super::SnapshotFact::ChildExited(ChildExitOutcome::Exited(
        0,
    )));
    assert!(!child_first.can_finish());
    child_first.apply_fact(super::SnapshotFact::PipeEof);
    assert!(child_first.can_finish());

    let mut read_failed = super::SlotSnapshotCompletionState::default();
    read_failed.apply_fact(super::SnapshotFact::ReadError);
    read_failed.apply_fact(super::SnapshotFact::ChildExited(ChildExitOutcome::Exited(
        0,
    )));
    read_failed.apply_fact(super::SnapshotFact::PipeEof);
    assert!(!read_failed.can_finish());
    assert!(read_failed.failed());

    let mut child_failed = super::SlotSnapshotCompletionState::default();
    child_failed.apply_fact(super::SnapshotFact::PipeEof);
    child_failed.apply_fact(super::SnapshotFact::ChildExited(ChildExitOutcome::Exited(
        1,
    )));
    assert!(!child_failed.can_finish());
    assert!(child_failed.failed());
}

#[test]
fn remaining_repl_size_includes_post_client_output_but_excludes_snapshot_allocation() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    let id = MigrationJobId::parse(b"dddddddddddddddddddddddddddddddddddddddd").unwrap();
    let token = Token(95);
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(token);
    connection.slot_migration_role = Some(SlotMigrationConnectionRole::Export(id.clone()));
    let mut control = Vec::with_capacity(4096);
    control.extend_from_slice(&[b'c'; 51]);
    connection.queue_reply(control);
    connection.queue_replication_reply(vec![b'r'; 7]);
    let mut snapshot = Vec::with_capacity(8192);
    snapshot.extend_from_slice(&[b's'; 13]);
    connection.queue_slot_snapshot_reply(snapshot);
    let connection_token = runtime.connection_registry.insert(connection);
    runtime.slot_migration_runtime.register_export(
        crate::framework::distributed::migration::SlotExportLink::new(id.clone(), token.0),
    );
    let deferred_frame = vec![b'd'; 11];
    assert!(runtime
        .slot_migration_runtime
        .queue_export_mutation(&id, 0, deferred_frame.clone(), true)
        .is_empty());
    let deferred_bytes =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SELECT", b"0"]).len()
            + deferred_frame.len();
    let reply_bytes = runtime.slot_export_reply_bytes(&id, token.0);
    let output_memory = runtime.slot_export_output_memory_usage(&id, token.0);
    let all_pending_memory = runtime.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)]
    .output_buffer_allocation_bytes()
    .saturating_add(
        runtime
            .slot_migration_runtime
            .deferred_output_memory_usage(&id),
    );

    assert_eq!(reply_bytes, 51 + 7 + deferred_bytes);
    assert!(output_memory > reply_bytes);
    assert!(output_memory < all_pending_memory);
}

#[test]
fn allocator_exclusion_tracks_deferred_transfer_and_connection_teardown() {
    let (mut runtime, _peer, id) = setup_export_runtime();
    let token_value = runtime
        .slot_migration_runtime
        .export_connection(&id)
        .expect("connected export has a runtime connection");
    let connection_token = token_value;
    runtime.track_slot_export_connection_output(connection_token);
    let base_allocation = runtime.writes.slot_export_allocation_bytes;
    assert_eq!(runtime.ctx.maxmemory_state().not_counted, base_allocation);

    assert!(runtime
        .queue_slot_export_mutation(&id, 0, vec![b'd'; 257], true)
        .is_empty());
    let deferred_allocation = runtime
        .slot_migration_runtime
        .deferred_output_memory_usage(&id);
    assert!(deferred_allocation >= 257);
    assert_eq!(
        runtime.writes.slot_export_allocation_bytes,
        base_allocation + deferred_allocation
    );
    assert_eq!(
        runtime.ctx.maxmemory_state().not_counted,
        runtime.writes.slot_export_allocation_bytes
    );

    let frames = runtime.take_slot_export_deferred_mutations(&id);
    assert_eq!(runtime.writes.slot_export_allocation_bytes, base_allocation);
    for frame in frames {
        assert!(runtime.queue_slot_migration_output(&id, token_value, frame));
    }
    assert_eq!(
        runtime.writes.slot_export_allocation_bytes,
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .output_buffer_allocation_bytes()
    );
    assert_eq!(
        runtime.ctx.maxmemory_state().not_counted,
        runtime.writes.slot_export_allocation_bytes
    );

    runtime.remove_connection(token_value);
    assert_eq!(runtime.writes.slot_export_allocation_bytes, 0);
    assert_eq!(runtime.ctx.maxmemory_state().not_counted, 0);
}

#[test]
fn export_tcp_buffers_are_deferred_until_connect_succeeds() {
    let (mut runtime, _peer, id) = setup_export_runtime();
    let token = Token(
        runtime
            .slot_migration_runtime
            .export_connection(&id)
            .unwrap(),
    );
    assert!(!runtime.connection_registry[token].slot_migration_tcp_buffers_applied);

    runtime.handle_writable(token);

    assert!(runtime.connection_registry[token].slot_migration_tcp_buffers_applied);
    assert_eq!(
        runtime
            .ctx
            .cluster_state()
            .as_deref()
            .unwrap()
            .migrations
            .get(&id)
            .map(|job| job.phase),
        Some(MigrationPhase::Export(
            ExportPhase::ReadingEstablishResponse
        ))
    );
}

#[test]
fn refused_export_connect_preserves_socket_error_before_tcp_buffer_configuration() {
    let target = TcpListener::bind("127.0.0.1:0").unwrap();
    let target_addr = target.local_addr().unwrap();
    drop(target);
    let mut runtime = ServerWorkerState::new_with_slot_router(1, 14, |key| {
        crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
    })
    .unwrap();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let source = NodeId::parse(state.myself.name.as_bytes()).unwrap();
    let target_id = NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
    let mut target_node = ClusterNode::new(
        target_id.as_str().to_string(),
        target_addr.ip().to_string(),
        target_addr.port(),
        fixture_cluster_bus_port(target_addr.port()),
        true,
    );
    target_node.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(target_node.name.clone(), target_node);
    state.add_slot(1).unwrap();
    let id = MigrationJobId::parse(b"cccccccccccccccccccccccccccccccccccccccc").unwrap();
    let actions = state
        .migrations
        .start(MigrationJob::new(
            id.clone(),
            source,
            target_id,
            SlotRanges::new(vec![SlotRange::new(1, 1).unwrap()]).unwrap(),
            MigrationOperation::Export,
            1,
        ))
        .unwrap();
    state.queue_migration_actions(actions);
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));
    runtime.drive_slot_migration_actions();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    while !runtime
        .ctx
        .cluster_state()
        .as_deref()
        .unwrap()
        .migrations
        .get(&id)
        .map(|job| job.phase)
        .unwrap()
        .is_terminal()
    {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(!remaining.is_zero(), "refused export connect must complete");
        let events = runtime.transport.poll_events(Some(remaining)).unwrap();
        for event in events {
            runtime.dispatch_transport_event(
                crate::runtime::server::worker::reactor::ReadyEvent {
                    token: event.token,
                    readable: event.readable,
                    writable: event.writable,
                    error: event.error,
                    read_hup: event.read_hup,
                },
                false,
            );
        }
    }

    let job = runtime
        .ctx
        .cluster_state()
        .as_deref()
        .unwrap()
        .migrations
        .get(&id)
        .unwrap();
    assert_eq!(
        job.phase,
        MigrationPhase::Terminal(
            crate::engine::distributed::cluster::migration::TerminalPhase::Failed
        )
    );
    let refused = std::io::Error::from_raw_os_error(libc::ECONNREFUSED);
    assert_eq!(
        job.message.as_deref(),
        Some(format!("Unable to connect to target node: {refused}").as_str())
    );
    assert!(runtime
        .slot_migration_runtime
        .export_connection(&id)
        .is_none());
    assert_eq!(runtime.connected_clients(), 0);
}

#[test]
fn successful_connect_queues_establish_and_enters_reply_phase() {
    let (mut runtime, mut peer, id) = setup_export_runtime();
    let connection_id = runtime
        .slot_migration_runtime
        .export_connection(&id)
        .unwrap();

    runtime.handle_writable(Token(connection_id));
    let mut bytes = [0; 4096];
    let count = peer.read(&mut bytes).unwrap();
    assert!(bytes[..count]
        .windows(b"ESTABLISH".len())
        .any(|window| window == b"ESTABLISH"));
    assert_eq!(
        runtime
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .migrations
            .get(&id)
            .unwrap()
            .phase,
        MigrationPhase::Export(ExportPhase::ReadingEstablishResponse)
    );
}

#[test]
fn fragmented_establish_reply_starts_snapshot() {
    let (mut runtime, mut peer, id) = setup_export_runtime();
    let connection_id = runtime
        .slot_migration_runtime
        .export_connection(&id)
        .unwrap();
    runtime.handle_writable(Token(connection_id));
    peer.write_all(b"+O").unwrap();
    runtime
        .transport
        .wait_readable_for_test(Token(connection_id));
    runtime.handle_slot_export_readable(connection_id);
    peer.write_all(b"K\r\n").unwrap();
    runtime
        .transport
        .wait_readable_for_test(Token(connection_id));
    runtime.handle_slot_export_readable(connection_id);

    let phase = runtime
        .ctx
        .cluster_state()
        .as_ref()
        .unwrap()
        .migrations
        .get(&id)
        .unwrap()
        .phase;
    assert!(matches!(
        phase,
        MigrationPhase::Export(ExportPhase::WaitingToSnapshot | ExportPhase::Snapshotting)
    ));
}

#[test]
fn establish_response_before_peer_close_is_processed_before_teardown() {
    let (mut runtime, mut peer, id) = setup_export_runtime();
    let connection_id = runtime
        .slot_migration_runtime
        .export_connection(&id)
        .expect("export connection");
    runtime.handle_writable(Token(connection_id));
    let mut request = [0_u8; 4096];
    let count = peer.read(&mut request).expect("read establish request");
    assert!(request[..count]
        .windows(b"ESTABLISH".len())
        .any(|window| window == b"ESTABLISH"));
    peer.write_all(b"-ERR target rejected\r\n")
        .expect("write complete establish response");
    peer.shutdown(Shutdown::Write)
        .expect("half-close migration peer");

    runtime
        .transport
        .wait_readable_for_test(Token(connection_id));
    runtime.handle_slot_export_readable(connection_id);

    assert_eq!(
        runtime
            .ctx
            .cluster_state()
            .as_deref()
            .expect("cluster state")
            .migrations
            .get(&id)
            .expect("migration job")
            .message
            .as_deref(),
        Some("Received error during handshake to target: ERR target rejected")
    );
}

#[test]
fn auth_error_uses_auth_specific_failure_message() {
    let (mut runtime, mut peer, id) = setup_export_runtime();
    runtime.ctx.config_mut().security.primaryauth = Some(b"wrong-password".to_vec());
    let connection_id = runtime
        .slot_migration_runtime
        .export_connection(&id)
        .unwrap();
    runtime.handle_writable(Token(connection_id));
    runtime.handle_writable(Token(connection_id));
    let mut bytes = [0; 4096];
    let count = peer.read(&mut bytes).unwrap();
    assert!(bytes[..count]
        .windows(b"AUTH".len())
        .any(|window| window == b"AUTH"));
    peer.write_all(b"-WRONGPASS invalid username-password pair or user is disabled.\r\n")
        .unwrap();

    runtime
        .transport
        .wait_readable_for_test(Token(connection_id));
    runtime.handle_slot_export_readable(connection_id);

    assert_eq!(
        runtime
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .migrations
            .get(&id)
            .unwrap()
            .message
            .as_deref(),
        Some("Failed to AUTH to target node: WRONGPASS invalid username-password pair or user is disabled.")
    );
}

#[test]
fn successful_auth_updates_export_connection_acl_identity() {
    let (mut runtime, mut peer, id) = setup_export_runtime();
    runtime.ctx.config_mut().security.primaryuser = Some(b"admin".to_vec());
    runtime.ctx.config_mut().security.primaryauth = Some(b"password".to_vec());
    let connection_id = runtime
        .slot_migration_runtime
        .export_connection(&id)
        .unwrap();
    runtime.handle_writable(Token(connection_id));
    runtime.handle_writable(Token(connection_id));
    let mut bytes = [0; 4096];
    let _ = peer.read(&mut bytes).unwrap();
    peer.write_all(b"+OK\r\n").unwrap();

    runtime
        .transport
        .wait_readable_for_test(Token(connection_id));
    runtime.handle_slot_export_readable(connection_id);

    let connection_token = connection_id;
    assert_eq!(
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .username,
        "admin"
    );
    assert!(
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .authenticated
    );
}

#[test]
fn cleanup_preserves_replication_connection_used_for_tracking_import() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    let id = MigrationJobId::parse(b"cccccccccccccccccccccccccccccccccccccccc").unwrap();
    let token = Token(91);
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(token);
    connection.slot_migration_role = Some(SlotMigrationConnectionRole::Import(id.clone()));
    let connection_token = runtime.connection_registry.insert(connection);
    runtime
        .replication_runtime
        .set_replica_link(ReplicaLink::new(
            ConnectionId(token.0),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        ));
    let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
    state.queue_migration_actions([MigrationAction::Cleanup(id)]);
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));

    runtime.drive_slot_migration_actions();

    assert_eq!(
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .state,
        ConnectionState::Connected
    );
    assert_eq!(
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .slot_migration_role,
        None
    );
    assert!(runtime.replication_runtime.replica_link().is_some());
}

#[test]
fn cleanup_releases_export_allocation_before_clearing_a_retained_connection_role() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    let id = MigrationJobId::parse(b"abababababababababababababababababababab").unwrap();
    let token = Token(98);
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(token);
    connection.slot_migration_role = Some(SlotMigrationConnectionRole::Export(id.clone()));
    connection.queue_replication_reply(vec![b'x'; 257]);
    let connection_token = runtime.connection_registry.insert(connection);
    runtime.slot_migration_runtime.register_export(
        crate::framework::distributed::migration::SlotExportLink::new(id.clone(), token.0),
    );
    runtime.track_slot_export_connection_output(connection_token);
    runtime
        .replication_runtime
        .set_replica_link(ReplicaLink::new(
            ConnectionId(token.0),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        ));
    let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
    state.queue_migration_actions([MigrationAction::Cleanup(id)]);
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));

    runtime.drive_slot_migration_actions();

    assert_eq!(
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .state,
        ConnectionState::Connected
    );
    assert_eq!(
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .slot_migration_role,
        None
    );
    assert_eq!(runtime.writes.slot_export_allocation_bytes, 0);
}

#[test]
fn cleanup_keeps_export_role_until_connection_is_closed() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    let id = MigrationJobId::parse(b"cccccccccccccccccccccccccccccccccccccccc").unwrap();
    let token = Token(92);
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(token);
    connection.slot_migration_role = Some(SlotMigrationConnectionRole::Export(id.clone()));
    let connection_token = runtime.connection_registry.insert(connection);
    runtime.slot_migration_runtime.register_export(
        crate::framework::distributed::migration::SlotExportLink::new(id.clone(), token.0),
    );
    let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
    state.queue_migration_actions([MigrationAction::Cleanup(id.clone())]);
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));

    runtime.drive_slot_migration_actions();

    assert_eq!(
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .state,
        ConnectionState::Closing
    );
    assert_eq!(
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .slot_migration_role,
        Some(SlotMigrationConnectionRole::Export(id))
    );
}

#[test]
fn failed_import_propagates_terminal_state_to_replicas() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
    let id = MigrationJobId::parse(b"dddddddddddddddddddddddddddddddddddddddd").unwrap();
    let source = NodeId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    let target = NodeId::parse(state.myself.name.as_bytes()).unwrap();
    let job = MigrationJob::new(
        id.clone(),
        source,
        target,
        SlotRanges::new(vec![SlotRange::new(1, 1).unwrap()]).unwrap(),
        MigrationOperation::Import,
        1,
    );
    state.migrations.start(job).unwrap();
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));
    runtime
        .ctx
        .replication_mut()
        .backlog
        .append(b"seed")
        .unwrap();
    let start = runtime.ctx.replication().backlog.next_stream_cursor();

    runtime.fail_slot_migration(&id, "Connection lost during slot migration");
    runtime.drive_slot_migration_actions();

    let bytes = runtime
        .ctx
        .replication()
        .backlog
        .read_from(start)
        .expect("terminal frame remains in backlog");
    for expected in [
        b"SYNCSLOTS".as_slice(),
        b"FINISH",
        b"FAILED",
        id.as_str().as_bytes(),
        b"Connection lost during slot migration",
    ] {
        assert!(
            bytes
                .windows(expected.len())
                .any(|window| window == expected),
            "missing {:?} in propagated terminal frame",
            String::from_utf8_lossy(expected)
        );
    }
}

#[test]
fn slot_import_direct_frames_wait_for_their_own_aof_stamp() {
    use crate::foundation::io::persistence::{AofAppender, AofFsync};
    let mut runtime = ServerWorkerState::new_with_slot_router(1, 14, |key| {
        crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
    })
    .unwrap();
    crate::framework::commands::register_all(&mut runtime.registry);
    let id = MigrationJobId::parse(b"ffffffffffffffffffffffffffffffffffffffff").unwrap();
    let key = b"{atomic}key";
    let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
    let ranges = SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
    state
        .migrations
        .start(MigrationJob::new(
            id.clone(),
            NodeId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
            NodeId::parse(state.myself.name.as_bytes()).unwrap(),
            ranges.clone(),
            MigrationOperation::Import,
            1,
        ))
        .unwrap();
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));
    runtime.ctx.create_slot_import(id.clone(), &ranges);
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(Token(93));
    connection.slot_migration_role = Some(SlotMigrationConnectionRole::Import(id.clone()));
    let token = runtime.connection_registry.insert(connection);
    let path =
        std::env::temp_dir().join(format!("versal-import-stamp-{}.aof", rand::random::<u64>()));
    let mut appender = AofAppender::open_append_path(&path, AofFsync::No).unwrap();
    appender.force_fsync().unwrap();
    runtime.ctx.aof_mut().journal =
        Some(crate::foundation::io::persistence::journal::AofJournal::new(appender));
    runtime.ctx.aof_mut().replication_baseline = crate::engine::persistence::AofBaseline::Ready;
    runtime.ctx.config_mut().persistence.aof_enabled = true;
    runtime.ctx.config_mut().persistence.aof_fsync = AofFsync::No;
    runtime.ctx.config_mut().replication.repl_after_aof_fsync = true;
    runtime.ctx.replication_mut().backlog.activate();
    let args = crate::engine::command::CommandArgs::from_parts(vec![
        bytes::Bytes::from_static(b"SET"),
        bytes::Bytes::copy_from_slice(key),
        bytes::Bytes::from_static(b"value"),
    ]);
    assert!(runtime.apply_slot_import_command(token, args));
    assert!(runtime.ctx.replication().backlog.unreleased_bytes() > 0);
    runtime.flush_aof_if_needed();
    runtime
        .ctx
        .aof_mut()
        .journal
        .as_mut()
        .unwrap()
        .appender
        .force_fsync()
        .unwrap();
    runtime.release_synced_replication();
    assert_eq!(runtime.ctx.replication().backlog.unreleased_bytes(), 0);
    runtime.fail_slot_migration(&id, "test terminal");
    runtime.drive_slot_migration_actions();
    assert!(
        runtime.ctx.replication().backlog.unreleased_bytes() > 0,
        "terminal metadata needs its own AOF stamp"
    );
    drop(runtime);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn target_oom_fails_live_import_before_applying_write() {
    let mut runtime = ServerWorkerState::new_with_slot_router(1, 14, |key| {
        crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
    })
    .unwrap();
    crate::framework::commands::register_all(&mut runtime.registry);
    runtime.ctx.config_mut().server.maxmemory = 1;
    let id = MigrationJobId::parse(b"ffffffffffffffffffffffffffffffffffffffff").unwrap();
    let key = b"{atomic}oom";
    let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
    let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
    let job = MigrationJob::new(
        id.clone(),
        NodeId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
        NodeId::parse(state.myself.name.as_bytes()).unwrap(),
        SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap(),
        MigrationOperation::Import,
        1,
    );
    state.migrations.start(job).unwrap();
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));
    runtime.ctx.create_slot_import(
        id.clone(),
        &SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap(),
    );
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(Token(93));
    connection.slot_migration_role = Some(SlotMigrationConnectionRole::Import(id.clone()));
    let connection_token = runtime.connection_registry.insert(connection);
    let args = crate::engine::command::CommandArgs::from_parts(vec![
        bytes::Bytes::from_static(b"SET"),
        bytes::Bytes::copy_from_slice(key),
        bytes::Bytes::from_static(b"value"),
    ]);

    assert!(runtime.apply_slot_import_command(connection_token, args));

    assert_eq!(
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .state,
        ConnectionState::Closing
    );
    let job = runtime
        .ctx
        .cluster_state()
        .as_ref()
        .unwrap()
        .migrations
        .get(&id)
        .unwrap();
    assert_eq!(
        job.phase,
        MigrationPhase::Terminal(
            crate::engine::distributed::cluster::migration::TerminalPhase::Failed
        )
    );
    assert!(job.message.as_deref().unwrap().contains("OOM"));
    assert_eq!(runtime.ctx.slot_import_key_count(0, slot as usize), 0);
}

#[test]
fn strict_target_import_rejects_durability_faults_before_mutation() {
    use crate::foundation::io::persistence::{AofAppender, AofFsync};
    for has_pending_backlog in [false, true] {
        let mut runtime = ServerWorkerState::new_with_slot_router(1, 14, |key| {
            crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
        })
        .unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        let id = MigrationJobId::parse(b"ffffffffffffffffffffffffffffffffffffffff").unwrap();
        let key = b"{atomic}fault";
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
        let ranges = SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        state
            .migrations
            .start(MigrationJob::new(
                id.clone(),
                NodeId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
                NodeId::parse(state.myself.name.as_bytes()).unwrap(),
                ranges.clone(),
                MigrationOperation::Import,
                1,
            ))
            .unwrap();
        *runtime.ctx.cluster_state_mut() = Some(Box::new(state));
        runtime.ctx.create_slot_import(id.clone(), &ranges);
        runtime.ctx.db_mut(0).unwrap().set(
            key.to_vec(),
            crate::foundation::io::store::Robj::string("live-before"),
        );
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(93));
        connection.slot_migration_role = Some(SlotMigrationConnectionRole::Import(id));
        let token = runtime.connection_registry.insert(connection);
        let path =
            std::env::temp_dir().join(format!("versal-import-fault-{}.aof", rand::random::<u64>()));
        runtime.ctx.aof_mut().journal = Some(
            crate::foundation::io::persistence::journal::AofJournal::new(
                AofAppender::open_append_path(&path, AofFsync::No).unwrap(),
            ),
        );
        runtime.ctx.config_mut().persistence.aof_enabled = true;
        runtime.ctx.config_mut().replication.repl_after_aof_fsync = true;
        if has_pending_backlog {
            runtime.ctx.aof_mut().replication_baseline =
                crate::engine::persistence::AofBaseline::Ready;
            runtime
                .ctx
                .config_mut()
                .replication
                .replica_output_buffer_hard_limit = 1;
            runtime.ctx.config_mut().server.repl_backlog_size = 1;
            runtime.ctx.replication_mut().backlog.activate();
            let frame =
                crate::engine::distributed::replication::protocol::encode_command(&[b"PING"]);
            runtime.feed_aof(&frame);
            runtime.propagate_to_replicas(&frame);
        }
        let args = crate::engine::command::CommandArgs::from_parts(
            [b"SET".as_slice(), key, b"value"]
                .into_iter()
                .map(bytes::Bytes::copy_from_slice)
                .collect(),
        );
        assert!(runtime.apply_slot_import_command(token, args));
        assert_eq!(
            runtime.connection_registry[Token(token)].state,
            ConnectionState::Closing
        );
        assert_eq!(runtime.ctx.slot_import_key_count(0, slot as usize), 0);
        assert_eq!(
            runtime
                .ctx
                .db(0)
                .unwrap()
                .get_no_expire(key)
                .unwrap()
                .as_bytes(),
            b"live-before"
        );
        drop(runtime);
        std::fs::remove_file(path).unwrap();
    }
}

fn terminal_migration_fixture(
    operation: MigrationOperation,
    slots: &[u16],
) -> (ServerWorkerState, MigrationJobId, std::path::PathBuf) {
    use crate::foundation::io::persistence::{AofAppender, AofFsync};
    let mut runtime = ServerWorkerState::new_with_slot_router(1, 14, |key| {
        crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
    })
    .unwrap();
    crate::framework::commands::register_all(&mut runtime.registry);
    let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
    let local = NodeId::parse(state.myself.name.as_bytes()).unwrap();
    let remote = NodeId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    let node = ClusterNode::new(
        remote.as_str().to_string(),
        "127.0.0.1".into(),
        6379,
        16379,
        true,
    );
    state.nodes.insert(node.name.clone(), node);
    let id = MigrationJobId::parse(b"ffffffffffffffffffffffffffffffffffffffff").unwrap();
    let ranges = SlotRanges::new(
        slots
            .iter()
            .map(|slot| SlotRange::new(*slot, *slot).unwrap())
            .collect(),
    )
    .unwrap();
    let (source, target, phase) = match operation {
        MigrationOperation::Import => (
            remote.clone(),
            local,
            MigrationPhase::Import(
                crate::engine::distributed::cluster::migration::ImportPhase::FailoverGranted,
            ),
        ),
        MigrationOperation::Export => (
            local,
            remote.clone(),
            MigrationPhase::Export(ExportPhase::FailoverGranted),
        ),
    };
    let mut job = MigrationJob::new(
        id.clone(),
        source,
        target,
        ranges.clone(),
        operation,
        crate::foundation::io::sys::time::unixtime(),
    );
    job.phase = phase;
    state.migrations.start(job).unwrap();
    for slot in slots {
        state
            .set_slot_node(*slot as usize, remote.as_str())
            .unwrap();
    }
    state.finalize_slot_changes();
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));
    if operation == MigrationOperation::Import {
        runtime.ctx.create_slot_import(id.clone(), &ranges);
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(93));
        connection.slot_migration_role = Some(SlotMigrationConnectionRole::Import(id.clone()));
        runtime.connection_registry.insert(connection);
    } else {
        runtime.ctx.client_pause_mut().action = crate::framework::context::PauseAction::Write;
        runtime.ctx.client_pause_mut().reason = "slot_migration_in_progress";
        runtime.ctx.client_pause_mut().end_time_ms =
            crate::foundation::io::sys::time::mstime() + 60_000;
    }
    let path = std::env::temp_dir().join(format!("versal-terminal-{}.aof", rand::random::<u64>()));
    runtime.ctx.aof_mut().journal = Some(
        crate::foundation::io::persistence::journal::AofJournal::new(
            AofAppender::open_append_path(&path, AofFsync::No).unwrap(),
        ),
    );
    runtime.ctx.aof_mut().replication_baseline = crate::engine::persistence::AofBaseline::Ready;
    runtime.ctx.config_mut().persistence.aof_enabled = true;
    runtime.ctx.config_mut().persistence.aof_fsync = AofFsync::No;
    runtime.ctx.config_mut().replication.repl_after_aof_fsync = true;
    runtime.ctx.replication_mut().backlog.activate();
    (runtime, id, path)
}

fn terminal_migration_fault(runtime: &mut ServerWorkerState, fault: &str) {
    match fault {
        "unavailable" => {
            runtime.ctx.aof_mut().replication_baseline =
                crate::engine::persistence::AofBaseline::Unavailable
        }
        "write-error" => {
            runtime.ctx.aof_mut().last_write_status =
                crate::engine::persistence::OperationStatus::Err;
            // A write-only flush during another job's cleanup must not clear
            // this sync failure before the explicit recovery step.
            let progress = runtime
                .ctx
                .aof()
                .journal
                .as_ref()
                .unwrap()
                .appender
                .sync_handle();
            progress.complete_for_test(
                progress.written_stamp_for_test(),
                Err(std::io::Error::other("injected terminal sync failure")),
            );
        }
        "pending" => {
            runtime
                .ctx
                .config_mut()
                .replication
                .replica_output_buffer_hard_limit = 1;
            runtime.ctx.config_mut().server.repl_backlog_size = 1;
            let frame =
                crate::engine::distributed::replication::protocol::encode_command(&[b"PING"]);
            runtime.feed_aof(&frame);
            runtime.propagate_to_replicas(&frame);
        }
        _ => panic!("unknown durability fault"),
    }
    assert!(runtime.ctx.replication_aof_write_error().is_some());
}

fn recover_terminal_migration(runtime: &mut ServerWorkerState) {
    runtime.ctx.aof_mut().replication_baseline = crate::engine::persistence::AofBaseline::Ready;
    runtime.ctx.aof_mut().last_write_status = crate::engine::persistence::OperationStatus::Ok;
    runtime
        .ctx
        .aof_mut()
        .journal
        .as_mut()
        .unwrap()
        .appender
        .force_fsync()
        .unwrap();
    runtime.release_synced_replication();
    assert!(runtime.ctx.replication_aof_write_error().is_none());
}

#[test]
fn strict_terminal_import_durability_wait_does_not_hide_another_slots_timeout() {
    for fault in ["unavailable", "write-error", "pending"] {
        let keys = [
            b"{terminal}import".as_slice(),
            b"{stalled}import".as_slice(),
        ];
        let slots = keys.map(crate::engine::distributed::cluster::node::key_hash_slot);
        assert_ne!(slots[0], slots[1]);
        let (mut runtime, id, path) =
            terminal_migration_fixture(MigrationOperation::Import, &slots[..1]);
        runtime.ctx.config_mut().replication.repl_timeout = 2;
        let stalled_id =
            MigrationJobId::parse(b"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee").unwrap();
        let stalled_ranges =
            SlotRanges::new(vec![SlotRange::new(slots[1], slots[1]).unwrap()]).unwrap();
        let state = runtime.ctx.cluster_state_mut().as_deref_mut().unwrap();
        let first = state.migrations.get(&id).unwrap();
        let source = first.source.clone();
        state
            .migrations
            .start(MigrationJob::new(
                stalled_id.clone(),
                source.clone(),
                first.target.clone(),
                stalled_ranges.clone(),
                MigrationOperation::Import,
                crate::foundation::io::sys::time::unixtime(),
            ))
            .unwrap();
        state
            .set_slot_node(slots[1] as usize, source.as_str())
            .unwrap();
        state.finalize_slot_changes();
        runtime
            .ctx
            .create_slot_import(stalled_id.clone(), &stalled_ranges);
        for (job_id, key) in [(&id, keys[0]), (&stalled_id, keys[1])] {
            runtime
                .ctx
                .apply_slot_import(
                    job_id,
                    crate::engine::command::CommandArgs::from_parts(
                        [b"SET".as_slice(), key, b"staged"]
                            .into_iter()
                            .map(bytes::Bytes::copy_from_slice)
                            .collect(),
                    ),
                    &runtime.registry,
                )
                .unwrap();
        }
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(94));
        connection.last_interaction = std::time::Instant::now() - std::time::Duration::from_secs(3);
        connection.slot_migration_role =
            Some(SlotMigrationConnectionRole::Import(stalled_id.clone()));
        let stalled_token = runtime.connection_registry.insert(connection);
        let ranges = runtime
            .ctx
            .cluster_state()
            .as_deref()
            .unwrap()
            .migrations
            .get(&id)
            .unwrap()
            .ranges
            .clone();
        let staged = runtime.ctx.slot_import_snapshot(&id, &ranges).unwrap();
        terminal_migration_fault(&mut runtime, fault);

        for _ in 0..2 {
            runtime.drive_slot_migration_progress();
            let state = runtime.ctx.cluster_state().as_deref().unwrap();
            assert_eq!(
                state.migrations.get(&id).unwrap().phase,
                MigrationPhase::Import(
                    crate::engine::distributed::cluster::migration::ImportPhase::FailoverGranted
                )
            );
            assert_eq!(
                state.declared_node_by_slot(slots[0] as usize).unwrap().name,
                source.as_str()
            );
            assert_eq!(runtime.ctx.slot_import_key_count(0, slots[0] as usize), 1);
            assert_eq!(
                runtime.ctx.slot_import_snapshot(&id, &ranges).unwrap(),
                staged
            );
            assert_eq!(
                state.migrations.get(&stalled_id).unwrap().phase,
                MigrationPhase::Terminal(
                    crate::engine::distributed::cluster::migration::TerminalPhase::Failed
                )
            );
            assert_eq!(
                state
                    .migrations
                    .get(&stalled_id)
                    .unwrap()
                    .message
                    .as_deref(),
                Some("Timed out after too long with no interaction")
            );
            assert_eq!(runtime.ctx.slot_import_key_count(0, slots[1] as usize), 0);
            assert_eq!(
                runtime.connection_registry[Token(stalled_token)].state,
                ConnectionState::Closing
            );
            assert!(keys
                .iter()
                .all(|key| runtime.ctx.db(0).unwrap().get_no_expire(key).is_none()));
        }
        for (_, connection) in runtime.connection_registry.iter_mut() {
            connection.last_interaction =
                std::time::Instant::now() - std::time::Duration::from_secs(3);
        }
        recover_terminal_migration(&mut runtime);
        runtime.drive_slot_migration_progress();
        let state = runtime.ctx.cluster_state().as_deref().unwrap();
        assert_eq!(
            state.migrations.get(&id).unwrap().phase,
            MigrationPhase::Terminal(
                crate::engine::distributed::cluster::migration::TerminalPhase::Success
            )
        );
        assert_eq!(
            state.declared_node_by_slot(slots[0] as usize).unwrap().name,
            state.myself.name
        );
        assert_eq!(
            runtime
                .ctx
                .db(0)
                .unwrap()
                .get_no_expire(keys[0])
                .unwrap()
                .as_bytes(),
            b"staged"
        );
        assert!(runtime.ctx.db(0).unwrap().get_no_expire(keys[1]).is_none());
        drop(runtime);
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn strict_terminal_export_without_durability_wait_keeps_pause_timeout() {
    let key = b"{terminal}export";
    let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
    let (mut runtime, id, path) = terminal_migration_fixture(MigrationOperation::Export, &[slot]);
    runtime.ctx.db_mut(0).unwrap().set(
        key.to_vec(),
        crate::foundation::io::store::Robj::string("live"),
    );
    assert!(runtime.ctx.replication_aof_write_error().is_none());
    assert_eq!(runtime.ctx.replication().backlog.unreleased_bytes(), 0);
    runtime.ctx.client_pause_mut().end_time_ms = 1;
    let dirty = runtime.ctx.persistence().dirty();
    let offset = runtime.ctx.replication().backlog.reported_offset();

    runtime.drive_slot_migration_progress();

    let state = runtime.ctx.cluster_state().as_deref().unwrap();
    let job = state.migrations.get(&id).unwrap();
    assert_eq!(
        job.phase,
        MigrationPhase::Terminal(
            crate::engine::distributed::cluster::migration::TerminalPhase::Failed
        )
    );
    assert!(job
        .message
        .as_deref()
        .unwrap()
        .contains("Unpaused before migration completed"));
    assert_eq!(
        state.declared_node_by_slot(slot as usize).unwrap().name,
        job.target.as_str()
    );
    assert_eq!(
        runtime
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(key)
            .unwrap()
            .as_bytes(),
        b"live"
    );
    assert_eq!(runtime.ctx.persistence().dirty(), dirty);
    assert_eq!(runtime.ctx.replication().backlog.reported_offset(), offset);
    drop(runtime);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn strict_terminal_import_without_durability_wait_keeps_connection_timeout() {
    let key = b"{terminal}import";
    let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
    let (mut runtime, id, path) = terminal_migration_fixture(MigrationOperation::Import, &[slot]);
    runtime.ctx.config_mut().replication.repl_timeout = 2;
    runtime
        .ctx
        .apply_slot_import(
            &id,
            crate::engine::command::CommandArgs::from_parts(
                [b"SET".as_slice(), key, b"staged"]
                    .into_iter()
                    .map(bytes::Bytes::copy_from_slice)
                    .collect(),
            ),
            &runtime.registry,
        )
        .unwrap();
    for (_, connection) in runtime.connection_registry.iter_mut() {
        connection.last_interaction = std::time::Instant::now() - std::time::Duration::from_secs(3);
    }
    assert!(runtime.ctx.replication_aof_write_error().is_none());

    runtime.drive_slot_migration_progress();

    let state = runtime.ctx.cluster_state().as_deref().unwrap();
    assert_eq!(
        state.migrations.get(&id).unwrap().phase,
        MigrationPhase::Terminal(
            crate::engine::distributed::cluster::migration::TerminalPhase::Failed
        )
    );
    assert_ne!(
        state.declared_node_by_slot(slot as usize).unwrap().name,
        state.myself.name
    );
    assert_eq!(runtime.ctx.slot_import_key_count(0, slot as usize), 0);
    assert!(runtime.ctx.db(0).unwrap().get_no_expire(key).is_none());
    drop(runtime);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn strict_terminal_durability_wait_clears_on_explicit_failure() {
    for operation in [MigrationOperation::Import, MigrationOperation::Export] {
        let key = b"{terminal}cancel";
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
        let (mut runtime, id, path) = terminal_migration_fixture(operation, &[slot]);
        terminal_migration_fault(&mut runtime, "unavailable");
        runtime.drive_slot_migration_progress();
        assert!(runtime.slot_migration_durability_waits.contains(&id));

        runtime.fail_slot_migration(&id, "Canceled while waiting for local durability");

        assert!(runtime.slot_migration_durability_waits.is_empty());
        assert_eq!(
            runtime
                .ctx
                .cluster_state()
                .as_deref()
                .unwrap()
                .migrations
                .get(&id)
                .unwrap()
                .phase,
            MigrationPhase::Terminal(
                crate::engine::distributed::cluster::migration::TerminalPhase::Failed
            )
        );
        drop(runtime);
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn strict_terminal_import_preserves_staged_data_until_durability_recovers() {
    for fault in ["unavailable", "write-error", "pending"] {
        let key = b"{terminal}import";
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
        let (mut runtime, id, path) =
            terminal_migration_fixture(MigrationOperation::Import, &[slot]);
        runtime.ctx.config_mut().replication.repl_timeout = 2;
        runtime
            .ctx
            .apply_slot_import(
                &id,
                crate::engine::command::CommandArgs::from_parts(
                    [b"SET".as_slice(), key, b"staged"]
                        .into_iter()
                        .map(bytes::Bytes::copy_from_slice)
                        .collect(),
                ),
                &runtime.registry,
            )
            .unwrap();
        runtime.ctx.db_mut(0).unwrap().set(
            b"unrelated".to_vec(),
            crate::foundation::io::store::Robj::string("live"),
        );
        let ranges = runtime
            .ctx
            .cluster_state()
            .as_deref()
            .unwrap()
            .migrations
            .get(&id)
            .unwrap()
            .ranges
            .clone();
        let staged = runtime.ctx.slot_import_snapshot(&id, &ranges).unwrap();
        if fault == "write-error" {
            for (_, connection) in runtime.connection_registry.iter_mut() {
                connection.last_interaction =
                    std::time::Instant::now() - std::time::Duration::from_secs(120);
            }
        }
        terminal_migration_fault(&mut runtime, fault);
        let offset = runtime.ctx.replication().backlog.reported_offset();
        for _ in 0..2 {
            runtime.drive_slot_migration_progress();
            let state = runtime.ctx.cluster_state().as_deref().unwrap();
            assert_eq!(
                state.migrations.get(&id).unwrap().phase,
                MigrationPhase::Import(
                    crate::engine::distributed::cluster::migration::ImportPhase::FailoverGranted
                )
            );
            assert_ne!(
                state.declared_node_by_slot(slot as usize).unwrap().name,
                state.myself.name
            );
            assert_eq!(runtime.ctx.slot_import_key_count(0, slot as usize), 1);
            assert_eq!(
                runtime.ctx.slot_import_snapshot(&id, &ranges).unwrap(),
                staged
            );
            assert!(runtime.ctx.db(0).unwrap().get_no_expire(key).is_none());
            assert_eq!(runtime.ctx.replication().backlog.reported_offset(), offset);
            assert!(runtime.slot_migration_durability_waits.contains(&id));
            for (_, connection) in runtime.connection_registry.iter_mut() {
                connection.last_interaction =
                    std::time::Instant::now() - std::time::Duration::from_secs(3);
            }
        }
        recover_terminal_migration(&mut runtime);
        runtime.drive_slot_migration_progress();
        let state = runtime.ctx.cluster_state().as_deref().unwrap();
        assert_eq!(
            state.migrations.get(&id).unwrap().phase,
            MigrationPhase::Terminal(
                crate::engine::distributed::cluster::migration::TerminalPhase::Success
            )
        );
        assert_eq!(
            state.declared_node_by_slot(slot as usize).unwrap().name,
            state.myself.name
        );
        assert!(runtime.slot_migration_durability_waits.is_empty());
        assert_eq!(runtime.ctx.slot_import_key_count(0, slot as usize), 0);
        assert_eq!(
            runtime
                .ctx
                .db(0)
                .unwrap()
                .get_no_expire(key)
                .unwrap()
                .as_bytes(),
            b"staged"
        );
        assert_eq!(
            runtime
                .ctx
                .db(0)
                .unwrap()
                .get_no_expire(b"unrelated")
                .unwrap()
                .as_bytes(),
            b"live"
        );
        let offset = runtime.ctx.replication().backlog.reported_offset();
        runtime.drive_slot_migration_progress();
        assert_eq!(runtime.ctx.replication().backlog.reported_offset(), offset);
        drop(runtime);
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn strict_terminal_export_preserves_live_data_until_durability_recovers() {
    for fault in ["unavailable", "write-error", "pending"] {
        let key = b"{terminal}export";
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
        let (mut runtime, id, path) =
            terminal_migration_fixture(MigrationOperation::Export, &[slot]);
        runtime.ctx.db_mut(0).unwrap().set(
            key.to_vec(),
            crate::foundation::io::store::Robj::string("live"),
        );
        terminal_migration_fault(&mut runtime, fault);
        if fault != "unavailable" {
            runtime.ctx.client_pause_mut().end_time_ms = 1;
        }
        let offset = runtime.ctx.replication().backlog.reported_offset();
        let dirty = runtime.ctx.persistence().dirty();
        for _ in 0..2 {
            runtime.drive_slot_migration_progress();
            assert_eq!(
                runtime
                    .ctx
                    .cluster_state()
                    .as_deref()
                    .unwrap()
                    .migrations
                    .get(&id)
                    .unwrap()
                    .phase,
                MigrationPhase::Export(ExportPhase::FailoverGranted)
            );
            assert_eq!(
                runtime
                    .ctx
                    .db(0)
                    .unwrap()
                    .get_no_expire(key)
                    .unwrap()
                    .as_bytes(),
                b"live"
            );
            assert_eq!(runtime.ctx.replication().backlog.reported_offset(), offset);
            assert_eq!(runtime.ctx.persistence().dirty(), dirty);
            assert!(runtime.slot_migration_durability_waits.contains(&id));
            runtime.ctx.client_pause_mut().end_time_ms = 1;
        }
        recover_terminal_migration(&mut runtime);
        runtime.drive_slot_migration_progress();
        // A single slot may fill the pending budget; completing cleanup must remain retryable.
        recover_terminal_migration(&mut runtime);
        runtime.drive_slot_migration_progress();
        assert_eq!(
            runtime
                .ctx
                .cluster_state()
                .as_deref()
                .unwrap()
                .migrations
                .get(&id)
                .unwrap()
                .phase,
            MigrationPhase::Terminal(
                crate::engine::distributed::cluster::migration::TerminalPhase::Success
            )
        );
        assert!(runtime.ctx.db(0).unwrap().get_no_expire(key).is_none());
        assert!(runtime.slot_migration_durability_waits.is_empty());
        assert_eq!(runtime.ctx.persistence().dirty(), dirty + 1);
        let offset = runtime.ctx.replication().backlog.reported_offset();
        runtime.drive_slot_migration_progress();
        assert_eq!(runtime.ctx.replication().backlog.reported_offset(), offset);
        assert_eq!(runtime.ctx.persistence().dirty(), dirty + 1);
        drop(runtime);
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn strict_terminal_export_rechecks_pending_between_slots_and_jobs() {
    let mut keys = [b"{first}key".as_slice(), b"{second}key".as_slice()];
    keys.sort_by_key(|key| crate::engine::distributed::cluster::node::key_hash_slot(key));
    for separate_jobs in [false, true] {
        let slots = keys.map(crate::engine::distributed::cluster::node::key_hash_slot);
        let (mut runtime, id, path) = terminal_migration_fixture(
            MigrationOperation::Export,
            if separate_jobs { &slots[..1] } else { &slots },
        );
        let mut ids = vec![id.clone()];
        if separate_jobs {
            let state = runtime.ctx.cluster_state_mut().as_deref_mut().unwrap();
            let first = state.migrations.get(&id).unwrap();
            let second_id =
                MigrationJobId::parse(b"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee").unwrap();
            let mut job = MigrationJob::new(
                second_id.clone(),
                first.source.clone(),
                first.target.clone(),
                SlotRanges::new(vec![SlotRange::new(slots[1], slots[1]).unwrap()]).unwrap(),
                MigrationOperation::Export,
                crate::foundation::io::sys::time::unixtime(),
            );
            job.phase = MigrationPhase::Export(ExportPhase::FailoverGranted);
            let target = job.target.clone();
            state.migrations.start(job).unwrap();
            state
                .set_slot_node(slots[1] as usize, target.as_str())
                .unwrap();
            state.finalize_slot_changes();
            ids.push(second_id);
        }
        for key in keys {
            runtime.ctx.db_mut(0).unwrap().set(
                key.to_vec(),
                crate::foundation::io::store::Robj::string("live"),
            );
        }
        runtime
            .ctx
            .config_mut()
            .replication
            .replica_output_buffer_hard_limit = 1;
        runtime.ctx.config_mut().server.repl_backlog_size = 1;
        runtime.drive_slot_migration_progress();
        assert!(runtime.ctx.replication_aof_write_error().is_some());
        assert_eq!(
            keys.iter()
                .filter(|key| runtime.ctx.db(0).unwrap().get_no_expire(key).is_some())
                .count(),
            1
        );
        assert!(ids.iter().all(|id| !runtime
            .ctx
            .cluster_state()
            .as_deref()
            .unwrap()
            .migrations
            .get(id)
            .unwrap()
            .phase
            .is_terminal()));
        assert!(ids
            .iter()
            .all(|id| runtime.slot_migration_durability_waits.contains(id)));
        runtime.ctx.client_pause_mut().end_time_ms = 1;
        let offset = runtime.ctx.replication().backlog.reported_offset();
        runtime.drive_slot_migration_progress();
        assert_eq!(runtime.ctx.replication().backlog.reported_offset(), offset);
        recover_terminal_migration(&mut runtime);
        runtime.drive_slot_migration_progress();
        recover_terminal_migration(&mut runtime);
        runtime.drive_slot_migration_progress();
        assert!(keys
            .iter()
            .all(|key| runtime.ctx.db(0).unwrap().get_no_expire(key).is_none()));
        assert!(ids.iter().all(|id| runtime
            .ctx
            .cluster_state()
            .as_deref()
            .unwrap()
            .migrations
            .get(id)
            .unwrap()
            .phase
            == MigrationPhase::Terminal(
                crate::engine::distributed::cluster::migration::TerminalPhase::Success
            )));
        assert_eq!(runtime.ctx.persistence().dirty(), 2);
        assert!(runtime.slot_migration_durability_waits.is_empty());
        recover_terminal_migration(&mut runtime);
        let mut decoder = crate::foundation::io::persistence::AofDecoder::new();
        let bytes = std::fs::read(&path).unwrap();
        let mut deleted = Vec::new();
        loop {
            match decoder.step(&bytes).unwrap() {
                crate::foundation::io::persistence::AofStep::Command(parts)
                    if parts[0] == b"UNLINK" =>
                {
                    deleted.push(parts[1].clone())
                }
                crate::foundation::io::persistence::AofStep::Command(_) => {}
                crate::foundation::io::persistence::AofStep::Finished(_) => break,
                other => panic!("unexpected AOF step: {other:?}"),
            }
        }
        deleted.sort();
        let mut expected = keys.map(<[u8]>::to_vec);
        expected.sort();
        assert_eq!(deleted, expected);
        let offset = runtime.ctx.replication().backlog.reported_offset();
        runtime.drive_slot_migration_progress();
        assert_eq!(runtime.ctx.replication().backlog.reported_offset(), offset);
        drop(runtime);
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn terminal_migration_default_off_does_not_require_a_durable_aof() {
    for operation in [MigrationOperation::Import, MigrationOperation::Export] {
        let key = b"{terminal}default-off";
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
        let (mut runtime, id, path) = terminal_migration_fixture(operation, &[slot]);
        // This test isolates the legacy admission policy from connection timeouts.
        runtime.ctx.config_mut().replication.repl_timeout = 0;
        if operation == MigrationOperation::Import {
            runtime
                .ctx
                .apply_slot_import(
                    &id,
                    crate::engine::command::CommandArgs::from_parts(
                        [b"SET".as_slice(), key, b"staged"]
                            .into_iter()
                            .map(bytes::Bytes::copy_from_slice)
                            .collect(),
                    ),
                    &runtime.registry,
                )
                .unwrap();
        } else {
            runtime.ctx.db_mut(0).unwrap().set(
                key.to_vec(),
                crate::foundation::io::store::Robj::string("live"),
            );
        }
        terminal_migration_fault(&mut runtime, "unavailable");
        runtime.ctx.config_mut().replication.repl_after_aof_fsync = false;
        runtime.drive_slot_migration_progress();
        assert_eq!(
            runtime
                .ctx
                .cluster_state()
                .as_deref()
                .unwrap()
                .migrations
                .get(&id)
                .unwrap()
                .phase,
            MigrationPhase::Terminal(
                crate::engine::distributed::cluster::migration::TerminalPhase::Success
            )
        );
        assert_eq!(
            runtime.ctx.db(0).unwrap().get_no_expire(key).is_some(),
            operation == MigrationOperation::Import
        );
        drop(runtime);
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn export_pause_is_installed_and_timeout_fails_the_job() {
    use crate::framework::context::PauseAction;

    let mut runtime = ServerWorkerState::new(1).unwrap();
    runtime
        .ctx
        .config_mut()
        .server
        .cluster_manual_failover_timeout = 100;
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let source = NodeId::parse(state.myself.name.as_bytes()).unwrap();
    let target = NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
    let id = MigrationJobId::parse(b"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee").unwrap();
    let now = crate::foundation::io::sys::time::unixtime();
    let job = MigrationJob::new(
        id.clone(),
        source,
        target,
        SlotRanges::new(vec![SlotRange::new(1, 1).unwrap()]).unwrap(),
        MigrationOperation::Export,
        now,
    );
    state.migrations.start(job).unwrap();
    for event in [
        crate::engine::distributed::cluster::migration::MigrationEvent::Connected {
            auth_required: false,
        },
        crate::engine::distributed::cluster::migration::MigrationEvent::EstablishCommandSent,
        crate::engine::distributed::cluster::migration::MigrationEvent::Established,
        crate::engine::distributed::cluster::migration::MigrationEvent::SnapshotStarted,
        crate::engine::distributed::cluster::migration::MigrationEvent::SnapshotFinished,
        crate::engine::distributed::cluster::migration::MigrationEvent::PauseRequested,
    ] {
        state.migrations.transition(&id, event, now).unwrap();
    }
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));

    runtime.drive_slot_migration_progress();

    assert_eq!(runtime.ctx.client_pause().action, PauseAction::Write);
    assert_eq!(
        runtime.ctx.client_pause().reason,
        "slot_migration_in_progress"
    );
    assert_eq!(
        runtime
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .migrations
            .get(&id)
            .unwrap()
            .phase,
        MigrationPhase::Export(ExportPhase::FailoverPaused)
    );

    runtime
        .ctx
        .cluster_state_mut()
        .as_deref_mut()
        .unwrap()
        .migrations
        .transition(
            &id,
            crate::engine::distributed::cluster::migration::MigrationEvent::FailoverGranted,
            now,
        )
        .unwrap();
    runtime.ctx.client_pause_mut().end_time_ms = 1;
    runtime.drive_slot_migration_progress();

    let state = runtime.ctx.cluster_state().as_ref().unwrap();
    let job = state.migrations.get(&id).unwrap();
    assert_eq!(
        job.phase,
        MigrationPhase::Terminal(
            crate::engine::distributed::cluster::migration::TerminalPhase::Failed
        )
    );
    assert!(job
        .message
        .as_deref()
        .unwrap()
        .contains("Unpaused before migration completed"));
    assert_eq!(runtime.ctx.client_pause().action, PauseAction::None);
    assert_eq!(runtime.ctx.client_pause().reason, "none");
}

#[test]
fn export_pause_waits_for_the_configured_replication_byte_limit() {
    use crate::framework::context::PauseAction;

    let mut runtime = ServerWorkerState::new(1).unwrap();
    runtime
        .ctx
        .config_mut()
        .server
        .cluster_manual_failover_timeout = 100;
    runtime
        .ctx
        .config_mut()
        .server
        .slot_migration_max_failover_repl_bytes = 1;
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let source = NodeId::parse(state.myself.name.as_bytes()).unwrap();
    let target = NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
    let id = MigrationJobId::parse(b"abababababababababababababababababababab").unwrap();
    let now = crate::foundation::io::sys::time::unixtime();
    let job = MigrationJob::new(
        id.clone(),
        source,
        target,
        SlotRanges::new(vec![SlotRange::new(1, 1).unwrap()]).unwrap(),
        MigrationOperation::Export,
        now,
    );
    state.migrations.start(job).unwrap();
    for event in [
        MigrationEvent::Connected {
            auth_required: false,
        },
        MigrationEvent::EstablishCommandSent,
        MigrationEvent::Established,
        MigrationEvent::SnapshotStarted,
        MigrationEvent::SnapshotFinished,
        MigrationEvent::PauseRequested,
    ] {
        state.migrations.transition(&id, event, now).unwrap();
    }
    state.set_migration_remaining_repl_size(&id, 2);
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));

    runtime.drive_slot_migration_progress();

    assert_eq!(runtime.ctx.client_pause().action, PauseAction::None);
    assert_eq!(
        runtime
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .migrations
            .get(&id)
            .unwrap()
            .phase,
        MigrationPhase::Export(ExportPhase::WaitingToPause)
    );

    runtime
        .ctx
        .config_mut()
        .server
        .slot_migration_max_failover_repl_bytes = 2;
    runtime.drive_slot_migration_progress();

    assert_eq!(runtime.ctx.client_pause().action, PauseAction::Write);
    assert_eq!(
        runtime
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .migrations
            .get(&id)
            .unwrap()
            .phase,
        MigrationPhase::Export(ExportPhase::FailoverPaused)
    );
}

#[test]
fn stalled_import_uses_last_incoming_connection_activity() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    runtime.ctx.config_mut().replication.repl_timeout = 2;
    let now = crate::foundation::io::sys::time::unixtime();
    let id = MigrationJobId::parse(b"ffffffffffffffffffffffffffffffffffffffff").unwrap();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
    let job = MigrationJob::new(
        id.clone(),
        NodeId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
        NodeId::parse(state.myself.name.as_bytes()).unwrap(),
        SlotRanges::new(vec![SlotRange::new(1, 1).unwrap()]).unwrap(),
        MigrationOperation::Import,
        now,
    );
    state.migrations.start(job).unwrap();
    *runtime.ctx.cluster_state_mut() = Some(Box::new(state));
    let token = Token(94);
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(token);
    connection.last_interaction = std::time::Instant::now() - std::time::Duration::from_secs(3);
    connection.slot_migration_role = Some(SlotMigrationConnectionRole::Import(id.clone()));
    let _connection_token = runtime.connection_registry.insert(connection);

    runtime.drive_slot_migration_progress();

    let job = runtime
        .ctx
        .cluster_state()
        .as_ref()
        .unwrap()
        .migrations
        .get(&id)
        .unwrap();
    assert_eq!(
        job.phase,
        MigrationPhase::Terminal(
            crate::engine::distributed::cluster::migration::TerminalPhase::Failed
        )
    );
    assert_eq!(
        job.message.as_deref(),
        Some("Timed out after too long with no interaction")
    );
}
