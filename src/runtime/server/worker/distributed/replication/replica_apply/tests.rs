use super::*;
use crate::engine::command::context::{BlockedCmd, BlockedState, WatchedKey};
use crate::engine::distributed::cluster::migration::{MigrationJob, MigrationJobId, SlotRanges};
use crate::engine::distributed::cluster::{ClusterState, NodeId, SlotRange};
use crate::foundation::io::store::structure::list::ListValue;
use crate::foundation::io::store::Robj;
use crate::framework::capabilities::connection::Connection;
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::{ConnectionId, MasterInfo, ProvisionalPrimary};
use crate::runtime::server::worker::flows::request::dispatch::CommandEffects;
use crate::runtime::server::worker::flows::request::invocation::CommandTarget;
use crate::runtime::server::worker::flows::request::outcome::{
    CommandDisposition, SpecialCommandResult,
};
use std::collections::VecDeque;
use std::io::Write;
use std::net::{TcpListener, TcpStream};

fn offset(value: i64) -> ReplicationOffset {
    ReplicationOffset::try_from(value).expect("valid test offset")
}

fn add_watching_client(
    backend: &mut ServerWorkerState,
    token: Token,
    db_id: u32,
    key: Bytes,
    existed: bool,
) -> Token {
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(token);
    connection.set_custom_reply_enabled(false);
    connection
        .transaction
        .watch_many(vec![WatchedKey::new(db_id, key, existed)]);
    assert!(connection.transaction.begin(i64::from(db_id), 0));
    backend.connection_registry.insert(connection);
    token
}

fn add_blocked_list_client(backend: &mut ServerWorkerState, token: Token, db_id: u32, key: Bytes) {
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"BLPOP"),
        key.clone(),
        Bytes::from_static(b"0"),
    ]);
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(token);
    connection.selected_db = db_id;
    connection.blocked = Some(BlockedState {
        cmd: BlockedCmd::Blpop,
        keys: vec![key.clone()],
        timeout_ms: 0,
        start_time_ms: crate::foundation::io::sys::time::mstime(),
        orig_args: args,
        req_bytes: 32,
        orig_wrapped: None,
    });
    backend.connection_registry.insert(connection);
    let session = backend.connection_registry[token].session_id();
    backend
        .ctx
        .blocking_registry_mut()
        .register_client(db_id, &[key], session);
}

struct ReplicaSwapdbFixture {
    backend: ServerWorkerState,
    connection_token: usize,
    link: ReplicaLink,
    affected: Token,
    unaffected: Token,
    blocked: Token,
    watched_key: Bytes,
}

fn replica_swapdb_fixture() -> ReplicaSwapdbFixture {
    let mut backend = ServerWorkerState::new(3).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let watched_key = Bytes::from_static(b"watched");
    let list = ListValue::from_vecdeque(VecDeque::from([Bytes::from_static(b"value")]));
    backend
        .ctx
        .db_mut(1)
        .unwrap()
        .set(watched_key.to_vec(), Robj::ListQuicklist(list));
    let affected = add_watching_client(&mut backend, Token(74), 0, watched_key.clone(), false);
    let unaffected = add_watching_client(&mut backend, Token(75), 2, watched_key.clone(), false);
    let blocked = Token(76);
    add_blocked_list_client(&mut backend, blocked, 0, watched_key.clone());
    let (connection_token, main, _peer) = add_live_replay_connection(&mut backend);
    let link = ReplicaLink::new(
        main,
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    ReplicaSwapdbFixture {
        backend,
        connection_token,
        link,
        affected,
        unaffected,
        blocked,
        watched_key,
    }
}

fn assert_replica_swapdb_outcome(
    backend: &mut ServerWorkerState,
    affected: Token,
    unaffected: Token,
    blocked: Token,
    watched_key: &[u8],
) {
    backend.process_ready_keys();
    assert!(backend.connection_registry[blocked].blocked.is_none());
    assert!(backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(watched_key)
        .is_none());
    assert!(backend.connection_registry[affected]
        .transaction
        .is_watch_dirty());
    assert!(!backend.connection_registry[unaffected]
        .transaction
        .is_watch_dirty());
    let mut effects = CommandEffects::default();
    assert_eq!(
        backend.try_handle_exec(CommandTarget::new(affected), "exec", false, &mut effects),
        SpecialCommandResult::Handled(CommandDisposition::Completed)
    );
    assert_eq!(effects.replies(), &[b"*-1\r\n".to_vec()]);
    assert!(backend.connection_registry[unaffected]
        .transaction
        .is_active());
}

#[test]
fn hosted_replica_apply_handles_upstream_removal_before_replacement() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_command_registry_snapshot(&backend.registry);
    backend.ctx.config_mut().server.lua_time_limit = 1;
    backend.ctx.config_mut().replication.replica_read_only = false;
    let (connection_token, main, _primary_peer) = add_live_replay_connection(&mut backend);
    let (controller_token, _controller_main, mut controller_peer) =
        add_live_replay_connection(&mut backend);
    let target_id = backend.connection_registry[Token(connection_token)]
        .client_id()
        .to_string();
    let commands = format!(
        "*4\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$2\r\nID\r\n${}\r\n{}\r\n*2\r\n$6\r\nSCRIPT\r\n$4\r\nKILL\r\n",
        target_id.len(),
        target_id
    );
    controller_peer
        .write_all(commands.as_bytes())
        .expect("queue upstream removal and script kill");
    let mut link = ReplicaLink::new(
        main,
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"EVAL"),
        Bytes::from_static(b"while true do end"),
        Bytes::from_static(b"0"),
    ]);

    let result = backend.apply_replication_command(
        connection_token,
        &mut link,
        ReplicationApply::Command(args),
        CommandOrigin::Replication,
    );

    assert!(result.is_err());
    assert!(!backend
        .connection_registry
        .contains(Token(connection_token)));
    assert!(backend
        .connection_registry
        .contains(Token(controller_token)));
    let (replacement_token, _, _replacement_peer) = add_live_replay_connection(&mut backend);
    assert_ne!(replacement_token, connection_token);
    assert!(backend
        .connection_registry
        .contains(Token(replacement_token)));
}

#[test]
#[should_panic(expected = "panicked applying unknown command")]
fn readonly_replica_panics_on_unknown_primary_command_when_configured() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().replication.replica_read_only = true;
    backend
        .ctx
        .config_mut()
        .replication
        .propagation_error_behavior = "panic".to_string();
    let token = Token(97);
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(token);
    let connection_token = backend.connection_registry.insert(connection);
    let mut link = ReplicaLink::new(
        ConnectionId(token.0),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let args = CommandArgs::from_parts(vec![Bytes::from_static(b"UNKNOWN-PRIMARY-COMMAND")]);

    let _ = backend.apply_replication_command(
        connection_token,
        &mut link,
        ReplicationApply::Command(args),
        CommandOrigin::Replication,
    );
}

#[test]
#[should_panic(expected = "panicked applying an error from its primary")]
fn readonly_replica_panics_on_known_command_error_when_configured() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().replication.replica_read_only = true;
    backend
        .ctx
        .config_mut()
        .replication
        .propagation_error_behavior = "panic-on-replicas".to_string();
    let token = Token(98);
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(token);
    let connection_token = backend.connection_registry.insert(connection);
    let mut link = ReplicaLink::new(
        ConnectionId(token.0),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    backend.ctx.db_mut(0).expect("DB 0").set(
        b"not-an-integer".to_vec(),
        crate::foundation::io::store::Robj::string("value"),
    );
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"INCR"),
        Bytes::from_static(b"not-an-integer"),
    ]);

    let _ = backend.apply_replication_command(
        connection_token,
        &mut link,
        ReplicationApply::Command(args),
        CommandOrigin::Replication,
    );
}

fn add_live_replay_connection(backend: &mut ServerWorkerState) -> (usize, ConnectionId, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind replay test peer");
    let peer_addr = listener.local_addr().expect("replay test peer address");
    let connected = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect replay test transport");
    let (peer, _) = listener.accept().expect("accept replay test transport");
    let main = ConnectionId(connected.token.0);
    backend.add_connection(connected);
    let connection_token = main.0;
    (connection_token, main, peer)
}

fn loaded_replay_link(main: ConnectionId, received_offset: i64) -> ReplicaLink {
    let replid = "0123456789abcdef0123456789abcdef01234567".to_string();
    let mut link = ReplicaLink::streaming(
        main,
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        replid.clone(),
        offset(received_offset),
    );
    for state in [
        ReplicaRdbChannelState::Connecting,
        ReplicaRdbChannelState::Handshake,
        ReplicaRdbChannelState::WaitingForEndOffset,
        ReplicaRdbChannelState::ReceivingRdb,
        ReplicaRdbChannelState::RdbLoaded,
    ] {
        link.transition_rdb_channel_state(state).unwrap();
    }
    link.provisional_primary = Some(ProvisionalPrimary {
        replid,
        end_offset: offset(100),
        db_id: 0,
        rdb_client_id: 0,
    });
    link
}

#[test]
fn full_sync_deletes_received_rdb_when_persistence_is_disabled() {
    let dir = std::env::temp_dir().join(format!(
        "valkey-rdb-del-sync-files-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    backend.ctx.config_mut().persistence.dbfilename = "dump.rdb".to_string();
    backend.ctx.config_mut().persistence.rdb_del_sync_files = true;
    backend.ctx.config_mut().persistence.saveparams.clear();
    let token = Token(96);
    let mut conn = Connection::new_fake();
    conn.set_token_for_test(token);
    let connection_token = backend.connection_registry.insert(conn);
    let mut link = ReplicaLink::streaming(
        ConnectionId(token.0),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "0123456789abcdef0123456789abcdef01234567".to_string(),
        offset(0),
    );
    let incoming = crate::foundation::io::store::kv_engine::DbSet::new(1);
    let raw = crate::foundation::io::persistence::serialize_all(&incoming, 0);
    let commit = crate::framework::distributed::replication::RdbCommit {
        function_codes: Vec::new(),
        aux_fields: Vec::new(),
        repl_stream_db: 0,
        diskless: true,
        disk_backed: true,
        raw: Some(raw),
        swapped: Some(incoming),
    };

    backend
        .commit_streamed_rdb(connection_token, &mut link, commit)
        .expect("full sync commit succeeds");
    assert!(!dir.join("dump.rdb").exists());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn received_sync_rdb_deletion_requires_all_persistence_to_be_disabled() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().persistence.rdb_del_sync_files = true;
    assert!(!backend.should_delete_received_sync_rdb());

    backend.ctx.config_mut().persistence.saveparams.clear();
    assert!(backend.should_delete_received_sync_rdb());

    backend.ctx.config_mut().persistence.aof_enabled = true;
    assert!(!backend.should_delete_received_sync_rdb());
}

#[test]
fn pending_replay_continuation_transfers_to_same_token_replacement() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let (connection_token, main, _peer) = add_live_replay_connection(&mut backend);
    let original = loaded_replay_link(main, 101);
    let original_session_id = original.session_id();
    backend.replication_runtime.set_replica_link(original);
    let mut pending = PendingReplicationBuffer::new(1 << 20, offset(100), 0);
    pending
        .push(ReplicaEvent::Apply {
            apply: ReplicationApply::Command(CommandArgs::from_parts(vec![
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"stale"),
                Bytes::from_static(b"value"),
            ])),
            end_offset: offset(101),
        })
        .unwrap();
    let _ = backend
        .replication_runtime
        .set_replica_pending_buffer(original_session_id, pending);
    backend.pending_replication_replay = Some(PendingReplayContinuation::new_for_test(
        main,
        original_session_id,
        crate::foundation::io::sys::time::mstime(),
    ));
    backend.pause_replica_main_reads(connection_token).unwrap();
    backend.transport.clear_interest_requests_for_test();

    let replacement = loaded_replay_link(main, 201);
    let replacement_session_id = replacement.session_id();
    backend.replication_runtime.set_replica_link(replacement);
    let mut replacement_pending = PendingReplicationBuffer::new(1 << 20, offset(200), 0);
    replacement_pending
        .push(ReplicaEvent::Apply {
            apply: ReplicationApply::Ping,
            end_offset: offset(201),
        })
        .unwrap();
    let displaced = backend
        .replication_runtime
        .set_replica_pending_buffer(replacement_session_id, replacement_pending);
    backend.dispose_replica_pending_buffer(displaced);
    backend.drive_one_pending_replication_replay();

    assert_eq!(
        backend
            .replication_runtime
            .replica_link()
            .unwrap()
            .session_id(),
        replacement_session_id
    );
    assert!(backend
        .ctx
        .db(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .selected_db)
        .unwrap()
        .get_no_expire(b"stale")
        .is_none());
    assert_eq!(
        backend
            .replication_runtime
            .replica_pending_buffer()
            .unwrap()
            .snapshot_end_offset(),
        offset(200)
    );
    assert_eq!(
        backend
            .pending_replication_replay
            .as_ref()
            .map(|continuation| continuation.session_id),
        Some(replacement_session_id),
        "a loaded same-token replacement must receive its own replay continuation"
    );
    assert!(backend.replication_runtime.replica_main_read_paused());
    assert!(backend
        .ctx
        .db(backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .selected_db)
        .unwrap()
        .get_no_expire(b"stale")
        .is_none());

    backend.drive_one_pending_replication_replay();

    assert!(backend.pending_replication_replay.is_none());
    assert!(!backend.replication_runtime.replica_main_read_paused());
    assert_eq!(
        backend.transport.last_set_interest_calls_for_test().last(),
        Some(&(
            Token(main.0),
            crate::foundation::io::transport::Interest::ReadWritable
        )),
        "a same-token replacement must not inherit the old replay's read pause"
    );
}

#[test]
fn pending_replay_abort_preserves_replacement_after_nested_service() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let (_connection_token, main, _peer) = add_live_replay_connection(&mut backend);
    let original = loaded_replay_link(main, 101);
    let original_session_id = original.session_id();
    backend.replication_runtime.set_replica_link(original);
    let old_buffer = PendingReplicationBuffer::new(1 << 20, offset(100), 0);
    let _ = backend
        .replication_runtime
        .set_replica_pending_buffer(original_session_id, old_buffer);
    backend.pending_replication_replay = Some(PendingReplayContinuation::new_for_test(
        main,
        original_session_id,
        crate::foundation::io::sys::time::mstime(),
    ));

    let replacement = loaded_replay_link(main, 201);
    let replacement_session_id = replacement.session_id();
    backend.replication_runtime.set_replica_link(replacement);
    let new_buffer = PendingReplicationBuffer::new(1 << 20, offset(200), 0);
    let displaced = backend
        .replication_runtime
        .set_replica_pending_buffer(replacement_session_id, new_buffer);
    backend.dispose_replica_pending_buffer(displaced);
    backend.abort_pending_replication_replay(main, original_session_id);

    assert_eq!(
        backend
            .replication_runtime
            .replica_link()
            .unwrap()
            .session_id(),
        replacement_session_id
    );
    assert_eq!(
        backend
            .replication_runtime
            .replica_pending_buffer()
            .unwrap()
            .snapshot_end_offset(),
        offset(200)
    );
    assert!(backend.pending_replication_replay.is_none());
    assert!(backend.transport.is_registered(Token(main.0)));
}

#[test]
fn handoff_and_outer_loop_drive_only_one_pending_replay_unit() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend
        .ctx
        .config_mut()
        .persistence
        .loading_process_events_interval_bytes = 1;
    backend.ctx.config_mut().server.enable_debug_command =
        crate::engine::extension::config::server::ProtectedAction::Yes;
    let (connection_token, main, _peer) = add_live_replay_connection(&mut backend);
    let mut link = loaded_replay_link(main, 102);
    let mut pending = PendingReplicationBuffer::new(1 << 20, offset(100), 0);
    for event in [
        ReplicaEvent::Apply {
            apply: ReplicationApply::Command(CommandArgs::from_parts(vec![
                Bytes::from_static(b"DEBUG"),
                Bytes::from_static(b"SLEEP"),
                Bytes::from_static(b"0.11"),
            ])),
            end_offset: offset(101),
        },
        ReplicaEvent::Apply {
            apply: ReplicationApply::Command(CommandArgs::from_parts(vec![
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"after-yield"),
                Bytes::from_static(b"value"),
            ])),
            end_offset: offset(102),
        },
    ] {
        pending.push(event).unwrap();
    }
    let _ = backend
        .replication_runtime
        .set_replica_pending_buffer(link.session_id(), pending);

    backend
        .commit_and_replay_dual_handoff_only(connection_token, &mut link)
        .unwrap();

    assert!(
        backend.replication_runtime.replica_main_read_paused(),
        "C disables the primary read handler before replaying pending replication data"
    );
    assert_eq!(
        backend
            .replication_runtime
            .replica_pending_buffer()
            .unwrap()
            .len(),
        2,
        "handoff readiness must only schedule the continuation"
    );
    backend.replication_runtime.set_replica_link(link);
    backend.drive_one_pending_replication_replay();
    assert_eq!(
        backend
            .replication_runtime
            .replica_pending_buffer()
            .unwrap()
            .len(),
        1,
        "the outer loop must drive exactly one bounded replay unit"
    );
    assert!(backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"after-yield")
        .is_none());

    backend.drive_one_pending_replication_replay();
    assert!(backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"after-yield")
        .is_some());
    assert!(
        !backend.replication_runtime.replica_main_read_paused(),
        "main-channel reads resume only after the pending buffer is empty"
    );
}

#[test]
fn pending_replay_readable_error_aborts_immediately() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let (_connection_token, main, _peer) = add_live_replay_connection(&mut backend);
    let link = loaded_replay_link(main, 101);
    let session_id = link.session_id();
    backend.replication_runtime.set_replica_link(link);
    let _ = backend.replication_runtime.set_replica_pending_buffer(
        session_id,
        PendingReplicationBuffer::new(1 << 20, offset(100), 0),
    );
    backend.pending_replication_replay = Some(PendingReplayContinuation::new_for_test(
        main,
        session_id,
        crate::foundation::io::sys::time::mstime(),
    ));
    backend.transport.queue_test_transport_event_batch(&[(
        Token(main.0),
        true,
        false,
        false,
        true,
    )]);

    let result = backend.process_events_while_blocked(BlockedOperation::PendingReplay { main });

    assert_eq!(result.dispatched_events, 1);
    assert_eq!(result.deferred_events, 0);
    assert!(backend.blocked_events.pop_front_deferred().is_none());
    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(backend.pending_replication_replay.is_none());
}

#[test]
fn terminal_shutdown_during_pending_replay_aborts_resume() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let (_connection_token, main, _peer) = add_live_replay_connection(&mut backend);
    let mut link = loaded_replay_link(main, 101);
    backend.ctx.set_shutdown_requested(true);

    assert!(matches!(
        backend.service_pending_replay_events(&mut link),
        ResumeDecision::Aborted
    ));
    assert!(backend.replication_runtime.replica_link().is_some());
}

#[test]
fn pending_replay_newline_failure_does_not_poll_events() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend
        .ctx
        .config_mut()
        .persistence
        .loading_process_events_interval_bytes = 1;
    let (_connection_token, main, _peer) = add_live_replay_connection(&mut backend);
    let link = loaded_replay_link(main, 102);
    let session_id = link.session_id();
    backend.replication_runtime.set_replica_link(link);
    let mut pending = PendingReplicationBuffer::new(1 << 20, offset(100), 0);
    for end_offset in [101, 102] {
        pending
            .push(ReplicaEvent::Apply {
                apply: ReplicationApply::Ping,
                end_offset: offset(end_offset),
            })
            .unwrap();
    }
    let _ = backend
        .replication_runtime
        .set_replica_pending_buffer(session_id, pending);
    backend.pending_replication_replay = Some(PendingReplayContinuation::new_for_test(
        main,
        session_id,
        crate::foundation::io::sys::time::mstime().saturating_sub(101),
    ));
    backend
        .transport
        .deregister_for_test(Token(main.0))
        .unwrap();
    backend.transport.queue_test_transport_event_batch(&[(
        Token(usize::MAX - 1),
        false,
        true,
        false,
        false,
    )]);

    backend.drive_one_pending_replication_replay();

    let unpolled = backend
        .transport
        .poll_events(Some(std::time::Duration::ZERO))
        .unwrap();
    assert_eq!(unpolled.len(), 1);
    assert_eq!(unpolled[0].token, Token(usize::MAX - 1));
    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(backend.pending_replication_replay.is_none());
}

#[test]
fn ping_from_primary_is_relayed_to_subreplicas() {
    // A chained replica must forward the primary's PING downstream so its
    // sub-replicas advance their replication offset in lockstep. Otherwise
    // wait_for_ofs_sync between a replica and its sub-replica never
    // converges (integration/replication-multi-cmd chained setup).
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use crate::framework::distributed::replication::{ConnectionId, MasterInfo};

    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.replication_mut().backlog.activate();
    let token_value = 71usize;
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(Token(token_value));
    let connection_token = backend.connection_registry.insert(connection);

    let mut link = ReplicaLink::new(
        ConnectionId(token_value),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );

    let before = backend.ctx.replication().backlog.reported_offset().get();
    backend
        .apply_replication_command(
            connection_token,
            &mut link,
            ReplicationApply::Ping,
            CommandOrigin::Replication,
        )
        .unwrap();
    let after = backend.ctx.replication().backlog.reported_offset().get();

    // The relayed PING frame is appended to the downstream backlog verbatim,
    // advancing the offset by exactly its wire length.
    let ping = crate::engine::distributed::replication::protocol::encode_command(&[b"PING"]);
    assert_eq!(after - before, ping.len() as i64);
}

#[test]
fn replica_applied_swapdb_invalidates_only_the_affected_local_watch() {
    let ReplicaSwapdbFixture {
        mut backend,
        connection_token,
        mut link,
        affected,
        unaffected,
        blocked,
        watched_key,
    } = replica_swapdb_fixture();

    backend
        .apply_replication_command(
            connection_token,
            &mut link,
            ReplicationApply::Command(CommandArgs::from_parts(vec![
                Bytes::from_static(b"SWAPDB"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"1"),
            ])),
            CommandOrigin::Replication,
        )
        .unwrap();

    assert_replica_swapdb_outcome(&mut backend, affected, unaffected, blocked, &watched_key);
}

#[test]
fn replicated_transaction_waits_for_exec_before_applying_lazy_expire_replacement() {
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use crate::framework::distributed::replication::{ConnectionId, MasterInfo};

    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.replication_mut().switch_to_replica();
    let token_value = 73usize;
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(Token(token_value));
    let connection_token = backend.connection_registry.insert(connection);
    let mut link = ReplicaLink::new(
        ConnectionId(token_value),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let command = |parts: &[&[u8]]| {
        ReplicationApply::Command(CommandArgs::from_parts(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part))
                .collect(),
        ))
    };

    for apply in [
        command(&[b"MULTI"]),
        command(&[b"UNLINK", b"set"]),
        command(&[b"SADD", b"set", b"member"]),
    ] {
        backend
            .apply_replication_command(
                connection_token,
                &mut link,
                apply,
                CommandOrigin::Replication,
            )
            .unwrap();
    }

    assert_eq!(
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .transaction
            .queued_commands()
            .len(),
        2
    );
    assert!(backend.ctx.db(0).unwrap().get_no_expire(b"set").is_none());

    backend
        .apply_replication_command(
            connection_token,
            &mut link,
            command(&[b"EXEC"]),
            CommandOrigin::Replication,
        )
        .unwrap();

    assert!(!backend.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)]
    .transaction
    .is_active());
    assert!(backend.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)]
    .transaction
    .queued_commands()
    .is_empty());
    let set = backend
        .ctx
        .db_mut(0)
        .unwrap()
        .get_set(b"set")
        .unwrap()
        .unwrap();
    assert!(set.data.contains_key(b"member".as_slice()));
}

#[test]
fn replicated_transaction_advances_resume_offset_only_after_exec() {
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use crate::framework::distributed::replication::{ConnectionId, MasterInfo};

    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.replication_mut().switch_to_replica();
    let token_value = 77usize;
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(Token(token_value));
    let connection_token = backend.connection_registry.insert(connection);
    let mut link = ReplicaLink::streaming(
        ConnectionId(token_value),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "replid".to_string(),
        offset(100),
    );
    let stream = [
        crate::engine::distributed::replication::protocol::encode_command(&[b"MULTI"]),
        crate::engine::distributed::replication::protocol::encode_command(&[
            b"LPUSH", b"tx-list", b"value",
        ]),
        crate::engine::distributed::replication::protocol::encode_command(&[b"EXEC"]),
    ]
    .concat();
    let (events, _) = link.on_bytes(&stream, 6379).unwrap();
    assert_eq!(events.len(), 3);

    backend
        .apply_replica_event(connection_token, &mut link, events[0].clone(), true)
        .unwrap();
    assert_eq!(link.applied_position(), offset(100));

    backend
        .apply_replica_event(connection_token, &mut link, events[1].clone(), true)
        .unwrap();
    assert_eq!(
        link.applied_position(),
        offset(100),
        "queued transaction writes are not safe resume boundaries"
    );
    assert!(backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"tx-list")
        .is_none());

    backend
        .apply_replica_event(connection_token, &mut link, events[2].clone(), true)
        .unwrap();
    let ReplicaEvent::Apply {
        end_offset: exec_offset,
        ..
    } = events[2];
    assert_eq!(link.applied_position(), exec_offset);
    assert!(backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"tx-list")
        .is_some());
}

#[test]
fn replicated_write_for_tracking_import_stays_hidden() {
    let mut backend = ServerWorkerState::new_with_slot_router(1, 14, |key| {
        crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
    })
    .unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(Token(72));
    let replication_key = backend.connection_registry.insert(connection);
    let mut link = ReplicaLink::new(
        ConnectionId(72),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let key = b"{atomic}replicated";
    let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
    let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
    let id = MigrationJobId::parse(b"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee").unwrap();
    let job = MigrationJob::new_tracking_import(
        id.clone(),
        NodeId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
        NodeId::parse(state.myself.name.as_bytes()).unwrap(),
        SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap(),
        1,
    );
    state.migrations.start(job).unwrap();
    *backend.ctx.cluster_state_mut() = Some(Box::new(state));
    backend.ctx.create_slot_import(
        id.clone(),
        &SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap(),
    );
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SET"),
        Bytes::copy_from_slice(key),
        Bytes::from_static(b"value"),
    ]);

    backend
        .apply_replication_command(
            replication_key,
            &mut link,
            ReplicationApply::Command(args),
            CommandOrigin::Replication,
        )
        .unwrap();

    assert_eq!(backend.ctx.dbs().count_keys_in_slot(slot as usize), 0);
    assert_eq!(backend.ctx.slot_import_key_count(0, slot as usize), 1);
    assert_eq!(
        backend.connection_registry[crate::framework::capabilities::io::Token(replication_key)]
            .slot_migration_role,
        None
    );
}

#[test]
fn disk_backed_rdb_commit_keeps_old_data_when_snapshot_rename_fails() {
    use crate::foundation::io::persistence::serialize_all;
    use crate::foundation::io::store::kv_engine::DbSet;
    use crate::foundation::io::store::Robj;
    use crate::framework::distributed::replication::{ConnectionId, MasterInfo};

    let dir = std::env::temp_dir().join(format!(
        "valkey-replica-rdb-rename-failure-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("dump.rdb")).unwrap();

    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    backend.ctx.config_mut().persistence.dbfilename = "dump.rdb".to_string();
    backend.ctx.config_mut().replication.repl_diskless_load = "disabled".to_string();
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(Token(81));
    let connection_token = backend.connection_registry.insert(connection);
    backend
        .ctx
        .db_mut(0)
        .unwrap()
        .set(b"old".to_vec(), Robj::string("value"));
    let mut replacement = DbSet::new(16);
    replacement
        .get_mut(0)
        .unwrap()
        .set(b"new".to_vec(), Robj::string("value"));
    let raw = serialize_all(&replacement, 0);
    let mut link = ReplicaLink::new(
        ConnectionId(81),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let commit = RdbCommit {
        function_codes: Vec::new(),
        aux_fields: Vec::new(),
        repl_stream_db: 0,
        diskless: true,
        disk_backed: true,
        raw: Some(raw),
        swapped: Some(replacement),
    };

    assert!(backend
        .commit_streamed_rdb(connection_token, &mut link, commit)
        .is_err());
    assert!(backend.ctx.db(0).unwrap().get_no_expire(b"old").is_some());
    assert!(backend.ctx.db(0).unwrap().get_no_expire(b"new").is_none());

    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn invalid_aux_keeps_full_sync_commit_state_unpublished() {
    use crate::foundation::io::store::kv_engine::DbSet;
    use crate::foundation::io::store::Robj;

    let dir = std::env::temp_dir().join(format!(
        "valkey-replica-invalid-aux-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("dump.rdb"), b"old-rdb").unwrap();

    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    backend.ctx.config_mut().persistence.dbfilename = "dump.rdb".to_string();
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(Token(82));
    let connection_token = backend.connection_registry.insert(connection);
    backend
        .ctx
        .db_mut(0)
        .unwrap()
        .set(b"old".to_vec(), Robj::string("value"));
    let old_replid = backend.ctx.replication().backlog.replid().to_string();
    let mut cluster = ClusterState::new("127.0.0.1", 6380, 16_380);
    let old_slot_state = b"8>aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,";
    cluster.apply_open_slots_aux_field(old_slot_state).unwrap();
    *backend.ctx.cluster_state_mut() = Some(Box::new(cluster));

    let mut replacement = DbSet::new(16);
    replacement
        .get_mut(0)
        .unwrap()
        .set(b"new".to_vec(), Robj::string("value"));
    let mut link = ReplicaLink::new(
        ConnectionId(82),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.set_resume_position(
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
        offset(7),
        0,
    );
    let commit = RdbCommit {
        function_codes: Vec::new(),
        aux_fields: vec![(b"cluster-slot-states".to_vec(), b"8>short,".to_vec())],
        repl_stream_db: 0,
        diskless: true,
        disk_backed: true,
        raw: Some(b"new-rdb".to_vec()),
        swapped: Some(replacement),
    };

    assert!(backend
        .commit_streamed_rdb(connection_token, &mut link, commit)
        .is_err());
    assert!(backend.ctx.db(0).unwrap().get_no_expire(b"old").is_some());
    assert!(backend.ctx.db(0).unwrap().get_no_expire(b"new").is_none());
    assert_eq!(backend.ctx.replication().backlog.replid(), old_replid);
    assert_eq!(
        backend
            .ctx
            .cluster_state()
            .as_deref()
            .unwrap()
            .encode_open_slots_aux_field(),
        Some(old_slot_state.to_vec())
    );
    assert_eq!(std::fs::read(dir.join("dump.rdb")).unwrap(), b"old-rdb");

    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn invalid_function_keeps_full_sync_dataset_and_registry_unpublished() {
    use crate::foundation::io::store::kv_engine::DbSet;
    use crate::foundation::io::store::Robj;

    let mut backend = ServerWorkerState::new(16).unwrap();
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(Token(83));
    let connection_token = backend.connection_registry.insert(connection);
    backend
        .ctx
        .db_mut(0)
        .unwrap()
        .set(b"old".to_vec(), Robj::string("value"));
    let old_function =
        "#!lua name=oldlib\nserver.register_function('oldfn', function() return 'old' end)";
    backend
        .ctx
        .script_mut()
        .function_registry
        .load_library("oldlib", old_function)
        .unwrap();
    let old_replid = backend.ctx.replication().backlog.replid().to_string();
    let mut replacement = DbSet::new(16);
    replacement
        .get_mut(0)
        .unwrap()
        .set(b"new".to_vec(), Robj::string("value"));
    let mut link = ReplicaLink::new(
        ConnectionId(83),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.set_resume_position(
        "cccccccccccccccccccccccccccccccccccccccc".to_string(),
        offset(8),
        0,
    );
    let commit = RdbCommit {
        function_codes: vec![b"not a function library".to_vec()],
        aux_fields: Vec::new(),
        repl_stream_db: 0,
        diskless: true,
        disk_backed: false,
        raw: None,
        swapped: Some(replacement),
    };

    assert!(backend
        .commit_streamed_rdb(connection_token, &mut link, commit)
        .is_err());
    assert!(backend.ctx.db(0).unwrap().get_no_expire(b"old").is_some());
    assert!(backend.ctx.db(0).unwrap().get_no_expire(b"new").is_none());
    assert_eq!(backend.ctx.replication().backlog.replid(), old_replid);
    assert!(backend
        .ctx
        .script()
        .function_registry
        .get_library("oldlib")
        .is_some());
}
