use super::*;
use crate::engine::command::context::{BlockedCmd, BlockedState, WatchedKey};
use crate::engine::distributed::cluster::migration::{MigrationJob, MigrationJobId, SlotRanges};
use crate::engine::distributed::cluster::{ClusterState, NodeId, SlotRange};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::structure::list::ListValue;
use crate::foundation::io::store::Robj;
use crate::framework::capabilities::connection::Connection;
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::{ConnectionId, MasterInfo, ProvisionalPrimary};
use crate::runtime::server::worker::command::dispatch::CommandEffects;
use crate::runtime::server::worker::command::invocation::CommandTarget;
use crate::runtime::server::worker::command::outcome::{CommandDisposition, SpecialCommandResult};
use std::collections::VecDeque;
use std::io::Write;
use std::net::{TcpListener, TcpStream};

fn offset(value: i64) -> ReplicationOffset {
    ReplicationOffset::try_from(value).expect("valid test offset")
}

#[test]
fn paused_dual_handoff_keeps_new_socket_bytes_until_local_replay_finishes() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let connected = backend.transport.connect_tcp(addr).unwrap();
    let (mut peer, _) = listener.accept().unwrap();
    backend.add_connection(
        crate::runtime::server::worker::event_loop::transport::RuntimeConnectedPeer {
            token: connected.token,
            peer_addr: addr,
        },
    );
    let main = connected.token;
    let mut link = ReplicaLink::streaming(
        ConnectionId(main.0),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "0123456789abcdef0123456789abcdef01234567".to_string(),
        offset(100),
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
        replid: "0123456789abcdef0123456789abcdef01234567".to_string(),
        end_offset: offset(100),
        db_id: 0,
        rdb_client_id: 42,
    });
    backend.replication_runtime.set_replica_link(link);
    backend
        .replication_runtime
        .set_replica_main_read_paused(true);
    let mut link = backend.replication_runtime.take_replica_link().unwrap();
    let frame = crate::engine::distributed::replication::protocol::encode_command(&[b"PING"]);
    peer.write_all(&frame).unwrap();
    backend
        .transport
        .wait_pending_bytes_for_test(main, frame.len());

    backend
        .drain_dual_main_before_handoff(main.0, &mut link)
        .unwrap();

    assert_eq!(link.received_position(), offset(100));
    assert!(backend
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    backend
        .replication_runtime
        .set_replica_main_read_paused(false);
    backend
        .drain_dual_main_before_handoff(main.0, &mut link)
        .unwrap();
    assert_eq!(link.received_position(), offset(100 + frame.len() as i64));
}

fn no_command_host(
    request: crate::engine::host::HostRequest<'_>,
) -> Result<crate::engine::host::HostReply, crate::engine::host::HostError> {
    match request {
        crate::engine::host::HostRequest::Yield(_) => Ok(crate::engine::host::HostReply::Continue),
        _ => Err(crate::engine::host::HostError::Runtime(
            "unexpected host request".to_string(),
        )),
    }
}

fn call_registered_function(backend: &ServerWorkerState, name: &str) -> Value {
    let compiled = backend
        .ctx
        .script()
        .function_registry
        .find_function(name)
        .expect("function remains registered")
        .clone();
    let engine = backend
        .ctx
        .script()
        .function_registry
        .function_engine(&compiled)
        .expect("function engine exists");
    let mut run = crate::engine::extension::script::ScriptRunCtx::new_bare_for_test(compiled.flags);
    let mut host = no_command_host;
    engine
        .call(&compiled, &mut run, &[], &[], &mut host)
        .expect("function remains callable")
}

#[test]
fn unknown_replication_command_log_obeys_user_data_policy() {
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"notacommand"),
        Bytes::from_static(b"secretvalue"),
    ]);

    assert_eq!(
        replication_unknown_command_error(&args, false),
        "ERR unknown command 'notacommand', with args beginning with: 'secretvalue'"
    );
    assert_eq!(replication_unknown_command_error(&args, true), "*redacted*");
}

#[test]
fn unknown_replication_command_log_truncates_on_utf8_boundary() {
    let prefix = "ERR unknown command 'cmd', with args beginning with: '";
    let mut argument = vec![b'x'; 4095 - prefix.len()];
    argument.extend_from_slice("é".as_bytes());
    argument.extend_from_slice(&vec![b'y'; 1024 * 1024]);
    let args = CommandArgs::from_parts(vec![Bytes::from_static(b"cmd"), Bytes::from(argument)]);

    let rendered = replication_unknown_command_error(&args, false);

    assert!(rendered.len() <= 4096);
    assert!(rendered.is_char_boundary(rendered.len()));
}

#[test]
fn known_replication_command_error_obeys_user_data_policy() {
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"INCR"),
        Bytes::from_static(b"not-an-integer"),
    ]);

    assert_eq!(
        replication_command_error_for_log(b"ERR secret\r\nline", &args, true),
        "*redacted*"
    );
    let rendered = replication_command_error_for_log(b"ERR secret\r\nline", &args, false);
    assert_eq!(
        rendered,
        "ERR secret  line, with args beginning with: 'not-an-integer'"
    );
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

#[test]
fn replication_priority_samples_cover_controls_and_outer_transaction() {
    for is_priority in [false, true] {
        let mut backend = ServerWorkerState::new(2).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        let (token, main, _peer) = add_live_replay_connection(&mut backend);
        let mut link = ReplicaLink::new(
            main,
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        if is_priority {
            backend.prioritize_connection(Token(token));
        }
        let sample = u64::from(is_priority);
        for (index, apply) in [
            ReplicationApply::Select(1),
            ReplicationApply::Ping,
            ReplicationApply::GetAck,
        ]
        .into_iter()
        .enumerate()
        {
            backend
                .apply_replication_command(token, &mut link, apply, CommandOrigin::Replication)
                .unwrap();
            assert_eq!(
                backend.ctx.stats().priority_commands_processed,
                (index as u64 + 1) * sample
            );
        }
        for parts in [
            vec!["MULTI"],
            vec!["SET", "first", "one"],
            vec!["SELECT", "0"],
            vec!["SET", "second", "two"],
            vec!["EXEC"],
        ] {
            let args = CommandArgs::from_parts(
                parts
                    .into_iter()
                    .map(|part| Bytes::copy_from_slice(part.as_bytes()))
                    .collect(),
            );
            backend
                .apply_replication_command(
                    token,
                    &mut link,
                    ReplicationApply::Command(args),
                    CommandOrigin::Replication,
                )
                .unwrap();
        }
        assert_eq!(backend.ctx.stats().priority_commands_processed, 5 * sample);
        assert!(backend
            .ctx
            .db(1)
            .unwrap()
            .get_allow_expired(b"first")
            .is_some());
        assert!(backend
            .ctx
            .db(0)
            .unwrap()
            .get_allow_expired(b"second")
            .is_some());
    }
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
    backend.process_ready_keys();

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
fn forkless_capture_defers_replica_writes_without_reordering_the_stream() {
    use crate::engine::extension::config::persistence::BgsaveMethod;

    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.replication_mut().switch_to_replica();
    for index in 0..256 {
        backend.ctx.db_mut(0).unwrap().set(
            format!("capture-{index}").into_bytes(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
    }
    let dir = std::env::temp_dir().join(format!(
        "versal-forkless-replica-apply-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    backend.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    backend.ctx.config_mut().persistence.dbfilename = "dump.rdb".to_string();
    backend.ctx.config_mut().persistence.bgsave_default_method = BgsaveMethod::Forkless;
    crate::framework::commands::server::save::start_bgsave(&mut backend.ctx).unwrap();
    assert!(backend.ctx.persistence().forkless_capture_in_progress());

    let (connection_token, main, _peer) = add_live_replay_connection(&mut backend);
    let mut link = ReplicaLink::streaming(
        main,
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        "replid".to_string(),
        offset(0),
    );
    let stream = [
        crate::engine::distributed::replication::protocol::encode_command(&[
            b"SET",
            b"replicated",
            b"first",
        ]),
        crate::engine::distributed::replication::protocol::encode_command(&[
            b"SET",
            b"replicated",
            b"second",
        ]),
    ]
    .concat();
    let (events, _) = link.on_bytes(&stream, 6379).unwrap();
    let ReplicaEvent::Apply {
        end_offset: expected_offset,
        ..
    } = events.last().cloned().unwrap();

    backend
        .apply_replica_events(connection_token, &mut link, events)
        .unwrap();
    let value_during_capture = backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"replicated")
        .map(|value| value.as_bytes().to_vec());
    let applied_during_capture = link.applied_position();
    let reads_paused_during_capture = backend.replication_runtime.replica_main_read_paused();
    let pause_interest = backend
        .transport
        .last_set_interest_calls_for_test()
        .last()
        .copied();
    backend.replication_runtime.set_replica_link(link);
    backend.queue_client_reply(Token(main.0), b"ack".to_vec());
    let queued_ack_interest = backend
        .transport
        .last_set_interest_calls_for_test()
        .last()
        .copied();
    backend.dispatch_client_write(Token(main.0));
    let drained_ack_interest = backend
        .transport
        .last_set_interest_calls_for_test()
        .last()
        .copied();

    backend
        .ctx
        .abort_forkless_save(crate::foundation::io::sys::time::mstime() / 1_000);
    backend.apply_deferred_replica_events();
    let value_after_capture = backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"replicated")
        .map(|value| value.as_bytes().to_vec());
    let applied_after_capture = backend
        .replication_runtime
        .replica_link()
        .unwrap()
        .applied_position();
    let reads_paused_after_capture = backend.replication_runtime.replica_main_read_paused();
    let _ = std::fs::remove_dir_all(dir);

    assert_eq!(value_during_capture, None);
    assert_eq!(applied_during_capture, offset(0));
    assert!(reads_paused_during_capture);
    assert_eq!(
        pause_interest,
        Some((
            Token(main.0),
            crate::foundation::io::transport::Interest::None
        ))
    );
    assert_eq!(
        queued_ack_interest,
        Some((
            Token(main.0),
            crate::foundation::io::transport::Interest::Writable
        ))
    );
    assert_eq!(
        drained_ack_interest,
        Some((
            Token(main.0),
            crate::foundation::io::transport::Interest::None
        ))
    );
    assert_eq!(value_after_capture, Some(b"second".to_vec()));
    assert_eq!(applied_after_capture, expected_offset);
    assert!(!reads_paused_after_capture);
}

#[test]
fn replica_hup_drain_applies_a_frame_larger_than_the_fair_read_budget() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.replication_mut().switch_to_replica();
    let (connection_token, main, mut peer) = add_live_replay_connection(&mut backend);
    backend
        .replication_runtime
        .set_replica_link(ReplicaLink::streaming(
            main,
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
            "replid".to_string(),
            offset(0),
        ));
    let value = vec![b'x'; crate::runtime::server::worker::NORMAL_CLIENT_READ_BUDGET * 2];
    let frame = crate::engine::distributed::replication::protocol::encode_command(&[
        b"SET",
        b"hup-large",
        &value,
    ]);
    backend
        .transport
        .reserve_test_receive_buffer(Token(connection_token), frame.len())
        .unwrap();
    peer.write_all(&frame).unwrap();
    peer.shutdown(std::net::Shutdown::Write).unwrap();
    // Wait for the complete frame without consuming it, so one HUP drain
    // must apply bytes beyond the normal fair read budget.
    backend
        .transport
        .wait_pending_bytes_for_test(Token(connection_token), frame.len());

    backend.recv_replication_stream_after_hup(connection_token, connection_token);

    assert_eq!(
        backend
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"hup-large")
            .map(|value| value.as_bytes().len()),
        Some(value.len())
    );
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

#[test]
fn function_install_resource_failure_returns_error_and_preserves_state() {
    use crate::engine::extension::lua::{reset_lua_runtime, with_lua_runtime};
    use crate::engine::extension::script::ScriptError;
    use crate::foundation::memory::metrics::memory_stats;

    let _memory_guard = memory_stats::lock_global_for_test();
    reset_lua_runtime();
    let test_outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend
            .ctx
            .db_mut(0)
            .unwrap()
            .set(b"old".to_vec(), Robj::string("value"));
        let old_function = "#!lua name=oldlib\nlocal counter = 0\nserver.register_function('oldfn', function() counter = counter + 1 return counter end)";
        backend
            .ctx
            .script_mut()
            .function_registry
            .load_library("oldlib", old_function)
            .unwrap();
        assert_eq!(
            call_registered_function(&backend, "oldfn"),
            Value::Integer(1)
        );

        let payload = "x".repeat(512 * 1024);
        let incoming_code = format!(
            "#!lua name=newlib\nlocal payload = '{payload}'\nserver.register_function('newfn', function() return payload end)"
        );
        let function_codes = vec![incoming_code.into_bytes()];
        with_lua_runtime(false, |lua| {
            lua.set_memory_limit(lua.used_memory().saturating_add(4096))
                .map(|_| ())
                .map_err(|error| ScriptError::Runtime(error.to_string()))
        })
        .unwrap();

        let install_outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            backend.install_prepared_functions(&function_codes)
        }));
        (backend, install_outcome)
    }));

    let limit_cleanup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        with_lua_runtime(false, |lua| {
            lua.set_memory_limit(0)
                .map(|_| ())
                .map_err(|error| ScriptError::Runtime(error.to_string()))
        })
    }));

    let (backend, install_outcome) = match test_outcome {
        Ok(outcome) => outcome,
        Err(payload) => {
            let _ = limit_cleanup;
            drop(_memory_guard);
            std::panic::resume_unwind(payload);
        }
    };
    match limit_cleanup {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            drop(backend);
            drop(_memory_guard);
            panic!("failed to clear Lua memory limit: {error}");
        }
        Err(payload) => {
            drop(backend);
            drop(_memory_guard);
            std::panic::resume_unwind(payload);
        }
    }

    let (did_panic, returned_error) = match install_outcome {
        Ok(result) => (false, result.is_err()),
        Err(_) => (true, false),
    };
    let old_key_is_visible = backend.ctx.db(0).unwrap().get_no_expire(b"old").is_some();
    let old_library_is_visible = backend
        .ctx
        .script()
        .function_registry
        .get_library("oldlib")
        .is_some();
    let old_function_is_visible = backend
        .ctx
        .script()
        .function_registry
        .find_function("oldfn")
        .is_some();
    let old_function_result = call_registered_function(&backend, "oldfn");
    drop(backend);
    reset_lua_runtime();
    drop(_memory_guard);

    assert!(
        !did_panic,
        "function installation failure must return Err instead of panicking"
    );
    assert!(
        returned_error,
        "constrained function installation must return Err"
    );
    assert!(old_key_is_visible);
    assert!(old_library_is_visible);
    assert!(old_function_is_visible);
    assert_eq!(old_function_result, Value::Integer(2));
}
