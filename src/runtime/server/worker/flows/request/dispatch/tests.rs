use super::script_helpers::script_has_no_writes_flag;
use super::side_effects::{CommandEffects, ReadBatchEffects};
use super::types::LoopAction;
use super::*;
use crate::engine::command::CommandArgs;
use crate::engine::distributed::replication::ReplicationOffset;
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::{
    BlockedCmd, CommandEffectDrain, Connection, ConnectionState,
};
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::{
    ConnectionId, PrimaryLinkState, ReplicaConnectionInfo,
};
use crate::framework::spec::CommandHandler;
use crate::framework::CommandSpec;
use crate::framework::DispatchGroup;
use crate::runtime::server::worker::flows::request::dispatch::command_exec::CommandPropagationInput;
use crate::runtime::server::worker::flows::request::invocation::*;
use crate::runtime::server::worker::flows::request::outcome::*;
use crate::runtime::server::worker::sessions::ClientReadMode;
use bytes::Bytes;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;

type DispatchSideEffects = CommandEffects;

fn offset(value: i64) -> ReplicationOffset {
    ReplicationOffset::try_from(value).expect("valid test offset")
}

fn dummy_handler(
    _conn: &mut dyn crate::engine::command::context::CommandConn,
    _args: &CommandArgs,
    _ctx: &mut crate::framework::context::ServerContext,
) -> Value {
    Value::ok()
}

fn large_reply_handler(
    _conn: &mut dyn crate::engine::command::context::CommandConn,
    _args: &CommandArgs,
    _ctx: &mut crate::framework::context::ServerContext,
) -> Value {
    Value::bulk(vec![b'x'; 64 * 1024])
}

fn command_time_observing_handler(
    _conn: &mut dyn crate::engine::command::context::CommandConn,
    _args: &CommandArgs,
    _ctx: &mut crate::framework::context::ServerContext,
) -> Value {
    let before = crate::foundation::io::sys::time::mstime();
    std::thread::sleep(std::time::Duration::from_millis(10));
    let after = crate::foundation::io::sys::time::mstime();
    Value::Array(vec![Value::integer(before), Value::integer(after)])
}

fn make_spec(
    name: &'static str,
    arity: i32,
    flags: u64,
    first_key: i32,
    last_key: i32,
    key_step: i32,
) -> CommandSpec {
    CommandSpec {
        name: crate::engine::command::CommandName::Static(name),
        arity,
        flags,
        dispatch_group: crate::engine::command::DispatchGroup::Default,
        first_key,
        last_key,
        key_step,
        container: None,
        key_flags: &[],
        acl_categories: &[],
        executor: crate::framework::CommandExecutor::Immediate(dummy_handler as CommandHandler),
        get_keys: None,
    }
}

fn make_args(parts: &[&[u8]]) -> CommandArgs {
    let bytes: Vec<Bytes> = parts.iter().map(|b| Bytes::copy_from_slice(b)).collect();
    CommandArgs::from_parts(bytes)
}

fn meta_from_spec(spec: &CommandSpec) -> CmdPreconditions {
    CmdPreconditions {
        name: spec.name.clone(),
        arity: spec.arity,
        flags: spec.flags,
        first_key: spec.first_key,
        last_key: spec.last_key,
        key_step: spec.key_step,
        key_flags: spec.key_flags,
        acl_categories: spec.acl_categories,
        get_keys: spec.get_keys,
    }
}

#[test]
fn preconditions_keep_shared_names_after_registry_removal() {
    use crate::engine::command::CommandName;
    use std::sync::Arc;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_fake_connection(&mut backend);
    let owner: Arc<str> = Arc::from("Module.Preflight");
    let weak = Arc::downgrade(&owner);
    let mut spec = make_spec("unused", 2, 0, 1, 1, 1);
    spec.name = CommandName::Shared(owner);
    backend.registry.register(spec);
    let spec = backend
        .registry
        .lookup(b"module.preflight")
        .unwrap()
        .clone();
    let meta = meta_from_spec(&spec);
    let transaction_meta = meta_from_spec(&spec.clone());
    drop(spec);
    assert!(backend.registry.unregister("MODULE.PREFLIGHT"));
    assert_eq!(weak.strong_count(), 2);
    assert_eq!(meta.name.as_str(), "Module.Preflight");
    let outcome = backend.pre_dispatch_exec(
        connection_token,
        meta,
        &make_args(&[b"MODULE.PREFLIGHT", b"key"]),
    );
    assert_eq!(outcome.action, DispatchAction::Execute);
    assert_eq!(weak.strong_count(), 1);
    assert_eq!(transaction_meta.name.as_str(), "Module.Preflight");
    drop(transaction_meta);
    assert!(weak.upgrade().is_none());
}

fn add_fake_connection(backend: &mut ServerWorkerState) -> usize {
    let token = Token(42);
    let mut conn = Connection::new_fake();
    conn.set_token_for_test(token);

    backend.connection_registry.insert(conn)
}

fn start_test_forkless_capture(backend: &mut ServerWorkerState, name: &str) -> std::path::PathBuf {
    use crate::engine::extension::config::persistence::BgsaveMethod;
    use crate::foundation::io::store::Robj;

    for index in 0..256 {
        backend.ctx.db_mut(0).unwrap().set(
            format!("capture-{index}").into_bytes(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
    }
    let dir = std::env::temp_dir().join(format!(
        "versal-{name}-{}-{:?}",
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
    dir
}

fn stop_test_forkless_capture(backend: &mut ServerWorkerState, dir: std::path::PathBuf) {
    backend
        .ctx
        .abort_forkless_save(crate::foundation::io::sys::time::mstime() / 1_000);
    let _ = std::fs::remove_dir_all(dir);
}

fn take_connection_command_effects(
    backend: &mut ServerWorkerState,
    connection_token: usize,
) -> crate::framework::capabilities::connection::ConnectionCommandEffects {
    let connection = &mut backend.connection_registry[Token(connection_token)];
    if let Some(effects) = connection.take_command_effects(CommandEffectDrain::Completed) {
        return effects;
    }
    connection.begin_nested_command_effects();
    connection
        .take_command_effects(CommandEffectDrain::Completed)
        .expect("test command effects are drained once")
}

#[test]
fn forkless_capture_postpones_potentially_writing_scripts_but_allows_read_only_scripts() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    let dir = start_test_forkless_capture(&mut backend, "forkless-script-barrier");

    let script_actions: Vec<_> = [
        make_args(&[
            b"EVAL",
            b"return redis.call('SET', KEYS[1], ARGV[1])",
            b"1",
            b"key",
            b"value",
        ]),
        make_args(&[b"FCALL", b"writer", b"1", b"key"]),
    ]
    .into_iter()
    .map(|args| {
        let spec = backend.registry.lookup(&args.name).unwrap();
        backend
            .pre_dispatch_exec(connection_token, meta_from_spec(spec), &args)
            .action
    })
    .collect();

    let args = make_args(&[
        b"EVAL_RO",
        b"#!lua flags=no-writes\nreturn redis.call('GET', KEYS[1])",
        b"1",
        b"key",
    ]);
    let spec = backend.registry.lookup(&args.name).unwrap();
    let mut meta = meta_from_spec(spec);
    meta.flags = super::subcmd_flags::effective_command_flags_for_call(
        spec.name.as_str(),
        spec.flags,
        &args,
        &backend.ctx,
    );
    let read_only_action = backend
        .pre_dispatch_exec(connection_token, meta, &args)
        .action;

    stop_test_forkless_capture(&mut backend, dir);

    assert_eq!(
        script_actions,
        vec![
            DispatchAction::Postpone(PostponeReason::ForklessKey),
            DispatchAction::Postpone(PostponeReason::ForklessKey),
        ]
    );
    assert_eq!(read_only_action, DispatchAction::Execute);
}

#[test]
fn test_tcp_buffer_policy_is_applied_when_an_ordinary_command_identifies_the_client() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let peer = backend
        .transport
        .connect_tcp(listener.local_addr().unwrap())
        .unwrap();
    let (_remote, _) = listener.accept().unwrap();
    backend
        .connection_registry
        .insert(Connection::new(peer.token, peer.peer_addr, false));
    let fd = backend.transport.raw_fd(peer.token).unwrap();
    let defaults = backend.transport.inner_mut().tcp_buffer_sizes_for_test(fd);
    backend
        .transport
        .inner_mut()
        .set_test_tcp_buffer_limits_for_test(4096, 4096);
    let mut batch = ReadBatchEffects::default();

    for parts in [&[b"PING".as_slice()][..], &[b"HELLO".as_slice(), b"2"][..]] {
        let args = make_args(parts);
        let name = String::from_utf8(args.name.to_ascii_lowercase()).unwrap();
        backend.process_parsed_command(peer.token, args, name, false, &mut batch);
        assert_eq!(
            backend.transport.inner_mut().tcp_buffer_sizes_for_test(fd),
            defaults
        );
    }
    backend.process_parsed_command(
        peer.token,
        make_args(&[b"PING", b"payload"]),
        "ping".to_string(),
        false,
        &mut batch,
    );
    let actual = backend.transport.inner_mut().tcp_buffer_sizes_for_test(fd);
    assert!(actual.0 < defaults.0 && actual.1 < defaults.1);
    backend.process_parsed_command(
        peer.token,
        make_args(&[b"GET", b"missing"]),
        "get".to_string(),
        false,
        &mut batch,
    );
    assert_eq!(
        backend.transport.inner_mut().tcp_buffer_sizes_for_test(fd),
        actual
    );
}

#[test]
fn test_tcp_buffer_policy_does_not_classify_replica_negotiation_as_an_ordinary_client() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let peer = backend
        .transport
        .connect_tcp(listener.local_addr().unwrap())
        .unwrap();
    let (_remote, _) = listener.accept().unwrap();
    backend
        .connection_registry
        .insert(Connection::new(peer.token, peer.peer_addr, false));
    let fd = backend.transport.raw_fd(peer.token).unwrap();
    let defaults = backend.transport.inner_mut().tcp_buffer_sizes_for_test(fd);
    backend
        .transport
        .inner_mut()
        .set_test_tcp_buffer_limits_for_test(4096, 4096);
    let mut batch = ReadBatchEffects::default();
    for parts in [
        &[b"PING".as_slice()][..],
        &[b"REPLCONF".as_slice(), b"listening-port", b"6380"][..],
        &[b"REPLCONF".as_slice(), b"capa", b"eof"][..],
        &[b"INFO".as_slice()][..],
    ] {
        let args = make_args(parts);
        let name = String::from_utf8(args.name.to_ascii_lowercase()).unwrap();
        backend.process_parsed_command(peer.token, args, name, false, &mut batch);
        assert_eq!(
            backend.transport.inner_mut().tcp_buffer_sizes_for_test(fd),
            defaults
        );
    }
}

#[test]
fn bulk_parse_limits_apply_configured_limit_after_authentication() {
    assert_eq!(
        crate::runtime::server::worker::sessions::read::command_bulk_limits(false, 1024 * 1024),
        (
            super::super::UNAUTH_MBULK_MAX,
            super::super::UNAUTH_BULK_MAX
        )
    );
    assert_eq!(
        crate::runtime::server::worker::sessions::read::command_bulk_limits(true, 1024 * 1024),
        (0, 1024 * 1024)
    );
}

fn add_socket_connection(backend: &mut ServerWorkerState) -> (TcpStream, usize, Token) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind socket-backed test listener");
    let peer_addr = listener
        .local_addr()
        .expect("get socket-backed test address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect socket-backed test transport");
    let (stream, _) = listener
        .accept()
        .expect("accept socket-backed test transport");
    backend.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr,
        },
    );
    let connection_token = peer.token.0;
    (stream, connection_token, peer.token)
}

fn enable_test_aof(backend: &mut ServerWorkerState, name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "versal-dispatch-aof-{}-{}-{:?}",
        name,
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    backend.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    backend.ctx.config_mut().persistence.aof_filename = "appendonly.aof".to_string();
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.flush_aof_if_needed();
    dir
}

fn read_test_aof_increment(dir: &std::path::Path) -> Vec<u8> {
    std::fs::read(dir.join("appendonlydir").join("appendonly.aof.1.incr.aof")).unwrap()
}

#[test]
fn pipelined_direct_and_exec_writes_feed_aof_with_streaming_replica() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    let (_client, connection_token, token) = add_socket_connection(&mut backend);
    let replica_token = Token(84);
    let mut replica = Connection::new_fake();
    replica.set_token_for_test(replica_token);
    backend.connection_registry.insert(replica);
    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(replica_token.0),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .set_state(PrimaryLinkState::Streaming);
    let dir = enable_test_aof(&mut backend, "pipeline");
    let request = b"*3\r\n$3\r\nSET\r\n$5\r\norder\r\n$6\r\nbefore\r\n\
        *1\r\n$5\r\nMULTI\r\n\
        *3\r\n$3\r\nSET\r\n$5\r\norder\r\n$5\r\nafter\r\n\
        *1\r\n$4\r\nEXEC\r\n";

    backend.process_client_read_result(token, ClientReadMode::Fair, request.to_vec(), false, false);
    backend.flush_aof_if_needed();

    let aof = read_test_aof_increment(&dir);
    assert!(
        aof.windows(b"before".len())
            .any(|window| window == b"before"),
        "direct SET was not written to AOF: {}",
        String::from_utf8_lossy(&aof)
    );
    assert!(
        aof.windows(b"after".len()).any(|window| window == b"after"),
        "EXEC SET was not written to AOF: {}",
        String::from_utf8_lossy(&aof)
    );
    assert!(backend.connection_registry[Token(connection_token)].has_pending_output());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn hosted_eval_writes_feed_aof() {
    let mut backend = ServerWorkerState::new(16).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_command_registry_snapshot(&backend.registry);
    let (_client, _connection_token, token) = add_socket_connection(&mut backend);
    let dir = enable_test_aof(&mut backend, "eval");
    let request = b"*3\r\n$4\r\nEVAL\r\n$92\r\nredis.call('set','aof-db0','1'); redis.call('select','10'); redis.call('set','aof-db10','2')\r\n$1\r\n0\r\n";

    backend.process_client_read_result(token, ClientReadMode::Fair, request.to_vec(), false, false);
    backend.flush_aof_if_needed();

    let aof = read_test_aof_increment(&dir);
    assert!(
        aof.windows(b"aof-db0".len())
            .any(|window| window == b"aof-db0"),
        "DB0 script effect was not written to AOF: {}",
        String::from_utf8_lossy(&aof)
    );
    assert!(
        aof.windows(b"aof-db10".len())
            .any(|window| window == b"aof-db10"),
        "DB10 script effect was not written to AOF: {}",
        String::from_utf8_lossy(&aof)
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn command_finalization_submits_deferred_lazyfree_objects() {
    use crate::foundation::io::store::structure::list::ListValue;
    use crate::foundation::io::store::Robj;
    use std::collections::VecDeque;

    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[Token(connection_token)].defer_command_lazyfree_object(
        Robj::ListQuicklist(ListValue::from_vecdeque(VecDeque::from(
            (0..65)
                .map(|_| Bytes::from_static(b"value"))
                .collect::<Vec<_>>(),
        ))),
    );
    let args = make_args(&[b"PING"]);
    let mut effects = CommandEffects::default();

    let mut connection_effects = take_connection_command_effects(&mut backend, connection_token);
    let disposition = backend.complete_command(
        CommandCompletion {
            invocation: CommandInvocation::direct(
                CommandTarget::new(Token(42)),
                &args,
                "ping",
                0,
                DispatchGroup::Default,
                &[],
            ),
            reply: Value::ok(),
            status: CommandExecutionStatus::Executed,
            duration_us: 0,
            was_blocked_before: false,
        },
        Some(&mut connection_effects),
        &mut effects,
    );
    assert_eq!(disposition, CommandDisposition::Completed);
    assert!(backend.connection_registry[Token(connection_token)]
        .pending_lazyfree_objects()
        .is_empty());
    assert_eq!(backend.ctx.stats().lazyfree_pending_objects, 65);
}

#[test]
fn pipelined_pending_replies_trigger_client_eviction_before_the_next_command() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    backend.ctx.config_mut().server.maxmemory_clients = "128kb".to_string();
    backend.registry.register(CommandSpec {
        name: crate::engine::command::CommandName::Static("bigreply"),
        arity: 1,
        flags: 0,
        dispatch_group: DispatchGroup::Default,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        container: None,
        key_flags: &[],
        acl_categories: &[],
        executor: crate::framework::CommandExecutor::Immediate(
            large_reply_handler as CommandHandler,
        ),
        get_keys: None,
    });
    let (_peer, _connection_token, token) = add_socket_connection(&mut backend);
    let request = b"*1\r\n$8\r\nBIGREPLY\r\n".repeat(4);

    backend.process_client_read_result(token, ClientReadMode::Fair, request, false, false);

    assert!(!backend.connection_registry.contains(Token(token.0)));
    assert_eq!(backend.ctx.stats().evicted_clients, 1);
}

#[test]
fn pipeline_key_prefetch_warms_multiple_command_keys_without_changing_replies() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_command_registry_snapshot(&backend.registry);
    backend.ctx.config_mut().server.prefetch_batch_max_size = 2;
    backend.ctx.db_mut(0).expect("database zero").set(
        b"first".to_vec(),
        crate::foundation::io::store::Robj::string("one"),
    );
    backend.ctx.db_mut(0).expect("database zero").set(
        b"second".to_vec(),
        crate::foundation::io::store::Robj::string("two"),
    );
    let (mut peer, _connection_token, token) = add_socket_connection(&mut backend);
    let request = b"*2\r\n$3\r\nGET\r\n$5\r\nfirst\r\n*2\r\n$3\r\nGET\r\n$6\r\nsecond\r\n";

    backend.process_client_read_result(token, ClientReadMode::Fair, request.to_vec(), false, false);
    backend.writes.ordinary.begin_turn();
    backend.drive_pending_ordinary_writes();

    let mut reply = [0_u8; 18];
    peer.read_exact(&mut reply)
        .expect("read pipelined GET replies");
    assert_eq!(&reply, b"$3\r\none\r\n$3\r\ntwo\r\n");
    assert_eq!(backend.ctx.stats().io_threaded_total_prefetch_batches, 1);
    assert_eq!(backend.ctx.stats().io_threaded_total_prefetch_entries, 2);
}

#[test]
fn pipeline_key_prefetch_caps_multi_key_commands_at_the_configured_budget() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_command_registry_snapshot(&backend.registry);
    backend.ctx.config_mut().server.prefetch_batch_max_size = 2;
    for (key, value) in [
        (b"first".as_slice(), "one"),
        (b"second", "two"),
        (b"third", "three"),
    ] {
        backend.ctx.db_mut(0).expect("database zero").set(
            key.to_vec(),
            crate::foundation::io::store::Robj::string(value),
        );
    }
    let (_peer, _connection_token, token) = add_socket_connection(&mut backend);
    let request = b"*4\r\n$4\r\nMGET\r\n$5\r\nfirst\r\n$6\r\nsecond\r\n$5\r\nthird\r\n";

    backend.process_client_read_result(token, ClientReadMode::Fair, request.to_vec(), false, false);

    assert_eq!(backend.ctx.stats().io_threaded_total_prefetch_batches, 1);
    assert_eq!(backend.ctx.stats().io_threaded_total_prefetch_entries, 2);
}

#[test]
fn finalization_schedules_replication_and_defers_origin_reply_to_pending_writes() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    backend.ctx.replication_mut().backlog.activate();
    let (mut client, client_slab, client_token) = add_socket_connection(&mut backend);
    let (mut replica, _replica_slab, replica_token) = add_socket_connection(&mut backend);
    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(replica_token.0),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("registered replica link")
        .set_state(PrimaryLinkState::Streaming);
    let replication_frame = b"*1\r\n$4\r\nPING\r\n".to_vec();
    let client_reply = b"+OK\r\n".to_vec();
    let mut effects = ReadBatchEffects::default();
    effects.replication_only.push(replication_frame.clone());
    effects.replies.push(client_reply.clone());

    backend.writes.ordinary.begin_turn();
    backend.finalize_handle_readable(client_slab, client_token.0, effects);

    replica
        .set_nonblocking(true)
        .expect("set replica peer nonblocking");
    let mut byte = [0_u8; 1];
    let error = replica
        .read(&mut byte)
        .expect_err("replication must wait for distributed writes");
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    client
        .set_nonblocking(true)
        .expect("set client nonblocking");
    let error = client
        .read(&mut byte)
        .expect_err("ordinary reply must wait for pending writes");
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    assert!(backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(replica_token)
    ));
    assert!(backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(client_token)
    ));

    backend
        .dispatch_before_poll_slot(
            crate::runtime::server::worker::reactor::BeforePollSlot::DistributedWrites,
            crate::runtime::server::worker::reactor::DispatchContext::Normal,
        )
        .expect("dispatch distributed writes");

    replica
        .set_nonblocking(false)
        .expect("restore replica peer blocking mode");
    replica
        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
        .expect("set replica peer read timeout");
    let mut replicated = vec![0_u8; replication_frame.len()];
    replica
        .read_exact(&mut replicated)
        .expect("read replication after distributed writes");
    assert_eq!(replicated, replication_frame);
    let error = client
        .read(&mut byte)
        .expect_err("distributed writes must not serve the ordinary reply");
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);

    backend.drive_pending_ordinary_writes();

    client
        .set_nonblocking(false)
        .expect("restore blocking mode");
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .expect("set client read timeout");
    let mut reply = vec![0_u8; client_reply.len()];
    client.read_exact(&mut reply).expect("read origin reply");
    assert_eq!(reply, client_reply);
}

#[test]
fn ordinary_read_refreshes_dirty_replication_memory_before_info() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    let (mut client, _client_slab, client_token) = add_socket_connection(&mut backend);
    backend.ctx.replication_mut().replica_output_buffer_bytes = 123;
    backend.replication_buffer_accounting_dirty = true;

    backend.writes.ordinary.begin_turn();
    client
        .write_all(b"*2\r\n$4\r\nINFO\r\n$6\r\nMEMORY\r\n")
        .expect("write INFO MEMORY");
    backend.transport.wait_readable_for_test(client_token);
    backend.handle_readable(client_token.0, ClientReadMode::Fair);
    backend.drive_pending_ordinary_writes();

    client
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .expect("set INFO read timeout");
    let mut reply = vec![0_u8; 64 * 1024];
    let count = client.read(&mut reply).expect("read INFO MEMORY reply");
    let reply = String::from_utf8_lossy(&reply[..count]);
    assert!(
        reply.contains("mem_total_replication_buffers:0\r\n"),
        "INFO used stale replication memory: {reply}"
    );
}

#[test]
fn close_after_reply_does_not_take_a_second_ordinary_write_service() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let (mut client, connection_token, token) = add_socket_connection(&mut backend);
    backend.writes.ordinary.begin_turn();
    assert!(backend.writes.ordinary.claim_service(token));
    backend.connection_registry[Token(connection_token)].state = ConnectionState::CloseAfterReply;
    let mut effects = ReadBatchEffects::default();
    effects.replies.push(b"+LATE\r\n".to_vec());

    backend.finalize_handle_readable(connection_token, token.0, effects);

    client
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .expect("set client read timeout");
    let mut received = Vec::new();
    client
        .read_to_end(&mut received)
        .expect("connection close must reach the peer");
    assert!(
        received.is_empty(),
        "a second same-turn flush leaked output"
    );
    assert!(!backend.connection_registry.contains(Token(token.0)));
}

#[test]
fn quit_stops_pipelined_commands_and_sends_one_terminal_reply() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    let (mut client, _connection_token, token) = add_socket_connection(&mut backend);
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .expect("set client read timeout");
    backend.writes.ordinary.begin_turn();
    assert!(backend.writes.ordinary.claim_service(token));
    client
        .write_all(b"*1\r\n$4\r\nQUIT\r\n")
        .expect("write QUIT after this turn's writable service");
    backend.transport.wait_readable_for_test(token);

    backend.handle_readable(token.0, ClientReadMode::Fair);
    client
        .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
        .expect("write the later pipelined SET segment");
    backend.transport.wait_readable_for_test(token);
    backend.handle_readable(token.0, ClientReadMode::Fair);

    backend.drive_pending_ordinary_writes();
    backend.event_batch.begin_next_turn();
    backend.writes.ordinary.begin_turn();
    backend.drive_pending_ordinary_writes();

    let mut received = Vec::new();
    if let Err(error) = client.read_to_end(&mut received) {
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
    }
    assert_eq!(received, b"+OK\r\n");
    assert!(!backend
        .ctx
        .db(0)
        .expect("default db")
        .exists_no_expire(b"foo"));
    assert!(!backend.connection_registry.contains(Token(token.0)));
}

#[test]
fn registered_handler_uses_one_command_time_snapshot() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_fake_connection(&mut backend);
    let args = make_args(&[b"TIME-SNAPSHOT"]);

    let reply = backend.execute_registered_handler(
        DispatchGroup::Default,
        connection_token,
        crate::framework::CommandExecutor::Immediate(command_time_observing_handler),
        &args,
    );

    let Value::Array(samples) = reply else {
        panic!("time observer must return both samples");
    };
    assert_eq!(samples.len(), 2);
    assert_eq!(samples[0], samples[1]);
}

#[test]
fn standalone_replica_redirect_precedes_readonly_for_capable_clients() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_fake_connection(&mut backend);
    backend.ctx.replication_mut().switch_to_replica();
    backend.ctx.config_mut().replication.master_host = Some("192.0.2.1".to_string());
    backend.ctx.config_mut().replication.master_port = 6380;
    backend.connection_registry[Token(connection_token)]
        .capa_flags
        .push("redirect".to_string());
    let spec = make_spec("set", 3, crate::framework::CMD_WRITE, 1, 1, 1);
    let args = make_args(&[b"SET", b"key", b"value"]);

    let outcome = backend.pre_dispatch_exec(connection_token, meta_from_spec(&spec), &args);

    assert_eq!(outcome.action, DispatchAction::Rejected);
    assert_eq!(
        outcome.reject_reply,
        Some(Value::error("REDIRECT 192.0.2.1:6380"))
    );
}

#[test]
fn standalone_replica_redirect_postpones_during_failover() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_fake_connection(&mut backend);
    backend.ctx.replication_mut().switch_to_replica();
    backend.ctx.replication_mut().failover_state = "failover-in-progress".to_string();
    backend.ctx.config_mut().replication.master_host = Some("192.0.2.1".to_string());
    backend.ctx.config_mut().replication.master_port = 6380;
    backend.connection_registry[Token(connection_token)]
        .capa_flags
        .push("redirect".to_string());
    let spec = make_spec("get", 2, crate::framework::CMD_READONLY, 1, 1, 1);
    let args = make_args(&[b"GET", b"key"]);

    let outcome = backend.pre_dispatch_exec(connection_token, meta_from_spec(&spec), &args);

    assert_eq!(
        outcome.action,
        DispatchAction::Postpone(PostponeReason::ClientPause)
    );
    assert!(outcome.reject_reply.is_none());
}

#[test]
fn acl_denial_precedes_standalone_replica_redirect() {
    const USERNAME: &str = "redirect-order-test";
    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_fake_connection(&mut backend);
    backend.ctx.replication_mut().switch_to_replica();
    backend.ctx.config_mut().replication.master_host = Some("192.0.2.1".to_string());
    backend.ctx.config_mut().replication.master_port = 6380;
    backend.connection_registry[Token(connection_token)].username = USERNAME.to_string();
    backend.connection_registry[Token(connection_token)]
        .capa_flags
        .push("redirect".to_string());
    let mut user = crate::engine::extension::acl::AclUser::new_disabled();
    user.apply_rule("on").unwrap();
    user.apply_rule("nopass").unwrap();
    user.apply_rule("+get").unwrap();
    user.apply_rule("allkeys").unwrap();
    crate::engine::extension::acl::acl_users()
        .lock()
        .unwrap()
        .insert(USERNAME.to_string(), user);
    let spec = make_spec("set", 3, crate::framework::CMD_WRITE, 1, 1, 1);
    let args = make_args(&[b"SET", b"key", b"value"]);

    let outcome = backend.pre_dispatch_exec(connection_token, meta_from_spec(&spec), &args);
    crate::engine::extension::acl::acl_users()
        .lock()
        .unwrap()
        .remove(USERNAME);

    assert_eq!(outcome.action, DispatchAction::Rejected);
    assert!(matches!(
        outcome.reject_reply,
        Some(Value::Error(ref error)) if error.starts_with(b"NOPERM ")
    ));
}

#[test]
fn postponed_transaction_command_breaks_the_pipeline() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_command_registry_snapshot(&backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[Token(connection_token)].authenticated = true;
    assert!(backend.connection_registry[Token(connection_token)]
        .transaction
        .begin(0, 0));
    backend.connection_registry[Token(connection_token)]
        .capa_flags
        .push("redirect".to_string());
    backend.ctx.replication_mut().switch_to_replica();
    backend.ctx.replication_mut().failover_state = "failover-in-progress".to_string();
    backend.ctx.config_mut().replication.master_host = Some("192.0.2.1".to_string());
    backend.ctx.config_mut().replication.master_port = 6380;
    let args = make_args(&[b"SET", b"key", b"value"]);
    let mut effects = ReadBatchEffects::default();

    assert_eq!(
        backend.process_parsed_command(Token(42), args, "set".to_string(), false, &mut effects,),
        super::types::LoopAction::Break
    );
    assert!(backend.connection_registry[Token(connection_token)]
        .blocked
        .is_some());
}

#[test]
fn slot_migration_control_connection_bypasses_client_pause() {
    use crate::engine::distributed::cluster::migration::{
        MigrationJobId, SlotMigrationConnectionRole,
    };
    use crate::framework::context::PauseAction;

    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[Token(connection_token)].slot_migration_role =
        Some(SlotMigrationConnectionRole::Export(
            MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
        ));
    backend.ctx.client_pause_mut().action = PauseAction::Write;
    backend.ctx.client_pause_mut().end_time_ms = i64::MAX;
    backend.ctx.client_pause_mut().reason = "slot_migration_in_progress";
    let spec = backend.registry.lookup(b"cluster").unwrap();
    let mut meta = meta_from_spec(spec);
    meta.flags = crate::engine::command::CMD_MAY_REPLICATE;
    let args = make_args(&[b"CLUSTER", b"SYNCSLOTS", b"REQUEST-FAILOVER"]);

    let outcome = backend.pre_dispatch_exec(connection_token, meta, &args);

    assert_eq!(outcome.action, DispatchAction::Execute);
}

#[test]
fn generic_export_reply_reconciles_two_export_aggregate_through_teardown() {
    use crate::engine::distributed::cluster::migration::{
        MigrationJobId, SlotMigrationConnectionRole,
    };
    use crate::framework::distributed::migration::SlotExportLink;

    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let (_export_a_peer, export_a_slab, export_a_token) = add_socket_connection(&mut backend);
    let export_a_id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    backend.connection_registry[Token(export_a_slab)].slot_migration_role =
        Some(SlotMigrationConnectionRole::Export(export_a_id.clone()));
    backend.connection_registry[Token(export_a_slab)]
        .queue_replication_reply(vec![b'a'; 128 * 1024]);
    backend
        .slot_migration_runtime
        .register_export(SlotExportLink::new(export_a_id, export_a_token.0));
    backend.track_slot_export_connection_output(export_a_slab);

    let export_b_id = MigrationJobId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
    let export_b_token = Token(4242);
    let mut export_b = Connection::new_fake();
    export_b.set_token_for_test(export_b_token);
    export_b.slot_migration_role = Some(SlotMigrationConnectionRole::Export(export_b_id.clone()));
    export_b.queue_replication_reply(vec![b'b'; 4096]);
    let export_b_slab = backend.connection_registry.insert(export_b);
    backend
        .slot_migration_runtime
        .register_export(SlotExportLink::new(export_b_id, export_b_token.0));
    backend.track_slot_export_connection_output(export_b_slab);

    let expected_initial = backend.connection_registry[Token(export_a_slab)]
        .output_buffer_allocation_bytes()
        .saturating_add(
            backend.connection_registry[Token(export_b_slab)].output_buffer_allocation_bytes(),
        );
    assert_eq!(
        backend.writes.slot_export_allocation_bytes,
        expected_initial
    );

    let args = make_args(&[b"PING"]);
    let mut effects = ReadBatchEffects::default();
    assert_eq!(
        backend.process_parsed_command(
            export_a_token,
            args,
            "ping".to_string(),
            false,
            &mut effects,
        ),
        LoopAction::Continue
    );
    backend.finalize_handle_readable(export_a_slab, export_a_token.0, effects);

    let expected_after_reply = backend.connection_registry[Token(export_a_slab)]
        .output_buffer_allocation_bytes()
        .saturating_add(
            backend.connection_registry[Token(export_b_slab)].output_buffer_allocation_bytes(),
        );
    assert_eq!(
        backend.writes.slot_export_allocation_bytes, expected_after_reply,
        "postloop generic reply allocation must be reconciled after a partial export flush"
    );
    assert!(backend.queue_client_reply(export_a_token, vec![b'p'; 31]));
    assert_eq!(
        backend.writes.slot_export_allocation_bytes,
        backend.connection_registry[Token(export_a_slab)]
            .output_buffer_allocation_bytes()
            .saturating_add(
                backend.connection_registry[Token(export_b_slab)].output_buffer_allocation_bytes(),
            ),
        "generic push/control queueing must reconcile the same export owner"
    );

    backend.remove_connection(export_a_token.0);

    assert_eq!(
        backend.writes.slot_export_allocation_bytes,
        backend.connection_registry[Token(export_b_slab)].output_buffer_allocation_bytes(),
        "removing export A must preserve exactly export B's registered ownership"
    );
}

#[test]
fn generic_export_reply_waits_for_pending_write_phase() {
    use crate::engine::distributed::cluster::migration::{
        MigrationJobId, SlotMigrationConnectionRole,
    };
    use crate::framework::distributed::migration::SlotExportLink;

    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    let (mut target, connection_token, token) = add_socket_connection(&mut backend);
    let id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .expect("valid migration id");
    backend.connection_registry[Token(connection_token)].slot_migration_role =
        Some(SlotMigrationConnectionRole::Export(id.clone()));
    backend
        .slot_migration_runtime
        .register_export(SlotExportLink::new(id, token.0));
    backend.writes.slot_exports.begin_turn();
    let reply = b"+GENERIC\r\n".to_vec();
    let mut effects = ReadBatchEffects::default();
    effects.replies.push(reply.clone());

    backend.finalize_handle_readable(connection_token, token.0, effects);

    assert_eq!(
        backend.connection_registry[Token(connection_token)].pending_reply_bytes(),
        reply.len(),
        "generic export output must remain owned until before-sleep"
    );
    assert_eq!(
        backend.writes.slot_export_allocation_bytes,
        backend.connection_registry[Token(connection_token)].output_buffer_allocation_bytes()
    );
    target
        .set_nonblocking(true)
        .expect("set export target nonblocking");
    let mut byte = [0_u8; 1];
    let error = target
        .read(&mut byte)
        .expect_err("generic export reply must not bypass the pending phase");
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);

    backend.drive_pending_slot_export_writes();

    target
        .set_nonblocking(false)
        .expect("restore blocking export target");
    target
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .expect("set export target read timeout");
    let mut received = vec![0_u8; reply.len()];
    target
        .read_exact(&mut received)
        .expect("pending phase must write the generic reply");
    assert_eq!(received, reply);
    assert_eq!(
        backend.connection_registry[Token(connection_token)].pending_reply_bytes(),
        0
    );
}

#[test]
fn client_last_command_uses_cluster_subcommand_fullname() {
    let args = make_args(&[b"CLUSTER", b"SYNCSLOTS", b"ACK"]);
    assert_eq!(
        super::command_exec::client_last_command_name("cluster", &args),
        "cluster|syncslots"
    );
}

#[test]
fn replica_reply_diagnostics_include_xinfo_and_slowlog_subcommands() {
    for (command, subcommand, expected) in [
        ("xinfo", b"HELP".as_slice(), "xinfo|help"),
        ("slowlog", b"GET".as_slice(), "slowlog|get"),
    ] {
        let args = make_args(&[command.as_bytes(), subcommand]);
        assert_eq!(
            super::command_exec::client_last_command_name(command, &args),
            expected
        );
    }
}

#[test]
fn multi_queue_honors_asking_for_an_importing_slot() {
    use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};

    const SOURCE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().server.cluster_enabled = true;
    let connection_token = add_fake_connection(&mut backend);
    assert!(backend.connection_registry[Token(connection_token)]
        .transaction
        .begin(0, 0));
    backend.connection_registry[Token(connection_token)].asking = true;
    let mut cluster =
        crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16_379);
    let slot = crate::engine::distributed::cluster::node::key_hash_slot(b"{3560}missing");
    let mut source = ClusterNode::new(
        SOURCE.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    source.flags &= !CLUSTER_NODE_MYSELF;
    source.slots[slot as usize] = true;
    source.numslots = 1;
    cluster.nodes.insert(SOURCE.to_string(), source);
    cluster.seed_importing_slot(slot as usize, SOURCE);
    *backend.ctx.cluster_state_mut() = Some(Box::new(cluster));
    let args = make_args(&[b"EXISTS", b"{3560}missing"]);
    let mut replies = Vec::new();

    assert_eq!(
        backend.try_handle_multi_queue(
            Token(connection_token),
            &args,
            "exists",
            false,
            &mut replies
        ),
        Some(false)
    );
    assert!(
        replies.is_empty(),
        "fake connections suppress direct replies"
    );
    let queued = &backend.connection_registry[Token(connection_token)]
        .transaction
        .queued_commands();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].name, args.name);
    assert_eq!(queued[0].args, args.args);
}

#[test]
fn multi_queue_defers_migrating_slot_redirect_until_exec() {
    use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};

    const TARGET: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().server.cluster_enabled = true;
    let connection_token = add_fake_connection(&mut backend);
    assert!(backend.connection_registry[Token(connection_token)]
        .transaction
        .begin(0, 0));
    let mut cluster =
        crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16_379);
    let slot = crate::engine::distributed::cluster::node::key_hash_slot(b"{3560}missing");
    cluster.add_slot(slot as usize).unwrap();
    let mut target = ClusterNode::new(
        TARGET.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    target.flags &= !CLUSTER_NODE_MYSELF;
    cluster.nodes.insert(TARGET.to_string(), target);
    cluster.seed_migrating_slot(slot as usize, TARGET);
    *backend.ctx.cluster_state_mut() = Some(Box::new(cluster));
    let args = make_args(&[b"EXISTS", b"{3560}missing"]);
    let mut replies = Vec::new();

    assert_eq!(
        backend.try_handle_multi_queue(
            Token(connection_token),
            &args,
            "exists",
            false,
            &mut replies
        ),
        Some(false)
    );
    assert_eq!(
        backend.connection_registry[Token(connection_token)]
            .transaction
            .queued_commands()
            .len(),
        1
    );
}

#[test]
fn bounded_normal_client_read_yields_after_one_io_buffer() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
    let address = listener.local_addr().expect("read test listener address");
    let (written_tx, written_rx) = mpsc::channel();

    let mut backend = ServerWorkerState::new(1).expect("create event loop");
    let peer = backend
        .transport
        .connect_tcp(address)
        .expect("connect test transport");
    let payload_len = crate::runtime::server::worker::NORMAL_CLIENT_READ_BUDGET * 2;
    backend
        .transport
        .reserve_test_receive_buffer(peer.token, payload_len)
        .expect("size receive buffer for the unread read-budget payload");
    let writer = thread::spawn(move || {
        let (mut peer, _) = listener.accept().expect("accept test connection");
        peer.write_all(&vec![b'x'; payload_len])
            .expect("write test payload");
        written_tx.send(()).expect("signal test payload write");
    });

    let mut connection = Connection::new(peer.token, peer.peer_addr, false);
    connection.authenticated = true;
    let connection_token = backend.connection_registry.insert(connection);

    written_rx.recv().expect("wait for payload write");
    backend
        .transport
        .wait_pending_bytes_for_test(peer.token, payload_len);
    let (read, close, yielded) =
        backend.read_from_connection_bounded(peer.token.0, connection_token);

    assert!(!close);
    assert!(yielded);
    assert!(!read.is_empty());
    assert!(read.len() <= crate::runtime::server::worker::NORMAL_CLIENT_READ_BUDGET);
    writer.join().expect("join test writer");
}

#[test]
fn shutdown_uses_l4_dispatch_group() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);

    assert_eq!(
        backend.registry.lookup(b"shutdown").unwrap().dispatch_group,
        DispatchGroup::Shutdown
    );
}

#[test]
fn busy_script_shutdown_blocks_request_until_shutdown_finishes() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    let token_value = backend.connection_registry[Token(connection_token)]
        .token()
        .0;
    let run = Box::new(
        crate::engine::extension::script::ScriptRunCtx::new_bare_for_test(
            crate::engine::extension::script::SCRIPT_EVAL_MODE,
        ),
    );
    run.control.mark_timed_out();
    backend.ctx.script_mut().running_script = Some(run.control.clone());
    backend.ctx.set_script_busy(false, true);
    let args = make_args(&[b"shutdown", b"nosave"]);
    let mut effects = ReadBatchEffects::default();

    assert_eq!(
        backend.process_parsed_command(
            Token(token_value),
            args,
            "shutdown".to_string(),
            false,
            &mut effects,
        ),
        LoopAction::Break
    );

    assert!(matches!(
        backend.connection_registry[Token(connection_token)]
            .blocked
            .as_ref()
            .map(|blocked| &blocked.cmd),
        Some(BlockedCmd::Shutdown)
    ));
    assert!(effects.replies.is_empty());
    assert_ne!(
        backend.connection_registry[Token(connection_token)].state,
        ConnectionState::CloseAfterReply
    );
    assert!(backend.ctx.shutdown_state().asap);
    assert!(run.is_killed());
}

#[test]
fn immediate_shutdown_blocks_without_reply_and_stops_pipeline() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener_token = backend.listeners.all[0];
    let connection_token = add_fake_connection(&mut backend);
    let token_value = backend.connection_registry[Token(connection_token)]
        .token()
        .0;
    let args = make_args(&[b"shutdown", b"nosave"]);
    let mut effects = ReadBatchEffects::default();

    assert_eq!(
        backend.process_parsed_command(
            Token(token_value),
            args,
            "shutdown".to_string(),
            false,
            &mut effects,
        ),
        LoopAction::Break
    );

    assert!(backend.ctx.is_shutdown_requested());
    assert!(backend.listeners.all.is_empty());
    assert!(!backend.transport.is_registered(listener_token));
    assert!(
        effects.replies.is_empty(),
        "successful shutdown must not publish a RESP reply"
    );
    assert!(matches!(
        backend.connection_registry[Token(connection_token)]
            .blocked
            .as_ref()
            .map(|blocked| &blocked.cmd),
        Some(BlockedCmd::Shutdown)
    ));
}

#[test]
fn pre_dispatch_accepts_correct_arity() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_fake_connection(&mut backend);
    let spec = make_spec("set", 3, CMD_WRITE, 1, 1, 1);
    let meta = meta_from_spec(&spec);
    let args = make_args(&[b"set", b"k", b"v"]);
    let outcome = backend.pre_dispatch_exec(connection_token, meta, &args);
    assert_eq!(outcome.action, DispatchAction::Execute);
    assert!(outcome.call_flags.is_write);
    assert!(outcome.reject_reply.is_none());
}

#[test]
fn pre_dispatch_rejects_exact_arity_mismatch() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token = add_fake_connection(&mut backend);
    let spec = make_spec("set", 3, 0, 1, 1, 1);
    let meta = meta_from_spec(&spec);
    let args = make_args(&[b"set", b"k"]);
    let outcome = backend.pre_dispatch_exec(token, meta, &args);
    assert_eq!(outcome.action, DispatchAction::Rejected);
    assert!(outcome.reject_reply.is_some());
    let err = outcome.reject_reply.unwrap();
    match err {
        Value::Error(msg) => {
            let s = String::from_utf8_lossy(&msg);
            assert!(
                s.contains("wrong number of arguments"),
                "expected arity error, got: {}",
                s
            );
        }
        other => panic!("expected Error, got {:?}", other),
    }
}

#[test]
fn pre_dispatch_accepts_min_arity_with_extra_args() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_fake_connection(&mut backend);
    // Negative arity -N means at least N args (command + N-1)
    // arity=-2: command + at least 1 arg (like EXISTS key [key ...])
    let spec = make_spec("exists", -2, 0, 1, -1, 1);
    let meta = meta_from_spec(&spec);
    let args = make_args(&[b"exists", b"k1", b"k2", b"k3"]);
    let outcome = backend.pre_dispatch_exec(connection_token, meta, &args);
    assert_eq!(outcome.action, DispatchAction::Execute);
    assert!(!outcome.call_flags.is_write);
}

#[test]
fn pre_dispatch_rejects_below_min_arity() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token = add_fake_connection(&mut backend);
    let spec = make_spec("exists", -2, 0, 1, -1, 1);
    let meta = meta_from_spec(&spec);
    let args = make_args(&[b"exists"]);
    let outcome = backend.pre_dispatch_exec(token, meta, &args);
    assert_eq!(outcome.action, DispatchAction::Rejected);
}

#[test]
fn no_multi_rejects_flagged_command_inside_tx() {
    let spec = make_spec("save", 1, crate::framework::CMD_NO_MULTI, 0, 0, 0);
    let meta = meta_from_spec(&spec);
    let err = ServerWorkerState::check_no_multi_pure(&meta, "save", "save");
    assert!(err.is_some(), "SAVE should be rejected inside MULTI");
    let msg = err.unwrap();
    match msg {
        Value::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.contains("not allowed inside a transaction"));
        }
        other => panic!("expected Error, got {:?}", other),
    }
}

#[test]
fn no_multi_exempts_tx_control_commands() {
    for &name in &["exec", "discard", "multi", "reset", "watch", "unwatch"] {
        let spec = make_spec(name, 1, crate::framework::CMD_NO_MULTI, 0, 0, 0);
        let meta = meta_from_spec(&spec);
        let err = ServerWorkerState::check_no_multi_pure(&meta, name, name);
        assert!(err.is_none(), "{} should be exempt from NO_MULTI", name);
    }
}

#[test]
fn no_multi_allows_commands_without_flag() {
    let spec = make_spec("get", 2, 0, 1, 1, 1);
    let meta = meta_from_spec(&spec);
    let err = ServerWorkerState::check_no_multi_pure(&meta, "get", "get");
    assert!(
        err.is_none(),
        "GET (without NO_MULTI flag) should be allowed in MULTI"
    );
}

#[test]
fn no_multi_error_uses_the_resolved_subcommand_name() {
    let spec = make_spec("client", -2, crate::framework::CMD_NO_MULTI, 0, 0, 0);
    let meta = meta_from_spec(&spec);
    assert_eq!(
        ServerWorkerState::check_no_multi_pure(&meta, "client", "client|reply"),
        Some(Value::error(
            "ERR Command 'client|reply' not allowed inside a transaction"
        ))
    );
}

#[test]
fn call_flags_write_bit_set_for_write_commands() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_fake_connection(&mut backend);
    let spec = make_spec("set", 3, CMD_WRITE, 1, 1, 1);
    let meta = meta_from_spec(&spec);
    let args = make_args(&[b"set", b"k", b"v"]);
    let outcome = backend.pre_dispatch_exec(connection_token, meta, &args);
    assert!(outcome.call_flags.is_write);
}

#[test]
fn call_flags_write_bit_clear_for_read_commands() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_fake_connection(&mut backend);
    let spec = make_spec("get", 2, 0, 1, 1, 1);
    let meta = meta_from_spec(&spec);
    let args = make_args(&[b"get", b"k"]);
    let outcome = backend.pre_dispatch_exec(connection_token, meta, &args);
    assert!(!outcome.call_flags.is_write);
}

#[test]
fn good_replicas_count_excludes_stale_acknowledgements() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().replication.min_replicas_max_lag = 1;
    let connection_id = backend.replication_runtime.register_primary(
        ConnectionId(50),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let stale_ack_ms = crate::foundation::io::sys::time::mstime() - 2_000;
    let link = backend
        .replication_runtime
        .primary_link_mut(connection_id)
        .expect("registered replica link");
    link.set_state(PrimaryLinkState::Streaming);
    link.acknowledge(offset(0), stale_ack_ms);

    assert_eq!(backend.good_replicas_count(), 0);
}

#[test]
fn zero_max_lag_disables_direct_and_script_min_replicas_gates() {
    use crate::engine::extension::script::ScriptRuntime;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_fake_connection(&mut backend);
    backend.ctx.config_mut().replication.min_replicas_to_write = 1;
    backend.ctx.config_mut().replication.min_replicas_max_lag = 0;
    let spec = make_spec("set", 3, CMD_WRITE, 1, 1, 1);
    let meta = meta_from_spec(&spec);
    let args = make_args(&[b"set", b"key", b"value"]);

    let facts =
        super::CommandClientFacts::capture(&backend.connection_registry[Token(connection_token)]);
    assert!(
        backend
            .stage_min_replicas(&facts, &meta, &args, CmdCallFlags::default())
            .is_none(),
        "C disables the write gate when min-replicas-max-lag is zero"
    );
    assert!(backend.ctx.has_enough_replicas_to_write());
}

#[test]
fn script_write_protection_excludes_stale_replicas() {
    use crate::engine::extension::script::ScriptRuntime;

    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().replication.min_replicas_to_write = 1;
    backend.ctx.config_mut().replication.min_replicas_max_lag = 1;
    let id = backend.replication_runtime.register_primary(
        ConnectionId(51),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let link = backend
        .replication_runtime
        .primary_link_mut(id)
        .expect("registered replica link");
    link.set_state(PrimaryLinkState::Streaming);
    link.acknowledge(
        offset(0),
        crate::foundation::io::sys::time::mstime() - 2_000,
    );

    assert!(!backend.ctx.has_enough_replicas_to_write());
}

#[test]
fn script_write_protection_rejects_replica_without_an_ack() {
    use crate::engine::extension::script::ScriptRuntime;

    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().replication.min_replicas_to_write = 1;
    backend.ctx.config_mut().replication.min_replicas_max_lag = 2;
    let id = backend.replication_runtime.register_primary(
        ConnectionId(52),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6382,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(id)
        .expect("registered replica link")
        .set_state(PrimaryLinkState::Streaming);
    backend.refresh_replication_view();

    assert!(!backend.ctx.has_enough_replicas_to_write());
}

#[test]
fn cluster_rejected_swapdb_never_enters_the_replication_stream() {
    let mut backend = ServerWorkerState::new(2).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().server.cluster_enabled = true;
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[Token(connection_token)].set_skip_propagation(false);
    let token = backend.connection_registry[Token(connection_token)]
        .token()
        .0;
    let mut effects = ReadBatchEffects::default();

    assert_eq!(
        backend.process_parsed_command(
            Token(token),
            make_args(&[b"SWAPDB", b"0", b"1"]),
            "swapdb".to_string(),
            false,
            &mut effects,
        ),
        LoopAction::Continue
    );

    assert!(effects.replication_only.is_empty());
    assert!(effects.aof_only.is_empty());
    assert_eq!(backend.ctx.persistence().dirty(), 0);
}

#[test]
fn direct_set_builds_compatible_frames_for_both_sinks() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().server.cluster_enabled = true;
    backend.ctx.config_mut().server.cluster_slot_stats_enabled = true;
    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(51),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .unwrap()
        .set_state(PrimaryLinkState::Streaming);
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[Token(connection_token)].set_skip_propagation(false);
    backend.ctx.set_repl_seldb(0);
    backend.ctx.persistence_mut().set_aof_selected_db(0);
    let args = make_args(&[b"SET", b"key", b"value"]);
    let flags = backend.registry.lookup(b"set").unwrap().flags;
    let mut effects = DispatchSideEffects::default();
    let expected = crate::engine::distributed::replication::protocol::encode_command(&[
        b"SET", b"key", b"value",
    ]);
    let connection_effects = take_connection_command_effects(&mut backend, connection_token);

    backend.build_command_propagation(
        CommandPropagationInput {
            connection_token,
            token_value: 42,
            args: &args,
            name: "set",
            command_flags: flags,
            rejected: false,
        },
        connection_effects,
        &mut effects,
    );

    assert_eq!(effects.replication_only, vec![expected.clone()]);
    assert_eq!(effects.aof_only, vec![expected]);
    assert_eq!(backend.ctx.repl_seldb(), 0);
    assert_eq!(backend.ctx.persistence().aof_selected_db(), 0);
    let slot = crate::engine::distributed::cluster::node::key_hash_slot(b"key") as usize;
    assert_eq!(
        backend
            .ctx
            .slot_metrics()
            .counters(slot)
            .unwrap()
            .network_bytes_out,
        33
    );
}

#[test]
fn writable_replica_local_write_does_not_append_to_replication_history() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.replication_mut().switch_to_replica();
    backend.ctx.config_mut().replication.replica_read_only = false;
    backend.ctx.replication_mut().backlog.activate();
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[Token(connection_token)].set_skip_propagation(false);
    let token = backend.connection_registry[Token(connection_token)]
        .token()
        .0;
    let mut effects = ReadBatchEffects::default();

    assert_eq!(
        backend.process_parsed_command(
            Token(token),
            make_args(&[b"SET", b"local-key", b"value"]),
            "set".to_string(),
            false,
            &mut effects,
        ),
        LoopAction::Continue
    );
    backend.finalize_handle_readable(connection_token, token, effects);

    assert_eq!(backend.ctx.replication().backlog.history_len(), 0);
    assert_eq!(
        backend
            .ctx
            .db(0)
            .and_then(|db| db.get_no_expire(b"local-key"))
            .map(crate::foundation::io::store::Robj::as_bytes),
        Some(b"value".as_slice())
    );
}

#[test]
fn rewrite_force_select_resets_only_the_aof_cursor() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[Token(connection_token)].set_skip_propagation(false);
    backend.ctx.set_repl_seldb(0);
    backend.ctx.persistence_mut().set_aof_selected_db(0);
    backend.ctx.aof_mut().rewrite.force_select = true;
    let args = make_args(&[b"SET", b"key", b"value"]);
    let flags = backend.registry.lookup(b"set").unwrap().flags;
    let mut effects = DispatchSideEffects::default();
    let set = crate::engine::distributed::replication::protocol::encode_command(&[
        b"SET", b"key", b"value",
    ]);
    let select =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SELECT", b"0"]);
    let connection_effects = take_connection_command_effects(&mut backend, connection_token);

    backend.build_command_propagation(
        CommandPropagationInput {
            connection_token,
            token_value: 42,
            args: &args,
            name: "set",
            command_flags: flags,
            rejected: false,
        },
        connection_effects,
        &mut effects,
    );

    assert_eq!(effects.replication_only, vec![set.clone()]);
    assert_eq!(effects.aof_only, vec![select, set]);
    assert_eq!(backend.ctx.repl_seldb(), 0);
    assert_eq!(backend.ctx.persistence().aof_selected_db(), 0);
}

#[test]
fn command_time_eviction_queues_unlink_from_independent_sink_cursors() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.ctx.replication_mut().backlog.activate();
    backend.ctx.set_repl_seldb(10);
    backend.ctx.persistence_mut().set_aof_selected_db(11);
    let mut effects = DispatchSideEffects::default();
    let unlink =
        crate::engine::distributed::replication::protocol::encode_command(&[b"UNLINK", b"victim"]);
    let select =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SELECT", b"10"]);

    backend._queue_eviction_unlink(10, b"victim", &mut effects);

    assert_eq!(effects.replication_only, vec![unlink.clone()]);
    assert_eq!(effects.aof_only, vec![select, unlink]);
    assert_eq!(backend.ctx.repl_seldb(), 10);
    assert_eq!(backend.ctx.persistence().aof_selected_db(), 10);
}

#[test]
fn lazy_expiry_sequences_selects_from_each_sink_cursor() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    backend.ctx.set_repl_seldb(10);
    backend.ctx.persistence_mut().set_aof_selected_db(11);
    backend
        .ctx
        .db_mut(10)
        .unwrap()
        .expired_deletions
        .push(b"expired".to_vec());
    let args = make_args(&[b"GET", b"expired"]);
    let flags = backend.registry.lookup(b"get").unwrap().flags;
    let mut effects = DispatchSideEffects::default();
    let unlink =
        crate::engine::distributed::replication::protocol::encode_command(&[b"UNLINK", b"expired"]);
    let select =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SELECT", b"10"]);
    let connection_effects = take_connection_command_effects(&mut backend, connection_token);

    backend.build_command_propagation(
        CommandPropagationInput {
            connection_token,
            token_value: 42,
            args: &args,
            name: "get",
            command_flags: flags,
            rejected: false,
        },
        connection_effects,
        &mut effects,
    );

    assert_eq!(effects.replication_only, vec![unlink.clone()]);
    assert_eq!(effects.aof_only, vec![select, unlink]);
    assert_eq!(backend.ctx.repl_seldb(), 10);
    assert_eq!(backend.ctx.persistence().aof_selected_db(), 10);
}

#[test]
fn lazy_expiry_uses_del_when_lazyfree_expire_is_disabled() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    backend.ctx.config_mut().server.lazyfree_lazy_expire = false;
    backend.ctx.set_repl_seldb(0);
    backend.ctx.persistence_mut().set_aof_selected_db(0);
    backend
        .ctx
        .db_mut(0)
        .unwrap()
        .expired_deletions
        .push(b"expired".to_vec());
    let args = make_args(&[b"GET", b"expired"]);
    let flags = backend.registry.lookup(b"get").unwrap().flags;
    let mut effects = DispatchSideEffects::default();

    let connection_effects = take_connection_command_effects(&mut backend, connection_token);

    backend.build_command_propagation(
        CommandPropagationInput {
            connection_token,
            token_value: 42,
            args: &args,
            name: "get",
            command_flags: flags,
            rejected: false,
        },
        connection_effects,
        &mut effects,
    );

    let del =
        crate::engine::distributed::replication::protocol::encode_command(&[b"DEL", b"expired"]);
    assert_eq!(effects.replication_only, vec![del.clone()]);
    assert_eq!(effects.aof_only, vec![del]);
}

#[test]
fn ordinary_hash_read_hides_ghost_without_propagating_or_notifying() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    backend.ctx.config_mut().server.notify_keyspace_events = "KEA".to_string();
    let db = backend.ctx.db_mut(0).unwrap();
    db.get_or_create_hash(b"hash")
        .unwrap()
        .insert(Bytes::from_static(b"field"), Bytes::from_static(b"value"));
    db.set_hash_field_expire(
        b"hash",
        b"field",
        crate::foundation::io::store::kv_engine::now_ms().saturating_sub(1),
    );
    let args = make_args(&[b"HGET", b"hash", b"field"]);
    let spec = backend.registry.lookup(b"hget").unwrap().clone();
    assert_eq!(
        spec.execute(
            &mut backend.connection_registry[Token(connection_token)],
            &args,
            &mut backend.ctx
        ),
        Value::Null
    );
    let flags = spec.flags;
    let mut effects = DispatchSideEffects::default();
    let connection_effects = take_connection_command_effects(&mut backend, connection_token);

    backend.build_command_propagation(
        CommandPropagationInput {
            connection_token,
            token_value: 42,
            args: &args,
            name: "hget",
            command_flags: flags,
            rejected: false,
        },
        connection_effects,
        &mut effects,
    );

    assert!(effects.replication_only.is_empty());
    assert!(effects.aof_only.is_empty());
    assert!(effects.publish.is_empty());
    let db = backend.ctx.db_mut(0).unwrap();
    assert_eq!(db.get_hash_object(b"hash").unwrap().unwrap().hash_len(), 1);
    assert!(db.get_hash_field_expire(b"hash", b"field").is_some());
    assert_eq!(db.lazy_expired_hash_fields, 0);
}

#[test]
fn lazy_expiry_precedes_the_write_inside_propagated_transaction() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[Token(connection_token)].set_skip_propagation(false);
    backend.ctx.set_repl_seldb(0);
    backend.ctx.persistence_mut().set_aof_selected_db(0);
    backend
        .ctx
        .db_mut(0)
        .unwrap()
        .expired_deletions
        .push(b"set".to_vec());
    let args = make_args(&[b"SADD", b"set", b"member"]);
    let flags = backend.registry.lookup(b"sadd").unwrap().flags;
    let mut effects = DispatchSideEffects::default();
    let frame =
        |parts: &[&[u8]]| crate::engine::distributed::replication::protocol::encode_command(parts);
    let connection_effects = take_connection_command_effects(&mut backend, connection_token);

    backend.build_command_propagation(
        CommandPropagationInput {
            connection_token,
            token_value: 42,
            args: &args,
            name: "sadd",
            command_flags: flags,
            rejected: false,
        },
        connection_effects,
        &mut effects,
    );

    let expected = vec![
        frame(&[b"MULTI"]),
        frame(&[b"UNLINK", b"set"]),
        frame(&[b"SADD", b"set", b"member"]),
        frame(&[b"EXEC"]),
    ];
    assert_eq!(effects.replication_only, expected);
    assert_eq!(effects.aof_only, effects.replication_only);
}

#[test]
fn may_replicate_command_commits_explicit_script_dirty_keys() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    let key = Bytes::from_static(b"list-key");
    backend.connection_registry[Token(connection_token)].set_skip_propagation(true);
    backend.connection_registry[Token(connection_token)]
        .record_custom_propagation(b"*1\r\n$4\r\nPING\r\n".to_vec());
    backend.connection_registry[Token(connection_token)].record_dirty_key((0, key.clone()));
    let args = make_args(&[b"FCALL", b"test", b"1", b"list-key"]);
    let flags = backend.registry.lookup(b"fcall").unwrap().flags;
    let mut effects = DispatchSideEffects::default();

    let connection_effects = take_connection_command_effects(&mut backend, connection_token);

    backend.build_command_propagation(
        CommandPropagationInput {
            connection_token,
            token_value: 42,
            args: &args,
            name: "fcall",
            command_flags: flags,
            rejected: false,
        },
        connection_effects,
        &mut effects,
    );

    assert_eq!(effects.watch_dirty, vec![(0, key)]);
    assert!(backend.connection_registry[Token(connection_token)]
        .dirty_keys()
        .is_empty());
    assert_eq!(backend.ctx.persistence().dirty(), 0);
}

#[test]
fn cluster_redirect_uses_dynamic_migrate_keys() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let spec = backend.registry.lookup(b"migrate").unwrap();
    let meta = CmdPreconditions {
        name: spec.name.clone(),
        arity: spec.arity,
        flags: spec.flags,
        first_key: spec.first_key,
        last_key: spec.last_key,
        key_step: spec.key_step,
        key_flags: spec.key_flags,
        acl_categories: spec.acl_categories,
        get_keys: spec.get_keys,
    };
    let args = make_args(&[
        b"MIGRATE",
        b"127.0.0.1",
        b"6380",
        b"",
        b"0",
        b"5000",
        b"KEYS",
        b"actual-key",
    ]);

    let keys = ServerWorkerState::cluster_redirect_keys(&meta, &args);

    assert_eq!(keys, vec![b"actual-key".to_vec()]);
}

#[test]
fn loading_cluster_node_redirects_remote_slot_before_loading_gate() {
    use crate::engine::distributed::cluster::node::{
        key_hash_slot, ClusterNode, CLUSTER_NODE_MYSELF,
    };
    use crate::engine::distributed::cluster::ClusterState;
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().server.cluster_enabled = true;
    backend.ctx.set_loading(true);
    let connection = add_fake_connection(&mut backend);
    let key = b"remote-slot";
    let slot = key_hash_slot(key) as usize;
    let owner = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let mut peer = ClusterNode::new(
        owner.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    peer.flags &= !CLUSTER_NODE_MYSELF;
    peer.slots[slot] = true;
    peer.numslots = 1;
    state.nodes.insert(owner.to_string(), peer);
    state.assign_slot_to_remote(slot, owner).unwrap();
    *backend.ctx.cluster_state_mut() = Some(Box::new(state));
    let args = make_args(&[b"GET", key]);
    let spec = backend.registry.lookup(b"get").unwrap();
    let outcome = backend.pre_dispatch_exec(connection, meta_from_spec(spec), &args);
    assert_eq!(
        outcome.reject_reply,
        Some(Value::error(format!("MOVED {slot} 127.0.0.1:6380")))
    );
    let cluster = backend.ctx.cluster_state_mut().as_deref_mut().unwrap();
    cluster.assign_slot_to_myself(slot).unwrap();
    let spec = backend.registry.lookup(b"get").unwrap();
    let outcome = backend.pre_dispatch_exec(connection, meta_from_spec(spec), &args);
    assert_eq!(
        outcome.reject_reply,
        Some(Value::error(
            "LOADING Valkey is loading the dataset in memory"
        ))
    );
}

#[test]
fn host_task_loading_rejects_replicaof_until_startup_load_finishes() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_host_task_loading(true);
    let connection = add_fake_connection(&mut backend);
    let args = make_args(&[b"REPLICAOF", b"127.0.0.1", b"6379"]);
    let spec = backend.registry.lookup(b"replicaof").unwrap();

    let outcome = backend.pre_dispatch_exec(connection, meta_from_spec(spec), &args);

    assert_eq!(
        outcome.reject_reply,
        Some(Value::error(
            "LOADING Valkey is loading the dataset in memory"
        ))
    );
}

#[test]
fn cluster_redirect_uses_one_shot_asking_on_importing_replica() {
    use crate::engine::distributed::cluster::node::{
        key_hash_slot, ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
    };
    use crate::engine::distributed::cluster::ClusterState;

    const IMPORT_PRIMARY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const SOURCE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let keys = [b"{same}one".as_slice(), b"{same}two".as_slice()];
    let slot = key_hash_slot(keys[0]) as usize;
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().server.cluster_enabled = true;
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[Token(connection_token)].readonly = true;
    backend.connection_registry[Token(connection_token)].asking = true;

    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = IMPORT_PRIMARY.to_string();
    let mut importing_primary = ClusterNode::new(
        IMPORT_PRIMARY.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    importing_primary.flags &= !CLUSTER_NODE_MYSELF;
    state
        .nodes
        .insert(importing_primary.name.clone(), importing_primary);
    let mut source = ClusterNode::new(
        SOURCE.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16_381,
        true,
    );
    source.flags &= !CLUSTER_NODE_MYSELF;
    source.slots[slot] = true;
    source.numslots = 1;
    state.nodes.insert(source.name.clone(), source);
    state.seed_importing_slot(slot, SOURCE);
    *backend.ctx.cluster_state_mut() = Some(Box::new(state));

    let args = make_args(&[b"MGET", keys[0], keys[1]]);
    let spec = backend.registry.lookup(b"mget").unwrap();
    let outcome = backend.pre_dispatch_exec(connection_token, meta_from_spec(spec), &args);

    assert_eq!(outcome.action, DispatchAction::Rejected);
    assert!(matches!(
        outcome.reject_reply,
        Some(Value::Error(ref error)) if error.starts_with(b"TRYAGAIN")
    ));
}

#[test]
fn sharded_pubsub_is_rejected_while_cluster_is_down_when_config_disabled() {
    use crate::engine::distributed::cluster::node::key_hash_slot;
    use crate::engine::distributed::cluster::ClusterState;

    let channel = b"{shard-down}channel";
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().server.cluster_enabled = true;
    backend
        .ctx
        .config_mut()
        .server
        .cluster_allow_pubsubshard_when_down = false;
    let connection_token = add_fake_connection(&mut backend);
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.add_slot(key_hash_slot(channel) as usize).unwrap();
    state.state = 1; // CLUSTER_FAIL is intentionally not part of the public state API.
    *backend.ctx.cluster_state_mut() = Some(Box::new(state));

    let args = make_args(&[b"SSUBSCRIBE", channel]);
    let spec = backend.registry.lookup(b"ssubscribe").unwrap();
    let outcome = backend.pre_dispatch_exec(connection_token, meta_from_spec(spec), &args);

    assert_eq!(outcome.action, DispatchAction::Rejected);
    assert_eq!(
        outcome.reject_reply,
        Some(Value::error("CLUSTERDOWN The cluster is down"))
    );
}

#[test]
fn eval_on_a_migrating_source_asks_for_a_key_already_moved_to_the_target() {
    use crate::engine::distributed::cluster::node::{
        key_hash_slot, ClusterNode, CLUSTER_NODE_MYSELF,
    };
    use crate::engine::distributed::cluster::ClusterState;

    const TARGET: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let key = b"{eval-migrating}already-moved";
    let slot = key_hash_slot(key) as usize;
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().server.cluster_enabled = true;
    let connection_token = add_fake_connection(&mut backend);

    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.add_slot(slot).unwrap();
    let mut target = ClusterNode::new(
        TARGET.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    target.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(target.name.clone(), target);
    state.set_slot_migrating(slot, TARGET).unwrap();
    *backend.ctx.cluster_state_mut() = Some(Box::new(state));

    let args = make_args(&[
        b"EVAL",
        b"return redis.call('RPUSH', KEYS[1], ARGV[1])",
        b"1",
        key,
        b"value",
    ]);
    let spec = backend.registry.lookup(b"eval").unwrap();
    let outcome = backend.pre_dispatch_exec(connection_token, meta_from_spec(spec), &args);

    assert_eq!(outcome.action, DispatchAction::Rejected);
    assert!(matches!(
        outcome.reject_reply,
        Some(Value::Error(ref error)) if error.starts_with(b"ASK ")
    ));
}

#[test]
fn asking_authorizes_nested_script_write_on_an_importing_slot_for_the_outer_command() {
    use crate::engine::distributed::cluster::node::{
        key_hash_slot, ClusterNode, CLUSTER_NODE_MYSELF,
    };
    use crate::engine::distributed::cluster::state::CLUSTER_OK;
    use crate::engine::distributed::cluster::ClusterState;
    use crate::foundation::io::store::Robj;

    const SOURCE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let key = b"{eval-importing}key";
    let slot = key_hash_slot(key) as usize;
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_command_registry_snapshot(&backend.registry);
    backend.ctx.config_mut().server.cluster_enabled = true;
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[Token(connection_token)].asking = true;

    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let mut source = ClusterNode::new(
        SOURCE.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    source.flags &= !CLUSTER_NODE_MYSELF;
    source.slots[slot] = true;
    source.numslots = 1;
    state.nodes.insert(source.name.clone(), source);
    state.set_slot_importing(slot, SOURCE).unwrap();
    state.state = CLUSTER_OK;
    *backend.ctx.cluster_state_mut() = Some(Box::new(state));

    let args = make_args(&[
        b"EVAL",
        b"return redis.call('RPUSH', KEYS[1], ARGV[1])",
        b"1",
        key,
        b"value",
    ]);
    let mut effects = ReadBatchEffects::default();

    assert_eq!(
        backend.process_parsed_command(Token(42), args, "eval".to_string(), false, &mut effects,),
        super::types::LoopAction::Continue
    );
    backend
        .ctx
        .cluster_state_mut()
        .as_deref_mut()
        .unwrap()
        .set_slot_stable(slot)
        .unwrap();
    assert_eq!(
        backend
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(key)
            .map(Robj::list_len),
        Some(1)
    );
    assert!(!backend.connection_registry[Token(connection_token)].asking);
}

#[test]
fn pre_dispatch_consumes_asking_for_the_next_non_asking_command() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[Token(connection_token)].asking = true;
    let spec = make_spec("get", 2, 0, 1, 1, 1);
    let args = make_args(&[b"GET", b"key"]);

    let _ = backend.pre_dispatch_exec(connection_token, meta_from_spec(&spec), &args);

    assert!(!backend.connection_registry[Token(connection_token)].asking);
}

#[test]
fn script_has_no_writes_flag_detects_shebang() {
    assert!(script_has_no_writes_flag("#!lua flags=no-writes\nreturn 1"));
    assert!(script_has_no_writes_flag(
        "#!lua name=f1 flags=no-writes\nreturn 1"
    ));
    assert!(!script_has_no_writes_flag("#!lua\nreturn 1"));
    assert!(!script_has_no_writes_flag("return 1"));
    assert!(!script_has_no_writes_flag(
        "#!lua flags=allow-stale\nreturn 1"
    ));
    assert!(script_has_no_writes_flag(
        "#!lua flags=no-writes,allow-oom\nreturn 1"
    ));
}

const SETSLOT_REMOTE_NODE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SETSLOT_REPLICA_TOKEN_VALUE: usize = 9_101;

fn setslot_cluster_state() -> crate::engine::distributed::cluster::ClusterState {
    use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};
    use crate::engine::distributed::cluster::ClusterState;
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let mut remote = ClusterNode::new(
        SETSLOT_REMOTE_NODE_ID.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    remote.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(remote.name.clone(), remote);
    state
}

fn install_setslot_cluster(backend: &mut ServerWorkerState) {
    *backend.ctx.cluster_state_mut() = Some(Box::new(setslot_cluster_state()));
}

/// Register a fake primary-side replica connection, promote its link to
/// Streaming with a new enough version for SETSLOT acknowledgement gating, and
/// activate the replication backlog so command frames are appended.
fn add_streaming_setslot_replica(
    backend: &mut ServerWorkerState,
) -> crate::framework::distributed::replication::ReplicaId {
    let mut replica = Connection::new_fake();
    replica.set_token_for_test(Token(SETSLOT_REPLICA_TOKEN_VALUE));
    backend.connection_registry.insert(replica);
    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(SETSLOT_REPLICA_TOKEN_VALUE),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["eof".to_string(), "psync2".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let now = crate::foundation::io::sys::time::mstime();
    let link = backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("registered primary link");
    link.set_state(PrimaryLinkState::PreparingFullResync);
    link.finish_full_resync();
    link.acknowledge(offset(0), now);
    assert!(link.promote_full_resync_on_ack());
    link.set_replica_version(0x0007_0300);
    backend.ctx.replication_mut().backlog.activate_new_history();
    replica_id
}

fn setslot_command_frame(timeout_ms: Option<i64>) -> Vec<u8> {
    let mut parts: Vec<&[u8]> = vec![
        b"CLUSTER",
        b"SETSLOT",
        b"1",
        b"IMPORTING",
        SETSLOT_REMOTE_NODE_ID.as_bytes(),
    ];
    let timeout_text;
    if let Some(ms) = timeout_ms {
        timeout_text = ms.to_string();
        parts.push(b"TIMEOUT");
        parts.push(timeout_text.as_bytes());
    }
    crate::engine::distributed::replication::protocol::encode_command(&parts)
}

fn dispatch_client_frame(
    backend: &mut ServerWorkerState,
    client: &TcpStream,
    token: Token,
    frame: &[u8],
) {
    let mut client = client;
    client.write_all(frame).expect("client sends command frame");
    backend.transport.wait_readable_for_test(token);
    backend.handle_readable(token.0, ClientReadMode::Fair);
}

fn queued_frame_occurrences(backend: &ServerWorkerState, token: Token, needle: &[u8]) -> usize {
    backend.connection_registry[token]
        .output
        .queue
        .iter()
        .map(|frame| {
            frame
                .windows(needle.len())
                .filter(|window| *window == needle)
                .count()
        })
        .sum()
}

fn queued_flat_bytes(backend: &ServerWorkerState, token: Token) -> Vec<u8> {
    backend.connection_registry[token]
        .output
        .queue
        .iter()
        .flat_map(|frame| frame.iter().copied())
        .collect()
}

#[test]
fn setslot_blocks_before_running_body_and_propagates_frame_once_before_acks() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    install_setslot_cluster(&mut backend);
    let replica_id = add_streaming_setslot_replica(&mut backend);
    let (client, _connection_token, token) = add_socket_connection(&mut backend);
    let offset_before = backend.ctx.replication().primary_repl_offset();

    dispatch_client_frame(&mut backend, &client, token, &setslot_command_frame(None));

    let blocked = backend.connection_registry[token]
        .blocked
        .as_ref()
        .expect("SETSLOT client must block for replica ACKs");
    assert!(
        matches!(blocked.cmd, BlockedCmd::SetSlot { required: 1, .. }),
        "unexpected blocked command: {:?}",
        blocked.cmd
    );
    assert!(
        backend.connection_registry[token].output.queue.is_empty(),
        "no reply is allowed before the ACK threshold is met"
    );
    assert!(
        backend
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .importing_slot_source(1)
            .is_none(),
        "the SETSLOT body must not run while blocked"
    );
    let offset_blocked = backend.ctx.replication().primary_repl_offset();
    assert!(
        offset_blocked.get() > offset_before.get(),
        "the preflight must append exactly one SETSLOT frame to the backlog"
    );
    assert_eq!(
        queued_frame_occurrences(&backend, Token(SETSLOT_REPLICA_TOKEN_VALUE), b"SETSLOT"),
        1
    );
    let _ = replica_id;
}

#[test]
fn setslot_wakeup_reruns_body_once_and_replies_ok_without_a_second_frame() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    install_setslot_cluster(&mut backend);
    let replica_id = add_streaming_setslot_replica(&mut backend);
    let (client, _connection_token, token) = add_socket_connection(&mut backend);

    dispatch_client_frame(&mut backend, &client, token, &setslot_command_frame(None));

    let target_offset = match &backend.connection_registry[token]
        .blocked
        .as_ref()
        .expect("SETSLOT client must block")
        .cmd
    {
        BlockedCmd::SetSlot { target_offset, .. } => *target_offset,
        other => panic!("unexpected blocked command: {other:?}"),
    };
    let offset_blocked = backend.ctx.replication().primary_repl_offset();
    backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("primary link")
        .acknowledge(target_offset, crate::foundation::io::sys::time::mstime());

    backend.check_wait_replica_wakeups();

    assert_eq!(
        backend
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .importing_slot_source(1),
        Some(SETSLOT_REMOTE_NODE_ID)
    );
    assert!(backend.connection_registry[token].blocked.is_none());
    let reply = queued_flat_bytes(&backend, token);
    assert!(
        reply.windows(5).any(|window| window == b"+OK\r\n"),
        "wake-up reply must be +OK, got: {}",
        String::from_utf8_lossy(&reply)
    );
    assert_eq!(
        backend.ctx.replication().primary_repl_offset(),
        offset_blocked,
        "re-running SETSLOT must not propagate the command frame a second time"
    );
    assert_eq!(
        queued_frame_occurrences(&backend, Token(SETSLOT_REPLICA_TOKEN_VALUE), b"SETSLOT"),
        1
    );
}

#[test]
fn setslot_timeout_replies_noreplicas_without_rerunning_body() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    install_setslot_cluster(&mut backend);
    let _replica_id = add_streaming_setslot_replica(&mut backend);
    let (client, _connection_token, token) = add_socket_connection(&mut backend);

    dispatch_client_frame(
        &mut backend,
        &client,
        token,
        &setslot_command_frame(Some(1)),
    );

    assert!(matches!(
        backend.connection_registry[token]
            .blocked
            .as_ref()
            .expect("SETSLOT client must block")
            .cmd,
        BlockedCmd::SetSlot { required: 1, .. }
    ));
    backend.connection_registry[token]
        .blocked
        .as_mut()
        .expect("blocked state")
        .start_time_ms -= 10;

    backend.check_blocked_timeouts();

    assert!(backend.connection_registry[token].blocked.is_none());
    assert!(
        backend
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .importing_slot_source(1)
            .is_none(),
        "the SETSLOT body must not run on timeout"
    );
    assert_eq!(
        backend.connection_registry[token].output.queue.front(),
        Some(&b"-NOREPLICAS Not enough good replicas to write.\r\n".to_vec())
    );
    assert!(!backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientRead(token)
    ));
}

#[test]
fn setslot_runs_immediately_when_no_eligible_replica() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    install_setslot_cluster(&mut backend);
    let (client, _connection_token, token) = add_socket_connection(&mut backend);

    dispatch_client_frame(&mut backend, &client, token, &setslot_command_frame(None));

    assert!(backend.connection_registry[token].blocked.is_none());
    assert_eq!(
        backend
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .importing_slot_source(1),
        Some(SETSLOT_REMOTE_NODE_ID)
    );
    let reply = queued_flat_bytes(&backend, token);
    assert!(
        reply.windows(5).any(|window| window == b"+OK\r\n"),
        "direct SETSLOT must reply +OK, got: {}",
        String::from_utf8_lossy(&reply)
    );
}

mod completion;
mod eviction;
mod ownership;
mod reply_policy;
