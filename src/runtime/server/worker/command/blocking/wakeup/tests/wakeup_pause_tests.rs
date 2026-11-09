use super::*;
use std::io::{Read, Write};
use std::net::TcpListener;

#[test]
fn postponed_direct_set_propagates_after_successful_wakeup() {
    let (mut backend, connection_token, token) = _backend_with_connection();
    let connection = &mut backend.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)];
    connection.authenticated = true;
    connection.selected_db = 0;
    connection.transaction.end(TransactionEnd::Reset);
    connection.record_direct_output_bytes(17);
    backend.ctx.client_pause_mut().action = PauseAction::Write;
    backend.ctx.client_pause_mut().end_time_ms = i64::MAX;
    backend.ctx.client_pause_mut().reason = "failover";
    let request = b"*3\r\n$3\r\nSET\r\n$13\r\npostponed-key\r\n$5\r\nvalue\r\n";

    backend.process_client_read_result(
        token,
        crate::runtime::server::worker::connection::ClientReadMode::Fair,
        request.to_vec(),
        false,
        false,
    );

    assert!(matches!(
        backend.connection_registry[token]
            .blocked
            .as_ref()
            .map(|blocked| &blocked.cmd),
        Some(BlockedCmd::Paused)
    ));
    assert_eq!(
        backend.connection_registry[token].request_bytes_current_command(),
        request.len()
    );
    assert_eq!(
        backend.connection_registry[token].output_before_current_command(),
        17
    );
    assert!(!backend.connection_registry[token].skip_propagate());
    backend.ctx.client_pause_mut().action = PauseAction::None;
    backend.ctx.client_pause_mut().end_time_ms = 0;
    let (mut replication, mut aof) = (Vec::new(), Vec::new());

    let dirty = backend.wakeup_blocked_client(token, Some(&mut replication), Some(&mut aof));

    assert_eq!(replication, aof);
    assert_eq!(replication.last(), Some(&request.to_vec()));
    assert_eq!(dirty, vec![(0, Bytes::from_static(b"postponed-key"))]);
    assert_eq!(backend.ctx.persistence().dirty(), 1);
    assert_eq!(
        backend
            .ctx
            .db(0)
            .expect("default database")
            .get_no_expire(b"postponed-key")
            .map(|value| value.as_bytes()),
        Some(b"value".as_slice())
    );
    assert_eq!(
        backend.connection_registry[token].request_bytes_current_command(),
        0
    );
}

#[test]
fn postponed_publish_queues_command_reply_before_its_push_notification() {
    let (mut backend, connection_token, token) = _backend_with_connection();
    let channel = Bytes::from_static(b"wake-channel");
    let connection = &mut backend.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)];
    connection.authenticated = true;
    connection.resp_version = 3;
    connection.transaction.end(TransactionEnd::Reset);
    backend.ctx.pubsub_mut().subscribe(token.0, channel.clone());
    backend.ctx.client_pause_mut().action = PauseAction::Write;
    backend.ctx.client_pause_mut().end_time_ms = i64::MAX;
    let request = b"*3\r\n$7\r\nPUBLISH\r\n$12\r\nwake-channel\r\n$7\r\nmessage\r\n";

    backend.process_client_read_result(
        token,
        crate::runtime::server::worker::connection::ClientReadMode::Fair,
        request.to_vec(),
        false,
        false,
    );
    assert!(backend.connection_registry[token].blocked.is_some());
    backend.ctx.client_pause_mut().action = PauseAction::None;
    backend.ctx.client_pause_mut().end_time_ms = 0;

    backend.wakeup_blocked_client(token, None, None);

    let output = _delivered_bytes(&backend, connection_token);
    assert!(
        output.starts_with(b":1\r\n>3\r\n"),
        "unexpected reply/push order: {}",
        String::from_utf8_lossy(&output)
    );
}

#[test]
fn postponed_writable_replica_set_does_not_relay_local_write() {
    let (mut backend, connection_token, token) = _backend_with_connection();
    let connection = &mut backend.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)];
    connection.authenticated = true;
    connection.selected_db = 0;
    connection.transaction.end(TransactionEnd::Reset);
    backend.ctx.replication_mut().switch_to_replica();
    backend.ctx.replication_mut().backlog.activate();
    backend.ctx.config_mut().replication.replica_read_only = false;
    backend.ctx.client_pause_mut().action = PauseAction::Write;
    backend.ctx.client_pause_mut().end_time_ms = i64::MAX;
    let request = b"*3\r\n$3\r\nSET\r\n$9\r\nlocal-key\r\n$5\r\nvalue\r\n";

    backend.process_client_read_result(
        token,
        crate::runtime::server::worker::connection::ClientReadMode::Fair,
        request.to_vec(),
        false,
        false,
    );
    assert!(backend.connection_registry[token].blocked.is_some());
    backend.ctx.client_pause_mut().action = PauseAction::None;
    backend.ctx.client_pause_mut().end_time_ms = 0;
    let offset_before = backend.ctx.replication().backlog.reported_offset().get();
    let mut aof = Vec::new();

    backend.wakeup_blocked_client(token, None, Some(&mut aof));

    assert_eq!(
        backend.ctx.replication().backlog.reported_offset().get(),
        offset_before
    );
    assert_eq!(aof.last(), Some(&request.to_vec()));
    assert_eq!(backend.ctx.persistence().dirty(), 1);
}

#[test]
fn postponed_writable_replica_exec_does_not_relay_local_write() {
    let (mut backend, connection_token, token) = _backend_with_connection();
    let connection = &mut backend.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)];
    connection.authenticated = true;
    connection.selected_db = 0;
    connection.transaction.end(TransactionEnd::Reset);
    assert!(connection.transaction.begin(0, 0));
    assert!(connection.transaction.queue(
        backend.ctx.num_dbs(),
        CommandArgs::from_parts(vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"exec-local-key"),
            Bytes::from_static(b"value"),
        ])
    ));
    backend.ctx.replication_mut().switch_to_replica();
    backend.ctx.replication_mut().backlog.activate();
    backend.ctx.config_mut().replication.replica_read_only = false;
    backend.ctx.client_pause_mut().action = PauseAction::Write;
    backend.ctx.client_pause_mut().end_time_ms = i64::MAX;
    let request = b"*1\r\n$4\r\nEXEC\r\n";

    backend.process_client_read_result(
        token,
        crate::runtime::server::worker::connection::ClientReadMode::Fair,
        request.to_vec(),
        false,
        false,
    );
    assert!(backend.connection_registry[token].blocked.is_some());
    backend.ctx.client_pause_mut().action = PauseAction::None;
    backend.ctx.client_pause_mut().end_time_ms = 0;
    let offset_before = backend.ctx.replication().backlog.reported_offset().get();
    let mut aof = Vec::new();

    backend.wakeup_blocked_client(token, None, Some(&mut aof));

    assert_eq!(
        backend.ctx.replication().backlog.reported_offset().get(),
        offset_before
    );
    assert!(!aof.is_empty());
    assert_eq!(
        backend
            .ctx
            .db(0)
            .expect("default database")
            .get_no_expire(b"exec-local-key")
            .map(|value| value.as_bytes()),
        Some(b"value".as_slice())
    );
}

#[test]
fn postponed_exec_honors_client_reply_off() {
    let (mut backend, _connection_token, token) = _backend_with_connection();
    let connection = &mut backend.connection_registry[token];
    connection.authenticated = true;
    connection.selected_db = 0;
    connection.transaction.end(TransactionEnd::Reset);
    assert!(connection.transaction.begin(0, 0));
    assert!(connection.transaction.queue(
        backend.ctx.num_dbs(),
        CommandArgs::from_parts(vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"exec-off-key"),
            Bytes::from_static(b"value"),
        ])
    ));
    connection.set_reply_off(true);
    let args = CommandArgs::from_parts(vec![Bytes::from_static(b"EXEC")]);
    connection.postpone_transaction_command(&args);
    backend.ctx.paused_clients_mut().insert(token);

    backend.wakeup_blocked_client(token, None, None);

    assert!(backend.connection_registry[token].output.queue.is_empty());
    assert!(backend.connection_registry[token].reply_off());
    assert_eq!(
        backend
            .ctx
            .db(0)
            .expect("default database")
            .get_no_expire(b"exec-off-key")
            .map(|value| value.as_bytes()),
        Some(b"value".as_slice())
    );
    assert_eq!(
        backend.connection_registry[token].request_bytes_current_command(),
        0
    );
}

#[test]
fn postponed_exec_honors_client_reply_skip_once() {
    let (mut backend, _connection_token, token) = _backend_with_connection();
    let connection = &mut backend.connection_registry[token];
    connection.authenticated = true;
    connection.selected_db = 0;
    connection.transaction.end(TransactionEnd::Reset);
    assert!(connection.transaction.begin(0, 0));
    assert!(connection.transaction.queue(
        backend.ctx.num_dbs(),
        CommandArgs::from_parts(vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"exec-skip-key"),
            Bytes::from_static(b"value"),
        ])
    ));
    connection.set_reply_skip(true);
    let args = CommandArgs::from_parts(vec![Bytes::from_static(b"EXEC")]);
    connection.postpone_transaction_command(&args);
    backend.ctx.paused_clients_mut().insert(token);

    backend.wakeup_blocked_client(token, None, None);

    assert!(backend.connection_registry[token].output.queue.is_empty());
    assert!(!backend.connection_registry[token].reply_skip());
    assert!(backend
        .ctx
        .db(0)
        .expect("default database")
        .exists_no_expire(b"exec-skip-key"));
}

#[test]
fn postponed_multi_queue_honors_client_reply_skip_once() {
    let (mut backend, _connection_token, token) = _backend_with_connection();
    let connection = &mut backend.connection_registry[token];
    connection.authenticated = true;
    connection.transaction.take_queued_commands();
    connection.set_reply_skip(true);
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"queued-key"),
        Bytes::from_static(b"value"),
    ]);
    connection.postpone_transaction_command(&args);
    backend.ctx.paused_clients_mut().insert(token);

    backend.wakeup_blocked_client(token, None, None);

    assert!(backend.connection_registry[token].output.queue.is_empty());
    assert!(!backend.connection_registry[token].reply_skip());
    let queued = backend.connection_registry[token]
        .transaction
        .queued_commands();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].name, args.name);
    assert_eq!(queued[0].args, args.args);
}

#[test]
fn postponed_exec_reply_precedes_same_client_pubsub_push() {
    let (mut backend, connection_token, token) = _backend_with_connection();
    let channel = Bytes::from_static(b"exec-wake-channel");
    let connection = &mut backend.connection_registry[token];
    connection.authenticated = true;
    connection.resp_version = 3;
    connection.selected_db = 0;
    connection.transaction.end(TransactionEnd::Reset);
    assert!(connection.transaction.begin(0, 0));
    assert!(connection.transaction.queue(
        backend.ctx.num_dbs(),
        CommandArgs::from_parts(vec![
            Bytes::from_static(b"PUBLISH"),
            channel.clone(),
            Bytes::from_static(b"message"),
        ])
    ));
    backend.ctx.pubsub_mut().subscribe(token.0, channel);
    let args = CommandArgs::from_parts(vec![Bytes::from_static(b"EXEC")]);
    connection.postpone_transaction_command(&args);
    backend.ctx.paused_clients_mut().insert(token);

    backend.wakeup_blocked_client(token, None, None);

    let output = _delivered_bytes(&backend, connection_token);
    assert!(
        output.starts_with(b"*1\r\n:1\r\n>3\r\n"),
        "unexpected EXEC reply/push order: {}",
        String::from_utf8_lossy(&output)
    );
}

#[test]
fn postponed_exec_with_psubscribe_custom_push_completes_outer_cycle() {
    let (mut backend, connection_token, token) = _backend_with_connection();
    let connection = &mut backend.connection_registry[token];
    connection.authenticated = true;
    connection.resp_version = 3;
    connection.transaction.take_queued_commands();
    assert!(connection.transaction.queue(
        backend.ctx.num_dbs(),
        CommandArgs::from_parts(vec![
            Bytes::from_static(b"PSUBSCRIBE"),
            Bytes::from_static(b"pattern*"),
        ])
    ));
    let args = CommandArgs::from_parts(vec![Bytes::from_static(b"EXEC")]);
    connection.postpone_transaction_command(&args);
    backend.ctx.paused_clients_mut().insert(token);

    backend.wakeup_blocked_client(token, None, None);

    let output = _delivered_bytes(&backend, connection_token);
    assert!(output.windows(10).any(|part| part == b"psubscribe"));
    assert!(!backend.connection_registry[token].has_custom_reply());
    assert_eq!(
        backend.connection_registry[token].request_bytes_current_command(),
        0
    );
}

fn add_live_client(backend: &mut ServerWorkerState) -> (Token, std::net::TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind client listener");
    let peer_addr = listener.local_addr().expect("client address");
    let connected = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect client transport");
    let (peer, _) = listener.accept().expect("accept client transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: connected.token,
        peer_addr,
    });
    (connected.token, peer)
}

fn postpone_exec_with_commands(
    backend: &mut ServerWorkerState,
    token: Token,
    commands: Vec<CommandArgs>,
) {
    let connection = &mut backend.connection_registry[token];
    assert!(connection.transaction.begin(0, 0));
    for command in commands {
        assert!(connection.transaction.queue(backend.ctx.num_dbs(), command));
    }
    let exec = CommandArgs::from_parts(vec![Bytes::from_static(b"EXEC")]);
    connection.postpone_transaction_command(&exec);
    backend.ctx.paused_clients_mut().insert(token);
}

fn queue_client_removal_and_script_kill(
    backend: &mut ServerWorkerState,
    target: Token,
) -> std::net::TcpStream {
    let (controller, mut peer) = add_live_client(backend);
    let target_id = backend.connection_registry[target].client_id().to_string();
    let commands = format!(
        "*4\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$2\r\nID\r\n${}\r\n{}\r\n*2\r\n$6\r\nSCRIPT\r\n$4\r\nKILL\r\n",
        target_id.len(),
        target_id
    );
    peer.write_all(commands.as_bytes())
        .expect("queue target removal and script kill");
    assert!(backend.connection_registry.contains(controller));
    peer
}

fn assert_matching_effect_streams(replication: &[Vec<u8>], aof: &[Vec<u8>], key: &[u8]) {
    assert_eq!(replication, aof);
    assert!(replication
        .concat()
        .windows(key.len())
        .any(|part| part == key));
}

#[test]
fn postponed_hosted_exec_preserves_connection_gone() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_command_registry_snapshot(&backend.registry);
    backend.ctx.config_mut().server.lua_time_limit = 1;
    let (target, _target_peer) = add_live_client(&mut backend);
    postpone_exec_with_commands(
        &mut backend,
        target,
        vec![CommandArgs::from_parts(vec![
            Bytes::from_static(b"EVAL"),
            Bytes::from_static(b"while true do end"),
            Bytes::from_static(b"0"),
        ])],
    );
    let _controller_peer = queue_client_removal_and_script_kill(&mut backend, target);

    backend.wakeup_blocked_client(target, None, None);

    assert!(!backend.connection_registry.contains(target));
}

#[test]
fn postponed_hosted_exec_commits_prior_item_effects_before_connection_gone() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_command_registry_snapshot(&backend.registry);
    backend.ctx.config_mut().server.lua_time_limit = 1;
    backend.ctx.config_mut().server.notify_keyspace_events = "KEA".to_string();
    let key = Bytes::from_static(b"effects-before-gone");
    let channel = Bytes::from_static(b"__keyspace@0__:effects-before-gone");
    let subscriber = _add_subscriber(&mut backend, channel);
    let (target, _target_peer) = add_live_client(&mut backend);
    postpone_exec_with_commands(
        &mut backend,
        target,
        vec![
            CommandArgs::from_parts(vec![
                Bytes::from_static(b"SET"),
                key.clone(),
                Bytes::from_static(b"value"),
            ]),
            CommandArgs::from_parts(vec![
                Bytes::from_static(b"EVAL"),
                Bytes::from_static(b"while true do end"),
                Bytes::from_static(b"0"),
            ]),
        ],
    );
    let _controller_peer = queue_client_removal_and_script_kill(&mut backend, target);
    let (mut replication, mut aof) = (Vec::new(), Vec::new());

    let dirty = backend.wakeup_blocked_client(target, Some(&mut replication), Some(&mut aof));

    assert!(!backend.connection_registry.contains(target));
    assert_eq!(dirty, vec![(0, key.clone())]);
    assert_matching_effect_streams(&replication, &aof, &key);
    assert_eq!(backend.ctx.persistence().dirty(), 1);
    assert!(_delivered_bytes(&backend, subscriber)
        .windows(b"set".len())
        .any(|part| part == b"set"));
}

#[test]
fn postponed_direct_write_error_does_not_propagate_or_increment_dirty() {
    let (mut backend, connection_token, token) = _backend_with_connection();
    let connection = &mut backend.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)];
    connection.authenticated = true;
    connection.selected_db = 0;
    connection.transaction.end(TransactionEnd::Reset);
    backend.ctx.db_mut(0).expect("default database").set(
        b"counter".to_vec(),
        crate::foundation::io::store::Robj::string("not-an-integer"),
    );
    backend.ctx.client_pause_mut().action = PauseAction::Write;
    backend.ctx.client_pause_mut().end_time_ms = i64::MAX;
    backend.ctx.client_pause_mut().reason = "failover";
    let request = b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n";

    backend.process_client_read_result(
        token,
        crate::runtime::server::worker::connection::ClientReadMode::Fair,
        request.to_vec(),
        false,
        false,
    );
    assert!(backend.connection_registry[token].blocked.is_some());
    backend.ctx.client_pause_mut().action = PauseAction::None;
    backend.ctx.client_pause_mut().end_time_ms = 0;
    let (mut replication, mut aof) = (Vec::new(), Vec::new());

    let dirty = backend.wakeup_blocked_client(token, Some(&mut replication), Some(&mut aof));

    assert!(replication.is_empty());
    assert!(aof.is_empty());
    assert!(dirty.is_empty());
    assert_eq!(backend.ctx.persistence().dirty(), 0);
    assert!(_delivered_bytes(&backend, connection_token).starts_with(b"-ERR "));
    assert_eq!(
        backend
            .ctx
            .db(0)
            .expect("default database")
            .get_no_expire(b"counter")
            .map(|value| value.as_bytes()),
        Some(b"not-an-integer".as_slice())
    );
}

fn add_watcher(backend: &mut ServerWorkerState, token: Token, db_id: u32, key: &'static [u8]) {
    let mut watcher = Connection::new_fake();
    watcher.set_token_for_test(token);
    watcher
        .transaction
        .watch_many(vec![crate::engine::command::context::WatchedKey::new(
            db_id,
            Bytes::from_static(key),
            false,
        )]);
    backend.connection_registry.insert(watcher);
}

fn add_blocked_swapdb_key(backend: &mut ServerWorkerState, token: Token, db_id: u32, key: Bytes) {
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"BLPOP"),
        key.clone(),
        Bytes::from_static(b"0"),
    ]);
    let mut blocked = Connection::new_fake();
    blocked.set_token_for_test(token);
    blocked.selected_db = db_id;
    blocked.enter_blocking(BlockedCmd::Blpop, vec![key.clone()], 0, &args, 32);
    backend.connection_registry.insert(blocked);
    let session = backend.connection_registry[token].session_id();
    backend
        .ctx
        .blocking_registry_mut()
        .register_client(db_id, &[key], session);
}

struct SwapdbWatchers {
    affected: Token,
    unrelated: Token,
    absent: Token,
}

fn postponed_swapdb_fixture() -> (ServerWorkerState, Token, SwapdbWatchers) {
    let (mut backend, _connection_token, token) = _backend_with_connection();
    let connection = &mut backend.connection_registry[token];
    connection.authenticated = true;
    connection.selected_db = 0;
    connection.transaction.end(TransactionEnd::Reset);
    backend.ctx.db_mut(1).expect("database one").set(
        b"affected".to_vec(),
        crate::foundation::io::store::Robj::string("value"),
    );
    let watchers = SwapdbWatchers {
        affected: Token(43),
        unrelated: Token(44),
        absent: Token(45),
    };
    add_watcher(&mut backend, watchers.affected, 0, b"affected");
    add_watcher(&mut backend, watchers.unrelated, 2, b"unrelated");
    add_watcher(&mut backend, watchers.absent, 0, b"absent");
    add_blocked_swapdb_key(&mut backend, Token(46), 0, Bytes::from_static(b"absent"));
    backend.ctx.client_pause_mut().action = PauseAction::Write;
    backend.ctx.client_pause_mut().end_time_ms = i64::MAX;
    let request = b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$1\r\n1\r\n";
    backend.process_client_read_result(
        token,
        crate::runtime::server::worker::connection::ClientReadMode::Fair,
        request.to_vec(),
        false,
        false,
    );
    assert!(backend.connection_registry[token].blocked.is_some());
    (backend, token, watchers)
}

#[test]
fn postponed_swapdb_uses_swap_semantics_before_wakeup_only_keys() {
    let (mut backend, token, watchers) = postponed_swapdb_fixture();
    backend.ctx.client_pause_mut().action = PauseAction::None;
    backend.ctx.client_pause_mut().end_time_ms = 0;

    let wakeup_only = backend.wakeup_blocked_client(token, None, None);

    assert_eq!(wakeup_only, vec![(0, Bytes::from_static(b"absent"))]);
    assert!(backend.connection_registry[watchers.affected]
        .transaction
        .is_watch_dirty());
    assert!(!backend.connection_registry[watchers.unrelated]
        .transaction
        .is_watch_dirty());
    assert!(!backend.connection_registry[watchers.absent]
        .transaction
        .is_watch_dirty());
    assert!(backend.ctx.db(0).unwrap().exists_no_expire(b"affected"));
}

#[test]
fn postponed_client_reply_off_suppresses_its_wakeup_reply() {
    let (mut backend, connection_token, token) = _backend_with_connection();
    let connection = &mut backend.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)];
    connection.authenticated = true;
    connection.selected_db = 0;
    connection.transaction.end(TransactionEnd::Reset);
    backend.ctx.client_pause_mut().action = PauseAction::All;
    backend.ctx.client_pause_mut().end_time_ms = i64::MAX;
    let request = b"*3\r\n$6\r\nCLIENT\r\n$5\r\nREPLY\r\n$3\r\nOFF\r\n";

    backend.process_client_read_result(
        token,
        crate::runtime::server::worker::connection::ClientReadMode::Fair,
        request.to_vec(),
        false,
        false,
    );
    assert!(backend.connection_registry[token].blocked.is_some());
    backend.ctx.client_pause_mut().action = PauseAction::None;
    backend.ctx.client_pause_mut().end_time_ms = 0;

    backend.wakeup_blocked_client(token, None, None);

    assert!(backend.connection_registry[token].output.queue.is_empty());
}

#[test]
fn postponed_custom_reply_is_suppressed_when_reply_mode_is_off() {
    let (mut backend, connection_token, token) = _backend_with_connection();
    backend.registry.register(crate::framework::CommandSpec {
        name: crate::engine::command::CommandName::Static("custom-write"),
        arity: 1,
        flags: CMD_WRITE,
        dispatch_group: crate::engine::command::DispatchGroup::Default,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        container: None,
        key_flags: &[],
        acl_categories: &[],
        executor: crate::framework::CommandExecutor::Immediate(custom_reply_write_handler),
        get_keys: None,
    });
    let connection = &mut backend.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)];
    connection.authenticated = true;
    connection.transaction.end(TransactionEnd::Reset);
    connection.set_reply_off(true);
    backend.ctx.client_pause_mut().action = PauseAction::Write;
    backend.ctx.client_pause_mut().end_time_ms = i64::MAX;
    let request = b"*1\r\n$12\r\ncustom-write\r\n";

    backend.process_client_read_result(
        token,
        crate::runtime::server::worker::connection::ClientReadMode::Fair,
        request.to_vec(),
        false,
        false,
    );
    assert!(backend.connection_registry[token].blocked.is_some());
    backend.ctx.client_pause_mut().action = PauseAction::None;
    backend.ctx.client_pause_mut().end_time_ms = 0;

    backend.wakeup_blocked_client(token, None, None);

    assert!(backend.connection_registry[token].output.queue.is_empty());
}

#[test]
fn postponed_wakeup_applies_dynamic_argument_redaction_to_command_log() {
    let (mut backend, connection_token, token) = _backend_with_connection();
    backend.registry.register(crate::framework::CommandSpec {
        name: crate::engine::command::CommandName::Static("redact-write"),
        arity: 2,
        flags: CMD_WRITE,
        dispatch_group: crate::engine::command::DispatchGroup::Default,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        container: None,
        key_flags: &[],
        acl_categories: &[],
        executor: crate::framework::CommandExecutor::Immediate(redacting_write_handler),
        get_keys: None,
    });
    let connection = &mut backend.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)];
    connection.authenticated = true;
    connection.transaction.end(TransactionEnd::Reset);
    backend.ctx.slowlog_mut().reconfigure(0, 10);
    backend.ctx.client_pause_mut().action = PauseAction::Write;
    backend.ctx.client_pause_mut().end_time_ms = i64::MAX;
    let request = b"*2\r\n$12\r\nredact-write\r\n$6\r\nsecret\r\n";

    backend.process_client_read_result(
        token,
        crate::runtime::server::worker::connection::ClientReadMode::Fair,
        request.to_vec(),
        false,
        false,
    );
    assert!(backend.connection_registry[token].blocked.is_some());
    backend.ctx.client_pause_mut().action = PauseAction::None;
    backend.ctx.client_pause_mut().end_time_ms = 0;

    backend.wakeup_blocked_client(token, None, None);

    let entries = backend.ctx.slowlog().get(1);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].argv[0], Bytes::from_static(b"redact-write"));
    assert_eq!(entries[0].argv[1], Bytes::from_static(b"(redacted)"));
}

#[test]
fn postponed_eval_resumes_through_hosted_event_pump() {
    let (mut backend, connection_token, token) = _backend_with_connection();
    backend.ctx.set_command_registry_snapshot(&backend.registry);
    backend.ctx.config_mut().server.lua_time_limit = 1;
    let connection = &mut backend.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)];
    connection.authenticated = true;
    connection.selected_db = 0;
    connection.transaction.end(TransactionEnd::Reset);
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"EVAL"),
        Bytes::from_static(b"local x=0; for i=1,10000000 do x=x+i end; return 42"),
        Bytes::from_static(b"0"),
    ]);
    connection.postpone_command(BlockedCmd::Paused, Vec::new(), 0, &args, 64);
    backend.ctx.paused_clients_mut().insert(token);

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind controller listener");
    let peer_addr = listener.local_addr().expect("controller listener address");
    let controller = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect controller transport");
    let (mut controller_peer, _) = listener.accept().expect("accept controller transport");
    backend.add_connection(RuntimeConnectedPeer {
        token: controller.token,
        peer_addr,
    });
    controller_peer
        .write_all(b"*3\r\n$6\r\nSCRIPT\r\n$6\r\nEXISTS\r\n$8\r\ndeadbeef\r\n")
        .expect("queue controller SCRIPT EXISTS");

    backend.wakeup_blocked_client(token, None, None);

    controller_peer
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .expect("set controller read timeout");
    let mut pumped_reply = [0_u8; 8];
    controller_peer
        .read_exact(&mut pumped_reply)
        .expect("hosted EVAL must pump the controller command");
    assert_eq!(&pumped_reply, b"*1\r\n:0\r\n");
    assert!(_delivered_bytes(&backend, connection_token).starts_with(b":42\r\n"));
}

#[test]
fn errored_wakeup_preserves_explicit_effects_and_clears_transients() {
    let (mut backend, connection_token, _) = _backend_with_connection();
    _seed_command_transients(
        &mut backend.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)],
    );
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"BLPOP"),
        Bytes::from_static(b"key"),
        Bytes::from_static(b"0"),
    ]);

    let mut effects = backend.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)]
    .take_command_effects(CommandEffectDrain::Completed)
    .expect("seeded command effects");
    let (propagation, dirty) =
        backend.wakeup_collect_propagation_and_dirty(connection_token, &args, true, &mut effects);

    assert_eq!(propagation.len(), 1);
    assert_eq!(propagation[0].bytes, b"script-frame".to_vec());
    assert_eq!(dirty, vec![(7, Bytes::from_static(b"dirty"))]);
    _assert_command_transients_cleared(
        &backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)],
    );
}

#[test]
fn paused_wakeup_restores_wrapped_block_state() {
    let (mut backend, connection_token, token) = _backend_with_connection();
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"BLPOP"),
        Bytes::from_static(b"key"),
        Bytes::from_static(b"1"),
    ]);
    let start_time_ms = mstime().saturating_sub(10);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .blocked = Some(BlockedState {
        cmd: BlockedCmd::Paused,
        keys: Vec::new(),
        timeout_ms: 0,
        start_time_ms: mstime(),
        orig_args: args.clone(),
        req_bytes: 32,
        orig_wrapped: Some(Box::new(BlockedState {
            cmd: BlockedCmd::Blpop,
            keys: vec![Bytes::from_static(b"key")],
            timeout_ms: 1_000,
            start_time_ms,
            orig_args: args,
            req_bytes: 32,
            orig_wrapped: None,
        })),
    });
    backend.ctx.paused_clients_mut().insert(token);
    let session = backend.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)]
    .session_id();
    backend
        .ctx
        .blocking_registry_mut()
        .register_client(7, &[Bytes::from_static(b"key")], session);

    let (_, args, restored_start_ms, _, was_paused) = backend.wakeup_prepare(token).unwrap();

    assert!(!was_paused);
    assert_eq!(args.name, Bytes::from_static(b"BLPOP"));
    assert_eq!(restored_start_ms, start_time_ms);
    assert!(!backend.ctx.paused_clients().contains(&token));
    assert!(!backend.ctx.blocking_registry().is_client_blocked(&session));
}

#[test]
fn paused_eval_is_rerouted_after_local_node_becomes_replica() {
    use crate::engine::distributed::cluster::node::{
        key_hash_slot, ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
    };
    use crate::engine::distributed::cluster::ClusterState;

    let (mut backend, connection_token, token) = _backend_with_connection();
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .authenticated = true;
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .transaction
        .end(TransactionEnd::Reset);
    backend.ctx.config_mut().server.cluster_enabled = true;
    backend.ctx.replication_mut().switch_to_replica();

    let key = Bytes::from_static(b"key:manual-failover");
    let slot = key_hash_slot(&key) as usize;
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = primary_id.to_string();
    let mut primary = ClusterNode::new(
        primary_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    primary.flags &= !CLUSTER_NODE_MYSELF;
    primary.slots[slot] = true;
    primary.numslots = 1;
    state.nodes.insert(primary_id.to_string(), primary);
    *backend.ctx.cluster_state_mut() = Some(Box::new(state));

    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"EVAL"),
        Bytes::from_static(b"return server.call('rpush',KEYS[1],ARGV[1])"),
        Bytes::from_static(b"1"),
        key,
        Bytes::from_static(b"value"),
    ]);
    let (reply, rejected, is_exec_paused) =
        backend.wakeup_handle_paused(connection_token, &args, 64, token);

    assert!(rejected);
    assert!(!is_exec_paused);
    assert!(matches!(reply, Value::Error(ref error) if error.starts_with(b"MOVED ")));
}

#[test]
fn paused_writable_eval_is_redirected_after_standalone_failover() {
    let (mut backend, connection_token, token) = _backend_with_connection();
    backend.ctx.set_command_registry_snapshot(&backend.registry);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .authenticated = true;
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .transaction
        .end(TransactionEnd::Reset);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .capa_flags
        .push("redirect".to_string());
    backend.ctx.replication_mut().switch_to_replica();
    backend.ctx.config_mut().replication.master_host = Some("192.0.2.1".to_string());
    backend.ctx.config_mut().replication.master_port = 6380;
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"EVAL"),
        Bytes::from_static(b"#!lua\nreturn server.call('set','key','value')"),
        Bytes::from_static(b"0"),
    ]);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .postpone_command(BlockedCmd::Paused, Vec::new(), 0, &args, 64);
    backend.ctx.paused_clients_mut().insert(token);

    backend.wakeup_blocked_client(token, None, None);

    let reply = _delivered_bytes(&backend, connection_token);
    assert_eq!(reply, b"-REDIRECT 192.0.2.1:6380\r\n");
    assert!(backend.ctx.db(7).unwrap().get_no_expire(b"key").is_none());
}

#[test]
fn pause_write_releases_no_writes_eval() {
    let (mut backend, connection_token, _) = _backend_with_connection();
    backend.ctx.set_command_registry_snapshot(&backend.registry);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .transaction
        .end(TransactionEnd::Reset);
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"EVAL"),
        Bytes::from_static(b"#!lua flags=no-writes\nreturn 1"),
        Bytes::from_static(b"0"),
    ]);

    assert!(!backend.pause_should_block(
        &backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)],
        crate::framework::context::PauseAction::Write,
        false,
        &args,
    ));
}

#[test]
fn slot_migration_wakeup_rechecks_cluster_routing() {
    use crate::engine::distributed::cluster::migration::MigrationJobId;
    use crate::engine::distributed::cluster::node::{
        key_hash_slot, ClusterNode, CLUSTER_NODE_MYSELF,
    };
    use crate::engine::distributed::cluster::ClusterState;

    let (mut backend, connection_token, token) = _backend_with_connection();
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .authenticated = true;
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .transaction
        .end(TransactionEnd::Reset);
    backend.ctx.config_mut().server.cluster_enabled = true;
    let key = Bytes::from_static(b"key:slot-migration-wakeup");
    let slot = key_hash_slot(&key) as usize;
    let remote_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let mut remote = ClusterNode::new(
        remote_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    remote.flags &= !CLUSTER_NODE_MYSELF;
    remote.slots[slot] = true;
    remote.numslots = 1;
    state.slots[slot] = true;
    state.nodes.insert(remote_id.to_string(), remote);
    *backend.ctx.cluster_state_mut() = Some(Box::new(state));
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SET"),
        key.clone(),
        Bytes::from_static(b"value"),
    ]);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .postpone_command(
            BlockedCmd::SlotMigration(
                MigrationJobId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
            ),
            Vec::new(),
            0,
            &args,
            32,
        );
    backend.ctx.paused_clients_mut().insert(token);

    backend.wakeup_blocked_client(token, None, None);

    let reply = _delivered_bytes(&backend, connection_token);
    assert!(
        reply.starts_with(b"-MOVED "),
        "reply={:?}",
        String::from_utf8_lossy(&reply)
    );
    assert!(backend.ctx.db(7).unwrap().get_no_expire(&key).is_none());
}
