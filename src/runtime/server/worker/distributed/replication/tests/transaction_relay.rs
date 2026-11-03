use super::*;

#[test]
fn replicated_transaction_wakes_waiters_in_first_ready_key_order() {
    use crate::engine::command::context::BlockedState;

    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.replication_mut().switch_to_replica();
    backend.ctx.config_mut().replication.replica_read_only = false;
    backend.ctx.replication_mut().backlog.activate();
    let (token, mut primary) = add_runtime_connection(&mut backend);
    backend
        .replication_runtime
        .set_replica_link(ReplicaLink::streaming(
            ConnectionId(token),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
            backend.ctx.replication().backlog.replid().to_string(),
            offset(0),
        ));
    let (first, mut first_client) = add_runtime_connection(&mut backend);
    let (second, mut second_client) = add_runtime_connection(&mut backend);
    for (waiting, keys) in [
        (first, vec![Bytes::from_static(b"a")]),
        (
            second,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        ),
    ] {
        let mut parts = vec![Bytes::from_static(b"BLPOP")];
        parts.extend(keys.iter().cloned());
        parts.push(Bytes::from_static(b"0"));
        backend.connection_registry[Token(waiting)].blocked = Some(BlockedState {
            cmd: crate::engine::command::context::BlockedCmd::Blpop,
            keys: keys.clone(),
            timeout_ms: 0,
            start_time_ms: crate::foundation::io::sys::time::mstime(),
            orig_args: CommandArgs::from_parts(parts),
            req_bytes: 32,
            orig_wrapped: None,
        });
        let session = backend.connection_registry[Token(waiting)].session_id();
        backend
            .ctx
            .blocking_registry_mut()
            .register_client(0, &keys, session);
    }
    let stream = [
        protocol::encode_command(&[b"MULTI"]),
        protocol::encode_command(&[b"RPUSH", b"a", b"A"]),
        protocol::encode_command(&[b"RPUSH", b"b", b"B"]),
        protocol::encode_command(&[b"EXEC"]),
    ]
    .concat();
    primary.write_all(&stream).unwrap();
    backend.transport.wait_readable_for_test(Token(token));
    backend.recv_replication_stream(token, token);
    assert!(
        backend.connection_registry[Token(first)].blocked.is_none(),
        "the first waiter for the first ready key must receive its value"
    );
    assert!(backend.connection_registry[Token(second)].blocked.is_none());
    for (waiting, client, expected) in [
        (first, &mut first_client, b"*2\r\n$1\r\na\r\n$1\r\nA\r\n"),
        (second, &mut second_client, b"*2\r\n$1\r\nb\r\n$1\r\nB\r\n"),
    ] {
        backend.flush_connection_output(waiting);
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        let mut reply = [0; 18];
        client.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, expected);
    }
    assert!(backend.ctx.db(0).unwrap().get_no_expire(b"a").is_none());
    assert!(backend.ctx.db(0).unwrap().get_no_expire(b"b").is_none());
}

#[test]
fn unfinished_replication_transactions_do_not_cross_snapshot_or_promotion_boundaries() {
    use crate::foundation::io::persistence::load_aof_bytes;
    use crate::framework::aof_replay::AofReplayPlayer;
    use crate::framework::context::ServerContext;

    for should_commit in [false, true] {
        let mut backend = ServerWorkerState::new(2).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.replication_mut().backlog.activate();
        let start = backend.ctx.replication().backlog.next_stream_cursor();
        let (token, mut primary) = add_runtime_connection(&mut backend);
        backend
            .replication_runtime
            .set_replica_link(ReplicaLink::streaming(
                ConnectionId(token),
                MasterInfo {
                    host: "127.0.0.1".to_string(),
                    port: 6379,
                },
                backend.ctx.replication().backlog.replid().to_string(),
                offset(0),
            ));
        let aof_path = std::env::temp_dir().join(format!(
            "versal-transaction-relay-{}-{:?}-{should_commit}.aof",
            std::process::id(),
            std::thread::current().id()
        ));
        backend.ctx.config_mut().persistence.aof_enabled = true;
        backend.ctx.config_mut().persistence.aof_fsync =
            crate::foundation::io::persistence::AofFsync::No;
        backend.ctx.aof_mut().journal = Some(
            crate::foundation::io::persistence::journal::AofJournal::new(
                crate::foundation::io::persistence::AofAppender::open_append_path(
                    &aof_path,
                    crate::foundation::io::persistence::AofFsync::No,
                )
                .unwrap(),
            ),
        );
        let mut stream = [
            protocol::encode_command(&[b"MULTI"]),
            protocol::encode_command(&[b"SELECT", b"1"]),
        ]
        .concat();
        primary.write_all(&stream).unwrap();
        backend.transport.wait_readable_for_test(Token(token));
        backend.recv_replication_stream(token, token);
        assert_eq!(
            backend.ctx.replication().backlog.next_stream_cursor(),
            start,
            "a snapshot must not start inside an uncommitted transaction"
        );
        assert_eq!(backend.ctx.repl_seldb(), -1);
        assert_eq!(backend.ctx.persistence().aof_selected_db(), -1);
        assert!(std::fs::read(&aof_path).unwrap().is_empty());

        let mut writes = Vec::new();
        for index in 0..63 {
            writes.extend(protocol::encode_command(&[
                b"SET",
                format!("key:{index}").as_bytes(),
                b"value",
            ]));
        }
        primary.write_all(&writes).unwrap();
        backend.transport.wait_readable_for_test(Token(token));
        backend.recv_replication_stream(token, token);
        stream.extend_from_slice(&writes);
        assert_eq!(backend.ctx.db(1).unwrap().dbsize(), 0);
        assert_eq!(
            backend.ctx.replication().backlog.next_stream_cursor(),
            start,
            "queued writes cannot become a post-snapshot replication tail"
        );
        assert!(std::fs::read(&aof_path).unwrap().is_empty());

        if should_commit {
            let finish = [
                protocol::encode_command(&[b"RPUSH", b"list", b"once"]),
                protocol::encode_command(&[b"EXEC"]),
            ]
            .concat();
            primary.write_all(&finish).unwrap();
            backend.transport.wait_readable_for_test(Token(token));
            backend.recv_replication_stream(token, token);
            stream.extend_from_slice(&finish);
            assert_eq!(backend.ctx.db(1).unwrap().dbsize(), 64);
            assert_eq!(backend.ctx.repl_seldb(), 1);
            assert_eq!(backend.ctx.persistence().aof_selected_db(), 1);
            assert_eq!(
                backend.ctx.replication().backlog.read_from(start),
                Some(stream.clone())
            );
            assert_eq!(
                std::fs::read(&aof_path).unwrap(),
                [protocol::encode_command(&[b"SELECT", b"0"]), stream.clone()].concat()
            );
            let mut downstream = ServerContext::new(2);
            let mut player = AofReplayPlayer::new(&backend.registry, &mut downstream);
            load_aof_bytes(&stream, &mut player).unwrap();
            assert_eq!(player.ctx.db(0).unwrap().dbsize(), 0);
            assert_eq!(player.ctx.db(1).unwrap().dbsize(), 64);
            let list = player.ctx.db(1).unwrap().get_no_expire(b"list").unwrap();
            assert_eq!(list.list_len(), 1);
            assert_eq!(list.list_get(0).unwrap(), b"once");
        } else {
            backend.ctx.replication_mut().switch_to_primary();
            backend.ctx.replication_mut().pending_replicaof_request = true;
            backend.apply_pending_replicaof_request();
            assert!(backend.replication_runtime.replica_link().is_none());
            assert_eq!(backend.ctx.db(1).unwrap().dbsize(), 0);
            assert_eq!(
                backend.ctx.replication().backlog.next_stream_cursor(),
                start,
                "promotion must discard the unfinished transaction without orphaned writes"
            );
            assert!(std::fs::read(&aof_path).unwrap().is_empty());
        }
        drop(backend);
        std::fs::remove_file(aof_path).unwrap();
    }
}

#[test]
fn committed_replication_transaction_restores_the_aof_starting_database() {
    use crate::foundation::io::persistence::load_aof_bytes;
    use crate::framework::aof_replay::AofReplayPlayer;
    use crate::framework::context::ServerContext;

    let mut backend = ServerWorkerState::new(2).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.replication_mut().switch_to_replica();
    backend.ctx.replication_mut().backlog.activate();
    let (token, mut primary) = add_runtime_connection(&mut backend);
    let mut link = ReplicaLink::streaming(
        ConnectionId(token),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        backend.ctx.replication().backlog.replid().to_string(),
        offset(0),
    );
    link.set_selected_db(1);
    backend.replication_runtime.set_replica_link(link);
    backend.connection_registry[Token(token)].selected_db = 1;
    backend.ctx.set_repl_seldb(1);
    backend.ctx.persistence_mut().set_aof_selected_db(0);
    let aof_path = std::env::temp_dir().join(format!(
        "versal-transaction-aof-db-{}-{:?}.aof",
        std::process::id(),
        std::thread::current().id()
    ));
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.ctx.config_mut().persistence.aof_fsync =
        crate::foundation::io::persistence::AofFsync::No;
    backend.ctx.aof_mut().journal = Some(
        crate::foundation::io::persistence::journal::AofJournal::new(
            crate::foundation::io::persistence::AofAppender::open_append_path(
                &aof_path,
                crate::foundation::io::persistence::AofFsync::No,
            )
            .unwrap(),
        ),
    );
    let stream = [
        protocol::encode_command(&[b"MULTI"]),
        protocol::encode_command(&[b"SET", b"transaction-db", b"one"]),
        protocol::encode_command(&[b"SELECT", b"0"]),
        protocol::encode_command(&[b"SET", b"transaction-db", b"zero"]),
        protocol::encode_command(&[b"EXEC"]),
    ]
    .concat();
    primary.write_all(&stream).unwrap();
    backend.transport.wait_readable_for_test(Token(token));
    backend.recv_replication_stream(token, token);
    assert_eq!(backend.ctx.persistence().aof_selected_db(), 0);
    let journal = std::fs::read(&aof_path).unwrap();
    let mut restored = ServerContext::new(2);
    let mut player = AofReplayPlayer::new(&backend.registry, &mut restored);
    load_aof_bytes(&journal, &mut player).unwrap();
    for (db, expected) in [(0, b"zero".as_slice()), (1, b"one".as_slice())] {
        assert_eq!(
            player
                .ctx
                .db(db)
                .unwrap()
                .get_no_expire(b"transaction-db")
                .map(|value| value.as_bytes()),
            Some(expected),
            "AOF must select the transaction's starting DB before MULTI"
        );
    }
    drop(backend);
    std::fs::remove_file(aof_path).unwrap();
}

#[test]
fn replicated_transaction_is_journaled_before_writable_replica_blocked_pops() {
    use crate::engine::command::context::{BlockedCmd, BlockedState};
    use crate::foundation::io::persistence::load_aof_bytes;
    use crate::framework::aof_replay::AofReplayPlayer;
    use crate::framework::context::ServerContext;

    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.replication_mut().switch_to_replica();
    backend.ctx.config_mut().replication.replica_read_only = false;
    backend.ctx.replication_mut().backlog.activate();
    backend.ctx.set_repl_seldb(0);
    backend.ctx.persistence_mut().set_aof_selected_db(0);
    let (token, mut primary) = add_runtime_connection(&mut backend);
    backend
        .replication_runtime
        .set_replica_link(ReplicaLink::streaming(
            ConnectionId(token),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
            backend.ctx.replication().backlog.replid().to_string(),
            offset(0),
        ));
    let (blocked, _client) = add_runtime_connection(&mut backend);
    let key = Bytes::from_static(b"list");
    backend.connection_registry[Token(blocked)].blocked = Some(BlockedState {
        cmd: BlockedCmd::Blpop,
        keys: vec![key.clone()],
        timeout_ms: 0,
        start_time_ms: crate::foundation::io::sys::time::mstime(),
        orig_args: CommandArgs::from_parts(vec![
            Bytes::from_static(b"BLPOP"),
            key.clone(),
            Bytes::from_static(b"0"),
        ]),
        req_bytes: 32,
        orig_wrapped: None,
    });
    let session = backend.connection_registry[Token(blocked)].session_id();
    backend
        .ctx
        .blocking_registry_mut()
        .register_client(0, &[key], session);
    let aof_path = std::env::temp_dir().join(format!(
        "versal-transaction-wakeup-{}-{:?}.aof",
        std::process::id(),
        std::thread::current().id()
    ));
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.ctx.config_mut().persistence.aof_fsync =
        crate::foundation::io::persistence::AofFsync::No;
    backend.ctx.aof_mut().journal = Some(
        crate::foundation::io::persistence::journal::AofJournal::new(
            crate::foundation::io::persistence::AofAppender::open_append_path(
                &aof_path,
                crate::foundation::io::persistence::AofFsync::No,
            )
            .unwrap(),
        ),
    );
    let stream = [
        protocol::encode_command(&[b"MULTI"]),
        protocol::encode_command(&[b"RPUSH", b"list", b"value"]),
        protocol::encode_command(&[b"SET", b"marker", b"done"]),
        protocol::encode_command(&[b"EXEC"]),
    ]
    .concat();
    primary.write_all(&stream).unwrap();
    backend.transport.wait_readable_for_test(Token(token));
    backend.recv_replication_stream(token, token);
    assert!(backend.connection_registry[Token(blocked)]
        .blocked
        .is_none());
    assert!(backend.ctx.db(0).unwrap().get_no_expire(b"list").is_none());
    let journal = std::fs::read(&aof_path).unwrap();
    let mut restored = ServerContext::new(1);
    let mut player = AofReplayPlayer::new(&backend.registry, &mut restored);
    load_aof_bytes(&journal, &mut player).unwrap();
    assert_eq!(
        player
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"marker")
            .unwrap()
            .as_bytes(),
        b"done"
    );
    assert!(
        player.ctx.db(0).unwrap().get_no_expire(b"list").is_none(),
        "the blocked pop must be journaled after the transaction that produced its value"
    );
    assert!(journal.starts_with(&stream));
    drop(backend);
    std::fs::remove_file(aof_path).unwrap();
}
