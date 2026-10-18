use super::*;
use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};

#[test]
fn hosted_exec_connection_removal_stops_without_reindexing_the_registry() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_command_registry_snapshot(&backend.registry);
    backend.ctx.config_mut().server.lua_time_limit = 1;
    let (_target_peer, _, target) = add_socket_connection(&mut backend);
    let (mut controller, _, _) = add_socket_connection(&mut backend);
    assert!(backend.connection_registry[target].transaction.begin(0, 0));
    assert!(backend.connection_registry[target]
        .transaction
        .queue(make_args(&[b"EVAL", b"while true do end", b"0"])));
    let target_id = backend.connection_registry[target].client_id().to_string();
    let controller_commands = format!(
        "*4\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$2\r\nID\r\n${}\r\n{}\r\n*2\r\n$6\r\nSCRIPT\r\n$4\r\nKILL\r\n",
        target_id.len(),
        target_id
    );
    controller
        .write_all(controller_commands.as_bytes())
        .expect("queue controller kill commands");
    let mut effects = ReadBatchEffects::default();

    let action = backend.process_parsed_command(
        target,
        make_args(&[b"EXEC"]),
        "exec".to_string(),
        false,
        &mut effects,
    );

    assert_eq!(action, LoopAction::Break);
    assert!(!backend.connection_registry.contains(target));
}

#[test]
fn setslot_ack_wait_defers_client_kill_until_the_wait_finishes() {
    const TARGET: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_command_registry_snapshot(&backend.registry);
    let (_initiator_peer, _, initiator) = add_socket_connection(&mut backend);
    let (mut controller, _, _) = add_socket_connection(&mut backend);
    let initiator_id = backend.connection_registry[initiator]
        .client_id()
        .to_string();
    let kill = format!(
        "*4\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$2\r\nID\r\n${}\r\n{}\r\n",
        initiator_id.len(),
        initiator_id
    );
    controller
        .write_all(kill.as_bytes())
        .expect("queue controller CLIENT KILL");

    let mut cluster =
        crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16_379);
    cluster.add_slot(0).expect("assign source slot");
    let mut target = ClusterNode::new(
        TARGET.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    target.flags &= !CLUSTER_NODE_MYSELF;
    cluster.nodes.insert(TARGET.to_string(), target);
    *backend.ctx.cluster_state_mut() = Some(Box::new(cluster));
    let replica_id = backend.replication_runtime.register_primary(
        crate::framework::distributed::replication::ConnectionId(99),
        crate::framework::distributed::replication::ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let replica = backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("registered replica link");
    replica.set_state(crate::framework::distributed::replication::PrimaryLinkState::Streaming);
    replica.set_replica_version(0x080000);
    let args = make_args(&[
        b"CLUSTER",
        b"SETSLOT",
        b"0",
        b"MIGRATING",
        TARGET.as_bytes(),
        b"TIMEOUT",
        b"50",
    ]);

    let reply = backend.wait_for_cluster_setslot_replication(initiator, &args);

    assert!(matches!(reply, Some(Value::Error(_))));
    assert!(backend.connection_registry.contains(initiator));
    assert!(!backend.connection_registry[initiator].protected);
    assert_eq!(
        backend.connection_registry[initiator].state,
        ConnectionState::Closing
    );
}

#[test]
fn blocked_client_buffers_readable_data_and_reprocesses_it_after_unblock() {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    let (mut peer, _connection_token, token) = add_socket_connection(&mut backend);
    backend.ctx.config_mut().server.maxmemory_clients = "1gb".to_string();
    let accounted_before = backend.connection_registry[token].accounted_normal_memory();
    let blocked_args = make_args(&[b"SET", b"first", b"value"]);
    backend.connection_registry[token].begin_nested_command_effects();
    backend.connection_registry[token].postpone_command(
        BlockedCmd::Paused,
        Vec::new(),
        0,
        &blocked_args,
        32,
    );
    let sequence = backend.connection_registry[token].command_seq;
    let value_len = crate::runtime::server::worker::NORMAL_CLIENT_READ_BUDGET + 4096;
    let mut request = format!("*3\r\n$3\r\nSET\r\n$6\r\nsecond\r\n${value_len}\r\n").into_bytes();
    request.extend(std::iter::repeat_n(b'x', value_len));
    request.extend_from_slice(b"\r\n");

    backend
        .transport
        .reserve_test_receive_buffer(token, request.len())
        .expect("size receive buffer for the unread blocked-client command");
    peer.write_all(&request)
        .expect("queue a command while the client is blocked");
    backend
        .transport
        .wait_pending_bytes_for_test(token, request.len());
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    while !backend.connection_registry[token].parser.has_pending_data() {
        backend.handle_readable(token.0, ClientReadMode::Fair);
        assert!(
            std::time::Instant::now() < deadline,
            "server socket never received the blocked client's command"
        );
        std::thread::yield_now();
    }
    let first_read = backend.connection_registry[token].parser.pending_len();
    assert!(first_read <= crate::runtime::server::worker::NORMAL_CLIENT_READ_BUDGET);
    assert!(first_read < request.len());
    assert!(backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientRead(token)
    ));

    while backend.connection_registry[token].parser.pending_len() <= first_read {
        assert!(
            std::time::Instant::now() < deadline,
            "a blocked client did not continue draining its readable socket"
        );
        if backend.event_batch.contains_runnable(
            crate::runtime::server::worker::reactor::RunnableKey::ClientRead(token),
        ) {
            backend.drive_pending_client_reads();
        } else {
            backend.handle_readable(token.0, ClientReadMode::Fair);
        }
        assert_eq!(backend.connection_registry[token].command_seq, sequence);
        assert!(backend.connection_registry[token].blocked.is_some());
        std::thread::yield_now();
    }
    while backend.connection_registry[token].parser.pending_len() < request.len() {
        assert!(
            std::time::Instant::now() < deadline,
            "server socket did not buffer the complete blocked command"
        );
        if backend.event_batch.contains_runnable(
            crate::runtime::server::worker::reactor::RunnableKey::ClientRead(token),
        ) {
            backend.drive_pending_client_reads();
        } else {
            backend.handle_readable(token.0, ClientReadMode::Fair);
        }
        assert_eq!(backend.connection_registry[token].command_seq, sequence);
        assert!(backend.connection_registry[token].blocked.is_some());
        std::thread::yield_now();
    }
    backend.drive_pending_client_reads();

    assert_eq!(backend.connection_registry[token].command_seq, sequence);
    assert!(backend.connection_registry[token].blocked.is_some());
    assert_eq!(
        backend.connection_registry[token].parser.pending_len(),
        request.len()
    );
    assert!(backend.connection_registry[token].accounted_normal_memory() > accounted_before);
    assert!(!backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientRead(token)
    ));

    backend.wakeup_blocked_client(token, None, None);
    assert!(backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientRead(token)
    ));
    backend.drive_pending_client_reads();

    assert!(backend.connection_registry[token].blocked.is_none());
    assert_eq!(backend.connection_registry[token].command_seq, sequence + 1);
    assert!(!backend.connection_registry[token].parser.has_pending_data());
    assert!(!backend.event_batch.contains_runnable(
        crate::runtime::server::worker::reactor::RunnableKey::ClientRead(token)
    ));
    assert!(backend
        .ctx
        .db(0)
        .expect("default database")
        .exists_no_expire(b"first"));
    assert!(backend
        .ctx
        .db(0)
        .expect("default database")
        .exists_no_expire(b"second"));
}
