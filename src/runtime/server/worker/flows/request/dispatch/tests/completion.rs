use super::*;
use crate::engine::command::context::CommandConn;

fn check_client_cache_selection(command: &[u8], is_optin: bool, has_selection: bool) {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    if !command.eq_ignore_ascii_case(b"CLIENT") {
        backend.registry.rename("client", "cachectl");
    }
    let token = Token(add_fake_connection(&mut backend));
    backend.connection_registry[token].set_custom_reply_enabled(false);
    let owner = backend.connection_registry[token].token().0;
    let mode = if is_optin {
        b"OPTIN".as_slice()
    } else {
        b"OPTOUT".as_slice()
    };
    let mut commands = vec![vec![command, b"TRACKING", b"ON", mode]];
    if has_selection {
        commands.push(vec![
            command,
            b"CACHING",
            if is_optin {
                b"YES".as_slice()
            } else {
                b"NO".as_slice()
            },
        ]);
    }
    commands.extend([
        vec![b"GET".as_slice(), b"first"],
        vec![b"GET".as_slice(), b"following"],
    ]);
    let mut batch = ReadBatchEffects::default();
    for parts in commands {
        let args = make_args(&parts);
        let name = String::from_utf8_lossy(&args.name).to_ascii_lowercase();
        backend.process_parsed_command(token, args, name, false, &mut batch);
    }
    let mut expected = vec![b"+OK\r\n".to_vec()];
    if has_selection {
        expected.push(b"+OK\r\n".to_vec());
    }
    expected.extend([b"$-1\r\n".to_vec(), b"$-1\r\n".to_vec()]);
    assert_eq!(
        batch.replies, expected,
        "command={command:?}, is_optin={is_optin}, has_selection={has_selection}"
    );
    for (key, should_track) in [
        (b"first".as_slice(), has_selection == is_optin),
        (b"following".as_slice(), !is_optin),
    ] {
        assert_eq!(
            backend
                .ctx
                .tracking_mut()
                .take_clients_for_key(&Bytes::copy_from_slice(key)),
            if should_track {
                vec![owner]
            } else {
                Vec::new()
            },
            "command={command:?}, is_optin={is_optin}, has_selection={has_selection}, key={key:?}"
        );
    }
    assert!(backend.ctx.tracking().keys.is_empty());
}

#[test]
fn renamed_client_preserves_optin_selection_for_the_next_command() {
    for command in [b"CLIENT".as_slice(), b"cachectl", b"CaChEcTl"] {
        check_client_cache_selection(command, true, true);
    }
}

#[test]
fn renamed_client_preserves_optout_selection_for_the_next_command() {
    for command in [b"CLIENT".as_slice(), b"cachectl", b"CaChEcTl"] {
        check_client_cache_selection(command, false, true);
    }
}

#[test]
fn renamed_client_preserves_default_tracking_without_a_cache_selection() {
    for command in [b"CLIENT".as_slice(), b"cachectl", b"CaChEcTl"] {
        for is_optin in [true, false] {
            check_client_cache_selection(command, is_optin, false);
        }
    }
}

fn check_script_cache_selection(is_optin: bool) {
    for caching in [true, false] {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.set_command_registry_snapshot(&backend.registry);
        let token = Token(add_fake_connection(&mut backend));
        let conn = &mut backend.connection_registry[token];
        conn.set_custom_reply_enabled(false);
        conn.set_skip_propagation(false);
        conn.set_tracking(true);
        conn.set_tracking_optin(is_optin);
        conn.set_tracking_optout(!is_optin);
        conn.set_tracking_caching(caching);
        let owner = conn.token().0;
        for key in [b"first".as_slice(), b"second", b"following"] {
            backend.ctx.db_mut(0).unwrap().set(
                key.to_vec(),
                crate::foundation::io::store::Robj::string(b"value".as_slice()),
            );
        }
        let mut batch = ReadBatchEffects::default();
        for parts in [
            vec![
                b"EVAL".as_slice(),
                b"local a=redis.call('GET',KEYS[1]); return redis.call('GET',KEYS[2])",
                b"2",
                b"first",
                b"second",
            ],
            vec![b"GET".as_slice(), b"following"],
        ] {
            let args = make_args(&parts);
            let name = String::from_utf8_lossy(&args.name).to_ascii_lowercase();
            backend.process_parsed_command(token, args, name, false, &mut batch);
        }
        assert_eq!(
            batch.replies,
            vec![b"$5\r\nvalue\r\n".to_vec(), b"$5\r\nvalue\r\n".to_vec()]
        );
        for (key, should_track) in [
            (b"first".as_slice(), caching == is_optin),
            (b"second".as_slice(), caching == is_optin),
            (b"following".as_slice(), !is_optin),
        ] {
            assert_eq!(
                backend
                    .ctx
                    .tracking_mut()
                    .take_clients_for_key(&Bytes::copy_from_slice(key)),
                if should_track {
                    vec![owner]
                } else {
                    Vec::new()
                },
                "is_optin={is_optin}, caching={caching}, key={key:?}"
            );
        }
        assert!(backend.ctx.tracking().keys.is_empty());
    }
}

#[test]
fn script_completion_consumes_optin_selection_before_the_next_read() {
    check_script_cache_selection(true);
}

#[test]
fn script_completion_consumes_optout_selection_before_the_next_read() {
    check_script_cache_selection(false);
}

#[test]
fn cache_selection_survives_client_metadata_but_not_another_completed_command() {
    for (parts, should_track) in [
        (vec![b"CLIENT".as_slice(), b"ID"], true),
        (vec![b"PING".as_slice()], false),
        (vec![b"SET".as_slice(), b"other", b"value"], false),
        (vec![b"GET".as_slice()], false),
    ] {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        let token = Token(add_fake_connection(&mut backend));
        let conn = &mut backend.connection_registry[token];
        conn.set_custom_reply_enabled(false);
        conn.set_tracking(true);
        conn.set_tracking_optin(true);
        conn.set_tracking_caching(true);
        let owner = conn.token().0;
        let mut batch = ReadBatchEffects::default();
        for args in [make_args(&parts), make_args(&[b"GET", b"following"])] {
            let name = String::from_utf8_lossy(&args.name).to_ascii_lowercase();
            backend.process_parsed_command(token, args, name, false, &mut batch);
        }
        assert_eq!(batch.replies.len(), 2, "parts={parts:?}");
        assert_eq!(batch.replies[1], b"$-1\r\n", "parts={parts:?}");
        if parts == vec![b"GET".as_slice()] {
            assert!(
                batch.replies[0].starts_with(b"-ERR wrong number"),
                "parts={parts:?}"
            );
        }
        assert_eq!(
            backend
                .ctx
                .tracking_mut()
                .take_clients_for_key(&Bytes::from_static(b"following")),
            if should_track {
                vec![owner]
            } else {
                Vec::new()
            },
            "parts={parts:?}"
        );
    }
}

#[test]
fn cache_selection_survives_multi_and_is_consumed_by_empty_exec() {
    for is_optin in [true, false] {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        let token = Token(add_fake_connection(&mut backend));
        let conn = &mut backend.connection_registry[token];
        conn.set_custom_reply_enabled(false);
        conn.set_tracking(true);
        conn.set_tracking_optin(is_optin);
        conn.set_tracking_optout(!is_optin);
        conn.set_tracking_caching(true);
        let owner = conn.token().0;
        let mut batch = ReadBatchEffects::default();
        backend.process_parsed_command(
            token,
            make_args(&[b"MULTI"]),
            "multi".to_string(),
            false,
            &mut batch,
        );
        assert!(backend.connection_registry[token].tracking_caching());
        assert!(backend.connection_registry[token].in_transaction());
        backend.process_parsed_command(
            token,
            make_args(&[b"EXEC"]),
            "exec".to_string(),
            false,
            &mut batch,
        );
        assert!(!backend.connection_registry[token].in_transaction());
        assert_eq!(batch.replies, vec![b"+OK\r\n".to_vec(), b"*0\r\n".to_vec()]);
        backend.process_parsed_command(
            token,
            make_args(&[b"GET", b"following"]),
            "get".to_string(),
            false,
            &mut batch,
        );
        assert_eq!(
            backend
                .ctx
                .tracking_mut()
                .take_clients_for_key(&Bytes::from_static(b"following")),
            if is_optin { Vec::new() } else { vec![owner] }
        );
    }
}

#[test]
fn stream_final_initial_block_notifications_follow_prior_batch_reply() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().server.notify_keyspace_events = "Kt".to_string();
    let token = Token(add_fake_connection(&mut backend));
    backend.connection_registry[token].resp_version = 3;
    backend.connection_registry[token].set_custom_reply_enabled(false);
    backend.connection_registry[token].set_skip_propagation(false);
    backend
        .ctx
        .pubsub_mut()
        .subscribe(token.0, Bytes::from_static(b"__keyspace@0__:s"));
    let mut batch = ReadBatchEffects::default();
    for parts in [
        vec![
            b"XGROUP".as_slice(),
            b"CREATE",
            b"s",
            b"g",
            b"0",
            b"MKSTREAM",
        ],
        vec![
            b"XREADGROUP".as_slice(),
            b"GROUP",
            b"g",
            b"c",
            b"BLOCK",
            b"0",
            b"NOACK",
            b"STREAMS",
            b"s",
            b">",
        ],
    ] {
        let args = make_args(&parts);
        let name = String::from_utf8_lossy(&args.name).to_ascii_lowercase();
        backend.process_parsed_command(token, args, name, true, &mut batch);
    }
    assert!(backend.connection_registry[token].blocked.is_some());
    assert!(backend.connection_registry[token].output.queue.is_empty());
    assert_eq!(batch.publish.len(), 2);
    backend.finalize_handle_readable(token.0, token.0, batch);
    let output: Vec<u8> = backend.connection_registry[token]
        .output
        .queue
        .iter()
        .flat_map(|frame| frame.iter().copied())
        .collect();
    assert!(output.starts_with(b"+OK\r\n>3\r\n"), "output: {output:?}");
    assert_eq!(
        output
            .windows(21)
            .filter(|part| *part == b"xgroup-createconsumer")
            .count(),
        1
    );
}

#[test]
fn stream_final_reblock_notifications_follow_triggering_batch_reply() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let reader = Token(add_fake_connection(&mut backend));
    backend.connection_registry[reader].set_custom_reply_enabled(false);
    backend.connection_registry[reader].set_skip_propagation(false);
    let mut initial = ReadBatchEffects::default();
    for parts in [
        vec![
            b"XGROUP".as_slice(),
            b"CREATE",
            b"s",
            b"g",
            b"0",
            b"MKSTREAM",
        ],
        vec![
            b"XREADGROUP".as_slice(),
            b"GROUP",
            b"g",
            b"c",
            b"BLOCK",
            b"0",
            b"NOACK",
            b"STREAMS",
            b"s",
            b">",
        ],
    ] {
        let args = make_args(&parts);
        let name = String::from_utf8_lossy(&args.name).to_ascii_lowercase();
        backend.process_parsed_command(reader, args, name, false, &mut initial);
    }
    backend.finalize_handle_readable(reader.0, reader.0, initial);
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(Token(44));
    connection.resp_version = 3;
    connection.set_custom_reply_enabled(false);
    connection.set_skip_propagation(false);
    let trigger = Token(backend.connection_registry.insert(connection));
    backend.ctx.config_mut().server.notify_keyspace_events = "Kt".to_string();
    backend
        .ctx
        .pubsub_mut()
        .subscribe(trigger.0, Bytes::from_static(b"__keyspace@0__:s"));
    let mut batch = ReadBatchEffects::default();
    let args = make_args(&[b"XGROUP", b"DELCONSUMER", b"s", b"g", b"c"]);
    backend.process_parsed_command(trigger, args, "xgroup".to_string(), true, &mut batch);
    backend.process_dirty_keys_in_loop(trigger.0, &mut batch);
    assert!(backend.connection_registry[reader].blocked.is_some());
    assert!(backend.connection_registry[trigger].output.queue.is_empty());
    assert_eq!(batch.publish.len(), 2);
    assert_eq!(
        batch.replication_only.len(),
        2,
        "frames: {:?}",
        batch.replication_only
    );
    backend.finalize_handle_readable(trigger.0, trigger.0, batch);
    let output: Vec<u8> = backend.connection_registry[trigger]
        .output
        .queue
        .iter()
        .flat_map(|frame| frame.iter().copied())
        .collect();
    assert!(output.starts_with(b":0\r\n>3\r\n"), "output: {output:?}");
    assert_eq!(
        output
            .windows(21)
            .filter(|part| *part == b"xgroup-createconsumer")
            .count(),
        1
    );
}

#[test]
fn stream_final_initial_block_preserves_read_batch_propagation_order() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let token = Token(add_fake_connection(&mut backend));
    let mut batch = ReadBatchEffects::default();
    for parts in [
        vec![
            b"XGROUP".as_slice(),
            b"CREATE",
            b"s",
            b"g",
            b"0",
            b"MKSTREAM",
        ],
        vec![
            b"XREADGROUP".as_slice(),
            b"GROUP",
            b"g",
            b"c",
            b"BLOCK",
            b"0",
            b"NOACK",
            b"STREAMS",
            b"s",
            b">",
        ],
    ] {
        let args = make_args(&parts);
        let name = String::from_utf8_lossy(&args.name).to_ascii_lowercase();
        backend.process_parsed_command(token, args, name, false, &mut batch);
    }
    assert!(backend.connection_registry[token].blocked.is_some());
    assert_eq!(batch.replication_only.len(), 3);
    assert_eq!(batch.aof_only, batch.replication_only);
    assert!(batch.replication_only[1]
        .windows(6)
        .any(|part| part == b"CREATE"));
    assert!(batch.replication_only[2]
        .windows(14)
        .any(|part| part == b"CREATECONSUMER"));
}

#[test]
fn stream_final_reblock_preserves_pending_propagation_order() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let token = Token(add_fake_connection(&mut backend));
    let mut batch = ReadBatchEffects::default();
    for parts in [
        vec![
            b"XGROUP".as_slice(),
            b"CREATE",
            b"s",
            b"g",
            b"0",
            b"MKSTREAM",
        ],
        vec![
            b"XREADGROUP".as_slice(),
            b"GROUP",
            b"g",
            b"c",
            b"BLOCK",
            b"0",
            b"NOACK",
            b"STREAMS",
            b"s",
            b">",
        ],
    ] {
        let args = make_args(&parts);
        let name = String::from_utf8_lossy(&args.name).to_ascii_lowercase();
        backend.process_parsed_command(token, args, name, false, &mut batch);
    }
    backend
        .ctx
        .db_mut(0)
        .unwrap()
        .get_stream_mut(b"s")
        .unwrap()
        .unwrap()
        .delete_consumer(b"g", b"c")
        .unwrap();
    let deletion = crate::runtime::server::worker::distributed::propagation::encode_command_args(
        &make_args(&[b"XGROUP", b"DELCONSUMER", b"s", b"g", b"c"]),
    );
    let mut replication = vec![deletion.clone()];
    let mut aof = vec![deletion.clone()];
    backend.wakeup_blocked_client(token, Some(&mut replication), Some(&mut aof));
    assert!(backend.connection_registry[token].blocked.is_some());
    assert_eq!(replication.len(), 2);
    assert_eq!(replication[0], deletion);
    assert!(replication[1]
        .windows(14)
        .any(|part| part == b"CREATECONSUMER"));
    assert_eq!(aof, replication);
}

#[test]
fn aof_write_without_backlog_records_one_logical_offset_on_the_connection() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().persistence.aof_enabled = true;
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[Token(connection_token)].set_skip_propagation(false);
    let args = make_args(&[b"SET", b"key", b"value"]);
    let flags = backend.registry.lookup(b"set").unwrap().flags;
    let mut effects = DispatchSideEffects::default();
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

    assert_eq!(
        backend.connection_registry[Token(connection_token)]
            .last_write_offset()
            .get(),
        1
    );
    assert!(!backend.ctx.replication().backlog.is_active());
    assert_eq!(backend.ctx.replication().backlog.history_len(), 0);
}

#[test]
fn may_replicate_command_builds_propagation_without_dirty_write() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[Token(connection_token)].set_skip_propagation(false);
    let args = make_args(&[b"SPUBLISH", b"channel", b"message"]);
    let flags = backend.registry.lookup(b"spublish").unwrap().flags;
    let mut effects = DispatchSideEffects::default();
    let connection_effects = take_connection_command_effects(&mut backend, connection_token);

    backend.build_command_propagation(
        CommandPropagationInput {
            connection_token,
            token_value: 42,
            args: &args,
            name: "spublish",
            command_flags: flags,
            rejected: false,
        },
        connection_effects,
        &mut effects,
    );

    assert_eq!(effects.replication_only.len(), 2);
    assert_eq!(effects.aof_only, effects.replication_only);
    assert!(effects.replication_only[1]
        .windows(8)
        .any(|part| part == b"SPUBLISH"));
    assert_eq!(backend.ctx.persistence().dirty(), 0);
}

#[test]
fn may_replicate_command_commits_dirty_keys_when_propagation_is_disabled() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    let key = Bytes::from_static(b"local-only-key");
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .set_skip_propagation(true);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .record_dirty_key((0, key.clone()));
    let args = make_args(&[b"EVAL", b"return 1", b"0"]);
    let flags = backend.registry.lookup(b"eval").unwrap().flags;
    let mut effects = CommandEffects::default();
    let connection_effects = take_connection_command_effects(&mut backend, connection_token);

    backend.build_command_propagation(
        CommandPropagationInput {
            connection_token,
            token_value: 42,
            args: &args,
            name: "eval",
            command_flags: flags,
            rejected: false,
        },
        connection_effects,
        &mut effects,
    );

    assert_eq!(effects.watch_dirty, vec![(0, key)]);
    assert!(backend.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)]
    .dirty_keys()
    .is_empty());
    assert!(effects.replication_only.is_empty());
    assert!(effects.aof_only.is_empty());
}

#[test]
fn completed_keyed_command_records_slot_metrics_once() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().server.cluster_enabled = true;
    backend.ctx.config_mut().server.cluster_slot_stats_enabled = true;
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .set_custom_reply_enabled(false);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .set_request_bytes_for_test(33);
    let args = make_args(&[b"SET", b"{one}key", b"value"]);
    let flags = backend.registry.lookup(b"SET").unwrap().flags;
    let mut effects = DispatchSideEffects::default();
    let mut connection_effects = take_connection_command_effects(&mut backend, connection_token);

    let disposition = backend.complete_command(
        CommandCompletion {
            invocation: CommandInvocation::direct(
                CommandTarget::new(Token(42)),
                &args,
                "set",
                flags,
                DispatchGroup::Default,
                &[],
            ),
            reply: Value::ok(),
            status: CommandExecutionStatus::Executed,
            duration_us: 7,
            was_blocked_before: false,
        },
        Some(&mut connection_effects),
        &mut effects,
    );
    assert_eq!(disposition, CommandDisposition::Completed);

    let slot = crate::engine::distributed::cluster::node::key_hash_slot(b"{one}key") as usize;
    let counters = backend.ctx.slot_metrics().counters(slot).unwrap();
    assert_eq!(counters.cpu_usec, 7);
    assert_eq!(counters.network_bytes_in, 33);
    assert_eq!(counters.network_bytes_out, 5);
}

#[test]
fn postloop_preserves_blocked_command_input_until_unblock() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_fake_connection(&mut backend);
    let args = make_args(&[b"BLPOP", b"key", b"0"]);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .set_request_bytes_for_test(31);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .blocked = Some(crate::engine::command::context::BlockedState {
        cmd: BlockedCmd::Blpop,
        keys: vec![Bytes::from_static(b"key")],
        timeout_ms: 0,
        start_time_ms: crate::foundation::io::sys::time::mstime(),
        orig_args: args,
        req_bytes: 31,
        orig_wrapped: None,
    });

    backend.finalize_handle_readable(connection_token, 42, ReadBatchEffects::default());

    assert_eq!(
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .request_bytes_current_command(),
        31
    );
}

#[test]
fn replica_link_reply_is_dropped_and_marks_the_connection_closing() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_fake_connection(&mut backend);
    backend.replication_runtime.register_primary(
        ConnectionId(42),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let psync = make_args(&[b"PSYNC", b"?", b"-1"]);
    let mut psync_replies = Vec::new();
    let mut unblocks = Vec::new();
    assert_eq!(
        backend.try_handle_transport_cmd(
            connection_token,
            42,
            &psync,
            "psync",
            false,
            &mut psync_replies,
            &mut unblocks,
        ),
        Some(false)
    );
    assert!(backend
        .replication_runtime
        .primary_for_connection(ConnectionId(42))
        .unwrap()
        .accept_seq()
        .is_some());
    let args = make_args(&[b"PING"]);
    let fullresync = b"+FULLRESYNC replid 0\r\n".to_vec();
    let mut batch = ReadBatchEffects::default();
    batch.replies.push(fullresync.clone());
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
            reply: Value::SimpleString(Bytes::from_static(b"PONG")),
            status: CommandExecutionStatus::Executed,
            duration_us: 1,
            was_blocked_before: false,
        },
        Some(&mut connection_effects),
        &mut effects,
    );
    assert_eq!(disposition, CommandDisposition::Closing);
    batch.commit(effects);

    assert_eq!(batch.replies, vec![fullresync]);
    assert!(batch.mark_closing);
    assert_eq!(
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .state,
        crate::framework::capabilities::connection::ConnectionState::Closing
    );
}

#[test]
fn pre_psync_replica_link_allows_an_ordinary_reply() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let connection_token = add_fake_connection(&mut backend);
    backend.connection_registry[Token(connection_token)].set_custom_reply_enabled(false);
    backend.replication_runtime.register_primary(
        ConnectionId(42),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
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
            reply: Value::SimpleString(Bytes::from_static(b"PONG")),
            status: CommandExecutionStatus::Executed,
            duration_us: 1,
            was_blocked_before: false,
        },
        Some(&mut connection_effects),
        &mut effects,
    );
    assert_eq!(disposition, CommandDisposition::Completed);
    let mut batch = ReadBatchEffects::default();
    batch.commit(effects);
    assert_eq!(batch.replies, vec![b"+PONG\r\n".to_vec()]);
    assert!(!batch.mark_closing);
    assert_eq!(
        backend.connection_registry[Token(connection_token)].state,
        crate::framework::capabilities::connection::ConnectionState::Connected
    );
    assert!(backend
        .replication_runtime
        .primary_for_connection(ConnectionId(42))
        .unwrap()
        .accept_seq()
        .is_none());
}

#[test]
fn exec_aggregate_reply_is_accounted_once_at_the_command_boundary() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    let token = Token(connection_token);
    backend.connection_registry[token].set_custom_reply_enabled(false);
    assert!(backend.connection_registry[token].transaction.begin(0, 0));
    assert!(backend.connection_registry[token]
        .transaction
        .queue(backend.ctx.num_dbs(), make_args(&[b"PING"])));
    let mut effects = ReadBatchEffects::default();

    backend.process_parsed_command(
        token,
        make_args(&[b"EXEC"]),
        "exec".to_string(),
        false,
        &mut effects,
    );

    let expected: usize = effects.replies.iter().map(Vec::len).sum();
    assert!(expected > 0);
    assert_eq!(
        backend.connection_registry[token].reply_bytes_current_command(),
        expected
    );
}

#[test]
fn direct_reply_is_not_double_counted_by_the_command_boundary() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    let token = Token(connection_token);
    backend.connection_registry[token].set_custom_reply_enabled(false);
    let mut effects = ReadBatchEffects::default();

    backend.process_parsed_command(
        token,
        make_args(&[b"PING"]),
        "ping".to_string(),
        false,
        &mut effects,
    );

    let expected: usize = effects.replies.iter().map(Vec::len).sum();
    assert_eq!(expected, b"+PONG\r\n".len());
    assert_eq!(
        backend.connection_registry[token].reply_bytes_current_command(),
        expected
    );
}

#[test]
fn queued_transaction_reply_is_accounted_once_at_the_command_boundary() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    let token = Token(connection_token);
    backend.connection_registry[token].set_custom_reply_enabled(false);
    assert!(backend.connection_registry[token].transaction.begin(0, 0));
    let mut effects = ReadBatchEffects::default();

    backend.process_parsed_command(
        token,
        make_args(&[b"SET", b"key", b"value"]),
        "set".to_string(),
        false,
        &mut effects,
    );

    assert_eq!(effects.replies, [b"+QUEUED\r\n".to_vec()]);
    assert_eq!(
        backend.connection_registry[token].reply_bytes_current_command(),
        b"+QUEUED\r\n".len()
    );
}

#[test]
fn publish_reply_is_accounted_without_charging_async_delivery() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let connection_token = add_fake_connection(&mut backend);
    let token = Token(connection_token);
    backend.connection_registry[token].set_custom_reply_enabled(false);
    let mut effects = ReadBatchEffects::default();

    backend.process_parsed_command(
        token,
        make_args(&[b"PUBLISH", b"channel", b"message"]),
        "publish".to_string(),
        false,
        &mut effects,
    );

    let expected: usize = effects.replies.iter().map(Vec::len).sum();
    assert!(expected > 0);
    assert_eq!(
        backend.connection_registry[token].reply_bytes_current_command(),
        expected
    );
    backend.queue_client_reply(token, b"async-push".to_vec());
    assert_eq!(
        backend.connection_registry[token].reply_bytes_current_command(),
        expected
    );
}
