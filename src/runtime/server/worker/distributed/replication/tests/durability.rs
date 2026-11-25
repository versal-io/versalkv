use super::*;
use crate::foundation::io::persistence::{AofAppender, AofFsync};

fn strict_apply_fixture() -> (ServerWorkerState, usize, ReplicaLink, std::path::PathBuf) {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let token = add_connection(&mut backend, 41);
    backend.ctx.replication_mut().backlog.activate();
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.ctx.config_mut().persistence.aof_fsync = AofFsync::No;
    backend.ctx.config_mut().replication.repl_after_aof_fsync = true;
    let path =
        std::env::temp_dir().join(format!("versal-strict-apply-{}.aof", rand::random::<u64>()));
    backend.ctx.aof_mut().journal = Some(
        crate::foundation::io::persistence::journal::AofJournal::new(
            AofAppender::open_append_path(&path, AofFsync::No).unwrap(),
        ),
    );
    backend.ctx.aof_mut().replication_baseline = crate::engine::persistence::AofBaseline::Ready;
    let link = ReplicaLink::new(
        ConnectionId(token),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    (backend, token, link, path)
}

pub(super) fn apply_parts(
    backend: &mut ServerWorkerState,
    token: usize,
    link: &mut ReplicaLink,
    parts: &[&[u8]],
) -> Result<(), ()> {
    backend.apply_replication_command(
        token,
        link,
        ReplicationApply::Command(CommandArgs::from_parts(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part))
                .collect(),
        )),
        CommandOrigin::Replication,
    )
}

pub(super) fn cross_client_config(
    backend: &mut ServerWorkerState,
    token: usize,
    settings: &[&[u8]],
) -> crate::foundation::io::protocol::Value {
    let args = CommandArgs::from_parts(
        [b"CONFIG".as_slice(), b"SET"]
            .into_iter()
            .chain(settings.iter().copied())
            .map(Bytes::copy_from_slice)
            .collect(),
    );
    crate::framework::commands::server::config::config_set(
        &mut backend.connection_registry[Token(token)],
        &args,
        &mut backend.ctx,
    )
}

fn assert_cross_client_transaction_policy(initial_strict: bool, settings: &[&[u8]]) {
    use crate::foundation::io::protocol::Value;
    let (mut backend, token, mut link, path) = strict_apply_fixture();
    backend.ctx.config_mut().persistence.aof_fsync = AofFsync::EverySec;
    backend.ctx.config_mut().replication.repl_after_aof_fsync = initial_strict;
    let config_token = add_connection(&mut backend, 42);
    let (downstream_token, mut reader) = add_runtime_connection(&mut backend);
    let id = backend.replication_runtime.register_primary(
        ConnectionId(downstream_token),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 0,
            capability: vec![],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(id)
        .unwrap()
        .set_state(PrimaryLinkState::Streaming);
    apply_parts(&mut backend, token, &mut link, &[b"MULTI"]).unwrap();
    apply_parts(
        &mut backend,
        token,
        &mut link,
        &[b"SET", b"cross-client", b"persistent"],
    )
    .unwrap();
    let previous = backend.ctx.config().clone();
    let result = cross_client_config(&mut backend, config_token, settings);
    assert!(
        matches!(result, Value::Error(ref error) if error.windows(b"transaction".len()).any(|part| part == b"transaction")),
        "an independent client must not change transaction durability: {result:?}"
    );
    assert_eq!(
        backend.ctx.config().replication.repl_after_aof_fsync,
        previous.replication.repl_after_aof_fsync
    );
    assert_eq!(
        backend.ctx.config().persistence.aof_enabled,
        previous.persistence.aof_enabled
    );
    assert_eq!(
        backend.ctx.config().persistence.aof_fsync,
        previous.persistence.aof_fsync
    );
    assert_eq!(
        backend.ctx.config().persistence.aof_no_fsync_on_rewrite,
        previous.persistence.aof_no_fsync_on_rewrite
    );
    apply_parts(&mut backend, token, &mut link, &[b"EXEC"]).unwrap();
    backend
        .ctx
        .aof_mut()
        .journal
        .as_mut()
        .unwrap()
        .appender
        .force_fsync()
        .unwrap();
    backend.release_synced_replication();
    let expected = [
        protocol::encode_command(&[b"MULTI"]),
        protocol::encode_command(&[b"SET", b"cross-client", b"persistent"]),
        protocol::encode_command(&[b"EXEC"]),
    ]
    .concat();
    let bytes = std::fs::read(&path).unwrap();
    let expected_aof = [
        protocol::encode_command(&[b"SELECT", b"0"]),
        expected.clone(),
    ]
    .concat();
    assert_eq!(
        bytes, expected_aof,
        "AOF must contain the complete transaction in DB 0"
    );
    backend.flush_connection_output(downstream_token);
    reader
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .unwrap();
    let mut received = vec![0; expected.len()];
    reader.read_exact(&mut received).unwrap();
    assert_eq!(
        received, expected,
        "downstream must receive complete wire frames"
    );
    reader.set_nonblocking(true).unwrap();
    assert_eq!(
        reader.read(&mut [0; 1]).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );

    let mut replay = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut replay.registry);
    replay.ctx.set_command_registry_snapshot(&replay.registry);
    let mut replay_conn = Connection::new_fake();
    replay_conn.import_source = true;
    replay
        .run_aof_file_task(bytes, &mut replay_conn, None)
        .unwrap();
    assert_eq!(
        replay
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"cross-client")
            .unwrap()
            .as_bytes(),
        b"persistent"
    );
    assert_eq!(
        cross_client_config(&mut backend, config_token, settings),
        Value::ok()
    );
    drop(backend);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn cross_client_strict_disable_waits_for_complete_upstream_exec() {
    assert_cross_client_transaction_policy(true, &[b"repl-after-aof-fsync", b"no"]);
}

#[test]
fn cross_client_strict_enable_waits_for_complete_upstream_exec() {
    assert_cross_client_transaction_policy(false, &[b"repl-after-aof-fsync", b"yes"]);
}

#[test]
fn cross_client_strict_policy_changes_wait_for_complete_upstream_exec() {
    for settings in [
        &[b"appendfsync".as_slice(), b"always"][..],
        &[
            b"repl-after-aof-fsync".as_slice(),
            b"no",
            b"appendonly",
            b"no",
        ][..],
        &[
            b"repl-after-aof-fsync".as_slice(),
            b"no",
            b"no-appendfsync-on-rewrite",
            b"yes",
        ][..],
    ] {
        assert_cross_client_transaction_policy(true, settings);
    }
}

#[test]
fn cross_client_upstream_transaction_keeps_noop_unrelated_and_nonstrict_configs() {
    use crate::foundation::io::protocol::Value;
    for is_strict in [false, true] {
        let (mut backend, token, mut link, path) = strict_apply_fixture();
        backend.ctx.config_mut().persistence.aof_fsync = AofFsync::EverySec;
        backend.ctx.config_mut().replication.repl_after_aof_fsync = is_strict;
        let config_token = add_connection(&mut backend, 42);
        apply_parts(&mut backend, token, &mut link, &[b"MULTI"]).unwrap();
        apply_parts(&mut backend, token, &mut link, &[b"SET", b"key", b"value"]).unwrap();
        let mode = if is_strict {
            b"yes".as_slice()
        } else {
            b"no".as_slice()
        };
        assert_eq!(
            cross_client_config(&mut backend, config_token, &[b"repl-after-aof-fsync", mode]),
            Value::ok()
        );
        assert_eq!(
            cross_client_config(&mut backend, config_token, &[b"timeout", b"1"]),
            Value::ok()
        );
        if !is_strict {
            assert_eq!(
                cross_client_config(&mut backend, config_token, &[b"appendfsync", b"no"]),
                Value::ok()
            );
        }
        apply_parts(&mut backend, token, &mut link, &[b"EXEC"]).unwrap();
        assert_eq!(
            backend
                .ctx
                .db(0)
                .unwrap()
                .get_no_expire(b"key")
                .unwrap()
                .as_bytes(),
            b"value"
        );
        drop(backend);
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn cross_client_upstream_transaction_clears_on_disconnect_and_role_change() {
    use crate::foundation::io::protocol::Value;
    for cleanup in 0..3 {
        let (mut backend, token, mut link, path) = strict_apply_fixture();
        backend.ctx.config_mut().persistence.aof_fsync = AofFsync::EverySec;
        backend.ctx.replication_mut().switch_to_replica();
        let config_token = add_connection(&mut backend, 42);
        apply_parts(&mut backend, token, &mut link, &[b"MULTI"]).unwrap();
        assert!(matches!(
            cross_client_config(
                &mut backend,
                config_token,
                &[b"repl-after-aof-fsync", b"no"]
            ),
            Value::Error(_)
        ));
        match cleanup {
            0 => backend.remove_connection(token),
            1 => {
                backend.ctx.replication_mut().detach_to_primary();
            }
            _ => {
                backend.ctx.config_mut().replication.master_host = Some("127.0.0.1".to_string());
                let args = CommandArgs::from_parts(
                    ["REPLICAOF", "NO", "ONE"]
                        .into_iter()
                        .map(Bytes::from)
                        .collect(),
                );
                assert_eq!(
                    crate::framework::commands::server::replicaof::replicaof(
                        &mut backend.connection_registry[Token(config_token)],
                        &args,
                        &mut backend.ctx,
                    ),
                    Value::ok()
                );
                assert!(backend.ctx.replication().has_active_upstream_transaction());
                assert!(backend.connection_registry[Token(token)]
                    .transaction
                    .is_active());
                assert!(matches!(
                    cross_client_config(
                        &mut backend,
                        config_token,
                        &[b"repl-after-aof-fsync", b"no"]
                    ),
                    Value::Error(_)
                ));
                backend.apply_pending_replicaof_request();
                assert!(!backend.connection_registry[Token(token)]
                    .transaction
                    .is_active());
            }
        }
        assert_eq!(
            cross_client_config(
                &mut backend,
                config_token,
                &[b"repl-after-aof-fsync", b"no"]
            ),
            Value::ok()
        );
        drop(backend);
        std::fs::remove_file(path).unwrap();
    }
}

fn assert_cross_client_role_change_pipeline(is_strict: bool) {
    use crate::engine::persistence::AofBaseline;
    use crate::foundation::io::protocol::Value;
    use crate::runtime::server::worker::sessions::ClientReadMode;
    use std::io::BufRead;

    let (mut backend, fixture_token, _, path) = strict_apply_fixture();
    backend.remove_connection(fixture_token);
    let (token, mut upstream) = add_runtime_connection(&mut backend);
    let (config_token, mut client) = add_runtime_connection(&mut backend);
    let mut link = ReplicaLink::new(
        ConnectionId(token),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    backend.ctx.config_mut().persistence.aof_fsync = AofFsync::EverySec;
    backend.ctx.config_mut().replication.repl_after_aof_fsync = is_strict;
    backend.ctx.replication_mut().switch_to_replica();
    backend.ctx.config_mut().replication.master_host = Some("127.0.0.1".to_string());
    backend.ctx.config_mut().replication.master_port = 6379;
    apply_parts(&mut backend, token, &mut link, &[b"MULTI"]).unwrap();
    apply_parts(
        &mut backend,
        token,
        &mut link,
        &[b"SET", b"role-pipeline", b"uncommitted"],
    )
    .unwrap();
    assert!(backend.ctx.replication().has_active_upstream_transaction());
    assert_eq!(
        backend.connection_registry[Token(token)]
            .transaction
            .queued_commands()
            .len(),
        1
    );
    backend
        .ctx
        .aof_mut()
        .journal
        .as_mut()
        .unwrap()
        .appender
        .force_fsync()
        .unwrap();
    let expected_aof = if is_strict {
        Vec::new()
    } else {
        [
            protocol::encode_command(&[b"SELECT", b"0"]),
            protocol::encode_command(&[b"MULTI"]),
            protocol::encode_command(&[b"SET", b"role-pipeline", b"uncommitted"]),
        ]
        .concat()
    };
    assert_eq!(std::fs::read(&path).unwrap(), expected_aof);
    assert_eq!(backend.ctx.aof().replication_baseline, AofBaseline::Ready);
    backend.replication_runtime.set_replica_link(link);
    let target_mode = if is_strict {
        b"no".as_slice()
    } else {
        b"yes".as_slice()
    };
    let pipeline = [
        protocol::encode_command(&[b"REPLICAOF", b"NO", b"ONE"]),
        protocol::encode_command(&[b"CONFIG", b"SET", b"repl-after-aof-fsync", target_mode]),
    ]
    .concat();

    client.write_all(&pipeline).unwrap();
    backend
        .transport
        .wait_readable_for_test(Token(config_token));
    let (read_data, outcome) = backend.read_from_connection_bounded_outcome(config_token);
    // Both TCP frames enter one production read batch and its parser loop.
    assert_eq!(read_data, pipeline);
    backend.process_client_read_outcome(
        Token(config_token),
        ClientReadMode::Fair,
        read_data,
        outcome,
    );
    assert_eq!(
        backend.connection_registry[Token(config_token)]
            .parser
            .pending_len(),
        0
    );
    backend.flush_connection_output(config_token);
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .unwrap();
    let mut reader = std::io::BufReader::new(&mut client);
    let mut reply = String::new();
    reader.read_line(&mut reply).unwrap();
    assert_eq!(reply, "+OK\r\n");
    reply.clear();
    reader.read_line(&mut reply).unwrap();
    assert!(
        reply.starts_with('-') && reply.contains("transaction"),
        "the second pipelined command must retain the live upstream guard: {reply:?}"
    );
    assert_eq!(
        backend.ctx.config().replication.repl_after_aof_fsync,
        is_strict
    );
    assert!(!backend.ctx.replication().has_active_upstream_transaction());
    assert!(!backend.connection_registry.contains(Token(token)));
    assert!(backend.replication_runtime.replica_link().is_none());
    upstream
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .unwrap();
    assert_eq!(upstream.read(&mut [0; 1]).unwrap(), 0);
    assert!(backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"role-pipeline")
        .is_none());
    assert_eq!(
        backend.ctx.aof().replication_baseline,
        if is_strict {
            AofBaseline::Ready
        } else {
            AofBaseline::Unavailable
        }
    );
    assert_eq!(std::fs::read(&path).unwrap(), expected_aof);
    if is_strict {
        assert_eq!(
            cross_client_config(
                &mut backend,
                config_token,
                &[b"repl-after-aof-fsync", b"no"]
            ),
            Value::ok()
        );
        assert!(!backend.ctx.config().replication.repl_after_aof_fsync);
    } else {
        let result = cross_client_config(
            &mut backend,
            config_token,
            &[b"repl-after-aof-fsync", b"yes"],
        );
        assert!(
            matches!(result, Value::Error(ref error) if error.windows(b"baseline".len()).any(|part| part == b"baseline")),
            "the abandoned AOF prefix must still block strict enable: {result:?}"
        );
        assert_eq!(
            cross_client_config(&mut backend, config_token, &[b"appendfsync", b"no"]),
            Value::ok()
        );
    }
    drop(backend);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn cross_client_strict_role_change_pipeline_keeps_guard_until_runtime_cleanup() {
    assert_cross_client_role_change_pipeline(true);
}

#[test]
fn cross_client_nonstrict_role_change_pipeline_invalidates_aof_at_runtime_cleanup() {
    assert_cross_client_role_change_pipeline(false);
}

#[test]
fn cross_client_upstream_transaction_error_aborts_queue_and_allows_policy_change() {
    use crate::foundation::io::protocol::Value;
    let (mut backend, token, mut link, path) = strict_apply_fixture();
    backend.ctx.config_mut().persistence.aof_fsync = AofFsync::EverySec;
    let config_token = add_connection(&mut backend, 42);
    apply_parts(&mut backend, token, &mut link, &[b"MULTI"]).unwrap();
    apply_parts(
        &mut backend,
        token,
        &mut link,
        &[b"SET", b"aborted", b"value"],
    )
    .unwrap();
    assert!(backend
        .apply_replication_command(
            token,
            &mut link,
            ReplicationApply::Select(99),
            CommandOrigin::Replication
        )
        .is_err());
    assert!(!backend.connection_registry[Token(token)]
        .transaction
        .is_active());
    assert!(backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"aborted")
        .is_none());
    assert_eq!(
        cross_client_config(
            &mut backend,
            config_token,
            &[b"repl-after-aof-fsync", b"no"]
        ),
        Value::ok()
    );
    drop(backend);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn cross_client_upstream_exec_keeps_policy_guard_while_executing_the_queue() {
    use crate::foundation::io::protocol::Value;
    fn check_policy(
        _: &mut dyn crate::engine::command::context::CommandConn,
        _: &CommandArgs,
        ctx: &mut crate::framework::context::ServerContext,
    ) -> Value {
        let args = CommandArgs::from_parts(
            ["CONFIG", "SET", "repl-after-aof-fsync", "no"]
                .into_iter()
                .map(Bytes::from)
                .collect(),
        );
        let result = crate::framework::commands::server::config::config_set(
            &mut Connection::new_fake(),
            &args,
            ctx,
        );
        assert!(
            matches!(result, Value::Error(_)),
            "policy guard must outlive the taken queue: {result:?}"
        );
        Value::ok()
    }
    let (mut backend, token, mut link, path) = strict_apply_fixture();
    backend.ctx.config_mut().persistence.aof_fsync = AofFsync::EverySec;
    let mut spec = backend.registry.lookup(b"PING").unwrap().clone();
    spec.name = "checkpolicy";
    spec.executor = crate::framework::CommandExecutor::Immediate(check_policy);
    backend.registry.register(spec);
    apply_parts(&mut backend, token, &mut link, &[b"MULTI"]).unwrap();
    apply_parts(&mut backend, token, &mut link, &[b"CHECKPOLICY"]).unwrap();
    apply_parts(&mut backend, token, &mut link, &[b"SET", b"key", b"value"]).unwrap();
    apply_parts(&mut backend, token, &mut link, &[b"EXEC"]).unwrap();
    let config_token = add_connection(&mut backend, 42);
    assert_eq!(
        cross_client_config(
            &mut backend,
            config_token,
            &[b"repl-after-aof-fsync", b"no"]
        ),
        Value::ok()
    );
    drop(backend);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn cross_client_nonstrict_abandoned_aof_prefix_blocks_strict_enable() {
    use crate::foundation::io::protocol::Value;
    for is_role_change in [false, true] {
        let (mut backend, token, mut link, path) = strict_apply_fixture();
        backend.ctx.config_mut().replication.repl_after_aof_fsync = false;
        backend.ctx.config_mut().persistence.aof_fsync = AofFsync::EverySec;
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.config_mut().replication.master_host = Some("127.0.0.1".to_string());
        let config_token = add_connection(&mut backend, 42);
        apply_parts(&mut backend, token, &mut link, &[b"MULTI"]).unwrap();
        apply_parts(
            &mut backend,
            token,
            &mut link,
            &[b"SET", b"abandoned", b"value"],
        )
        .unwrap();
        backend
            .ctx
            .aof_mut()
            .journal
            .as_mut()
            .unwrap()
            .appender
            .force_fsync()
            .unwrap();
        let expected = [
            protocol::encode_command(&[b"SELECT", b"0"]),
            protocol::encode_command(&[b"MULTI"]),
            protocol::encode_command(&[b"SET", b"abandoned", b"value"]),
        ]
        .concat();
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        if is_role_change {
            let args = CommandArgs::from_parts(
                ["REPLICAOF", "NO", "ONE"]
                    .into_iter()
                    .map(Bytes::from)
                    .collect(),
            );
            assert_eq!(
                crate::framework::commands::server::replicaof::replicaof(
                    &mut backend.connection_registry[Token(config_token)],
                    &args,
                    &mut backend.ctx,
                ),
                Value::ok()
            );
            assert!(backend.ctx.replication().has_active_upstream_transaction());
            assert_eq!(
                backend.ctx.aof().replication_baseline,
                crate::engine::persistence::AofBaseline::Ready
            );
            backend.apply_pending_replicaof_request();
            assert!(!backend.connection_registry[Token(token)]
                .transaction
                .is_active());
        } else {
            backend.remove_connection(token);
        }
        assert!(!backend.ctx.replication().has_active_upstream_transaction());
        assert_eq!(
            backend.ctx.aof().replication_baseline,
            crate::engine::persistence::AofBaseline::Unavailable
        );
        let result = cross_client_config(
            &mut backend,
            config_token,
            &[b"repl-after-aof-fsync", b"yes"],
        );
        assert!(
            matches!(result, Value::Error(ref error) if error.windows(b"baseline".len()).any(|part| part == b"baseline")),
            "{result:?}"
        );
        assert!(!backend.ctx.config().replication.repl_after_aof_fsync);
        backend.flush_aof_if_needed();
        assert_eq!(
            backend.ctx.aof().replication_baseline,
            crate::engine::persistence::AofBaseline::Unavailable
        );
        drop(backend);
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn cross_client_failed_upstream_exec_invalidates_unrecorded_dataset_changes() {
    let (mut backend, token, mut link, path) = strict_apply_fixture();
    backend.ctx.config_mut().persistence.aof_fsync = AofFsync::EverySec;
    apply_parts(&mut backend, token, &mut link, &[b"MULTI"]).unwrap();
    apply_parts(
        &mut backend,
        token,
        &mut link,
        &[b"SET", b"partial-exec", b"value"],
    )
    .unwrap();
    // Exercise the executor's fallible SELECT after a successful queued write.
    apply_parts(&mut backend, token, &mut link, &[b"SELECT", b"99"]).unwrap();
    assert!(apply_parts(&mut backend, token, &mut link, &[b"EXEC"]).is_err());
    assert_eq!(
        backend
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"partial-exec")
            .unwrap()
            .as_bytes(),
        b"value"
    );
    backend
        .ctx
        .aof_mut()
        .journal
        .as_mut()
        .unwrap()
        .appender
        .force_fsync()
        .unwrap();
    assert!(std::fs::read(&path).unwrap().is_empty());
    assert!(!backend.ctx.replication().has_active_upstream_transaction());
    assert!(!backend.connection_registry[Token(token)]
        .transaction
        .is_active());
    assert_eq!(
        backend.ctx.aof().replication_baseline,
        crate::engine::persistence::AofBaseline::Unavailable
    );
    assert!(backend.ctx.replication_aof_write_error().is_some());
    assert!(apply_parts(
        &mut backend,
        token,
        &mut link,
        &[b"SET", b"must-not-write", b"value"]
    )
    .is_err());
    assert!(backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"must-not-write")
        .is_none());
    drop(backend);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn strict_upstream_exec_restores_the_independent_aof_database_cursor() {
    for selects_inside in [false, true] {
        let (mut backend, token, mut link, path) = strict_apply_fixture();
        // Local writable-replica traffic has selected DB 1 in the AOF only.
        backend.feed_aof(&protocol::encode_command(&[b"SELECT", b"1"]));
        backend.ctx.persistence_mut().set_aof_selected_db(1);
        apply_parts(&mut backend, token, &mut link, &[b"MULTI"]).unwrap();
        if selects_inside {
            backend
                .apply_replication_command(
                    token,
                    &mut link,
                    ReplicationApply::Select(0),
                    CommandOrigin::Replication,
                )
                .unwrap();
        }
        apply_parts(
            &mut backend,
            token,
            &mut link,
            &[b"SET", b"upstream", b"value"],
        )
        .unwrap();
        apply_parts(&mut backend, token, &mut link, &[b"EXEC"]).unwrap();
        backend
            .ctx
            .aof_mut()
            .journal
            .as_mut()
            .unwrap()
            .appender
            .force_fsync()
            .unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let mut decoder = crate::foundation::io::persistence::AofDecoder::new();
        let mut selected_db = 0;
        loop {
            match decoder.step(&bytes).unwrap() {
                crate::foundation::io::persistence::AofStep::Command(parts) => {
                    if parts[0] == b"SELECT" {
                        selected_db = std::str::from_utf8(&parts[1])
                            .unwrap()
                            .parse::<u32>()
                            .unwrap();
                    }
                    if parts[0] == b"SET" {
                        assert_eq!(selected_db, 0, "upstream transaction must replay in DB 0");
                    }
                }
                crate::foundation::io::persistence::AofStep::Finished(_) => break,
                other => panic!("unexpected AOF decode result: {other:?}"),
            }
        }
        assert_eq!(backend.ctx.persistence().aof_selected_db(), 0);
        drop(backend);
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn strict_upstream_exec_multiple_selects_preserve_the_final_aof_cursor_and_replay() {
    let (mut backend, token, mut link, path) = strict_apply_fixture();
    *backend.ctx.dbs_mut() = crate::foundation::io::store::kv_engine::DbSet::new(2);
    backend.feed_aof(&protocol::encode_command(&[b"SELECT", b"1"]));
    backend.feed_aof(&protocol::encode_command(&[
        b"SET",
        b"local",
        b"local-value",
    ]));
    backend.ctx.persistence_mut().set_aof_selected_db(1);
    apply_parts(&mut backend, token, &mut link, &[b"MULTI"]).unwrap();
    apply_parts(
        &mut backend,
        token,
        &mut link,
        &[b"SET", b"start", b"db-zero"],
    )
    .unwrap();
    for (db, key, value) in [
        (1, b"first-one".as_slice(), b"db-one".as_slice()),
        (0, b"middle-zero".as_slice(), b"db-zero".as_slice()),
        (1, b"last-one".as_slice(), b"db-one".as_slice()),
    ] {
        backend
            .apply_replication_command(
                token,
                &mut link,
                ReplicationApply::Select(db),
                CommandOrigin::Replication,
            )
            .unwrap();
        apply_parts(&mut backend, token, &mut link, &[b"SET", key, value]).unwrap();
    }
    apply_parts(&mut backend, token, &mut link, &[b"EXEC"]).unwrap();
    assert_eq!(link.selected_db(), 1);
    assert_eq!(backend.ctx.persistence().aof_selected_db(), 1);
    // The upstream is already in DB 1, so no new SELECT precedes this write.
    apply_parts(
        &mut backend,
        token,
        &mut link,
        &[b"SET", b"after-exec", b"db-one"],
    )
    .unwrap();
    assert_eq!(backend.ctx.persistence().aof_selected_db(), 1);
    backend
        .ctx
        .aof_mut()
        .journal
        .as_mut()
        .unwrap()
        .appender
        .force_fsync()
        .unwrap();
    let bytes = std::fs::read(&path).unwrap();
    let mut decoder = crate::foundation::io::persistence::AofDecoder::new();
    let mut replay = ServerWorkerState::new(2).unwrap();
    crate::framework::commands::register_all(&mut replay.registry);
    let replay_token = add_connection(&mut replay, 42);
    let mut replay_link = ReplicaLink::new(
        ConnectionId(replay_token),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let mut selected_db = 0;
    let mut writes = Vec::new();
    loop {
        match decoder.step(&bytes).unwrap() {
            crate::foundation::io::persistence::AofStep::Command(parts) => {
                if parts[0] == b"SELECT" {
                    selected_db = std::str::from_utf8(&parts[1])
                        .unwrap()
                        .parse::<u32>()
                        .unwrap();
                    replay
                        .apply_replication_command(
                            replay_token,
                            &mut replay_link,
                            ReplicationApply::Select(selected_db),
                            CommandOrigin::Replication,
                        )
                        .unwrap();
                } else {
                    if parts[0] == b"SET" {
                        writes.push((selected_db, parts[1].clone()));
                    }
                    let refs = parts.iter().map(Vec::as_slice).collect::<Vec<_>>();
                    apply_parts(&mut replay, replay_token, &mut replay_link, &refs).unwrap();
                }
            }
            crate::foundation::io::persistence::AofStep::Finished(_) => break,
            other => panic!("unexpected AOF decode result: {other:?}"),
        }
    }
    assert_eq!(selected_db, 1);
    assert_eq!(
        writes,
        [
            (1, b"local".to_vec()),
            (0, b"start".to_vec()),
            (1, b"first-one".to_vec()),
            (0, b"middle-zero".to_vec()),
            (1, b"last-one".to_vec()),
            (1, b"after-exec".to_vec()),
        ]
    );
    for (db, key, value) in [
        (1, b"local".as_slice(), b"local-value".as_slice()),
        (0, b"start".as_slice(), b"db-zero".as_slice()),
        (1, b"first-one".as_slice(), b"db-one".as_slice()),
        (0, b"middle-zero".as_slice(), b"db-zero".as_slice()),
        (1, b"last-one".as_slice(), b"db-one".as_slice()),
        (1, b"after-exec".as_slice(), b"db-one".as_slice()),
    ] {
        assert_eq!(
            replay
                .ctx
                .db(db)
                .unwrap()
                .get_no_expire(key)
                .unwrap()
                .as_bytes(),
            value
        );
        assert!(replay.ctx.db(1 - db).unwrap().get_no_expire(key).is_none());
        if key != b"local" {
            assert_eq!(
                backend
                    .ctx
                    .db(db)
                    .unwrap()
                    .get_no_expire(key)
                    .unwrap()
                    .as_bytes(),
                value
            );
        }
    }
    drop(backend);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn unlogged_upstream_write_invalidates_the_rewrite_snapshot_epoch() {
    let (mut backend, token, mut link, path) = strict_apply_fixture();
    backend.ctx.config_mut().replication.repl_after_aof_fsync = false;
    backend.ctx.config_mut().persistence.aof_enabled = false;
    backend
        .ctx
        .persistence_mut()
        .begin_aof_rewrite_child(4568, 0)
        .unwrap();
    let captured = backend.ctx.aof().rewrite.baseline_epoch;
    for _ in 0..2 {
        apply_parts(&mut backend, token, &mut link, &[b"SET", b"late", b"value"]).unwrap();
        assert!(backend.ctx.aof().baseline_epoch > captured);
    }
    assert_eq!(backend.ctx.aof().baseline_epoch, captured + 2);
    backend.ctx.persistence_mut().abort_child();
    drop(backend);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn strict_failed_install_cannot_recover_from_a_normal_old_journal_flush() {
    let (mut backend, _, _, path) = strict_apply_fixture();
    backend.ctx.config_mut().persistence.dir = path.parent().unwrap().to_str().unwrap().to_string();
    backend.ctx.config_mut().persistence.aof_filename =
        path.file_name().unwrap().to_str().unwrap().to_string();
    backend.ctx.config_mut().persistence.aof_appenddirname = String::new();
    let options = crate::foundation::io::persistence::aof_rewrite::AofRewriteFileOptions {
        dir: path.parent().unwrap().to_path_buf(),
        filename: path.file_name().unwrap().to_str().unwrap().to_string(),
        appenddirname: String::new(),
        use_rdb_preamble: false,
        timestamp_enabled: false,
        key_delay_us: 0,
        rdb_compression: true,
        incremental_fsync: true,
    };
    let pid = rand::random::<u16>() as i32 + 100000;
    backend
        .ctx
        .persistence_mut()
        .begin_aof_rewrite_child(pid, 0)
        .unwrap();
    crate::foundation::io::persistence::aof_rewrite::write_rewrite_temp(
        backend.ctx.dbs(),
        &[],
        &[],
        &options,
        pid,
    )
    .unwrap();
    crate::foundation::io::persistence::aof_rewrite::FAIL_NEXT_INSTALL_DIRECTORY_SYNC
        .with(|fail| fail.set(true));
    assert_eq!(
        crate::framework::commands::server::bgrewriteaof::apply_aof_rewrite_child_completion(
            &mut backend.ctx,
            pid,
            true
        ),
        Some(crate::framework::commands::server::bgrewriteaof::ReapResult::Error),
    );
    backend.flush_aof_if_needed();
    assert_eq!(
        backend.ctx.aof().replication_baseline,
        crate::engine::persistence::AofBaseline::Unavailable
    );
    assert_eq!(
        backend.ctx.aof().last_write_status,
        crate::engine::persistence::OperationStatus::Err
    );
    drop(backend);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn strict_upstream_data_and_exec_reject_local_durability_faults_before_mutation() {
    for fault in 0..5 {
        for is_exec in [false, true] {
            let (mut backend, token, mut link, path) = strict_apply_fixture();
            if is_exec {
                apply_parts(&mut backend, token, &mut link, &[b"MULTI"]).unwrap();
                apply_parts(&mut backend, token, &mut link, &[b"SET", b"key", b"value"]).unwrap();
            }
            match fault {
                0 => {
                    backend.ctx.aof_mut().replication_baseline =
                        crate::engine::persistence::AofBaseline::Unavailable
                }
                1 => {
                    backend.ctx.aof_mut().last_write_status =
                        crate::engine::persistence::OperationStatus::Err;
                }
                2 => {
                    backend
                        .ctx
                        .config_mut()
                        .replication
                        .replica_output_buffer_hard_limit = 1;
                    backend.ctx.config_mut().server.repl_backlog_size = 1;
                    let frame = protocol::encode_command(&[b"SET", b"pending", b"1"]);
                    backend.feed_aof(&frame);
                    backend.propagate_replication_frame(&frame);
                }
                3 => {
                    let appender = &backend.ctx.aof().journal.as_ref().unwrap().appender;
                    appender.sync_handle().complete_for_test(
                        appender.appended_stamp(),
                        Err(std::io::ErrorKind::StorageFull.into()),
                    );
                }
                _ => {
                    backend.ctx.aof_mut().journal.take();
                }
            }
            let parts = if is_exec {
                &[b"EXEC".as_slice()][..]
            } else {
                &[b"SET".as_slice(), b"key", b"value"][..]
            };
            assert!(
                apply_parts(&mut backend, token, &mut link, parts).is_err(),
                "fault={fault}, exec={is_exec}"
            );
            assert!(backend.ctx.db(0).unwrap().get_no_expire(b"key").is_none());
            drop(backend);
            std::fs::remove_file(path).unwrap();
        }
    }
}

#[test]
fn strict_upstream_exec_records_complete_transaction_before_local_blocked_pop() {
    use crate::engine::command::context::BlockedCmd;
    let (mut backend, token, mut link, path) = strict_apply_fixture();
    backend.ctx.config_mut().replication.replica_read_only = false;
    let blocked_token = add_connection(&mut backend, 42);
    let key = Bytes::from_static(b"list");
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"BLPOP"),
        key.clone(),
        Bytes::from_static(b"0"),
    ]);
    backend.connection_registry[crate::framework::capabilities::io::Token(blocked_token)]
        .enter_blocking(BlockedCmd::Blpop, vec![key.clone()], 0, &args, 0);
    let session = backend.connection_registry
        [crate::framework::capabilities::io::Token(blocked_token)]
    .session_id();
    backend
        .ctx
        .blocking_registry_mut()
        .register_client(0, &[key], session);
    for parts in [
        &[b"MULTI".as_slice()][..],
        &[b"RPUSH".as_slice(), b"list", b"first"][..],
        &[b"RPUSH".as_slice(), b"list", b"second"][..],
        &[b"EXEC".as_slice()][..],
    ] {
        apply_parts(&mut backend, token, &mut link, parts).unwrap();
    }
    backend
        .ctx
        .aof_mut()
        .journal
        .as_mut()
        .unwrap()
        .appender
        .force_fsync()
        .unwrap();
    let bytes = std::fs::read(&path).unwrap();
    let mut player = crate::foundation::io::persistence::AofDecoder::new();
    let mut commands = Vec::new();
    loop {
        match player.step(&bytes) {
            Ok(crate::foundation::io::persistence::AofStep::Command(parts)) => commands.push(parts),
            Ok(crate::foundation::io::persistence::AofStep::Finished(_)) => break,
            other => panic!("unexpected AOF decode result: {other:?}"),
        }
    }
    let names = commands
        .iter()
        .map(|parts| parts[0].as_slice())
        .collect::<Vec<_>>();
    let multi = names.iter().position(|name| *name == b"MULTI").unwrap();
    let exec = names.iter().position(|name| *name == b"EXEC").unwrap();
    let pop = names.iter().position(|name| *name == b"LPOP").unwrap();
    assert_eq!(
        &names[multi..=exec],
        &[b"MULTI".as_slice(), b"RPUSH", b"RPUSH", b"EXEC"]
    );
    assert!(
        pop > exec,
        "blocked pop must follow the complete upstream transaction"
    );
    assert!(
        backend.connection_registry[crate::framework::capabilities::io::Token(blocked_token)]
            .blocked
            .is_none()
    );
    drop(backend);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn failed_sync_pins_backlog_against_idle_expiry_until_release() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().replication.repl_backlog_ttl = 1;
    backend.ctx.config_mut().replication.repl_after_aof_fsync = true;
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.ctx.config_mut().persistence.aof_fsync = AofFsync::No;
    let path = std::env::temp_dir().join(format!(
        "versal-backlog-expiry-{}.aof",
        rand::random::<u64>()
    ));
    backend.ctx.aof_mut().journal = Some(
        crate::foundation::io::persistence::journal::AofJournal::new(
            AofAppender::open_append_path(&path, AofFsync::No).unwrap(),
        ),
    );
    backend.ctx.aof_mut().replication_baseline = crate::engine::persistence::AofBaseline::Ready;
    backend.ctx.replication_mut().backlog.activate();
    let frame = protocol::encode_command(&[b"SET", b"pending", b"value"]);
    backend.feed_aof(&frame);
    backend.flush_aof_if_needed();
    backend.propagate_replication_frame(&frame);
    let appender = &backend.ctx.aof().journal.as_ref().unwrap().appender;
    let stamp = appender.appended_stamp();
    let sync = appender.sync_handle();
    sync.complete_for_test(stamp, Err(std::io::ErrorKind::StorageFull.into()));
    backend.ctx.replication_mut().backlog_idle_deadline_ms = 1;
    backend.expire_idle_replication_backlog();
    assert!(backend.ctx.replication().backlog.is_active());
    assert_eq!(backend.ctx.replication().backlog.history_len(), frame.len());
    assert_eq!(backend.ctx.replication().backlog_idle_deadline_ms, 0);
    backend.ctx.replication_mut().backlog.expire_history();
    assert!(backend.ctx.replication().backlog.is_active());
    sync.complete_for_test(stamp, Ok(()));
    backend.release_synced_replication();
    backend.expire_idle_replication_backlog();
    assert!(backend.ctx.replication().backlog_idle_deadline_ms > 1);
    backend.ctx.replication_mut().backlog_idle_deadline_ms = 1;
    backend.expire_idle_replication_backlog();
    assert!(!backend.ctx.replication().backlog.is_active());
    assert_eq!(backend.ctx.replication().backlog.unreleased_bytes(), 0);
    assert!(!backend
        .ctx
        .replication_mut()
        .backlog
        .release_durable_replication());
    drop(backend);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn deferred_snapshot_does_not_expose_an_incomplete_aof_baseline() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token = add_connection(&mut backend, 41);
    let id = backend.replication_runtime.register_primary(
        ConnectionId(token),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 0,
            capability: vec![],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend.ctx.config_mut().replication.repl_after_aof_fsync = true;
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.fire_deferred_full_resyncs(vec![id]);
    assert!(backend.replication_runtime.primary_link(id).is_none());
    assert!(!backend.ctx.persistence().has_active_child());
}

#[test]
fn cascading_transaction_waits_for_the_local_complete_exec_stamp() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let token = add_connection(&mut backend, 41);
    backend.ctx.replication_mut().backlog.activate();
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.ctx.config_mut().persistence.aof_fsync = AofFsync::No;
    backend.ctx.config_mut().replication.repl_after_aof_fsync = true;
    let path = std::env::temp_dir().join(format!(
        "versal-cascade-durability-{}-{}.aof",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    backend.ctx.aof_mut().journal = Some(
        crate::foundation::io::persistence::journal::AofJournal::new(
            AofAppender::open_append_path(&path, AofFsync::No).unwrap(),
        ),
    );
    backend.ctx.aof_mut().replication_baseline = crate::engine::persistence::AofBaseline::Ready;
    let mut link = ReplicaLink::new(
        ConnectionId(token),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    for parts in [
        &[b"MULTI".as_slice()][..],
        &[b"SET".as_slice(), b"key", b"value"][..],
    ] {
        backend
            .apply_replication_command(
                token,
                &mut link,
                ReplicationApply::Command(CommandArgs::from_parts(
                    parts
                        .iter()
                        .map(|part| Bytes::copy_from_slice(part))
                        .collect(),
                )),
                CommandOrigin::Replication,
            )
            .unwrap();
    }
    assert_eq!(backend.ctx.replication().backlog.history_len(), 0);
    assert_eq!(
        backend
            .ctx
            .aof()
            .journal
            .as_ref()
            .unwrap()
            .appender
            .current_size(),
        0
    );
    backend
        .apply_replication_command(
            token,
            &mut link,
            ReplicationApply::Command(CommandArgs::from_parts(vec![Bytes::from_static(b"EXEC")])),
            CommandOrigin::Replication,
        )
        .unwrap();
    let expected = [
        protocol::encode_command(&[b"MULTI"]),
        protocol::encode_command(&[b"SET", b"key", b"value"]),
        protocol::encode_command(&[b"EXEC"]),
    ]
    .concat();
    let start = crate::engine::distributed::replication::BacklogCursor::ZERO;
    assert_eq!(
        backend.ctx.replication().backlog.read_released_from(start),
        Some(Vec::new())
    );
    assert_eq!(
        backend.ctx.replication().backlog.read_from(start),
        Some(expected.clone())
    );
    let appender = &backend.ctx.aof().journal.as_ref().unwrap().appender;
    appender
        .sync_handle()
        .complete_for_test(appender.appended_stamp(), Ok(()));
    backend.release_synced_replication();
    assert_eq!(
        backend.ctx.replication().backlog.read_released_from(start),
        Some(expected)
    );
    drop(backend);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn downstream_socket_only_receives_successfully_synced_complete_prefixes() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let (token, mut reader) = add_runtime_connection(&mut backend);
    reader.set_nonblocking(true).unwrap();
    let id = backend.replication_runtime.register_primary(
        ConnectionId(token),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 0,
            capability: vec![],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(id)
        .unwrap()
        .set_state(PrimaryLinkState::Streaming);
    backend.ctx.replication_mut().backlog.activate();
    let dir = std::env::temp_dir().join(format!(
        "versal-replication-durability-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let appender = AofAppender::open_append_path(&dir.join("test.aof"), AofFsync::No).unwrap();
    // Disable automatic synchronization to inject exact completion snapshots.
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.ctx.config_mut().persistence.aof_fsync = AofFsync::No;
    backend.ctx.config_mut().replication.repl_after_aof_fsync = true;
    backend.ctx.config_mut().server.repl_backlog_size = 1;
    backend.ctx.aof_mut().journal =
        Some(crate::foundation::io::persistence::journal::AofJournal::new(appender));
    backend.ctx.aof_mut().replication_baseline = crate::engine::persistence::AofBaseline::Ready;
    let first = protocol::encode_command(&[b"SET", b"first", b"1"]);
    let second = protocol::encode_command(&[b"SET", b"second", b"2"]);
    backend.feed_aof(&first);
    backend.propagate_to_replicas(&first);
    backend.flush_aof_if_needed();
    let appender = &backend.ctx.aof().journal.as_ref().unwrap().appender;
    let first_stamp = appender.appended_stamp();
    let sync = appender.sync_handle();
    backend.feed_aof(&second);
    backend.propagate_to_replicas(&second);
    backend.flush_aof_if_needed();
    let second_stamp = backend
        .ctx
        .aof()
        .journal
        .as_ref()
        .unwrap()
        .appender
        .appended_stamp();

    backend.handle_writable(Token(token));
    backend.flush_connection_output(token);
    let mut bytes = [0u8; 4096];
    assert_eq!(
        reader.read(&mut bytes).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(
        backend.ctx.replication().backlog.history_len(),
        first.len() + second.len()
    );

    sync.complete_for_test(first_stamp, Ok(()));
    assert!(sync.is_durable(first_stamp));
    backend.release_synced_replication();
    assert_eq!(
        backend
            .ctx
            .replication()
            .backlog
            .replication_release_cursor()
            .get(),
        first.len() as i64
    );
    assert_eq!(_queued(&backend.connection_registry[Token(token)]), first);
    let result = backend.flush_connection_output(token);
    assert!(
        matches!(
            result,
            crate::foundation::io::transport::WriteOutcome::Complete
        ),
        "{result:?}"
    );
    reader.set_nonblocking(false).unwrap();
    reader
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .unwrap();
    reader.read_exact(&mut bytes[..first.len()]).unwrap();
    reader.set_nonblocking(true).unwrap();
    assert_eq!(&bytes[..first.len()], first.as_slice());
    assert_eq!(
        reader.read(&mut bytes).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );

    sync.complete_for_test(
        second_stamp,
        Err(std::io::Error::other("injected fsync failure")),
    );
    backend.release_synced_replication();
    backend.handle_writable(Token(token));
    assert_eq!(
        reader.read(&mut bytes).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );

    sync.complete_for_test(second_stamp, Ok(()));
    backend.release_synced_replication();
    backend.flush_connection_output(token);
    reader.set_nonblocking(false).unwrap();
    reader.read_exact(&mut bytes[..second.len()]).unwrap();
    reader.set_nonblocking(true).unwrap();
    assert_eq!(&bytes[..second.len()], second.as_slice());
    backend.release_synced_replication();
    backend.flush_connection_output(token);
    assert_eq!(
        reader.read(&mut bytes).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    drop(backend);
    std::fs::remove_dir_all(dir).unwrap();
}
