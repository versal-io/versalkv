use super::*;
use crate::foundation::io::persistence::rdb::compression::{RdbCompression, RdbSnapshotOptions};
use crate::foundation::io::persistence::StreamStep;
use crate::foundation::io::store::kv_engine::DbSet;
use crate::foundation::io::store::Robj;

fn snapshot(compression: RdbCompression) -> Vec<u8> {
    let mut source = DbSet::new(1);
    source
        .get_mut(0)
        .unwrap()
        .set(b"key".to_vec(), Robj::string("value"));
    crate::foundation::io::persistence::rdb::serialize::serialize_replication_snapshot(
        &source,
        0,
        &[],
        true,
        None,
        &[],
        RdbSnapshotOptions {
            compression,
            checksum: true,
        },
    )
}

#[test]
fn codec_fix_aof_preamble_freezes_server_checksum_policy() {
    for compression in [RdbCompression::No, RdbCompression::Lz4] {
        let mut bytes = snapshot(compression);
        *bytes.last_mut().unwrap() ^= 1;
        for (checksum, debug_skip) in [(true, false), (false, false), (true, true)] {
            let mut backend = ServerWorkerState::new(1).unwrap();
            backend.ctx.config_mut().persistence.rdbchecksum = checksum;
            backend.ctx.config_mut().server.skip_checksum_validation = debug_skip;
            let mut connection = Connection::new_fake();
            let result = backend.run_aof_file_task(bytes.clone(), &mut connection, None);
            assert_eq!(
                result.is_ok(),
                !checksum || debug_skip,
                "{compression:?}, checksum={checksum}, debug_skip={debug_skip}"
            );
            if result.is_ok() {
                assert_eq!(
                    backend
                        .ctx
                        .db_mut(0)
                        .unwrap()
                        .get(b"key")
                        .unwrap()
                        .as_bytes(),
                    b"value"
                );
            }
        }
    }
}

fn receiving_link(backend: &ServerWorkerState, bytes: &[u8]) -> ReplicaLink {
    let mut link = ReplicaLink::new(
        ConnectionId(3),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    backend.pin_replica_rdb_load_mode(&mut link);
    link.connected(None, None);
    let mut input = format!(
        "+PONG\r\n+OK\r\n+OK\r\n+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0\r\n${}\r\n",
        bytes.len()
    )
    .into_bytes();
    input.extend_from_slice(bytes);
    link.on_bytes(&input, 6379).unwrap();
    link
}

#[test]
fn codec_fix_receive_checksum_is_independent_of_transcoded_output() {
    for input_compression in [RdbCompression::No, RdbCompression::Lz4] {
        let mut bytes = snapshot(input_compression);
        *bytes.last_mut().unwrap() ^= 1;
        for output_compression in [RdbCompression::No, RdbCompression::Lz4] {
            for checksum in [true, false] {
                let mut backend = ServerWorkerState::new(1).unwrap();
                backend.ctx.config_mut().persistence.rdbchecksum = checksum;
                backend.ctx.config_mut().persistence.rdbcompression = output_compression;
                let mut link = receiving_link(&backend, &bytes);
                // Changing configuration after the header cannot alter this load.
                backend.ctx.config_mut().persistence.rdbchecksum = !checksum;
                backend.pin_replica_rdb_load_mode(&mut link);
                let receive = link.rdb_receive_mut().unwrap();
                receive.set_direct();
                let result = receive.drive(backend.ctx.dbs_mut(), 0);
                assert_eq!(
                    result.is_ok(), !checksum,
                    "input={input_compression:?}, output={output_compression:?}, checksum={checksum}"
                );
                if !checksum {
                    assert_eq!(result.unwrap(), StreamStep::Finished);
                    receive.validate_complete().unwrap();
                    let raw = link.take_rdb_receive().unwrap().finish().raw.unwrap();
                    if input_compression == output_compression {
                        assert_eq!(raw, bytes);
                    } else {
                        assert_eq!(
                            raw.starts_with(b"VCS"),
                            output_compression == RdbCompression::Lz4
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn codec_fix_partial_resync_does_not_require_disk_compression_capability() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().persistence.rdbcompression = RdbCompression::Lz4;
    let (token, _) = setup_primary(&mut backend, 41, vec!["psync2".to_string()], false, false);
    backend.ctx.replication_mut().backlog.activate();
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"history")
        .unwrap();
    let backlog = &backend.ctx.replication().backlog;
    let request = backlog.first_byte_reported_offset();
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"PSYNC"),
        Bytes::copy_from_slice(backlog.replid().as_bytes()),
        Bytes::from(request.get().to_string()),
    ]);
    let mut replies = Vec::new();
    backend.try_handle_transport_cmd(
        token,
        41,
        &args,
        "psync",
        false,
        &mut replies,
        &mut Vec::new(),
    );
    let mut wire = _queued(&backend.connection_registry[Token(token)]);
    wire.extend(replies.concat());
    assert!(
        wire.starts_with(b"+CONTINUE\r\n"),
        "a valid backlog must not require the disk RDB codec: {wire:?}"
    );

    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().persistence.rdbcompression = RdbCompression::Lz4;
    let (token, _) = setup_primary(&mut backend, 41, vec!["psync2".to_string()], false, false);
    let replies = run_psync(&mut backend, token, 41, b"PSYNC");
    assert!(replies.iter().any(|reply| reply.starts_with(b"-ERR")));
    assert_eq!(backend.ctx.stats().sync_full, 0);
    assert!(backend.full_sync.pending_dual_rdb_snapshots.is_empty());
}

fn assert_fatal_child(test_name: &str, marker: &str) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([test_name, "--exact", "--nocapture"])
        .env(marker, "1")
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "expected fatal compressed RDB error, output: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn codec_fix_complete_truncated_frame_is_fatal_without_socket_eof() {
    const MARKER: &str = "VERSAL_CODEC_FIX_TRUNCATED_CHILD";
    if std::env::var_os(MARKER).is_none() {
        assert_fatal_child(
            concat!(
                "runtime::server::worker::replication::tests::codec_fix_tests::",
                "codec_fix_complete_truncated_frame_is_fatal_without_socket_eof"
            ),
            MARKER,
        );
        return;
    }
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().persistence.rdbcompression = RdbCompression::Lz4;
    let mut bytes = snapshot(RdbCompression::Lz4);
    bytes.truncate(bytes.len() - 8);
    let mut link = receiving_link(&backend, &bytes);
    add_connection(&mut backend, 3);
    assert!(!link.is_receiving_rdb());
    let _ = backend.drive_streaming_decode(3, 3, &mut link);
}

#[test]
fn codec_fix_extra_logical_data_is_a_fatal_compression_error() {
    const MARKER: &str = "VERSAL_CODEC_FIX_EXTRA_CHILD";
    if std::env::var_os(MARKER).is_none() {
        assert_fatal_child(
            concat!(
                "runtime::server::worker::replication::tests::codec_fix_tests::",
                "codec_fix_extra_logical_data_is_a_fatal_compression_error"
            ),
            MARKER,
        );
        return;
    }
    let mut logical = snapshot(RdbCompression::No);
    logical.extend_from_slice(b"X");
    let bytes =
        crate::foundation::io::persistence::rdb::compression::encode_rdb_stream(&logical, true)
            .unwrap();
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().replication.repl_diskless_load = "swapdb".to_string();
    let mut link = receiving_link(&backend, &bytes);
    add_connection(&mut backend, 3);
    let _ = backend.drive_streaming_decode(3, 3, &mut link);
}

#[test]
fn codec_fix_filtered_cohorts_progress_while_all_snapshot_child_is_active() {
    use crate::framework::distributed::replication::RdbFilterMode;

    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().replication.repl_diskless_sync = true;
    let mut targets = Vec::new();
    for (token, mode) in [
        (41, RdbFilterMode::All),
        (42, RdbFilterMode::Functions),
        (43, RdbFilterMode::Empty),
    ] {
        add_connection(&mut backend, token);
        let id = backend.replication_runtime.register_primary(
            ConnectionId(token),
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: vec!["eof".to_string(), "psync2".to_string()],
                rdb_filter_mode: mode,
            },
        );
        let link = backend.replication_runtime.primary_link_mut(id).unwrap();
        link.plan_full_resync(&backend.ctx.replication().backlog);
        link.defer_diskless_sync(0, 0, true);
        targets.push((id, token, mode));
    }
    backend
        .ctx
        .persistence_mut()
        .begin_diskless_child(424242)
        .unwrap();
    backend.drive_deferred_full_resyncs();
    backend.drive_deferred_full_resyncs();
    for (id, token, mode) in targets {
        let is_deferred = backend
            .replication_runtime
            .primary_link(id)
            .unwrap()
            .is_deferred_diskless();
        assert_eq!(is_deferred, mode == RdbFilterMode::All, "{mode:?}");
        if mode != RdbFilterMode::All {
            assert!(_queued(&backend.connection_registry[Token(token)]).starts_with(b"+FULLRESYNC"));
        }
    }
}

fn final_edges_transport(
    backend: &mut ServerWorkerState,
    token: usize,
    parts: Vec<Bytes>,
) -> Vec<Vec<u8>> {
    let args = CommandArgs::from_parts(parts);
    let name = std::str::from_utf8(&args.name)
        .unwrap()
        .to_ascii_lowercase();
    let mut replies = Vec::new();
    backend.try_handle_transport_cmd(
        token,
        token,
        &args,
        &name,
        false,
        &mut replies,
        &mut Vec::new(),
    );
    replies
}

#[test]
fn final_edges_filtered_without_eof_rejects_continue_before_admission() {
    for filter in [b"functions".as_slice(), b"".as_slice()] {
        for compression in [RdbCompression::No, RdbCompression::Lz4] {
            for is_full in [false, true] {
                let mut backend = ServerWorkerState::new(1).unwrap();
                backend.ctx.config_mut().persistence.rdbcompression = compression;
                let (token, _peer) = add_runtime_connection(&mut backend);
                assert_eq!(
                    final_edges_transport(
                        &mut backend,
                        token,
                        vec![
                            Bytes::from_static(b"REPLCONF"),
                            Bytes::from_static(b"capa"),
                            Bytes::from_static(b"psync2"),
                            Bytes::from_static(b"rdb-filter-only"),
                            Bytes::copy_from_slice(filter),
                        ],
                    ),
                    vec![b"+OK\r\n".to_vec()]
                );
                backend.ctx.replication_mut().backlog.activate();
                backend
                    .ctx
                    .replication_mut()
                    .backlog
                    .append(b"history")
                    .unwrap();
                let backlog = &backend.ctx.replication().backlog;
                let replid = if is_full { "?" } else { backlog.replid() }.to_string();
                let offset = if is_full {
                    "-1".to_string()
                } else {
                    backlog.first_byte_reported_offset().get().to_string()
                };
                let boundary = backlog.snapshot_boundary();
                let replies = final_edges_transport(
                    &mut backend,
                    token,
                    vec![
                        Bytes::from_static(b"PSYNC"),
                        Bytes::from(replid),
                        Bytes::from(offset),
                    ],
                );
                assert_eq!(
                    replies,
                    vec![b"-ERR Filtered replica requires EOF capability\r\n".to_vec()],
                    "filter={filter:?}, compression={compression:?}, full={is_full}"
                );
                assert_eq!(backend.replication_runtime.role(ConnectionId(token)), None);
                assert!(backend.replication_runtime.primary_links().is_empty());
                assert!(_queued(&backend.connection_registry[Token(token)]).is_empty());
                assert!(backend.full_sync.pending_dual_rdb_snapshots.is_empty());
                assert_eq!(
                    backend.ctx.replication().backlog.snapshot_boundary(),
                    boundary
                );
                assert_eq!(
                    (
                        backend.ctx.stats().sync_full,
                        backend.ctx.stats().sync_partial_ok
                    ),
                    (0, 0)
                );
            }
        }
    }
}

#[test]
fn final_edges_filtered_eof_priority_precedes_invalid_offset() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let (token, _peer) = add_runtime_connection(&mut backend);
    final_edges_transport(
        &mut backend,
        token,
        vec![
            Bytes::from_static(b"REPLCONF"),
            Bytes::from_static(b"rdb-filter-only"),
            Bytes::from_static(b"functions"),
        ],
    );
    let replies = final_edges_transport(
        &mut backend,
        token,
        vec![
            Bytes::from_static(b"PSYNC"),
            Bytes::from_static(b"?"),
            Bytes::from_static(b"invalid"),
        ],
    );
    assert_eq!(
        replies,
        vec![b"-ERR Filtered replica requires EOF capability\r\n".to_vec()]
    );
    assert_eq!(backend.replication_runtime.role(ConnectionId(token)), None);
}

#[test]
fn final_edges_filtered_full_sync_eof_error_precedes_disk_capability_error() {
    for filter in [b"functions".as_slice(), b"".as_slice()] {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().persistence.rdbcompression = RdbCompression::Lz4;
        let (token, _peer) = add_runtime_connection(&mut backend);
        let replies = final_edges_transport(
            &mut backend,
            token,
            vec![
                Bytes::from_static(b"REPLCONF"),
                Bytes::from_static(b"rdb-filter-only"),
                Bytes::copy_from_slice(filter),
            ],
        );
        assert_eq!(replies, vec![b"+OK\r\n".to_vec()]);
        assert_eq!(
            run_psync(&mut backend, token, token, b"PSYNC"),
            vec![b"-ERR Filtered replica requires EOF capability\r\n".to_vec()]
        );
        assert_eq!(backend.replication_runtime.role(ConnectionId(token)), None);
        assert!(backend.replication_runtime.primary_links().is_empty());
        assert_eq!(backend.ctx.stats().sync_full, 0);
    }
}

#[test]
fn final_edges_rdb_channel_keeps_eof_and_channel_error_priority() {
    for supports_eof in [false, true] {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().persistence.rdbcompression = RdbCompression::Lz4;
        let (token, _peer) = add_runtime_connection(&mut backend);
        let mut parts = vec![Bytes::from_static(b"REPLCONF")];
        if supports_eof {
            parts.extend([Bytes::from_static(b"capa"), Bytes::from_static(b"eof")]);
        }
        parts.extend([
            Bytes::from_static(b"rdb-only"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"rdb-channel"),
            Bytes::from_static(b"1"),
        ]);
        assert_eq!(
            final_edges_transport(&mut backend, token, parts),
            vec![b"+OK\r\n".to_vec()]
        );
        let replies = run_psync(&mut backend, token, token, b"PSYNC");
        let expected = if supports_eof {
            b"-ERR RDB channel cannot issue SYNC or PSYNC\r\n".as_slice()
        } else {
            b"-ERR Filtered replica requires EOF capability\r\n".as_slice()
        };
        assert_eq!(replies, vec![expected.to_vec()]);
        assert_eq!(backend.ctx.stats().sync_full, 0);
        assert!(backend.full_sync.pending_dual_rdb_snapshots.is_empty());
    }
}

fn final_edges_assert_filtered_body(wire: &[u8], expected_functions: Vec<Vec<u8>>) {
    let header_end = wire.windows(2).position(|bytes| bytes == b"\r\n").unwrap();
    assert!(wire.starts_with(b"+FULLRESYNC "));
    let bulk = &wire[header_end + 2..];
    let bulk_header_end = bulk.windows(2).position(|bytes| bytes == b"\r\n").unwrap();
    let marker = bulk[..bulk_header_end].strip_prefix(b"$EOF:").unwrap();
    assert_eq!(marker.len(), 40);
    assert!(bulk.ends_with(marker));
    let body = &bulk[bulk_header_end + 2..bulk.len() - marker.len()];
    let mut dbs = DbSet::new(1);
    let (loaded, consumed, expired, functions) =
        crate::foundation::io::persistence::rdb::load_rdb_bytes_checked_with_functions(
            body, &mut dbs, true,
        )
        .unwrap();
    assert_eq!((loaded, consumed, expired), (0, body.len(), 0));
    assert_eq!(dbs.get(0).unwrap().dbsize(), 0);
    assert_eq!(functions, expected_functions);
}

#[test]
fn final_edges_max_replica_threshold_preserves_all_defer_during_active_child() {
    use crate::framework::distributed::replication::RdbFilterMode;

    let code = "#!lua name=final_edges\nserver.register_function('edge', function() return 1 end)";
    for compression in [RdbCompression::No, RdbCompression::Lz4] {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().persistence.rdbcompression = compression;
        backend.ctx.config_mut().replication.repl_diskless_sync = true;
        backend
            .ctx
            .config_mut()
            .replication
            .repl_diskless_sync_delay = 30;
        backend
            .ctx
            .config_mut()
            .replication
            .repl_diskless_sync_max_replicas = 2;
        backend
            .ctx
            .db_mut(0)
            .unwrap()
            .set(b"key".to_vec(), Robj::string("value"));
        backend
            .ctx
            .script_mut()
            .function_registry
            .load_library("final_edges", code)
            .unwrap();
        backend
            .ctx
            .persistence_mut()
            .begin_diskless_child(424242)
            .unwrap();
        let mut peers = Vec::new();
        let mut all_connections = Vec::new();
        for (mode, filter) in [
            (RdbFilterMode::All, None),
            (RdbFilterMode::Functions, Some(b"functions".as_slice())),
            (RdbFilterMode::Empty, Some(b"".as_slice())),
        ] {
            let mut tokens = Vec::new();
            for _ in 0..2 {
                let (token, peer) = add_runtime_connection(&mut backend);
                peers.push(peer);
                let mut parts = vec![
                    Bytes::from_static(b"REPLCONF"),
                    Bytes::from_static(b"capa"),
                    Bytes::from_static(b"eof"),
                    Bytes::from_static(b"capa"),
                    Bytes::from_static(b"psync2"),
                ];
                if let Some(filter) = filter {
                    parts.extend([
                        Bytes::from_static(b"rdb-filter-only"),
                        Bytes::copy_from_slice(filter),
                    ]);
                }
                assert_eq!(
                    final_edges_transport(&mut backend, token, parts),
                    vec![b"+OK\r\n".to_vec()]
                );
                assert!(run_psync(&mut backend, token, token, b"PSYNC").is_empty());
                tokens.push(token);
            }
            for token in tokens {
                let link = backend
                    .replication_runtime
                    .primary_for_connection(ConnectionId(token))
                    .unwrap();
                assert_eq!(
                    link.is_deferred_diskless(),
                    mode == RdbFilterMode::All,
                    "{mode:?}"
                );
                let wire = _queued(&backend.connection_registry[Token(token)]);
                if mode == RdbFilterMode::All {
                    assert!(
                        wire.is_empty(),
                        "All must not serialize while a child is active"
                    );
                    assert!(!link.diskless_delay_ready(crate::foundation::io::sys::time::mstime()));
                    all_connections.push(token);
                } else {
                    let functions = if mode == RdbFilterMode::Functions {
                        vec![code.as_bytes().to_vec()]
                    } else {
                        Vec::new()
                    };
                    final_edges_assert_filtered_body(&wire, functions);
                }
            }
            assert_eq!(backend.ctx.persistence().diskless_child_pid(), Some(424242));
            assert_eq!(backend.ctx.stats().total_forks, 0);
            assert!(backend.full_sync.disk_sync_batch.is_none());
        }
        for token in all_connections {
            assert!(backend
                .replication_runtime
                .primary_for_connection(ConnectionId(token))
                .unwrap()
                .is_deferred_diskless());
            assert!(_queued(&backend.connection_registry[Token(token)]).is_empty());
        }
    }
}

#[test]
fn final_edges_shared_fire_filters_active_child_before_cohort_selection() {
    use crate::framework::distributed::replication::RdbFilterMode;

    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().replication.repl_diskless_sync = true;
    backend
        .ctx
        .persistence_mut()
        .begin_diskless_child(424242)
        .unwrap();
    let mut targets = Vec::new();
    for (token, mode) in [
        (51, RdbFilterMode::All),
        (52, RdbFilterMode::Functions),
        (53, RdbFilterMode::Empty),
    ] {
        add_connection(&mut backend, token);
        let id = backend.replication_runtime.register_primary(
            ConnectionId(token),
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: vec!["eof".to_string(), "psync2".to_string()],
                rdb_filter_mode: mode,
            },
        );
        let link = backend.replication_runtime.primary_link_mut(id).unwrap();
        link.plan_full_resync(&backend.ctx.replication().backlog);
        link.defer_diskless_sync(30_000, 0, true);
        targets.push((id, token, mode));
    }
    for mode in [RdbFilterMode::Functions, RdbFilterMode::Empty] {
        let ids = targets
            .iter()
            .filter(|(_, _, filter)| *filter == RdbFilterMode::All || *filter == mode)
            .map(|(id, _, _)| *id)
            .collect();
        backend.fire_deferred_full_resyncs(ids);
    }
    for (id, token, mode) in targets {
        assert_eq!(
            backend
                .replication_runtime
                .primary_link(id)
                .unwrap()
                .is_deferred_diskless(),
            mode == RdbFilterMode::All
        );
        let wire = _queued(&backend.connection_registry[Token(token)]);
        if mode == RdbFilterMode::All {
            assert!(wire.is_empty());
        } else {
            final_edges_assert_filtered_body(&wire, Vec::new());
        }
    }
    assert_eq!(backend.ctx.persistence().diskless_child_pid(), Some(424242));
    assert_eq!(backend.ctx.stats().total_forks, 0);
}
