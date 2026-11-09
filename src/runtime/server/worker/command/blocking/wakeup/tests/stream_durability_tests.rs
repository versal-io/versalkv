use super::*;
use crate::engine::distributed::replication::ReplicationOffset;
use crate::foundation::io::persistence::{AofAppender, AofFsync};
use crate::foundation::io::store::structure::stream::StreamID;
use crate::runtime::server::worker::command::dispatch::ReadBatchEffects;

struct AofFile(std::path::PathBuf);

impl Drop for AofFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn backend_with_reader() -> (ServerWorkerState, Token) {
    let mut backend = ServerWorkerState::new(2).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend
        .ctx
        .db_mut(1)
        .unwrap()
        .get_or_create_stream(b"s")
        .unwrap()
        .create_group(Bytes::from_static(b"g"), StreamID::zero(), None)
        .unwrap();
    let reader = add_connection(&mut backend);
    backend.connection_registry[reader].selected_db = 1;
    (backend, reader)
}

fn add_connection(backend: &mut ServerWorkerState) -> Token {
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(Token(42 + backend.connection_registry.len()));
    connection.set_custom_reply_enabled(false);
    connection.set_skip_propagation(false);
    Token(backend.connection_registry.insert(connection))
}

fn args(parts: &[&[u8]]) -> CommandArgs {
    CommandArgs::from_parts(
        parts
            .iter()
            .map(|part| Bytes::copy_from_slice(part))
            .collect(),
    )
}

fn dispatch(
    backend: &mut ServerWorkerState,
    token: Token,
    parts: &[&[u8]],
    batch: &mut ReadBatchEffects,
) {
    let args = args(parts);
    let name = String::from_utf8_lossy(&args.name).to_ascii_lowercase();
    backend.process_parsed_command(token, args, name, true, batch);
}

fn block_reader(backend: &mut ServerWorkerState, reader: Token, noack: bool) -> ReadBatchEffects {
    let mut parts = vec![
        b"XREADGROUP".as_slice(),
        b"GROUP",
        b"g",
        b"c",
        b"BLOCK",
        b"1",
    ];
    if noack {
        parts.push(b"NOACK");
    }
    parts.extend([b"STREAMS".as_slice(), b"s", b">"]);
    let mut batch = ReadBatchEffects::default();
    dispatch(backend, reader, &parts, &mut batch);
    assert!(matches!(
        backend.connection_registry[reader]
            .blocked
            .as_ref()
            .map(|state| &state.cmd),
        Some(BlockedCmd::Xreadgroup)
    ));
    batch
}

fn install_aof(backend: &mut ServerWorkerState, reader: Token) -> AofFile {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "versal-stream-durability-{}-{nonce}.aof",
        std::process::id()
    ));
    let appender = AofAppender::open_append_path(&path, AofFsync::No).unwrap();
    backend.ctx.config_mut().persistence.aof_enabled = true;
    // Keep disk writes deterministic; WAITAOF observes the everysec policy.
    backend.ctx.config_mut().persistence.aof_fsync = AofFsync::EverySec;
    backend.ctx.aof_mut().journal =
        Some(crate::foundation::io::persistence::journal::AofJournal::new(appender));
    assert!(backend
        .ctx
        .db_mut(1)
        .unwrap()
        .get_stream_mut(b"s")
        .unwrap()
        .unwrap()
        .destroy_group(b"g"));
    let mut batch = ReadBatchEffects::default();
    dispatch(
        backend,
        reader,
        &[b"XGROUP", b"CREATE", b"s", b"g", b"0", b"MKSTREAM"],
        &mut batch,
    );
    finish_without_fsync(backend, reader, batch);
    sync_aof(backend);
    assert_eq!(backend.ctx.aof().fsynced_repl_offset, Some(1));
    backend.connection_registry[reader].clear_output_queue();
    AofFile(path)
}

fn sync_aof(backend: &mut ServerWorkerState) {
    backend.ctx.config_mut().persistence.aof_fsync = AofFsync::Always;
    backend.flush_aof_if_needed();
    let appender = &mut backend.ctx.aof_mut().journal.as_mut().unwrap().appender;
    appender.flush(true).unwrap();
    let durable = appender.fsynced_reported_offset().unwrap();
    backend.ctx.aof_mut().fsynced_repl_offset = Some(durable);
    backend.ctx.config_mut().persistence.aof_fsync = AofFsync::EverySec;
    backend
        .ctx
        .aof_mut()
        .journal
        .as_mut()
        .unwrap()
        .appender
        .set_fsync_mode(AofFsync::No)
        .unwrap();
}

fn finish_without_fsync(backend: &mut ServerWorkerState, reader: Token, batch: ReadBatchEffects) {
    backend.ctx.config_mut().persistence.aof_fsync = AofFsync::No;
    backend.complete_client_read(reader.0, reader.0, batch);
    backend.flush_aof_if_needed();
    backend.ctx.config_mut().persistence.aof_fsync = AofFsync::EverySec;
}

fn assert_waitaof_blocks(backend: &mut ServerWorkerState, reader: Token) -> ReadBatchEffects {
    let target = backend.connection_registry[reader].last_write_offset();
    let mut batch = ReadBatchEffects::default();
    dispatch(backend, reader, &[b"WAITAOF", b"1", b"0", b"0"], &mut batch);
    assert!(
        matches!(
            backend.connection_registry[reader].blocked.as_ref().map(|state| &state.cmd),
            Some(BlockedCmd::WaitAof { target_offset, .. }) if *target_offset == target
        ),
        "unsynced consumer effects must not be acknowledged: {:?}",
        batch.replies
    );
    assert!(batch.replies.is_empty());
    backend.check_wait_replica_wakeups();
    assert!(backend.connection_registry[reader].blocked.is_some());
    batch
}

#[test]
fn stream_durability_initial_timeout_waitaof_waits_for_real_fsync() {
    let (mut backend, reader) = backend_with_reader();
    let file = install_aof(&mut backend, reader);
    let old_fsync = backend.ctx.aof().fsynced_repl_offset;
    let batch = block_reader(&mut backend, reader, true);
    finish_without_fsync(&mut backend, reader, batch);
    assert!(std::fs::read(&file.0)
        .unwrap()
        .windows(14)
        .any(|part| part == b"CREATECONSUMER"));
    assert_eq!(backend.ctx.aof().fsynced_repl_offset, old_fsync);
    backend.connection_registry[reader]
        .blocked
        .as_mut()
        .unwrap()
        .start_time_ms = mstime() - 10;
    backend.check_blocked_timeouts();
    assert!(backend.connection_registry[reader].blocked.is_none());
    backend.connection_registry[reader].clear_output_queue();
    let wait_batch = assert_waitaof_blocks(&mut backend, reader);
    let target = backend.connection_registry[reader].last_write_offset();
    assert_eq!(target.get(), old_fsync.unwrap() + 1);
    assert_eq!(backend.ctx.replication().primary_repl_offset(), target);
    assert!(!backend.ctx.replication().backlog.is_active());
    assert_eq!(backend.ctx.replication().backlog.history_len(), 0);
    finish_without_fsync(&mut backend, reader, wait_batch);
    sync_aof(&mut backend);
    assert_eq!(backend.ctx.aof().fsynced_repl_offset, Some(target.get()));
    backend.check_wait_replica_wakeups();
    assert!(backend.connection_registry[reader].blocked.is_none());
    assert_eq!(_delivered_bytes(&backend, reader.0), b"*2\r\n:1\r\n:0\r\n");
}

#[test]
fn stream_durability_initial_dispatch_records_once_before_sink() {
    for is_aof_enabled in [false, true] {
        for is_backlog_active in [false, true] {
            let (mut backend, reader) = backend_with_reader();
            backend.ctx.config_mut().persistence.aof_enabled = is_aof_enabled;
            if is_backlog_active {
                backend.ctx.replication_mut().backlog.activate();
            }
            let mut batch = block_reader(&mut backend, reader, true);
            let expected = if is_backlog_active {
                batch.replication_only.iter().map(Vec::len).sum::<usize>() as i64
            } else {
                i64::from(is_aof_enabled)
            };
            assert_eq!(
                backend.ctx.replication().primary_repl_offset().get(),
                expected
            );
            assert_eq!(
                backend.connection_registry[reader]
                    .last_write_offset()
                    .get(),
                expected
            );
            let frames = batch.replication_only.clone();
            backend.commit_blocked_stream_read_effects(reader.0, None, None, Some(&mut batch));
            assert_eq!(batch.replication_only, frames);
            assert_eq!(
                backend.ctx.replication().primary_repl_offset().get(),
                expected
            );
            // Feed only the real replication sink, without opening a journal.
            for frame in &frames {
                backend.propagate_to_replicas(frame);
            }
            assert_eq!(
                backend.ctx.replication().primary_repl_offset().get(),
                expected
            );
            assert_eq!(
                backend.ctx.replication().backlog.history_len(),
                if is_backlog_active {
                    expected as usize
                } else {
                    0
                }
            );
        }
    }
}

#[test]
fn stream_durability_reblock_owns_its_new_offset_not_trigger_offset() {
    let (mut backend, reader) = backend_with_reader();
    let _file = install_aof(&mut backend, reader);
    let initial = block_reader(&mut backend, reader, true);
    finish_without_fsync(&mut backend, reader, initial);
    sync_aof(&mut backend);
    let before = backend.ctx.replication().primary_repl_offset().get();
    let trigger = add_connection(&mut backend);
    backend.connection_registry[trigger].selected_db = 1;
    let mut batch = ReadBatchEffects::default();
    dispatch(
        &mut backend,
        trigger,
        &[b"XGROUP", b"DELCONSUMER", b"s", b"g", b"c"],
        &mut batch,
    );
    assert_eq!(
        backend.connection_registry[trigger]
            .last_write_offset()
            .get(),
        before + 1
    );
    backend.process_dirty_keys_in_loop(trigger.0, &mut batch);
    backend.process_ready_keys();
    assert!(backend.connection_registry[reader].blocked.is_some());
    assert_eq!(
        backend.ctx.replication().primary_repl_offset().get(),
        before + 2
    );
    assert_eq!(
        backend.connection_registry[reader]
            .last_write_offset()
            .get(),
        before + 2
    );
    assert_eq!(
        backend.connection_registry[trigger]
            .last_write_offset()
            .get(),
        before + 1
    );
    assert_eq!(batch.replication_only.len(), 2);
    assert!(batch.replication_only[0]
        .windows(11)
        .any(|part| part == b"DELCONSUMER"));
    assert!(batch.replication_only[1]
        .windows(14)
        .any(|part| part == b"CREATECONSUMER"));
    backend.process_dirty_keys_in_loop(trigger.0, &mut batch);
    assert_eq!(
        backend.ctx.replication().primary_repl_offset().get(),
        before + 2
    );
    finish_without_fsync(&mut backend, trigger, batch);
    backend.connection_registry[reader]
        .blocked
        .as_mut()
        .unwrap()
        .start_time_ms = mstime() - 10;
    backend.check_blocked_timeouts();
    assert_waitaof_blocks(&mut backend, reader);
    sync_aof(&mut backend);
    backend.check_wait_replica_wakeups();
    assert!(backend.connection_registry[reader].blocked.is_none());
}

#[test]
fn stream_durability_terminal_wakeup_same_batch_waitaof_uses_new_target() {
    for noack in [false, true] {
        let (mut backend, reader) = backend_with_reader();
        let _file = install_aof(&mut backend, reader);
        let initial = block_reader(&mut backend, reader, noack);
        finish_without_fsync(&mut backend, reader, initial);
        sync_aof(&mut backend);
        let before = backend.ctx.replication().primary_repl_offset().get();
        let stream = backend
            .ctx
            .db_mut(1)
            .unwrap()
            .get_stream_mut(b"s")
            .unwrap()
            .unwrap();
        stream
            .append(
                StreamID { ms: 1, seq: 0 },
                vec![(Bytes::from_static(b"field"), Bytes::from_static(b"value"))],
                true,
                false,
                1,
            )
            .unwrap();
        let mut batch = ReadBatchEffects::default();
        backend.wakeup_blocked_client_with_effect_queue(reader, None, None, Some(&mut batch));
        assert!(backend.connection_registry[reader].blocked.is_none());
        assert!(!batch.replication_only.is_empty());
        assert_eq!(batch.replication_only, batch.aof_only);
        assert_eq!(
            backend.ctx.replication().primary_repl_offset().get(),
            before + 1
        );
        assert_eq!(
            backend.connection_registry[reader]
                .last_write_offset()
                .get(),
            before + 1
        );
        dispatch(
            &mut backend,
            reader,
            &[b"WAITAOF", b"1", b"0", b"0"],
            &mut batch,
        );
        assert!(matches!(
            backend.connection_registry[reader].blocked.as_ref().map(|state| &state.cmd),
            Some(BlockedCmd::WaitAof { target_offset, .. }) if target_offset.get() == before + 1
        ));
        finish_without_fsync(&mut backend, reader, batch);
        backend.check_wait_replica_wakeups();
        assert!(backend.connection_registry[reader].blocked.is_some());
        sync_aof(&mut backend);
        backend.check_wait_replica_wakeups();
        assert!(backend.connection_registry[reader].blocked.is_none());
    }
}

#[test]
fn stream_durability_empty_wakeup_preserves_previous_writer_target() {
    let (mut backend, reader) = backend_with_reader();
    backend.ctx.config_mut().persistence.aof_enabled = true;
    let initial = block_reader(&mut backend, reader, true);
    let before = backend.ctx.replication().primary_repl_offset();
    let target = backend.connection_registry[reader].last_write_offset();
    let mut replication = initial.replication_only.clone();
    let mut aof = initial.aof_only.clone();
    let frames = replication.clone();
    backend.wakeup_blocked_client(reader, Some(&mut replication), Some(&mut aof));
    assert!(backend.connection_registry[reader].blocked.is_some());
    assert_eq!(replication, frames);
    assert_eq!(aof, replication);
    assert_eq!(backend.ctx.replication().primary_repl_offset(), before);
    assert_eq!(
        backend.connection_registry[reader].last_write_offset(),
        target
    );
}

#[test]
fn stream_durability_target_filters_follow_ordinary_offset_rules() {
    for is_replica in [false, true] {
        for is_backlog_active in [false, true] {
            for targets in [
                PropagationTargets::Aof,
                PropagationTargets::Replication,
                PropagationTargets::Both,
            ] {
                let (mut backend, reader) = backend_with_reader();
                let _ = block_reader(&mut backend, reader, true);
                backend.ctx.config_mut().persistence.aof_enabled = true;
                if is_backlog_active {
                    backend.ctx.replication_mut().backlog.activate();
                }
                if is_replica {
                    backend.ctx.replication_mut().role =
                        crate::engine::distributed::replication::ReplicationRole::Replica;
                }
                backend.connection_registry[reader].record_dirty_key((1, Bytes::from_static(b"s")));
                let frame =
                    encode_command_args(&args(&[b"XGROUP", b"CREATECONSUMER", b"s", b"g", b"c"]));
                backend.connection_registry[reader]
                    .record_script_propagation(PropagationOp::new(1, targets, frame));
                let mut pending = EffectQueue::default();
                backend.commit_blocked_stream_read_effects(
                    reader.0,
                    None,
                    None,
                    Some(&mut pending),
                );
                let expected = if is_replica {
                    0
                } else if is_backlog_active {
                    pending.replication_only.iter().map(Vec::len).sum::<usize>() as i64
                } else {
                    i64::from(targets.includes_replication())
                };
                assert_eq!(
                    backend.ctx.replication().primary_repl_offset().get(),
                    expected
                );
                assert_eq!(
                    backend.connection_registry[reader]
                        .last_write_offset()
                        .get(),
                    expected
                );
                assert_eq!(
                    pending.replication_only.is_empty(),
                    !targets.includes_replication()
                );
                assert_eq!(pending.aof_only.is_empty(), !targets.includes_aof());
            }
        }
    }
}

#[test]
fn stream_durability_mixed_targets_count_serialized_wrappers_once() {
    for is_backlog_active in [false, true] {
        let (mut backend, reader) = backend_with_reader();
        let _ = block_reader(&mut backend, reader, true);
        backend.ctx.config_mut().persistence.aof_enabled = true;
        if is_backlog_active {
            backend.ctx.replication_mut().backlog.activate();
        }
        let aof_frame = encode_command_args(&args(&[b"INCR", b"aof-key"]));
        let repl_frame = encode_command_args(&args(&[b"INCR", b"repl-key"]));
        backend.connection_registry[reader].record_dirty_key((1, Bytes::from_static(b"s")));
        backend.connection_registry[reader].record_script_propagation(PropagationOp::new(
            0,
            PropagationTargets::Aof,
            aof_frame.clone(),
        ));
        backend.connection_registry[reader].record_script_propagation(PropagationOp::new(
            1,
            PropagationTargets::Replication,
            repl_frame.clone(),
        ));
        let mut pending = EffectQueue::default();
        backend.commit_blocked_stream_read_effects(reader.0, None, None, Some(&mut pending));
        assert_eq!(
            pending.replication_only,
            vec![
                encode_command_args(&args(&[b"MULTI"])),
                repl_frame,
                encode_command_args(&args(&[b"EXEC"])),
            ]
        );
        assert_eq!(
            pending.aof_only,
            vec![
                encode_command_args(&args(&[b"MULTI"])),
                encode_command_args(&args(&[b"SELECT", b"0"])),
                aof_frame,
                encode_command_args(&args(&[b"EXEC"])),
            ]
        );
        let expected = if is_backlog_active {
            pending.replication_only.iter().map(Vec::len).sum::<usize>() as i64
        } else {
            1
        };
        assert_eq!(
            backend.ctx.replication().primary_repl_offset().get(),
            expected
        );
        assert_eq!(
            backend.connection_registry[reader]
                .last_write_offset()
                .get(),
            expected
        );
        for frame in &pending.replication_only {
            backend.propagate_to_replicas(frame);
        }
        assert_eq!(
            backend.ctx.replication().primary_repl_offset().get(),
            expected
        );
    }
}

#[test]
fn stream_durability_direct_commit_records_before_aof_flush() {
    let (mut backend, reader) = backend_with_reader();
    let file = install_aof(&mut backend, reader);
    let old_fsync = backend.ctx.aof().fsynced_repl_offset;
    let read_args = args(&[
        b"XREADGROUP",
        b"GROUP",
        b"g",
        b"c",
        b"BLOCK",
        b"1",
        b"NOACK",
        b"STREAMS",
        b"s",
        b">",
    ]);
    let reply = crate::framework::commands::stream::xreadgroup::xreadgroup(
        &mut backend.connection_registry[reader],
        &read_args,
        &mut backend.ctx,
    );
    assert_eq!(reply, Value::Null);
    backend.ctx.config_mut().persistence.aof_fsync = AofFsync::No;
    backend.commit_blocked_stream_read_effects(reader.0, None, None, None);
    let target = old_fsync.unwrap() + 1;
    assert_eq!(
        backend.ctx.replication().primary_repl_offset().get(),
        target
    );
    assert_eq!(
        backend.connection_registry[reader]
            .last_write_offset()
            .get(),
        target
    );
    assert_eq!(backend.ctx.aof().fsynced_repl_offset, old_fsync);
    let disk = std::fs::read(&file.0).unwrap();
    assert!(disk.windows(14).any(|part| part == b"CREATECONSUMER"));
    backend.commit_blocked_stream_read_effects(reader.0, None, None, None);
    assert_eq!(
        backend.ctx.replication().primary_repl_offset().get(),
        target
    );
    assert_eq!(std::fs::read(&file.0).unwrap(), disk);
}

#[test]
fn stream_durability_overflow_requests_shutdown_without_advancing_target() {
    let (mut backend, reader) = backend_with_reader();
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend
        .ctx
        .replication_mut()
        .backlog
        .align_inactive_offset(ReplicationOffset::try_from(i64::MAX).unwrap())
        .unwrap();
    backend.connection_registry[reader]
        .set_last_write_offset(ReplicationOffset::try_from(7).unwrap());
    let batch = block_reader(&mut backend, reader, true);
    assert!(!batch.replication_only.is_empty());
    assert!(backend.ctx.is_shutdown_requested());
    assert_eq!(
        backend.ctx.replication().primary_repl_offset().get(),
        i64::MAX
    );
    assert_eq!(
        backend.connection_registry[reader]
            .last_write_offset()
            .get(),
        7
    );
}
