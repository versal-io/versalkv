use super::*;
use std::sync::Arc;
use std::thread;

struct TestFile(std::path::PathBuf);

impl TestFile {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "versal-aof-stamp-{}-{}.aof",
            std::process::id(),
            rand::random::<u64>()
        ));
        Self(path)
    }

    fn open(&self, mode: AofFsync) -> AofAppender {
        AofAppender::open_append_path(&self.0, mode).unwrap()
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[test]
fn opening_new_aof_requires_a_durable_directory_install() {
    for appenddirname in ["", "appendonlydir"] {
        let file = TestFile::new();
        std::fs::create_dir(&file.0).unwrap();
        super::manifest::FAIL_NEXT_DIRECTORY_SYNC.with(|fail| fail.set(true));
        let result = AofAppender::open(
            file.0.to_str().unwrap(),
            "appendonly.aof",
            appenddirname,
            AofOpenMode::CreateNew,
            AofFsync::No,
        );
        super::manifest::FAIL_NEXT_DIRECTORY_SYNC.with(|fail| fail.set(false));
        assert!(
            result.is_err(),
            "new AOF installation must sync its directory"
        );
        std::fs::remove_dir_all(&file.0).unwrap();
    }
}

#[test]
fn append_existing_rejects_missing_manifest_increment() {
    let file = TestFile::new();
    let app_dir = file.0.join("appendonlydir");
    std::fs::create_dir_all(&app_dir).unwrap();
    std::fs::write(
        app_dir.join("appendonly.aof.manifest"),
        b"file appendonly.aof.7.incr.aof seq 7 type i\n",
    )
    .unwrap();
    let result = AofAppender::open(
        file.0.to_str().unwrap(),
        "appendonly.aof",
        "appendonlydir",
        AofOpenMode::AppendExisting,
        AofFsync::No,
    );
    assert!(
        result.is_err(),
        "missing committed increment must not be recreated"
    );
    assert!(!app_dir.join("appendonly.aof.1.incr.aof").exists());
    assert!(!app_dir.join("appendonly.aof.7.incr.aof").exists());
    assert!(
        ensure_aof_increment(file.0.to_str().unwrap(), "appendonly.aof", "appendonlydir").is_err()
    );
    std::fs::remove_dir_all(&file.0).unwrap();
}

#[test]
fn fresh_always_empty_flush_does_not_sync_or_confirm_the_initial_stamp() {
    let mut appender =
        AofAppender::open_append_path(Path::new("/dev/null"), AofFsync::Always).unwrap();
    let initial = appender.appended_stamp();
    appender.flush(false).unwrap();
    assert!(appender.last_write_ok());
    assert!(!appender.sync_handle().is_durable(initial));
    assert!(appender.force_fsync().is_err());
    assert!(!appender.last_write_ok());
    assert!(appender.flush(false).is_err());
}

#[test]
fn always_empty_flush_retries_a_real_failed_sync_without_rewriting_bytes() {
    let file = TestFile::new();
    let mut appender = file.open(AofFsync::Always);
    appender.feed(b"pending");
    let original = std::mem::replace(
        &mut appender.fd,
        OpenOptions::new().append(true).open("/dev/null").unwrap(),
    );
    assert!(appender.flush(false).is_err());
    assert!(appender.buf.is_empty());
    let stamp = appender.appended_stamp();
    assert!(!appender.sync_handle().is_durable(stamp));
    assert!(appender.flush(false).is_err());
    appender.fd = original;
    appender.flush(false).unwrap();
    assert!(appender.sync_handle().is_durable(stamp));
    assert!(appender.last_write_ok());
    assert_eq!(appender.current_size(), 7);
}

#[test]
fn truncate_starts_a_new_unsynced_generation_without_an_empty_flush_sync() {
    let file = TestFile::new();
    let mut appender = file.open(AofFsync::Always);
    appender.feed(b"old");
    appender.flush(false).unwrap();
    let old = appender.appended_stamp();
    appender.truncate(0).unwrap();
    let initial = appender.appended_stamp();
    appender.flush(false).unwrap();
    assert!(!appender.sync_handle().is_durable(old));
    assert!(!appender.sync_handle().is_durable(initial));
    appender.feed(b"new");
    appender.flush(false).unwrap();
    assert!(appender.sync_handle().is_durable(appender.appended_stamp()));
    assert_eq!(std::fs::read(&file.0).unwrap(), b"new");
}

#[test]
fn force_fsync_writes_buffered_frames() {
    let file = TestFile::new();
    let mut appender = file.open(AofFsync::No);
    appender.feed(b"buffered-command");
    appender.force_fsync().unwrap();
    assert_eq!(std::fs::read(&file.0).unwrap(), b"buffered-command");
    assert_eq!(appender.current_size(), 16);
}

#[test]
fn switching_to_always_syncs_pending_frames() {
    let file = TestFile::new();
    let mut appender = file.open(AofFsync::No);
    appender.feed(b"pending-command");
    appender.set_fsync_mode(AofFsync::Always).unwrap();
    assert_eq!(appender.fsync_mode(), AofFsync::Always);
    assert_eq!(std::fs::read(&file.0).unwrap(), b"pending-command");
}

#[test]
fn switching_to_always_preserves_mode_on_sync_error() {
    let mut appender = AofAppender::open_append_path(Path::new("/dev/null"), AofFsync::No).unwrap();
    appender.feed(b"pending-command");
    assert!(appender.set_fsync_mode(AofFsync::Always).is_err());
    assert_eq!(appender.fsync_mode(), AofFsync::No);
    assert!(!appender.last_write_ok());
}

#[test]
fn write_only_flush_does_not_confirm_buffered_or_written_stamps() {
    let file = TestFile::new();
    let mut appender = file.open(AofFsync::No);
    let progress = appender.sync_handle();
    let initial = appender.appended_stamp();
    assert!(!progress.is_durable(initial));
    appender.feed(b"first");
    let first = appender.appended_stamp();
    // Other appenders can reserve intervening offsets while these tests run concurrently.
    assert!(first.offset - initial.offset >= 5);
    progress.complete_for_test(first, Ok(()));
    assert!(!progress.is_durable(first));
    appender.flush(false).unwrap();
    assert_eq!(std::fs::read(&file.0).unwrap(), b"first");
    assert!(!progress.is_durable(first));
    appender.force_fsync().unwrap();
    assert!(progress.is_durable(first));
    appender.feed(b"second");
    assert!(!progress.is_durable(appender.appended_stamp()));
    assert!(progress.is_durable(first));
}

#[test]
fn in_flight_sync_only_confirms_its_written_snapshot() {
    let file = TestFile::new();
    let mut appender = file.open(AofFsync::No);
    appender.feed(b"first");
    appender.flush(false).unwrap();
    let first = appender.appended_stamp();
    let progress = appender.sync_handle();
    let fd = appender.fd.try_clone().unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let sync_progress = progress.clone();
    let worker = thread::spawn(move || {
        sync_progress.sync_with(|| {
            entered_tx.send(()).unwrap();
            resume_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            fd.sync_data()
        })
    });
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    appender.feed(b"second");
    appender.flush(false).unwrap();
    let second = appender.appended_stamp();
    resume_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
    assert!(progress.is_durable(first));
    assert!(!progress.is_durable(second));
    appender.force_fsync().unwrap();
    assert!(progress.is_durable(second));
}

#[test]
fn failed_sync_is_visible_and_later_success_recovers() {
    let file = TestFile::new();
    let mut appender = file.open(AofFsync::No);
    appender.feed(b"first");
    appender.flush(false).unwrap();
    let stamp = appender.appended_stamp();
    let progress = appender.sync_handle();
    assert_eq!(
        progress
            .sync_with(|| Err(io::ErrorKind::StorageFull.into()))
            .unwrap_err()
            .kind(),
        io::ErrorKind::StorageFull
    );
    assert_eq!(progress.last_error(), Some(io::ErrorKind::StorageFull));
    assert!(!appender.last_write_ok());
    assert!(!progress.is_durable(stamp));
    appender.force_fsync().unwrap();
    assert_eq!(progress.last_error(), None);
    assert!(appender.last_write_ok());
    assert!(progress.is_durable(stamp));
}

#[test]
fn foreground_sync_failure_then_background_success_restores_write_status() {
    let file = TestFile::new();
    let mut appender = file.open(AofFsync::No);
    appender.feed(b"persisted");
    appender.force_fsync().unwrap();
    let stamp = appender.appended_stamp();
    let progress = appender.sync_handle();
    let original = std::mem::replace(
        &mut appender.fd,
        OpenOptions::new().append(true).open("/dev/null").unwrap(),
    );
    assert!(appender.force_fsync().is_err());
    assert!(progress.last_error().is_some());
    assert!(!appender.last_write_ok());
    appender.fd = original;
    let fd = appender.fd.try_clone().unwrap();
    let mut worker = FsyncThread::spawn_with_sync(
        progress.clone(),
        move || fd.sync_data(),
        std::time::Duration::from_secs(60),
    )
    .unwrap();

    worker.stop(true).unwrap();

    assert_eq!(progress.last_error(), None);
    assert!(progress.is_durable(stamp));
    assert!(
        appender.last_write_ok(),
        "successful background sync must clear a foreground sync failure"
    );
}

#[test]
fn reopening_uses_distinct_generation_and_global_logical_offsets() {
    let file = TestFile::new();
    let mut first = file.open(AofFsync::No);
    first.feed(b"first");
    first.force_fsync().unwrap();
    let old_stamp = first.appended_stamp();
    let old_handle = first.sync_handle();
    first.shutdown().unwrap();
    drop(first);
    let mut second = file.open(AofFsync::Always);
    assert_ne!(old_stamp.generation, second.appended_stamp().generation);
    assert!(second.appended_stamp().offset > old_stamp.offset);
    second.feed(b"second");
    second.flush(false).unwrap();
    let new_handle = second.sync_handle();
    assert!(old_handle.is_durable(old_stamp));
    assert!(!new_handle.is_durable(old_stamp));
    assert!(!old_handle.is_durable(second.appended_stamp()));
    new_handle.complete_for_test(old_stamp, Err(io::ErrorKind::Other.into()));
    assert_eq!(new_handle.last_error(), None);
    assert!(new_handle.is_durable(second.appended_stamp()));
}

#[test]
fn truncate_invalidates_old_stamps_without_rewinding_logical_offset() {
    let file = TestFile::new();
    let mut appender = file.open(AofFsync::EverySec);
    appender.feed(b"first");
    appender.force_fsync().unwrap();
    let old_stamp = appender.appended_stamp();
    let handle = appender.sync_handle();
    appender.truncate(0).unwrap();
    let truncated = appender.appended_stamp();
    assert_ne!(old_stamp.generation, truncated.generation);
    assert!(truncated.offset > old_stamp.offset);
    assert!(!handle.is_durable(old_stamp));
    assert!(!handle.is_durable(truncated));
    appender.feed(b"new");
    appender.force_fsync().unwrap();
    assert_eq!(std::fs::read(&file.0).unwrap(), b"new");
    assert!(handle.is_durable(appender.appended_stamp()));
    appender.shutdown().unwrap();
}

#[test]
fn every_mode_transition_preserves_progress_and_thread_ownership() {
    for start in [AofFsync::No, AofFsync::Always, AofFsync::EverySec] {
        for target in [AofFsync::No, AofFsync::Always, AofFsync::EverySec] {
            let file = TestFile::new();
            let mut appender = file.open(start);
            let handle = appender.sync_handle();
            appender.feed(b"pending");
            let stamp = appender.appended_stamp();
            appender.set_fsync_mode(target).unwrap();
            assert_eq!(appender.fsync_mode(), target);
            assert_eq!(
                appender.fsync_thread.is_some(),
                target == AofFsync::EverySec
            );
            if start != target && target == AofFsync::Always {
                assert!(handle.is_durable(stamp));
            }
            appender.shutdown().unwrap();
            assert!(handle.is_durable(stamp));
            assert_eq!(std::fs::read(&file.0).unwrap(), b"pending");
        }
    }
}

#[test]
fn always_flush_retries_sync_after_buffer_has_been_written() {
    let file = TestFile::new();
    let mut appender = file.open(AofFsync::Always);
    appender.feed(b"pending");
    appender.write_buffer().unwrap();
    let stamp = appender.appended_stamp();
    let handle = appender.sync_handle();
    handle.complete_for_test(stamp, Err(io::ErrorKind::Other.into()));
    appender.flush(false).unwrap();
    assert_eq!(handle.last_error(), None);
    assert!(handle.is_durable(stamp));
}

struct CountWake(std::sync::mpsc::Sender<()>);

impl crate::foundation::io::transport::TransportWake for CountWake {
    fn wake(&self) -> io::Result<()> {
        self.0.send(()).map_err(io::Error::other)
    }
}

fn install_waker(appender: &mut AofAppender) -> std::sync::mpsc::Receiver<()> {
    let (tx, rx) = std::sync::mpsc::channel();
    appender.set_sync_waker(crate::foundation::io::transport::TransportWakeHandle::new(
        Arc::new(CountWake(tx)),
    ));
    rx
}

#[test]
fn background_error_and_recovery_both_wake_main_thread() {
    let file = TestFile::new();
    let mut appender = file.open(AofFsync::No);
    appender.feed(b"pending");
    appender.flush(false).unwrap();
    let handle = appender.sync_handle();
    let stamp = appender.appended_stamp();
    let wakes = install_waker(&mut appender);
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let fd = appender.fd.try_clone().unwrap();
    let mut worker = FsyncThread::spawn_with_sync(
        handle.clone(),
        move || match result_rx.recv_timeout(std::time::Duration::from_secs(5)) {
            Ok(false) => Err(io::ErrorKind::StorageFull.into()),
            Ok(true) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => fd.sync_data(),
            Err(error) => panic!("Sync test controller timed out: {error}"),
        },
        std::time::Duration::from_millis(1),
    )
    .unwrap();
    result_tx.send(false).unwrap();
    wakes
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert_eq!(handle.last_error(), Some(io::ErrorKind::StorageFull));
    assert!(!handle.is_durable(stamp));
    result_tx.send(true).unwrap();
    wakes
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert_eq!(handle.last_error(), None);
    assert!(handle.is_durable(stamp));
    // Unblock a possible third periodic sync before stopping the worker.
    result_tx.send(true).unwrap();
    drop(result_tx);
    worker.stop(false).unwrap();
}

#[test]
fn stopping_background_thread_returns_final_sync_error() {
    let file = TestFile::new();
    let appender = file.open(AofFsync::No);
    let handle = appender.sync_handle();
    let mut worker = FsyncThread::spawn_with_sync(
        handle.clone(),
        || Err(io::ErrorKind::StorageFull.into()),
        std::time::Duration::from_secs(60),
    )
    .unwrap();
    assert_eq!(
        worker.stop(true).unwrap_err().kind(),
        io::ErrorKind::StorageFull
    );
    assert_eq!(handle.last_error(), Some(io::ErrorKind::StorageFull));
    assert!(!handle.is_durable(appender.appended_stamp()));
    assert_eq!(
        worker.stop(true).unwrap_err().kind(),
        io::ErrorKind::StorageFull
    );
}

#[test]
fn shutdown_returns_background_final_sync_error_without_masking_it() {
    let file = TestFile::new();
    let mut appender = file.open(AofFsync::No);
    let handle = appender.sync_handle();
    appender.fsync_thread = Some(
        FsyncThread::spawn_with_sync(
            handle.clone(),
            || Err(io::ErrorKind::StorageFull.into()),
            std::time::Duration::from_secs(60),
        )
        .unwrap(),
    );
    appender.feed(b"pending");
    let stamp = appender.appended_stamp();
    assert_eq!(
        appender.shutdown().unwrap_err().kind(),
        io::ErrorKind::StorageFull
    );
    assert!(!handle.is_durable(stamp));
    assert_eq!(handle.last_error(), Some(io::ErrorKind::StorageFull));
    assert_eq!(std::fs::read(&file.0).unwrap(), b"pending");
    appender.shutdown().unwrap();
    assert_eq!(handle.last_error(), None);
    assert!(handle.is_durable(stamp));
}

#[test]
fn failed_shutdown_restores_everysec_worker_and_recovers_sync_progress() {
    let file = TestFile::new();
    let mut appender = file.open(AofFsync::EverySec);
    appender.fsync_thread.take().unwrap().stop(false).unwrap();
    let handle = appender.sync_handle();
    appender.fsync_thread = Some(
        FsyncThread::spawn_with_sync(
            handle.clone(),
            || Err(io::ErrorKind::StorageFull.into()),
            std::time::Duration::from_secs(60),
        )
        .unwrap(),
    );
    appender.feed(b"pending");
    let stamp = appender.appended_stamp();
    let wakes = install_waker(&mut appender);

    assert_eq!(
        appender.shutdown().unwrap_err().kind(),
        io::ErrorKind::StorageFull
    );

    assert!(
        appender.fsync_thread.is_some(),
        "failed shutdown must retain background sync ownership"
    );
    assert_eq!(appender.fsync_mode(), AofFsync::EverySec);
    assert_eq!(handle.last_error(), Some(io::ErrorKind::StorageFull));
    while handle.last_error().is_some() {
        wakes
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
    }
    assert!(handle.is_durable(stamp));
    assert!(appender.last_write_ok());
    appender.shutdown().unwrap();
}

#[test]
fn retained_everysec_appender_restarts_worker_when_used_after_shutdown() {
    let file = TestFile::new();
    let mut appender = file.open(AofFsync::EverySec);
    appender.shutdown().unwrap();
    assert!(appender.fsync_thread.is_none());

    appender.set_fsync_mode(AofFsync::EverySec).unwrap();
    assert!(appender.fsync_thread.is_some());
    appender.shutdown().unwrap();
    appender.feed(b"retry");
    appender.flush(false).unwrap();
    assert!(appender.fsync_thread.is_some());
    appender.shutdown().unwrap();
}

#[test]
fn shutdown_flushes_buffer_and_confirms_stamps_in_all_modes() {
    for mode in [AofFsync::No, AofFsync::Always, AofFsync::EverySec] {
        let file = TestFile::new();
        let mut appender = file.open(mode);
        let handle = appender.sync_handle();
        appender.feed(b"pending");
        let stamp = appender.appended_stamp();
        appender.shutdown().unwrap();
        assert!(handle.is_durable(stamp));
        assert!(appender.fsync_thread.is_none());
        assert_eq!(std::fs::read(&file.0).unwrap(), b"pending");
    }
}

#[test]
fn partial_write_retry_does_not_duplicate_prefix() {
    struct PartialWriter {
        bytes: Vec<u8>,
        calls: usize,
    }
    impl Write for PartialWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.calls += 1;
            match self.calls {
                1 => Err(io::ErrorKind::Interrupted.into()),
                2 => {
                    self.bytes.extend_from_slice(&bytes[..3]);
                    Ok(3)
                }
                3 => Err(io::ErrorKind::StorageFull.into()),
                _ => {
                    self.bytes.extend_from_slice(bytes);
                    Ok(bytes.len())
                }
            }
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = PartialWriter {
        bytes: Vec::new(),
        calls: 0,
    };
    let file = TestFile::new();
    let mut appender = file.open(AofFsync::No);
    let handle = appender.sync_handle();
    let before = handle.written_stamp_for_test();
    appender.feed(b"abcdef");
    let accepted = appender.appended_stamp();
    let result = write_pending(&mut writer, &mut appender.buf, &mut appender.current_size);
    assert_eq!(
        appender.record_buffer_write(result).unwrap_err().kind(),
        io::ErrorKind::StorageFull
    );
    assert_eq!(appender.current_size(), 3);
    assert_eq!(appender.buf, b"def");
    assert!(!appender.last_write_ok());
    assert_eq!(handle.written_stamp_for_test(), before);
    handle.complete_for_test(accepted, Ok(()));
    assert!(!handle.is_durable(accepted));
    let result = write_pending(&mut writer, &mut appender.buf, &mut appender.current_size);
    appender.record_buffer_write(result).unwrap();
    assert_eq!(handle.written_stamp_for_test(), accepted);
    assert!(appender.last_write_ok());
    assert_eq!(appender.current_size(), 6);
    assert!(appender.buf.is_empty());
    assert_eq!(writer.bytes, b"abcdef");
}
