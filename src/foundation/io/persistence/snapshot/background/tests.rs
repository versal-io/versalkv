use super::*;

impl RdbSaveFeeder {
    pub(crate) fn test_channel(capacity: usize) -> (Self, Receiver<FrozenWorkItem>) {
        let (sender, receiver) = sync_channel(capacity);
        (
            Self::new(sender, Arc::new(RdbSaveControl::new(0, 0))),
            receiver,
        )
    }
}

fn freeze_source(source: &crate::foundation::io::store::kv_engine::DbSet) -> FrozenRdb {
    FrozenRdb::capture(
        source,
        &[],
        &[],
        None,
        RdbSerializeOptions::default(),
        0,
        true,
    )
}

#[test]
fn buffered_pipe_serializer_preserves_bytes_with_fewer_writes() {
    use crate::foundation::io::store::{kv_engine::DbSet, Robj};

    #[derive(Default)]
    struct CountingWriter {
        bytes: Vec<u8>,
        calls: usize,
    }
    impl Write for CountingWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.calls += 1;
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn feed_with_fixed_header(
        frozen: FrozenRdb,
        sender: SyncSender<FrozenWorkItem>,
        control: Arc<RdbSaveControl>,
    ) {
        for item in frozen.into_work_items() {
            let item = match item {
                // Production AUX metadata includes the current wall clock.
                FrozenWorkItem::Header(_) => FrozenWorkItem::Header(b"VALKEY081".to_vec()),
                other => other,
            };
            if matches!(item, FrozenWorkItem::Entry(_)) {
                control.queued_item().unwrap();
            }
            sender.send(item).unwrap();
            control.notify_feed_changed();
        }
        drop(sender);
        control.notify_feed_changed();
    }

    let mut source = DbSet::new(1);
    for index in 0..1_000 {
        source.get_mut(0).unwrap().set(
            format!("key:{index}").into_bytes(),
            Robj::string("a small value"),
        );
    }
    let mut reference = Vec::new();
    let (sender, receiver) = sync_channel(WORK_QUEUE_CAPACITY);
    let control = Arc::new(RdbSaveControl::new(1_000, 0));
    let frozen = freeze_source(&source);
    let feeder_control = Arc::clone(&control);
    let feeder = std::thread::spawn(move || feed_with_fixed_header(frozen, sender, feeder_control));
    serialize_work_items(
        receiver,
        RdbSerializeOptions::default(),
        &control,
        None,
        |output| {
            if let RdbOutput::Bytes(chunk) = output {
                reference.extend_from_slice(chunk);
            }
            Ok(())
        },
    )
    .unwrap();
    feeder.join().unwrap();

    let (sender, receiver) = sync_channel(WORK_QUEUE_CAPACITY);
    let control = Arc::new(RdbSaveControl::new(1_000, 0));
    let frozen = freeze_source(&source);
    let feeder_control = Arc::clone(&control);
    let feeder = std::thread::spawn(move || feed_with_fixed_header(frozen, sender, feeder_control));
    let mut writer = CountingWriter::default();
    serialize_pipe_work_items(
        receiver,
        RdbSerializeOptions::default(),
        &control,
        &mut writer,
    )
    .unwrap();
    feeder.join().unwrap();
    assert_eq!(writer.bytes, reference);
    assert!(writer.calls < 10, "small entries must share output writes");
    assert_eq!(control.progress().processed, 1_000);
}

#[test]
fn buffered_pipe_serializer_does_not_retry_a_failed_flush() {
    struct FailingWriter(usize);
    impl Write for FailingWriter {
        fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
            self.0 += 1;
            Err(io::Error::other("Output failed"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let (sender, receiver) = sync_channel(1);
    sender
        .send(FrozenWorkItem::Header(b"header".to_vec()))
        .unwrap();
    drop(sender);
    let control = RdbSaveControl::new(0, 0);
    let mut writer = FailingWriter(0);
    assert!(serialize_pipe_work_items(
        receiver,
        RdbSerializeOptions::default(),
        &control,
        &mut writer
    )
    .is_err());
    assert_eq!(writer.0, 1, "dropping the buffer must not retry output");
}

#[test]
fn item_delay_change_flushes_without_holding_the_control_lock() {
    let control = RdbSaveControl::new(1, 0);
    control.queued_item().unwrap();
    control.begin_item().unwrap();
    // Model a configuration change after the bytes entered an output buffer.
    control.set_key_delay(10_000_000);
    let mut has_flushed = false;
    control
        .finish_item(|| {
            has_flushed = true;
            control.set_key_delay(0);
            Ok(())
        })
        .unwrap();
    assert!(
        has_flushed,
        "the actual delay decision must flush pending bytes"
    );
    assert_eq!(control.progress().processed, 1);
}

#[test]
fn item_delay_flush_can_be_cancelled_before_progress_is_published() {
    let control = RdbSaveControl::new(1, 10_000_000);
    control.queued_item().unwrap();
    control.begin_item().unwrap();
    let result = control.finish_item(|| {
        control.cancel();
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(control.progress().processed, 0);
}

#[test]
fn pipe_serializer_preserves_frozen_values_and_reports_actual_progress() {
    use crate::foundation::io::store::{kv_engine::DbSet, Robj};
    use std::io::Read;
    use std::os::fd::AsFd;
    let mut source = DbSet::new(1);
    source
        .get_mut(0)
        .unwrap()
        .set(b"key".to_vec(), Robj::string("before"));
    let frozen = freeze_source(&source);
    source
        .get_mut(0)
        .unwrap()
        .set(b"key".to_vec(), Robj::string("after"));
    let (mut task, mut read) = BackgroundRdbSave::start_to_pipe(
        frozen,
        0,
        RdbSnapshotOptions {
            compression: RdbCompression::No,
            checksum: true,
        },
    )
    .unwrap();
    crate::foundation::io::sys::fd::set_nonblocking(read.as_fd(), false).unwrap();
    let mut bytes = Vec::new();
    read.read_to_end(&mut bytes).unwrap();
    assert!(matches!(task.finish(), RdbSaveOutcome::Saved));
    let progress = task.control().progress();
    assert_eq!(progress.total_keys, 1);
    assert_eq!(progress.enqueued, 1);
    assert_eq!(progress.processed, 1);
    let mut loaded = DbSet::new(1);
    crate::foundation::io::persistence::rdb::load_rdb_bytes(&bytes, &mut loaded).unwrap();
    assert_eq!(
        loaded
            .get(0)
            .unwrap()
            .get_no_expire(b"key")
            .unwrap()
            .as_bytes(),
        b"before"
    );
}

#[test]
fn buffered_pipe_serializer_publishes_an_entry_before_its_delay() {
    use crate::foundation::io::store::{kv_engine::DbSet, Robj};
    use std::io::Read;
    use std::os::fd::AsFd;

    let mut source = DbSet::new(1);
    source
        .get_mut(0)
        .unwrap()
        .set(b"key".to_vec(), Robj::string("delayed value"));
    let (mut task, mut read) = BackgroundRdbSave::start_to_pipe(
        freeze_source(&source),
        10_000_000,
        RdbSnapshotOptions {
            compression: RdbCompression::No,
            checksum: true,
        },
    )
    .unwrap();
    let control = task.control();
    let deadline = Instant::now() + Duration::from_secs(2);
    while control.key_delay_wait_us_for_test().is_none() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let mut bytes = Vec::new();
    let mut buffer = [0; 1024];
    loop {
        match read.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => bytes.extend_from_slice(&buffer[..count]),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) => panic!("failed to read delayed output: {error}"),
        }
    }
    let has_visible_value = bytes.windows(13).any(|part| part == b"delayed value");
    let is_waiting = control.key_delay_wait_us_for_test() == Some(10_000_000);
    // Release the worker before asserting, including a failing visibility case.
    control.set_key_delay(0);
    crate::foundation::io::sys::fd::set_nonblocking(read.as_fd(), false).unwrap();
    read.read_to_end(&mut bytes).unwrap();
    assert!(matches!(task.finish(), RdbSaveOutcome::Saved));
    assert!(is_waiting, "the worker must reach the configured delay");
    assert!(
        has_visible_value,
        "buffered entries must flush before waiting"
    );
    let mut loaded = DbSet::new(1);
    crate::foundation::io::persistence::rdb::load_rdb_bytes(&bytes, &mut loaded).unwrap();
    assert_eq!(
        loaded
            .get(0)
            .unwrap()
            .get_no_expire(b"key")
            .unwrap()
            .as_bytes(),
        b"delayed value"
    );
}

#[test]
fn pipe_serializer_cancels_with_a_live_reader_and_a_full_pipe() {
    use crate::foundation::io::store::{kv_engine::DbSet, Robj};
    let mut source = DbSet::new(1);
    source.get_mut(0).unwrap().set(
        b"large".to_vec(),
        Robj::string_raw("x".repeat(4 * 1024 * 1024)),
    );
    let (task, read) = BackgroundRdbSave::start_to_pipe(
        freeze_source(&source),
        0,
        RdbSnapshotOptions {
            compression: RdbCompression::No,
            checksum: true,
        },
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while task.control().progress().current_item_ms == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(
        !task.is_finished(),
        "a live unread pipe must backpressure the serializer"
    );
    let (sent, received) = channel();
    let dropping = std::thread::spawn(move || {
        drop(task);
        sent.send(()).unwrap();
    });
    received
        .recv_timeout(Duration::from_secs(2))
        .expect("cancellation must not depend on closing the reader");
    dropping.join().unwrap();
    drop(read);
}

#[cfg(unix)]
#[test]
fn pipe_writer_resumes_when_reader_drains_before_waiting() {
    use crate::foundation::io::sys::fd::{pipe, PipeFlags};
    use std::io::Read;

    let (mut read, mut write) = pipe(PipeFlags::CLOEXEC_NONBLOCK).unwrap().into_parts();
    let chunk = [0_u8; 4096];
    loop {
        match write.write(&chunk) {
            Ok(written) => assert!(written > 0),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) => panic!("failed to fill the pipe: {error}"),
        }
    }
    let control = Arc::new(RdbSaveControl::new(0, 0));
    let (entered, waiting) = channel();
    let (resume, resumed) = channel();
    let (completed, completion) = channel();
    let mut writer = CancellablePipeWriter {
        file: write,
        control: Arc::clone(&control),
        test_wait: Some(PipeWriteTestWait {
            entered,
            resume: resumed,
            timeout: Duration::from_secs(10),
        }),
    };
    let writing = std::thread::spawn(move || {
        let result = writer.write_all(b"x");
        let _ = completed.send(result);
    });

    let has_entered_wait = waiting.recv_timeout(Duration::from_secs(2)).is_ok();
    if has_entered_wait {
        let mut buffer = [0_u8; 4096];
        loop {
            match read.read(&mut buffer) {
                Ok(count) => assert!(count > 0),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("failed to drain the pipe: {error}"),
            }
        }
    }
    let _ = resume.send(());
    let result = completion.recv_timeout(Duration::from_secs(1));
    // Wake and join the writer even when the old timed wait loses readiness.
    control.cancel();
    writing.join().unwrap();
    assert!(
        has_entered_wait,
        "the full pipe must backpressure the writer"
    );
    assert!(
        matches!(result, Ok(Ok(()))),
        "draining the pipe must resume writing without waiting for a timeout: {result:?}"
    );
}

#[cfg(unix)]
#[test]
fn pipe_writer_reports_a_disconnected_reader() {
    use crate::foundation::io::sys::fd::{pipe, PipeFlags};

    const CHILD_MARKER: &str = "VERSAL_RDB_PIPE_DISCONNECT_TEST_CHILD";
    if std::env::var_os(CHILD_MARKER).is_none() {
        // Other parallel tests fork and may inherit a live pipe reader.
        // Create this pipe only after exec in an isolated test process.
        let module = module_path!().split_once("::").unwrap().1;
        let test = format!("{module}::pipe_writer_reports_a_disconnected_reader");
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &test])
            .env(CHILD_MARKER, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success()
                && String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed;"),
            "the isolated disconnect test must run and pass: {output:?}"
        );
        return;
    }
    let (read, write) = pipe(PipeFlags::CLOEXEC_NONBLOCK).unwrap().into_parts();
    drop(read);
    let mut writer = CancellablePipeWriter {
        file: write,
        control: Arc::new(RdbSaveControl::new(0, 0)),
        test_wait: None,
    };
    assert_eq!(
        writer.write_all(b"x").unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn incremental_save_drop_cancels_an_idle_receiver_with_a_live_feeder() {
    let dir = std::env::temp_dir().join(format!(
        "versal-rdb-idle-cancel-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let (task, feeder) = BackgroundRdbSave::start_incremental(
        1,
        dir.clone(),
        "dump.rdb".to_string(),
        1,
        0,
        false,
        RdbSnapshotOptions {
            compression: RdbCompression::No,
            checksum: true,
        },
    )
    .unwrap();
    let (sender, receiver) = channel();
    let dropping = std::thread::spawn(move || {
        drop(task);
        sender.send(()).unwrap();
    });
    let cancelled = receiver.recv_timeout(Duration::from_secs(1)).is_ok();
    // Always close the feed and join, including the pre-fix failure case.
    drop(feeder);
    dropping.join().unwrap();
    assert!(!dir.join("dump.rdb").exists());
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
    std::fs::remove_dir_all(dir).unwrap();
    assert!(cancelled, "cancellation must wake an idle work receiver");
}

#[test]
fn incremental_feeder_reports_full_without_consuming_item() {
    let control = Arc::new(RdbSaveControl::new(2, 0));
    let (sender, receiver) = sync_channel(1);
    let feeder = RdbSaveFeeder::new(sender, Arc::clone(&control));
    assert!(feeder.try_send(FrozenWorkItem::Header(vec![1])).is_ok());
    let item = FrozenWorkItem::Header(vec![2]);
    assert!(matches!(
        feeder.try_send(item),
        Err(FeedError::Full(item))
            if matches!(item.as_ref(), FrozenWorkItem::Header(bytes) if bytes == &vec![2])
    ));
    assert!(matches!(
        receiver.try_recv(),
        Ok(FrozenWorkItem::Header(bytes)) if bytes == vec![1]
    ));
}

#[test]
fn queue_progress_tracks_only_materialized_items() {
    let control = RdbSaveControl::new(65_536, 0);
    for _ in 0..WORK_QUEUE_CAPACITY {
        control.queued_item().unwrap();
    }
    let progress = control.progress();
    assert_eq!(progress.queue_length, WORK_QUEUE_CAPACITY);
    assert_eq!(progress.queue_length_target, WORK_QUEUE_CAPACITY);

    control.begin_item().unwrap();
    let progress = control.progress();
    assert_eq!(progress.queue_length, WORK_QUEUE_CAPACITY);
    assert_eq!(progress.enqueued, WORK_QUEUE_CAPACITY);
    control.finish_item(|| Ok(())).unwrap();
    assert_eq!(control.progress().queue_length, WORK_QUEUE_CAPACITY - 1);
}

#[test]
fn incremental_lz4_save_streams_before_the_feed_finishes() {
    let dir = std::env::temp_dir().join(format!(
        "versal-forkless-lz4-stream-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let save_counter = 7;
    let temporary = dir.join(format!(
        "temp-forkless-{}-{save_counter}.rdb",
        std::process::id()
    ));
    let (mut task, feeder) = BackgroundRdbSave::start_incremental(
        0,
        dir.clone(),
        "dump.rdb".to_string(),
        save_counter,
        0,
        false,
        RdbSnapshotOptions {
            compression: RdbCompression::Lz4,
            checksum: true,
        },
    )
    .unwrap();
    assert!(feeder
        .try_send(FrozenWorkItem::Header(vec![b'x'; 256 * 1024]))
        .is_ok());
    let deadline = Instant::now() + Duration::from_secs(5);
    while std::fs::metadata(&temporary)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
        == 0
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(1));
    }

    assert!(
        std::fs::metadata(&temporary).unwrap().len() > 0,
        "the compressed stream must reach disk before the feeder closes"
    );

    task.control().cancel();
    drop(feeder);
    let _ = task.finish();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn directory_sync_failure_does_not_mark_snapshot_committed() {
    let dir = std::env::temp_dir().join(format!(
        "versal-rdb-commit-sync-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let temporary = dir.join("temporary.rdb");
    let destination = dir.join("dump.rdb");
    std::fs::write(&temporary, b"snapshot").unwrap();
    let control = RdbSaveControl::new(0, 0);

    let result = commit_temporary_snapshot(&temporary, &destination, &control, || {
        Err(io::Error::other("injected directory sync failure"))
    });

    assert!(result.is_err());
    assert!(!control.lock().is_committed);
    assert_eq!(std::fs::read(destination).unwrap(), b"snapshot");
    let _ = std::fs::remove_dir_all(dir);
}
