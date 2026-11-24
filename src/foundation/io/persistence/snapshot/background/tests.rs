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
    control.finish_item().unwrap();
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
