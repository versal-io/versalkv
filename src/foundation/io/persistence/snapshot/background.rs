//! Cancellable background file saving with owned frozen input.

use super::rdb::RDB_AUTOSYNC_BYTES;
use crate::foundation::io::persistence::rdb::save_key_value;
use crate::foundation::io::persistence::rdb::{
    compression::{new_rdb_frame_encoder, RdbCompression, RdbSnapshotOptions, VCS_RDB_HEADER},
    frozen::{ForklessItemId, FrozenRdb, FrozenWorkItem},
    RdbSerializeOptions,
};
use crate::foundation::io::persistence::{
    crc64, save_len, RDB_OPCODE_EOF, RDB_OPCODE_RESIZEDB, RDB_OPCODE_SELECTDB,
};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct SaveCancelled;

impl std::fmt::Display for SaveCancelled {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Background RDB save cancelled")
    }
}

impl std::error::Error for SaveCancelled {}

struct TemporarySnapshot(PathBuf);

impl Drop for TemporarySnapshot {
    fn drop(&mut self) {
        // A successful rename has already removed this path. Other exits,
        // including unwinding, must not leave an abandoned temporary snapshot.
        let _ = std::fs::remove_file(&self.0);
    }
}

struct IncrementalSyncWriter {
    file: std::fs::File,
    unsynced: usize,
    should_sync: bool,
}

impl IncrementalSyncWriter {
    fn new(file: std::fs::File, should_sync: bool) -> Self {
        Self {
            file,
            unsynced: 0,
            should_sync,
        }
    }

    fn sync_all(mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.sync_all()
    }
}

impl Write for IncrementalSyncWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.file.write(bytes)?;
        self.unsynced = self.unsynced.saturating_add(written);
        if self.should_sync && self.unsynced >= RDB_AUTOSYNC_BYTES {
            self.file.sync_data()?;
            self.unsynced = 0;
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

struct ControlState {
    is_cancelled: bool,
    is_committed: bool,
    key_delay_us: i64,
    processed: usize,
    queued: usize,
    enqueued: usize,
    item_started: Option<Instant>,
}

pub struct RdbSaveControl {
    started: Instant,
    total_keys: usize,
    state: Mutex<ControlState>,
    changed: Condvar,
}

const WORK_QUEUE_CAPACITY: usize = 64;

pub(crate) enum FeedError {
    Full(Box<FrozenWorkItem>),
    Closed,
}

pub(crate) struct RdbSaveFeeder {
    sender: Option<SyncSender<FrozenWorkItem>>,
    control: Arc<RdbSaveControl>,
}

impl RdbSaveFeeder {
    fn new(sender: SyncSender<FrozenWorkItem>, control: Arc<RdbSaveControl>) -> Self {
        Self {
            sender: Some(sender),
            control,
        }
    }

    pub(crate) fn try_send(&self, item: FrozenWorkItem) -> Result<(), FeedError> {
        let is_entry = matches!(item, FrozenWorkItem::Entry(_));
        if is_entry && self.control.queued_item().is_err() {
            return Err(FeedError::Closed);
        }
        let Some(sender) = &self.sender else {
            if is_entry {
                self.control.unqueued_item();
            }
            return Err(FeedError::Closed);
        };
        match sender.try_send(item) {
            Ok(()) => Ok(()),
            Err(std::sync::mpsc::TrySendError::Full(item)) => {
                if is_entry {
                    self.control.unqueued_item();
                }
                Err(FeedError::Full(Box::new(item)))
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_item)) => {
                if is_entry {
                    self.control.unqueued_item();
                }
                Err(FeedError::Closed)
            }
        }
    }

    pub(crate) fn finish(&mut self) {
        self.sender.take();
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RdbSaveProgress {
    pub total_keys: usize,
    pub processed: usize,
    pub enqueued: usize,
    pub queue_length: usize,
    pub queue_length_target: usize,
    pub runtime_ms: u64,
    pub current_item_ms: i64,
}

impl RdbSaveProgress {
    pub fn estimated_seconds_remaining(self) -> i64 {
        (self.total_keys.saturating_sub(self.processed) as u64)
            .saturating_mul(self.runtime_ms)
            .checked_div(self.processed as u64)
            .map_or(-1, |ms| (ms / 1000) as i64)
    }
}

impl RdbSaveControl {
    fn new(total_keys: usize, key_delay_us: i64) -> Self {
        Self {
            started: Instant::now(),
            total_keys,
            state: Mutex::new(ControlState {
                is_cancelled: false,
                is_committed: false,
                key_delay_us,
                processed: 0,
                queued: 0,
                enqueued: 0,
                item_started: None,
            }),
            changed: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, ControlState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn cancel(&self) {
        let mut state = self.lock();
        if !state.is_committed {
            state.is_cancelled = true;
            self.changed.notify_all();
        }
    }

    pub fn set_key_delay(&self, key_delay_us: i64) {
        self.lock().key_delay_us = key_delay_us;
        self.changed.notify_all();
    }

    pub fn progress(&self) -> RdbSaveProgress {
        let state = self.lock();
        RdbSaveProgress {
            total_keys: self.total_keys,
            processed: state.processed,
            enqueued: state.enqueued,
            queue_length: state
                .queued
                .saturating_add(usize::from(state.item_started.is_some())),
            queue_length_target: WORK_QUEUE_CAPACITY,
            runtime_ms: self.started.elapsed().as_millis() as u64,
            current_item_ms: state
                .item_started
                .map_or(0, |t| t.elapsed().as_millis() as i64),
        }
    }

    pub(crate) fn check_cancelled(&self) -> io::Result<()> {
        if self.lock().is_cancelled {
            Err(io::Error::new(io::ErrorKind::Interrupted, SaveCancelled))
        } else {
            Ok(())
        }
    }

    pub(crate) fn begin_item(&self) -> io::Result<()> {
        let mut state = self.lock();
        if state.is_cancelled {
            return Err(io::Error::new(io::ErrorKind::Interrupted, SaveCancelled));
        }
        state.queued = state.queued.saturating_sub(1);
        state.item_started = Some(Instant::now());
        Ok(())
    }

    fn queued_item(&self) -> io::Result<()> {
        let mut state = self.lock();
        if state.is_cancelled {
            return Err(io::Error::new(io::ErrorKind::Interrupted, SaveCancelled));
        }
        state.queued = state.queued.saturating_add(1);
        state.enqueued = state.enqueued.saturating_add(1);
        Ok(())
    }

    fn unqueued_item(&self) {
        let mut state = self.lock();
        state.queued = state.queued.saturating_sub(1);
        state.enqueued = state.enqueued.saturating_sub(1);
    }

    pub(crate) fn finish_item(&self) -> io::Result<()> {
        let delay_started = Instant::now();
        let mut state = self.lock();
        if state.key_delay_us < 0 {
            let delay_us = state.key_delay_us;
            drop(state);
            crate::foundation::io::sys::time::sleep_debug_delay_interruptible(delay_us, || {
                let current = self.lock();
                current.is_cancelled || current.key_delay_us != delay_us
            });
            state = self.lock();
        }
        while !state.is_cancelled {
            let delay = Duration::from_micros(state.key_delay_us.max(0) as u64);
            let elapsed = delay_started.elapsed();
            if elapsed >= delay {
                state.processed += 1;
                state.item_started = None;
                return Ok(());
            }
            state = self
                .changed
                .wait_timeout(state, delay - elapsed)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        Err(io::Error::new(io::ErrorKind::Interrupted, SaveCancelled))
    }
}

pub enum RdbSaveOutcome {
    Saved,
    Cancelled,
    Failed(io::Error),
}

pub struct BackgroundRdbSave {
    control: Arc<RdbSaveControl>,
    thread: Option<JoinHandle<RdbSaveOutcome>>,
    completed_items: Option<Receiver<ForklessItemId>>,
}

fn feed_work_items(
    frozen: FrozenRdb,
    sender: SyncSender<FrozenWorkItem>,
    control: Arc<RdbSaveControl>,
) {
    for item in frozen.into_work_items() {
        if control.check_cancelled().is_err() {
            break;
        }
        if matches!(item, FrozenWorkItem::Entry(_)) && control.queued_item().is_err() {
            break;
        }
        if sender.send(item).is_err() {
            break;
        }
    }
}

fn serialize_work_items<F>(
    receiver: Receiver<FrozenWorkItem>,
    options: RdbSerializeOptions,
    control: &RdbSaveControl,
    completed_items: Option<&Sender<ForklessItemId>>,
    mut emit: F,
) -> io::Result<()>
where
    F: FnMut(&[u8]) -> io::Result<()>,
{
    let mut crc = 0;
    let mut write = |bytes: &[u8]| {
        control.check_cancelled()?;
        crc = crc64(crc, bytes);
        emit(bytes)
    };
    while let Ok(item) = receiver.recv() {
        match item {
            FrozenWorkItem::Header(header) => write(&header)?,
            FrozenWorkItem::Database {
                id,
                entries,
                expires,
            } => {
                let mut chunk = Vec::new();
                chunk.push(RDB_OPCODE_SELECTDB);
                save_len(&mut chunk, id as u64);
                chunk.push(RDB_OPCODE_RESIZEDB);
                save_len(&mut chunk, entries as u64);
                save_len(&mut chunk, expires as u64);
                write(&chunk)?;
            }
            FrozenWorkItem::Entry(entry) => {
                control.begin_item()?;
                let mut chunk = Vec::new();
                save_key_value(
                    &mut chunk,
                    &entry.key,
                    entry.value.as_ref(),
                    entry.expire_ms,
                    entry.hash_field_expires.as_ref(),
                    options.compression,
                );
                write(&chunk)?;
                let completion = entry.completion.clone();
                drop(entry);
                control.finish_item()?;
                if let (Some(completion), Some(sender)) = (completion, completed_items) {
                    let _ = sender.send(completion);
                }
            }
        }
    }
    write(&[RDB_OPCODE_EOF])?;
    let checksum: u64 = if options.checksum { crc } else { 0 };
    emit(&checksum.to_le_bytes())
}

fn commit_temporary_snapshot<F>(
    temporary: &std::path::Path,
    destination: &std::path::Path,
    control: &RdbSaveControl,
    sync_directory: F,
) -> io::Result<()>
where
    F: FnOnce() -> io::Result<()>,
{
    let mut state = control.lock();
    if state.is_cancelled {
        return Err(io::Error::new(io::ErrorKind::Interrupted, SaveCancelled));
    }
    std::fs::rename(temporary, destination)?;
    sync_directory()?;
    state.is_committed = true;
    Ok(())
}

impl BackgroundRdbSave {
    pub(crate) fn start_incremental(
        total_keys: usize,
        dir: PathBuf,
        filename: String,
        save_counter: u64,
        key_delay_us: i64,
        should_incremental_fsync: bool,
        serialization: RdbSnapshotOptions,
    ) -> io::Result<(Self, RdbSaveFeeder)> {
        let temporary = dir.join(format!(
            "temp-forkless-{}-{save_counter}.rdb",
            std::process::id()
        ));
        let file = std::fs::File::create(&temporary)?;
        let control = Arc::new(RdbSaveControl::new(total_keys, key_delay_us));
        let worker_control = Arc::clone(&control);
        let (sender, receiver) = sync_channel(WORK_QUEUE_CAPACITY);
        let (completed_sender, completed_receiver) = channel();
        let worker_temporary = temporary.clone();
        let thread = std::thread::Builder::new()
            .name("rdb-save".into())
            .spawn(move || {
                let temporary_snapshot = TemporarySnapshot(worker_temporary);
                let options = RdbSerializeOptions {
                    compression: serialization.compression.uses_lzf(),
                    checksum: serialization.checksum
                        && serialization.compression != RdbCompression::Lz4,
                };
                let mut file = file;
                let result: io::Result<()> = (|| {
                    let serialize = |emit: &mut dyn FnMut(&[u8]) -> io::Result<()>| {
                        serialize_work_items(
                            receiver,
                            options,
                            &worker_control,
                            Some(&completed_sender),
                            |chunk| emit(chunk),
                        )
                    };
                    if serialization.compression == RdbCompression::Lz4 {
                        let mut writer = IncrementalSyncWriter::new(file, should_incremental_fsync);
                        writer.write_all(VCS_RDB_HEADER)?;
                        let mut encoder = new_rdb_frame_encoder(writer, serialization.checksum);
                        serialize(&mut |chunk| encoder.write_all(chunk))?;
                        let writer = encoder.finish().map_err(io::Error::other)?;
                        worker_control.check_cancelled()?;
                        writer.sync_all()?;
                    } else {
                        let mut unsynced = 0;
                        serialize(&mut |chunk| {
                            for bytes in chunk.chunks(RDB_AUTOSYNC_BYTES) {
                                worker_control.check_cancelled()?;
                                file.write_all(bytes)?;
                                unsynced += bytes.len();
                                if should_incremental_fsync && unsynced >= RDB_AUTOSYNC_BYTES {
                                    file.sync_data()?;
                                    unsynced = 0;
                                }
                            }
                            Ok(())
                        })?;
                        worker_control.check_cancelled()?;
                        file.flush()?;
                        file.sync_all()?;
                        drop(file);
                    }
                    worker_control.check_cancelled()?;
                    let directory = std::fs::File::open(&dir)?;
                    commit_temporary_snapshot(
                        &temporary_snapshot.0,
                        &dir.join(filename),
                        &worker_control,
                        || directory.sync_all(),
                    )?;
                    Ok(())
                })();
                match result {
                    Ok(()) => RdbSaveOutcome::Saved,
                    Err(error)
                        if error
                            .get_ref()
                            .is_some_and(|cause| cause.is::<SaveCancelled>()) =>
                    {
                        RdbSaveOutcome::Cancelled
                    }
                    Err(error) => RdbSaveOutcome::Failed(error),
                }
            })?;
        let feeder = RdbSaveFeeder::new(sender, Arc::clone(&control));
        Ok((
            Self {
                control,
                thread: Some(thread),
                completed_items: Some(completed_receiver),
            },
            feeder,
        ))
    }

    pub fn start(
        frozen: FrozenRdb,
        dir: PathBuf,
        filename: String,
        save_counter: u64,
        key_delay_us: i64,
        should_incremental_fsync: bool,
        serialization: RdbSnapshotOptions,
    ) -> io::Result<Self> {
        let temporary = dir.join(format!(
            "temp-forkless-{}-{save_counter}.rdb",
            std::process::id()
        ));
        // Validate and open the destination before acknowledging BGSAVE.
        let mut file = std::fs::File::create(&temporary)?;
        let control = Arc::new(RdbSaveControl::new(frozen.total_keys(), key_delay_us));
        let worker_control = Arc::clone(&control);
        let feeder_control = Arc::clone(&control);
        let (sender, receiver) = sync_channel(WORK_QUEUE_CAPACITY);
        let feeder = std::thread::Builder::new()
            .name("rdb-save-feed".into())
            .spawn(move || feed_work_items(frozen, sender, feeder_control))?;
        let worker_temporary = temporary.clone();
        let thread = std::thread::Builder::new()
            .name("rdb-save".into())
            .spawn(move || {
                let temporary_snapshot = TemporarySnapshot(worker_temporary);
                let options = RdbSerializeOptions {
                    compression: serialization.compression.uses_lzf(),
                    checksum: serialization.checksum
                        && serialization.compression != RdbCompression::Lz4,
                };
                let result: io::Result<()> = (|| {
                    let serialize = |emit: &mut dyn FnMut(&[u8]) -> io::Result<()>| {
                        serialize_work_items(receiver, options, &worker_control, None, |chunk| {
                            emit(chunk)
                        })
                    };
                    if serialization.compression == RdbCompression::Lz4 {
                        let mut writer = IncrementalSyncWriter::new(file, should_incremental_fsync);
                        writer.write_all(VCS_RDB_HEADER)?;
                        let mut encoder = new_rdb_frame_encoder(writer, serialization.checksum);
                        serialize(&mut |chunk| encoder.write_all(chunk))?;
                        let writer = encoder.finish().map_err(io::Error::other)?;
                        worker_control.check_cancelled()?;
                        writer.sync_all()?;
                    } else {
                        let mut unsynced = 0;
                        serialize(&mut |chunk| {
                            for bytes in chunk.chunks(RDB_AUTOSYNC_BYTES) {
                                worker_control.check_cancelled()?;
                                file.write_all(bytes)?;
                                unsynced += bytes.len();
                                if should_incremental_fsync && unsynced >= RDB_AUTOSYNC_BYTES {
                                    file.sync_data()?;
                                    unsynced = 0;
                                }
                            }
                            Ok(())
                        })?;
                        worker_control.check_cancelled()?;
                        file.flush()?;
                        file.sync_all()?;
                        drop(file);
                    }
                    // Cancellation and the durable commit are serialized. A cancel
                    // arriving after this boundary must not undo a successful save.
                    let directory = std::fs::File::open(&dir)?;
                    commit_temporary_snapshot(
                        &temporary_snapshot.0,
                        &dir.join(filename),
                        &worker_control,
                        || directory.sync_all(),
                    )?;
                    Ok(())
                })();
                let _ = feeder.join();
                match result {
                    Ok(()) => RdbSaveOutcome::Saved,
                    Err(error)
                        if error
                            .get_ref()
                            .is_some_and(|cause| cause.is::<SaveCancelled>()) =>
                    {
                        RdbSaveOutcome::Cancelled
                    }
                    Err(error) => RdbSaveOutcome::Failed(error),
                }
            });
        match thread {
            Ok(thread) => Ok(Self {
                control,
                thread: Some(thread),
                completed_items: None,
            }),
            Err(error) => {
                let _ = std::fs::remove_file(temporary);
                Err(error)
            }
        }
    }

    pub fn control(&self) -> &RdbSaveControl {
        &self.control
    }

    pub fn is_finished(&self) -> bool {
        self.thread.as_ref().is_none_or(JoinHandle::is_finished)
    }

    pub(crate) fn take_completed_items(&self) -> Vec<ForklessItemId> {
        self.completed_items
            .as_ref()
            .map(|receiver| receiver.try_iter().collect())
            .unwrap_or_default()
    }

    pub fn finish(&mut self) -> RdbSaveOutcome {
        self.thread
            .take()
            .map_or(RdbSaveOutcome::Cancelled, |thread| {
                thread.join().unwrap_or_else(|_| {
                    RdbSaveOutcome::Failed(io::Error::other("Background RDB save thread failed"))
                })
            })
    }
}

impl Drop for BackgroundRdbSave {
    fn drop(&mut self) {
        self.control.cancel();
        let _ = self.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
