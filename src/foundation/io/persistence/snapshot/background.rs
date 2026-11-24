//! Cancellable background file saving with owned frozen input.

use super::rdb::RDB_AUTOSYNC_BYTES;
use crate::foundation::io::persistence::rdb::{
    compression::{new_rdb_frame_encoder, RdbCompression, RdbSnapshotOptions, VCS_RDB_HEADER},
    frozen::{ForklessItemId, FrozenRdb, FrozenWorkItem},
    RdbSerializeOptions,
};
use crate::foundation::io::persistence::rdb::{save_key_value, LzfWorkspace};
use crate::foundation::io::persistence::{
    crc64, save_len, RDB_OPCODE_EOF, RDB_OPCODE_RESIZEDB, RDB_OPCODE_SELECTDB,
};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

// Cancellation is terminal; Interrupted would make Write::write_all retry forever.
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

struct CancellablePipeWriter {
    file: std::fs::File,
    control: Arc<RdbSaveControl>,
    #[cfg(test)]
    test_wait: Option<PipeWriteTestWait>,
}

#[cfg(test)]
struct PipeWriteTestWait {
    entered: Sender<()>,
    resume: Receiver<()>,
    timeout: Duration,
}

impl Write for CancellablePipeWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        loop {
            self.control.check_cancelled()?;
            match self.file.write(bytes) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    let timeout = Duration::from_millis(1);
                    #[cfg(test)]
                    let timeout = if let Some(wait) = self.test_wait.take() {
                        wait.entered.send(()).map_err(io::Error::other)?;
                        wait.resume.recv().map_err(io::Error::other)?;
                        wait.timeout
                    } else {
                        timeout
                    };
                    // Wait for pipe space rather than an unrelated feed notification.
                    // Bound the wait so cancellation also works with an idle reader.
                    #[cfg(unix)]
                    {
                        use crate::foundation::io::sys::os::{poll_descriptors, PollDescriptor};
                        use std::os::fd::AsRawFd;

                        self.control.check_cancelled()?;
                        let mut descriptors =
                            [PollDescriptor::new(self.file.as_raw_fd(), libc::POLLOUT)];
                        match poll_descriptors(&mut descriptors, timeout) {
                            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                            Err(error) => return Err(error),
                            Ok(_) => {}
                        }
                    }
                    #[cfg(not(unix))]
                    {
                        let state = self.control.lock();
                        if state.is_cancelled {
                            return Err(io::Error::other(SaveCancelled));
                        }
                        drop(
                            self.control
                                .changed
                                .wait_timeout(state, timeout)
                                .unwrap_or_else(std::sync::PoisonError::into_inner),
                        );
                    }
                }
                result => return result,
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.control.check_cancelled()
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
    #[cfg(test)]
    key_delay_wait_us: Option<i64>,
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
            Ok(()) => {
                self.control.notify_feed_changed();
                Ok(())
            }
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
        self.control.notify_feed_changed();
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.sender.is_none() || self.control.check_cancelled().is_err()
    }
}

impl Drop for RdbSaveFeeder {
    fn drop(&mut self) {
        self.finish();
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
                #[cfg(test)]
                key_delay_wait_us: None,
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
        let mut state = self.lock();
        if state.key_delay_us != key_delay_us {
            state.key_delay_us = key_delay_us;
            self.changed.notify_all();
        }
    }

    /// Current per-key save delay in microseconds. Test-only helper used to
    /// assert that event-loop barriers never rewrite the configured pace.
    #[cfg(test)]
    pub fn key_delay_us_for_test(&self) -> i64 {
        self.lock().key_delay_us
    }

    /// Delay used by the current finish_item Condvar wait, not its configuration.
    /// The same state lock makes this observable only after the wait releases it.
    #[cfg(test)]
    pub fn key_delay_wait_us_for_test(&self) -> Option<i64> {
        self.lock().key_delay_wait_us
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
            Err(io::Error::other(SaveCancelled))
        } else {
            Ok(())
        }
    }

    fn notify_feed_changed(&self) {
        // Pair notification with the receiver's state lock so an enqueue or
        // disconnect between try_recv and wait cannot lose its wakeup.
        let _state = self.lock();
        self.changed.notify_all();
    }

    fn receive_work_item(
        &self,
        receiver: &Receiver<FrozenWorkItem>,
    ) -> io::Result<Option<FrozenWorkItem>> {
        let mut state = self.lock();
        loop {
            if state.is_cancelled {
                return Err(io::Error::other(SaveCancelled));
            }
            match receiver.try_recv() {
                Ok(item) => return Ok(Some(item)),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => return Ok(None),
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    state = self
                        .changed
                        .wait(state)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
            }
        }
    }

    pub(crate) fn begin_item(&self) -> io::Result<()> {
        let mut state = self.lock();
        if state.is_cancelled {
            return Err(io::Error::other(SaveCancelled));
        }
        state.queued = state.queued.saturating_sub(1);
        state.item_started = Some(Instant::now());
        Ok(())
    }

    fn queued_item(&self) -> io::Result<()> {
        let mut state = self.lock();
        if state.is_cancelled {
            return Err(io::Error::other(SaveCancelled));
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

    pub(crate) fn finish_item<F>(&self, flush: F) -> io::Result<()>
    where
        F: FnOnce() -> io::Result<()>,
    {
        let mut state = self.lock();
        if !state.is_cancelled && state.key_delay_us != 0 {
            // Select the wait while holding the configuration lock, but never
            // hold that lock during a potentially backpressured output flush.
            drop(state);
            flush()?;
            state = self.lock();
        }
        let delay_started = Instant::now();
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
            #[cfg(test)]
            {
                state.key_delay_wait_us = Some(state.key_delay_us);
            }
            state = self
                .changed
                .wait_timeout(state, delay - elapsed)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
            #[cfg(test)]
            {
                state.key_delay_wait_us = None;
            }
        }
        Err(io::Error::other(SaveCancelled))
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
        control.notify_feed_changed();
    }
    drop(sender);
    control.notify_feed_changed();
}

enum RdbOutput<'a> {
    Bytes(&'a [u8]),
    Flush,
}

fn serialize_work_items<F>(
    receiver: Receiver<FrozenWorkItem>,
    options: RdbSerializeOptions,
    control: &RdbSaveControl,
    completed_items: Option<&Sender<ForklessItemId>>,
    mut emit: F,
) -> io::Result<()>
where
    F: FnMut(RdbOutput<'_>) -> io::Result<()>,
{
    let mut crc = 0;
    let mut ws = LzfWorkspace::new(options.compression);
    let mut write = |output: RdbOutput<'_>| {
        control.check_cancelled()?;
        if let RdbOutput::Bytes(bytes) = output {
            crc = crc64(crc, bytes);
        }
        emit(output)
    };
    while let Some(item) = control.receive_work_item(&receiver)? {
        match item {
            FrozenWorkItem::Header(header) => write(RdbOutput::Bytes(&header))?,
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
                write(RdbOutput::Bytes(&chunk))?;
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
                    &mut ws,
                );
                write(RdbOutput::Bytes(&chunk))?;
                let completion = entry.completion.clone();
                drop(entry);
                if let (Some(completion), Some(sender)) = (completion, completed_items) {
                    let _ = sender.send(completion);
                }
                // Publish progress only after the release notification is visible.
                control.finish_item(|| write(RdbOutput::Flush))?;
            }
        }
    }
    write(RdbOutput::Bytes(&[RDB_OPCODE_EOF]))?;
    let checksum: u64 = if options.checksum { crc } else { 0 };
    emit(RdbOutput::Bytes(&checksum.to_le_bytes()))
}

fn serialize_pipe_work_items<W: Write>(
    receiver: Receiver<FrozenWorkItem>,
    options: RdbSerializeOptions,
    control: &RdbSaveControl,
    writer: W,
) -> io::Result<()> {
    let mut writer = io::BufWriter::with_capacity(16 * 1024, writer);
    let result = serialize_work_items(receiver, options, control, None, |output| match output {
        RdbOutput::Bytes(chunk) => writer.write_all(chunk),
        RdbOutput::Flush => writer.flush(),
    })
    .and_then(|()| writer.flush());
    // Never let BufWriter's destructor retry after a write error or cancel.
    // A successful finish already flushed the RDB checksum.
    let (_writer, _pending) = writer.into_parts();
    result
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
        return Err(io::Error::other(SaveCancelled));
    }
    std::fs::rename(temporary, destination)?;
    sync_directory()?;
    state.is_committed = true;
    Ok(())
}

impl BackgroundRdbSave {
    /// Serialize owned snapshot entries directly into a bounded OS pipe.
    pub fn start_to_pipe(
        frozen: FrozenRdb,
        key_delay_us: i64,
        serialization: RdbSnapshotOptions,
    ) -> io::Result<(Self, std::fs::File)> {
        let (read, write) = crate::foundation::io::sys::fd::pipe(
            crate::foundation::io::sys::fd::PipeFlags::CLOEXEC_NONBLOCK,
        )?
        .into_parts();
        let control = Arc::new(RdbSaveControl::new(frozen.total_keys(), key_delay_us));
        let feeder_control = Arc::clone(&control);
        let worker_control = Arc::clone(&control);
        let (sender, receiver) = sync_channel(WORK_QUEUE_CAPACITY);
        let feeder = std::thread::Builder::new()
            .name("rdb-sync-feed".into())
            .spawn(move || feed_work_items(frozen, sender, feeder_control))?;
        let thread = std::thread::Builder::new()
            .name("rdb-sync".into())
            .spawn(move || {
                let options = RdbSerializeOptions {
                    compression: serialization.compression.uses_lzf(),
                    checksum: serialization.checksum
                        && serialization.compression != RdbCompression::Lz4,
                };
                let mut writer = CancellablePipeWriter {
                    file: write,
                    control: Arc::clone(&worker_control),
                    #[cfg(test)]
                    test_wait: None,
                };
                let result = if serialization.compression == RdbCompression::Lz4 {
                    (|| -> io::Result<()> {
                        writer.write_all(VCS_RDB_HEADER)?;
                        let mut encoder = new_rdb_frame_encoder(writer, serialization.checksum);
                        serialize_work_items(receiver, options, &worker_control, None, |output| {
                            match output {
                                RdbOutput::Bytes(chunk) => encoder.write_all(chunk),
                                RdbOutput::Flush => Ok(()),
                            }
                        })?;
                        encoder.finish().map_err(io::Error::other)?;
                        Ok(())
                    })()
                } else {
                    serialize_pipe_work_items(receiver, options, &worker_control, writer)
                };
                // Dropping the receiver unblocks the feeder on every error.
                // The pipe writer closes before a successful result is joined.
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
            })?;
        Ok((
            Self {
                control,
                thread: Some(thread),
                completed_items: None,
            },
            read,
        ))
    }

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
                            |output| match output {
                                RdbOutput::Bytes(chunk) => emit(chunk),
                                RdbOutput::Flush => Ok(()),
                            },
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
                        serialize_work_items(receiver, options, &worker_control, None, |output| {
                            match output {
                                RdbOutput::Bytes(chunk) => emit(chunk),
                                RdbOutput::Flush => Ok(()),
                            }
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
mod tests;
