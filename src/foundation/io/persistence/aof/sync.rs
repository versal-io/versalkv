use crate::foundation::io::transport::TransportWakeHandle;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
static LOGICAL_OFFSET: AtomicU64 = AtomicU64::new(0);

/// Identifies an accepted AOF prefix, including bytes still buffered in memory.
/// Offsets are process-wide logical positions, not physical file lengths.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AofWriteStamp {
    pub generation: u64,
    pub offset: u64,
}

pub(super) fn reserve_logical_offset(bytes: u64) -> u64 {
    // Exhaustion requires accepting more than 16 EiB in one process lifetime.
    let previous = LOGICAL_OFFSET
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |offset| {
            offset.checked_add(bytes)
        })
        .expect("AOF logical offset exhausted");
    previous + bytes
}

pub(super) fn new_stamp() -> io::Result<AofWriteStamp> {
    let generation = NEXT_GENERATION
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |generation| {
            generation.checked_add(1)
        })
        .map_err(|_| io::Error::other("AOF generation exhausted"))?;
    Ok(AofWriteStamp {
        generation,
        offset: reserve_logical_offset(1),
    })
}

struct SyncState {
    written: AofWriteStamp,
    durable: Option<AofWriteStamp>,
    last_error: Option<io::ErrorKind>,
    waker: Option<TransportWakeHandle>,
}

struct SyncProgress {
    state: Mutex<SyncState>,
    // Serialize snapshot -> sync -> completion, but never block appends.
    sync_gate: Mutex<()>,
}

/// Shared completion state. A successful write alone never establishes durability.
#[derive(Clone)]
pub struct AofSyncHandle(Arc<SyncProgress>);

impl fmt::Debug for AofSyncHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state();
        formatter
            .debug_struct("AofSyncHandle")
            .field("written", &state.written)
            .field("durable", &state.durable)
            .field("last_error", &state.last_error)
            .finish_non_exhaustive()
    }
}

impl AofSyncHandle {
    pub(super) fn new(written: AofWriteStamp) -> Self {
        Self(Arc::new(SyncProgress {
            state: Mutex::new(SyncState {
                written,
                durable: None,
                last_error: None,
                waker: None,
            }),
            sync_gate: Mutex::new(()),
        }))
    }

    fn state(&self) -> MutexGuard<'_, SyncState> {
        self.0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    pub(super) fn sync_guard(&self) -> MutexGuard<'_, ()> {
        self.0
            .sync_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    /// Only stamps from this handle's current generation can be confirmed.
    /// A later error does not revoke a prefix that was already synced.
    pub fn is_durable(&self, stamp: AofWriteStamp) -> bool {
        let state = self.state();
        state.durable.is_some_and(|durable| {
            stamp.generation == state.written.generation
                && stamp.generation == durable.generation
                && stamp.offset <= durable.offset
        })
    }

    /// The latest sync failure, cleared by a later successful sync.
    pub fn last_error(&self) -> Option<io::ErrorKind> {
        self.state().last_error
    }

    pub(super) fn publish_written(&self, stamp: AofWriteStamp) {
        let mut state = self.state();
        debug_assert_eq!(stamp.generation, state.written.generation);
        debug_assert!(stamp.offset >= state.written.offset);
        state.written = stamp;
    }

    pub(super) fn reset(&self, stamp: AofWriteStamp) {
        let mut state = self.state();
        state.written = stamp;
        state.durable = None;
        state.last_error = None;
    }

    pub(super) fn set_waker(&self, waker: TransportWakeHandle) {
        self.state().waker = Some(waker);
    }

    fn complete(&self, snapshot: AofWriteStamp, result: &io::Result<()>) {
        let waker = {
            let mut state = self.state();
            if snapshot.generation != state.written.generation
                || snapshot.offset > state.written.offset
            {
                return;
            }
            match result {
                Ok(()) => {
                    if state
                        .durable
                        .is_none_or(|durable| snapshot.offset > durable.offset)
                    {
                        state.durable = Some(snapshot);
                    }
                    state.last_error = None;
                }
                Err(error) => state.last_error = Some(error.kind()),
            }
            state.waker.clone()
        };
        if let Some(waker) = waker {
            if let Err(error) = waker.wake() {
                log::warn!("Failed to wake AOF sync observer: {error}");
            }
        }
    }

    pub(super) fn sync_with(&self, sync: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        let _guard = self.sync_guard();
        let snapshot = self.state().written;
        let result = sync();
        self.complete(snapshot, &result);
        result
    }

    /// Inject a completion for deterministic in-crate runtime tests.
    /// Unwritten and obsolete-generation snapshots cannot advance progress.
    #[cfg(test)]
    pub fn complete_for_test(&self, snapshot: AofWriteStamp, result: io::Result<()>) {
        let _guard = self.sync_guard();
        self.complete(snapshot, &result);
    }

    #[cfg(test)]
    pub fn written_stamp_for_test(&self) -> AofWriteStamp {
        self.state().written
    }
}
