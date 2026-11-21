use crate::engine::extension::config::persistence::BgsaveMethod;
use crate::foundation::io::persistence::journal::AofJournal;
use crate::foundation::io::persistence::rdb::frozen::{ForklessFeed, ForklessItemId};
use crate::foundation::io::persistence::snapshot::background::{
    BackgroundRdbSave, RdbSaveOutcome, RdbSaveProgress,
};
use crate::foundation::io::store::kv_engine::DbSet;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationStatus {
    Ok,
    Err,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForklessSavePurpose {
    Persistence,
    ReplicationFile,
    ReplicationSocket,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceChild {
    Rdb {
        pid: i32,
        dirty_at_fork: i64,
    },
    AofRewrite {
        pid: i32,
        started_ms: i64,
    },
    // Diskless replication full-sync child: serializes the RDB to a pipe for
    // the parent to relay. Not a disk save, so it carries no dirty snapshot.
    DisklessSync {
        pid: i32,
    },
    // Disk-based replication full-sync child: serializes the RDB (prefixed with
    // its own `$<len>\r\n` bulk header) to a pipe for the parent to relay to a
    // replica that cannot accept the EOF-framed diskless stream. Distinct from
    // `DisklessSync` so its relay (tracked separately in the event loop) is
    // reaped by its own cron fallback, keeping each reap gate sound. Not a disk
    // save, so it carries no dirty snapshot.
    DiskSync {
        pid: i32,
    },
    /// Slot-filtered RESP snapshot child used by atomic Cluster migration.
    SlotMigration {
        pid: i32,
    },
}

#[derive(Debug)]
pub struct RdbState {
    pub last_save_time_sec: i64,
    pub last_bgsave_status: OperationStatus,
    pub last_bgsave_try_sec: i64,
    pub saves: u64,
    pub current_bgsave_type: Option<BgsaveMethod>,
    pub last_bgsave_type: Option<BgsaveMethod>,
    pub last_bgsave_duration_sec: i64,
    started: Option<Instant>,
    pub last_load_keys_loaded: usize,
    pub last_load_keys_expired: usize,
    pub last_cow_size: usize,
    pub current_cow_size: usize,
    pub current_save_keys_processed: usize,
    pub current_save_keys_total: usize,
    pub current_fork_perc: f64,
}

pub struct AofRewriteState {
    pub buffer: Vec<u8>,
    pub force_select: bool,
    pub last_duration_sec: i64,
    pub last_status: OperationStatus,
    pub total_rewrites: u64,
    pub consecutive_failures: u32,
    pub limit_logged: bool,
}

pub struct AofState {
    pub journal: Option<AofJournal>,
    pub started_this_session: bool,
    pub current_size: u64,
    pub rewrite_base_size: u64,
    /// Largest replication offset confirmed to be fsynced to the AOF. Mirrors
    /// C's `server.fsynced_reploff`: `None` matches C's `-1` sentinel meaning
    /// "do not report an AOF ack" (AOF disabled, or an initial rewrite has not
    /// yet completed). Only this value is consumed by WAITAOF / FACK.
    pub fsynced_repl_offset: Option<i64>,
    /// Optimistic companion to `fsynced_repl_offset`. Mirrors C's atomic
    /// `server.fsynced_reploff_pending`: the largest replication offset whose
    /// data has been handed to an fsync. It is promoted into
    /// `fsynced_repl_offset` only while the AOF is fully on (not during the
    /// initial rewrite). Stays frozen while `appendfsync no` is active.
    pub fsynced_repl_offset_pending: i64,
    /// True while an initial AOF rewrite is outstanding (C's AOF_WAIT_REWRITE).
    /// While set, `fsynced_repl_offset_pending` must not be promoted into
    /// `fsynced_repl_offset`, because not all data is guaranteed durable yet.
    pub aof_wait_rewrite: bool,
    pub last_fsync_sec: i64,
    pub last_write_status: OperationStatus,
    pub delayed_fsync: u64,
    pub last_timestamp_annotation_sec: i64,
    pub rewrite: AofRewriteState,
}

impl AofState {
    pub fn buffer_allocation_bytes(&self) -> usize {
        self.journal
            .as_ref()
            .map_or(0, |journal| journal.appender.buffer_allocation_bytes())
    }

    /// Optimistically record that data up to `offset` has been handed to an
    /// fsync. Mirrors the `atomic_store(fsynced_reploff_pending, ...)` calls in
    /// C's `flushAppendOnlyFile`, `aof_background_fsync`, and the bio worker.
    pub fn set_fsynced_pending(&mut self, offset: i64) {
        self.fsynced_repl_offset_pending = offset;
    }

    /// Promote the optimistic pending offset into the confirmed one. Mirrors
    /// C's `beforeSleep` advance: it is a no-op while an initial rewrite is
    /// outstanding (`aof_wait_rewrite`) or while the AOF reports nothing
    /// (`fsynced_repl_offset == None`, i.e. C's `-1`). Returns the new
    /// confirmed offset when it actually changed, so callers can wake blocked
    /// WAITAOF clients.
    pub fn advance_fsynced_from_pending(&mut self) -> Option<i64> {
        if self.aof_wait_rewrite || self.fsynced_repl_offset.is_none() {
            return None;
        }
        let pending = self.fsynced_repl_offset_pending;
        if self.fsynced_repl_offset == Some(pending) {
            return None;
        }
        self.fsynced_repl_offset = Some(pending);
        Some(pending)
    }

    /// Reset both offsets to C's "AOF disabled" state: confirmed `-1` (never
    /// report a FACK) and pending `0`. Mirrors `stopAppendOnly`.
    pub fn reset_fsynced_disabled(&mut self) {
        self.fsynced_repl_offset = None;
        self.fsynced_repl_offset_pending = 0;
        self.aof_wait_rewrite = false;
    }
}

pub struct PersistenceState {
    dirty: i64,
    aof_selected_db: i64,
    rdb: RdbState,
    aof: AofState,
    child: Option<PersistenceChild>,
    forkless: Option<(BackgroundRdbSave, i64)>,
    forkless_purpose: ForklessSavePurpose,
    forkless_feed: Option<ForklessFeed>,
    forkless_keys: HashSet<ForklessItemId>,
    forkless_captured_keys: HashSet<ForklessItemId>,
    forkless_eviction_keys: HashMap<u32, CapturedEvictionKeys>,
    forkless_current_to_original_db: HashMap<u32, u32>,
    forkless_original_to_current_db: HashMap<u32, u32>,
    bgsave_scheduled: Option<BgsaveMethod>,
    aof_rewrite_scheduled: bool,
}

#[derive(Default)]
struct CapturedEvictionKeys {
    keys: Vec<Vec<u8>>,
    cursor: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateError {
    ChildAlreadyRunning,
    WrongChild,
}

impl PersistenceState {
    pub fn new() -> Self {
        Self {
            dirty: 0,
            aof_selected_db: -1,
            rdb: RdbState {
                last_save_time_sec: 0,
                last_bgsave_status: OperationStatus::Ok,
                last_bgsave_try_sec: 0,
                saves: 0,
                current_bgsave_type: None,
                last_bgsave_type: None,
                last_bgsave_duration_sec: -1,
                started: None,
                last_load_keys_loaded: 0,
                last_load_keys_expired: 0,
                last_cow_size: 0,
                current_cow_size: 0,
                current_save_keys_processed: 0,
                current_save_keys_total: 0,
                current_fork_perc: 0.0,
            },
            aof: AofState {
                journal: None,
                started_this_session: false,
                current_size: 0,
                rewrite_base_size: 0,
                fsynced_repl_offset: None,
                fsynced_repl_offset_pending: 0,
                aof_wait_rewrite: false,
                last_fsync_sec: 0,
                last_write_status: OperationStatus::Ok,
                delayed_fsync: 0,
                last_timestamp_annotation_sec: -1,
                rewrite: AofRewriteState {
                    buffer: Vec::new(),
                    force_select: false,
                    last_duration_sec: -1,
                    last_status: OperationStatus::Ok,
                    total_rewrites: 0,
                    consecutive_failures: 0,
                    limit_logged: false,
                },
            },
            child: None,
            forkless: None,
            forkless_purpose: ForklessSavePurpose::Persistence,
            forkless_feed: None,
            forkless_keys: HashSet::new(),
            forkless_captured_keys: HashSet::new(),
            forkless_eviction_keys: HashMap::new(),
            forkless_current_to_original_db: HashMap::new(),
            forkless_original_to_current_db: HashMap::new(),
            bgsave_scheduled: None,
            aof_rewrite_scheduled: false,
        }
    }

    pub fn dirty(&self) -> i64 {
        self.dirty
    }

    pub fn add_dirty(&mut self, count: i64) {
        self.dirty = self.dirty.saturating_add(count);
    }

    pub fn set_dirty(&mut self, count: i64) {
        self.dirty = count;
    }

    pub fn aof_selected_db(&self) -> i64 {
        self.aof_selected_db
    }

    pub fn set_aof_selected_db(&mut self, db: i64) {
        self.aof_selected_db = db;
    }

    pub fn reset_aof_selected_db(&mut self) {
        self.aof_selected_db = -1;
    }

    pub fn record_rdb_save(&mut self, now_sec: i64) {
        self.dirty = 0;
        self.rdb.saves = self.rdb.saves.saturating_add(1);
        self.rdb.last_save_time_sec = now_sec;
        self.rdb.last_bgsave_status = OperationStatus::Ok;
    }

    pub fn record_promoted_rdb_save(&mut self, dirty_at_fork: i64, success: bool, now_sec: i64) {
        self.rdb.last_bgsave_try_sec = now_sec;
        self.rdb.last_bgsave_status = if success {
            self.rdb.last_save_time_sec = now_sec;
            self.dirty = self.dirty.saturating_sub(dirty_at_fork);
            OperationStatus::Ok
        } else {
            OperationStatus::Err
        };
        self.rdb.last_cow_size = self.rdb.current_cow_size;
        self.rdb.current_cow_size = 0;
        self.rdb.current_save_keys_processed = 0;
        self.rdb.current_save_keys_total = 0;
        self.rdb.current_fork_perc = 0.0;
    }

    pub fn record_rdb_load(&mut self, keys_loaded: usize, keys_expired: usize) {
        self.rdb.last_load_keys_loaded = keys_loaded;
        self.rdb.last_load_keys_expired = keys_expired;
    }

    pub fn begin_rdb_metrics(&mut self, total_keys: usize) {
        self.rdb.current_save_keys_total = total_keys;
        self.rdb.current_save_keys_processed = 0;
        self.rdb.current_cow_size = 0;
        self.rdb.current_fork_perc = 0.0;
    }

    pub fn sample_rdb_metrics(&mut self, dirty_delta: i64) {
        if self.child.is_none() {
            return;
        }
        let total = self.rdb.current_save_keys_total.max(1);
        let step = (total / 100).max(1);
        self.rdb.current_save_keys_processed = self
            .rdb
            .current_save_keys_processed
            .saturating_add(step)
            .min(total);
        self.rdb.current_fork_perc =
            self.rdb.current_save_keys_processed as f64 * 100.0 / total as f64;
        self.rdb.current_cow_size = (dirty_delta.max(0) as usize).saturating_mul(4096);
    }

    pub fn record_aof_size(&mut self, size: u64) {
        self.aof.current_size = size;
    }

    pub fn record_aof_rewrite_base_size(&mut self, size: u64) {
        self.aof.rewrite_base_size = size;
    }

    pub fn child(&self) -> Option<PersistenceChild> {
        self.child
    }

    fn replace_child(&mut self, child: Option<PersistenceChild>) -> Option<PersistenceChild> {
        let previous = std::mem::replace(&mut self.child, child);
        // Match the parent's fork registration/reset boundary, not the next cron.
        crate::foundation::io::store::structure::zset::ordered_index::set_leaf_compaction_allowed(
            self.child.is_none(),
        );
        previous
    }

    pub fn has_active_child(&self) -> bool {
        self.child.is_some() || self.forkless.is_some()
    }

    /// Equivalent to C `hasActiveChildProcess()`: only a real forked child is
    /// counted. A forkless background save runs without a child process and
    /// must not block slot-export snapshot scheduling on its own.
    pub fn has_child_process(&self) -> bool {
        self.child.is_some()
    }

    pub fn forkless_save_in_progress(&self) -> bool {
        self.forkless.is_some()
    }

    pub fn forkless_save_purpose(&self) -> Option<ForklessSavePurpose> {
        self.forkless.as_ref().map(|_| self.forkless_purpose)
    }

    pub fn begin_forkless_transfer(
        &mut self,
        task: BackgroundRdbSave,
        purpose: ForklessSavePurpose,
    ) -> Result<(), StateError> {
        if self.has_active_child() {
            return Err(StateError::ChildAlreadyRunning);
        }
        debug_assert!(purpose != ForklessSavePurpose::Persistence);
        self.begin_rdb_metrics(task.control().progress().total_keys);
        self.rdb.current_bgsave_type = Some(BgsaveMethod::Forkless);
        self.rdb.started = Some(Instant::now());
        self.forkless_purpose = purpose;
        self.forkless = Some((task, self.dirty));
        Ok(())
    }

    pub fn forkless_capture_in_progress(&self) -> bool {
        self.forkless_feed.is_some()
    }

    pub fn forkless_progress(&self) -> Option<RdbSaveProgress> {
        self.forkless
            .as_ref()
            .map(|(task, _)| task.control().progress())
    }

    pub fn current_bgsave_duration_sec(&self) -> i64 {
        self.rdb
            .started
            .map_or(-1, |started| started.elapsed().as_secs() as i64)
    }

    fn begin_save_metrics(&mut self, method: BgsaveMethod, now_sec: i64) {
        self.rdb.saves = self.rdb.saves.saturating_add(1);
        self.rdb.current_bgsave_type = Some(method);
        self.rdb.last_bgsave_try_sec = now_sec;
        self.rdb.started = Some(Instant::now());
    }

    fn begin_transfer_metrics(&mut self) {
        self.rdb.current_bgsave_type = Some(BgsaveMethod::Fork);
        self.rdb.started = Some(Instant::now());
    }

    fn clear_save_metrics(&mut self) {
        self.rdb.last_bgsave_duration_sec = self.current_bgsave_duration_sec();
        self.rdb.started = None;
        self.rdb.current_bgsave_type = None;
        self.rdb.current_cow_size = 0;
        self.rdb.current_save_keys_processed = 0;
        self.rdb.current_save_keys_total = 0;
        self.rdb.current_fork_perc = 0.0;
    }

    pub fn begin_forkless_save(
        &mut self,
        task: BackgroundRdbSave,
        now_sec: i64,
        forkless_keys: Vec<(u32, Vec<u8>)>,
        feed: Option<ForklessFeed>,
    ) -> Result<(), StateError> {
        if self.has_active_child() {
            return Err(StateError::ChildAlreadyRunning);
        }
        self.begin_rdb_metrics(task.control().progress().total_keys);
        self.begin_save_metrics(BgsaveMethod::Forkless, now_sec);
        self.forkless_purpose = ForklessSavePurpose::Persistence;
        self.forkless_keys = forkless_keys
            .into_iter()
            .map(|(db_id, key)| ForklessItemId {
                dataset_id: None,
                db_id,
                key,
            })
            .collect();
        self.forkless_captured_keys.clear();
        self.forkless_eviction_keys.clear();
        self.forkless_current_to_original_db.clear();
        self.forkless_original_to_current_db.clear();
        self.forkless_feed = feed;
        self.forkless = Some((task, self.dirty));
        Ok(())
    }

    pub fn drive_forkless_feed(
        &mut self,
        primary: &mut DbSet,
        additional_datasets: &mut [(Vec<u8>, &mut DbSet)],
    ) -> bool {
        let completed = self
            .forkless
            .as_ref()
            .map(|(task, _)| task.take_completed_items())
            .unwrap_or_default();
        let mut released_any = false;
        for item in completed {
            let current_item = self.forkless_item_at_current_db(&item);
            if release_forkless_item(primary, additional_datasets, &current_item) {
                released_any |= self.forkless_keys.remove(&item);
            }
        }
        if let Some(feed) = self.forkless_feed.as_mut() {
            feed.drive(primary, additional_datasets, 128);
            self.forkless_keys.extend(feed.take_queued_keys());
            let captured = feed.take_captured_keys();
            released_any |= !feed.take_resolved_keys().is_empty();
            if feed.is_finished() {
                self.forkless_feed = None;
            }
            self.record_forkless_captured_keys(captured);
        }
        released_any
    }

    pub fn forkless_key_blocked(&self, db_id: u32, key: &[u8]) -> bool {
        self.forkless_keys.contains(&ForklessItemId {
            dataset_id: None,
            db_id: self.forkless_original_db(db_id),
            key: key.to_vec(),
        })
    }

    pub fn forkless_key_captured(&self, db_id: u32, key: &[u8]) -> bool {
        self.forkless_captured_keys.contains(&ForklessItemId {
            dataset_id: None,
            db_id: self.forkless_original_db(db_id),
            key: key.to_vec(),
        })
    }

    fn record_forkless_captured_keys(&mut self, items: Vec<ForklessItemId>) {
        for item in items {
            if self.forkless_captured_keys.contains(&item) {
                continue;
            }
            if item.dataset_id.is_none() {
                self.forkless_eviction_keys
                    .entry(item.db_id)
                    .or_default()
                    .keys
                    .push(item.key.clone());
            }
            self.forkless_captured_keys.insert(item);
        }
    }

    /// Visit at most `budget` historical captures in the current primary DB.
    /// Missing live keys still consume budget; unrelated DBs are never scanned.
    pub(crate) fn forkless_eviction_candidates(
        &self,
        db_id: u32,
        budget: usize,
    ) -> impl Iterator<Item = &[u8]> {
        let (keys, start) = self
            .forkless_eviction_keys
            .get(&self.forkless_original_db(db_id))
            .map_or((&[][..], 0), |index| (index.keys.as_slice(), index.cursor));
        keys[start..]
            .iter()
            .chain(keys[..start].iter())
            .take(budget)
            .map(Vec::as_slice)
    }

    pub(crate) fn advance_forkless_eviction_candidates(&mut self, db_id: u32, budget: usize) {
        let original_db = self.forkless_original_db(db_id);
        if let Some(index) = self.forkless_eviction_keys.get_mut(&original_db) {
            let count = budget.min(index.keys.len());
            let remaining = index.keys.len() - index.cursor;
            index.cursor = if count < remaining {
                index.cursor + count
            } else {
                count - remaining
            };
        }
    }

    pub fn prioritize_forkless_keys(
        &mut self,
        db_id: u32,
        keys: impl IntoIterator<Item = Vec<u8>>,
    ) -> bool {
        let db_id = self.forkless_original_db(db_id);
        if let Some(feed) = self.forkless_feed.as_mut() {
            // Captured keys may still block writers until serialization ends.
            // They must wait for completion without being written a second time.
            let captured = &self.forkless_captured_keys;
            return feed.prioritize_keys(
                db_id,
                keys.into_iter().filter(|key| {
                    !captured.contains(&ForklessItemId {
                        dataset_id: None,
                        db_id,
                        key: key.clone(),
                    })
                }),
            );
        }
        false
    }

    pub fn swap_forkless_databases(&mut self, first: u32, second: u32) {
        if first == second || !self.forkless_save_in_progress() {
            return;
        }
        let first_original = self.forkless_original_db(first);
        let second_original = self.forkless_original_db(second);
        set_non_identity_mapping(
            &mut self.forkless_current_to_original_db,
            first,
            second_original,
        );
        set_non_identity_mapping(
            &mut self.forkless_current_to_original_db,
            second,
            first_original,
        );
        set_non_identity_mapping(
            &mut self.forkless_original_to_current_db,
            first_original,
            second,
        );
        set_non_identity_mapping(
            &mut self.forkless_original_to_current_db,
            second_original,
            first,
        );
        if let Some(feed) = self.forkless_feed.as_mut() {
            feed.swap_databases(first, second);
        }
    }

    fn forkless_original_db(&self, current_db: u32) -> u32 {
        self.forkless_current_to_original_db
            .get(&current_db)
            .copied()
            .unwrap_or(current_db)
    }

    fn forkless_item_at_current_db(&self, item: &ForklessItemId) -> ForklessItemId {
        let mut current = item.clone();
        if current.dataset_id.is_none() {
            current.db_id = self
                .forkless_original_to_current_db
                .get(&current.db_id)
                .copied()
                .unwrap_or(current.db_id);
        }
        current
    }

    pub fn set_forkless_key_delay(&self, delay_us: i64) {
        if let Some((task, _)) = &self.forkless {
            task.control().set_key_delay(delay_us);
        }
    }

    pub(crate) fn forkless_capture_poll_delay(&self) -> Option<Duration> {
        let feed = self.forkless_feed.as_ref()?;
        Some(if feed.is_backpressured() {
            // Retry a full feed promptly without spinning while its consumer
            // waits for a configured per-key delay or storage I/O.
            Duration::from_millis(1)
        } else {
            Duration::ZERO
        })
    }

    /// Current per-key save delay of the in-flight forkless save, if any.
    /// Test-only helper for asserting the configured save pace is preserved.
    #[cfg(test)]
    pub fn forkless_key_delay_for_test(&self) -> Option<i64> {
        self.forkless
            .as_ref()
            .map(|(task, _)| task.control().key_delay_us_for_test())
    }

    pub fn cancel_forkless_save(&self) {
        if let Some((task, _)) = &self.forkless {
            task.control().cancel();
        }
    }

    pub fn poll_forkless_save(
        &mut self,
        now_sec: i64,
        primary: &mut DbSet,
        additional_datasets: &mut [(Vec<u8>, &mut DbSet)],
    ) -> Option<RdbSaveOutcome> {
        if !self
            .forkless
            .as_ref()
            .is_some_and(|(task, _)| task.is_finished())
        {
            return None;
        }
        self.finish_forkless_save(now_sec, primary, additional_datasets)
    }

    /// Shutdown joins only after cancellation, before a final synchronous save.
    pub fn abort_forkless_save(
        &mut self,
        now_sec: i64,
        primary: &mut DbSet,
        additional_datasets: &mut [(Vec<u8>, &mut DbSet)],
    ) {
        self.cancel_forkless_save();
        let _ = self.finish_forkless_save(now_sec, primary, additional_datasets);
    }

    #[cfg(test)]
    pub(crate) fn finish_primary_forkless_save_for_test(
        &mut self,
        primary: &mut DbSet,
    ) -> Option<RdbSaveOutcome> {
        assert!(!self.forkless_capture_in_progress());
        self.finish_forkless_save(0, primary, &mut [])
    }

    fn finish_forkless_save(
        &mut self,
        now_sec: i64,
        primary: &mut DbSet,
        additional_datasets: &mut [(Vec<u8>, &mut DbSet)],
    ) -> Option<RdbSaveOutcome> {
        let (mut task, dirty_at_start) = self.forkless.take()?;
        self.forkless_feed = None;
        self.forkless_captured_keys.clear();
        self.forkless_eviction_keys.clear();
        let outcome = task.finish();
        let original_to_current = &self.forkless_original_to_current_db;
        for mut item in self.forkless_keys.drain() {
            if item.dataset_id.is_none() {
                item.db_id = original_to_current
                    .get(&item.db_id)
                    .copied()
                    .unwrap_or(item.db_id);
            }
            let released = release_forkless_item(primary, additional_datasets, &item);
            debug_assert!(
                released,
                "background save completion must release every shared value"
            );
        }
        self.forkless_current_to_original_db.clear();
        self.forkless_original_to_current_db.clear();
        if self.forkless_purpose != ForklessSavePurpose::Persistence {
            match &outcome {
                RdbSaveOutcome::Saved => {
                    self.rdb.last_bgsave_type = Some(BgsaveMethod::Forkless);
                    self.rdb.last_bgsave_status = OperationStatus::Ok;
                }
                RdbSaveOutcome::Failed(_) => {
                    self.rdb.last_bgsave_type = Some(BgsaveMethod::Forkless);
                    self.rdb.last_bgsave_status = OperationStatus::Err;
                }
                RdbSaveOutcome::Cancelled => {}
            }
            self.clear_save_metrics();
            return Some(outcome);
        }
        match &outcome {
            RdbSaveOutcome::Saved => {
                self.rdb.last_bgsave_status = OperationStatus::Ok;
                self.rdb.last_save_time_sec = now_sec;
                self.dirty = self.dirty.saturating_sub(dirty_at_start);
                self.rdb.last_bgsave_type = Some(BgsaveMethod::Forkless);
            }
            RdbSaveOutcome::Failed(_) => {
                self.rdb.last_bgsave_status = OperationStatus::Err;
                self.rdb.last_bgsave_type = Some(BgsaveMethod::Forkless);
            }
            RdbSaveOutcome::Cancelled => {}
        }
        self.clear_save_metrics();
        Some(outcome)
    }

    pub fn bgsave_scheduled(&self) -> bool {
        self.bgsave_scheduled.is_some()
    }

    pub fn set_bgsave_scheduled(&mut self, scheduled: bool) {
        self.bgsave_scheduled = scheduled.then_some(BgsaveMethod::Fork);
    }

    pub fn schedule_bgsave(&mut self, method: BgsaveMethod) {
        self.bgsave_scheduled = Some(method);
    }

    pub fn aof_rewrite_scheduled(&self) -> bool {
        self.aof_rewrite_scheduled
    }

    pub fn set_aof_rewrite_scheduled(&mut self, scheduled: bool) {
        self.aof_rewrite_scheduled = scheduled;
    }

    pub fn rdb_child_pid(&self) -> Option<i32> {
        match self.child {
            Some(PersistenceChild::Rdb { pid, .. }) => Some(pid),
            _ => None,
        }
    }

    pub fn diskless_child_pid(&self) -> Option<i32> {
        match self.child {
            Some(PersistenceChild::DisklessSync { pid }) => Some(pid),
            _ => None,
        }
    }

    pub fn disk_sync_child_pid(&self) -> Option<i32> {
        match self.child {
            Some(PersistenceChild::DiskSync { pid }) => Some(pid),
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn slot_migration_child_pid(&self) -> Option<i32> {
        match self.child {
            Some(PersistenceChild::SlotMigration { pid }) => Some(pid),
            _ => None,
        }
    }

    /// True while any RDB-shaped child (disk BGSAVE, diskless full-sync, or
    /// disk-based full-sync) runs. Drives INFO `rdb_bgsave_in_progress`; C sets
    /// that flag for disk BGSAVE and both full-sync transfer flavors.
    pub fn rdb_bgsave_in_progress(&self) -> bool {
        self.forkless.is_some()
            || matches!(
                self.child,
                Some(PersistenceChild::Rdb { .. })
                    | Some(PersistenceChild::DisklessSync { .. })
                    | Some(PersistenceChild::DiskSync { .. })
                    | Some(PersistenceChild::SlotMigration { .. })
            )
    }

    pub fn aof_rewrite_child_pid(&self) -> Option<i32> {
        match self.child {
            Some(PersistenceChild::AofRewrite { pid, .. }) => Some(pid),
            _ => None,
        }
    }

    pub fn rdb(&self) -> &RdbState {
        &self.rdb
    }

    pub fn aof(&self) -> &AofState {
        &self.aof
    }

    pub fn aof_mut(&mut self) -> &mut AofState {
        &mut self.aof
    }

    pub fn feed_aof(&mut self, frame: &[u8]) {
        if let Some(journal) = self.aof.journal.as_mut() {
            journal.appender.feed(frame);
        }
        if matches!(self.child, Some(PersistenceChild::AofRewrite { .. })) {
            self.aof.rewrite.buffer.extend_from_slice(frame);
        }
    }

    pub fn should_feed_aof_timestamp(&mut self, timestamp_sec: i64) -> bool {
        if self.aof.last_timestamp_annotation_sec >= timestamp_sec {
            return false;
        }
        self.aof.last_timestamp_annotation_sec = timestamp_sec;
        true
    }

    pub fn take_rewrite_force_select(&mut self) -> bool {
        std::mem::take(&mut self.aof.rewrite.force_select)
    }

    pub fn auto_aof_rewrite_limited(&self) -> bool {
        self.aof.rewrite.consecutive_failures >= 3
    }

    pub fn mark_auto_aof_rewrite_limit_logged(&mut self) -> bool {
        if self.aof.rewrite.consecutive_failures < 3 || self.aof.rewrite.limit_logged {
            return false;
        }
        self.aof.rewrite.limit_logged = true;
        true
    }

    pub fn begin_rdb_child(&mut self, pid: i32, now_sec: i64) -> Result<(), StateError> {
        if self.has_active_child() {
            return Err(StateError::ChildAlreadyRunning);
        }
        self.begin_save_metrics(BgsaveMethod::Fork, now_sec);
        self.replace_child(Some(PersistenceChild::Rdb {
            pid,
            dirty_at_fork: self.dirty,
        }));
        crate::foundation::io::sys::signal::set_rdb_child_pid(pid);
        Ok(())
    }

    pub fn complete_rdb_child(&mut self, success: bool, now_sec: i64) -> Result<(), StateError> {
        let dirty_at_fork = match self.child {
            Some(PersistenceChild::Rdb { dirty_at_fork, .. }) => dirty_at_fork,
            _ => return Err(StateError::WrongChild),
        };
        self.replace_child(None);
        self.rdb.last_bgsave_status = if success {
            self.rdb.last_save_time_sec = now_sec;
            self.dirty = self.dirty.saturating_sub(dirty_at_fork);
            OperationStatus::Ok
        } else {
            OperationStatus::Err
        };
        self.rdb.last_cow_size = self.rdb.current_cow_size;
        self.rdb.last_bgsave_type = Some(BgsaveMethod::Fork);
        self.clear_save_metrics();
        crate::foundation::io::sys::signal::set_rdb_child_pid(-1);
        Ok(())
    }

    pub fn begin_diskless_child(&mut self, pid: i32) -> Result<(), StateError> {
        if self.has_active_child() {
            return Err(StateError::ChildAlreadyRunning);
        }
        self.begin_transfer_metrics();
        self.replace_child(Some(PersistenceChild::DisklessSync { pid }));
        Ok(())
    }

    /// Clear the diskless child. Unlike `complete_rdb_child`, this performs no
    /// dirty accounting: a diskless full-sync is not a disk persistence point.
    pub fn complete_diskless_child(&mut self) {
        if matches!(self.child, Some(PersistenceChild::DisklessSync { .. })) {
            self.replace_child(None);
            self.clear_save_metrics();
        }
    }

    pub fn begin_disk_sync_child(&mut self, pid: i32) -> Result<(), StateError> {
        if self.has_active_child() {
            return Err(StateError::ChildAlreadyRunning);
        }
        self.begin_transfer_metrics();
        self.replace_child(Some(PersistenceChild::DiskSync { pid }));
        Ok(())
    }

    /// Clear the disk-based full-sync child. Mirrors `complete_diskless_child`:
    /// no dirty accounting, because a full-sync transfer is not a disk save.
    pub fn complete_disk_sync_child(&mut self) {
        if matches!(self.child, Some(PersistenceChild::DiskSync { .. })) {
            self.replace_child(None);
            self.clear_save_metrics();
        }
    }

    pub fn begin_slot_migration_child(&mut self, pid: i32) -> Result<(), StateError> {
        if self.has_active_child() {
            return Err(StateError::ChildAlreadyRunning);
        }
        self.replace_child(Some(PersistenceChild::SlotMigration { pid }));
        Ok(())
    }

    pub fn complete_slot_migration_child(&mut self) {
        if matches!(self.child, Some(PersistenceChild::SlotMigration { .. })) {
            self.replace_child(None);
        }
    }

    pub fn begin_aof_rewrite_child(&mut self, pid: i32, started_ms: i64) -> Result<(), StateError> {
        if self.has_active_child() {
            return Err(StateError::ChildAlreadyRunning);
        }
        self.replace_child(Some(PersistenceChild::AofRewrite { pid, started_ms }));
        self.aof_rewrite_scheduled = false;
        self.reset_aof_selected_db();
        self.aof.rewrite.buffer.clear();
        self.aof.rewrite.force_select = true;
        self.aof.rewrite.total_rewrites = self.aof.rewrite.total_rewrites.saturating_add(1);
        crate::foundation::io::sys::signal::set_aof_child_pid(pid);
        Ok(())
    }

    pub fn complete_aof_rewrite_child(
        &mut self,
        success: bool,
        completed_ms: i64,
    ) -> Result<Vec<u8>, StateError> {
        let started_ms = match self.child {
            Some(PersistenceChild::AofRewrite { started_ms, .. }) => started_ms,
            _ => return Err(StateError::WrongChild),
        };
        self.replace_child(None);
        self.aof.rewrite.last_duration_sec = completed_ms.saturating_sub(started_ms) / 1_000;
        self.aof.rewrite.last_status = if success {
            OperationStatus::Ok
        } else {
            OperationStatus::Err
        };
        if success {
            crate::foundation::io::sys::signal::set_aof_child_pid(-1);
            Ok(std::mem::take(&mut self.aof.rewrite.buffer))
        } else {
            self.aof.rewrite.consecutive_failures =
                self.aof.rewrite.consecutive_failures.saturating_add(1);
            self.aof.rewrite.buffer.clear();
            crate::foundation::io::sys::signal::set_aof_child_pid(-1);
            Ok(Vec::new())
        }
    }

    pub fn record_aof_rewrite_commit_success(&mut self) {
        self.aof.rewrite.last_status = OperationStatus::Ok;
        self.aof.rewrite.consecutive_failures = 0;
        self.aof.rewrite.limit_logged = false;
    }

    pub fn record_aof_rewrite_commit_failure(&mut self) {
        self.aof.rewrite.last_status = OperationStatus::Err;
        self.aof.rewrite.consecutive_failures =
            self.aof.rewrite.consecutive_failures.saturating_add(1);
    }

    pub fn abort_child(&mut self) -> Option<PersistenceChild> {
        let child = self.replace_child(None);
        match child {
            Some(PersistenceChild::Rdb { .. }) => {
                self.clear_save_metrics();
                crate::foundation::io::sys::signal::set_rdb_child_pid(-1);
            }
            Some(PersistenceChild::AofRewrite { .. }) => {
                self.aof.rewrite.buffer.clear();
                crate::foundation::io::sys::signal::set_aof_child_pid(-1);
            }
            // Replication transfers clear timing and progress without changing
            // persistence status, dirty accounting, or process signal slots.
            Some(PersistenceChild::DisklessSync { .. })
            | Some(PersistenceChild::DiskSync { .. }) => self.clear_save_metrics(),
            Some(PersistenceChild::SlotMigration { .. }) => {}
            None => {}
        }
        child
    }
}

fn set_non_identity_mapping(map: &mut HashMap<u32, u32>, from: u32, to: u32) {
    if from == to {
        map.remove(&from);
    } else {
        map.insert(from, to);
    }
}

fn release_forkless_item(
    primary: &mut DbSet,
    additional_datasets: &mut [(Vec<u8>, &mut DbSet)],
    item: &ForklessItemId,
) -> bool {
    if item.dataset_id.is_none() {
        return primary
            .get_mut(item.db_id)
            .is_none_or(|db| db.release_background_snapshot(&item.key));
    }
    additional_datasets
        .iter_mut()
        .find(|(id, _)| Some(id.as_slice()) == item.dataset_id.as_deref())
        .and_then(|(_, dataset)| dataset.get_mut(item.db_id))
        .is_none_or(|db| db.release_background_snapshot(&item.key))
}

impl OperationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Err => "err",
        }
    }
}

impl Default for PersistenceState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{OperationStatus, PersistenceChild, PersistenceState, StateError};

    #[test]
    fn forkless_eviction_candidates_count_stale_entries_and_exclude_other_datasets() {
        use crate::foundation::io::persistence::rdb::frozen::ForklessItemId;
        use crate::foundation::io::store::kv_engine::{Db, EvictionPolicy};
        use crate::foundation::io::store::Robj;
        use bytes::Bytes;

        let mut state = PersistenceState::new();
        let item = |db_id, key: &[u8], dataset_id| ForklessItemId {
            db_id,
            key: key.to_vec(),
            dataset_id,
        };
        state.record_forkless_captured_keys(vec![
            item(0, b"stale-0", None),
            item(0, b"stale-1", None),
            item(0, b"safe", None),
            item(0, b"safe", None),
        ]);
        state.record_forkless_captured_keys(
            (0..2_048)
                .flat_map(|index| {
                    let key = format!("unrelated-{index}");
                    [
                        item(1, key.as_bytes(), None),
                        item(0, key.as_bytes(), Some(b"module".to_vec())),
                    ]
                })
                .collect(),
        );
        let mut db = Db::new();
        db.set(
            b"safe".to_vec(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
        let mut checked = 0;
        let candidates: Vec<_> = state
            .forkless_eviction_candidates(0, 2)
            .inspect(|_| checked += 1)
            .filter_map(|key| db.eviction_candidate_for_key(key, EvictionPolicy::AllkeysLru))
            .collect();
        assert!(candidates.is_empty());
        assert_eq!(
            checked, 2,
            "stale entries must consume the inspection budget"
        );
        state.advance_forkless_eviction_candidates(0, 2);
        assert_eq!(
            state.forkless_eviction_candidates(0, 2).collect::<Vec<_>>(),
            vec![b"safe".as_slice(), b"stale-0".as_slice()]
        );
        assert!(state.forkless_eviction_candidates(0, 0).next().is_none());
        assert_eq!(state.forkless_eviction_candidates(0, usize::MAX).count(), 3);
        assert_eq!(state.forkless_eviction_candidates(2, usize::MAX).count(), 0);
        db.del(b"safe");
        assert!(state.forkless_key_captured(0, b"safe"));
        state.advance_forkless_eviction_candidates(0, usize::MAX);
        assert_eq!(
            state.forkless_eviction_candidates(0, 1).next(),
            Some(b"safe".as_slice())
        );
        eprintln!("bounded capture lookup: inspected=2, stale=2, unrelated_captures=4096, primary_index=3");
    }

    #[test]
    fn forkless_eviction_index_tracks_swaps_and_save_lifecycle() {
        use crate::foundation::io::persistence::rdb::compression::{
            RdbCompression, RdbSnapshotOptions,
        };
        use crate::foundation::io::persistence::rdb::frozen::FrozenRdb;
        use crate::foundation::io::persistence::rdb::RdbSerializeOptions;
        use crate::foundation::io::persistence::snapshot::background::BackgroundRdbSave;
        use crate::foundation::io::store::kv_engine::DbSet;
        use crate::foundation::io::store::Robj;
        use bytes::Bytes;

        let mut state = PersistenceState::new();
        let mut primary = DbSet::new(2);
        primary.get_mut(0).unwrap().set(
            b"first".to_vec(),
            Robj::string_raw(Bytes::from_static(b"original-0")),
        );
        primary.get_mut(1).unwrap().set(
            b"second".to_vec(),
            Robj::string_raw(Bytes::from_static(b"original-1")),
        );
        let dir = std::env::temp_dir().join(format!(
            "versal-captured-eviction-index-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        for should_abort in [true, false] {
            let frozen = FrozenRdb::capture_header(
                &primary,
                &[],
                &[],
                None,
                RdbSerializeOptions {
                    compression: false,
                    checksum: true,
                },
            );
            let (task, feeder) = BackgroundRdbSave::start_incremental(
                frozen.total_keys(),
                dir.clone(),
                "dump.rdb".into(),
                1,
                i64::MAX,
                false,
                RdbSnapshotOptions {
                    compression: RdbCompression::No,
                    checksum: true,
                },
            )
            .unwrap();
            let feed = frozen.into_incremental_feed(
                feeder,
                crate::foundation::io::store::kv_engine::now_ms(),
                false,
            );
            state
                .begin_forkless_save(task, 0, Vec::new(), Some(feed))
                .unwrap();
            assert!(state.forkless_eviction_keys.is_empty());
            assert!(state.forkless_captured_keys.is_empty());
            assert!(state.forkless_current_to_original_db.is_empty());
            state.drive_forkless_feed(&mut primary, &mut []);
            assert_eq!(
                state.forkless_eviction_candidates(0, 1).next(),
                Some(b"first".as_slice())
            );
            primary.swap(0, 1);
            state.swap_forkless_databases(0, 1);
            assert_eq!(
                state.forkless_eviction_candidates(0, 1).next(),
                Some(b"second".as_slice())
            );
            assert_eq!(
                state.forkless_eviction_candidates(1, 1).next(),
                Some(b"first".as_slice())
            );
            assert!(state.forkless_key_captured(1, b"first"));
            assert!(primary.get(1).unwrap().get_no_expire(b"first").is_some());
            state.advance_forkless_eviction_candidates(1, 1);
            primary.swap(0, 1);
            state.swap_forkless_databases(0, 1);
            assert_eq!(
                state.forkless_eviction_candidates(0, 1).next(),
                Some(b"first".as_slice())
            );
            if should_abort {
                state.abort_forkless_save(0, &mut primary, &mut []);
            } else {
                state.set_forkless_key_delay(0);
                assert!(matches!(
                    state.finish_primary_forkless_save_for_test(&mut primary),
                    Some(crate::foundation::io::persistence::snapshot::background::RdbSaveOutcome::Saved)
                ));
            }
            assert!(state.forkless_eviction_keys.is_empty());
            assert!(state.forkless_captured_keys.is_empty());
            assert!(state.forkless_current_to_original_db.is_empty());
            assert!(state.forkless_original_to_current_db.is_empty());
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    mod child_compaction_lifecycle {
        use super::{PersistenceChild, PersistenceState, StateError};
        use crate::foundation::io::store::structure::zset::{
            ordered_index::set_leaf_compaction_allowed, ZSetSl,
        };
        use crate::foundation::memory::value::ZSetOps;

        const CHILDREN: [PersistenceChild; 5] = [
            PersistenceChild::Rdb {
                pid: 101,
                dirty_at_fork: 0,
            },
            PersistenceChild::AofRewrite {
                pid: 102,
                started_ms: 1_000,
            },
            PersistenceChild::DisklessSync { pid: 103 },
            PersistenceChild::DiskSync { pid: 104 },
            PersistenceChild::SlotMigration { pid: 105 },
        ];

        struct ResetCompaction;

        impl Drop for ResetCompaction {
            fn drop(&mut self) {
                set_leaf_compaction_allowed(true);
            }
        }

        fn begin_child(
            state: &mut PersistenceState,
            child: PersistenceChild,
        ) -> Result<(), StateError> {
            match child {
                PersistenceChild::Rdb { pid, .. } => state.begin_rdb_child(pid, 1),
                PersistenceChild::AofRewrite { pid, started_ms } => {
                    state.begin_aof_rewrite_child(pid, started_ms)
                }
                PersistenceChild::DisklessSync { pid } => state.begin_diskless_child(pid),
                PersistenceChild::DiskSync { pid } => state.begin_disk_sync_child(pid),
                PersistenceChild::SlotMigration { pid } => state.begin_slot_migration_child(pid),
            }
        }

        fn complete_child(
            state: &mut PersistenceState,
            child: PersistenceChild,
            is_success: bool,
        ) -> Result<(), StateError> {
            match child {
                PersistenceChild::Rdb { .. } => state.complete_rdb_child(is_success, 2),
                PersistenceChild::AofRewrite { .. } => state
                    .complete_aof_rewrite_child(is_success, 2_000)
                    .map(|_| ()),
                PersistenceChild::DisklessSync { .. } => {
                    state.complete_diskless_child();
                    Ok(())
                }
                PersistenceChild::DiskSync { .. } => {
                    state.complete_disk_sync_child();
                    Ok(())
                }
                PersistenceChild::SlotMigration { .. } => {
                    state.complete_slot_migration_child();
                    Ok(())
                }
            }
        }

        fn sparse_zset() -> ZSetSl {
            // Keep every original leaf nonempty so leaf loss means compaction,
            // not the permitted removal of an empty subtree.
            set_leaf_compaction_allowed(false);
            let mut zset = ZSetSl::new();
            for index in 0..4_096 {
                assert!(zset
                    .add(&index.to_string().into_bytes(), index as f64)
                    .unwrap());
            }
            let leaves_before = zset.ordered_index().memory_layout().leaf_nodes;
            assert!(leaves_before > 1);
            for index in 0..4_096 {
                if index % 16 >= 4 {
                    assert!(zset.remove(index.to_string().as_bytes()).unwrap());
                }
            }
            assert_eq!(zset.len(), 1_024);
            assert_eq!(
                zset.ordered_index().memory_layout().leaf_nodes,
                leaves_before
            );
            zset
        }

        fn assert_compaction(mut zset: ZSetSl, is_allowed: bool, boundary: &str) {
            let leaves_before = zset.ordered_index().memory_layout().leaf_nodes;
            for index in 0..4_096 {
                if (1..4).contains(&(index % 16)) {
                    assert!(zset.remove(index.to_string().as_bytes()).unwrap());
                }
            }
            let leaves_after = zset.ordered_index().memory_layout().leaf_nodes;
            println!("{boundary}: allowed={is_allowed}, leaves={leaves_before}->{leaves_after}");
            assert_eq!(zset.len(), 256);
            assert_eq!(zset.ordered_index().len(), 256);
            for (rank, (member, score)) in zset.sorted_entries().into_iter().enumerate() {
                let index = rank * 16;
                assert_eq!(member.as_ref(), index.to_string().as_bytes());
                assert_eq!(score, index as f64);
                assert_eq!(zset.rank(&member), Some(rank));
            }
            if is_allowed {
                assert!(
                    leaves_after < leaves_before,
                    "{boundary}: clearing a child must immediately resume leaf compaction"
                );
            } else {
                assert_eq!(
                    leaves_after, leaves_before,
                    "{boundary}: a registered child must immediately preserve nonempty leaves"
                );
            }
        }

        fn assert_begin_pauses(child: PersistenceChild) {
            let _reset = ResetCompaction;
            let zset = sparse_zset();
            set_leaf_compaction_allowed(true);
            let mut state = PersistenceState::new();
            begin_child(&mut state, child).unwrap();
            assert_eq!(state.child(), Some(child));
            assert_compaction(zset, false, &format!("begin {child:?}"));
            assert_eq!(state.abort_child(), Some(child));
        }

        #[test]
        fn begin_rdb_pauses_nonempty_leaf_compaction() {
            assert_begin_pauses(CHILDREN[0]);
        }

        #[test]
        fn begin_aof_rewrite_pauses_nonempty_leaf_compaction() {
            assert_begin_pauses(CHILDREN[1]);
        }

        #[test]
        fn begin_diskless_pauses_nonempty_leaf_compaction() {
            assert_begin_pauses(CHILDREN[2]);
        }

        #[test]
        fn begin_disk_sync_pauses_nonempty_leaf_compaction() {
            assert_begin_pauses(CHILDREN[3]);
        }

        #[test]
        fn begin_slot_migration_pauses_nonempty_leaf_compaction() {
            assert_begin_pauses(CHILDREN[4]);
        }

        #[test]
        fn completion_immediately_resumes_for_every_child_and_outcome() {
            let _reset = ResetCompaction;
            for child in CHILDREN {
                for is_success in [true, false] {
                    let zset = sparse_zset();
                    let mut state = PersistenceState::new();
                    begin_child(&mut state, child).unwrap();
                    complete_child(&mut state, child, is_success).unwrap();
                    assert_eq!(state.child(), None);
                    assert_compaction(
                        zset,
                        true,
                        &format!("complete {child:?}, success={is_success}"),
                    );
                }
            }
        }

        #[test]
        fn abort_immediately_resumes_for_every_child() {
            let _reset = ResetCompaction;
            for child in CHILDREN {
                let zset = sparse_zset();
                let mut state = PersistenceState::new();
                begin_child(&mut state, child).unwrap();
                assert_eq!(state.abort_child(), Some(child));
                assert_eq!(state.child(), None);
                assert_compaction(zset, true, &format!("abort {child:?}"));
                assert_eq!(state.abort_child(), None);
            }
        }

        #[test]
        fn rejected_registration_preserves_child_and_compaction_gate() {
            let _reset = ResetCompaction;
            for child in CHILDREN {
                for attempted in CHILDREN {
                    let zset = sparse_zset();
                    let mut state = PersistenceState::new();
                    begin_child(&mut state, child).unwrap();
                    assert_eq!(
                        begin_child(&mut state, attempted),
                        Err(StateError::ChildAlreadyRunning)
                    );
                    assert_eq!(state.child(), Some(child));
                    assert_compaction(
                        zset,
                        false,
                        &format!("reject {attempted:?} while {child:?}"),
                    );
                    assert_eq!(state.abort_child(), Some(child));
                }
            }
        }

        #[test]
        fn wrong_completion_preserves_live_child_and_compaction_gate() {
            let _reset = ResetCompaction;
            for child in CHILDREN {
                for attempted in CHILDREN {
                    if child == attempted {
                        continue;
                    }
                    let zset = sparse_zset();
                    let mut state = PersistenceState::new();
                    begin_child(&mut state, child).unwrap();
                    let result = complete_child(&mut state, attempted, false);
                    if matches!(
                        attempted,
                        PersistenceChild::Rdb { .. } | PersistenceChild::AofRewrite { .. }
                    ) {
                        assert_eq!(result, Err(StateError::WrongChild));
                    } else {
                        assert_eq!(result, Ok(()));
                    }
                    assert_eq!(state.child(), Some(child));
                    assert_compaction(
                        zset,
                        false,
                        &format!("wrong completion {attempted:?} while {child:?}"),
                    );
                    assert_eq!(state.abort_child(), Some(child));
                }
            }
        }

        #[test]
        fn completion_without_child_does_not_change_compaction_gate() {
            let _reset = ResetCompaction;
            for child in CHILDREN {
                for is_allowed in [true, false] {
                    let zset = sparse_zset();
                    set_leaf_compaction_allowed(is_allowed);
                    let mut state = PersistenceState::new();
                    let result = complete_child(&mut state, child, false);
                    if matches!(
                        child,
                        PersistenceChild::Rdb { .. } | PersistenceChild::AofRewrite { .. }
                    ) {
                        assert_eq!(result, Err(StateError::WrongChild));
                    } else {
                        assert_eq!(result, Ok(()));
                    }
                    assert_eq!(state.child(), None);
                    assert_compaction(zset, is_allowed, &format!("no child, complete {child:?}"));
                }
            }
        }

        #[test]
        fn forkless_begin_rejection_completion_and_abort_do_not_pause_compaction() {
            use crate::foundation::io::persistence::rdb::{
                compression::{RdbCompression, RdbSnapshotOptions},
                frozen::FrozenRdb,
            };
            use crate::foundation::io::persistence::snapshot::background::BackgroundRdbSave;
            use crate::foundation::io::persistence::RdbSerializeOptions;
            use crate::foundation::io::store::kv_engine::DbSet;

            let _reset = ResetCompaction;
            for purpose in [
                super::super::ForklessSavePurpose::Persistence,
                super::super::ForklessSavePurpose::ReplicationFile,
                super::super::ForklessSavePurpose::ReplicationSocket,
            ] {
                for should_abort in [true, false] {
                    let mut primary = DbSet::new(1);
                    let frozen = FrozenRdb::capture(
                        &primary,
                        &[],
                        &[],
                        None,
                        RdbSerializeOptions::default(),
                        0,
                        true,
                    );
                    let (task, reader) = BackgroundRdbSave::start_to_pipe(
                        frozen,
                        0,
                        RdbSnapshotOptions {
                            compression: RdbCompression::No,
                            checksum: true,
                        },
                    )
                    .unwrap();
                    let zset = sparse_zset();
                    set_leaf_compaction_allowed(true);
                    let mut state = PersistenceState::new();
                    if purpose == super::super::ForklessSavePurpose::Persistence {
                        state
                            .begin_forkless_save(task, 1, Vec::new(), None)
                            .unwrap();
                    } else {
                        state.begin_forkless_transfer(task, purpose).unwrap();
                    }
                    assert!(state.has_active_child());
                    assert_eq!(state.child(), None);
                    assert_compaction(zset, true, &format!("begin forkless {purpose:?}"));
                    for child in CHILDREN {
                        let zset = sparse_zset();
                        set_leaf_compaction_allowed(true);
                        assert_eq!(
                            begin_child(&mut state, child),
                            Err(StateError::ChildAlreadyRunning)
                        );
                        assert_eq!(state.child(), None);
                        assert!(state.forkless_save_in_progress());
                        assert_compaction(zset, true, &format!("reject {child:?} during forkless"));
                    }
                    let zset = sparse_zset();
                    set_leaf_compaction_allowed(true);
                    if should_abort {
                        state.abort_forkless_save(2, &mut primary, &mut []);
                    } else {
                        assert!(state
                            .finish_primary_forkless_save_for_test(&mut primary)
                            .is_some());
                    }
                    assert!(!state.has_active_child());
                    assert_compaction(
                        zset,
                        true,
                        &format!("finish forkless {purpose:?}, abort={should_abort}"),
                    );
                    drop(reader);
                }
            }
        }
    }

    #[test]
    fn forkless_socket_completion_preserves_dirty_and_lastsave_without_a_pid() {
        use crate::foundation::io::persistence::rdb::{
            compression::{RdbCompression, RdbSnapshotOptions},
            frozen::FrozenRdb,
        };
        use crate::foundation::io::persistence::snapshot::background::{
            BackgroundRdbSave, RdbSaveOutcome,
        };
        use crate::foundation::io::persistence::RdbSerializeOptions;
        use crate::foundation::io::store::kv_engine::DbSet;
        let mut primary = DbSet::new(1);
        let frozen = FrozenRdb::capture(
            &primary,
            &[],
            &[],
            None,
            RdbSerializeOptions::default(),
            0,
            true,
        );
        let (task, reader) = BackgroundRdbSave::start_to_pipe(
            frozen,
            0,
            RdbSnapshotOptions {
                compression: RdbCompression::No,
                checksum: true,
            },
        )
        .unwrap();
        let mut state = PersistenceState::new();
        state.set_dirty(13);
        state.rdb.last_save_time_sec = 42;
        state
            .begin_forkless_transfer(task, super::ForklessSavePurpose::ReplicationSocket)
            .unwrap();
        assert!(state.has_active_child());
        assert_eq!(state.child(), None);
        assert_eq!(state.rdb_child_pid(), None);
        assert_eq!(state.diskless_child_pid(), None);
        state.add_dirty(7);
        assert!(matches!(
            state.finish_forkless_save(99, &mut primary, &mut []),
            Some(RdbSaveOutcome::Saved),
        ));
        assert_eq!(state.dirty(), 20);
        assert_eq!(state.rdb.last_save_time_sec, 42);
        assert_eq!(
            state.rdb.last_bgsave_type,
            Some(super::BgsaveMethod::Forkless)
        );
        assert!(!state.has_active_child());
        drop(reader);
    }

    #[test]
    fn replication_transfer_registers_timing_without_persistence_accounting() {
        for is_diskless in [true, false] {
            let mut state = PersistenceState::new();
            state.add_dirty(13);
            state.rdb.saves = 10;
            state.rdb.last_bgsave_try_sec = 41;
            if is_diskless {
                state.begin_diskless_child(101).unwrap();
            } else {
                state.begin_disk_sync_child(101).unwrap();
            }

            assert_eq!(
                state.rdb.current_bgsave_type,
                Some(super::BgsaveMethod::Fork)
            );
            assert!(state.current_bgsave_duration_sec() >= 0);
            assert_eq!(state.rdb.saves, 10);
            assert_eq!(state.rdb.last_bgsave_try_sec, 41);
            assert_eq!(state.dirty(), 13);
        }
    }

    #[test]
    fn replication_transfer_completion_and_abort_clear_save_metrics() {
        for is_diskless in [true, false] {
            for should_abort in [true, false] {
                let mut state = PersistenceState::new();
                state.add_dirty(13);
                state.rdb.last_bgsave_status = OperationStatus::Err;
                state.rdb.last_bgsave_type = Some(super::BgsaveMethod::Forkless);
                state.rdb.last_save_time_sec = 42;
                if is_diskless {
                    state.begin_diskless_child(101).unwrap();
                } else {
                    state.begin_disk_sync_child(101).unwrap();
                }
                state.rdb.started = Some(
                    super::Instant::now()
                        .checked_sub(std::time::Duration::from_secs(7))
                        .unwrap(),
                );
                state.rdb.current_cow_size = 100;
                state.rdb.current_save_keys_processed = 10;
                state.rdb.current_save_keys_total = 100;
                state.rdb.current_fork_perc = 50.0;

                if should_abort {
                    assert!(state.abort_child().is_some());
                } else if is_diskless {
                    state.complete_diskless_child();
                } else {
                    state.complete_disk_sync_child();
                }

                assert!(state.rdb.last_bgsave_duration_sec >= 7);
                assert_eq!(state.current_bgsave_duration_sec(), -1);
                assert_eq!(state.rdb.current_bgsave_type, None);
                assert_eq!(state.rdb.current_cow_size, 0);
                assert_eq!(state.rdb.current_save_keys_processed, 0);
                assert_eq!(state.rdb.current_save_keys_total, 0);
                assert_eq!(state.rdb.current_fork_perc, 0.0);
                assert!(!state.rdb_bgsave_in_progress());
                assert_eq!(state.dirty(), 13);
                assert_eq!(state.rdb.last_bgsave_status, OperationStatus::Err);
                assert_eq!(
                    state.rdb.last_bgsave_type,
                    Some(super::BgsaveMethod::Forkless)
                );
                assert_eq!(state.rdb.last_save_time_sec, 42);
            }
        }
    }

    #[test]
    fn diskless_child_registers_and_reports_bgsave_in_progress() {
        let mut st = PersistenceState::new();
        assert!(!st.rdb_bgsave_in_progress());
        st.begin_diskless_child(4242).unwrap();
        assert_eq!(st.diskless_child_pid(), Some(4242));
        // A diskless child must surface as an in-progress bgsave for INFO parity.
        assert!(st.rdb_bgsave_in_progress());
        // It must not be mistaken for a disk RDB child (no dirty accounting).
        assert_eq!(st.rdb_child_pid(), None);
        assert!(st.has_active_child());
    }

    #[test]
    fn diskless_child_blocks_second_child_and_completes_without_dirty_reset() {
        let mut st = PersistenceState::new();
        st.add_dirty(7);
        st.begin_diskless_child(11).unwrap();
        // Single-child invariant: no BGSAVE child may start meanwhile.
        assert_eq!(
            st.begin_rdb_child(12, 0),
            Err(StateError::ChildAlreadyRunning)
        );
        // Completing a diskless child must not touch dirty (it is not a disk save).
        st.complete_diskless_child();
        assert_eq!(st.dirty(), 7);
        assert!(!st.has_active_child());
        assert!(!st.rdb_bgsave_in_progress());
    }

    #[test]
    fn abort_diskless_child_returns_it_without_dirty_accounting() {
        let mut st = PersistenceState::new();
        st.add_dirty(3);
        st.begin_diskless_child(9).unwrap();
        assert_eq!(
            st.abort_child(),
            Some(PersistenceChild::DisklessSync { pid: 9 })
        );
        assert!(!st.has_active_child());
        // abort performs no dirty accounting for a diskless child
        assert_eq!(st.dirty(), 3);
    }

    #[test]
    fn disk_sync_child_registers_and_reports_bgsave_in_progress() {
        let mut st = PersistenceState::new();
        assert!(!st.rdb_bgsave_in_progress());
        st.begin_disk_sync_child(7373).unwrap();
        assert_eq!(st.disk_sync_child_pid(), Some(7373));
        // A disk-based full-sync child must surface as an in-progress bgsave for
        // INFO parity (C sets rdb_bgsave_in_progress for disk full-sync too).
        assert!(st.rdb_bgsave_in_progress());
        // It is neither a disk RDB (BGSAVE) child nor a diskless one, so the
        // reap gating stays sound (its relay is tracked separately).
        assert_eq!(st.rdb_child_pid(), None);
        assert_eq!(st.diskless_child_pid(), None);
        assert!(st.has_active_child());
    }

    #[test]
    fn disk_sync_child_blocks_second_child_and_completes_without_dirty_reset() {
        let mut st = PersistenceState::new();
        st.add_dirty(5);
        st.begin_disk_sync_child(21).unwrap();
        // Single-child invariant: no BGSAVE / diskless child may start meanwhile.
        assert_eq!(
            st.begin_rdb_child(22, 0),
            Err(StateError::ChildAlreadyRunning)
        );
        assert_eq!(
            st.begin_diskless_child(23),
            Err(StateError::ChildAlreadyRunning)
        );
        // Completing a disk-sync child must not touch dirty (it is a replication
        // transfer, not a disk persistence point).
        st.complete_disk_sync_child();
        assert_eq!(st.dirty(), 5);
        assert!(!st.has_active_child());
        assert!(!st.rdb_bgsave_in_progress());
    }

    #[test]
    fn slot_migration_child_obeys_single_child_invariant() {
        let mut state = PersistenceState::new();
        state.begin_slot_migration_child(31).unwrap();
        assert_eq!(state.slot_migration_child_pid(), Some(31));
        assert!(state.has_active_child());
        assert_eq!(
            state.begin_diskless_child(32),
            Err(StateError::ChildAlreadyRunning)
        );
        state.complete_slot_migration_child();
        assert!(!state.has_active_child());
    }

    #[test]
    fn abort_disk_sync_child_returns_it_without_dirty_accounting() {
        let mut st = PersistenceState::new();
        st.add_dirty(2);
        st.begin_disk_sync_child(8).unwrap();
        assert_eq!(
            st.abort_child(),
            Some(PersistenceChild::DiskSync { pid: 8 })
        );
        assert!(!st.has_active_child());
        assert_eq!(st.dirty(), 2);
    }

    #[test]
    fn initial_state_has_no_child_or_journal() {
        let state = PersistenceState::new();

        assert_eq!(state.dirty(), 0);
        assert_eq!(state.aof_selected_db(), -1);
        assert!(state.child().is_none());
        assert!(state.aof().journal.is_none());
        assert_eq!(state.rdb().last_bgsave_status, OperationStatus::Ok);
    }

    #[test]
    fn aof_selected_db_can_be_updated_and_reset() {
        let mut state = PersistenceState::new();

        state.set_aof_selected_db(10);
        assert_eq!(state.aof_selected_db(), 10);

        state.reset_aof_selected_db();
        assert_eq!(state.aof_selected_db(), -1);
    }

    #[test]
    fn aof_cursor_resets_before_replacement_stream() {
        let mut state = PersistenceState::new();
        state.set_aof_selected_db(10);

        state.begin_aof_rewrite_child(123, 1_000).unwrap();

        assert_eq!(state.aof_selected_db(), -1);
    }

    #[test]
    fn one_child_owner_prevents_overlapping_background_operations() {
        let mut state = PersistenceState::new();
        state.add_dirty(5);
        state.begin_rdb_child(41, 100).unwrap();

        assert_eq!(
            state.child(),
            Some(PersistenceChild::Rdb {
                pid: 41,
                dirty_at_fork: 5,
            })
        );
        assert!(state.begin_aof_rewrite_child(42, 101).is_err());
    }

    #[test]
    fn scheduled_aof_rewrite_is_cleared_when_rewrite_starts() {
        let mut state = PersistenceState::new();
        state.set_aof_rewrite_scheduled(true);

        assert!(state.aof_rewrite_scheduled());
        state.begin_aof_rewrite_child(42, 101).unwrap();

        assert!(!state.aof_rewrite_scheduled());
    }

    #[test]
    fn successful_bgsave_preserves_writes_after_fork() {
        let mut state = PersistenceState::new();
        state.add_dirty(5);
        state.begin_rdb_child(41, 100).unwrap();
        state.add_dirty(3);

        state.complete_rdb_child(true, 110).unwrap();

        assert_eq!(state.dirty(), 3);
        assert_eq!(state.rdb().last_save_time_sec, 110);
        assert_eq!(state.rdb().last_bgsave_status, OperationStatus::Ok);
        assert!(state.child().is_none());
    }

    #[test]
    fn failed_bgsave_keeps_all_dirty_writes() {
        let mut state = PersistenceState::new();
        state.add_dirty(5);
        state.begin_rdb_child(41, 100).unwrap();
        state.add_dirty(3);

        state.complete_rdb_child(false, 110).unwrap();

        assert_eq!(state.dirty(), 8);
        assert_eq!(state.rdb().last_bgsave_status, OperationStatus::Err);
    }

    #[test]
    fn feeding_aof_without_a_journal_is_a_noop() {
        let mut state = PersistenceState::new();

        state.feed_aof(b"*1\r\n$4\r\nPING\r\n");

        assert!(state.aof().rewrite.buffer.is_empty());
    }

    #[test]
    fn aof_timestamp_annotation_is_emitted_once_per_second() {
        let mut state = PersistenceState::new();

        assert!(state.should_feed_aof_timestamp(42));
        assert!(!state.should_feed_aof_timestamp(42));
        assert!(!state.should_feed_aof_timestamp(41));
        assert!(state.should_feed_aof_timestamp(43));
    }

    #[test]
    fn aof_feed_is_buffered_while_rewrite_child_runs() {
        let mut state = PersistenceState::new();
        state.begin_aof_rewrite_child(42, 1_000).unwrap();

        state.feed_aof(b"first");
        state.feed_aof(b"-second");

        assert_eq!(state.aof().rewrite.buffer, b"first-second");
        assert!(state.take_rewrite_force_select());
        assert!(!state.take_rewrite_force_select());
    }

    #[test]
    fn completing_aof_rewrite_returns_buffer_and_updates_status() {
        let mut state = PersistenceState::new();
        state.begin_aof_rewrite_child(42, 1_000).unwrap();
        state.set_aof_selected_db(10);
        state.feed_aof(b"increment");

        let buffer = state.complete_aof_rewrite_child(true, 4_500).unwrap();

        assert_eq!(buffer, b"increment");
        assert!(state.child().is_none());
        assert_eq!(state.aof().rewrite.last_duration_sec, 3);
        assert_eq!(state.aof().rewrite.last_status, OperationStatus::Ok);
        assert!(state.aof().rewrite.buffer.is_empty());
        assert_eq!(state.aof_selected_db(), 10);
    }

    #[test]
    fn failed_aof_rewrite_discards_buffer() {
        let mut state = PersistenceState::new();
        state.begin_aof_rewrite_child(42, 1_000).unwrap();
        state.feed_aof(b"increment");

        let buffer = state.complete_aof_rewrite_child(false, 2_000).unwrap();

        assert!(buffer.is_empty());
        assert!(state.child().is_none());
        assert_eq!(state.aof().rewrite.last_status, OperationStatus::Err);
        assert!(state.aof().rewrite.buffer.is_empty());
    }

    #[test]
    fn three_failures_limit_only_auto_rewrite_until_success() {
        let mut state = PersistenceState::new();
        for pid in 1..=3 {
            state.begin_aof_rewrite_child(pid, 1_000).unwrap();
            state.complete_aof_rewrite_child(false, 2_000).unwrap();
        }
        assert!(state.auto_aof_rewrite_limited());
        assert!(state.mark_auto_aof_rewrite_limit_logged());
        assert!(!state.mark_auto_aof_rewrite_limit_logged());

        state.begin_aof_rewrite_child(4, 3_000).unwrap();
        state.complete_aof_rewrite_child(true, 4_000).unwrap();
        state.record_aof_rewrite_commit_success();
        assert!(!state.auto_aof_rewrite_limited());
        assert!(!state.mark_auto_aof_rewrite_limit_logged());
    }
}
