use crate::foundation::io::persistence::journal::AofJournal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationStatus {
    Ok,
    Err,
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
    /// Upstream replication offset represented by this session's durable AOF
    /// base. This stays fixed while `appendfsync no` is active.
    pub fsynced_repl_offset: Option<i64>,
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
            .map_or(0, |journal| journal.buffer_allocation_bytes())
    }
}

pub struct PersistenceState {
    dirty: i64,
    aof_selected_db: i64,
    rdb: RdbState,
    aof: AofState,
    child: Option<PersistenceChild>,
    bgsave_scheduled: bool,
    aof_rewrite_scheduled: bool,
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
            bgsave_scheduled: false,
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

    pub fn has_active_child(&self) -> bool {
        self.child.is_some()
    }

    pub fn bgsave_scheduled(&self) -> bool {
        self.bgsave_scheduled
    }

    pub fn set_bgsave_scheduled(&mut self, scheduled: bool) {
        self.bgsave_scheduled = scheduled;
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
        matches!(
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
            journal.feed(frame);
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
        if self.child.is_some() {
            return Err(StateError::ChildAlreadyRunning);
        }
        self.rdb.last_bgsave_try_sec = now_sec;
        self.child = Some(PersistenceChild::Rdb {
            pid,
            dirty_at_fork: self.dirty,
        });
        crate::foundation::io::sys::signal::set_rdb_child_pid(pid);
        Ok(())
    }

    pub fn complete_rdb_child(&mut self, success: bool, now_sec: i64) -> Result<(), StateError> {
        let dirty_at_fork = match self.child.take() {
            Some(PersistenceChild::Rdb { dirty_at_fork, .. }) => dirty_at_fork,
            other => {
                self.child = other;
                return Err(StateError::WrongChild);
            }
        };
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
        crate::foundation::io::sys::signal::set_rdb_child_pid(-1);
        Ok(())
    }

    pub fn begin_diskless_child(&mut self, pid: i32) -> Result<(), StateError> {
        if self.child.is_some() {
            return Err(StateError::ChildAlreadyRunning);
        }
        self.child = Some(PersistenceChild::DisklessSync { pid });
        Ok(())
    }

    /// Clear the diskless child. Unlike `complete_rdb_child`, this performs no
    /// dirty accounting: a diskless full-sync is not a disk persistence point.
    pub fn complete_diskless_child(&mut self) {
        if matches!(self.child, Some(PersistenceChild::DisklessSync { .. })) {
            self.child = None;
        }
    }

    pub fn begin_disk_sync_child(&mut self, pid: i32) -> Result<(), StateError> {
        if self.child.is_some() {
            return Err(StateError::ChildAlreadyRunning);
        }
        self.child = Some(PersistenceChild::DiskSync { pid });
        Ok(())
    }

    /// Clear the disk-based full-sync child. Mirrors `complete_diskless_child`:
    /// no dirty accounting, because a full-sync transfer is not a disk save.
    pub fn complete_disk_sync_child(&mut self) {
        if matches!(self.child, Some(PersistenceChild::DiskSync { .. })) {
            self.child = None;
        }
    }

    pub fn begin_slot_migration_child(&mut self, pid: i32) -> Result<(), StateError> {
        if self.child.is_some() {
            return Err(StateError::ChildAlreadyRunning);
        }
        self.child = Some(PersistenceChild::SlotMigration { pid });
        Ok(())
    }

    pub fn complete_slot_migration_child(&mut self) {
        if matches!(self.child, Some(PersistenceChild::SlotMigration { .. })) {
            self.child = None;
        }
    }

    pub fn begin_aof_rewrite_child(&mut self, pid: i32, started_ms: i64) -> Result<(), StateError> {
        if self.child.is_some() {
            return Err(StateError::ChildAlreadyRunning);
        }
        self.child = Some(PersistenceChild::AofRewrite { pid, started_ms });
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
        let started_ms = match self.child.take() {
            Some(PersistenceChild::AofRewrite { started_ms, .. }) => started_ms,
            other => {
                self.child = other;
                return Err(StateError::WrongChild);
            }
        };
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
        let child = self.child.take();
        match child {
            Some(PersistenceChild::Rdb { .. }) => {
                crate::foundation::io::sys::signal::set_rdb_child_pid(-1);
            }
            Some(PersistenceChild::AofRewrite { .. }) => {
                self.aof.rewrite.buffer.clear();
                crate::foundation::io::sys::signal::set_aof_child_pid(-1);
            }
            // Diskless full-sync owns no signal PID slot or rewrite buffer, so
            // dropping the child is all that is needed to release ownership.
            Some(PersistenceChild::DisklessSync { .. }) => {}
            // Disk-based full-sync likewise owns no signal PID slot or buffer.
            Some(PersistenceChild::DiskSync { .. }) => {}
            Some(PersistenceChild::SlotMigration { .. }) => {}
            None => {}
        }
        child
    }
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
