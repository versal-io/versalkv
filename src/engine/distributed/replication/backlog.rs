use std::collections::VecDeque;

use super::{
    BacklogCursor, InvalidReplicationPosition, PsyncRequestOffset, ReplicationOffset,
    SnapshotBoundary,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorRange {
    pub start: BacklogCursor,
    pub end: BacklogCursor,
}

impl CursorRange {
    pub const fn new(start: BacklogCursor, end: BacklogCursor) -> Self {
        Self { start, end }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinueMatch {
    Primary,
    Second,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContinuePosition {
    pub history: ContinueMatch,
    pub cursor: BacklogCursor,
}

#[derive(Debug, Clone)]
pub struct ReplicationBacklog {
    replid: String,
    /// PSYNC2 second replid: set after failover to allow partial resync with
    /// replicas that were connected to the old primary.
    second_replid: String,
    /// Highest next-byte PSYNC wire request accepted for the secondary replication ID.
    second_replid_offset: Option<PsyncRequestOffset>,
    capacity: usize,
    next_cursor: BacklogCursor,
    /// After loading a primary RDB, C exposes the last replicated offset while
    /// the empty backlog starts at the following byte. The normal append path
    /// uses `next_cursor` as the half-open cursor; this marker preserves the
    /// one-byte distinction as subsequent frames advance both domains.
    restored_reported_offset: Option<ReplicationOffset>,
    /// Whether an empty backlog may accept its exact current cursor. This is
    /// valid after local history activation and after a replica commits a new
    /// upstream dataset, matching C's empty post-sync backlog boundary.
    empty_cursor_valid: bool,
    /// Lowest byte cursor still needed by a connected replica. Eviction never
    /// drops bytes at or beyond this cursor, even when the retained
    /// history exceeds capacity. Mirrors C Valkey's replication buffer, where
    /// blocks referenced by a lagging replica are held past repl-backlog-size
    /// until the replica drains them. `None` means no replica pin, so trimming
    /// proceeds strictly to capacity.
    min_protected_cursor: Option<BacklogCursor>,
    /// Ring buffer of replication bytes. Eviction drops from the front (oldest).
    bytes: VecDeque<u8>,
    history_valid: bool,
    active: bool,
}

impl ReplicationBacklog {
    const BLOCK_SIZE: usize = 16 * 1024;
    pub fn new(replid: String, capacity: usize) -> Self {
        Self {
            replid,
            second_replid: String::new(),
            second_replid_offset: None,
            capacity,
            next_cursor: BacklogCursor::try_from(0)
                .expect("zero is a valid initial backlog cursor"),
            restored_reported_offset: None,
            empty_cursor_valid: false,
            min_protected_cursor: None,
            bytes: VecDeque::with_capacity(capacity),
            history_valid: false,
            active: false,
        }
    }

    pub fn append(&mut self, frame: &[u8]) -> Result<CursorRange, InvalidReplicationPosition> {
        let start = self.next_cursor;
        let end = start
            .checked_advance(frame.len())
            .ok_or(InvalidReplicationPosition)?;
        let restored_reported_offset = self
            .restored_reported_offset
            .map(|reported| {
                reported
                    .checked_advance(frame.len())
                    .ok_or(InvalidReplicationPosition)
            })
            .transpose()?;

        self.bytes.extend(frame);
        self.next_cursor = end;
        self.restored_reported_offset = restored_reported_offset;
        self.history_valid = true;
        self.active = true;
        self.evict_excess();

        Ok(CursorRange::new(start, end))
    }

    pub fn reported_offset(&self) -> ReplicationOffset {
        self.restored_reported_offset
            .unwrap_or_else(|| self.next_cursor.as_reported_offset())
    }

    pub const fn next_stream_cursor(&self) -> BacklogCursor {
        self.next_cursor
    }

    pub fn snapshot_boundary(&self) -> SnapshotBoundary {
        SnapshotBoundary::new(self.reported_offset(), self.next_stream_cursor())
    }

    /// Validate an RDB's inclusive reported offset and derive its next-byte
    /// stream cursor without mutating backlog identity or history.
    pub fn rdb_snapshot_boundary(
        reported_offset: ReplicationOffset,
    ) -> Result<SnapshotBoundary, InvalidReplicationPosition> {
        let stream_cursor = reported_offset
            .checked_next_cursor()
            .ok_or(InvalidReplicationPosition)?;
        Ok(SnapshotBoundary::new(reported_offset, stream_cursor))
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Align an inactive empty backlog with the server's independent primary
    /// replication offset without retaining history.
    pub fn align_inactive_offset(
        &mut self,
        offset: ReplicationOffset,
    ) -> Result<(), InvalidReplicationPosition> {
        if self.active || !self.bytes.is_empty() {
            return Err(InvalidReplicationPosition);
        }
        self.next_cursor =
            BacklogCursor::try_from(offset.get()).map_err(|_| InvalidReplicationPosition)?;
        self.restored_reported_offset = None;
        self.history_valid = false;
        self.empty_cursor_valid = false;
        self.min_protected_cursor = None;
        Ok(())
    }

    /// Create the first history accepted by a replica full-sync session.
    /// C Valkey changes both replication IDs when the first replica creates a
    /// backlog, because the identity advertised before that point has no
    /// retained history behind it. The absolute stream cursor is preserved.
    /// Returns whether a new history was created.
    pub fn activate_new_history(&mut self) -> bool {
        if self.active {
            return false;
        }
        self.replid = crate::engine::distributed::replication::protocol::generate_replid();
        self.second_replid.clear();
        self.second_replid_offset = None;
        self.bytes.clear();
        self.history_valid = false;
        self.active = true;
        self.empty_cursor_valid = true;
        self.min_protected_cursor = None;
        true
    }

    /// Start retaining subsequent primary writes without inventing history.
    /// A primary activates its backlog as soon as it accepts a replica sync.
    /// If that replica disconnects before any later write, the first write
    /// must still be retained for its PSYNC reconnect.
    pub fn activate(&mut self) {
        self.active = true;
        self.empty_cursor_valid = true;
    }

    pub fn first_byte_cursor(&self) -> BacklogCursor {
        self.next_cursor
            .checked_retreat(self.bytes.len())
            .expect("retained backlog bytes cannot precede cursor zero")
    }

    /// Project the first retained byte into C Valkey's inclusive reported
    /// offset domain: `reported_offset - history_len + 1`.
    pub fn first_byte_reported_offset(&self) -> ReplicationOffset {
        if !self.active {
            return ReplicationOffset::ZERO;
        }
        self.reported_offset()
            .checked_retreat(self.bytes.len())
            .and_then(|offset| offset.checked_advance(1))
            .expect("retained history must fit within the reported replication range")
    }

    /// Return the logical start of the current shared replication-buffer tail.
    /// Valkey protects the block referenced by a waiting RDB client, not every
    /// byte retained before that block. The flat Rust backlog uses the same
    /// 16 KiB minimum block size for the equivalent eviction boundary.
    pub fn tail_block_start_cursor(&self) -> BacklogCursor {
        let block_start = self
            .next_cursor
            .checked_retreat(Self::BLOCK_SIZE)
            .unwrap_or(BacklogCursor::ZERO);
        block_start.max(self.first_byte_cursor())
    }

    pub fn history_len(&self) -> usize {
        self.bytes.len()
    }

    pub fn allocation_bytes(&self) -> usize {
        self.bytes
            .capacity()
            .saturating_mul(std::mem::size_of::<u8>())
    }

    pub fn clear_history(&mut self) {
        self.bytes.clear();
        self.history_valid = false;
        self.active = false;
        self.empty_cursor_valid = false;
        self.min_protected_cursor = None;
    }

    /// Expire an idle primary backlog as one lineage transition.
    ///
    /// This mirrors C Valkey's `changeReplicationId()` +
    /// `clearReplicationId2()` + `freeReplicationBacklog()` sequence. The
    /// absolute replication boundary survives, while every identity and byte
    /// that could authorize partial resynchronization is invalidated and the
    /// retained allocation is returned to the allocator.
    pub fn expire_history(&mut self) {
        self.replid = crate::engine::distributed::replication::protocol::generate_replid();
        self.second_replid.clear();
        self.second_replid_offset = None;
        self.bytes = VecDeque::new();
        self.history_valid = false;
        self.active = false;
        self.empty_cursor_valid = false;
        self.min_protected_cursor = None;
    }

    /// Resolve a checked next-byte PSYNC request into the internal backlog
    /// cursor that owns reads and retention checks.
    pub fn resolve_continue(
        &self,
        replid: &str,
        requested: PsyncRequestOffset,
    ) -> Option<ContinuePosition> {
        let history = self.continue_history(replid)?;
        // C Valkey `primaryTryPartialResynchronization()` compares the PSYNC
        // wire request directly with `second_replid_offset` before backlog
        // range checks (thirdparty/valkey/src/replication.c:895-898). Keeping
        // this check in the wire domain avoids a state-dependent off-by-one.
        if history == ContinueMatch::Second && requested > self.second_replid_offset? {
            return None;
        }
        let cursor = if self.restored_reported_offset.is_some() {
            requested.as_cursor()?
        } else {
            requested.previous_cursor()?
        };
        self.resolve_continue_cursor_for_history(history, cursor)
    }

    #[cfg(test)]
    fn resolve_continue_cursor(
        &self,
        replid: &str,
        cursor: BacklogCursor,
    ) -> Option<ContinuePosition> {
        let history = self.continue_history(replid)?;
        if history == ContinueMatch::Second {
            let boundary_cursor = if self.restored_reported_offset.is_some() {
                self.second_replid_offset?.as_cursor()?
            } else {
                self.second_replid_offset?.previous_cursor()?
            };
            if cursor > boundary_cursor {
                return None;
            }
        }
        self.resolve_continue_cursor_for_history(history, cursor)
    }

    fn continue_history(&self, replid: &str) -> Option<ContinueMatch> {
        if replid == self.replid {
            Some(ContinueMatch::Primary)
        } else if !self.second_replid.is_empty() && replid == self.second_replid {
            Some(ContinueMatch::Second)
        } else {
            None
        }
    }

    fn resolve_continue_cursor_for_history(
        &self,
        history: ContinueMatch,
        cursor: BacklogCursor,
    ) -> Option<ContinuePosition> {
        if self.bytes.is_empty()
            && cursor == self.next_cursor
            && ((history == ContinueMatch::Primary && self.empty_cursor_valid)
                || (history == ContinueMatch::Second && self.history_valid))
        {
            return Some(ContinuePosition { history, cursor });
        }
        if !self.history_valid || cursor < self.first_byte_cursor() || cursor > self.next_cursor {
            return None;
        }
        Some(ContinuePosition { history, cursor })
    }

    pub fn read_from(&self, cursor: BacklogCursor) -> Option<Vec<u8>> {
        let first = self.first_byte_cursor();
        if cursor < first || cursor > self.next_cursor {
            return None;
        }
        let skip = cursor.checked_distance_from(first)?;
        Some(self.bytes.iter().skip(skip).copied().collect())
    }

    pub fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
        self.evict_excess();
    }

    /// Set the eviction floor to the lowest cursor still needed by a connected
    /// replica. `None` trims strictly to capacity. Re-runs eviction so a raised
    /// floor releases newly evictable bytes immediately.
    pub fn set_min_protected_cursor(&mut self, cursor: Option<BacklogCursor>) {
        self.min_protected_cursor = cursor;
        self.evict_excess();
    }

    #[cfg(test)]
    pub(crate) const fn min_protected_cursor(&self) -> Option<BacklogCursor> {
        self.min_protected_cursor
    }

    pub fn replid(&self) -> &str {
        &self.replid
    }

    pub fn second_replid(&self) -> &str {
        &self.second_replid
    }

    pub(crate) const fn second_replid_request_boundary(&self) -> Option<PsyncRequestOffset> {
        self.second_replid_offset
    }

    /// Replace the current replication history identity and invalidate the
    /// secondary identity. This mirrors C Valkey's `changeReplicationId()` +
    /// `clearReplicationId2()` pair used by DEBUG CHANGE-REPL-ID.
    pub fn change_replication_id(&mut self) {
        self.replid = crate::engine::distributed::replication::protocol::generate_replid();
        self.second_replid.clear();
        self.second_replid_offset = None;
    }

    /// PSYNC2 replid rotation — called when a replica is promoted to primary
    /// (SLAVEOF/REPLICAOF NO ONE, or FAILOVER target handoff).
    /// Semantics (aligned with C Valkey `shiftReplicationId`):
    /// - If `second_replid` is empty (first promotion since startup), copy the
    ///   old `replid` into `second_replid` and set `second_replid_offset` to
    ///   the next stream offset. This is the cached-primary PSYNC cursor used
    ///   by C's `shiftReplicationId()` (`primary_repl_offset + 1`).
    /// - Generate and install a new primary `replid`.
    /// - Existing backlog bytes remain valid for both replids up to the boundary.
    ///
    /// Returns the newly generated primary replid.
    pub fn shift_replication_id(&mut self) -> &str {
        use crate::engine::distributed::replication::protocol;
        self.second_replid = std::mem::take(&mut self.replid);
        // C Valkey `shiftReplicationId()` stores `primary_repl_offset + 1`
        // because replid2 validity is expressed in the replica's next-byte
        // PSYNC request domain (thirdparty/valkey/src/replication.c:2148-2157).
        self.second_replid_offset = self.reported_offset().checked_next_request();
        if self.active {
            self.history_valid = true;
        }
        self.replid = protocol::generate_replid();
        &self.replid
    }

    /// Restore a primary from its persisted RDB replication boundary while
    /// rotating the local replication ID.
    pub fn restore_primary_from_rdb(
        &mut self,
        replid: &str,
        reported_offset: ReplicationOffset,
    ) -> Result<(), InvalidReplicationPosition> {
        let boundary = Self::rdb_snapshot_boundary(reported_offset)?;
        self.restore_primary_from_rdb_boundary(replid, boundary);
        Ok(())
    }

    pub fn restore_primary_from_rdb_boundary(&mut self, replid: &str, boundary: SnapshotBoundary) {
        use crate::engine::distributed::replication::protocol;

        self.second_replid = replid.to_string();
        self.second_replid_offset = Some(
            boundary
                .reported_offset()
                .checked_next_request()
                .expect("validated RDB boundary has a next PSYNC request"),
        );
        self.replid = protocol::generate_replid();
        self.bytes.clear();
        self.next_cursor = boundary.stream_cursor();
        self.restored_reported_offset = Some(boundary.reported_offset());
        self.history_valid = true;
        self.active = true;
        self.empty_cursor_valid = true;
        self.min_protected_cursor = None;
    }

    /// Install the primary identity received in a full resynchronization.
    /// A replica adopts the upstream history directly; unlike a primary
    /// loading its own RDB, it must not create a new local replication ID.
    pub fn adopt_primary_from_rdb(
        &mut self,
        replid: &str,
        reported_offset: ReplicationOffset,
    ) -> Result<(), InvalidReplicationPosition> {
        let boundary = Self::rdb_snapshot_boundary(reported_offset)?;
        self.adopt_primary_from_rdb_boundary(replid, boundary);
        Ok(())
    }

    /// Publish a previously validated RDB boundary as the active upstream history.
    pub fn adopt_primary_from_rdb_boundary(&mut self, replid: &str, boundary: SnapshotBoundary) {
        self.replid = replid.to_string();
        self.second_replid.clear();
        self.second_replid_offset = None;
        self.bytes.clear();
        self.next_cursor = boundary.stream_cursor();
        self.restored_reported_offset = Some(boundary.reported_offset());
        self.history_valid = false;
        self.active = true;
        self.empty_cursor_valid = true;
        self.min_protected_cursor = None;
    }

    /// Update a cascading replica after its primary accepts PSYNC with a new
    /// replication ID. Existing bytes remain valid under the old ID, while
    /// future bytes use the new upstream ID. This mirrors C's
    /// `second_replid_offset = primary_repl_offset + 1` path.
    pub fn try_adopt_changed_primary_replid(
        &mut self,
        replid: &str,
        primary_offset: ReplicationOffset,
    ) -> Result<(), InvalidReplicationPosition> {
        let boundary = primary_offset
            .checked_next_request()
            .ok_or(InvalidReplicationPosition)?;
        self.second_replid = self.replid.clone();
        self.second_replid_offset = Some(boundary);
        self.replid = replid.to_string();
        Ok(())
    }

    fn evict_excess(&mut self) {
        if self.bytes.len() <= self.capacity {
            return;
        }
        let excess = self.bytes.len() - self.capacity;
        // Never evict bytes a connected replica still needs to read. C Valkey
        // holds replication-buffer blocks referenced by a lagging replica past
        // `repl-backlog-size`; here the pin is the lowest send cursor
        // among streaming links (`min_protected_cursor`). Trim only up to that
        // floor, so an aggressively small `repl-backlog-size` (the shutdown
        // test sets 1 byte) can't tear a still-live replica link down.
        let first = self.first_byte_cursor();
        let drain = match self.min_protected_cursor {
            None => excess,
            Some(protected) if protected <= first => 0,
            Some(protected) => {
                let distance = protected
                    .checked_distance_from(first)
                    .expect("a later protection cursor has a non-negative distance");
                if distance >= excess {
                    excess
                } else {
                    distance
                }
            }
        };
        self.bytes.drain(..drain);
    }
}

#[cfg(test)]
mod tests {
    use super::{ContinueMatch, CursorRange, ReplicationBacklog};
    use crate::engine::distributed::replication::{
        BacklogCursor, InvalidReplicationPosition, PsyncRequestOffset, ReplicationOffset,
        SnapshotBoundary,
    };

    fn make_backlog(replid: &str, capacity: usize) -> ReplicationBacklog {
        ReplicationBacklog::new(replid.to_string(), capacity)
    }

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("test offset must be non-negative")
    }

    fn cursor(value: i64) -> BacklogCursor {
        BacklogCursor::try_from(value).expect("test cursor must be non-negative")
    }

    fn request(value: i64) -> PsyncRequestOffset {
        PsyncRequestOffset::try_from_raw(value).expect("test request must be non-negative")
    }

    fn continue_from_cursor(
        backlog: &ReplicationBacklog,
        replid: &str,
        value: i64,
    ) -> Option<ContinueMatch> {
        let cursor = BacklogCursor::try_from(value).ok()?;
        backlog
            .resolve_continue_cursor(replid, cursor)
            .map(|position| position.history)
    }

    #[test]
    fn normal_snapshot_boundary_uses_the_current_stream_cursor() {
        let mut backlog = make_backlog("replid", 64);
        backlog.append(b"abc").unwrap();

        assert_eq!(
            backlog.snapshot_boundary(),
            SnapshotBoundary::new(offset(3), cursor(3))
        );
    }

    #[test]
    fn restored_primary_keeps_reported_offset_and_next_cursor_distinct() {
        let mut backlog = make_backlog("replid", 64);
        backlog
            .restore_primary_from_rdb("rdb-replid", offset(41))
            .expect("valid persisted offset");

        assert_eq!(backlog.reported_offset(), offset(41));
        assert_eq!(backlog.next_stream_cursor(), cursor(42));
        assert_eq!(
            backlog.snapshot_boundary(),
            SnapshotBoundary::new(offset(41), cursor(42))
        );
        assert_eq!(
            backlog.append(b"x").unwrap(),
            CursorRange::new(cursor(42), cursor(43))
        );
        assert_eq!(backlog.read_from(cursor(42)), Some(b"x".to_vec()));
        assert_eq!(backlog.snapshot_boundary().reported_offset(), offset(42));
        assert_eq!(backlog.next_stream_cursor(), cursor(43));
    }

    #[test]
    fn restored_boundary_survives_history_reset_and_activation() {
        let mut backlog = make_backlog("local", 64);
        backlog
            .restore_primary_from_rdb("persisted", offset(41))
            .expect("valid persisted offset");

        backlog.clear_history();
        assert_eq!(
            backlog.snapshot_boundary(),
            SnapshotBoundary::new(offset(41), cursor(42))
        );

        assert!(backlog.activate_new_history());
        assert_eq!(
            backlog.snapshot_boundary(),
            SnapshotBoundary::new(offset(41), cursor(42))
        );
        assert_eq!(
            backlog.append(b"x").unwrap(),
            CursorRange::new(cursor(42), cursor(43))
        );
        assert_eq!(
            backlog.snapshot_boundary(),
            SnapshotBoundary::new(offset(42), cursor(43))
        );
    }

    #[test]
    fn adopted_primary_uses_the_rdb_reported_offset_and_next_cursor() {
        let mut backlog = make_backlog("local", 64);
        backlog
            .adopt_primary_from_rdb("upstream", offset(41))
            .expect("valid upstream offset");

        assert_eq!(backlog.reported_offset(), offset(41));
        assert_eq!(backlog.next_stream_cursor(), cursor(42));
        assert_eq!(
            backlog.snapshot_boundary(),
            SnapshotBoundary::new(offset(41), cursor(42))
        );
    }

    #[test]
    fn optional_protection_cursor_releases_trim_when_absent() {
        let mut backlog = make_backlog("replid", 1);
        backlog.set_min_protected_cursor(Some(cursor(0)));
        backlog.append(b"abc").unwrap();
        assert_eq!(backlog.first_byte_cursor(), cursor(0));

        backlog.set_min_protected_cursor(None);

        assert_eq!(backlog.first_byte_cursor(), cursor(2));
    }

    #[test]
    fn checked_rdb_restore_rejects_negative_and_overflowing_positions() {
        let mut backlog = make_backlog("replid", 64);

        assert_eq!(
            ReplicationBacklog::rdb_snapshot_boundary(offset(41)),
            Ok(SnapshotBoundary::new(offset(41), cursor(42)))
        );

        assert_eq!(
            ReplicationOffset::try_from(-1),
            Err(InvalidReplicationPosition)
        );
        assert_eq!(
            ReplicationBacklog::rdb_snapshot_boundary(offset(i64::MAX)),
            Err(InvalidReplicationPosition)
        );
        assert_eq!(
            backlog.restore_primary_from_rdb("rdb-replid", offset(i64::MAX)),
            Err(InvalidReplicationPosition)
        );
        assert_eq!(
            backlog.adopt_primary_from_rdb("upstream", offset(i64::MAX)),
            Err(InvalidReplicationPosition)
        );
        assert_eq!(
            backlog.try_adopt_changed_primary_replid("changed", offset(i64::MAX)),
            Err(InvalidReplicationPosition)
        );
        assert_eq!(backlog.replid(), "replid");
    }

    #[test]
    fn append_rejects_cursor_overflow_without_allocating() {
        let mut backlog = make_backlog("replid", 64);
        backlog.next_cursor = cursor(i64::MAX);

        assert_eq!(backlog.append(b"x"), Err(InvalidReplicationPosition));
        assert_eq!(backlog.next_stream_cursor(), cursor(i64::MAX));
        assert_eq!(backlog.history_len(), 0);
    }

    #[test]
    fn append_assigns_half_open_byte_offsets() {
        let mut backlog = make_backlog("replid", 16);

        assert_eq!(
            backlog.append(b"abc"),
            Ok(CursorRange::new(cursor(0), cursor(3)))
        );
        assert_eq!(
            backlog.append(b"de"),
            Ok(CursorRange::new(cursor(3), cursor(5)))
        );
        assert_eq!(backlog.reported_offset().get(), 5);
        assert_eq!(backlog.first_byte_cursor().get(), 0);
        assert_eq!(backlog.history_len(), 5);
    }

    #[test]
    fn allocation_bytes_include_retained_vecdeque_capacity() {
        let mut backlog = make_backlog("replid", 1024);
        let initial = backlog.allocation_bytes();
        assert!(initial >= 1024);

        backlog.append(&vec![b'x'; 4096]).unwrap();
        let grown = backlog.allocation_bytes();
        assert!(grown >= 4096);
        backlog.clear_history();
        assert_eq!(backlog.allocation_bytes(), grown);
    }

    #[test]
    fn expiring_history_rotates_lineage_and_releases_retained_allocation() {
        let mut backlog = make_backlog("original", 1024);
        backlog.append(&vec![b'x'; 4096]).unwrap();
        backlog.shift_replication_id();
        backlog.set_min_protected_cursor(Some(cursor(1)));
        let old_current = backlog.replid().to_string();
        let old_second = backlog.second_replid().to_string();
        let request = backlog
            .second_replid_request_boundary()
            .expect("shifted history has a second replid boundary");
        let boundary = backlog.snapshot_boundary();
        assert!(backlog.resolve_continue(&old_current, request).is_some());
        assert!(backlog.resolve_continue(&old_second, request).is_some());
        assert!(backlog.allocation_bytes() >= 4096);

        backlog.expire_history();

        assert_ne!(backlog.replid(), old_current);
        assert_ne!(backlog.replid(), old_second);
        assert!(backlog.second_replid().is_empty());
        assert_eq!(backlog.second_replid_request_boundary(), None);
        assert!(!backlog.is_active());
        assert_eq!(backlog.history_len(), 0);
        assert_eq!(backlog.allocation_bytes(), 0);
        assert_eq!(backlog.min_protected_cursor(), None);
        assert_eq!(backlog.snapshot_boundary(), boundary);
        assert!(backlog.resolve_continue(&old_current, request).is_none());
        assert!(backlog.resolve_continue(&old_second, request).is_none());
        assert!(backlog
            .resolve_continue(backlog.replid(), request)
            .is_none());
    }

    #[test]
    fn expiring_restored_history_preserves_its_absolute_boundary() {
        let mut backlog = make_backlog("local", 1024);
        backlog
            .restore_primary_from_rdb("persisted", offset(41))
            .expect("valid persisted offset");
        let boundary = backlog.snapshot_boundary();
        let old_replid = backlog.replid().to_string();

        backlog.expire_history();

        assert_ne!(backlog.replid(), old_replid);
        assert_eq!(boundary, SnapshotBoundary::new(offset(41), cursor(42)));
        assert_eq!(backlog.snapshot_boundary(), boundary);
        assert_eq!(backlog.reported_offset(), offset(41));
        assert_eq!(backlog.next_stream_cursor(), cursor(42));
    }

    #[test]
    fn activating_empty_backlog_accepts_its_current_cursor() {
        let mut backlog = make_backlog("replid", 16);
        assert_eq!(continue_from_cursor(&backlog, "replid", 0), None);

        backlog.activate();

        assert_eq!(
            continue_from_cursor(&backlog, "replid", 0),
            Some(ContinueMatch::Primary)
        );
        assert_eq!(backlog.read_from(cursor(0)), Some(Vec::new()));
    }

    #[test]
    fn read_from_returns_exact_retained_suffix() {
        let mut backlog = make_backlog("replid", 16);
        backlog.append(b"abcdef").unwrap();

        assert_eq!(backlog.read_from(cursor(0)), Some(b"abcdef".to_vec()));
        assert_eq!(backlog.read_from(cursor(3)), Some(b"def".to_vec()));
        assert_eq!(backlog.read_from(cursor(6)), Some(Vec::new()));
        assert_eq!(backlog.read_from(cursor(7)), None);
    }

    #[test]
    fn eviction_preserves_byte_offset_boundaries() {
        let mut backlog = make_backlog("replid", 4);
        backlog.append(b"abc").unwrap();
        backlog.append(b"def").unwrap();

        assert_eq!(backlog.first_byte_cursor().get(), 2);
        assert_eq!(backlog.reported_offset().get(), 6);
        assert_eq!(backlog.history_len(), 4);
        assert_eq!(backlog.read_from(cursor(1)), None);
        assert_eq!(backlog.read_from(cursor(2)), Some(b"cdef".to_vec()));
    }

    #[test]
    fn continuation_requires_matching_replid_and_retained_boundary() {
        let mut backlog = make_backlog("replid", 4);
        backlog.append(b"abcdef").unwrap();

        assert_eq!(continue_from_cursor(&backlog, "other", 2), None);
        assert_eq!(continue_from_cursor(&backlog, "replid", 1), None);
        assert_eq!(
            continue_from_cursor(&backlog, "replid", 2),
            Some(ContinueMatch::Primary)
        );
        assert_eq!(
            continue_from_cursor(&backlog, "replid", 6),
            Some(ContinueMatch::Primary)
        );
        assert_eq!(continue_from_cursor(&backlog, "replid", 7), None);
    }

    #[test]
    fn resizing_evicts_oldest_bytes_and_zero_capacity_keeps_offsets() {
        let mut backlog = make_backlog("replid", 8);
        backlog.append(b"abcdef").unwrap();
        backlog.set_capacity(3);

        assert_eq!(backlog.first_byte_cursor().get(), 3);
        assert_eq!(backlog.read_from(cursor(3)), Some(b"def".to_vec()));

        backlog.set_capacity(0);
        assert_eq!(backlog.history_len(), 0);
        assert_eq!(backlog.first_byte_cursor().get(), 6);
        assert_eq!(backlog.reported_offset().get(), 6);
        assert_eq!(backlog.read_from(cursor(6)), Some(Vec::new()));
    }

    #[test]
    fn protected_offset_pins_bytes_below_capacity() {
        // A tiny capacity (1 byte, as the shutdown test sets) must not evict
        // bytes a connected replica still needs. With the floor pinned at
        // offset 2, a resize to 1 keeps offsets 2..6 readable.
        let mut backlog = make_backlog("replid", 8);
        backlog.append(b"abcdef").unwrap(); // cursors 0..6
        backlog.set_min_protected_cursor(Some(cursor(2)));
        backlog.set_capacity(1);

        assert_eq!(backlog.first_byte_cursor().get(), 2);
        assert_eq!(backlog.history_len(), 4);
        assert_eq!(backlog.read_from(cursor(2)), Some(b"cdef".to_vec()));
    }

    #[test]
    fn appending_past_capacity_holds_bytes_for_lagging_replica() {
        // A 1-byte backlog with a replica pinned at offset 0 retains everything
        // appended after the pin, so pumping from cursor zero never hits an
        // evicted cursor — the shutdown-test teardown loop root cause.
        let mut backlog = make_backlog("replid", 1);
        backlog.set_min_protected_cursor(Some(cursor(0)));
        backlog.append(b"abc").unwrap();
        backlog.append(b"def").unwrap();

        assert_eq!(backlog.first_byte_cursor().get(), 0);
        assert_eq!(backlog.read_from(cursor(0)), Some(b"abcdef".to_vec()));
    }

    #[test]
    fn raising_protected_offset_releases_evictable_bytes() {
        // Once the replica drains up to offset 4, raising the floor lets the
        // over-capacity prefix be reclaimed down to `capacity`.
        let mut backlog = make_backlog("replid", 2);
        backlog.set_min_protected_cursor(Some(cursor(0)));
        backlog.append(b"abcdef").unwrap(); // held: floor pins cursor 0

        assert_eq!(backlog.history_len(), 6);
        backlog.set_min_protected_cursor(Some(cursor(4)));

        // capacity is 2, floor now at offset 4 → keep offsets 4..6.
        assert_eq!(backlog.first_byte_cursor().get(), 4);
        assert_eq!(backlog.read_from(cursor(4)), Some(b"ef".to_vec()));
    }

    #[test]
    fn no_protection_trims_strictly_to_capacity() {
        // An absent protection cursor trims strictly to capacity when no
        // replica is connected.
        let mut backlog = make_backlog("replid", 3);
        backlog.append(b"abcdef").unwrap();
        assert_eq!(backlog.first_byte_cursor().get(), 3);
        assert_eq!(backlog.history_len(), 3);
    }

    #[test]
    fn cleared_history_rejects_exact_current_tail_until_reactivated() {
        let mut backlog = make_backlog("replid", 64);
        backlog.append(b"abcdef").unwrap();
        backlog.clear_history();

        assert_eq!(backlog.reported_offset().get(), 6);
        assert_eq!(continue_from_cursor(&backlog, "replid", 6), None);

        backlog.activate();

        assert_eq!(
            continue_from_cursor(&backlog, "replid", 6),
            Some(ContinueMatch::Primary)
        );
        assert_eq!(backlog.read_from(cursor(6)), Some(Vec::new()));
    }

    #[test]
    fn upstream_rdb_install_rejects_a_cursor_past_the_empty_backlog() {
        let mut backlog = make_backlog("local", 64);
        backlog
            .adopt_primary_from_rdb("upstream", offset(42))
            .unwrap();

        assert_eq!(continue_from_cursor(&backlog, "upstream", 44), None);
    }

    #[test]
    fn upstream_rdb_install_accepts_its_exact_empty_cursor() {
        let mut backlog = make_backlog("local", 64);
        backlog
            .adopt_primary_from_rdb("upstream", offset(42))
            .unwrap();

        assert_eq!(
            continue_from_cursor(&backlog, "upstream", 43),
            Some(ContinueMatch::Primary)
        );
        assert_eq!(backlog.read_from(cursor(43)), Some(Vec::new()));
    }

    #[test]
    fn empty_backlog_accepts_second_replid_boundary() {
        let mut backlog = make_backlog("current", 64);
        backlog.second_replid = "previous".to_string();
        backlog.second_replid_offset = Some(request(13));
        backlog.next_cursor = cursor(12);
        backlog.history_valid = true;

        assert_eq!(
            continue_from_cursor(&backlog, "previous", 12),
            Some(ContinueMatch::Second)
        );
    }

    #[test]
    fn fully_evicted_history_rejects_the_stale_second_replid_boundary() {
        let mut backlog = make_backlog("current", 64);
        backlog
            .restore_primary_from_rdb("rdb-replid", offset(41))
            .expect("valid persisted offset");
        backlog.append(b"new").unwrap();
        backlog.set_capacity(0);

        assert_eq!(continue_from_cursor(&backlog, "rdb-replid", 42), None);
    }

    #[test]
    fn cleared_history_rejects_the_stale_second_replid_boundary() {
        let mut backlog = make_backlog("current", 64);
        backlog
            .restore_primary_from_rdb("rdb-replid", offset(41))
            .expect("valid persisted offset");
        backlog.clear_history();

        assert_eq!(continue_from_cursor(&backlog, "rdb-replid", 42), None);
    }

    #[test]
    fn exposes_replid() {
        let backlog = make_backlog("abc", 1);
        assert_eq!(backlog.replid(), "abc");
    }

    #[test]
    fn second_replid_allows_continuation_up_to_boundary() {
        let mut backlog = make_backlog("new-id", 64);
        // Simulate: after promotion, second_replid = "old-id" at offset 6,
        // and 3 more bytes written under new-id.
        backlog.second_replid = "old-id".to_string();
        backlog.second_replid_offset = Some(request(6));
        backlog.next_cursor = cursor(0);
        backlog.append(b"abcdef").unwrap(); // cursors 0..6 under old-id regime
        backlog.append(b"ghi").unwrap(); // cursors 6..9 under new-id

        // Offset 3 under old-id: within backlog AND within second_replid_offset (5) → ok
        assert_eq!(
            continue_from_cursor(&backlog, "old-id", 3),
            Some(ContinueMatch::Second)
        );
        // Offset 5 under old-id: last byte under old id → ok
        assert_eq!(
            continue_from_cursor(&backlog, "old-id", 5),
            Some(ContinueMatch::Second)
        );
        // Offset 6 under old-id: past second_replid_offset (5) — new-id data, can't serve
        assert_eq!(continue_from_cursor(&backlog, "old-id", 6), None);
        // Offset 3 under new-id (current replid): normal continuation
        assert_eq!(
            continue_from_cursor(&backlog, "new-id", 3),
            Some(ContinueMatch::Primary)
        );
        // Offset evicted
        assert!(BacklogCursor::try_from(-1).is_err());
    }

    #[test]
    fn shift_replication_id_promotes_old_to_second_at_boundary() {
        let mut backlog = make_backlog("old-primary", 64);
        backlog.append(b"abcdef").unwrap();
        let cur_before = backlog.reported_offset().get();
        assert_eq!(cur_before, 6);

        let new_id = backlog.shift_replication_id().to_string();
        assert_ne!(new_id, "old-primary");
        assert_eq!(backlog.second_replid(), "old-primary");
        assert_eq!(
            backlog
                .second_replid_request_boundary()
                .map(|request| request.get())
                .unwrap_or(-1),
            7
        ); // highest accepted PSYNC wire request
        assert_eq!(backlog.replid(), new_id);
        assert_eq!(backlog.reported_offset().get(), 6);
        assert_eq!(backlog.read_from(cursor(0)), Some(b"abcdef".to_vec()));

        // Appending after shift still works; offsets continue
        backlog.append(b"gh").unwrap();
        assert_eq!(backlog.reported_offset().get(), 8);
        assert_eq!(backlog.read_from(cursor(0)), Some(b"abcdefgh".to_vec()));

        // Every promotion records the immediately preceding history, matching
        // C Valkey's shiftReplicationId().
        let second_new = backlog.shift_replication_id().to_string();
        assert_ne!(second_new, new_id);
        assert_eq!(backlog.second_replid(), new_id);
        assert_eq!(
            backlog
                .second_replid_request_boundary()
                .map(|request| request.get())
                .unwrap_or(-1),
            9
        );
    }

    #[test]
    fn second_replid_validity_is_a_wire_request_boundary_for_normal_lineage() {
        let mut backlog = make_backlog("old-primary", 64);
        backlog.append(b"abcdef").unwrap();
        backlog.shift_replication_id();

        let at_boundary = backlog
            .resolve_continue("old-primary", request(7))
            .expect("the next-byte request at the replid2 boundary must continue");
        assert_eq!(at_boundary.history, ContinueMatch::Second);
        assert_eq!(at_boundary.cursor, cursor(6));
        assert!(backlog
            .resolve_continue("old-primary", request(8))
            .is_none());

        backlog.append(b"g").unwrap();
        assert!(backlog
            .resolve_continue("old-primary", request(7))
            .is_some());
        assert!(backlog
            .resolve_continue("old-primary", request(8))
            .is_none());
    }

    #[test]
    fn second_replid_validity_is_equivalent_for_restored_lineage() {
        let mut backlog = make_backlog("local", 64);
        backlog
            .restore_primary_from_rdb("persisted", offset(41))
            .expect("valid persisted offset");
        let old_primary = backlog.replid().to_string();
        backlog.shift_replication_id();

        let at_boundary = backlog
            .resolve_continue(&old_primary, request(42))
            .expect("the restored next-byte request at the boundary must continue");
        assert_eq!(at_boundary.history, ContinueMatch::Second);
        assert_eq!(at_boundary.cursor, cursor(42));
        assert!(backlog
            .resolve_continue(&old_primary, request(43))
            .is_none());

        backlog.append(b"x").unwrap();
        assert!(backlog
            .resolve_continue(&old_primary, request(42))
            .is_some());
        assert!(backlog
            .resolve_continue(&old_primary, request(43))
            .is_none());
    }

    #[test]
    fn normal_lineage_resolves_next_byte_requests_to_retained_cursors() {
        let mut backlog = make_backlog("current", 4);
        backlog.append(b"abcdef").unwrap();

        assert_eq!(
            backlog.snapshot_boundary(),
            SnapshotBoundary::new(offset(6), cursor(6))
        );
        let first = backlog.resolve_continue("current", request(3)).unwrap();
        assert_eq!(first.history, ContinueMatch::Primary);
        assert_eq!(first.cursor, cursor(2));
        assert_eq!(backlog.read_from(first.cursor), Some(b"cdef".to_vec()));

        let last = backlog.resolve_continue("current", request(6)).unwrap();
        assert_eq!(last.cursor, cursor(5));
        assert_eq!(backlog.read_from(last.cursor), Some(b"f".to_vec()));

        let empty = backlog.resolve_continue("current", request(7)).unwrap();
        assert_eq!(empty.cursor, cursor(6));
        assert_eq!(backlog.read_from(empty.cursor), Some(Vec::new()));
        assert!(backlog.resolve_continue("current", request(2)).is_none());
        assert!(backlog.resolve_continue("current", request(8)).is_none());
    }

    #[test]
    fn restored_lineage_resolves_wire_requests_directly_to_retained_cursors() {
        let mut backlog = make_backlog("local", 4);
        backlog
            .restore_primary_from_rdb("previous", offset(1))
            .unwrap();
        let current = backlog.replid().to_string();
        backlog.append(b"abcdef").unwrap();

        assert_eq!(
            backlog.snapshot_boundary(),
            SnapshotBoundary::new(offset(7), cursor(8))
        );
        let first = backlog.resolve_continue(&current, request(4)).unwrap();
        assert_eq!(first.history, ContinueMatch::Primary);
        assert_eq!(first.cursor, cursor(4));
        assert_eq!(backlog.read_from(first.cursor), Some(b"cdef".to_vec()));

        let last = backlog.resolve_continue(&current, request(7)).unwrap();
        assert_eq!(last.cursor, cursor(7));
        assert_eq!(backlog.read_from(last.cursor), Some(b"f".to_vec()));

        let empty = backlog.resolve_continue(&current, request(8)).unwrap();
        assert_eq!(empty.cursor, cursor(8));
        assert_eq!(backlog.read_from(empty.cursor), Some(Vec::new()));
        assert!(backlog.resolve_continue(&current, request(3)).is_none());
        assert!(backlog.resolve_continue(&current, request(9)).is_none());
    }

    #[test]
    fn continuation_rejects_zero_negative_and_overflowing_wire_requests() {
        let mut normal = make_backlog("normal", 64);
        normal.append(b"abc").unwrap();
        assert!(normal.resolve_continue("normal", request(0)).is_none());
        assert!(normal
            .resolve_continue("normal", request(i64::MAX))
            .is_none());
        assert!(PsyncRequestOffset::try_from_raw(-1).is_err());

        let mut restored = make_backlog("local", 64);
        restored
            .restore_primary_from_rdb("previous", offset(41))
            .unwrap();
        let current = restored.replid().to_string();
        assert!(restored.resolve_continue(&current, request(0)).is_none());
        assert!(restored
            .resolve_continue(&current, request(i64::MAX))
            .is_none());
    }

    #[test]
    fn second_replid_accepts_only_its_highest_wire_boundary_in_both_lineages() {
        let mut normal = make_backlog("normal-old", 64);
        normal.append(b"abcdef").unwrap();
        normal.shift_replication_id();
        normal.append(b"gh").unwrap();
        let accepted = normal.resolve_continue("normal-old", request(7)).unwrap();
        assert_eq!(accepted.history, ContinueMatch::Second);
        assert_eq!(accepted.cursor, cursor(6));
        assert!(normal.resolve_continue("normal-old", request(8)).is_none());

        let mut restored = make_backlog("local", 64);
        restored
            .restore_primary_from_rdb("restored-old", offset(41))
            .unwrap();
        restored.append(b"gh").unwrap();
        let accepted = restored
            .resolve_continue("restored-old", request(42))
            .unwrap();
        assert_eq!(accepted.history, ContinueMatch::Second);
        assert_eq!(accepted.cursor, cursor(42));
        assert!(restored
            .resolve_continue("restored-old", request(43))
            .is_none());
    }
}
