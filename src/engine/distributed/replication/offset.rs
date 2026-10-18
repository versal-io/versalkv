#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicationOffset(i64);

impl ReplicationOffset {
    pub const ZERO: Self = Self(0);

    pub const fn get(self) -> i64 {
        self.0
    }

    pub fn checked_advance(self, byte_count: usize) -> Option<Self> {
        let byte_count = i64::try_from(byte_count).ok()?;
        self.0.checked_add(byte_count).map(Self)
    }

    pub(crate) fn checked_retreat(self, byte_count: usize) -> Option<Self> {
        let byte_count = i64::try_from(byte_count).ok()?;
        self.0
            .checked_sub(byte_count)
            .filter(|value| *value >= 0)
            .map(Self)
    }

    pub(crate) fn checked_next_request(self) -> Option<PsyncRequestOffset> {
        PsyncRequestOffset::from_reported_next(self)
    }

    pub(crate) fn checked_next_cursor(self) -> Option<BacklogCursor> {
        self.0.checked_add(1).map(BacklogCursor)
    }
}

impl TryFrom<i64> for ReplicationOffset {
    type Error = InvalidReplicationPosition;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value < 0 {
            Err(InvalidReplicationPosition)
        } else {
            Ok(Self(value))
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BacklogCursor(i64);

impl BacklogCursor {
    pub const ZERO: Self = Self(0);

    pub const fn get(self) -> i64 {
        self.0
    }

    pub(crate) const fn as_reported_offset(self) -> ReplicationOffset {
        ReplicationOffset(self.0)
    }

    pub fn checked_advance(self, byte_count: usize) -> Option<Self> {
        let byte_count = i64::try_from(byte_count).ok()?;
        self.0.checked_add(byte_count).map(Self)
    }

    pub(crate) fn checked_retreat(self, byte_count: usize) -> Option<Self> {
        let byte_count = i64::try_from(byte_count).ok()?;
        self.0
            .checked_sub(byte_count)
            .filter(|value| *value >= 0)
            .map(Self)
    }

    pub(crate) fn checked_distance_from(self, earlier: Self) -> Option<usize> {
        usize::try_from(self.0.checked_sub(earlier.0)?).ok()
    }
}

impl TryFrom<i64> for BacklogCursor {
    type Error = InvalidReplicationPosition;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value < 0 {
            Err(InvalidReplicationPosition)
        } else {
            Ok(Self(value))
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PsyncRequestOffset(i64);

impl PsyncRequestOffset {
    pub(crate) fn try_from_raw(value: i64) -> Result<Self, InvalidReplicationPosition> {
        if value < 0 {
            Err(InvalidReplicationPosition)
        } else {
            Ok(Self(value))
        }
    }

    pub(crate) const fn get(self) -> i64 {
        self.0
    }

    pub(crate) fn as_cursor(self) -> Option<BacklogCursor> {
        BacklogCursor::try_from(self.0).ok()
    }

    pub(crate) fn previous_cursor(self) -> Option<BacklogCursor> {
        BacklogCursor::try_from(self.0.checked_sub(1)?).ok()
    }

    pub(crate) fn from_reported_next(reported_offset: ReplicationOffset) -> Option<Self> {
        reported_offset.0.checked_add(1).map(Self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotBoundary {
    reported_offset: ReplicationOffset,
    stream_cursor: BacklogCursor,
}

impl SnapshotBoundary {
    pub(super) const fn new(
        reported_offset: ReplicationOffset,
        stream_cursor: BacklogCursor,
    ) -> Self {
        Self {
            reported_offset,
            stream_cursor,
        }
    }

    pub const fn reported_offset(self) -> ReplicationOffset {
        self.reported_offset
    }

    pub const fn stream_cursor(self) -> BacklogCursor {
        self.stream_cursor
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InvalidReplicationPosition;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_is_a_valid_reported_offset() {
        assert_eq!(ReplicationOffset::ZERO.get(), 0);
        assert_eq!(
            ReplicationOffset::ZERO,
            ReplicationOffset::try_from(0).unwrap()
        );
    }

    #[test]
    fn snapshot_boundary_preserves_distinct_reported_and_stream_positions() {
        let boundary = SnapshotBoundary::new(
            ReplicationOffset::try_from(41).unwrap(),
            BacklogCursor::try_from(42).unwrap(),
        );

        assert_eq!(boundary.reported_offset().get(), 41);
        assert_eq!(boundary.stream_cursor().get(), 42);
    }

    #[test]
    fn live_offsets_reject_negative_and_overflowing_values() {
        assert!(ReplicationOffset::try_from(-1).is_err());
        assert!(BacklogCursor::try_from(-1).is_err());
        assert!(ReplicationOffset::try_from(i64::MAX)
            .unwrap()
            .checked_next_request()
            .is_none());
    }

    #[test]
    fn checked_advancement_preserves_each_offset_domain() {
        let reported = ReplicationOffset::try_from(41).unwrap();
        let cursor = BacklogCursor::try_from(42).unwrap();

        let advanced_reported: ReplicationOffset = reported.checked_advance(2).unwrap();
        let advanced_cursor: BacklogCursor = cursor.checked_advance(3).unwrap();

        assert_eq!(advanced_reported.get(), 43);
        assert_eq!(advanced_cursor.get(), 45);
        assert_eq!(
            cursor.as_reported_offset(),
            ReplicationOffset::try_from(42).unwrap()
        );
        assert!(ReplicationOffset::try_from(i64::MAX)
            .unwrap()
            .checked_advance(1)
            .is_none());
        assert!(BacklogCursor::try_from(i64::MAX)
            .unwrap()
            .checked_advance(1)
            .is_none());
    }

    #[test]
    fn typed_cursor_arithmetic_preserves_domain_boundaries() {
        let reported = ReplicationOffset::try_from(41).unwrap();
        let first = BacklogCursor::try_from(42).unwrap();
        let end = BacklogCursor::try_from(47).unwrap();
        let request = PsyncRequestOffset::try_from_raw(42).unwrap();

        assert_eq!(
            reported.checked_retreat(1),
            Some(ReplicationOffset::try_from(40).unwrap())
        );
        assert_eq!(end.checked_retreat(5), Some(first));
        assert_eq!(end.checked_distance_from(first), Some(5));
        assert_eq!(request.as_cursor(), Some(first));
        assert_eq!(
            request.previous_cursor(),
            Some(BacklogCursor::try_from(41).unwrap())
        );
        assert_eq!(BacklogCursor::ZERO.checked_retreat(1), None);
    }

    #[test]
    fn checked_advancement_rejects_byte_counts_outside_i64() {
        if i64::try_from(usize::MAX).is_err() {
            let reported = ReplicationOffset::try_from(0).unwrap();
            let cursor = BacklogCursor::try_from(0).unwrap();

            assert!(reported.checked_advance(usize::MAX).is_none());
            assert!(cursor.checked_advance(usize::MAX).is_none());
        }
    }

    #[test]
    fn psync_request_offsets_are_checked_transient_values() {
        let reported = ReplicationOffset::try_from(41).unwrap();
        let request = PsyncRequestOffset::from_reported_next(reported).unwrap();

        assert_eq!(request.get(), 42);
        assert_eq!(PsyncRequestOffset::try_from_raw(42).unwrap(), request);
        assert!(PsyncRequestOffset::try_from_raw(-1).is_err());
    }
}
