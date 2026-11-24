//! Bounded independent target copies retained until ordered serialization.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

const EARLY_CAPTURE_CAPACITY: usize = 64;
const EARLY_CAPTURE_BYTES: usize = EARLY_CAPTURE_CAPACITY * LARGE_VALUE_HANDOFF_THRESHOLD;

pub(super) struct EarlyCapture {
    pub(super) entry: Option<FrozenEntry>,
    bytes: usize,
    reservation: Option<EarlyCaptureReservation>,
}

#[derive(Default)]
pub(super) struct FrozenCapture {
    count: AtomicUsize,
    bytes: AtomicUsize,
}

impl FrozenCapture {
    fn reserve(self: &Arc<Self>, bytes: usize) -> EarlyCaptureReservation {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        EarlyCaptureReservation {
            budget: Arc::clone(self),
            bytes,
        }
    }
}

pub(super) struct EarlyCaptureReservation {
    budget: Arc<FrozenCapture>,
    bytes: usize,
}

impl Drop for EarlyCaptureReservation {
    fn drop(&mut self) {
        self.budget.bytes.fetch_sub(self.bytes, Ordering::Relaxed);
        self.budget.count.fetch_sub(1, Ordering::Relaxed);
    }
}

impl ForklessFeed {
    pub(super) fn capture_future_priorities(
        &mut self,
        primary: &DbSet,
        additional_datasets: &[(Vec<u8>, &mut DbSet)],
        budget: usize,
    ) {
        let mut idle_databases = self.priority_databases.len();
        for _ in 0..budget.min(EARLY_CAPTURE_CAPACITY) {
            if self.early_capture_budget.count.load(Ordering::Relaxed) == EARLY_CAPTURE_CAPACITY
                || idle_databases == 0
            {
                break;
            }
            let Some(id) = self.priority_databases.pop_front() else {
                break;
            };
            self.queued_priority_databases.remove(&id);
            let database_index = self.database_lookup.get(&id).copied();
            if database_index == Some(self.database_index) && !self.is_backpressured() {
                self.queue_priority_database(id);
                idle_databases -= 1;
                continue;
            }
            let Some(completion) = self
                .priority_keys
                .get_mut(&id)
                .and_then(VecDeque::pop_front)
            else {
                self.priority_keys.remove(&id);
                idle_databases -= 1;
                continue;
            };
            if !self.priority_requested.contains(&completion) {
                // Ordinary scanning can consume a requested key before its
                // priority queue slot is visited. Never capture that slot twice.
                if self.priority_keys.get(&id).is_some_and(VecDeque::is_empty) {
                    self.priority_keys.remove(&id);
                }
                self.queue_priority_database(id);
                idle_databases = self.priority_databases.len();
                continue;
            }
            let current_db_id = database_index
                .filter(|index| *index >= self.database_index)
                .map(|index| self.databases[index].current_id);
            let Some(mut capture) = capture_early_forkless_entry(
                primary,
                additional_datasets,
                &completion,
                current_db_id,
                self.capture_ms,
                self.should_preserve_expired,
                EARLY_CAPTURE_BYTES - self.early_capture_budget.bytes.load(Ordering::Relaxed),
            ) else {
                // Oversized targets rotate behind other requests. They remain
                // blocked until space is available or the ordinary scan arrives.
                self.priority_keys
                    .entry(id.clone())
                    .or_default()
                    .push_back(completion);
                self.queue_priority_database(id);
                idle_databases -= 1;
                continue;
            };
            self.priority_requested.remove(&completion);
            self.captured_keys.push(completion.clone());
            self.resolved_keys.push(completion.clone());
            if let Some(index) = database_index.filter(|index| *index >= self.database_index) {
                // Tombstones also occupy a slot: creating an absent target after
                // release must not introduce it into the point-in-time snapshot.
                self.scheduled_keys.insert(completion);
                let reservation = self.early_capture_budget.reserve(capture.bytes);
                if let Some(entry) = &mut capture.entry {
                    entry._early_capture_reservation = Some(reservation);
                } else {
                    capture.reservation = Some(reservation);
                }
                self.databases[index].early_captures.push_back(capture);
            }
            if self.priority_keys.get(&id).is_some_and(VecDeque::is_empty) {
                self.priority_keys.remove(&id);
            }
            self.queue_priority_database(id);
            idle_databases = self.priority_databases.len();
        }
    }

    pub(super) fn queue_priority_database(&mut self, id: ForklessDatabaseId) {
        if self.priority_keys.contains_key(&id) && self.queued_priority_databases.insert(id.clone())
        {
            self.priority_databases.push_back(id);
        }
    }
}

fn capture_early_forkless_entry(
    primary: &DbSet,
    additional_datasets: &[(Vec<u8>, &mut DbSet)],
    completion: &ForklessItemId,
    current_db_id: Option<u32>,
    capture_ms: u64,
    should_preserve_expired: bool,
    available_bytes: usize,
) -> Option<EarlyCapture> {
    let mut bytes = std::mem::size_of::<EarlyCapture>()
        .saturating_add(std::mem::size_of::<ForklessItemId>())
        .saturating_add(completion.key.len().saturating_mul(4))
        .saturating_add(completion.dataset_id.as_ref().map_or(0, Vec::len));
    if bytes > available_bytes {
        return None;
    }
    let missing = || EarlyCapture {
        entry: None,
        bytes,
        reservation: None,
    };
    let Some(db) = current_db_id.and_then(|id| {
        find_dataset(
            primary,
            additional_datasets,
            completion.dataset_id.as_deref(),
        )?
        .get(id)
    }) else {
        return Some(missing());
    };
    let expire_ms = db.get_expire(&completion.key);
    if !should_preserve_expired && expire_ms.is_some_and(|expire| expire <= capture_ms) {
        return Some(missing());
    }
    let _suppression = crate::foundation::memory::key_access::KeyAccessSuppression::enter();
    let Some(value) = db.get_allow_expired(&completion.key) else {
        return Some(missing());
    };
    bytes = bytes.saturating_add(early_value_bytes(value, available_bytes - bytes)?);
    let hash_field_expires = db.hash_field_expires_for(&completion.key);
    if let Some(expires) = hash_field_expires {
        bytes = bytes.saturating_add(
            expires
                .capacity()
                .saturating_mul(2 * (std::mem::size_of::<(Vec<u8>, u64)>() + 1)),
        );
        if bytes > available_bytes {
            return None;
        }
        for field in expires.keys() {
            bytes = bytes.saturating_add(field.len());
            if bytes > available_bytes {
                return None;
            }
        }
    }
    if bytes > available_bytes {
        return None;
    }
    // Clone the Robj itself, never its live Arc. Mutable collection storage is
    // independent; immutable Bytes payloads retain their normal sharing contract.
    Some(EarlyCapture {
        entry: Some(FrozenEntry {
            key: completion.key.clone(),
            value: SnapshotValue::Owned(value.clone()),
            expire_ms,
            hash_field_expires: hash_field_expires.cloned(),
            completion: None,
            _early_capture_reservation: None,
        }),
        bytes,
        reservation: None,
    })
}

fn early_value_bytes(
    value: &crate::foundation::io::store::Robj,
    available_bytes: usize,
) -> Option<usize> {
    use crate::foundation::io::store::Robj;

    let retained_bytes = match value {
        Robj::ListQuicklist(list) => list
            .capacity()
            .saturating_mul(std::mem::size_of::<bytes::Bytes>()),
        Robj::HashHashtable(hash) => hash.capacity().saturating_mul(
            2 * (2 * std::mem::size_of::<bytes::Bytes>() + 2 * std::mem::size_of::<usize>() + 1),
        ),
        Robj::SetHashtable(set) => set
            .data
            .bucket_count()
            .saturating_mul(2 * std::mem::size_of::<usize>())
            .saturating_add(
                set.data
                    .len()
                    .saturating_mul(8 * std::mem::size_of::<bytes::Bytes>()),
            ),
        Robj::ZSetSkiplist(zset) => zset.dict().allocated_bytes(),
        // The generic stream estimator excludes groups and retained deleted
        // nodes. Keep the ordinary handoff until their copy cost is measurable.
        Robj::StreamListpack(_) => return None,
        _ => 0,
    };
    let bytes = std::mem::size_of::<Robj>().saturating_add(retained_bytes);
    if bytes > available_bytes
        || value
            .as_collection()
            .is_some_and(|collection| collection.len() > available_bytes / 32)
    {
        return None;
    }
    let bytes = bytes.saturating_add(value.estimated_size());
    (bytes <= available_bytes).then_some(bytes)
}

#[cfg(test)]
#[path = "capture_tests.rs"]
mod tests;
