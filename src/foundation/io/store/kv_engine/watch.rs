//! Per-key WATCH epochs for value replacement and physical deletion.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
struct KeyEpoch {
    writes: AtomicU64,
    deletions: AtomicU64,
    snapshots: AtomicUsize,
}

#[derive(Debug, Default)]
struct Registry {
    keys: Mutex<HashMap<Vec<u8>, Arc<KeyEpoch>>>,
    key_count: AtomicUsize,
}

#[derive(Default)]
pub(super) struct WatchEpochs {
    registry: Option<Arc<Registry>>,
}

#[derive(Debug)]
pub struct WatchSnapshot {
    registry: Arc<Registry>,
    key: Vec<u8>,
    epoch: Arc<KeyEpoch>,
    writes: u64,
    deletions: u64,
}

impl WatchEpochs {
    pub(super) fn watch(&mut self, key: &[u8]) -> WatchSnapshot {
        let registry = self.registry.get_or_insert_with(Default::default);
        let mut keys = registry
            .keys
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let epoch = keys.entry(key.to_vec()).or_insert_with(|| {
            registry.key_count.fetch_add(1, Ordering::Relaxed);
            Arc::default()
        });
        epoch.snapshots.fetch_add(1, Ordering::Relaxed);
        WatchSnapshot {
            registry: Arc::clone(registry),
            key: key.to_vec(),
            writes: epoch.writes.load(Ordering::Relaxed),
            deletions: epoch.deletions.load(Ordering::Relaxed),
            epoch: Arc::clone(epoch),
        }
    }

    fn advance(&self, key: &[u8], is_deletion: bool) {
        let Some(registry) = &self.registry else {
            return;
        };
        if registry.key_count.load(Ordering::Relaxed) == 0 {
            return;
        }
        let keys = registry
            .keys
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(epoch) = keys.get(key) {
            let counter = if is_deletion {
                &epoch.deletions
            } else {
                &epoch.writes
            };
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn written(&self, key: &[u8]) {
        self.advance(key, false);
    }

    pub(super) fn deleted(&self, key: &[u8]) {
        self.advance(key, true);
    }
}

impl WatchSnapshot {
    pub fn has_changed(&self, was_expired: bool) -> bool {
        self.epoch.writes.load(Ordering::Relaxed) != self.writes
            || (!was_expired && self.epoch.deletions.load(Ordering::Relaxed) != self.deletions)
    }

    pub fn acknowledge_deletion(&mut self) {
        self.deletions = self.epoch.deletions.load(Ordering::Relaxed);
    }
}

impl PartialEq for WatchSnapshot {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.epoch, &other.epoch)
            && self.writes == other.writes
            && self.deletions == other.deletions
    }
}

impl Eq for WatchSnapshot {}

impl Clone for WatchSnapshot {
    fn clone(&self) -> Self {
        let snapshot = Self {
            registry: Arc::clone(&self.registry),
            key: self.key.clone(),
            epoch: Arc::clone(&self.epoch),
            writes: self.writes,
            deletions: self.deletions,
        };
        self.epoch.snapshots.fetch_add(1, Ordering::Relaxed);
        snapshot
    }
}

impl Drop for WatchSnapshot {
    fn drop(&mut self) {
        let mut keys = self
            .registry
            .keys
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // Count leases explicitly: another snapshot's fields may still own an
        // Arc after its destructor has released this registry lock.
        if self.epoch.snapshots.fetch_sub(1, Ordering::Relaxed) == 1 {
            keys.remove(self.key.as_slice());
            self.registry.key_count.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::WatchEpochs;
    use std::sync::atomic::Ordering;

    #[test]
    fn every_expired_watcher_observes_an_overwrite() {
        let mut epochs = WatchEpochs::default();
        let first = epochs.watch(b"x");
        let second = epochs.watch(b"x");

        epochs.written(b"x");

        assert!(first.has_changed(true));
        assert!(second.has_changed(true));
    }

    #[test]
    fn expired_deletion_is_ignored_but_recreation_is_not() {
        let mut epochs = WatchEpochs::default();
        let stale = epochs.watch(b"x");
        let live = epochs.watch(b"x");

        epochs.deleted(b"x");

        assert!(!stale.has_changed(true));
        assert!(live.has_changed(false));
        epochs.written(b"x");
        assert!(stale.has_changed(true));
    }

    #[test]
    fn acknowledgement_only_clears_the_deletion_revision() {
        let mut epochs = WatchEpochs::default();
        let mut watched = epochs.watch(b"x");
        epochs.deleted(b"x");
        watched.acknowledge_deletion();
        assert!(!watched.has_changed(false));

        epochs.written(b"x");
        watched.acknowledge_deletion();
        assert!(watched.has_changed(false));
    }

    #[test]
    fn epochs_are_scoped_to_the_key_and_database() {
        let mut first_db = WatchEpochs::default();
        let mut second_db = WatchEpochs::default();
        let first = first_db.watch(b"x");
        let unrelated = first_db.watch(b"y");
        let other_db = second_db.watch(b"x");

        first_db.written(b"x");

        assert!(first.has_changed(true));
        assert!(!unrelated.has_changed(true));
        assert!(!other_db.has_changed(true));
        assert!(!first_db.watch(b"x").has_changed(true));
    }

    #[test]
    fn last_snapshot_releases_the_registry_entry() {
        let mut epochs = WatchEpochs::default();
        let first = epochs.watch(b"x");
        let second = first.clone();
        let registry = epochs.registry.as_ref().unwrap();
        drop(first);
        assert_eq!(registry.key_count.load(Ordering::Relaxed), 1);
        drop(second);
        assert_eq!(registry.key_count.load(Ordering::Relaxed), 0);
        assert!(registry.keys.lock().unwrap().is_empty());

        epochs.written(b"x");
        assert!(!epochs.watch(b"x").has_changed(false));
    }

    #[test]
    fn snapshot_outlives_the_database_registry_owner() {
        let watched = {
            let mut epochs = WatchEpochs::default();
            epochs.watch(b"x")
        };
        assert!(!watched.has_changed(true));
    }

    #[test]
    fn concurrent_snapshot_release_removes_the_last_registry_entry() {
        let mut epochs = WatchEpochs::default();
        let first = epochs.watch(b"x");
        let mut snapshots = vec![first];
        for _ in 0..15 {
            snapshots.push(snapshots[0].clone());
        }
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(snapshots.len()));
        let threads: Vec<_> = snapshots
            .into_iter()
            .map(|snapshot| {
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    drop(snapshot);
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }

        let registry = epochs.registry.as_ref().unwrap();
        assert_eq!(registry.key_count.load(Ordering::Relaxed), 0);
        assert!(registry.keys.lock().unwrap().is_empty());
    }
}
