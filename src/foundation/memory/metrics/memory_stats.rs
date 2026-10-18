use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryCategory {
    Dataset = 0,
    ReplBacklog = 1,
    ClientBuffers = 2,
    AofBuffer = 3,
    LuaVm = 4,
    FunctionsVm = 5,
    ScriptsCaches = 6,
    ClusterLinks = 7,
    Other = 8,
}

impl MemoryCategory {
    pub const NUM_CATEGORIES: usize = 9;
}

pub struct MemoryCategoryTracker {
    categories: [AtomicUsize; MemoryCategory::NUM_CATEGORIES],
    startup_snapshot: AtomicUsize,
}

impl MemoryCategoryTracker {
    pub fn new() -> Self {
        let tracker = Self {
            categories: Default::default(),
            startup_snapshot: AtomicUsize::new(0),
        };
        let initial = crate::foundation::memory::util_alloc::total_allocated();
        tracker.startup_snapshot.store(initial, Ordering::Relaxed);
        tracker
    }

    #[inline]
    pub fn add(&self, cat: MemoryCategory, size: usize) {
        self.categories[cat as usize].fetch_add(size, Ordering::Relaxed);
    }

    #[inline]
    pub fn sub(&self, cat: MemoryCategory, size: usize) {
        self.categories[cat as usize].fetch_sub(size, Ordering::Relaxed);
    }

    #[inline]
    pub fn get(&self, cat: MemoryCategory) -> usize {
        self.categories[cat as usize].load(Ordering::Relaxed)
    }

    pub fn overhead(&self) -> usize {
        self.get(MemoryCategory::ReplBacklog)
            + self.get(MemoryCategory::ClientBuffers)
            + self.get(MemoryCategory::AofBuffer)
            + self.get(MemoryCategory::LuaVm)
            + self.get(MemoryCategory::FunctionsVm)
            + self.get(MemoryCategory::ScriptsCaches)
            + self.get(MemoryCategory::ClusterLinks)
            + self.get(MemoryCategory::Other)
    }

    pub fn startup_allocated(&self) -> usize {
        self.startup_snapshot.load(Ordering::Relaxed)
    }

    pub fn update_startup_snapshot(&self) {
        let current = crate::foundation::memory::util_alloc::total_allocated();
        self.startup_snapshot.store(current, Ordering::Relaxed);
    }
}

impl Default for MemoryCategoryTracker {
    fn default() -> Self {
        Self::new()
    }
}

use std::sync::OnceLock;

fn global_tracker() -> &'static MemoryCategoryTracker {
    static TRACKER: OnceLock<MemoryCategoryTracker> = OnceLock::new();
    TRACKER.get_or_init(MemoryCategoryTracker::new)
}

#[cfg(test)]
use std::cell::Cell;
#[cfg(test)]
use std::sync::{Mutex, MutexGuard};

#[cfg(test)]
static GLOBAL_TEST_LOCK: Mutex<()> = Mutex::new(());

#[cfg(test)]
thread_local! {
    static HOLDS_GLOBAL_TEST_LOCK: Cell<bool> = const { Cell::new(false) };
}

#[cfg(test)]
pub(crate) struct GlobalTestGuard {
    _guard: MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for GlobalTestGuard {
    fn drop(&mut self) {
        HOLDS_GLOBAL_TEST_LOCK.with(|holds| holds.set(false));
    }
}

#[cfg(test)]
pub(crate) fn lock_global_for_test() -> GlobalTestGuard {
    let guard = GLOBAL_TEST_LOCK
        .lock()
        .expect("global memory test lock must not be poisoned");
    HOLDS_GLOBAL_TEST_LOCK.with(|holds| holds.set(true));
    GlobalTestGuard { _guard: guard }
}

#[inline]
pub fn track_add(cat: MemoryCategory, size: usize) {
    #[cfg(test)]
    let _guard = (!HOLDS_GLOBAL_TEST_LOCK.with(Cell::get)).then(lock_global_for_test);
    global_tracker().add(cat, size);
}

#[inline]
pub fn track_sub(cat: MemoryCategory, size: usize) {
    #[cfg(test)]
    let _guard = (!HOLDS_GLOBAL_TEST_LOCK.with(Cell::get)).then(lock_global_for_test);
    global_tracker().sub(cat, size);
}

pub fn get_tracker() -> &'static MemoryCategoryTracker {
    global_tracker()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_sub() {
        let t = MemoryCategoryTracker::new();
        assert_eq!(t.get(MemoryCategory::Dataset), 0);
        t.add(MemoryCategory::Dataset, 100);
        assert_eq!(t.get(MemoryCategory::Dataset), 100);
        t.sub(MemoryCategory::Dataset, 50);
        assert_eq!(t.get(MemoryCategory::Dataset), 50);
    }

    #[test]
    fn test_overhead() {
        let t = MemoryCategoryTracker::new();
        t.add(MemoryCategory::ReplBacklog, 1000);
        t.add(MemoryCategory::ClientBuffers, 500);
        assert_eq!(t.overhead(), 1500);
    }

    #[test]
    fn test_default() {
        let t: MemoryCategoryTracker = Default::default();
        if cfg!(feature = "wasm") {
            assert_eq!(t.startup_allocated(), 0);
        } else {
            assert_ne!(t.startup_allocated(), 0);
        }
    }

    #[test]
    fn test_update_startup_snapshot() {
        let t = MemoryCategoryTracker::new();
        let before = t.startup_allocated();
        t.update_startup_snapshot();
        let after = t.startup_allocated();
        if cfg!(feature = "wasm") {
            assert_eq!((before, after), (0, 0));
        } else {
            assert_ne!(before, 0);
            assert_ne!(after, 0);
        }
    }

    #[test]
    fn test_num_categories() {
        assert_eq!(MemoryCategory::NUM_CATEGORIES, 9);
    }

    #[test]
    fn test_global_track_add_sub() {
        let _guard = lock_global_for_test();
        let initial = get_tracker().get(MemoryCategory::Dataset);
        track_add(MemoryCategory::Dataset, 42);
        assert_eq!(get_tracker().get(MemoryCategory::Dataset), initial + 42);
        track_sub(MemoryCategory::Dataset, 12);
        assert_eq!(
            get_tracker().get(MemoryCategory::Dataset),
            initial + 42 - 12
        );
    }

    #[test]
    fn test_all_categories_are_distinct() {
        let t = MemoryCategoryTracker::new();
        let cats = [
            MemoryCategory::Dataset,
            MemoryCategory::ReplBacklog,
            MemoryCategory::ClientBuffers,
            MemoryCategory::AofBuffer,
            MemoryCategory::LuaVm,
            MemoryCategory::FunctionsVm,
            MemoryCategory::ScriptsCaches,
            MemoryCategory::ClusterLinks,
            MemoryCategory::Other,
        ];
        for (i, cat) in cats.iter().enumerate() {
            t.add(*cat, (i + 1) * 10);
        }
        for (i, cat) in cats.iter().enumerate() {
            assert_eq!(t.get(*cat), (i + 1) * 10);
        }
    }
}
