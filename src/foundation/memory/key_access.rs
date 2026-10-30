//! Scoped observation of command-owned key access, independent of storage.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

pub trait KeyAccessObserver {
    fn record(&self, db_id: u32, key: &[u8]);
}

struct ActiveObserver {
    observer: Rc<dyn KeyAccessObserver>,
    record_deletions: bool,
}

thread_local! {
    static ACTIVE: RefCell<Option<ActiveObserver>> = const { RefCell::new(None) };
    static SUPPRESSED: Cell<u32> = const { Cell::new(0) };
}

/// Restores the enclosing command's observer when nested execution finishes.
pub struct KeyAccessScope {
    previous: Option<ActiveObserver>,
}

impl KeyAccessScope {
    pub fn enter(observer: Option<Rc<dyn KeyAccessObserver>>, record_deletions: bool) -> Self {
        let active = observer.map(|observer| ActiveObserver {
            observer,
            record_deletions,
        });
        Self {
            previous: ACTIVE.with(|slot| slot.replace(active)),
        }
    }
}

impl Drop for KeyAccessScope {
    fn drop(&mut self) {
        ACTIVE.with(|slot| slot.replace(self.previous.take()));
    }
}

/// Excludes internal replay and storage bookkeeping even under nested calls.
pub struct KeyAccessSuppression;

impl KeyAccessSuppression {
    pub fn enter() -> Self {
        SUPPRESSED.with(|depth| depth.set(depth.get() + 1));
        Self
    }
}

impl Drop for KeyAccessSuppression {
    fn drop(&mut self) {
        SUPPRESSED.with(|depth| depth.set(depth.get() - 1));
    }
}

#[inline]
pub fn record_lookup(db_id: u32, key: &[u8]) {
    record(db_id, key, false);
}

#[inline]
pub fn record_delete(db_id: u32, key: &[u8]) {
    record(db_id, key, true);
}

fn record(db_id: u32, key: &[u8], is_deletion: bool) {
    if SUPPRESSED.with(Cell::get) != 0 {
        return;
    }
    let observer = ACTIVE.with(|slot| {
        slot.borrow()
            .as_ref()
            .filter(|active| !is_deletion || active.record_deletions)
            .map(|active| Rc::clone(&active.observer))
    });
    if let Some(observer) = observer {
        observer.record(db_id, key);
    }
}
