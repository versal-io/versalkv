//! Shared slot tables with isolated writes and cached wire bitmaps.

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, OnceLock};

use super::msg::SLOT_BYTES;

/// A dense slot table whose snapshots share storage until a mutable access.
#[derive(Clone, Debug)]
pub struct SharedSlots<T>(Arc<Vec<T>>);

impl<T> From<Vec<T>> for SharedSlots<T> {
    fn from(values: Vec<T>) -> Self {
        Self(Arc::new(values))
    }
}

impl<T> SharedSlots<T> {
    pub fn shares_storage(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl<T: PartialEq> PartialEq for SharedSlots<T> {
    fn eq(&self, other: &Self) -> bool {
        self.shares_storage(other) || self.0 == other.0
    }
}

impl<T: Eq> Eq for SharedSlots<T> {}

impl<T> Deref for SharedSlots<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Clone> DerefMut for SharedSlots<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.0).as_mut_slice()
    }
}

impl<'a, T> IntoIterator for &'a SharedSlots<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: Clone> IntoIterator for &'a mut SharedSlots<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

#[derive(Clone, Debug)]
struct BitmapStorage {
    slots: Vec<bool>,
    wire: OnceLock<[u8; SLOT_BYTES]>,
}

/// Canonical node slots. Every mutable slice access invalidates the wire cache.
#[derive(Clone, Debug)]
pub struct SlotBitmap(Arc<BitmapStorage>);

impl From<Vec<bool>> for SlotBitmap {
    fn from(slots: Vec<bool>) -> Self {
        Self(Arc::new(BitmapStorage {
            slots,
            wire: OnceLock::new(),
        }))
    }
}

impl SlotBitmap {
    pub fn wire_bitmap(&self) -> &[u8; SLOT_BYTES] {
        self.0.wire.get_or_init(|| {
            let mut bitmap = [0; SLOT_BYTES];
            for (slot, &assigned) in self.0.slots.iter().enumerate() {
                if assigned {
                    bitmap[slot / 8] |= 1 << (7 - slot % 8);
                }
            }
            bitmap
        })
    }
}

impl PartialEq for SlotBitmap {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || self.0.slots == other.0.slots
    }
}

impl Eq for SlotBitmap {}

impl Deref for SlotBitmap {
    type Target = [bool];

    fn deref(&self) -> &Self::Target {
        &self.0.slots
    }
}

impl DerefMut for SlotBitmap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let storage = Arc::make_mut(&mut self.0);
        storage.wire.take();
        &mut storage.slots
    }
}

impl<'a> IntoIterator for &'a SlotBitmap {
    type Item = &'a bool;
    type IntoIter = std::slice::Iter<'a, bool>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutable_access_preserves_existing_snapshots() {
        let mut slots = SlotBitmap::from(vec![false; 16384]);
        slots[0] = true;
        assert_eq!(slots.wire_bitmap()[0], 0x80);
        let original = slots.clone();
        slots[7] = true;
        assert_eq!(slots.wire_bitmap()[0], 0x81);
        assert_eq!(original.wire_bitmap()[0], 0x80);
        slots.fill(true);
        assert_eq!(slots.wire_bitmap()[SLOT_BYTES - 1], 0xff);
        slots.iter_mut().for_each(|slot| *slot = false);
        assert_eq!(slots.wire_bitmap(), &[0; SLOT_BYTES]);
        slots = vec![true; 16384].into();
        assert!(!original[7]);
        assert_eq!(slots.wire_bitmap(), &[0xff; SLOT_BYTES]);
    }

    #[test]
    fn shared_tables_compare_contents_after_writes() {
        let original = SharedSlots::from(vec![None::<String>; 16384]);
        let mut changed = original.clone();
        changed[8] = Some("node".to_string());
        assert!(original[8].is_none());
        assert_ne!(original, changed);
        changed[8] = None;
        assert_eq!(original, changed);
        assert!(!original.shares_storage(&changed));
    }

    #[test]
    fn shared_slot_storage_can_cross_threads() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SlotBitmap>();
        assert_send_sync::<SharedSlots<Option<String>>>();
    }
}
