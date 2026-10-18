use super::{Bucket, BucketEntries, Entry, Table};
use std::alloc::{alloc_zeroed, handle_alloc_error, Layout};

pub(crate) const MAX_FILL_PERCENT_SOFT: usize = 100;
pub(crate) const MAX_FILL_PERCENT_HARD: usize = 500;
pub(crate) const MIN_FILL_PERCENT_SOFT: usize = 13;
pub(crate) const MIN_FILL_PERCENT_HARD: usize = 3;

impl<K, V> Default for Table<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Table<K, V> {
    pub(super) fn new() -> Self {
        Self {
            table: Vec::new(),
            used: 0,
            bucket_exp: -1,
        }
    }

    pub(super) fn allocate(exp: i32) -> Self {
        let size = if exp < 0 { 0 } else { 1usize << exp };
        if size == 0 {
            return Self {
                table: Vec::new(),
                used: 0,
                bucket_exp: exp,
            };
        }
        // COW zero-page allocation: alloc_zeroed returns anonymous COW zero-page maps.
        // For ≥8 buckets (≥64 bytes) glibc/jemalloc naturally aligns to cache lines,
        // reducing false sharing during rehash scans.
        // SAFETY: Option<Box<T>>'s None variant is all-zero bytes (null pointer niche),
        // so a COW zero page directly corresponds to a valid None state (aligns with C zcalloc).
        let layout = match Layout::array::<Bucket<K, V>>(size) {
            Ok(l) => l,
            Err(_) => handle_alloc_error(Layout::new::<Bucket<K, V>>()),
        };
        // SAFETY: layout returned by Layout::array successfully; size did not overflow isize::MAX.
        let raw = unsafe { alloc_zeroed(layout) };
        if raw.is_null() {
            handle_alloc_error(layout);
        }
        // SAFETY: `raw` comes from alloc_zeroed with size/alignment matching the Bucket<K,V> array;
        // zero-init memory is valid for Option<Box<_>> None variant (null-pointer niche = all zeros),
        // so each element is a valid None and the from_raw_parts Vec is safe to drop/access.
        let table: Vec<Bucket<K, V>> =
            unsafe { Vec::from_raw_parts(raw.cast::<Bucket<K, V>>(), size, size) };
        Self {
            table,
            used: 0,
            bucket_exp: exp,
        }
    }

    pub(super) fn bucket_count(&self) -> usize {
        if self.bucket_exp < 0 {
            0
        } else {
            1usize << self.bucket_exp
        }
    }

    pub(super) fn bucket_slice(&self, idx: usize) -> &[Entry<K, V>] {
        match &self.table[idx] {
            Some(b) => match b.as_ref() {
                BucketEntries::Single(entry) => std::slice::from_ref(entry),
                BucketEntries::Many(entries) => entries,
            },
            None => &[],
        }
    }

    pub(super) fn bucket_entries_mut(&mut self, idx: usize) -> Option<&mut [Entry<K, V>]> {
        match self.table[idx].as_deref_mut() {
            Some(BucketEntries::Single(entry)) => Some(std::slice::from_mut(entry)),
            Some(BucketEntries::Many(entries)) => Some(entries),
            None => None,
        }
    }

    pub(super) fn insert_entry(&mut self, idx: usize, entry: Entry<K, V>) {
        let bucket = self.table[idx].take();
        self.table[idx] = Some(Box::new(match bucket.map(|bucket| *bucket) {
            None => BucketEntries::Single(entry),
            Some(BucketEntries::Single(old)) => BucketEntries::Many(vec![old, entry]),
            Some(BucketEntries::Many(mut entries)) => {
                entries.push(entry);
                BucketEntries::Many(entries)
            }
        }));
    }

    pub(super) fn remove_entry(&mut self, idx: usize, pos: usize) -> Option<Entry<K, V>> {
        let bucket = self.table[idx].take()?;
        match *bucket {
            BucketEntries::Single(entry) if pos == 0 => Some(entry),
            BucketEntries::Single(entry) => {
                self.table[idx] = Some(Box::new(BucketEntries::Single(entry)));
                None
            }
            BucketEntries::Many(mut entries) => {
                if pos >= entries.len() {
                    self.table[idx] = Some(Box::new(BucketEntries::Many(entries)));
                    return None;
                }
                let entry = entries.remove(pos);
                self.table[idx] = match entries.len() {
                    0 => None,
                    1 => Some(Box::new(BucketEntries::Single(entries.pop().unwrap()))),
                    _ => Some(Box::new(BucketEntries::Many(entries))),
                };
                Some(entry)
            }
        }
    }

    #[cfg(test)]
    pub(super) fn bucket_entry_count(&self, idx: usize) -> usize {
        self.bucket_slice(idx).len()
    }

    pub(super) fn take_bucket(&mut self, idx: usize) -> Vec<Entry<K, V>> {
        match self.table[idx].take().map(|bucket| *bucket) {
            Some(BucketEntries::Single(entry)) => vec![entry],
            Some(BucketEntries::Many(entries)) => entries,
            None => Vec::new(),
        }
    }

    pub(super) fn recycle_bucket(&mut self, idx: usize) {
        self.table[idx] = None;
    }
}

pub(super) fn rev(mut v: u64) -> u64 {
    v = ((v >> 1) & 0x5555555555555555) | ((v & 0x5555555555555555) << 1);
    v = ((v >> 2) & 0x3333333333333333) | ((v & 0x3333333333333333) << 2);
    v = ((v >> 4) & 0x0F0F0F0F0F0F0F0F) | ((v & 0x0F0F0F0F0F0F0F0F) << 4);
    v.swap_bytes()
}

pub fn next_cursor(v: u64, mask: u64) -> u64 {
    let mut v = v | !mask;
    v = rev(v);
    v = v.wrapping_add(1);
    v = rev(v);
    v
}
