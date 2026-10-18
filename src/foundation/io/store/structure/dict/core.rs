#[path = "table.rs"]
mod table;

use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use table::next_cursor;

use table::MAX_FILL_PERCENT_HARD;
use table::MAX_FILL_PERCENT_SOFT;
use table::MIN_FILL_PERCENT_HARD;
use table::MIN_FILL_PERCENT_SOFT;

const ENTRIES_PER_BUCKET: usize = 7;
const STATS_ENTRIES_PER_BUCKET: usize = ENTRIES_PER_BUCKET;
const STATS_VECTLEN: usize = 50;

const BUCKET_FACTOR: usize = 5;
const BUCKET_DIVISOR: usize = 32;

fn next_bucket_exp(min_capacity: usize) -> i32 {
    if min_capacity == 0 {
        return -1;
    }
    // ceil(x / y) = floor((x - 1) / y) + 1
    let scaled = min_capacity.saturating_mul(BUCKET_FACTOR);
    let min_buckets = (scaled - 1) / BUCKET_DIVISOR + 1;
    if min_buckets == 1 {
        return 0;
    }
    (usize::BITS - (min_buckets - 1).leading_zeros()) as i32
}

fn chain_len_for_occupancy(occupancy: usize) -> usize {
    if occupancy <= STATS_ENTRIES_PER_BUCKET {
        0
    } else {
        let extra = occupancy - STATS_ENTRIES_PER_BUCKET;
        (extra + STATS_ENTRIES_PER_BUCKET - 2) / (STATS_ENTRIES_PER_BUCKET - 1)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ResizePolicy {
    Allow,
    Avoid,
    Forbid,
}

#[derive(Clone, Debug)]
pub(crate) struct ResizeBudget(Arc<AtomicUsize>);

impl ResizeBudget {
    pub(crate) fn new(bytes: Option<usize>) -> Self {
        Self(Arc::new(AtomicUsize::new(bytes.unwrap_or(usize::MAX))))
    }

    pub(crate) fn reset(&self, bytes: Option<usize>) {
        self.0.store(bytes.unwrap_or(usize::MAX), Ordering::Release);
    }

    fn reserve(&self, bytes: usize) -> bool {
        let mut available = self.0.load(Ordering::Acquire);
        loop {
            if available == usize::MAX {
                return true;
            }
            if available < bytes {
                return false;
            }
            match self.0.compare_exchange_weak(
                available,
                available - bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(current) => available = current,
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct Entry<K, V> {
    key: K,
    value: V,
}

#[derive(Clone, Debug)]
pub(super) enum BucketEntries<K, V> {
    Single(Entry<K, V>),
    Many(Vec<Entry<K, V>>),
}

type Bucket<K, V> = Option<Box<BucketEntries<K, V>>>;

#[derive(Clone, Debug)]
struct Table<K, V> {
    table: Vec<Bucket<K, V>>,
    used: usize,
    bucket_exp: i32,
}

#[derive(Clone, Debug)]
pub struct Dict<K, V> {
    tables: [Table<K, V>; 2],
    rehash_idx: isize,
    resize_policy: ResizePolicy,
    resize_budget: ResizeBudget,
    iterators: i32,
}

impl<K: Eq + Hash, V> Default for Dict<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash, V> Dict<K, V> {
    pub fn new() -> Self {
        Self::with_resize_budget(ResizeBudget::new(None))
    }

    pub(crate) fn with_resize_budget(resize_budget: ResizeBudget) -> Self {
        Self {
            tables: [Table::new(), Table::new()],
            rehash_idx: -1,
            resize_policy: ResizePolicy::Allow,
            resize_budget,
            iterators: 0,
        }
    }

    pub fn set_resize_policy(&mut self, policy: ResizePolicy) {
        self.resize_policy = policy;
    }

    pub fn set_resize_budget(&mut self, budget: Option<usize>) {
        self.resize_budget.reset(budget);
    }

    pub fn len(&self) -> usize {
        self.tables[0].used + self.tables[1].used
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_rehashing(&self) -> bool {
        self.rehash_idx != -1
    }

    pub fn rehashing_bucket_count(&self) -> usize {
        if self.is_rehashing() {
            self.tables[0].bucket_count()
        } else {
            0
        }
    }

    fn hash_key<Q: Hash + ?Sized>(key: &Q) -> u64 {
        super::super::configurable_hash::hash_value(key)
    }

    pub fn bucket_count(&self) -> usize {
        self.tables[0]
            .bucket_count()
            .max(self.tables[1].bucket_count())
    }

    pub fn stats_msg(&self, full: bool) -> String {
        let mut out = self.table_stats_msg(0, full);
        if self.is_rehashing() {
            out.push_str(&self.table_stats_msg(1, full));
        }
        out
    }

    fn table_stats_msg(&self, table_index: usize, full: bool) -> String {
        let t = &self.tables[table_index];
        let toplevel = t.bucket_count();
        let table_size = toplevel * ENTRIES_PER_BUCKET;
        let table_name = if table_index == 0 {
            "main hash table"
        } else {
            "rehashing target"
        };
        let mut out = format!(
            "Hash table {} stats ({}):\n table size: {}\n number of entries: {}\n",
            table_index, table_name, table_size, t.used,
        );
        if table_index == 0 {
            out.push_str(&format!(" rehashing index: {}\n", self.rehash_idx));
        }
        if !full || toplevel == 0 {
            return out;
        }
        let mut clvector = vec![0usize; STATS_VECTLEN];
        let mut child_buckets = 0usize;
        let mut max_chain = 0usize;
        for slot in 0..toplevel {
            let chainlen = chain_len_for_occupancy(t.bucket_slice(slot).len());
            child_buckets += chainlen;
            max_chain = max_chain.max(chainlen);
            clvector[chainlen.min(STATS_VECTLEN - 1)] += 1;
        }
        let avg = child_buckets as f64 / toplevel as f64;
        out.push_str(&format!(
            " top-level buckets: {}\n child buckets: {}\n max chain length: {}\n avg chain length: {:.6}\n chain length distribution:\n",
            toplevel, child_buckets, max_chain, avg,
        ));
        for (i, &count) in clvector.iter().enumerate() {
            if count == 0 {
                continue;
            }
            let pct = (count as f64 / toplevel as f64) * 100.0;
            out.push_str(&format!("   {}: {} ({:.2}%)\n", i, count, pct));
        }
        out
    }

    #[cfg(test)]
    pub(crate) fn bucket_entry_count_for_key<Q: Hash + ?Sized>(&self, key: &Q) -> usize
    where
        K: Borrow<Q>,
    {
        let hash = Self::hash_key(key);
        for table in &self.tables {
            let bucket_count = table.bucket_count();
            if bucket_count == 0 {
                continue;
            }
            let bucket = (hash & (bucket_count as u64 - 1)) as usize;
            let entries = table.bucket_entry_count(bucket);
            if entries > 0 {
                return entries;
            }
        }
        0
    }

    #[cfg(test)]
    pub(crate) fn max_bucket_entry_count(&self) -> usize {
        self.tables
            .iter()
            .flat_map(|table| (0..table.bucket_count()).map(|idx| table.bucket_entry_count(idx)))
            .max()
            .unwrap_or(0)
    }

    const REHASH_STEP_EMPTY_SKIP: usize = 10;
    const REHASH_STEP_ENTRY_CAP: usize = 64;
    const REHASH_CRON_BULK: usize = 1000;

    pub fn rehash(&mut self, n: usize) -> usize {
        let mut moved = 0usize;
        for _ in 0..n {
            if !self.is_rehashing() {
                break;
            }
            let mut empty_skip = 0usize;
            loop {
                if !self.is_rehashing() {
                    return moved;
                }
                let idx = self.rehash_idx as usize;
                if idx >= self.tables[0].bucket_count() {
                    self.finish_rehash();
                    return moved;
                }
                if self.tables[0].bucket_slice(idx).is_empty() {
                    self.tables[0].recycle_bucket(idx);
                    self.rehash_idx += 1;
                    empty_skip += 1;
                    if empty_skip > Self::REHASH_STEP_EMPTY_SKIP {
                        return moved;
                    }
                    continue;
                }
                self.rehash_bucket(idx);
                self.rehash_idx += 1;
                moved += 1;
                if self.tables[0].used == 0 {
                    self.finish_rehash();
                    return moved;
                }
                break;
            }
        }
        moved
    }

    pub fn rehash_entries(&mut self, max_entries: usize) -> usize {
        let mut entries_moved = 0usize;
        let mut empty_skip = 0usize;
        while entries_moved < max_entries && self.is_rehashing() {
            let idx = self.rehash_idx as usize;
            if idx >= self.tables[0].bucket_count() {
                self.finish_rehash();
                return entries_moved;
            }
            if self.tables[0].bucket_slice(idx).is_empty() {
                self.tables[0].recycle_bucket(idx);
                self.rehash_idx += 1;
                empty_skip += 1;
                if empty_skip > Self::REHASH_STEP_EMPTY_SKIP {
                    return entries_moved;
                }
                continue;
            }
            empty_skip = 0;
            let chain_len = self.tables[0].bucket_slice(idx).len();
            if entries_moved + chain_len > max_entries && entries_moved > 0 {
                return entries_moved;
            }
            self.rehash_bucket(idx);
            self.rehash_idx += 1;
            entries_moved += chain_len;
            if self.tables[0].used == 0 {
                self.finish_rehash();
                return entries_moved;
            }
        }
        entries_moved
    }

    fn finish_rehash(&mut self) {
        debug_assert!(self.tables[0].used == 0);
        self.tables[0] = Table::new();
        // swap: tables[1] becomes the new tables[0]
        let mut t1 = Table::new();
        std::mem::swap(&mut self.tables[1], &mut t1);
        self.tables[0] = t1;
        self.rehash_idx = -1;
    }

    fn rehash_bucket(&mut self, idx: usize) {
        let chain = self.tables[0].take_bucket(idx);
        let moved = chain.len();
        for entry in chain {
            let new_idx =
                (Self::hash_key(&entry.key) & (self.tables[1].bucket_count() as u64 - 1)) as usize;
            self.tables[1].insert_entry(new_idx, entry);
            self.tables[1].used += 1;
        }
        self.tables[0].used -= moved;
    }

    fn rehash_step_if_needed(&mut self) {
        if !self.is_rehashing() || self.iterators > 0 {
            return;
        }
        let _ = self.rehash_entries(Self::REHASH_STEP_ENTRY_CAP);
    }

    fn is_expandable(&self) -> bool {
        self.resize_policy != ResizePolicy::Forbid
    }

    fn reserve_expansion(&mut self, min_capacity: usize) -> bool {
        let target_bc = 1usize << next_bucket_exp(min_capacity);
        let bytes = target_bc.saturating_mul(std::mem::size_of::<Bucket<K, V>>());
        self.resize_budget.reserve(bytes)
    }

    fn expand(&mut self, min_capacity: usize) -> bool {
        if self.is_rehashing() || !self.is_expandable() {
            return false;
        }
        let target_exp = next_bucket_exp(min_capacity);
        let target_bc = if target_exp < 0 {
            0
        } else {
            1usize << target_exp
        };
        if self.tables[0].bucket_count() >= target_bc {
            return false;
        }
        self.tables[1] = Table::allocate(target_exp);
        self.rehash_idx = 0;
        if self.tables[0].used == 0 && self.tables[0].bucket_count() == 0 {
            self.finish_rehash();
        }
        true
    }

    fn expand_if_needed(&mut self) {
        if self.is_rehashing() {
            return;
        }
        if !self.is_expandable() {
            return;
        }
        if self.tables[0].bucket_count() == 0 {
            self.expand(1);
            return;
        }
        let bc = self.tables[0].bucket_count();
        let current_capacity = bc * ENTRIES_PER_BUCKET;
        let projected = self.tables[0].used + 1;
        let hard_limit = projected * 100 > current_capacity * MAX_FILL_PERCENT_HARD;
        let soft_limit = self.resize_policy == ResizePolicy::Allow
            && projected * 100 > current_capacity * MAX_FILL_PERCENT_SOFT;
        if hard_limit || (soft_limit && self.reserve_expansion(projected)) {
            self.expand(projected);
        }
    }

    fn shrink_if_needed(&mut self) {
        if self.is_rehashing() {
            return;
        }
        let min_fill = match self.resize_policy {
            ResizePolicy::Allow => MIN_FILL_PERCENT_SOFT,
            ResizePolicy::Avoid => MIN_FILL_PERCENT_HARD,
            ResizePolicy::Forbid => return,
        };
        let bc = self.tables[0].bucket_count();
        let current_capacity = bc * ENTRIES_PER_BUCKET;
        if self.tables[0].used * 100 > current_capacity * min_fill {
            return;
        }
        let min_capacity = self.tables[0].used.max(1);
        self.shrink(min_capacity);
    }

    fn shrink(&mut self, min_capacity: usize) -> bool {
        if self.is_rehashing() || !self.is_expandable() {
            return false;
        }
        let target_exp = next_bucket_exp(min_capacity);
        let target_bc = 1usize << target_exp.max(0);
        if target_bc >= self.tables[0].bucket_count() {
            return false;
        }
        self.tables[1] = Table::allocate(target_exp);
        self.rehash_idx = 0;
        if self.tables[0].used == 0 && self.tables[0].bucket_count() == 0 {
            self.finish_rehash();
        }
        true
    }

    pub fn try_resize(&mut self) {
        self.try_resize_with_active_rehash(true);
    }

    /// Cron-side resize with an independently configurable incremental-rehash
    /// phase. C keeps resize decisions active when `activerehashing` is off,
    /// but pauses migration of an already-created rehash target until it is
    /// re-enabled.
    pub fn try_resize_with_active_rehash(&mut self, active_rehash: bool) {
        if self.is_rehashing() {
            if !active_rehash {
                return;
            }
            let _ = self.rehash(Self::REHASH_CRON_BULK);
            return;
        }
        if self.resize_policy == ResizePolicy::Forbid {
            return;
        }
        let bc = self.tables[0].bucket_count();
        if bc == 0 {
            return;
        }
        let used = self.tables[0].used;
        let current_capacity = bc * ENTRIES_PER_BUCKET;

        let min_fill = if self.resize_policy == ResizePolicy::Allow {
            MIN_FILL_PERCENT_SOFT
        } else {
            MIN_FILL_PERCENT_HARD
        };
        if used * 100 <= current_capacity * min_fill {
            self.shrink(used.max(1));
            return;
        }

        let max_fill = if self.resize_policy == ResizePolicy::Allow {
            MAX_FILL_PERCENT_SOFT
        } else {
            MAX_FILL_PERCENT_HARD
        };
        let projected = used + 1;
        if projected * 100 <= current_capacity * max_fill {
            return;
        }
        if max_fill == MAX_FILL_PERCENT_HARD || self.reserve_expansion(projected) {
            self.expand(projected);
        }
    }

    pub fn get<Q: Hash + Eq + ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
    {
        if self.is_empty() {
            return None;
        }
        let hash = Self::hash_key(key);
        for t in 0..2 {
            if self.tables[t].bucket_count() == 0 {
                continue;
            }
            let bc = self.tables[t].bucket_count() as u64;
            let idx = (hash & (bc - 1)) as usize;
            for entry in self.tables[t].bucket_slice(idx) {
                if entry.key.borrow() == key {
                    return Some(&entry.value);
                }
            }
            if !self.is_rehashing() {
                break;
            }
        }
        None
    }

    pub fn get_key_value<Q: Hash + Eq + ?Sized>(&self, key: &Q) -> Option<(&K, &V)>
    where
        K: Borrow<Q>,
    {
        if self.is_empty() {
            return None;
        }
        let hash = Self::hash_key(key);
        for t in 0..2 {
            if self.tables[t].bucket_count() == 0 {
                continue;
            }
            let bc = self.tables[t].bucket_count() as u64;
            let idx = (hash & (bc - 1)) as usize;
            for entry in self.tables[t].bucket_slice(idx) {
                if entry.key.borrow() == key {
                    return Some((&entry.key, &entry.value));
                }
            }
            if !self.is_rehashing() {
                break;
            }
        }
        None
    }

    pub fn get_mut<Q: Hash + Eq + ?Sized>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
    {
        if self.is_empty() {
            return None;
        }
        self.rehash_step_if_needed();
        let hash = Self::hash_key(key);
        let rehashing = self.is_rehashing();
        let (t0, t1_rest) = self.tables.split_at_mut(1);
        let t0 = &mut t0[0];
        let t1 = &mut t1_rest[0];
        {
            let bc0 = t0.bucket_count() as u64;
            if bc0 > 0 {
                let idx = (hash & (bc0 - 1)) as usize;
                if let Some(bucket) = t0.bucket_entries_mut(idx) {
                    for entry in bucket {
                        if entry.key.borrow() == key {
                            return Some(&mut entry.value);
                        }
                    }
                }
            }
        }
        if rehashing {
            let bc1 = t1.bucket_count() as u64;
            if bc1 > 0 {
                let idx = (hash & (bc1 - 1)) as usize;
                if let Some(bucket) = t1.bucket_entries_mut(idx) {
                    for entry in bucket {
                        if entry.key.borrow() == key {
                            return Some(&mut entry.value);
                        }
                    }
                }
            }
        }
        None
    }

    pub fn contains_key<Q: Hash + Eq + ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
    {
        self.get(key).is_some()
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.rehash_step_if_needed();
        self.expand_if_needed();
        let hash = Self::hash_key(&key);
        let t = if self.is_rehashing() { 1 } else { 0 };
        if self.tables[t].bucket_count() == 0 {
            if !self.is_rehashing() && t == 0 {
                let _ = self.expand(1);
            } else if self.tables[1].bucket_count() == 0 {
                self.tables[1] = Table::allocate(3);
            }
        }
        let bc = if t == 0 {
            self.tables[0].bucket_count()
        } else {
            self.tables[1].bucket_count()
        } as u64;
        let idx = (hash & (bc - 1)) as usize;

        let wt = if t == 0 {
            &mut self.tables[0]
        } else {
            &mut self.tables[1]
        };
        if let Some(bucket) = wt.bucket_entries_mut(idx) {
            for entry in bucket.iter_mut() {
                if entry.key == key {
                    return Some(std::mem::replace(&mut entry.value, value));
                }
            }
        }
        if t == 1 {
            let bc0 = self.tables[0].bucket_count() as u64;
            if bc0 > 0 {
                let idx0 = (hash & (bc0 - 1)) as usize;
                if let Some(bucket) = self.tables[0].bucket_entries_mut(idx0) {
                    for entry in bucket.iter_mut() {
                        if entry.key == key {
                            return Some(std::mem::replace(&mut entry.value, value));
                        }
                    }
                }
            }
        }
        let wt = if t == 0 {
            &mut self.tables[0]
        } else {
            &mut self.tables[1]
        };
        wt.insert_entry(idx, Entry { key, value });
        wt.used += 1;
        None
    }

    fn remove_entry_inner<Q: Hash + Eq + ?Sized>(&mut self, key: &Q) -> Option<Entry<K, V>>
    where
        K: Borrow<Q>,
    {
        if self.is_empty() {
            return None;
        }
        self.rehash_step_if_needed();
        let hash = Self::hash_key(key);
        for t in [0usize, 1] {
            let bc = if t == 0 {
                self.tables[0].bucket_count()
            } else {
                self.tables[1].bucket_count()
            };
            if bc == 0 {
                continue;
            }
            let idx = (hash & (bc as u64 - 1)) as usize;
            let tbl = if t == 0 {
                &mut self.tables[0]
            } else {
                &mut self.tables[1]
            };
            if let Some(bucket) = tbl.bucket_entries_mut(idx) {
                if let Some(pos) = bucket.iter().position(|e| e.key.borrow() == key) {
                    let entry = tbl
                        .remove_entry(idx, pos)
                        .expect("bucket position was found before removal");
                    tbl.used -= 1;
                    self.shrink_if_needed();
                    return Some(entry);
                }
            }
            if !self.is_rehashing() {
                break;
            }
        }
        None
    }

    pub fn remove<Q: Hash + Eq + ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
    {
        self.remove_entry_inner(key).map(|e| e.value)
    }

    pub fn remove_entry<Q: Hash + Eq + ?Sized>(&mut self, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
    {
        self.remove_entry_inner(key).map(|e| (e.key, e.value))
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.iter().map(|(k, _)| k)
    }

    /// Pick a pseudo-random entry from the main table (or the rehash table if
    /// the main table's chosen bucket is empty). Mirrors C's `dictGetRandomKey`
    /// used by eviction / RANDOMKEY: O(1) per call instead of materializing
    /// the full key set. Returns None only when the dict is empty.
    pub fn random_entry(&self) -> Option<(&K, &V)> {
        if self.is_empty() {
            return None;
        }
        let bc0 = self.tables[0].bucket_count();
        if bc0 > 0 {
            let idx = fast_random_idx(bc0);
            let slice = self.tables[0].bucket_slice(idx);
            if !slice.is_empty() {
                let e = &slice[fast_random_idx(slice.len())];
                return Some((&e.key, &e.value));
            }
        }
        let bc1 = self.tables[1].bucket_count();
        if bc1 > 0 {
            let idx = fast_random_idx(bc1);
            let slice = self.tables[1].bucket_slice(idx);
            if !slice.is_empty() {
                let e = &slice[fast_random_idx(slice.len())];
                return Some((&e.key, &e.value));
            }
        }
        // Fallback: the random bucket was empty (sparse table); scan for the
        // first non-empty bucket. This keeps correctness without an unbounded
        // loop.
        for t in 0..2 {
            let bc = if t == 0 { bc0 } else { bc1 };
            for idx in 0..bc {
                let slice = if t == 0 {
                    self.tables[0].bucket_slice(idx)
                } else {
                    self.tables[1].bucket_slice(idx)
                };
                if !slice.is_empty() {
                    let e = &slice[0];
                    return Some((&e.key, &e.value));
                }
            }
        }
        None
    }

    pub fn clear(&mut self) {
        self.tables = [Table::new(), Table::new()];
        self.rehash_idx = -1;
    }

    pub fn drain(&mut self) -> impl Iterator<Item = (K, V)> + '_ {
        self.iterators = 0;
        let [t0, t1] = std::mem::take(&mut self.tables);
        self.rehash_idx = -1;
        t0.table
            .into_iter()
            .chain(t1.table)
            .filter_map(|bucket| bucket.map(|bucket| *bucket))
            .flat_map(|bucket| match bucket {
                BucketEntries::Single(entry) => vec![(entry.key, entry.value)],
                BucketEntries::Many(entries) => entries
                    .into_iter()
                    .map(|entry| (entry.key, entry.value))
                    .collect(),
            })
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.tables
            .iter()
            .flat_map(|table| (0..table.bucket_count()).flat_map(|idx| table.bucket_slice(idx)))
            .map(|entry| (&entry.key, &entry.value))
    }

    pub fn scan<F>(&self, cursor: u64, mut f: F) -> u64
    where
        F: FnMut(&K, &V),
    {
        if self.is_empty() {
            return 0;
        }
        if !self.is_rehashing() {
            let bc = self.tables[0].bucket_count() as u64;
            if bc == 0 {
                return 0;
            }
            let mask = bc - 1;
            let idx = (cursor & mask) as usize;
            for entry in self.tables[0].bucket_slice(idx) {
                f(&entry.key, &entry.value);
            }
            return next_cursor(cursor, mask);
        }

        let (small, large) = if self.tables[0].bucket_count() <= self.tables[1].bucket_count() {
            (0usize, 1usize)
        } else {
            (1usize, 0usize)
        };
        let mask_small = self.tables[small].bucket_count() as u64 - 1;
        let mask_large = self.tables[large].bucket_count() as u64 - 1;

        let idx_small = cursor & mask_small;
        if self.bucket_emittable(small, idx_small) {
            for entry in self.tables[small].bucket_slice(idx_small as usize) {
                f(&entry.key, &entry.value);
            }
        }

        let mut cur = cursor;
        loop {
            let idx_large = cur & mask_large;
            if self.bucket_emittable(large, idx_large) {
                for entry in self.tables[large].bucket_slice(idx_large as usize) {
                    f(&entry.key, &entry.value);
                }
            }
            cur = next_cursor(cur, mask_large);
            if cur & (mask_small ^ mask_large) == 0 {
                return cur;
            }
        }
    }

    fn bucket_emittable(&self, table: usize, idx: u64) -> bool {
        table == 1 || self.rehash_idx == -1 || idx >= self.rehash_idx as u64
    }
}

fn fast_random_idx(bound: usize) -> usize {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let entropy = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let r = entropy
        .wrapping_add(COUNTER.fetch_add(1, Ordering::Relaxed))
        .wrapping_mul(0x9e3779b97f4a7c15)
        .rotate_left(17);
    (r % bound as u64) as usize
}

// ===== Tests =====
