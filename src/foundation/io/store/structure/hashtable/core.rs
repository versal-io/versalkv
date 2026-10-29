use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;

pub const ENTRIES_PER_BUCKET: usize = 7;

const BUCKET_FACTOR: usize = 5;
const BUCKET_DIVISOR: usize = 32;
const MAX_FILL_PERCENT_SOFT: usize = 100;
const MAX_FILL_PERCENT_HARD: usize = 500;
const MIN_FILL_PERCENT_SOFT: usize = 13;

const STATS_VECTLEN: usize = 50;

static CAN_ABORT_SHRINK: AtomicBool = AtomicBool::new(true);

pub fn set_can_abort_shrink(enabled: bool) {
    CAN_ABORT_SHRINK.store(enabled, Ordering::Relaxed);
}

pub fn can_abort_shrink() -> bool {
    CAN_ABORT_SHRINK.load(Ordering::Relaxed)
}

#[derive(Clone, Debug, Default)]
pub struct TableStats {
    pub table_index: usize,
    pub rehash_index: isize,
    pub toplevel_buckets: usize,
    pub child_buckets: usize,
    pub size: usize,
    pub used: usize,
    pub max_chain_len: usize,
    pub clvector: Vec<usize>,
}

impl TableStats {
    fn format(&self) -> String {
        let table_name = if self.table_index == 0 {
            "main hash table"
        } else {
            "rehashing target"
        };
        let mut out = format!(
            "Hash table {} stats ({}):\n table size: {}\n number of entries: {}\n",
            self.table_index, table_name, self.size, self.used,
        );
        if self.table_index == 0 {
            out.push_str(&format!(" rehashing index: {}\n", self.rehash_index));
        }
        if self.clvector.is_empty() {
            return out;
        }
        let avg = self.child_buckets as f64 / self.toplevel_buckets as f64;
        out.push_str(&format!(
            " top-level buckets: {}\n child buckets: {}\n max chain length: {}\n avg chain length: {:.6}\n chain length distribution:\n",
            self.toplevel_buckets, self.child_buckets, self.max_chain_len, avg,
        ));
        for (i, &count) in self.clvector.iter().enumerate() {
            if count == 0 {
                continue;
            }
            let pct = (count as f64 / self.toplevel_buckets as f64) * 100.0;
            out.push_str(&format!("   {}: {} ({:.2}%)\n", i, count, pct));
        }
        out
    }
}

#[derive(Clone, Debug)]
struct Bucket<V> {
    presence: u8,
    chained: bool,
    hashes: [u8; ENTRIES_PER_BUCKET],
    entries: [Option<(Bytes, V)>; ENTRIES_PER_BUCKET],
    child: Option<Box<Bucket<V>>>,
}

impl<V: Clone> Bucket<V> {
    fn new() -> Self {
        Self {
            presence: 0,
            chained: false,
            hashes: [0; ENTRIES_PER_BUCKET],
            entries: Default::default(),
            child: None,
        }
    }

    fn num_positions(&self) -> usize {
        ENTRIES_PER_BUCKET - if self.chained { 1 } else { 0 }
    }

    fn is_full(&self) -> bool {
        self.presence == ((1u16 << self.num_positions()) - 1) as u8
    }

    fn is_filled(&self, pos: usize) -> bool {
        self.presence & (1 << pos) != 0
    }

    fn set(&mut self, pos: usize, entry: (Bytes, V), h2: u8) {
        self.entries[pos] = Some(entry);
        self.hashes[pos] = h2;
        self.presence |= 1 << pos;
    }
}

fn high_bits(hash: u64) -> u8 {
    (hash >> 56) as u8
}

fn rev_bits(mut v: u64) -> u64 {
    v = ((v >> 1) & 0x5555555555555555) | ((v & 0x5555555555555555) << 1);
    v = ((v >> 2) & 0x3333333333333333) | ((v & 0x3333333333333333) << 2);
    v = ((v >> 4) & 0x0F0F0F0F0F0F0F0F) | ((v & 0x0F0F0F0F0F0F0F0F) << 4);
    v.swap_bytes()
}

fn next_cursor(v: u64, mask: u64) -> u64 {
    let mut v = v | !mask;
    v = rev_bits(v);
    v = v.wrapping_add(1);
    v = rev_bits(v);
    v
}

fn num_buckets(exp: i8) -> usize {
    if exp < 0 {
        0
    } else {
        1usize << exp
    }
}

fn exp_to_mask(exp: i8) -> u64 {
    if exp < 0 {
        0
    } else {
        (num_buckets(exp) as u64) - 1
    }
}

fn next_bucket_exp(min_capacity: usize) -> i8 {
    if min_capacity == 0 {
        return -1;
    }
    let min_buckets = (min_capacity * BUCKET_FACTOR - 1) / BUCKET_DIVISOR + 1;
    if min_buckets == 1 {
        return 0;
    }
    (64 - (min_buckets - 1).leading_zeros()) as i8
}

fn hash_key(key: &[u8]) -> u64 {
    super::super::configurable_hash::hash_bytes(key)
}

#[derive(Clone, Debug)]
struct Table<V> {
    buckets: Vec<Bucket<V>>,
    exp: i8,
    used: usize,
    child_buckets: usize,
}

impl<V: Clone> Table<V> {
    fn empty() -> Self {
        Self {
            buckets: Vec::new(),
            exp: -1,
            used: 0,
            child_buckets: 0,
        }
    }

    fn allocate(exp: i8) -> Self {
        Self {
            buckets: vec![Bucket::new(); num_buckets(exp)],
            exp,
            used: 0,
            child_buckets: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HashTable<V> {
    tables: [Table<V>; 2],
    rehash_idx: isize,
    pause_auto_shrink: u32,
    peak_len: usize,
}

impl<V: Clone> Default for HashTable<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: Clone> HashTable<V> {
    pub fn new() -> Self {
        Self {
            tables: [Table::empty(), Table::empty()],
            rehash_idx: -1,
            pause_auto_shrink: 0,
            peak_len: 0,
        }
    }

    pub fn peak_len(&self) -> usize {
        self.peak_len
    }

    /// Retained bucket allocations, excluding inline storage and key payloads.
    pub fn allocated_bytes(&self) -> usize {
        self.tables
            .iter()
            .map(|table| {
                (table.buckets.capacity() + table.child_buckets) * std::mem::size_of::<Bucket<V>>()
            })
            .sum()
    }

    pub fn pause_auto_shrink(&mut self) {
        self.pause_auto_shrink += 1;
    }

    pub fn resume_auto_shrink(&mut self) {
        self.pause_auto_shrink -= 1;
        if self.pause_auto_shrink == 0 {
            self.shrink_if_needed();
        }
    }

    pub fn is_rehashing(&self) -> bool {
        self.rehash_idx != -1
    }

    pub fn len(&self) -> usize {
        self.tables[0].used + self.tables[1].used
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, key: &[u8]) -> Option<&V> {
        self.get_key_value(key).map(|(_, value)| value)
    }

    pub fn get_key_value(&self, key: &[u8]) -> Option<(&Bytes, &V)> {
        if self.is_empty() {
            return None;
        }
        let hash = hash_key(key);
        let (t, top, depth, pos) = self.find(hash, key)?;
        let (stored_key, value) = Self::bucket_at(&self.tables[t].buckets, top, depth).entries[pos]
            .as_ref()
            .expect("find returned an occupied bucket position");
        Some((stored_key, value))
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        if self.is_empty() {
            return None;
        }
        let hash = hash_key(key);
        let loc = self.find(hash, key)?;
        let (t, top, depth, pos) = loc;
        Some(
            &mut Self::bucket_at_mut(&mut self.tables[t].buckets, top, depth).entries[pos]
                .as_mut()
                .unwrap()
                .1,
        )
    }

    pub fn insert(&mut self, key: Bytes, value: V) -> Option<V> {
        let hash = hash_key(&key);
        self.rehash_step_on_read_if_needed();
        if let Some((t, top, depth, pos)) = self.find(hash, &key) {
            let old = Self::bucket_at_mut(&mut self.tables[t].buckets, top, depth).entries[pos]
                .as_mut()
                .unwrap();
            return Some(std::mem::replace(&mut old.1, value));
        }
        self.rehash_step_on_read_if_needed();
        self.expand_if_needed();
        let h2 = high_bits(hash);
        self.insert_new(hash, (key, value), h2);
        if self.len() > self.peak_len {
            self.peak_len = self.len();
        }
        None
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<V> {
        if self.is_empty() {
            return None;
        }
        let hash = hash_key(key);
        self.rehash_step_on_read_if_needed();
        let (t, top, depth, pos) = self.find(hash, key)?;
        let b = Self::bucket_at_mut(&mut self.tables[t].buckets, top, depth);
        let removed = b.entries[pos].take().map(|(_, v)| v);
        b.presence &= !(1 << pos);
        self.tables[t].used -= 1;
        self.shrink_if_needed();
        removed
    }

    pub fn clear(&mut self) {
        *self = Self::new();
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &V)> {
        self.tables.iter().flat_map(|t| {
            t.buckets.iter().flat_map(|top| {
                BucketChainIter { cur: Some(top) }.flat_map(|b| {
                    (0..b.num_positions()).filter_map(move |pos| {
                        if b.is_filled(pos) {
                            b.entries[pos].as_ref().map(|(k, v)| (k, v))
                        } else {
                            None
                        }
                    })
                })
            })
        })
    }

    pub fn keys(&self) -> impl Iterator<Item = &Bytes> {
        self.iter().map(|(k, _)| k)
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, v)| v)
    }

    pub fn scan<F: FnMut(&Bytes, &V)>(&self, cursor: u64, mut f: F) -> u64 {
        if self.is_empty() {
            return 0;
        }
        let bc0 = num_buckets(self.tables[0].exp) as u64;
        if bc0 == 0 {
            return 0;
        }
        let mask0 = bc0 - 1;
        let idx0 = (cursor & mask0) as usize;

        if !(self.is_rehashing() && idx0 < self.rehash_idx as usize)
            && idx0 < self.tables[0].buckets.len()
        {
            Self::walk_chain(&self.tables[0].buckets[idx0], &mut f);
        }

        if self.is_rehashing() {
            let bc1 = num_buckets(self.tables[1].exp) as u64;
            if bc1 > 0 {
                let ratio = (bc1 / bc0).max(1);
                for k in 0..ratio {
                    let idx1 = (k * bc0 + idx0 as u64) as usize;
                    if idx1 < self.tables[1].buckets.len() {
                        Self::walk_chain(&self.tables[1].buckets[idx1], &mut f);
                    }
                }
            }
        }

        next_cursor(cursor, mask0)
    }

    fn walk_chain<F: FnMut(&Bytes, &V)>(top: &Bucket<V>, f: &mut F) {
        let mut b = Some(top);
        while let Some(cur) = b {
            for pos in 0..cur.num_positions() {
                if cur.is_filled(pos) {
                    if let Some((k, v)) = cur.entries[pos].as_ref() {
                        f(k, v);
                    }
                }
            }
            b = cur.child.as_deref();
        }
    }

    pub fn extend<I: IntoIterator<Item = (Bytes, V)>>(&mut self, iter: I) {
        for (k, v) in iter {
            self.insert(k, v);
        }
    }

    pub fn rehash_index(&self) -> isize {
        self.rehash_idx
    }

    pub fn stats(&self, table_index: usize, full: bool) -> TableStats {
        let t = &self.tables[table_index];
        let mut stats = TableStats {
            table_index,
            rehash_index: self.rehash_idx,
            toplevel_buckets: num_buckets(t.exp),
            child_buckets: t.child_buckets,
            size: num_buckets(t.exp) * ENTRIES_PER_BUCKET,
            used: t.used,
            max_chain_len: 0,
            clvector: Vec::new(),
        };
        if !full {
            return stats;
        }
        stats.clvector = vec![0; STATS_VECTLEN];
        for top in &t.buckets {
            let mut chainlen = 0usize;
            let mut b = top;
            while b.chained {
                chainlen += 1;
                b = b.child.as_ref().unwrap();
            }
            if chainlen > stats.max_chain_len {
                stats.max_chain_len = chainlen;
            }
            let slot = chainlen.min(STATS_VECTLEN - 1);
            stats.clvector[slot] += 1;
        }
        stats
    }

    pub fn stats_msg(&self, full: bool) -> String {
        let mut out = self.stats(0, full).format();
        if self.is_rehashing() {
            out.push_str(&self.stats(1, full).format());
        }
        out
    }

    fn find(&self, hash: u64, key: &[u8]) -> Option<(usize, usize, usize, usize)> {
        let h2 = high_bits(hash);
        for t in 0..2 {
            if self.tables[t].used == 0 {
                continue;
            }
            let mask = exp_to_mask(self.tables[t].exp);
            let bucket_idx = (hash & mask) as usize;
            if t == 0 && self.rehash_idx >= 0 && bucket_idx < self.rehash_idx as usize {
                continue;
            }
            let mut b = &self.tables[t].buckets[bucket_idx];
            let mut depth = 0;
            loop {
                for pos in 0..b.num_positions() {
                    if b.is_filled(pos) && b.hashes[pos] == h2 {
                        if let Some((k, _)) = &b.entries[pos] {
                            if k.as_ref() == key {
                                return Some((t, bucket_idx, depth, pos));
                            }
                        }
                    }
                }
                match &b.child {
                    Some(c) => {
                        b = c;
                        depth += 1;
                    }
                    None => break,
                }
            }
        }
        None
    }

    fn bucket_at(buckets: &[Bucket<V>], top: usize, depth: usize) -> &Bucket<V> {
        let mut b = &buckets[top];
        for _ in 0..depth {
            b = b.child.as_ref().unwrap();
        }
        b
    }

    fn bucket_at_mut(buckets: &mut [Bucket<V>], top: usize, depth: usize) -> &mut Bucket<V> {
        let mut b = &mut buckets[top];
        for _ in 0..depth {
            b = b.child.as_mut().unwrap();
        }
        b
    }

    fn insert_new(&mut self, hash: u64, entry: (Bytes, V), h2: u8) {
        let t = if self.is_rehashing() { 1 } else { 0 };
        let mask = exp_to_mask(self.tables[t].exp);
        let top = (hash & mask) as usize;
        let created_child = Self::chain_insert(&mut self.tables[t].buckets[top], entry, h2);
        if created_child {
            self.tables[t].child_buckets += 1;
        }
        self.tables[t].used += 1;
    }

    fn chain_insert(top: &mut Bucket<V>, entry: (Bytes, V), h2: u8) -> bool {
        let mut created = false;
        let mut b = top;
        loop {
            if b.is_full() {
                if !b.chained {
                    let pos = ENTRIES_PER_BUCKET - 1;
                    let mut child = Box::new(Bucket::new());
                    let moved = b.entries[pos].take().unwrap();
                    let moved_h2 = b.hashes[pos];
                    child.set(0, moved, moved_h2);
                    b.presence &= !(1 << pos);
                    b.chained = true;
                    b.child = Some(child);
                    created = true;
                }
                b = b.child.as_mut().unwrap();
                continue;
            }
            break;
        }
        for pos in 0..ENTRIES_PER_BUCKET {
            if !b.is_filled(pos) {
                b.set(pos, entry, h2);
                return created;
            }
        }
        unreachable!("non-full bucket must have a free slot");
    }

    fn swap_tables(&mut self) {
        self.tables.swap(0, 1);
    }

    fn rehashing_completed(&mut self) {
        self.tables[0] = Table::empty();
        self.swap_tables();
        self.tables[1] = Table::empty();
        self.rehash_idx = -1;
    }

    fn resize(&mut self, min_capacity: usize) -> bool {
        debug_assert!(!self.is_rehashing());
        let min_capacity = min_capacity.max(1);
        let exp = next_bucket_exp(min_capacity);
        let old_exp = self.tables[0].exp;
        if exp == old_exp {
            return false;
        }
        self.tables[1] = Table::allocate(exp);
        self.rehash_idx = 0;
        if self.tables[0].buckets.is_empty()
            || (self.tables[0].used == 0 && self.tables[0].child_buckets == 0)
        {
            self.rehashing_completed();
        }
        true
    }

    fn expand_if_needed(&mut self) {
        if self.is_rehashing() {
            if self.tables[1].exp >= self.tables[0].exp {
                return;
            }
            self.abort_shrink_if_needed();
            return;
        }
        let min_capacity = self.tables[0].used + 1;
        let num = num_buckets(self.tables[0].exp);
        let current_capacity = num * ENTRIES_PER_BUCKET;
        if min_capacity * 100 <= current_capacity * MAX_FILL_PERCENT_SOFT {
            return;
        }
        self.resize(min_capacity);
    }

    fn shrink_if_needed(&mut self) {
        if self.is_rehashing() || self.pause_auto_shrink > 0 {
            return;
        }
        let current_capacity = num_buckets(self.tables[0].exp) * ENTRIES_PER_BUCKET;
        if self.tables[0].used * 100 > current_capacity * MIN_FILL_PERCENT_SOFT {
            return;
        }
        self.resize(self.tables[0].used);
    }

    fn abort_shrink_if_needed(&mut self) {
        if !can_abort_shrink() {
            return;
        }
        if !self.is_rehashing() || self.tables[1].exp >= self.tables[0].exp {
            return;
        }
        let num_elements = self.tables[0].used + self.tables[1].used + 1;
        let ht1_capacity = num_buckets(self.tables[1].exp) * ENTRIES_PER_BUCKET;
        if num_elements * 100 <= ht1_capacity * MAX_FILL_PERCENT_HARD {
            return;
        }
        self.swap_tables();
        self.rehash_idx = 0;
    }

    fn rehash_step_on_read_if_needed(&mut self) {
        if !self.is_rehashing() {
            return;
        }
        // One bucket chain per access — matches C zsetAdd / hashtableFindRef pacing.
        // Batching this breaks the shrink-then-abort-grow rhythm that zset.tcl
        // "resize test - shrink rehashing can be abort" depends on.
        self.rehash_step();
    }

    fn rehash_step(&mut self) {
        if self.tables[1].exp < self.tables[0].exp {
            self.rehash_step_shrink();
        } else {
            self.rehash_step_expand();
        }
    }

    fn rehash_step_finalize(&mut self) {
        let idx = self.rehash_idx as usize;
        let mut chain = self.tables[0].buckets[idx].child.take();
        while let Some(mut b) = chain {
            let next = b.child.take();
            self.tables[0].child_buckets -= 1;
            chain = next;
        }
        let b = &mut self.tables[0].buckets[idx];
        b.chained = false;
        b.presence = 0;

        self.rehash_idx += 1;

        if self.tables[0].used == 0 && self.tables[0].child_buckets == 0 {
            self.rehashing_completed();
        }
    }

    fn drain_chain(&mut self, idx: usize) -> Vec<(Bytes, V)> {
        let mut out = Vec::new();
        let top = &mut self.tables[0].buckets[idx];
        for pos in 0..top.num_positions() {
            if top.is_filled(pos) {
                if let Some(e) = top.entries[pos].take() {
                    out.push(e);
                }
            }
        }
        let mut child = top.child.take();
        let mut freed = 0usize;
        while let Some(mut b) = child {
            for pos in 0..b.num_positions() {
                if b.is_filled(pos) {
                    if let Some(e) = b.entries[pos].take() {
                        out.push(e);
                    }
                }
            }
            freed += 1;
            child = b.child.take();
        }
        self.tables[0].child_buckets -= freed;
        out
    }

    fn rehash_entry(&mut self, entry: (Bytes, V), hash: u64, h2: u8) {
        let mask = exp_to_mask(self.tables[1].exp);
        let top = (hash & mask) as usize;
        let created = Self::chain_insert(&mut self.tables[1].buckets[top], entry, h2);
        if created {
            self.tables[1].child_buckets += 1;
        }
        self.tables[0].used -= 1;
        self.tables[1].used += 1;
    }

    fn rehash_step_expand(&mut self) {
        let idx = self.rehash_idx as usize;
        let entries = self.drain_chain(idx);
        for (key, val) in entries {
            let hash = hash_key(&key);
            self.rehash_entry((key, val), hash, high_bits(hash));
        }
        self.rehash_step_finalize();
    }

    fn rehash_step_shrink(&mut self) {
        let mut empty_visits = 10;
        loop {
            let idx = self.rehash_idx as usize;
            if !self.chain_is_empty(idx) {
                break;
            }
            self.rehash_step_finalize();
            if !self.is_rehashing() {
                return;
            }
            empty_visits -= 1;
            if empty_visits == 0 {
                return;
            }
        }
        let idx = self.rehash_idx as usize;
        let entries = self.drain_chain(idx);
        let hash = self.rehash_idx as u64;
        for (key, val) in entries {
            let h2 = high_bits(hash_key(&key));
            self.rehash_entry((key, val), hash, h2);
        }
        self.rehash_step_finalize();
    }

    fn chain_is_empty(&self, idx: usize) -> bool {
        let mut b = &self.tables[0].buckets[idx];
        loop {
            if b.presence != 0 {
                return false;
            }
            match &b.child {
                Some(c) => b = c,
                None => return true,
            }
        }
    }
}

struct BucketChainIter<'a, V> {
    cur: Option<&'a Bucket<V>>,
}

impl<'a, V> Iterator for BucketChainIter<'a, V> {
    type Item = &'a Bucket<V>;
    fn next(&mut self) -> Option<Self::Item> {
        let b = self.cur?;
        self.cur = b.child.as_deref();
        Some(b)
    }
}

impl<'a, V: Clone> IntoIterator for &'a HashTable<V> {
    type Item = (&'a Bytes, &'a V);
    type IntoIter = Box<dyn Iterator<Item = (&'a Bytes, &'a V)> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

impl<V: Clone + PartialEq> PartialEq for HashTable<V> {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        use std::collections::HashMap;
        let lhs: HashMap<&[u8], &V> = self.iter().map(|(k, v)| (k.as_ref(), v)).collect();
        other
            .iter()
            .all(|(k, v)| lhs.get(k.as_ref()).is_some_and(|lv| *lv == v))
    }
}
