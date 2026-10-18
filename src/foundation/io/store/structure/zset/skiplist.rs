//! - insert / delete: O(log N)

use bytes::Bytes;
use std::fmt;
use std::ptr::NonNull;

#[path = "skiplist_range.rs"]
pub(crate) mod range_impl;
#[path = "skiplist_rank.rs"]
pub(crate) mod rank_impl;

const ZSKIPLIST_MAXLEVEL: usize = 32;

#[derive(Debug)]
pub struct Level {
    pub forward: Option<NonNull<Node>>,
    pub span: usize,
}

impl Level {
    fn new() -> Self {
        Self {
            forward: None,
            span: 0,
        }
    }
}

#[derive(Debug)]
pub struct Node {
    pub score: f64,
    pub member: Bytes,
    pub levels: Vec<Level>,
    pub backward: Option<NonNull<Node>>,
}

impl Node {
    fn new(level: usize, score: f64, member: Bytes) -> NonNull<Self> {
        let mut levels = Vec::with_capacity(level);
        for _ in 0..level {
            levels.push(Level::new());
        }
        let node = Box::new(Self {
            score,
            member,
            levels,
            backward: None,
        });
        // SAFETY: Box::into_raw is valid non-null aligned pointer.
        unsafe { NonNull::new_unchecked(Box::into_raw(node)) }
    }
    fn new_header() -> NonNull<Self> {
        let mut levels = Vec::with_capacity(ZSKIPLIST_MAXLEVEL);
        for _ in 0..ZSKIPLIST_MAXLEVEL {
            levels.push(Level::new());
        }
        let node = Box::new(Self {
            score: f64::NAN,
            member: Bytes::new(),
            levels,
            backward: None,
        });
        // SAFETY: Box::into_raw returns a valid non-null aligned pointer.
        unsafe { NonNull::new_unchecked(Box::into_raw(node)) }
    }
}

pub struct SkipList {
    pub(crate) header: NonNull<Node>,
    pub(crate) tail: Option<NonNull<Node>>,
    pub(crate) length: usize,
    pub(crate) level: usize,
}
// SAFETY: SkipList owns all nodes and only exposes shared reads through &self;
// structural mutation requires &mut self.
unsafe impl Send for SkipList {}
// SAFETY: shared access does not mutate internal raw pointers, and mutation is
// guarded by Rust's &mut self requirement.
unsafe impl Sync for SkipList {}

#[inline]
pub(crate) fn score_gte_min(s: f64, min: (f64, bool)) -> bool {
    if min.1 {
        s >= min.0
    } else {
        s > min.0
    }
}
#[inline]
pub(crate) fn score_lte_max(s: f64, max: (f64, bool)) -> bool {
    if max.1 {
        s <= max.0
    } else {
        s < max.0
    }
}

fn zsl_random_level() -> usize {
    use std::cell::RefCell;
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};
    thread_local! {
        static RNG: RefCell<u64> = RefCell::new({
            let s = RandomState::new();
            let mut h = s.build_hasher();
            h.write_u64(SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0));
            h.finish()
        });
    }
    RNG.with(|state| {
        let mut x = *state.borrow();
        let mut level = 1usize;
        while level < ZSKIPLIST_MAXLEVEL {
            x = x.wrapping_add(0x9E3779B97F4A7C15);
            let mut z = x;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
            z = z ^ (z >> 31);
            if (z & 0x3FFFFFFF) < (1u64 << 28) {
                level += 1;
            } else {
                break;
            }
        }
        *state.borrow_mut() = x;
        level
    })
}

impl SkipList {
    pub fn new() -> Self {
        Self {
            header: Node::new_header(),
            tail: None,
            length: 0,
            level: 1,
        }
    }
    pub fn len(&self) -> usize {
        self.length
    }
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn insert(&mut self, score: f64, member: Bytes) {
        let mut update: [Option<NonNull<Node>>; ZSKIPLIST_MAXLEVEL] = [None; ZSKIPLIST_MAXLEVEL];
        let mut rank: [usize; ZSKIPLIST_MAXLEVEL] = [0; ZSKIPLIST_MAXLEVEL];
        let hdr_p = self.header.as_ptr();
        let mut x = self.header;
        for i in (0..self.level).rev() {
            rank[i] = if i + 1 < self.level { rank[i + 1] } else { 0 };
            loop {
                // SAFETY: x is header or a node currently linked into this list.
                let xr = unsafe { x.as_ref() };
                match xr.levels[i].forward {
                    Some(next) => {
                        // SAFETY: forward pointers only reference live nodes while linked.
                        let nr = unsafe { next.as_ref() };
                        if nr.score < score
                            || (nr.score == score && nr.member.as_ref() < member.as_ref())
                        {
                            rank[i] += xr.levels[i].span;
                            x = next;
                        } else {
                            break;
                        }
                    }
                    None => break,
                }
            }
            update[i] = Some(x);
        }
        let new_level = zsl_random_level();
        if new_level > self.level {
            for i in self.level..new_level {
                rank[i] = 0;
                update[i] = Some(self.header);
                // SAFETY: header lives for &mut self lifetime; exclusive access.
                unsafe {
                    (&mut *hdr_p).levels[i].span = self.length;
                }
            }
            self.level = new_level;
        }
        let new_node = Node::new(new_level, score, member);
        let new_p = new_node.as_ptr();
        for i in 0..new_level {
            let upd = update[i].unwrap();
            let up = upd.as_ptr();
            // SAFETY: up is a predecessor in this list and new_p is a freshly
            // allocated node, both valid for exclusive mutation under &mut self.
            unsafe {
                let up_mut = &mut *up;
                let new_mut = &mut *new_p;
                let old_fwd = up_mut.levels[i].forward;
                let old_sp = up_mut.levels[i].span;
                let d = rank[0] + 1 - rank[i];
                new_mut.levels[i].forward = old_fwd;
                // new span: if no forward (tail at this level), span is 0.
                // Otherwise the formula dist(new, fwd) = old_sp + 1 - d.
                // Use saturating_sub for safety; tail case always yields 0 via the branch.
                if old_fwd.is_some() {
                    new_mut.levels[i].span = old_sp + 1 - d;
                } else {
                    new_mut.levels[i].span = 0;
                }
                up_mut.levels[i].forward = Some(new_node);
                up_mut.levels[i].span = d;
            }
        }
        for (i, upd_slot) in update.iter().enumerate().take(self.level).skip(new_level) {
            if let Some(upd) = upd_slot {
                let up = upd.as_ptr();
                // SAFETY: exclusive &mut self, up is a valid node pointer; we
                // only mutate its span field (no structural aliasing).
                unsafe {
                    let up_mut = &mut *up;
                    if up_mut.levels[i].forward.is_some() {
                        up_mut.levels[i].span += 1;
                    }
                }
            }
        }
        let pred = update[0].unwrap();
        let pp = pred.as_ptr();
        // SAFETY: new_p is the freshly inserted node and its level-0 forward
        // pointer, if present, still refers to a live neighbor in this list.
        unsafe {
            let new_mut = &mut *new_p;
            if std::ptr::eq(pp, hdr_p) {
                new_mut.backward = None;
            } else {
                new_mut.backward = Some(pred);
            }
            match new_mut.levels[0].forward {
                Some(nx) => {
                    (*nx.as_ptr()).backward = Some(new_node);
                }
                None => {
                    self.tail = Some(new_node);
                }
            }
        }
        self.length += 1;
    }

    pub fn delete(&mut self, score: f64, member: &[u8]) -> bool {
        let mut update: [Option<NonNull<Node>>; ZSKIPLIST_MAXLEVEL] = [None; ZSKIPLIST_MAXLEVEL];
        let mut x = self.header;
        for i in (0..self.level).rev() {
            loop {
                // SAFETY: x is header or a live node reached from this list.
                let xr = unsafe { x.as_ref() };
                match xr.levels[i].forward {
                    Some(next) => {
                        // SAFETY: forward pointers only reference live linked nodes.
                        let nr = unsafe { next.as_ref() };
                        if nr.score < score || (nr.score == score && nr.member.as_ref() < member) {
                            x = next;
                        } else {
                            break;
                        }
                    }
                    None => break,
                }
            }
            update[i] = Some(x);
        }
        // SAFETY: update[0] is set to header or a valid predecessor above.
        let candidate = unsafe { update[0].unwrap().as_ref().levels[0].forward };
        let target = match candidate {
            Some(n) => {
                // SAFETY: candidate is a level-0 forward pointer in this list.
                let r = unsafe { n.as_ref() };
                if r.score == score && r.member.as_ref() == member {
                    n
                } else {
                    return false;
                }
            }
            None => return false,
        };
        self.delete_node(target, &update);
        true
    }

    pub(crate) fn delete_node(
        &mut self,
        node: NonNull<Node>,
        update: &[Option<NonNull<Node>>; ZSKIPLIST_MAXLEVEL],
    ) {
        let np = node.as_ptr();
        // SAFETY: node is a live node selected from this list.
        let nh = unsafe { (*np).levels.len() };
        let hdr_p = self.header.as_ptr();
        for (i, upd_slot) in update.iter().enumerate().take(self.level) {
            let upd = match upd_slot {
                Some(u) => *u,
                None => break,
            };
            let up = upd.as_ptr();
            // SAFETY: update entries are live predecessors in this list and np
            // is the node being unlinked under exclusive &mut self access.
            unsafe {
                let fwd = (&*up).levels[i].forward;
                let direct = match fwd {
                    Some(f) => std::ptr::eq(f.as_ptr(), np),
                    None => false,
                };
                if direct {
                    let ts = (&*np).levels[i].span;
                    (&mut *up).levels[i].forward = (&*np).levels[i].forward;
                    (&mut *up).levels[i].span = (&*up).levels[i].span - 1 + ts;
                } else if i >= nh && (&*up).levels[i].forward.is_some() {
                    (&mut *up).levels[i].span -= 1;
                }
            }
        }
        // SAFETY: np is still linked at level 0; its neighbor pointers are live
        // and are updated before np is freed.
        unsafe {
            match (&*np).levels[0].forward {
                Some(nx) => {
                    (*nx.as_ptr()).backward = (&*np).backward;
                }
                None => {
                    self.tail = (&*np).backward;
                }
            }
        }
        while self.level > 1 {
            let top = self.level - 1;
            // SAFETY: header lives for the SkipList lifetime and has max levels.
            if unsafe { (&*hdr_p).levels[top].forward.is_none() } {
                self.level -= 1;
            } else {
                break;
            }
        }
        self.length -= 1;
        // SAFETY: np came from Box::into_raw and has been unlinked exactly once.
        unsafe {
            drop(Box::from_raw(np));
        }
    }

    pub fn sorted_vec(&self) -> Vec<(Bytes, f64)> {
        let mut out = Vec::with_capacity(self.length);
        // SAFETY: header is live for the SkipList lifetime.
        let mut cur = unsafe { self.header.as_ref().levels[0].forward };
        while let Some(n) = cur {
            // SAFETY: cur walks level-0 forward pointers of live linked nodes.
            let nr = unsafe { n.as_ref() };
            out.push((nr.member.clone(), nr.score));
            cur = nr.levels[0].forward;
        }
        out
    }

    pub fn pop_min(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        if count == 0 || self.is_empty() {
            return Vec::new();
        }
        let take = count.min(self.length);
        let mut out = Vec::with_capacity(take);
        for _ in 0..take {
            // SAFETY: non-empty lists maintain a live first node after header.
            let first = match unsafe { self.header.as_ref().levels[0].forward } {
                Some(n) => n,
                None => break,
            };
            let (m, s) = {
                // SAFETY: first is the live level-0 head node.
                let r = unsafe { first.as_ref() };
                (r.member.clone(), r.score)
            };
            self.delete(s, &m);
            out.push((m, s));
        }
        out
    }
    pub fn pop_max(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        if count == 0 || self.is_empty() {
            return Vec::new();
        }
        let take = count.min(self.length);
        let mut out = Vec::with_capacity(take);
        for _ in 0..take {
            let last = match self.tail {
                Some(n) => n,
                None => break,
            };
            let (m, s) = {
                // SAFETY: last is the live tail node maintained by the list.
                let r = unsafe { last.as_ref() };
                (r.member.clone(), r.score)
            };
            self.delete(s, &m);
            out.push((m, s));
        }
        out
    }
    pub fn iter(&self) -> SkipListIter<'_> {
        // SAFETY: header is live and iteration borrows the list immutably.
        let first = unsafe { self.header.as_ref().levels[0].forward };
        SkipListIter {
            cursor: first,
            _mk: std::marker::PhantomData,
        }
    }
}

impl fmt::Debug for SkipList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SkipList")
            .field("len", &self.length)
            .field("level", &self.level)
            .finish_non_exhaustive()
    }
}
impl Clone for SkipList {
    fn clone(&self) -> Self {
        let mut n = Self::new();
        for (m, s) in self.sorted_vec() {
            n.insert(s, m);
        }
        n
    }
}
impl PartialEq for SkipList {
    fn eq(&self, other: &Self) -> bool {
        if self.length != other.length {
            return false;
        }
        self.sorted_vec() == other.sorted_vec()
    }
}
impl Eq for SkipList {}
impl Drop for SkipList {
    fn drop(&mut self) {
        // SAFETY: header is still live while Drop walks and frees level-0 nodes.
        let mut cur = unsafe { self.header.as_ref().levels[0].forward };
        while let Some(n) = cur {
            let p = n.as_ptr();
            // SAFETY: p is the next live node, read its successor before freeing it.
            unsafe {
                cur = (&*p).levels[0].forward;
                drop(Box::from_raw(p));
            }
        }
        // SAFETY: header came from Box::into_raw and is freed exactly once here.
        unsafe {
            drop(Box::from_raw(self.header.as_ptr()));
        }
    }
}
impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SkipListIter<'a> {
    cursor: Option<NonNull<Node>>,
    _mk: std::marker::PhantomData<&'a SkipList>,
}
impl<'a> Iterator for SkipListIter<'a> {
    type Item = (&'a Bytes, &'a f64);
    fn next(&mut self) -> Option<Self::Item> {
        let c = self.cursor?;
        // SAFETY: the iterator lifetime is tied to &SkipList, and nodes are not
        // mutated or freed while that immutable borrow is alive.
        let r: &'a Node = unsafe { &*c.as_ptr() };
        self.cursor = r.levels[0].forward;
        Some((&r.member, &r.score))
    }
}
