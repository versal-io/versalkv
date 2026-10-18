use bytes::Bytes;
use std::ptr::NonNull;

use super::score_gte_min;
use super::score_lte_max;
use super::{Node, SkipList};

impl SkipList {
    fn in_range(&self, min: (f64, bool), max: (f64, bool)) -> bool {
        if self.is_empty() {
            return false;
        }
        // SAFETY: non-empty lists always maintain a live tail pointer.
        let last_score = unsafe { self.tail.unwrap().as_ref().score };
        if !score_gte_min(last_score, min) {
            return false;
        }
        // SAFETY: non-empty lists always have a live first node after header.
        let first_score = unsafe {
            let first = self.header.as_ref().levels[0].forward.unwrap();
            first.as_ref().score
        };
        if !score_lte_max(first_score, max) {
            return false;
        }
        true
    }

    fn first_in_range_node(&self, min: (f64, bool), max: (f64, bool)) -> Option<NonNull<Node>> {
        if !self.in_range(min, max) {
            return None;
        }
        let mut x = self.header;
        for i in (0..self.level).rev() {
            loop {
                // SAFETY: x is header or a live node reached from this list.
                let xr = unsafe { x.as_ref() };
                match xr.levels[i].forward {
                    Some(next) => {
                        // SAFETY: next is a live forward pointer in this list.
                        if !score_gte_min(unsafe { next.as_ref() }.score, min) {
                            x = next;
                        } else {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
        // SAFETY: in_range guaranteed a candidate exists after x at level 0.
        let cand = unsafe { x.as_ref().levels[0].forward.unwrap() };
        // SAFETY: cand is a live level-0 node in this list.
        if score_lte_max(unsafe { cand.as_ref() }.score, max) {
            Some(cand)
        } else {
            None
        }
    }

    fn last_in_range_node(&self, min: (f64, bool), max: (f64, bool)) -> Option<NonNull<Node>> {
        if !self.in_range(min, max) {
            return None;
        }
        let mut x = self.header;
        for i in (0..self.level).rev() {
            loop {
                // SAFETY: x is header or a live node reached from this list.
                let xr = unsafe { x.as_ref() };
                match xr.levels[i].forward {
                    Some(next) => {
                        // SAFETY: next is a live forward pointer in this list.
                        if score_lte_max(unsafe { next.as_ref() }.score, max) {
                            x = next;
                        } else {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
        // SAFETY: x is the last node not greater than max, produced by traversal.
        if score_gte_min(unsafe { x.as_ref() }.score, min) {
            Some(x)
        } else {
            None
        }
    }

    pub fn count_in_score_range(&self, min: (f64, bool), max: (f64, bool)) -> usize {
        let f = match self.first_in_range_node(min, max) {
            Some(n) => n,
            None => return 0,
        };
        let l = match self.last_in_range_node(min, max) {
            Some(n) => n,
            None => return 0,
        };
        let rf = self.rank_of(f);
        let rl = self.rank_of(l);
        rl - rf + 1
    }

    pub fn first_in_score_range(&self, min: (f64, bool), max: (f64, bool)) -> Option<(Bytes, f64)> {
        let n = self.first_in_range_node(min, max)?;
        // SAFETY: first_in_range_node returns a live node pointer from this list.
        let r = unsafe { n.as_ref() };
        Some((r.member.clone(), r.score))
    }

    /// - min/max: (bound, inclusive)
    pub fn range_by_score(
        &self,
        min: (f64, bool),
        max: (f64, bool),
        off: i64,
        lim: i64,
        rev: bool,
    ) -> Vec<(Bytes, f64)> {
        if self.is_empty() {
            return Vec::new();
        }
        let skip = if off < 0 { 0 } else { off as usize };
        let rem = if lim < 0 { usize::MAX } else { lim as usize };
        if rem == 0 {
            return Vec::new();
        }

        if !rev {
            let start = match self.first_in_range_node(min, max) {
                Some(n) => n,
                None => return Vec::new(),
            };
            let start_rank = self.rank_of(start) - 1; // 0-based
            let target_rank = start_rank + skip;
            // O(log N) jump to offset start node via rank span.
            let mut cur = if target_rank == start_rank {
                Some(start)
            } else {
                self.get_node_by_rank(target_rank)
            };
            // Collect up to rem nodes while score <= max.
            let mut out = Vec::with_capacity(rem.min(64));
            while out.len() < rem {
                match cur {
                    Some(n) => {
                        // SAFETY: cur is a live node in the forward score walk.
                        let nr = unsafe { n.as_ref() };
                        if !score_lte_max(nr.score, max) {
                            break;
                        }
                        out.push((nr.member.clone(), nr.score));
                        cur = nr.levels[0].forward;
                    }
                    None => break,
                }
            }
            out
        } else {
            let start = match self.last_in_range_node(min, max) {
                Some(n) => n,
                None => return Vec::new(),
            };
            let start_rank = self.rank_of(start) - 1;
            // Reverse: offset moves backward from the 0-based start rank by `skip`.
            if start_rank < skip {
                return Vec::new();
            }
            let target_rank = start_rank - skip;
            // O(log N) jump via rank span.
            let mut cur = if target_rank == start_rank {
                Some(start)
            } else {
                self.get_node_by_rank(target_rank)
            };
            let mut out = Vec::with_capacity(rem.min(64));
            while out.len() < rem {
                match cur {
                    Some(n) => {
                        // SAFETY: cur is a live node in the reverse score walk.
                        let nr = unsafe { n.as_ref() };
                        if !score_gte_min(nr.score, min) {
                            break;
                        }
                        out.push((nr.member.clone(), nr.score));
                        cur = nr.backward;
                    }
                    None => break,
                }
            }
            out
        }
    }

    pub fn range_by_rank(&self, start: i64, end: i64, rev: bool) -> Vec<(Bytes, f64)> {
        if self.is_empty() {
            return Vec::new();
        }
        let l = self.length as i64;
        let mut s = if start < 0 { l + start } else { start };
        let mut e = if end < 0 { l + end } else { end };
        if s < 0 {
            s = 0;
        }
        if e >= l {
            e = l - 1;
        }
        if s > e {
            return Vec::new();
        }
        let (s, e) = if rev {
            let s_fwd = (l - 1 - e) as usize;
            let e_fwd = (l - 1 - s) as usize;
            (s_fwd, e_fwd)
        } else {
            (s as usize, e as usize)
        };
        let count = e - s + 1;
        let mut out = Vec::with_capacity(count);
        let start_rank = if !rev { s } else { e };
        let mut cur = self.get_node_by_rank(start_rank);
        if !rev {
            for _ in 0..count {
                match cur {
                    Some(n) => {
                        // SAFETY: n was produced by rank lookup, walks level-0 forward.
                        let nr = unsafe { n.as_ref() };
                        out.push((nr.member.clone(), nr.score));
                        cur = nr.levels[0].forward;
                    }
                    None => break,
                }
            }
        } else {
            for _ in 0..count {
                match cur {
                    Some(n) => {
                        // SAFETY: n was produced by rank lookup, walks backward.
                        let nr = unsafe { n.as_ref() };
                        out.push((nr.member.clone(), nr.score));
                        cur = nr.backward;
                    }
                    None => break,
                }
            }
        }
        out
    }

    pub fn range_by_lex(
        &self,
        min: &[u8],
        min_inc: bool,
        max: &[u8],
        max_inc: bool,
        rev: bool,
    ) -> Vec<(Bytes, f64)> {
        let mut all = self.sorted_vec();
        let min_is_minf = min == b"-";
        let min_is_pinf = min == b"+";
        let max_is_minf = max == b"-";
        let max_is_pinf = max == b"+";
        all.retain(|(m, _)| {
            let ok_mn = if min_is_minf {
                true
            } else if min_is_pinf {
                false
            } else if min_inc {
                m.as_ref() >= min
            } else {
                m.as_ref() > min
            };
            let ok_mx = if max_is_pinf {
                true
            } else if max_is_minf {
                false
            } else if max_inc {
                m.as_ref() <= max
            } else {
                m.as_ref() < max
            };
            ok_mn && ok_mx
        });
        if rev {
            all.reverse();
        }
        all
    }

    pub fn count_in_lex_range(
        &self,
        min: &[u8],
        min_inc: bool,
        max: &[u8],
        max_inc: bool,
    ) -> usize {
        let min_is_minf = min == b"-";
        let min_is_pinf = min == b"+";
        let max_is_minf = max == b"-";
        let max_is_pinf = max == b"+";
        let mut c = 0usize;
        for (m, _) in self.sorted_vec() {
            let ok_mn = if min_is_minf {
                true
            } else if min_is_pinf {
                false
            } else if min_inc {
                m.as_ref() >= min
            } else {
                m.as_ref() > min
            };
            let ok_mx = if max_is_pinf {
                true
            } else if max_is_minf {
                false
            } else if max_inc {
                m.as_ref() <= max
            } else {
                m.as_ref() < max
            };
            if ok_mn && ok_mx {
                c += 1;
            }
        }
        c
    }

    pub fn delete_range_by_score(&mut self, min: (f64, bool), max: (f64, bool)) -> usize {
        let nodes = self.range_by_score(min, max, 0, -1, false);
        let c = nodes.len();
        for (m, s) in nodes {
            self.delete(s, &m);
        }
        c
    }
    pub fn delete_range_by_rank(&mut self, start: usize, end: usize) -> usize {
        let nodes = self.range_by_rank(start as i64, end as i64, false);
        let c = nodes.len();
        for (m, s) in nodes {
            self.delete(s, &m);
        }
        c
    }
    pub fn delete_range_by_lex(
        &mut self,
        min: &[u8],
        min_inc: bool,
        max: &[u8],
        max_inc: bool,
    ) -> usize {
        let nodes = self.range_by_lex(min, min_inc, max, max_inc, false);
        let c = nodes.len();
        for (m, s) in nodes {
            self.delete(s, &m);
        }
        c
    }
}
