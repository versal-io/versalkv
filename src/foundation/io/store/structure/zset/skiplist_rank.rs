use bytes::Bytes;
use std::ptr::NonNull;

use super::{Node, SkipList};

impl SkipList {
    pub fn get_rank(&self, score: f64, member: &[u8]) -> Option<usize> {
        let mut rank = 0usize;
        let mut x = self.header;
        for i in (0..self.level).rev() {
            loop {
                // SAFETY: x is header or a live node reached from this list.
                let xr = unsafe { x.as_ref() };
                match xr.levels[i].forward {
                    Some(next) => {
                        // SAFETY: forward pointers only reference live linked nodes.
                        let nr = unsafe { next.as_ref() };
                        if nr.score < score || (nr.score == score && nr.member.as_ref() <= member) {
                            rank += xr.levels[i].span;
                            if nr.score == score && nr.member.as_ref() == member {
                                return Some(rank - 1);
                            }
                            x = next;
                        } else {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
        None
    }

    pub fn get_element_by_rank(&self, rank0: usize) -> Option<(Bytes, f64)> {
        if rank0 >= self.length {
            return None;
        }
        self.get_node_by_rank(rank0).map(|n| {
            // SAFETY: n is a live node returned by rank lookup.
            let r = unsafe { n.as_ref() };
            (r.member.clone(), r.score)
        })
    }

    pub(crate) fn get_node_by_rank(&self, rank0: usize) -> Option<NonNull<Node>> {
        if rank0 >= self.length {
            return None;
        }
        let tgt = rank0 + 1;
        let mut trav = 0usize;
        let mut x = self.header;
        for i in (0..self.level).rev() {
            loop {
                // SAFETY: x is header or a live node reached from this list.
                let xr = unsafe { x.as_ref() };
                match xr.levels[i].forward {
                    Some(next) => {
                        let sp = xr.levels[i].span;
                        if trav + sp > tgt {
                            break;
                        }
                        trav += sp;
                        x = next;
                        if trav == tgt {
                            return Some(x);
                        }
                    }
                    None => break,
                }
            }
        }
        None
    }

    pub(crate) fn rank_of(&self, node: NonNull<Node>) -> usize {
        let tp = node.as_ptr();
        let mut r = 0usize;
        let mut x = self.header;
        for i in (0..self.level).rev() {
            loop {
                // SAFETY: x is header or a live node reached from this list.
                let xr = unsafe { x.as_ref() };
                match xr.levels[i].forward {
                    Some(next) => {
                        r += xr.levels[i].span;
                        if std::ptr::eq(next.as_ptr(), tp) {
                            return r;
                        }
                        // SAFETY: next and target node are live nodes in this list.
                        let nr = unsafe { next.as_ref() };
                        let tr = unsafe { &*tp };
                        if nr.score > tr.score
                            || (nr.score == tr.score && nr.member.as_ref() > tr.member.as_ref())
                        {
                            r -= xr.levels[i].span;
                            break;
                        }
                        x = next;
                    }
                    None => break,
                }
            }
        }
        r
    }
}
