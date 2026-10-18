#![allow(dead_code)]

use bytes::Bytes;
use std::ptr::NonNull;

const FANOUT: usize = 16;

struct Node {
    entries: Vec<Bytes>,
    prev: Option<NonNull<Node>>,
    next: Option<NonNull<Node>>,
}

impl Node {
    fn new() -> Box<Node> {
        Box::new(Node {
            entries: Vec::with_capacity(FANOUT),
            prev: None,
            next: None,
        })
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_full(&self) -> bool {
        self.entries.len() == FANOUT
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn push_back(&mut self, val: Bytes) {
        debug_assert!(!self.is_full());
        self.entries.push(val);
    }

    fn push_front(&mut self, val: Bytes) {
        debug_assert!(!self.is_full());
        self.entries.insert(0, val);
    }

    fn pop_front(&mut self) -> Bytes {
        debug_assert!(!self.is_empty());
        self.entries.remove(0)
    }

    fn pop_back(&mut self) -> Bytes {
        debug_assert!(!self.is_empty());
        self.entries.pop().unwrap()
    }

    fn get(&self, i: usize) -> &Bytes {
        &self.entries[i]
    }
}

pub struct Quicklist {
    head: Option<NonNull<Node>>,
    tail: Option<NonNull<Node>>,
    len: usize,
}

// SAFETY: Quicklist owns all its Nodes via Box, and all access to Nodes goes through
// &mut self / &self on Quicklist; Send is safe because there is no interior mutability
// or shared aliasing of Node allocations.
unsafe impl Send for Quicklist {}

impl Drop for Quicklist {
    fn drop(&mut self) {
        let mut cur = self.head;
        while let Some(node_ptr) = cur {
            // SAFETY: node_ptr came from Box::into_raw in push_front/push_back and we
            // have &mut self with exclusive access; reading next before dropping the
            // Box is safe because the field is copied out before deallocation.
            unsafe {
                let next = node_ptr.as_ref().next;
                drop(Box::from_raw(node_ptr.as_ptr()));
                cur = next;
            }
        }
    }
}

impl Quicklist {
    pub fn new() -> Self {
        Self {
            head: None,
            tail: None,
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn push_front(&mut self, val: Bytes) {
        // SAFETY: NonNull<Node> pointers obtained from Box::into_raw are valid and
        // aligned; as_ref()/as_mut() are safe because we have &mut self on Quicklist
        // which guarantees exclusive access to all Nodes.
        unsafe {
            match self.head {
                Some(mut head) if !head.as_ref().is_full() => {
                    head.as_mut().push_front(val);
                    self.len += 1;
                }
                _ => {
                    let mut new_node = Node::new();
                    new_node.push_back(val);
                    let mut new_ptr = NonNull::new(Box::into_raw(new_node)).unwrap();
                    if let Some(mut old_head) = self.head {
                        new_ptr.as_mut().next = Some(old_head);
                        old_head.as_mut().prev = Some(new_ptr);
                    } else {
                        self.tail = Some(new_ptr);
                    }
                    self.head = Some(new_ptr);
                    self.len += 1;
                }
            }
        }
    }

    pub fn push_back(&mut self, val: Bytes) {
        // SAFETY: same invariants as push_front — NonNull pointers are valid and
        // exclusive access is guaranteed by &mut self.
        unsafe {
            match self.tail {
                Some(mut tail) if !tail.as_ref().is_full() => {
                    tail.as_mut().push_back(val);
                    self.len += 1;
                }
                _ => {
                    let mut new_node = Node::new();
                    new_node.push_back(val);
                    let mut new_ptr = NonNull::new(Box::into_raw(new_node)).unwrap();
                    if let Some(mut old_tail) = self.tail {
                        new_ptr.as_mut().prev = Some(old_tail);
                        old_tail.as_mut().next = Some(new_ptr);
                    } else {
                        self.head = Some(new_ptr);
                    }
                    self.tail = Some(new_ptr);
                    self.len += 1;
                }
            }
        }
    }

    pub fn pop_front(&mut self) -> Option<Bytes> {
        // SAFETY: head is either None or a valid NonNull<Node> from a prior
        // Box::into_raw; after popping, if the node is empty we reclaim ownership
        // via Box::from_raw and drop it; reading next before deallocation is safe.
        unsafe {
            let mut head = self.head?;
            let val = head.as_mut().pop_front();
            self.len -= 1;
            if head.as_ref().is_empty() {
                let next = head.as_ref().next;
                drop(Box::from_raw(head.as_ptr()));
                self.head = next;
                match self.head {
                    Some(mut n) => n.as_mut().prev = None,
                    None => self.tail = None,
                }
            }
            Some(val)
        }
    }

    pub fn pop_back(&mut self) -> Option<Bytes> {
        // SAFETY: same invariants as pop_front for the tail pointer.
        unsafe {
            let mut tail = self.tail?;
            let val = tail.as_mut().pop_back();
            self.len -= 1;
            if tail.as_ref().is_empty() {
                let prev = tail.as_ref().prev;
                drop(Box::from_raw(tail.as_ptr()));
                self.tail = prev;
                match self.tail {
                    Some(mut n) => n.as_mut().next = None,
                    None => self.head = None,
                }
            }
            Some(val)
        }
    }

    pub fn to_vec(&self) -> Vec<Bytes> {
        let mut result = Vec::with_capacity(self.len);
        // SAFETY: traversing the linked list via NonNull is safe with &self because
        // we only read through the pointers and never mutate; all Nodes are valid
        // while Quicklist is alive.
        unsafe {
            let mut cur = self.head;
            while let Some(node_ptr) = cur {
                let node = node_ptr.as_ref();
                for i in 0..node.len() {
                    result.push(node.get(i).clone());
                }
                cur = node.next;
            }
        }
        result
    }

    pub fn node_count(&self) -> usize {
        let mut count = 0;
        // SAFETY: same traversal invariants as to_vec.
        unsafe {
            let mut cur = self.head;
            while let Some(node_ptr) = cur {
                count += 1;
                cur = node_ptr.as_ref().next;
            }
        }
        count
    }
}

impl Default for Quicklist {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_pop_fifo_order_small() {
        let mut q = Quicklist::new();
        for i in 0..100 {
            q.push_back(Bytes::from(format!("{i}")));
        }
        assert_eq!(q.len(), 100);
        for i in 0..100 {
            let v = q.pop_front().unwrap();
            assert_eq!(v, Bytes::from(format!("{i}")));
        }
        assert!(q.is_empty());
    }

    #[test]
    fn push_pop_lifo_from_back() {
        let mut q = Quicklist::new();
        for i in 0..50 {
            q.push_back(Bytes::from(format!("{i}")));
        }
        for i in (0..50).rev() {
            let v = q.pop_back().unwrap();
            assert_eq!(v, Bytes::from(format!("{i}")));
        }
        assert!(q.is_empty());
    }

    #[test]
    fn push_front_pop_back_reverse() {
        let mut q = Quicklist::new();
        for i in 0..50 {
            q.push_front(Bytes::from(format!("{i}")));
        }
        for i in 0..50 {
            let v = q.pop_back().unwrap();
            assert_eq!(v, Bytes::from(format!("{i}")));
        }
    }

    #[test]
    fn alternating_push_both_ends() {
        let mut q = Quicklist::new();
        q.push_back(Bytes::from("B0"));
        q.push_front(Bytes::from("F0"));
        q.push_back(Bytes::from("B1"));
        q.push_front(Bytes::from("F1"));
        let vec = q.to_vec();
        assert_eq!(
            vec,
            vec![
                Bytes::from("F1"),
                Bytes::from("F0"),
                Bytes::from("B0"),
                Bytes::from("B1"),
            ]
        );
    }

    #[test]
    fn node_splitting_occurs() {
        let mut q = Quicklist::new();
        for _ in 0..FANOUT * 3 {
            q.push_back(Bytes::from(vec![b'x'; 1]));
        }
        assert_eq!(q.len(), FANOUT * 3);
        assert!(q.node_count() >= 3);
    }

    #[test]
    fn million_push_pop_correctness() {
        let n: u64 = 1_000_000;
        let mut q = Quicklist::new();
        for i in 0..n {
            q.push_back(Bytes::from(i.to_le_bytes().to_vec()));
        }
        assert_eq!(q.len(), n as usize);
        for i in 0..n {
            let v = q.pop_front().unwrap();
            assert_eq!(v.as_ref(), i.to_le_bytes().as_ref(), "mismatch at {i}");
        }
        assert!(q.is_empty());
        assert_eq!(q.node_count(), 0);
    }

    #[test]
    fn million_alternating_push_pop() {
        let n: u64 = 1_000_000;
        let mut q = Quicklist::new();
        for i in 0..n {
            if i % 2 == 0 {
                q.push_back(Bytes::from(i.to_le_bytes().to_vec()));
            } else {
                q.push_front(Bytes::from(i.to_le_bytes().to_vec()));
            }
        }
        assert_eq!(q.len(), n as usize);
        let vec = q.to_vec();
        assert_eq!(vec.len(), n as usize);
        while q.pop_front().is_some() {}
        assert!(q.is_empty());
        assert_eq!(q.node_count(), 0);
    }
}
