use bytes::Bytes;
use slab::Slab;
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::ops::RangeBounds;

use super::PathHashPayload;

#[derive(Debug, Clone)]
pub(super) struct PathEntry {
    pub path: Bytes,
    pub payload: PathHashPayload,
}

impl PathEntry {
    fn tracked_bytes(&self) -> usize {
        self.path.len()
            + std::mem::size_of::<Bytes>()
            + std::mem::size_of::<usize>() * 4
            + self.payload.heap_usage()
    }
}

/// Native byte-sorted paths with an immutable range interface.
#[derive(Debug, Default)]
pub struct OrderedPaths {
    pub(super) tree: BTreeMap<Bytes, usize>,
    pub(super) entries: Slab<PathEntry>,
    pub(super) tracked_bytes: usize,
}

impl Clone for OrderedPaths {
    fn clone(&self) -> Self {
        let mut entries = self.entries.clone();
        entries.reserve_exact(self.entries.capacity() - entries.len());
        Self {
            tree: self.tree.clone(),
            entries,
            tracked_bytes: self.tracked_bytes,
        }
    }
}

impl OrderedPaths {
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&Bytes, &PathHashPayload)> {
        self.tree
            .iter()
            .map(|(path, id)| (path, &self.entries[*id].payload))
    }

    pub fn range<T: Ord + ?Sized, R: RangeBounds<T>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = (&Bytes, &PathHashPayload)>
    where
        Bytes: Borrow<T>,
    {
        self.tree
            .range(range)
            .map(|(path, id)| (path, &self.entries[*id].payload))
    }

    pub(super) fn insert(&mut self, path: Bytes, payload: PathHashPayload) -> usize {
        let entry = PathEntry {
            path: path.clone(),
            payload,
        };
        self.tracked_bytes += entry.tracked_bytes();
        let id = self.entries.insert(entry);
        self.tree.insert(path, id);
        id
    }

    pub(super) fn remove_entry(&mut self, id: usize) {
        let entry = self.entries.remove(id);
        self.tracked_bytes -= entry.tracked_bytes();
    }

    pub(super) fn heap_usage(&self) -> usize {
        self.tracked_bytes
            + self.entries.capacity()
                * (std::mem::size_of::<PathEntry>() + std::mem::size_of::<usize>())
    }

    #[cfg(test)]
    pub(super) fn assert_accounting(&self) {
        assert_eq!(
            self.tracked_bytes,
            self.entries
                .iter()
                .map(|(_, entry)| entry.tracked_bytes())
                .sum::<usize>()
        );
        assert_eq!(self.tree.len(), self.entries.len());
        for (path, id) in &self.tree {
            assert_eq!(path, &self.entries[*id].path);
        }
    }
}
