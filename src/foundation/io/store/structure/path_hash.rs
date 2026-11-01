use bytes::Bytes;
use std::ops::Deref;

mod ordered;
mod payload;
mod prefix_index;

pub use ordered::OrderedPaths;
pub use payload::PathHashPayload;
use prefix_index::PrefixIndex;

/// Read-only compatibility view for ordered scans. No mutable view is exposed.
#[derive(Debug, Clone, Default)]
pub struct PathHashView {
    pub paths: OrderedPaths,
}

/// Ordered paths remain present only while their payload has fields. An empty
/// outer object is valid and retains the owning key's metadata.
#[derive(Debug, Clone, Default)]
pub struct PathHash {
    view: PathHashView,
    index: PrefixIndex,
}

impl Deref for PathHash {
    type Target = PathHashView;

    fn deref(&self) -> &Self::Target {
        &self.view
    }
}

impl PartialEq for PathHash {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl PathHash {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.view.paths.tree.len()
    }

    pub fn is_empty(&self) -> bool {
        self.view.paths.tree.is_empty()
    }

    pub fn get(&self, path: &[u8]) -> Option<&PathHashPayload> {
        let paths = &self.view.paths;
        paths.tree.get(path).map(|id| &paths.entries[*id].payload)
    }

    pub fn contains_path(&self, path: &[u8]) -> bool {
        self.view.paths.tree.contains_key(path)
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&Bytes, &PathHashPayload)> {
        self.view.paths.iter()
    }

    /// Reject invalid load entries without changing any of the indexes.
    pub fn insert_checked(&mut self, path: Bytes, payload: PathHashPayload) -> bool {
        if payload.is_empty() || self.contains_path(&path) {
            return false;
        }
        let id = self.view.paths.insert(path.clone(), payload);
        self.index.insert(&path, id);
        true
    }

    pub fn set_pairs(
        &mut self,
        path: &[u8],
        pairs: &[(Bytes, Bytes)],
        max_entries: usize,
        max_value_size: usize,
    ) {
        if pairs.is_empty() {
            return;
        }
        let paths = &mut self.view.paths;
        if let Some(&id) = paths.tree.get(path) {
            let payload = &mut paths.entries[id].payload;
            let old_size = payload.heap_usage();
            payload.set_pairs(pairs, max_entries, max_value_size);
            paths.tracked_bytes = paths.tracked_bytes - old_size + payload.heap_usage();
        } else {
            let mut payload = PathHashPayload::default();
            payload.set_pairs(pairs, max_entries, max_value_size);
            self.insert_checked(Bytes::copy_from_slice(path), payload);
        }
    }

    pub fn delete_path(&mut self, path: &[u8]) -> bool {
        let Some(id) = self.view.paths.tree.remove(path) else {
            return false;
        };
        self.index.remove(path);
        self.view.paths.remove_entry(id);
        if self.is_empty() {
            // Release retained arena capacities once the last path is removed.
            self.view.paths = OrderedPaths::default();
            self.index = PrefixIndex::default();
        }
        true
    }

    pub fn delete_fields(&mut self, path: &[u8], fields: &[Bytes]) -> usize {
        let paths = &mut self.view.paths;
        let Some(&id) = paths.tree.get(path) else {
            return 0;
        };
        let payload = &mut paths.entries[id].payload;
        let old_size = payload.heap_usage();
        let removed = fields.iter().filter(|field| payload.delete(field)).count();
        paths.tracked_bytes = paths.tracked_bytes - old_size + payload.heap_usage();
        if payload.is_empty() {
            self.delete_path(path);
        }
        removed
    }

    pub fn delete_prefix(&mut self, prefix: &[u8]) -> usize {
        if prefix.is_empty() {
            let removed = self.len();
            self.view.paths = OrderedPaths::default();
            self.index = PrefixIndex::default();
            return removed;
        }
        let mut removed = 0;
        loop {
            let paths: Vec<Bytes> = self
                .view
                .paths
                .range::<[u8], _>((
                    std::ops::Bound::Included(prefix),
                    std::ops::Bound::Unbounded,
                ))
                .take_while(|(path, _)| path.starts_with(prefix))
                .take(256)
                .map(|(path, _)| path.clone())
                .collect();
            if paths.is_empty() {
                break;
            }
            for path in paths {
                removed += usize::from(self.delete_path(&path));
            }
        }
        removed
    }

    /// Walk each query byte at most once, then yield deepest matches first.
    pub fn prefix_matches(
        &self,
        query: &[u8],
        max_path_len: Option<usize>,
    ) -> PathHashPrefixes<'_> {
        let length = query.len().min(max_path_len.unwrap_or(usize::MAX));
        PathHashPrefixes {
            paths: &self.view.paths,
            ids: self.index.match_ids(&query[..length]),
        }
    }

    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>() + self.view.paths.heap_usage() + self.index.heap_usage()
    }
}

pub struct PathHashPrefixes<'a> {
    paths: &'a OrderedPaths,
    ids: Vec<usize>,
}

impl<'a> Iterator for PathHashPrefixes<'a> {
    type Item = (&'a Bytes, &'a PathHashPayload);

    fn next(&mut self) -> Option<Self::Item> {
        let entry = &self.paths.entries[self.ids.pop()?];
        Some((&entry.path, &entry.payload))
    }
}

#[cfg(test)]
mod tests;
