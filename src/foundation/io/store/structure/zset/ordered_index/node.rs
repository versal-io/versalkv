use super::OrderedIndexMemory;
use bytes::Bytes;
use std::cmp::Ordering;
use std::mem::size_of;

pub(super) const MAX_ITEMS: usize = 32;
pub(super) const MIN_ITEMS: usize = MAX_ITEMS / 2;

#[derive(Clone, Debug, PartialEq)]
pub(super) struct Entry {
    pub score: f64,
    pub member: Bytes,
}

impl Entry {
    pub fn compare(&self, score: f64, member: &[u8]) -> Ordering {
        self.score
            .total_cmp(&score)
            .then_with(|| self.member.as_ref().cmp(member))
    }

    pub fn to_pair(&self) -> (Bytes, f64) {
        (self.member.clone(), self.score)
    }
}

#[derive(Clone)]
pub(super) struct Node {
    pub count: usize,
    pub minimum: Option<Entry>,
    pub data: NodeData,
}

#[derive(Clone)]
pub(super) enum NodeData {
    Leaf(Vec<Entry>),
    Branch {
        children: Vec<Node>,
        /// Separator i is the cached minimum of child i + 1.
        separators: Vec<Entry>,
    },
}

impl Node {
    pub fn empty() -> Self {
        Self::leaf(Vec::new())
    }

    fn leaf(entries: Vec<Entry>) -> Self {
        Self {
            count: entries.len(),
            minimum: entries.first().cloned(),
            data: NodeData::Leaf(entries),
        }
    }

    pub fn branch(children: Vec<Node>) -> Self {
        let mut node = Self {
            count: 0,
            minimum: None,
            data: NodeData::Branch {
                children,
                separators: Vec::new(),
            },
        };
        node.refresh();
        node
    }

    pub fn occupancy(&self) -> usize {
        match &self.data {
            NodeData::Leaf(entries) => entries.len(),
            NodeData::Branch { children, .. } => children.len(),
        }
    }

    fn refresh(&mut self) {
        match &mut self.data {
            NodeData::Leaf(entries) => {
                self.count = entries.len();
                self.minimum = entries.first().cloned();
            }
            NodeData::Branch {
                children,
                separators,
            } => {
                self.count = children.iter().map(|child| child.count).sum();
                self.minimum = children.first().and_then(|child| child.minimum.clone());
                separators.clear();
                separators.extend(
                    children
                        .iter()
                        .skip(1)
                        .filter_map(|child| child.minimum.clone()),
                );
                debug_assert_eq!(separators.len() + 1, children.len());
            }
        }
    }

    pub fn insert(&mut self, entry: Entry) -> Option<Self> {
        match &mut self.data {
            NodeData::Leaf(entries) => {
                let position =
                    match entries.binary_search_by(|key| key.compare(entry.score, &entry.member)) {
                        Ok(_) => return None,
                        Err(position) => position,
                    };
                entries.insert(position, entry);
            }
            NodeData::Branch {
                children,
                separators,
            } => {
                let position = separators
                    .partition_point(|key| key.compare(entry.score, &entry.member).is_le());
                if let Some(right) = children[position].insert(entry) {
                    children.insert(position + 1, right);
                }
            }
        }
        self.refresh();
        if self.occupancy() <= MAX_ITEMS {
            return None;
        }
        let midpoint = self.occupancy() / 2;
        let right = match &mut self.data {
            NodeData::Leaf(entries) => Self::leaf(entries.split_off(midpoint)),
            NodeData::Branch { children, .. } => Self::branch(children.split_off(midpoint)),
        };
        self.refresh();
        Some(right)
    }

    pub fn delete(&mut self, score: f64, member: &[u8]) -> bool {
        match &mut self.data {
            NodeData::Leaf(entries) => {
                let Ok(position) = entries.binary_search_by(|key| key.compare(score, member))
                else {
                    return false;
                };
                entries.remove(position);
            }
            NodeData::Branch {
                children,
                separators,
            } => {
                let position = separators.partition_point(|key| key.compare(score, member).is_le());
                if !children[position].delete(score, member) {
                    return false;
                }
                if children[position].occupancy() < MIN_ITEMS {
                    Self::rebalance(children, position);
                }
            }
        }
        self.refresh();
        true
    }

    fn rebalance(children: &mut Vec<Self>, position: usize) {
        if position > 0 && children[position - 1].occupancy() > MIN_ITEMS {
            let (left, right) = children.split_at_mut(position);
            let donor = &mut left[position - 1];
            let receiver = &mut right[0];
            match (&mut donor.data, &mut receiver.data) {
                (NodeData::Leaf(source), NodeData::Leaf(target)) => {
                    if let Some(entry) = source.pop() {
                        target.insert(0, entry);
                    }
                }
                (
                    NodeData::Branch {
                        children: source, ..
                    },
                    NodeData::Branch {
                        children: target, ..
                    },
                ) => {
                    if let Some(child) = source.pop() {
                        target.insert(0, child);
                    }
                }
                _ => unreachable!("Sibling nodes always have equal height"),
            }
            donor.refresh();
            receiver.refresh();
        } else if position + 1 < children.len() && children[position + 1].occupancy() > MIN_ITEMS {
            let (left, right) = children.split_at_mut(position + 1);
            let receiver = &mut left[position];
            let donor = &mut right[0];
            match (&mut receiver.data, &mut donor.data) {
                (NodeData::Leaf(target), NodeData::Leaf(source)) => target.push(source.remove(0)),
                (
                    NodeData::Branch {
                        children: target, ..
                    },
                    NodeData::Branch {
                        children: source, ..
                    },
                ) => {
                    target.push(source.remove(0));
                }
                _ => unreachable!("Sibling nodes always have equal height"),
            }
            donor.refresh();
            receiver.refresh();
        } else if children.len() > 1 {
            let left = position.saturating_sub(1);
            let right = children.remove(left + 1);
            children[left].merge(right);
        }
    }

    fn merge(&mut self, right: Self) {
        match (&mut self.data, right.data) {
            (NodeData::Leaf(left), NodeData::Leaf(mut right)) => left.append(&mut right),
            (
                NodeData::Branch { children: left, .. },
                NodeData::Branch {
                    children: mut right,
                    ..
                },
            ) => {
                left.append(&mut right);
            }
            _ => unreachable!("Sibling nodes always have equal height"),
        }
        self.refresh();
    }

    pub fn measure_memory(&self, memory: &mut OrderedIndexMemory) {
        match &self.data {
            NodeData::Leaf(entries) => {
                memory.leaf_nodes += 1;
                memory.entry_capacity += entries.capacity();
                memory.heap_bytes += entries.capacity() * size_of::<Entry>();
            }
            NodeData::Branch {
                children,
                separators,
            } => {
                memory.branch_nodes += 1;
                memory.child_capacity += children.capacity();
                memory.separator_capacity += separators.capacity();
                memory.heap_bytes += children.capacity() * size_of::<Self>();
                memory.heap_bytes += separators.capacity() * size_of::<Entry>();
                for child in children {
                    child.measure_memory(memory);
                }
            }
        }
    }
}
