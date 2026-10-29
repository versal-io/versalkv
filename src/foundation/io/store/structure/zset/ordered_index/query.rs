use super::node::{Entry, Node, NodeData};

impl Node {
    pub fn get_rank(&self, score: f64, member: &[u8]) -> Option<usize> {
        match &self.data {
            NodeData::Leaf(entries) => entries
                .binary_search_by(|key| key.compare(score, member))
                .ok(),
            NodeData::Branch {
                children,
                separators,
            } => {
                let position = separators.partition_point(|key| key.compare(score, member).is_le());
                let prefix: usize = children[..position].iter().map(|child| child.count).sum();
                children[position]
                    .get_rank(score, member)
                    .map(|rank| prefix + rank)
            }
        }
    }

    pub fn entry_by_rank(&self, mut rank: usize) -> Option<&Entry> {
        if rank >= self.count {
            return None;
        }
        match &self.data {
            NodeData::Leaf(entries) => entries.get(rank),
            NodeData::Branch { children, .. } => {
                for child in children {
                    if rank < child.count {
                        return child.entry_by_rank(rank);
                    }
                    rank -= child.count;
                }
                None
            }
        }
    }

    /// Count keys with scores below the bound, optionally including equality.
    pub fn rank_before_score(&self, score: f64, include_equal: bool) -> usize {
        let is_before = |key: &Entry| key.score < score || (include_equal && key.score == score);
        match &self.data {
            NodeData::Leaf(entries) => entries.partition_point(is_before),
            NodeData::Branch {
                children,
                separators,
            } => {
                let position = separators.partition_point(is_before);
                let prefix: usize = children[..position].iter().map(|child| child.count).sum();
                prefix + children[position].rank_before_score(score, include_equal)
            }
        }
    }

    /// Only the two boundary paths are searched; covered subtrees are walked.
    pub fn collect_ranks(
        &self,
        start: usize,
        end: usize,
        is_reverse: bool,
        output: &mut Vec<(bytes::Bytes, f64)>,
    ) {
        if start >= end {
            return;
        }
        match &self.data {
            NodeData::Leaf(entries) => {
                let slice = &entries[start..end];
                if is_reverse {
                    output.extend(slice.iter().rev().map(Entry::to_pair));
                } else {
                    output.extend(slice.iter().map(Entry::to_pair));
                }
            }
            NodeData::Branch { children, .. } => {
                let mut prefix = if is_reverse { self.count } else { 0 };
                let visit = |child: &Node, prefix: usize, output: &mut Vec<_>| {
                    if start < prefix + child.count && end > prefix {
                        child.collect_ranks(
                            start.saturating_sub(prefix),
                            (end - prefix).min(child.count),
                            is_reverse,
                            output,
                        );
                    }
                };
                if is_reverse {
                    for child in children.iter().rev() {
                        prefix -= child.count;
                        visit(child, prefix, output);
                        if prefix <= start {
                            break;
                        }
                    }
                } else {
                    for child in children {
                        visit(child, prefix, output);
                        prefix += child.count;
                        if prefix >= end {
                            break;
                        }
                    }
                }
            }
        }
    }
}
