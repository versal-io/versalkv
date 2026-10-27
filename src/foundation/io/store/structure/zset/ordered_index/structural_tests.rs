use super::{
    node::{Entry, Node, NodeData, MAX_ITEMS, MIN_ITEMS},
    OrderedIndex, OrderedIndexMemory,
};
use bytes::Bytes;
use std::mem::size_of;

struct Summary<'a> {
    first: Option<&'a Entry>,
    last: Option<&'a Entry>,
    count: usize,
    height: usize,
}

fn is_before(left: &Entry, right: &Entry) -> bool {
    left.score < right.score || (left.score == right.score && left.member < right.member)
}

fn inspect(node: &Node, is_root: bool) -> Summary<'_> {
    assert!(node.occupancy() <= MAX_ITEMS);
    if !is_root {
        assert!(node.occupancy() >= MIN_ITEMS);
    }
    let summary = match &node.data {
        NodeData::Leaf(entries) => {
            for entry in entries {
                assert!(!entry.score.is_nan());
                if entry.score == 0.0 {
                    assert_eq!(entry.score.to_bits(), 0);
                }
            }
            for pair in entries.windows(2) {
                assert!(is_before(&pair[0], &pair[1]));
            }
            Summary {
                first: entries.first(),
                last: entries.last(),
                count: entries.len(),
                height: 0,
            }
        }
        NodeData::Branch {
            children,
            separators,
        } => {
            assert!(children.len() >= 2);
            assert_eq!(separators.len() + 1, children.len());
            let summaries: Vec<_> = children.iter().map(|child| inspect(child, false)).collect();
            let height = summaries[0].height;
            for (position, child) in summaries.iter().enumerate() {
                assert_eq!(child.height, height);
                assert!(child.first.is_some());
                if position > 0 {
                    assert_eq!(Some(&separators[position - 1]), child.first);
                    assert!(is_before(
                        summaries[position - 1].last.unwrap(),
                        child.first.unwrap()
                    ));
                }
            }
            Summary {
                first: summaries[0].first,
                last: summaries.last().unwrap().last,
                count: summaries.iter().map(|child| child.count).sum(),
                height: height + 1,
            }
        }
    };
    assert_eq!(node.count, summary.count);
    assert_eq!(node.minimum.as_ref(), summary.first);
    summary
}

fn inspect_memory(index: &OrderedIndex) {
    let mut expected = OrderedIndexMemory {
        struct_overhead: size_of::<OrderedIndex>(),
        ..OrderedIndexMemory::default()
    };
    let mut allocations = Vec::new();
    let mut nodes = vec![&index.root];
    while let Some(node) = nodes.pop() {
        match &node.data {
            NodeData::Leaf(entries) => {
                expected.leaf_nodes += 1;
                expected.entry_capacity += entries.capacity();
                allocations.push((entries.capacity(), size_of::<Entry>()));
            }
            NodeData::Branch {
                children,
                separators,
            } => {
                expected.branch_nodes += 1;
                expected.child_capacity += children.capacity();
                expected.separator_capacity += separators.capacity();
                allocations.push((children.capacity(), size_of::<Node>()));
                allocations.push((separators.capacity(), size_of::<Entry>()));
                nodes.extend(children);
            }
        }
    }
    expected.heap_bytes = allocations
        .iter()
        .map(|(capacity, size)| capacity * size)
        .sum();
    assert_eq!(index.memory_layout(), expected);
    assert_eq!(index.memory_usage(), expected.heap_bytes);
}

pub(super) fn assert_valid(index: &OrderedIndex) -> usize {
    let summary = inspect(&index.root, true);
    assert_eq!(index.len(), summary.count);
    inspect_memory(index);
    summary.height
}

fn member(id: usize) -> Bytes {
    Bytes::copy_from_slice(&id.to_be_bytes())
}

fn ascending(count: usize) -> OrderedIndex {
    let mut index = OrderedIndex::new();
    for id in 0..count {
        index.insert(id as f64, member(id));
    }
    assert_valid(&index);
    index
}

fn root_occupancies(index: &OrderedIndex) -> Vec<usize> {
    match &index.root.data {
        NodeData::Branch { children, .. } => children.iter().map(Node::occupancy).collect(),
        NodeData::Leaf(_) => vec![index.len()],
    }
}

#[test]
fn leaf_underflow_borrows_from_the_right_sibling() {
    let mut index = ascending(33);
    assert_eq!(root_occupancies(&index), [16, 17]);
    assert!(index.delete(0.0, &member(0)));
    assert_eq!(root_occupancies(&index), [16, 16]);
    assert_valid(&index);
}

#[test]
fn leaf_underflow_borrows_from_the_left_sibling() {
    let mut index = ascending(34);
    index.insert(-1.0, Bytes::from_static(b"left"));
    assert_eq!(root_occupancies(&index), [17, 18]);
    for id in [33, 32, 31] {
        assert!(index.delete(id as f64, &member(id)));
        assert_valid(&index);
    }
    assert_eq!(root_occupancies(&index), [16, 16]);
}

#[test]
fn merging_two_leaves_collapses_the_root() {
    let mut index = ascending(33);
    assert!(index.delete(32.0, &member(32)));
    assert_eq!(root_occupancies(&index), [16, 16]);
    assert!(index.delete(0.0, &member(0)));
    assert_eq!(assert_valid(&index), 0);
    assert_eq!(index.get_element_by_rank(0), Some((member(1), 1.0)));
    assert_eq!(index.len(), 31);
}

#[test]
fn internal_splits_rebalancing_and_repeated_root_shrink_preserve_all_invariants() {
    let mut index = ascending(9000);
    let mut maximum_height = assert_valid(&index);
    assert!(maximum_height >= 3);
    let mut ids: Vec<_> = (0..9000).collect();
    let mut state = 42u64;
    for end in (1..ids.len()).rev() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        ids.swap(end, (state as usize) % (end + 1));
    }
    for (position, id) in ids.into_iter().enumerate() {
        assert!(index.delete(id as f64, &member(id)));
        assert_eq!(index.len(), 8999 - position);
        let height = assert_valid(&index);
        assert!(height <= maximum_height);
        maximum_height = height;
    }
    assert_eq!(index.memory_usage(), 0);
    for id in (0..4096).rev() {
        index.insert(id as f64, member(id));
        assert_valid(&index);
    }
    assert!(assert_valid(&index) >= 2);
}

#[test]
fn equal_scores_span_many_leaves_and_keep_score_bound_ranks_correct() {
    let mut index = OrderedIndex::new();
    for id in (0..4096).rev() {
        index.insert(if id % 2 == 0 { -0.0 } else { 0.0 }, member(id));
    }
    assert!(assert_valid(&index) >= 2);
    assert_eq!(index.count_in_score_range((-0.0, true), (0.0, true)), 4096);
    for bounds in [((-0.0, false), (0.0, true)), ((-0.0, true), (0.0, false))] {
        assert_eq!(index.count_in_score_range(bounds.0, bounds.1), 0);
    }
    for is_reverse in [false, true] {
        let mut expected: Vec<_> = (0..4096).map(|id| (member(id), 0.0)).collect();
        if is_reverse {
            expected.reverse();
        }
        assert_eq!(
            index.range_by_score((0.0, true), (0.0, true), 1024, 2048, is_reverse),
            expected[1024..3072]
        );
    }
    for id in 0..4096 {
        assert_eq!(index.get_rank(-0.0, &member(id)), Some(id));
        assert_eq!(index.get_element_by_rank(id), Some((member(id), 0.0)));
    }
}

#[test]
fn clone_equality_is_independent_of_tree_shape_and_vector_capacity() {
    let mut index = ascending(2048);
    for id in (0..2048).step_by(3) {
        assert!(index.delete(id as f64, &member(id)));
    }
    let clone = index.clone();
    let mut rebuilt = OrderedIndex::new();
    for (member, score) in clone.sorted_vec().into_iter().rev() {
        rebuilt.insert(score, member);
    }
    assert_eq!(index, clone);
    assert_eq!(index, rebuilt);
    assert_valid(&clone);
    assert_valid(&rebuilt);
}

#[test]
fn public_iterator_is_exact_sized_fused_and_automatically_send_sync() {
    fn require_traits<T: Send + Sync + Clone + Eq + Default + std::fmt::Debug>() {}
    require_traits::<OrderedIndex>();
    let index = ascending(2048);
    let mut iter = index.iter();
    for id in 0..2048 {
        assert_eq!(iter.len(), 2048 - id);
        let (bytes, score) = iter.next().unwrap();
        assert_eq!(bytes, &member(id));
        assert_eq!(*score, id as f64);
    }
    assert_eq!(iter.size_hint(), (0, Some(0)));
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next(), None);
    assert_eq!(OrderedIndex::default().iter().next(), None);
}
