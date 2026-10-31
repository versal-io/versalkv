use slab::Slab;

#[derive(Debug, Default)]
struct Node {
    label: Vec<u8>,
    terminal: Option<usize>,
    children: Vec<(u8, usize)>,
}

impl Clone for Node {
    fn clone(&self) -> Self {
        let mut label = Vec::with_capacity(self.label.capacity());
        label.extend_from_slice(&self.label);
        let mut children = Vec::with_capacity(self.children.capacity());
        children.extend_from_slice(&self.children);
        Self {
            label,
            terminal: self.terminal,
            children,
        }
    }
}

impl Node {
    fn heap_usage(&self) -> usize {
        self.label.capacity() + self.children.capacity() * std::mem::size_of::<(u8, usize)>()
    }

    fn find_child(&self, byte: u8) -> Result<usize, usize> {
        self.children.binary_search_by_key(&byte, |(byte, _)| *byte)
    }
}

/// Compressed edges and flat node ownership bound node count by path count,
/// rather than byte count. Clone, removal, and drop never recurse.
#[derive(Debug, Default)]
pub(super) struct PrefixIndex {
    nodes: Slab<Node>,
    tracked_bytes: usize,
}

impl Clone for PrefixIndex {
    fn clone(&self) -> Self {
        let mut nodes = self.nodes.clone();
        nodes.reserve_exact(self.nodes.capacity() - nodes.len());
        Self {
            nodes,
            tracked_bytes: self.tracked_bytes,
        }
    }
}

impl PrefixIndex {
    fn insert_node(&mut self, node: Node) -> usize {
        self.tracked_bytes += node.heap_usage();
        self.nodes.insert(node)
    }

    fn remove_node(&mut self, id: usize) -> Node {
        let node = self.nodes.remove(id);
        self.tracked_bytes -= node.heap_usage();
        node
    }

    fn edit_node<T>(&mut self, id: usize, operation: impl FnOnce(&mut Node) -> T) -> T {
        let node = &mut self.nodes[id];
        self.tracked_bytes -= node.heap_usage();
        let result = operation(node);
        self.tracked_bytes += node.heap_usage();
        result
    }

    fn add_child(&mut self, parent: usize, byte: u8, child: usize) {
        self.edit_node(parent, |node| {
            let position = node.find_child(byte).unwrap_or_else(|position| position);
            node.children.insert(position, (byte, child));
        });
    }

    pub(super) fn insert(&mut self, path: &[u8], terminal: usize) {
        if self.nodes.is_empty() {
            self.insert_node(Node::default());
        }
        let mut parent = 0;
        let mut offset = 0;
        loop {
            if offset == path.len() {
                self.nodes[parent].terminal = Some(terminal);
                return;
            }
            let position = match self.nodes[parent].find_child(path[offset]) {
                Ok(position) => position,
                Err(_) => {
                    let child = self.insert_node(Node {
                        label: path[offset..].to_vec(),
                        terminal: Some(terminal),
                        children: Vec::new(),
                    });
                    self.add_child(parent, path[offset], child);
                    return;
                }
            };
            let child = self.nodes[parent].children[position].1;
            let label = &self.nodes[child].label;
            let common = label
                .iter()
                .zip(&path[offset..])
                .take_while(|(left, right)| left == right)
                .count();
            offset += common;
            if common == label.len() {
                parent = child;
                continue;
            }
            // Split only at a branching point or at a newly inserted terminal.
            let suffix = self.edit_node(child, |node| {
                let label = node.label.split_off(common);
                node.label.shrink_to_fit();
                Node {
                    label,
                    terminal: node.terminal.take(),
                    children: std::mem::take(&mut node.children),
                }
            });
            let byte = suffix.label[0];
            let suffix_id = self.insert_node(suffix);
            self.add_child(child, byte, suffix_id);
            parent = child;
        }
    }

    pub(super) fn remove(&mut self, path: &[u8]) {
        let mut current = 0;
        let mut offset = 0;
        let mut ancestors = Vec::new();
        while offset < path.len() {
            let Ok(position) = self.nodes[current].find_child(path[offset]) else {
                debug_assert!(false, "missing indexed path");
                return;
            };
            let child = self.nodes[current].children[position].1;
            let label = &self.nodes[child].label;
            if !path[offset..].starts_with(label) {
                debug_assert!(false, "mismatched indexed path");
                return;
            }
            offset += label.len();
            ancestors.push((current, position));
            current = child;
        }
        debug_assert!(self.nodes[current].terminal.is_some());
        self.nodes[current].terminal = None;
        while let Some((parent, position)) = ancestors.pop() {
            let node = &self.nodes[current];
            if node.terminal.is_some() || node.children.len() > 1 {
                break;
            }
            if let Some(&(_, child)) = node.children.first() {
                let child = self.remove_node(child);
                self.edit_node(current, |node| {
                    node.label.extend_from_slice(&child.label);
                    node.terminal = child.terminal;
                    node.children = child.children;
                });
                break;
            }
            self.remove_node(current);
            self.edit_node(parent, |node| {
                node.children.remove(position);
            });
            current = parent;
        }
    }

    pub(super) fn match_ids(&self, query: &[u8]) -> Vec<usize> {
        let mut matches = Vec::new();
        if self.nodes.is_empty() {
            return matches;
        }
        let mut current = 0;
        let mut offset = 0;
        loop {
            let node = &self.nodes[current];
            if let Some(terminal) = node.terminal {
                matches.push(terminal);
            }
            if offset == query.len() {
                break;
            }
            let Ok(position) = node.find_child(query[offset]) else {
                break;
            };
            let child = node.children[position].1;
            let label = &self.nodes[child].label;
            if !query[offset..].starts_with(label) {
                break;
            }
            offset += label.len();
            current = child;
        }
        matches
    }

    pub(super) fn heap_usage(&self) -> usize {
        self.tracked_bytes
            + self.nodes.capacity() * (std::mem::size_of::<Node>() + std::mem::size_of::<usize>())
    }

    #[cfg(test)]
    pub(super) fn assert_structure(&self, path_count: usize) {
        assert_eq!(
            self.tracked_bytes,
            self.nodes
                .iter()
                .map(|(_, node)| node.heap_usage())
                .sum::<usize>()
        );
        assert!(self.nodes.len() <= path_count * 2 + 1);
        if self.nodes.is_empty() {
            assert_eq!(path_count, 0);
            return;
        }
        let mut pending = vec![0];
        let mut visited = std::collections::BTreeSet::new();
        let mut terminals = std::collections::BTreeSet::new();
        while let Some(id) = pending.pop() {
            assert!(visited.insert(id));
            let node = &self.nodes[id];
            if id == 0 {
                assert!(node.label.is_empty());
            } else {
                assert!(!node.label.is_empty());
                assert!(node.terminal.is_some() || node.children.len() >= 2);
            }
            if let Some(terminal) = node.terminal {
                assert!(terminals.insert(terminal));
            }
            assert!(node.children.windows(2).all(|pair| pair[0].0 < pair[1].0));
            for &(byte, child) in &node.children {
                assert_eq!(self.nodes[child].label.first(), Some(&byte));
                pending.push(child);
            }
        }
        assert_eq!(visited.len(), self.nodes.len());
        assert_eq!(terminals.len(), path_count);
    }
}
