use super::leaf::{LeafScanner, ARRAY_SIZE};
use super::Leaf;
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use std::cmp::Ordering;
use std::fmt::Display;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

pub enum Error<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// Duplicated key found: returns the given key-value pair.
    Duplicated((K, V)),
    /// Full: the tree, node, or leaf could not accommodate the entry
    Full((K, V)),
    /// Retry: return the given key-value pair.
    Retry((K, V)),
}

/// Intermediate split node
///
/// It does not own the children, thus only nullifying pointers when drop.
struct NewNodes<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// None: unbounded node
    origin_node_key: Option<K>,
    origin_node_ptr: Atomic<Node<K, V>>,
    low_key_node_anchor: Atomic<LeafNodeAnchor<K, V>>,
    low_key_node: Option<Box<Node<K, V>>>,
    middle_key: Option<K>,
    high_key_node: Option<Box<Node<K, V>>>,
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Drop for NewNodes<K, V> {
    fn drop(&mut self) {
        self.low_key_node.take().map(|node| node.unlink(true));
        self.high_key_node.take().map(|node| node.unlink(false));
    }
}

/// Intermediate split leaf.
///
/// It owns all the instances, thus deallocating all when drop.
struct NewLeaves<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    origin_leaf_key: Option<K>,
    origin_leaf_ptr: Atomic<Leaf<K, V>>,
    low_key_leaf: Option<Box<Leaf<K, V>>>,
    high_key_leaf: Option<Box<Leaf<K, V>>>,
}

/// Minimum leaf node anchor for Scanner.
struct LeafNodeAnchor<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    min_leaf_node: Atomic<Node<K, V>>,
}

/// Node types.
enum NodeType<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// InternalNode: |ptr(children)/max(child keys)|...|ptr(children)|
    InternalNode {
        children: (Leaf<K, Atomic<Node<K, V>>>, Atomic<Node<K, V>>),
        new_children: Atomic<NewNodes<K, V>>,
        leaf_node_anchor: Atomic<LeafNodeAnchor<K, V>>,
        floor: usize,
    },
    /// LeafNode: |ptr(entry array)/max(child keys)|...|ptr(entry array)|
    LeafNode {
        leaves: (Leaf<K, Atomic<Leaf<K, V>>>, Atomic<Leaf<K, V>>),
        new_leaves: Atomic<NewLeaves<K, V>>,
        next_node_anchor: Atomic<LeafNodeAnchor<K, V>>,
        side_link: Atomic<Node<K, V>>,
    },
}

/// Node type.
pub struct Node<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    entry: NodeType<K, V>,
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Node<K, V> {
    pub fn new(floor: usize) -> Node<K, V> {
        Node {
            entry: if floor > 0 {
                NodeType::InternalNode {
                    children: (Leaf::new(), Atomic::null()),
                    new_children: Atomic::null(),
                    leaf_node_anchor: Atomic::null(),
                    floor,
                }
            } else {
                NodeType::LeafNode {
                    leaves: (Leaf::new(), Atomic::null()),
                    new_leaves: Atomic::null(),
                    next_node_anchor: Atomic::null(),
                    side_link: Atomic::null(),
                }
            },
        }
    }

    /// Searches for the given key.
    pub fn search<'a>(&'a self, key: &K, guard: &'a Guard) -> Option<&'a V> {
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children: _,
                leaf_node_anchor: _,
                floor: _,
            } => loop {
                let unbounded_node = (children.1).load(Acquire, guard);
                let result = (children.0).min_greater_equal(&key);
                if let Some((_, child)) = result.0 {
                    let child_node = child.load(Acquire, guard);
                    if !(children.0).validate(result.1) {
                        // after reading the pointer, need to validate the version
                        continue;
                    }
                    return unsafe { child_node.deref().search(key, guard) };
                }
                if unbounded_node == children.1.load(Acquire, guard) {
                    if !(children.0).validate(result.1) {
                        // after reading the pointer, need to validate the version
                        continue;
                    }
                    if unbounded_node.is_null() {
                        return None;
                    }
                    return unsafe { unbounded_node.deref().search(key, guard) };
                }
            },
            NodeType::LeafNode {
                leaves,
                new_leaves: _,
                next_node_anchor: _,
                side_link: _,
            } => {
                loop {
                    let unbounded_leaf = (leaves.1).load(Relaxed, guard);
                    let result = (leaves.0).min_greater_equal(&key);
                    if let Some((_, child)) = result.0 {
                        let child_leaf = child.load(Acquire, guard);
                        if !(leaves.0).validate(result.1) {
                            // after reading the pointer, need to validate the version
                            continue;
                        }
                        return unsafe { child_leaf.deref().search(key) };
                    } else if unbounded_leaf == leaves.1.load(Acquire, guard) {
                        if !(leaves.0).validate(result.1) {
                            // after reading the pointer, need to validate the version
                            continue;
                        }
                        if unbounded_leaf.is_null() {
                            return None;
                        }
                        return unsafe { unbounded_leaf.deref().search(key) };
                    }
                }
            }
        }
    }

    /// Returns the minimum key-value pair.
    pub fn min<'a>(&'a self, guard: &'a Guard) -> Option<LeafNodeScanner<'a, K, V>> {
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children: _,
                leaf_node_anchor: _,
                floor: _,
            } => loop {
                let mut scanner = LeafScanner::new(&children.0);
                if let Some(child) = scanner.next() {
                    let child_node = child.1.load(Acquire, guard);
                    return unsafe { child_node.deref().min(guard) };
                }
                let unbounded_node = (children.1).load(Acquire, guard);
                if !unbounded_node.is_null() {
                    return unsafe { unbounded_node.deref().min(guard) };
                }
            },
            NodeType::LeafNode {
                leaves: _,
                new_leaves: _,
                next_node_anchor: _,
                side_link: _,
            } => {
                let mut scanner = LeafNodeScanner::new(self, guard);
                if let Some(_) = scanner.next() {
                    return Some(scanner);
                }
                None
            }
        }
    }

    /// Inserts a key-value pair.
    ///
    /// It is a recursive call, and therefore stack-overflow may occur.
    pub fn insert(&self, key: K, value: V, guard: &Guard) -> Result<(), Error<K, V>> {
        // possible data race: the node is being split, for instance,
        //  - node state: ((15, ptr), (25, ptr)), 15 is being split
        //  - insert 10: min_greater_equal returns (15, ptr)
        //  - split 15: insert 11, and replace 15 with a new pointer, therefore ((11, ptr), (15, new_ptr), (25, ptr))
        //  - insert 10: load new_ptr, and try insert, that is incorrect as it is supposed to be inserted into (11, ptr)
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children,
                leaf_node_anchor,
                floor: _,
            } => {
                loop {
                    let unbounded_child = children.1.load(Relaxed, guard);
                    let result = (children.0).min_greater_equal(&key);
                    if let Some((child_key, child)) = result.0 {
                        let child_node = child.load(Acquire, guard);
                        if !(children.0).validate(result.1) {
                            // after reading the pointer, need to validate the version
                            continue;
                        }
                        match unsafe { child_node.deref().insert(key, value, guard) } {
                            Ok(_) => return Ok(()),
                            Err(err) => match err {
                                Error::Duplicated(_) => return Err(err),
                                Error::Full(entry) => {
                                    return self.split_node(
                                        entry,
                                        Some(child_key.clone()),
                                        child_node,
                                        &child,
                                        &children,
                                        &new_children,
                                        &leaf_node_anchor,
                                        false,
                                        guard,
                                    );
                                }
                                Error::Retry(_) => return Err(err),
                            },
                        }
                    } else if unbounded_child == self.unbounded_node(guard) {
                        if !(children.0).validate(result.1) {
                            // after reading the pointer, need to validate the version
                            continue;
                        }
                        // try to insert into the unbounded child, and try to split the unbounded if it is full
                        match unsafe { unbounded_child.deref().insert(key, value, guard) } {
                            Ok(_) => return Ok(()),
                            Err(err) => match err {
                                Error::Duplicated(_) => return Err(err),
                                Error::Full(entry) => {
                                    return self.split_node(
                                        entry,
                                        None,
                                        unbounded_child,
                                        &(children.1),
                                        &children,
                                        &new_children,
                                        &leaf_node_anchor,
                                        false,
                                        guard,
                                    );
                                }
                                Error::Retry(_) => return Err(err),
                            },
                        };
                    }
                }
            }
            NodeType::LeafNode {
                leaves,
                new_leaves,
                next_node_anchor: _,
                side_link: _,
            } => loop {
                let unbounded_leaf = leaves.1.load(Relaxed, guard);
                let result = (leaves.0).min_greater_equal(&key);
                if let Some((child_key, child)) = result.0 {
                    let child_leaf = child.load(Acquire, guard);
                    if !(leaves.0).validate(result.1) {
                        // after reading the pointer, need to validate the version
                        continue;
                    }
                    return unsafe { child_leaf.deref().insert(key, value, false) }.map_or_else(
                        || Ok(()),
                        |result| {
                            if result.1 {
                                return Err(Error::Duplicated(result.0));
                            }
                            debug_assert!(unsafe { child_leaf.deref().full() });
                            self.split_leaf(
                                result.0,
                                Some(child_key.clone()),
                                child_leaf,
                                &child,
                                leaves,
                                new_leaves,
                                guard,
                            )
                        },
                    );
                } else if unbounded_leaf == self.unbounded_leaf(guard) {
                    if !(leaves.0).validate(result.1) {
                        // after reading the pointer, need to validate the version
                        continue;
                    }
                    // try to insert into the unbounded leaf, and try to split the unbounded if it is full
                    return unsafe { unbounded_leaf.deref().insert(key, value, false) }
                        .map_or_else(
                            || Ok(()),
                            |result| {
                                if result.1 {
                                    return Err(Error::Duplicated(result.0));
                                }
                                debug_assert!(unsafe { unbounded_leaf.deref().full() });
                                self.split_leaf(
                                    result.0,
                                    None,
                                    unbounded_leaf,
                                    &leaves.1,
                                    leaves,
                                    new_leaves,
                                    guard,
                                )
                            },
                        );
                }
            },
        }
    }

    /// Removes the given key.
    pub fn remove<'a>(&'a self, key: &K, guard: &'a Guard) -> bool {
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children: _,
                leaf_node_anchor: _,
                floor: _,
            } => loop {
                let unbounded_node = (children.1).load(Acquire, guard);
                let result = (children.0).min_greater_equal(&key);
                if let Some((_, child)) = result.0 {
                    let child_node = child.load(Acquire, guard);
                    if !(children.0).validate(result.1) {
                        // after reading the pointer, need to validate the version
                        continue;
                    }
                    return unsafe { child_node.deref().remove(key, guard) };
                }
                if unbounded_node == children.1.load(Acquire, guard) {
                    if !(children.0).validate(result.1) {
                        // after reading the pointer, need to validate the version
                        continue;
                    }
                    if unbounded_node.is_null() {
                        return false;
                    }
                    return unsafe { unbounded_node.deref().remove(key, guard) };
                }
            },
            NodeType::LeafNode {
                leaves,
                new_leaves: _,
                next_node_anchor: _,
                side_link: _,
            } => {
                loop {
                    let unbounded_leaf = (leaves.1).load(Relaxed, guard);
                    let result = (leaves.0).min_greater_equal(&key);
                    if let Some((_, child)) = result.0 {
                        let child_leaf = child.load(Acquire, guard);
                        if !(leaves.0).validate(result.1) {
                            // after reading the pointer, need to validate the version
                            continue;
                        }
                        return unsafe { child_leaf.deref().remove(key) };
                    } else if unbounded_leaf == leaves.1.load(Acquire, guard) {
                        if !(leaves.0).validate(result.1) {
                            // after reading the pointer, need to validate the version
                            continue;
                        }
                        if unbounded_leaf.is_null() {
                            return false;
                        }
                        return unsafe { unbounded_leaf.deref().remove(key) };
                    }
                }
            }
        }
    }

    /// Splits the current root node.
    pub fn split_root(&self, entry: (K, V), root_ptr: &Atomic<Node<K, V>>, guard: &Guard) {
        // the fact that the TreeIndex calls this function means that the root is in a split procedure,
        // and the procedure has not been intervened by other threads, thus concluding that the root has stayed the same.
        debug_assert_eq!(
            self as *const Node<K, V>,
            root_ptr.load(Relaxed, guard).as_raw()
        );
        let new_root: Node<K, V> = if let NodeType::InternalNode {
            children: _,
            new_children: _,
            leaf_node_anchor: _,
            floor,
        } = &self.entry
        {
            Node::new(floor + 1)
        } else {
            Node::new(1)
        };
        if let NodeType::InternalNode {
            children,
            new_children,
            leaf_node_anchor,
            floor,
        } = &new_root.entry
        {
            children.1.store(Owned::new(Node::new(floor - 1)), Relaxed);
            if new_root
                .split_node(
                    entry,
                    None,
                    root_ptr.load(Relaxed, guard),
                    root_ptr,
                    &children,
                    &new_children,
                    &leaf_node_anchor,
                    true,
                    guard,
                )
                .is_ok()
            {
                let mut new_nodes = unsafe {
                    new_children
                        .swap(Shared::null(), Release, guard)
                        .into_owned()
                };

                // insert the newly allocated internal nodes into the main array
                if let Some(new_low_key_node) = new_nodes.low_key_node.take() {
                    children.0.insert(
                        new_nodes.middle_key.take().unwrap().clone(),
                        Atomic::from(new_low_key_node),
                        false,
                    );
                }
                if let Some(new_high_key_node) = new_nodes.high_key_node.take() {
                    children.1.store(Owned::from(new_high_key_node), Relaxed);
                }

                debug_assert_eq!(
                    self as *const Node<K, V>,
                    root_ptr.load(Relaxed, guard).as_raw()
                );
                unsafe {
                    guard.defer_destroy(root_ptr.swap(Owned::new(new_root), Release, guard));
                };
            }
        }
    }

    /// Splits a full node.
    fn split_node(
        &self,
        entry: (K, V),
        full_node_key: Option<K>,
        full_node_shared: Shared<Node<K, V>>,
        full_node_ptr: &Atomic<Node<K, V>>,
        self_children: &(Leaf<K, Atomic<Node<K, V>>>, Atomic<Node<K, V>>),
        self_new_children: &Atomic<NewNodes<K, V>>,
        self_min_node_anchor: &Atomic<LeafNodeAnchor<K, V>>,
        root_node_split: bool,
        guard: &Guard,
    ) -> Result<(), Error<K, V>> {
        let mut new_split_nodes;
        match self_new_children.compare_and_set(
            Shared::null(),
            Owned::new(NewNodes {
                origin_node_key: full_node_key,
                origin_node_ptr: full_node_ptr.clone(),
                low_key_node_anchor: Atomic::null(),
                low_key_node: None,
                middle_key: None,
                high_key_node: None,
            }),
            Acquire,
            guard,
        ) {
            Ok(result) => new_split_nodes = result,
            Err(_) => {
                unsafe {
                    full_node_shared.deref().rollback(guard);
                };
                return Err(Error::Retry(entry));
            }
        }

        // check the full node pointer after locking the node
        if full_node_shared != full_node_ptr.load(Relaxed, guard) {
            // overtaken by another thread
            let unused_children = self_new_children.swap(Shared::null(), Relaxed, guard);
            drop(unsafe { unused_children.into_owned() });
            unsafe {
                full_node_shared.deref().rollback(guard);
            };
            return Err(Error::Retry(entry));
        }
        debug_assert!(unsafe { full_node_shared.deref().full(guard) });

        // copy entries to the newly allocated leaves
        let new_split_nodes_ref = unsafe { new_split_nodes.deref_mut() };

        match unsafe { &full_node_shared.deref().entry } {
            NodeType::InternalNode {
                children,
                new_children,
                leaf_node_anchor,
                floor,
            } => {
                debug_assert!(self_min_node_anchor.load(Relaxed, guard).is_null());
                debug_assert!(!new_children.load(Relaxed, guard).is_null());
                let new_children_ref = unsafe { new_children.load(Relaxed, guard).deref_mut() };

                // copy nodes except for the known full node to the newly allocated internal node entries
                let internal_nodes = (Box::new(Node::new(*floor)), Box::new(Node::new(*floor)));
                let low_key_node_anchor = leaf_node_anchor.swap(Shared::null(), Relaxed, guard);
                let low_key_nodes = if let NodeType::InternalNode {
                    children,
                    new_children: _,
                    leaf_node_anchor,
                    floor: _,
                } = &internal_nodes.0.entry
                {
                    // copy the full node's anchor to the
                    // move the full node's anchor to the low key node
                    leaf_node_anchor.swap(low_key_node_anchor.clone(), Relaxed, guard);
                    Some(children)
                } else {
                    None
                };
                let (high_key_nodes, high_key_node_anchor) = if let NodeType::InternalNode {
                    children,
                    new_children: _,
                    leaf_node_anchor,
                    floor,
                } = &internal_nodes.1.entry
                {
                    // the low key node will link the last node to the high key node anchor
                    if *floor == 1 {
                        leaf_node_anchor.store(
                            Owned::new(LeafNodeAnchor {
                                min_leaf_node: Atomic::null(),
                            }),
                            Relaxed,
                        );
                        (Some(children), Some(leaf_node_anchor))
                    } else {
                        (Some(children), None)
                    }
                } else {
                    (None, None)
                };

                // if the origin is an unbounded node, assign the high key node to the high key node's unbounded,
                // otherwise, assign the unbounded node to the high key node's unbounded.
                let array_size = children.0.cardinality();
                let low_key_node_array_size = array_size / 2;
                let high_key_node_array_size = array_size - low_key_node_array_size;
                let mut current_low_key_node_array_size = 0;
                let mut current_high_key_node_array_size = 0;
                let mut entry_to_link_to_anchor = Shared::null();
                let mut scanner = LeafScanner::new(&children.0);
                while let Some(entry) = scanner.next() {
                    let mut entries: [Option<(K, Atomic<Node<K, V>>)>; 2] = [None, None];
                    if new_children_ref
                        .origin_node_key
                        .as_ref()
                        .map_or_else(|| false, |key| entry.0.cmp(key) == Ordering::Equal)
                    {
                        // link state adjustment not required as the linked list is correctly constructed by the remedy_leaf_node_link function
                        if let Some(node) = new_children_ref.low_key_node.take() {
                            entries[0].replace((
                                new_children_ref.middle_key.as_ref().unwrap().clone(),
                                Atomic::from(node),
                            ));
                        }
                        if let Some(node) = new_children_ref.high_key_node.take() {
                            entries[1].replace((entry.0.clone(), Atomic::from(node)));
                        }
                    } else {
                        entries[0].replace((entry.0.clone(), entry.1.clone()));
                    }
                    for entry in entries.iter_mut() {
                        if let Some(entry) = entry.take() {
                            if current_low_key_node_array_size < low_key_node_array_size {
                                low_key_nodes
                                    .as_ref()
                                    .unwrap()
                                    .0
                                    .insert(entry.0, entry.1, false);
                                current_low_key_node_array_size += 1;
                            } else if current_low_key_node_array_size == low_key_node_array_size {
                                new_split_nodes_ref.middle_key.replace(entry.0);
                                let child_node_ptr = entry.1.load(Relaxed, guard);
                                low_key_nodes
                                    .as_ref()
                                    .unwrap()
                                    .1
                                    .store(child_node_ptr.clone(), Relaxed);
                                if high_key_node_anchor.is_some() {
                                    // the entry needs to be linked to the high key node anchor once the anchor is updated
                                    entry_to_link_to_anchor = child_node_ptr;
                                }
                                current_low_key_node_array_size += 1;
                            } else if current_high_key_node_array_size < high_key_node_array_size {
                                if current_high_key_node_array_size == 0 {
                                    // update the anchor
                                    if let Some(anchor) = high_key_node_anchor.as_ref() {
                                        let high_key_node_leaf_anchor = anchor.load(Relaxed, guard);
                                        unsafe {
                                            high_key_node_leaf_anchor
                                                .deref()
                                                .min_leaf_node
                                                .store(entry.1.load(Relaxed, guard), Relaxed);
                                        }
                                    }
                                }
                                high_key_nodes
                                    .as_ref()
                                    .unwrap()
                                    .0
                                    .insert(entry.0, entry.1, false);
                                current_high_key_node_array_size += 1;
                            } else {
                                high_key_nodes
                                    .as_ref()
                                    .unwrap()
                                    .0
                                    .insert(entry.0, entry.1, false);
                            }
                        }
                    }
                }
                if !new_children_ref.origin_node_key.is_none() {
                    let unbounded_node = children.1.load(Acquire, guard);
                    debug_assert!(!unbounded_node.is_null());
                    high_key_nodes
                        .as_ref()
                        .unwrap()
                        .1
                        .store(unbounded_node, Relaxed);
                } else {
                    if let Some(node) = new_children_ref.low_key_node.take() {
                        high_key_nodes.as_ref().unwrap().0.insert(
                            new_children_ref.middle_key.as_ref().unwrap().clone(),
                            Atomic::from(node),
                            false,
                        );
                    }
                    if let Some(node) = new_children_ref.high_key_node.take() {
                        high_key_nodes
                            .as_ref()
                            .unwrap()
                            .1
                            .store(Owned::from(node), Relaxed);
                    }
                }

                // link the max low key leaf node to the new anchor
                if let Some(anchor) = high_key_node_anchor.as_ref() {
                    unsafe {
                        entry_to_link_to_anchor
                            .deref()
                            .update_next_node_anchor(anchor.load(Relaxed, guard));
                        entry_to_link_to_anchor
                            .deref()
                            .update_side_link(Shared::null());
                    };
                }

                // turn the new nodes into internal nodes
                new_split_nodes_ref
                    .low_key_node_anchor
                    .store(low_key_node_anchor, Relaxed);
                new_split_nodes_ref.low_key_node.replace(internal_nodes.0);
                new_split_nodes_ref.high_key_node.replace(internal_nodes.1);
            }
            NodeType::LeafNode {
                leaves,
                new_leaves,
                next_node_anchor,
                side_link,
            } => {
                debug_assert!(!new_leaves.load(Relaxed, guard).is_null());
                let new_leaves_ref = unsafe { new_leaves.load(Relaxed, guard).deref_mut() };

                // copy leaves except for the known full leaf to the newly allocated leaf node entries
                let leaf_nodes = (Box::new(Node::new(0)), Box::new(Node::new(0)));
                let low_key_leaves = if let NodeType::LeafNode {
                    leaves,
                    new_leaves: _,
                    next_node_anchor: _,
                    side_link: _,
                } = &leaf_nodes.0.entry
                {
                    Some(leaves)
                } else {
                    None
                };
                let high_key_leaves = if let NodeType::LeafNode {
                    leaves,
                    new_leaves: _,
                    next_node_anchor: _,
                    side_link: _,
                } = &leaf_nodes.1.entry
                {
                    Some(leaves)
                } else {
                    None
                };

                // if the origin is an unbounded leaf, assign the high key leaf to the high key node's unbounded,
                // otherwise, assign the unbounded leaf to the high key node's unbounded.
                let array_size = leaves.0.cardinality();
                let low_key_leaf_array_size = array_size / 2;
                let high_key_leaf_array_size = array_size - low_key_leaf_array_size;
                let mut current_low_key_leaf_array_size = 0;
                let mut current_high_key_leaf_array_size = 0;
                let mut scanner = LeafScanner::new(&leaves.0);
                while let Some(entry) = scanner.next() {
                    let mut entries: [Option<(K, Atomic<Leaf<K, V>>)>; 2] = [None, None];
                    if new_leaves_ref
                        .origin_leaf_key
                        .as_ref()
                        .map_or_else(|| false, |key| entry.0.cmp(key) == Ordering::Equal)
                    {
                        if let Some(leaf) = new_leaves_ref.low_key_leaf.take() {
                            entries[0]
                                .replace((leaf.max_key().unwrap().clone(), Atomic::from(leaf)));
                        }
                        if let Some(leaf) = new_leaves_ref.high_key_leaf.take() {
                            entries[1].replace((
                                new_leaves_ref.origin_leaf_key.as_ref().unwrap().clone(),
                                Atomic::from(leaf),
                            ));
                        }
                    } else {
                        entries[0].replace((entry.0.clone(), entry.1.clone()));
                    }
                    for entry in entries.iter_mut() {
                        if let Some(entry) = entry.take() {
                            if current_low_key_leaf_array_size < low_key_leaf_array_size {
                                low_key_leaves
                                    .as_ref()
                                    .unwrap()
                                    .0
                                    .insert(entry.0, entry.1, false);
                                current_low_key_leaf_array_size += 1;
                            } else if current_low_key_leaf_array_size == low_key_leaf_array_size {
                                new_split_nodes_ref.middle_key.replace(entry.0);
                                low_key_leaves
                                    .as_ref()
                                    .unwrap()
                                    .1
                                    .store(entry.1.load(Relaxed, guard), Relaxed);
                                current_low_key_leaf_array_size += 1;
                            } else if current_high_key_leaf_array_size < high_key_leaf_array_size {
                                high_key_leaves
                                    .as_ref()
                                    .unwrap()
                                    .0
                                    .insert(entry.0, entry.1, false);
                                current_high_key_leaf_array_size += 1;
                            } else {
                                high_key_leaves
                                    .as_ref()
                                    .unwrap()
                                    .0
                                    .insert(entry.0, entry.1, false);
                            }
                        }
                    }
                }
                if !new_leaves_ref.origin_leaf_key.is_none() {
                    let unbounded_leaf = leaves.1.load(Acquire, guard);
                    debug_assert!(!unbounded_leaf.is_null());
                    high_key_leaves
                        .as_ref()
                        .unwrap()
                        .1
                        .store(unbounded_leaf, Relaxed);
                } else {
                    if let Some(leaf) = new_leaves_ref.low_key_leaf.take() {
                        high_key_leaves.as_ref().unwrap().0.insert(
                            leaf.max_key().unwrap().clone(),
                            Atomic::from(leaf),
                            false,
                        );
                    }
                    if let Some(leaf) = new_leaves_ref.high_key_leaf.take() {
                        high_key_leaves
                            .as_ref()
                            .unwrap()
                            .1
                            .store(Owned::from(leaf), Relaxed);
                    }
                }

                // reconstruct the linked list
                let immediate_less_key_node = self_children
                    .0
                    .max_less(new_split_nodes_ref.middle_key.as_ref().unwrap());
                self.remedy_leaf_node_link(
                    self_min_node_anchor,
                    immediate_less_key_node,
                    &leaf_nodes.0,
                    &leaf_nodes.1,
                    &next_node_anchor,
                    &side_link,
                    guard,
                );

                // turn the new leaves into leaf nodes
                new_split_nodes_ref.low_key_node.replace(leaf_nodes.0);
                new_split_nodes_ref.high_key_node.replace(leaf_nodes.1);
            }
        };

        // the full node is the current root: split_root processes the rest.
        if root_node_split {
            return Ok(());
        }

        // insert the newly allocated internal nodes into the main array
        let unused_node;
        let low_key_node_ptr = Atomic::from(new_split_nodes_ref.low_key_node.take().unwrap());
        if let Some(node) = self_children.0.insert(
            new_split_nodes_ref.middle_key.take().unwrap(),
            low_key_node_ptr,
            false,
        ) {
            // insertion failed: expect that the parent splits this node
            new_split_nodes_ref
                .low_key_node
                .replace(unsafe { (node.0).1.into_owned().into_box() });
            new_split_nodes_ref.middle_key.replace((node.0).0);
            return Err(Error::Full(entry));
        }

        // replace the full node with the high-key node
        unused_node = full_node_ptr.swap(
            Owned::from(new_split_nodes_ref.high_key_node.take().unwrap()),
            Release,
            &guard,
        );

        // deallocate the deprecated nodes
        let new_split_nodes = self_new_children.swap(Shared::null(), Release, guard);
        unsafe {
            let new_split_nodes = new_split_nodes.into_owned();
            debug_assert_eq!(
                new_split_nodes.origin_node_ptr.load(Relaxed, guard),
                unused_node
            );
            guard.defer_destroy(unused_node);
        };

        Ok(())
    }

    /// Remedy the broken link state
    fn remedy_leaf_node_link(
        &self,
        min_node_anchor: &Atomic<LeafNodeAnchor<K, V>>,
        immediate_less_key_node: Option<(&K, &Atomic<Node<K, V>>)>,
        low_key_leaf_node: &Box<Node<K, V>>,
        high_key_leaf_node: &Box<Node<K, V>>,
        full_node_next_node_anchor: &Atomic<LeafNodeAnchor<K, V>>,
        full_node_side_link: &Atomic<Node<K, V>>,
        guard: &Guard,
    ) {
        // firstly, replace the high key node's side link with the new side link
        // secondly, link the low key node with the high key node
        low_key_leaf_node.update_side_link(
            Atomic::from(&(**high_key_leaf_node) as *const _).load(Relaxed, guard),
        );
        high_key_leaf_node.update_side_link(full_node_side_link.load(Relaxed, guard));
        high_key_leaf_node.update_next_node_anchor(full_node_next_node_anchor.load(Relaxed, guard));

        // lastly, link the immediate less key node with the low key node
        //  - the changes in this step need to be rolled back on operation failure
        if let Some(entry) = immediate_less_key_node {
            let node = entry.1.load(Relaxed, guard);
            unsafe {
                node.deref().update_side_link(
                    Atomic::from(&(**low_key_leaf_node) as *const _).load(Relaxed, guard),
                )
            };
        } else {
            // the low key node is the minimum node
            let anchor = min_node_anchor.load(Relaxed, guard);
            if !anchor.is_null() {
                unsafe {
                    anchor.deref().min_leaf_node.store(
                        Atomic::from(&(**low_key_leaf_node) as *const _).load(Relaxed, guard),
                        Release,
                    )
                };
            }
        }
    }

    /// Splits a full leaf.
    fn split_leaf(
        &self,
        entry: (K, V),
        full_leaf_key: Option<K>,
        full_leaf_shared: Shared<Leaf<K, V>>,
        full_leaf_ptr: &Atomic<Leaf<K, V>>,
        leaves: &(Leaf<K, Atomic<Leaf<K, V>>>, Atomic<Leaf<K, V>>),
        new_leaves: &Atomic<NewLeaves<K, V>>,
        guard: &Guard,
    ) -> Result<(), Error<K, V>> {
        let mut new_leaves_ptr;
        match new_leaves.compare_and_set(
            Shared::null(),
            Owned::new(NewLeaves {
                origin_leaf_key: full_leaf_key,
                origin_leaf_ptr: full_leaf_ptr.clone(),
                low_key_leaf: None,
                high_key_leaf: None,
            }),
            Acquire,
            guard,
        ) {
            Ok(result) => new_leaves_ptr = result,
            Err(_) => return Err(Error::Retry(entry)),
        }

        // check the full leaf pointer after locking the leaf node
        if full_leaf_shared != full_leaf_ptr.load(Relaxed, guard) {
            // overtaken by another thread
            let unused_leaf = new_leaves.swap(Shared::null(), Relaxed, guard);
            drop(unsafe { unused_leaf.into_owned() });
            return Err(Error::Retry(entry));
        }
        debug_assert!(unsafe { full_leaf_shared.deref().full() });

        // copy entries to the newly allocated leaves
        let new_leaves_ref = unsafe { new_leaves_ptr.deref_mut() };
        unsafe {
            full_leaf_shared.deref().distribute(
                &mut new_leaves_ref.low_key_leaf,
                &mut new_leaves_ref.high_key_leaf,
            )
        };

        // insert the given entry
        if new_leaves_ref.low_key_leaf.is_none() {
            new_leaves_ref.low_key_leaf.replace(Box::new(Leaf::new()));
            new_leaves_ref.low_key_leaf.as_ref().unwrap().insert(
                entry.0.clone(),
                entry.1.clone(),
                false,
            );
        } else if new_leaves_ref.high_key_leaf.is_none()
            || new_leaves_ref
                .low_key_leaf
                .as_ref()
                .unwrap()
                .min_greater_equal(&entry.0)
                .0
                .is_some()
        {
            // insert the entry into the low-key leaf if the high-key leaf is empty, or the key fits the low-key leaf
            new_leaves_ref.low_key_leaf.as_ref().unwrap().insert(
                entry.0.clone(),
                entry.1.clone(),
                false,
            );
        } else {
            // insert the entry into the high-key leaf
            new_leaves_ref.high_key_leaf.as_ref().unwrap().insert(
                entry.0.clone(),
                entry.1.clone(),
                false,
            );
        }

        // insert the newly added leaves into the main array
        let unused_leaf;
        if new_leaves_ref.high_key_leaf.is_none() {
            // replace the full leaf with the low-key leaf
            unused_leaf = full_leaf_ptr.swap(
                Owned::from(new_leaves_ref.low_key_leaf.take().unwrap()),
                Release,
                &guard,
            );
        } else {
            let max_key = new_leaves_ref
                .low_key_leaf
                .as_ref()
                .unwrap()
                .max_key()
                .unwrap();
            if let Some(leaf) = leaves.0.insert(
                max_key.clone(),
                Atomic::from(new_leaves_ref.low_key_leaf.take().unwrap()),
                false,
            ) {
                // insertion failed: expect that the parent splits the leaf node
                new_leaves_ref
                    .low_key_leaf
                    .replace(unsafe { (leaf.0).1.into_owned().into_box() });
                return Err(Error::Full(entry));
            }

            // replace the full leaf with the high-key leaf
            unused_leaf = full_leaf_ptr.swap(
                Owned::from(new_leaves_ref.high_key_leaf.take().unwrap()),
                Release,
                &guard,
            );
        }

        // deallocate the deprecated leaves
        let unused_leaves = new_leaves.swap(Shared::null(), Release, guard);
        unsafe {
            let new_split_leaves = unused_leaves.into_owned();
            debug_assert_eq!(
                new_split_leaves.origin_leaf_ptr.load(Relaxed, guard),
                unused_leaf
            );
            debug_assert!(unused_leaf.deref().full());
            guard.defer_destroy(unused_leaf);
        };

        // OK
        return Ok(());
    }

    fn unbounded_node<'a>(&self, guard: &'a Guard) -> Shared<'a, Node<K, V>> {
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children: _,
                leaf_node_anchor: _,
                floor,
            } => {
                let shared_ptr = children.1.load(Relaxed, guard);
                if shared_ptr.is_null() {
                    match children.1.compare_and_set(
                        Shared::null(),
                        Owned::new(Node::new(floor - 1)),
                        Relaxed,
                        guard,
                    ) {
                        Ok(result) => return result,
                        Err(result) => return result.current,
                    }
                }
                shared_ptr
            }
            NodeType::LeafNode {
                leaves: _,
                new_leaves: _,
                next_node_anchor: _,
                side_link: _,
            } => Shared::null(),
        }
    }

    fn unbounded_leaf<'a>(&self, guard: &'a Guard) -> Shared<'a, Leaf<K, V>> {
        match &self.entry {
            NodeType::InternalNode {
                children: _,
                new_children: _,
                leaf_node_anchor: _,
                floor: _,
            } => Shared::null(),
            NodeType::LeafNode {
                leaves,
                new_leaves: _,
                next_node_anchor: _,
                side_link: _,
            } => {
                let shared_ptr = leaves.1.load(Relaxed, guard);
                if shared_ptr.is_null() {
                    match leaves.1.compare_and_set(
                        Shared::null(),
                        Owned::new(Leaf::new()),
                        Relaxed,
                        guard,
                    ) {
                        Ok(result) => return result,
                        Err(result) => return result.current,
                    }
                }
                shared_ptr
            }
        }
    }

    fn update_next_node_anchor(&self, ptr: Shared<LeafNodeAnchor<K, V>>) {
        if let NodeType::LeafNode {
            leaves: _,
            new_leaves: _,
            next_node_anchor,
            side_link: _,
        } = &self.entry
        {
            next_node_anchor.store(ptr, Relaxed);
            return;
        }
        unreachable!()
    }

    fn update_side_link(&self, ptr: Shared<Node<K, V>>) {
        if let NodeType::LeafNode {
            leaves: _,
            new_leaves: _,
            next_node_anchor: _,
            side_link,
        } = &self.entry
        {
            side_link.store(ptr, Relaxed);
            return;
        }
        unreachable!()
    }

    fn next_leaf_node<'a>(&self, guard: &'a Guard) -> Option<&'a Node<K, V>> {
        if let NodeType::LeafNode {
            leaves: _,
            new_leaves: _,
            next_node_anchor,
            side_link,
        } = &self.entry
        {
            let next_node_anchor = next_node_anchor.load(Relaxed, guard);
            if !next_node_anchor.is_null() {
                let next_leaf_node_ptr =
                    unsafe { next_node_anchor.deref().min_leaf_node.load(Acquire, guard) };
                if !next_leaf_node_ptr.is_null() {
                    return Some(unsafe { next_leaf_node_ptr.deref() });
                }
            }
            let side_link_ptr = side_link.load(Acquire, guard);
            if !side_link_ptr.is_null() {
                return Some(unsafe { side_link_ptr.deref() });
            }
        }
        None
    }

    /// Checks if the node is full.
    fn full(&self, guard: &Guard) -> bool {
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children: _,
                leaf_node_anchor: _,
                floor: _,
            } => children.0.full() && !children.1.load(Relaxed, guard).is_null(),
            NodeType::LeafNode {
                leaves,
                new_leaves: _,
                next_node_anchor: _,
                side_link: _,
            } => leaves.0.full() && !leaves.1.load(Relaxed, guard).is_null(),
        }
    }

    /// Rolls back the ongoing split operation recursively.
    fn rollback(&self, guard: &Guard) {
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children,
                leaf_node_anchor,
                floor,
            } => {
                if *floor == 1 {
                    // need to rollback the linked list modification
                    let new_children_ref = unsafe { new_children.load(Relaxed, guard).deref() };
                    let origin_node_ref = unsafe {
                        new_children_ref
                            .origin_node_ptr
                            .load(Relaxed, guard)
                            .deref()
                    };
                    let immediate_less_key_node = children
                        .0
                        .max_less(new_children_ref.middle_key.as_ref().unwrap());
                    if let Some(entry) = immediate_less_key_node {
                        let node = entry.1.load(Relaxed, guard);
                        unsafe {
                            // need to update the side link first as there can be readers who are traversing the linked list
                            node.deref().update_side_link(
                                Atomic::from(origin_node_ref as *const _).load(Relaxed, guard),
                            );
                            node.deref().update_next_node_anchor(Shared::null());
                        };
                    } else {
                        let anchor = leaf_node_anchor.load(Relaxed, guard);
                        if anchor.is_null() {
                            unsafe {
                                anchor.deref().min_leaf_node.store(
                                    Atomic::from(origin_node_ref as *const _).load(Relaxed, guard),
                                    Release,
                                )
                            };
                        }
                    }
                } else if *floor == 2 {
                    let new_children_ref = unsafe { new_children.load(Relaxed, guard).deref() };
                    let origin_node_ref = unsafe {
                        new_children_ref
                            .origin_node_ptr
                            .load(Relaxed, guard)
                            .deref()
                    };
                    // restore the anchor
                    if let NodeType::InternalNode {
                        children: _,
                        new_children: _,
                        leaf_node_anchor,
                        floor,
                    } = &origin_node_ref.entry
                    {
                        debug_assert_eq!(floor, &1);
                        let origin_anchor = new_children_ref.low_key_node_anchor.swap(
                            Shared::null(),
                            Relaxed,
                            guard,
                        );
                        leaf_node_anchor.swap(origin_anchor, Relaxed, guard);
                    }
                }

                let intermediate_split = unsafe {
                    new_children
                        .swap(Shared::null(), Relaxed, guard)
                        .into_owned()
                };
                let child = intermediate_split
                    .origin_node_ptr
                    .swap(Shared::null(), Relaxed, guard);
                unsafe { child.deref().rollback(guard) };
            }
            NodeType::LeafNode {
                leaves: _,
                new_leaves,
                next_node_anchor: _,
                side_link: _,
            } => {
                unsafe { new_leaves.swap(Shared::null(), Release, guard).into_owned() };
            }
        }
    }

    /// Clear all the children for drop the deprecated split nodes.
    fn unlink(&self, reset_anchor: bool) {
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children: _,
                leaf_node_anchor,
                floor: _,
            } => {
                let mut scanner = LeafScanner::new(&children.0);
                while let Some(entry) = scanner.next() {
                    entry.1.store(Shared::null(), Relaxed);
                }
                children.1.store(Shared::null(), Relaxed);
                if reset_anchor {
                    leaf_node_anchor.store(Shared::null(), Relaxed);
                }
            }
            NodeType::LeafNode {
                leaves,
                new_leaves: _,
                next_node_anchor: _,
                side_link: _,
            } => {
                let mut scanner = LeafScanner::new(&leaves.0);
                while let Some(entry) = scanner.next() {
                    entry.1.store(Shared::null(), Relaxed);
                }
                leaves.1.store(Shared::null(), Relaxed);
            }
        }
    }
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Drop for Node<K, V> {
    fn drop(&mut self) {
        let guard = crossbeam_epoch::pin();
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children,
                leaf_node_anchor,
                floor: _,
            } => {
                // destroy entries related to the unused child
                let unused_nodes = new_children.load(Acquire, &guard);
                if !unused_nodes.is_null() {
                    // destroy only the origin node, assuming that the rest are copied
                    debug_assert!(leaf_node_anchor.load(Relaxed, &guard).is_null());
                    let unused_nodes = unsafe { unused_nodes.into_owned() };
                    let obsolete_node = unused_nodes.origin_node_ptr.load(Relaxed, &guard);
                    if !obsolete_node.is_null() {
                        drop(unsafe { obsolete_node.into_owned() });
                    }
                } else {
                    // destroy all: in order to avoid stack overflow, destroy them without the thread pinned
                    let mut scanner = LeafScanner::new(&children.0);
                    while let Some(entry) = scanner.next() {
                        let child = entry.1.load(Acquire, &guard);
                        if !child.is_null() {
                            drop(unsafe { child.into_owned() });
                        }
                    }
                    let unbounded_child = children.1.load(Acquire, &guard);
                    if !unbounded_child.is_null() {
                        drop(unsafe { unbounded_child.into_owned() });
                    }
                    let anchor = leaf_node_anchor.load(Relaxed, &guard);
                    if !anchor.is_null() {
                        drop(unsafe { anchor.into_owned() });
                    }
                }
            }
            NodeType::LeafNode {
                leaves,
                new_leaves,
                next_node_anchor: _,
                side_link: _,
            } => {
                // destroy entries related to the unused child
                let unused_leaves = new_leaves.load(Acquire, &guard);
                if !unused_leaves.is_null() {
                    // destroy only the origin node, assuming that the rest are copied
                    let unused_leaves = unsafe { unused_leaves.into_owned() };
                    let obsolete_leaf = unused_leaves.origin_leaf_ptr.load(Relaxed, &guard);
                    if !obsolete_leaf.is_null() {
                        drop(unsafe { obsolete_leaf.into_owned() });
                    }
                } else {
                    // destroy all
                    let mut scanner = LeafScanner::new(&leaves.0);
                    while let Some(entry) = scanner.next() {
                        let child = entry.1.load(Acquire, &guard);
                        if !child.is_null() {
                            drop(unsafe { child.into_owned() });
                        }
                    }
                    let unbounded_leaf = leaves.1.load(Acquire, &guard);
                    if !unbounded_leaf.is_null() {
                        drop(unsafe { unbounded_leaf.into_owned() });
                    }
                }
            }
        }
    }
}

impl<K: Clone + Display + Ord + Send + Sync, V: Clone + Display + Send + Sync> Node<K, V> {
    fn print(&self) -> usize {
        let mut scanned = 0;
        let guard = crossbeam_epoch::pin();
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children: _,
                leaf_node_anchor: _,
                floor,
            } => {
                let mut index = 0;
                let mut scanner = LeafScanner::new(&children.0);
                while let Some(entry) = scanner.next() {
                    println!("floor: {}, index: {}, node_key: {}", floor, index, entry.0);
                    let child_ref = unsafe { entry.1.load(Relaxed, &guard).deref() };
                    scanned += child_ref.print();
                    index += 1;
                }
                println!("floor {}, unbounded node", floor);
                let unbounded_ptr = children.1.load(Relaxed, &guard);
                if unbounded_ptr.is_null() {
                    println!(" null");
                    return scanned;
                }
                let unbounded_ref = unsafe { unbounded_ptr.deref() };
                scanned += unbounded_ref.print()
            }
            NodeType::LeafNode {
                leaves,
                new_leaves: _,
                next_node_anchor: _,
                side_link: _,
            } => {
                let mut index = 0;
                let mut scanner = LeafScanner::new(&leaves.0);
                while let Some(entry) = scanner.next() {
                    println!("index: {}, leaf_max_key: {}", index, entry.0);
                    let leaf_ref = unsafe { entry.1.load(Relaxed, &guard).deref() };
                    let mut inner_scanner = LeafScanner::new(leaf_ref);
                    while let Some(entry) = inner_scanner.next() {
                        scanned += 1;
                        println!(" entry: {} {}", entry.0, entry.1);
                    }
                    index += 1;
                }
                println!("unbounded node");
                let unbounded_ptr = leaves.1.load(Relaxed, &guard);
                if unbounded_ptr.is_null() {
                    println!(" null");
                    return scanned;
                }
                let unbounded_ref = unsafe { unbounded_ptr.deref() };
                let mut inner_scanner = LeafScanner::new(unbounded_ref);
                while let Some(entry) = inner_scanner.next() {
                    scanned += 1;
                    println!(" entry: {} {}", entry.0, entry.1);
                }
            }
        };
        scanned
    }
}

pub struct LeafNodeScanner<'a, K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    leaf_node: &'a Node<K, V>,
    leaf_pointer_array: [Option<Shared<'a, Leaf<K, V>>>; ARRAY_SIZE + 1],
    current_leaf_index: usize,
    leaf_scanner: Option<LeafScanner<'a, K, V>>,
    guard: &'a Guard,
}

impl<'a, K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> LeafNodeScanner<'a, K, V> {
    fn new(leaf_node: &'a Node<K, V>, guard: &'a Guard) -> LeafNodeScanner<'a, K, V> {
        LeafNodeScanner::<'a, K, V> {
            leaf_node,
            leaf_pointer_array: [None; ARRAY_SIZE + 1],
            current_leaf_index: 0,
            leaf_scanner: None,
            guard,
        }
    }

    pub fn jump(&self, guard: &'a Guard) -> Option<LeafNodeScanner<'a, K, V>> {
        if let Some(next_node) = self.leaf_node.next_leaf_node(guard) {
            Some(LeafNodeScanner::new(next_node, guard))
        } else {
            None
        }
    }

    /// Returns a reference to the entry that the scanner is currently pointing to
    pub fn get(&self) -> Option<(&'a K, &'a V)> {
        if let Some(leaf_scanner) = self.leaf_scanner.as_ref() {
            return leaf_scanner.get();
        }
        None
    }
}

impl<'a, K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Iterator
    for LeafNodeScanner<'a, K, V>
{
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(leaf_scanner) = self.leaf_scanner.as_mut() {
            // leaf iteration
            if let Some(entry) = leaf_scanner.next() {
                return Some(entry);
            }
            // end of iteration
            if self.current_leaf_index == usize::MAX {
                return None;
            }
        }

        if self.current_leaf_index == 0 && self.leaf_scanner.is_none() {
            // start scanning
            match &self.leaf_node.entry {
                NodeType::InternalNode {
                    children: _,
                    new_children: _,
                    leaf_node_anchor: _,
                    floor: _,
                } => return None,
                NodeType::LeafNode {
                    leaves,
                    new_leaves: _,
                    next_node_anchor: _,
                    side_link: _,
                } => loop {
                    // read all the pointers prior to reading the contents
                    let mut index = 0;
                    let mut scanner = LeafScanner::new(&leaves.0);
                    while let Some(entry) = scanner.next() {
                        let ptr = entry.1.load(Relaxed, &self.guard);
                        if !ptr.is_null() {
                            self.leaf_pointer_array[index].replace(ptr);
                            index += 1;
                        }
                    }
                    let ptr = leaves.1.load(Relaxed, &self.guard);
                    if !ptr.is_null() {
                        self.leaf_pointer_array[index].replace(ptr);
                        index += 1;
                    }
                    if leaves.0.validate(scanner.metadata()) {
                        break;
                    }
                    for i in 0..index {
                        self.leaf_pointer_array[i].take();
                    }
                },
            }
        }

        if self.current_leaf_index < self.leaf_pointer_array.len() {
            // proceed to the next leaf
            while let Some(leaf_ptr) = self.leaf_pointer_array[self.current_leaf_index].take() {
                self.leaf_scanner
                    .replace(LeafScanner::new(unsafe { leaf_ptr.deref() }));
                self.current_leaf_index += 1;
                if let Some(leaf_scanner) = self.leaf_scanner.as_mut() {
                    // leaf iteration
                    if let Some(entry) = leaf_scanner.next() {
                        return Some(entry);
                    }
                }
                self.leaf_scanner.take();
                if self.current_leaf_index >= self.leaf_pointer_array.len() {
                    break;
                }
            }
        }
        self.current_leaf_index = usize::MAX;

        // end of iteration
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::treeindex::leaf::ARRAY_SIZE;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;

    #[test]
    fn node() {
        for depth in 0..4 {
            // sequential
            let node = Node::new(depth);
            for key in 0..(ARRAY_SIZE * ARRAY_SIZE * ARRAY_SIZE * ARRAY_SIZE) {
                let guard = crossbeam_epoch::pin();
                match node.insert(key, 10, &guard) {
                    Ok(_) => match node.insert(key, 11, &guard) {
                        Ok(_) => assert!(false),
                        Err(result) => match result {
                            Error::Duplicated(entry) => assert_eq!(entry, (key, 11)),
                            Error::Full(_) => assert!(false),
                            Error::Retry(_) => assert!(false),
                        },
                    },
                    Err(result) => match result {
                        Error::Duplicated(_) => assert!(false),
                        Error::Full(_) => {
                            for key_to_check in 0..key {
                                assert_eq!(node.search(&key_to_check, &guard).unwrap(), &10);
                            }
                            break;
                        }
                        Error::Retry(_) => assert!(false),
                    },
                }
            }
            // non-sequential
            let node = Node::new(depth);
            let mut inserted = Vec::new();
            let mut done = false;
            for i in 0..ARRAY_SIZE {
                for j in 0..ARRAY_SIZE * ARRAY_SIZE {
                    let key = (i + 1) * ARRAY_SIZE * ARRAY_SIZE - j + ARRAY_SIZE * ARRAY_SIZE / 2;
                    let guard = crossbeam_epoch::pin();
                    match node.insert(key, 10, &guard) {
                        Ok(_) => {
                            inserted.push(key);
                            match node.insert(key, 11, &guard) {
                                Ok(_) => assert!(false),
                                Err(result) => match result {
                                    Error::Duplicated(entry) => assert_eq!(entry, (key, 11)),
                                    Error::Full(_) => assert!(false),
                                    Error::Retry(_) => assert!(false),
                                },
                            };
                        }
                        Err(result) => match result {
                            Error::Duplicated(_) => assert!(false),
                            Error::Full(_) => {
                                for key_to_check in inserted.iter() {
                                    assert_eq!(node.search(key_to_check, &guard).unwrap(), &10);
                                }
                                done = true;
                            }
                            Error::Retry(_) => assert!(false),
                        },
                    }
                    if done {
                        break;
                    }
                }
                if done {
                    break;
                }
            }
        }
    }

    #[test]
    fn node_multithreaded() {
        let num_threads = 16;
        let range = 16384;
        let barrier = Arc::new(Barrier::new(num_threads));
        let node = Arc::new(Node::new(4));
        assert!(node.insert(0, 0, &crossbeam_epoch::pin()).is_ok());
        let inserted = Arc::new(Mutex::new(Vec::new()));
        inserted.lock().unwrap().push(0);
        let full = Arc::new(AtomicBool::new(false));
        let mut thread_handles = Vec::with_capacity(num_threads);
        for thread_id in 0..num_threads {
            let barrier_copied = barrier.clone();
            let node_copied = node.clone();
            let inserted_copied = inserted.clone();
            let full_copied = full.clone();
            thread_handles.push(thread::spawn(move || {
                let mut inserted_keys = Vec::new();
                let first_key = thread_id * range + 1;
                barrier_copied.wait();
                for key in first_key..(first_key + range) {
                    let guard = crossbeam_epoch::pin();
                    loop {
                        match node_copied.insert(key, key, &guard) {
                            Ok(()) => {
                                inserted_keys.push(key);
                                break;
                            }
                            Err(err) => match err {
                                Error::Duplicated(_) => assert!(false),
                                Error::Retry(_) => {
                                    if full_copied.load(Relaxed) {
                                        break;
                                    }
                                    continue;
                                }
                                Error::Full(_) => {
                                    full_copied.store(true, Relaxed);
                                    for key_to_check in first_key..key {
                                        assert!(node_copied
                                            .search(&key_to_check, &guard)
                                            .is_some());
                                    }
                                    break;
                                }
                            },
                        }
                    }
                    if full_copied.load(Relaxed) {
                        break;
                    }
                }
                let result = inserted_copied.lock();
                let mut vector = result.unwrap();
                for key in inserted_keys.iter() {
                    vector.push(*key);
                }
            }));
        }
        for handle in thread_handles {
            handle.join().unwrap();
        }
        for key in inserted.lock().unwrap().iter() {
            let guard = crossbeam_epoch::pin();
            if node.search(key, &guard).is_none() {
                println!("{}", key);
            }
        }
        let num_entries = node.print();
        println!("{}", num_entries);
        assert_eq!(num_entries, inserted.lock().unwrap().len());

        let guard = crossbeam_epoch::pin();
        let mut prev = 0;
        let mut scanner = node.min(&guard).unwrap();
        let mut iterated = 0;
        assert_eq!(*scanner.get().unwrap().0, 0);
        while let Some(entry) = scanner.next() {
            println!("{} {}", entry.0, entry.1);
            assert!(prev < *entry.0);
            assert_eq!(entry.0, entry.1);
            iterated += 1;
            prev = *entry.0;
        }
        println!("iterated: {}", iterated);
    }
}
