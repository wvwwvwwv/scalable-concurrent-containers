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
    low_key_node: Option<Box<Node<K, V>>>,
    high_key_node: Option<Box<Node<K, V>>>,
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Drop for NewNodes<K, V> {
    fn drop(&mut self) {
        self.low_key_node.take().map(|node| node.nullify_children());
        self.high_key_node
            .take()
            .map(|node| node.nullify_children());
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
                    leaf_node_anchor: if floor == 1 {
                        Atomic::new(LeafNodeAnchor {
                            min_leaf_node: Atomic::null(),
                        })
                    } else {
                        Atomic::null()
                    },
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
    pub fn search<'a>(&'a self, key: &K, guard: &'a Guard) -> Option<LeafNodeScanner<'a, K, V>> {
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
                        let leaf_node_scanner =
                            LeafNodeScanner::from(key, self, unsafe { child_leaf.deref() }, guard);
                        if leaf_node_scanner.get().is_some() {
                            return Some(leaf_node_scanner);
                        } else {
                            return None;
                        }
                    } else if unbounded_leaf == leaves.1.load(Acquire, guard) {
                        if !(leaves.0).validate(result.1) {
                            // after reading the pointer, need to validate the version
                            continue;
                        }
                        if unbounded_leaf.is_null() {
                            return None;
                        }
                        let leaf_node_scanner = LeafNodeScanner::from(
                            key,
                            self,
                            unsafe { unbounded_leaf.deref() },
                            guard,
                        );
                        if leaf_node_scanner.get().is_some() {
                            return Some(leaf_node_scanner);
                        } else {
                            return None;
                        }
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
    pub fn insert(
        &self,
        key: K,
        value: V,
        max_key: Option<&K>,
        guard: &Guard,
    ) -> Result<(), Error<K, V>> {
        // possible data race: the node is being split, for instance,
        //  - node state: ((15, ptr), (25, ptr)), 15 is being split
        //  - insert 10: min_greater_equal returns (15, ptr)
        //  - split 15: insert 11, and replace 15 with a new pointer, therefore ((11, ptr), (15, new_ptr), (25, ptr))
        //  - insert 10: load new_ptr, and try insert, that is incorrect as it is supposed to be inserted into (11, ptr)
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children,
                leaf_node_anchor: _,
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
                        match unsafe {
                            child_node
                                .deref()
                                .insert(key, value, Some(child_key), guard)
                        } {
                            Ok(_) => return Ok(()),
                            Err(err) => match err {
                                Error::Duplicated(_) => return Err(err),
                                Error::Full(entry) => {
                                    self.unbounded_node(guard);
                                    return self.split_node(
                                        entry,
                                        Some(child_key.clone()),
                                        Some(child_key),
                                        child_node,
                                        &child,
                                        &children,
                                        &new_children,
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
                        match unsafe { unbounded_child.deref().insert(key, value, max_key, guard) }
                        {
                            Ok(_) => return Ok(()),
                            Err(err) => match err {
                                Error::Duplicated(_) => return Err(err),
                                Error::Full(entry) => {
                                    return self.split_node(
                                        entry,
                                        None,
                                        max_key,
                                        unbounded_child,
                                        &(children.1),
                                        &children,
                                        &new_children,
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
                            self.unbounded_leaf(guard);
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
            leaf_node_anchor: _,
            floor,
        } = &new_root.entry
        {
            children.1.store(Owned::new(Node::new(floor - 1)), Relaxed);
            if new_root
                .split_node(
                    entry,
                    None,
                    None,
                    root_ptr.load(Relaxed, guard),
                    root_ptr,
                    &children,
                    &new_children,
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
                        new_low_key_node.max_bounded_child_key().unwrap().clone(),
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
        full_node_max_key: Option<&K>,
        full_node_shared: Shared<Node<K, V>>,
        full_node_ptr: &Atomic<Node<K, V>>,
        self_children: &(Leaf<K, Atomic<Node<K, V>>>, Atomic<Node<K, V>>),
        self_new_children: &Atomic<NewNodes<K, V>>,
        root_node_split: bool,
        guard: &Guard,
    ) -> Result<(), Error<K, V>> {
        let mut new_split_nodes;
        match self_new_children.compare_and_set(
            Shared::null(),
            Owned::new(NewNodes {
                origin_node_key: full_node_key,
                origin_node_ptr: full_node_ptr.clone(),
                low_key_node: None,
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
        let mut new_node_key = None;
        let new_split_nodes_ref = unsafe { new_split_nodes.deref_mut() };
        let mut full_node_side_link = None;
        match unsafe { &full_node_shared.deref().entry } {
            NodeType::InternalNode {
                children,
                new_children,
                leaf_node_anchor: _,
                floor,
            } => {
                // [TODO] if the full node manages a leaf node anchor (floor = 1), it needs to reconstruct links
                debug_assert!(!new_children.load(Relaxed, guard).is_null());
                let new_children_ref = unsafe { new_children.load(Relaxed, guard).deref_mut() };

                // copy nodes except for the known full node to the newly allocated internal node entries
                let internal_nodes = (Box::new(Node::new(*floor)), Box::new(Node::new(*floor)));
                let low_key_nodes = if let NodeType::InternalNode {
                    children,
                    new_children: _,
                    leaf_node_anchor: _,
                    floor: _,
                } = &internal_nodes.0.entry
                {
                    Some(children)
                } else {
                    None
                };
                let high_key_nodes = if let NodeType::InternalNode {
                    children,
                    new_children: _,
                    leaf_node_anchor: _,
                    floor: _,
                } = &internal_nodes.1.entry
                {
                    Some(children)
                } else {
                    None
                };

                (children.0).distribute_except(
                    new_children_ref.origin_node_key.as_ref(),
                    &low_key_nodes.as_ref().unwrap().0,
                    &high_key_nodes.as_ref().unwrap().0,
                );

                // move the new nodes attached to the full internal node to the newly allocated node entries
                let mut high_key_nodes_empty = true;
                if let Some(new_low_key_node) = new_children_ref.low_key_node.take() {
                    // the low key node never stores entries in its unbounded node
                    debug_assert!(!new_low_key_node.has_unbounded_child(guard));
                    if low_key_nodes
                        .as_ref()
                        .unwrap()
                        .0
                        .min_greater_equal(new_low_key_node.max_bounded_child_key().unwrap())
                        .0
                        .is_some()
                    {
                        low_key_nodes.as_ref().unwrap().0.insert(
                            new_low_key_node.max_bounded_child_key().unwrap().clone(),
                            Atomic::from(new_low_key_node),
                            false,
                        );
                    } else {
                        high_key_nodes.as_ref().unwrap().0.insert(
                            new_low_key_node.max_bounded_child_key().unwrap().clone(),
                            Atomic::from(new_low_key_node),
                            false,
                        );
                        high_key_nodes_empty = false;
                    }
                }
                if let Some(new_high_key_node) = new_children_ref.high_key_node.take() {
                    // the high key node may store entries in its unbounded node
                    //  - if the origin node key is None, it comes from an unbounded node, therefore use the calculated value.
                    //  - if the origin node key is Some, it comes from a bounded node, and thus will copy the unbounded node, use the origin key.
                    let max_key = if new_high_key_node.has_unbounded_child(guard) {
                        if let Some(full_node_key) = new_children_ref.origin_node_key.as_ref() {
                            // origin is a bounded node
                            Some(full_node_key)
                        } else if let Some(full_node_max_key) = full_node_max_key {
                            // origin is an unbounded node, but an ancestor is a bounded node
                            Some(full_node_max_key)
                        } else {
                            // all the ancestors are unbounded
                            None
                        }
                    } else {
                        new_high_key_node.max_bounded_child_key()
                    };

                    if let Some(max_key) = max_key {
                        if low_key_nodes
                            .as_ref()
                            .unwrap()
                            .0
                            .min_greater_equal(max_key)
                            .0
                            .is_some()
                        {
                            low_key_nodes.as_ref().unwrap().0.insert(
                                max_key.clone(),
                                Atomic::from(new_high_key_node),
                                false,
                            );
                        } else {
                            high_key_nodes.as_ref().unwrap().0.insert(
                                max_key.clone(),
                                Atomic::from(new_high_key_node),
                                false,
                            );
                            high_key_nodes_empty = false;
                        }
                    } else {
                        // cannot derive a max key value, implying that all the ancestors of the node are unbounded nodes
                        high_key_nodes
                            .as_ref()
                            .unwrap()
                            .1
                            .store(Owned::from(new_high_key_node), Relaxed);
                        high_key_nodes_empty = false;
                    }
                }

                // move the unbounded node if the origin is a bounded node
                //  - firstly, try to use the given max key
                //  - otherwise, make it an unbounded node
                if !new_children_ref.origin_node_key.is_none() {
                    if let Some(full_node_max_key) = full_node_max_key {
                        high_key_nodes.as_ref().unwrap().0.insert(
                            full_node_max_key.clone(),
                            children.1.clone(),
                            false,
                        );
                    } else {
                        let unbounded_node = children.1.load(Acquire, guard);
                        high_key_nodes
                            .as_ref()
                            .unwrap()
                            .1
                            .store(unbounded_node, Relaxed);
                    }
                    high_key_nodes_empty = false;
                } else if high_key_nodes_empty
                    && high_key_nodes.as_ref().unwrap().0.cardinality() > 0
                {
                    high_key_nodes_empty = false;
                }

                // if the high key node is not empty, the low key node needs to be inserted using a new key
                if !high_key_nodes_empty {
                    new_node_key
                        .replace(low_key_nodes.as_ref().unwrap().0.max_key().unwrap().clone());
                }

                // turn the new nodes into internal nodes
                new_split_nodes_ref.low_key_node.replace(internal_nodes.0);
                if !high_key_nodes_empty {
                    new_split_nodes_ref.high_key_node.replace(internal_nodes.1);
                }
            }
            NodeType::LeafNode {
                leaves,
                new_leaves,
                next_node_anchor: _,
                side_link,
            } => {
                debug_assert!(!new_leaves.load(Relaxed, guard).is_null());
                let new_leaves_ref = unsafe { new_leaves.load(Relaxed, guard).deref_mut() };

                // keep the side link
                full_node_side_link.replace(side_link.clone());

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
                (leaves.0).distribute_except(
                    new_leaves_ref.origin_leaf_key.as_ref(),
                    &low_key_leaves.as_ref().unwrap().0,
                    &high_key_leaves.as_ref().unwrap().0,
                );

                // move the new leaves attached to the full leaf node to the newly allocated leaf node entries
                let mut high_key_leaves_empty = true;
                for new_leaf in [
                    new_leaves_ref.low_key_leaf.take(),
                    new_leaves_ref.high_key_leaf.take(),
                ]
                .iter_mut()
                {
                    if let Some(new_leaf) = new_leaf.take() {
                        if low_key_leaves
                            .as_ref()
                            .unwrap()
                            .0
                            .min_greater_equal(new_leaf.max_key().unwrap())
                            .0
                            .is_some()
                        {
                            let result = low_key_leaves.as_ref().unwrap().0.insert(
                                new_leaf.max_key().unwrap().clone(),
                                Atomic::from(new_leaf),
                                false,
                            );
                            debug_assert!(result.is_none());
                        } else {
                            let result = high_key_leaves.as_ref().unwrap().0.insert(
                                new_leaf.max_key().unwrap().clone(),
                                Atomic::from(new_leaf),
                                false,
                            );
                            debug_assert!(result.is_none());
                            high_key_leaves_empty = false;
                        }
                    }
                }

                // move the unbounded leaf if the origin is a bounded leaf
                if !new_leaves_ref.origin_leaf_key.is_none() {
                    let unbounded_leaf = leaves.1.load(Acquire, guard);
                    debug_assert!(!unbounded_leaf.is_null());
                    high_key_leaves
                        .as_ref()
                        .unwrap()
                        .1
                        .store(unbounded_leaf, Relaxed);
                    high_key_leaves_empty = false;
                }

                // decide whether to move the high key leaf node
                if high_key_leaves_empty && high_key_leaves.as_ref().unwrap().0.cardinality() > 0 {
                    high_key_leaves_empty = false;
                }

                // if the high key leaf node is not empty, the low key leaf node needs to be inserted using a new key
                if !high_key_leaves_empty {
                    new_node_key.replace(
                        low_key_leaves
                            .as_ref()
                            .unwrap()
                            .0
                            .max_key()
                            .unwrap()
                            .clone(),
                    );
                }

                // turn the new leaves into leaf nodes
                new_split_nodes_ref.low_key_node.replace(leaf_nodes.0);
                if !high_key_leaves_empty {
                    new_split_nodes_ref.high_key_node.replace(leaf_nodes.1);
                }
            }
        };

        // the full node is the current root: split_root processes the rest.
        if root_node_split {
            return Ok(());
        }

        // insert the newly allocated internal nodes into the main array
        let unused_node;
        let mut immediate_less_key_node = None;
        let mut low_key_leaf_node = Shared::null();
        let mut high_key_leaf_node = Shared::null();
        if new_node_key.is_none() {
            // get the immediate less key node for linked list construction
            if full_node_side_link.is_some() {
                if new_split_nodes_ref.origin_node_key.is_some() {
                    immediate_less_key_node = self_children
                        .0
                        .max_less(new_split_nodes_ref.origin_node_key.as_ref().unwrap());
                } else {
                    // the origin node is an unbounded leaf node
                    immediate_less_key_node = self_children.0.max();
                }
            }
            // replace the full leaf with the low-key leaf
            unused_node = full_node_ptr.swap(
                Owned::from(new_split_nodes_ref.low_key_node.take().unwrap()),
                Release,
                &guard,
            );
            // get the node pointer for linked list adjustment
            if full_node_side_link.is_some() {
                low_key_leaf_node = full_node_ptr.load(Relaxed, guard);
            }
        } else {
            // get the immediate less key node for linked list construction
            if full_node_side_link.is_some() {
                immediate_less_key_node = self_children.0.max_less(new_node_key.as_ref().unwrap());
            }
            let low_key_node_ptr = Atomic::from(new_split_nodes_ref.low_key_node.take().unwrap());
            if let Some(node) =
                self_children
                    .0
                    .insert(new_node_key.unwrap(), low_key_node_ptr.clone(), false)
            {
                // insertion failed: expect that the caller handles the situation
                new_split_nodes_ref
                    .low_key_node
                    .replace(unsafe { (node.0).1.into_owned().into_box() });
                return Err(Error::Full(entry));
            }

            // replace the full node with the high-key node
            unused_node = full_node_ptr.swap(
                Owned::from(new_split_nodes_ref.high_key_node.take().unwrap()),
                Release,
                &guard,
            );
            // get the node pointers for linked list adjustment
            if full_node_side_link.is_some() {
                low_key_leaf_node = low_key_node_ptr.load(Relaxed, guard);
                high_key_leaf_node = full_node_ptr.load(Relaxed, guard);
            }
        }

        // reconstruct the linked list if the child is a leaf node
        if let Some(new_side_link) = full_node_side_link.take() {
            // firstly, replace the high key node's side link with the new side link
            // secondly, link the low key node with the high key node
            if !high_key_leaf_node.is_null() {
                unsafe {
                    low_key_leaf_node
                        .deref()
                        .update_side_link(high_key_leaf_node.clone());
                    high_key_leaf_node
                        .deref()
                        .update_side_link(new_side_link.load(Relaxed, guard))
                };
            } else {
                unsafe {
                    low_key_leaf_node
                        .deref()
                        .update_side_link(new_side_link.load(Relaxed, guard))
                };
            }
            // lastly, link the immediate less key node with the low key node
            if let Some(entry) = immediate_less_key_node {
                let node = entry.1.load(Relaxed, guard);
                unsafe { node.deref().update_side_link(low_key_leaf_node.clone()) };
            } else {
                // the low key node is the minimum node
                // [TODO] update its anchor
            }
        }

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
            full_leaf_shared.deref().distribute_boxed(
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
                // insertion failed: expect that the splits the leaf node
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

    fn max_bounded_child_key(&self) -> Option<&K> {
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children: _,
                leaf_node_anchor: _,
                floor: _,
            } => children.0.max_key(),
            NodeType::LeafNode {
                leaves,
                new_leaves: _,
                next_node_anchor: _,
                side_link: _,
            } => leaves.0.max_key(),
        }
    }

    fn has_unbounded_child(&self, guard: &Guard) -> bool {
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children: _,
                leaf_node_anchor: _,
                floor: _,
            } => !children.1.load(Relaxed, guard).is_null(),
            NodeType::LeafNode {
                leaves,
                new_leaves: _,
                next_node_anchor: _,
                side_link: _,
            } => !leaves.1.load(Relaxed, guard).is_null(),
        }
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

    fn update_side_link(&self, ptr: Shared<Node<K, V>>) {
        if let NodeType::LeafNode {
            leaves: _,
            new_leaves: _,
            next_node_anchor: _,
            side_link,
        } = &self.entry
        {
            side_link.store(ptr, Relaxed);
        }
    }

    fn next_leaf_node<'a>(&self, guard: &'a Guard) -> Option<&'a Node<K, V>> {
        if let NodeType::LeafNode {
            leaves: _,
            new_leaves: _,
            next_node_anchor,
            side_link,
        } = &self.entry
        {
            let side_link_ptr = side_link.load(Acquire, guard);
            if !side_link_ptr.is_null() {
                return Some(unsafe { side_link_ptr.deref() });
            }
            let next_node_anchor = next_node_anchor.load(Relaxed, guard);
            if !next_node_anchor.is_null() {
                let next_leaf_node_ptr =
                    unsafe { next_node_anchor.deref().min_leaf_node.load(Acquire, guard) };
                if !next_leaf_node_ptr.is_null() {
                    return Some(unsafe { next_leaf_node_ptr.deref() });
                }
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
                children: _,
                new_children,
                leaf_node_anchor: _,
                floor: _,
            } => {
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
    fn nullify_children(&self) {
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children: _,
                leaf_node_anchor: _,
                floor: _,
            } => {
                let mut scanner = LeafScanner::new(&children.0);
                while let Some(entry) = scanner.next() {
                    entry.1.store(Shared::null(), Relaxed);
                }
                children.1.store(Shared::null(), Relaxed);
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
                // destroy the leaf node anchor
                let anchor = leaf_node_anchor.load(Relaxed, &guard);
                if !anchor.is_null() {
                    drop(unsafe { anchor.into_owned() });
                }
                // destroy entries related to the unused child
                let unused_nodes = new_children.load(Acquire, &guard);
                if !unused_nodes.is_null() {
                    // destroy only the origin node, assuming that the rest are copied
                    let unused_nodes = unsafe { unused_nodes.into_owned() };
                    let obsolete_node = unused_nodes.origin_node_ptr.load(Relaxed, &guard);
                    let mut target_node = None;
                    if !obsolete_node.is_null() {
                        target_node.replace(unsafe { obsolete_node.into_owned() });
                    }
                    drop(guard);
                    drop(target_node);
                } else {
                    // destroy all: in order to avoid stack overflow, destroy them without the thread pinned
                    let mut scanner = LeafScanner::new(&children.0);
                    let mut child_nodes: [Option<Owned<Node<K, V>>>; ARRAY_SIZE + 1] = [
                        None, None, None, None, None, None, None, None, None, None, None, None,
                        None,
                    ];
                    let mut child_node_index = 0;
                    while let Some(entry) = scanner.next() {
                        let child = entry.1.load(Acquire, &guard);
                        if !child.is_null() {
                            child_nodes[child_node_index].replace(unsafe { child.into_owned() });
                            child_node_index += 1;
                        }
                    }
                    let unbounded_child = children.1.load(Acquire, &guard);
                    if !unbounded_child.is_null() {
                        child_nodes[ARRAY_SIZE].replace(unsafe { unbounded_child.into_owned() });
                    }
                    drop(guard);
                    drop(child_nodes);
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
                        let obsolete_leaf = unsafe { obsolete_leaf.into_owned() };
                        drop(guard);
                        drop(obsolete_leaf);
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
    node_scanner: Option<LeafScanner<'a, K, Atomic<Leaf<K, V>>>>,
    leaf_scanner: Option<LeafScanner<'a, K, V>>,
    guard: &'a Guard,
}

impl<'a, K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> LeafNodeScanner<'a, K, V> {
    fn new(leaf_node: &'a Node<K, V>, guard: &'a Guard) -> LeafNodeScanner<'a, K, V> {
        LeafNodeScanner::<'a, K, V> {
            leaf_node,
            node_scanner: None,
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

    fn from(
        key: &K,
        leaf_node: &'a Node<K, V>,
        leaf: &'a Leaf<K, V>,
        guard: &'a Guard,
    ) -> LeafNodeScanner<'a, K, V> {
        LeafNodeScanner::<'a, K, V> {
            leaf_node,
            node_scanner: None,
            leaf_scanner: Some(LeafScanner::from(key, leaf)),
            guard,
        }
    }

    fn from_greater_equal(
        key: &K,
        leaf_node: &'a Node<K, V>,
        guard: &'a Guard,
    ) -> LeafNodeScanner<'a, K, V> {
        if let NodeType::LeafNode {
            leaves,
            new_leaves: _,
            next_node_anchor: _,
            side_link: _,
        } = &leaf_node.entry
        {
            let result = leaves.0.min_greater_equal(key);
            if let Some(leaf) = result.0 {
                let leaf_ref = unsafe { leaf.1.load(Acquire, guard).deref() };
                return LeafNodeScanner::<'a, K, V> {
                    leaf_node,
                    node_scanner: None,
                    leaf_scanner: Some(LeafScanner::from_greater_equal(key, leaf_ref)),
                    guard,
                };
            } else {
                let unbounded_leaf = leaves.1.load(Acquire, guard);
                if !unbounded_leaf.is_null() {
                    return LeafNodeScanner::<'a, K, V> {
                        leaf_node,
                        node_scanner: None,
                        leaf_scanner: Some(LeafScanner::from_greater_equal(key, unsafe {
                            unbounded_leaf.deref()
                        })),
                        guard,
                    };
                }
            }
        }
        return LeafNodeScanner::<'a, K, V> {
            leaf_node,
            node_scanner: None,
            leaf_scanner: None,
            guard,
        };
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
            if self.node_scanner.is_none() {
                return None;
            }
        }

        if self.node_scanner.is_none() && self.leaf_scanner.is_none() {
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
                } => {
                    self.node_scanner.replace(LeafScanner::new(&leaves.0));
                }
            }
        }

        if let Some(node_scanner) = self.node_scanner.as_mut() {
            // proceed to the next leaf
            while let Some(leaf) = node_scanner.next() {
                self.leaf_scanner.replace(LeafScanner::new(unsafe {
                    leaf.1.load(Acquire, self.guard).deref()
                }));
                if let Some(leaf_scanner) = self.leaf_scanner.as_mut() {
                    // leaf iteration
                    if let Some(entry) = leaf_scanner.next() {
                        return Some(entry);
                    }
                }
                self.leaf_scanner.take();
            }
        }
        self.node_scanner.take();

        let unbounded_child = match &self.leaf_node.entry {
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
            } => leaves.1.load(Acquire, self.guard),
        };
        if !unbounded_child.is_null() {
            self.leaf_scanner
                .replace(LeafScanner::new(unsafe { unbounded_child.deref() }));
            if let Some(leaf_scanner) = self.leaf_scanner.as_mut() {
                // leaf iteration
                if let Some(entry) = leaf_scanner.next() {
                    return Some(entry);
                }
            }
        }

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
                match node.insert(key, 10, None, &guard) {
                    Ok(_) => match node.insert(key, 11, None, &guard) {
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
                                assert_eq!(
                                    node.search(&key_to_check, &guard).unwrap().get().unwrap(),
                                    (&key_to_check, &10)
                                );
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
                    match node.insert(key, 10, None, &guard) {
                        Ok(_) => {
                            inserted.push(key);
                            match node.insert(key, 11, None, &guard) {
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
                                    assert_eq!(
                                        node.search(key_to_check, &guard).unwrap().get().unwrap(),
                                        (key_to_check, &10)
                                    );
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
        assert!(node.insert(0, 0, None, &crossbeam_epoch::pin()).is_ok());
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
                        match node_copied.insert(key, key, None, &guard) {
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
