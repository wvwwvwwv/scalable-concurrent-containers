use super::leaf::{LeafScanner, ARRAY_SIZE};
use super::Leaf;
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use std::cmp::Ordering;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

pub enum Error<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// Duplicated key found: returns the given key-value pair.
    Duplicated((K, V)),
    /// Full: returns a newly allocated node for the parent to consume
    Full((K, V)),
    /// Retry: return the given key-value pair.
    Retry((K, V)),
}

struct NewNodes<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// None: unbounded node
    origin_node_key: Option<K>,
    origin_node_unbounded: bool,
    low_key_node_key: Option<K>,
    low_key_node: Option<Atomic<Node<K, V>>>,
    high_key_node_key: Option<K>,
    high_key_node: Option<Atomic<Node<K, V>>>,
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Drop for NewNodes<K, V> {
    fn drop(&mut self) {
        self.low_key_node
            .take()
            .map(|node| drop(unsafe { node.into_owned().nullify_children() }));
        self.high_key_node
            .take()
            .map(|node| drop(unsafe { node.into_owned().nullify_children() }));
    }
}

struct NewLeaves<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// None: unbounded leaf
    origin_leaf_key: K,
    origin_leaf_unbounded: bool,
    low_key_leaf: Option<Box<Leaf<K, V>>>,
    high_key_leaf: Option<Box<Leaf<K, V>>>,
}

enum NodeType<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// InternalNode: |ptr(children)/max(child keys)|...|ptr(children)|
    InternalNode {
        children: (Leaf<K, Atomic<Node<K, V>>>, Atomic<Node<K, V>>),
        new_children: Atomic<NewNodes<K, V>>,
        floor: usize,
    },
    /// LeafNode: |ptr(entry array)/max(child keys)|...|ptr(entry array)|
    LeafNode {
        leaves: (Leaf<K, Atomic<Leaf<K, V>>>, Atomic<Leaf<K, V>>),
        new_leaves: Atomic<NewLeaves<K, V>>,
        side_link: Atomic<Node<K, V>>,
    },
}

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
                    floor,
                }
            } else {
                NodeType::LeafNode {
                    leaves: (Leaf::new(), Atomic::null()),
                    new_leaves: Atomic::null(),
                    side_link: Atomic::null(),
                }
            },
        }
    }

    pub fn search<'a>(&'a self, key: &K, guard: &'a Guard) -> Option<LeafNodeScanner<'a, K, V>> {
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children: _,
                floor: _,
            } => {
                if let Some((_, child)) = (children.0).min_ge(&key) {
                    unsafe { child.load(Acquire, guard).deref().search(key, guard) }
                } else {
                    let unbounded_node = (children.1).load(Relaxed, guard);
                    if unbounded_node.is_null() {
                        // non-leaf node: invalid
                        return None;
                    }
                    unsafe { unbounded_node.deref().search(key, guard) }
                }
            }
            NodeType::LeafNode {
                leaves,
                new_leaves: _,
                side_link: _,
            } => {
                if let Some((_, child)) = (leaves.0).min_ge(&key) {
                    let leaf_node_scanner = LeafNodeScanner::from(
                        key,
                        self,
                        unsafe { child.load(Acquire, guard).deref() },
                        guard,
                    );
                    if leaf_node_scanner.get().is_some() {
                        Some(leaf_node_scanner)
                    } else {
                        None
                    }
                } else {
                    let unbounded_leaf = (leaves.1).load(Relaxed, guard);
                    if unbounded_leaf.is_null() {
                        return None;
                    }
                    let leaf_node_scanner =
                        LeafNodeScanner::from(key, self, unsafe { unbounded_leaf.deref() }, guard);
                    if leaf_node_scanner.get().is_some() {
                        Some(leaf_node_scanner)
                    } else {
                        None
                    }
                }
            }
        }
    }

    /// Inserts a key-value pair.
    ///
    /// It is a recursive call, and therefore stack-overflow may occur.
    /// B+ tree assures that the tree is filled up from the very bottom nodes.
    pub fn insert(
        &self,
        key: K,
        value: V,
        parent_key: Option<&K>,
        guard: &Guard,
    ) -> Result<(), Error<K, V>> {
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children,
                floor,
            } => {
                loop {
                    if let Some((child_key, child)) = (children.0).min_ge(&key) {
                        let child_node = child.load(Acquire, guard);
                        match unsafe {
                            child_node
                                .deref()
                                .insert(key, value, Some(&child_key), guard)
                        } {
                            Ok(_) => return Ok(()),
                            Err(err) => match err {
                                Error::Duplicated(_) => return Err(err),
                                Error::Full(entry) => {
                                    return self.split_node(
                                        entry,
                                        Some(&child_key),
                                        &child,
                                        &children,
                                        &new_children,
                                        guard,
                                    )
                                }
                                Error::Retry(_) => return Err(err),
                            },
                        }
                    } else if !(children.0).full() {
                        if let Some(result) = (children.0).insert(
                            key.clone(),
                            Atomic::new(Node::new(floor - 1)),
                            false,
                        ) {
                            drop(unsafe { (result.0).1.into_owned() });
                        }
                    } else {
                        break;
                    }
                }
                let unbounded_child = self.get_or_allocate_unbounded_child(
                    &children.1,
                    || Node::new(floor - 1),
                    guard,
                );
                match unsafe {
                    unbounded_child
                        .deref()
                        .insert(key, value, parent_key, guard)
                } {
                    Ok(_) => return Ok(()),
                    Err(err) => match err {
                        Error::Duplicated(_) => Err(err),
                        Error::Full(entry) => self.split_node(
                            entry,
                            parent_key,
                            &(children.1),
                            &children,
                            &new_children,
                            guard,
                        ),
                        Error::Retry(_) => Err(err),
                    },
                }
            }
            NodeType::LeafNode {
                leaves,
                new_leaves,
                side_link: _,
            } => {
                loop {
                    if let Some((child_key, child)) = (leaves.0).min_ge(&key) {
                        let child_node = child.load(Acquire, guard);
                        return unsafe { child_node.deref().insert(key, value, false) }
                            .map_or_else(
                                || Ok(()),
                                |result| {
                                    if result.1 {
                                        return Err(Error::Duplicated(result.0));
                                    }
                                    self.split_leaf(
                                        result.0,
                                        child_key.clone(),
                                        &child,
                                        false,
                                        leaves,
                                        new_leaves,
                                        guard,
                                    )
                                },
                            );
                    } else if !(leaves.0).full() {
                        if let Some(result) =
                            (leaves.0).insert(key.clone(), Atomic::new(Leaf::new()), false)
                        {
                            drop(unsafe { (result.0).1.into_owned() });
                        }
                    } else {
                        break;
                    }
                }
                let unbounded_child =
                    self.get_or_allocate_unbounded_child(&leaves.1, Leaf::new, guard);
                return unsafe { unbounded_child.deref().insert(key, value, false) }.map_or_else(
                    || Ok(()),
                    |result| {
                        if result.1 {
                            return Err(Error::Duplicated(result.0));
                        }
                        let max_key = unsafe { unbounded_child.deref().max_key() }.map_or_else(
                            || (result.0).0.clone(),
                            |key| {
                                if key.cmp(&(result.0).0) == Ordering::Less {
                                    (result.0).0.clone()
                                } else {
                                    key.clone()
                                }
                            },
                        );
                        self.split_leaf(
                            result.0, max_key, &leaves.1, true, leaves, new_leaves, guard,
                        )
                    },
                );
            }
        }
    }

    fn get_or_allocate_unbounded_child<'a, T, F: FnOnce() -> T>(
        &self,
        unbounded_child_ptr: &Atomic<T>,
        constructor: F,
        guard: &'a Guard,
    ) -> Shared<'a, T> {
        let mut unbounded_child = unbounded_child_ptr.load(Relaxed, guard);
        if unbounded_child.is_null() {
            match unbounded_child_ptr.compare_and_set(
                unbounded_child,
                Owned::new(constructor()),
                Relaxed,
                guard,
            ) {
                Ok(result) => unbounded_child = result,
                Err(result) => unbounded_child = result.current,
            }
        }
        unbounded_child
    }

    fn full(&self, guard: &Guard) -> bool {
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children: _,
                floor: _,
            } => children.0.full() && !children.1.load(Relaxed, guard).is_null(),
            NodeType::LeafNode {
                leaves,
                new_leaves: _,
                side_link: _,
            } => leaves.0.full() && !leaves.1.load(Relaxed, guard).is_null(),
        }
    }

    /// Rollback the ongoing split operation recursively.
    fn rollback(&self, key: &K, guard: &Guard) {
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children,
                floor: _,
            } => {
                let intermediate_split = new_children.swap(Shared::null(), Release, guard);
                drop(unsafe { intermediate_split.into_owned() });
                if let Some((_, child)) = (children.0).min_ge(&key) {
                    unsafe { child.load(Acquire, guard).deref().rollback(key, guard) };
                } else {
                    let unbounded_child = (children.1).load(Relaxed, guard);
                    if !unbounded_child.is_null() {
                        unsafe { unbounded_child.deref().rollback(key, guard) }
                    }
                }
            }
            NodeType::LeafNode {
                leaves: _,
                new_leaves,
                side_link: _,
            } => {
                let intermediate_split = new_leaves.swap(Shared::null(), Release, guard);
                drop(unsafe { intermediate_split.into_owned() });
            }
        }
    }

    fn split_node(
        &self,
        entry: (K, V),
        full_node_key: Option<&K>,
        full_node: &Atomic<Node<K, V>>,
        children: &(Leaf<K, Atomic<Node<K, V>>>, Atomic<Node<K, V>>),
        new_children: &Atomic<NewNodes<K, V>>,
        guard: &Guard,
    ) -> Result<(), Error<K, V>> {
        let full_node_unbounded = full_node_key.is_none();
        let full_node_shared = full_node.load(Relaxed, guard);

        debug_assert!(unsafe { full_node_shared.deref().full(guard) });
        let mut new_split_nodes;
        match new_children.compare_and_set(
            Shared::null(),
            Owned::new(NewNodes {
                origin_node_key: full_node_key.map(|key| key.clone()),
                origin_node_unbounded: full_node_unbounded,
                low_key_node_key: None,
                low_key_node: None,
                high_key_node_key: None,
                high_key_node: None,
            }),
            Relaxed,
            guard,
        ) {
            Ok(result) => new_split_nodes = result,
            Err(_) => {
                unsafe {
                    full_node_shared.deref().rollback(&entry.0, guard);
                };
                return Err(Error::Retry(entry));
            }
        }

        // copy entries to the newly allocated leaves
        let new_split_nodes_ref = unsafe { new_split_nodes.deref_mut() };
        let mut deprecated_new_nodes = Shared::null();
        match unsafe { &full_node_shared.deref().entry } {
            NodeType::InternalNode {
                children,
                new_children,
                floor,
            } => {
                debug_assert!(!new_children.load(Relaxed, guard).is_null());
                deprecated_new_nodes = new_children.load(Relaxed, guard);
                let new_children_ref = unsafe { deprecated_new_nodes.deref_mut() };

                // copy nodes except for the known full node to the newly allocated internal node entries
                let mut node_entries = (None, None);
                (children.0).distribute_except(
                    new_children_ref.origin_node_key.as_ref(),
                    &mut node_entries,
                );

                // need to move the unbounded leaf if the origin is a bounded node, and the key is that associated with the full node
                let unbounded_node = children.1.load(Acquire, guard);
                if !unbounded_node.is_null() && !new_children_ref.origin_node_unbounded {
                    // the keys in the unbounded node are greater than those keys in 'children' as the origin is a bounded leaf
                    if node_entries.1.is_none() {
                        if node_entries.0.is_none() {
                            node_entries.0.replace(Leaf::new());
                        }
                        node_entries.0.as_ref().unwrap().insert(
                            new_children_ref.origin_node_key.as_ref().unwrap().clone(),
                            children.1.clone(),
                            false,
                        );
                    } else {
                        node_entries.1.as_ref().unwrap().insert(
                            new_children_ref.origin_node_key.as_ref().unwrap().clone(),
                            children.1.clone(),
                            false,
                        );
                    }
                }

                // move the new leaves attached to the full node to the newly allocated node entries
                if node_entries.1.is_none() {
                    // trivial insert
                    if node_entries.0.is_none() {
                        node_entries.0.replace(Leaf::new());
                    }
                    node_entries.0.as_ref().map(|node| {
                        for (new_node, new_node_key) in [
                            (
                                new_children_ref.low_key_node.as_ref(),
                                new_children_ref.low_key_node_key.take(),
                            ),
                            (
                                new_children_ref.high_key_node.as_ref(),
                                new_children_ref.high_key_node_key.take(),
                            ),
                        ]
                        .iter_mut()
                        {
                            if let Some(new_node) = new_node {
                                node.insert(new_node_key.take().unwrap(), new_node.clone(), false);
                            }
                        }
                    });
                } else {
                    // insert into an appropriate node entry
                    for (new_node, new_node_key) in [
                        (
                            new_children_ref.low_key_node.as_ref(),
                            new_children_ref.low_key_node_key.take(),
                        ),
                        (
                            new_children_ref.high_key_node.as_ref(),
                            new_children_ref.high_key_node_key.take(),
                        ),
                    ]
                    .iter_mut()
                    {
                        if let Some(new_node) = new_node.take() {
                            for target in [
                                node_entries.0.as_ref().unwrap(),
                                node_entries.0.as_ref().unwrap(),
                            ]
                            .iter()
                            {
                                if target.min_ge(new_node_key.as_ref().unwrap()).is_some() {
                                    target.insert(
                                        new_node_key.take().unwrap(),
                                        new_node.clone(),
                                        false,
                                    );
                                    break;
                                }
                            }
                        }
                    }
                }

                // turn the new nodes into internal nodes
                if let Some(node_entry) = node_entries.0.take() {
                    new_split_nodes_ref
                        .low_key_node_key
                        .replace(node_entry.max_key().unwrap().clone());
                    new_split_nodes_ref.low_key_node.replace(Atomic::new(Node {
                        entry: {
                            NodeType::InternalNode {
                                children: (node_entry, Atomic::null()),
                                new_children: Atomic::null(),
                                floor: *floor,
                            }
                        },
                    }));
                }
                if let Some(node_entry) = node_entries.1.take() {
                    new_split_nodes_ref
                        .high_key_node_key
                        .replace(node_entry.max_key().unwrap().clone());
                    new_split_nodes_ref.high_key_node.replace(Atomic::new(Node {
                        entry: {
                            NodeType::InternalNode {
                                children: (node_entry, Atomic::null()),
                                new_children: Atomic::null(),
                                floor: *floor,
                            }
                        },
                    }));
                }
            }
            NodeType::LeafNode {
                leaves,
                new_leaves,
                side_link: _,
            } => {
                debug_assert!(!new_leaves.load(Relaxed, guard).is_null());
                let new_leaves_ref = unsafe { new_leaves.load(Relaxed, guard).deref_mut() };

                // copy leaves except for the known full leaf to the newly allocated leaf node entries
                let mut leaf_entries = (None, None);
                let key_to_exclude = if !new_leaves_ref.origin_leaf_unbounded {
                    Some(&new_leaves_ref.origin_leaf_key)
                } else {
                    None
                };
                (leaves.0).distribute_except(key_to_exclude, &mut leaf_entries);

                // need to move the unbounded leaf if the origin is a bounded leaf
                let unbounded_leaf = leaves.1.load(Acquire, guard);
                if !unbounded_leaf.is_null() && !new_leaves_ref.origin_leaf_unbounded {
                    // the keys in the unbounded leaf are greater than those keys in 'leaves' as the origin is a bounded leaf
                    if leaf_entries.1.is_none() {
                        if leaf_entries.0.is_none() {
                            leaf_entries.0.replace(Leaf::new());
                        }
                        leaf_entries.0.as_ref().unwrap().insert(
                            new_leaves_ref.origin_leaf_key.clone(),
                            leaves.1.clone(),
                            false,
                        );
                    } else {
                        leaf_entries.1.as_ref().unwrap().insert(
                            new_leaves_ref.origin_leaf_key.clone(),
                            leaves.1.clone(),
                            false,
                        );
                    }
                }

                // move the new leaves attached to the full leaf node to the newly allocated leaf node entries
                if leaf_entries.1.is_none() {
                    // trivial insert
                    if leaf_entries.0.is_none() {
                        leaf_entries.0.replace(Leaf::new());
                    }
                    leaf_entries.0.as_ref().map(|leaf| {
                        for new_leaf in [
                            new_leaves_ref.low_key_leaf.take(),
                            new_leaves_ref.high_key_leaf.take(),
                        ]
                        .iter_mut()
                        {
                            if let Some(new_leaf) = new_leaf.take() {
                                leaf.insert(
                                    new_leaf.max_key().unwrap().clone(),
                                    Atomic::from(new_leaf),
                                    false,
                                );
                            }
                        }
                    });
                } else {
                    // insert into an appropriate leaf node entry
                    for new_leaf in [
                        new_leaves_ref.low_key_leaf.take(),
                        new_leaves_ref.high_key_leaf.take(),
                    ]
                    .iter_mut()
                    {
                        if let Some(new_leaf) = new_leaf.take() {
                            for target in [
                                leaf_entries.0.as_ref().unwrap(),
                                leaf_entries.0.as_ref().unwrap(),
                            ]
                            .iter()
                            {
                                if target.min_ge(new_leaf.max_key().unwrap()).is_some() {
                                    target.insert(
                                        new_leaf.max_key().unwrap().clone(),
                                        Atomic::from(new_leaf),
                                        false,
                                    );
                                    break;
                                }
                            }
                        }
                    }
                }

                // turn the new leaves into leaf nodes
                if let Some(leaf_entry) = leaf_entries.0.take() {
                    new_split_nodes_ref
                        .low_key_node_key
                        .replace(leaf_entry.max_key().unwrap().clone());
                    new_split_nodes_ref.low_key_node.replace(Atomic::new(Node {
                        entry: {
                            NodeType::LeafNode {
                                leaves: (leaf_entry, Atomic::null()),
                                new_leaves: Atomic::null(),
                                side_link: Atomic::null(),
                            }
                        },
                    }));
                }
                if let Some(leaf_entry) = leaf_entries.1.take() {
                    new_split_nodes_ref
                        .high_key_node_key
                        .replace(leaf_entry.max_key().unwrap().clone());
                    new_split_nodes_ref.high_key_node.replace(Atomic::new(Node {
                        entry: {
                            NodeType::LeafNode {
                                leaves: (leaf_entry, Atomic::null()),
                                new_leaves: Atomic::null(),
                                side_link: Atomic::null(),
                            }
                        },
                    }));
                }
            }
        };

        // insert the newly allocated internal nodes into the main array
        let unused_node;
        if new_split_nodes_ref.high_key_node.is_none() {
            // replace the full leaf with the low-key leaf
            unused_node = full_node.swap(
                unsafe {
                    new_split_nodes_ref
                        .low_key_node
                        .take()
                        .unwrap()
                        .into_owned()
                },
                Release,
                &guard,
            );
        } else {
            let max_key = new_split_nodes_ref.low_key_node_key.as_ref().unwrap();
            if let Some(node) = children.0.insert(
                max_key.clone(),
                new_split_nodes_ref.low_key_node.take().unwrap(),
                false,
            ) {
                // insertion failed: expect that the caller handles the situation
                new_split_nodes_ref.low_key_node.replace((node.0).1);
                // it is required to ensure that the unbounded node exists before returning Error::Full
                self.get_or_allocate_unbounded_child(&children.1, || Node::new(0), guard);
                return Err(Error::Full(entry));
            }

            // replace the full node with the high-key node
            unused_node = full_node.swap(
                unsafe {
                    new_split_nodes_ref
                        .high_key_node
                        .take()
                        .unwrap()
                        .into_owned()
                },
                Release,
                &guard,
            );
        }

        // it is practically un-locking the node
        let unused_children = new_children.swap(Shared::null(), Release, guard);

        // prevent temporary nodes to be dropped as they are reachable
        if !deprecated_new_nodes.is_null() {
            let new_children_ref = unsafe { deprecated_new_nodes.deref_mut() };
            new_children_ref.low_key_node.take();
            new_children_ref.high_key_node.take();
        }

        // deallocate the deprecated nodes
        unsafe {
            guard.defer_destroy(unused_node);
            guard.defer_destroy(unused_children);
        };

        Ok(())
    }

    fn split_leaf(
        &self,
        entry: (K, V),
        full_leaf_key: K,
        full_leaf: &Atomic<Leaf<K, V>>,
        full_leaf_unbounded: bool,
        leaves: &(Leaf<K, Atomic<Leaf<K, V>>>, Atomic<Leaf<K, V>>),
        new_leaves: &Atomic<NewLeaves<K, V>>,
        guard: &Guard,
    ) -> Result<(), Error<K, V>> {
        let full_leaf_shared = full_leaf.load(Relaxed, guard);
        debug_assert!(unsafe { full_leaf_shared.deref().full() });
        let mut new_leaves_ptr;
        match new_leaves.compare_and_set(
            Shared::null(),
            Owned::new(NewLeaves {
                origin_leaf_key: full_leaf_key,
                origin_leaf_unbounded: full_leaf_unbounded,
                low_key_leaf: None,
                high_key_leaf: None,
            }),
            Relaxed,
            guard,
        ) {
            Ok(result) => new_leaves_ptr = result,
            Err(_) => return Err(Error::Retry(entry)),
        }

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
                .min_ge(&entry.0)
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
            unused_leaf = full_leaf.swap(
                unsafe {
                    Owned::from_raw(Box::into_raw(new_leaves_ref.low_key_leaf.take().unwrap()))
                },
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
                // insertion failed: expect that the caller handles the situation
                new_leaves_ref
                    .low_key_leaf
                    .replace(unsafe { (leaf.0).1.into_owned().into_box() });
                // it is required to ensure that the unbounded leaf exists before returning Error::Full
                self.get_or_allocate_unbounded_child(&leaves.1, Leaf::new, guard);
                return Err(Error::Full(entry));
            }

            // replace the full leaf with the high-key leaf
            unused_leaf = full_leaf.swap(
                unsafe {
                    Owned::from_raw(Box::into_raw(new_leaves_ref.high_key_leaf.take().unwrap()))
                },
                Release,
                &guard,
            );
        }

        // it is practically un-locking the leaf node
        let unused_leaves = new_leaves.swap(Shared::null(), Release, guard);

        // deallocate the deprecated leaves
        unsafe {
            guard.defer_destroy(unused_leaf);
            guard.defer_destroy(unused_leaves);
        };

        // OK
        return Ok(());
    }

    fn nullify_children(&self) {
        match &self.entry {
            NodeType::InternalNode {
                children,
                new_children: _,
                floor: _,
            } => {
                let mut scanner = LeafScanner::new(&children.0);
                while let Some(entry) = scanner.next() {
                    entry.1.store(Shared::null(), Relaxed);
                }
            }
            NodeType::LeafNode {
                leaves,
                new_leaves: _,
                side_link: _,
            } => {
                let mut scanner = LeafScanner::new(&leaves.0);
                while let Some(entry) = scanner.next() {
                    entry.1.store(Shared::null(), Relaxed);
                }
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
                floor: _,
            } => {
                // destroy entries related to the unused child
                let mut unused_nodes = new_children.load(Acquire, &guard);
                if !unused_nodes.is_null() {
                    // destroy only the origin node, assuming that the rest are copied
                    let mut target_shr_ptr = Shared::null();
                    let unused_nodes_ref = unsafe { unused_nodes.deref_mut() };
                    if !unused_nodes_ref.origin_node_unbounded {
                        if let Some(key) = &unused_nodes_ref.origin_node_key {
                            if let Some(target_ptr) = children.0.search(key) {
                                target_shr_ptr = target_ptr.load(Acquire, &guard);
                            }
                        }
                    } else {
                        target_shr_ptr = children.1.load(Acquire, &guard);
                    }
                    if !target_shr_ptr.is_null() {
                        drop(unsafe { target_shr_ptr.into_owned() });
                    }
                    unused_nodes_ref.low_key_node.take();
                    unused_nodes_ref.high_key_node.take();
                    drop(unsafe { unused_nodes.into_owned() });
                } else {
                    // destroy all
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
                }
            }
            NodeType::LeafNode {
                leaves,
                new_leaves,
                side_link: _,
            } => {
                // destroy entries related to the unused child
                let unused_leaves = new_leaves.load(Acquire, &guard);
                if !unused_leaves.is_null() {
                    // destroy only the origin node, assuming that the rest are copied
                    let mut target_shr_ptr = Shared::null();
                    let unused_leaves_ref = unsafe { unused_leaves.deref() };
                    if !unused_leaves_ref.origin_leaf_unbounded {
                        if let Some(target_ptr) =
                            leaves.0.search(&unused_leaves_ref.origin_leaf_key)
                        {
                            target_shr_ptr = target_ptr.load(Acquire, &guard);
                        }
                    } else {
                        target_shr_ptr = leaves.1.load(Acquire, &guard);
                    }
                    if !target_shr_ptr.is_null() {
                        drop(unsafe { target_shr_ptr.into_owned() });
                    }
                    drop(unsafe { unused_leaves.into_owned() });
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

    fn from_ge(key: &K, leaf_node: &'a Node<K, V>, guard: &'a Guard) -> LeafNodeScanner<'a, K, V> {
        // TODO
        LeafNodeScanner::<'a, K, V> {
            leaf_node,
            node_scanner: None,
            leaf_scanner: None,
            guard,
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
                    floor: _,
                } => return None,
                NodeType::LeafNode {
                    leaves,
                    new_leaves: _,
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
                floor: _,
            } => Shared::null(),
            NodeType::LeafNode {
                leaves,
                new_leaves: _,
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
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn leaf_node() {
        let guard = crossbeam_epoch::pin();
        // sequential
        let node = Node::new(0);
        for i in 0..ARRAY_SIZE {
            for j in 0..(ARRAY_SIZE + 1) {
                assert!(node
                    .insert((j + 1) * (ARRAY_SIZE + 1) - i, 10, None, &guard)
                    .is_ok());
                match node.insert((j + 1) * (ARRAY_SIZE + 1) - i, 11, None, &guard) {
                    Ok(_) => assert!(false),
                    Err(result) => match result {
                        Error::Duplicated(entry) => {
                            assert_eq!(entry, ((j + 1) * (ARRAY_SIZE + 1) - i, 11))
                        }
                        Error::Full(_) => assert!(false),
                        Error::Retry(_) => assert!(false),
                    },
                }
            }
        }
        match node.insert(0, 11, None, &guard) {
            Ok(_) => assert!(false),
            Err(result) => match result {
                Error::Duplicated(_) => assert!(false),
                Error::Full(entry) => assert_eq!(entry, (0, 11)),
                Error::Retry(_) => assert!(false),
            },
        }

        // expect that the previous attempt left split children
        match node.insert(240, 11, None, &guard) {
            Ok(_) => assert!(false),
            Err(result) => match result {
                Error::Duplicated(_) => assert!(false),
                Error::Full(_) => assert!(false),
                Error::Retry(entry) => assert_eq!(entry, (240, 11)),
            },
        }
        // induce split
        let node = Node::new(0);
        for i in 0..ARRAY_SIZE {
            for j in 0..ARRAY_SIZE {
                if j == ARRAY_SIZE / 2 {
                    continue;
                }
                assert!(node
                    .insert((j + 1) * (ARRAY_SIZE + 1) - i, 10, None, &guard)
                    .is_ok());
                match node.insert((j + 1) * (ARRAY_SIZE + 1) - i, 11, None, &guard) {
                    Ok(_) => assert!(false),
                    Err(result) => match result {
                        Error::Duplicated(entry) => {
                            assert_eq!(entry, ((j + 1) * (ARRAY_SIZE + 1) - i, 11))
                        }
                        Error::Full(_) => assert!(false),
                        Error::Retry(_) => assert!(false),
                    },
                }
            }
        }
        for i in 0..(ARRAY_SIZE / 2) {
            assert!(node
                .insert(
                    (ARRAY_SIZE / 2 + 1) * (ARRAY_SIZE + 1) - i,
                    10,
                    None,
                    &guard
                )
                .is_ok());
            match node.insert(
                (ARRAY_SIZE / 2 + 1) * (ARRAY_SIZE + 1) - i,
                11,
                None,
                &guard,
            ) {
                Ok(_) => assert!(false),
                Err(result) => match result {
                    Error::Duplicated(entry) => {
                        assert_eq!(entry, ((ARRAY_SIZE / 2 + 1) * (ARRAY_SIZE + 1) - i, 11))
                    }
                    Error::Full(_) => assert!(false),
                    Error::Retry(_) => assert!(false),
                },
            }
        }
        for i in 0..ARRAY_SIZE {
            assert!(node
                .insert((ARRAY_SIZE + 2) * (ARRAY_SIZE + 1) - i, 10, None, &guard)
                .is_ok());
            match node.insert((ARRAY_SIZE + 2) * (ARRAY_SIZE + 1) - i, 11, None, &guard) {
                Ok(_) => assert!(false),
                Err(result) => match result {
                    Error::Duplicated(entry) => {
                        assert_eq!(entry, ((ARRAY_SIZE + 2) * (ARRAY_SIZE + 1) - i, 11))
                    }
                    Error::Full(_) => assert!(false),
                    Error::Retry(_) => assert!(false),
                },
            }
        }

        // full
        match node.insert(240, 11, None, &guard) {
            Ok(_) => assert!(false),
            Err(result) => match result {
                Error::Duplicated(_) => assert!(false),
                Error::Full(entry) => assert_eq!(entry, (240, 11)),
                Error::Retry(_) => assert!(false),
            },
        }

        // retry
        match node.insert(240, 11, None, &guard) {
            Ok(_) => assert!(false),
            Err(result) => match result {
                Error::Duplicated(_) => assert!(false),
                Error::Full(_) => assert!(false),
                Error::Retry(entry) => assert_eq!(entry, (240, 11)),
            },
        }

        let mut scanner = LeafNodeScanner::new(&node, &guard);
        let mut iterated = 0;
        let mut prev = 0;
        while let Some(entry) = scanner.next() {
            assert!(prev < *entry.0);
            assert_eq!(*entry.1, 10);
            prev = *entry.0;
            iterated += 1;
            let searched = node.search(entry.0, &guard);
            assert_eq!(
                searched.map_or_else(
                    || 0,
                    |scanner| scanner.get().map_or_else(|| 0, |entry| *entry.1)
                ),
                10
            )
        }
        assert_eq!(iterated, ARRAY_SIZE * (ARRAY_SIZE + 1) - ARRAY_SIZE / 2);
    }

    #[test]
    fn internal_leaf_node() {
        let guard = crossbeam_epoch::pin();
        // sequential
        let node = Node::new(1);
        for key in (0..(ARRAY_SIZE * ARRAY_SIZE * ARRAY_SIZE)).rev() {
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
                        println!("{}", key);
                        for key_to_check in (key + 1)..(ARRAY_SIZE * ARRAY_SIZE * ARRAY_SIZE) {
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
    }

    #[test]
    fn internal_node() {
        let guard = crossbeam_epoch::pin();
        // sequential
        let node = Node::new(2);
        for key in 0..(ARRAY_SIZE * ARRAY_SIZE * ARRAY_SIZE) {
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
                        println!("{}", key);
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
    }
}
