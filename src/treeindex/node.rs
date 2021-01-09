extern crate scopeguard;

use super::leaf::LeafScanner;
use super::leafnode::{LeafNode, LeafNodeAnchor, LeafNodeScanner};
use super::Error;
use super::Leaf;
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use std::cmp::Ordering;
use std::fmt::Display;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// Node types.
enum NodeType<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    Internal(InternalNode<K, V>),
    Leaf(LeafNode<K, V>),
}

/// Node.
pub struct Node<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    entry: NodeType<K, V>,
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Node<K, V> {
    pub fn new(floor: usize) -> Node<K, V> {
        Node {
            entry: if floor > 0 {
                NodeType::Internal(InternalNode::new(floor))
            } else {
                NodeType::Leaf(LeafNode::new())
            },
        }
    }

    /// Searches for the given key.
    pub fn search<'a>(&'a self, key: &'a K, guard: &'a Guard) -> Option<&'a V> {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.search(key, guard),
            NodeType::Leaf(leaf_node) => leaf_node.search(key, guard),
        }
    }

    /// Returns the minimum key-value pair.
    pub fn min<'a>(&'a self, guard: &'a Guard) -> Option<LeafNodeScanner<'a, K, V>> {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.min(guard),
            NodeType::Leaf(leaf_node) => leaf_node.min(guard),
        }
    }

    /// Inserts a key-value pair.
    ///
    /// It is a recursive call, and therefore stack-overflow may occur.
    pub fn insert(&self, key: K, value: V, guard: &Guard) -> Result<(), Error<K, V>> {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.insert(key, value, guard),
            NodeType::Leaf(leaf_node) => leaf_node.insert(key, value, guard),
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
        let new_root: Node<K, V> = if let NodeType::Internal(internal_node) = &self.entry {
            Node::new(internal_node.floor + 1)
        } else {
            Node::new(1)
        };
        if let NodeType::Internal(internal_node) = &new_root.entry {
            internal_node
                .children
                .1
                .store(Owned::new(Node::new(internal_node.floor - 1)), Relaxed);
            if internal_node
                .split_node(
                    entry,
                    None,
                    root_ptr.load(Relaxed, guard),
                    root_ptr,
                    true,
                    guard,
                )
                .is_ok()
            {
                let mut new_nodes = unsafe {
                    internal_node
                        .new_children
                        .swap(Shared::null(), Release, guard)
                        .into_owned()
                };

                // insert the newly allocated internal nodes into the main array
                if let Some(new_low_key_node) = new_nodes.low_key_node.take() {
                    internal_node.children.0.insert(
                        new_nodes.middle_key.take().unwrap(),
                        Atomic::from(new_low_key_node),
                        false,
                    );
                }
                if let Some(new_high_key_node) = new_nodes.high_key_node.take() {
                    internal_node
                        .children
                        .1
                        .store(Owned::from(new_high_key_node), Relaxed);
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

    /// Removes the given key.
    ///
    /// The first value of the result tuple indicates that the key has been removed.
    /// The second value of the result tuple indicates that a retry is required.
    /// The third value of the result tuple indicates that the leaf/node has become obsolete.
    pub fn remove<'a>(&'a self, key: &K, guard: &'a Guard) -> (bool, bool, bool) {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.remove(key, guard),
            NodeType::Leaf(leaf_node) => leaf_node.remove(key, guard),
        }
    }

    /// Checks if the node is full.
    fn full(&self, guard: &Guard) -> bool {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.full(guard),
            NodeType::Leaf(leaf_node) => leaf_node.full(guard),
        }
    }

    /// Checks if the node is unusable.
    fn unusable(&self, guard: &Guard) -> bool {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.unusable(guard),
            NodeType::Leaf(leaf_node) => leaf_node.unusable(guard),
        }
    }

    /// Rolls back the ongoing split operation recursively.
    fn rollback(&self, guard: &Guard) {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.rollback(guard),
            NodeType::Leaf(leaf_node) => leaf_node.rollback(guard),
        }
    }

    /// Clear all the children for drop the deprecated split nodes.
    fn unlink(&self, reset_anchor: bool) {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.unlink(reset_anchor),
            NodeType::Leaf(leaf_node) => leaf_node.unlink(),
        }
    }
}

impl<K: Clone + Display + Ord + Send + Sync, V: Clone + Display + Send + Sync> Node<K, V> {
    fn print(&self, guard: &Guard) -> usize {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.print(guard),
            NodeType::Leaf(leaf_node) => leaf_node.print(guard),
        }
    }
}

/// Internal node.
///
/// The layout of an internal node: |ptr(children)/max(child keys)|...|ptr(children)|
struct InternalNode<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// Child nodes.
    children: (Leaf<K, Atomic<Node<K, V>>>, Atomic<Node<K, V>>),
    /// New nodes in an intermediate state during merge and split.
    ///
    /// A valid pointer stored in the variable acts as a mutex for merge and split operations.
    new_children: Atomic<NewNodes<K, V>>,
    /// An anchor for scan operations that moves around during merge and split.
    leaf_node_anchor: Atomic<LeafNodeAnchor<K, V>>,
    /// The floor that the node is on.
    floor: usize,
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> InternalNode<K, V> {
    fn new(floor: usize) -> InternalNode<K, V> {
        debug_assert!(floor > 0);
        InternalNode {
            children: (Leaf::new(), Atomic::null()),
            new_children: Atomic::null(),
            leaf_node_anchor: Atomic::null(),
            floor,
        }
    }

    fn full(&self, guard: &Guard) -> bool {
        self.children.0.full() && !self.children.1.load(Relaxed, guard).is_null()
    }

    fn unusable(&self, guard: &Guard) -> bool {
        let unbounded_child = self.children.1.load(Relaxed, guard);
        if !unbounded_child.is_null() {
            self.children.0.unusable() && unsafe { unbounded_child.deref().unusable(guard) }
        } else {
            false
        }
    }

    fn search<'a>(&self, key: &'a K, guard: &'a Guard) -> Option<&'a V> {
        loop {
            let unbounded_node = (self.children.1).load(Acquire, guard);
            let result = (self.children.0).min_greater_equal(&key);
            if let Some((_, child)) = result.0 {
                let child_node = child.load(Acquire, guard);
                if !(self.children.0).validate(result.1) {
                    // data race resolution: validate metadata
                    //  - writer: start to insert an intermediate low key node
                    //  - reader: read the metadata not including the intermediate low key node
                    //  - writer: insert the intermediate low key node and replace the high key node pointer
                    //  - reader: read the new high key node pointer
                    // consequently, the reader may miss keys in the low key node
                    continue;
                }
                return unsafe { child_node.deref().search(key, guard) };
            }
            if unbounded_node == self.children.1.load(Acquire, guard) {
                if !(self.children.0).validate(result.1) {
                    // data race resolution: validate metadata - see above
                    continue;
                }
                if unbounded_node.is_null() {
                    return None;
                }
                return unsafe { unbounded_node.deref().search(key, guard) };
            }
        }
    }

    fn min<'a>(&'a self, guard: &'a Guard) -> Option<LeafNodeScanner<'a, K, V>> {
        loop {
            let mut scanner = LeafScanner::new(&self.children.0);
            if let Some(child) = scanner.next() {
                let child_node = child.1.load(Acquire, guard);
                return unsafe { child_node.deref().min(guard) };
            }
            let unbounded_node = (self.children.1).load(Acquire, guard);
            if !unbounded_node.is_null() {
                return unsafe { unbounded_node.deref().min(guard) };
            }
        }
    }

    fn insert(&self, key: K, value: V, guard: &Guard) -> Result<(), Error<K, V>> {
        // possible data race: the node is being split, for instance,
        //  - node state: ((15, ptr), (25, ptr)), 15 is being split
        //  - insert 10: min_greater_equal returns (15, ptr)
        //  - split 15: insert 11, and replace 15 with a new pointer, therefore ((11, ptr), (15, new_ptr), (25, ptr))
        //  - insert 10: load new_ptr, and try insert, that is incorrect as it is supposed to be inserted into (11, ptr)
        loop {
            let unbounded_child = self.children.1.load(Relaxed, guard);
            let result = (self.children.0).min_greater_equal(&key);
            if let Some((child_key, child)) = result.0 {
                let child_node = child.load(Acquire, guard);
                if !(self.children.0).validate(result.1) {
                    // data race resolution: validate metadata - see the 'search' function
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
                                false,
                                guard,
                            );
                        }
                        Error::Retry(_) => return Err(err),
                    },
                }
            } else if unbounded_child == self.unbounded_node(guard) {
                if !(self.children.0).validate(result.1) {
                    // data race resolution: validate metadata - see the 'search' function
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
                                &(self.children.1),
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

    /// Returns or allocates a new unbounded node
    fn unbounded_node<'a>(&self, guard: &'a Guard) -> Shared<'a, Node<K, V>> {
        let shared_ptr = self.children.1.load(Relaxed, guard);
        if shared_ptr.is_null() {
            match self.children.1.compare_and_set(
                Shared::null(),
                Owned::new(Node::new(self.floor - 1)),
                Relaxed,
                guard,
            ) {
                Ok(result) => return result,
                Err(result) => return result.current,
            }
        }
        shared_ptr
    }

    /// Splits a full node.
    fn split_node(
        &self,
        entry: (K, V),
        full_node_key: Option<K>,
        full_node_shared: Shared<Node<K, V>>,
        full_node_ptr: &Atomic<Node<K, V>>,
        root_node_split: bool,
        guard: &Guard,
    ) -> Result<(), Error<K, V>> {
        let mut new_split_nodes;
        match self.new_children.compare_and_set(
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
            let unused_children = self.new_children.swap(Shared::null(), Relaxed, guard);
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
            NodeType::Internal(full_internal_node) => {
                debug_assert!(self.leaf_node_anchor.load(Relaxed, guard).is_null());
                debug_assert!(!full_internal_node
                    .new_children
                    .load(Relaxed, guard)
                    .is_null());
                let new_children_ref = unsafe {
                    full_internal_node
                        .new_children
                        .load(Relaxed, guard)
                        .deref_mut()
                };

                // copy nodes except for the known full node to the newly allocated internal node entries
                let internal_nodes = (
                    Box::new(Node::new(full_internal_node.floor)),
                    Box::new(Node::new(full_internal_node.floor)),
                );
                let low_key_node_anchor =
                    full_internal_node
                        .leaf_node_anchor
                        .swap(Shared::null(), Relaxed, guard);
                let low_key_nodes =
                    if let NodeType::Internal(low_key_internal_node) = &internal_nodes.0.entry {
                        // copy the full node's anchor to the
                        // move the full node's anchor to the low key node
                        low_key_internal_node.leaf_node_anchor.swap(
                            low_key_node_anchor,
                            Relaxed,
                            guard,
                        );
                        Some(&low_key_internal_node.children)
                    } else {
                        None
                    };
                let (high_key_nodes, high_key_node_anchor) =
                    if let NodeType::Internal(high_key_internal_node) = &internal_nodes.1.entry {
                        // the low key node will link the last node to the high key node anchor
                        if high_key_internal_node.floor == 1 {
                            high_key_internal_node
                                .leaf_node_anchor
                                .store(Owned::new(LeafNodeAnchor::new()), Relaxed);
                            (
                                Some(&high_key_internal_node.children),
                                Some(&high_key_internal_node.leaf_node_anchor),
                            )
                        } else {
                            (Some(&high_key_internal_node.children), None)
                        }
                    } else {
                        (None, None)
                    };

                // if the origin is an unbounded node, assign the high key node to the high key node's unbounded,
                // otherwise, assign the unbounded node to the high key node's unbounded.
                let array_size = full_internal_node.children.0.cardinality();
                let low_key_node_array_size = array_size / 2;
                let high_key_node_array_size = array_size - low_key_node_array_size;
                let mut current_low_key_node_array_size = 0;
                let mut current_high_key_node_array_size = 0;
                let mut entry_to_link_to_anchor = Shared::null();
                for entry in LeafScanner::new(&full_internal_node.children.0) {
                    let mut entries: [Option<(K, Atomic<Node<K, V>>)>; 2] = [None, None];
                    if new_children_ref
                        .origin_node_key
                        .as_ref()
                        .map_or_else(|| false, |key| entry.0.cmp(key) == Ordering::Equal)
                    {
                        // link state adjustment not required as the linked list is correctly constructed by the remediate_leaf_node_link function
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
                                    .store(child_node_ptr, Relaxed);
                                if high_key_node_anchor.is_some() {
                                    // the entry needs to be linked to the high key node anchor once the anchor is updated
                                    entry_to_link_to_anchor = child_node_ptr;
                                }
                                current_low_key_node_array_size += 1;
                            } else if current_high_key_node_array_size < high_key_node_array_size {
                                if current_high_key_node_array_size == 0 {
                                    // update the anchor
                                    //  - the first entry is of the high key node is anchored
                                    if let Some(anchor) = high_key_node_anchor.as_ref() {
                                        let high_key_node_leaf_anchor = anchor.load(Relaxed, guard);
                                        let entry_ref =
                                            unsafe { entry.1.load(Relaxed, guard).deref() };
                                        if let NodeType::Leaf(leaf_node) = &entry_ref.entry {
                                            unsafe {
                                                high_key_node_leaf_anchor.deref().set(
                                                    Atomic::from(leaf_node as *const _),
                                                    guard,
                                                );
                                            }
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
                if new_children_ref.origin_node_key.is_some() {
                    let unbounded_node = full_internal_node.children.1.load(Acquire, guard);
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
                    let entry_ref = unsafe { entry_to_link_to_anchor.deref() };
                    if let NodeType::Leaf(leaf_node) = &entry_ref.entry {
                        leaf_node.update_next_node_anchor(anchor.load(Relaxed, guard));
                        leaf_node.update_side_link(Shared::null());
                    }
                }

                // turn the new nodes into internal nodes
                new_split_nodes_ref
                    .low_key_node_anchor
                    .store(low_key_node_anchor, Relaxed);
                new_split_nodes_ref.low_key_node.replace(internal_nodes.0);
                new_split_nodes_ref.high_key_node.replace(internal_nodes.1);
            }
            NodeType::Leaf(leaf_node) => {
                // copy leaves except for the known full leaf to the newly allocated leaf node entries
                let leaf_nodes = (Box::new(Node::new(0)), Box::new(Node::new(0)));
                let low_key_leaf_node =
                    if let NodeType::Leaf(low_key_leaf_node) = &leaf_nodes.0.entry {
                        Some(low_key_leaf_node)
                    } else {
                        None
                    };
                let high_key_leaf_node =
                    if let NodeType::Leaf(high_key_leaf_node) = &leaf_nodes.1.entry {
                        Some(high_key_leaf_node)
                    } else {
                        None
                    };
                leaf_node
                    .split_leaf_node(
                        low_key_leaf_node.unwrap(),
                        high_key_leaf_node.unwrap(),
                        guard,
                    )
                    .map(|middle_key| new_split_nodes_ref.middle_key.replace(middle_key));

                // reconstruct the linked list
                let immediate_smaller_key_node = self
                    .children
                    .0
                    .max_less(new_split_nodes_ref.middle_key.as_ref().unwrap());
                let immediate_smaller_key_leaf_node =
                    if let Some((_, ptr)) = immediate_smaller_key_node {
                        let entry_ptr = ptr.load(Acquire, guard);
                        if !entry_ptr.is_null() {
                            if let NodeType::Leaf(leaf_node) = unsafe { &entry_ptr.deref().entry } {
                                Some(leaf_node)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                leaf_node.remediate_leaf_node_link(
                    &self.leaf_node_anchor,
                    immediate_smaller_key_leaf_node,
                    low_key_leaf_node.unwrap(),
                    high_key_leaf_node.unwrap(),
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
        if let Some(node) = self.children.0.insert(
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
        let new_split_nodes = self.new_children.swap(Shared::null(), Release, guard);
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

    fn rollback(&self, guard: &Guard) {
        let new_children_ref = unsafe { self.new_children.load(Relaxed, guard).deref() };
        let origin_node_ref = unsafe {
            new_children_ref
                .origin_node_ptr
                .load(Relaxed, guard)
                .deref()
        };
        if let NodeType::Leaf(origin_leaf_node) = &origin_node_ref.entry {
            // need to rollback the linked list modification
            let immediate_smaller_key_node = self
                .children
                .0
                .max_less(new_children_ref.middle_key.as_ref().unwrap());
            let immediate_smaller_key_leaf_node = if let Some((_, ptr)) = immediate_smaller_key_node
            {
                let entry_ptr = ptr.load(Acquire, guard);
                if !entry_ptr.is_null() {
                    if let NodeType::Leaf(leaf_node) = unsafe { &entry_ptr.deref().entry } {
                        Some(leaf_node)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(node) = immediate_smaller_key_leaf_node {
                // need to update the side link first as there can be readers who are traversing the linked list
                node.update_side_link(
                    Atomic::from(origin_leaf_node as *const _).load(Relaxed, guard),
                );
                node.update_next_node_anchor(Shared::null());
            } else {
                let anchor = self.leaf_node_anchor.load(Relaxed, guard);
                if !anchor.is_null() {
                    unsafe {
                        anchor
                            .deref()
                            .set(Atomic::from(origin_leaf_node as *const _), guard)
                    };
                }
            }
        } else if self.floor == 2 {
            // restore the anchor
            if let NodeType::Internal(internal_node) = &origin_node_ref.entry {
                debug_assert_eq!(internal_node.floor, 1);
                let origin_anchor =
                    new_children_ref
                        .low_key_node_anchor
                        .swap(Shared::null(), Relaxed, guard);
                internal_node
                    .leaf_node_anchor
                    .swap(origin_anchor, Relaxed, guard);
            }
        }

        let intermediate_split = unsafe {
            self.new_children
                .swap(Shared::null(), Relaxed, guard)
                .into_owned()
        };
        let child = intermediate_split
            .origin_node_ptr
            .swap(Shared::null(), Relaxed, guard);
        unsafe { child.deref().rollback(guard) };
    }

    fn remove<'a>(&'a self, key: &K, guard: &'a Guard) -> (bool, bool, bool) {
        loop {
            let unbounded_node = (self.children.1).load(Acquire, guard);
            let result = (self.children.0).min_greater_equal(&key);
            if let Some((_, child)) = result.0 {
                let child_node = child.load(Acquire, guard);
                if !(self.children.0).validate(result.1) {
                    // data race resolution: validate metadata - see the 'search' function
                    continue;
                }
                return unsafe { child_node.deref().remove(key, guard) };
            }
            if unbounded_node == self.children.1.load(Acquire, guard) {
                if !(self.children.0).validate(result.1) {
                    // data race resolution: validate metadata - see the 'search' function
                    continue;
                }
                if unbounded_node.is_null() {
                    return (false, false, false);
                }
                return unsafe { unbounded_node.deref().remove(key, guard) };
            }
        }
    }

    /// Removes the full node.
    ///
    /// The first value of the result tuple indicates that a retry is required.
    /// The second value of the result tuple indicates that the current node has become obsolete.
    fn remove_full_node(
        &self,
        unusable: bool,
        child_key: Option<&K>,
        node_shared: Shared<Node<K, V>>,
        node_ptr: &Atomic<Node<K, V>>,
        guard: &Guard,
    ) -> (bool, bool) {
        // there is a chance that the target key value pair has been copied to new_leaves
        if !unusable {
            if !self.new_children.load(Acquire, guard).is_null() {
                return (true, false);
            }
            if node_ptr.load(Relaxed, guard) != node_shared {
                return (true, false);
            }
            (false, false)
        } else {
            // if locked and the pointer has remained the same, invalidate the leaf, and return invalid
            let mut new_nodes_dummy = NewNodes {
                origin_node_key: None,
                origin_node_ptr: Atomic::null(),
                low_key_node_anchor: Atomic::null(),
                low_key_node: None,
                middle_key: None,
                high_key_node: None,
            };
            if let Err(error) = self.new_children.compare_and_set(
                Shared::null(),
                unsafe { Owned::from_raw(&mut new_nodes_dummy as *mut NewNodes<K, V>) },
                Acquire,
                guard,
            ) {
                error.new.into_shared(guard);
                return (true, false);
            }
            let lock_guard = scopeguard::guard(&self.new_children, |new_children| {
                new_children.store(Shared::null(), Release);
            });

            let obsolete_node = node_ptr.load(Relaxed, guard);
            if obsolete_node != node_shared {
                return (true, false);
            }

            let obsolete = child_key.map_or_else(
                || {
                    // unbounded node: do not deallocate
                    self.children.0.unusable()
                },
                |key| {
                    // bounded node
                    let obsolete = self.children.0.remove(key).2;
                    // once the key is removed, it is safe to deallocate the leaf as the validation loop ensures the absence of readers
                    node_ptr.store(Shared::null(), Release);

                    // [TODO]
                    // Remediate the linked list
                    match unsafe { &obsolete_node.deref().entry } {
                        NodeType::Internal(internal_node) => {}
                        NodeType::Leaf(leaf_node) => {}
                    };

                    unsafe { guard.defer_destroy(obsolete_node) };
                    if obsolete {
                        let unbounded_node = self.children.1.load(Acquire, guard);
                        if unbounded_node.is_null() {
                            false
                        } else {
                            unsafe { unbounded_node.deref().unusable(guard) }
                        }
                    } else {
                        false
                    }
                },
            );

            drop(lock_guard);
            (false, obsolete)
        }
    }

    fn unlink(&self, reset_anchor: bool) {
        for entry in LeafScanner::new(&self.children.0) {
            entry.1.store(Shared::null(), Relaxed);
        }
        self.children.1.store(Shared::null(), Relaxed);
        if reset_anchor {
            self.leaf_node_anchor.store(Shared::null(), Relaxed);
        }
    }
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Drop for InternalNode<K, V> {
    fn drop(&mut self) {
        let guard = crossbeam_epoch::pin();
        // destroy entries related to the unused child
        let unused_nodes = self.new_children.load(Acquire, &guard);
        if !unused_nodes.is_null() {
            // destroy only the origin node, assuming that the rest are copied
            debug_assert!(self.leaf_node_anchor.load(Relaxed, &guard).is_null());
            let unused_nodes = unsafe { unused_nodes.into_owned() };
            let obsolete_node = unused_nodes.origin_node_ptr.load(Relaxed, &guard);
            if !obsolete_node.is_null() {
                drop(unsafe { obsolete_node.into_owned() });
            }
        } else {
            // destroy all: in order to avoid stack overflow, destroy them without the thread pinned
            for entry in LeafScanner::new(&self.children.0) {
                let child = entry.1.load(Acquire, &guard);
                if !child.is_null() {
                    drop(unsafe { child.into_owned() });
                }
            }
            let unbounded_child = self.children.1.load(Acquire, &guard);
            if !unbounded_child.is_null() {
                drop(unsafe { unbounded_child.into_owned() });
            }
            let anchor = self.leaf_node_anchor.load(Relaxed, &guard);
            if !anchor.is_null() {
                drop(unsafe { anchor.into_owned() });
            }
        }
    }
}

impl<K: Clone + Display + Ord + Send + Sync, V: Clone + Display + Send + Sync> InternalNode<K, V> {
    fn print(&self, guard: &Guard) -> usize {
        let mut scanned = 0;
        for (index, entry) in LeafScanner::new(&self.children.0).enumerate() {
            println!(
                "floor: {}, index: {}, node_key: {}",
                self.floor, index, entry.0
            );
            let child_ref = unsafe { entry.1.load(Relaxed, &guard).deref() };
            scanned += child_ref.print(guard);
        }
        println!("floor {}, unbounded node", self.floor);
        let unbounded_ptr = self.children.1.load(Relaxed, &guard);
        if unbounded_ptr.is_null() {
            println!(" null");
            return scanned;
        }
        let unbounded_ref = unsafe { unbounded_ptr.deref() };
        scanned += unbounded_ref.print(guard);
        scanned
    }
}

/// Intermediate split node.
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
        if let Some(node) = self.low_key_node.take() {
            node.unlink(true)
        }
        if let Some(node) = self.high_key_node.take() {
            node.unlink(false)
        }
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
        let num_entries = node.print(&crossbeam_epoch::pin());
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
