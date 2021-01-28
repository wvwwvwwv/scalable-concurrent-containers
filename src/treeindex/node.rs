use super::leaf::{LeafScanner, ARRAY_SIZE};
use super::leafnode::{LeafNode, LeafNodeAnchor, LeafNodeLocker, LeafNodeScanner};
use super::Leaf;
use super::{InsertError, RemoveError, SearchError};
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
    /// Stores an array of child nodes/leaves, interleaving keys, and metadata.
    entry: NodeType<K, V>,
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Node<K, V> {
    pub fn new(floor: usize, allocate_unbounded_node: bool) -> Node<K, V> {
        Node {
            entry: if floor > 0 {
                NodeType::Internal(InternalNode::new(floor, allocate_unbounded_node))
            } else {
                NodeType::Leaf(LeafNode::new(allocate_unbounded_node))
            },
        }
    }

    /// Takes the memory address of self.entry as an identifier.
    pub fn id(&self) -> usize {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.id(),
            NodeType::Leaf(leaf_node) => leaf_node.id(),
        }
    }

    /// Checks if the node is obsolete.
    pub fn obsolete(&self, check_unbounded: bool, guard: &Guard) -> bool {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.obsolete(check_unbounded, guard),
            NodeType::Leaf(leaf_node) => leaf_node.obsolete(check_unbounded, guard),
        }
    }

    /// Searches for the given key.
    pub fn search<'a>(
        &'a self,
        key: &'a K,
        guard: &'a Guard,
    ) -> Result<Option<&'a V>, SearchError> {
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

    /// Returns a LeafNodeScanner pointing to a key-value pair that is adequately similar to the given key.
    ///
    /// It may return a scanner pointing to a key that is less than the given key,
    /// however, it does not return a scanner pointing to a key that is,
    ///  - 1. Greater than the given key when the given key exists.
    ///  - 2. Greater than any other keys that are greater than the given key.
    pub fn from<'a>(&'a self, key: &K, guard: &'a Guard) -> Option<LeafNodeScanner<'a, K, V>> {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.from(key, guard),
            NodeType::Leaf(leaf_node) => leaf_node.from(key, guard),
        }
    }

    /// Inserts a key-value pair.
    ///
    /// It is a recursive call, and therefore stack-overflow may occur.
    pub fn insert(&self, key: K, value: V, guard: &Guard) -> Result<(), InsertError<K, V>> {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.insert(key, value, guard),
            NodeType::Leaf(leaf_node) => leaf_node.insert(key, value, guard),
        }
    }

    /// Splits the current root node.
    pub fn split_root(&self, entry: (K, V), root_ptr: &Atomic<Node<K, V>>, guard: &Guard) {
        // The fact that the TreeIndex calls this function means that the root is in a split procedure,
        // and the procedure has not been intervened by other threads, thus concluding that the root has stayed the same.
        debug_assert_eq!(
            self as *const Node<K, V>,
            root_ptr.load(Relaxed, guard).as_raw()
        );
        let new_root: Node<K, V> = if let NodeType::Internal(internal_node) = &self.entry {
            Node::new(internal_node.floor + 1, false)
        } else {
            Node::new(1, false)
        };
        if let NodeType::Internal(internal_node) = &new_root.entry {
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

                // Inserts the newly allocated internal nodes into the main array.
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
    pub fn remove<'a>(&'a self, key: &K, guard: &'a Guard) -> Result<bool, RemoveError> {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.remove(key, guard),
            NodeType::Leaf(leaf_node) => leaf_node.remove(key, guard),
        }
    }

    /// Updates the current root node.
    pub fn update_root(
        mut current_root: Shared<Node<K, V>>,
        root_ptr: &Atomic<Node<K, V>>,
        guard: &Guard,
    ) {
        while let NodeType::Internal(internal_node) = unsafe { &current_root.deref().entry } {
            // If locked and the pointer has remained the same, invalidate the leaf, and return invalid.
            let root_lock = InternalNodeLocker::lock(internal_node, guard);
            if root_lock.is_none() {
                return;
            }
            let new_root = internal_node.children.1.load(Acquire, guard);
            if new_root.is_null() {
                return;
            }

            match unsafe { &new_root.deref().entry } {
                NodeType::Internal(new_root_internal) => {
                    let new_root_lock = InternalNodeLocker::lock(new_root_internal, guard);
                    if new_root_lock.is_some() {
                        if root_ptr
                            .compare_and_set(current_root, new_root, Release, guard)
                            .is_ok()
                        {
                            internal_node.children.1.store(Shared::null(), Relaxed);
                            unsafe {
                                debug_assert!(current_root.deref().obsolete(true, guard));
                                guard.defer_destroy(current_root)
                            };
                            if new_root_internal.obsolete(false, guard) {
                                current_root = Shared::from(new_root.as_raw());
                                continue;
                            }
                        }
                    }
                }
                NodeType::Leaf(new_root_leaf) => {
                    let new_root_lock = LeafNodeLocker::lock(new_root_leaf, guard);
                    if new_root_lock.is_some() {
                        if root_ptr
                            .compare_and_set(current_root, new_root, Release, guard)
                            .is_ok()
                        {
                            internal_node.children.1.store(Shared::null(), Relaxed);
                            unsafe {
                                debug_assert!(current_root.deref().obsolete(true, guard));
                                guard.defer_destroy(current_root)
                            };
                        }
                    }
                }
            }
            return;
        }
    }

    /// Returns the floor.
    pub fn floor(&self) -> usize {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.floor,
            NodeType::Leaf(_) => 0,
        }
    }

    /// Rolls back the ongoing split operation recursively.
    fn rollback(&self, guard: &Guard) {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.rollback(guard),
            NodeType::Leaf(leaf_node) => leaf_node.rollback(guard),
        }
    }

    /// Clears all the children for drop the deprecated split nodes.
    fn unlink(&self, reset_anchor: bool) {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.unlink(reset_anchor),
            NodeType::Leaf(leaf_node) => leaf_node.unlink(),
        }
    }

    /// Tries to merge two adjacent nodes.
    ///
    /// The prev_prev_node argument being a valid reference to a Node means that the linked list has to be updated.
    fn try_merge(
        &self,
        prev_node_key: &K,
        prev_node: &Node<K, V>,
        prev_prev_node: Option<&Node<K, V>>,
        guard: &Guard,
    ) -> bool {
        match (&self.entry, &prev_node.entry) {
            (NodeType::Internal(internal_node), NodeType::Internal(prev_internal_node)) => {
                internal_node.try_merge(prev_node_key, prev_internal_node, guard)
            }
            (NodeType::Leaf(leaf_node), NodeType::Leaf(prev_leaf_node)) => prev_prev_node
                .map_or_else(
                    || leaf_node.try_merge(prev_node_key, prev_leaf_node, None, guard),
                    |prev_prev_node| {
                        if let NodeType::Leaf(prev_prev_leaf_node) = &prev_prev_node.entry {
                            leaf_node.try_merge(
                                prev_node_key,
                                prev_leaf_node,
                                Some(prev_prev_leaf_node),
                                guard,
                            )
                        } else {
                            false
                        }
                    },
                ),
            (_, _) => false,
        }
    }
}

impl<K: Clone + Display + Ord + Send + Sync, V: Clone + Display + Send + Sync> Node<K, V> {
    pub fn print<T: std::io::Write>(&self, output: &mut T, guard: &Guard) -> std::io::Result<()> {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.print(output, guard),
            NodeType::Leaf(leaf_node) => leaf_node.print(output, guard),
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
    fn new(floor: usize, allocate_unbounded_node: bool) -> InternalNode<K, V> {
        debug_assert!(floor > 0);
        let unbounded_node: Atomic<Node<K, V>> = if allocate_unbounded_node {
            Atomic::from(Owned::new(Node::new(floor - 1, allocate_unbounded_node)))
        } else {
            Atomic::null()
        };
        InternalNode {
            children: (Leaf::new(), unbounded_node),
            new_children: Atomic::null(),
            leaf_node_anchor: Atomic::null(),
            floor,
        }
    }

    /// Takes the memory address of the instance as an identifier.
    fn id(&self) -> usize {
        self as *const _ as usize
    }

    /// Checks if the internal node is obsolete.
    fn obsolete(&self, check_unbounded: bool, guard: &Guard) -> bool {
        self.children.0.obsolete()
            && (!check_unbounded || self.children.1.load(Relaxed, guard).is_null())
    }

    fn search<'a>(&self, key: &'a K, guard: &'a Guard) -> Result<Option<&'a V>, SearchError> {
        loop {
            let unbounded_node = (self.children.1).load(Acquire, guard);
            let result = (self.children.0).min_greater_equal(&key);
            if let Some((_, child)) = result.0 {
                let child_node = child.load(Acquire, guard);
                if !(self.children.0).validate(result.1) {
                    // Data race resolution: validate metadata.
                    //  - writer: start to insert an intermediate low key node
                    //  - reader: read the metadata not including the intermediate low key node
                    //  - writer: insert the intermediate low key node and replace the high key node pointer
                    //  - reader: read the new high key node pointer
                    // Consequently, the reader may miss keys in the low key node.
                    // In order to resolve this, the leaf metadata is validated.
                    continue;
                }
                return unsafe { child_node.deref().search(key, guard) };
            }
            if unbounded_node == self.children.1.load(Acquire, guard) {
                if !(self.children.0).validate(result.1) {
                    // Data race resolution: validate metadata - see above.
                    continue;
                }
                if unbounded_node.is_null() {
                    // unbounded_node being null indicates that the node is bound to be freed.
                    return Err(SearchError::Retry);
                }
                return unsafe { unbounded_node.deref().search(key, guard) };
            }
        }
    }

    fn min<'a>(&'a self, guard: &'a Guard) -> Option<LeafNodeScanner<'a, K, V>> {
        let mut scanner = LeafScanner::new(&self.children.0);
        while let Some(child) = scanner.next() {
            let child_node = child.1.load(Acquire, guard);
            if let Some(leaf_scanner) = unsafe { child_node.deref().min(guard) } {
                return Some(leaf_scanner);
            }
        }
        let unbounded_node = (self.children.1).load(Acquire, guard);
        if !unbounded_node.is_null() {
            return unsafe { unbounded_node.deref().min(guard) };
        }
        None
    }

    fn from<'a>(&'a self, key: &K, guard: &'a Guard) -> Option<LeafNodeScanner<'a, K, V>> {
        loop {
            let unbounded_node = (self.children.1).load(Acquire, guard);
            let mut scanner = LeafScanner::new(&self.children.0);
            let metadata = scanner.metadata();
            let mut retry = false;
            while let Some(child) = scanner.next() {
                if child.0.cmp(key) == Ordering::Less {
                    continue;
                }
                let child_node = child.1.load(Acquire, guard);
                if !(self.children.0).validate(metadata) {
                    // Data race resolution: validate metadata - see 'InternalNode::search'.
                    retry = true;
                    break;
                }
                if let Some(leaf_scanner) = unsafe { child_node.deref().from(key, guard) } {
                    return Some(leaf_scanner);
                }
            }
            if retry {
                continue;
            }
            if unbounded_node == self.children.1.load(Acquire, guard) {
                if !(self.children.0).validate(metadata) {
                    // Data race resolution: validate metadata - see above.
                    continue;
                }
            }
            if !unbounded_node.is_null() {
                return unsafe { unbounded_node.deref().from(key, guard) };
            }
            return None;
        }
    }

    fn insert(&self, key: K, value: V, guard: &Guard) -> Result<(), InsertError<K, V>> {
        // Possible data race: the node is being split, for instance,
        //  - Node state: ((15, ptr), (25, ptr)), 15 is being split
        //  - Insert 10: min_greater_equal returns (15, ptr)
        //  - Split 15: insert 11, and replace 15 with a new pointer, therefore ((11, ptr), (15, new_ptr), (25, ptr))
        //  - Insert 10: load new_ptr, and try insert, that is incorrect as it is supposed to be inserted into (11, ptr)
        loop {
            let unbounded_node = self.children.1.load(Relaxed, guard);
            let result = (self.children.0).min_greater_equal(&key);
            if let Some((child_key, child)) = result.0 {
                let child_node = child.load(Acquire, guard);
                if !(self.children.0).validate(result.1) {
                    // Data race resolution: validate metadata - see 'InternalNode::search'.
                    continue;
                }
                match unsafe { child_node.deref().insert(key, value, guard) } {
                    Ok(_) => return Ok(()),
                    Err(err) => match err {
                        InsertError::Duplicated(_) => return Err(err),
                        InsertError::Full(entry) => {
                            return self.split_node(
                                entry,
                                Some(child_key.clone()),
                                child_node,
                                &child,
                                false,
                                guard,
                            );
                        }
                        InsertError::Retry(_) => return Err(err),
                    },
                }
            } else if unbounded_node == self.children.1.load(Relaxed, guard) {
                if !(self.children.0).validate(result.1) {
                    // Data race resolution: validate metadata - see 'InternalNode::search'.
                    continue;
                }
                if unbounded_node.is_null() {
                    // unbounded_node being null indicates that the leaf node is bound to be freed.
                    return Err(InsertError::Retry((key, value)));
                }
                // Tries to insert into the unbounded child, and tries to split the unbounded if it is full.
                match unsafe { unbounded_node.deref().insert(key, value, guard) } {
                    Ok(_) => return Ok(()),
                    Err(err) => match err {
                        InsertError::Duplicated(_) => return Err(err),
                        InsertError::Full(entry) => {
                            return self.split_node(
                                entry,
                                None,
                                unbounded_node,
                                &(self.children.1),
                                false,
                                guard,
                            );
                        }
                        InsertError::Retry(_) => return Err(err),
                    },
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
        root_node_split: bool,
        guard: &Guard,
    ) -> Result<(), InsertError<K, V>> {
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
                return Err(InsertError::Retry(entry));
            }
        }

        // Checks if the node is ready for a child split.
        if full_node_shared != full_node_ptr.load(Relaxed, guard) {
            // The unbounded child being null indicates that the node is being dropped,
            // and the mismatch between pointer values indicates that there has been a change to the node.
            let unused_children = self.new_children.swap(Shared::null(), Relaxed, guard);
            drop(unsafe { unused_children.into_owned() });
            unsafe {
                full_node_shared.deref().rollback(guard);
            };
            return Err(InsertError::Retry(entry));
        }

        // Copies entries to the newly allocated leaves.
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

                // Copies nodes except for the known full node to the newly allocated internal node entries.
                let internal_nodes = (
                    Box::new(Node::new(full_internal_node.floor, false)),
                    Box::new(Node::new(full_internal_node.floor, false)),
                );
                let low_key_node_anchor =
                    full_internal_node
                        .leaf_node_anchor
                        .swap(Shared::null(), Relaxed, guard);
                let low_key_nodes =
                    if let NodeType::Internal(low_key_internal_node) = &internal_nodes.0.entry {
                        // Moves the full node's anchor to the low key node.
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
                        // The low key node will link the last node to the high key node anchor.
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

                // Builds a list of valid nodes.
                let mut entry_array: [Option<(Option<&K>, Atomic<Node<K, V>>)>; ARRAY_SIZE + 2] = [
                    None, None, None, None, None, None, None, None, None, None, None, None, None,
                    None,
                ];
                let mut num_entries = 0;
                for entry in LeafScanner::new(&full_internal_node.children.0) {
                    if new_children_ref
                        .origin_node_key
                        .as_ref()
                        .map_or_else(|| false, |key| entry.0.cmp(key) == Ordering::Equal)
                    {
                        if let Some(node) = new_children_ref.low_key_node.take() {
                            entry_array[num_entries].replace((
                                Some(new_children_ref.middle_key.as_ref().unwrap()),
                                Atomic::from(node),
                            ));
                            num_entries += 1;
                        }
                        if let Some(node) = new_children_ref.high_key_node.take() {
                            entry_array[num_entries].replace((Some(entry.0), Atomic::from(node)));
                            num_entries += 1;
                        }
                    } else {
                        entry_array[num_entries].replace((Some(entry.0), entry.1.clone()));
                        num_entries += 1;
                    }
                }
                if new_children_ref.origin_node_key.is_some() {
                    // If the origin is a bounded node, assign the unbounded node to the high key node's unbounded.
                    entry_array[num_entries].replace((None, full_internal_node.children.1.clone()));
                    num_entries += 1;
                } else {
                    // If the origin is an unbounded node, assign the high key node to the high key node's unbounded.
                    if let Some(node) = new_children_ref.low_key_node.take() {
                        entry_array[num_entries].replace((
                            Some(new_children_ref.middle_key.as_ref().unwrap()),
                            Atomic::from(node),
                        ));
                        num_entries += 1;
                    }
                    if let Some(node) = new_children_ref.high_key_node.take() {
                        entry_array[num_entries].replace((None, Atomic::from(node)));
                        num_entries += 1;
                    }
                }
                debug_assert!(new_children_ref.low_key_node.is_none());
                debug_assert!(new_children_ref.high_key_node.is_none());
                debug_assert!(num_entries >= 2);

                let low_key_node_array_size = num_entries / 2;
                let mut entry_to_link_to_anchor = Shared::null();

                for (index, entry) in entry_array.iter().enumerate() {
                    if let Some(entry) = entry {
                        if (index + 1) < low_key_node_array_size {
                            low_key_nodes.as_ref().unwrap().0.insert(
                                entry.0.unwrap().clone(),
                                entry.1.clone(),
                                false,
                            );
                        } else if (index + 1) == low_key_node_array_size {
                            new_split_nodes_ref
                                .middle_key
                                .replace(entry.0.unwrap().clone());
                            let child_node_ptr = entry.1.load(Relaxed, guard);
                            low_key_nodes
                                .as_ref()
                                .unwrap()
                                .1
                                .store(child_node_ptr, Relaxed);
                            if high_key_node_anchor.is_some() {
                                // The entry needs to be linked to the high key node anchor once the anchor is updated.
                                entry_to_link_to_anchor = child_node_ptr;
                            }
                        } else {
                            if index == low_key_node_array_size {
                                // Updates the anchor.
                                if let Some(anchor) = high_key_node_anchor.as_ref() {
                                    let high_key_node_leaf_anchor = anchor.load(Relaxed, guard);
                                    let entry_ref = unsafe { entry.1.load(Relaxed, guard).deref() };
                                    if let NodeType::Leaf(leaf_node) = &entry_ref.entry {
                                        unsafe {
                                            high_key_node_leaf_anchor
                                                .deref()
                                                .set(Atomic::from(leaf_node as *const _), guard);
                                        }
                                    }
                                }
                            }
                            if let Some(key) = entry.0 {
                                high_key_nodes.as_ref().unwrap().0.insert(
                                    key.clone(),
                                    entry.1.clone(),
                                    false,
                                );
                            } else {
                                high_key_nodes
                                    .as_ref()
                                    .unwrap()
                                    .1
                                    .store(entry.1.load(Relaxed, guard), Relaxed);
                            }
                        }
                    } else {
                        break;
                    }
                }

                // Links the max low key leaf node to the new anchor.
                if let Some(anchor) = high_key_node_anchor.as_ref() {
                    let entry_ref = unsafe { entry_to_link_to_anchor.deref() };
                    if let NodeType::Leaf(leaf_node) = &entry_ref.entry {
                        leaf_node.update_link(anchor.load(Relaxed, guard), Shared::null());
                    }
                }

                // Turns the new nodes into internal nodes.
                new_split_nodes_ref
                    .low_key_node_anchor
                    .store(low_key_node_anchor, Relaxed);
                new_split_nodes_ref.low_key_node.replace(internal_nodes.0);
                new_split_nodes_ref.high_key_node.replace(internal_nodes.1);
            }
            NodeType::Leaf(leaf_node) => {
                // Copies leaves except for the known full leaf to the newly allocated leaf node entries.
                let leaf_nodes = (Box::new(Node::new(0, false)), Box::new(Node::new(0, false)));
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

                // Reconstructs the linked list.
                let immediate_smaller_key_node = self
                    .children
                    .0
                    .max_less(new_split_nodes_ref.middle_key.as_ref().unwrap());
                let immediate_smaller_key_leaf_node =
                    if let Some((_, node_ptr)) = immediate_smaller_key_node {
                        Self::cast_to_leaf_node(&node_ptr, guard)
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
                // Turns the new leaves into leaf nodes.
                new_split_nodes_ref.low_key_node.replace(leaf_nodes.0);
                new_split_nodes_ref.high_key_node.replace(leaf_nodes.1);
            }
        };

        // The full node is the current root: split_root processes the rest.
        if root_node_split {
            return Ok(());
        }

        // Inserts the newly allocated internal nodes into the main array.
        let unused_node;
        let low_key_node_ptr = Atomic::from(new_split_nodes_ref.low_key_node.take().unwrap());
        if let Some(node) = self.children.0.insert(
            new_split_nodes_ref.middle_key.take().unwrap(),
            low_key_node_ptr,
            false,
        ) {
            // Insertion failed: expect that the parent splits this node.
            new_split_nodes_ref
                .low_key_node
                .replace(unsafe { (node.0).1.into_owned().into_box() });
            new_split_nodes_ref.middle_key.replace((node.0).0);
            return Err(InsertError::Full(entry));
        }

        // Replaces the full node with the high-key node.
        unused_node = full_node_ptr.swap(
            Owned::from(new_split_nodes_ref.high_key_node.take().unwrap()),
            Release,
            &guard,
        );

        // Drops the deprecated nodes.
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
            // Needs to rollback the linked list modification.
            let immediate_smaller_key_node = self
                .children
                .0
                .max_less(new_children_ref.middle_key.as_ref().unwrap());
            let immediate_smaller_key_leaf_node =
                if let Some((_, node_ptr)) = immediate_smaller_key_node {
                    Self::cast_to_leaf_node(&node_ptr, guard)
                } else {
                    None
                };
            if let Some(node) = immediate_smaller_key_leaf_node {
                // Needs to update the side link first as there can be readers who are traversing the linked list.
                node.update_link(
                    Shared::null(),
                    Atomic::from(origin_leaf_node as *const _).load(Relaxed, guard),
                );
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
            // Restores the anchor.
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

    fn remove<'a>(&'a self, key: &K, guard: &'a Guard) -> Result<bool, RemoveError> {
        loop {
            let unbounded_node = (self.children.1).load(Acquire, guard);
            let result = (self.children.0).min_greater_equal(&key);
            if let Some((child_key, child)) = result.0 {
                let child_node = child.load(Acquire, guard);
                if !(self.children.0).validate(result.1) {
                    // Data race resolution: validate metadata - see 'InternalNode::search'.
                    continue;
                }
                return match unsafe { child_node.deref().remove(key, guard) } {
                    Ok(removed) => Ok(removed),
                    Err(remove_error) => match remove_error {
                        RemoveError::Coalesce(removed) => {
                            self.coalesce_node(removed, child_key, child_node, guard)
                        }
                        RemoveError::Retry(_) => Err(remove_error),
                    },
                };
            }
            if unbounded_node == self.children.1.load(Acquire, guard) {
                if !(self.children.0).validate(result.1) {
                    // Data race resolution: validate metadata - see 'InternalNode::search'.
                    continue;
                }
                if unbounded_node.is_null() {
                    // unbounded_node being null indicates that the leaf node is bound to be freed.
                    return Err(RemoveError::Retry(false));
                }
                return match unsafe { unbounded_node.deref().remove(key, guard) } {
                    Ok(removed) => Ok(removed),
                    Err(remove_error) => match remove_error {
                        RemoveError::Coalesce(removed) => {
                            if self.children.0.obsolete() {
                                Err(remove_error)
                            } else {
                                Ok(removed)
                            }
                        }
                        RemoveError::Retry(_) => Err(remove_error),
                    },
                };
            }
        }
    }

    fn cast_to_leaf_node<'a>(
        node_ptr: &Atomic<Node<K, V>>,
        guard: &'a Guard,
    ) -> Option<&'a LeafNode<K, V>> {
        let node_shr_ptr = node_ptr.load(Acquire, guard);
        if !node_shr_ptr.is_null() {
            if let NodeType::Leaf(leaf_node) = unsafe { &node_shr_ptr.deref().entry } {
                Some(leaf_node)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Coalesces the node with the adjacent node.
    fn coalesce_node(
        &self,
        removed: bool,
        child_key: &K,
        child_shared: Shared<Node<K, V>>,
        guard: &Guard,
    ) -> Result<bool, RemoveError> {
        // If locked and the pointer has remained the same, invalidate the node.
        let lock = InternalNodeLocker::lock(self, guard);
        if lock.is_none() {
            return Err(RemoveError::Retry(removed));
        }

        // Merges the node and the next node only if the pointers match, otherwise retry.
        let result = self.children.0.search(child_key);
        if let Some(child) = result {
            if child_shared == child.load(Relaxed, guard) {
                let child_ref = unsafe { child_shared.deref() };
                let next_node_ref = unsafe {
                    if let Some((_, next_child)) = self.children.0.min_greater(child_key) {
                        next_child.load(Acquire, guard).deref()
                    } else {
                        self.children.1.load(Acquire, guard).deref()
                    }
                };
                debug_assert!(child_ref.obsolete(false, guard));

                // If self.floor == 1, self may point to a valid anchor that needs to be updated,
                // and child_shared.side_link has to be updated.
                let prev_node_ref = if self.floor == 1 {
                    if let Some((_, prev_child)) = self.children.0.max_less(child_key) {
                        unsafe { Some(prev_child.load(Acquire, guard).deref()) }
                    } else {
                        None
                    }
                } else {
                    None
                };
                let update_anchor = self.floor == 1 && prev_node_ref.is_none();
                if !next_node_ref.try_merge(child_key, child_ref, prev_node_ref, guard) {
                    // Failed to coalesce.
                    return Ok(removed);
                }

                // Updates the leaf node anchor.
                if update_anchor {
                    let anchor = self.leaf_node_anchor.load(Relaxed, guard);
                    if !anchor.is_null() {
                        if let NodeType::Leaf(next_leaf_node) = &next_node_ref.entry {
                            unsafe {
                                anchor
                                    .deref()
                                    .set(Atomic::from(next_leaf_node as *const _), guard)
                            };
                        }
                    }
                }

                // Removes the node.
                let coalesce = self.children.0.remove(child_key, true).2;
                // Once the key is removed, it is safe to deallocate the leaf as the validation loop ensures the absence of readers.
                child.store(Shared::null(), Release);
                unsafe {
                    // Needs to nullify the unbounded node/leaf pointer as the instance is referenced by the adjacent node.
                    debug_assert!(child_shared.deref().obsolete(true, guard));
                    guard.defer_destroy(child_shared);
                };

                if coalesce {
                    return Err(RemoveError::Coalesce(removed));
                } else {
                    return Ok(removed);
                }
            }
        }
        Err(RemoveError::Retry(removed))
    }

    /// Tries to merge two adjacent internal nodes.
    fn try_merge(
        &self,
        prev_internal_node_key: &K,
        prev_internal_node: &InternalNode<K, V>,
        guard: &Guard,
    ) -> bool {
        // In order to avoid conflicts with a thread splitting the node, lock itself and prev_internal_node.
        let self_lock = InternalNodeLocker::lock(self, guard);
        if self_lock.is_none() {
            return false;
        }
        let prev_lock = InternalNodeLocker::lock(prev_internal_node, guard);
        if prev_lock.is_none() {
            return false;
        }
        debug_assert!(prev_internal_node.obsolete(false, guard));

        // Inserts the unbounded child of the previous internal node into the node array.
        let new_node_ptr = prev_internal_node.children.1.clone();
        if self
            .children
            .0
            .insert(prev_internal_node_key.clone(), new_node_ptr.clone(), false)
            .is_none()
        {
            if self.floor == 1 {
                // Makes the newly inserted node to point to the next node.
                let anchor = self.leaf_node_anchor.load(Relaxed, guard);
                let next_node_ref = unsafe {
                    if let Some((_, next_child)) =
                        self.children.0.min_greater(prev_internal_node_key)
                    {
                        next_child.load(Acquire, guard).deref()
                    } else {
                        self.children.1.load(Acquire, guard).deref()
                    }
                };
                if let (NodeType::Leaf(new_leaf_node), NodeType::Leaf(next_leaf_node)) = (
                    unsafe { &new_node_ptr.load(Relaxed, guard).deref().entry },
                    &next_node_ref.entry,
                ) {
                    new_leaf_node.update_link(
                        Shared::null(),
                        Atomic::from(next_leaf_node as *const _).load(Relaxed, guard),
                    );

                    // Updates the anchor.
                    unsafe {
                        anchor
                            .deref()
                            .set(Atomic::from(new_leaf_node as *const _), guard);
                    }
                }
                // Makes a link from the prev node anchor to the current node anchor.
                let prev_node_anchor =
                    prev_internal_node
                        .leaf_node_anchor
                        .swap(Shared::null(), Relaxed, guard);
                if !prev_node_anchor.is_null() {
                    unsafe { prev_node_anchor.deref() }.deprecate(anchor, guard);
                }
            }
            prev_internal_node.children.1.store(Shared::null(), Relaxed);
            debug_assert!(prev_internal_node.obsolete(true, guard));
            true
        } else {
            false
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
        // Destroys entries related to the unused child.debug_assert!
        let unused_nodes = self.new_children.load(Acquire, &guard);
        if !unused_nodes.is_null() {
            // Destroys only the origin node, assuming that the rest are copied.
            debug_assert!(self.leaf_node_anchor.load(Relaxed, &guard).is_null());
            let unused_nodes = unsafe { unused_nodes.into_owned() };
            let obsolete_node = unused_nodes.origin_node_ptr.load(Relaxed, &guard);
            if !obsolete_node.is_null() {
                drop(unsafe { obsolete_node.into_owned() });
            }
        } else {
            // Destroys all: in order to avoid stack overflow, destroy them without the thread pinned.
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
    fn print<T: std::io::Write>(&self, output: &mut T, guard: &Guard) -> std::io::Result<()> {
        // Collects information.
        let mut child_ref_array: [Option<(Option<&Node<K, V>>, Option<&K>, usize)>;
            ARRAY_SIZE + 1] = [None; ARRAY_SIZE + 1];
        let mut scanner = LeafScanner::new_including_removed(&self.children.0);
        let mut index = 0;
        while let Some(entry) = scanner.next() {
            if !scanner.removed() {
                let child_share_ptr = entry.1.load(Relaxed, &guard);
                let child_ref = unsafe { child_share_ptr.deref() };
                child_ref_array[index].replace((Some(child_ref), Some(entry.0), index));
            } else {
                child_ref_array[index].replace((None, Some(entry.0), index));
            }
            index += 1;
        }
        let unbounded_child_ptr = self.children.1.load(Relaxed, &guard);
        if !unbounded_child_ptr.is_null() {
            let child_ref = unsafe { unbounded_child_ptr.deref() };
            child_ref_array[index].replace((Some(child_ref), None, index));
        }

        // Prints the label.
        output.write_fmt(format_args!(
                "{} [shape=plaintext\nlabel=<\n<table border='1' cellborder='1'>\n<tr><td colspan='{}'>ID: {}, Level: {}, Cardinality: {}</td></tr>\n<tr>",
                self.id(),
                index + 1,
                self.id(),
                self.floor,
                index + 1,
            ))?;
        for child_info in child_ref_array.iter() {
            if let Some((child_ref, key_ref, index)) = child_info {
                let font_color = if child_ref.is_some() { "black" } else { "red" };
                if let Some(key_ref) = key_ref {
                    output.write_fmt(format_args!(
                        "<td port='p_{}'><font color='{}'>{}</font></td>",
                        index, font_color, key_ref,
                    ))?;
                } else {
                    output.write_fmt(format_args!(
                        "<td port='p_{}'><font color='{}'>∞</font></td>",
                        index, font_color,
                    ))?;
                }
            }
        }
        output.write_fmt(format_args!("</tr>\n</table>\n>]\n"))?;

        // Prints the edges and children.
        for child_info in child_ref_array.iter() {
            if let Some((Some(child_ref), _, index)) = child_info {
                output.write_fmt(format_args!(
                    "{}:p_{} -> {}\n",
                    self.id(),
                    index,
                    child_ref.id()
                ))?;
                child_ref.print(output, guard)?;
            }
        }

        // Prints the leaf node anchor.
        let mut leaf_node_anchor = self.leaf_node_anchor.load(Relaxed, guard);
        let mut own_anchor = true;
        while !leaf_node_anchor.is_null() {
            let leaf_node_anchor_ref = unsafe { leaf_node_anchor.deref() };
            if own_anchor {
                own_anchor = false;
                output.write_fmt(format_args!(
                    "{} -> {}\n",
                    self.id(),
                    leaf_node_anchor_ref.id()
                ))?;
            }

            let min_leaf_node = leaf_node_anchor_ref.min_leaf_node(guard);
            if !min_leaf_node.is_null() {
                output.write_fmt(format_args!(
                    "{} -> {}\n",
                    leaf_node_anchor_ref.id(),
                    unsafe { min_leaf_node.deref().id() }
                ))?;
            }

            // Proceeds to the next anchor.
            let next_leaf_node_anchor = leaf_node_anchor_ref.next_valid_node_anchor(guard);
            if !next_leaf_node_anchor.is_null() {
                output.write_fmt(format_args!(
                    "{} -> {}\n",
                    leaf_node_anchor_ref.id(),
                    unsafe { next_leaf_node_anchor.deref().id() }
                ))?;
            }

            // Prints the label.
            output.write_fmt(format_args!(
                "{} [shape=plaintext\nlabel=<\n<table border='1' cellborder='1'>\n<tr><td>Anchor</td></tr>\n</table>\n>]\n",
                leaf_node_anchor_ref.id(),
            ))?;
            leaf_node_anchor = next_leaf_node_anchor;
        }

        std::io::Result::Ok(())
    }
}

/// Internal node locker.
struct InternalNodeLocker<'a, K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    lock: &'a Atomic<NewNodes<K, V>>,
}

impl<'a, K, V> InternalNodeLocker<'a, K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    fn lock(
        internal_node: &'a InternalNode<K, V>,
        guard: &Guard,
    ) -> Option<InternalNodeLocker<'a, K, V>> {
        let mut new_nodes_dummy = NewNodes::new();
        if let Err(error) = internal_node.new_children.compare_and_set(
            Shared::null(),
            unsafe { Owned::from_raw(&mut new_nodes_dummy as *mut NewNodes<K, V>) },
            Acquire,
            guard,
        ) {
            error.new.into_shared(guard);
            None
        } else {
            Some(InternalNodeLocker {
                lock: &internal_node.new_children,
            })
        }
    }
}

impl<'a, K, V> Drop for InternalNodeLocker<'a, K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    fn drop(&mut self) {
        self.lock.store(Shared::null(), Release);
    }
}

/// Intermediate split node.
///
/// It does not own the children, thus only nullifying pointers when drop.
struct NewNodes<K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    /// None: unbounded node
    origin_node_key: Option<K>,
    origin_node_ptr: Atomic<Node<K, V>>,
    low_key_node_anchor: Atomic<LeafNodeAnchor<K, V>>,
    low_key_node: Option<Box<Node<K, V>>>,
    middle_key: Option<K>,
    high_key_node: Option<Box<Node<K, V>>>,
}

impl<K, V> NewNodes<K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    fn new() -> NewNodes<K, V> {
        NewNodes {
            origin_node_key: None,
            origin_node_ptr: Atomic::null(),
            low_key_node_anchor: Atomic::null(),
            low_key_node: None,
            middle_key: None,
            high_key_node: None,
        }
    }
}

impl<K, V> Drop for NewNodes<K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
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
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;

    #[test]
    fn node() {
        let num_threads = 16;
        let range = 16384;
        let barrier = Arc::new(Barrier::new(num_threads));
        let node = Arc::new(Node::new(4, true));
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
                                InsertError::Duplicated(_) => assert!(false),
                                InsertError::Retry(_) => {
                                    if full_copied.load(Relaxed) {
                                        break;
                                    }
                                    continue;
                                }
                                InsertError::Full(_) => {
                                    full_copied.store(true, Relaxed);
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
