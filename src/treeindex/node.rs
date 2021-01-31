use super::leaf::{LeafScanner, ARRAY_SIZE};
use super::leafnode::{LeafNode, LeafNodeLocker};
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
    pub fn min<'a>(&'a self, guard: &'a Guard) -> Result<LeafScanner<'a, K, V>, SearchError> {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.min(guard),
            NodeType::Leaf(leaf_node) => leaf_node.min(guard),
        }
    }

    /// Returns a LeafNodeScanner pointing to a key-value pair that is large enough, but smaller than the given key.
    pub fn max_less<'a>(
        &'a self,
        key: &K,
        guard: &'a Guard,
    ) -> Result<LeafScanner<'a, K, V>, SearchError> {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.max_less(key, guard),
            NodeType::Leaf(leaf_node) => leaf_node.max_less(key, guard),
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
    pub fn split_root(&self, root_ptr: &Atomic<Node<K, V>>, guard: &Guard) {
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
            if internal_node.split_node(None, root_ptr.load(Relaxed, guard), root_ptr, true, guard)
            {
                let mut new_nodes = unsafe {
                    internal_node
                        .new_children
                        .swap(Shared::null(), Release, guard)
                        .into_owned()
                };

                // Inserts the newly allocated internal nodes into the main array.
                let low_key_node_shared = new_nodes.low_key_node.load(Relaxed, guard);
                if !low_key_node_shared.is_null() {
                    internal_node.children.0.insert(
                        new_nodes.middle_key.take().unwrap(),
                        new_nodes.low_key_node.clone(),
                    );
                }
                let high_key_node_shared = new_nodes.high_key_node.load(Relaxed, guard);
                if !high_key_node_shared.is_null() {
                    internal_node
                        .children
                        .1
                        .store(high_key_node_shared, Relaxed);
                }

                debug_assert_eq!(
                    self as *const Node<K, V>,
                    root_ptr.load(Relaxed, guard).as_raw()
                );
                unsafe {
                    let old_root = root_ptr.swap(Owned::new(new_root), Release, guard);
                    old_root.deref().unlink(guard);
                    guard.defer_destroy(old_root);
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
            // If locked and the pointer has remained the same, invalidate the node, and return invalid.
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
    fn unlink(&self, guard: &Guard) {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.unlink(guard),
            NodeType::Leaf(leaf_node) => leaf_node.unlink(guard),
        }
    }

    /// Tries to merge two adjacent nodes.
    ///
    /// The prev_prev_node argument being a valid reference to a Node means that the linked list has to be updated.
    fn try_merge(&self, prev_node_key: &K, prev_node: &Node<K, V>, guard: &Guard) -> bool {
        match (&self.entry, &prev_node.entry) {
            (NodeType::Internal(internal_node), NodeType::Internal(prev_internal_node)) => {
                internal_node.try_merge(prev_node_key, prev_internal_node, guard)
            }
            (NodeType::Leaf(leaf_node), NodeType::Leaf(prev_leaf_node)) => {
                leaf_node.try_merge(prev_node_key, prev_leaf_node, guard)
            }
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
                if child_node.is_null() {
                    // child_node being null indicates that the node is bound to be freed.
                    return Err(SearchError::Retry);
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

    fn min<'a>(&'a self, guard: &'a Guard) -> Result<LeafScanner<'a, K, V>, SearchError> {
        loop {
            let mut scanner = LeafScanner::new(&self.children.0);
            let metadata = scanner.metadata();
            if let Some(child) = scanner.next() {
                let child_node = child.1.load(Acquire, guard);
                if !(self.children.0).validate(metadata) {
                    // Data race resolution: validate metadata - see 'InternalNode::search'.
                    continue;
                }
                if child_node.is_null() {
                    // child_node being null indicates that the node is bound to be freed.
                    return Err(SearchError::Retry);
                }
                return unsafe { child_node.deref().min(guard) };
            }
            let unbounded_node = (self.children.1).load(Acquire, guard);
            if !unbounded_node.is_null() {
                if !(self.children.0).validate(metadata) {
                    // Data race resolution: validate metadata - see 'InternalNode::search'.
                    continue;
                }
                return unsafe { unbounded_node.deref().min(guard) };
            }
            // unbounded_node being null indicates that the node is bound to be freed.
            return Err(SearchError::Retry);
        }
    }

    fn max_less<'a>(
        &'a self,
        key: &K,
        guard: &'a Guard,
    ) -> Result<LeafScanner<'a, K, V>, SearchError> {
        loop {
            let mut scanner = LeafScanner::max_less(&self.children.0, key);
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
                if child_node.is_null() {
                    // child_node being null indicates that the node is bound to be freed.
                    return Err(SearchError::Retry);
                }
                return unsafe { child_node.deref().max_less(key, guard) };
            }
            if retry {
                continue;
            }
            let unbounded_node = (self.children.1).load(Acquire, guard);
            if !(self.children.0).validate(metadata) {
                // Data race resolution: validate metadata - see above.
                continue;
            }
            if !unbounded_node.is_null() {
                return unsafe { unbounded_node.deref().max_less(key, guard) };
            }
            // unbounded_node being null indicates that the node is bound to be freed.
            return Err(SearchError::Retry);
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
                if child_node.is_null() {
                    // child_node being null indicates that the node is bound to be freed.
                    return Err(InsertError::Retry((key, value)));
                }
                match unsafe { child_node.deref().insert(key, value, guard) } {
                    Ok(_) => return Ok(()),
                    Err(err) => match err {
                        InsertError::Duplicated(_) => return Err(err),
                        InsertError::Full(entry) => {
                            if !self.split_node(
                                Some(child_key.clone()),
                                child_node,
                                &child,
                                false,
                                guard,
                            ) {
                                return Err(InsertError::Full(entry));
                            }
                            return Err(InsertError::Retry(entry));
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
                    // unbounded_node being null indicates that the node is bound to be freed.
                    return Err(InsertError::Retry((key, value)));
                }
                // Tries to insert into the unbounded child, and tries to split the unbounded if it is full.
                match unsafe { unbounded_node.deref().insert(key, value, guard) } {
                    Ok(_) => return Ok(()),
                    Err(err) => match err {
                        InsertError::Duplicated(_) => return Err(err),
                        InsertError::Full(entry) => {
                            if !self.split_node(
                                None,
                                unbounded_node,
                                &(self.children.1),
                                false,
                                guard,
                            ) {
                                return Err(InsertError::Full(entry));
                            }
                            return Err(InsertError::Retry(entry));
                        }
                        InsertError::Retry(_) => return Err(err),
                    },
                };
            }
        }
    }

    /// Splits a full node.
    ///
    /// Returns true if the node is successfully split or a conflict is detected, false otherwise.
    fn split_node(
        &self,
        full_node_key: Option<K>,
        full_node_shared: Shared<Node<K, V>>,
        full_node_ptr: &Atomic<Node<K, V>>,
        root_node_split: bool,
        guard: &Guard,
    ) -> bool {
        let mut new_split_nodes;
        match self.new_children.compare_and_set(
            Shared::null(),
            Owned::new(NewNodes {
                origin_node_key: full_node_key,
                origin_node_ptr: full_node_ptr.clone(),
                low_key_node: Atomic::null(),
                middle_key: None,
                high_key_node: Atomic::null(),
            }),
            Acquire,
            guard,
        ) {
            Ok(result) => new_split_nodes = result,
            Err(_) => {
                unsafe {
                    full_node_shared.deref().rollback(guard);
                };
                return true;
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
            return true;
        }

        // Copies entries to the newly allocated leaves.
        let new_split_nodes_ref = unsafe { new_split_nodes.deref_mut() };

        match unsafe { &full_node_shared.deref().entry } {
            NodeType::Internal(full_internal_node) => {
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
                let low_key_nodes =
                    if let NodeType::Internal(low_key_internal_node) = &internal_nodes.0.entry {
                        Some(&low_key_internal_node.children)
                    } else {
                        None
                    };
                let high_key_nodes =
                    if let NodeType::Internal(high_key_internal_node) = &internal_nodes.1.entry {
                        Some(&high_key_internal_node.children)
                    } else {
                        None
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
                        let low_key_node_shared =
                            new_children_ref.low_key_node.load(Relaxed, guard);
                        if !low_key_node_shared.is_null() {
                            entry_array[num_entries].replace((
                                Some(new_children_ref.middle_key.as_ref().unwrap()),
                                new_children_ref.low_key_node.clone(),
                            ));
                            num_entries += 1;
                        }
                        let high_key_node_shared =
                            new_children_ref.high_key_node.load(Relaxed, guard);
                        if !high_key_node_shared.is_null() {
                            entry_array[num_entries]
                                .replace((Some(entry.0), new_children_ref.high_key_node.clone()));
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
                    let low_key_node_shared = new_children_ref.low_key_node.load(Relaxed, guard);
                    if !low_key_node_shared.is_null() {
                        entry_array[num_entries].replace((
                            Some(new_children_ref.middle_key.as_ref().unwrap()),
                            new_children_ref.low_key_node.clone(),
                        ));
                        num_entries += 1;
                    }
                    let high_key_node_shared = new_children_ref.high_key_node.load(Relaxed, guard);
                    if !high_key_node_shared.is_null() {
                        entry_array[num_entries]
                            .replace((None, new_children_ref.high_key_node.clone()));
                        num_entries += 1;
                    }
                }
                debug_assert!(num_entries >= 2);

                let low_key_node_array_size = num_entries / 2;
                for (index, entry) in entry_array.iter().enumerate() {
                    if let Some(entry) = entry {
                        if (index + 1) < low_key_node_array_size {
                            low_key_nodes
                                .as_ref()
                                .unwrap()
                                .0
                                .insert(entry.0.unwrap().clone(), entry.1.clone());
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
                        } else {
                            if let Some(key) = entry.0 {
                                high_key_nodes
                                    .as_ref()
                                    .unwrap()
                                    .0
                                    .insert(key.clone(), entry.1.clone());
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

                // Turns the new nodes into internal nodes.
                new_split_nodes_ref.low_key_node = Atomic::from(internal_nodes.0);
                new_split_nodes_ref.high_key_node = Atomic::from(internal_nodes.1);
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

                // Turns the new leaves into leaf nodes.
                new_split_nodes_ref.low_key_node = Atomic::from(leaf_nodes.0);
                new_split_nodes_ref.high_key_node = Atomic::from(leaf_nodes.1);
            }
        };

        // The full node is the current root: split_root processes the rest.
        if root_node_split {
            return true;
        }

        // Inserts the newly allocated internal nodes into the main array.
        if let Some(((middle_key, _), _)) = self.children.0.insert(
            new_split_nodes_ref.middle_key.take().unwrap(),
            new_split_nodes_ref.low_key_node.clone(),
        ) {
            // Insertion failed: expect that the parent splits this node.
            new_split_nodes_ref.middle_key.replace(middle_key);
            return false;
        }

        // Replaces the full node with the high-key node.
        let unused_node = full_node_ptr.swap(
            new_split_nodes_ref.high_key_node.load(Relaxed, guard),
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
            unused_node.deref().unlink(guard);
            guard.defer_destroy(unused_node);
        };
        true
    }

    fn rollback(&self, guard: &Guard) {
        let new_children = self.new_children.load(Relaxed, guard);
        unsafe {
            let origin_node = new_children.deref().origin_node_ptr.load(Relaxed, guard);
            origin_node.deref().rollback(guard);
            self.new_children.store(Shared::null(), Release);
            let low_key_node = new_children.deref().low_key_node.load(Relaxed, guard);
            if !low_key_node.is_null() {
                low_key_node.deref().unlink(guard);
                low_key_node.into_owned();
            }
            let high_key_node = new_children.deref().high_key_node.load(Relaxed, guard);
            if !high_key_node.is_null() {
                high_key_node.deref().unlink(guard);
                high_key_node.into_owned();
            }
        };
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
                if child_node.is_null() {
                    // child_node being null indicates that the node is bound to be freed.
                    return Err(RemoveError::Retry(false));
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
                    // unbounded_node being null indicates that the node is bound to be freed.
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

                if !next_node_ref.try_merge(child_key, child_ref, guard) {
                    // Failed to coalesce.
                    return Ok(removed);
                }

                // Removes the node.
                let coalesce = self.children.0.remove(child_key, true).2;
                // Once the key is removed, it is safe to deallocate the node as the validation loop ensures the absence of readers.
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
            .insert(prev_internal_node_key.clone(), new_node_ptr)
            .is_none()
        {
            prev_internal_node.children.1.store(Shared::null(), Relaxed);
            debug_assert!(prev_internal_node.obsolete(true, guard));
            true
        } else {
            false
        }
    }

    fn unlink(&self, guard: &Guard) {
        for entry in LeafScanner::new(&self.children.0) {
            entry.1.store(Shared::null(), Relaxed);
        }
        self.children.1.store(Shared::null(), Relaxed);

        // In case the node is locked, implying that it has been split, unlinks recursively.
        let unused_nodes = self.new_children.load(Acquire, &guard);
        if !unused_nodes.is_null() {
            let unused_nodes = unsafe { unused_nodes.into_owned() };
            let obsolete_node = unused_nodes.origin_node_ptr.load(Relaxed, &guard);
            if !obsolete_node.is_null() {
                unsafe {
                    obsolete_node.deref().unlink(guard);
                    guard.defer_destroy(obsolete_node)
                };
            }
        }
    }
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Drop for InternalNode<K, V> {
    fn drop(&mut self) {
        let guard = crossbeam_epoch::pin();
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
    low_key_node: Atomic<Node<K, V>>,
    middle_key: Option<K>,
    high_key_node: Atomic<Node<K, V>>,
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
            low_key_node: Atomic::null(),
            middle_key: None,
            high_key_node: Atomic::null(),
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
        if let Ok(mut scanner) = node.min(&guard) {
            let mut iterated = 0;
            while let Some(entry) = scanner.next() {
                println!("{} {}", entry.0, entry.1);
                assert!(prev == 0 || prev < *entry.0);
                assert_eq!(entry.0, entry.1);
                iterated += 1;
                prev = *entry.0;
            }
            println!("iterated: {}", iterated);
        }
    }
}
