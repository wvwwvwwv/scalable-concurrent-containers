use super::leaf::{LeafScanner, ARRAY_SIZE};
use super::leaf_node::{LeafNode, LeafNodeLocker};
use super::Leaf;
use super::{InsertError, RemoveError, SearchError};
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use std::borrow::Borrow;
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
    /// Creates a new internal node.
    pub fn new_internal_node() -> Node<K, V> {
        Node {
            entry: NodeType::Internal(InternalNode::new()),
        }
    }

    /// Creates a new leaf node.
    pub fn new_leaf_node() -> Node<K, V> {
        Node {
            entry: NodeType::Leaf(LeafNode::new()),
        }
    }

    /// Takes the memory address of self.entry as an identifier.
    pub fn id(&self) -> usize {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.id(),
            NodeType::Leaf(leaf_node) => leaf_node.id(),
        }
    }

    /// Returns the depth of the node.
    pub fn depth(&self, depth: usize, guard: &Guard) -> usize {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.depth(depth, guard),
            NodeType::Leaf(_) => depth,
        }
    }

    /// Checks if the node is obsolete.
    pub fn obsolete(&self, guard: &Guard) -> bool {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.obsolete(guard),
            NodeType::Leaf(leaf_node) => leaf_node.obsolete(guard),
        }
    }

    /// Searches for an entry associated with the given key.
    pub fn search<'g, Q>(&self, key: &'g Q, guard: &'g Guard) -> Result<Option<&'g V>, SearchError>
    where
        K: 'g + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.search(key, guard),
            NodeType::Leaf(leaf_node) => leaf_node.search(key, guard),
        }
    }

    /// Returns the minimum key-value pair.
    pub fn min<'g>(&self, guard: &'g Guard) -> Result<LeafScanner<'g, K, V>, SearchError> {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.min(guard),
            NodeType::Leaf(leaf_node) => leaf_node.min(guard),
        }
    }

    /// Returns the maximum key entry less than the given key.
    pub fn max_less<'g>(
        &self,
        key: &K,
        guard: &'g Guard,
    ) -> Result<LeafScanner<'g, K, V>, SearchError> {
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

    /// Removes an entry associated with the given key.
    pub fn remove<Q>(&self, key: &Q, guard: &Guard) -> Result<bool, RemoveError>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.remove(key, guard),
            NodeType::Leaf(leaf_node) => leaf_node.remove(key, guard),
        }
    }

    /// Splits the current root node.
    pub fn split_root(&self, root_ptr: &Atomic<Node<K, V>>, guard: &Guard) {
        // The fact that the TreeIndex calls this function means that the root is in a split procedure,
        // and the root is locked.
        debug_assert_eq!(
            self as *const Node<K, V>,
            root_ptr.load(Relaxed, guard).as_raw()
        );
        let new_root: Node<K, V> = Node::new_internal_node();
        if let NodeType::Internal(internal_node) = &new_root.entry {
            if internal_node.split_node(None, root_ptr.load(Relaxed, guard), root_ptr, true, guard)
            {
                let new_nodes =
                    unsafe { internal_node.new_children.load(Relaxed, guard).deref_mut() };

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
                // Updates the pointer.
                let old_root = root_ptr.swap(Owned::new(new_root), Release, guard);
                unsafe {
                    // Unlinks the former root node before dropping it.
                    old_root.deref().unlink(guard);
                    guard.defer_destroy(old_root);

                    // Unlocks the new root.
                    let new_root = root_ptr.load(Relaxed, guard).deref();
                    if let NodeType::Internal(internal_node) = &new_root.entry {
                        internal_node
                            .new_children
                            .swap(Shared::null(), Release, guard)
                            .into_owned();
                    }
                };
            }
        }
    }

    /// Removes the current root node.
    pub fn remove_root(
        root_ptr: &Atomic<Node<K, V>>,
        donot_remove_valid_root: bool,
        guard: &Guard,
    ) -> bool {
        let mut current_root_node = root_ptr.load(Acquire, &guard);
        loop {
            if !current_root_node.is_null() {
                let mut internal_node_locker = None;
                let mut leaf_node_locker = None;
                match unsafe { &current_root_node.deref().entry } {
                    NodeType::Internal(internal_node) => {
                        if let Some(locker) = InternalNodeLocker::try_lock(internal_node, guard) {
                            internal_node_locker.replace(locker);
                        }
                    }
                    NodeType::Leaf(leaf_node) => {
                        if let Some(locker) = LeafNodeLocker::try_lock(leaf_node, guard) {
                            leaf_node_locker.replace(locker);
                        }
                    }
                };
                if internal_node_locker.is_none() && leaf_node_locker.is_none() {
                    // The root node is locked by another thread.
                    current_root_node = root_ptr.load(Acquire, &guard);
                    continue;
                }
                let root_ref = unsafe { current_root_node.deref() };
                if donot_remove_valid_root && !root_ref.obsolete(guard) {
                    break;
                }
                match root_ptr.compare_exchange(
                    current_root_node,
                    Shared::null(),
                    Release,
                    Relaxed,
                    &guard,
                ) {
                    Ok(_) => {
                        // It is important to keep the locked state until the node is dropped
                        // in order for all the on-going structural changes being made to its
                        // child nodes to fail.
                        if let Some(internal_node_locker) = internal_node_locker.as_mut() {
                            internal_node_locker.deprecate();
                        }
                        if let Some(leaf_node_locker) = leaf_node_locker.as_mut() {
                            leaf_node_locker.deprecate();
                        }
                        unsafe { guard.defer_destroy(current_root_node) };
                    }
                    Err(err) => {
                        current_root_node = err.current;
                        continue;
                    }
                }
            }
            return true;
        }
        false
    }

    /// Rolls back the ongoing split operation recursively.
    fn rollback(&self, guard: &Guard) {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.rollback(guard),
            NodeType::Leaf(leaf_node) => leaf_node.rollback(guard),
        }
    }

    /// Clears links to all the children to drop the deprecated node.
    ///
    /// It is called only when the node is a temporary one for split/merge,
    /// or has become unreachable after split/merge/remove.
    fn unlink(&self, guard: &Guard) {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.unlink(guard),
            NodeType::Leaf(leaf_node) => leaf_node.unlink(guard),
        }
    }
}

impl<K: Clone + Display + Ord + Send + Sync, V: Clone + Display + Send + Sync> Node<K, V> {
    pub fn print<T: std::io::Write>(
        &self,
        output: &mut T,
        depth: usize,
        guard: &Guard,
    ) -> std::io::Result<()> {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.print(output, depth, guard),
            NodeType::Leaf(leaf_node) => leaf_node.print(output, depth, guard),
        }
    }
}

/// Internal node.
///
/// The layout of an internal node: |ptr(children)/max(child keys)|...|ptr(children)|
struct InternalNode<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// Child nodes.
    ///
    /// The pointer to the unbounded node storing a non-zero tag indicates that the leaf is obsolete.
    children: (Leaf<K, Atomic<Node<K, V>>>, Atomic<Node<K, V>>),
    /// New nodes in an intermediate state during merge and split.
    ///
    /// A valid pointer stored in the variable acts as a mutex for merge and split operations.
    new_children: Atomic<NewNodes<K, V>>,
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> InternalNode<K, V> {
    /// Creates a new empty internal node.
    fn new() -> InternalNode<K, V> {
        InternalNode {
            children: (Leaf::new(), Atomic::null()),
            new_children: Atomic::null(),
        }
    }

    /// Takes the memory address of the instance as an identifier.
    fn id(&self) -> usize {
        self as *const _ as usize
    }

    /// Returns the depth of the node.
    fn depth(&self, depth: usize, guard: &Guard) -> usize {
        let unbounded_shared = self.children.1.load(Relaxed, guard);
        if !unbounded_shared.is_null() {
            return unsafe { unbounded_shared.deref().depth(depth + 1, guard) };
        }
        depth
    }

    /// Checks if the internal node is obsolete.
    fn obsolete(&self, guard: &Guard) -> bool {
        if self.children.0.obsolete() {
            let unbounded_shared = self.children.1.load(Relaxed, guard);
            // The unbounded node is specially marked when becoming obsolete.
            return unbounded_shared.tag() == 1;
        }
        false
    }

    /// Searches for an entry associated with the given key.
    fn search<'g, Q>(&self, key: &'g Q, guard: &'g Guard) -> Result<Option<&'g V>, SearchError>
    where
        K: 'g + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        loop {
            let result = (self.children.0).min_greater_equal(&key);
            if let Some((_, child)) = result.0 {
                let child_node = child.load(Acquire, guard);
                if !(self.children.0).validate(result.1) {
                    // Data race resolution: validate metadata - see LeafNode::search.
                    continue;
                }
                if child_node.is_null() {
                    // child_node being null indicates that the node is bound to be freed.
                    return Err(SearchError::Retry);
                }
                return unsafe { child_node.deref().search(key, guard) };
            }
            let unbounded_shared = (self.children.1).load(Acquire, guard);
            if !unbounded_shared.is_null() {
                debug_assert!(unbounded_shared.tag() == 0);
                if !(self.children.0).validate(result.1) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                return unsafe { unbounded_shared.deref().search(key, guard) };
            }
            // unbounded_shared being null indicates that the node is bound to be freed.
            debug_assert!(unbounded_shared.tag() == 1);
            return Err(SearchError::Retry);
        }
    }

    /// Returns the minimum key entry.
    fn min<'g>(&self, guard: &'g Guard) -> Result<LeafScanner<'g, K, V>, SearchError> {
        loop {
            let mut scanner = LeafScanner::new(&self.children.0);
            let metadata = scanner.metadata();
            if let Some(child) = scanner.next() {
                let child_node = child.1.load(Acquire, guard);
                if !(self.children.0).validate(metadata) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                if child_node.is_null() {
                    // child_node being null indicates that the node is bound to be freed.
                    return Err(SearchError::Retry);
                }
                return unsafe { child_node.deref().min(guard) };
            }
            let unbounded_shared = (self.children.1).load(Acquire, guard);
            if !unbounded_shared.is_null() {
                debug_assert!(unbounded_shared.tag() == 0);
                if !(self.children.0).validate(metadata) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                return unsafe { unbounded_shared.deref().min(guard) };
            }
            // unbounded_shared being null indicates that the node is bound to be freed.
            debug_assert!(unbounded_shared.tag() == 1);
            return Err(SearchError::Retry);
        }
    }

    /// Returns the maximum key entry less than the given key.
    fn max_less<'g>(
        &self,
        key: &K,
        guard: &'g Guard,
    ) -> Result<LeafScanner<'g, K, V>, SearchError> {
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
                    // Data race resolution - see LeafNode::search.
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
            let unbounded_shared = (self.children.1).load(Acquire, guard);
            if !(self.children.0).validate(metadata) {
                // Data race resolution - see above.
                continue;
            }
            if !unbounded_shared.is_null() {
                debug_assert!(unbounded_shared.tag() == 0);
                return unsafe { unbounded_shared.deref().max_less(key, guard) };
            }
            // unbounded_shared being null indicates that the node is bound to be freed.
            debug_assert!(unbounded_shared.tag() == 1);
            return Err(SearchError::Retry);
        }
    }

    /// Inserts a key-value pair.
    fn insert(&self, key: K, value: V, guard: &Guard) -> Result<(), InsertError<K, V>> {
        // Possible data race: the node is being split, for instance,
        //  - Node state: ((15, ptr), (25, ptr)), 15 is being split
        //  - Insert 10: min_greater_equal returns (15, ptr)
        //  - Split 15: insert 11, and replace 15 with a new pointer, therefore ((11, ptr), (15, new_ptr), (25, ptr))
        //  - Insert 10: load new_ptr, and try insert, that is incorrect as it is supposed to be inserted into (11, ptr)
        loop {
            let result = (self.children.0).min_greater_equal(&key);
            if let Some((child_key, child)) = result.0 {
                let child_node = child.load(Acquire, guard);
                if !(self.children.0).validate(result.1) {
                    // Data race resolution - see LeafNode::search.
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
            }

            let unbounded_shared = self.children.1.load(Relaxed, guard);
            if !unbounded_shared.is_null() {
                debug_assert!(unbounded_shared.tag() == 0);
                if !(self.children.0).validate(result.1) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                // Tries to insert into the unbounded child, and tries to split the unbounded if it is full.
                match unsafe { unbounded_shared.deref().insert(key, value, guard) } {
                    Ok(_) => return Ok(()),
                    Err(err) => match err {
                        InsertError::Duplicated(_) => return Err(err),
                        InsertError::Full(entry) => {
                            if !self.split_node(
                                None,
                                unbounded_shared,
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
            // unbounded_shared being null indicates that the node is bound to be freed.
            debug_assert!(unbounded_shared.tag() == 1);
            return Err(InsertError::Retry((key, value)));
        }
    }

    /// Removes an entry associated with the given key.
    fn remove<Q>(&self, key: &Q, guard: &Guard) -> Result<bool, RemoveError>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        loop {
            let result = (self.children.0).min_greater_equal(&key);
            if let Some((_, child)) = result.0 {
                let child_node = child.load(Acquire, guard);
                if !(self.children.0).validate(result.1) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                if child_node.is_null() {
                    // child_node being null indicates that the node is bound to be freed.
                    return Err(RemoveError::Retry(false));
                }
                return match unsafe { child_node.deref().remove(key, guard) } {
                    Ok(removed) => Ok(removed),
                    Err(remove_error) => match remove_error {
                        RemoveError::Empty(removed) => self.coalesce(removed, guard),
                        RemoveError::Retry(_) => Err(remove_error),
                    },
                };
            }
            let unbounded_shared = (self.children.1).load(Acquire, guard);
            if !unbounded_shared.is_null() {
                debug_assert!(unbounded_shared.tag() == 0);
                if !(self.children.0).validate(result.1) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                return match unsafe { unbounded_shared.deref().remove(key, guard) } {
                    Ok(removed) => Ok(removed),
                    Err(remove_error) => match remove_error {
                        RemoveError::Empty(removed) => self.coalesce(removed, guard),
                        RemoveError::Retry(_) => Err(remove_error),
                    },
                };
            }
            // unbounded_node being null indicates that the node is bound to be freed.
            debug_assert!(unbounded_shared.tag() == 1);
            return Err(RemoveError::Empty(false));
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
        match self.new_children.compare_exchange(
            Shared::null(),
            Owned::new(NewNodes {
                origin_node_key: full_node_key,
                origin_node_ptr: full_node_ptr.clone(),
                low_key_node: Atomic::null(),
                middle_key: None,
                high_key_node: Atomic::null(),
            }),
            Acquire,
            Relaxed,
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
                    Box::new(Node::new_internal_node()),
                    Box::new(Node::new_internal_node()),
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
                let mut entry_array: [Option<(Option<&K>, Atomic<Node<K, V>>)>; ARRAY_SIZE + 2] =
                    [None, None, None, None, None, None, None, None, None, None];
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
                        match (index + 1).cmp(&low_key_node_array_size) {
                            Ordering::Less => {
                                low_key_nodes
                                    .as_ref()
                                    .unwrap()
                                    .0
                                    .insert(entry.0.unwrap().clone(), entry.1.clone());
                            }
                            Ordering::Equal => {
                                new_split_nodes_ref
                                    .middle_key
                                    .replace(entry.0.unwrap().clone());
                                let child_node_ptr = entry.1.load(Relaxed, guard);
                                low_key_nodes
                                    .as_ref()
                                    .unwrap()
                                    .1
                                    .store(child_node_ptr, Relaxed);
                            }
                            Ordering::Greater => {
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
                        };
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
                let leaf_nodes = (
                    Box::new(Node::new_leaf_node()),
                    Box::new(Node::new_leaf_node()),
                );
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
            // Insertion failed: expects that the parent splits this node.
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
        // - Still, the deprecated full leaf can be reachable by Scanners.
        unsafe {
            // Cleans up the split operation by unlinking the unused node.
            unused_node.deref().unlink(guard);
            guard.defer_destroy(unused_node);

            // Unlocks the node.
            let new_split_nodes = self.new_children.swap(Shared::null(), Release, guard);
            let new_split_nodes = new_split_nodes.into_owned();
            debug_assert_eq!(
                new_split_nodes.origin_node_ptr.load(Relaxed, guard),
                unused_node
            );
        };
        true
    }

    /// Rolls back the ongoing split operation recursively.
    fn rollback(&self, guard: &Guard) {
        let new_children = self.new_children.load(Relaxed, guard);
        unsafe {
            let origin_node = new_children.deref().origin_node_ptr.load(Relaxed, guard);
            origin_node.deref().rollback(guard);
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
            drop(
                self.new_children
                    .swap(Shared::null(), Release, guard)
                    .into_owned(),
            );
        };
    }

    /// Unlinks all the leaves.
    ///
    /// It is called only when the internal node is a temporary one for split/merge,
    /// or has become unreachable after split/merge/remove.
    fn unlink(&self, guard: &Guard) {
        for entry in LeafScanner::new(&self.children.0) {
            entry.1.store(Shared::null(), Relaxed);
        }
        self.children.1.store(Shared::null().with_tag(1), Relaxed);

        // In case the node is locked, implying that it has been split, unlinks recursively.
        //
        // Keeps the internal node locked to prevent locking attempts.
        let unused_nodes = self
            .new_children
            .swap(Shared::null().with_tag(1), Relaxed, &guard);
        if !unused_nodes.is_null() {
            let unused_nodes = unsafe { unused_nodes.into_owned() };
            let obsolete_node = unused_nodes.origin_node_ptr.load(Relaxed, &guard);
            unsafe {
                obsolete_node.deref().unlink(guard);
                guard.defer_destroy(obsolete_node)
            };
        }
    }

    /// Tries to coalesce nodes.
    fn coalesce(&self, removed: bool, guard: &Guard) -> Result<bool, RemoveError> {
        let lock = InternalNodeLocker::try_lock(self, guard);
        if lock.is_none() {
            return Err(RemoveError::Retry(removed));
        }

        for entry in LeafScanner::new(&self.children.0) {
            let node_shared = entry.1.load(Relaxed, guard);
            let node_ref = unsafe { node_shared.deref() };
            if node_ref.obsolete(guard) {
                self.children.0.remove(entry.0);
                // Once the key is removed, it is safe to deallocate the node as the validation loop ensures the absence of readers.
                entry.1.store(Shared::null(), Release);
                unsafe {
                    guard.defer_destroy(node_shared);
                };
            }
        }

        let unbounded_shared = self.children.1.load(Relaxed, guard);
        if !unbounded_shared.is_null() && unsafe { unbounded_shared.deref().obsolete(guard) } {
            // If the unbounded node has become obsolete, either marks the node obsolete, or replaces the unbounded node with another.
            if let Some(max_entry) = self.children.0.max() {
                // Firstly, replaces the unbounded node with the max entry.
                self.children
                    .1
                    .store(max_entry.1.load(Relaxed, guard), Release);
                // Then, removes the node from the children list.
                if self.children.0.remove(max_entry.0).2 {
                    // Retires the children if it was the last child.
                    let result = self.children.0.retire();
                    debug_assert!(result);
                }
                max_entry.1.store(Shared::null(), Release);
            } else {
                // Deprecates this internal node.
                let result = self.children.0.retire();
                debug_assert!(result);
                self.children.1.store(Shared::null().with_tag(1), Release);
            }
            unsafe {
                unbounded_shared.deref().unlink(guard);
                guard.defer_destroy(unbounded_shared);
            }
        }

        if self.children.1.load(Relaxed, guard).is_null() {
            debug_assert!(self.children.0.obsolete());
            Err(RemoveError::Empty(removed))
        } else if removed {
            Ok(removed)
        } else {
            // Retry is necessary as it may have not attempted removal.
            Err(RemoveError::Retry(false))
        }
    }
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Drop for InternalNode<K, V> {
    fn drop(&mut self) {
        debug_assert!(self
            .new_children
            .load(Relaxed, unsafe { crossbeam_epoch::unprotected() })
            .is_null());

        // The internal node has become unreachable, and so have all the children, therefore pinning is unnecessary.
        for entry in LeafScanner::new(&self.children.0) {
            let child = entry
                .1
                .load(Acquire, unsafe { crossbeam_epoch::unprotected() });
            if !child.is_null() {
                drop(unsafe { child.into_owned() });
            }
        }
        let unbounded_child = self
            .children
            .1
            .load(Acquire, unsafe { crossbeam_epoch::unprotected() });
        if !unbounded_child.is_null() {
            drop(unsafe { unbounded_child.into_owned() });
        }
    }
}

impl<K: Clone + Display + Ord + Send + Sync, V: Clone + Display + Send + Sync> InternalNode<K, V> {
    fn print<T: std::io::Write>(
        &self,
        output: &mut T,
        depth: usize,
        guard: &Guard,
    ) -> std::io::Result<()> {
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
            depth,
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
                child_ref.print(output, depth + 1, guard)?;
            }
        }

        std::io::Result::Ok(())
    }
}

/// Internal node locker.
struct InternalNodeLocker<'n, K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    lock: &'n Atomic<NewNodes<K, V>>,
    /// When the internal node is bound to be dropped, the flag may be set true.
    deprecate: bool,
}

impl<'n, K, V> InternalNodeLocker<'n, K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    fn try_lock(
        internal_node: &'n InternalNode<K, V>,
        guard: &Guard,
    ) -> Option<InternalNodeLocker<'n, K, V>> {
        if internal_node
            .new_children
            .compare_exchange(
                Shared::null(),
                Shared::null().with_tag(1),
                Acquire,
                Relaxed,
                guard,
            )
            .is_ok()
        {
            Some(InternalNodeLocker {
                lock: &internal_node.new_children,
                deprecate: false,
            })
        } else {
            None
        }
    }

    fn deprecate(&mut self) {
        self.deprecate = true;
    }
}

impl<'n, K, V> Drop for InternalNodeLocker<'n, K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    fn drop(&mut self) {
        if !self.deprecate {
            self.lock.store(Shared::null(), Release);
        }
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
        let node = Arc::new(Node::new_leaf_node());
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
        if full.load(Relaxed) {
            node.rollback(&guard);
        }
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
