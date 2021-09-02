use super::leaf::{Scanner, ARRAY_SIZE};
use super::leaf_node::LeafNode;
use super::leaf_node::Locker as LeafLocker;
use super::Leaf;
use super::{InsertError, RemoveError, SearchError};

use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::Display;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// Node types.
enum NodeType<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    Internal(InternalNode<K, V>),
    Leaf(LeafNode<K, V>),
}

/// Node.
pub struct Node<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    /// Stores an array of child nodes/leaves, interleaving keys, and metadata.
    entry: NodeType<K, V>,
}

impl<K, V> Node<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
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
    pub fn depth(&self, depth: usize, barrier: &Barrier) -> usize {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.depth(depth, barrier),
            NodeType::Leaf(_) => depth,
        }
    }

    /// Checks if the node is obsolete.
    pub fn obsolete(&self, barrier: &Barrier) -> bool {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.obsolete(barrier),
            NodeType::Leaf(leaf_node) => leaf_node.obsolete(barrier),
        }
    }

    /// Searches for an entry associated with the given key.
    pub fn search<'b, Q>(
        &self,
        key: &'b Q,
        barrier: &'b Barrier,
    ) -> Result<Option<&'b V>, SearchError>
    where
        K: 'b + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.search(key, barrier),
            NodeType::Leaf(leaf_node) => leaf_node.search(key, barrier),
        }
    }

    /// Returns the minimum key-value pair.
    pub fn min<'b>(&self, barrier: &'b Barrier) -> Result<Scanner<'b, K, V>, SearchError> {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.min(barrier),
            NodeType::Leaf(leaf_node) => leaf_node.min(barrier),
        }
    }

    /// Returns the maximum key entry less than the given key.
    pub fn max_less<'b>(
        &self,
        key: &K,
        barrier: &'b Barrier,
    ) -> Result<Scanner<'b, K, V>, SearchError> {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.max_less(key, barrier),
            NodeType::Leaf(leaf_node) => leaf_node.max_less(key, barrier),
        }
    }

    /// Inserts a key-value pair.
    ///
    /// It is a recursive call, and therefore stack-overflow may occur.
    pub fn insert(&self, key: K, value: V, barrier: &Barrier) -> Result<(), InsertError<K, V>> {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.insert(key, value, barrier),
            NodeType::Leaf(leaf_node) => leaf_node.insert(key, value, barrier),
        }
    }

    /// Removes an entry associated with the given key.
    pub fn remove<Q>(&self, key: &Q, barrier: &Barrier) -> Result<bool, RemoveError>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.remove(key, barrier),
            NodeType::Leaf(leaf_node) => leaf_node.remove(key, barrier),
        }
    }

    /// Splits the current root node.
    pub fn split_root(&self, root: &AtomicArc<Node<K, V>>, barrier: &Barrier) {
        // The fact that the TreeIndex calls this function means that the root is in a split
        // procedure, and the root is locked.
        debug_assert_eq!(
            self as *const Node<K, V>,
            root.load(Relaxed, barrier).as_raw()
        );
        let new_root: Node<K, V> = Node::new_internal_node();
        if let NodeType::Internal(internal_node) = &new_root.entry {
            if internal_node.split_node(None, root.load(Relaxed, barrier), root, true, barrier) {
                let new_nodes = unsafe {
                    #[allow(clippy::cast_ref_to_mut)]
                    &mut *(internal_node
                        .new_children
                        .load(Relaxed, barrier)
                        .as_ref()
                        .unwrap() as *const NewNodes<K, V>
                        as *mut NewNodes<K, V>)
                };

                // Inserts the newly allocated internal nodes into the main array.
                let low_key_node_cloned = new_nodes.low_key_node.clone(Relaxed, barrier);
                if !low_key_node_cloned.is_null(Relaxed) {
                    internal_node
                        .children
                        .0
                        .insert(new_nodes.middle_key.take().unwrap(), low_key_node_cloned);
                }
                let high_key_node_cloned = new_nodes.high_key_node.clone(Relaxed, barrier);
                if !high_key_node_cloned.is_null(Relaxed) {
                    internal_node.children.1.swap(
                        (high_key_node_cloned.get_arc(Relaxed, barrier), Tag::None),
                        Relaxed,
                    );
                }

                debug_assert_eq!(
                    self as *const Node<K, V>,
                    root.load(Relaxed, barrier).as_raw()
                );
                // Updates the pointer.
                if let Some(old_root) = root.swap((Some(Arc::new(new_root)), Tag::None), Release) {
                    // Unlinks the former root node before dropping it.
                    old_root.unlink(barrier);
                    barrier.reclaim(old_root);

                    // Unlocks the new root.
                    let new_root = root.load(Relaxed, barrier).as_ref().unwrap();
                    if let NodeType::Internal(internal_node) = &new_root.entry {
                        internal_node.new_children.swap((None, Tag::None), Release);
                    }
                };
            }
        }
    }

    /// Removes the current root node.
    pub fn remove_root(
        root: &AtomicArc<Node<K, V>>,
        do_not_remove_valid_root: bool,
        barrier: &Barrier,
    ) -> bool {
        let mut root_ptr = root.load(Acquire, barrier);
        loop {
            if let Some(root_ref) = root_ptr.as_ref() {
                let mut internal_node_locker = None;
                let mut leaf_node_locker = None;
                match &root_ref.entry {
                    NodeType::Internal(internal_node) => {
                        if let Some(locker) = InternalNodeLocker::try_lock(internal_node) {
                            internal_node_locker.replace(locker);
                        }
                    }
                    NodeType::Leaf(leaf_node) => {
                        if let Some(locker) = LeafLocker::try_lock(leaf_node) {
                            leaf_node_locker.replace(locker);
                        }
                    }
                };
                if internal_node_locker.is_none() && leaf_node_locker.is_none() {
                    // The root node is locked by another thread.
                    root_ptr = root.load(Acquire, barrier);
                    continue;
                }
                if do_not_remove_valid_root && !root_ref.obsolete(barrier) {
                    break;
                }
                match root.compare_exchange(root_ptr, (None, Tag::None), Release, Relaxed) {
                    Ok((old_root, _)) => {
                        // It is important to keep the locked state until the node is dropped
                        // in order for all the on-going structural changes being made to its
                        // child nodes to fail.
                        if let Some(internal_node_locker) = internal_node_locker.as_mut() {
                            internal_node_locker.deprecate();
                        }
                        if let Some(leaf_node_locker) = leaf_node_locker.as_mut() {
                            leaf_node_locker.deprecate();
                        }
                        if let Some(old_root) = old_root {
                            barrier.reclaim(old_root);
                        }
                    }
                    Err((_, actual)) => {
                        root_ptr = actual;
                        continue;
                    }
                }
            }
            return true;
        }
        false
    }

    /// Rolls back the ongoing split operation recursively.
    fn rollback(&self, barrier: &Barrier) {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.rollback(barrier),
            NodeType::Leaf(leaf_node) => leaf_node.rollback(barrier),
        }
    }

    /// Clears links to all the children to drop the deprecated node.
    ///
    /// It is called only when the node is a temporary one for split/merge,
    /// or has become unreachable after split/merge/remove.
    fn unlink(&self, barrier: &Barrier) {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.unlink(barrier),
            NodeType::Leaf(leaf_node) => leaf_node.unlink(barrier),
        }
    }
}

impl<K, V> Node<K, V>
where
    K: 'static + Clone + Display + Ord + Send + Sync,
    V: 'static + Clone + Display + Send + Sync,
{
    pub fn print<T: std::io::Write>(
        &self,
        output: &mut T,
        depth: usize,
        barrier: &Barrier,
    ) -> std::io::Result<()> {
        match &self.entry {
            NodeType::Internal(internal_node) => internal_node.print(output, depth, barrier),
            NodeType::Leaf(leaf_node) => leaf_node.print(output, depth, barrier),
        }
    }
}

/// Internal node.
///
/// The layout of an internal node: |ptr(children)/max(child keys)|...|ptr(children)|
struct InternalNode<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    /// Child nodes.
    ///
    /// The pointer to the unbounded node storing a non-zero tag indicates that the leaf is obsolete.
    #[allow(clippy::type_complexity)]
    children: (Leaf<K, AtomicArc<Node<K, V>>>, AtomicArc<Node<K, V>>),
    /// New nodes in an intermediate state during merge and split.
    ///
    /// A valid pointer stored in the variable acts as a mutex for merge and split operations.
    new_children: AtomicArc<NewNodes<K, V>>,
}

impl<K, V> InternalNode<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    /// Creates a new empty internal node.
    fn new() -> InternalNode<K, V> {
        InternalNode {
            children: (Leaf::new(), AtomicArc::null()),
            new_children: AtomicArc::null(),
        }
    }

    /// Takes the memory address of the instance as an identifier.
    fn id(&self) -> usize {
        self as *const _ as usize
    }

    /// Returns the depth of the node.
    fn depth(&self, depth: usize, barrier: &Barrier) -> usize {
        let unbounded_ptr = self.children.1.load(Relaxed, barrier);
        if let Some(unbounded_ref) = unbounded_ptr.as_ref() {
            return unbounded_ref.depth(depth + 1, barrier);
        }
        depth
    }

    /// Checks if the internal node is obsolete.
    fn obsolete(&self, barrier: &Barrier) -> bool {
        if self.children.0.obsolete() {
            let unbounded_ptr = self.children.1.load(Relaxed, barrier);
            // The unbounded node is specially marked when becoming obsolete.
            return unbounded_ptr.tag() == Tag::First;
        }
        false
    }

    /// Searches for an entry associated with the given key.
    fn search<'b, Q>(
        &self,
        key_ref: &'b Q,
        barrier: &'b Barrier,
    ) -> Result<Option<&'b V>, SearchError>
    where
        K: 'b + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        loop {
            let result = (self.children.0).min_greater_equal(key_ref);
            if let Some((_, child)) = result.0 {
                let child_ptr = child.load(Acquire, barrier);
                if !(self.children.0).validate(result.1) {
                    // Data race resolution: validate metadata - see LeafNode::search.
                    continue;
                }
                if let Some(child_ref) = child_ptr.as_ref() {
                    return child_ref.search(key_ref, barrier);
                }
                // `child_ptr` being null indicates that the node is bound to be freed.
                return Err(SearchError::Retry);
            }
            let unbounded_ptr = (self.children.1).load(Acquire, barrier);
            if let Some(unbounded_ref) = unbounded_ptr.as_ref() {
                debug_assert!(unbounded_ptr.tag() == Tag::None);
                if !(self.children.0).validate(result.1) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                return unbounded_ref.search(key_ref, barrier);
            }
            // `unbounded_ptr` being null indicates that the node is bound to be freed.
            debug_assert!(unbounded_ptr.tag() == Tag::First);
            return Err(SearchError::Retry);
        }
    }

    /// Returns the minimum key entry.
    fn min<'b>(&self, barrier: &'b Barrier) -> Result<Scanner<'b, K, V>, SearchError> {
        loop {
            let mut scanner = Scanner::new(&self.children.0);
            let metadata = scanner.metadata();
            if let Some(child) = scanner.next() {
                let child_ptr = child.1.load(Acquire, barrier);
                if !(self.children.0).validate(metadata) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                if let Some(child_ref) = child_ptr.as_ref() {
                    return child_ref.min(barrier);
                }
                // `child_ptr` being null indicates that the node is bound to be freed.
                return Err(SearchError::Retry);
            }
            let unbounded_ptr = (self.children.1).load(Acquire, barrier);
            if let Some(unbounded_ref) = unbounded_ptr.as_ref() {
                debug_assert!(unbounded_ptr.tag() == Tag::None);
                if !(self.children.0).validate(metadata) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                return unbounded_ref.min(barrier);
            }
            // `unbounded_ptr` being null indicates that the node is bound to be freed.
            debug_assert!(unbounded_ptr.tag() == Tag::First);
            return Err(SearchError::Retry);
        }
    }

    /// Returns the maximum key entry less than the given key.
    fn max_less<'b>(
        &self,
        key: &K,
        barrier: &'b Barrier,
    ) -> Result<Scanner<'b, K, V>, SearchError> {
        loop {
            let scanner = Scanner::max_less(&self.children.0, key);
            let metadata = scanner.metadata();
            let mut retry = false;
            for child in scanner {
                if child.0.cmp(key) == Ordering::Less {
                    continue;
                }
                let child_ptr = child.1.load(Acquire, barrier);
                if !(self.children.0).validate(metadata) {
                    // Data race resolution - see LeafNode::search.
                    retry = true;
                    break;
                }
                if let Some(child_ref) = child_ptr.as_ref() {
                    return child_ref.max_less(key, barrier);
                }
                // `child_ptr` being null indicates that the node is bound to be freed.
                return Err(SearchError::Retry);
            }
            if retry {
                continue;
            }
            let unbounded_ptr = (self.children.1).load(Acquire, barrier);
            if !(self.children.0).validate(metadata) {
                // Data race resolution - see above.
                continue;
            }
            if let Some(unbounded_ref) = unbounded_ptr.as_ref() {
                debug_assert!(unbounded_ptr.tag() == Tag::None);
                return unbounded_ref.max_less(key, barrier);
            }
            // `unbounded_ptr` being null indicates that the node is bound to be freed.
            debug_assert!(unbounded_ptr.tag() == Tag::First);
            return Err(SearchError::Retry);
        }
    }

    /// Inserts a key-value pair.
    fn insert(&self, key: K, value: V, barrier: &Barrier) -> Result<(), InsertError<K, V>> {
        // Possible data race: the node is being split, for instance,
        //  - Node state: ((15, ptr), (25, ptr)), 15 is being split
        //  - Insert 10: min_greater_equal returns (15, ptr)
        //  - Split 15: insert 11, and replace 15 with a new pointer, therefore ((11, ptr), (15, new_ptr), (25, ptr))
        //  - Insert 10: load new_ptr, and try insert, that is incorrect as it is supposed to be inserted into (11, ptr)
        loop {
            let result = (self.children.0).min_greater_equal(&key);
            if let Some((child_key, child)) = result.0 {
                let child_ptr = child.load(Acquire, barrier);
                if !(self.children.0).validate(result.1) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                if let Some(child_ref) = child_ptr.as_ref() {
                    match child_ref.insert(key, value, barrier) {
                        Ok(_) => return Ok(()),
                        Err(err) => match err {
                            InsertError::Duplicated(_) | InsertError::Retry(_) => return Err(err),
                            InsertError::Full(entry) => {
                                if !self.split_node(
                                    Some(child_key.clone()),
                                    child_ptr,
                                    child,
                                    false,
                                    barrier,
                                ) {
                                    return Err(InsertError::Full(entry));
                                }
                                return Err(InsertError::Retry(entry));
                            }
                        },
                    }
                }
                // `child_ptr` being null indicates that the node is bound to be freed.
                return Err(InsertError::Retry((key, value)));
            }

            let unbounded_ptr = self.children.1.load(Relaxed, barrier);
            if let Some(unbounded_ref) = unbounded_ptr.as_ref() {
                debug_assert!(unbounded_ptr.tag() == Tag::None);
                if !(self.children.0).validate(result.1) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                // Tries to insert into the unbounded child, and tries to split the unbounded if it is full.
                match unbounded_ref.insert(key, value, barrier) {
                    Ok(_) => return Ok(()),
                    Err(err) => match err {
                        InsertError::Duplicated(_) | InsertError::Retry(_) => return Err(err),
                        InsertError::Full(entry) => {
                            if !self.split_node(
                                None,
                                unbounded_ptr,
                                &(self.children.1),
                                false,
                                barrier,
                            ) {
                                return Err(InsertError::Full(entry));
                            }
                            return Err(InsertError::Retry(entry));
                        }
                    },
                };
            }
            // `unbounded_ptr` being null indicates that the node is bound to be freed.
            debug_assert!(unbounded_ptr.tag() == Tag::First);
            return Err(InsertError::Retry((key, value)));
        }
    }

    /// Removes an entry associated with the given key.
    fn remove<Q>(&self, key: &Q, barrier: &Barrier) -> Result<bool, RemoveError>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        loop {
            let result = (self.children.0).min_greater_equal(key);
            if let Some((_, child)) = result.0 {
                let child_ptr = child.load(Acquire, barrier);
                if !(self.children.0).validate(result.1) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                if let Some(child_ref) = child_ptr.as_ref() {
                    return match child_ref.remove(key, barrier) {
                        Ok(removed) => Ok(removed),
                        Err(remove_error) => match remove_error {
                            RemoveError::Empty(removed) => self.coalesce(removed, barrier),
                            RemoveError::Retry(_) => Err(remove_error),
                        },
                    };
                }
                // `child_ptr` being null indicates that the node is bound to be freed.
                return Err(RemoveError::Retry(false));
            }
            let unbounded_ptr = (self.children.1).load(Acquire, barrier);
            if let Some(unbounded_ref) = unbounded_ptr.as_ref() {
                debug_assert!(unbounded_ptr.tag() == Tag::None);
                if !(self.children.0).validate(result.1) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                return match unbounded_ref.remove(key, barrier) {
                    Ok(removed) => Ok(removed),
                    Err(remove_error) => match remove_error {
                        RemoveError::Empty(removed) => self.coalesce(removed, barrier),
                        RemoveError::Retry(_) => Err(remove_error),
                    },
                };
            }
            // `unbounded_ptr` being null indicates that the node is bound to be freed.
            debug_assert!(unbounded_ptr.tag() == Tag::First);
            return Err(RemoveError::Empty(false));
        }
    }

    /// Splits a full node.
    ///
    /// Returns `true` if the node is successfully split or a conflict is detected, `false`
    /// otherwise.
    #[allow(clippy::too_many_lines)]
    fn split_node(
        &self,
        full_node_key: Option<K>,
        full_node_ptr: Ptr<Node<K, V>>,
        full_node: &AtomicArc<Node<K, V>>,
        root_node_split: bool,
        barrier: &Barrier,
    ) -> bool {
        let new_split_nodes_ptr;
        let full_node_ref = full_node_ptr.as_ref().unwrap();
        if let Ok((_, ptr)) = self.new_children.compare_exchange(
            Ptr::null(),
            (
                Some(Arc::new(NewNodes {
                    origin_node_key: full_node_key,
                    origin_node: full_node.clone(Relaxed, barrier),
                    low_key_node: AtomicArc::null(),
                    middle_key: None,
                    high_key_node: AtomicArc::null(),
                })),
                Tag::None,
            ),
            Acquire,
            Relaxed,
        ) {
            new_split_nodes_ptr = ptr;
        } else {
            full_node_ref.rollback(barrier);
            return true;
        }

        // Checks if the node is ready for a child split.
        if full_node_ptr != full_node.load(Relaxed, barrier) {
            // The unbounded child being null indicates that the node is being dropped,
            // and the mismatch between pointer values indicates that there has been a change
            // to the node.
            if let Some(unused_children) = self.new_children.swap((None, Tag::None), Relaxed) {
                barrier.reclaim(unused_children);
            }
            full_node_ref.rollback(barrier);
            return true;
        }

        // Copies entries to the newly allocated leaves.
        let new_split_nodes_mut_ref = unsafe {
            #[allow(clippy::cast_ref_to_mut)]
            &mut *(new_split_nodes_ptr.as_ref().unwrap() as *const NewNodes<K, V>
                as *mut NewNodes<K, V>)
        };

        match &full_node_ref.entry {
            NodeType::Internal(full_internal_node) => {
                debug_assert!(!full_internal_node
                    .new_children
                    .load(Relaxed, barrier)
                    .is_null());
                let new_children_ref = full_internal_node
                    .new_children
                    .load(Relaxed, barrier)
                    .as_ref()
                    .unwrap();

                // Copies nodes except for the known full node to the newly allocated internal node entries.
                let internal_nodes = (
                    Arc::new(Node::new_internal_node()),
                    Arc::new(Node::new_internal_node()),
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
                #[allow(clippy::type_complexity)]
                let mut entry_array: [Option<(Option<&K>, AtomicArc<Node<K, V>>)>;
                    ARRAY_SIZE + 2] = Default::default();
                let mut num_entries = 0;
                for entry in Scanner::new(&full_internal_node.children.0) {
                    if new_children_ref
                        .origin_node_key
                        .as_ref()
                        .map_or_else(|| false, |key| entry.0.cmp(key) == Ordering::Equal)
                    {
                        let low_key_node_shared =
                            new_children_ref.low_key_node.load(Relaxed, barrier);
                        if !low_key_node_shared.is_null() {
                            entry_array[num_entries].replace((
                                Some(new_children_ref.middle_key.as_ref().unwrap()),
                                new_children_ref.low_key_node.clone(Relaxed, barrier),
                            ));
                            num_entries += 1;
                        }
                        let high_key_node_shared =
                            new_children_ref.high_key_node.load(Relaxed, barrier);
                        if !high_key_node_shared.is_null() {
                            entry_array[num_entries].replace((
                                Some(entry.0),
                                new_children_ref.high_key_node.clone(Relaxed, barrier),
                            ));
                            num_entries += 1;
                        }
                    } else {
                        entry_array[num_entries]
                            .replace((Some(entry.0), entry.1.clone(Relaxed, barrier)));
                        num_entries += 1;
                    }
                }
                if new_children_ref.origin_node_key.is_some() {
                    // If the origin is a bounded node, assign the unbounded node to the high key node's unbounded.
                    entry_array[num_entries]
                        .replace((None, full_internal_node.children.1.clone(Relaxed, barrier)));
                    num_entries += 1;
                } else {
                    // If the origin is an unbounded node, assign the high key node to the high key node's unbounded.
                    let low_key_node_shared = new_children_ref.low_key_node.load(Relaxed, barrier);
                    if !low_key_node_shared.is_null() {
                        entry_array[num_entries].replace((
                            Some(new_children_ref.middle_key.as_ref().unwrap()),
                            new_children_ref.low_key_node.clone(Relaxed, barrier),
                        ));
                        num_entries += 1;
                    }
                    let high_key_node_shared =
                        new_children_ref.high_key_node.load(Relaxed, barrier);
                    if !high_key_node_shared.is_null() {
                        entry_array[num_entries].replace((
                            None,
                            new_children_ref.high_key_node.clone(Relaxed, barrier),
                        ));
                        num_entries += 1;
                    }
                }
                debug_assert!(num_entries >= 2);

                let low_key_node_array_size = num_entries / 2;
                for (index, entry) in entry_array.iter().enumerate() {
                    if let Some(entry) = entry {
                        match (index + 1).cmp(&low_key_node_array_size) {
                            Ordering::Less => {
                                low_key_nodes.as_ref().unwrap().0.insert(
                                    entry.0.unwrap().clone(),
                                    entry.1.clone(Relaxed, barrier),
                                );
                            }
                            Ordering::Equal => {
                                new_split_nodes_mut_ref
                                    .middle_key
                                    .replace(entry.0.unwrap().clone());
                                low_key_nodes
                                    .as_ref()
                                    .unwrap()
                                    .1
                                    .swap((entry.1.get_arc(Relaxed, barrier), Tag::None), Relaxed);
                            }
                            Ordering::Greater => {
                                if let Some(key) = entry.0 {
                                    high_key_nodes
                                        .as_ref()
                                        .unwrap()
                                        .0
                                        .insert(key.clone(), entry.1.clone(Relaxed, barrier));
                                } else {
                                    high_key_nodes.as_ref().unwrap().1.swap(
                                        (entry.1.get_arc(Relaxed, barrier), Tag::None),
                                        Relaxed,
                                    );
                                }
                            }
                        };
                    } else {
                        break;
                    }
                }

                // Turns the new nodes into internal nodes.
                new_split_nodes_mut_ref
                    .low_key_node
                    .swap((Some(internal_nodes.0), Tag::None), Relaxed);
                new_split_nodes_mut_ref
                    .high_key_node
                    .swap((Some(internal_nodes.1), Tag::None), Relaxed);
            }
            NodeType::Leaf(leaf_node) => {
                // Copies leaves except for the known full leaf to the newly allocated leaf node entries.
                let leaf_nodes = (
                    Arc::new(Node::new_leaf_node()),
                    Arc::new(Node::new_leaf_node()),
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
                        barrier,
                    )
                    .map(|middle_key| new_split_nodes_mut_ref.middle_key.replace(middle_key));

                // Turns the new leaves into leaf nodes.
                new_split_nodes_mut_ref
                    .low_key_node
                    .swap((Some(leaf_nodes.0), Tag::None), Relaxed);
                new_split_nodes_mut_ref
                    .high_key_node
                    .swap((Some(leaf_nodes.1), Tag::None), Relaxed);
            }
        };

        // The full node is the current root: split_root processes the rest.
        if root_node_split {
            return true;
        }

        // Inserts the newly allocated internal nodes into the main array.
        if let Some(((middle_key, _), _)) = self.children.0.insert(
            new_split_nodes_mut_ref.middle_key.take().unwrap(),
            new_split_nodes_mut_ref.low_key_node.clone(Relaxed, barrier),
        ) {
            // Insertion failed: expects that the parent splits this node.
            new_split_nodes_mut_ref.middle_key.replace(middle_key);
            return false;
        }

        // Replaces the full node with the high-key node.
        let unused_node = full_node.swap(
            (
                new_split_nodes_mut_ref
                    .high_key_node
                    .get_arc(Relaxed, barrier),
                Tag::None,
            ),
            Release,
        );

        // Drops the deprecated nodes.
        // - Still, the deprecated full leaf can be reachable by Scanners.
        if let Some(unused_node) = unused_node {
            // Cleans up the split operation by unlinking the unused node.
            unused_node.unlink(barrier);
            barrier.reclaim(unused_node);
        }

        // Unlocks the node.
        if let Some(new_split_nodes) = self.new_children.swap((None, Tag::None), Release) {
            barrier.reclaim(new_split_nodes);
        }
        true
    }

    /// Rolls back the ongoing split operation recursively.
    fn rollback(&self, barrier: &Barrier) {
        let new_children_ptr = self.new_children.load(Relaxed, barrier);
        if let Some(new_children_ref) = new_children_ptr.as_ref() {
            let origin_node_ptr = new_children_ref.origin_node.load(Relaxed, barrier);
            origin_node_ptr.as_ref().unwrap().rollback(barrier);
            let low_key_node_ptr = new_children_ref.low_key_node.load(Relaxed, barrier);
            if let Some(low_key_node_ref) = low_key_node_ptr.as_ref() {
                low_key_node_ref.unlink(barrier);
            }
            let high_key_node_ptr = new_children_ref.high_key_node.load(Relaxed, barrier);
            if let Some(high_key_node_ref) = high_key_node_ptr.as_ref() {
                high_key_node_ref.unlink(barrier);
            }
            self.new_children.swap((None, Tag::None), Release);
        };
    }

    /// Unlinks all the leaves.
    ///
    /// It is called only when the internal node is a temporary one for split/merge,
    /// or has become unreachable after split/merge/remove.
    fn unlink(&self, barrier: &Barrier) {
        for entry in Scanner::new(&self.children.0) {
            entry.1.swap((None, Tag::None), Relaxed);
        }
        self.children.1.swap((None, Tag::First), Relaxed);

        // In case the node is locked, implying that it has been split, unlinks recursively.
        //
        // Keeps the internal node locked to prevent locking attempts.
        let unused_nodes = self.new_children.swap((None, Tag::First), Relaxed);
        if let Some(unused_node_ref) = unused_nodes.as_ref() {
            let obsolete_node_ptr = unused_node_ref.origin_node.load(Relaxed, barrier);
            if let Some(obsolete_node_ref) = obsolete_node_ptr.as_ref() {
                obsolete_node_ref.unlink(barrier);
            };
        }
    }

    /// Tries to coalesce nodes.
    fn coalesce(&self, removed: bool, barrier: &Barrier) -> Result<bool, RemoveError> {
        let lock = InternalNodeLocker::try_lock(self);
        if lock.is_none() {
            return Err(RemoveError::Retry(removed));
        }

        for entry in Scanner::new(&self.children.0) {
            let node_ptr = entry.1.load(Relaxed, barrier);
            let node_ref = node_ptr.as_ref().unwrap();
            if node_ref.obsolete(barrier) {
                self.children.0.remove(entry.0);
                // Once the key is removed, it is safe to deallocate the node as the validation
                // loop ensures the absence of readers.
                if let Some(node) = entry.1.swap((None, Tag::None), Release) {
                    barrier.reclaim(node);
                }
            }
        }

        let unbounded_ptr = self.children.1.load(Relaxed, barrier);
        if let Some(unbounded_ref) = unbounded_ptr.as_ref() {
            if unbounded_ref.obsolete(barrier) {
                // If the unbounded node has become obsolete, either marks the node obsolete,
                // or replaces the unbounded node with another.
                if let Some(max_entry) = self.children.0.max() {
                    // Firstly, replaces the unbounded node with the max entry.
                    self.children.1.swap(
                        (max_entry.1.load(Relaxed, barrier).try_into_arc(), Tag::None),
                        Release,
                    );
                    // Then, removes the node from the children list.
                    if self.children.0.remove(max_entry.0).2 {
                        // Retires the children if it was the last child.
                        let result = self.children.0.retire();
                        debug_assert!(result);
                    }
                    max_entry.1.swap((None, Tag::None), Release);
                } else {
                    // Deprecates this internal node.
                    let result = self.children.0.retire();
                    debug_assert!(result);
                    self.children.1.swap((None, Tag::First), Release);
                }
                unbounded_ref.unlink(barrier);
            }
        }

        if self.children.1.load(Relaxed, barrier).is_null() {
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

impl<K, V> InternalNode<K, V>
where
    K: 'static + Clone + Display + Ord + Send + Sync,
    V: 'static + Clone + Display + Send + Sync,
{
    fn print<T: std::io::Write>(
        &self,
        output: &mut T,
        depth: usize,
        barrier: &Barrier,
    ) -> std::io::Result<()> {
        // Collects information.
        #[allow(clippy::type_complexity)]
        let mut child_ref_array: [Option<(Option<&Node<K, V>>, Option<&K>, usize)>;
            ARRAY_SIZE + 1] = [None; ARRAY_SIZE + 1];
        let mut scanner = Scanner::new_including_removed(&self.children.0);
        let mut index = 0;
        while let Some(entry) = scanner.next() {
            if scanner.removed() {
                child_ref_array[index].replace((None, Some(entry.0), index));
            } else {
                let child_share_ptr = entry.1.load(Relaxed, barrier);
                child_ref_array[index].replace((
                    Some(child_share_ptr.as_ref().unwrap()),
                    Some(entry.0),
                    index,
                ));
            }
            index += 1;
        }
        let unbounded_ptr = self.children.1.load(Relaxed, barrier);
        if let Some(unbounded_ref) = unbounded_ptr.as_ref() {
            child_ref_array[index].replace((Some(unbounded_ref), None, index));
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
        #[allow(clippy::manual_flatten)]
        for child_info in &child_ref_array {
            if let Some((child_ref, key_ref, index)) = child_info {
                let font_color = if child_ref.is_some() { "black" } else { "red" };
                if let Some(key_ref) = key_ref {
                    output.write_fmt(format_args!(
                        "<td port='p_{}'><font color='{}'>{}</font></td>",
                        index, font_color, key_ref,
                    ))?;
                } else {
                    output.write_fmt(format_args!(
                        "<td port='p_{}'><font color='{}'>\u{221e}</font></td>",
                        index, font_color,
                    ))?;
                }
            }
        }
        output.write_fmt(format_args!("</tr>\n</table>\n>]\n"))?;

        // Prints the edges and children.
        #[allow(clippy::manual_flatten)]
        for child_info in &child_ref_array {
            if let Some((Some(child_ref), _, index)) = child_info {
                output.write_fmt(format_args!(
                    "{}:p_{} -> {}\n",
                    self.id(),
                    index,
                    child_ref.id()
                ))?;
                child_ref.print(output, depth + 1, barrier)?;
            }
        }

        std::io::Result::Ok(())
    }
}

/// Internal node locker.
struct InternalNodeLocker<'n, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    lock: &'n AtomicArc<NewNodes<K, V>>,
    /// When the internal node is bound to be dropped, the flag may be set true.
    deprecate: bool,
}

impl<'n, K, V> InternalNodeLocker<'n, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    fn try_lock(internal_node: &'n InternalNode<K, V>) -> Option<InternalNodeLocker<'n, K, V>> {
        if internal_node
            .new_children
            .compare_exchange(Ptr::null(), (None, Tag::First), Acquire, Relaxed)
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
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    fn drop(&mut self) {
        if !self.deprecate {
            self.lock.swap((None, Tag::None), Release);
        }
    }
}

/// Intermediate split node.
///
/// It does not own the children, thus only nullifying pointers when drop.
struct NewNodes<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    /// None: unbounded node
    origin_node_key: Option<K>,
    origin_node: AtomicArc<Node<K, V>>,
    low_key_node: AtomicArc<Node<K, V>>,
    middle_key: Option<K>,
    high_key_node: AtomicArc<Node<K, V>>,
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::ebr;

    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;

    #[test]
    fn node() {
        let num_threads = 16;
        let range = 16384;
        let barrier = Arc::new(Barrier::new(num_threads));
        let node = Arc::new(Node::new_leaf_node());
        assert!(node.insert(0, 0, &ebr::Barrier::new()).is_ok());
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
                    let ebr_barrier = ebr::Barrier::new();
                    loop {
                        match node_copied.insert(key, key, &ebr_barrier) {
                            Ok(()) => {
                                inserted_keys.push(key);
                                break;
                            }
                            Err(err) => match err {
                                InsertError::Duplicated(_) => unreachable!(),
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
                for key in &inserted_keys {
                    vector.push(*key);
                }
            }));
        }
        for handle in thread_handles {
            handle.join().unwrap();
        }

        let ebr_barrier = ebr::Barrier::new();
        if full.load(Relaxed) {
            node.rollback(&ebr_barrier);
        }
        let mut prev = 0;
        if let Ok(scanner) = node.min(&ebr_barrier) {
            let mut iterated = 0;
            for entry in scanner {
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
