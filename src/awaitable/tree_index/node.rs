use super::internal_node::InternalNode;
use super::leaf::{InsertResult, RemoveResult, Scanner};
use super::leaf_node::LeafNode;

use crate::ebr::{AtomicArc, Barrier};

use std::borrow::Borrow;
use std::sync::atomic::Ordering;

/// [`Type`] indicates the type of a [`Node`].
pub enum Type<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    Internal(InternalNode<K, V>),
    Leaf(LeafNode<K, V>),
}

/// [`Node`] is either [`Type::Internal`] or [`Type::Leaf`].
pub struct Node<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    node: Type<K, V>,
}

impl<K, V> Node<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    /// Creates a new [`InternalNode`].
    pub fn new_internal_node() -> Node<K, V> {
        Node {
            node: Type::Internal(InternalNode::new()),
        }
    }

    /// Creates a new [`LeafNode`].
    pub fn new_leaf_node() -> Node<K, V> {
        Node {
            node: Type::Leaf(LeafNode::new()),
        }
    }

    /// Returns a reference to the node internal.
    pub fn node(&self) -> &Type<K, V> {
        &self.node
    }

    /// Returns the depth of the node.
    pub fn depth(&self, depth: usize, barrier: &Barrier) -> usize {
        match &self.node {
            Type::Internal(internal_node) => internal_node.depth(depth, barrier),
            Type::Leaf(_) => depth,
        }
    }

    /// Checks if the node is obsolete.
    pub fn obsolete(&self, mo: Ordering) -> bool {
        match &self.node {
            Type::Internal(internal_node) => internal_node.obsolete(mo),
            Type::Leaf(leaf_node) => leaf_node.obsolete(mo),
        }
    }

    /// Searches for an entry associated with the given key.
    pub fn search<'b, Q>(&self, key: &Q, barrier: &'b Barrier) -> Option<&'b V>
    where
        K: 'b + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match &self.node {
            Type::Internal(internal_node) => internal_node.search(key, barrier),
            Type::Leaf(leaf_node) => leaf_node.search(key, barrier),
        }
    }

    /// Returns the minimum key-value pair.
    ///
    /// This method is not linearlizable.
    pub fn min<'b>(&self, barrier: &'b Barrier) -> Option<Scanner<'b, K, V>> {
        match &self.node {
            Type::Internal(internal_node) => internal_node.min(barrier),
            Type::Leaf(leaf_node) => leaf_node.min(barrier),
        }
    }

    /// Returns a [`Scanner`] pointing to an entry that is close enough to the entry with the
    /// maximum key among those keys smaller than the given key.
    ///
    /// This method is not linearlizable.
    pub fn max_less_appr<'b, Q>(&self, key: &Q, barrier: &'b Barrier) -> Option<Scanner<'b, K, V>>
    where
        K: 'b + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match &self.node {
            Type::Internal(internal_node) => internal_node.max_less_appr(key, barrier),
            Type::Leaf(leaf_node) => leaf_node.max_less_appr(key, barrier),
        }
    }

    /// Inserts a key-value pair.
    pub fn insert(
        &self,
        key: K,
        value: V,
        barrier: &Barrier,
    ) -> Result<InsertResult<K, V>, (K, V)> {
        match &self.node {
            Type::Internal(internal_node) => internal_node.insert(key, value, barrier),
            Type::Leaf(leaf_node) => leaf_node.insert(key, value, barrier),
        }
    }

    /// Removes an entry associated with the given key.
    pub fn remove_if<Q, F: FnMut(&V) -> bool>(
        &self,
        key: &Q,
        condition: &mut F,
        barrier: &Barrier,
    ) -> Result<RemoveResult, bool>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match &self.node {
            Type::Internal(internal_node) => internal_node.remove_if(key, condition, barrier),
            Type::Leaf(leaf_node) => leaf_node.remove_if(key, condition, barrier),
        }
    }

    /// Splits the current root node.
    pub fn split_root(&self, _root: &AtomicArc<Node<K, V>>, _barrier: &Barrier) {
        /*
        // The fact that the TreeIndex calls this function means that the root is in a split
        // procedure, and the root is locked.
        debug_assert_eq!(
            self as *const Node<K, V>,
            root.load(Relaxed, barrier).as_raw()
        );
        let new_root: Node<K, V> = Node::new_internal_node();
        if let Type::Internal(internal_node) = &new_root.node {
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
                    if let Type::Internal(internal_node) = &new_root.node {
                        internal_node.new_children.swap((None, Tag::None), Release);
                    }
                };
            }
        }
        */
    }

    /// Removes the current root node.
    pub fn remove_root(_root: &AtomicArc<Node<K, V>>, _barrier: &Barrier) -> bool {
        /*
        let mut root_ptr = root.load(Acquire, barrier);
        loop {
            if let Some(root_ref) = root_ptr.as_ref() {
                let mut internal_node_locker = None;
                let mut leaf_node_locker = None;
                match &root_ref.node {
                    Type::Internal(internal_node) => {
                        if let Some(locker) = InternalNodeLocker::try_lock(internal_node) {
                            internal_node_locker.replace(locker);
                        }
                    }
                    Type::Leaf(leaf_node) => {
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
                if !root_ref.obsolete(barrier) {
                    break;
                }
                match root.compare_exchange(root_ptr, (None, Tag::None), Acquire, Acquire) {
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
        */
        false
    }

    /// Commits an on-going structural change.
    pub fn commit(&self, barrier: &Barrier) {
        match &self.node {
            Type::Internal(internal_node) => internal_node.commit(barrier),
            Type::Leaf(leaf_node) => leaf_node.commit(barrier),
        }
    }

    /// Rolls back an on-going structural change.
    pub fn rollback(&self, barrier: &Barrier) {
        match &self.node {
            Type::Internal(internal_node) => internal_node.rollback(barrier),
            Type::Leaf(leaf_node) => leaf_node.rollback(barrier),
        }
    }
}
