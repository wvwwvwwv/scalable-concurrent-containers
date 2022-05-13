use super::internal_node::{self, InternalNode};
use super::leaf::{InsertResult, RemoveResult, Scanner};
use super::leaf_node::{self, LeafNode};

use crate::ebr::{Arc, AtomicArc, Barrier, Tag};
use crate::wait_queue::AsyncWait;

use std::borrow::Borrow;
use std::sync::atomic::Ordering::{self, Acquire, Relaxed, Release};

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
    pub(super) node: Type<K, V>,
}

impl<K, V> Node<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    /// Creates a new [`InternalNode`].
    pub(crate) fn new_internal_node() -> Node<K, V> {
        Node {
            node: Type::Internal(InternalNode::new()),
        }
    }

    /// Creates a new [`LeafNode`].
    pub(crate) fn new_leaf_node() -> Node<K, V> {
        Node {
            node: Type::Leaf(LeafNode::new()),
        }
    }

    /// Returns a reference to the node internal.
    pub(crate) fn node(&self) -> &Type<K, V> {
        &self.node
    }

    /// Returns the depth of the node.
    pub(crate) fn depth(&self, depth: usize, barrier: &Barrier) -> usize {
        match &self.node {
            Type::Internal(internal_node) => internal_node.depth(depth, barrier),
            Type::Leaf(_) => depth,
        }
    }

    /// Checks if the node has retired.
    pub(crate) fn retired(&self, mo: Ordering) -> bool {
        match &self.node {
            Type::Internal(internal_node) => internal_node.retired(mo),
            Type::Leaf(leaf_node) => leaf_node.retired(mo),
        }
    }

    /// Searches for an entry associated with the given key.
    pub(crate) fn search<'b, Q>(&self, key: &Q, barrier: &'b Barrier) -> Option<&'b V>
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
    pub(crate) fn min<'b>(&self, barrier: &'b Barrier) -> Option<Scanner<'b, K, V>> {
        match &self.node {
            Type::Internal(internal_node) => internal_node.min(barrier),
            Type::Leaf(leaf_node) => leaf_node.min(barrier),
        }
    }

    /// Returns a [`Scanner`] pointing to an entry that is close enough to the entry with the
    /// maximum key among those keys smaller than or equal to the given key.
    ///
    /// This method is not linearlizable.
    pub(crate) fn max_le_appr<'b, Q>(
        &self,
        key: &Q,
        barrier: &'b Barrier,
    ) -> Option<Scanner<'b, K, V>>
    where
        K: 'b + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match &self.node {
            Type::Internal(internal_node) => internal_node.max_le_appr(key, barrier),
            Type::Leaf(leaf_node) => leaf_node.max_le_appr(key, barrier),
        }
    }

    /// Inserts a key-value pair.
    pub(crate) fn insert(
        &self,
        key: K,
        value: V,
        async_wait: Option<*mut AsyncWait>,
        barrier: &Barrier,
    ) -> Result<InsertResult<K, V>, (K, V)> {
        match &self.node {
            Type::Internal(internal_node) => internal_node.insert(key, value, async_wait, barrier),
            Type::Leaf(leaf_node) => leaf_node.insert(key, value, async_wait, barrier),
        }
    }

    /// Removes an entry associated with the given key.
    pub(crate) fn remove_if<Q, F: FnMut(&V) -> bool>(
        &self,
        key: &Q,
        condition: &mut F,
        async_wait: Option<*mut AsyncWait>,
        barrier: &Barrier,
    ) -> Result<RemoveResult, bool>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match &self.node {
            Type::Internal(internal_node) => {
                internal_node.remove_if::<_, _>(key, condition, async_wait, barrier)
            }
            Type::Leaf(leaf_node) => {
                leaf_node.remove_if::<_, _>(key, condition, async_wait, barrier)
            }
        }
    }

    /// Splits the current root node.
    pub(crate) fn split_root<const ASYNC: bool>(
        key: K,
        value: V,
        root: &AtomicArc<Node<K, V>>,
        barrier: &Barrier,
    ) -> (K, V) {
        // The fact that the `TreeIndex` calls this function means that the root is full and
        // locked.
        let mut new_root: Node<K, V> = Node::new_internal_node();
        if let Type::Internal(internal_node) = &mut new_root.node {
            internal_node.unbounded_child = root.clone(Relaxed, barrier);
            let result = internal_node.split_node(
                key,
                value,
                None,
                root.load(Relaxed, barrier),
                &internal_node.unbounded_child,
                true,
                None,
                barrier,
            );
            let (key, value) = match result {
                Ok(InsertResult::Retry(k, v)) => (k, v),
                _ => unreachable!(),
            };

            // Updates the pointer before unlocking the root.
            let new_root = Arc::new(new_root);
            if let Some(old_root) = root.swap((Some(new_root.clone()), Tag::None), Release).0 {
                if let Type::Internal(internal_node) = &new_root.node {
                    internal_node.finish_split(barrier);
                    old_root.commit(barrier);
                }
                barrier.reclaim(old_root);
            };

            (key, value)
        } else {
            (key, value)
        }
    }

    /// Removes the current root node.
    ///
    /// # Errors
    ///
    /// Returns an error if a conflict is detected.
    pub(crate) fn remove_root(
        root: &AtomicArc<Node<K, V>>,
        async_wait: Option<*mut AsyncWait>,
        barrier: &Barrier,
    ) -> Result<bool, ()> {
        let root_ptr = root.load(Acquire, barrier);
        if let Some(root_ref) = root_ptr.as_ref() {
            let mut internal_node_locker = None;
            let mut leaf_node_locker = None;
            match &root_ref.node {
                Type::Internal(internal_node) => {
                    if let Some(locker) = internal_node::Locker::try_lock(internal_node, barrier) {
                        internal_node_locker.replace(locker);
                    } else {
                        internal_node.wait(async_wait, barrier);
                    }
                }
                Type::Leaf(leaf_node) => {
                    if let Some(locker) = leaf_node::Locker::try_lock(leaf_node, barrier) {
                        leaf_node_locker.replace(locker);
                    } else {
                        leaf_node.wait(async_wait, barrier);
                    }
                }
            };
            if internal_node_locker.is_none() && leaf_node_locker.is_none() {
                // The root node is locked by another thread.
                return Err(());
            }
            if !root_ref.retired(Relaxed) {
                // The root node is still usable.
                return Ok(false);
            }

            match root.compare_exchange(root_ptr, (None, Tag::None), Acquire, Acquire, barrier) {
                Ok((old_root, _)) => {
                    if let Some(old_root) = old_root {
                        barrier.reclaim(old_root);
                    }
                    return Ok(true);
                }
                Err(_) => {
                    return Ok(false);
                }
            }
        }

        Err(())
    }

    /// Commits an on-going structural change.
    pub(crate) fn commit(&self, barrier: &Barrier) {
        match &self.node {
            Type::Internal(internal_node) => internal_node.commit(barrier),
            Type::Leaf(leaf_node) => leaf_node.commit(barrier),
        }
    }

    /// Rolls back an on-going structural change.
    pub(crate) fn rollback(&self, barrier: &Barrier) {
        match &self.node {
            Type::Internal(internal_node) => internal_node.rollback(barrier),
            Type::Leaf(leaf_node) => leaf_node.rollback(barrier),
        }
    }

    /// Cleans up logically deleted [`LeafNode`] instances in the linked list.
    ///
    /// If the target leaf node does not exist in the sub-tree, returns `false`.
    pub(crate) fn cleanup_link<'b, Q>(
        &self,
        key: &Q,
        traverse_max: bool,
        barrier: &'b Barrier,
    ) -> bool
    where
        K: 'b + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match &self.node {
            Type::Internal(internal_node) => internal_node.cleanup_link(key, traverse_max, barrier),
            Type::Leaf(leaf_node) => leaf_node.cleanup_link(key, traverse_max, barrier),
        }
    }
}
