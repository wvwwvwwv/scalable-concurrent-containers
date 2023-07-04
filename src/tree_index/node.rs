use super::internal_node::{self, InternalNode};
use super::leaf::{InsertResult, RemoveResult, Scanner};
use super::leaf_node::{self, LeafNode};
use crate::ebr::{Arc, AtomicArc, Barrier, Tag};
use crate::wait_queue::DeriveAsyncWait;
use std::borrow::Borrow;
use std::fmt::{self, Debug};
use std::sync::atomic::Ordering::{self, Acquire, Relaxed, Release};

/// [`Node`] is either [`Self::Internal`] or [`Self::Leaf`].
pub enum Node<K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
{
    /// Internal node.
    Internal(InternalNode<K, V>),

    /// Leaf node.
    Leaf(LeafNode<K, V>),
}

impl<K, V> Node<K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
{
    /// Creates a new [`InternalNode`].
    #[inline]
    pub(super) fn new_internal_node() -> Self {
        Self::Internal(InternalNode::new())
    }

    /// Creates a new [`LeafNode`].
    #[inline]
    pub(super) fn new_leaf_node() -> Self {
        Self::Leaf(LeafNode::new())
    }

    /// Returns the depth of the node.
    #[inline]
    pub(super) fn depth(&self, depth: usize, barrier: &Barrier) -> usize {
        match &self {
            Self::Internal(internal_node) => internal_node.depth(depth, barrier),
            Self::Leaf(_) => depth,
        }
    }

    /// Checks if the node has retired.
    #[inline]
    pub(super) fn retired(&self, mo: Ordering) -> bool {
        match &self {
            Self::Internal(internal_node) => internal_node.retired(mo),
            Self::Leaf(leaf_node) => leaf_node.retired(mo),
        }
    }

    /// Searches for an entry associated with the given key.
    #[inline]
    pub(super) fn search<'b, Q>(&self, key: &Q, barrier: &'b Barrier) -> Option<&'b V>
    where
        K: 'b + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match &self {
            Self::Internal(internal_node) => internal_node.search(key, barrier),
            Self::Leaf(leaf_node) => leaf_node.search(key, barrier),
        }
    }

    /// Returns the minimum key-value pair.
    ///
    /// This method is not linearizable.
    #[inline]
    pub(super) fn min<'b>(&self, barrier: &'b Barrier) -> Option<Scanner<'b, K, V>> {
        match &self {
            Self::Internal(internal_node) => internal_node.min(barrier),
            Self::Leaf(leaf_node) => leaf_node.min(barrier),
        }
    }

    /// Returns a [`Scanner`] pointing to an entry that is close enough to the entry with the
    /// maximum key among those keys smaller than or equal to the given key.
    ///
    /// This method is not linearizable.
    #[inline]
    pub(super) fn max_le_appr<'b, Q>(
        &self,
        key: &Q,
        barrier: &'b Barrier,
    ) -> Option<Scanner<'b, K, V>>
    where
        K: 'b + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match &self {
            Self::Internal(internal_node) => internal_node.max_le_appr(key, barrier),
            Self::Leaf(leaf_node) => leaf_node.max_le_appr(key, barrier),
        }
    }

    /// Inserts a key-value pair.
    #[inline]
    pub(super) fn insert<D: DeriveAsyncWait>(
        &self,
        key: K,
        val: V,
        async_wait: &mut D,
        barrier: &Barrier,
    ) -> Result<InsertResult<K, V>, (K, V)> {
        match &self {
            Self::Internal(internal_node) => internal_node.insert(key, val, async_wait, barrier),
            Self::Leaf(leaf_node) => leaf_node.insert(key, val, async_wait, barrier),
        }
    }

    /// Removes an entry associated with the given key.
    #[inline]
    pub(super) fn remove_if<Q, F: FnMut(&V) -> bool, D>(
        &self,
        key: &Q,
        condition: &mut F,
        async_wait: &mut D,
        barrier: &Barrier,
    ) -> Result<RemoveResult, bool>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
        D: DeriveAsyncWait,
    {
        match &self {
            Self::Internal(internal_node) => {
                internal_node.remove_if::<_, _, _>(key, condition, async_wait, barrier)
            }
            Self::Leaf(leaf_node) => {
                leaf_node.remove_if::<_, _, _>(key, condition, async_wait, barrier)
            }
        }
    }

    /// Splits the current root node.
    #[inline]
    pub(super) fn split_root(
        key: K,
        val: V,
        root: &AtomicArc<Node<K, V>>,
        barrier: &Barrier,
    ) -> (K, V) {
        // The fact that the `TreeIndex` calls this function means that the root is full and
        // locked.
        let mut new_root = Arc::new(Node::new_internal_node());
        if let Some(Self::Internal(internal_node)) = unsafe { new_root.get_mut() } {
            internal_node.unbounded_child = root.clone(Relaxed, barrier);
            let result = internal_node.split_node(
                key,
                val,
                None,
                root.load(Relaxed, barrier),
                &internal_node.unbounded_child,
                true,
                &mut (),
                barrier,
            );
            let Ok(InsertResult::Retry(key, val)) = result else {
                unreachable!()
            };

            // Updates the pointer before unlocking the root.
            let new_root_ref = new_root.ptr(barrier).as_ref();
            if let Some(old_root) = root.swap((Some(new_root), Tag::None), Release).0 {
                if let Some(Self::Internal(internal_node)) = new_root_ref.as_ref() {
                    internal_node.finish_split(barrier);
                    old_root.commit(barrier);
                }
                let _: bool = old_root.release(barrier);
            };

            (key, val)
        } else {
            (key, val)
        }
    }

    /// Removes the current root node.
    ///
    /// # Errors
    ///
    /// Returns an error if a conflict is detected.
    #[inline]
    pub(super) fn remove_root<D: DeriveAsyncWait>(
        root: &AtomicArc<Node<K, V>>,
        async_wait: &mut D,
        barrier: &Barrier,
    ) -> Result<bool, ()> {
        let root_ptr = root.load(Acquire, barrier);
        if let Some(root_ref) = root_ptr.as_ref() {
            let mut internal_node_locker = None;
            let mut leaf_node_locker = None;
            match root_ref {
                Self::Internal(internal_node) => {
                    if let Some(locker) = internal_node::Locker::try_lock(internal_node) {
                        internal_node_locker.replace(locker);
                    } else {
                        internal_node.wait(async_wait);
                    }
                }
                Self::Leaf(leaf_node) => {
                    if let Some(locker) = leaf_node::Locker::try_lock(leaf_node) {
                        leaf_node_locker.replace(locker);
                    } else {
                        leaf_node.wait(async_wait);
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
                    old_root.map(|r| r.release(barrier));
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
    #[inline]
    pub(super) fn commit(&self, barrier: &Barrier) {
        match &self {
            Self::Internal(internal_node) => internal_node.commit(barrier),
            Self::Leaf(leaf_node) => leaf_node.commit(barrier),
        }
    }

    /// Rolls back an on-going structural change.
    #[inline]
    pub(super) fn rollback(&self, barrier: &Barrier) {
        match &self {
            Self::Internal(internal_node) => internal_node.rollback(barrier),
            Self::Leaf(leaf_node) => leaf_node.rollback(barrier),
        }
    }

    /// Cleans up logically deleted [`LeafNode`] instances in the linked list.
    ///
    /// If the target leaf node does not exist in the sub-tree, returns `false`.
    #[inline]
    pub(super) fn cleanup_link<'b, Q>(
        &self,
        key: &Q,
        traverse_max: bool,
        barrier: &'b Barrier,
    ) -> bool
    where
        K: 'b + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match &self {
            Self::Internal(internal_node) => internal_node.cleanup_link(key, traverse_max, barrier),
            Self::Leaf(leaf_node) => leaf_node.cleanup_link(key, traverse_max, barrier),
        }
    }
}

impl<K, V> Debug for Node<K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Internal(_) => f.debug_tuple("Internal").finish(),
            Self::Leaf(_) => f.debug_tuple("Leaf").finish(),
        }
    }
}
