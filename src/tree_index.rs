//! The module implements [`TreeIndex`].

pub use crate::awaitable::tree_index::{Range, Visitor};

use super::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};

use crate::awaitable::tree_index::leaf::{InsertResult, RemoveResult};
use crate::awaitable::tree_index::node::Node;

use std::borrow::Borrow;
use std::ops::RangeBounds;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};

/// Scalable concurrent B+ tree.
///
/// [`TreeIndex`] is a B+ tree variant that is optimized for read operations. Read operations,
/// such as read, scan, are neither blocked nor interrupted by other threads. Write operations,
/// such as insert, remove, do not block if they do not entail structural changes to the tree.
///
/// ## The key features of [`TreeIndex`]
///
/// * Write-free read: read operations never modify the shared data.
/// * Near lock-free write: write operations do not block unless a structural change is needed.
///
/// ## The key statistics for [`TreeIndex`]
///
/// * The maximum number of key-value pairs that a leaf can store: 14.
/// * The maximum number of leaves or child nodes that a node can point to: 15.
/// * The size of metadata per key-value pair in a leaf: ~3-byte.
pub struct TreeIndex<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    root: AtomicArc<Node<K, V>>,
}

impl<K, V> TreeIndex<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    /// Creates an empty [`TreeIndex`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// assert!(treeindex.read(&1, |_, v| *v).is_none());
    /// ```
    #[must_use]
    pub fn new() -> TreeIndex<K, V> {
        TreeIndex {
            root: AtomicArc::null(),
        }
    }

    /// Inserts a key-value pair.
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied key-value pair if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// assert!(treeindex.insert(1, 10).is_ok());
    /// assert_eq!(treeindex.insert(1, 11).err().unwrap(), (1, 11));
    /// assert_eq!(treeindex.read(&1, |k, v| *v).unwrap(), 10);
    /// ```
    #[inline]
    pub fn insert(&self, mut key: K, mut value: V) -> Result<(), (K, V)> {
        let barrier = Barrier::new();
        loop {
            if let Some(root_ref) = self.root.load(Acquire, &barrier).as_ref() {
                match root_ref.insert(key, value, &barrier) {
                    Ok(r) => match r {
                        InsertResult::Success => return Ok(()),
                        InsertResult::Frozen(k, v) => {
                            key = k;
                            value = v;
                            std::thread::yield_now();
                        }
                        InsertResult::Duplicate(k, v) => return Err((k, v)),
                        InsertResult::Full(k, v) => {
                            let (k, v) = Node::split_root(k, v, &self.root, &barrier);
                            key = k;
                            value = v;
                            continue;
                        }
                        InsertResult::Retired(k, v) => {
                            key = k;
                            value = v;
                            if !matches!(Node::remove_root(&self.root, &barrier), Ok(true)) {
                                std::thread::yield_now();
                            }
                        }
                    },
                    Err((k, v)) => {
                        key = k;
                        value = v;
                        std::thread::yield_now();
                    }
                }
            }

            let new_root = Arc::new(Node::new_leaf_node());
            let _result = self.root.compare_exchange(
                Ptr::null(),
                (Some(new_root), Tag::None),
                AcqRel,
                Acquire,
            );
        }
    }

    /// Removes a key-value pair.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// assert!(!treeindex.remove(&1));
    /// assert!(treeindex.insert(1, 10).is_ok());
    /// assert!(treeindex.remove(&1));
    /// ```
    #[inline]
    pub fn remove<Q>(&self, key_ref: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.remove_if(key_ref, |_| true)
    }

    /// Removes a key-value pair if the given condition is met.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// assert!(treeindex.insert(1, 10).is_ok());
    /// assert!(!treeindex.remove_if(&1, |v| *v == 0));
    /// assert!(treeindex.remove_if(&1, |v| *v == 10));
    /// ```
    #[inline]
    pub fn remove_if<Q, F: FnMut(&V) -> bool>(&self, key_ref: &Q, mut condition: F) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let barrier = Barrier::new();
        let mut has_been_removed = false;
        loop {
            if let Some(root_ref) = self.root.load(Acquire, &barrier).as_ref() {
                match root_ref.remove_if(key_ref, &mut condition, &barrier) {
                    Ok(r) => match r {
                        RemoveResult::Success => return true,
                        RemoveResult::Fail => return has_been_removed,
                        RemoveResult::Retired => {
                            if matches!(Node::remove_root(&self.root, &barrier), Ok(true)) {
                                return true;
                            }
                            has_been_removed = true;
                            std::thread::yield_now();
                        }
                    },
                    Err(removed) => {
                        if removed {
                            has_been_removed = true;
                        }
                        std::thread::yield_now();
                    }
                }
            } else {
                return has_been_removed;
            }
        }
    }

    /// Reads a key-value pair.
    ///
    /// It returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// assert!(treeindex.read(&1, |k, v| *v).is_none());
    /// assert!(treeindex.insert(1, 10).is_ok());
    /// assert_eq!(treeindex.read(&1, |k, v| *v).unwrap(), 10);
    /// ```
    #[inline]
    pub fn read<Q, R, F: FnOnce(&Q, &V) -> R>(&self, key_ref: &Q, reader: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let barrier = Barrier::new();
        self.read_with(key_ref, reader, &barrier)
    }

    /// Reads a key-value pair using the supplied [`Barrier`].
    ///
    /// It enables the caller to use the value reference outside the method. It returns `None`
    /// if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    /// use scc::ebr::Barrier;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// assert!(treeindex.insert(1, 10).is_ok());
    ///
    /// let barrier = Barrier::new();
    /// let value_ref = treeindex.read_with(&1, |k, v| v, &barrier).unwrap();
    /// assert_eq!(*value_ref, 10);
    /// ```
    #[inline]
    pub fn read_with<'b, Q, R, F: FnOnce(&Q, &'b V) -> R>(
        &self,
        key_ref: &Q,
        reader: F,
        barrier: &'b Barrier,
    ) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        if let Some(root_ref) = self.root.load(Acquire, barrier).as_ref() {
            if let Some(value) = root_ref.search(key_ref, barrier) {
                return Some(reader(key_ref, value));
            }
        }
        None
    }

    /// Clears the [`TreeIndex`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// for key in 0..16_u64 {
    ///     assert!(treeindex.insert(key, 10).is_ok());
    /// }
    ///
    /// treeindex.clear();
    ///
    /// assert_eq!(treeindex.len(), 0);
    /// ```
    #[inline]
    pub fn clear(&self) {
        self.root.swap((None, Tag::None), Relaxed);
    }

    /// Returns the size of the [`TreeIndex`].
    ///
    /// It internally scans all the leaf nodes, and therefore the time complexity is O(N).
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// for key in 0..16_u64 {
    ///     assert!(treeindex.insert(key, 10).is_ok());
    /// }
    ///
    /// assert_eq!(treeindex.len(), 16);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        let barrier = Barrier::new();
        self.iter(&barrier).count()
    }

    /// Returns `true` if the [`TreeIndex`] is empty.
    ///
    /// It internally scans all the leaf nodes, and therefore the time complexity is O(N).
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// assert!(treeindex.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the depth of the [`TreeIndex`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// for key in 0..16_u64 {
    ///     let result = treeindex.insert(key, 10);
    ///     assert!(result.is_ok());
    /// }
    ///
    /// assert_eq!(treeindex.depth(), 1);
    /// ```
    #[inline]
    pub fn depth(&self) -> usize {
        let barrier = Barrier::new();
        self.root
            .load(Acquire, &barrier)
            .as_ref()
            .map_or(0, |root_ref| root_ref.depth(1, &barrier))
    }

    /// Returns a [`Visitor`].
    ///
    /// The returned [`Visitor`] starts scanning from the minimum key-value pair.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    /// use scc::ebr::Barrier;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// assert!(treeindex.insert(1, 10).is_ok());
    /// assert!(treeindex.insert(2, 11).is_ok());
    /// assert!(treeindex.insert(3, 13).is_ok());
    ///
    /// let barrier = Barrier::new();
    ///
    /// let mut visitor = treeindex.iter(&barrier);
    /// assert_eq!(visitor.next().unwrap(), (&1, &10));
    /// assert_eq!(visitor.next().unwrap(), (&2, &11));
    /// assert_eq!(visitor.next().unwrap(), (&3, &13));
    /// assert!(visitor.next().is_none());
    /// ```
    #[inline]
    pub fn iter<'t, 'b>(&'t self, barrier: &'b Barrier) -> Visitor<'t, 'b, K, V> {
        Visitor::new(&self.root, barrier)
    }

    /// Returns a [`Range`] that scans keys in the given range.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    /// use scc::ebr::Barrier;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// for i in 0..10 {
    ///     assert!(treeindex.insert(i, 10).is_ok());
    /// }
    ///
    /// let barrier = Barrier::new();
    ///
    /// assert_eq!(treeindex.range(1..1, &barrier).count(), 0);
    /// assert_eq!(treeindex.range(4..8, &barrier).count(), 4);
    /// assert_eq!(treeindex.range(4..=8, &barrier).count(), 5);
    /// ```
    #[inline]
    pub fn range<'t, 'b, R: RangeBounds<K>>(
        &'t self,
        range: R,
        barrier: &'b Barrier,
    ) -> Range<'t, 'b, K, V, R> {
        Range::new(&self.root, range, barrier)
    }
}

impl<K, V> Default for TreeIndex<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    /// Creates a [`TreeIndex`] with the default parameters.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::default();
    ///
    /// assert!(treeindex.read(&1, |_, v| *v).is_none());
    /// ```
    fn default() -> Self {
        TreeIndex::new()
    }
}
