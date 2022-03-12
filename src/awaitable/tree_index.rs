#![allow(dead_code)]
#![allow(clippy::unused_self)]

//! The module implements [`TreeIndex`].

mod internal_node;
mod leaf;
mod leaf_node;
mod node;

use leaf::Leaf;
use leaf_node::LeafNode;

use crate::ebr::AtomicArc;

use std::borrow::Borrow;
use std::ops::{RangeBounds, RangeFull};

/// Scalable concurrent B+ tree.
pub struct TreeIndex<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    root: AtomicArc<Leaf<K, V>>,
    non_root: AtomicArc<LeafNode<K, V>>,
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
    /// use scc::awaitable::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    /// ```
    #[must_use]
    pub fn new() -> TreeIndex<K, V> {
        TreeIndex {
            root: AtomicArc::null(),
            non_root: AtomicArc::null(),
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
    /// use scc::awaitable::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    /// ```
    #[inline]
    pub async fn insert(&self, key: K, value: V) -> Result<(), (K, V)> {
        Err((key, value))
    }

    /// Removes a key-value pair.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    /// ```
    #[inline]
    pub async fn remove<Q>(&self, key_ref: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.remove_if(key_ref, |_| true).await
    }

    /// Removes a key-value pair if the given condition is met.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    /// ```
    #[inline]
    pub async fn remove_if<Q, F: FnMut(&V) -> bool>(&self, _key_ref: &Q, _condition: F) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        false
    }

    /// Reads a key-value pair.
    ///
    /// It returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    /// ```
    #[inline]
    pub fn read<Q, R, F: FnOnce(&Q, &V) -> R>(&self, _key_ref: &Q, _reader: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        None
    }

    /// Clears the [`TreeIndex`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    /// ```
    #[inline]
    pub async fn clear(&self) {}

    /// Returns the size of the [`TreeIndex`].
    ///
    /// It internally scans all the leaf nodes, and therefore the time complexity is O(N).
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    /// assert_eq!(treeindex.len(), 0);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        0
    }

    /// Returns `true` if the [`TreeIndex`] is empty.
    ///
    /// It internally scans all the leaf nodes, and therefore the time complexity is O(N).
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::TreeIndex;
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
    /// use scc::awaitable::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    /// assert_eq!(treeindex.depth(), 0);
    /// ```
    #[inline]
    pub fn depth(&self) -> usize {
        0
    }

    /// Iterates over all the entries in the [`TreeIndex`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    /// ```
    #[inline]
    pub fn for_each<F: FnMut(&K, &V)>(&self, f: F) {
        self.for_each_range(f, RangeFull);
    }

    /// Iterates over entries in the specified key range.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    /// ```
    #[inline]
    pub fn for_each_range<F: FnMut(&K, &V), R: RangeBounds<K>>(&self, _f: F, _range: R) {}
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
    /// use scc::awaitable::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::default();
    /// ```
    fn default() -> Self {
        TreeIndex::new()
    }
}
