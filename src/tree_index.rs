//! The module implements [`TreeIndex`].

mod internal_node;
mod leaf;
mod leaf_node;
mod node;

use crate::async_yield;
use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};

use leaf::{InsertResult, Leaf, RemoveResult, Scanner};
use node::Node;

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::iter::FusedIterator;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};

/// Scalable concurrent B+ tree.
///
/// [`TreeIndex`] is a B+ tree variant that is optimized for read operations. Read operations, such
/// as read, scan, are neither blocked nor interrupted by other threads. Write operations, such as
/// insert, remove, do not block if they do not entail structural changes to the tree.
///
/// ## The key features of [`TreeIndex`]
///
/// * Lock-free-read: read and scan operations do not modify shared data and are never blocked.
/// * Near lock-free write: write operations do not block unless a structural change is needed.
/// * No busy waiting: each node has a wait queue to avoid spinning.
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
        loop {
            let barrier = Barrier::new();
            if let Some(root_ref) = self.root.load(Acquire, &barrier).as_ref() {
                match root_ref.insert::<false>(key, value, &barrier) {
                    Ok(r) => match r {
                        InsertResult::Success => return Ok(()),
                        InsertResult::Frozen(k, v) => {
                            key = k;
                            value = v;
                        }
                        InsertResult::Duplicate(k, v) => return Err((k, v)),
                        InsertResult::Full(k, v) => {
                            let (k, v) = Node::split_root::<false>(k, v, &self.root, &barrier);
                            key = k;
                            value = v;
                            continue;
                        }
                        InsertResult::Retired(k, v) => {
                            key = k;
                            value = v;
                            let _result = Node::remove_root::<false>(&self.root, &barrier);
                        }
                    },
                    Err((k, v)) => {
                        key = k;
                        value = v;
                    }
                }
            }

            let new_root = Arc::new(Node::new_leaf_node());
            let _result = self.root.compare_exchange(
                Ptr::null(),
                (Some(new_root), Tag::None),
                AcqRel,
                Acquire,
                &barrier,
            );
        }
    }

    /// Inserts a key-value pair.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await or poll.
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
    /// let future_insert = treeindex.insert_async(1, 10);
    /// ```
    #[inline]
    pub async fn insert_async(&self, mut key: K, mut value: V) -> Result<(), (K, V)> {
        loop {
            let need_await = {
                let barrier = Barrier::new();
                if let Some(root_ref) = self.root.load(Acquire, &barrier).as_ref() {
                    match root_ref.insert::<true>(key, value, &barrier) {
                        Ok(r) => match r {
                            InsertResult::Success => return Ok(()),
                            InsertResult::Frozen(k, v) => {
                                key = k;
                                value = v;
                                true
                            }
                            InsertResult::Duplicate(k, v) => return Err((k, v)),
                            InsertResult::Full(k, v) => {
                                let (k, v) = Node::split_root::<true>(k, v, &self.root, &barrier);
                                key = k;
                                value = v;
                                continue;
                            }
                            InsertResult::Retired(k, v) => {
                                key = k;
                                value = v;
                                !matches!(Node::remove_root::<true>(&self.root, &barrier), Ok(true))
                            }
                        },
                        Err((k, v)) => {
                            key = k;
                            value = v;
                            true
                        }
                    }
                } else {
                    false
                }
            };

            if need_await {
                async_yield::async_yield().await;
            }

            let new_root = Arc::new(Node::new_leaf_node());
            let _result = self.root.compare_exchange(
                Ptr::null(),
                (Some(new_root), Tag::None),
                AcqRel,
                Acquire,
                &Barrier::new(),
            );
        }
    }

    /// Removes a key-value pair.
    ///
    /// The removed key-value pair may be reachable via [`Range`] or [`Visitor`] momentarily if the
    /// node that contained the key-value pair is being split.
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

    /// Removes a key-value pair.
    ///
    /// The removed key-value pair may be reachable via [`Range`] or [`Visitor`] momentarily if the
    /// node that contained the key-value pair is being split.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await or poll.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    /// let future_remove = treeindex.remove_async(&1);
    /// ```
    #[inline]
    pub async fn remove_async<Q>(&self, key_ref: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.remove_if_async(key_ref, |_| true).await
    }

    /// Removes a key-value pair if the given condition is met.
    ///
    /// The removed key-value pair may be reachable via [`Range`] or [`Visitor`] momentarily if the
    /// node that contained the key-value pair is being split.
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
        let mut has_been_removed = false;
        loop {
            let barrier = Barrier::new();
            if let Some(root_ref) = self.root.load(Acquire, &barrier).as_ref() {
                match root_ref.remove_if::<_, _, false>(key_ref, &mut condition, &barrier) {
                    Ok(r) => match r {
                        RemoveResult::Success => return true,
                        RemoveResult::Fail => return has_been_removed,
                        RemoveResult::Retired => {
                            if matches!(Node::remove_root::<false>(&self.root, &barrier), Ok(true))
                            {
                                return true;
                            }
                            has_been_removed = true;
                        }
                    },
                    Err(removed) => {
                        if removed {
                            has_been_removed = true;
                        }
                    }
                }
            } else {
                return has_been_removed;
            }
        }
    }

    /// Removes a key-value pair if the given condition is met.
    ///
    /// The removed key-value pair may be reachable via [`Range`] or [`Visitor`] momentarily if the
    /// node that contained the key-value pair is being split.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await or poll.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    /// let future_remove = treeindex.remove_if_async(&1, |v| *v == 0);
    /// ```
    #[inline]
    pub async fn remove_if_async<Q, F: FnMut(&V) -> bool>(
        &self,
        key_ref: &Q,
        mut condition: F,
    ) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut has_been_removed = false;
        loop {
            let need_await = {
                let barrier = Barrier::new();
                if let Some(root_ref) = self.root.load(Acquire, &barrier).as_ref() {
                    match root_ref.remove_if::<_, _, true>(key_ref, &mut condition, &barrier) {
                        Ok(r) => match r {
                            RemoveResult::Success => return true,
                            RemoveResult::Fail => return has_been_removed,
                            RemoveResult::Retired => {
                                if matches!(
                                    Node::remove_root::<true>(&self.root, &barrier),
                                    Ok(true)
                                ) {
                                    return true;
                                }
                                has_been_removed = true;
                                true
                            }
                        },
                        Err(removed) => {
                            if removed {
                                has_been_removed = true;
                            }
                            true
                        }
                    }
                } else {
                    return has_been_removed;
                }
            };

            if need_await {
                async_yield::async_yield().await;
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
    /// assert!(treeindex.read(&1, |k, v| *v).is_none());
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
    /// let barrier = Barrier::new();
    /// assert!(treeindex.read_with(&1, |k, v| v, &barrier).is_none());
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
    /// treeindex.clear();
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
    /// assert_eq!(treeindex.len(), 0);
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
    /// assert_eq!(treeindex.depth(), 0);
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
    /// The returned [`Visitor`] starts scanning from the minimum key-value pair. Key-value pairs
    /// are scanned in ascending order, and key-value pairs that have existed since the invocation
    /// of the method are guaranteed to be visited if they are not removed. However, it is possible
    /// to visit removed key-value pairs momentarily.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    /// use scc::ebr::Barrier;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// let barrier = Barrier::new();
    /// let mut visitor = treeindex.iter(&barrier);
    /// assert!(visitor.next().is_none());
    /// ```
    #[inline]
    pub fn iter<'t, 'b>(&'t self, barrier: &'b Barrier) -> Visitor<'t, 'b, K, V> {
        Visitor::new(&self.root, barrier)
    }

    /// Returns a [`Range`] that scans keys in the given range.
    ///
    /// Key-value pairs in the range are scanned in ascending order, and key-value pairs that have
    /// existed since the invocation of the method are guaranteed to be visited if they are not
    /// removed. However, it is possible to visit removed key-value pairs momentarily.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    /// use scc::ebr::Barrier;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// let barrier = Barrier::new();
    /// assert_eq!(treeindex.range(4..=8, &barrier).count(), 0);
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
    /// ```
    fn default() -> Self {
        TreeIndex::new()
    }
}

/// [`Visitor`] scans all the key-value pairs in the [`TreeIndex`].
///
/// It is guaranteed to visit all the key-value pairs that outlive the [`Visitor`], and it
/// scans keys in monotonically increasing order.
pub struct Visitor<'t, 'b, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    root: &'t AtomicArc<Node<K, V>>,
    leaf_scanner: Option<Scanner<'b, K, V>>,
    barrier: &'b Barrier,
}

impl<'t, 'b, K, V> Visitor<'t, 'b, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    pub(crate) fn new(
        root: &'t AtomicArc<Node<K, V>>,
        barrier: &'b Barrier,
    ) -> Visitor<'t, 'b, K, V> {
        Visitor::<'t, 'b, K, V> {
            root,
            leaf_scanner: None,
            barrier,
        }
    }
}

impl<'t, 'b, K, V> Iterator for Visitor<'t, 'b, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    type Item = (&'b K, &'b V);
    fn next(&mut self) -> Option<Self::Item> {
        // Starts scanning.
        if self.leaf_scanner.is_none() {
            let root_ptr = self.root.load(Acquire, self.barrier);
            if let Some(root_ref) = root_ptr.as_ref() {
                if let Some(scanner) = root_ref.min(self.barrier) {
                    self.leaf_scanner.replace(scanner);
                }
            } else {
                return None;
            }
        }

        // Proceeds to the next entry.
        if let Some(mut scanner) = self.leaf_scanner.take() {
            let min_allowed_key = scanner.get().map(|(key, _)| key);
            if let Some(result) = scanner.next() {
                self.leaf_scanner.replace(scanner);
                return Some(result);
            }
            // Proceeds to the next leaf node.
            if let Some(new_scanner) = scanner.jump(min_allowed_key, self.barrier) {
                if let Some(entry) = new_scanner.get() {
                    self.leaf_scanner.replace(new_scanner);
                    return Some(entry);
                }
            }
        }
        None
    }
}

impl<'t, 'b, K, V> FusedIterator for Visitor<'t, 'b, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
}

/// [`Range`] represents a range of keys in the [`TreeIndex`].
///
/// It is identical to [`Visitor`] except that it does not traverse keys outside of the given
/// range.
pub struct Range<'t, 'b, K, V, R>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
    R: 'static + RangeBounds<K>,
{
    root: &'t AtomicArc<Node<K, V>>,
    leaf_scanner: Option<Scanner<'b, K, V>>,
    range: R,
    check_lower_bound: bool,
    check_upper_bound: bool,
    barrier: &'b Barrier,
}

impl<'t, 'b, K, V, R> Range<'t, 'b, K, V, R>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
    R: RangeBounds<K>,
{
    pub(crate) fn new(
        root: &'t AtomicArc<Node<K, V>>,
        range: R,
        barrier: &'b Barrier,
    ) -> Range<'t, 'b, K, V, R> {
        Range::<'t, 'b, K, V, R> {
            root,
            leaf_scanner: None,
            range,
            check_lower_bound: true,
            check_upper_bound: false,
            barrier,
        }
    }

    fn next_unbounded(&mut self) -> Option<(&'b K, &'b V)> {
        // Start scanning.
        if self.leaf_scanner.is_none() {
            let root_ptr = self.root.load(Acquire, self.barrier);
            if let Some(root_ref) = root_ptr.as_ref() {
                let min_allowed_key = match self.range.start_bound() {
                    Excluded(key) | Included(key) => Some(key),
                    Unbounded => {
                        self.check_lower_bound = false;
                        None
                    }
                };
                if let Some(leaf_scanner) = min_allowed_key.map_or_else(
                    || {
                        // Take the min entry.
                        if let Some(mut min_scanner) = root_ref.min(self.barrier) {
                            min_scanner.next();
                            Some(min_scanner)
                        } else {
                            None
                        }
                    },
                    |min_allowed_key| {
                        // Take an entry that is close enough to the lower bound.
                        root_ref.max_le_appr(min_allowed_key, self.barrier)
                    },
                ) {
                    // Need to check the upper bound.
                    self.check_upper_bound = match self.range.end_bound() {
                        Excluded(key) => leaf_scanner
                            .max_entry()
                            .map_or(false, |max_entry| max_entry.0.cmp(key) != Ordering::Less),
                        Included(key) => leaf_scanner
                            .max_entry()
                            .map_or(false, |max_entry| max_entry.0.cmp(key) == Ordering::Greater),
                        Unbounded => false,
                    };
                    if let Some(result) = leaf_scanner.get() {
                        self.leaf_scanner.replace(leaf_scanner);
                        return Some(result);
                    }
                }
            } else {
                // Empty.
                return None;
            }
        }

        // Go to the next entry.
        if let Some(mut scanner) = self.leaf_scanner.take() {
            let min_allowed_key = scanner.get().map(|(key, _)| key);
            if let Some(result) = scanner.next() {
                self.leaf_scanner.replace(scanner);
                return Some(result);
            }
            // Go to the next leaf node.
            if let Some(new_scanner) = scanner.jump(min_allowed_key, self.barrier).take() {
                if let Some(entry) = new_scanner.get() {
                    self.check_upper_bound = match self.range.end_bound() {
                        Excluded(key) => new_scanner
                            .max_entry()
                            .map_or(false, |max_entry| max_entry.0.cmp(key) != Ordering::Less),
                        Included(key) => new_scanner
                            .max_entry()
                            .map_or(false, |max_entry| max_entry.0.cmp(key) == Ordering::Greater),
                        Unbounded => false,
                    };
                    self.leaf_scanner.replace(new_scanner);
                    return Some(entry);
                }
            }
        }
        None
    }
}

impl<'t, 'b, K, V, R> Iterator for Range<'t, 'b, K, V, R>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
    R: RangeBounds<K>,
{
    type Item = (&'b K, &'b V);
    fn next(&mut self) -> Option<Self::Item> {
        while let Some((key_ref, value_ref)) = self.next_unbounded() {
            if self.check_lower_bound {
                match self.range.start_bound() {
                    Excluded(key) => {
                        if key_ref.cmp(key) != Ordering::Greater {
                            continue;
                        }
                    }
                    Included(key) => {
                        if key_ref.cmp(key) == Ordering::Less {
                            continue;
                        }
                    }
                    Unbounded => (),
                }
            }
            self.check_lower_bound = false;
            if self.check_upper_bound {
                match self.range.end_bound() {
                    Excluded(key) => {
                        if key_ref.cmp(key) == Ordering::Less {
                            return Some((key_ref, value_ref));
                        }
                    }
                    Included(key) => {
                        if key_ref.cmp(key) != Ordering::Greater {
                            return Some((key_ref, value_ref));
                        }
                    }
                    Unbounded => {
                        return Some((key_ref, value_ref));
                    }
                }
                break;
            }
            return Some((key_ref, value_ref));
        }
        None
    }
}

impl<'t, 'b, K, V, R> FusedIterator for Range<'t, 'b, K, V, R>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
    R: RangeBounds<K>,
{
}
