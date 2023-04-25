//! [`TreeIndex`] is a read-optimized concurrent and asynchronous B+ tree.

mod internal_node;
mod leaf;
mod leaf_node;
mod node;

use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};
use crate::wait_queue::AsyncWait;
use leaf::{InsertResult, Leaf, RemoveResult, Scanner};
use node::Node;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::{self, Debug};
use std::iter::FusedIterator;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::panic::UnwindSafe;
use std::pin::Pin;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};

/// Scalable concurrent B+ tree.
///
/// [`TreeIndex`] is a concurrent and asynchronous B+ tree variant that is optimized for read
/// operations. Read operations, such as read, scan, are neither blocked nor interrupted by other
/// threads or tasks. Write operations, such as insert, remove, do not block if structural changes
/// are not required.
///
/// ## Notes
///
/// [`TreeIndex`] methods are linearizable, however its iterator methods are not; [`Visitor`] and
/// [`Range`] are only guaranteed to observe events happened before the first call to
/// [`Iterator::next`].
///
/// ## The key features of [`TreeIndex`]
///
/// * Lock-free-read: read and scan operations do not modify shared data and are never blocked.
/// * Near lock-free write: write operations do not block unless a structural change is needed.
/// * No busy waiting: each node has a wait queue to avoid spinning.
/// * Immutability: the data in the container is immutable until it becomes unreachable.
///
/// ## The key statistics for [`TreeIndex`]
///
/// * The maximum number of entries that a leaf can contain: 14.
/// * The maximum number of leaves or child nodes that a node can point to: 15.
///
/// ## Locking behavior
///
/// Read access is always lock-free and non-blocking. Write access to an entry is also lock-free
/// and non-blocking as long as no structural changes are required. However, when nodes are being
/// split or merged by a write operation, other write operations on keys in the affected range are
/// blocked.
///
/// ### Unwind safety
///
/// [`TreeIndex`] is impervious to out-of-memory errors and panics in user specified code on one
/// condition; `K::drop` and `V::drop` must not panic.
pub struct TreeIndex<K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
{
    root: AtomicArc<Node<K, V>>,
}

impl<K, V> TreeIndex<K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
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
    #[inline]
    #[must_use]
    pub const fn new() -> TreeIndex<K, V> {
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
    pub fn insert(&self, mut key: K, mut val: V) -> Result<(), (K, V)> {
        let mut new_root = None;
        loop {
            let barrier = Barrier::new();
            if let Some(root_ref) = self.root.load(Acquire, &barrier).as_ref() {
                match root_ref.insert(key, val, &mut (), &barrier) {
                    Ok(r) => match r {
                        InsertResult::Success => return Ok(()),
                        InsertResult::Frozen(k, v) | InsertResult::Retry(k, v) => {
                            key = k;
                            val = v;
                            root_ref.cleanup_link(key.borrow(), false, &barrier);
                        }
                        InsertResult::Duplicate(k, v) => return Err((k, v)),
                        InsertResult::Full(k, v) => {
                            let (k, v) = Node::split_root(k, v, &self.root, &barrier);
                            key = k;
                            val = v;
                            continue;
                        }
                        InsertResult::Retired(k, v) => {
                            key = k;
                            val = v;
                            let _result = Node::remove_root(&self.root, &mut (), &barrier);
                        }
                    },
                    Err((k, v)) => {
                        key = k;
                        val = v;
                    }
                }
            }

            let node = if let Some(new_root) = new_root.take() {
                new_root
            } else {
                Arc::new(Node::new_leaf_node())
            };
            if let Err((node, _)) = self.root.compare_exchange(
                Ptr::null(),
                (Some(node), Tag::None),
                AcqRel,
                Acquire,
                &barrier,
            ) {
                new_root = node;
            }
        }
    }

    /// Inserts a key-value pair.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
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
    pub async fn insert_async(&self, mut key: K, mut val: V) -> Result<(), (K, V)> {
        let mut new_root = None;
        loop {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);

            let need_await = {
                let barrier = Barrier::new();
                if let Some(root_ref) = self.root.load(Acquire, &barrier).as_ref() {
                    match root_ref.insert(key, val, &mut async_wait_pinned, &barrier) {
                        Ok(r) => match r {
                            InsertResult::Success => return Ok(()),
                            InsertResult::Frozen(k, v) | InsertResult::Retry(k, v) => {
                                key = k;
                                val = v;
                                root_ref.cleanup_link(key.borrow(), false, &barrier);
                                true
                            }
                            InsertResult::Duplicate(k, v) => return Err((k, v)),
                            InsertResult::Full(k, v) => {
                                let (k, v) = Node::split_root(k, v, &self.root, &barrier);
                                key = k;
                                val = v;
                                continue;
                            }
                            InsertResult::Retired(k, v) => {
                                key = k;
                                val = v;
                                !matches!(
                                    Node::remove_root(&self.root, &mut async_wait_pinned, &barrier),
                                    Ok(true)
                                )
                            }
                        },
                        Err((k, v)) => {
                            key = k;
                            val = v;
                            true
                        }
                    }
                } else {
                    false
                }
            };

            if need_await {
                async_wait_pinned.await;
            }

            let node = if let Some(new_root) = new_root.take() {
                new_root
            } else {
                Arc::new(Node::new_leaf_node())
            };
            if let Err((node, _)) = self.root.compare_exchange(
                Ptr::null(),
                (Some(node), Tag::None),
                AcqRel,
                Acquire,
                &Barrier::new(),
            ) {
                new_root = node;
            }
        }
    }

    /// Removes a key-value pair.
    ///
    /// It returns `false` if the key does not exist.
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
    pub fn remove<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.remove_if(key, |_| true)
    }

    /// Removes a key-value pair.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
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
    pub async fn remove_async<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.remove_if_async(key, |_| true).await
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
    pub fn remove_if<Q, F: FnMut(&V) -> bool>(&self, key: &Q, mut condition: F) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut has_been_removed = false;
        loop {
            let barrier = Barrier::new();
            if let Some(root_ref) = self.root.load(Acquire, &barrier).as_ref() {
                match root_ref.remove_if::<_, _, _>(key, &mut condition, &mut (), &barrier) {
                    Ok(r) => match r {
                        RemoveResult::Success => return true,
                        RemoveResult::Cleanup => {
                            root_ref.cleanup_link(key, false, &barrier);
                            return true;
                        }
                        RemoveResult::Retired => {
                            if matches!(Node::remove_root(&self.root, &mut (), &barrier), Ok(true))
                            {
                                return true;
                            }
                            has_been_removed = true;
                        }
                        RemoveResult::Fail => return has_been_removed,
                        RemoveResult::Frozen => (),
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
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
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
    pub async fn remove_if_async<Q, F: FnMut(&V) -> bool>(&self, key: &Q, mut condition: F) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut has_been_removed = false;
        loop {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);
            {
                let barrier = Barrier::new();
                if let Some(root_ref) = self.root.load(Acquire, &barrier).as_ref() {
                    match root_ref.remove_if::<_, _, _>(
                        key,
                        &mut condition,
                        &mut async_wait_pinned,
                        &barrier,
                    ) {
                        Ok(r) => match r {
                            RemoveResult::Success => return true,
                            RemoveResult::Cleanup => {
                                root_ref.cleanup_link(key, false, &barrier);
                                return true;
                            }
                            RemoveResult::Retired => {
                                if matches!(
                                    Node::remove_root(&self.root, &mut async_wait_pinned, &barrier),
                                    Ok(true)
                                ) {
                                    return true;
                                }
                                has_been_removed = true;
                            }
                            RemoveResult::Fail => return has_been_removed,
                            RemoveResult::Frozen => (),
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
            async_wait_pinned.await;
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
    pub fn read<Q, R, F: FnOnce(&Q, &V) -> R>(&self, key: &Q, reader: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let barrier = Barrier::new();
        self.read_with(key, reader, &barrier)
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
        key: &Q,
        reader: F,
        barrier: &'b Barrier,
    ) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        if let Some(root_ref) = self.root.load(Acquire, barrier).as_ref() {
            if let Some(val) = root_ref.search(key, barrier) {
                return Some(reader(key, val));
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
        let barrier = Barrier::new();
        !self.iter(&barrier).any(|_| true)
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

impl<K, V> Clone for TreeIndex<K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
{
    #[inline]
    fn clone(&self) -> Self {
        let self_clone = Self::default();
        for (k, v) in self.iter(&Barrier::new()) {
            let _reuslt = self_clone.insert(k.clone(), v.clone());
        }
        self_clone
    }
}

impl<K, V> Debug for TreeIndex<K, V>
where
    K: 'static + Clone + Debug + Ord,
    V: 'static + Clone + Debug,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let barrier = Barrier::new();
        f.debug_map().entries(self.iter(&barrier)).finish()
    }
}

impl<K, V> Default for TreeIndex<K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
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
    #[inline]
    fn default() -> Self {
        TreeIndex::new()
    }
}

impl<K, V> PartialEq for TreeIndex<K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone + PartialEq,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        // The key order is preserved, therefore comparing iterators suffices.
        let barrier = Barrier::new();
        Iterator::eq(self.iter(&barrier), other.iter(&barrier))
    }
}

/// [`Visitor`] scans all the key-value pairs in the [`TreeIndex`].
///
/// It is guaranteed to visit all the key-value pairs that outlive the [`Visitor`], and it
/// scans keys in monotonically increasing order.
pub struct Visitor<'t, 'b, K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
{
    root: &'t AtomicArc<Node<K, V>>,
    leaf_scanner: Option<Scanner<'b, K, V>>,
    barrier: &'b Barrier,
}

impl<'t, 'b, K, V> Visitor<'t, 'b, K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
{
    #[inline]
    fn new(root: &'t AtomicArc<Node<K, V>>, barrier: &'b Barrier) -> Visitor<'t, 'b, K, V> {
        Visitor::<'t, 'b, K, V> {
            root,
            leaf_scanner: None,
            barrier,
        }
    }
}

impl<'t, 'b, K, V> Iterator for Visitor<'t, 'b, K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
{
    type Item = (&'b K, &'b V);

    #[inline]
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
    K: 'static + Clone + Ord,
    V: 'static + Clone,
{
}

impl<'t, 'b, K, V> UnwindSafe for Visitor<'t, 'b, K, V>
where
    K: 'static + Clone + Ord + UnwindSafe,
    V: 'static + Clone + UnwindSafe,
{
}

/// [`Range`] represents a range of keys in the [`TreeIndex`].
///
/// It is identical to [`Visitor`] except that it does not traverse keys outside of the given
/// range.
pub struct Range<'t, 'b, K, V, R>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
    R: RangeBounds<K>,
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
    K: 'static + Clone + Ord,
    V: 'static + Clone,
    R: RangeBounds<K>,
{
    #[inline]
    fn new(
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

    #[inline]
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
                            .max_key()
                            .map_or(false, |max_key| max_key.cmp(key) != Ordering::Less),
                        Included(key) => leaf_scanner
                            .max_key()
                            .map_or(false, |max_key| max_key.cmp(key) == Ordering::Greater),
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
                            .max_key()
                            .map_or(false, |max_key| max_key.cmp(key) != Ordering::Less),
                        Included(key) => new_scanner
                            .max_key()
                            .map_or(false, |max_key| max_key.cmp(key) == Ordering::Greater),
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
    K: 'static + Clone + Ord,
    V: 'static + Clone,
    R: RangeBounds<K>,
{
    type Item = (&'b K, &'b V);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while let Some((k, v)) = self.next_unbounded() {
            if self.check_lower_bound {
                match self.range.start_bound() {
                    Excluded(key) => {
                        if k.cmp(key) != Ordering::Greater {
                            continue;
                        }
                    }
                    Included(key) => {
                        if k.cmp(key) == Ordering::Less {
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
                        if k.cmp(key) == Ordering::Less {
                            return Some((k, v));
                        }
                    }
                    Included(key) => {
                        if k.cmp(key) != Ordering::Greater {
                            return Some((k, v));
                        }
                    }
                    Unbounded => {
                        return Some((k, v));
                    }
                }
                break;
            }
            return Some((k, v));
        }
        None
    }
}

impl<'t, 'b, K, V, R> FusedIterator for Range<'t, 'b, K, V, R>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
    R: RangeBounds<K>,
{
}

impl<'t, 'b, K, V, R> UnwindSafe for Range<'t, 'b, K, V, R>
where
    K: 'static + Clone + Ord + UnwindSafe,
    V: 'static + Clone + UnwindSafe,
    R: RangeBounds<K> + UnwindSafe,
{
}
