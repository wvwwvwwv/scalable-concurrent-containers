//! The module implements [`TreeIndex`].

mod error;
mod leaf;
mod leaf_node;
mod node;

use super::ebr::{Arc, AtomicArc, Barrier, Tag};

use error::{InsertError, RemoveError, SearchError};
use leaf::{Leaf, Scanner};
use node::Node;

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::iter::FusedIterator;
use std::ops::Bound::{Excluded, Included, Unbounded};
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
/// * The maximum number of key-value pairs that a leaf can store: 8.
/// * The maximum number of leaves or child nodes that a node can point to: 9.
/// * The size of metadata per key-value pair in a leaf: 3-byte.
/// * The size of metadata per leaf or node in a node: `size_of(K)` + 4.
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
        let mut root_ptr = self.root.load(Acquire, &barrier);
        loop {
            if let Some(root_ref) = root_ptr.as_ref() {
                match root_ref.insert(key, value, &barrier) {
                    Ok(_) => return Ok(()),
                    Err(error) => match error {
                        InsertError::Duplicated(entry) => return Err(entry),
                        InsertError::Full(entry) => {
                            root_ref.split_root(&self.root, &barrier);
                            key = entry.0;
                            value = entry.1;
                        }
                        InsertError::Retry(entry) => {
                            std::thread::yield_now();
                            key = entry.0;
                            value = entry.1;
                        }
                    },
                }
                root_ptr = self.root.load(Acquire, &barrier);
                continue;
            }
            let new_root = Arc::new(Node::new_leaf_node());
            match self
                .root
                .compare_exchange(root_ptr, (Some(new_root), Tag::None), AcqRel, Acquire)
            {
                Ok((_, ptr)) | Err((_, ptr)) => root_ptr = ptr,
            }
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
        let mut has_been_removed = false;
        let mut has_leaf_been_removed = false;
        let barrier = Barrier::new();
        let mut root_ptr = self.root.load(Acquire, &barrier);
        while let Some(root_ref) = root_ptr.as_ref() {
            match root_ref.remove_if(key_ref, &mut condition, &barrier) {
                Ok((removed, leaf_removed)) => {
                    if leaf_removed || has_leaf_been_removed {
                        if let Ok(leaf_scanner) = root_ref.max_less(key_ref, &barrier) {
                            let _result = leaf_scanner.jump(None, &barrier);
                        }
                    }
                    return removed || has_been_removed;
                }
                Err(remove_error) => match remove_error {
                    RemoveError::Empty((removed, leaf_removed)) => {
                        if removed {
                            has_been_removed = true;
                        }
                        if leaf_removed {
                            has_leaf_been_removed = true;
                        }
                        if Node::remove_root(&self.root, &barrier) {
                            return has_been_removed;
                        }
                    }
                    RemoveError::Retry((removed, leaf_removed)) => {
                        std::thread::yield_now();
                        if removed {
                            has_been_removed = true;
                        }
                        if leaf_removed {
                            has_leaf_been_removed = true;
                        }
                    }
                },
            };
            root_ptr = self.root.load(Acquire, &barrier);
        }
        has_been_removed
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
        let mut root_ptr = self.root.load(Acquire, barrier);
        while let Some(root_ref) = root_ptr.as_ref() {
            match root_ref.search(key_ref, barrier) {
                Ok(result) => {
                    if let Some(value) = result {
                        return Some(reader(key_ref, value));
                    }
                    return None;
                }
                Err(err) => match err {
                    SearchError::Empty => return None,
                    SearchError::Retry => {
                        std::thread::yield_now();
                        root_ptr = self.root.load(Acquire, barrier);
                        continue;
                    }
                },
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
        Visitor::new(self, barrier)
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
        Range::new(self, range, barrier)
    }
}

impl<K, V> TreeIndex<K, V>
where
    K: 'static + Clone + fmt::Display + Ord + Send + Sync,
    V: 'static + Clone + fmt::Display + Send + Sync,
{
    /// Prints the [`TreeIndex`] contents to the given output in the DOT language.
    ///
    /// # Errors
    ///
    /// An [`io::Error`](std::io::Error) can be returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// assert!(treeindex.insert(1, 10).is_ok());
    ///
    /// treeindex.print(&mut std::io::stdout());
    /// ```
    pub fn print<T: std::io::Write>(&self, output: &mut T) -> std::io::Result<()> {
        output.write_fmt(format_args!("digraph {{\n"))?;
        let barrier = Barrier::new();
        if let Some(root_ref) = self.root.load(Acquire, &barrier).as_ref() {
            root_ref.print(output, 1, &barrier)?;
        }
        output.write_fmt(format_args!("}}"))
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

/// [`Visitor`] scans all the key-value pairs in the [`TreeIndex`].
///
/// It is guaranteed to visit all the key-value pairs that outlive the [`Visitor`], and it
/// scans keys in monotonically increasing order.
pub struct Visitor<'t, 'b, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    tree: &'t TreeIndex<K, V>,
    leaf_scanner: Option<Scanner<'b, K, V>>,
    barrier: &'b Barrier,
}

impl<'t, 'b, K, V> Visitor<'t, 'b, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    fn new(tree: &'t TreeIndex<K, V>, barrier: &'b Barrier) -> Visitor<'t, 'b, K, V> {
        Visitor::<'t, 'b, K, V> {
            tree,
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
            loop {
                let root_ptr = self.tree.root.load(Acquire, self.barrier);
                if let Some(root_ref) = root_ptr.as_ref() {
                    if let Ok(scanner) = root_ref.min(self.barrier) {
                        self.leaf_scanner.replace(scanner);
                        break;
                    }
                } else {
                    return None;
                }
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
    tree: &'t TreeIndex<K, V>,
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
    fn new(tree: &'t TreeIndex<K, V>, range: R, barrier: &'b Barrier) -> Range<'t, 'b, K, V, R> {
        Range::<'t, 'b, K, V, R> {
            tree,
            leaf_scanner: None,
            range,
            check_lower_bound: true,
            check_upper_bound: false,
            barrier,
        }
    }

    fn next_unbounded(&mut self) -> Option<(&'b K, &'b V)> {
        // Starts scanning.
        if self.leaf_scanner.is_none() {
            loop {
                let root_ptr = self.tree.root.load(Acquire, self.barrier);
                if let Some(root_ref) = root_ptr.as_ref() {
                    let min_allowed_key = match self.range.start_bound() {
                        Excluded(key) | Included(key) => Some(key),
                        Unbounded => {
                            self.check_lower_bound = false;
                            None
                        }
                    };
                    if let Ok(leaf_scanner) = min_allowed_key.map_or_else(
                        || root_ref.min(self.barrier),
                        |min_allowed_key| root_ref.max_less(min_allowed_key, self.barrier),
                    ) {
                        self.check_upper_bound = match self.range.end_bound() {
                            Excluded(key) => leaf_scanner
                                .max_entry()
                                .map_or(false, |max_entry| max_entry.0.cmp(key) != Ordering::Less),
                            Included(key) => leaf_scanner.max_entry().map_or(false, |max_entry| {
                                max_entry.0.cmp(key) == Ordering::Greater
                            }),
                            Unbounded => false,
                        };
                        self.leaf_scanner.replace(leaf_scanner);
                        break;
                    }
                } else {
                    // Empty.
                    return None;
                }
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
