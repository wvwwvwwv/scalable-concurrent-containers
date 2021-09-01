pub mod error;
pub mod leaf;
pub mod leaf_node;
pub mod node;

use crate::ebr::{Arc, AtomicArc, Barrier, Tag};

use error::{InsertError, RemoveError, SearchError};
use leaf::{Leaf, LeafScanner};
use node::Node;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::iter::FusedIterator;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::atomic::Ordering::{AcqRel, Acquire};

/// A scalable concurrent B+ tree.
///
/// [`TreeIndex`] is a B+ tree variant that is optimized for read operations. Read operations,
/// such as read, scan, are neither blocked nor interrupted by other threads. Write operations,
/// such as insert, remove, do not block if they do not entail structural changes to the tree.
///
/// ## The key features of [`TreeIndex`]
/// * Write-free read: read operations never modify the shared data.
/// * Near lock-free write: write operations do not block unless a structural change is needed.
///
/// ## The key statistics for [`TreeIndex`]
/// * The maximum number of key-value pairs that a leaf can store: 8.
/// * The maximum number of leaves or child nodes that a node can point to: 9.
/// * The size of metadata per key-value pair in a leaf: 3-byte.
/// * The size of metadata per leaf or node in a node: size_of(K) + 4.
pub struct TreeIndex<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    root: AtomicArc<Node<K, V>>,
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
    /// let treeindex: TreeIndex<u64, u32> = Default::default();
    ///
    /// let result = treeindex.read(&1, |key, value| *value);
    /// assert!(result.is_none());
    /// ```
    fn default() -> Self {
        TreeIndex::new()
    }
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
    /// let result = treeindex.read(&1, |key, value| *value);
    /// assert!(result.is_none());
    /// ```
    pub fn new() -> TreeIndex<K, V> {
        TreeIndex {
            root: AtomicArc::null(),
        }
    }

    /// Inserts a key-value pair.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// let result = treeindex.insert(1, 10);
    /// assert!(result.is_ok());
    ///
    /// let result = treeindex.insert(1, 11);
    /// assert_eq!(result.err().unwrap(), (1, 11));
    ///
    /// let result = treeindex.read(&1, |key, value| *value);
    /// assert_eq!(result.unwrap(), 10);
    /// ```
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
    /// let result = treeindex.remove(&1);
    /// assert!(!result);
    ///
    /// let result = treeindex.insert(1, 10);
    /// assert!(result.is_ok());
    ///
    /// let result = treeindex.remove(&1);
    /// assert!(result);
    /// ```
    pub fn remove<Q>(&self, key_ref: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut has_been_removed = false;
        let barrier = Barrier::new();
        let mut root_ptr = self.root.load(Acquire, &barrier);
        while let Some(root_ref) = root_ptr.as_ref() {
            match root_ref.remove(key_ref, &barrier) {
                Ok(removed) => return removed || has_been_removed,
                Err(remove_error) => match remove_error {
                    RemoveError::Empty(removed) => {
                        if removed && !has_been_removed {
                            has_been_removed = true;
                        }
                        if Node::remove_root(&self.root, true, &barrier) {
                            return has_been_removed;
                        }
                    }
                    RemoveError::Retry(removed) => {
                        std::thread::yield_now();
                        if removed && !has_been_removed {
                            has_been_removed = true;
                        }
                    }
                },
            };
            root_ptr = self.root.load(Acquire, &barrier);
        }
        false
    }

    /// Reads a key-value pair.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// let result = treeindex.read(&1, |key, value| *value);
    /// assert!(result.is_none());
    ///
    /// let result = treeindex.insert(1, 10);
    /// assert!(result.is_ok());
    ///
    /// let result = treeindex.read(&1, |key, value| *value);
    /// assert_eq!(result.unwrap(), 10);
    /// ```
    pub fn read<Q, R, F: FnOnce(&Q, &V) -> R>(&self, key_ref: &Q, f: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let barrier = Barrier::new();
        let mut root_ptr = self.root.load(Acquire, &barrier);
        while let Some(root_ref) = root_ptr.as_ref() {
            match root_ref.search(key_ref, &barrier) {
                Ok(result) => {
                    if let Some(value) = result {
                        return Some(f(key_ref, value));
                    } else {
                        return None;
                    }
                }
                Err(err) => match err {
                    SearchError::Empty => return None,
                    SearchError::Retry => {
                        std::thread::yield_now();
                        root_ptr = self.root.load(Acquire, &barrier);
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
    /// for key in 0..16u64 {
    ///     let result = treeindex.insert(key, 10);
    ///     assert!(result.is_ok());
    /// }
    ///
    /// treeindex.clear();
    ///
    /// let result = treeindex.len();
    /// assert_eq!(result, 0);
    /// ```
    pub fn clear(&self) {
        Node::remove_root(&self.root, false, &Barrier::new());
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
    /// for key in 0..16u64 {
    ///     let result = treeindex.insert(key, 10);
    ///     assert!(result.is_ok());
    /// }
    ///
    /// let result = treeindex.len();
    /// assert_eq!(result, 16);
    /// ```
    pub fn len(&self) -> usize {
        let barrier = Barrier::new();
        self.iter(&barrier).count()
    }

    /// Returns the depth of the [`TreeIndex`].
    ///
    /// # Examples
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// for key in 0..16u64 {
    ///     let result = treeindex.insert(key, 10);
    ///     assert!(result.is_ok());
    /// }
    ///
    /// let result = treeindex.depth();
    /// assert_eq!(result, 1);
    /// ```
    pub fn depth(&self) -> usize {
        let barrier = Barrier::new();
        if let Some(root_ref) = self.root.load(Acquire, &barrier).as_ref() {
            root_ref.depth(1, &barrier)
        } else {
            0
        }
    }

    /// Returns a [`Scanner`].
    ///
    /// The returned [`Scanner`] starts scanning from the minimum key-value pair.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// let result = treeindex.insert(1, 10);
    /// assert!(result.is_ok());
    ///
    /// let result = treeindex.insert(2, 11);
    /// assert!(result.is_ok());
    ///
    /// let result = treeindex.insert(3, 13);
    /// assert!(result.is_ok());
    ///
    /// let barrier = Barrier::new();
    ///
    /// let mut scanner = treeindex.iter(&barrier);
    /// assert_eq!(scanner.next().unwrap(), (&1, &10));
    /// assert_eq!(scanner.next().unwrap(), (&2, &11));
    /// assert_eq!(scanner.next().unwrap(), (&3, &13));
    /// assert!(scanner.next().is_none());
    /// ```
    pub fn iter<'t, 'b>(&'t self, barrier: &'b Barrier) -> Scanner<'t, 'b, K, V> {
        Scanner::new(self, barrier)
    }

    /// Returns a [`Range`] that scans keys in the given range.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// for i in 0..10 {
    ///     let result = treeindex.insert(i, 10);
    ///     assert!(result.is_ok());
    /// }
    ///
    /// let barrier = Barrier::new();
    ///
    /// for entry in treeindex.range(1..1, &barrier) {
    ///     assert!(false);
    /// }
    ///
    /// let mut scanned = 0;
    /// for entry in treeindex.range(4..8, &barrier) {
    ///     assert!(*entry.0 >= 4 && *entry.0 < 8);
    ///     scanned += 1;
    /// }
    /// assert_eq!(scanned, 4);
    ///
    /// scanned = 0;
    /// for entry in treeindex.range(4..=8, &barrier) {
    ///     assert!(*entry.0 >= 4 && *entry.0 <= 8);
    ///     scanned += 1;
    /// }
    /// assert_eq!(scanned, 5);

    /// ```
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
    /// Prints the TreeIndex contents to the given output in the DOT language.
    ///
    /// # Examples
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// let result = treeindex.insert(1, 10);
    /// assert!(result.is_ok());
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

impl<K, V> Drop for TreeIndex<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    fn drop(&mut self) {
        Node::remove_root(&self.root, false, &Barrier::new());
    }
}

/// Scanner scans all the key-value pairs in the TreeIndex.
///
/// It is guaranteed to visit all the key-value pairs that outlive the Scanner,
/// and it scans keys in monotonically increasing order.
pub struct Scanner<'t, 'b, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    tree: &'t TreeIndex<K, V>,
    leaf_scanner: Option<LeafScanner<'b, K, V>>,
    barrier: &'b Barrier,
}

impl<'t, 'b, K, V> Scanner<'t, 'b, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    fn new(tree: &'t TreeIndex<K, V>, barrier: &'b Barrier) -> Scanner<'t, 'b, K, V> {
        Scanner::<'t, 'b, K, V> {
            tree,
            leaf_scanner: None,
            barrier,
        }
    }
}

impl<'t, 'b, K, V> Iterator for Scanner<'t, 'b, K, V>
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
                    if let Ok(leaf_scanner) = root_ref.min(self.barrier) {
                        self.leaf_scanner.replace(leaf_scanner);
                        break;
                    }
                }
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

impl<'t, 'b, K, V> FusedIterator for Scanner<'t, 'b, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
}

/// Range represents a range of keys in the TreeIndex.
///
/// It is identical to Scanner except that it does not traverse keys outside of the given range.
pub struct Range<'t, 'b, K, V, R>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
    R: 'static + RangeBounds<K>,
{
    tree: &'t TreeIndex<K, V>,
    leaf_scanner: Option<LeafScanner<'b, K, V>>,
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
                        Excluded(key) => Some(key),
                        Included(key) => Some(key),
                        Unbounded => {
                            self.check_lower_bound = false;
                            None
                        }
                    };
                    if let Ok(leaf_scanner) = if let Some(min_allowed_key) = min_allowed_key {
                        root_ref.max_less(min_allowed_key, self.barrier)
                    } else {
                        root_ref.min(self.barrier)
                    } {
                        self.check_upper_bound = match self.range.end_bound() {
                            Excluded(key) => {
                                if let Some(max_entry) = leaf_scanner.max_entry() {
                                    max_entry.0.cmp(key) != Ordering::Less
                                } else {
                                    false
                                }
                            }
                            Included(key) => {
                                if let Some(max_entry) = leaf_scanner.max_entry() {
                                    max_entry.0.cmp(key) == Ordering::Greater
                                } else {
                                    false
                                }
                            }
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
                        Excluded(key) => {
                            if let Some(max_entry) = new_scanner.max_entry() {
                                max_entry.0.cmp(key) != Ordering::Less
                            } else {
                                false
                            }
                        }
                        Included(key) => {
                            if let Some(max_entry) = new_scanner.max_entry() {
                                max_entry.0.cmp(key) == Ordering::Greater
                            } else {
                                false
                            }
                        }
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
