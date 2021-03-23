pub mod error;
pub mod leaf;
pub mod leafnode;
pub mod node;

use crossbeam_epoch::{Atomic, Guard, Owned};
use error::{InsertError, RemoveError, SearchError};
use leaf::{Leaf, LeafScanner};
use node::Node;
use std::cmp::Ordering;
use std::fmt;
use std::iter::FusedIterator;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::atomic::Ordering::{AcqRel, Acquire};

/// A scalable concurrent tree map implementation.
///
/// scc::TreeIndex is a B+ tree variant that is optimized for read operations.
/// Read operations, such as read, scan, are neither blocked nor interrupted by all the other types of operations.
/// Write operations, such as insert, remove, do not block if they do not entail structural changes to the tree.
///
/// ## The key features of scc::TreeIndex
/// * Write-free read: read operations never modify the shared data.
/// * Near lock-free write: write operations do not block unless a structural change is required.
///
/// ## The key statistics for scc::TreeIndex
/// * The maximum number of entries that a leaf can store: 8.
/// * The maximum number of leaves or nodes that a node can store: 9.
/// * The size of metadata per entry in a leaf: 3-byte.
/// * The size of metadata per leaf or node in a node: size_of(K) + 4.
pub struct TreeIndex<K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    root: Atomic<Node<K, V>>,
}

impl<K, V> Default for TreeIndex<K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    /// Creates a TreeIndex instance with the default parameters.
    ///
    /// # Examples
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
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    /// Creates an empty TreeIndex instance.
    ///
    /// # Examples
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
            root: Atomic::null(),
        }
    }

    /// Inserts a key-value pair.
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
    /// let result = treeindex.insert(1, 11);
    /// assert_eq!(result.err().unwrap(), (1, 11));
    ///
    /// let result = treeindex.read(&1, |key, value| *value);
    /// assert_eq!(result.unwrap(), 10);
    /// ```
    pub fn insert(&self, mut key: K, mut value: V) -> Result<(), (K, V)> {
        loop {
            let guard = crossbeam_epoch::pin();
            let mut root_node = self.root.load(Acquire, &guard);
            if root_node.is_null() {
                let new_root = Owned::new(Node::new_leaf_node());
                match self
                    .root
                    .compare_exchange(root_node, new_root, AcqRel, Acquire, &guard)
                {
                    Ok(new_root) => root_node = new_root,
                    Err(_) => continue,
                }
            }
            let root_node_ref = unsafe { root_node.deref() };
            match root_node_ref.insert(key, value, &guard) {
                Ok(_) => return Ok(()),
                Err(error) => match error {
                    InsertError::Duplicated(entry) => return Err(entry),
                    InsertError::Full(entry) => {
                        root_node_ref.split_root(&self.root, &guard);
                        key = entry.0;
                        value = entry.1;
                    }
                    InsertError::Retry(entry) => {
                        key = entry.0;
                        value = entry.1;
                    }
                },
            }
        }
    }

    /// Removes a key-value pair.
    ///
    /// # Examples
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
    pub fn remove(&self, key: &K) -> bool {
        let mut has_been_removed = false;
        let guard = crossbeam_epoch::pin();
        let mut root_node = self.root.load(Acquire, &guard);
        loop {
            if root_node.is_null() {
                return has_been_removed;
            }
            let root_node_ref = unsafe { root_node.deref() };
            match root_node_ref.remove(key, &guard) {
                Ok(removed) => return removed || has_been_removed,
                Err(remove_error) => match remove_error {
                    RemoveError::Empty(removed) => {
                        if removed && !has_been_removed {
                            has_been_removed = true;
                        }
                        if Node::remove_root(&self.root, true, &guard) {
                            return has_been_removed;
                        }
                    }
                    RemoveError::Retry(removed) => {
                        if removed && !has_been_removed {
                            has_been_removed = true;
                        }
                    }
                },
            };
            let root_node_new = self.root.load(Acquire, &guard);
            root_node = root_node_new;
        }
    }

    /// Reads a key-value pair.
    ///
    /// # Examples
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
    pub fn read<R, F: FnOnce(&K, &V) -> R>(&self, key: &K, f: F) -> Option<R> {
        let guard = crossbeam_epoch::pin();
        loop {
            let root_node = self.root.load(Acquire, &guard);
            if root_node.is_null() {
                return None;
            }
            match unsafe { root_node.deref().search(key, &guard) } {
                Ok(result) => {
                    if let Some(value) = result {
                        return Some(f(key, value));
                    } else {
                        return None;
                    }
                }
                Err(err) => match err {
                    SearchError::Empty => return None,
                    SearchError::Retry => continue,
                },
            }
        }
    }

    /// Clears the TreeIndex.
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
    /// treeindex.clear();
    ///
    /// let result = treeindex.len();
    /// assert_eq!(result, 0);
    /// ```
    pub fn clear(&self) {
        let guard = crossbeam_epoch::pin();
        Node::remove_root(&self.root, false, &guard);
    }

    /// Returns the size of the TreeIndex.
    ///
    /// It internally scans all the leaf nodes, and therefore the time complexity is O(N).
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
    /// let result = treeindex.len();
    /// assert_eq!(result, 16);
    /// ```
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Returns the depth of the TreeIndex.
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
        let guard = crossbeam_epoch::pin();
        let root_node = self.root.load(Acquire, &guard);
        if !root_node.is_null() {
            unsafe { root_node.deref().depth(1, &guard) }
        } else {
            0
        }
    }

    /// Returns a Scanner.
    ///
    /// The returned Scanner starts scanning from the minimum key-value pair.
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
    /// let result = treeindex.insert(2, 11);
    /// assert!(result.is_ok());
    ///
    /// let result = treeindex.insert(3, 13);
    /// assert!(result.is_ok());
    ///
    /// let mut scanner = treeindex.iter();
    /// assert_eq!(scanner.next().unwrap(), (&1, &10));
    /// assert_eq!(scanner.next().unwrap(), (&2, &11));
    /// assert_eq!(scanner.next().unwrap(), (&3, &13));
    /// assert!(scanner.next().is_none());
    /// ```
    pub fn iter(&self) -> Scanner<K, V> {
        Scanner::new(self)
    }

    /// Returns a Range that scans keys in the given range.
    ///
    /// # Examples
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// for i in 0..10 {
    ///     let result = treeindex.insert(i, 10);
    ///     assert!(result.is_ok());
    /// }
    ///
    /// for entry in treeindex.range(1..1) {
    ///     assert!(false);
    /// }
    ///
    /// let mut scanned = 0;
    /// for entry in treeindex.range(4..8) {
    ///     assert!(*entry.0 >= 4 && *entry.0 < 8);
    ///     scanned += 1;
    /// }
    /// assert_eq!(scanned, 4);
    ///
    /// scanned = 0;
    /// for entry in treeindex.range(4..=8) {
    ///     assert!(*entry.0 >= 4 && *entry.0 <= 8);
    ///     scanned += 1;
    /// }
    /// assert_eq!(scanned, 5);

    /// ```
    pub fn range<R: RangeBounds<K>>(&self, range: R) -> Range<K, V, R> {
        Range::new(self, range)
    }
}

impl<K, V> TreeIndex<K, V>
where
    K: Clone + fmt::Display + Ord + Send + Sync,
    V: Clone + fmt::Display + Send + Sync,
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
        let guard = crossbeam_epoch::pin();
        let root_node = self.root.load(Acquire, &guard);
        if !root_node.is_null() {
            unsafe { root_node.deref().print(output, 1, &guard) }?
        }
        output.write_fmt(format_args!("}}"))
    }
}

impl<K, V> Drop for TreeIndex<K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    fn drop(&mut self) {
        // The TreeIndex has become unreachable, therefore pinning is unnecessary.
        let guard = unsafe { crossbeam_epoch::unprotected() };
        Node::remove_root(&self.root, false, guard);
    }
}

/// Scanner implements Iterator for TreeIndex.
pub struct Scanner<'t, K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    tree: &'t TreeIndex<K, V>,
    leaf_scanner: Option<LeafScanner<'t, K, V>>,
    guard: Guard,
}

impl<'t, K, V> Scanner<'t, K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    fn new(tree: &'t TreeIndex<K, V>) -> Scanner<'t, K, V> {
        Scanner::<'t, K, V> {
            tree,
            leaf_scanner: None,
            guard: crossbeam_epoch::pin(),
        }
    }
}

impl<'t, K, V> Iterator for Scanner<'t, K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    type Item = (&'t K, &'t V);
    fn next(&mut self) -> Option<Self::Item> {
        // Starts scanning.
        if self.leaf_scanner.is_none() {
            loop {
                let root_node = self.tree.root.load(Acquire, &self.guard);
                if root_node.is_null() {
                    return None;
                }
                if let Ok(leaf_scanner) = unsafe { &*root_node.as_raw() }.min(&self.guard) {
                    self.leaf_scanner.replace(unsafe {
                        // Prolongs the lifetime as the Rust type system cannot infer the actual lifetime correctly.
                        std::mem::transmute::<_, LeafScanner<'t, K, V>>(leaf_scanner)
                    });
                    break;
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
            while let Some(mut new_scanner) = unsafe {
                // Checking min_allowed_key is necessary, because,
                //  - Remove: merges two leaf nodes, therefore the unbounded leaf of the lower key node is relocated.
                //  - Scanner: the unbounded leaf of the lower key node can be scanned twice after a jump.
                std::mem::transmute::<_, Option<LeafScanner<'t, K, V>>>(scanner.jump(&self.guard))
                    .take()
            } {
                while let Some(entry) = new_scanner.next() {
                    if min_allowed_key
                        .as_ref()
                        .map_or_else(|| true, |&key| key.cmp(entry.0) == Ordering::Less)
                    {
                        self.leaf_scanner.replace(new_scanner);
                        return Some(entry);
                    }
                }
                scanner = new_scanner;
            }
        }
        None
    }
}

impl<'t, K, V> FusedIterator for Scanner<'t, K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
}

/// Range represents a range of keys in the TreeIndex.
pub struct Range<'t, K, V, R>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
    R: RangeBounds<K>,
{
    tree: &'t TreeIndex<K, V>,
    leaf_scanner: Option<LeafScanner<'t, K, V>>,
    range: R,
    check_lower_bound: bool,
    check_upper_bound: bool,
    guard: Guard,
}

impl<'t, K, V, R> Range<'t, K, V, R>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
    R: RangeBounds<K>,
{
    fn new(tree: &'t TreeIndex<K, V>, range: R) -> Range<'t, K, V, R> {
        Range::<'t, K, V, R> {
            tree,
            leaf_scanner: None,
            range,
            check_lower_bound: true,
            check_upper_bound: false,
            guard: crossbeam_epoch::pin(),
        }
    }

    fn next_unbounded(&mut self) -> Option<(&'t K, &'t V)> {
        // Starts scanning.
        if self.leaf_scanner.is_none() {
            loop {
                let root_node = self.tree.root.load(Acquire, &self.guard);
                if root_node.is_null() {
                    // Empty.
                    return None;
                }
                let min_allowed_key = match self.range.start_bound() {
                    Excluded(key) => Some(key),
                    Included(key) => Some(key),
                    Unbounded => {
                        self.check_lower_bound = false;
                        None
                    }
                };
                if let Ok(leaf_scanner) = if let Some(min_allowed_key) = min_allowed_key {
                    unsafe { &*root_node.as_raw() }.max_less(min_allowed_key, &self.guard)
                } else {
                    unsafe { &*root_node.as_raw() }.min(&self.guard)
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
                    self.leaf_scanner.replace(unsafe {
                        // Prolongs the lifetime as the Rust type system cannot infer the actual lifetime correctly.
                        std::mem::transmute::<_, LeafScanner<'t, K, V>>(leaf_scanner)
                    });
                    break;
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
            while let Some(mut new_scanner) = unsafe {
                // Checking min_allowed_key is necessary, because,
                //  - Remove: merges two leaf nodes, therefore the unbounded leaf of the lower key node is relocated.
                //  - Scanner: the unbounded leaf of the lower key node can be scanned twice after a jump.
                std::mem::transmute::<_, Option<LeafScanner<'t, K, V>>>(scanner.jump(&self.guard))
                    .take()
            } {
                while let Some(entry) = new_scanner.next() {
                    if min_allowed_key
                        .as_ref()
                        .map_or_else(|| true, |&key| key.cmp(entry.0) == Ordering::Less)
                    {
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
                scanner = new_scanner;
            }
        }
        None
    }
}

impl<'t, K, V, R> Iterator for Range<'t, K, V, R>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
    R: RangeBounds<K>,
{
    type Item = (&'t K, &'t V);
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

impl<'t, K, V, R> FusedIterator for Range<'t, K, V, R>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
    R: RangeBounds<K>,
{
}
