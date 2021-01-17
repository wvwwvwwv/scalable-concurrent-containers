extern crate crossbeam_epoch;

pub mod error;
pub mod leaf;
pub mod leafnode;
pub mod node;

use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use error::{InsertError, RemoveError};
use leaf::Leaf;
use leafnode::LeafNodeScanner;
use node::Node;
use std::fmt;
use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::Ordering::Relaxed;

/// A scalable concurrent tree map implementation.
///
/// scc::TreeIndex is a B+ tree variant that is optimized for read operations.
/// Read operations, such as scan, read, are neither blocked nor interrupted by all the other types of operations.
/// Write operations, such as insert, remove, do not block if they do not entail structural changes to the tree.
pub struct TreeIndex<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    root: Atomic<Node<K, V>>,
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Default for TreeIndex<K, V> {
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

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> TreeIndex<K, V> {
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

    /// Inserts a a key-value pair.
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
                let new_root = Owned::new(Node::new(0));
                match self
                    .root
                    .compare_and_set(root_node, new_root, Relaxed, &guard)
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
                        root_node_ref.split_root(entry, &self.root, &guard);
                        return Ok(());
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
                    RemoveError::Coalesce(removed) => {
                        if removed && !has_been_removed {
                            has_been_removed = true;
                        }
                        Node::update_root(root_node, &self.root, &guard);
                        return has_been_removed;
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
    pub fn read<U, F: FnOnce(&K, &V) -> U>(&self, key: &K, f: F) -> Option<U> {
        let guard = crossbeam_epoch::pin();
        let root_node = self.root.load(Acquire, &guard);
        if root_node.is_null() {
            return None;
        }
        if let Some(value) = unsafe { root_node.deref().search(key, &guard) } {
            Some(f(key, value))
        } else {
            None
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
        let old_root_node = self.root.swap(Shared::null(), Acquire, &guard);
        if !old_root_node.is_null() {
            unsafe { guard.defer_destroy(old_root_node) };
        }
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
        let mut num_entries = 0;
        for _ in self.iter() {
            num_entries += 1;
        }
        num_entries
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

    /// Returns a Scanner that starts from a key that is equal to the given key.
    ///
    /// In case the key does not exist, the adjacent key that is greater than the given key is returned.
    /// If the given key does not exists, and no keys are greater than the given key, None is returned.
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
    /// if let Some(mut scanner) = treeindex.from(&2) {
    ///     assert_eq!(scanner.get().unwrap(), (&2, &11));
    ///     assert_eq!(scanner.next().unwrap(), (&3, &13));
    ///     assert!(scanner.next().is_none());
    /// }
    /// ```
    pub fn from(&self, key: &K) -> Option<Scanner<K, V>> {
        Scanner::from(self, key)
    }

    /// (work-in-progress) Returns the statistics of the current state of the TreeIndex.
    ///
    /// It returns the size, depth, allocated nodes, and load factor.
    ///
    /// # Examples
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    /// ```
    pub fn statistics(&self) -> Statistics {
        let scanner = Scanner::new(self);
        let root_ptr = self.root.load(Relaxed, &scanner.guard);
        if root_ptr.is_null() {
            Statistics {
                capacity: 0,
                num_entries: 0,
                depth: 0,
            }
        } else {
            let root_ref = unsafe { root_ptr.deref() };
            let mut num_entries = 0;
            for _ in self.iter() {
                num_entries += 1;
            }
            Statistics {
                capacity: 0,
                num_entries,
                depth: root_ref.floor() + 1,
            }
        }
    }
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Drop for TreeIndex<K, V> {
    fn drop(&mut self) {}
}

/// Scanner implements Iterator for TreeIndex.
pub struct Scanner<'a, K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    tree_index: &'a TreeIndex<K, V>,
    leaf_node_scanner: Option<LeafNodeScanner<'a, K, V>>,
    guard: Guard,
}

impl<'a, K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Scanner<'a, K, V> {
    fn new(tree_index: &'a TreeIndex<K, V>) -> Scanner<'a, K, V> {
        Scanner::<'a, K, V> {
            tree_index,
            leaf_node_scanner: None,
            guard: crossbeam_epoch::pin(),
        }
    }

    fn from(tree_index: &'a TreeIndex<K, V>, key: &K) -> Option<Scanner<'a, K, V>> {
        let mut scanner = Scanner::<'a, K, V> {
            tree_index,
            leaf_node_scanner: None,
            guard: crossbeam_epoch::pin(),
        };
        let root_node = scanner.tree_index.root.load(Acquire, &scanner.guard);
        if root_node.is_null() {
            return None;
        }
        scanner.leaf_node_scanner = unsafe {
            // prolong the lifetime as the rust typesystem cannot infer the actual lifetime correctly
            std::mem::transmute::<_, Option<LeafNodeScanner<'a, K, V>>>(
                (*root_node.as_raw()).from(key, &scanner.guard),
            )
        };

        if scanner.leaf_node_scanner.is_none() {
            None
        } else {
            Some(scanner)
        }
    }

    /// Returns a reference to the entry that the scanner is currently pointing to
    pub fn get(&self) -> Option<(&'a K, &'a V)> {
        if let Some(leaf_node_scanner) = self.leaf_node_scanner.as_ref() {
            return leaf_node_scanner.get();
        }
        None
    }
}

impl<'a, K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Iterator for Scanner<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        if self.leaf_node_scanner.is_some() {
            while let Some(mut scanner) = self.leaf_node_scanner.take() {
                let min_allowed_key = scanner.get().map(|(key, _)| key);
                if let Some(result) = scanner.next() {
                    self.leaf_node_scanner.replace(scanner);
                    return Some(result);
                }
                // proceed to the next leaf node
                let next = unsafe {
                    // giving the min_allowed_key argument is necessary, because
                    //  - remove: merge two leaf nodes, therefore the unbounded leaf of the lower key node is relocated
                    //  - scanner: the unbounded leaf of the lower key node can be scanned twice after a jump
                    std::mem::transmute::<_, Option<LeafNodeScanner<'a, K, V>>>(
                        scanner.jump(min_allowed_key, &self.guard),
                    )
                };
                if let Some(next) = next {
                    self.leaf_node_scanner.replace(next);
                }
            }
            None
        } else {
            let root_node = self.tree_index.root.load(Acquire, &self.guard);
            if root_node.is_null() {
                return None;
            }
            self.leaf_node_scanner = unsafe {
                // prolong the lifetime as the rust typesystem cannot infer the actual lifetime correctly
                std::mem::transmute::<_, Option<LeafNodeScanner<'a, K, V>>>(
                    (*root_node.as_raw()).min(&self.guard),
                )
            };
            self.leaf_node_scanner
                .as_ref()
                .map_or_else(|| None, |scanner| scanner.get())
        }
    }
}

/// Statistics shows aggregated views of the TreeIndex.
///
/// # Examples
/// ```
/// use scc::TreeIndex;
///
/// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
/// ```
pub struct Statistics {
    capacity: usize,
    num_entries: usize,
    depth: usize,
}

impl Statistics {
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn num_entries(&self) -> usize {
        self.num_entries
    }
    pub fn depth(&self) -> usize {
        self.depth
    }
}

impl fmt::Display for Statistics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "capacity: {}, entries: {}, depth: {}",
            self.capacity, self.num_entries, self.depth,
        )
    }
}
