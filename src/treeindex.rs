extern crate crossbeam_epoch;

pub mod leaf;
pub mod node;

use crossbeam_epoch::{Atomic, Guard};
use leaf::Leaf;
use node::{Error, LeafNodeScanner, Node};
use std::fmt;
use std::sync::atomic::Ordering::Acquire;

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
            root: Atomic::new(Node::new(0)),
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
            let root_node = self.root.load(Acquire, &guard);
            if root_node.is_null() {
                return Err((key, value));
            }
            let root_node_ref = unsafe { root_node.deref() };
            match root_node_ref.insert(key, value, &guard) {
                Ok(_) => return Ok(()),
                Err(error) => match error {
                    Error::Duplicated(entry) => return Err(entry),
                    Error::Full(entry) => {
                        root_node_ref.split_root(entry, &self.root, &guard);
                        return Ok(());
                    }
                    Error::Retry(entry) => {
                        key = entry.0;
                        value = entry.1;
                    }
                },
            }
        }
    }

    /// (work-in-progress) Removes a key-value pair.
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
        let guard = crossbeam_epoch::pin();
        let mut root_node = self.root.load(Acquire, &guard);
        loop {
            if root_node.is_null() {
                return false;
            }
            let root_node_ref = unsafe { root_node.deref() };
            if root_node_ref.remove(key, &guard) {
                return true;
            }
            let root_node_new = self.root.load(Acquire, &guard);
            if root_node == root_node_new {
                return false;
            }
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

    /// (work-in-progress) Returns a Scanner that starts from the specified key.
    ///
    /// The returned Scanner starts scanning from the minimum key-value pair.
    ///
    /// # Examples
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    /// ```
    pub fn from(&self, _: &K) -> Scanner<K, V> {
        Scanner::new(self)
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
        Statistics {
            capacity: 0,
            num_entries: 0,
            depth: 0,
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
                if let Some(result) = scanner.next() {
                    self.leaf_node_scanner.replace(scanner);
                    return Some(result);
                }
                // proceed to the next leaf node
                let next = unsafe {
                    std::mem::transmute::<_, Option<LeafNodeScanner<'a, K, V>>>(
                        scanner.jump(&self.guard),
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
            return self
                .leaf_node_scanner
                .as_ref()
                .map_or_else(|| None, |scanner| scanner.get());
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
