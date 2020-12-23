extern crate crossbeam_epoch;

pub mod leaf;
pub mod node;

use crossbeam_epoch::{Atomic, Guard};
use leaf::Leaf;
use node::{Error, LeafNodeScanner, Node};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// A scalable concurrent tree map implementation.
///
/// scc::TreeIndex is a B+ tree variant that is optimized for read operations.
/// Read operations, such as scan, read, are neither blocked nor interrupted by all the other types of operations.
/// Write operations, such as insert, remove, do not block if they do not entail structural changes to the tree.
pub struct TreeIndex<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    root: Atomic<Node<K, V>>,
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
            match root_node_ref.insert(key, value, None, &guard) {
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
        let leaf_node_scanner = unsafe { root_node.deref().search(key, &guard) };
        leaf_node_scanner.map_or_else(
            || None,
            |scanner| {
                let entry = scanner.get();
                entry.map(|(key, value)| f(key, value))
            },
        )
    }

    /// (unimplemented!) Returns a Scanner.
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
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Drop for TreeIndex<K, V> {
    fn drop(&mut self) {}
}

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
        if let Some(scanner) = &mut self.leaf_node_scanner {
            return scanner.next();
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
