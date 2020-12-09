extern crate crossbeam_epoch;

pub mod leaf;
pub mod node;

use crossbeam_epoch::{Atomic, Guard};
use leaf::Leaf;
use node::{LeafNodeScanner, Node};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// A scalable concurrent tree map implementation.
///
/// It implements an in-memory B+-tree variant.
/// The customized structure of TreeIndex allows the scanning operation to perform very efficiently without being blocked.
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
    /// let guard = crossbeam_epoch::pin();
    /// let result = treeindex.search(&1, &guard);
    /// assert!(result.is_none());
    ///
    /// treeindex.insert(1, 0);
    /// let result = treeindex.search(&1, &guard);
    /// assert_eq!(result.map_or_else(|| (&1, &1), |scanner| scanner.get().unwrap()), (&1, &0));
    /// ```
    pub fn new() -> TreeIndex<K, V> {
        TreeIndex {
            root: Atomic::new(Node::new(0)),
        }
    }

    /// Returns a reference to the key-value pair.
    ///
    /// # Examples
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// let guard = crossbeam_epoch::pin();
    /// let result = treeindex.search(&1, &guard);
    /// assert!(result.is_none());
    ///
    /// treeindex.insert(1, 0);
    /// let result = treeindex.search(&1, &guard);
    /// assert_eq!(result.map_or_else(|| (&1, &1), |scanner| scanner.get().unwrap()), (&1, &0));
    /// ```
    pub fn search<'a>(&'a self, key: &K, guard: &'a Guard) -> Option<Scanner<'a, K, V>> {
        let mut scanner = Scanner::new(guard);
        let root_node = self.root.load(Acquire, scanner.guard);
        if root_node.is_null() {
            return None;
        }
        let leaf_node_scanner = unsafe { root_node.deref().search(key, scanner.guard) };
        if leaf_node_scanner.is_none() {
            return None;
        }
        scanner.leaf_node_scanner = leaf_node_scanner;
        Some(scanner)
    }

    /// Inserts a a key-value pair.
    ///
    /// # Examples
    /// ```
    /// use scc::TreeIndex;
    ///
    /// let treeindex: TreeIndex<u64, u32> = TreeIndex::new();
    ///
    /// let guard = crossbeam_epoch::pin();
    /// let result = treeindex.search(&1, &guard);
    /// assert!(result.is_none());
    ///
    /// treeindex.insert(1, 0);
    /// let result = treeindex.search(&1, &guard);
    /// assert_eq!(result.map_or_else(|| (&1, &1), |scanner| scanner.get().unwrap()), (&1, &0));
    /// ```
    pub fn insert(&self, key: K, value: V) {
        let guard = crossbeam_epoch::pin();
        let root_node = self.root.load(Acquire, &guard);
        if root_node.is_null() {
            return;
        }
        unsafe { root_node.deref().insert(key, value, &guard) };
    }
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Drop for TreeIndex<K, V> {
    fn drop(&mut self) {}
}

pub struct Scanner<'a, K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    leaf_node_scanner: Option<LeafNodeScanner<'a, K, V>>,
    guard: &'a Guard,
}

impl<'a, K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Scanner<'a, K, V> {
    fn new(guard: &'a Guard) -> Scanner<'a, K, V> {
        Scanner::<'a, K, V> {
            leaf_node_scanner: None,
            guard,
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
