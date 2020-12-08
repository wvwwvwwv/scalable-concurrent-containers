extern crate crossbeam_epoch;

pub mod leaf;
pub mod node;

use crossbeam_epoch::{Atomic, Guard};
use leaf::Leaf;
use node::Node;

/// A scalable concurrent tree map implementation.
///
/// It implements an in-memory B+-tree variant.
/// The customized structure of TreeIndex allows the scanning operation to perform very efficiently without being blocked.
pub struct TreeIndex<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    root: Atomic<Node<K, V>>,
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> TreeIndex<K, V> {
    pub fn new() -> TreeIndex<K, V> {
        TreeIndex {
            root: Atomic::null(),
        }
    }

    pub fn search<'a>(&'a self, key: &K) -> Scanner<'a, K, V> {
        Scanner::new()
    }
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Drop for TreeIndex<K, V> {
    fn drop(&mut self) {}
}

pub struct Scanner<'a, K: Ord + Sync, V: Sync> {
    guard: Guard,
    key: Option<K>,
    value: Option<&'a V>,
}

impl<'a, K: Ord + Sync, V: Sync> Scanner<'a, K, V> {
    fn new() -> Scanner<'a, K, V> {
        Scanner::<'a, K, V> {
            guard: crossbeam_epoch::pin(),
            key: None,
            value: None,
        }
    }
}
