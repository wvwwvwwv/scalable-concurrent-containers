extern crate crossbeam_epoch;

pub mod leaf;
pub mod node;

use crossbeam_epoch::Atomic;
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
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Drop for TreeIndex<K, V> {
    fn drop(&mut self) {}
}
