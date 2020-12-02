extern crate crossbeam_epoch;

pub mod leaf;
pub mod node;

use crossbeam_epoch::Atomic;
use leaf::Leaf;
use node::Node;

/// A scalable concurrent tree map implementation.
///
/// It implements an in-memory B+-tree variant.
/// The customized structure of TreeMap allows the scanning operation to perform very efficiently without being blocked.
pub struct TreeIndex<K: Clone + Ord + Sync, V: Clone + Sync> {
    root: Atomic<Node<K, V>>,
    degree: usize,
}

impl<K: Clone + Ord + Sync, V: Clone + Sync> TreeIndex<K, V> {
    pub fn new(block_size: usize) -> TreeIndex<K, V> {
        let size_of_key = std::mem::size_of::<K>();
        let size_of_value = std::mem::size_of::<V>();
        let size_of_ptr = std::mem::size_of::<Atomic<Node<K, V>>>();
        let adjusted_block_size = block_size.max(size_of_key + size_of_value + size_of_ptr);
        let inner_node_degree = (adjusted_block_size - size_of_ptr) / (size_of_ptr + size_of_key);
        let leaf_node_degree = (adjusted_block_size - size_of_ptr) / (size_of_value + size_of_key);
        let adjusted_degree = inner_node_degree.min(leaf_node_degree);
        TreeIndex {
            root: Atomic::null(),
            degree: adjusted_degree,
        }
    }
}

impl<K: Clone + Ord + Sync, V: Clone + Sync> Drop for TreeIndex<K, V> {
    fn drop(&mut self) {}
}
