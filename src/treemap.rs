extern crate crossbeam_epoch;

use crossbeam_epoch::Atomic;

/// A scalable concurrent tree map implementation.
///
/// It implements an in-memory BwTree.
pub struct TreeMap<K: Ord + Sync, V: Sync> {
    root: Atomic<Node<K, V>>,
}

struct Node<K: Ord + Sync, V: Sync> {
    key: K,
    value: V,
    next: Atomic<Node<K, V>>,
}
