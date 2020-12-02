use crossbeam_epoch::Atomic;

pub struct Node<K: Clone + Ord + Sync, V: Clone + Sync> {
    key: K,
    value: V,
    next: Atomic<Node<K, V>>,
}