use crossbeam_epoch::Atomic;

pub struct Node<K: Ord + Sync, V: Sync> {
    key: K,
    value: V,
    next: Atomic<Node<K, V>>,
}