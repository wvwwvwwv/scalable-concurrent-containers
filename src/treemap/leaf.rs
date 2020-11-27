use crossbeam_epoch::Atomic;

pub struct Leaf<K: Ord + Sync, V: Sync> {
    key: K,
    value: V,
    next: Atomic<Leaf<K, V>>,
}