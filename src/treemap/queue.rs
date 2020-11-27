pub struct Queue<K: Ord + Sync, V: Sync> {
    key: K,
    value: V,
}
