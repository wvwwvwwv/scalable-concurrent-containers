/// Error types.
pub enum Error<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// Duplicated key found: returns the given key-value pair.
    Duplicated((K, V)),
    /// Full: the tree, node, or leaf could not accommodate the entry.
    Full((K, V)),
    /// Retry: return the given key-value pair.
    Retry((K, V)),
}
