/// InsertError types.
pub enum InsertError<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// Duplicated key found: returns the given key-value pair.
    Duplicated((K, V)),
    /// Full: the tree, node, or leaf could not accommodate the entry.
    Full((K, V)),
    /// Retry: return the given key-value pair.
    Retry((K, V)),
}

/// RemoveError types.
///
/// The boolean value tagged to the error code indicates that the target entry has been removed.
pub enum RemoveError {
    /// Coalesce: the node, or leaf cannot accommodate any other key-value pairs.
    Coalesce(bool),
    /// Retry: a conflict detected.
    Retry(bool),
}
