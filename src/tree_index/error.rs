#![allow(clippy::module_name_repetitions)]

/// [`InsertError`] types.
pub enum InsertError<K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    /// Duplicated: the same key is found.
    Duplicated((K, V)),
    /// Full: the tree, node, or leaf could not accommodate the entry.
    Full((K, V)),
    /// Retry: the target node, or leaf is being modified.
    Retry((K, V)),
}

/// [`RemoveError`] types.
///
/// The boolean value tagged to the error code indicates that the target entry has been removed.
pub enum RemoveError {
    /// Empty: the node is empty.
    Empty(bool),
    /// Retry: the target node, or leaf is being modified.
    Retry(bool),
}

/// [`SearchError`] types.
pub enum SearchError {
    /// Empty: the tree, node, or leaf is empty.
    Empty,
    /// Retry: the target node, or leaf is being modified.
    Retry,
}
