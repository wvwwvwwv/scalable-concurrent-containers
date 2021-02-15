use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::AtomicBool;

const DEFAULT_CAPACITY: usize = 256;

/// A scalable concurrent hash index implementation.
///
/// scc::HashIndex is a concurrent hash index data structure that is aimed at read-most workloads.
pub struct HashIndex<K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
    _array: Option<(K, V)>,
    _minimum_capacity: usize,
    _resize_mutex: AtomicBool,
    build_hasher: H,
}

impl<K, V> Default for HashIndex<K, V, RandomState>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
{
    /// Creates a HashIndex instance with the default parameters.
    ///
    /// The default hash builder is RandomState, and the default capacity is 256.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails.
    ///
    /// ```
    /// use scc::HashIndex;
    /// ```
    fn default() -> Self {
        HashIndex {
            _array: None,
            _minimum_capacity: DEFAULT_CAPACITY,
            _resize_mutex: AtomicBool::new(false),
            build_hasher: RandomState::new(),
        }
    }
}

impl<K, V, H> HashIndex<K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
    /// Creates an empty HashIndex instance with the given capacity and build hasher.
    ///
    /// The actual capacity is equal to or greater than the given capacity.
    /// It is recommended to give a capacity value that is larger than 16 * the number of threads to access the HashMap.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// use std::collections::hash_map::RandomState;
    /// ```
    pub fn new(capacity: usize, build_hasher: H) -> HashIndex<K, V, H> {
        let initial_capacity = capacity.max(DEFAULT_CAPACITY);
        HashIndex {
            _array: None,
            _minimum_capacity: initial_capacity,
            _resize_mutex: AtomicBool::new(false),
            build_hasher,
        }
    }

    /// Inserts a key-value pair into the HashIndex.
    ///
    /// Returns an error with the given key-value pair attached if the key exists.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails, or the number of entries in the target cell reaches u32::MAX.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// ```
    pub fn insert(&self, key: K, value: V) -> Result<(), (K, V)> {
        Err((key, value))
    }

    /// Removes a key-value pair.
    ///
    /// Returns false if the key does not exist.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// ```
    pub fn remove(&self, _tkey: &K) -> bool {
        false
    }

    /// Reads a key-value pair.
    ///
    /// # Errors
    ///
    /// Returns None if the key does not exist.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// ```
    pub fn read<U, F: FnOnce(&K, &V) -> U>(&self, _key: &K, _f: F) -> Option<U> {
        None
    }

    /// Retains the key-value pairs that satisfy the given predicate.
    ///
    /// It returns the number of entries remaining and removed.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// ```
    pub fn retain<F: Fn(&K, &V) -> bool>(&self, _f: F) -> (usize, usize) {
        (0, 0)
    }

    /// Clears all the key-value pairs.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// ```
    pub fn clear(&self) -> usize {
        self.retain(|_, _| false).1
    }

    /// Returns an estimated size of the HashIndex.
    ///
    /// The given function determines the sampling size.
    /// A function returning a fixed number larger than u16::MAX yields around 99% accuracy.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// ```
    pub fn len<F: FnOnce(usize) -> usize>(&self, _f: F) -> usize {
        0
    }

    /// Returns the capacity of the HashIndex.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// ```
    pub fn capacity(&self) -> usize {
        0
    }

    /// Returns a reference to its build hasher.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashindex: HashIndex<u64, u32, _> = Default::default();
    /// let result: &RandomState = hashindex.hasher();
    /// ```
    pub fn hasher(&self) -> &H {
        &self.build_hasher
    }

    /// Returns a Visitor.
    ///
    /// It is guaranteed to go through all the key-value pairs pertaining in the HashIndex at the moment,
    /// however the same key-value pair can be visited more than once if the HashIndex is being resized.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// ```
    pub fn iter(&self) -> Visitor<K, V, H> {
        Visitor { _hash_index: self }
    }
}

impl<K, V, H> Drop for HashIndex<K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
    fn drop(&mut self) {}
}

/// Visitor.
pub struct Visitor<'h, K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
    _hash_index: &'h HashIndex<K, V, H>,
}

impl<'h, K, V, H> Iterator for Visitor<'h, K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
    type Item = (&'h K, &'h V);
    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
