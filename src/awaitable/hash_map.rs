//! The module implements [`HashMap`].

use super::async_yield::async_yield;
use super::hash_table::cell_array::CellArray;
use super::hash_table::HashTable;

use crate::ebr::{Arc, AtomicArc, Barrier};

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::AtomicU8;

/// [`HashMap`].
pub struct HashMap<K, V, H = RandomState>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    array: AtomicArc<CellArray<K, V, false>>,
    minimum_capacity: usize,
    resize_mutex: AtomicU8,
    build_hasher: H,
}

impl<K, V, H> HashMap<K, V, H>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    /// Creates an empty [`HashMap`] with the given capacity and [`BuildHasher`].
    ///
    /// The actual capacity is equal to or greater than the given capacity.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashmap: HashMap<u64, u32, RandomState> = HashMap::new(1000, RandomState::new());
    ///
    /// let result = hashmap.capacity();
    /// assert_eq!(result, 1024);
    /// ```
    pub fn new(capacity: usize, build_hasher: H) -> HashMap<K, V, H> {
        let initial_capacity = capacity.max(Self::default_capacity());
        let array = Arc::new(CellArray::<K, V, false>::new(
            initial_capacity,
            AtomicArc::null(),
        ));
        let current_capacity = array.num_entries();
        HashMap {
            array: AtomicArc::from(array),
            minimum_capacity: current_capacity,
            resize_mutex: AtomicU8::new(0),
            build_hasher,
        }
    }

    /// Inserts a key-value pair into the [`HashMap`].
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied key-value pair.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert(11, 17);
    /// ```
    #[inline]
    pub async fn insert(&self, mut key: K, mut val: V) -> Result<(), (K, V)> {
        loop {
            match self.insert_entry(key, val, &Barrier::new()) {
                Err(err) => {
                    key = err.0;
                    val = err.1;
                }
                _ => return Ok(()),
            }
            async_yield().await;
        }
    }

    /// Reads a key-value pair.
    ///
    /// It returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert(11, 17);
    /// let future_read = hashmap.read(&11, |_, v| *v);
    /// ```
    #[inline]
    pub async fn read<Q, R, F: FnMut(&K, &V) -> R>(&self, key_ref: &Q, mut reader: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        loop {
            if let Ok(result) = self.read_entry(key_ref, &mut reader, &Barrier::new()) {
                return result;
            }
            async_yield().await;
        }
    }

    /// Removes a key-value pair if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert(11, 17);
    /// let future_remove = hashmap.remove(&11);
    /// ```
    #[inline]
    pub async fn remove<Q>(&self, key_ref: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.remove_if(key_ref, |_| true).await
    }

    /// Removes a key-value pair if the key exists and the given condition is met.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert(11, 17);
    /// let future_remove = hashmap.remove_if(&11, |_| true);
    /// ```
    #[inline]
    pub async fn remove_if<Q, F: FnMut(&V) -> bool>(
        &self,
        key_ref: &Q,
        mut condition: F,
    ) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        loop {
            if let Ok(result) = self.remove_entry(key_ref, &mut condition, &Barrier::new()) {
                return result.0;
            }
            async_yield().await;
        }
    }

    /// Returns the number of entries in the [`HashMap`].
    ///
    /// It scans the entire array to calculate the number of valid entries, making its time
    /// complexity `O(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert_eq!(hashmap.len(), 0);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        self.num_entries(&Barrier::new())
    }

    /// Returns `true` if the [`HashMap`] is empty.
    ///
    /// It scans the entire array to calculate the number of valid entries, making its time
    /// complexity `O(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the capacity of the [`HashMap`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashmap: HashMap<u64, u32, RandomState> = HashMap::new(1000000, RandomState::new());
    /// assert_eq!(hashmap.capacity(), 1048576);
    /// ```
    #[inline]
    pub fn capacity(&self) -> usize {
        self.num_slots(&Barrier::new())
    }
}

impl<K, V> Default for HashMap<K, V, RandomState>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
{
    /// Creates a [`HashMap`] with the default parameters.
    ///
    /// The default hash builder is [`RandomState`], and the default capacity is `64`.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// ```
    fn default() -> Self {
        HashMap {
            array: AtomicArc::new(CellArray::<K, V, false>::new(
                Self::default_capacity(),
                AtomicArc::null(),
            )),
            minimum_capacity: Self::default_capacity(),
            resize_mutex: AtomicU8::new(0),
            build_hasher: RandomState::new(),
        }
    }
}

impl<K, V, H> HashTable<K, V, H, false> for HashMap<K, V, H>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    fn hasher(&self) -> &H {
        &self.build_hasher
    }

    fn copier(_key: &K, _val: &V) -> Option<(K, V)> {
        None
    }

    fn cell_array(&self) -> &AtomicArc<CellArray<K, V, false>> {
        &self.array
    }

    fn minimum_capacity(&self) -> usize {
        self.minimum_capacity
    }

    fn resize_mutex(&self) -> &AtomicU8 {
        &self.resize_mutex
    }
}
