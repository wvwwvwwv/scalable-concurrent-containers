#![allow(dead_code)]
#![allow(unused_variables)]

//! The module implements [`HashMap`].

use super::async_yield::async_yield;
use super::hash_table::cell_array::CellArray;
use super::hash_table::HashTable;

use crate::ebr::{AtomicArc, Barrier};

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicU8, AtomicUsize};

/// [`HashMap`].
pub struct HashMap<K, V, H = RandomState>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    array: AtomicArc<CellArray<K, V, false>>,
    minimum_capacity: usize,
    additional_capacity: AtomicUsize,
    resize_mutex: AtomicU8,
    build_hasher: H,
}

impl<K, V, H> HashMap<K, V, H>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
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
            additional_capacity: AtomicUsize::new(0),
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
