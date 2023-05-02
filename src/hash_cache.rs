//! [`HashCache`] is a concurrent and asynchronous pseudo-LRU cache backed by
//! [`HashMap`](super::HashMap).

#![allow(dead_code, unused_imports)]

use super::ebr::{Arc, AtomicArc, Barrier, Tag};
use super::hash_table::bucket::{
    DataBlock, EntryPtr, Evictable, Locker, Reader, BUCKET_LEN, CACHE,
};
use super::hash_table::bucket_array::BucketArray;
use super::hash_table::HashTable;
use super::wait_queue::AsyncWait;
use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::fmt::{self, Debug};
use std::hash::{BuildHasher, Hash, Hasher};
use std::mem::replace;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed};

/// Scalable concurrent pseudo-LRU cache based on [`HashMap`](super::HashMap).
///
/// ### Unwind safety
///
/// [`HashCache`] is impervious to out-of-memory errors and panics in user specified code on one
/// condition; `H::Hasher::hash`, `K::drop` and `V::drop` must not panic.
pub struct HashCache<K, V, H = RandomState>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    array: AtomicArc<BucketArray<K, Evictable<V>, CACHE>>,
    minimum_capacity: AtomicUsize,
    maximum_capacity: usize,
    build_hasher: H,
}

/// The default maximum capacity of a [`HashCache`] is `256`.
pub const DEFAULT_MAXIMUM_CAPACITY: usize = 256;

impl<K, V, H> HashCache<K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    /// Creates an empty [`HashCache`] with the given [`BuildHasher`].
    ///
    /// The minimum capacity is set to `0`, and the maximum capacity is set to
    /// [`DEFAULT_MAXIMUM_CAPACITY`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashcache: HashCache<u64, u32, RandomState> = HashCache::with_hasher(RandomState::new());
    /// ```
    #[inline]
    pub fn with_hasher(build_hasher: H) -> Self {
        HashCache {
            array: AtomicArc::null(),
            minimum_capacity: AtomicUsize::new(0),
            maximum_capacity: DEFAULT_MAXIMUM_CAPACITY,
            build_hasher,
        }
    }

    /// Creates an empty [`HashCache`] with the specified capacity and [`BuildHasher`].
    ///
    /// The actual capacity is equal to or greater than the specified capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashcache: HashCache<u64, u32, RandomState> =
    ///     HashCache::with_capacity_and_hasher(1000, 2000, RandomState::new());
    ///
    /// let result = hashcache.capacity();
    /// assert_eq!(result, 1024);
    /// ```
    #[inline]
    pub fn with_capacity_and_hasher(
        minimum_capacity: usize,
        maximum_capacity: usize,
        build_hasher: H,
    ) -> Self {
        let array = if minimum_capacity == 0 {
            AtomicArc::null()
        } else {
            AtomicArc::from(unsafe {
                Arc::new_unchecked(BucketArray::<K, Evictable<V>, CACHE>::new(
                    minimum_capacity,
                    AtomicArc::null(),
                ))
            })
        };
        let maximum_capacity = maximum_capacity
            .min(1_usize << (usize::BITS - 1))
            .next_power_of_two();
        HashCache {
            array,
            minimum_capacity: AtomicUsize::new(minimum_capacity),
            maximum_capacity,
            build_hasher,
        }
    }

    /// Returns the capacity of the [`HashCache`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashcache_default: HashCache<u64, u32, RandomState> = HashCache::default();
    /// assert_eq!(hashcache_default.capacity(), 0);
    ///
    /// let hashcache: HashCache<u64, u32, RandomState> = HashCache::with_capacity(1000000, 2000000);
    /// assert_eq!(hashcache.capacity(), 1048576);
    /// ```
    #[inline]
    pub fn capacity(&self) -> usize {
        self.num_slots(&Barrier::new())
    }
}

impl<K, V> HashCache<K, V, RandomState>
where
    K: Eq + Hash,
{
    /// Creates an empty default [`HashCache`].
    ///
    /// The maximum capacity is set to [`DEFAULT_MAXIMUM_CAPACITY`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::new();
    ///
    /// let result = hashcache.capacity();
    /// assert_eq!(result, 0);
    /// ```
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty [`HashCache`] with the specified capacity.
    ///
    /// The supplied minimum and maximum capacity values are adjusted to any suitable
    /// `power-of-two` values that are close to them.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashcache: HashCache<u64, u32, RandomState> = HashCache::with_capacity(1000, 2000);
    ///
    /// let result = hashcache.capacity();
    /// assert_eq!(result, 1024);
    /// ```
    #[inline]
    #[must_use]
    pub fn with_capacity(minimum_capacity: usize, maximum_capacity: usize) -> Self {
        Self::with_capacity_and_hasher(minimum_capacity, maximum_capacity, RandomState::new())
    }
}

impl<K, V, H> Default for HashCache<K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher + Default,
{
    /// Creates an empty default [`HashCache`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// let result = hashcache.capacity();
    /// assert_eq!(result, 0);
    /// ```
    #[inline]
    fn default() -> Self {
        Self::with_hasher(H::default())
    }
}

impl<K, V, H> HashTable<K, Evictable<V>, H, CACHE> for HashCache<K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    #[inline]
    fn hasher(&self) -> &H {
        &self.build_hasher
    }
    #[inline]
    fn try_clone(_entry: &(K, Evictable<V>)) -> Option<(K, Evictable<V>)> {
        None
    }
    #[inline]
    fn try_reset(value: &mut Evictable<V>) {
        value.reset_link();
    }
    #[inline]
    fn bucket_array(&self) -> &AtomicArc<BucketArray<K, Evictable<V>, CACHE>> {
        &self.array
    }
    #[inline]
    fn minimum_capacity(&self) -> &AtomicUsize {
        &self.minimum_capacity
    }
    #[inline]
    fn maximum_capacity(&self) -> usize {
        self.maximum_capacity
    }
}
