//! [`HashCache`] is a concurrent and asynchronous sampling-based LRU cache backed by
//! [`HashMap`](super::HashMap).

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
use std::hash::{BuildHasher, Hash};
use std::mem::replace;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed};

/// Scalable concurrent sampling-based LRU cache backed by [`HashMap`](super::HashMap).
///
/// [`HashCache`] is a concurrent sampling-based LRU cache that is based on the
/// [`HashMap`](super::HashMap) implementation. [`HashCache`] does not keep track of the least
/// recently used entry in the entire cache, instead each bucket maintains a doubly linked list of
/// occupied entries which is updated on access to entries in order to keep track of the least
/// recently used entry within the bucket.
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

/// [`Entry`] represents a single cache entry in a [`HashCache`].
pub enum Entry<'h, K, V, H = RandomState>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    /// An occupied entry.
    Occupied(OccupiedEntry<'h, K, V, H>),

    /// A vacant entry.
    Vacant(VacantEntry<'h, K, V, H>),
}

/// [`OccupiedEntry`] is a view into an occupied cache entry in a [`HashCache`].
pub struct OccupiedEntry<'h, K, V, H = RandomState>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    hashcache: &'h HashCache<K, V, H>,
    index: usize,
    data_block_mut: &'h mut DataBlock<K, Evictable<V>, BUCKET_LEN>,
    locker: Locker<'h, K, Evictable<V>, CACHE>,
    entry_ptr: EntryPtr<'h, K, Evictable<V>, CACHE>,
}

/// [`VacantEntry`] is a view into a vacant cache entry in a [`HashCache`].
pub struct VacantEntry<'h, K, V, H = RandomState>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    hashcache: &'h HashCache<K, V, H>,
    key: K,
    hash: u64,
    index: usize,
    data_block_mut: &'h mut DataBlock<K, Evictable<V>, BUCKET_LEN>,
    locker: Locker<'h, K, Evictable<V>, CACHE>,
}

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
        let (array, minimum_capacity) = if minimum_capacity == 0 {
            (AtomicArc::null(), AtomicUsize::new(0))
        } else {
            let array = unsafe {
                Arc::new_unchecked(BucketArray::<K, Evictable<V>, CACHE>::new(
                    minimum_capacity,
                    AtomicArc::null(),
                ))
            };
            let minimum_capacity = array.num_entries();
            (AtomicArc::from(array), AtomicUsize::new(minimum_capacity))
        };
        let maximum_capacity = maximum_capacity
            .max(minimum_capacity.load(Relaxed))
            .max(BucketArray::<K, Evictable<V>, CACHE>::minimum_capacity())
            .min(1_usize << (usize::BITS - 1))
            .next_power_of_two();
        HashCache {
            array,
            minimum_capacity,
            maximum_capacity,
            build_hasher,
        }
    }

    /// Gets the entry associated with the given key in the map for in-place manipulation.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<char, u32> = HashCache::default();
    ///
    /// for ch in "a short treatise on fungi".chars() {
    ///     hashcache.entry(ch).and_modify(|counter| *counter += 1).or_insert(1);
    /// }
    ///
    /// assert_eq!(*hashcache.get(&'s').unwrap().get(), 2);
    /// assert_eq!(*hashcache.get(&'t').unwrap().get(), 3);
    /// assert!(hashcache.get(&'y').is_none());
    /// ```
    #[inline]
    pub fn entry(&self, key: K) -> Entry<K, V, H> {
        let barrier = Barrier::new();
        let hash = self.hash(&key);
        let (mut locker, data_block_mut, entry_ptr, index) = unsafe {
            self.reserve_entry(&key, hash, &mut (), self.prolonged_barrier_ref(&barrier))
                .ok()
                .unwrap_unchecked()
        };
        if entry_ptr.is_valid() {
            locker.update_lru_tail(data_block_mut, &entry_ptr);
            Entry::Occupied(OccupiedEntry {
                hashcache: self,
                index,
                data_block_mut,
                locker,
                entry_ptr,
            })
        } else {
            Entry::Vacant(VacantEntry {
                hashcache: self,
                key,
                hash,
                index,
                data_block_mut,
                locker,
            })
        }
    }

    /// Gets the entry associated with the given key in the map for in-place manipulation.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<char, u32> = HashCache::default();
    ///
    /// let future_entry = hashcache.entry_async('b');
    /// ```
    #[inline]
    pub async fn entry_async(&self, key: K) -> Entry<K, V, H> {
        let hash = self.hash(&key);
        loop {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);
            {
                let barrier = Barrier::new();
                if let Ok((mut locker, data_block_mut, entry_ptr, index)) = self.reserve_entry(
                    &key,
                    hash,
                    &mut async_wait_pinned,
                    self.prolonged_barrier_ref(&barrier),
                ) {
                    if entry_ptr.is_valid() {
                        locker.update_lru_tail(data_block_mut, &entry_ptr);
                        return Entry::Occupied(OccupiedEntry {
                            hashcache: self,
                            index,
                            data_block_mut,
                            locker,
                            entry_ptr,
                        });
                    }
                    return Entry::Vacant(VacantEntry {
                        hashcache: self,
                        key,
                        hash,
                        index,
                        data_block_mut,
                        locker,
                    });
                }
            }
            async_wait_pinned.await;
        }
    }

    /// Puts a key-value pair into the [`HashCache`].
    ///
    /// Returns `Some` if an entry was evicted for the new key-value pair.
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied key-value pair if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// assert!(hashcache.put(1, 0).is_ok());
    /// assert_eq!(hashcache.put(1, 1).unwrap_err(), (1, 1));
    /// ```
    #[inline]
    pub fn put(&self, key: K, val: V) -> Result<Option<(K, V)>, (K, V)> {
        let barrier = Barrier::new();
        let hash = self.hash(&key);
        let result = match self.reserve_entry(&key, hash, &mut (), &barrier) {
            Ok((mut locker, data_block_mut, entry_ptr, _)) => {
                if entry_ptr.is_valid() {
                    return Err((key, val));
                }
                let evicted = locker
                    .evict_lru_head(data_block_mut)
                    .map(|(k, v)| (k, v.take()));
                let entry_ptr = locker.insert_with(
                    data_block_mut,
                    BucketArray::<K, V, CACHE>::partial_hash(hash),
                    || (key, Evictable::new(val)),
                    &barrier,
                );
                locker.update_lru_tail(data_block_mut, &entry_ptr);
                Ok(evicted)
            }
            Err(_) => Err((key, val)),
        };
        result
    }

    /// Puts a key-value pair into the [`HashCache`].
    ///
    /// Returns `Some` if an entry was evicted for the new key-value pair. It is an asynchronous
    /// method returning an `impl Future` for the caller to await.
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied key-value pair if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    /// let future_put = hashcache.put_async(11, 17);
    /// ```
    #[inline]
    pub async fn put_async(&self, key: K, val: V) -> Result<Option<(K, V)>, (K, V)> {
        let hash = self.hash(&key);
        loop {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);
            {
                let barrier = Barrier::new();
                if let Ok((mut locker, data_block_mut, entry_ptr, _)) =
                    self.reserve_entry(&key, hash, &mut async_wait_pinned, &barrier)
                {
                    if entry_ptr.is_valid() {
                        return Err((key, val));
                    }
                    let evicted = locker
                        .evict_lru_head(data_block_mut)
                        .map(|(k, v)| (k, v.take()));
                    let entry_ptr = locker.insert_with(
                        data_block_mut,
                        BucketArray::<K, V, CACHE>::partial_hash(hash),
                        || (key, Evictable::new(val)),
                        &barrier,
                    );
                    locker.update_lru_tail(data_block_mut, &entry_ptr);
                    return Ok(evicted);
                };
            }
            async_wait_pinned.await;
        }
    }

    /// Gets an occupied entry corresponding to the key.
    ///
    /// Returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// assert!(hashcache.get(&1).is_none());
    /// assert!(hashcache.put(1, 10).is_ok());
    /// assert_eq!(*hashcache.get(&1).unwrap().get(), 10);
    /// ```
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<OccupiedEntry<K, V, H>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let barrier = Barrier::new();
        let (mut locker, data_block_mut, entry_ptr, index) = self
            .get_entry(
                key,
                self.hash(key.borrow()),
                &mut (),
                self.prolonged_barrier_ref(&barrier),
            )
            .ok()
            .flatten()?;
        locker.update_lru_tail(data_block_mut, &entry_ptr);
        Some(OccupiedEntry {
            hashcache: self,
            index,
            data_block_mut,
            locker,
            entry_ptr,
        })
    }

    /// Gets an occupied entry corresponding to the key.
    ///
    /// Returns `None` if the key does not exist. It is an asynchronous method returning an
    /// `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    /// let future_put = hashcache.put_async(11, 17);
    /// let future_get = hashcache.get_async(&11);
    /// ```
    #[inline]
    pub async fn get_async<Q>(&self, key: &Q) -> Option<OccupiedEntry<K, V, H>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hash(key);
        loop {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);
            if let Ok(result) = self.get_entry(
                key,
                hash,
                &mut async_wait_pinned,
                self.prolonged_barrier_ref(&Barrier::new()),
            ) {
                if let Some((mut locker, data_block_mut, entry_ptr, index)) = result {
                    locker.update_lru_tail(data_block_mut, &entry_ptr);
                    return Some(OccupiedEntry {
                        hashcache: self,
                        index,
                        data_block_mut,
                        locker,
                        entry_ptr,
                    });
                }
                return None;
            }
            async_wait_pinned.await;
        }
    }

    /// Removes a key-value pair if the key exists.
    ///
    /// Returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// assert!(hashcache.remove(&1).is_none());
    /// assert!(hashcache.put(1, 0).is_ok());
    /// assert_eq!(hashcache.remove(&1).unwrap(), (1, 0));
    /// assert_eq!(hashcache.capacity(), 0);
    /// ```
    #[inline]
    pub fn remove<Q>(&self, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.remove_if(key, |_| true)
    }

    /// Removes a key-value pair if the key exists.
    ///
    /// Returns `None` if the key does not exist. It is an asynchronous method returning an
    /// `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    /// let future_put = hashcache.put_async(11, 17);
    /// let future_remove = hashcache.remove_async(&11);
    /// ```
    #[inline]
    pub async fn remove_async<Q>(&self, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.remove_if_async(key, |_| true).await
    }

    /// Removes a key-value pair if the key exists and the given condition is met.
    ///
    /// Returns `None` if the key does not exist or the condition was not met.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// assert!(hashcache.put(1, 0).is_ok());
    /// assert!(hashcache.remove_if(&1, |v| { *v += 1; false }).is_none());
    /// assert_eq!(hashcache.remove_if(&1, |v| *v == 1).unwrap(), (1, 1));
    /// assert_eq!(hashcache.capacity(), 0);
    /// ```
    #[inline]
    pub fn remove_if<Q, F: FnOnce(&mut V) -> bool>(&self, key: &Q, condition: F) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.get(key).and_then(|mut o| {
            if condition(o.get_mut()) {
                Some(o.remove_entry())
            } else {
                None
            }
        })
    }

    /// Removes a key-value pair if the key exists and the given condition is met.
    ///
    /// Returns `None` if the key does not exist or the condition was not met. It is an
    /// asynchronous method returning an `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    /// let future_insert = hashcache.put_async(11, 17);
    /// let future_remove = hashcache.remove_if_async(&11, |_| true);
    /// ```
    #[inline]
    pub async fn remove_if_async<Q, F: FnOnce(&mut V) -> bool>(
        &self,
        key: &Q,
        condition: F,
    ) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        if let Some(mut occupied_entry) = self.get_async(key).await {
            if condition(occupied_entry.get_mut()) {
                return Some(occupied_entry.remove_entry());
            }
        }
        None
    }

    /// Scans all the entries.
    ///
    /// This method does not affect the LRU information in each bucket.
    ///
    /// Key-value pairs that have existed since the invocation of the method are guaranteed to be
    /// visited if they are not removed, however the same key-value pair can be visited more than
    /// once if the [`HashCache`] gets resized by another thread.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<usize, usize> = HashCache::default();
    ///
    /// assert!(hashcache.put(1, 0).is_ok());
    /// assert!(hashcache.put(2, 1).is_ok());
    ///
    /// let mut sum = 0;
    /// hashcache.scan(|k, v| { sum += *k + *v; });
    /// assert_eq!(sum, 4);
    /// ```
    #[inline]
    pub fn scan<F: FnMut(&K, &V)>(&self, mut scanner: F) {
        let barrier = Barrier::new();
        let mut current_array_ptr = self.array.load(Acquire, &barrier);
        while let Some(current_array) = current_array_ptr.as_ref() {
            self.clear_old_array(current_array, &barrier);
            for index in 0..current_array.num_buckets() {
                let bucket = current_array.bucket(index);
                if let Some(locker) = Reader::lock(bucket, &barrier) {
                    let data_block = current_array.data_block(index);
                    let mut entry_ptr = EntryPtr::new(&barrier);
                    while entry_ptr.next(*locker, &barrier) {
                        let (k, v) = entry_ptr.get(data_block);
                        scanner(k, v);
                    }
                }
            }
            let new_current_array_ptr = self.array.load(Acquire, &barrier);
            if current_array_ptr.without_tag() == new_current_array_ptr.without_tag() {
                break;
            }
            current_array_ptr = new_current_array_ptr;
        }
    }

    /// Scans all the entries.
    ///
    /// This method does not affect the LRU information in each bucket.
    ///
    /// Key-value pairs that have existed since the invocation of the method are guaranteed to be
    /// visited if they are not removed, however the same key-value pair can be visited more than
    /// once if the [`HashCache`] gets resized by another task.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<usize, usize> = HashCache::default();
    ///
    /// let future_put = hashcache.put_async(1, 0);
    /// let future_scan = hashcache.scan_async(|k, v| println!("{k} {v}"));
    /// ```
    #[inline]
    pub async fn scan_async<F: FnMut(&K, &V)>(&self, mut scanner: F) {
        let mut current_array_holder = self.array.get_arc(Acquire, &Barrier::new());
        while let Some(current_array) = current_array_holder.take() {
            self.cleanse_old_array_async(&current_array).await;
            for index in 0..current_array.num_buckets() {
                loop {
                    let mut async_wait = AsyncWait::default();
                    let mut async_wait_pinned = Pin::new(&mut async_wait);
                    {
                        let barrier = Barrier::new();
                        let bucket = current_array.bucket(index);
                        if let Ok(reader) =
                            Reader::try_lock_or_wait(bucket, &mut async_wait_pinned, &barrier)
                        {
                            if let Some(reader) = reader {
                                let data_block = current_array.data_block(index);
                                let mut entry_ptr = EntryPtr::new(&barrier);
                                while entry_ptr.next(*reader, &barrier) {
                                    let (k, v) = entry_ptr.get(data_block);
                                    scanner(k, v);
                                }
                            }
                            break;
                        };
                    }
                    async_wait_pinned.await;
                }
            }

            if let Some(new_current_array) = self.array.get_arc(Acquire, &Barrier::new()) {
                if new_current_array.as_ptr() == current_array.as_ptr() {
                    break;
                }
                current_array_holder.replace(new_current_array);
                continue;
            }
            break;
        }
    }

    /// Retains the entries specified by the predicate.
    ///
    /// This method allows the predicate closure to modify the value field.
    ///
    /// Entries that have existed since the invocation of the method are guaranteed to be visited
    /// if they are not removed, however the same entry can be visited more than once if the
    /// [`HashCache`] gets resized by another thread.
    ///
    /// Returns `(number of remaining entries, number of removed entries)` where the number of
    /// remaining entries can be larger than the actual number since the same entry can be visited
    /// more than once.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// assert!(hashcache.put(1, 0).is_ok());
    /// assert!(hashcache.put(2, 1).is_ok());
    /// assert!(hashcache.put(3, 2).is_ok());
    ///
    /// assert_eq!(hashcache.retain(|k, v| *k == 1 && *v == 0), (1, 2));
    /// ```
    #[inline]
    pub fn retain<F: FnMut(&K, &mut V) -> bool>(&self, mut pred: F) -> (usize, usize) {
        self.retain_entries(|k, v| pred(k, v))
    }

    /// Retains the entries specified by the predicate.
    ///
    /// This method allows the predicate closure to modify the value field.
    ///
    /// Entries that have existed since the invocation of the method are guaranteed to be visited
    /// if they are not removed, however the same entry can be visited more than once if the
    /// [`HashCache`] gets resized by another thread.
    ///
    /// Returns `(number of remaining entries, number of removed entries)` where the number of
    /// remaining entries can be larger than the actual number since the same entry can be visited
    /// more than once. It is an asynchronous method returning an `impl Future` for the caller to
    /// await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// let future_put = hashcache.put_async(1, 0);
    /// let future_retain = hashcache.retain_async(|k, v| *k == 1);
    /// ```
    #[inline]
    pub async fn retain_async<F: FnMut(&K, &mut V) -> bool>(
        &self,
        mut filter: F,
    ) -> (usize, usize) {
        let mut num_retained: usize = 0;
        let mut num_removed: usize = 0;
        let mut current_array_holder = self.array.get_arc(Acquire, &Barrier::new());
        while let Some(current_array) = current_array_holder.take() {
            self.cleanse_old_array_async(&current_array).await;
            for index in 0..current_array.num_buckets() {
                loop {
                    let mut async_wait = AsyncWait::default();
                    let mut async_wait_pinned = Pin::new(&mut async_wait);
                    {
                        let barrier = Barrier::new();
                        let bucket = current_array.bucket_mut(index);
                        if let Ok(locker) =
                            Locker::try_lock_or_wait(bucket, &mut async_wait_pinned, &barrier)
                        {
                            if let Some(mut locker) = locker {
                                let data_block_mut = current_array.data_block_mut(index);
                                let mut entry_ptr = EntryPtr::new(&barrier);
                                while entry_ptr.next(&locker, &barrier) {
                                    let (k, v) = entry_ptr.get_mut(data_block_mut, &mut locker);
                                    if filter(k, v) {
                                        num_retained = num_retained.saturating_add(1);
                                    } else {
                                        locker.erase(data_block_mut, &mut entry_ptr);
                                        num_removed = num_removed.saturating_add(1);
                                    }
                                }
                            }
                            break;
                        };
                    }
                    async_wait_pinned.await;
                }
            }

            if let Some(new_current_array) = self.array.get_arc(Acquire, &Barrier::new()) {
                if new_current_array.as_ptr() == current_array.as_ptr() {
                    break;
                }
                num_retained = 0;
                current_array_holder.replace(new_current_array);
                continue;
            }
            break;
        }

        if num_removed >= num_retained {
            self.try_resize(0, &Barrier::new());
        }

        (num_retained, num_removed)
    }

    /// Clears all the key-value pairs.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// assert!(hashcache.put(1, 0).is_ok());
    /// assert_eq!(hashcache.clear(), 1);
    /// ```
    #[inline]
    pub fn clear(&self) -> usize {
        self.retain(|_, _| false).1
    }

    /// Clears all the key-value pairs.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// let future_put = hashcache.put_async(1, 0);
    /// let future_clear = hashcache.clear_async();
    /// ```
    #[inline]
    pub async fn clear_async(&self) -> usize {
        self.retain_async(|_, _| false).await.1
    }

    /// Returns the number of entries in the [`HashCache`].
    ///
    /// It reads the entire metadata area of the bucket array to calculate the number of valid
    /// entries, making its time complexity `O(N)`. Furthermore, it may overcount entries if an old
    /// bucket array has yet to be dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// assert!(hashcache.put(1, 0).is_ok());
    /// assert_eq!(hashcache.len(), 1);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        self.num_entries(&Barrier::new())
    }

    /// Returns `true` if the [`HashCache`] is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// assert!(hashcache.is_empty());
    /// assert!(hashcache.put(1, 0).is_ok());
    /// assert!(!hashcache.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        !self.has_entry(&Barrier::new())
    }

    /// Returns the capacity of the [`HashCache`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache_default: HashCache<u64, u32> = HashCache::default();
    /// assert_eq!(hashcache_default.capacity(), 0);
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::with_capacity(1000000, 2000000);
    /// assert_eq!(hashcache.capacity(), 1048576);
    /// ```
    #[inline]
    pub fn capacity(&self) -> usize {
        self.num_slots(&Barrier::new())
    }

    /// Returns the current capacity range of the [`HashCache`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// assert_eq!(hashcache.capacity_range(), 0..=256);
    /// ```
    #[inline]
    pub fn capacity_range(&self) -> RangeInclusive<usize> {
        self.minimum_capacity.load(Relaxed)..=self.maximum_capacity()
    }

    /// Clears the old array asynchronously.
    async fn cleanse_old_array_async(&self, current_array: &BucketArray<K, Evictable<V>, CACHE>) {
        while !current_array.old_array(&Barrier::new()).is_null() {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);
            if self.incremental_rehash::<_, _, false>(
                current_array,
                &mut async_wait_pinned,
                &Barrier::new(),
            ) == Ok(true)
            {
                break;
            }
            async_wait_pinned.await;
        }
    }

    /// Returns a reference to the specified [`Barrier`] whose lifetime matches that of `self`.
    fn prolonged_barrier_ref<'h>(&'h self, barrier: &Barrier) -> &'h Barrier {
        let _: &HashCache<_, _, _> = self;
        unsafe { std::mem::transmute::<&Barrier, &'h Barrier>(barrier) }
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
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::with_capacity(1000, 2000);
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

impl<K, V, H> Debug for HashCache<K, V, H>
where
    K: Debug + Eq + Hash,
    V: Debug,
    H: BuildHasher,
{
    /// Iterates over all the entries in the [`HashCache`] to print them.
    ///
    /// ## Locking behavior
    ///
    /// Shared locks on buckets are acquired during iteration, therefore any [`Entry`],
    /// [`OccupiedEntry`] or [`VacantEntry`] owned by the current thread will lead to a deadlock.
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_map();
        self.scan(|k, v| {
            d.entry(k, v);
        });
        d.finish()
    }
}

impl<K, V, H> Drop for HashCache<K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    #[inline]
    fn drop(&mut self) {
        self.array
            .swap((None, Tag::None), Relaxed)
            .0
            .map(|a| unsafe {
                // The entire array does not need to wait for an epoch change as no references will
                // remain outside the lifetime of the `HashMap`.
                a.release_drop_in_place()
            });
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

impl<'h, K, V, H> Entry<'h, K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    /// Ensures a value is in the entry by inserting the supplied instance if empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(3).or_insert(7);
    /// assert_eq!(*hashcache.get(&3).unwrap().get(), 7);
    /// ```
    #[inline]
    pub fn or_insert(self, val: V) -> OccupiedEntry<'h, K, V, H> {
        self.or_insert_with(|| val)
    }

    /// Ensures a value is in the entry by inserting the result of the supplied closure if empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(19).or_insert_with(|| 5);
    /// assert_eq!(*hashcache.get(&19).unwrap().get(), 5);
    /// ```
    #[inline]
    pub fn or_insert_with<F: FnOnce() -> V>(self, constructor: F) -> OccupiedEntry<'h, K, V, H> {
        self.or_insert_with_key(|_| constructor())
    }

    /// Ensures a value is in the entry by inserting the result of the supplied closure if empty.
    ///
    /// The reference to the moved key is provided, therefore cloning or copying the key is
    /// unnecessary.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(11).or_insert_with_key(|k| if *k == 11 { 7 } else { 3 });
    /// assert_eq!(*hashcache.get(&11).unwrap().get(), 7);
    /// ```
    #[inline]
    pub fn or_insert_with_key<F: FnOnce(&K) -> V>(
        self,
        constructor: F,
    ) -> OccupiedEntry<'h, K, V, H> {
        match self {
            Self::Occupied(o) => o,
            Self::Vacant(v) => {
                let val = constructor(v.key());
                v.insert_entry(val)
            }
        }
    }

    /// Returns a reference to the key of this entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    /// assert_eq!(hashcache.entry(31).key(), &31);
    /// ```
    #[inline]
    pub fn key(&self) -> &K {
        match self {
            Self::Occupied(o) => o.key(),
            Self::Vacant(v) => v.key(),
        }
    }

    /// Provides in-place mutable access to an occupied entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(37).and_modify(|v| { *v += 1 }).or_insert(47);
    /// assert_eq!(*hashcache.get(&37).unwrap().get(), 47);
    ///
    /// hashcache.entry(37).and_modify(|v| { *v += 1 }).or_insert(3);
    /// assert_eq!(*hashcache.get(&37).unwrap().get(), 48);
    /// ```
    #[inline]
    #[must_use]
    pub fn and_modify<F>(self, f: F) -> Self
    where
        F: FnOnce(&mut V),
    {
        match self {
            Self::Occupied(mut o) => {
                f(o.get_mut());
                Self::Occupied(o)
            }
            Self::Vacant(_) => self,
        }
    }

    /// Sets the value of the entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    /// let entry = hashcache.entry(11).insert_entry(17);
    /// assert_eq!(entry.key(), &11);
    /// ```
    #[inline]
    pub fn insert_entry(self, val: V) -> OccupiedEntry<'h, K, V, H> {
        match self {
            Self::Occupied(mut o) => {
                o.insert(val);
                o
            }
            Self::Vacant(v) => v.insert_entry(val),
        }
    }
}

impl<'h, K, V, H> Entry<'h, K, V, H>
where
    K: Eq + Hash,
    V: Default,
    H: BuildHasher,
{
    /// Ensures a value is in the entry by inserting the default value if empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    /// hashcache.entry(11).or_default();
    /// assert_eq!(*hashcache.get(&11).unwrap().get(), 0);
    /// ```
    #[inline]
    pub fn or_default(self) -> OccupiedEntry<'h, K, V, H> {
        match self {
            Self::Occupied(o) => o,
            Self::Vacant(v) => v.insert_entry(Default::default()),
        }
    }
}

impl<'h, K, V, H> Debug for Entry<'h, K, V, H>
where
    K: Debug + Eq + Hash,
    V: Debug,
    H: BuildHasher,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Vacant(v) => f.debug_tuple("Entry").field(v).finish(),
            Self::Occupied(o) => f.debug_tuple("Entry").field(o).finish(),
        }
    }
}

impl<'h, K, V, H> OccupiedEntry<'h, K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    /// Gets a reference to the key in the entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// assert_eq!(hashcache.entry(29).or_default().key(), &29);
    /// ```
    #[inline]
    #[must_use]
    pub fn key(&self) -> &K {
        &self.entry_ptr.get(self.data_block_mut).0
    }

    /// Takes ownership of the key and value from the [`HashCache`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use scc::hash_cache::Entry;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(11).or_insert(17);
    ///
    /// if let Entry::Occupied(o) = hashcache.entry(11) {
    ///     assert_eq!(o.remove_entry(), (11, 17));
    /// };
    /// ```
    #[inline]
    #[must_use]
    pub fn remove_entry(mut self) -> (K, V) {
        let (k, v) = unsafe {
            self.locker
                .remove_from_lru_list(self.data_block_mut, &self.entry_ptr);
            self.locker
                .erase(self.data_block_mut, &mut self.entry_ptr)
                .unwrap_unchecked()
        };
        if self.locker.num_entries() <= 1 || self.locker.need_rebuild() {
            let barrier = Barrier::new();
            let hashcache = self.hashcache;
            if let Some(current_array) = hashcache.bucket_array().load(Acquire, &barrier).as_ref() {
                if current_array.old_array(&barrier).is_null() {
                    let index = self.index;
                    if current_array.within_sampling_range(index) {
                        drop(self);
                        hashcache.try_shrink_or_rebuild(current_array, index, &barrier);
                    }
                }
            }
        }
        (k, v.take())
    }

    /// Gets a reference to the value in the entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use scc::hash_cache::Entry;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(19).or_insert(11);
    ///
    /// if let Entry::Occupied(o) = hashcache.entry(19) {
    ///     assert_eq!(o.get(), &11);
    /// };
    /// ```
    #[inline]
    #[must_use]
    pub fn get(&self) -> &V {
        &self.entry_ptr.get(self.data_block_mut).1
    }

    /// Gets a mutable reference to the value in the entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use scc::hash_cache::Entry;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(37).or_insert(11);
    ///
    /// if let Entry::Occupied(mut o) = hashcache.entry(37) {
    ///     *o.get_mut() += 18;
    ///     assert_eq!(*o.get(), 29);
    /// }
    ///
    /// assert_eq!(*hashcache.get(&37).unwrap().get(), 29);
    /// ```
    #[inline]
    pub fn get_mut(&mut self) -> &mut V {
        &mut self
            .entry_ptr
            .get_mut(self.data_block_mut, &mut self.locker)
            .1
    }

    /// Sets the value of the entry, and returns the old value.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use scc::hash_cache::Entry;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(37).or_insert(11);
    ///
    /// if let Entry::Occupied(mut o) = hashcache.entry(37) {
    ///     assert_eq!(o.insert(17), 11);
    /// }
    ///
    /// assert_eq!(*hashcache.get(&37).unwrap().get(), 17);
    /// ```
    #[inline]
    pub fn insert(&mut self, val: V) -> V {
        replace(self.get_mut(), val)
    }

    /// Takes the value out of the entry, and returns it.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use scc::hash_cache::Entry;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(11).or_insert(17);
    ///
    /// if let Entry::Occupied(o) = hashcache.entry(11) {
    ///     assert_eq!(o.remove(), 17);
    /// };
    /// ```
    #[inline]
    #[must_use]
    pub fn remove(self) -> V {
        self.remove_entry().1
    }
}

impl<'h, K, V, H> Debug for OccupiedEntry<'h, K, V, H>
where
    K: Debug + Eq + Hash,
    V: Debug,
    H: BuildHasher,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OccupiedEntry")
            .field("key", self.key())
            .field("value", self.get())
            .finish_non_exhaustive()
    }
}

/// [`OccupiedEntry`] can be sent across threads and awaits with the lock on the bucket held in it.
unsafe impl<'h, K, V, H> Send for OccupiedEntry<'h, K, V, H>
where
    K: Eq + Hash + Send,
    V: Send,
    H: BuildHasher,
{
}

impl<'h, K, V, H> VacantEntry<'h, K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    /// Gets a reference to the key.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    /// assert_eq!(hashcache.entry(11).key(), &11);
    /// ```
    #[inline]
    pub fn key(&self) -> &K {
        &self.key
    }

    /// Takes ownership of the key.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use scc::hash_cache::Entry;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// if let Entry::Vacant(v) = hashcache.entry(17) {
    ///     assert_eq!(v.into_key(), 17);
    /// };
    /// ```
    #[inline]
    pub fn into_key(self) -> K {
        self.key
    }

    /// Sets the value of the entry with its key, and returns an [`OccupiedEntry`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use scc::hash_cache::Entry;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// if let Entry::Vacant(o) = hashcache.entry(19) {
    ///     o.insert_entry(29);
    /// }
    ///
    /// assert_eq!(*hashcache.get(&19).unwrap().get(), 29);
    /// ```
    #[inline]
    pub fn insert_entry(mut self, val: V) -> OccupiedEntry<'h, K, V, H> {
        let barrier = Barrier::new();
        let entry_ptr = self.locker.insert_with(
            self.data_block_mut,
            BucketArray::<K, V, CACHE>::partial_hash(self.hash),
            || (self.key, Evictable::new(val)),
            self.hashcache.prolonged_barrier_ref(&barrier),
        );
        self.locker.update_lru_tail(self.data_block_mut, &entry_ptr);
        OccupiedEntry {
            hashcache: self.hashcache,
            index: self.index,
            data_block_mut: self.data_block_mut,
            locker: self.locker,
            entry_ptr,
        }
    }
}

impl<'h, K, V, H> Debug for VacantEntry<'h, K, V, H>
where
    K: Debug + Eq + Hash,
    V: Debug,
    H: BuildHasher,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("VacantEntry").field(self.key()).finish()
    }
}
