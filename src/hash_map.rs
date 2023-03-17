//! [`HashMap`] is a concurrent and asynchronous hash map.

use super::ebr::{Arc, AtomicArc, Barrier, Tag};
use super::hash_table::bucket::{DataBlock, EntryPtr, Locker, Reader, BUCKET_LEN};
use super::hash_table::bucket_array::BucketArray;
use super::hash_table::HashTable;
use super::wait_queue::AsyncWait;

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::fmt::{self, Debug};
use std::hash::{BuildHasher, Hash};
use std::mem::replace;
use std::pin::Pin;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::atomic::{AtomicU8, AtomicUsize};

/// Scalable concurrent hash map.
///
/// [`HashMap`] is a concurrent and asynchronous hash map data structure that is targeted at a
/// highly concurrent workload. [`HashMap`] has a dynamically sized array of buckets where a bucket
/// is a fixed size hash table with linear probing that can be expanded by allocating a linked list
/// of smaller buckets when the bucket is full.
///
/// ## The key features of [`HashMap`]
///
/// * Non-sharded: the data is stored in a single array of entry buckets.
/// * Non-blocking resizing: resizing does not block other threads or tasks.
/// * Automatic resizing: it automatically grows or shrinks.
/// * Incremental resizing: each access to the hash map is mandated to move a fixed
///   number of entries if an old array is present.
/// * No busy waiting: the thread or asynchronous task is suspended until the desired resource
///   becomes available.
/// * Linearizability: [`HashMap`] insert/read/remove/update/upsert methods are linearizable.
///
/// ## The key statistics for [`HashMap`]
///
/// * The expected size of metadata for a single entry: 2-byte.
/// * The expected number of atomic write operations required for an operation on a single key: 2.
/// * The expected number of atomic variables accessed during a single key operation: 2.
/// * The number of entries managed by a single bucket without a linked list: 32.
/// * The expected maximum linked list length when a resize is triggered: log(capacity) / 8.
///
/// ## Locking behavior
///
/// ### Bucket access
///
/// Bucket arrays are protected by [`ebr`](super::ebr), thus allowing lock-free access to them.
///
/// ### Entry access
///
/// Each read/write access to an entry is serialized by the read-write lock in the bucket
/// containing the entry. There are no container-level locks, therefore, the larger the [`HashMap`]
/// gets, the lower the chance that the bucket-level lock being contended.
///
/// ### Resize
///
/// Resizing of the [`HashMap`] is totally non-blocking and lock-free; resizing does not block any
/// other read/write access to the [`HashMap`] or resizing attempts. Resizing is analogous to
/// pushing a new bucket array into a lock-free stack. Each individual entry in the old bucket
/// array will be incrementally relocated to the new bucket array on future access to the
/// [`HashMap`], and the old bucket array gets dropped when it becomes empty and unreachable.
pub struct HashMap<K, V, H = RandomState>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    array: AtomicArc<BucketArray<K, V, false>>,
    minimum_capacity: usize,
    additional_capacity: AtomicUsize,
    resize_mutex: AtomicU8,
    build_hasher: H,
}

/// [`Entry`] represents a single entry in a [`HashMap`].
pub enum Entry<'h, K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    /// An occupied entry.
    Occupied(OccupiedEntry<'h, K, V, H>),

    /// A vacant entry.
    Vacant(VacantEntry<'h, K, V, H>),
}

/// [`OccupiedEntry`] is a view into an occupied entry in a [`HashMap`].
pub struct OccupiedEntry<'h, K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    hashmap: &'h HashMap<K, V, H>,
    hash: u64,
    data_block: &'h DataBlock<K, V, BUCKET_LEN>,
    locker: Locker<'h, K, V, false>,
    entry_ptr: EntryPtr<'h, K, V, false>,
}

/// [`VacantEntry`] is a view into a vacant entry in a [`HashMap`].
pub struct VacantEntry<'h, K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    hashmap: &'h HashMap<K, V, H>,
    key: K,
    hash: u64,
    data_block: &'h DataBlock<K, V, BUCKET_LEN>,
    locker: Locker<'h, K, V, false>,
}

/// [`Ticket`] keeps the increased minimum capacity of the [`HashMap`] during its lifetime.
///
/// The minimum capacity is lowered when the [`Ticket`] is dropped, thereby allowing unused
/// memory to be reclaimed.
pub struct Ticket<'h, K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    hashmap: &'h HashMap<K, V, H>,
    increment: usize,
}

impl<K, V, H> HashMap<K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    /// Creates an empty [`HashMap`] with the given [`BuildHasher`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashmap: HashMap<u64, u32, RandomState> = HashMap::with_hasher(RandomState::new());
    /// ```
    #[inline]
    pub fn with_hasher(build_hasher: H) -> HashMap<K, V, H> {
        HashMap {
            array: AtomicArc::from(unsafe {
                Arc::new_unchecked(BucketArray::<K, V, false>::new(
                    Self::DEFAULT_CAPACITY,
                    AtomicArc::null(),
                ))
            }),
            minimum_capacity: Self::DEFAULT_CAPACITY,
            additional_capacity: AtomicUsize::new(0),
            resize_mutex: AtomicU8::new(0),
            build_hasher,
        }
    }

    /// Creates an empty [`HashMap`] with the specified capacity and [`BuildHasher`].
    ///
    /// The actual capacity is equal to or greater than the specified capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashmap: HashMap<u64, u32, RandomState> =
    ///     HashMap::with_capacity_and_hasher(1000, RandomState::new());
    ///
    /// let result = hashmap.capacity();
    /// assert_eq!(result, 1024);
    /// ```
    #[inline]
    pub fn with_capacity_and_hasher(capacity: usize, build_hasher: H) -> HashMap<K, V, H> {
        let initial_capacity = capacity.max(Self::DEFAULT_CAPACITY);
        let array = unsafe {
            Arc::new_unchecked(BucketArray::<K, V, false>::new(
                initial_capacity,
                AtomicArc::null(),
            ))
        };
        let current_capacity = array.num_entries();
        HashMap {
            array: AtomicArc::from(array),
            minimum_capacity: current_capacity,
            additional_capacity: AtomicUsize::new(0),
            resize_mutex: AtomicU8::new(0),
            build_hasher,
        }
    }

    /// Temporarily increases the minimum capacity of the [`HashMap`].
    ///
    /// The reserved space is not exclusively owned by the [`Ticket`], thus can be overtaken.
    /// Unused space is immediately reclaimed when the [`Ticket`] is dropped.
    ///
    /// # Errors
    ///
    /// Returns `None` if a too large number is given.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashmap: HashMap<usize, usize, RandomState> = HashMap::with_capacity(1000);
    /// assert_eq!(hashmap.capacity(), 1024);
    ///
    /// let ticket = hashmap.reserve(10000);
    /// assert!(ticket.is_some());
    /// assert_eq!(hashmap.capacity(), 16384);
    /// for i in 0..16 {
    ///     assert!(hashmap.insert(i, i).is_ok());
    /// }
    /// drop(ticket);
    ///
    /// assert_eq!(hashmap.capacity(), 1024);
    /// ```
    #[inline]
    pub fn reserve(&self, capacity: usize) -> Option<Ticket<K, V, H>> {
        let mut current_additional_capacity = self.additional_capacity.load(Relaxed);
        loop {
            if usize::MAX - self.minimum_capacity - current_additional_capacity <= capacity {
                // The given value is too large.
                return None;
            }
            match self.additional_capacity.compare_exchange(
                current_additional_capacity,
                current_additional_capacity + capacity,
                Relaxed,
                Relaxed,
            ) {
                Ok(_) => {
                    self.resize(&Barrier::new());
                    return Some(Ticket {
                        hashmap: self,
                        increment: capacity,
                    });
                }
                Err(current) => current_additional_capacity = current,
            }
        }
    }

    /// Gets the entry associated with the given key in the map for in-place manipulation.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<char, u32> = HashMap::default();
    ///
    /// for ch in "a short treatise on fungi".chars() {
    ///     hashmap.entry(ch).and_modify(|counter| *counter += 1).or_insert(1);
    /// }
    ///
    /// assert_eq!(hashmap.read(&'s', |_, v| *v), Some(2));
    /// assert_eq!(hashmap.read(&'t', |_, v| *v), Some(3));
    /// assert!(hashmap.read(&'y', |_, v| *v).is_none());
    /// ```
    #[inline]
    pub fn entry(&self, key: K) -> Entry<K, V, H> {
        let barrier = Barrier::new();
        let hash = self.hash(key.borrow());
        let (locker, data_block, entry_ptr) = unsafe {
            self.acquire_entry(
                key.borrow(),
                hash,
                &mut (),
                self.prolonged_barrier_ref(&barrier),
            )
            .ok()
            .unwrap_unchecked()
        };
        if entry_ptr.is_valid() {
            Entry::Occupied(OccupiedEntry {
                hashmap: self,
                hash,
                data_block,
                locker,
                entry_ptr,
            })
        } else {
            Entry::Vacant(VacantEntry {
                hashmap: self,
                key,
                hash,
                data_block,
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
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<char, u32> = HashMap::default();
    ///
    /// let future_entry = hashmap.entry_async('b');
    /// ```
    #[inline]
    pub async fn entry_async(&self, key: K) -> Entry<K, V, H> {
        let hash = self.hash(key.borrow());
        loop {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);
            let barrier = Barrier::new();
            if let Ok((locker, data_block, entry_ptr)) = self.acquire_entry(
                key.borrow(),
                hash,
                &mut async_wait_pinned,
                self.prolonged_barrier_ref(&barrier),
            ) {
                if entry_ptr.is_valid() {
                    return Entry::Occupied(OccupiedEntry {
                        hashmap: self,
                        hash,
                        data_block,
                        locker,
                        entry_ptr,
                    });
                }
                return Entry::Vacant(VacantEntry {
                    hashmap: self,
                    key,
                    hash,
                    data_block,
                    locker,
                });
            }
            async_wait_pinned.await;
        }
    }

    /// Inserts a key-value pair into the [`HashMap`].
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied key-value pair if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert_eq!(hashmap.insert(1, 1).unwrap_err(), (1, 1));
    /// ```
    #[inline]
    pub fn insert(&self, key: K, val: V) -> Result<(), (K, V)> {
        let barrier = Barrier::new();
        let hash = self.hash(key.borrow());
        if let Ok(Some((k, v))) = self.insert_entry(key, val, hash, &mut (), &barrier) {
            Err((k, v))
        } else {
            Ok(())
        }
    }

    /// Inserts a key-value pair into the [`HashMap`].
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied key-value pair if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert_async(11, 17);
    /// ```
    #[inline]
    pub async fn insert_async(&self, mut key: K, mut val: V) -> Result<(), (K, V)> {
        let hash = self.hash(key.borrow());
        loop {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);
            match self.insert_entry(key, val, hash, &mut async_wait_pinned, &Barrier::new()) {
                Ok(Some(returned)) => return Err(returned),
                Ok(None) => return Ok(()),
                Err(returned) => {
                    key = returned.0;
                    val = returned.1;
                }
            }
            async_wait_pinned.await;
        }
    }

    /// Updates an existing key-value pair.
    ///
    /// It returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.update(&1, |_, _| true).is_none());
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert_eq!(hashmap.update(&1, |_, v| { *v = 2; *v }).unwrap(), 2);
    /// assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
    /// ```
    #[inline]
    pub fn update<Q, U, R>(&self, key: &Q, updater: U) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        U: FnOnce(&K, &mut V) -> R,
    {
        let barrier = Barrier::new();
        let (mut locker, data_block, mut entry_ptr) = self
            .acquire_entry(key, self.hash(key.borrow()), &mut (), &barrier)
            .ok()?;
        if entry_ptr.is_valid() {
            let (k, v) = entry_ptr.get_mut(data_block, &mut locker);
            return Some(updater(k, v));
        }
        None
    }

    /// Updates an existing key-value pair.
    ///
    /// It returns `None` if the key does not exist. It is an asynchronous method returning an
    /// `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// let future_update = hashmap.update_async(&1, |_, v| { *v = 2; *v });
    /// ```
    #[inline]
    pub async fn update_async<Q, U, R>(&self, key: &Q, updater: U) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        U: FnOnce(&K, &mut V) -> R,
    {
        let hash = self.hash(key);
        loop {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);
            if let Ok((mut locker, data_block, mut entry_ptr)) =
                self.acquire_entry(key, hash, &mut async_wait_pinned, &Barrier::new())
            {
                if entry_ptr.is_valid() {
                    let (k, v) = entry_ptr.get_mut(data_block, &mut locker);
                    return Some(updater(k, v));
                }
                return None;
            }
            async_wait_pinned.await;
        }
    }

    /// Constructs the value in-place, or modifies an existing value corresponding to the key.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// hashmap.upsert(1, || 2, |_, v| *v = 2);
    /// assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
    /// hashmap.upsert(1, || 2, |_, v| *v = 3);
    /// assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 3);
    /// ```
    #[inline]
    pub fn upsert<C: FnOnce() -> V, U: FnOnce(&K, &mut V)>(
        &self,
        key: K,
        constructor: C,
        updater: U,
    ) {
        let barrier = Barrier::new();
        let hash = self.hash(key.borrow());
        if let Ok((mut locker, data_block, mut entry_ptr)) =
            self.acquire_entry(&key, hash, &mut (), &barrier)
        {
            if entry_ptr.is_valid() {
                let (k, v) = entry_ptr.get_mut(data_block, &mut locker);
                updater(k, v);
                return;
            }
            let val = constructor();
            locker.insert_with(
                data_block,
                BucketArray::<K, V, false>::partial_hash(hash),
                || (key, val),
                &barrier,
            );
        };
    }

    /// Constructs the value in-place, or modifies an existing value corresponding to the key.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// let future_upsert = hashmap.upsert_async(1, || 2, |_, v| *v = 3);
    /// ```
    #[inline]
    pub async fn upsert_async<C: FnOnce() -> V, U: FnOnce(&K, &mut V)>(
        &self,
        key: K,
        constructor: C,
        updater: U,
    ) {
        let hash = self.hash(key.borrow());
        loop {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);
            {
                let barrier = Barrier::new();
                if let Ok((mut locker, data_block, mut entry_ptr)) =
                    self.acquire_entry(&key, hash, &mut async_wait_pinned, &barrier)
                {
                    if entry_ptr.is_valid() {
                        let (k, v) = entry_ptr.get_mut(data_block, &mut locker);
                        updater(k, v);
                    } else {
                        let val = constructor();
                        locker.insert_with(
                            data_block,
                            BucketArray::<K, V, false>::partial_hash(hash),
                            || (key, val),
                            &barrier,
                        );
                    }
                    return;
                };
            }
            async_wait_pinned.await;
        }
    }

    /// Removes a key-value pair if the key exists.
    ///
    /// It returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.remove(&1).is_none());
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert_eq!(hashmap.remove(&1).unwrap(), (1, 0));
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
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert_async(11, 17);
    /// let future_remove = hashmap.remove_async(&11);
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
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.remove_if(&1, |v| *v == 1).is_none());
    /// assert_eq!(hashmap.remove_if(&1, |v| *v == 0).unwrap(), (1, 0));
    /// ```
    #[inline]
    pub fn remove_if<Q, F: FnOnce(&V) -> bool>(&self, key: &Q, condition: F) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.remove_entry(
            key,
            self.hash(key.borrow()),
            condition,
            Option::flatten,
            &mut (),
            &Barrier::new(),
        )
        .ok()
        .flatten()
    }

    /// Removes a key-value pair if the key exists and the given condition is met.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert_async(11, 17);
    /// let future_remove = hashmap.remove_if_async(&11, |_| true);
    /// ```
    #[inline]
    pub async fn remove_if_async<Q, F: FnOnce(&V) -> bool>(
        &self,
        key: &Q,
        mut condition: F,
    ) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hash(key);
        loop {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);
            match self.remove_entry(
                key,
                hash,
                condition,
                Option::flatten,
                &mut async_wait_pinned,
                &Barrier::new(),
            ) {
                Ok(r) => return r,
                Err(c) => condition = c,
            };
            async_wait_pinned.await;
        }
    }

    /// Reads a key-value pair.
    ///
    /// It returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.read(&1, |_, v| *v).is_none());
    /// assert!(hashmap.insert(1, 10).is_ok());
    /// assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 10);
    /// ```
    #[inline]
    pub fn read<Q, R, F: FnOnce(&K, &V) -> R>(&self, key: &Q, reader: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.read_entry(key, self.hash(key), &mut (), &Barrier::new())
            .ok()
            .flatten()
            .map(|(k, v)| reader(k, v))
    }

    /// Reads a key-value pair.
    ///
    /// It returns `None` if the key does not exist. It is an asynchronous method returning an
    /// `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert_async(11, 17);
    /// let future_read = hashmap.read_async(&11, |_, v| *v);
    /// ```
    #[inline]
    pub async fn read_async<Q, R, F: FnOnce(&K, &V) -> R>(&self, key: &Q, reader: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hash(key);
        loop {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);
            if let Ok(result) = self.read_entry(key, hash, &mut async_wait_pinned, &Barrier::new())
            {
                return result.map(|(k, v)| reader(k, v));
            }
            async_wait_pinned.await;
        }
    }

    /// Checks if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(!hashmap.contains(&1));
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.contains(&1));
    /// ```
    #[inline]
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.read(key, |_, _| ()).is_some()
    }

    /// Checks if the key exists.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// let future_contains = hashmap.contains_async(&1);
    /// ```
    #[inline]
    pub async fn contains_async<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.read_async(key, |_, _| ()).await.is_some()
    }

    /// Scans all the entries.
    ///
    /// Key-value pairs that have existed since the invocation of the method are guaranteed to be
    /// visited if they are not removed, however the same key-value pair can be visited more than
    /// once if the [`HashMap`] gets resized by another thread.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<usize, usize> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.insert(2, 1).is_ok());
    ///
    /// let mut sum = 0;
    /// hashmap.scan(|k, v| { sum += *k + *v; });
    /// assert_eq!(sum, 4);
    /// ```
    #[inline]
    pub fn scan<F: FnMut(&K, &V)>(&self, mut scanner: F) {
        self.any(|k, v| {
            scanner(k, v);
            false
        });
    }

    /// Scans all the entries.
    ///
    /// Key-value pairs that have existed since the invocation of the method are guaranteed to be
    /// visited if they are not removed, however the same key-value pair can be visited more than
    /// once if the [`HashMap`] gets resized by another task.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<usize, usize> = HashMap::default();
    ///
    /// let future_insert = hashmap.insert_async(1, 0);
    /// let future_scan = hashmap.scan_async(|k, v| println!("{k} {v}"));
    /// ```
    #[inline]
    pub async fn scan_async<F: FnMut(&K, &V)>(&self, mut scanner: F) {
        self.any_async(|k, v| {
            scanner(k, v);
            false
        })
        .await;
    }

    /// Searches for any entry that satisfies the given predicate.
    ///
    /// Key-value pairs that have existed since the invocation of the method are guaranteed to be
    /// visited if they are not removed, however the same key-value pair can be visited more than
    /// once if the [`HashMap`] gets resized by another thread.
    ///
    /// It returns `true` if an entry satisfying the predicate is found.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.insert(2, 1).is_ok());
    /// assert!(hashmap.insert(3, 2).is_ok());
    ///
    /// assert!(hashmap.any(|k, v| *k == 1 && *v == 0));
    /// assert!(!hashmap.any(|k, v| *k == 2 && *v == 0));
    /// ```
    #[inline]
    pub fn any<P: FnMut(&K, &V) -> bool>(&self, mut pred: P) -> bool {
        let barrier = Barrier::new();

        // An acquire fence is required to correctly load the contents of the array.
        let mut current_array_ptr = self.array.load(Acquire, &barrier);
        while let Some(current_array) = current_array_ptr.as_ref() {
            while !current_array.old_array(&barrier).is_null() {
                if self.partial_rehash::<_, _, false>(current_array, &mut (), &barrier) == Ok(true)
                {
                    break;
                }
            }
            debug_assert!(current_array.old_array(&barrier).is_null());

            for index in 0..current_array.num_buckets() {
                let bucket = current_array.bucket(index);
                if let Some(locker) = Reader::lock(bucket, &barrier) {
                    let data_block = current_array.data_block(index);
                    let mut entry_ptr = EntryPtr::new(&barrier);
                    while entry_ptr.next(locker.bucket(), &barrier) {
                        let (k, v) = entry_ptr.get(data_block);
                        if pred(k, v) {
                            return true;
                        }
                    }
                }
            }

            let new_current_array_ptr = self.array.load(Acquire, &barrier);
            if current_array_ptr == new_current_array_ptr {
                break;
            }
            current_array_ptr = new_current_array_ptr;
        }

        false
    }

    /// Searches for any entry that satisfies the given predicate.
    ///
    /// Key-value pairs that have existed since the invocation of the method are guaranteed to be
    /// visited if they are not removed, however the same key-value pair can be visited more than
    /// once if the [`HashMap`] gets resized by another task.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
    ///
    /// It returns `true` if an entry satisfying the predicate is found.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// let future_insert = hashmap.insert_async(1, 0);
    /// let future_any = hashmap.any_async(|k, _| *k == 1);
    /// ```
    #[inline]
    pub async fn any_async<P: FnMut(&K, &V) -> bool>(&self, mut pred: P) -> bool {
        // An acquire fence is required to correctly load the contents of the array.
        let mut current_array_holder = self.array.get_arc(Acquire, &Barrier::new());
        while let Some(current_array) = current_array_holder.take() {
            while !current_array.old_array(&Barrier::new()).is_null() {
                let mut async_wait = AsyncWait::default();
                let mut async_wait_pinned = Pin::new(&mut async_wait);
                if self.partial_rehash::<_, _, false>(
                    &current_array,
                    &mut async_wait_pinned,
                    &Barrier::new(),
                ) == Ok(true)
                {
                    break;
                }
                async_wait_pinned.await;
            }
            debug_assert!(current_array.old_array(&Barrier::new()).is_null());

            for index in 0..current_array.num_buckets() {
                let killed = loop {
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
                                while entry_ptr.next(reader.bucket(), &barrier) {
                                    let (k, v) = entry_ptr.get(data_block);
                                    if pred(k, v) {
                                        // Found one entry satisfying the predicate.
                                        return true;
                                    }
                                }
                                break false;
                            }

                            // The bucket having been killed means that a new array has been
                            // allocated.
                            break true;
                        };
                    }
                    async_wait_pinned.await;
                };
                if killed {
                    break;
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

        false
    }

    /// Iterates over all the entries in the [`HashMap`].
    ///
    /// This method allows modifying each value.
    ///
    /// Key-value pairs that have existed since the invocation of the method are guaranteed to be
    /// visited if they are not removed, however the same key-value pair can be visited more than
    /// once if the [`HashMap`] gets resized by another thread.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.insert(2, 1).is_ok());
    ///
    /// let mut acc = 0;
    /// hashmap.for_each(|k, v| { acc += *k; *v = 2; });
    /// assert_eq!(acc, 3);
    /// assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
    /// assert_eq!(hashmap.read(&2, |_, v| *v).unwrap(), 2);
    /// ```
    #[inline]
    pub fn for_each<F: FnMut(&K, &mut V)>(&self, mut f: F) {
        self.retain(|k, v| {
            f(k, v);
            true
        });
    }

    /// Iterates over all the entries in the [`HashMap`].
    ///
    /// This method allows modifying each value.
    ///
    /// Key-value pairs that have existed since the invocation of the method are guaranteed to be
    /// visited if they are not removed, however the same key-value pair can be visited more than
    /// once if the [`HashMap`] gets resized by another task.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// let future_insert = hashmap.insert_async(1, 0);
    /// let future_for_each = hashmap.for_each_async(|k, v| println!("{} {}", k, v));
    /// ```
    #[inline]
    pub async fn for_each_async<F: FnMut(&K, &mut V)>(&self, mut f: F) {
        self.retain_async(|k, v| {
            f(k, v);
            true
        })
        .await;
    }

    /// Retains key-value pairs that satisfy the given predicate.
    ///
    /// This method allows modifying each value.
    ///
    /// Key-value pairs that have existed since the invocation of the method are guaranteed to be
    /// visited if they are not removed, however the same key-value pair can be visited more than
    /// once if the [`HashMap`] gets resized by another thread.
    ///
    /// It returns the number of entries remaining and removed.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.insert(2, 1).is_ok());
    /// assert!(hashmap.insert(3, 2).is_ok());
    ///
    /// assert_eq!(hashmap.retain(|k, v| *k == 1 && *v == 0), (1, 2));
    /// ```
    #[inline]
    pub fn retain<F: FnMut(&K, &mut V) -> bool>(&self, mut filter: F) -> (usize, usize) {
        let mut num_retained: usize = 0;
        let mut num_removed: usize = 0;

        let barrier = Barrier::new();

        // An acquire fence is required to correctly load the contents of the array.
        let mut current_array_ptr = self.array.load(Acquire, &barrier);
        while let Some(current_array) = current_array_ptr.as_ref() {
            while !current_array.old_array(&barrier).is_null() {
                if self.partial_rehash::<_, _, false>(current_array, &mut (), &barrier) == Ok(true)
                {
                    break;
                }
            }
            debug_assert!(current_array.old_array(&barrier).is_null());

            for index in 0..current_array.num_buckets() {
                let bucket = current_array.bucket_mut(index);
                if let Some(mut locker) = Locker::lock(bucket, &barrier) {
                    let data_block = current_array.data_block(index);
                    let mut entry_ptr = EntryPtr::new(&barrier);
                    while entry_ptr.next(locker.bucket(), &barrier) {
                        let (k, v) = entry_ptr.get_mut(data_block, &mut locker);
                        if filter(k, v) {
                            num_retained = num_retained.saturating_add(1);
                        } else {
                            locker.erase(data_block, &mut entry_ptr);
                            num_removed = num_removed.saturating_add(1);
                        }
                    }
                }
            }

            let new_current_array_ptr = self.array.load(Acquire, &barrier);
            if current_array_ptr == new_current_array_ptr {
                break;
            }
            num_retained = 0;
            current_array_ptr = new_current_array_ptr;
        }

        if num_removed >= num_retained {
            self.resize(&barrier);
        }

        (num_retained, num_removed)
    }

    /// Retains key-value pairs that satisfy the given predicate.
    ///
    /// This method allows modifying each value.
    ///
    /// Key-value pairs that have existed since the invocation of the method are guaranteed to be
    /// visited if they are not removed, however the same key-value pair can be visited more than
    /// once if the [`HashMap`] gets resized by another task.
    ///
    /// It returns the number of entries remaining and removed. It is an asynchronous method
    /// returning an `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// let future_insert = hashmap.insert_async(1, 0);
    /// let future_retain = hashmap.retain_async(|k, v| *k == 1);
    /// ```
    #[inline]
    pub async fn retain_async<F: FnMut(&K, &mut V) -> bool>(
        &self,
        mut filter: F,
    ) -> (usize, usize) {
        let mut num_retained: usize = 0;
        let mut num_removed: usize = 0;

        // An acquire fence is required to correctly load the contents of the array.
        let mut current_array_holder = self.array.get_arc(Acquire, &Barrier::new());
        while let Some(current_array) = current_array_holder.take() {
            while !current_array.old_array(&Barrier::new()).is_null() {
                let mut async_wait = AsyncWait::default();
                let mut async_wait_pinned = Pin::new(&mut async_wait);
                if self.partial_rehash::<_, _, false>(
                    &current_array,
                    &mut async_wait_pinned,
                    &Barrier::new(),
                ) == Ok(true)
                {
                    break;
                }
                async_wait_pinned.await;
            }
            debug_assert!(current_array.old_array(&Barrier::new()).is_null());

            for index in 0..current_array.num_buckets() {
                let killed = loop {
                    let mut async_wait = AsyncWait::default();
                    let mut async_wait_pinned = Pin::new(&mut async_wait);
                    {
                        let barrier = Barrier::new();
                        let bucket = current_array.bucket_mut(index);
                        if let Ok(locker) =
                            Locker::try_lock_or_wait(bucket, &mut async_wait_pinned, &barrier)
                        {
                            if let Some(mut locker) = locker {
                                let data_block = current_array.data_block(index);
                                let mut entry_ptr = EntryPtr::new(&barrier);
                                while entry_ptr.next(locker.bucket(), &barrier) {
                                    let (k, v) = entry_ptr.get_mut(data_block, &mut locker);
                                    if filter(k, v) {
                                        num_retained = num_retained.saturating_add(1);
                                    } else {
                                        locker.erase(data_block, &mut entry_ptr);
                                        num_removed = num_removed.saturating_add(1);
                                    }
                                }
                                break false;
                            }

                            // The bucket having been killed means that a new array has been
                            // allocated.
                            break true;
                        };
                    }
                    async_wait_pinned.await;
                };
                if killed {
                    break;
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
            self.resize(&Barrier::new());
        }

        (num_retained, num_removed)
    }

    /// Clears all the key-value pairs.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert_eq!(hashmap.clear(), 1);
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
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// let future_insert = hashmap.insert_async(1, 0);
    /// let future_clear = hashmap.clear_async();
    /// ```
    #[inline]
    pub async fn clear_async(&self) -> usize {
        self.retain_async(|_, _| false).await.1
    }

    /// Returns the number of entries in the [`HashMap`].
    ///
    /// It scans the entire bucket array to calculate the number of valid entries, making its time
    /// complexity `O(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert_eq!(hashmap.len(), 1);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        self.num_entries(&Barrier::new())
    }

    /// Returns `true` if the [`HashMap`] is empty.
    ///
    /// It may scan the entire bucket array to check if it is empty, therefore the time complexity
    /// is `O(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.is_empty());
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(!hashmap.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        !self.has_entry(&Barrier::new())
    }

    /// Returns the capacity of the [`HashMap`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashmap: HashMap<u64, u32, RandomState> = HashMap::with_capacity(1000000);
    /// assert_eq!(hashmap.capacity(), 1048576);
    /// ```
    #[inline]
    pub fn capacity(&self) -> usize {
        self.num_slots(&Barrier::new())
    }

    /// Returns a reference to the specified [`Barrier`] whose lifetime matches that of `self`.
    fn prolonged_barrier_ref<'h>(&'h self, barrier: &Barrier) -> &'h Barrier {
        let _: &HashMap<_, _, _> = self;
        unsafe { std::mem::transmute::<&Barrier, &'h Barrier>(barrier) }
    }
}

impl<K, V, H> Clone for HashMap<K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher + Clone,
{
    #[inline]
    fn clone(&self) -> Self {
        let cloned = Self::with_capacity_and_hasher(self.capacity(), self.hasher().clone());
        self.scan(|k, v| {
            let _reuslt = cloned.insert(k.clone(), v.clone());
        });
        cloned
    }
}

impl<K, V, H> Debug for HashMap<K, V, H>
where
    K: Debug + Eq + Hash + Sync,
    V: Debug + Sync,
    H: BuildHasher,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_map();
        self.scan(|k, v| {
            d.entry(k, v);
        });
        d.finish()
    }
}

impl<K, V> HashMap<K, V, RandomState>
where
    K: Eq + Hash + Sync,
    V: Sync,
{
    /// Creates an empty default [`HashMap`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::new();
    ///
    /// let result = hashmap.capacity();
    /// assert_eq!(result, 64);
    /// ```
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty [`HashMap`] with the specified capacity.
    ///
    /// The actual capacity is equal to or greater than the specified capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashmap: HashMap<u64, u32, RandomState> = HashMap::with_capacity(1000);
    ///
    /// let result = hashmap.capacity();
    /// assert_eq!(result, 1024);
    /// ```
    #[inline]
    #[must_use]
    pub fn with_capacity(capacity: usize) -> HashMap<K, V, RandomState> {
        let initial_capacity = capacity.max(Self::DEFAULT_CAPACITY);
        let array = unsafe {
            Arc::new_unchecked(BucketArray::<K, V, false>::new(
                initial_capacity,
                AtomicArc::null(),
            ))
        };
        let current_capacity = array.num_entries();
        HashMap {
            array: AtomicArc::from(array),
            minimum_capacity: current_capacity,
            additional_capacity: AtomicUsize::new(0),
            resize_mutex: AtomicU8::new(0),
            build_hasher: RandomState::new(),
        }
    }
}

impl<K, V> Default for HashMap<K, V, RandomState>
where
    K: Eq + Hash + Sync,
    V: Sync,
{
    /// Creates an empty default [`HashMap`].
    ///
    /// The default hash builder is [`RandomState`], and the default capacity is `64`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// let result = hashmap.capacity();
    /// assert_eq!(result, 64);
    /// ```
    #[inline]
    fn default() -> Self {
        HashMap {
            array: AtomicArc::from(unsafe {
                Arc::new_unchecked(BucketArray::<K, V, false>::new(
                    Self::DEFAULT_CAPACITY,
                    AtomicArc::null(),
                ))
            }),
            minimum_capacity: Self::DEFAULT_CAPACITY,
            additional_capacity: AtomicUsize::new(0),
            resize_mutex: AtomicU8::new(0),
            build_hasher: RandomState::new(),
        }
    }
}

impl<K, V, H> Drop for HashMap<K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
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

impl<K, V, H> HashTable<K, V, H, false> for HashMap<K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    #[inline]
    fn hasher(&self) -> &H {
        &self.build_hasher
    }
    #[inline]
    fn cloner(_: &(K, V)) -> Option<(K, V)> {
        None
    }
    #[inline]
    fn bucket_array(&self) -> &AtomicArc<BucketArray<K, V, false>> {
        &self.array
    }
    #[inline]
    fn minimum_capacity(&self) -> usize {
        self.minimum_capacity + self.additional_capacity.load(Relaxed)
    }
    #[inline]
    fn resize_mutex(&self) -> &AtomicU8 {
        &self.resize_mutex
    }
}

impl<K, V, H> PartialEq for HashMap<K, V, H>
where
    K: Eq + Hash + Sync,
    V: PartialEq + Sync,
    H: BuildHasher,
{
    /// Compares two [`HashMap`] instances.
    ///
    /// It may lead to a deadlock if the instances are being modified by another thread.
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        if !self.any(|k, v| other.read(k, |_, ov| v == ov) != Some(true)) {
            return !other.any(|k, v| self.read(k, |_, sv| v == sv) != Some(true));
        }
        false
    }
}

impl<'h, K, V, H> Entry<'h, K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    /// Ensures a value is in the entry by inserting the supplied instance if empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// hashmap.entry(3).or_insert(7);
    /// assert_eq!(hashmap.read(&3, |_, v| *v), Some(7));
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
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// hashmap.entry(19).or_insert_with(|| 5);
    /// assert_eq!(hashmap.read(&19, |_, v| *v), Some(5));
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
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// hashmap.entry(11).or_insert_with_key(|k| if *k == 11 { 7 } else { 3 });
    /// assert_eq!(hashmap.read(&11, |_, v| *v), Some(7));
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
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// assert_eq!(hashmap.entry(31).key(), &31);
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
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// hashmap.entry(37).and_modify(|v| { *v += 1 }).or_insert(47);
    /// assert_eq!(hashmap.read(&37, |_, v| *v), Some(47));
    ///
    /// hashmap.entry(37).and_modify(|v| { *v += 1 }).or_insert(3);
    /// assert_eq!(hashmap.read(&37, |_, v| *v), Some(48));
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
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let entry = hashmap.entry(11).insert_entry(17);
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
    K: Eq + Hash + Sync,
    V: Default + Sync,
    H: BuildHasher,
{
    /// Ensures a value is in the entry by inserting the default value if empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// hashmap.entry(11).or_default();
    /// assert_eq!(hashmap.read(&11, |_, v| *v), Some(0));
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
    K: Debug + Eq + Hash + Sync,
    V: Debug + Sync,
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
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    /// Gets a reference to the key in the entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert_eq!(hashmap.entry(29).or_default().key(), &29);
    /// ```
    #[inline]
    #[must_use]
    pub fn key(&self) -> &K {
        &self.entry_ptr.get(self.data_block).0
    }

    /// Takes ownership of the key and value from the [`HashMap`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    /// use scc::hash_map::Entry;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// hashmap.entry(11).or_insert(17);
    ///
    /// if let Entry::Occupied(o) = hashmap.entry(11) {
    ///     assert_eq!(o.remove_entry(), (11, 17));
    /// };
    /// ```
    #[inline]
    #[must_use]
    pub fn remove_entry(mut self) -> (K, V) {
        let entry = unsafe {
            self.locker
                .erase(self.data_block, &mut self.entry_ptr)
                .unwrap_unchecked()
        };
        if self.locker.bucket().num_entries() == 0 {
            let barrier = Barrier::new();
            if let Some(current_array) =
                self.hashmap.bucket_array().load(Acquire, &barrier).as_ref()
            {
                let bucket_index = current_array.calculate_bucket_index(self.hash);
                if bucket_index % BUCKET_LEN == 0 {
                    let hashmap = self.hashmap;
                    drop(self);
                    if let Some(current_array) =
                        hashmap.bucket_array().load(Acquire, &barrier).as_ref()
                    {
                        hashmap.try_shrink(current_array, bucket_index, &barrier);
                    }
                }
            }
        }
        entry
    }

    /// Gets a reference to the value in the entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    /// use scc::hash_map::Entry;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// hashmap.entry(19).or_insert(11);
    ///
    /// if let Entry::Occupied(o) = hashmap.entry(19) {
    ///     assert_eq!(o.get(), &11);
    /// };
    /// ```
    #[inline]
    #[must_use]
    pub fn get(&self) -> &V {
        &self.entry_ptr.get(self.data_block).1
    }

    /// Gets a mutable reference to the value in the entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    /// use scc::hash_map::Entry;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// hashmap.entry(37).or_insert(11);
    ///
    /// if let Entry::Occupied(mut o) = hashmap.entry(37) {
    ///     *o.get_mut() += 18;
    ///     assert_eq!(*o.get(), 29);
    /// }
    ///
    /// assert_eq!(hashmap.read(&37, |_, v| *v), Some(29));
    /// ```
    #[inline]
    pub fn get_mut(&mut self) -> &mut V {
        &mut self.entry_ptr.get_mut(self.data_block, &mut self.locker).1
    }

    /// Sets the value of the entry, and returns the old value.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    /// use scc::hash_map::Entry;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// hashmap.entry(37).or_insert(11);
    ///
    /// if let Entry::Occupied(mut o) = hashmap.entry(37) {
    ///     assert_eq!(o.insert(17), 11);
    /// }
    ///
    /// assert_eq!(hashmap.read(&37, |_, v| *v), Some(17));
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
    /// use scc::HashMap;
    /// use scc::hash_map::Entry;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// hashmap.entry(11).or_insert(17);
    ///
    /// if let Entry::Occupied(o) = hashmap.entry(11) {
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
    K: Debug + Eq + Hash + Sync,
    V: Debug + Sync,
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

impl<'h, K, V, H> VacantEntry<'h, K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    /// Gets a reference to the key.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// assert_eq!(hashmap.entry(11).key(), &11);
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
    /// use scc::HashMap;
    /// use scc::hash_map::Entry;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// if let Entry::Vacant(v) = hashmap.entry(17) {
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
    /// use scc::HashMap;
    /// use scc::hash_map::Entry;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// if let Entry::Vacant(o) = hashmap.entry(19) {
    ///     o.insert_entry(29);
    /// }
    /// assert_eq!(hashmap.read(&19, |_, v| *v), Some(29));
    /// ```
    #[inline]
    pub fn insert_entry(mut self, val: V) -> OccupiedEntry<'h, K, V, H> {
        let barrier = Barrier::new();
        let entry_ptr = self.locker.insert_with(
            self.data_block,
            BucketArray::<K, V, false>::partial_hash(self.hash),
            || (self.key, val),
            self.hashmap.prolonged_barrier_ref(&barrier),
        );
        OccupiedEntry {
            hashmap: self.hashmap,
            hash: self.hash,
            data_block: self.data_block,
            locker: self.locker,
            entry_ptr,
        }
    }
}

impl<'h, K, V, H> Debug for VacantEntry<'h, K, V, H>
where
    K: Debug + Eq + Hash + Sync,
    V: Debug + Sync,
    H: BuildHasher,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("VacantEntry").field(self.key()).finish()
    }
}

impl<'h, K, V, H> Drop for Ticket<'h, K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    #[inline]
    fn drop(&mut self) {
        let result = self
            .hashmap
            .additional_capacity
            .fetch_sub(self.increment, Relaxed);
        self.hashmap.resize(&Barrier::new());
        debug_assert!(result >= self.increment);
    }
}
