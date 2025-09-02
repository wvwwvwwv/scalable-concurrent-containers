//! [`HashMap`] is a concurrent and asynchronous hash map.

use std::collections::hash_map::RandomState;
use std::fmt::{self, Debug};
use std::hash::{BuildHasher, Hash};
use std::mem::replace;
use std::ops::{Deref, DerefMut, RangeInclusive};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed};

use sdd::{AtomicShared, Guard, Shared, Tag};

use super::Equivalent;
use super::hash_table::bucket::{EntryPtr, SEQUENTIAL};
use super::hash_table::bucket_array::BucketArray;
use super::hash_table::{HashTable, LockedEntry};
use crate::async_helper::SendableGuard;
use crate::hash_table::bucket::{BUCKET_LEN, DataBlock, Writer};

/// Scalable concurrent hash map.
///
/// [`HashMap`] is a concurrent and asynchronous hash map data structure optimized for highly
/// concurrent workloads. [`HashMap`] has a dynamically sized array of buckets where a bucket is a
/// fixed-size hash table with linear probing that can be expanded by allocating a linked list of
/// smaller buckets when it is full.
///
/// ## The key features of [`HashMap`]
///
/// * Non-sharded: the data is stored in a single array of entry buckets.
/// * Non-blocking resizing: resizing does not block other threads or tasks.
/// * Automatic resizing: it automatically grows or shrinks.
/// * Incremental resizing: entries in the old bucket array are incrementally relocated.
/// * No busy waiting: no spin-locks or hot loops to wait for desired resources.
/// * Linearizability: [`HashMap`] manipulation methods are linearizable.
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
/// Bucket arrays are protected by [`sdd`], thus allowing lock-free access to them.
///
/// ### Entry access
///
/// Each read/write access to an entry is serialized by the read-write lock in the bucket containing
/// the entry. There are no container-level locks, therefore, the larger the [`HashMap`] gets, the
/// lower the chance that the bucket-level lock is being contended.
///
/// ### Resize
///
/// Resizing of the [`HashMap`] is non-blocking and lock-free; resizing does not block any other
/// read/write access to the [`HashMap`] or resizing attempts. Resizing is analogous to pushing a
/// new bucket array into a lock-free stack. Each entry in the old bucket array will be
/// incrementally relocated to the new bucket array on future access to the [`HashMap`], and the old
/// bucket array gets dropped when it becomes empty and unreachable.
///
/// ### Blocking methods in an asynchronous code block
///
/// It is generally not recommended to use blocking methods, such as [`HashMap::insert`], in an
/// asynchronous code block or [`poll`](std::future::Future::poll), since it may lead to deadlocks
/// or performance degradation.
///
/// ## Unwind safety
///
/// [`HashMap`] is impervious to out-of-memory errors and panics in user-specified code on one
/// condition; `H::Hasher::hash`, `K::drop` and `V::drop` must not panic.
pub struct HashMap<K, V, H = RandomState>
where
    H: BuildHasher,
{
    bucket_array: AtomicShared<BucketArray<K, V, (), SEQUENTIAL>>,
    minimum_capacity: AtomicUsize,
    build_hasher: H,
}

/// [`Entry`] represents a single entry in a [`HashMap`].
pub enum Entry<'h, K, V, H = RandomState>
where
    H: BuildHasher,
{
    /// An occupied entry.
    Occupied(OccupiedEntry<'h, K, V, H>),

    /// A vacant entry.
    Vacant(VacantEntry<'h, K, V, H>),
}

/// [`OccupiedEntry`] is a view into an occupied entry in a [`HashMap`].
pub struct OccupiedEntry<'h, K, V, H = RandomState>
where
    H: BuildHasher,
{
    hashmap: &'h HashMap<K, V, H>,
    locked_entry: LockedEntry<'h, K, V, (), SEQUENTIAL>,
}

/// [`VacantEntry`] is a view into a vacant entry in a [`HashMap`].
pub struct VacantEntry<'h, K, V, H = RandomState>
where
    H: BuildHasher,
{
    hashmap: &'h HashMap<K, V, H>,
    key: K,
    hash: u64,
    locked_entry: LockedEntry<'h, K, V, (), SEQUENTIAL>,
}

/// [`ConsumableEntry`] is a view into an occupied entry in a [`HashMap`] when iterating over
/// entries in it.
pub struct ConsumableEntry<'g, K, V> {
    /// Holds an exclusive lock on the entry bucket.
    writer: &'g Writer<'g, K, V, (), SEQUENTIAL>,
    /// Reference to the entry data.
    data_block: &'g DataBlock<K, V, BUCKET_LEN>,
    /// Pointer to the entry.
    entry_ptr: EntryPtr<'g, K, V, SEQUENTIAL>,
    /// Probes removal.
    remove_probe: &'g mut bool,
    /// Associated [`Guard`].
    guard: &'g Guard,
}

/// [`Reserve`] keeps the capacity of the associated [`HashMap`] higher than a certain level.
///
/// The [`HashMap`] does not shrink the capacity below the reserved capacity.
pub struct Reserve<'h, K, V, H = RandomState>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    hashmap: &'h HashMap<K, V, H>,
    additional: usize,
}

impl<K, V, H> HashMap<K, V, H>
where
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
    #[cfg(not(feature = "loom"))]
    #[inline]
    pub const fn with_hasher(build_hasher: H) -> Self {
        Self {
            bucket_array: AtomicShared::null(),
            minimum_capacity: AtomicUsize::new(0),
            build_hasher,
        }
    }

    /// Creates an empty [`HashMap`] with the given [`BuildHasher`].
    #[cfg(feature = "loom")]
    #[inline]
    pub fn with_hasher(build_hasher: H) -> Self {
        Self {
            bucket_array: AtomicShared::null(),
            minimum_capacity: AtomicUsize::new(0),
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
    pub fn with_capacity_and_hasher(capacity: usize, build_hasher: H) -> Self {
        let (array, minimum_capacity) = if capacity == 0 {
            (AtomicShared::null(), AtomicUsize::new(0))
        } else {
            let array = unsafe {
                Shared::new_unchecked(BucketArray::<K, V, (), SEQUENTIAL>::new(
                    capacity,
                    AtomicShared::null(),
                ))
            };
            let minimum_capacity = array.num_slots();
            (
                AtomicShared::from(array),
                AtomicUsize::new(minimum_capacity),
            )
        };
        Self {
            bucket_array: array,
            minimum_capacity,
            build_hasher,
        }
    }
}

impl<K, V, H> HashMap<K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    /// Temporarily increases the minimum capacity of the [`HashMap`].
    ///
    /// A [`Reserve`] is returned if the [`HashMap`] could increase the minimum capacity while the
    /// increased capacity is not exclusively owned by the returned [`Reserve`], allowing others to
    /// benefit from it. The memory for the additional space may not be immediately allocated if
    /// the [`HashMap`] is empty or currently being resized, however once the memory is reserved
    /// eventually, the capacity will not shrink below the additional capacity until the returned
    /// [`Reserve`] is dropped.
    ///
    /// # Errors
    ///
    /// Returns `None` if a too large number is given.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<usize, usize> = HashMap::with_capacity(1000);
    /// assert_eq!(hashmap.capacity(), 1024);
    ///
    /// let reserved = hashmap.reserve(10000);
    /// assert!(reserved.is_some());
    /// assert_eq!(hashmap.capacity(), 16384);
    ///
    /// assert!(hashmap.reserve(usize::MAX).is_none());
    /// assert_eq!(hashmap.capacity(), 16384);
    ///
    /// for i in 0..16 {
    ///     assert!(hashmap.insert(i, i).is_ok());
    /// }
    /// drop(reserved);
    ///
    /// assert_eq!(hashmap.capacity(), 1024);
    /// ```
    #[inline]
    pub fn reserve(&self, additional_capacity: usize) -> Option<Reserve<'_, K, V, H>> {
        let additional = self.reserve_capacity(additional_capacity);
        if additional == 0 {
            None
        } else {
            Some(Reserve {
                hashmap: self,
                additional,
            })
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
    pub fn entry(&self, key: K) -> Entry<'_, K, V, H> {
        let hash = self.hash(&key);
        let guard = Guard::new();
        self.writer_sync_with(hash, &guard, |writer, data_block, index, len| {
            let entry_ptr = writer.get_entry_ptr(data_block, &key, hash, &guard);
            let locked_entry =
                LockedEntry::new(writer, data_block, entry_ptr.clone(), index, len, &guard)
                    .prolong_lifetime(self);
            if entry_ptr.is_valid() {
                Entry::Occupied(OccupiedEntry {
                    hashmap: self,
                    locked_entry,
                })
            } else {
                let vacant_entry = VacantEntry {
                    hashmap: self,
                    key,
                    hash,
                    locked_entry,
                };
                Entry::Vacant(vacant_entry)
            }
        })
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
    pub async fn entry_async(&self, key: K) -> Entry<'_, K, V, H> {
        let hash = self.hash(&key);
        let sendable_guard = SendableGuard::default();
        self.writer_async_with(hash, &sendable_guard, |writer, data_block, index, len| {
            let guard = sendable_guard.guard();
            let entry_ptr = writer.get_entry_ptr(data_block, &key, hash, guard);
            let locked_entry =
                LockedEntry::new(writer, data_block, entry_ptr.clone(), index, len, guard)
                    .prolong_lifetime(self);
            if entry_ptr.is_valid() {
                Entry::Occupied(OccupiedEntry {
                    hashmap: self,
                    locked_entry,
                })
            } else {
                let vacant_entry = VacantEntry {
                    hashmap: self,
                    key,
                    hash,
                    locked_entry,
                };
                Entry::Vacant(vacant_entry)
            }
        })
        .await
    }

    /// Tries to get the entry associated with the given key in the map for in-place manipulation.
    ///
    /// Returns `None` if the entry could not be locked.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<usize, usize> = HashMap::default();
    ///
    /// assert!(hashmap.insert(0, 1).is_ok());
    /// assert!(hashmap.try_entry(0).is_some());
    /// ```
    #[inline]
    pub fn try_entry(&self, key: K) -> Option<Entry<'_, K, V, H>> {
        let hash = self.hash(&key);
        let locked_entry =
            self.try_reserve_entry(&key, hash, self.prolonged_guard_ref(&Guard::new()))?;
        if locked_entry.entry_ptr.is_valid() {
            Some(Entry::Occupied(OccupiedEntry {
                hashmap: self,
                locked_entry,
            }))
        } else {
            Some(Entry::Vacant(VacantEntry {
                hashmap: self,
                key,
                hash,
                locked_entry,
            }))
        }
    }

    /// Gets the first occupied entry for in-place manipulation.
    ///
    /// The returned [`OccupiedEntry`] in combination with [`OccupiedEntry::next`] or
    /// [`OccupiedEntry::next_async`] can act as a mutable iterator over entries.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    ///
    /// let mut first_entry = hashmap.first_entry().unwrap();
    /// *first_entry.get_mut() = 2;
    ///
    /// assert!(first_entry.next().is_none());
    /// assert_eq!(hashmap.read(&1, |_, v| *v), Some(2));
    /// ```
    #[inline]
    pub fn first_entry(&self) -> Option<OccupiedEntry<'_, K, V, H>> {
        self.any_entry(|_, _| true)
    }

    /// Gets the first occupied entry for in-place manipulation.
    ///
    /// The returned [`OccupiedEntry`] in combination with [`OccupiedEntry::next`] or
    /// [`OccupiedEntry::next_async`] can act as a mutable iterator over entries.
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
    /// let future_entry = hashmap.first_entry_async();
    /// ```
    #[inline]
    pub async fn first_entry_async(&self) -> Option<OccupiedEntry<'_, K, V, H>> {
        self.any_entry_async(|_, _| true).await
    }

    /// Finds any entry satisfying the supplied predicate for in-place manipulation.
    ///
    /// The returned [`OccupiedEntry`] in combination with [`OccupiedEntry::next`] or
    /// [`OccupiedEntry::next_async`] can act as a mutable iterator over entries.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.insert(2, 3).is_ok());
    ///
    /// let mut entry = hashmap.any_entry(|k, _| *k == 2).unwrap();
    /// assert_eq!(*entry.get(), 3);
    /// ```
    #[inline]
    pub fn any_entry<P: FnMut(&K, &V) -> bool>(
        &self,
        mut pred: P,
    ) -> Option<OccupiedEntry<'_, K, V, H>> {
        let mut entry = None;
        let guard = Guard::new();
        self.for_each_writer_sync_with(0, 0, &guard, |writer, data_block, index, len| {
            let mut entry_ptr = EntryPtr::new(&guard);
            while entry_ptr.move_to_next(&writer, &guard) {
                let (k, v) = entry_ptr.get(data_block);
                if pred(k, v) {
                    let locked_entry =
                        LockedEntry::new(writer, data_block, entry_ptr, index, len, &guard)
                            .prolong_lifetime(self);
                    entry = Some(OccupiedEntry {
                        hashmap: self,
                        locked_entry,
                    });
                    return (true, false);
                }
            }
            (false, false)
        });
        entry
    }

    /// Finds any entry satisfying the supplied predicate for in-place manipulation.
    ///
    /// The returned [`OccupiedEntry`] in combination with [`OccupiedEntry::next`] or
    /// [`OccupiedEntry::next_async`] can act as a mutable iterator over entries.
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
    /// let future_entry = hashmap.any_entry_async(|k, _| *k == 2);
    /// ```
    #[inline]
    pub async fn any_entry_async<P: FnMut(&K, &V) -> bool>(
        &self,
        mut pred: P,
    ) -> Option<OccupiedEntry<'_, K, V, H>> {
        let mut entry = None;
        let sendable_guard = SendableGuard::default();
        self.for_each_writer_async_with(0, 0, &sendable_guard, |writer, data_block, index, len| {
            let guard = sendable_guard.guard();
            let mut entry_ptr = EntryPtr::new(guard);
            while entry_ptr.move_to_next(&writer, guard) {
                let (k, v) = entry_ptr.get(data_block);
                if pred(k, v) {
                    let locked_entry =
                        LockedEntry::new(writer, data_block, entry_ptr, index, len, guard)
                            .prolong_lifetime(self);
                    entry = Some(OccupiedEntry {
                        hashmap: self,
                        locked_entry,
                    });
                    return (true, false);
                }
            }
            (false, false)
        })
        .await;
        entry
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
        let hash = self.hash(&key);
        let guard = Guard::new();
        self.writer_sync_with(hash, &guard, |writer, data_block, _, _| {
            if writer
                .get_entry_ptr(data_block, &key, hash, &guard)
                .is_valid()
            {
                Err((key, val))
            } else {
                writer.insert_with(data_block, hash, || (key, val), &guard);
                Ok(())
            }
        })
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
    pub async fn insert_async(&self, key: K, val: V) -> Result<(), (K, V)> {
        let hash = self.hash(&key);
        let sendable_guard = SendableGuard::default();
        self.writer_async_with(hash, &sendable_guard, |writer, data_block, _, _| {
            let guard = sendable_guard.guard();
            if writer
                .get_entry_ptr(data_block, &key, hash, guard)
                .is_valid()
            {
                Err((key, val))
            } else {
                writer.insert_with(data_block, hash, || (key, val), guard);
                Ok(())
            }
        })
        .await
    }

    /// Upserts a key-value pair into the [`HashMap`].
    ///
    /// Returns the old value if the [`HashMap`] has this key present, or returns `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.upsert(1, 0).is_none());
    /// assert_eq!(hashmap.upsert(1, 1).unwrap(), 0);
    /// assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 1);
    /// ```
    #[inline]
    pub fn upsert(&self, key: K, val: V) -> Option<V> {
        match self.entry(key) {
            Entry::Occupied(mut o) => Some(replace(o.get_mut(), val)),
            Entry::Vacant(v) => {
                v.insert_entry(val);
                None
            }
        }
    }

    /// Upserts a key-value pair into the [`HashMap`].
    ///
    /// Returns the old value if the [`HashMap`] has this key present, or returns `None`. It is an
    /// asynchronous method returning an `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_upsert = hashmap.upsert_async(11, 17);
    /// ```
    #[inline]
    pub async fn upsert_async(&self, key: K, val: V) -> Option<V> {
        match self.entry_async(key).await {
            Entry::Occupied(mut o) => Some(replace(o.get_mut(), val)),
            Entry::Vacant(v) => {
                v.insert_entry(val);
                None
            }
        }
    }

    /// Updates an existing key-value pair in-place.
    ///
    /// Returns `None` if the key does not exist.
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
        Q: Equivalent<K> + Hash + ?Sized,
        U: FnOnce(&K, &mut V) -> R,
    {
        let hash = self.hash(key);
        let guard = Guard::default();
        self.optional_writer_sync_with(hash, &guard, |writer, data_block, _, _| {
            let mut entry_ptr = writer.get_entry_ptr(data_block, key, hash, &guard);
            if entry_ptr.is_valid() {
                let (k, v) = entry_ptr.get_mut(data_block, &writer);
                (Some(updater(k, v)), false)
            } else {
                (None, false)
            }
        })
        .ok()
        .flatten()
    }

    /// Updates an existing key-value pair in-place.
    ///
    /// Returns `None` if the key does not exist. It is an asynchronous method returning an
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
        Q: Equivalent<K> + Hash + ?Sized,
        U: FnOnce(&K, &mut V) -> R,
    {
        let hash = self.hash(key);
        let sendable_guard = SendableGuard::default();
        self.optional_writer_async_with(hash, &sendable_guard, |writer, data_block, _, _| {
            let guard = sendable_guard.guard();
            let mut entry_ptr = writer.get_entry_ptr(data_block, key, hash, guard);
            if entry_ptr.is_valid() {
                let (k, v) = entry_ptr.get_mut(data_block, &writer);
                (Some(updater(k, v)), false)
            } else {
                (None, false)
            }
        })
        .await
        .ok()
        .flatten()
    }

    /// Removes a key-value pair if the key exists.
    ///
    /// Returns `None` if the key does not exist.
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
        Q: Equivalent<K> + Hash + ?Sized,
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
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert_async(11, 17);
    /// let future_remove = hashmap.remove_async(&11);
    /// ```
    #[inline]
    pub async fn remove_async<Q>(&self, key: &Q) -> Option<(K, V)>
    where
        Q: Equivalent<K> + Hash + ?Sized,
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
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.remove_if(&1, |v| { *v += 1; false }).is_none());
    /// assert_eq!(hashmap.remove_if(&1, |v| *v == 1).unwrap(), (1, 1));
    /// ```
    #[inline]
    pub fn remove_if<Q, F: FnOnce(&mut V) -> bool>(&self, key: &Q, condition: F) -> Option<(K, V)>
    where
        Q: Equivalent<K> + Hash + ?Sized,
    {
        let hash = self.hash(key);
        let guard = Guard::default();
        self.optional_writer_sync_with(hash, &guard, |writer, data_block, _, _| {
            let mut entry_ptr = writer.get_entry_ptr(data_block, key, hash, &guard);
            if entry_ptr.is_valid() && condition(&mut entry_ptr.get_mut(data_block, &writer).1) {
                (
                    Some(writer.remove(data_block, &mut entry_ptr, &guard)),
                    writer.len() <= 1,
                )
            } else {
                (None, false)
            }
        })
        .ok()
        .flatten()
    }

    /// Removes a key-value pair if the key exists and the given condition is met.
    ///
    /// Returns `None` if the key does not exist or the condition was not met. It is an
    /// asynchronous method returning an `impl Future` for the caller to await.
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
    pub async fn remove_if_async<Q, F: FnOnce(&mut V) -> bool>(
        &self,
        key: &Q,
        condition: F,
    ) -> Option<(K, V)>
    where
        Q: Equivalent<K> + Hash + ?Sized,
    {
        let hash = self.hash(key);
        let sendable_guard = SendableGuard::default();
        self.optional_writer_async_with(hash, &sendable_guard, |writer, data_block, _, _| {
            let mut entry_ptr = writer.get_entry_ptr(data_block, key, hash, sendable_guard.guard());
            if entry_ptr.is_valid() && condition(&mut entry_ptr.get_mut(data_block, &writer).1) {
                (
                    Some(writer.remove(data_block, &mut entry_ptr, sendable_guard.guard())),
                    writer.len() <= 1,
                )
            } else {
                (None, false)
            }
        })
        .await
        .ok()
        .flatten()
    }

    /// Gets an [`OccupiedEntry`] corresponding to the key for in-place modification.
    ///
    /// [`OccupiedEntry`] exclusively owns the entry, preventing others from gaining access to it:
    /// use [`read`](Self::read) if read-only access is sufficient.
    ///
    /// Returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.get(&1).is_none());
    /// assert!(hashmap.insert(1, 10).is_ok());
    /// assert_eq!(*hashmap.get(&1).unwrap().get(), 10);
    ///
    /// *hashmap.get(&1).unwrap() = 11;
    /// assert_eq!(*hashmap.get(&1).unwrap(), 11);
    /// ```
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<OccupiedEntry<'_, K, V, H>>
    where
        Q: Equivalent<K> + Hash + ?Sized,
    {
        let hash = self.hash(key);
        let guard = Guard::default();
        self.optional_writer_sync_with(hash, &guard, |writer, data_block, index, len| {
            let entry_ptr = writer.get_entry_ptr(data_block, key, hash, &guard);
            if entry_ptr.is_valid() {
                let locked_entry =
                    LockedEntry::new(writer, data_block, entry_ptr, index, len, &guard)
                        .prolong_lifetime(self);
                return (
                    Some(OccupiedEntry {
                        hashmap: self,
                        locked_entry,
                    }),
                    false,
                );
            }
            (None, false)
        })
        .ok()
        .flatten()
    }

    /// Gets an [`OccupiedEntry`] corresponding to the key for in-place modification.
    ///
    /// [`OccupiedEntry`] exclusively owns the entry, preventing others from gaining access to it:
    /// use [`read_async`](Self::read_async) if read-only access is sufficient.
    ///
    /// Returns `None` if the key does not exist. It is an asynchronous method returning an
    /// `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert_async(11, 17);
    /// let future_get = hashmap.get_async(&11);
    /// ```
    #[inline]
    pub async fn get_async<Q>(&self, key: &Q) -> Option<OccupiedEntry<'_, K, V, H>>
    where
        Q: Equivalent<K> + Hash + ?Sized,
    {
        let hash = self.hash(key);
        let sendable_guard = SendableGuard::default();
        self.optional_writer_async_with(hash, &sendable_guard, |writer, data_block, index, len| {
            let guard = sendable_guard.guard();
            let entry_ptr = writer.get_entry_ptr(data_block, key, hash, guard);
            if entry_ptr.is_valid() {
                let locked_entry =
                    LockedEntry::new(writer, data_block, entry_ptr, index, len, guard)
                        .prolong_lifetime(self);
                return (
                    Some(OccupiedEntry {
                        hashmap: self,
                        locked_entry,
                    }),
                    false,
                );
            }
            (None, false)
        })
        .await
        .ok()
        .flatten()
    }

    /// Reads a key-value pair.
    ///
    /// Returns `None` if the key does not exist.
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
        Q: Equivalent<K> + Hash + ?Sized,
    {
        let hash = self.hash(key);
        let guard = Guard::new();
        self.reader_sync_with(key, hash, reader, &guard)
    }

    /// Reads a key-value pair.
    ///
    /// Returns `None` if the key does not exist. It is an asynchronous method returning an
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
        Q: Equivalent<K> + Hash + ?Sized,
    {
        let hash = self.hash(key);
        let sendable_guard = SendableGuard::default();
        self.reader_async_with(key, hash, reader, &sendable_guard)
            .await
    }

    /// Returns `true` if the [`HashMap`] contains a value for the specified key.
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
        Q: Equivalent<K> + Hash + ?Sized,
    {
        self.read(key, |_, _| ()).is_some()
    }

    /// Returns `true` if the [`HashMap`] contains a value for the specified key.
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
        Q: Equivalent<K> + Hash + ?Sized,
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
    /// Returns `true` as soon as an entry satisfying the predicate is found.
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
        let mut found = false;
        let guard = Guard::new();
        self.for_each_writer_sync_with(0, 0, &guard, |writer, data_block, _, _| {
            let mut entry_ptr = EntryPtr::new(&guard);
            while entry_ptr.move_to_next(&writer, &guard) {
                let (k, v) = entry_ptr.get(data_block);
                if pred(k, v) {
                    // Found one entry satisfying the predicate.
                    found = true;
                    return (true, false);
                }
            }
            (false, false)
        });
        found
    }

    /// Searches for any entry that satisfies the given predicate.
    ///
    /// Key-value pairs that have existed since the invocation of the method are guaranteed to be
    /// visited if they are not removed, however the same key-value pair can be visited more than
    /// once if the [`HashMap`] gets resized by another task.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
    ///
    /// Returns `true` as soon as an entry satisfying the predicate is found.
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
        let mut found = false;
        let sendable_guard = SendableGuard::default();
        self.for_each_writer_async_with(0, 0, &sendable_guard, |writer, data_block, _, _| {
            let guard = sendable_guard.guard();
            let mut entry_ptr = EntryPtr::new(guard);
            while entry_ptr.move_to_next(&writer, guard) {
                let (k, v) = entry_ptr.get(data_block);
                if pred(k, v) {
                    // Found one entry satisfying the predicate.
                    found = true;
                    return (true, false);
                }
            }
            (false, false)
        })
        .await;
        found
    }

    /// Iterates over entries synchronously for reading entries.
    ///
    /// Stops iterating when the closure returns `false`, and this method also returns `false`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u64> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.insert(2, 1).is_ok());
    ///
    /// let mut acc = 0_u64;
    /// let result = hashmap.iter_sync(|k, v| {
    ///     acc += *k;
    ///     acc += *v;
    ///     true
    /// });
    ///
    /// assert!(result);
    /// assert_eq!(acc, 4);
    /// ```
    #[inline]
    pub fn iter_sync<F: FnMut(&K, &V) -> bool>(&self, mut f: F) -> bool {
        let mut result = true;
        let guard = Guard::new();
        self.for_each_reader_sync_with(0, 0, &guard, |reader, data_block, _, _| {
            let mut entry_ptr = EntryPtr::new(&guard);
            while entry_ptr.move_to_next(&reader, &guard) {
                let (k, v) = entry_ptr.get(data_block);
                if !f(k, v) {
                    result = false;
                    return false;
                }
            }
            true
        });
        result
    }

    /// Iterates over entries synchronously for updating entries.
    ///
    /// Stops iterating when the closure returns `false`, and this method also returns `false`.
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
    /// let result = hashmap.iter_mut_sync(|entry| {
    ///     if entry.0 == 1 {
    ///         entry.consume();
    ///         return false;
    ///     }
    ///     true
    /// });
    ///
    /// assert!(!result);
    /// assert!(!hashmap.contains(&1));
    /// assert_eq!(hashmap.len(), 2);
    /// ```
    #[inline]
    pub fn iter_mut_sync<F: FnMut(ConsumableEntry<'_, K, V>) -> bool>(&self, mut f: F) -> bool {
        let mut result = true;
        let guard = Guard::new();
        self.for_each_writer_sync_with(0, 0, &guard, |writer, data_block, _, _| {
            let mut removed = false;
            let mut entry_ptr = EntryPtr::new(&guard);
            while entry_ptr.move_to_next(&writer, &guard) {
                let consumable_entry = ConsumableEntry {
                    writer: &writer,
                    data_block,
                    entry_ptr: entry_ptr.clone(),
                    remove_probe: &mut removed,
                    guard: &guard,
                };
                if !f(consumable_entry) {
                    result = false;
                    return (true, removed);
                }
            }
            (false, removed)
        });
        result
    }

    /// Retains the entries specified by the predicate.
    ///
    /// This method allows the predicate closure to modify the value field.
    ///
    /// Entries that have existed since the invocation of the method are guaranteed to be visited
    /// if they are not removed, however the same entry can be visited more than once if the
    /// [`HashMap`] gets resized by another thread.
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
    /// hashmap.retain(|k, v| *k == 1 && *v == 0);
    ///
    /// assert!(hashmap.contains(&1));
    /// assert!(!hashmap.contains(&2));
    /// assert!(!hashmap.contains(&3));
    /// ```
    #[inline]
    pub fn retain<F: FnMut(&K, &mut V) -> bool>(&self, mut pred: F) {
        let guard = Guard::new();
        self.for_each_writer_sync_with(0, 0, &guard, |writer, data_block, _, _| {
            let mut removed = false;
            let mut entry_ptr = EntryPtr::new(&guard);
            while entry_ptr.move_to_next(&writer, &guard) {
                let (k, v) = entry_ptr.get_mut(data_block, &writer);
                if !pred(k, v) {
                    writer.remove(data_block, &mut entry_ptr, &guard);
                    removed = true;
                }
            }
            (false, removed)
        });
    }

    /// Retains the entries specified by the predicate.
    ///
    /// This method allows the predicate closure to modify the value field.
    ///
    /// Entries that have existed since the invocation of the method are guaranteed to be visited
    /// if they are not removed, however the same entry can be visited more than once if the
    /// [`HashMap`] gets resized by another thread.
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
    /// let future_retain = hashmap.retain_async(|k, v| *k == 1);
    /// ```
    #[inline]
    pub async fn retain_async<F: FnMut(&K, &mut V) -> bool>(&self, mut pred: F) {
        let sendable_guard = SendableGuard::default();
        self.for_each_writer_async_with(0, 0, &sendable_guard, |writer, data_block, _, _| {
            let mut removed = false;
            let guard = sendable_guard.guard();
            let mut entry_ptr = EntryPtr::new(guard);
            while entry_ptr.move_to_next(&writer, guard) {
                let (k, v) = entry_ptr.get_mut(data_block, &writer);
                if !pred(k, v) {
                    writer.remove(data_block, &mut entry_ptr, guard);
                    removed = true;
                }
            }
            (false, removed)
        })
        .await;
    }

    /// Prunes the entries specified by the predicate.
    ///
    /// If the value is consumed by the predicate, in other words, if the predicate returns `None`,
    /// the entry is removed, otherwise the entry is retained.
    ///
    /// Entries that have existed since the invocation of the method are guaranteed to be visited
    /// if they are not removed, however the same entry can be visited more than once if the
    /// [`HashMap`] gets resized by another thread.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, String> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, String::from("1")).is_ok());
    /// assert!(hashmap.insert(2, String::from("2")).is_ok());
    /// assert!(hashmap.insert(3, String::from("3")).is_ok());
    ///
    /// hashmap.prune(|k, v| if *k == 1 { Some(v) } else { None });
    /// assert_eq!(hashmap.len(), 1);
    /// ```
    #[inline]
    pub fn prune<F: FnMut(&K, V) -> Option<V>>(&self, mut pred: F) {
        let guard = Guard::default();
        self.for_each_writer_sync_with(0, 0, &guard, |writer, data_block, _, _| {
            let mut removed = false;
            let mut entry_ptr = EntryPtr::new(&guard);
            while entry_ptr.move_to_next(&writer, &guard) {
                if writer.keep_or_consume(data_block, &mut entry_ptr, &mut pred, &guard) {
                    removed = true;
                }
            }
            (false, removed)
        });
    }

    /// Prunes the entries specified by the predicate.
    ///
    /// If the value is consumed by the predicate, in other words, if the predicate returns `None`,
    /// the entry is removed, otherwise the entry is retained.
    ///
    /// Entries that have existed since the invocation of the method are guaranteed to be visited
    /// if they are not removed, however the same entry can be visited more than once if the
    /// [`HashMap`] gets resized by another thread.
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
    /// let future_prune = hashmap.prune_async(|k, v| if *k == 1 { Some(v) } else { None });
    /// ```
    #[inline]
    pub async fn prune_async<F: FnMut(&K, V) -> Option<V>>(&self, mut pred: F) {
        let sendable_guard = SendableGuard::default();
        self.for_each_writer_async_with(0, 0, &sendable_guard, |writer, data_block, _, _| {
            let mut removed = false;
            let guard = sendable_guard.guard();
            let mut entry_ptr = EntryPtr::new(guard);
            while entry_ptr.move_to_next(&writer, guard) {
                if writer.keep_or_consume(data_block, &mut entry_ptr, &mut pred, guard) {
                    removed = true;
                }
            }
            (false, removed)
        })
        .await;
    }

    /// Clears the [`HashMap`] by removing all key-value pairs.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// hashmap.clear();
    ///
    /// assert!(!hashmap.contains(&1));
    /// ```
    #[inline]
    pub fn clear(&self) {
        self.retain(|_, _| false);
    }

    /// Clears the [`HashMap`] by removing all key-value pairs.
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
    pub async fn clear_async(&self) {
        self.retain_async(|_, _| false).await;
    }

    /// Returns the number of entries in the [`HashMap`].
    ///
    /// It reads the entire metadata area of the bucket array to calculate the number of valid
    /// entries, making its time complexity `O(N)`. Furthermore, it may overcount entries if an old
    /// bucket array has yet to be dropped.
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
        self.num_entries(&Guard::new())
    }

    /// Returns `true` if the [`HashMap`] is empty.
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
        !self.has_entry(&Guard::new())
    }

    /// Returns the capacity of the [`HashMap`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap_default: HashMap<u64, u32> = HashMap::default();
    /// assert_eq!(hashmap_default.capacity(), 0);
    ///
    /// assert!(hashmap_default.insert(1, 0).is_ok());
    /// assert_eq!(hashmap_default.capacity(), 64);
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::with_capacity(1000);
    /// assert_eq!(hashmap.capacity(), 1024);
    /// ```
    #[inline]
    pub fn capacity(&self) -> usize {
        self.num_slots(&Guard::new())
    }

    /// Returns the current capacity range of the [`HashMap`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert_eq!(hashmap.capacity_range(), 0..=(1_usize << (usize::BITS - 1)));
    ///
    /// let reserved = hashmap.reserve(1000);
    /// assert_eq!(hashmap.capacity_range(), 1000..=(1_usize << (usize::BITS - 1)));
    /// ```
    #[inline]
    pub fn capacity_range(&self) -> RangeInclusive<usize> {
        self.minimum_capacity.load(Relaxed)..=self.maximum_capacity()
    }

    /// Returns the index of the bucket that may contain the key.
    ///
    /// The method returns the index of the bucket associated with the key. The number of buckets
    /// can be calculated by dividing `32` into the capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::with_capacity(1024);
    ///
    /// let bucket_index = hashmap.bucket_index(&11);
    /// assert!(bucket_index < hashmap.capacity() / 32);
    /// ```
    #[inline]
    pub fn bucket_index<Q>(&self, key: &Q) -> usize
    where
        Q: Equivalent<K> + Hash + ?Sized,
    {
        self.calculate_bucket_index(key)
    }
}

impl<K, V> HashMap<K, V, RandomState>
where
    K: Eq + Hash,
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
    /// assert_eq!(result, 0);
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
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::with_capacity(1000);
    ///
    /// let result = hashmap.capacity();
    /// assert_eq!(result, 1024);
    /// ```
    #[inline]
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, RandomState::new())
    }
}

impl<K, V, H> Clone for HashMap<K, V, H>
where
    K: Clone + Eq + Hash,
    V: Clone,
    H: BuildHasher + Clone,
{
    #[inline]
    fn clone(&self) -> Self {
        let self_clone = Self::with_capacity_and_hasher(self.capacity(), self.hasher().clone());
        self.scan(|k, v| {
            let _result = self_clone.insert(k.clone(), v.clone());
        });
        self_clone
    }
}

impl<K, V, H> Debug for HashMap<K, V, H>
where
    K: Debug + Eq + Hash,
    V: Debug,
    H: BuildHasher,
{
    /// Iterates over all the entries in the [`HashMap`] to print them.
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

impl<K, V, H> Default for HashMap<K, V, H>
where
    H: BuildHasher + Default,
{
    /// Creates an empty default [`HashMap`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// let result = hashmap.capacity();
    /// assert_eq!(result, 0);
    /// ```
    #[inline]
    fn default() -> Self {
        Self::with_hasher(H::default())
    }
}

impl<K, V, H> Drop for HashMap<K, V, H>
where
    H: BuildHasher,
{
    #[inline]
    fn drop(&mut self) {
        self.bucket_array
            .swap((None, Tag::None), Relaxed)
            .0
            .map(|a| unsafe {
                // The entire array does not need to wait for an epoch change as no references will
                // remain outside the lifetime of the `HashMap`.
                a.drop_in_place()
            });
    }
}

impl<K, V, H> FromIterator<(K, V)> for HashMap<K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher + Default,
{
    #[inline]
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let into_iter = iter.into_iter();
        let hashmap = Self::with_capacity_and_hasher(
            Self::capacity_from_size_hint(into_iter.size_hint()),
            H::default(),
        );
        into_iter.for_each(|e| {
            hashmap.upsert(e.0, e.1);
        });
        hashmap
    }
}

impl<K, V, H> HashTable<K, V, H, (), SEQUENTIAL> for HashMap<K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    #[inline]
    fn hasher(&self) -> &H {
        &self.build_hasher
    }
    #[inline]
    fn bucket_array(&self) -> &AtomicShared<BucketArray<K, V, (), SEQUENTIAL>> {
        &self.bucket_array
    }
    #[inline]
    fn minimum_capacity(&self) -> &AtomicUsize {
        &self.minimum_capacity
    }
    #[inline]
    fn maximum_capacity(&self) -> usize {
        1_usize << (usize::BITS - 1)
    }
}

impl<K, V, H> PartialEq for HashMap<K, V, H>
where
    K: Eq + Hash,
    V: PartialEq,
    H: BuildHasher,
{
    /// Compares two [`HashMap`] instances.
    ///
    /// ## Locking behavior
    ///
    /// Shared locks on buckets are acquired when comparing two instances of [`HashMap`], therefore
    /// it may lead to a deadlock if the instances are being modified by another thread.
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
    K: Eq + Hash,
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
    K: Eq + Hash,
    V: Default,
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

impl<K, V, H> Debug for Entry<'_, K, V, H>
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
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert_eq!(hashmap.entry(29).or_default().key(), &29);
    /// ```
    #[inline]
    #[must_use]
    pub fn key(&self) -> &K {
        &self
            .locked_entry
            .entry_ptr
            .get(self.locked_entry.data_block)
            .0
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
        let guard = Guard::new();
        let entry = self.locked_entry.writer.remove(
            self.locked_entry.data_block,
            &mut self.locked_entry.entry_ptr,
            self.hashmap.prolonged_guard_ref(&guard),
        );
        let hashmap = self.hashmap;
        let index = self.locked_entry.index;
        drop(self);
        hashmap.entry_removed(index, &guard);
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
        &self
            .locked_entry
            .entry_ptr
            .get(self.locked_entry.data_block)
            .1
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
        &mut self
            .locked_entry
            .entry_ptr
            .get_mut(self.locked_entry.data_block, &self.locked_entry.writer)
            .1
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

    /// Gets the next closest occupied entry.
    ///
    /// [`HashMap::first_entry`], [`HashMap::first_entry_async`], and this method together enables
    /// the [`OccupiedEntry`] to effectively act as a mutable iterator over entries. The method
    /// never acquires more than one lock even when it searches other buckets for the next closest
    /// occupied entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    /// use scc::hash_map::Entry;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.insert(2, 0).is_ok());
    ///
    /// let first_entry = hashmap.first_entry().unwrap();
    /// let first_key = *first_entry.key();
    /// let second_entry = first_entry.next().unwrap();
    /// let second_key = *second_entry.key();
    ///
    /// assert!(second_entry.next().is_none());
    /// assert_eq!(first_key + second_key, 3);
    /// ```
    #[inline]
    #[must_use]
    pub fn next(self) -> Option<Self> {
        let hashmap = self.hashmap;
        if let Some(locked_entry) = self.locked_entry.next_sync(hashmap) {
            return Some(OccupiedEntry {
                hashmap,
                locked_entry,
            });
        }
        None
    }

    /// Gets the next closest occupied entry.
    ///
    /// [`HashMap::first_entry`], [`HashMap::first_entry_async`], and this method together enables
    /// the [`OccupiedEntry`] to effectively act as a mutable iterator over entries. The method
    /// never acquires more than one lock even when it searches other buckets for the next closest
    /// occupied entry.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    /// use scc::hash_map::Entry;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.insert(2, 0).is_ok());
    ///
    /// let second_entry_future = hashmap.first_entry().unwrap().next_async();
    /// ```
    #[inline]
    pub async fn next_async(self) -> Option<OccupiedEntry<'h, K, V, H>> {
        let hashmap = self.hashmap;
        if let Some(locked_entry) = self.locked_entry.next_async(hashmap).await {
            return Some(OccupiedEntry {
                hashmap,
                locked_entry,
            });
        }
        None
    }

    /// Gets the next closest occupied entry after removing the entry.
    ///
    /// [`HashMap::first_entry`], [`HashMap::first_entry_async`], and this method together enables
    /// the [`OccupiedEntry`] to effectively act as a mutable iterator over entries. The method
    /// never acquires more than one lock even when it searches other buckets for the next closest
    /// occupied entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    /// use scc::hash_map::Entry;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.insert(2, 0).is_ok());
    ///
    /// let first_entry = hashmap.first_entry().unwrap();
    /// let first_key = *first_entry.key();
    /// let (removed, second_entry) = first_entry.remove_next();
    /// assert_eq!(removed.1, 0);
    /// assert_eq!(hashmap.len(), 1);
    ///
    /// let second_entry = second_entry.unwrap();
    /// let second_key = *second_entry.key();
    ///
    /// assert!(second_entry.remove_next().1.is_none());
    /// assert_eq!(first_key + second_key, 3);
    /// ```
    #[inline]
    #[must_use]
    pub fn remove_next(mut self) -> ((K, V), Option<Self>) {
        let guard = Guard::new();
        let entry = self.locked_entry.writer.remove(
            self.locked_entry.data_block,
            &mut self.locked_entry.entry_ptr,
            self.hashmap.prolonged_guard_ref(&guard),
        );
        let hashmap = self.hashmap;
        if let Some(locked_entry) = self.locked_entry.next_sync(hashmap) {
            return (
                entry,
                Some(OccupiedEntry {
                    hashmap,
                    locked_entry,
                }),
            );
        }
        (entry, None)
    }

    /// Gets the next closest occupied entry after removing the entry.
    ///
    /// [`HashMap::first_entry`], [`HashMap::first_entry_async`], and this method together enables
    /// the [`OccupiedEntry`] to effectively act as a mutable iterator over entries. The method
    /// never acquires more than one lock even when it searches other buckets for the next closest
    /// occupied entry.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    /// use scc::hash_map::Entry;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.insert(2, 0).is_ok());
    ///
    /// let second_entry_future = hashmap.first_entry().unwrap().remove_next_async();
    /// ```
    #[inline]
    pub async fn remove_next_async(mut self) -> ((K, V), Option<OccupiedEntry<'h, K, V, H>>) {
        let guard = Guard::new();
        let entry = self.locked_entry.writer.remove(
            self.locked_entry.data_block,
            &mut self.locked_entry.entry_ptr,
            self.hashmap.prolonged_guard_ref(&guard),
        );
        let hashmap = self.hashmap;
        if let Some(locked_entry) = self.locked_entry.next_async(hashmap).await {
            return (
                entry,
                Some(OccupiedEntry {
                    hashmap,
                    locked_entry,
                }),
            );
        }
        (entry, None)
    }
}

impl<K, V, H> Debug for OccupiedEntry<'_, K, V, H>
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

impl<K, V, H> Deref for OccupiedEntry<'_, K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    type Target = V;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.get()
    }
}

impl<K, V, H> DerefMut for OccupiedEntry<'_, K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.get_mut()
    }
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
    ///
    /// assert_eq!(hashmap.read(&19, |_, v| *v), Some(29));
    /// ```
    #[inline]
    pub fn insert_entry(self, val: V) -> OccupiedEntry<'h, K, V, H> {
        let guard = Guard::new();
        let entry_ptr = self.locked_entry.writer.insert_with(
            self.locked_entry.data_block,
            self.hash,
            || (self.key, val),
            self.hashmap.prolonged_guard_ref(&guard),
        );
        OccupiedEntry {
            hashmap: self.hashmap,
            locked_entry: LockedEntry {
                index: self.locked_entry.index,
                data_block: self.locked_entry.data_block,
                writer: self.locked_entry.writer,
                entry_ptr,
                len: 0,
            },
        }
    }
}

impl<K, V, H> Debug for VacantEntry<'_, K, V, H>
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

impl<K, V> ConsumableEntry<'_, K, V> {
    /// Consumes the entry by moving out the key and value.
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
    /// let mut consumed = None;
    ///
    /// hashmap.iter_mut_sync(|entry| {
    ///     if entry.0 == 1 {
    ///         consumed.replace(entry.consume().1);
    ///     }
    ///     true
    /// });
    ///
    /// assert!(!hashmap.contains(&1));
    /// assert_eq!(consumed, Some(0));
    /// ```
    #[inline]
    #[must_use]
    pub fn consume(mut self) -> (K, V) {
        *self.remove_probe |= true;
        self.writer
            .remove(self.data_block, &mut self.entry_ptr, self.guard)
    }
}

impl<K, V> Deref for ConsumableEntry<'_, K, V> {
    type Target = (K, V);

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.entry_ptr.get(self.data_block)
    }
}

impl<K, V> DerefMut for ConsumableEntry<'_, K, V> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.entry_ptr.get_mut(self.data_block, self.writer)
    }
}

impl<K, V, H> Reserve<'_, K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    /// Returns the number of reserved slots.
    #[inline]
    #[must_use]
    pub fn additional_capacity(&self) -> usize {
        self.additional
    }
}

impl<K, V, H> AsRef<HashMap<K, V, H>> for Reserve<'_, K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    #[inline]
    fn as_ref(&self) -> &HashMap<K, V, H> {
        self.hashmap
    }
}

impl<K, V, H> Debug for Reserve<'_, K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Reserve").field(&self.additional).finish()
    }
}

impl<K, V, H> Deref for Reserve<'_, K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    type Target = HashMap<K, V, H>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.hashmap
    }
}

impl<K, V, H> Drop for Reserve<'_, K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    #[inline]
    fn drop(&mut self) {
        let result = self
            .hashmap
            .minimum_capacity
            .fetch_sub(self.additional, Relaxed);
        debug_assert!(result >= self.additional);

        let guard = Guard::new();
        if let Some(current_array) = self.hashmap.bucket_array.load(Acquire, &guard).as_ref() {
            self.try_shrink_or_rebuild(current_array, 0, &guard);
        }
    }
}
