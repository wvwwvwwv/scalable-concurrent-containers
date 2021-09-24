//! [`HashMap`] implementation.

use crate::common::cell::Locker;
use crate::common::cell_array::CellArray;
use crate::common::hash_table::HashTable;
use crate::ebr::{Arc, AtomicArc, Barrier, Tag};

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::atomic::{AtomicBool, AtomicUsize};

const CELL_SIZE: usize = 32;
const DEFAULT_CAPACITY: usize = 64;

/// A scalable concurrent hash map data structure.
///
/// [`HashMap`] is a concurrent hash map data structure that is targeted at a highly concurrent
/// workload. The use of an epoch-based reclamation technique enables the data structure to
/// implement non-blocking resizing and fine-granular locking. It has a single array of entry
/// metadata, and each entry, called `Cell`, manages a fixed size entry array. Each `Cell` has
/// a customized 8-byte read-write mutex to protect the data structure, and a linked list for a
/// hash collision resolution.
///
/// ## The key features of [`HashMap`]
/// * Non-sharded: the data is managed by a single entry metadata array.
/// * Automatic resizing: it automatically grows or shrinks.
/// * Non-blocking resizing: resizing does not block other threads.
/// * Incremental resizing: each access to the data structure is mandated to rehash a fixed
///   number of key-value pairs.
/// * Optimized resizing: key-value pairs managed by a single cell are guaranteed to be
///   relocated to consecutive `Cell` instances.
/// * No busy waiting: the customized mutex never spins.
///
/// ## The key statistics for [`HashMap`]
/// * The expected size of metadata for a single key-value pair: 2-byte.
/// * The expected number of atomic operations required for an operation on a single key: 2.
/// * The expected number of atomic variables accessed during a single key operation: 1.
/// * The number of entries managed by a single metadata `Cell` without a linked list: 32.
/// * The number of entries a single linked list entry manages: 8.
/// * The expected maximum linked list length when resize is triggered: log(capacity) / 8.
pub struct HashMap<K, V, H = RandomState>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    array: AtomicArc<CellArray<K, V, CELL_SIZE, false>>,
    minimum_capacity: usize,
    additional_capacity: AtomicUsize,
    resizing_flag: AtomicBool,
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
    /// use scc::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashmap: HashMap<u64, u32, RandomState> = HashMap::new(1000, RandomState::new());
    ///
    /// let result = hashmap.capacity();
    /// assert_eq!(result, 1024);
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    /// let result = hashmap.capacity();
    /// assert_eq!(result, 64);
    /// ```
    pub fn new(capacity: usize, build_hasher: H) -> HashMap<K, V, H> {
        let initial_capacity = capacity.max(DEFAULT_CAPACITY);
        let array = Arc::new(CellArray::<K, V, CELL_SIZE, false>::new(
            initial_capacity,
            AtomicArc::null(),
        ));
        let current_capacity = array.num_cell_entries();
        HashMap {
            array: AtomicArc::from(array),
            minimum_capacity: current_capacity,
            additional_capacity: AtomicUsize::new(0),
            resizing_flag: AtomicBool::new(false),
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
    /// Returns [`None`] if a too large number is given.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashmap: HashMap<usize, usize, RandomState> = HashMap::new(1000, RandomState::new());
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
                        hash_map: self,
                        increment: capacity,
                    });
                }
                Err(current) => current_additional_capacity = current,
            }
        }
    }
    /// Inserts a key-value pair into the [`HashMap`].
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied key-value pair if the key exists.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails, or the number of entries in the target cell is
    /// reached `u32::MAX`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert_eq!(hashmap.insert(1, 1).unwrap_err(), (1, 1));
    /// ```
    #[inline]
    pub fn insert(&self, key: K, val: V) -> Result<(), (K, V)> {
        self.insert_entry(key, val)
    }

    /// Updates an existing key-value pair.
    ///
    /// Returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// assert!(hashmap.update(&1, |_, _| true).is_none());
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert_eq!(hashmap.update(&1, |_, v| { *v = 2; *v }).unwrap(), 2);
    /// assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
    /// ```
    #[inline]
    pub fn update<Q, F, R>(&self, key_ref: &Q, updater: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        F: FnOnce(&K, &mut V) -> R,
    {
        let (hash, partial_hash) = self.hash(key_ref);
        let barrier = Barrier::new();
        let (_, _, iterator) = self.acquire(key_ref, hash, partial_hash, &barrier);
        if let Some(iterator) = iterator {
            if let Some((k, v)) = iterator.get() {
                // The presence of `locker` prevents the entry from being modified outside it.
                #[allow(clippy::cast_ref_to_mut)]
                return Some(updater(k, unsafe { &mut *(v as *const V as *mut V) }));
            }
        }
        None
    }

    /// Constructs the value in-place, or modifies an existing value corresponding to the key.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails, or the number of entries in the target cell is
    /// reached `u32::MAX`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// hashmap.upsert(1, || 2, |_, v| *v = 2);
    /// assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
    /// hashmap.upsert(1, || 2, |_, v| *v = 3);
    /// assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 3);
    /// ```
    #[inline]
    pub fn upsert<FI: FnOnce() -> V, FU: FnOnce(&K, &mut V)>(
        &self,
        key: K,
        constructor: FI,
        updater: FU,
    ) {
        let (hash, partial_hash) = self.hash(&key);
        let barrier = Barrier::new();
        let (_, locker, iterator) = self.acquire(&key, hash, partial_hash, &barrier);
        if let Some(iterator) = iterator {
            if let Some((k, v)) = iterator.get() {
                // The presence of `locker` prevents the entry from being modified outside it.
                #[allow(clippy::cast_ref_to_mut)]
                updater(k, unsafe { &mut *(v as *const V as *mut V) });
                return;
            }
        }
        locker.insert(key, constructor(), partial_hash, &barrier);
    }

    /// Removes a key-value pair and returns the key-value-pair if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// assert!(hashmap.remove(&1).is_none());
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert_eq!(hashmap.remove(&1).unwrap(), (1, 0));
    /// ```
    #[inline]
    pub fn remove<Q>(&self, key_ref: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.remove_if(key_ref, |_| true)
    }

    /// Removes a key-value pair and returns the key-value-pair if the key exists and the given
    /// condition meets.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.remove_if(&1, |v| *v == 1).is_none());
    /// assert_eq!(hashmap.remove_if(&1, |v| *v == 0).unwrap(), (1, 0));
    /// ```
    #[inline]
    pub fn remove_if<Q, F: FnOnce(&V) -> bool>(&self, key_ref: &Q, condition: F) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (hash, partial_hash) = self.hash(key_ref);
        let barrier = Barrier::new();
        let (cell_index, locker, iterator) = self.acquire(key_ref, hash, partial_hash, &barrier);
        if let Some(mut iterator) = iterator {
            let remove = if let Some((_, v)) = iterator.get() {
                condition(v)
            } else {
                false
            };
            if remove {
                let result = locker.erase(&mut iterator);
                if cell_index < CELL_SIZE && locker.cell_ref().num_entries() < CELL_SIZE / 16 {
                    drop(locker);
                    let current_array_ptr = self.array.load(Acquire, &barrier);
                    if let Some(current_array_ref) = current_array_ptr.as_ref() {
                        if current_array_ref.old_array(&barrier).is_null()
                            && current_array_ref.num_cell_entries() > self.minimum_capacity()
                        {
                            let sample_size = current_array_ref.sample_size();
                            let mut num_entries = 0;
                            for i in 0..sample_size {
                                num_entries += current_array_ref.cell(i).num_entries();
                                if num_entries >= sample_size * CELL_SIZE / 16 {
                                    return result;
                                }
                            }
                            self.resize(&barrier);
                        }
                    }
                }
                return result;
            }
        }
        None
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
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// assert!(hashmap.read(&1, |_, v| *v).is_none());
    /// assert!(hashmap.insert(1, 10).is_ok());
    /// assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 10);
    /// ```
    #[inline]
    pub fn read<Q, R, F: FnOnce(&K, &V) -> R>(&self, key_ref: &Q, reader: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let barrier = Barrier::new();
        self.read_with(key_ref, reader, &barrier)
    }

    /// Reads a key-value pair using the supplied [`Barrier`].
    ///
    /// It enables the caller to use the value reference outside the method. It returns `None`
    /// if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// assert!(hashmap.insert(1, 10).is_ok());
    ///
    /// let barrier = Barrier::new();
    /// let value_ref = hashmap.read_with(&1, |k, v| v, &barrier).unwrap();
    /// assert_eq!(*value_ref, 10);
    /// ```
    #[inline]
    pub fn read_with<'b, Q, R, F: FnOnce(&'b K, &'b V) -> R>(
        &self,
        key_ref: &Q,
        reader: F,
        barrier: &'b Barrier,
    ) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.read_entry(key_ref, reader, barrier)
    }

    /// Checks if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
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

    /// It iterates over all the entries in the [`HashMap`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
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

    /// Retains the key-value pairs that satisfy the given predicate.
    ///
    /// It returns the number of entries remaining and removed.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.insert(2, 1).is_ok());
    /// assert!(hashmap.insert(3, 2).is_ok());
    ///
    /// assert_eq!(hashmap.retain(|k, v| *k == 1 && *v == 0), (1, 2));
    /// ```
    pub fn retain<F: FnMut(&K, &mut V) -> bool>(&self, mut filter: F) -> (usize, usize) {
        let mut retained_entries = 0;
        let mut removed_entries = 0;

        let barrier = Barrier::new();

        // An acquire fence is required to correctly load the contents of the array.
        let mut current_array_ptr = self.array.load(Acquire, &barrier);
        while let Some(current_array_ref) = current_array_ptr.as_ref() {
            if !current_array_ref.old_array(&barrier).is_null() {
                current_array_ref.partial_rehash(|key| self.hash(key), |_, _| None, &barrier);
                current_array_ptr = self.array.load(Acquire, &barrier);
                continue;
            }

            for cell_index in 0..current_array_ref.array_size() {
                if let Some(locker) = Locker::lock(current_array_ref.cell(cell_index), &barrier) {
                    let mut iterator = locker.cell_ref().iter(&barrier);
                    while iterator.next().is_some() {
                        let retain = if let Some((k, v)) = iterator.get() {
                            #[allow(clippy::cast_ref_to_mut)]
                            filter(k, unsafe { &mut *(v as *const V as *mut V) })
                        } else {
                            true
                        };
                        if retain {
                            retained_entries += 1;
                        } else {
                            locker.erase(&mut iterator);
                            removed_entries += 1;
                        }
                        if retained_entries == usize::MAX || removed_entries == usize::MAX {
                            // Gives up iteration on an integer overflow.
                            return (retained_entries, removed_entries);
                        }
                    }
                }
            }

            let new_current_array_ptr = self.array.load(Acquire, &barrier);
            if current_array_ptr == new_current_array_ptr {
                break;
            }
            retained_entries = 0;
            current_array_ptr = new_current_array_ptr;
        }

        if removed_entries >= retained_entries {
            self.resize(&barrier);
        }

        (retained_entries, removed_entries)
    }

    /// Clears all the key-value pairs.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert_eq!(hashmap.clear(), 1);
    /// ```
    #[inline]
    pub fn clear(&self) -> usize {
        self.retain(|_, _| false).1
    }

    /// Returns the number of entries in the [`HashMap`].
    ///
    /// It scans the entire array to calculate the number of valid entries, making its time
    /// complexity `O(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
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
    /// It scans the entire array to calculate the number of valid entries, making its time
    /// complexity `O(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
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
    /// use scc::HashMap;
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
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let result = hashmap.capacity();
    /// assert_eq!(result, 64);
    /// ```
    fn default() -> Self {
        HashMap {
            array: AtomicArc::new(CellArray::<K, V, CELL_SIZE, false>::new(
                DEFAULT_CAPACITY,
                AtomicArc::null(),
            )),
            minimum_capacity: DEFAULT_CAPACITY,
            additional_capacity: AtomicUsize::new(0),
            resizing_flag: AtomicBool::new(false),
            build_hasher: RandomState::new(),
        }
    }
}

impl<K, V, H> Drop for HashMap<K, V, H>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    fn drop(&mut self) {
        self.clear();
        let barrier = Barrier::new();
        let current_array_ptr = self.array.load(Acquire, &barrier);
        if let Some(current_array_ref) = current_array_ptr.as_ref() {
            current_array_ref.drop_old_array(&barrier);
            if let Some(current_array) = self.array.swap((None, Tag::None), Relaxed) {
                barrier.reclaim(current_array);
            }
        }
    }
}

impl<K, V, H> HashTable<K, V, H, CELL_SIZE, false> for HashMap<K, V, H>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    fn hasher(&self) -> &H {
        &self.build_hasher
    }
    fn copier(_: &K, _: &V) -> Option<(K, V)> {
        None
    }
    fn cell_array(&self) -> &AtomicArc<CellArray<K, V, CELL_SIZE, false>> {
        &self.array
    }
    fn minimum_capacity(&self) -> usize {
        self.minimum_capacity + self.additional_capacity.load(Relaxed)
    }
    fn resizing_flag_ref(&self) -> &AtomicBool {
        &self.resizing_flag
    }
}

/// [`Ticket`] keeps the increased minimum capacity of the [`HashMap`] during its lifetime.
///
/// The minimum capacity is lowered when the Ticket is dropped, thereby allowing unused space
/// to be reclaimed.
pub struct Ticket<'h, K, V, H>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    hash_map: &'h HashMap<K, V, H>,
    increment: usize,
}

impl<'h, K, V, H> Drop for Ticket<'h, K, V, H>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    fn drop(&mut self) {
        let result = self
            .hash_map
            .additional_capacity
            .fetch_sub(self.increment, Relaxed);
        self.hash_map.resize(&Barrier::new());
        debug_assert!(result >= self.increment);
    }
}
