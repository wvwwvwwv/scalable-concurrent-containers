use crate::common::cell::{Cell, CellIterator, CellLocker, CellReader};
use crate::common::cell_array::CellArray;
use crate::common::hash_table::HashTable;
use crate::ebr::{Arc, AtomicArc, Barrier, Tag};

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::iter::FusedIterator;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::atomic::{AtomicBool, AtomicUsize};

const CELL_SIZE: usize = 32;
const DEFAULT_CAPACITY: usize = 64;

/// A scalable concurrent hash map data structure.
///
/// [`HashMap`] is a concurrent hash map data structure that is targeted at a highly concurrent
/// workload. The use of an epoch-based reclamation technique enables the data structure to
/// implement non-blocking resizing and fine-granular locking. It has a single array of entry
/// metadata, and each entry, called [`Cell`], manages a fixed size entry array.
/// Each [`Cell`] has a customized 8-byte read-write mutex to protect the data structure, and a
/// linked list for a hash collision resolution.
///
/// ## The key features of [`HashMap`]
/// * Non-sharded: the data is managed by a single entry metadata array.
/// * Automatic resizing: it automatically grows or shrinks.
/// * Non-blocking resizing: resizing does not block other threads.
/// * Incremental resizing: each access to the data structure is mandated to rehash a fixed
///   number of key-value pairs.
/// * Optimized resizing: key-value pairs managed by a single cell are guaranteed to be
///   relocated to consecutive [`Cell`]s.
/// * No busy waiting: the customized mutex never spins.
///
/// ## The key statistics for [`HashMap`]
/// * The expected size of metadata for a single key-value pair: 2-byte.
/// * The expected number of atomic operations required for an operation on a single key: 2.
/// * The expected number of atomic variables accessed during a single key operation: 1.
/// * The number of entries managed by a single metadata [`Cell`] without a linked list: 32.
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

impl<K, V> Default for HashMap<K, V, RandomState>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
{
    /// Creates a [`HashMap`] with the default parameters.
    ///
    /// The default hash builder is [`RandomState`], and the default capacity is 64.
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
            array: AtomicArc::new(array),
            minimum_capacity: current_capacity,
            additional_capacity: AtomicUsize::new(0),
            resizing_flag: AtomicBool::new(false),
            build_hasher,
        }
    }

    /// Temporarily increases the minimum capacity of the HashMap.
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
    /// use scc::ebr::Barrier;
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
    ///     assert!(hashmap.insert(i, i, &Barrier::new()).is_ok());
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
                    let guard = crossbeam_epoch::pin();
                    self.resize(&guard);
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
    /// Returns an error with a mutable reference to the existing key-value pair.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails, or the number of entries in the target cell is
    /// reached `u32::MAX`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let barrier = Barrier::new();
    ///
    /// let result = hashmap.insert(1, 0, &barrier);
    /// result.unwrap().access(|k, v| {
    ///     assert_eq!((k, v), (&1, &mut 0));
    /// });
    ///
    /// let result = hashmap.insert(1, 1, &Barrier);
    /// assert!(result.is_err());
    /// ```
    #[inline]
    pub fn insert<'h, 'b>(
        &'h self,
        key: K,
        value: V,
        barrier: &'b Barrier,
    ) -> Result<Accessor<K, V, H>, (Accessor<K, V, H>, K, V)> {
        let (mut accessor, key, partial_hash) = self.lock(key, barrier);
        if accessor.cell_iterator.is_some() {
            return Err((accessor, key, value));
        }
        let (iterator, result) =
            accessor
                .cell_locker
                .as_ref()
                .unwrap()
                .insert(key, value, partial_hash, barrier);
        accessor.cell_iterator.replace(iterator);
        debug_assert!(result.is_none());
        Ok(accessor)
    }

    /// Constructs the value in-place.
    ///
    /// The given closure is never invoked if the key exists.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails, or the number of entries in the target cell is
    /// reached `u32::MAX`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let barrier = Barrier::new();
    ///
    /// let mut current = 0;
    /// let result = hashmap.emplace(1, || { current += 1; current }, &barrier);
    /// result.unwrap().access(|k, v| {
    ///     assert_eq!((k, v), (&1, &mut 1));
    /// });
    ///
    /// let result = hashmap.emplace(1, || { current += 1; current }, &barrier);
    /// assert!(result.is_err());
    /// ```
    #[inline]
    pub fn emplace<'h, 'b, F: FnOnce() -> V>(
        &'h self,
        key: K,
        constructor: F,
        barrier: &'b Barrier,
    ) -> Result<Accessor<K, V, H>, (Accessor<K, V, H>, K)> {
        let (mut accessor, key, partial_hash) = self.lock(key, barrier);
        if accessor.cell_iterator.is_some() {
            return Err((accessor, key));
        }
        let (iterator, result) = accessor.cell_locker.as_ref().unwrap().insert(
            key,
            constructor(),
            partial_hash,
            unsafe { crossbeam_epoch::unprotected() },
        );
        debug_assert!(result.is_none());
        accessor.cell_iterator.replace(iterator);
        Ok(accessor)
    }

    /// Gets a mutable reference to the value associated with the key.
    ///
    /// # Errors
    ///
    /// Returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let barrier = Barrier::new();
    ///
    /// let result = hashmap.get(&1, &barrier);
    /// assert!(result.is_none());
    ///
    /// assert!(hashmap.insert(1, 0, &barrier).is_ok());
    ///
    /// let result = hashmap.get(&1, &barrier);
    /// result.unwrap().access(|k, v| {
    ///     assert_eq!((k, v), (&1, &mut 0));
    /// });
    /// ```
    #[inline]
    pub fn get<'h, 'b, Q>(
        &'h self,
        key: &Q,
        barrier: &'b Barrier,
    ) -> Option<Accessor<'h, 'b, K, V, H>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (hash, partial_hash) = self.hash(key);
        let accessor = self.acquire(key, hash, partial_hash, barrier);
        if accessor.cell_iterator.is_none() {
            return None;
        }
        Some(accessor)
    }

    /// Removes a key-value pair.
    ///
    /// # Errors
    ///
    /// Returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let barrier = Barrier::new();
    ///
    /// let result = hashmap.remove(&1, &barrier);
    /// assert!(result.is_none());
    ///
    /// assert!(hashmap.insert(1, 0, &barrier).is_ok();
    ///
    /// let result = hashmap.remove(&1, &barrier);
    /// assert_eq!(result.unwrap(), 0);
    /// ```
    #[inline]
    pub fn remove<Q>(&self, key: &Q, barrier: &Barrier) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.get(key, barrier)
            .map_or_else(|| None, |accessor| accessor.erase())
    }

    /// Reads a key-value pair.
    ///
    /// # Errors
    ///
    /// Returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let barrier = Barrier::new();
    ///
    /// assert!(hashmap.insert(1, 0, &barrier).is_ok());
    ///
    /// let result = hashmap.read(&1, |key, value| *value, &barrier);
    /// assert_eq!(result.unwrap(), 0);
    /// ```
    #[inline]
    pub fn read<Q, R, F: FnOnce(&Q, &V) -> R>(&self, key: &Q, f: F, barrier: &Barrier) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (hash, partial_hash) = self.hash(key);

        // An acquire fence is required to correctly load the contents of the array.
        let mut current_array_ptr = self.array.load(Acquire, barrier);
        loop {
            let current_array_ref = Self::cell_array_ref(current_array_ptr);
            let old_array_ptr = current_array_ref.old_array(&barrier);
            if !old_array_ptr.is_null()
                && !current_array_ref.partial_rehash(|key| self.hash(key), |_, _| None, &barrier)
            {
                let old_array_ref = Self::cell_array_ref(old_array_ptr);
                let cell_index = old_array_ref.calculate_cell_index(hash);
                if let Some(reader) = CellReader::lock(old_array_ref.cell(cell_index), &barrier) {
                    if let Some((key, value)) =
                        reader.cell_ref().search(key, partial_hash, &barrier)
                    {
                        return Some(f(key.borrow(), value));
                    }
                }
            }
            let cell_index = current_array_ref.calculate_cell_index(hash);
            if let Some(reader) = CellReader::lock(current_array_ref.cell(cell_index), &barrier) {
                if let Some((key, value)) = reader.cell_ref().search(key, partial_hash, &barrier) {
                    return Some(f(key.borrow(), value));
                }
            }
            let new_current_array_ptr = self.array.load(Acquire, &barrier);
            if new_current_array_ptr == current_array_ptr {
                break;
            }
            // The pointer value has changed.
            current_array_ptr = new_current_array_ptr;
        }
        None
    }

    /// Checks if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let barrier = Barrier::new();
    ///
    /// assert!(!hashmap.contains(&1, &barrier));
    /// assert!(hashmap.insert(1, 0, &barrier).is_ok());
    /// assert!(hashmap.contains(&1, &barrier));
    /// ```
    #[inline]
    pub fn contains<Q>(&self, key: &Q, barrier: &Barrier) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.read(key, |_, _| (), barrier).is_some()
    }

    /// Retains the key-value pairs that satisfy the given predicate.
    ///
    /// It returns the number of entries remaining and removed.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let barrier = Barrier::new();
    ///
    /// assert!(hashmap.insert(1, 0, &barrier).is_ok());
    /// assert!(hashmap.insert(2, 0, &barrier).is_ok());
    ///
    /// let result = hashmap.retain(|key, value| *key == 1 && *value == 0, &barrier);
    /// assert_eq!(result, (1, 1));
    /// ```
    pub fn retain<F: Fn(&K, &mut V) -> bool>(&self, f: F, barrier: &Barrier) -> (usize, usize) {
        let mut retained_entries = 0;
        let mut removed_entries = 0;
        let mut accessor = self.iter();
        while accessor.next().is_some() {
            if !accessor.access(|k, v| f(k, v)) {
                accessor.erase();
                removed_entries += 1;
            } else {
                retained_entries += 1;
            }
        }

        let current_array_ptr = self.array.load(Acquire, barrier);
        let current_array_ref = Self::cell_array_ref(current_array_ptr);
        if retained_entries <= current_array_ref.num_cell_entries() / 8 {
            self.resize(barrier);
        }

        (retained_entries, removed_entries)
    }

    /// Clears all the key-value pairs.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let barrier = Barrier::new();
    ///
    /// assert!(hashmap.insert(1, 0, &barrier).is_ok());
    /// assert_eq!(hashmap.clear(&barrier), 1);
    /// ```
    #[inline]
    pub fn clear(&self, barrier: &Barrier) -> usize {
        self.retain(|_, _| false, barrier).1
    }

    /// Returns the number of entries in the HashMap.
    ///
    /// It scans the entire metadata cell array to calculate the number of valid entries,
    /// making its time complexity O(N).
    /// Apart from being inefficient, it may return a smaller number when the HashMap is being
    /// resized.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let barrier = Barrier::new();
    ///
    /// assert!(hashmap.insert(1, 0, &barrier).is_ok());
    /// assert_eq!(hashmap.len(&barrier), 1);
    /// ```
    #[inline]
    pub fn len(&self, barrier: &Barrier) -> usize {
        self.num_entries(barrier)
    }

    /// Returns the capacity of the HashMap.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashmap: HashMap<u64, u32, RandomState> = HashMap::new(1000000, RandomState::new());
    /// assert_eq!(hashmap.capacity(&Barrier::new()), 1048576);
    /// ```
    #[inline]
    pub fn capacity(&self, barrier: &Barrier) -> usize {
        self.num_slots(barrier)
    }

    /// Returns an [`Accessor`].
    ///
    /// It is guaranteed to go through all the key-value pairs pertaining in the [`HashMap`] at
    /// the moment, however the same key-value pair can be visited more than once if the
    /// [`HashMap`] is being resized.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let barrier = Barrier::new();
    ///
    /// assert!(hashmap.insert(1, 0, &barrier).is_ok());
    ///
    /// let mut iter = hashmap.iter(&barrier);
    /// assert!(iter.next().is_some());
    /// iter.access(|k, v| {
    ///     assert_eq!((k, v), (&1, &mut 0));
    /// });
    /// assert!(iter.next().is_none());
    /// ```
    pub fn iter(&self, barrier: &Barrier) -> Accessor<K, V, H> {
        Accessor {
            hash_map: &self,
            array_ptr: std::ptr::null(),
            cell_index: 0,
            cell_locker: None,
            cell_iterator: None,
            barrier_ref: barrier,
        }
    }

    /// Locks a [`Cell`] for inserting a new key-value pair.
    fn lock(&self, key: K, barrier: &Barrier) -> (Accessor<K, V, H>, K, u8) {
        let (hash, partial_hash) = self.hash(&key);
        let mut resize_triggered = false;
        loop {
            let accessor = self.acquire(&key, hash, partial_hash, barrier);
            if !resize_triggered
                && accessor.cell_index < CELL_SIZE
                && accessor
                    .cell_locker
                    .as_ref()
                    .unwrap()
                    .cell_ref()
                    .num_entries()
                    >= CELL_SIZE
            {
                drop(accessor);
                resize_triggered = true;
                let guard = crossbeam_epoch::pin();
                let current_array = self.array.load(Acquire, &guard);
                let current_array_ref = Self::cell_array_ref(current_array);
                if current_array_ref.old_array(&guard).is_null() {
                    // Triggers resize if the estimated load factor is greater than 7/8.
                    let sample_size = current_array_ref.sample_size();
                    let threshold = sample_size * (CELL_SIZE / 8) * 7;
                    let mut num_entries = 0;
                    for i in 0..sample_size {
                        num_entries += current_array_ref.cell(i).num_entries();
                        if num_entries > threshold {
                            self.resize(&guard);
                            break;
                        }
                    }
                }
                continue;
            }
            return (accessor, key, partial_hash);
        }
    }

    /// Acquires a cell.
    fn acquire<'h, 'b, Q>(
        &'h self,
        key: &Q,
        hash: u64,
        partial_hash: u8,
        barrier: &'b Barrier,
    ) -> Accessor<'h, 'b, K, V, H>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        // It is guaranteed that the thread reads a consistent snapshot of the current and old
        // array pair by a release memory barrier in the resize function, hence the following
        // procedure is correct.
        //  - The thread reads self.array, and it kills the target cell in the old array
        //    if there is one attached to it, and inserts the key into array.
        // There are two cases.
        //  1. The thread reads an old version of self.array.
        //    If there is another thread having read the latest version of self.array,
        //    trying to insert the same key, it will try to kill the Cell in the old version
        //    of self.array, thus competing with each other.
        //  2. The thread reads the latest version of self.array.
        //    If the array is deprecated while inserting the key, it falls into case 1.
        loop {
            // An acquire fence is required to correctly load the contents of the array.
            let current_array_ptr = self.array.load(Acquire, &barrier);
            let current_array_ref = current_array_ptr.as_ref().unwrap();
            let old_array_ptr = current_array_ref.old_array(&barrier);
            if let Some(old_array_ref) = old_array_ptr.as_ref() {
                if current_array_ref.partial_rehash(|key| self.hash(key), |_, _| None, &barrier) {
                    continue;
                }
                let cell_index = old_array_ref.calculate_cell_index(hash);
                if let Some(mut locker) = CellLocker::lock(old_array_ref.cell(cell_index), barrier)
                {
                    if let Some(iterator) = locker.cell_ref().get(key, partial_hash, barrier) {
                        return Accessor {
                            hash_map: &self,
                            array_ptr: old_array_ptr.as_raw(),
                            cell_index,
                            cell_locker: Some(locker),
                            cell_iterator: iterator,
                            barrier_ref: barrier,
                        };
                    }
                    // Kills the Cell.
                    current_array_ref.kill_cell(
                        &mut locker,
                        Self::cell_array_ref(old_array_ptr),
                        cell_index,
                        &|key| self.hash(key),
                        &|_, _| None,
                        &barrier,
                    );
                }
            }
            let cell_index = current_array_ref.calculate_cell_index(hash);
            if let Some(locker) = CellLocker::lock(current_array_ref.cell(cell_index), barrier) {
                if let Some(iterator) = locker.cell_ref().get(key, partial_hash, barrier) {
                    return Accessor {
                        hash_map: &self,
                        array_ptr: current_array_ptr.as_raw(),
                        cell_index,
                        cell_locker: Some(locker),
                        cell_iterator: iterator,
                        barrier_ref: barrier,
                    };
                }
                return Accessor {
                    hash_map: &self,
                    array_ptr: current_array_ptr.as_raw(),
                    cell_index,
                    cell_locker: Some(locker),
                    cell_iterator: None,
                    barrier_ref: barrier,
                };
            }

            // Reaching here indicates that self.array is updated.
        }
    }

    /// Erases a key-value pair owned by the Accessor.
    fn erase<'h, 'b>(&'h self, mut accessor: Accessor<'h, 'b, K, V, H>) -> V {
        let mut iterator = accessor.cell_iterator.take().unwrap();
        let value = accessor
            .cell_locker
            .as_ref()
            .unwrap()
            .erase(&mut iterator)
            .unwrap()
            .1;
        if accessor
            .cell_locker
            .as_ref()
            .unwrap()
            .cell_ref()
            .num_entries()
            == 0
            && accessor.cell_index < CELL_SIZE
        {
            drop(accessor);
            let guard = crossbeam_epoch::pin();
            let current_array = self.array.load(Acquire, &guard);
            let current_array_ref = Self::cell_array_ref(current_array);
            if current_array_ref.old_array(&guard).is_null()
                && current_array_ref.num_cell_entries() > self.minimum_capacity()
            {
                // Triggers resize if the estimated load factor is smaller than 1/16.
                let sample_size = current_array_ref.sample_size();
                let mut num_entries = 0;
                for i in 0..sample_size {
                    num_entries += current_array_ref.cell(i).num_entries();
                    if num_entries >= sample_size * CELL_SIZE / 16 {
                        return value;
                    }
                }
                self.resize(&guard);
            }
        }
        value
    }

    /// Returns a reference to the entry.
    fn entry<'h>(&'h self, entry_ptr: *const (K, V)) -> (&'h K, &'h mut V) {
        unsafe {
            let key_ptr = &(*entry_ptr).0 as *const K;
            let value_ptr = &(*entry_ptr).1 as *const V;
            let value_mut_ptr = value_ptr as *mut V;
            (&(*key_ptr), &mut (*value_mut_ptr))
        }
    }
}

impl<K, V, H> Drop for HashMap<K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    fn drop(&mut self) {
        // The HashMap has become unreachable, therefore pinning is unnecessary.
        self.clear();
        let barrier = Barrier::new();
        let current_array = self.array.load(Acquire, &barrier);
        let current_array_ref = Self::cell_array_ref(current_array);
        current_array_ref.drop_old_array(true, &barrier);
        if let Some(current_array) = self.array.swap((None, Tag::None), Relaxed) {
            barrier.reclaim(current_array);
        }
    }
}

impl<K, V, H> HashTable<K, V, H, CELL_SIZE, false> for HashMap<K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    fn hasher(&self) -> &H {
        &self.build_hasher
    }
    fn cell_array_ptr(&self) -> &AtomicArc<CellArray<K, V, CELL_SIZE, false>> {
        &self.array
    }
    fn minimum_capacity(&self) -> usize {
        self.minimum_capacity + self.additional_capacity.load(Relaxed)
    }
    fn resizing_flag_ref(&self) -> &AtomicBool {
        &self.resizing_flag
    }
}

/// Ticket keeps the increased minimum capacity of the HashMap during its lifetime.
///
/// The minimum capacity is lowered when the Ticket is dropped, thereby allowing unused space to be reclaimed.
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

/// Accessor owns a key-value pair in the HashMap, and it implements [`Iterator`].
///
/// It is !Send, thus disallowing other threads to have references to it.
/// It acquires an exclusive lock on the Cell managing the key.
/// A thread having multiple Accessor instances poses a possibility of deadlock.
pub struct Accessor<'h, 'b, K, V, H>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    hash_map: &'h HashMap<K, V, H>,
    array_ptr: *const CellArray<K, V, CELL_SIZE, false>,
    cell_index: usize,
    cell_locker: Option<CellLocker<'b, K, V, CELL_SIZE, false>>,
    cell_iterator: Option<CellIterator<'b, K, V, CELL_SIZE, false>>,
    barrier_ref: &'b Barrier,
}

impl<'h, 'b, K, V, H> Accessor<'h, 'b, K, V, H>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    /// Gains access to the entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let barrier = Barrier::new();
    ///
    /// let result = hashmap.get(&1, &barrier);
    /// assert!(result.is_none());
    ///
    /// let result = hashmap.insert(1, 0, &barrier);
    /// result.unwrap().access(|k, v| {
    ///     assert_eq!((k, v), (&1, &mut 0));
    ///     *v = 2;
    /// });
    ///
    /// let result = hashmap.get(&1, &barrier);
    /// result.unwrap().access(|k, v| {
    ///     assert_eq!((k, v), (&1, &mut 2));
    ///     *v = 2;
    /// });
    /// ```
    pub fn access<R, F: FnOnce(&K, &mut V) -> R>(&mut self, f: F) -> R {
        let itr_ref = self.cell_iterator.as_ref().unwrap();
        let entry_ref = itr_ref.get().unwrap();
        let (key_ref, value_mut_ref) = self.hash_map.entry(entry_ref as *const _);
        f(key_ref, value_mut_ref)
    }

    /// Erases the key-value pair owned by the Accessor.
    ///
    /// # Examples
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let result = hashmap.insert(1, 0);
    /// if let Ok(result) = result {
    ///     assert_eq!(result.get(), (&1, &mut 0));
    ///     let result = result.erase();
    ///     assert_eq!(result.unwrap(), 0);
    /// }
    ///
    /// let result = hashmap.get(&1);
    /// assert!(result.is_none());
    /// ```
    pub fn erase(self) -> Option<V> {
        if self.cell_iterator.is_none() {
            return None;
        }
        Some(self.hash_map.erase(self))
    }
}

impl<'h, 'b, K, V, H> FusedIterator for Accessor<'h, 'b, K, V, H>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: 'static + BuildHasher,
{
}

impl<'h, 'b, K, V, H> Iterator for Accessor<'h, 'b, K, V, H>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    type Item = (&'b K, &'b mut V);
    fn next(&mut self) -> Option<Self::Item> {
        if self.barrier_ref.is_none() {
            // It always owns a CellLocker preventing the array from being dropped,
            // therefore a dummy Guard is sufficient.
            self.barrier_ref
                .replace(unsafe { crossbeam_epoch::unprotected() });

            // A valid Guard is required to load Arrays.
            let guard = crossbeam_epoch::pin();
            loop {
                let current_array = self.hash_map.array.load(Acquire, &guard);
                let current_array_ref = unsafe { &*current_array.as_raw() };
                let old_array = current_array_ref.old_array(&guard);
                if !old_array.is_null() {
                    let old_array_ref = unsafe { &*old_array.as_raw() };
                    for index in 0..old_array_ref.array_size() {
                        if let Some(locker) = CellLocker::lock(
                            old_array_ref.cell(index),
                            self.barrier_ref.as_ref().unwrap(),
                        ) {
                            self.array_ptr = old_array.as_raw();
                            self.cell_index = index;
                            self.cell_locker.replace(locker);
                            break;
                        }
                    }
                    if self.cell_locker.is_some() {
                        break;
                    }
                }
                for index in 0..current_array_ref.array_size() {
                    if let Some(locker) = CellLocker::lock(
                        current_array_ref.cell(index),
                        self.barrier_ref.as_ref().unwrap(),
                    ) {
                        self.array_ptr = current_array.as_raw();
                        self.cell_index = index;
                        self.cell_locker.replace(locker);
                        break;
                    }
                }
                if self.cell_locker.is_some() {
                    break;
                }
                // Reaching here indicates that self.array is updated.
            }
        }

        if self.cell_iterator.is_none() {
            // Starts scanning.
            self.cell_iterator.replace(CellIterator::new(
                unsafe {
                    std::mem::transmute::<_, &'h Cell<K, V, CELL_SIZE, false>>(
                        self.cell_locker.as_ref().unwrap().cell_ref(),
                    )
                },
                self.barrier_ref.as_ref().unwrap(),
            ));
        }
        loop {
            if let Some(iterator) = self.cell_iterator.as_mut() {
                // Proceeds to the next entry in the Cell.
                if let Some(_) = iterator.next() {
                    return Some(self.get());
                }
            }
            // Proceeds to the next Cell.
            let mut array_ref = unsafe { &*self.array_ptr };
            self.cell_index += 1;
            self.cell_iterator.take();

            if self.cell_index == array_ref.array_size() {
                let current_array = self
                    .hash_map
                    .array
                    .load(Acquire, self.barrier_ref.as_ref().unwrap());
                if self.array_ptr == current_array.as_raw() {
                    // Finished scanning the entire array.
                    break;
                }

                // Proceeds to the current array.
                array_ref = unsafe { current_array.deref() };
                self.array_ptr = current_array.as_raw();
                self.cell_index = 0;
            }

            // Proceeds to the next Cell in the current array.
            for index in self.cell_index..array_ref.array_size() {
                let cell_ref = array_ref.cell(index);
                if let Some(locker) = CellLocker::lock(cell_ref, self.barrier_ref.as_ref().unwrap())
                {
                    self.cell_index = index;
                    self.cell_locker.replace(locker);
                    self.cell_iterator.replace(CellIterator::new(
                        cell_ref,
                        self.barrier_ref.as_ref().unwrap(),
                    ));
                    break;
                }
            }
            if self.cell_iterator.is_some() {
                continue;
            }
            break;
        }
        self.cell_locker.take();
        None
    }
}
