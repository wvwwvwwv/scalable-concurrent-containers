use crate::common::cell::{Cell, CellIterator, CellLocker};
use crate::common::cell_array::CellArray;
use crate::common::hash_table::HashTable;

use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
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
/// scc::HashMap is a concurrent hash map data structure that is targeted at a highly concurrent workload.
/// The use of epoch-based reclamation technique enables the data structure to implement non-blocking resizing and fine-granular locking.
/// It has a single array of entry metadata, and each entry, called a cell, manages a fixed size entry array.
/// Each cell has a customized 8-byte read-write mutex to protect the data structure, and a linked list for hash collision resolution.
///
/// ## The key features of scc::HashMap
/// * Non-sharded: the data is managed by a single entry metadata array.
/// * Automatic resizing: it automatically grows or shrinks.
/// * Non-blocking resizing: resizing does not block other threads.
/// * Incremental resizing: each access to the data structure is mandated to rehash a fixed number of key-value pairs.
/// * Optimized resizing: key-value pairs managed by a single cell are guaranteed to be relocated to adjacent cells.
/// * No busy waiting: the customized mutex never spins.
///
/// ## The key statistics for scc::HashMap
/// * The expected size of metadata for a single key-value pair: 2-byte.
/// * The expected number of atomic operations required for an operation on a single key: 2.
/// * The expected number of atomic variables accessed during a single key operation: 1.
/// * The number of entries managed by a single metadata cell without a linked list: 32.
/// * The number of entries a single linked list entry manages: 8.
/// * The expected maximum linked list length when resize is triggered: log(capacity) / 8.
pub struct HashMap<K, V, H = RandomState>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    array: Atomic<CellArray<K, V, CELL_SIZE, false>>,
    minimum_capacity: usize,
    additional_capacity: AtomicUsize,
    resizing_flag: AtomicBool,
    build_hasher: H,
}

impl<K, V> Default for HashMap<K, V, RandomState>
where
    K: Eq + Hash + Sync,
    V: Sync,
{
    /// Creates a HashMap instance with the default parameters.
    ///
    /// The default hash builder is RandomState, and the default capacity is 64.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails.
    ///
    /// # Examples
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
            array: Atomic::new(CellArray::<K, V, CELL_SIZE, false>::new(
                DEFAULT_CAPACITY,
                Atomic::null(),
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
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    /// Creates an empty HashMap instance with the given capacity and build hasher.
    ///
    /// The actual capacity is equal to or greater than the given capacity.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails.
    ///
    /// # Examples
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
        let array = Owned::new(CellArray::<K, V, CELL_SIZE, false>::new(
            initial_capacity,
            Atomic::null(),
        ));
        let current_capacity = array.num_cell_entries();
        HashMap {
            array: Atomic::from(array),
            minimum_capacity: current_capacity,
            additional_capacity: AtomicUsize::new(0),
            resizing_flag: AtomicBool::new(false),
            build_hasher,
        }
    }

    /// Temporarily increases the minimum capacity of the HashMap.
    ///
    /// The reserved space is not exclusively owned by the Ticket, there thus can be overtaken.
    /// Unused space is immediately reclaimed when the Ticket is dropped.
    ///
    /// # Errors
    ///
    /// Returns None if the given value is too large.
    ///
    /// # Examples
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

    /// Inserts a key-value pair into the HashMap.
    ///
    /// # Errors
    ///
    /// Returns an error with a mutable reference to the existing key-value pair.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails, or the number of entries in the target cell reaches u32::MAX.
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
    /// } else {
    ///     assert!(false);
    /// }
    ///
    /// let result = hashmap.insert(1, 1);
    /// if let Err((accessor, key, value)) = result {
    ///     assert_eq!(accessor.get(), (&1, &mut 0));
    ///     assert_eq!(key, 1);
    ///     assert_eq!(value, 1);
    /// } else {
    ///     assert!(false);
    /// }
    /// ```
    pub fn insert<'h>(
        &'h self,
        key: K,
        value: V,
    ) -> Result<Accessor<K, V, H>, (Accessor<K, V, H>, K, V)> {
        let (mut accessor, key, partial_hash) = self.lock(key);
        if accessor.cell_iterator.is_some() {
            return Err((accessor, key, value));
        }
        let (iterator, result) =
            accessor
                .cell_locker
                .as_ref()
                .unwrap()
                .insert(key, value, partial_hash, unsafe {
                    crossbeam_epoch::unprotected()
                });
        accessor.cell_iterator.replace(unsafe {
            std::mem::transmute::<_, CellIterator<'h, K, V, CELL_SIZE, false>>(iterator)
        });
        debug_assert!(result.is_none());
        drop(result);
        Ok(accessor)
    }

    /// Constructs the value in-place.
    ///
    /// The given closure is never invoked if the key exists.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails, or the number of entries in the target cell reaches u32::MAX.
    ///
    /// # Examples
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let mut current = 0;
    /// let result = hashmap.emplace(1, || { current += 1; current });
    /// if let Ok(result) = result {
    ///     assert_eq!(result.get(), (&1, &mut 1));
    /// } else {
    ///     assert!(false);
    /// }
    ///
    /// let result = hashmap.emplace(1, || { current += 1; current });
    /// if let Err((result, key)) = result {
    ///     assert_eq!(result.get(), (&1, &mut 1));
    ///     assert_eq!(key, 1);
    ///     assert_eq!(current, 1);
    /// } else {
    ///     assert!(false);
    /// }
    /// ```
    pub fn emplace<'h, F: FnOnce() -> V>(
        &'h self,
        key: K,
        constructor: F,
    ) -> Result<Accessor<K, V, H>, (Accessor<K, V, H>, K)> {
        let (mut accessor, key, partial_hash) = self.lock(key);
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
        accessor.cell_iterator.replace(unsafe {
            std::mem::transmute::<_, CellIterator<'h, K, V, CELL_SIZE, false>>(iterator)
        });
        Ok(accessor)
    }

    /// Upserts a key-value pair into the HashMap.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails, or the number of entries in the target cell reaches u32::MAX.
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
    /// } else {
    ///     assert!(false);
    /// }
    ///
    /// let result = hashmap.upsert(1, 1);
    /// assert_eq!(result.get(), (&1, &mut 1));
    /// ```
    pub fn upsert<'h>(&'h self, key: K, value: V) -> Accessor<K, V, H> {
        let (mut accessor, key, partial_hash) = self.lock(key);
        if accessor.cell_iterator.is_some() {
            drop(std::mem::replace(accessor.get().1, value));
            return accessor;
        }
        let (iterator, result) =
            accessor
                .cell_locker
                .as_ref()
                .unwrap()
                .insert(key, value, partial_hash, unsafe {
                    crossbeam_epoch::unprotected()
                });
        debug_assert!(result.is_none());
        accessor.cell_iterator.replace(unsafe {
            std::mem::transmute::<_, CellIterator<'h, K, V, CELL_SIZE, false>>(iterator)
        });
        accessor
    }

    /// Gets a mutable reference to the value associated with the key.
    ///
    /// # Errors
    ///
    /// Returns None if the key does not exist.
    ///
    /// # Examples
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let result = hashmap.get(&1);
    /// assert!(result.is_none());
    ///
    /// let result = hashmap.insert(1, 0);
    /// if let Ok(result) = result {
    ///     assert_eq!(result.get(), (&1, &mut 0));
    /// }
    ///
    /// let result = hashmap.get(&1);
    /// assert_eq!(result.unwrap().get(), (&1, &mut 0));
    /// ```
    pub fn get<'h, Q>(&'h self, key: &Q) -> Option<Accessor<'h, K, V, H>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (hash, partial_hash) = self.hash(key);
        let accessor = self.acquire(key, hash, partial_hash);
        if accessor.cell_iterator.is_none() {
            return None;
        }
        Some(accessor)
    }

    /// Removes a key-value pair.
    ///
    /// # Errors
    ///
    /// Returns None if the key does not exist.
    ///
    /// # Examples
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let result = hashmap.remove(&1);
    /// assert!(result.is_none());
    ///
    /// let result = hashmap.insert(1, 0);
    /// if let Ok(result) = result {
    ///     assert_eq!(result.get(), (&1, &mut 0));
    /// }
    ///
    /// let result = hashmap.remove(&1);
    /// assert_eq!(result.unwrap(), 0);
    /// ```
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.get(key)
            .map_or_else(|| None, |accessor| accessor.erase())
    }

    /// Reads a key-value pair.
    ///
    /// # Errors
    ///
    /// Returns None if the key does not exist.
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
    /// }
    ///
    /// let result = hashmap.read(&1, |key, value| *value);
    /// assert_eq!(result.unwrap(), 0);
    /// ```
    pub fn read<Q, R, F: FnOnce(&Q, &V) -> R>(&self, key: &Q, f: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (hash, partial_hash) = self.hash(key);
        let guard = crossbeam_epoch::pin();

        // An acquire fence is required to correctly load the contents of the array.
        let mut current_array_shared = self.array.load(Acquire, &guard);
        loop {
            let current_array_ref = Self::cell_array_ref(current_array_shared);
            let old_array_shared = current_array_ref.old_array(&guard);
            if !old_array_shared.is_null()
                && !current_array_ref.partial_rehash(|key| self.hash(key), |_, _| None, &guard)
            {
                let old_array_ref = Self::cell_array_ref(old_array_shared);
                let cell_index = old_array_ref.calculate_cell_index(hash);
                if let Some(locker) = CellLocker::lock(old_array_ref.cell(cell_index), &guard) {
                    if let Some((key, value)) = locker.cell_ref().search(key, partial_hash, &guard)
                    {
                        return Some(f(key.borrow(), value));
                    }
                }
            }
            let cell_index = current_array_ref.calculate_cell_index(hash);
            if let Some(locker) = CellLocker::lock(current_array_ref.cell(cell_index), &guard) {
                if let Some((key, value)) = locker.cell_ref().search(key, partial_hash, &guard) {
                    return Some(f(key.borrow(), value));
                }
            }
            let new_current_array_shared = self.array.load(Acquire, &guard);
            if new_current_array_shared == current_array_shared {
                break;
            }
            // The pointer value has changed.
            current_array_shared = new_current_array_shared;
        }
        None
    }

    /// Checks if the key exists.
    ///
    /// # Examples
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let result = hashmap.contains(&1);
    /// assert!(!result);
    ///
    /// let result = hashmap.insert(1, 0);
    /// if let Ok(result) = result {
    ///     assert_eq!(result.get(), (&1, &mut 0));
    /// }
    ///
    /// let result = hashmap.contains(&1);
    /// assert!(result);
    /// ```
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.read(key, |_, _| ()).is_some()
    }

    /// Retains the key-value pairs that satisfy the given predicate.
    ///
    /// It returns the number of entries remaining and removed.
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
    /// }
    ///
    /// let result = hashmap.insert(2, 0);
    /// if let Ok(result) = result {
    ///     assert_eq!(result.get(), (&2, &mut 0));
    /// }
    ///
    /// let result = hashmap.retain(|key, value| *key == 1 && *value == 0);
    /// assert_eq!(result, (1, 1));
    ///
    /// let result = hashmap.get(&1);
    /// assert_eq!(result.unwrap().get(), (&1, &mut 0));
    ///
    /// let result = hashmap.get(&2);
    /// assert!(result.is_none());
    /// ```
    pub fn retain<F: Fn(&K, &mut V) -> bool>(&self, f: F) -> (usize, usize) {
        let mut retained_entries = 0;
        let mut removed_entries = 0;
        let mut accessor = self.iter();
        while let Some((key, value)) = accessor.next() {
            if !f(key, value) {
                accessor
                    .cell_locker
                    .as_ref()
                    .unwrap()
                    .erase(accessor.cell_iterator.as_mut().unwrap());
                removed_entries += 1;
            } else {
                retained_entries += 1;
            }
        }

        let guard = crossbeam_epoch::pin();
        let current_array = self.array.load(Acquire, &guard);
        let current_array_ref = Self::cell_array_ref(current_array);
        if retained_entries <= current_array_ref.num_cell_entries() / 8 {
            self.resize(&guard);
        }

        (retained_entries, removed_entries)
    }

    /// Clears all the key-value pairs.
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
    /// }
    ///
    /// let result = hashmap.insert(2, 0);
    /// if let Ok(result) = result {
    ///     assert_eq!(result.get(), (&2, &mut 0));
    /// }
    ///
    /// let result = hashmap.clear();
    /// assert_eq!(result, 2);
    ///
    /// let result = hashmap.get(&1);
    /// assert!(result.is_none());
    ///
    /// let result = hashmap.get(&2);
    /// assert!(result.is_none());
    /// ```
    pub fn clear(&self) -> usize {
        self.retain(|_, _| false).1
    }

    /// Returns the number of entries in the HashMap.
    ///
    /// It scans the entire metadata cell array to calculate the number of valid entries,
    /// making its time complexity O(N).
    /// Apart from being inefficient, it may return a smaller number when the HashMap is being resized.
    ///
    /// # Examples
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let result = hashmap.insert(1, 0);
    /// assert!(result.is_ok());
    ///
    /// let result = hashmap.len();
    /// assert_eq!(result, 1);
    /// ```
    pub fn len(&self) -> usize {
        self.num_entries()
    }

    /// Returns the capacity of the HashMap.
    ///
    /// # Examples
    /// ```
    /// use scc::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashmap: HashMap<u64, u32, RandomState> = HashMap::new(1000000, RandomState::new());
    /// assert_eq!(hashmap.capacity(), 1048576);
    /// ```
    pub fn capacity(&self) -> usize {
        self.num_slots()
    }

    /// Returns an Accessor.
    ///
    /// It is guaranteed to go through all the key-value pairs pertaining in the HashMap at the moment,
    /// however the same key-value pair can be visited more than once if the HashMap is being resized.
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
    /// }
    ///
    /// let mut iter = hashmap.iter();
    /// assert_eq!(iter.next(), Some((&1, &mut 0)));
    /// assert_eq!(iter.next(), None);
    ///
    /// for iter in hashmap.iter() {
    ///     assert_eq!(iter, (&1, &mut 0));
    /// }
    /// ```
    pub fn iter(&self) -> Accessor<K, V, H> {
        Accessor {
            hash_map: &self,
            array_ptr: std::ptr::null(),
            cell_index: 0,
            cell_locker: None,
            cell_iterator: None,
            guard: None,
        }
    }

    /// Locks a Cell for inserting a new key-value pair.
    fn lock(&self, key: K) -> (Accessor<K, V, H>, K, u8) {
        let (hash, partial_hash) = self.hash(&key);
        let mut resize_triggered = false;
        loop {
            let accessor = self.acquire(&key, hash, partial_hash);
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
    fn acquire<'h, Q>(&'h self, key: &Q, hash: u64, partial_hash: u8) -> Accessor<'h, K, V, H>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        // The proper guard is used to read the array pointer.
        let guard = crossbeam_epoch::pin();
        // Once a Cell is locked, protection is not required.
        let unprotected_guard = unsafe { crossbeam_epoch::unprotected() };

        // It is guaranteed that the thread reads a consistent snapshot of the current and
        // old array pair by a release fence in the resize function, hence the following
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
            let current_array = self.array.load(Acquire, &guard);
            let current_array_ref = unsafe { &*current_array.as_raw() };
            let old_array = current_array_ref.old_array(&guard);
            if !old_array.is_null() {
                if current_array_ref.partial_rehash(|key| self.hash(key), |_, _| None, &guard) {
                    continue;
                }
                let old_array_ref = unsafe { &*old_array.as_raw() };
                let cell_index = old_array_ref.calculate_cell_index(hash);
                if let Some(mut locker) =
                    CellLocker::lock(old_array_ref.cell(cell_index), unprotected_guard)
                {
                    if let Some(iterator) =
                        locker.cell_ref().get(key, partial_hash, unprotected_guard)
                    {
                        let iterator = Some(unsafe {
                            std::mem::transmute::<_, CellIterator<'h, K, V, CELL_SIZE, false>>(
                                iterator,
                            )
                        });
                        return Accessor {
                            hash_map: &self,
                            array_ptr: old_array.as_raw(),
                            cell_index,
                            cell_locker: Some(locker),
                            cell_iterator: iterator,
                            guard: None,
                        };
                    }
                    // Kills the Cell.
                    current_array_ref.kill_cell(
                        &mut locker,
                        Self::cell_array_ref(old_array),
                        cell_index,
                        &|key| self.hash(key),
                        &|_, _| None,
                        &guard,
                    );
                }
            }
            let cell_index = current_array_ref.calculate_cell_index(hash);
            if let Some(locker) =
                CellLocker::lock(current_array_ref.cell(cell_index), unprotected_guard)
            {
                if let Some(iterator) = locker.cell_ref().get(key, partial_hash, unprotected_guard)
                {
                    let iterator = Some(unsafe {
                        std::mem::transmute::<_, CellIterator<'h, K, V, CELL_SIZE, false>>(iterator)
                    });
                    return Accessor {
                        hash_map: &self,
                        array_ptr: current_array.as_raw(),
                        cell_index,
                        cell_locker: Some(locker),
                        cell_iterator: iterator,
                        guard: None,
                    };
                }
                return Accessor {
                    hash_map: &self,
                    array_ptr: current_array.as_raw(),
                    cell_index,
                    cell_locker: Some(locker),
                    cell_iterator: None,
                    guard: None,
                };
            }

            // Reaching here indicates that self.array is updated.
        }
    }

    /// Erases a key-value pair owned by the Accessor.
    fn erase<'h>(&'h self, mut accessor: Accessor<'h, K, V, H>) -> V {
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
        let guard = unsafe { crossbeam_epoch::unprotected() };
        let current_array = self.array.load(Acquire, guard);
        let current_array_ref = Self::cell_array_ref(current_array);
        current_array_ref.drop_old_array(true, guard);
        let array = self.array.swap(Shared::null(), Relaxed, guard);
        if !array.is_null() {
            drop(unsafe { array.into_owned() });
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
    fn cell_array_ptr(&self) -> &Atomic<CellArray<K, V, CELL_SIZE, false>> {
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
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    hash_map: &'h HashMap<K, V, H>,
    increment: usize,
}

impl<'h, K, V, H> Drop for Ticket<'h, K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    fn drop(&mut self) {
        let result = self
            .hash_map
            .additional_capacity
            .fetch_sub(self.increment, Relaxed);
        let guard = crossbeam_epoch::pin();
        self.hash_map.resize(&guard);
        debug_assert!(result >= self.increment);
    }
}

/// Accessor owns a key-value pair in the HashMap, and it implements Iterator.
///
/// It is !Send, thus disallowing other threads to have references to it.
/// It acquires an exclusive lock on the Cell managing the key.
/// A thread having multiple Accessor instances poses a possibility of deadlock.
pub struct Accessor<'h, K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    hash_map: &'h HashMap<K, V, H>,
    array_ptr: *const CellArray<K, V, CELL_SIZE, false>,
    cell_index: usize,
    cell_locker: Option<CellLocker<'h, K, V, CELL_SIZE, false>>,
    cell_iterator: Option<CellIterator<'h, K, V, CELL_SIZE, false>>,
    guard: Option<&'h Guard>,
}

impl<'h, K, V, H> Accessor<'h, K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    /// Returns a reference to the key-value pair.
    ///
    /// # Examples
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    ///
    /// let result = hashmap.get(&1);
    /// assert!(result.is_none());
    ///
    /// let result = hashmap.insert(1, 0);
    /// if let Ok(result) = result {
    ///     assert_eq!(result.get(), (&1, &mut 0));
    ///     (*result.get().1) = 2;
    /// }
    ///
    /// let result = hashmap.get(&1);
    /// assert_eq!(result.unwrap().get(), (&1, &mut 2));
    /// ```
    pub fn get(&self) -> (&'h K, &'h mut V) {
        let itr_ref = self.cell_iterator.as_ref().unwrap();
        let entry_ref = itr_ref.get().unwrap();
        self.hash_map.entry(entry_ref as *const _)
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

impl<'h, K, V, H> Iterator for Accessor<'h, K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    type Item = (&'h K, &'h mut V);
    fn next(&mut self) -> Option<Self::Item> {
        if self.guard.is_none() {
            // It always owns a CellLocker preventing the array from being dropped,
            // therefore a dummy Guard is sufficient.
            self.guard
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
                            self.guard.as_ref().unwrap(),
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
                        self.guard.as_ref().unwrap(),
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
                self.guard.as_ref().unwrap(),
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
                    .load(Acquire, self.guard.as_ref().unwrap());
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
                if let Some(locker) = CellLocker::lock(cell_ref, self.guard.as_ref().unwrap()) {
                    self.cell_index = index;
                    self.cell_locker.replace(locker);
                    self.cell_iterator
                        .replace(CellIterator::new(cell_ref, self.guard.as_ref().unwrap()));
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

impl<'h, K, V, H> FusedIterator for Accessor<'h, K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
}
