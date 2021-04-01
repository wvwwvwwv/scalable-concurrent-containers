pub mod array;
pub mod cell;
pub mod link;

use crate::common::cell_array::CellSize;
use array::Array;
use cell::{Cell, CellLocker, CellReader};
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::convert::TryInto;
use std::hash::{BuildHasher, Hash, Hasher};
use std::iter::FusedIterator;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicBool, AtomicUsize};

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
    array: Atomic<Array<K, V>>,
    minimum_capacity: usize,
    additional_capacity: AtomicUsize,
    resize_mutex: AtomicBool,
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
            array: Atomic::new(Array::<K, V>::new(DEFAULT_CAPACITY, Atomic::null())),
            minimum_capacity: DEFAULT_CAPACITY,
            additional_capacity: AtomicUsize::new(0),
            resize_mutex: AtomicBool::new(false),
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
        let array = Owned::new(Array::<K, V>::new(initial_capacity, Atomic::null()));
        let current_capacity = array.num_cell_entries();
        HashMap {
            array: Atomic::from(array),
            minimum_capacity: current_capacity,
            additional_capacity: AtomicUsize::new(0),
            resize_mutex: AtomicBool::new(false),
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
    /// let hashmap: HashMap<u64, u32, RandomState> = HashMap::new(1000, RandomState::new());
    /// assert_eq!(hashmap.capacity(), 1024);
    ///
    /// let ticket = hashmap.reserve(10000);
    /// assert!(ticket.is_some());
    /// assert_eq!(hashmap.capacity(), 16384);
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
                    self.resize();
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
    pub fn insert(&self, key: K, value: V) -> Result<Accessor<K, V, H>, (Accessor<K, V, H>, K, V)> {
        match self.lock(key) {
            Ok((mut accessor, key, partial_hash)) => {
                let (sub_index, entry_array_link_ptr, entry_ptr) =
                    accessor.cell_locker.insert(key, partial_hash, value);
                accessor.sub_index = sub_index;
                accessor.entry_array_link_ptr = entry_array_link_ptr;
                accessor.entry_ptr = entry_ptr;
                Ok(accessor)
            }
            Err((accessor, key)) => Err((accessor, key, value)),
        }
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
    pub fn emplace<F: FnOnce() -> V>(
        &self,
        key: K,
        constructor: F,
    ) -> Result<Accessor<K, V, H>, (Accessor<K, V, H>, K)> {
        match self.lock(key) {
            Ok((mut accessor, key, partial_hash)) => {
                let (sub_index, entry_array_link_ptr, entry_ptr) =
                    accessor
                        .cell_locker
                        .insert(key, partial_hash, constructor());
                accessor.sub_index = sub_index;
                accessor.entry_array_link_ptr = entry_array_link_ptr;
                accessor.entry_ptr = entry_ptr;
                Ok(accessor)
            }
            Err((accessor, key)) => Err((accessor, key)),
        }
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
    pub fn upsert(&self, key: K, value: V) -> Accessor<K, V, H> {
        match self.insert(key, value) {
            Ok(result) => result,
            Err((accessor, _, value)) => {
                *self.entry(accessor.entry_ptr).1 = value;
                accessor
            }
        }
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
        if accessor.entry_ptr.is_null() {
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
            let current_array_ref = Self::array(current_array_shared);
            let old_array_shared = current_array_ref.old_array(&guard);
            if !old_array_shared.is_null()
                && !current_array_ref.partial_rehash(|key| self.hash(key), &guard)
            {
                let old_array_ref = Self::array(old_array_shared);
                let cell_index = old_array_ref.calculate_cell_index(hash);
                let reader =
                    CellReader::read(old_array_ref.cell(cell_index), key, partial_hash, &guard);
                if let Some((key, value)) = reader.get() {
                    return Some(f(key.borrow(), value));
                }
            }
            let cell_index = current_array_ref.calculate_cell_index(hash);
            let reader = CellReader::read(
                current_array_ref.cell(cell_index),
                key,
                partial_hash,
                &guard,
            );
            if let Some((key, value)) = reader.get() {
                return Some(f(key.borrow(), value));
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
        let mut cursor = self.iter();
        while let Some((key, value)) = cursor.next() {
            if !f(key, value) {
                cursor.erase_on_next = true;
                removed_entries += 1;
            } else {
                retained_entries += 1;
            }
        }

        let guard = crossbeam_epoch::pin();
        let current_array = self.array.load(Acquire, &guard);
        let current_array_ref = Self::array(current_array);
        if retained_entries <= current_array_ref.num_cell_entries() / 8 {
            self.resize();
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
        let guard = crossbeam_epoch::pin();
        let current_array = self.array.load(Acquire, &guard);
        let current_array_ref = Self::array(current_array);
        let mut num_entries = 0;
        for i in 0..current_array_ref.array_size() {
            num_entries += current_array_ref.cell(i).size();
        }
        let old_array = current_array_ref.old_array(&guard);
        if !old_array.is_null() {
            let old_array_ref = Self::array(old_array);
            for i in 0..old_array_ref.array_size() {
                num_entries += old_array_ref.cell(i).size();
            }
        }
        num_entries
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
        let guard = crossbeam_epoch::pin();
        let current_array = self.array.load(Acquire, &guard);
        let current_array_ref = Self::array(current_array);
        if !current_array_ref.old_array(&guard).is_null() {
            current_array_ref.partial_rehash(|key| self.hash(key), &guard);
        }
        current_array_ref.num_cell_entries()
    }

    /// Returns a reference to its build hasher.
    ///
    /// # Examples
    /// ```
    /// use scc::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashmap: HashMap<u64, u32> = Default::default();
    /// let result: &RandomState = hashmap.hasher();
    /// ```
    pub fn hasher(&self) -> &H {
        &self.build_hasher
    }

    /// Returns a Cursor.
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
    pub fn iter(&self) -> Cursor<K, V, H> {
        let (cell_locker, array_ptr, cell_index) = self.first();
        if let Some(cell_locker) = cell_locker {
            if let Some(cursor) = self.pick(cell_locker, array_ptr, cell_index) {
                return cursor;
            }
        }
        Cursor {
            accessor: None,
            array_ptr: std::ptr::null(),
            activated: false,
            erase_on_next: false,
        }
    }

    /// Returns the hash value of the given key.
    fn hash<Q>(&self, key: &Q) -> (u64, u8)
    where
        K: Borrow<Q>,
        Q: Hash + ?Sized,
    {
        // Generates a hash value.
        let mut h = self.build_hasher.build_hasher();
        key.hash(&mut h);
        let mut hash = h.finish();

        // Bitmix: https://mostlymangling.blogspot.com/2019/01/better-stronger-mixer-and-test-procedure.html
        hash = hash ^ (hash.rotate_right(25) ^ hash.rotate_right(50));
        hash = hash.overflowing_mul(0xA24BAED4963EE407u64).0;
        hash = hash ^ (hash.rotate_right(24) ^ hash.rotate_right(49));
        hash = hash.overflowing_mul(0x9FB21C651E98DF25u64).0;
        hash = hash ^ (hash >> 28);
        (hash, (hash & ((1 << 8) - 1)).try_into().unwrap())
    }

    /// Locks a Cell for inserting a new key-value pair.
    fn lock(&self, key: K) -> Result<(Accessor<K, V, H>, K, u8), (Accessor<K, V, H>, K)> {
        let (hash, partial_hash) = self.hash(&key);
        let mut resize_triggered = false;
        loop {
            let accessor = self.acquire(&key, hash, partial_hash);
            if !accessor.entry_ptr.is_null() {
                return Err((accessor, key));
            }
            if !resize_triggered
                && accessor.cell_index < Cell::<K, V>::cell_size()
                && accessor.cell_locker.size() >= Cell::<K, V>::cell_size()
            {
                drop(accessor);
                resize_triggered = true;
                let guard = crossbeam_epoch::pin();
                let current_array = self.array.load(Acquire, &guard);
                let current_array_ref = Self::array(current_array);
                if current_array_ref.old_array(&guard).is_null() {
                    // Triggers resize if the estimated load factor is greater than 7/8.
                    let sample_size = current_array_ref.sample_size();
                    let threshold = sample_size * (Cell::<K, V>::cell_size() / 8) * 7;
                    let mut num_entries = 0;
                    for i in 0..sample_size {
                        num_entries += current_array_ref.cell(i).size();
                        if num_entries > threshold {
                            self.resize();
                            break;
                        }
                    }
                }
                continue;
            }
            return Ok((accessor, key, partial_hash));
        }
    }

    /// Acquires a cell.
    fn acquire<'h, Q>(&'h self, key: &Q, hash: u64, partial_hash: u8) -> Accessor<'h, K, V, H>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let guard = crossbeam_epoch::pin();

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
            let current_array_ref = Self::array(current_array);
            let old_array = current_array_ref.old_array(&guard);
            if !old_array.is_null() {
                if current_array_ref.partial_rehash(|key| self.hash(key), &guard) {
                    continue;
                }
                let (mut cell_locker, cell_index, sub_index, entry_array_link_ptr, entry_ptr) =
                    self.search(key, hash, partial_hash, old_array.as_raw(), &guard);
                if !entry_ptr.is_null() {
                    return Accessor {
                        hash_map: &self,
                        cell_locker,
                        cell_index,
                        sub_index,
                        entry_array_link_ptr,
                        entry_ptr,
                    };
                } else if !cell_locker.killed() {
                    // Kills the Cell.
                    current_array_ref.kill_cell(
                        &mut cell_locker,
                        Self::array(old_array),
                        cell_index,
                        &|key| self.hash(key),
                        &guard,
                    );
                }
            }
            let (cell_locker, cell_index, sub_index, entry_array_link_ptr, entry_ptr) =
                self.search(key, hash, partial_hash, current_array.as_raw(), &guard);
            if !cell_locker.killed() {
                return Accessor {
                    hash_map: &self,
                    cell_locker,
                    cell_index,
                    sub_index,
                    entry_array_link_ptr,
                    entry_ptr,
                };
            }
            // Reaching here indicates that self.array is updated.
        }
    }

    /// Erases a key-value pair owned by the Accessor.
    fn erase<'h>(&'h self, mut accessor: Accessor<'h, K, V, H>) -> V {
        let value = Array::<K, V>::extract_key_value(accessor.entry_ptr).1;
        accessor.cell_locker.remove(
            false,
            accessor.sub_index,
            accessor.entry_array_link_ptr,
            accessor.entry_ptr,
        );
        if accessor.cell_locker.empty() && accessor.cell_index < Cell::<K, V>::cell_size() {
            drop(accessor);
            let guard = crossbeam_epoch::pin();
            let current_array = self.array.load(Acquire, &guard);
            let current_array_ref = Self::array(current_array);
            if current_array_ref.old_array(&guard).is_null()
                && current_array_ref.num_cell_entries()
                    > self.minimum_capacity + self.additional_capacity.load(Relaxed)
            {
                // Triggers resize if the estimated load factor is smaller than 1/16.
                let sample_size = current_array_ref.sample_size();
                let mut num_entries = 0;
                for i in 0..sample_size {
                    num_entries += current_array_ref.cell(i).size();
                    if num_entries >= sample_size * Cell::<K, V>::cell_size() / 16 {
                        return value;
                    }
                }
                self.resize();
            }
        }
        value
    }

    /// Searches a cell for the key.
    fn search<'c, Q>(
        &self,
        key: &Q,
        hash: u64,
        partial_hash: u8,
        array_ptr: *const Array<K, V>,
        guard: &Guard,
    ) -> (
        CellLocker<'c, K, V>,
        usize,
        u8,
        *const link::EntryArrayLink<K, V>,
        *const (K, V),
    )
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let array_ref = unsafe { &(*array_ptr) };
        let cell_index = array_ref.calculate_cell_index(hash);
        let cell_locker = CellLocker::lock(array_ref.cell(cell_index), guard);
        if !cell_locker.killed() && !cell_locker.empty() {
            if let Some((sub_index, entry_array_link_ptr, entry_ptr)) =
                cell_locker.search(key, partial_hash)
            {
                return (
                    cell_locker,
                    cell_index,
                    sub_index,
                    entry_array_link_ptr,
                    entry_ptr,
                );
            }
        }
        (
            cell_locker,
            cell_index,
            0,
            std::ptr::null(),
            std::ptr::null(),
        )
    }

    /// Returns the first valid cell.
    fn first(&self) -> (Option<CellLocker<K, V>>, *const Array<K, V>, usize) {
        let guard = crossbeam_epoch::pin();

        // An acquire fence is required to correctly load the contents of the array.
        let mut current_array = self.array.load(Acquire, &guard);
        loop {
            let old_array = Self::array(current_array).old_array(&guard);
            for array_ptr in [old_array.as_raw(), current_array.as_raw()].iter() {
                if array_ptr.is_null() {
                    continue;
                }
                let array_ref = unsafe { &(**array_ptr) };
                let num_cells = array_ref.array_size();
                for cell_index in 0..num_cells {
                    let cell_locker = CellLocker::lock(array_ref.cell(cell_index), &guard);
                    if !cell_locker.empty() {
                        // once a valid cell is locked, the array is guaranteed to retain
                        return (Some(cell_locker), *array_ptr, cell_index);
                    }
                }
            }
            // No valid cells found.
            let current_array_new = self.array.load(Acquire, &guard);
            if current_array == current_array_new {
                break;
            }

            // Resized in the meantime.
            current_array = current_array_new;
        }
        (None, std::ptr::null(), 0)
    }

    /// Returns the next valid cell.
    fn next(&self, array_ptr: *const Array<K, V>, current_index: usize) -> Option<Cursor<K, V, H>> {
        let guard = crossbeam_epoch::pin();

        // An acquire fence is required to correctly load the contents of the array.
        let current_array = self.array.load(Acquire, &guard);
        // Bypasses the lifetime checker by not calling Shared::deref().
        let current_array_ref = unsafe { &(*current_array.as_raw()) };
        let old_array = current_array_ref.old_array(&guard);

        // Either one of the two arrays must match with array_ptr.
        debug_assert!(array_ptr == current_array.as_raw() || array_ptr == old_array.as_raw());

        if old_array.as_raw() == array_ptr {
            // Bypasses the lifetime checker by not calling Shared::deref().
            let old_array_ref = unsafe { &(*old_array.as_raw()) };
            let num_cells = old_array_ref.array_size();
            for cell_index in (current_index + 1)..num_cells {
                let cell_locker = CellLocker::lock(old_array_ref.cell(cell_index), &guard);
                if !cell_locker.killed() && !cell_locker.empty() {
                    if let Some(cursor) = self.pick(cell_locker, old_array.as_raw(), cell_index) {
                        return Some(cursor);
                    }
                }
            }
        }

        let mut new_array = Shared::<Array<K, V>>::null();
        let num_cells = current_array_ref.array_size();
        let start_index = if old_array.as_raw() == array_ptr {
            0
        } else {
            current_index + 1
        };
        for cell_index in (start_index)..num_cells {
            let cell_locker = CellLocker::lock(current_array_ref.cell(cell_index), &guard);
            if !cell_locker.killed() && !cell_locker.empty() {
                if let Some(cursor) = self.pick(cell_locker, current_array.as_raw(), cell_index) {
                    return Some(cursor);
                }
            } else if cell_locker.killed() && new_array.is_null() {
                new_array = self.array.load(Acquire, &guard);
            }
        }

        if !new_array.is_null() {
            // Bypasses the lifetime checker by not calling Shared::deref().
            let new_array_ref = unsafe { &(*new_array.as_raw()) };
            let num_cells = new_array_ref.array_size();
            for cell_index in 0..num_cells {
                let cell_locker = CellLocker::lock(new_array_ref.cell(cell_index), &guard);
                if !cell_locker.killed() && !cell_locker.empty() {
                    if let Some(cursor) = self.pick(cell_locker, new_array.as_raw(), cell_index) {
                        return Some(cursor);
                    }
                }
            }
        }
        None
    }

    /// Picks a key-value pair entry using the given CellLocker.
    fn pick<'h>(
        &'h self,
        cell_locker: CellLocker<'h, K, V>,
        array_ptr: *const Array<K, V>,
        cell_index: usize,
    ) -> Option<Cursor<'h, K, V, H>> {
        if let Some((sub_index, entry_array_link_ptr, entry_ptr)) = cell_locker.first() {
            return Some(Cursor {
                accessor: Some(Accessor {
                    hash_map: &self,
                    cell_locker,
                    cell_index,
                    sub_index,
                    entry_array_link_ptr,
                    entry_ptr,
                }),
                array_ptr,
                activated: false,
                erase_on_next: false,
            });
        }
        None
    }

    /// Resizes the array.
    fn resize(&self) {
        // Initial rough size estimation using a small number of cells.
        let guard = crossbeam_epoch::pin();
        let current_array = self.array.load(Acquire, &guard);
        let current_array_ref = Self::array(current_array);
        let old_array = current_array_ref.old_array(&guard);
        if !old_array.is_null() {
            let old_array_removed = current_array_ref.partial_rehash(|key| self.hash(key), &guard);
            if !old_array_removed {
                return;
            }
        }

        if !self.resize_mutex.swap(true, Acquire) {
            let memory_ordering = Relaxed;
            let mut mutex_guard = scopeguard::guard(memory_ordering, |memory_ordering| {
                self.resize_mutex.store(false, memory_ordering);
            });
            if current_array != self.array.load(Acquire, &guard) {
                return;
            }

            // The resizing policies are as follows.
            //  - The load factor reaches 7/8, then the array grows up to 64x.
            //  - The load factor reaches 1/16, then the array shrinks to fit.
            let capacity = current_array_ref.num_cell_entries();
            let num_cells = current_array_ref.array_size();
            let num_cells_to_sample = (num_cells / 8)
                .max(DEFAULT_CAPACITY / Cell::<K, V>::cell_size())
                .min(4096);
            let estimated_num_entries = self.estimate(current_array_ref, num_cells_to_sample);
            let new_capacity = if estimated_num_entries >= (capacity / 8) * 7 {
                let max_capacity = 1usize << (std::mem::size_of::<usize>() * 8 - 1);
                if capacity == max_capacity {
                    // Do not resize if the capacity cannot be increased.
                    capacity
                } else if estimated_num_entries <= (capacity / 8) * 9 {
                    // Doubles if the estimated size marginally exceeds the capacity.
                    capacity * 2
                } else {
                    // Grows up to 64x
                    let new_capacity_candidate = estimated_num_entries
                        .next_power_of_two()
                        .min(max_capacity / 2)
                        * 2;
                    if new_capacity_candidate / capacity
                        > (1 << Cell::<K, V>::max_resizing_factor())
                    {
                        capacity * (1 << Cell::<K, V>::max_resizing_factor())
                    } else {
                        new_capacity_candidate
                    }
                }
            } else if estimated_num_entries <= capacity / 8 {
                // Shrinks to fit.
                estimated_num_entries
                    .max(self.minimum_capacity + self.additional_capacity.load(Relaxed))
                    .next_power_of_two()
            } else {
                capacity
            };

            // Array::new may not be able to allocate the requested number of cells.
            if new_capacity != capacity {
                self.array.store(
                    Owned::new(Array::<K, V>::new(
                        new_capacity,
                        Atomic::from(current_array),
                    )),
                    Release,
                );
                // The release fence assures that future calls to the function see the latest state.
                *mutex_guard = Release;
            }
        }
    }

    /// Estimates the number of entries using the given number of cells.
    fn estimate(&self, current_array_ref: &Array<K, V>, num_cells_to_sample: usize) -> usize {
        let mut num_entries = 0;
        for i in 0..num_cells_to_sample {
            num_entries += current_array_ref.cell(i).size();
        }
        num_entries * (current_array_ref.array_size() / num_cells_to_sample)
    }

    /// Returns a reference to the Array instance.
    fn array(array: Shared<Array<K, V>>) -> &Array<K, V> {
        unsafe { array.deref() }
    }

    /// Returns a reference to the entry.
    fn entry(&self, entry_ptr: *const (K, V)) -> (&K, &mut V) {
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
        let current_array_ref = Self::array(current_array);
        current_array_ref.drop_old_array(true, guard);
        let array = self.array.swap(Shared::null(), Relaxed, guard);
        if !array.is_null() {
            drop(unsafe { array.into_owned() });
        }
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
        self.hash_map.resize();
        debug_assert!(result >= self.increment);
    }
}

/// Accessor owns a key-value pair in the HashMap.
///
/// It is !Send, thus disallowing other threads to have references to it.
/// It acquires an exclusive lock on the Cell managing the key.
/// A thread having multiple Accessor or Cursor instances poses a possibility of deadlock.
pub struct Accessor<'h, K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    hash_map: &'h HashMap<K, V, H>,
    cell_locker: CellLocker<'h, K, V>,
    cell_index: usize,
    sub_index: u8,
    entry_array_link_ptr: *const link::EntryArrayLink<K, V>,
    entry_ptr: *const (K, V),
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
    pub fn get(&'h self) -> (&'h K, &'h mut V) {
        self.hash_map.entry(self.entry_ptr)
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
        if self.entry_ptr.is_null() {
            return None;
        }
        Some(self.hash_map.erase(self))
    }
}

/// Cursor scans all the key-value pairs in the HashMap.
///
/// It is guaranteed to visit all the key-value pairs that outlive the Cursor.
/// However, the same key-value pair can be visited more than once.
///
/// It acquires an exclusive lock on the Cell that is currently being visited.
/// A thread having multiple Accessor or Cursor instances poses a possibility of deadlock.
pub struct Cursor<'h, K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    accessor: Option<Accessor<'h, K, V, H>>,
    array_ptr: *const Array<K, V>,
    activated: bool,
    erase_on_next: bool,
}

impl<'h, K, V, H> Iterator for Cursor<'h, K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    type Item = (&'h K, &'h mut V);
    fn next(&mut self) -> Option<Self::Item> {
        if !self.activated {
            self.activated = true;
        } else if self.accessor.is_some() {
            let erase = self.erase_on_next;
            if erase {
                self.erase_on_next = false;
            }
            if let Some((next_sub_index, next_entry_array_link_ptr, next_entry_ptr)) =
                self.accessor.as_mut().map_or_else(
                    || None,
                    |accessor| {
                        accessor.cell_locker.next(
                            erase,
                            true,
                            accessor.sub_index,
                            accessor.entry_array_link_ptr,
                            accessor.entry_ptr,
                        )
                    },
                )
            {
                self.accessor.as_mut().map_or_else(
                    || (),
                    |accessor| {
                        accessor.sub_index = next_sub_index;
                        accessor.entry_array_link_ptr = next_entry_array_link_ptr;
                        accessor.entry_ptr = next_entry_ptr;
                    },
                );
            } else {
                let current_array_ptr = self.array_ptr;
                let cursor = self.accessor.as_ref().map_or_else(
                    || None,
                    |accessor| {
                        accessor
                            .hash_map
                            .next(current_array_ptr, accessor.cell_index)
                    },
                );
                self.accessor.take();
                if let Some(mut cursor) = cursor {
                    self.accessor = cursor.accessor.take();
                    self.array_ptr = cursor.array_ptr;
                }
            }
        }
        if let Some(accessor) = &self.accessor {
            return Some(accessor.hash_map.entry(accessor.entry_ptr));
        }
        None
    }
}

impl<'h, K, V, H> FusedIterator for Cursor<'h, K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
}
