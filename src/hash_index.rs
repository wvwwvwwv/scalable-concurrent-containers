use crate::common::cell::{CellIterator, CellLocker};
use crate::common::cell_array::CellArray;
use crate::common::hash_table::HashTable;

use crossbeam_epoch::{Atomic, Guard, Shared};
use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::iter::FusedIterator;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, Relaxed};

const CELL_SIZE: usize = 32;
const DEFAULT_CAPACITY: usize = 64;

/// A scalable concurrent hash index data structure.
///
/// scc::HashIndex is a concurrent hash index data structure that is optimized for read operations.
/// The key characteristics of scc::HashIndex are similar to that of scc::HashMap.
///
/// ## The key differences between scc::HashIndex and scc::HashMap
/// * Lock-free-read: read and scan operations do not entail shared data modification.
/// * Immutability: the data in the container is treated immutable until it becomes unreachable.
///
/// ## The key statistics for scc::HashIndex
/// * The expected size of metadata for a single key-value pair: 2-byte.
/// * The expected number of atomic operations required for an operation on a single key: 0 or 2.
/// * The expected number of atomic variables accessed during a single key operation: 1.
/// * The number of entries managed by a single metadata cell without a linked list: 32.
/// * The number of entries a single linked list entry manages: 32.
/// * The expected maximum linked list length when resize is triggered: log(capacity) / 8.
pub struct HashIndex<K, V, H = RandomState>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
    array: Atomic<CellArray<K, V, CELL_SIZE, true>>,
    minimum_capacity: usize,
    resizing_flag: AtomicBool,
    build_hasher: H,
}

impl<K, V> Default for HashIndex<K, V, RandomState>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
{
    /// Creates a HashIndex instance with the default parameters.
    ///
    /// The default hash builder is RandomState, and the default capacity is 64.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails.
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32, _> = Default::default();
    /// ```
    fn default() -> Self {
        HashIndex {
            array: Atomic::new(CellArray::<K, V, CELL_SIZE, true>::new(
                DEFAULT_CAPACITY,
                Atomic::null(),
            )),
            minimum_capacity: DEFAULT_CAPACITY,
            resizing_flag: AtomicBool::new(false),
            build_hasher: RandomState::new(),
        }
    }
}

impl<K, V, H> HashIndex<K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
    /// Creates an empty HashIndex instance with the given capacity and build hasher.
    ///
    /// The actual capacity is equal to or greater than the given capacity.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashindex: HashIndex<u64, u32, RandomState> = HashIndex::new(1000, RandomState::new());
    ///
    /// let result = hashindex.capacity();
    /// assert_eq!(result, 1024);
    ///
    ///
    /// let hashindex: HashIndex<u64, u32, _> = Default::default();
    /// let result = hashindex.capacity();
    /// assert_eq!(result, 64);
    /// ```
    pub fn new(capacity: usize, build_hasher: H) -> HashIndex<K, V, H> {
        let initial_capacity = capacity.max(DEFAULT_CAPACITY);
        HashIndex {
            array: Atomic::new(CellArray::<K, V, CELL_SIZE, true>::new(
                initial_capacity,
                Atomic::null(),
            )),
            minimum_capacity: initial_capacity,
            resizing_flag: AtomicBool::new(false),
            build_hasher,
        }
    }

    /// Inserts a key-value pair into the HashIndex.
    ///
    /// Returns an error with the given key-value pair attached if the key exists.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails, or the number of entries in the target cell reaches u32::MAX.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = Default::default();
    ///
    /// let result = hashindex.insert(1, 0);
    /// assert!(result.is_ok());
    ///
    /// let result = hashindex.insert(1, 1);
    /// if let Err((key, value)) = result {
    ///     assert_eq!(key, 1);
    ///     assert_eq!(value, 1);
    /// } else {
    ///     assert!(false);
    /// }
    /// ```
    pub fn insert(&self, key: K, value: V) -> Result<(), (K, V)> {
        let guard = crossbeam_epoch::pin();
        let (cell_locker, key, partial_hash) = self.acquire(key, &guard);
        if let Some((key, value)) = cell_locker.insert(key, value, partial_hash, &guard).1 {
            return Err((key, value));
        }
        Ok(())
    }

    /// Removes a key-value pair.
    ///
    /// Returns false if the key does not exist.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = Default::default();
    ///
    /// let result = hashindex.insert(1, 0);
    /// assert!(result.is_ok());
    ///
    /// let result = hashindex.remove(&1);
    /// assert!(result);
    /// ```
    pub fn remove<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (hash, partial_hash) = self.hash(key);
        let guard = crossbeam_epoch::pin();
        let (cell_locker, cell_index) = self.lock(hash, &guard);
        if cell_locker.mark_removed(key, partial_hash, &guard) {
            if cell_locker.cell_ref().num_entries() == 0 && cell_index < CELL_SIZE {
                drop(cell_locker);
                let current_array = self.array.load(Acquire, &guard);
                let current_array_ref = Self::cell_array_ref(current_array);
                if current_array_ref.old_array(&guard).is_null()
                    && current_array_ref.num_cell_entries() > self.minimum_capacity
                {
                    // Triggers resize if the estimated load factor is smaller than 1/16.
                    let sample_size = current_array_ref.sample_size();
                    let mut num_entries = 0;
                    for i in 0..sample_size {
                        num_entries += current_array_ref.cell(i).num_entries();
                        if num_entries >= sample_size * CELL_SIZE / 16 {
                            return true;
                        }
                    }
                    self.resize(&guard);
                }
            }
            return true;
        }
        false
    }

    /// Reads a key-value pair.
    ///
    /// # Errors
    ///
    /// Returns None if the key does not exist.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = Default::default();
    ///
    /// let result = hashindex.insert(1, 0);
    /// assert!(result.is_ok());
    ///
    /// let result = hashindex.read(&1, |_, &value| value);
    /// if let Some(result) = result {
    ///     assert_eq!(result, 0);
    /// } else {
    ///     assert!(false);
    /// }
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
                && !current_array_ref.partial_rehash(
                    |key| self.hash(key),
                    |key, value| Some((key.clone(), value.clone())),
                    &guard,
                )
            {
                let old_array_ref = Self::cell_array_ref(old_array_shared);
                let cell_index = old_array_ref.calculate_cell_index(hash);
                let cell_ref = old_array_ref.cell(cell_index);
                if let Some(entry) = cell_ref.search(key, partial_hash, &guard) {
                    return Some(f(entry.0.borrow(), &entry.1));
                }
            }
            let cell_index = current_array_ref.calculate_cell_index(hash);
            let cell_ref = current_array_ref.cell(cell_index);
            if let Some(entry) = cell_ref.search(key, partial_hash, &guard) {
                return Some(f(entry.0.borrow(), &entry.1));
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
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = Default::default();
    ///
    /// let result = hashindex.contains(&1);
    /// assert!(!result);
    ///
    /// let result = hashindex.insert(1, 0);
    /// assert!(result.is_ok());
    ///
    /// let result = hashindex.contains(&1);
    /// assert!(result);
    /// ```
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.read(key, |_, _| ()).is_some()
    }

    /// Clears all the key-value pairs.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = Default::default();
    ///
    /// let result = hashindex.insert(1, 0);
    /// assert!(result.is_ok());
    ///
    /// let result = hashindex.len();
    /// assert_eq!(result, 1);
    ///
    /// let result = hashindex.clear();
    /// assert_eq!(result, 1);
    ///
    /// let result = hashindex.len();
    /// assert_eq!(result, 0);
    /// ```
    pub fn clear(&self) -> usize {
        let mut num_removed = 0;
        let guard = crossbeam_epoch::pin();
        let mut current_array_shared = self.array.load(Acquire, &guard);
        loop {
            let current_array_ref = Self::cell_array_ref(current_array_shared);
            let old_array_shared = current_array_ref.old_array(&guard);
            if !old_array_shared.is_null() {
                while !current_array_ref.partial_rehash(
                    |key| self.hash(key),
                    |key, value| Some((key.clone(), value.clone())),
                    &guard,
                ) {
                    continue;
                }
            }
            for index in 0..current_array_ref.array_size() {
                if let Some(mut cell_locker) =
                    CellLocker::lock(current_array_ref.cell(index), &guard)
                {
                    num_removed += cell_locker.cell_ref().num_entries();
                    cell_locker.purge(&guard);
                }
            }
            let new_current_array_shared = self.array.load(Acquire, &guard);
            if current_array_shared == new_current_array_shared {
                self.resize(&guard);
                break;
            }
            current_array_shared = new_current_array_shared;
        }
        num_removed
    }

    /// Returns the number of entries in the HashIndex.
    ///
    /// It scans the entire metadata cell array to calculate the number of valid entries,
    /// making its time complexity O(N).
    /// Apart from being inefficient, it may return a smaller number when the HashIndex is being resized.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = Default::default();
    ///
    /// let result = hashindex.insert(1, 0);
    /// assert!(result.is_ok());
    ///
    /// let result = hashindex.len();
    /// assert_eq!(result, 1);
    /// ```
    pub fn len(&self) -> usize {
        self.num_entries()
    }

    /// Returns the capacity of the HashIndex.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashindex: HashIndex<u64, u32, RandomState> = HashIndex::new(1000000, RandomState::new());
    ///
    /// let result = hashindex.capacity();
    /// assert_eq!(result, 1048576);
    /// ```
    pub fn capacity(&self) -> usize {
        self.num_slots()
    }

    /// Returns a Visitor.
    ///
    /// It is guaranteed to go through all the key-value pairs pertaining in the HashIndex at the moment,
    /// however the same key-value pair can be visited more than once if the HashIndex is being resized.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = Default::default();
    ///
    /// let result = hashindex.insert(1, 0);
    /// assert!(result.is_ok());
    ///
    /// let mut iter = hashindex.iter();
    /// assert_eq!(iter.next(), Some((&1, &0)));
    /// assert_eq!(iter.next(), None);
    ///
    /// for iter in hashindex.iter() {
    ///     assert_eq!(iter, (&1, &0));
    /// }
    /// ```
    pub fn iter(&self) -> Visitor<K, V, H> {
        Visitor {
            hash_index: self,
            current_array: Shared::null(),
            current_index: 0,
            current_cell_iterator: None,
            guard: None,
        }
    }

    /// Acquires a Cell for inserting a new key-value pair.
    fn acquire<'g>(
        &self,
        key: K,
        guard: &'g Guard,
    ) -> (CellLocker<'g, K, V, CELL_SIZE, true>, K, u8) {
        let (hash, partial_hash) = self.hash(&key);
        let mut resize_triggered = false;
        loop {
            let (cell_locker, cell_index) = self.lock(hash, guard);
            if !resize_triggered
                && cell_index < CELL_SIZE
                && cell_locker.cell_ref().num_entries() > (CELL_SIZE / 16) * 15
            {
                drop(cell_locker);
                resize_triggered = true;
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
                            self.resize(guard);
                            break;
                        }
                    }
                }
                continue;
            }
            return (cell_locker, key, partial_hash);
        }
    }

    /// Locks a cell.
    fn lock<'g>(
        &self,
        hash: u64,
        guard: &'g Guard,
    ) -> (CellLocker<'g, K, V, CELL_SIZE, true>, usize) {
        // The description about the loop can be found in HashMap::acquire.
        loop {
            // An acquire fence is required to correctly load the contents of the array.
            let current_array_shared = self.array.load(Acquire, &guard);
            let current_array_ref = Self::cell_array_ref(current_array_shared);
            let old_array_shared = current_array_ref.old_array(&guard);
            if !old_array_shared.is_null() {
                if current_array_ref.partial_rehash(
                    |key| self.hash(key),
                    |key, value| Some((key.clone(), value.clone())),
                    &guard,
                ) {
                    continue;
                }
                let old_array_ref = Self::cell_array_ref(old_array_shared);
                let cell_index = old_array_ref.calculate_cell_index(hash);
                if let Some(mut cell_locker) =
                    CellLocker::lock(old_array_ref.cell(cell_index), guard)
                {
                    // Kills the Cell.
                    current_array_ref.kill_cell(
                        &mut cell_locker,
                        old_array_ref,
                        cell_index,
                        &|key| self.hash(key),
                        &|key, value| Some((key.clone(), value.clone())),
                        &guard,
                    );
                }
            }
            let cell_index = current_array_ref.calculate_cell_index(hash);
            if let Some(cell_locker) = CellLocker::lock(current_array_ref.cell(cell_index), guard) {
                return (cell_locker, cell_index);
            }
            // Reaching here indicates that self.array is updated.
        }
    }
}

impl<K, V, H> Drop for HashIndex<K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
    fn drop(&mut self) {
        // The HashIndex has become unreachable, therefore pinning is unnecessary.
        let guard = unsafe { crossbeam_epoch::unprotected() };
        let current_array = self.array.load(Acquire, guard);
        let current_array_ref = Self::cell_array_ref(current_array);
        current_array_ref.drop_old_array(true, guard);
        let array = self.array.swap(Shared::null(), Relaxed, guard);
        if !array.is_null() {
            let array = unsafe { array.into_owned() };
            for index in 0..array.array_size() {
                if let Some(mut cell_locker) = CellLocker::lock(array.cell(index), guard) {
                    cell_locker.purge(&guard);
                }
            }
        }
    }
}

impl<K, V, H> HashTable<K, V, H, CELL_SIZE, true> for HashIndex<K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
    fn hasher(&self) -> &H {
        &self.build_hasher
    }
    fn cell_array_ptr(&self) -> &Atomic<CellArray<K, V, CELL_SIZE, true>> {
        &self.array
    }
    fn minimum_capacity(&self) -> usize {
        self.minimum_capacity
    }
    fn resizing_flag_ref(&self) -> &AtomicBool {
        &self.resizing_flag
    }
}

/// Visitor traverses all the key-value pairs in the HashIndex.
///
/// It is guaranteed to visit all the key-value pairs that outlive the Visitor.
/// However, the same key-value pair can be visited more than once.
pub struct Visitor<'h, K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
    hash_index: &'h HashIndex<K, V, H>,
    current_array: Shared<'h, CellArray<K, V, CELL_SIZE, true>>,
    current_index: usize,
    current_cell_iterator: Option<CellIterator<'h, K, V, CELL_SIZE, true>>,
    guard: Option<Guard>,
}

impl<'h, K, V, H> Visitor<'h, K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
    fn guard_ref(&self) -> &'h Guard {
        // The Rust type system cannot prove that self.guard outlives.
        unsafe { std::mem::transmute::<_, &'h Guard>(self.guard.as_ref().unwrap()) }
    }
}

impl<'h, K, V, H> Iterator for Visitor<'h, K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
    type Item = (&'h K, &'h V);
    fn next(&mut self) -> Option<Self::Item> {
        if self.guard.is_none() {
            // Starts scanning.
            self.guard.replace(crossbeam_epoch::pin());
            let current_array = self.hash_index.array.load(Acquire, self.guard_ref());
            let current_array_ref = HashIndex::<K, V, H>::cell_array_ref(current_array);
            let old_array = current_array_ref.old_array(self.guard_ref());
            self.current_array = if !old_array.is_null() {
                old_array
            } else {
                current_array
            };
            self.current_cell_iterator.replace(CellIterator::new(
                HashIndex::<K, V, H>::cell_array_ref(self.current_array).cell(0),
                self.guard_ref(),
            ));
        }
        loop {
            if let Some(iterator) = self.current_cell_iterator.as_mut() {
                // Proceeds to the next entry in the Cell.
                if let Some(entry) = iterator.next() {
                    return Some((&entry.0 .0, &entry.0 .1));
                }
            }
            // Proceeds to the next Cell.
            let array_ref = HashIndex::<K, V, H>::cell_array_ref(self.current_array);
            self.current_index += 1;
            if self.current_index == array_ref.array_size() {
                let current_array = self.hash_index.array.load(Acquire, self.guard_ref());
                if self.current_array == current_array {
                    // Finished scanning the entire array.
                    break;
                }
                let current_array_ref = HashIndex::<K, V, H>::cell_array_ref(current_array);
                let old_array = current_array_ref.old_array(self.guard_ref());
                if self.current_array == old_array {
                    // Starts scanning the current array.
                    self.current_array = current_array;
                    self.current_index = 0;
                    self.current_cell_iterator.replace(CellIterator::new(
                        HashIndex::<K, V, H>::cell_array_ref(self.current_array).cell(0),
                        self.guard_ref(),
                    ));
                    continue;
                }
                // Starts from the very beginning.
                self.current_array = if !old_array.is_null() {
                    old_array
                } else {
                    current_array
                };
                self.current_index = 0;
                self.current_cell_iterator.replace(CellIterator::new(
                    HashIndex::<K, V, H>::cell_array_ref(self.current_array).cell(0),
                    self.guard_ref(),
                ));
                continue;
            } else {
                self.current_cell_iterator.replace(CellIterator::new(
                    array_ref.cell(self.current_index),
                    self.guard_ref(),
                ));
            }
        }
        None
    }
}

impl<'h, K, V, H> FusedIterator for Visitor<'h, K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
}
