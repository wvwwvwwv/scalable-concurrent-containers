use crate::common::cell::{CellIterator, CellLocker};
use crate::common::cell_array::CellArray;
use crate::common::hash_table::HashTable;
use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};

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
/// [`HashIndex`] is a concurrent hash index data structure that is optimized for read
/// operations. The key characteristics of [`HashIndex`] are similar to that of
/// [`HashMap`](crate::HashMap).
///
/// ## The key differences between [`HashIndex`] and [`HashMap`](crate::HashMap).
/// * Lock-free-read: read and scan operations do not entail shared data modification.
/// * Immutability: the data in the container is treated immutable until it becomes
///   unreachable.
///
/// ## The key statistics for [`HashIndex`]
/// * The expected size of metadata for a single key-value pair: 2-byte.
/// * The expected number of atomic operations required for an operation on a single key: 2.
/// * The expected number of atomic variables accessed during a single key operation: 1.
/// * The number of entries managed by a single metadata cell without a linked list: 32.
/// * The number of entries a single linked list entry manages: 32.
/// * The expected maximum linked list length when resize is triggered: log(capacity) / 8.
pub struct HashIndex<K, V, H = RandomState>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
    H: BuildHasher,
{
    array: AtomicArc<CellArray<K, V, CELL_SIZE, true>>,
    minimum_capacity: usize,
    resizing_flag: AtomicBool,
    build_hasher: H,
}

impl<K, V> Default for HashIndex<K, V, RandomState>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
{
    /// Creates a [`HashIndex`] with the default parameters.
    ///
    /// The default hash builder is RandomState, and the default capacity is `64`.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32, _> = Default::default();
    /// ```
    fn default() -> Self {
        HashIndex {
            array: AtomicArc::from(Arc::new(CellArray::<K, V, CELL_SIZE, true>::new(
                DEFAULT_CAPACITY,
                AtomicArc::null(),
            ))),
            minimum_capacity: DEFAULT_CAPACITY,
            resizing_flag: AtomicBool::new(false),
            build_hasher: RandomState::new(),
        }
    }
}

impl<K, V, H> HashIndex<K, V, H>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
    H: BuildHasher,
{
    /// Creates an empty [`HashIndex`] with the given capacity and build hasher.
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
            array: AtomicArc::from(Arc::new(CellArray::<K, V, CELL_SIZE, true>::new(
                initial_capacity,
                AtomicArc::null(),
            ))),
            minimum_capacity: initial_capacity,
            resizing_flag: AtomicBool::new(false),
            build_hasher,
        }
    }

    /// Inserts a key-value pair into the [`HashIndex`].
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
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = Default::default();
    ///
    /// assert!(hashindex.insert(1, 0).is_ok());
    /// assert_eq!(hashindex.insert(1, 1).unwrap_err(), (1, 1));
    /// ```
    #[inline]
    pub fn insert(&self, key: K, val: V) -> Result<(), (K, V)> {
        self.insert_entry(key, val)
    }

    /// Removes a key-value pair and returns the key-value-pair if the key exists.
    ///
    /// This methods only marks the entry unreachable, and the instances will be reclaimed
    /// later.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = Default::default();
    ///
    /// assert!(!hashindex.remove(&1));
    /// assert!(hashindex.insert(1, 0).is_ok());
    /// assert!(hashindex.remove(&1));
    /// ```
    #[inline]
    pub fn remove<Q>(&self, key_ref: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.remove_if(key_ref, |_| true)
    }

    /// Removes a key-value pair and returns the key-value-pair if the key exists and the given
    /// condition meets.
    ///
    /// This methods only marks the entry unreachable, and the instances will be reclaimed
    /// later.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = Default::default();
    ///
    /// assert!(hashindex.insert(1, 0).is_ok());
    /// assert!(!hashindex.remove_if(&1, |v| *v == 1));
    /// assert!(hashindex.remove_if(&1, |v| *v == 0));
    /// ```
    #[inline]
    pub fn remove_if<Q, F: FnOnce(&V) -> bool>(&self, key_ref: &Q, condition: F) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (hash, partial_hash) = self.hash(&key_ref);
        let barrier = Barrier::new();
        let (cell_index, locker, iterator) = self.acquire(key_ref, hash, partial_hash, &barrier);
        if let Some(iterator) = iterator {
            let remove = if let Some((_, v)) = iterator.get() {
                condition(v)
            } else {
                false
            };
            if remove {
                if locker.mark_removed(key_ref, partial_hash, &barrier) {
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
                                        return true;
                                    }
                                }
                                self.resize(&barrier);
                            }
                        }
                    }
                    return true;
                }
            }
        }
        false
    }

    /// Reads a key-value pair.
    ///
    /// Returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = Default::default();
    ///
    /// assert!(hashindex.insert(1, 0).is_ok());
    /// assert_eq!(hashindex.read(&1, |_, v| *v).unwrap(), 0);
    /// ```
    #[inline]
    pub fn read<Q, R, F: FnOnce(&K, &V) -> R>(&self, key_ref: &Q, reader: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.read_entry(key_ref, reader)
    }

    /// Checks if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = Default::default();
    ///
    /// assert!(!hashindex.contains(&1));
    /// assert!(hashindex.insert(1, 0).is_ok());
    /// assert!(hashindex.contains(&1));
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
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = Default::default();
    ///
    /// assert!(hashindex.insert(1, 0).is_ok());
    /// assert_eq!(hashindex.clear(), 1);
    /// ```
    pub fn clear(&self) -> usize {
        let mut num_removed = 0;
        let barrier = Barrier::new();
        let mut current_array_ptr = self.array.load(Acquire, &barrier);
        while let Some(current_array_ref) = current_array_ptr.as_ref() {
            if !current_array_ref.old_array(&barrier).is_null() {
                while !current_array_ref.partial_rehash(
                    |key| self.hash(key),
                    |key, value| Some((key.clone(), value.clone())),
                    &barrier,
                ) {
                    current_array_ptr = self.array.load(Acquire, &barrier);
                    continue;
                }
            }
            for index in 0..current_array_ref.array_size() {
                if let Some(mut cell_locker) =
                    CellLocker::lock(current_array_ref.cell(index), &barrier)
                {
                    num_removed += cell_locker.cell_ref().num_entries();
                    cell_locker.purge(&barrier);
                }
            }
            let new_current_array_ptr = self.array.load(Acquire, &barrier);
            if current_array_ptr == new_current_array_ptr {
                self.resize(&barrier);
                break;
            }
            current_array_ptr = new_current_array_ptr;
        }
        num_removed
    }

    /// Returns the number of entries in the [`HashIndex`].
    ///
    /// It scans the entire array to calculate the number of valid entries, making its time
    /// complexity `O(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = Default::default();
    ///
    /// assert!(hashindex.insert(1, 0).is_ok());
    /// assert_eq!(hashindex.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.num_entries(&Barrier::new())
    }

    /// Returns the capacity of the [`HashIndex`].
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashindex: HashIndex<u64, u32, RandomState> = HashIndex::new(1000000, RandomState::new());
    /// assert_eq!(hashindex.capacity(), 1048576);
    /// ```
    pub fn capacity(&self) -> usize {
        self.num_slots(&Barrier::new())
    }

    /// Returns a [`Visitor`] that iterates over all the entries in the [`HashIndex`].
    ///
    /// It is guaranteed to go through all the key-value pairs pertaining in the [`HashIndex`]
    /// at the moment, however the same key-value pair can be visited more than once if the
    /// [`HashIndex`] is being resized.
    ///
    /// It requires the user to supply a reference to a [`Barrier`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = Default::default();
    ///
    /// assert!(hashindex.insert(1, 0).is_ok());
    ///
    /// let barrier = Barrier::new();
    ///
    /// let mut iter = hashindex.iter(&barrier);
    /// let entry_ref = iter.next().unwrap();
    /// assert_eq!(iter.next(), None);
    ///
    /// for iter in hashindex.iter(&barrier) {
    ///     assert_eq!(iter, (&1, &0));
    /// }
    ///
    /// drop(hashindex);
    ///
    /// assert_eq!(entry_ref, (&1, &0));
    /// ```
    pub fn iter<'h, 'b>(&'h self, barrier: &'b Barrier) -> Visitor<'h, 'b, K, V, H> {
        Visitor {
            hash_index: self,
            current_array_ptr: Ptr::null(),
            current_index: 0,
            current_cell_iterator: None,
            barrier_ref: barrier,
        }
    }
}

impl<K, V, H> Drop for HashIndex<K, V, H>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
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

impl<K, V, H> HashTable<K, V, H, CELL_SIZE, true> for HashIndex<K, V, H>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
    H: BuildHasher,
{
    fn hasher(&self) -> &H {
        &self.build_hasher
    }
    fn copier(key: &K, val: &V) -> Option<(K, V)> {
        Some((key.clone(), val.clone()))
    }
    fn cell_array(&self) -> &AtomicArc<CellArray<K, V, CELL_SIZE, true>> {
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
pub struct Visitor<'h, 'b, K, V, H>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
    H: BuildHasher,
{
    hash_index: &'h HashIndex<K, V, H>,
    current_array_ptr: Ptr<'b, CellArray<K, V, CELL_SIZE, true>>,
    current_index: usize,
    current_cell_iterator: Option<CellIterator<'b, K, V, CELL_SIZE, true>>,
    barrier_ref: &'b Barrier,
}

impl<'h, 'b, K, V, H> Iterator for Visitor<'h, 'b, K, V, H>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
    H: 'static + BuildHasher,
{
    type Item = (&'b K, &'b V);
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_array_ptr.is_null() {
            // Starts scanning.
            let current_array_ptr = self.hash_index.array.load(Acquire, self.barrier_ref);
            let current_array_ref = current_array_ptr.as_ref().unwrap();
            let old_array_ptr = current_array_ref.old_array(self.barrier_ref);
            self.current_array_ptr = if !old_array_ptr.is_null() {
                old_array_ptr
            } else {
                current_array_ptr
            };
            let cell_ref = self.current_array_ptr.as_ref().unwrap().cell(0);
            self.current_cell_iterator
                .replace(CellIterator::new(cell_ref, self.barrier_ref));
        }
        loop {
            if let Some(iterator) = self.current_cell_iterator.as_mut() {
                // Proceeds to the next entry in the Cell.
                if let Some(entry) = iterator.next() {
                    return Some((&entry.0 .0, &entry.0 .1));
                }
            }
            // Proceeds to the next Cell.
            let array_ref = self.current_array_ptr.as_ref().unwrap();
            self.current_index += 1;
            if self.current_index == array_ref.array_size() {
                let current_array_ptr = self.hash_index.array.load(Acquire, self.barrier_ref);
                if self.current_array_ptr == current_array_ptr {
                    // Finished scanning the entire array.
                    break;
                }
                let current_array_ref = current_array_ptr.as_ref().unwrap();
                let old_array_ptr = current_array_ref.old_array(self.barrier_ref);
                if self.current_array_ptr == old_array_ptr {
                    // Starts scanning the current array.
                    self.current_array_ptr = current_array_ptr;
                    self.current_index = 0;
                    self.current_cell_iterator.replace(CellIterator::new(
                        current_array_ref.cell(0),
                        self.barrier_ref,
                    ));
                    continue;
                }
                // Starts from the very beginning.
                self.current_array_ptr = if !old_array_ptr.is_null() {
                    old_array_ptr
                } else {
                    current_array_ptr
                };
                self.current_index = 0;
                self.current_cell_iterator.replace(CellIterator::new(
                    self.current_array_ptr.as_ref().unwrap().cell(0),
                    self.barrier_ref,
                ));
                continue;
            } else {
                self.current_cell_iterator.replace(CellIterator::new(
                    array_ref.cell(self.current_index),
                    self.barrier_ref,
                ));
            }
        }
        None
    }
}

impl<'h, 'b, K, V, H> FusedIterator for Visitor<'h, 'b, K, V, H>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
    H: 'static + BuildHasher,
{
}
