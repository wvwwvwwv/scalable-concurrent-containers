pub mod array;
pub mod cell;

use array::Array;
use cell::{ARRAY_SIZE, MAX_RESIZING_FACTOR};
use crossbeam_epoch::{Atomic, Owned, Shared};
use std::collections::hash_map::RandomState;
use std::convert::TryInto;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

const DEFAULT_CAPACITY: usize = 64;

/// A scalable concurrent hash index implementation.
///
/// scc::HashIndex is a concurrent hash index data structure that is optimized for read operations.
pub struct HashIndex<K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
    array: Atomic<Array<K, V>>,
    minimum_capacity: usize,
    resize_mutex: AtomicBool,
    build_hasher: H,
}

impl<K, V> Default for HashIndex<K, V, RandomState>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
{
    /// Creates a HashIndex instance with the default parameters.
    ///
    /// The default hash builder is RandomState, and the default capacity is 256.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails.
    ///
    /// ```
    /// use scc::HashIndex;
    /// ```
    fn default() -> Self {
        HashIndex {
            array: Atomic::new(Array::<K, V>::new(DEFAULT_CAPACITY, Atomic::null())),
            minimum_capacity: DEFAULT_CAPACITY,
            resize_mutex: AtomicBool::new(false),
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
    /// It is recommended to give a capacity value that is larger than 16 * the number of threads to access the HashMap.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// use std::collections::hash_map::RandomState;
    /// ```
    pub fn new(capacity: usize, build_hasher: H) -> HashIndex<K, V, H> {
        let initial_capacity = capacity.max(DEFAULT_CAPACITY);
        HashIndex {
            array: Atomic::new(Array::<K, V>::new(initial_capacity, Atomic::null())),
            minimum_capacity: initial_capacity,
            resize_mutex: AtomicBool::new(false),
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
    /// ```
    pub fn insert(&self, key: K, value: V) -> Result<(), (K, V)> {
        let (hash, partial_hash) = self.hash(&key);
        if hash == 0 && partial_hash == 0 {
            self.resize();
        }
        Err((key, value))
    }

    /// Removes a key-value pair.
    ///
    /// Returns false if the key does not exist.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// ```
    pub fn remove(&self, _tkey: &K) -> bool {
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
    /// ```
    pub fn read<R, F: FnOnce(&K, &V) -> R>(&self, _key: &K, _f: F) -> Option<R> {
        None
    }

    /// Retains the key-value pairs that satisfy the given predicate.
    ///
    /// It returns the number of entries remaining and removed.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// ```
    pub fn retain<F: Fn(&K, &V) -> bool>(&self, _f: F) -> (usize, usize) {
        (0, 0)
    }

    /// Clears all the key-value pairs.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// ```
    pub fn clear(&self) -> usize {
        self.retain(|_, _| false).1
    }

    /// Returns an estimated size of the HashIndex.
    ///
    /// The given function determines the sampling size.
    /// A function returning a fixed number larger than u16::MAX yields around 99% accuracy.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// ```
    pub fn len<F: FnOnce(usize) -> usize>(&self, _f: F) -> usize {
        0
    }

    /// Returns the capacity of the HashIndex.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// ```
    pub fn capacity(&self) -> usize {
        0
    }

    /// Returns a reference to its build hasher.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashindex: HashIndex<u64, u32, _> = Default::default();
    /// let result: &RandomState = hashindex.hasher();
    /// ```
    pub fn hasher(&self) -> &H {
        &self.build_hasher
    }

    /// Returns a Visitor.
    ///
    /// It is guaranteed to go through all the key-value pairs pertaining in the HashIndex at the moment,
    /// however the same key-value pair can be visited more than once if the HashIndex is being resized.
    ///
    /// # Examples
    /// ```
    /// use scc::HashIndex;
    /// ```
    pub fn iter(&self) -> Visitor<K, V, H> {
        Visitor { _hash_index: self }
    }

    /// Returns the hash value of the given key.
    fn hash(&self, key: &K) -> (u64, u8) {
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

    /// Returns a reference to the given array.
    fn array_ref<'g>(&self, array_shared: Shared<'g, Array<K, V>>) -> &'g Array<K, V> {
        unsafe { array_shared.deref() }
    }

    /// Resizes the array.
    fn resize(&self) {
        // Initial rough size estimation using a small number of cells.
        let guard = crossbeam_epoch::pin();
        let current_array = self.array.load(Acquire, &guard);
        let current_array_ref = self.array_ref(current_array);
        let old_array = current_array_ref.old_array(&guard);
        if !old_array.is_null() {
            // [TODO] Rehash
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
            let capacity = current_array_ref.capacity();
            let num_cells = current_array_ref.num_cells();
            let num_cells_to_sample = (num_cells / 8).max(DEFAULT_CAPACITY / ARRAY_SIZE).min(4096);
            let estimated_num_entries = num_cells / num_cells_to_sample; // [TODO] Size estimation.
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
                    if new_capacity_candidate / capacity > (1 << MAX_RESIZING_FACTOR) {
                        capacity * (1 << MAX_RESIZING_FACTOR)
                    } else {
                        new_capacity_candidate
                    }
                }
            } else if estimated_num_entries <= capacity / 8 {
                // Shrinks to fit.
                estimated_num_entries
                    .next_power_of_two()
                    .max(self.minimum_capacity)
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
}

impl<K, V, H> Drop for HashIndex<K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
    fn drop(&mut self) {}
}

/// Visitor.
pub struct Visitor<'h, K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
    _hash_index: &'h HashIndex<K, V, H>,
}

impl<'h, K, V, H> Iterator for Visitor<'h, K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
    type Item = (&'h K, &'h V);
    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
