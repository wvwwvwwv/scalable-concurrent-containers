extern crate crossbeam;

pub mod array;
pub mod cell;

use array::Array;
use cell::{Cell, ExclusiveLocker, SharedLocker};
use crossbeam::epoch::{Atomic, Guard, Owned};
use std::convert::TryInto;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

// A hash map targeted at a highly concurrent workload.
//
// Keys are spread over a single array of metadata buckets, and each metadata bucket stores ten hash values and a singly linked list of key-value pairs.
// Access to each metadata bucket is protected by a custom mutex of the bucket, and the contents of the bucket is only allowed to be updated with the mutex acquired.
// The instance of a key-value pair is stored in a separate array, and the metadata bucket mutex serializes access to the key-value pair instance.
// This approach is very similar to what is implemented in Swisstable, or a proprietary hash table implementation used by various SAP products.
// It resizes or shrinks itself when the estimated load factor reaches 100% and 12.5%, and resizing is not a blocking operation.
// Once resized, the old array is kept intact, and the key-value pairs stored in the array is incrementally relocated to the new array on each access.
pub struct HashMap<K: Clone + Eq + Hash + Sync, V: Sync + Unpin, H: BuildHasher> {
    array: Atomic<Array<K, V, Cell<K, V>>>,
    minimum_capacity: usize,
    resize_mutex: AtomicBool,
    hasher: H,
}

/// Accessor
///
/// It is !Send, thus disallowing other threads to have references to it.
pub struct Accessor<'a: 'b, 'b, K: Clone + Eq + Hash + Sync, V: Sync + Unpin, H: BuildHasher> {
    hash_map: &'a HashMap<K, V, H>,
    cell_locker: Option<ExclusiveLocker<'b, K, V>>,
    key_value_pair: Option<*const (K, V)>,
}

impl<K: Clone + Eq + Hash + Sync, V: Sync + Unpin, H: BuildHasher> HashMap<K, V, H> {
    /// Create an empty HashMap instance with the given hasher and minimum capacity
    pub fn new(hasher: H, minimum_capacity: Option<usize>) -> HashMap<K, V, H> {
        let initial_capacity = if let Some(capacity) = minimum_capacity {
            capacity
        } else {
            160
        };
        HashMap {
            array: Atomic::new(Array::<K, V, Cell<K, V>>::new(
                initial_capacity,
                Atomic::null(),
            )),
            minimum_capacity: initial_capacity,
            resize_mutex: AtomicBool::new(false),
            hasher: hasher,
        }
    }

    /// Insert a key-value pair into the HashMap
    pub fn insert<'a, 'b>(
        &'a self,
        key: K,
        value: V,
    ) -> Result<Accessor<'a, 'b, K, V, H>, Accessor<'a, 'b, K, V, H>> {
        let (hash, partial_hash) = self.hash(&key);
        let guard = crossbeam::epoch::pin();

        // it is guaranteed that the thread reads a consistent snapshot of current and
        // old array pair by a Release fence at the resize function, hence the following
        // procedure is correct.
        //  - the thread reads self.array, and it kills the target cell in the old array
        //    if there is one attached to it, and inserts the key into array.
        // There are two cases.
        //  1. the thread reads an old version of self.array.
        //    if there is another thread having read the latest version of self.array,
        //    trying to insert the same key, it will try to kill the cell in the old version
        //    of self.array, thus competing with each other.
        //  2. Thread X reads the latest version of self.array.
        //    if the array is deprecated while inserting the key, it falls into case 1.
        loop {
            // an Acquire fence is required to correctly load the contents of the array
            let current_array_ptr = self.array.load(Acquire, &guard).as_raw();
            let old_array_ptr = unsafe { (*current_array_ptr).get_old_array(&guard) }.as_raw();
            if !old_array_ptr.is_null() {
                // relocate at most 16 cells
                // self.relocate(current_array_ptr, old_array_ptr);

                // new entries will never be inserted into a deprecated array
                let (locker, key_value_pair, _) =
                    self.search(&key, hash, partial_hash, old_array_ptr);
                if let Some(_) = key_value_pair {
                    return Err(Accessor {
                        hash_map: &self,
                        cell_locker: Some(locker),
                        key_value_pair: key_value_pair,
                    });
                } else if !locker.killed() {
                    // relocated the cell
                    // self.kill(locker, old_array_ptr, current_array_ptr);
                }
            }
            let (mut locker, key_value_pair, cell_index) =
                self.search(&key, hash, partial_hash, current_array_ptr);
            if let Some(_) = key_value_pair {
                return Err(Accessor {
                    hash_map: &self,
                    cell_locker: Some(locker),
                    key_value_pair: key_value_pair,
                });
            } else if !locker.killed() {
                match locker.insert(partial_hash) {
                    Some(index) => {
                        let key_value_array_index = cell_index * 10 + (index as usize);
                        let key_value_pair_ptr = unsafe {
                            (*current_array_ptr).get_key_value_pair(key_value_array_index)
                        };
                        let key_value_pair_mut_ptr = key_value_pair_ptr as *mut (K, V);
                        unsafe { key_value_pair_mut_ptr.write((key.clone(), value)) };
                        return Ok(Accessor {
                            hash_map: &self,
                            cell_locker: Some(locker),
                            key_value_pair: Some(key_value_pair_ptr),
                        });
                    }
                    None => {}
                }
            }
            // reaching here indicates that self.array is updated
        }
    }

    /// Upsert a key-value pair into the HashMap.
    pub fn upsert<'a, 'b>(&'a self, key: K, value: V) -> Accessor<'a, 'b, K, V, H> {
        let _ = self.hash(&key);
        let guard = crossbeam::epoch::pin();
        Accessor {
            hash_map: &self,
            cell_locker: None,
            key_value_pair: None,
        }
    }

    /// Get a reference to the value associated with the key.
    pub fn get<'a, 'b>(&self, key: K) -> Option<Accessor<'a, 'b, K, V, H>> {
        let _ = crossbeam::epoch::pin();
        None
    }

    /// Read the key-value pair.
    pub fn read<U, F: FnOnce(&K, &V) -> U>(&self, key: K, f: F) -> Option<U> {
        let _ = crossbeam::epoch::pin();
        None
    }

    /// Mutate the value associated with the given key.
    pub fn mutate<U, F: FnOnce(&K, &mut V) -> U>(&self, key: K, f: F) -> Option<U> {
        let _ = crossbeam::epoch::pin();
        None
    }

    /// Erase the key-value pair owned by the given Accessor.
    pub fn erase<'a, 'b>(&self, accessor: Accessor<'a, 'b, K, V, H>) -> bool {
        false
    }

    /// Remove a key-value pair.
    pub fn remove(&self, key: K) -> bool {
        if let Some(accessor) = self.get(key) {
            return self.erase(accessor);
        }
        false
    }

    /// Return the estimated size of the HashMap.
    pub fn len(&self) -> usize {
        let _ = crossbeam::epoch::pin();
        0
    }

    /// Return an iterable Accessor.
    pub fn iter<'a, 'b>(&'a self) -> Accessor<'a, 'b, K, V, H> {
        let guard = crossbeam::epoch::pin();
        Accessor {
            hash_map: &self,
            cell_locker: None,
            key_value_pair: None,
        }
    }

    /// Return a hash value of the given key.
    fn hash(&self, key: &K) -> (u64, u32) {
        // generate a hash value
        let mut h = self.hasher.build_hasher();
        key.hash(&mut h);
        let mut hash = h.finish();

        // bitmix: https://mostlymangling.blogspot.com/2019/01/better-stronger-mixer-and-test-procedure.html
        hash = hash ^ (hash.rotate_right(25) ^ hash.rotate_right(50));
        hash = hash.overflowing_mul(0xA24BAED4963EE407u64).0;
        hash = hash ^ (hash.rotate_right(24) ^ hash.rotate_right(49));
        hash = hash.overflowing_mul(0x9FB21C651E98DF25u64).0;
        hash = hash ^ (hash >> 28);
        (hash, (hash & ((1 << 32) - 1)).try_into().unwrap())
    }

    fn search<'a>(
        &self,
        key: &K,
        hash: u64,
        partial_hash: u32,
        array_ptr: *const Array<K, V, Cell<K, V>>,
    ) -> (ExclusiveLocker<'a, K, V>, Option<*const (K, V)>, usize) {
        let cell_index = unsafe { (*array_ptr).calculate_metadata_array_index(hash) };
        let cell = unsafe { (*array_ptr).get_cell(cell_index) };
        let mut locker = ExclusiveLocker::lock(cell);
        if !locker.killed() && !locker.empty() {
            if locker.overflowing() {
                let key_value_pair_ptr = locker.search_link(key);
                if !key_value_pair_ptr.is_null() {
                    return (locker, Some(key_value_pair_ptr), cell_index);
                }
            }
            if let Some(index) = locker.search(partial_hash) {
                let key_value_array_index = cell_index * 10 + (index as usize);
                let key_value_pair_ptr =
                    unsafe { (*array_ptr).get_key_value_pair(key_value_array_index) };
                if unsafe { (*key_value_pair_ptr).0 == *key } {
                    return (locker, Some(key_value_pair_ptr), cell_index);
                }
            }
        }
        (locker, None, cell_index)
    }
}

impl<K: Clone + Eq + Hash + Sync, V: Sync + Unpin, H: BuildHasher> Drop for HashMap<K, V, H> {
    fn drop(&mut self) {}
}

impl<'a: 'b, 'b, K: Clone + Eq + Hash + Sync, V: Sync + Unpin, H: BuildHasher>
    Accessor<'a, 'b, K, V, H>
{
    pub fn get(&self) -> Option<&'b (K, V)> {
        self.key_value_pair
            .as_ref()
            .map_or(None, |ptr| unsafe { Some(&(*(*ptr))) })
    }
}

impl<'a: 'b, 'b, K: Clone + Eq + Hash + Sync, V: Sync + Unpin, H: BuildHasher> Drop
    for Accessor<'a, 'b, K, V, H>
{
    fn drop(&mut self) {}
}

impl<'a: 'b, 'b, K: Clone + Eq + Hash + Sync, V: Sync + Unpin, H: BuildHasher> Iterator
    for Accessor<'a, 'b, K, V, H>
{
    type Item = &'b (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.get()
    }
}
