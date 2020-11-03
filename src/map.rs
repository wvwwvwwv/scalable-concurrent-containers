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
    current_array: Atomic<Array<K, V, Cell<K, V>>>,
    deprecated_array: Atomic<Array<K, V, Cell<K, V>>>,
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
            current_array: Atomic::new(Array::<K, V, Cell<K, V>>::new(initial_capacity)),
            deprecated_array: Atomic::null(),
            minimum_capacity: initial_capacity,
            resize_mutex: AtomicBool::new(false),
            hasher: hasher,
        }
    }

    /// Insert a key-value pair into the HashMap
    pub fn insert<'a, 'b>(
        &'a self,
        key: &K,
        value: V,
    ) -> Result<Accessor<'a, 'b, K, V, H>, Accessor<'a, 'b, K, V, H>> {
        let (hash, partial_hash) = self.hash(key);
        let guard = crossbeam::epoch::pin();
        let mut array_ptr = self.current_array.load(Relaxed, &guard);

        // c = check, i = insert, r = read, u = pointer update, x = retire
        // t1: u(D0) u(C1)       u(D1) u(C2) x(D0)
        // t2: r(C0) r(D-) i(C0)                                     r(C0)
        //  => IMPOSSIBLE SCHEDULE: i(C0) is locked, and therefore the array cannot be retired
        // t1: u(D0) u(C1)       u(D1) u(C2) x(D0)
        // t2:             r(C1) r(D0) c(D0)                   i(C1) r(C1) OK
        // t3:                               r(C2) r(D1) c(D1) i(C2) r(C2) OK
        loop {
            let deprecated_array_ptr = self.deprecated_array.load(Relaxed, &guard).as_raw();
            if !deprecated_array_ptr.is_null() {
                // relocate a certain number of cells
                //  self.relocate()
                // new entries will never be inserted into a deprecated array
                let locker = self.search(key, hash, partial_hash, deprecated_array_ptr);
                if locker.key_value_pair_associated() {
                    return Err(Accessor {
                        hash_map: &self,
                        cell_locker: Some(locker),
                    });
                } else if !locker.killed() {
                    // relocated the cell
                }
            }

            let array_ptr_again = self.current_array.load(Relaxed, &guard);
            if array_ptr == array_ptr_again {
                break;
            }

            // resize took place in the meantime, thus revert the entry insertion and try again
            array_ptr = array_ptr_again
        }

        Err(Accessor {
            hash_map: &self,
            cell_locker: None,
        })
    }

    /// Upsert a key-value pair into the HashMap.
    pub fn upsert<'a, 'b>(&'a self, key: &K, value: V) -> Accessor<'a, 'b, K, V, H> {
        let _ = self.hash(key);
        let guard = crossbeam::epoch::pin();
        Accessor {
            hash_map: &self,
            cell_locker: None,
        }
    }

    /// Get a mutable reference to the value associated with the key.
    pub fn get<'a, 'b>(&self, key: &K) -> Option<Accessor<'a, 'b, K, V, H>> {
        let _ = crossbeam::epoch::pin();
        None
    }

    /// Read the key-value pair.
    pub fn read<U, F: FnOnce(&K, &V) -> U>(&self, key: &K, f: F) -> Option<U> {
        let _ = crossbeam::epoch::pin();
        None
    }

    /// Mutate the value associated with the given key.
    pub fn mutate<U, F: FnOnce(&K, &mut V) -> U>(&self, key: &K, f: F) -> Option<U> {
        let _ = crossbeam::epoch::pin();
        None
    }

    /// Erase the key-value pair owned by the given Accessor.
    pub fn erase<'a, 'b>(&self, accessor: Accessor<'a, 'b, K, V, H>) -> bool {
        false
    }

    /// Remove a key-value pair.
    pub fn remove(&self, key: &K) -> bool {
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
        }
    }

    /// Return a hash value of the given key.
    fn hash(&self, key: &K) -> (u64, u32) {
        // generate a hash value
        let mut h = self.hasher.build_hasher();
        key.hash(&mut h);
        let mut hash = h.finish();

        // bitmix: https://mostlymangling.blogspot.com/2019/01/better-stronger-mixer-and-test-procedure.html
        hash = (hash ^ (((hash >> 25) | (hash << 39)) ^ ((hash >> 50) | (hash << 14))))
            * (0xA24BAED4963EE407 as u64);
        hash = (hash ^ (((hash >> 24) | (hash << 40)) ^ ((hash >> 49) | (hash << 15))))
            * (0x9FB21C651E98DF25 as u64);
        hash = hash ^ (hash >> 28);
        (hash, (hash & ((1 << 32) - 1)).try_into().unwrap())
    }

    fn search<'a>(
        &self,
        key: &K,
        hash: u64,
        partial_hash: u32,
        array_ptr: *const Array<K, V, Cell<K, V>>,
    ) -> ExclusiveLocker<'a, K, V> {
        let cell_index = unsafe { (*array_ptr).calculate_metadata_array_index(hash) };
        let cell = unsafe { (*array_ptr).get_cell(cell_index) };
        let mut locker = ExclusiveLocker::lock(cell);
        if !locker.killed() && !locker.empty() {
            if locker.overflowing() {
                if locker.search_link(key) {
                    return locker;
                }
            }
            if let Some(index) = locker.search_array(partial_hash) {
                let key_value_array_index = cell_index * 10 + (index as usize);
                let key_value_pair =
                    unsafe { (*array_ptr).get_key_value_pair(key_value_array_index) };
                locker.set_key_value_pair(key_value_pair);
                return locker;
            }
        }
        locker
    }
}

impl<K: Clone + Eq + Hash + Sync, V: Sync + Unpin, H: BuildHasher> Drop for HashMap<K, V, H> {
    fn drop(&mut self) {}
}

impl<'a: 'b, 'b, K: Clone + Eq + Hash + Sync, V: Sync + Unpin, H: BuildHasher>
    Accessor<'a, 'b, K, V, H>
{
    pub fn key(&self) -> Option<&K> {
        self.cell_locker.as_ref().map_or(None, |l| l.key())
    }

    pub fn value(&self) -> Option<&V> {
        self.cell_locker.as_ref().map_or(None, |l| l.value())
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
    type Item = Self;
    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
