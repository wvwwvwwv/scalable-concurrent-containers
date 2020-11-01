extern crate crossbeam;

pub mod array;
pub mod cell;

use array::Array;
use cell::{Cell, ExclusiveLocker, SharedLocker};
use crossbeam::epoch::{Atomic, Guard, Owned};
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
    minimum_log_base_two_capacity: u8,
    resize_mutex: AtomicBool,
    hasher: H,
}

/// Accessor
///
/// It is !Send, thus disallowing other threads to have references to it.
pub struct Accessor<'a, K, V> {
    pub key_ref: Option<&'a K>,
    pub value_ref: Option<&'a mut V>,
    cell_guard: Option<Guard>,
    cell_ref: *const Cell<K, V>,
    iterable: bool,
}

impl<K: Clone + Eq + Hash + Sync, V: Sync + Unpin, H: BuildHasher> HashMap<K, V, H> {
    /// Create an empty HashMap instance with the given hasher and minimum capacity
    pub fn new(hasher: H, minimum_capacity: Option<usize>) -> HashMap<K, V, H> {
        let adjusted_minimum_log_base_two_capacity = if let Some(capacity) = minimum_capacity {
            Self::log_base_two(capacity)
        } else {
            4
        };

        HashMap {
            current_array: Atomic::new(Array::new(adjusted_minimum_log_base_two_capacity)),
            deprecated_array: Atomic::null(),
            minimum_log_base_two_capacity: adjusted_minimum_log_base_two_capacity,
            resize_mutex: AtomicBool::new(false),
            hasher: hasher,
        }
    }

    /// Insert a key-value pair into the HashMap
    pub fn insert<'a>(&self, key: &K, value: V) -> Result<Accessor<'a, K, V>, Accessor<'a, K, V>> {
        let _ = self.hash(key);
        let guard = crossbeam::epoch::pin();
        Err(Accessor {
            key_ref: None,
            value_ref: None,
            cell_guard: Some(guard),
            cell_ref: std::ptr::null(),
            iterable: false,
        })
    }

    /// Upsert a key-value pair into the HashMap.
    pub fn upsert<'a>(&self, key: &K, value: V) -> Accessor<'a, K, V> {
        let _ = self.hash(key);
        let guard = crossbeam::epoch::pin();
        Accessor {
            key_ref: None,
            value_ref: None,
            cell_guard: Some(guard),
            cell_ref: std::ptr::null(),
            iterable: false,
        }
    }

    /// Get a mutable reference to the value associated with the key.
    pub fn get<'a>(&self, key: &K) -> Option<Accessor<'a, K, V>> {
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
    pub fn erase<'a>(&self, accessor: Accessor<'a, K, V>) -> bool {
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
    pub fn iter<'a>(&self) -> Accessor<'a, K, V> {
        let guard = crossbeam::epoch::pin();
        Accessor {
            key_ref: None,
            value_ref: None,
            cell_guard: Some(guard),
            cell_ref: std::ptr::null(),
            iterable: true,
        }
    }

    /// Return a hash value of the given key.
    fn hash(&self, key: &K) -> u64 {
        // generate a hash value
        let mut h = self.hasher.build_hasher();
        key.hash(&mut h);
        let mut hash = h.finish();

        // bitmix: https://mostlymangling.blogspot.com/2019/01/better-stronger-mixer-and-test-procedure.html
        hash = (hash ^ (((hash >> 25) | (hash << 39)) ^ ((hash >> 50) | (hash << 14))))
            * (0xA24BAED4963EE407 as u64);
        hash = (hash ^ (((hash >> 24) | (hash << 40)) ^ ((hash >> 49) | (hash << 15))))
            * (0x9FB21C651E98DF25 as u64);
        hash ^ (hash >> 28)
    }

    fn log_base_two(capacity: usize) -> u8 {
        let mut calculated_capacity = 2;
        let mut log_base_two = 1;

        // it supports both 32-bit and 64-bit processors
        let max_consumable_bits = if std::mem::size_of::<usize>() >= 8 {
            60
        } else {
            28
        };

        // the maximum size of the key-value pair array is (1 << 60) * 10
        for i in 1..max_consumable_bits {
            if calculated_capacity >= ((capacity + 9) / 10) {
                log_base_two = i;
                break;
            }
            calculated_capacity = calculated_capacity << 1;
        }
        debug_assert!(calculated_capacity * 10 >= capacity);
        debug_assert_eq!(calculated_capacity, 1 << log_base_two);
        log_base_two
    }
}

impl<K: Clone + Eq + Hash + Sync, V: Sync + Unpin, H: BuildHasher> Drop for HashMap<K, V, H> {
    fn drop(&mut self) {}
}

impl<'a, K, V> Drop for Accessor<'a, K, V> {
    fn drop(&mut self) {}
}

impl<'a, K, V> Iterator for Accessor<'a, K, V> {
    type Item = &'a Accessor<'a, K, V>;
    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
