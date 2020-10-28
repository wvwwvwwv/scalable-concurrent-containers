extern crate crossbeam;

pub mod array;
pub mod cell;

use array::Array;
use cell::{Cell, ExclusiveLocker};
use crossbeam::atomic::AtomicCell;
use crossbeam::epoch::Guard;
use crossbeam::utils::CachePadded;
use std::boxed::Box;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicBool, AtomicUsize};

// A hash map targeted at a highly concurrent workload.
//
// Keys are spread over a single array of metadata buckets, and each metadata bucket stores ten hash values and a singly linked list of key-value pairs.
// Access to each metadata bucket is protected by a custom mutex of the bucket, and the contents of the bucket is only allowed to be updated with the mutex acquired.
// The instance of a key-value pair is stored in a separate array, and the metadata bucket mutex serializes access to the key-value pair instance.
// This approach is very similar to what is implemented in Swisstable, or a proprietary hash table implementation used by various SAP products.
// It resizes or shrinks itself when the estimated load factor reaches 100% and 12.5%, and resizing is not a blocking operation.
// Once resized, the old array is kept intact, and the key-value pairs stored in the array is incrementally relocated to the new array on each access.
pub struct HashMap<K: Eq + Hash + Sync + Unpin, V: Sync + Unpin, H: BuildHasher> {
    current_array: CachePadded<AtomicCell<Option<Box<Array<K, V, Cell<K, V>>>>>>,
    deprecated_array: CachePadded<AtomicCell<Option<Box<Array<K, V, Cell<K, V>>>>>>,
    resize_mutex: AtomicBool,
    hasher: H,
}

/// Accessor
pub struct Accessor<'a, K, V> {
    pub key_ref: Option<&'a K>,
    pub value_ref: Option<&'a mut V>,
    cell_ref: *const Cell<K, V>,
    iterable: bool,
}

impl<K: Eq + Hash + Sync + Unpin, V: Sync + Unpin, H: BuildHasher> HashMap<K, V, H> {
    /// Create an empty HashMap instance with the given capacity
    pub fn new(hasher: H, _: Option<usize>) -> HashMap<K, V, H> {
        HashMap {
            current_array: CachePadded::new(AtomicCell::new(None)),
            deprecated_array: CachePadded::new(AtomicCell::new(None)),
            resize_mutex: AtomicBool::new(false),
            hasher: hasher,
        }
    }

    /// Insert a value into the HashMap.
    pub fn insert(&mut self, key: &K, value: V) -> bool {
        let hash = self.hash(key);
        false
    }

    /// Get a reference to the value associated with the key.
    pub fn get<'a>(&self, key: &K) -> Accessor<'a, K, V> {
        Accessor {
            key_ref: None,
            value_ref: None,
            cell_ref: std::ptr::null(),
            iterable: false,
        }
    }

    pub fn map<U, F: FnOnce(&K, &V) -> U>(&self, key: &K, f: F) -> Option<U> {
        None
    }

    /// Removes a key from the HashMap.
    pub fn remove(&mut self, key: &K) -> bool {
        false
    }

    /// Return the size of the HashMap.
    pub fn len(&self) -> usize {
        0
    }

    pub fn iter<'a>(&self) -> Accessor<'a, K, V> {
        Accessor {
            key_ref: None,
            value_ref: None,
            cell_ref: std::ptr::null(),
            iterable: true,
        }
    }

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
}

impl<K: Eq + Hash + Sync + Unpin, V: Sync + Unpin, H: BuildHasher> Drop for HashMap<K, V, H> {
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic_assumptions() {
        assert_eq!(std::mem::size_of::<Cell<u32, u32>>(), 64)
    }
}
