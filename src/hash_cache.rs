//! [`HashCache`] is a concurrent and asynchronous pseudo-LRU cache backed by
//! [`HashMap`](super::HashMap).

#![allow(dead_code, unused_imports)]

use super::ebr::{Arc, AtomicArc, Barrier, Tag};
use super::hash_table::bucket::{
    DataBlock, EntryPtr, Evictable, Locker, Reader, BUCKET_LEN, CACHE,
};
use super::hash_table::bucket_array::BucketArray;
use super::hash_table::HashTable;
use super::wait_queue::AsyncWait;
use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::fmt::{self, Debug};
use std::hash::{BuildHasher, Hash, Hasher};
use std::mem::replace;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed};

/// Scalable concurrent pseudo-LRU cache based on [`HashMap`](super::HashMap).
///
/// ### Unwind safety
///
/// [`HashCache`] is impervious to out-of-memory errors and panics in user specified code on one
/// condition; `H::Hasher::hash`, `K::drop` and `V::drop` must not panic.
pub struct HashCache<K, V, H = RandomState>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    array: AtomicArc<BucketArray<K, Evictable<V>, CACHE>>,
    minimum_capacity: AtomicUsize,
    maximum_capacity: usize,
    build_hasher: H,
}

impl<K: Eq + Hash, V, H: BuildHasher> HashTable<K, Evictable<V>, H, CACHE> for HashCache<K, V, H> {
    #[inline]
    fn hasher(&self) -> &H {
        &self.build_hasher
    }
    #[inline]
    fn try_clone(_entry: &(K, Evictable<V>)) -> Option<(K, Evictable<V>)> {
        None
    }
    #[inline]
    fn try_reset(value: &mut Evictable<V>) {
        value.reset_link();
    }
    #[inline]
    fn bucket_array(&self) -> &AtomicArc<BucketArray<K, Evictable<V>, CACHE>> {
        &self.array
    }
    #[inline]
    fn minimum_capacity(&self) -> &AtomicUsize {
        &self.minimum_capacity
    }
    #[inline]
    fn maximum_capacity(&self) -> usize {
        self.maximum_capacity
    }
}
