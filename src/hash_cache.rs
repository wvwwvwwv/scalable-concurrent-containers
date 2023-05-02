//! [`HashCache`] is a concurrent and asynchronous pseudo-LRU cache backed by
//! [`HashMap`](super::HashMap).

#![allow(dead_code, unused_imports)]

use super::ebr::{Arc, AtomicArc, Barrier, Tag};
use super::hash_table::bucket::{DataBlock, EntryPtr, Locker, Reader, CACHE, BUCKET_LEN};
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
/// condition; `K::drop` and `V::drop` must not panic.
pub struct HashCache<K, V, H = RandomState>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    array: AtomicArc<BucketArray<K, V, CACHE>>,
    build_hasher: H,
}
