#![allow(dead_code)]
#![allow(unused_variables)]

//! The module implements [`HashMap`].

use super::async_yield::async_yield;
use super::hash_table::cell_array::CellArray;
use super::hash_table::HashTable;

use crate::ebr::{AtomicArc, Barrier};

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicU8, AtomicUsize};

/// [`HashMap`].
pub struct HashMap<K, V, H = RandomState>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    array: AtomicArc<CellArray<K, V, false>>,
    minimum_capacity: usize,
    additional_capacity: AtomicUsize,
    resize_mutex: AtomicU8,
    build_hasher: H,
}

impl<K, V, H> HashMap<K, V, H>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    /// Inserts an entry into the [`HashTable`].
    #[inline]
    async fn insert_entry(&self, key: K, val: V) -> Result<(), (K, V)> {
        loop {
            let barrier = Barrier::new();
            drop(barrier);
            async_yield().await;
        }
    }
}

impl<K, V, H> HashTable<K, V, H, false> for HashMap<K, V, H>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    fn hasher(&self) -> &H {
        &self.build_hasher
    }

    fn copier(_key: &K, _val: &V) -> Option<(K, V)> {
        None
    }

    fn cell_array(&self) -> &AtomicArc<CellArray<K, V, false>> {
        &self.array
    }

    fn minimum_capacity(&self) -> usize {
        self.minimum_capacity
    }

    fn resize_mutex(&self) -> &AtomicU8 {
        &self.resize_mutex
    }
}
