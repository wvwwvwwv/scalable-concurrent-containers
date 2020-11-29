use crossbeam_epoch::Atomic;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicU32;

pub const ARRAY_SIZE: usize = 7;

/// Metadata layout: invalidated: 1-bit | max-key removed: 1-bit | rank-index map: 21-bit | occupancy: 7-bit
///
/// State interpretation
///  - !OCCUPIED && RANK = 0: initial state
///  - OCCUPIED && RANK = 0: locked
///  - OCCUPIED && RANK > 0: inserted
///  - !OCCUPIED && RANK > 0: removed
const RANK_INDEX_ENTRY_SIZE: usize = 3;
const RANK_INDEX_MAP_MASK: u32 = ((1u32 << (ARRAY_SIZE * RANK_INDEX_ENTRY_SIZE)) - 1) << ARRAY_SIZE;
const RANK_INDEX_ENTRY_MASK: u32 = ((1u32 << RANK_INDEX_ENTRY_SIZE) - 1) << ARRAY_SIZE;
const OCCUPANCY_MASK: u32 = (1u32 << ARRAY_SIZE) - 1;
const OCCUPANCY_BIT: u32 = 1;
const MAX_KEY_REMOVED: u32 = 1u32 << ARRAY_SIZE * (RANK_INDEX_ENTRY_SIZE + 1);
const INVALIDATED: u32 = MAX_KEY_REMOVED << 1;

pub type EntryArray<K, V> = [MaybeUninit<(K, V)>; ARRAY_SIZE];

/// Leaf stores key-value pairs.
pub struct Leaf<K: Clone + Ord + Sync, V: Clone + Sync> {
    max_key_entry: (K, V),
    entry_array: EntryArray<K, V>,
    metadata: AtomicU32,
    next: Atomic<Leaf<K, V>>,
}

impl<K: Clone + Ord + Sync, V: Clone + Sync> Leaf<K, V> {
    pub fn new(max_key: K, value: V) -> Leaf<K, V> {
        Leaf {
            max_key_entry: (max_key, value),
            entry_array: unsafe { MaybeUninit::uninit().assume_init() },
            metadata: AtomicU32::new(0),
            next: Atomic::null(),
        }
    }

    pub fn insert(key: K, value: V) -> Option<(K, V)> {
        Some((key, value))
    }

    pub fn remove(key: &K) -> Option<V> {
        None
    }

    pub fn search(key: &K) -> Option<&V> {
        None
    }

    pub fn invalidate() {}
}

impl<K: Clone + Ord + Sync, V: Clone + Sync> Drop for Leaf<K, V> {
    fn drop(&mut self) {}
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn static_assertions() {
        assert_eq!(MAX_KEY_REMOVED & OCCUPANCY_MASK, 0);
        assert_eq!(MAX_KEY_REMOVED & RANK_INDEX_MAP_MASK, 0);
        assert_eq!(RANK_INDEX_MAP_MASK & OCCUPANCY_MASK, 0);
        assert_eq!(
            RANK_INDEX_MAP_MASK & RANK_INDEX_ENTRY_MASK,
            RANK_INDEX_ENTRY_MASK
        );
        assert_eq!(OCCUPANCY_MASK & OCCUPANCY_BIT, OCCUPANCY_BIT);
    }

    #[test]
    fn modification() {
        let num_threads = (ARRAY_SIZE + 1) as usize;
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut thread_handles = Vec::with_capacity(num_threads);
        for tid in 0..num_threads {
            let barrier_copied = barrier.clone();
            thread_handles.push(thread::spawn(move || {
                barrier_copied.wait();
            }));
        }
    }

    #[test]
    fn iteration() {}
}
