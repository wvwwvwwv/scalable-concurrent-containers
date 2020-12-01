use crossbeam_epoch::Atomic;
use std::cmp::Ordering;
use std::convert::TryInto;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

pub const ARRAY_SIZE: usize = 7;

/// Metadata layout: invalidated: 1-bit | max-key removed: 1-bit | occupancy: 7-bit | rank-index map: 21-bit
///
/// State interpretation
///  - !OCCUPIED && RANK = 0: initial state
///  - OCCUPIED && RANK = 0: locked
///  - OCCUPIED && RANK > 0: inserted
///  - !OCCUPIED && RANK > 0: removed
const INDEX_RANK_ENTRY_SIZE: usize = 3;
const MAX_KEY_REMOVED: u32 = 1u32 << ARRAY_SIZE * (INDEX_RANK_ENTRY_SIZE + 1);
const INVALIDATED: u32 = MAX_KEY_REMOVED << 1;
const OCCUPANCY_BIT: u32 = 1u32 << (ARRAY_SIZE * INDEX_RANK_ENTRY_SIZE as usize);
const OCCUPANCY_MASK: u32 =
    ((1u32 << ARRAY_SIZE) - 1) << (ARRAY_SIZE * INDEX_RANK_ENTRY_SIZE as usize);
const INDEX_RANK_MAP_MASK: u32 = (1u32 << (ARRAY_SIZE * INDEX_RANK_ENTRY_SIZE)) - 1;
const INDEX_RANK_ENTRY_MASK: u32 = (1u32 << INDEX_RANK_ENTRY_SIZE) - 1;

/// Each entry in an EntryArray is never dropped until the Leaf is dropped once constructed.
pub type EntryArray<K, V> = [MaybeUninit<(K, V)>; ARRAY_SIZE];

/// Leaf stores key-value pairs.
pub struct Leaf<K: Clone + Ord + Sync, V: Clone + Sync> {
    max_key_entry: Option<(K, V)>,
    entry_array: EntryArray<K, V>,
    metadata: AtomicU32,
    next: Atomic<Leaf<K, V>>,
}

impl<K: Clone + Ord + Sync, V: Clone + Sync> Leaf<K, V> {
    pub fn new(max_key_entry: Option<(K, V)>) -> Leaf<K, V> {
        Leaf {
            max_key_entry,
            entry_array: unsafe { MaybeUninit::uninit().assume_init() },
            metadata: AtomicU32::new(0),
            next: Atomic::null(),
        }
    }

    pub fn insert(&self, key: K, value: V) -> Option<(K, V)> {
        if self
            .max_key_entry
            .as_ref()
            .map_or_else(|| false, |entry| entry.0.cmp(&key) != Ordering::Greater)
        {
            // the key doesn't fit the leaf
            return Some((key, value));
        }

        let mut entry = (key, value);
        while let Some(mut inserter) = Inserter::new(self) {
            // calculate the rank and check uniqueness
            let mut max_min_rank = 0;
            let mut min_max_rank = ARRAY_SIZE + 1;
            let mut updated_rank_map = inserter.metadata & INDEX_RANK_MAP_MASK;
            for i in 0..ARRAY_SIZE {
                if i == inserter.index {
                    continue;
                }
                let rank = ((updated_rank_map
                    & (INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE)))
                    >> (i * INDEX_RANK_ENTRY_SIZE)) as usize;
                if rank == 0 || rank < max_min_rank {
                    continue;
                }
                if rank > min_max_rank {
                    // update the rank
                    let rank_bits: u32 = ((rank + 1) << (i * INDEX_RANK_ENTRY_SIZE))
                        .try_into()
                        .unwrap();
                    updated_rank_map = (updated_rank_map
                        & (!(INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE))))
                        | rank_bits;
                    continue;
                }
                match self.compare(i, &entry.0) {
                    Ordering::Less => {
                        if max_min_rank < rank {
                            max_min_rank = rank;
                        }
                    }
                    Ordering::Greater => {
                        if min_max_rank > rank {
                            min_max_rank = rank;
                        }
                        // update the rank
                        let rank_bits: u32 = ((rank + 1) << (i * INDEX_RANK_ENTRY_SIZE))
                            .try_into()
                            .unwrap();
                        updated_rank_map = (updated_rank_map
                            & (!(INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE))))
                            | rank_bits;
                    }
                    Ordering::Equal => {
                        return Some(entry);
                    }
                }
            }
            let final_rank = max_min_rank + 1;
            debug_assert!(min_max_rank == ARRAY_SIZE + 1 || final_rank == min_max_rank);

            // update its own rank
            let rank_bits: u32 = (final_rank << (inserter.index * INDEX_RANK_ENTRY_SIZE))
                .try_into()
                .unwrap();
            updated_rank_map = (updated_rank_map
                & (!(INDEX_RANK_ENTRY_MASK << (inserter.index * INDEX_RANK_ENTRY_SIZE))))
                | rank_bits;

            // insert the key value
            self.write(inserter.index, entry.0, entry.1);

            // try commit
            if inserter.commit(updated_rank_map) {
                return None;
            }
            entry = self.take(inserter.index);
        }
        Some(entry)
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        None
    }

    pub fn search(&self, key: &K) -> Option<&V> {
        None
    }

    pub fn invalidate(&self) {}

    fn write(&self, index: usize, key: K, value: V) {
        unsafe {
            self.entry_array_mut_ref()[index]
                .as_mut_ptr()
                .write((key, value))
        };
    }

    fn compare(&self, index: usize, key: &K) -> std::cmp::Ordering {
        let entry_ref = unsafe { &*self.entry_array[index].as_ptr() };
        entry_ref.0.cmp(key)
    }

    fn take(&self, index: usize) -> (K, V) {
        let entry_ptr = &mut self.entry_array_mut_ref()[index] as *mut MaybeUninit<(K, V)>;
        unsafe { std::ptr::replace(entry_ptr, MaybeUninit::uninit()).assume_init() }
    }

    fn entry_array_mut_ref(&self) -> &mut EntryArray<K, V> {
        let entry_array_ptr = &self.entry_array as *const EntryArray<K, V>;
        let entry_array_mut_ptr = entry_array_ptr as *mut EntryArray<K, V>;
        unsafe { &mut (*entry_array_mut_ptr) }
    }
}

impl<K: Clone + Ord + Sync, V: Clone + Sync> Drop for Leaf<K, V> {
    fn drop(&mut self) {
        let metadata = self.metadata.swap(0, Acquire);
        for i in 0..ARRAY_SIZE {
            if metadata & (INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE)) != 0 {
                self.take(i);
            }
        }
    }
}

struct Inserter<'a, K: Clone + Ord + Sync, V: Clone + Sync> {
    leaf: &'a Leaf<K, V>,
    committed: bool,
    metadata: u32,
    index: usize,
}

impl<'a, K: Clone + Ord + Sync, V: Clone + Sync> Inserter<'a, K, V> {
    /// Returns Some if OCCUPIED && RANK == 0
    fn new(leaf: &'a Leaf<K, V>) -> Option<Inserter<'a, K, V>> {
        let mut current = leaf.metadata.load(Relaxed);
        loop {
            if (current & INVALIDATED) == INVALIDATED {
                // invalidated
                return None;
            }

            let mut full = true;
            let mut position = ARRAY_SIZE;
            for i in 0..ARRAY_SIZE {
                let rank = current & (INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE));
                if rank == 0 {
                    full = false;
                    if current & (OCCUPANCY_BIT << i) == 0 {
                        // initial state
                        position = i;
                        break;
                    }
                }
            }

            if full {
                // full
                return None;
            } else if position == ARRAY_SIZE {
                // in-doubt
                current = leaf.metadata.load(Relaxed);
                continue;
            }

            // found an empty position
            match leaf.metadata.compare_exchange(
                current,
                current | (OCCUPANCY_BIT << position),
                Acquire,
                Relaxed,
            ) {
                Ok(result) => {
                    return Some(Inserter {
                        leaf,
                        committed: false,
                        metadata: result | (OCCUPANCY_BIT << position),
                        index: position,
                    })
                }
                Err(result) => current = result,
            }
        }
        None
    }

    fn commit(&mut self, updated_rank_map: u32) -> bool {
        let mut current = self.metadata;
        loop {
            let next = (current & (!INDEX_RANK_MAP_MASK)) | updated_rank_map;
            if let Err(result) = self
                .leaf
                .metadata
                .compare_exchange(current, next, Release, Relaxed)
            {
                if (result & INDEX_RANK_MAP_MASK) == (current & INDEX_RANK_MAP_MASK) {
                    current = result;
                    continue;
                }
                // rollback metadata changes if not committed
                return false;
            }
            break;
        }
        self.committed = true;
        true
    }
}

impl<'a, K: Clone + Ord + Sync, V: Clone + Sync> Drop for Inserter<'a, K, V> {
    fn drop(&mut self) {
        if !self.committed {
            // rollback metadata changes if not committed
            let mut current = self.metadata;
            loop {
                if let Err(result) = self.leaf.metadata.compare_exchange(
                    current,
                    current & (!(OCCUPANCY_BIT << self.index)),
                    Release,
                    Relaxed,
                ) {
                    current = result;
                    continue;
                }
                break;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn static_assertions() {
        assert_eq!(MAX_KEY_REMOVED & OCCUPANCY_MASK, 0);
        assert_eq!(MAX_KEY_REMOVED & INDEX_RANK_MAP_MASK, 0);
        assert_eq!(INDEX_RANK_MAP_MASK & OCCUPANCY_MASK, 0);
        assert_eq!(
            INDEX_RANK_MAP_MASK & INDEX_RANK_ENTRY_MASK,
            INDEX_RANK_ENTRY_MASK
        );
        assert_eq!(OCCUPANCY_MASK & OCCUPANCY_BIT, OCCUPANCY_BIT);
    }

    #[test]
    fn modification() {
        let num_threads = (ARRAY_SIZE + 1) as usize;
        let mut leaf = Leaf::new(None);
        leaf.insert(10, 10);
        leaf.insert(11, 10);
        leaf.insert(12, 10);
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
