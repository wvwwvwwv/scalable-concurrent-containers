use std::cmp::Ordering;
use std::convert::TryInto;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

pub const ARRAY_SIZE: usize = 12;

/// Metadata layout: removed: 4-bit | occupancy: 12-bit | rank-index map: 48-bit
///
/// State interpretation
///  - !OCCUPIED && RANK = 0: initial state
///  - OCCUPIED && RANK = 0: locked
///  - OCCUPIED && RANK > 0: inserted
///  - !OCCUPIED && RANK > 0: removed
const INDEX_RANK_ENTRY_SIZE: usize = 4;
const REMOVED_BIT: u64 = 1u64 << 60;
const REMOVED: u64 = ((1u64 << INDEX_RANK_ENTRY_SIZE) - 1) << 60;
const OCCUPANCY_BIT: u64 = 1u64 << 48;
const OCCUPANCY_MASK: u64 = ((1u64 << ARRAY_SIZE) - 1) << 48;
const INDEX_RANK_MAP_MASK: u64 = OCCUPANCY_BIT - 1;
const INDEX_RANK_ENTRY_MASK: u64 = (1u64 << INDEX_RANK_ENTRY_SIZE) - 1;

/// Each entry in an EntryArray is never dropped until the Leaf is dropped once constructed.
pub type EntryArray<K, V> = [MaybeUninit<(K, V)>; ARRAY_SIZE];

/// Leaf is an ordered array of key-value pairs.
///
/// A constructed key-value pair entry is never dropped until the entire Leaf instance is dropped.
pub struct Leaf<K: Ord + Sync, V: Sync> {
    entry_array: EntryArray<K, V>,
    metadata: AtomicU64,
}

impl<K: Ord + Sync, V: Sync> Leaf<K, V> {
    pub fn new() -> Leaf<K, V> {
        Leaf {
            entry_array: unsafe { MaybeUninit::uninit().assume_init() },
            metadata: AtomicU64::new(0),
        }
    }

    pub fn cardinality(&self) -> usize {
        let metadata = self.metadata.load(Relaxed);
        (metadata & OCCUPANCY_MASK).count_ones() as usize
    }

    pub fn num_removed(&self) -> usize {
        let metadata = self.metadata.load(Relaxed);
        ((metadata & REMOVED) / REMOVED_BIT) as usize
    }

    pub fn full(&self) -> bool {
        let metadata = self.metadata.load(Relaxed);
        let cardinality = (metadata & OCCUPANCY_MASK).count_ones() as usize;
        let removed = ((metadata & REMOVED) / REMOVED_BIT) as usize;
        cardinality + removed == ARRAY_SIZE
    }

    pub fn max_key(&self) -> Option<&K> {
        let metadata = self.metadata.load(Acquire);
        let mut max_rank = 0;
        let mut max_index = ARRAY_SIZE;
        for i in 0..ARRAY_SIZE {
            let rank = ((metadata & (INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE)))
                >> (i * INDEX_RANK_ENTRY_SIZE)) as usize;
            if rank > max_rank && (metadata & (OCCUPANCY_BIT << i)) != 0 {
                max_rank = rank;
                max_index = i;
            }
        }
        if max_rank > 0 {
            return Some(self.read(max_index).0);
        }
        None
    }

    pub fn insert(&self, key: K, value: V, upsert: bool) -> Option<((K, V), bool)> {
        let mut duplicate_entry = usize::MAX;
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
                    let rank_bits: u64 = ((rank + 1) << (i * INDEX_RANK_ENTRY_SIZE))
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
                        let rank_bits: u64 = ((rank + 1) << (i * INDEX_RANK_ENTRY_SIZE))
                            .try_into()
                            .unwrap();
                        updated_rank_map = (updated_rank_map
                            & (!(INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE))))
                            | rank_bits;
                    }
                    Ordering::Equal => {
                        if inserter.metadata & (OCCUPANCY_BIT << i) != 0 {
                            if !upsert {
                                // uniqueness check failed
                                return Some((entry, true));
                            }
                            duplicate_entry = i;
                        }
                        // regard the entry as a lower ranked one
                        if max_min_rank < rank {
                            max_min_rank = rank;
                        }
                    }
                }
            }
            let final_rank = max_min_rank + 1;
            debug_assert!(min_max_rank == ARRAY_SIZE + 1 || final_rank == min_max_rank);

            // update its own rank
            let rank_bits: u64 = (final_rank << (inserter.index * INDEX_RANK_ENTRY_SIZE))
                .try_into()
                .unwrap();
            updated_rank_map = (updated_rank_map
                & (!(INDEX_RANK_ENTRY_MASK << (inserter.index * INDEX_RANK_ENTRY_SIZE))))
                | rank_bits;

            // insert the key value
            self.write(inserter.index, entry.0, entry.1);

            // try commit
            if inserter.commit(updated_rank_map, duplicate_entry) {
                // inserted
                return None;
            }
            entry = self.take(inserter.index);
        }

        // full
        let duplicate_key = self.search(&entry.0).is_some();
        Some((entry, duplicate_key))
    }

    pub fn remove(&self, key: &K) -> bool {
        let mut metadata = self.metadata.load(Acquire);
        let mut max_min_rank = 0;
        let mut min_max_rank = ARRAY_SIZE + 1;
        for i in 0..ARRAY_SIZE {
            let rank = ((metadata & (INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE)))
                >> (i * INDEX_RANK_ENTRY_SIZE)) as usize;
            if rank > max_min_rank && rank < min_max_rank && (metadata & (OCCUPANCY_BIT << i)) != 0
            {
                match self.compare(i, key) {
                    Ordering::Less => {
                        if max_min_rank < rank {
                            max_min_rank = rank;
                        }
                    }
                    Ordering::Greater => {
                        if min_max_rank > rank {
                            min_max_rank = rank;
                        }
                    }
                    Ordering::Equal => loop {
                        match self.metadata.compare_exchange(
                            metadata,
                            (metadata & (!(OCCUPANCY_BIT << i))) + REMOVED_BIT,
                            Release,
                            Relaxed,
                        ) {
                            Ok(_) => return true,
                            Err(result) => {
                                if result & (OCCUPANCY_BIT << i) == 0 {
                                    return false;
                                }
                                metadata = result;
                            }
                        }
                    },
                }
            }
        }
        false
    }

    pub fn search(&self, key: &K) -> Option<&V> {
        let metadata = self.metadata.load(Acquire);
        let mut max_min_rank = 0;
        let mut min_max_rank = ARRAY_SIZE + 1;
        for i in 0..ARRAY_SIZE {
            let rank = ((metadata & (INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE)))
                >> (i * INDEX_RANK_ENTRY_SIZE)) as usize;
            if rank > max_min_rank && rank < min_max_rank && (metadata & (OCCUPANCY_BIT << i)) != 0
            {
                match self.compare(i, key) {
                    Ordering::Less => {
                        if max_min_rank < rank {
                            max_min_rank = rank;
                        }
                    }
                    Ordering::Greater => {
                        if min_max_rank > rank {
                            min_max_rank = rank;
                        }
                    }
                    Ordering::Equal => {
                        return Some(self.read(i).1);
                    }
                }
            }
        }
        None
    }

    /// Returns the minimum entry among those that are not Ordering::Less than the given key.
    pub fn min_ge(&self, key: &K) -> Option<(&K, &V)> {
        let metadata = self.metadata.load(Acquire);
        let mut max_min_rank = 0;
        let mut min_max_rank = ARRAY_SIZE + 1;
        let mut min_max_index = ARRAY_SIZE;
        for i in 0..ARRAY_SIZE {
            let rank = ((metadata & (INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE)))
                >> (i * INDEX_RANK_ENTRY_SIZE)) as usize;
            if rank > max_min_rank && rank < min_max_rank && (metadata & (OCCUPANCY_BIT << i)) != 0
            {
                match self.compare(i, key) {
                    Ordering::Less => {
                        if max_min_rank < rank {
                            max_min_rank = rank;
                        }
                    }
                    Ordering::Greater => {
                        if min_max_rank > rank {
                            min_max_rank = rank;
                            min_max_index = i;
                        }
                    }
                    Ordering::Equal => {
                        return Some(self.read(i));
                    }
                }
            }
        }
        if min_max_rank <= ARRAY_SIZE {
            return Some(self.read(min_max_index));
        }
        None
    }

    fn write(&self, index: usize, key: K, value: V) {
        unsafe {
            self.entry_array_mut_ref()[index]
                .as_mut_ptr()
                .write((key, value))
        };
    }

    pub fn next(&self, rank: usize) -> (usize, *const (K, V)) {
        let metadata = self.metadata.load(Acquire);
        if rank < ARRAY_SIZE {
            let mut next_rank = ARRAY_SIZE + 1;
            let mut next_index = ARRAY_SIZE;
            for i in 0..ARRAY_SIZE {
                if metadata & (OCCUPANCY_BIT << i) == 0 {
                    continue;
                }
                let current_entry_rank =
                    ((metadata & (INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE)))
                        >> (i * INDEX_RANK_ENTRY_SIZE)) as usize;
                if rank < current_entry_rank && current_entry_rank < next_rank {
                    next_rank = current_entry_rank;
                    next_index = i;
                }
            }
            if next_rank <= ARRAY_SIZE {
                return (next_rank, unsafe {
                    &*self.entry_array[next_index].as_ptr()
                });
            }
        }
        (usize::MAX, std::ptr::null())
    }

    fn compare(&self, index: usize, key: &K) -> std::cmp::Ordering {
        let entry_ref = unsafe { &*self.entry_array[index].as_ptr() };
        entry_ref.0.cmp(key)
    }

    fn take(&self, index: usize) -> (K, V) {
        let entry_ptr = &mut self.entry_array_mut_ref()[index] as *mut MaybeUninit<(K, V)>;
        unsafe { std::ptr::replace(entry_ptr, MaybeUninit::uninit()).assume_init() }
    }

    fn read(&self, index: usize) -> (&K, &V) {
        let entry_ref = unsafe { &*self.entry_array[index].as_ptr() };
        (&entry_ref.0, &entry_ref.1)
    }

    fn entry_array_mut_ref(&self) -> &mut EntryArray<K, V> {
        let entry_array_ptr = &self.entry_array as *const EntryArray<K, V>;
        let entry_array_mut_ptr = entry_array_ptr as *mut EntryArray<K, V>;
        unsafe { &mut (*entry_array_mut_ptr) }
    }
}

impl<K: Ord + Sync, V: Sync> Drop for Leaf<K, V> {
    fn drop(&mut self) {
        let metadata = self.metadata.swap(0, Acquire);
        for i in 0..ARRAY_SIZE {
            if metadata & (INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE)) != 0 {
                self.take(i);
            }
        }
    }
}

struct Inserter<'a, K: Ord + Sync, V: Sync> {
    leaf: &'a Leaf<K, V>,
    committed: bool,
    metadata: u64,
    index: usize,
}

impl<'a, K: Ord + Sync, V: Sync> Inserter<'a, K, V> {
    /// Returns Some if OCCUPIED && RANK == 0
    fn new(leaf: &'a Leaf<K, V>) -> Option<Inserter<'a, K, V>> {
        let mut current = leaf.metadata.load(Relaxed);
        loop {
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
    }

    fn commit(&mut self, updated_rank_map: u64, entry_to_remove: usize) -> bool {
        let mut current = self.metadata;
        loop {
            let mut next = (current & (!INDEX_RANK_MAP_MASK)) | updated_rank_map;
            if entry_to_remove != usize::MAX {
                next &= !(OCCUPANCY_BIT << entry_to_remove);
                next += REMOVED_BIT;
            }
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

impl<'a, K: Ord + Sync, V: Sync> Drop for Inserter<'a, K, V> {
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

/// Leaf scanner.
pub struct Scanner<'a, K: Ord + Sync, V: Sync> {
    leaf: &'a Leaf<K, V>,
    entry_ptr: *const (K, V),
    entry_rank: usize,
}

impl<'a, K: Ord + Sync, V: Sync> Scanner<'a, K, V> {
    pub fn new(leaf: &'a Leaf<K, V>) -> Scanner<'a, K, V> {
        Scanner {
            leaf,
            entry_ptr: std::ptr::null(),
            entry_rank: 0,
        }
    }

    fn proceed(&mut self) {
        self.entry_ptr = std::ptr::null();
        if self.entry_rank == usize::MAX {
            return;
        }
        let (rank, entry_ptr) = self.leaf.next(self.entry_rank);
        self.entry_rank = rank;
        self.entry_ptr = entry_ptr;
    }
}

impl<'a, K: Ord + Sync, V: Sync> Iterator for Scanner<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        self.proceed();
        if self.entry_ptr.is_null() {
            return None;
        }
        unsafe { Some((&(*self.entry_ptr).0, &(*self.entry_ptr).1)) }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn static_assertions() {
        assert_eq!(REMOVED & OCCUPANCY_MASK, 0);
        assert_eq!(INDEX_RANK_MAP_MASK & OCCUPANCY_MASK, 0);
        assert_eq!(
            INDEX_RANK_MAP_MASK & INDEX_RANK_ENTRY_MASK,
            INDEX_RANK_ENTRY_MASK
        );
        assert_eq!(OCCUPANCY_MASK & OCCUPANCY_BIT, OCCUPANCY_BIT);
    }

    #[test]
    fn basic() {
        let leaf = Leaf::new();
        for i in 0..ARRAY_SIZE {
            assert!(leaf
                .insert((ARRAY_SIZE - i - 1) * 2 + 1, 10, false)
                .is_none());
        }
        for i in 0..ARRAY_SIZE {
            assert_eq!(leaf.min_ge(&(i * 2)), Some((&(i * 2 + 1), &10)));
            assert_eq!(leaf.min_ge(&(i * 2 + 1)), Some((&(i * 2 + 1), &10)));
        }
        assert!(leaf.min_ge(&(ARRAY_SIZE * 3)).is_none());
        let leaf = Leaf::new();
        assert_eq!(leaf.num_removed(), 0);
        assert_eq!(leaf.cardinality(), 0);
        assert!(leaf.insert(50, 51, false).is_none());
        assert_eq!(leaf.cardinality(), 1);
        assert_eq!(leaf.max_key(), Some(&50));
        assert!(leaf.insert(60, 60, false).is_none());
        assert_eq!(leaf.cardinality(), 2);
        assert!(leaf.insert(70, 71, false).is_none());
        assert_eq!(leaf.cardinality(), 3);
        assert!(leaf.insert(60, 61, true).is_none());
        assert_eq!(leaf.cardinality(), 3);
        assert_eq!(leaf.num_removed(), 1);
        assert_eq!(leaf.min_ge(&71), None);
        assert_eq!(leaf.min_ge(&51), Some((&60, &61)));
        assert_eq!(leaf.min_ge(&50), Some((&50, &51)));
        assert_eq!(leaf.min_ge(&49), Some((&50, &51)));
        assert!(leaf.remove(&60));
        assert_eq!(leaf.num_removed(), 2);
        assert_eq!(leaf.cardinality(), 2);
        assert!(!leaf.full());
        assert!(leaf.insert(40, 40, false).is_none());
        assert!(leaf.insert(30, 31, false).is_none());
        assert_eq!(leaf.cardinality(), 4);
        assert!(!leaf.full());
        assert!(leaf.insert(40, 41, true).is_none());
        assert_eq!(leaf.insert(30, 33, false), Some(((30, 33), true)));
        assert_eq!(leaf.num_removed(), 3);
        assert_eq!(leaf.cardinality(), 4);
        assert!(leaf.insert(10, 11, false).is_none());
        assert!(leaf.insert(11, 12, false).is_none());
        assert!(leaf.insert(13, 13, false).is_none());
        assert!(leaf.insert(54, 55, false).is_none());
        assert!(leaf.insert(13, 14, true).is_none());
        assert_eq!(leaf.num_removed(), 4);
        assert_eq!(leaf.cardinality(), 8);
        assert_eq!(leaf.max_key(), Some(&70));
        assert!(leaf.full());

        let mut scanner = Scanner::new(&leaf);
        let mut prev_key = 0;
        let mut iterated = 0;
        while let Some(entry) = scanner.next() {
            assert!(prev_key < *entry.0);
            assert_eq!(*entry.0 + 1, *entry.1);
            prev_key = *entry.0;
            iterated += 1;
        }
        assert_eq!(iterated, leaf.cardinality());
        drop(scanner);

        let leaf = Leaf::new();
        assert_eq!(leaf.num_removed(), 0);
        assert_eq!(leaf.cardinality(), 0);
        assert!(leaf.insert(20, 21, false).is_none());
        assert_eq!(leaf.cardinality(), 1);
        assert!(leaf.insert(10, 11, false).is_none());
        assert_eq!(leaf.cardinality(), 2);
        assert_eq!(*leaf.search(&10).unwrap(), 11);
        assert!(leaf.insert(11, 12, false).is_none());
        assert_eq!(leaf.max_key(), Some(&20));
        assert_eq!(leaf.cardinality(), 3);
        assert_eq!(leaf.insert(11, 12, false), Some(((11, 12), true)));
        assert_eq!(leaf.cardinality(), 3);
        assert_eq!(*leaf.search(&11).unwrap(), 12);
        assert!(leaf.insert(12, 13, false).is_none());
        assert_eq!(leaf.cardinality(), 4);
        assert_eq!(*leaf.search(&12).unwrap(), 13);
        assert_eq!(leaf.min_ge(&21), None);
        assert_eq!(leaf.min_ge(&20), Some((&20, &21)));
        assert_eq!(leaf.min_ge(&19), Some((&20, &21)));
        assert_eq!(leaf.min_ge(&0), Some((&10, &11)));
        assert!(leaf.insert(2, 3, false).is_none());
        assert_eq!(leaf.cardinality(), 5);
        assert_eq!(*leaf.search(&2).unwrap(), 3);
        assert_eq!(leaf.insert(2, 3, false), Some(((2, 3), true)));
        assert_eq!(leaf.min_ge(&8), Some((&10, &11)));
        assert_eq!(leaf.cardinality(), 5);
        assert_eq!(*leaf.search(&2).unwrap(), 3);
        assert!(leaf.insert(1, 2, false).is_none());
        assert_eq!(leaf.cardinality(), 6);
        assert_eq!(*leaf.search(&1).unwrap(), 2);
        assert!(leaf.insert(13, 14, false).is_none());
        assert_eq!(leaf.cardinality(), 7);
        assert_eq!(*leaf.search(&13).unwrap(), 14);
        assert_eq!(leaf.insert(13, 14, false), Some(((13, 14), true)));
        assert_eq!(leaf.cardinality(), 7);
        assert!(leaf.remove(&10));
        assert_eq!(leaf.cardinality(), 6);
        assert!(leaf.remove(&11));
        assert_eq!(leaf.cardinality(), 5);
        assert!(leaf.remove(&12));
        assert_eq!(leaf.cardinality(), 4);
        assert_eq!(leaf.num_removed(), 3);
        assert!(!leaf.full());
        assert!(leaf.insert(20, 21, true).is_none());
        assert_eq!(leaf.num_removed(), 4);
        assert_eq!(leaf.cardinality(), 4);
        assert!(leaf.insert(12, 13, false).is_none());
        assert!(leaf.insert(14, 15, false).is_none());
        assert_eq!(leaf.cardinality(), 6);
        assert!(leaf.search(&11).is_none());
        assert!(!leaf.remove(&10));
        assert!(!leaf.remove(&11));
        assert!(leaf.remove(&12));
        assert_eq!(leaf.cardinality(), 5);
        assert!(leaf.search(&12).is_none());
        assert_eq!(*leaf.search(&20).unwrap(), 21);
        assert!(leaf.insert(10, 11, false).is_none());
        assert!(leaf.insert(12, 13, false).is_none());
        assert_eq!(leaf.max_key(), Some(&20));
        assert!(leaf.full());
        assert_eq!(leaf.num_removed(), 5);
        assert_eq!(leaf.cardinality(), 7);

        let mut scanner = Scanner::new(&leaf);
        let mut prev_key = 0;
        let mut iterated = 0;
        while let Some(entry) = scanner.next() {
            assert!(prev_key < *entry.0);
            assert_eq!(*entry.0 + 1, *entry.1);
            prev_key = *entry.0;
            iterated += 1;
        }
        assert_eq!(iterated, leaf.cardinality());
        drop(scanner);
    }

    #[test]
    fn update() {
        // [TODO]: fix BUGS!!
        let num_threads = (ARRAY_SIZE + 1) as usize;
        for _ in 0..256 {
            let barrier = Arc::new(Barrier::new(num_threads));
            let leaf = Arc::new(Leaf::new());
            leaf.insert(num_threads * 2, 1, false);
            let mut thread_handles = Vec::with_capacity(num_threads);
            for tid in 0..num_threads {
                let barrier_copied = barrier.clone();
                let leaf_copied = leaf.clone();
                thread_handles.push(thread::spawn(move || {
                    barrier_copied.wait();
                    assert_eq!(
                        leaf_copied.insert(num_threads * 2, num_threads, false),
                        Some(((num_threads * 2, num_threads), true))
                    );
                    let result = leaf_copied.insert(tid, 1, false);
                    if result.is_none() {
                        assert_eq!(*leaf_copied.search(&tid).unwrap(), 1);
                        if tid % 2 != 0 {
                            assert!(leaf_copied.remove(&tid));
                        }
                    }
                    let mut scanner = Scanner::new(&leaf_copied);
                    let mut prev_key = 0;
                    while let Some(entry) = scanner.next() {
                        assert_eq!(entry.1, &1);
                        //assert!(prev_key < *entry.0);
                        prev_key = *entry.0;
                    }
                }));
            }
            for handle in thread_handles {
                handle.join().unwrap();
            }
            let mut scanner = Scanner::new(&leaf);
            let mut prev_key = 0;
            while let Some(entry) = scanner.next() {
                println!("{}", entry.0);
                assert_eq!(entry.1, &1);
                //assert!(prev_key < *entry.0);
                assert!(entry.0 % 2 == 0);
                prev_key = *entry.0;
            }
        }
    }
}
