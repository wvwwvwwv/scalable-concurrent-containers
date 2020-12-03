use crossbeam_epoch::{Atomic, Guard, Shared};
use std::cmp::Ordering;
use std::convert::TryInto;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

pub const ARRAY_SIZE: usize = 7;

/// Metadata layout: removed: 3-bit | max-key removed: 1-bit | occupancy: 7-bit | rank-index map: 21-bit
///
/// State interpretation
///  - !OCCUPIED && RANK = 0: initial state
///  - OCCUPIED && RANK = 0: locked
///  - OCCUPIED && RANK > 0: inserted
///  - !OCCUPIED && RANK > 0: removed
const INDEX_RANK_ENTRY_SIZE: usize = 3;
const REMOVED_BIT: u32 = 1u32 << 29;
const REMOVED: u32 = ((1 << INDEX_RANK_ENTRY_SIZE) - 1) << 29;
const MAX_KEY_VALID: u32 = 1u32 << ARRAY_SIZE * (INDEX_RANK_ENTRY_SIZE + 1);
const OCCUPANCY_BIT: u32 = 1u32 << (ARRAY_SIZE * INDEX_RANK_ENTRY_SIZE as usize);
const OCCUPANCY_MASK: u32 =
    ((1u32 << ARRAY_SIZE) - 1) << (ARRAY_SIZE * INDEX_RANK_ENTRY_SIZE as usize);
const INDEX_RANK_MAP_MASK: u32 = (1u32 << (ARRAY_SIZE * INDEX_RANK_ENTRY_SIZE)) - 1;
const INDEX_RANK_ENTRY_MASK: u32 = (1u32 << INDEX_RANK_ENTRY_SIZE) - 1;

/// Each entry in an EntryArray is never dropped until the Leaf is dropped once constructed.
pub type EntryArray<K, V> = [MaybeUninit<(K, V)>; ARRAY_SIZE];

/// Leaf stores at most eight key-value pairs.
///
/// A constructed key-value pair entry is never dropped until the Leaf instance is dropped.
pub struct Leaf<K: Ord + Sync, V: Sync> {
    entry_array: EntryArray<K, V>,
    max_key_entry: Option<(K, V)>,
    metadata: AtomicU32,
    next: Atomic<Leaf<K, V>>,
}

impl<K: Ord + Sync, V: Sync> Leaf<K, V> {
    pub fn new(max_key_entry: Option<(K, V)>, next: Atomic<Leaf<K, V>>) -> Leaf<K, V> {
        let initial_metadata = if max_key_entry.is_some() {
            MAX_KEY_VALID
        } else {
            0
        };
        Leaf {
            entry_array: unsafe { MaybeUninit::uninit().assume_init() },
            max_key_entry,
            metadata: AtomicU32::new(initial_metadata),
            next,
        }
    }

    pub fn cardinality(&self) -> usize {
        let metadata = self.metadata.load(Relaxed);
        let occupancy_metadata = metadata & (MAX_KEY_VALID | OCCUPANCY_MASK);
        occupancy_metadata.count_ones() as usize
    }

    pub fn num_removed(&self) -> usize {
        let metadata = self.metadata.load(Relaxed);
        self.max_key_entry
            .as_ref()
            .map_or_else(|| 0, |_| if metadata & MAX_KEY_VALID == 0 { 1 } else { 0 })
            + ((metadata & REMOVED) / REMOVED_BIT) as usize
    }

    pub fn full(&self) -> bool {
        let metadata = self.metadata.load(Relaxed);
        let cardinality = (metadata & (MAX_KEY_VALID | OCCUPANCY_MASK)).count_ones();
        let removed = self
            .max_key_entry
            .as_ref()
            .map_or_else(|| 1, |_| if metadata & MAX_KEY_VALID == 0 { 1 } else { 0 })
            + ((metadata & REMOVED) / REMOVED_BIT);
        cardinality + removed == (ARRAY_SIZE + 1).try_into().unwrap()
    }

    pub fn insert(&self, key: K, value: V, is_upsert: bool) -> Option<(K, V)> {
        let mut duplicate_entry = usize::MAX;
        match self
            .max_key_entry
            .as_ref()
            .map_or_else(|| Ordering::Greater, |entry| entry.0.cmp(&key))
        {
            Ordering::Less => {
                // the key doesn't fit the leaf
                return Some((key, value));
            }
            Ordering::Greater => (),
            Ordering::Equal => {
                if !is_upsert {
                    // the key doesn't fit the leaf
                    return Some((key, value));
                }
                duplicate_entry = ARRAY_SIZE;
            }
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
                        if inserter.metadata & (OCCUPANCY_BIT << i) != 0 {
                            if !is_upsert {
                                // duplicate entry
                                return Some(entry);
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
            let rank_bits: u32 = (final_rank << (inserter.index * INDEX_RANK_ENTRY_SIZE))
                .try_into()
                .unwrap();
            updated_rank_map = (updated_rank_map
                & (!(INDEX_RANK_ENTRY_MASK << (inserter.index * INDEX_RANK_ENTRY_SIZE))))
                | rank_bits;

            // insert the key value
            self.write(inserter.index, entry.0, entry.1);

            // try commit
            if inserter.commit(updated_rank_map, duplicate_entry) {
                return None;
            }
            entry = self.take(inserter.index);
        }
        Some(entry)
    }

    pub fn remove(&self, key: &K) -> bool {
        let mut metadata = self.metadata.load(Acquire);
        if metadata & MAX_KEY_VALID != 0
            && self
                .max_key_entry
                .as_ref()
                .map_or_else(|| false, |entry| entry.0.cmp(&key) == Ordering::Equal)
        {
            loop {
                match self.metadata.compare_exchange(
                    metadata,
                    metadata & (!MAX_KEY_VALID),
                    Release,
                    Relaxed,
                ) {
                    Ok(_) => return true,
                    Err(result) => {
                        if result & MAX_KEY_VALID == 0 {
                            return false;
                        }
                        metadata = result;
                    }
                }
            }
        }

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
        if metadata & MAX_KEY_VALID != 0
            && self
                .max_key_entry
                .as_ref()
                .map_or_else(|| false, |entry| entry.0.cmp(&key) == Ordering::Equal)
        {
            return self.max_key_entry.as_ref().map(|entry| &entry.1);
        }

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
        if min_max_rank < ARRAY_SIZE {
            return Some(self.read(min_max_index));
        } else if metadata & MAX_KEY_VALID != 0
            && self
                .max_key_entry
                .as_ref()
                .map_or_else(|| false, |entry| entry.0.cmp(&key) != Ordering::Less)
        {
            return self
                .max_key_entry
                .as_ref()
                .map(|entry| (&entry.0, &entry.1));
        }

        None
    }

    pub fn jump<'a>(&self, guard: &'a Guard) -> Shared<'a, Leaf<K, V>> {
        self.next.load(Relaxed, &guard)
    }

    pub fn update<'a>(
        &self,
        next: Shared<'a, Leaf<K, V>>,
        guard: &'a Guard,
    ) -> Shared<'a, Leaf<K, V>> {
        self.next.swap(next, Release, guard)
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

        if rank <= ARRAY_SIZE && metadata & MAX_KEY_VALID != 0 && self.max_key_entry.is_some() {
            return (
                ARRAY_SIZE + 1,
                self.max_key_entry
                    .as_ref()
                    .map_or_else(std::ptr::null, |entry| entry as *const (K, V)),
            );
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
    metadata: u32,
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

    fn commit(&mut self, updated_rank_map: u32, entry_to_remove: usize) -> bool {
        let mut current = self.metadata;
        loop {
            let mut next = (current & (!INDEX_RANK_MAP_MASK)) | updated_rank_map;
            if entry_to_remove < ARRAY_SIZE {
                next &= !(OCCUPANCY_BIT << entry_to_remove);
                next += REMOVED_BIT;
            } else if entry_to_remove == ARRAY_SIZE {
                next &= !MAX_KEY_VALID;
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
///
/// A scanner instance holds a Guard, thus preventing GC.
pub struct Scanner<'a, K: Ord + Sync, V: Sync> {
    /// It emulates TreeMap<'a, K, V>: to be replaced with treemap: TreeMap<'a, K, V>
    head: &'a Leaf<K, V>,
    leaf: *const Leaf<K, V>,
    guard: &'a Guard,
    entry_ptr: *const (K, V),
    entry_rank: usize,
    jump: bool,
}

impl<'a, K: Ord + Sync, V: Sync> Scanner<'a, K, V> {
    pub fn new(head: &'a Leaf<K, V>, jump: bool, guard: &'a Guard) -> Scanner<'a, K, V> {
        Scanner {
            head,
            leaf: std::ptr::null(),
            guard,
            entry_ptr: std::ptr::null(),
            entry_rank: 0,
            jump,
        }
    }

    fn proceed(&mut self) {
        self.entry_ptr = std::ptr::null();
        if self.leaf.is_null() {
            if self.entry_rank == 0 {
                // to be replaced with self.treemap.head();
                self.leaf = self.head as *const Leaf<K, V>;
            } else {
                return;
            }
        }

        let (rank, entry_ptr) = unsafe { (*self.leaf).next(self.entry_rank) };
        self.entry_rank = rank;
        self.entry_ptr = entry_ptr;
        while self.entry_rank == usize::MAX {
            self.leaf = if self.jump {
                unsafe { (*self.leaf).jump(self.guard) }.as_raw()
            } else {
                std::ptr::null()
            };
            if self.leaf.is_null() {
                self.entry_rank = usize::MAX;
                return;
            }
            let (rank, entry_ptr) = unsafe { (*self.leaf).next(0) };
            self.entry_rank = rank;
            self.entry_ptr = entry_ptr;
        }
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
        assert_eq!(MAX_KEY_VALID & OCCUPANCY_MASK, 0);
        assert_eq!(MAX_KEY_VALID & INDEX_RANK_MAP_MASK, 0);
        assert_eq!(INDEX_RANK_MAP_MASK & OCCUPANCY_MASK, 0);
        assert_eq!(
            INDEX_RANK_MAP_MASK & INDEX_RANK_ENTRY_MASK,
            INDEX_RANK_ENTRY_MASK
        );
        assert_eq!(OCCUPANCY_MASK & OCCUPANCY_BIT, OCCUPANCY_BIT);
    }

    #[test]
    fn basic() {
        let guard = crossbeam_epoch::pin();
        let tail = Atomic::new(Leaf::new(None, Atomic::null()));
        let tail_ref = unsafe { tail.load(Relaxed, &guard).deref() };
        assert_eq!(tail_ref.num_removed(), 0);
        assert_eq!(tail_ref.cardinality(), 0);
        assert!(tail_ref.insert(50, 51, false).is_none());
        assert_eq!(tail_ref.cardinality(), 1);
        assert!(tail_ref.insert(60, 60, false).is_none());
        assert_eq!(tail_ref.cardinality(), 2);
        assert!(tail_ref.insert(70, 71, false).is_none());
        assert_eq!(tail_ref.cardinality(), 3);
        assert!(tail_ref.insert(60, 61, true).is_none());
        assert_eq!(tail_ref.cardinality(), 3);
        assert_eq!(tail_ref.num_removed(), 1);
        assert_eq!(tail_ref.min_ge(&71), None);
        assert_eq!(tail_ref.min_ge(&51), Some((&60, &61)));
        assert_eq!(tail_ref.min_ge(&50), Some((&50, &51)));
        assert_eq!(tail_ref.min_ge(&49), Some((&50, &51)));
        assert!(tail_ref.remove(&60));
        assert_eq!(tail_ref.num_removed(), 2);
        assert_eq!(tail_ref.cardinality(), 2);
        assert!(!tail_ref.full());
        assert!(tail_ref.insert(40, 40, false).is_none());
        assert!(tail_ref.insert(30, 31, false).is_none());
        assert_eq!(tail_ref.cardinality(), 4);
        assert!(!tail_ref.full());
        assert!(tail_ref.insert(40, 41, true).is_none());
        assert_eq!(tail_ref.insert(30, 33, false), Some((30, 33)));
        assert_eq!(tail_ref.num_removed(), 3);
        assert_eq!(tail_ref.cardinality(), 4);
        assert!(tail_ref.full());
        drop(tail_ref);

        let leaf = Leaf::new(Some((20, 20)), tail);
        assert_eq!(leaf.num_removed(), 0);
        assert_eq!(leaf.cardinality(), 1);
        assert_eq!(leaf.insert(22, 23, false), Some((22, 23)));
        assert_eq!(leaf.cardinality(), 1);
        assert!(leaf.insert(10, 11, false).is_none());
        assert_eq!(leaf.cardinality(), 2);
        assert_eq!(*leaf.search(&10).unwrap(), 11);
        assert!(leaf.insert(11, 12, false).is_none());
        assert_eq!(leaf.cardinality(), 3);
        assert_eq!(leaf.insert(11, 12, false), Some((11, 12)));
        assert_eq!(leaf.cardinality(), 3);
        assert_eq!(*leaf.search(&11).unwrap(), 12);
        assert!(leaf.insert(12, 13, false).is_none());
        assert_eq!(leaf.cardinality(), 4);
        assert_eq!(*leaf.search(&12).unwrap(), 13);
        assert_eq!(leaf.min_ge(&21), None);
        assert_eq!(leaf.min_ge(&20), Some((&20, &20)));
        assert_eq!(leaf.min_ge(&19), Some((&20, &20)));
        assert_eq!(leaf.min_ge(&0), Some((&10, &11)));
        assert!(leaf.insert(2, 3, false).is_none());
        assert_eq!(leaf.cardinality(), 5);
        assert_eq!(*leaf.search(&2).unwrap(), 3);
        assert_eq!(leaf.insert(2, 3, false), Some((2, 3)));
        assert_eq!(leaf.min_ge(&8), Some((&10, &11)));
        assert_eq!(leaf.cardinality(), 5);
        assert_eq!(*leaf.search(&2).unwrap(), 3);
        assert!(leaf.insert(1, 2, false).is_none());
        assert_eq!(leaf.cardinality(), 6);
        assert_eq!(*leaf.search(&1).unwrap(), 2);
        assert!(leaf.insert(13, 14, false).is_none());
        assert_eq!(leaf.cardinality(), 7);
        assert_eq!(*leaf.search(&13).unwrap(), 14);
        assert_eq!(leaf.insert(13, 14, false), Some((13, 14)));
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
        assert!(leaf.full());
        assert_eq!(leaf.num_removed(), 4);
        assert_eq!(leaf.cardinality(), 4);
        assert_eq!(leaf.insert(12, 13, false), Some((12, 13)));
        assert_eq!(leaf.insert(14, 15, false), Some((14, 15)));
        assert_eq!(leaf.cardinality(), 4);
        assert!(leaf.search(&11).is_none());
        assert!(!leaf.remove(&10));
        assert!(!leaf.remove(&11));
        assert!(!leaf.remove(&12));
        assert_eq!(leaf.cardinality(), 4);
        assert!(leaf.search(&14).is_none());
        assert_eq!(*leaf.search(&20).unwrap(), 21);
        assert_eq!(leaf.insert(10, 11, false), Some((10, 11)));

        let mut scanner = Scanner::new(&leaf, true, &guard);
        let mut prev_key = 0;
        let mut iterated = 0;
        while let Some(entry) = scanner.next() {
            println!("{} {}", entry.0, entry.1);
            assert!(prev_key < *entry.0);
            assert_eq!(*entry.0 + 1, *entry.1);
            prev_key = *entry.0;
            iterated += 1;
        }
        assert_eq!(iterated, 8);
        drop(scanner);

        let mut scanner = Scanner::new(&leaf, false, &guard);
        let mut prev_key = 0;
        let mut iterated = 0;
        while let Some(entry) = scanner.next() {
            println!("{} {}", entry.0, entry.1);
            assert!(prev_key < *entry.0);
            assert_eq!(*entry.0 + 1, *entry.1);
            prev_key = *entry.0;
            iterated += 1;
        }
        assert_eq!(iterated, 4);
        drop(scanner);

        let tail_ptr = leaf.update(Shared::null(), &guard);
        unsafe { guard.defer_destroy(tail_ptr) };
    }

    #[test]
    fn update() {
        let num_threads = (ARRAY_SIZE + 1) as usize;
        for _ in 0..256 {
            let barrier = Arc::new(Barrier::new(num_threads));
            let leaf = Arc::new(Leaf::new(Some((10usize, 10usize)), Atomic::null()));
            let mut thread_handles = Vec::with_capacity(num_threads);
            for tid in 0..num_threads {
                let barrier_copied = barrier.clone();
                let leaf_copied = leaf.clone();
                thread_handles.push(thread::spawn(move || {
                    barrier_copied.wait();
                    assert_eq!(leaf_copied.insert(10, 12, false), Some((10, 12)));
                    let result = leaf_copied.insert(tid, 1, false);
                    if result.is_none() {
                        assert_eq!(*leaf_copied.search(&tid).unwrap(), 1);
                        assert!(leaf_copied.remove(&tid));
                    } else {
                        assert!(!leaf_copied.remove(&tid));
                    }
                }));
            }
            for handle in thread_handles {
                handle.join().unwrap();
            }
        }
    }

    #[test]
    fn scan() {
        let num_threads = (ARRAY_SIZE + 1) as usize;
        for _ in 0..256 {
            let barrier = Arc::new(Barrier::new(num_threads + 1));
            let mut leaves: Vec<Atomic<Leaf<usize, usize>>> = Vec::with_capacity(num_threads);
            let head = Atomic::new(Leaf::new(None, Atomic::null()));
            let guard = crossbeam_epoch::pin();
            let mut prev = head.load(Relaxed, &guard);
            for _ in 0..num_threads {
                let current = Atomic::new(Leaf::new(None, Atomic::null()));
                leaves.push(Atomic::from(current.load(Relaxed, &guard)));
                unsafe { prev.deref().update(current.load(Relaxed, &guard), &guard) };
                prev = current.load(Relaxed, &guard);
            }
            let tail = Atomic::new(Leaf::new(None, Atomic::null()));
            unsafe { prev.deref().update(tail.load(Relaxed, &guard), &guard) };
            let mut thread_handles = Vec::with_capacity(num_threads);
            for _ in 0..num_threads {
                let barrier_copied = barrier.clone();
                let head_copied: Atomic<Leaf<usize, usize>> =
                    Atomic::from(head.load(Relaxed, &guard));
                let tail_copied: Atomic<Leaf<usize, usize>> =
                    Atomic::from(tail.load(Relaxed, &guard));
                thread_handles.push(thread::spawn(move || {
                    barrier_copied.wait();
                    for _ in 0..16 {
                        let guard = crossbeam_epoch::pin();
                        let root_ref = unsafe { head_copied.load(Relaxed, &guard).deref() };
                        let mut scanner = Scanner::new(root_ref, true, &guard);
                        while let Some(_) = scanner.next() {}
                        let mut current =
                            unsafe { head_copied.load(Relaxed, &guard).deref().jump(&guard) };
                        while !current.is_null() {
                            let next = unsafe { current.deref().jump(&guard) };
                            if next.is_null() {
                                assert_eq!(
                                    current.as_raw(),
                                    tail_copied.load(Relaxed, &guard).as_raw()
                                );
                                break;
                            }
                            current = next;
                        }
                    }
                }));
            }
            drop(guard);

            barrier.wait();
            for i in 0..num_threads {
                let guard = crossbeam_epoch::pin();
                // 1. make the target leaf unreachable from the tree
                let leaf = leaves[i].swap(Shared::null(), Relaxed, &guard);
                // 2. make the target leaf unreachable by scanners
                let next = unsafe { leaf.deref().jump(&guard) };
                unsafe { head.load(Relaxed, &guard).deref().update(next, &guard) };
                // 3. deferred drop
                unsafe { guard.defer_destroy(leaf) };
            }
            let guard = crossbeam_epoch::pin();
            assert_eq!(
                unsafe { head.load(Relaxed, &guard).deref().jump(&guard).as_raw() },
                tail.load(Relaxed, &guard).as_raw()
            );
            drop(guard);

            for handle in thread_handles {
                handle.join().unwrap();
            }

            for ptr in [head, tail].iter() {
                let guard = crossbeam_epoch::pin();
                let leaf = ptr.swap(Shared::null(), Relaxed, &guard);
                if !leaf.is_null() {
                    unsafe { guard.defer_destroy(leaf) };
                }
            }
        }
    }
}
