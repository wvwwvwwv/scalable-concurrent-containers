use std::cmp::Ordering;
use std::convert::TryInto;
use std::mem::MaybeUninit;
use std::sync::atomic::fence;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

pub const ARRAY_SIZE: usize = 12;

/// Metadata layout: removed: 4-bit | occupancy: 12-bit | rank-index map: 48-bit
///
/// State interpretation
///  - !OCCUPIED && RANK = 0: initial state
///  - OCCUPIED && RANK = 0: locked
///  - OCCUPIED && ARRAY_SIZE >= RANK > 0: inserted
///  - OCCUPIED && RANK = INVALID_RANK: prohibited
///  - !OCCUPIED && ARRAY_SIZE >= RANK > 0: removed
///  - !OCCUPIED && RANK = INVALID_RANK: invalidated
const INDEX_RANK_ENTRY_SIZE: usize = 4;
const REMOVED_BIT: u64 = 1u64 << 60;
const REMOVED: u64 = ((1u64 << INDEX_RANK_ENTRY_SIZE) - 1) << 60;
const OCCUPANCY_BIT: u64 = 1u64 << 48;
const OCCUPANCY_MASK: u64 = ((1u64 << ARRAY_SIZE) - 1) << 48;
const INDEX_RANK_MAP_MASK: u64 = OCCUPANCY_BIT - 1;
const INDEX_RANK_ENTRY_MASK: u64 = (1u64 << INDEX_RANK_ENTRY_SIZE) - 1;
const INVALID_RANK: u64 = (1u64 << INDEX_RANK_ENTRY_SIZE) - 1;

/// Each entry in an EntryArray is never dropped until the Leaf is dropped once constructed.
pub type EntryArray<K, V> = [MaybeUninit<(K, V)>; ARRAY_SIZE];

/// Leaf is an ordered array of key-value pairs.
///
/// A constructed key-value pair entry is never dropped until the entire Leaf instance is dropped.
pub struct Leaf<K: Clone + Ord + Sync, V: Clone + Sync> {
    entry_array: EntryArray<K, V>,
    metadata: AtomicU64,
}

impl<K: Clone + Ord + Sync, V: Clone + Sync> Leaf<K, V> {
    /// Creates a new Leaf.
    pub fn new() -> Leaf<K, V> {
        Leaf {
            entry_array: unsafe { MaybeUninit::uninit().assume_init() },
            metadata: AtomicU64::new(0),
        }
    }

    /// Takes the memory address of the instance as an identifier.
    pub fn id(&self) -> usize {
        self as *const _ as usize
    }

    /// Returns true if the leaf is full.
    pub fn full(&self) -> bool {
        let metadata = self.metadata.load(Relaxed);
        let cardinality = (metadata & OCCUPANCY_MASK).count_ones() as usize;
        let removed = ((metadata & REMOVED) / REMOVED_BIT) as usize;
        cardinality + removed == ARRAY_SIZE
    }

    /// Returns true if the leaf is obsolete.
    pub fn obsolete(&self) -> bool {
        let metadata = self.metadata.load(Relaxed);
        let removed = ((metadata & REMOVED) / REMOVED_BIT) as usize;
        removed == ARRAY_SIZE
    }

    /// Returns a reference to the max key.
    pub fn max(&self) -> Option<(&K, &V)> {
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
            return Some(self.read(max_index));
        }
        None
    }

    /// Inserts a key value pair.ARRAY_SIZE
    ///
    /// It returns the passed key value pair on failure.
    /// The second returned value being true indicates that the same key exists.
    pub fn insert(&self, key: K, value: V, upsert: bool) -> Option<((K, V), bool)> {
        let mut duplicate_entry = usize::MAX;
        let mut entry = (key, value);
        while let Some(mut inserter) = Inserter::new(self) {
            // Calculates the rank and check uniqueness.
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
                if rank == 0 || rank == INVALID_RANK as usize || rank < max_min_rank {
                    continue;
                }
                if rank > min_max_rank {
                    // Updates the rank.
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
                        // Updates the rank.
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
                                // Uniqueness check failed.
                                return Some((entry, true));
                            }
                            duplicate_entry = i;
                        }
                        // Regards the entry as a lower ranked one.
                        if max_min_rank < rank {
                            max_min_rank = rank;
                        }
                    }
                }
            }
            let final_rank = max_min_rank + 1;
            debug_assert!(min_max_rank == ARRAY_SIZE + 1 || final_rank == min_max_rank);

            // Updates its own rank.
            let rank_bits: u64 = (final_rank << (inserter.index * INDEX_RANK_ENTRY_SIZE))
                .try_into()
                .unwrap();
            updated_rank_map = (updated_rank_map
                & (!(INDEX_RANK_ENTRY_MASK << (inserter.index * INDEX_RANK_ENTRY_SIZE))))
                | rank_bits;

            // Inserts the key value.
            self.write(inserter.index, entry.0, entry.1);

            if inserter.commit(updated_rank_map, duplicate_entry) {
                return None;
            }
            entry = self.take(inserter.index);
        }

        // full
        debug_assert!(self.full());
        let duplicate_key = self.search(&entry.0).is_some();
        Some((entry, duplicate_key))
    }

    /// Removes the key.
    ///
    /// If the mark_obsolete flag is set and the leaf has no valid entry, it tries to mark itself obsolete.
    ///
    /// The first value of the result tuple indicates that the key has been removed,
    /// The second value of the result tuple indicates that the leaf is full.
    /// The last value of the result tuple indicates that the leaf has become obsolete.
    pub fn remove(&self, key: &K, mark_obsolete: bool) -> (bool, bool, bool) {
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
                        let mut new_metadata = (metadata & (!(OCCUPANCY_BIT << i))) + REMOVED_BIT;
                        let new_cardinality = (new_metadata & OCCUPANCY_MASK).count_ones() as usize;
                        let new_removed = ((new_metadata & REMOVED) / REMOVED_BIT) as usize;
                        let (full, obsolete) = if mark_obsolete
                            && new_cardinality == 0
                            && new_removed >= ARRAY_SIZE / 2
                        {
                            // Deprecates itself by marking all the available slots invalid.
                            for i in 0..ARRAY_SIZE {
                                if (new_metadata
                                    & (INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE)))
                                    == 0
                                {
                                    new_metadata |= INVALID_RANK << (i * INDEX_RANK_ENTRY_SIZE);
                                }
                            }
                            new_metadata &= !REMOVED;
                            new_metadata += REMOVED_BIT * ARRAY_SIZE as u64;
                            (true, true)
                        } else {
                            (
                                new_cardinality + new_removed == ARRAY_SIZE,
                                new_removed == ARRAY_SIZE,
                            )
                        };
                        match self.metadata.compare_exchange(
                            metadata,
                            new_metadata,
                            Release,
                            Relaxed,
                        ) {
                            Ok(_) => {
                                return (true, full, obsolete);
                            }
                            Err(result) => {
                                metadata = result;
                                if metadata & (OCCUPANCY_BIT << i) == 0 {
                                    // Removed by another thread.
                                    let cardinality =
                                        (metadata & OCCUPANCY_MASK).count_ones() as usize;
                                    let removed = ((metadata & REMOVED) / REMOVED_BIT) as usize;
                                    return (
                                        false,
                                        cardinality + removed == ARRAY_SIZE,
                                        removed == ARRAY_SIZE,
                                    );
                                }
                            }
                        }
                    },
                }
            }
        }
        let cardinality = (metadata & OCCUPANCY_MASK).count_ones() as usize;
        let removed = ((metadata & REMOVED) / REMOVED_BIT) as usize;
        (
            false,
            cardinality + removed == ARRAY_SIZE,
            removed == ARRAY_SIZE,
        )
    }

    /// Returns a value associated with the key.
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

    /// Returns the index and a pointer to the key-value pair associated with the key.
    ///
    /// When the 'exact' argument is false the key does not exists,
    /// the minimum key of those that are greater than the key is returned.
    pub fn from(&self, metadata: u64, key: &K, exact: bool) -> (usize, *const (K, V)) {
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
                        return (i, unsafe { &*self.entry_array[i].as_ptr() });
                    }
                }
            }
        }
        if !exact && min_max_index < ARRAY_SIZE {
            return (min_max_index, unsafe {
                &*self.entry_array[min_max_index].as_ptr()
            });
        }
        (usize::MAX, std::ptr::null())
    }

    /// Returns the minimum entry among those that are not Ordering::Less than the given key.
    ///
    /// It additionally returns the current version of its metadata in order for the caller to validate the sanity of the result.
    pub fn min_greater_equal(&self, key: &K) -> (Option<(&K, &V)>, u64) {
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
                        return (Some(self.read(i)), metadata);
                    }
                }
            }
        }
        if min_max_rank <= ARRAY_SIZE {
            return (Some(self.read(min_max_index)), metadata);
        }
        (None, metadata)
    }

    /// Returns the minimum entry among those that are Ordering::Greater than the given key.
    pub fn min_greater(&self, key: &K) -> Option<(&K, &V)> {
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
                        max_min_rank = rank;
                    }
                }
            }
        }
        if min_max_rank <= ARRAY_SIZE {
            return Some(self.read(min_max_index));
        }
        None
    }

    /// Returns the maximum entry among those that are Ordering::Less than the given key.
    pub fn max_less(&self, key: &K) -> Option<(&K, &V)> {
        let metadata = self.metadata.load(Acquire);
        let mut max_min_rank = 0;
        let mut min_max_rank = ARRAY_SIZE + 1;
        let mut max_min_index = ARRAY_SIZE;
        for i in 0..ARRAY_SIZE {
            let rank = ((metadata & (INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE)))
                >> (i * INDEX_RANK_ENTRY_SIZE)) as usize;
            if rank > max_min_rank && rank < min_max_rank && (metadata & (OCCUPANCY_BIT << i)) != 0
            {
                match self.compare(i, key) {
                    Ordering::Less => {
                        if max_min_rank < rank {
                            max_min_rank = rank;
                            max_min_index = i;
                        }
                    }
                    Ordering::Greater => {
                        if min_max_rank > rank {
                            min_max_rank = rank;
                        }
                    }
                    Ordering::Equal => {
                        min_max_rank = rank;
                    }
                }
            }
        }
        if max_min_rank > 0 {
            return Some(self.read(max_min_index));
        }
        None
    }

    /// Compares the given metadata value with the current one.
    pub fn validate(&self, metadata: u64) -> bool {
        // The acquire fence ensures that a reader having read the latest state must read the updated metadata.
        fence(Acquire);
        self.metadata.load(Relaxed) == metadata
    }

    fn write(&self, index: usize, key: K, value: V) {
        unsafe {
            self.entry_array_mut_ref()[index]
                .as_mut_ptr()
                .write((key, value))
        };
    }

    pub fn next(&self, metadata: u64, index: usize) -> (usize, *const (K, V)) {
        if index != usize::MAX {
            let current_entry_rank = if index < ARRAY_SIZE {
                ((metadata & (INDEX_RANK_ENTRY_MASK << (index * INDEX_RANK_ENTRY_SIZE)))
                    >> (index * INDEX_RANK_ENTRY_SIZE)) as usize
            } else {
                0
            };
            if current_entry_rank < ARRAY_SIZE {
                let mut next_rank = ARRAY_SIZE + 1;
                let mut next_index = ARRAY_SIZE;
                for i in 0..ARRAY_SIZE {
                    if metadata & (OCCUPANCY_BIT << i) == 0 {
                        continue;
                    }
                    let rank = ((metadata & (INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE)))
                        >> (i * INDEX_RANK_ENTRY_SIZE)) as usize;
                    if current_entry_rank < rank && rank < next_rank {
                        next_rank = rank;
                        next_index = i;
                    }
                }
                if next_rank <= ARRAY_SIZE {
                    return (next_index, unsafe {
                        &*self.entry_array[next_index].as_ptr()
                    });
                }
            }
        }
        (usize::MAX, std::ptr::null())
    }

    pub fn distribute(
        &self,
        low_key_leaf: &mut Option<Box<Leaf<K, V>>>,
        high_key_leaf: &mut Option<Box<Leaf<K, V>>>,
    ) {
        let mut iterated = 0;
        for entry in LeafScanner::new(self) {
            if iterated < ARRAY_SIZE / 2 {
                if low_key_leaf.is_none() {
                    low_key_leaf.replace(Box::new(Leaf::new()));
                }
                low_key_leaf
                    .as_ref()
                    .unwrap()
                    .insert(entry.0.clone(), entry.1.clone(), false);
                iterated += 1;
            } else {
                if high_key_leaf.is_none() {
                    high_key_leaf.replace(Box::new(Leaf::new()));
                }
                high_key_leaf
                    .as_ref()
                    .unwrap()
                    .insert(entry.0.clone(), entry.1.clone(), false);
            }
        }
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

impl<K: Clone + Ord + Sync, V: Clone + Sync> Drop for Leaf<K, V> {
    fn drop(&mut self) {
        let metadata = self.metadata.swap(0, Acquire);
        for i in 0..ARRAY_SIZE {
            let rank = (metadata & (INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE)))
                >> (i * INDEX_RANK_ENTRY_SIZE);
            if rank != 0 && rank != INVALID_RANK {
                self.take(i);
            }
        }
    }
}

struct Inserter<'a, K: Clone + Ord + Sync, V: Clone + Sync> {
    leaf: &'a Leaf<K, V>,
    committed: bool,
    metadata: u64,
    index: usize,
}

impl<'a, K: Clone + Ord + Sync, V: Clone + Sync> Inserter<'a, K, V> {
    /// Returns Some if OCCUPIED && RANK == 0.
    fn new(leaf: &'a Leaf<K, V>) -> Option<Inserter<'a, K, V>> {
        let mut current = leaf.metadata.load(Relaxed);
        loop {
            let mut full = true;
            let mut position = ARRAY_SIZE;
            for i in 0..ARRAY_SIZE {
                let rank = current & (INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE));
                if rank == INVALID_RANK {
                    // The entire leaf has been invalidated.
                    debug_assert_eq!((current & OCCUPANCY_MASK).count_ones(), 0);
                    return None;
                }
                if rank == 0 {
                    full = false;
                    if current & (OCCUPANCY_BIT << i) == 0 {
                        // Initial state.
                        position = i;
                        break;
                    }
                }
            }

            if full {
                return None;
            } else if position == ARRAY_SIZE {
                // Another thread is inserting data into the last remaining slot.
                current = leaf.metadata.load(Relaxed);
                continue;
            }

            // Found an empty position.
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
                    });
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
                // Rolls back metadata changes if not committed.
                return false;
            }
            break;
        }
        self.committed = true;

        // Every store after commit must be visible along with the metadata update.
        fence(Release);
        true
    }
}

impl<'a, K: Clone + Ord + Sync, V: Clone + Sync> Drop for Inserter<'a, K, V> {
    fn drop(&mut self) {
        if !self.committed {
            // Rolls back metadata changes if not committed.
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
pub struct LeafScanner<'a, K: Clone + Ord + Sync, V: Clone + Sync> {
    leaf: &'a Leaf<K, V>,
    metadata: u64,
    removed_entries_to_scan: u64,
    entry_index: usize,
    entry_ptr: *const (K, V),
}

impl<'a, K: Clone + Ord + Sync, V: Clone + Sync> LeafScanner<'a, K, V> {
    pub fn new(leaf: &'a Leaf<K, V>) -> LeafScanner<'a, K, V> {
        LeafScanner {
            leaf,
            metadata: leaf.metadata.load(Acquire),
            removed_entries_to_scan: 0,
            entry_index: ARRAY_SIZE,
            entry_ptr: std::ptr::null(),
        }
    }

    pub fn new_including_removed(leaf: &'a Leaf<K, V>) -> LeafScanner<'a, K, V> {
        let metadata = leaf.metadata.load(Acquire);
        let mut removed_entries_to_scan = 0;
        for i in 0..ARRAY_SIZE {
            let rank = ((metadata & (INDEX_RANK_ENTRY_MASK << (i * INDEX_RANK_ENTRY_SIZE)))
                >> (i * INDEX_RANK_ENTRY_SIZE)) as usize;
            if rank != INVALID_RANK as usize && rank > 0 && metadata & (OCCUPANCY_BIT << i) == 0 {
                removed_entries_to_scan |= OCCUPANCY_BIT << i;
            }
        }
        LeafScanner {
            leaf,
            metadata: metadata,
            removed_entries_to_scan,
            entry_index: ARRAY_SIZE,
            entry_ptr: std::ptr::null(),
        }
    }

    pub fn metadata(&self) -> u64 {
        self.metadata
    }

    pub fn from(leaf: &'a Leaf<K, V>, key: &K, exact: bool) -> Option<LeafScanner<'a, K, V>> {
        let metadata = leaf.metadata.load(Acquire);
        let (index, ptr) = leaf.from(metadata, key, exact);
        if !ptr.is_null() {
            Some(LeafScanner {
                leaf,
                metadata,
                removed_entries_to_scan: 0,
                entry_index: index,
                entry_ptr: ptr,
            })
        } else {
            None
        }
    }

    /// Returns a reference to the entry that the scanner is currently pointing to
    pub fn get(&self) -> Option<(&'a K, &'a V)> {
        if self.entry_ptr.is_null() {
            return None;
        }
        unsafe { Some((&(*self.entry_ptr).0, &(*self.entry_ptr).1)) }
    }

    pub fn removed(&self) -> bool {
        self.removed_entries_to_scan & (OCCUPANCY_BIT << self.entry_index) != 0
    }

    fn proceed(&mut self) {
        self.entry_ptr = std::ptr::null();
        if self.entry_index == usize::MAX {
            return;
        }
        let (index, ptr) = self.leaf.next(
            self.metadata | self.removed_entries_to_scan,
            self.entry_index,
        );
        self.entry_index = index;
        self.entry_ptr = ptr;
    }
}

impl<'a, K: Clone + Ord + Sync, V: Clone + Sync> Iterator for LeafScanner<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        self.proceed();
        self.get()
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
        assert!(leaf.insert(50, 51, false).is_none());
        assert_eq!(leaf.max(), Some((&50, &51)));
        assert!(leaf.insert(60, 60, false).is_none());
        assert!(leaf.insert(70, 71, false).is_none());
        assert!(leaf.insert(60, 61, true).is_none());
        assert_eq!(leaf.remove(&60, false), (true, false, false));
        assert!(!leaf.full());
        assert!(leaf.insert(40, 40, false).is_none());
        assert!(leaf.insert(30, 31, false).is_none());
        assert!(!leaf.full());
        assert!(leaf.insert(40, 41, true).is_none());
        assert_eq!(leaf.insert(30, 33, false), Some(((30, 33), true)));
        assert!(leaf.insert(10, 11, false).is_none());
        assert!(leaf.insert(11, 12, false).is_none());
        assert!(leaf.insert(13, 13, false).is_none());
        assert!(leaf.insert(54, 55, false).is_none());
        assert!(leaf.insert(13, 14, true).is_none());
        assert_eq!(leaf.max(), Some((&70, &71)));
        assert!(leaf.full());

        let mut scanner = LeafScanner::new(&leaf);
        let mut prev_key = 0;
        while let Some(entry) = scanner.next() {
            assert_eq!(scanner.get(), Some(entry));
            assert!(prev_key < *entry.0);
            assert_eq!(*entry.0 + 1, *entry.1);
            prev_key = *entry.0;
        }
        drop(scanner);

        let mut scanner = LeafScanner::new_including_removed(&leaf);
        let mut prev_key = 0;
        let mut found_13_13 = false;
        let mut found_40_40 = false;
        let mut found_60_60 = false;
        let mut found_60_61 = false;
        while let Some(entry) = scanner.next() {
            assert_eq!(scanner.get(), Some(entry));
            if *entry.0 == 60 {
                assert!(prev_key <= *entry.0);
                assert!(scanner.removed());
                if *entry.1 == 60 {
                    found_60_60 = true;
                } else if *entry.1 == 61 {
                    found_60_61 = true;
                }
            } else if *entry.0 == 13 && *entry.1 == 13 {
                assert!(prev_key <= *entry.0);
                assert!(scanner.removed());
                found_13_13 = true;
            } else if *entry.0 == 40 && *entry.1 == 40 {
                assert!(prev_key <= *entry.0);
                assert!(scanner.removed());
                found_40_40 = true;
            } else {
                assert!(prev_key < *entry.0);
                assert_eq!(*entry.0 + 1, *entry.1);
                assert!(!scanner.removed());
                prev_key = *entry.0;
            }
        }
        assert!(found_40_40);
        assert!(found_13_13);
        assert!(found_60_60);
        assert!(found_60_61);
        drop(scanner);

        let mut scanner = LeafScanner::from(&leaf, &50, true).unwrap();
        assert_eq!(scanner.get(), Some((&50, &51)));
        let mut prev_key = 50;
        let mut iterated = 0;
        while let Some(entry) = scanner.next() {
            assert_eq!(scanner.get(), Some(entry));
            assert!(prev_key < *entry.0);
            assert_eq!(*entry.0 + 1, *entry.1);
            prev_key = *entry.0;
            iterated += 1;
        }
        assert_eq!(iterated, 2);
        drop(scanner);

        let mut scanner = LeafScanner::from(&leaf, &49, false).unwrap();
        assert_eq!(scanner.get(), Some((&50, &51)));
        let mut prev_key = 50;
        let mut iterated = 0;
        while let Some(entry) = scanner.next() {
            assert_eq!(scanner.get(), Some(entry));
            assert!(prev_key < *entry.0);
            assert_eq!(*entry.0 + 1, *entry.1);
            prev_key = *entry.0;
            iterated += 1;
        }
        assert_eq!(iterated, 2);
        drop(scanner);

        let leaf = Leaf::new();
        assert!(leaf.insert(20, 21, false).is_none());
        assert!(leaf.insert(10, 11, false).is_none());
        assert_eq!(*leaf.search(&10).unwrap(), 11);
        assert!(leaf.insert(11, 12, false).is_none());
        assert!(leaf.max_less(&10).is_none());
        assert_eq!(leaf.max_less(&11), Some((&10, &11)));
        assert_eq!(leaf.max_less(&12), Some((&11, &12)));
        assert_eq!(leaf.max_less(&100), Some((&20, &21)));
        assert_eq!(leaf.max(), Some((&20, &21)));
        assert_eq!(leaf.insert(11, 12, false), Some(((11, 12), true)));
        assert_eq!(*leaf.search(&11).unwrap(), 12);
        assert!(leaf.insert(12, 13, false).is_none());
        assert_eq!(*leaf.search(&12).unwrap(), 13);
        assert_eq!(leaf.min_greater_equal(&21).0, None);
        assert_eq!(leaf.min_greater_equal(&20).0, Some((&20, &21)));
        assert_eq!(leaf.min_greater_equal(&19).0, Some((&20, &21)));
        assert_eq!(leaf.min_greater_equal(&0).0, Some((&10, &11)));
        assert!(leaf.insert(2, 3, false).is_none());
        assert_eq!(*leaf.search(&2).unwrap(), 3);
        assert_eq!(leaf.insert(2, 3, false), Some(((2, 3), true)));
        assert_eq!(leaf.min_greater_equal(&8).0, Some((&10, &11)));
        assert_eq!(leaf.max_less(&11), Some((&10, &11)));
        assert_eq!(*leaf.search(&2).unwrap(), 3);
        assert!(leaf.insert(1, 2, false).is_none());
        assert_eq!(*leaf.search(&1).unwrap(), 2);
        assert!(leaf.insert(13, 14, false).is_none());
        assert_eq!(*leaf.search(&13).unwrap(), 14);
        assert_eq!(leaf.insert(13, 14, false), Some(((13, 14), true)));
        assert_eq!(leaf.remove(&10, false), (true, false, false));
        assert_eq!(leaf.remove(&11, false), (true, false, false));
        assert_eq!(leaf.remove(&12, false), (true, false, false));
        assert!(!leaf.full());
        assert!(leaf.insert(20, 21, true).is_none());
        assert!(leaf.insert(12, 13, false).is_none());
        assert!(leaf.insert(14, 15, false).is_none());
        assert!(leaf.search(&11).is_none());
        assert_eq!(leaf.remove(&10, false), (false, false, false));
        assert_eq!(leaf.remove(&11, false), (false, false, false));
        assert_eq!(*leaf.search(&20).unwrap(), 21);
        assert!(leaf.insert(10, 11, false).is_none());
        assert!(leaf.insert(15, 16, false).is_none());
        assert_eq!(leaf.max(), Some((&20, &21)));
        assert!(leaf.full());

        let mut scanner = LeafScanner::new(&leaf);
        let mut prev_key = 0;
        while let Some(entry) = scanner.next() {
            assert_eq!(scanner.get(), Some(entry));
            assert!(prev_key < *entry.0);
            assert_eq!(*entry.0 + 1, *entry.1);
            prev_key = *entry.0;
        }
        drop(scanner);

        let mut leaves_boxed = (None, None);
        leaf.distribute(&mut leaves_boxed.0, &mut leaves_boxed.1);
        let mut prev_key = 0;
        let mut iterated_low = 0;
        let mut scanner_low = LeafScanner::new(leaves_boxed.0.as_ref().unwrap());
        while let Some(entry) = scanner_low.next() {
            assert_eq!(scanner_low.get(), Some(entry));
            assert!(prev_key < *entry.0);
            assert_eq!(*entry.0 + 1, *entry.1);
            prev_key = *entry.0;
            iterated_low += 1;
        }
        assert_eq!(iterated_low, 6);
        drop(scanner_low);
        let mut iterated_high = 0;
        let mut scanner_high = LeafScanner::new(leaves_boxed.1.as_ref().unwrap());
        while let Some(entry) = scanner_high.next() {
            assert_eq!(scanner_high.get(), Some(entry));
            assert!(prev_key < *entry.0);
            assert_eq!(*entry.0 + 1, *entry.1);
            prev_key = *entry.0;
            iterated_high += 1;
        }
        assert_eq!(iterated_high, 2);
        drop(scanner_high);
    }

    #[test]
    fn complex() {
        let leaf = Leaf::new();
        for i in 0..ARRAY_SIZE / 2 {
            assert!(leaf.insert(i, i, false).is_none());
        }
        for i in 0..ARRAY_SIZE / 2 {
            if i < ARRAY_SIZE / 2 - 1 {
                assert_eq!(leaf.remove(&i, true), (true, false, false));
            } else {
                assert_eq!(leaf.remove(&i, true), (true, true, true));
            }
        }
        assert!(leaf.full());
        assert!(leaf.obsolete());
        assert_eq!(
            leaf.insert(ARRAY_SIZE, ARRAY_SIZE, false),
            Some(((ARRAY_SIZE, ARRAY_SIZE), false))
        );

        let mut scanner = LeafScanner::new_including_removed(&leaf);
        let mut expected = 0;
        while let Some(entry) = scanner.next() {
            assert_eq!(entry.0, &expected);
            assert_eq!(entry.1, &expected);
            assert!(scanner.removed());
            expected += 1;
        }
        assert_eq!(expected, ARRAY_SIZE / 2);

        let leaf = Leaf::new();
        for key in 0..ARRAY_SIZE {
            assert!(leaf.insert(key, key, false).is_none());
        }
        for key in 0..ARRAY_SIZE {
            assert_eq!(
                leaf.remove(&key, false),
                (true, true, key == ARRAY_SIZE - 1)
            );
        }
    }

    #[test]
    fn update() {
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
                            assert!(leaf_copied.remove(&tid, false).0);
                        }
                    }
                    let mut scanner = LeafScanner::new(&leaf_copied);
                    let mut prev_key = 0;
                    while let Some(entry) = scanner.next() {
                        assert_eq!(scanner.get(), Some(entry));
                        assert_eq!(entry.1, &1);
                        assert!((prev_key == 0 && *entry.0 == 0) || prev_key < *entry.0);
                        prev_key = *entry.0;
                    }
                }));
            }
            for handle in thread_handles {
                handle.join().unwrap();
            }
            let mut scanner = LeafScanner::new(&leaf);
            let mut prev_key = 0;
            while let Some(entry) = scanner.next() {
                assert_eq!(entry.1, &1);
                assert!((prev_key == 0 && *entry.0 == 0) || prev_key < *entry.0);
                assert!(entry.0 % 2 == 0);
                prev_key = *entry.0;
            }
        }
    }
}
