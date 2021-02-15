use crossbeam_epoch::{Atomic, Guard, Shared};
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
pub struct Leaf<K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    /// The array of key-value pairs.
    entry_array: EntryArray<K, V>,
    /// The metadata that manages the contents.
    metadata: AtomicU64,
    /// A pointer that points to the next adjacent leaf.
    forward_link: Atomic<Leaf<K, V>>,
    /// A pointer that points to the previous adjacent leaf.
    ///
    /// backward_link pointing the leaf itself means the leaf is locked.
    backward_link: Atomic<Leaf<K, V>>,
}

impl<K, V> Leaf<K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    /// Creates a new Leaf.
    pub fn new() -> Leaf<K, V> {
        Leaf {
            entry_array: unsafe { MaybeUninit::uninit().assume_init() },
            metadata: AtomicU64::new(0),
            forward_link: Atomic::null(),
            backward_link: Atomic::null(),
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

    pub fn forward_link<'a>(&self, guard: &'a Guard) -> Shared<'a, Leaf<K, V>> {
        self.forward_link.load(Acquire, guard)
    }

    pub fn backward_link<'a>(&self, guard: &'a Guard) -> Shared<'a, Leaf<K, V>> {
        self.backward_link.load(Acquire, guard)
    }

    /// Attaches the given leaf to its forward link.
    pub fn push_back(&self, leaf: &Leaf<K, V>, guard: &Guard) {
        // Locks the next leaf and self.
        let mut lockers = LeafListLocker::lock_next_self(self, guard);

        // Makes the new leaf point to self and the next leaf.
        //  - The update order is, store(leaf.back)|release|store(leaf.forward)
        leaf.backward_link
            .store(Shared::from(self as *const _), Relaxed);
        leaf.forward_link
            .store(self.forward_link.load(Relaxed, guard), Release);

        // Makes the next leaf point to the new leaf.
        //  - From here, the new leaf is reachable by LeafListLockers.
        if let Some(next_leaf_locker) = lockers.0.as_mut() {
            next_leaf_locker.update_backward_link(Shared::from(leaf as *const _));
        }

        // Makes self point to the new leaf.
        //  - From here, the new leaf is reachable by Scanners.
        self.forward_link
            .store(Shared::from(leaf as *const _), Release);
    }

    /// Unlinks itself from the linked list.
    pub fn unlink(&self, guard: &Guard) {
        // Locks the next leaf and self.
        let mut lockers = LeafListLocker::lock_next_self(self, guard);

        // Locks the previous leaf and modifies the linked list.
        let prev_leaf = lockers.1.backward_link;
        if !prev_leaf.is_null() {
            // Makes the prev leaf point to the next leaf.
            let prev_leaf_locker = LeafListLocker::lock(unsafe { prev_leaf.deref() }, guard);
            debug_assert_eq!(
                prev_leaf_locker.leaf.forward_link.load(Relaxed, guard),
                Shared::from(self as *const _)
            );
            prev_leaf_locker
                .leaf
                .forward_link
                .store(self.forward_link.load(Relaxed, guard), Release);
            if let Some(next_leaf_locker) = lockers.0.as_mut() {
                next_leaf_locker.update_backward_link(prev_leaf);
            }
        } else if let Some(next_leaf_locker) = lockers.0.as_mut() {
            next_leaf_locker.update_backward_link(prev_leaf);
        }
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

    /// Inserts a key value pair.
    ///
    /// It returns the passed key value pair on failure.
    /// The second returned value being true indicates that the same key exists.
    pub fn insert(&self, key: K, value: V) -> Option<((K, V), bool)> {
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
                            // Uniqueness check failed.
                            return Some((entry, true));
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

            if inserter.commit(updated_rank_map) {
                return None;
            }
            entry = self.take(inserter.index);
        }

        // Full.
        debug_assert!(self.full());
        let duplicate_key = self.search(&entry.0).is_some();
        Some((entry, duplicate_key))
    }

    /// Removes the key.
    ///
    /// If the number of removed entries exceeds the given threshold, it shrinks the leaf.
    /// It returns (removed, cardinality, vacant slots).
    pub fn remove(&self, key: &K, threshold: usize) -> (bool, usize, usize) {
        let mut removed = false;
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
                    Ordering::Equal => {
                        loop {
                            let mut new_metadata =
                                (metadata & (!(OCCUPANCY_BIT << i))) + REMOVED_BIT;
                            let num_removed = ((new_metadata & REMOVED) / REMOVED_BIT) as usize;
                            if num_removed >= threshold {
                                // Deprecates itself by marking all the vacant slots invalid.
                                for j in 0..ARRAY_SIZE {
                                    if (new_metadata
                                        & (INDEX_RANK_ENTRY_MASK << (j * INDEX_RANK_ENTRY_SIZE)))
                                        == 0
                                    {
                                        if (new_metadata & (OCCUPANCY_BIT << j)) != 0 {
                                            // Competes with the thread inserting an entry.
                                            new_metadata &= !(OCCUPANCY_BIT << j);
                                        }
                                        new_metadata |= INVALID_RANK << (j * INDEX_RANK_ENTRY_SIZE);
                                        new_metadata += REMOVED_BIT;
                                    }
                                }
                            }
                            match self.metadata.compare_exchange(
                                metadata,
                                new_metadata,
                                Release,
                                Relaxed,
                            ) {
                                Ok(_) => {
                                    removed = true;
                                    metadata = new_metadata;
                                    break;
                                }
                                Err(result) => {
                                    metadata = result;
                                    if metadata & (OCCUPANCY_BIT << i) == 0 {
                                        // Removed by another thread.
                                        break;
                                    }
                                }
                            }
                        }
                        break;
                    }
                }
            }
        }

        let cardinality = (metadata & OCCUPANCY_MASK).count_ones() as usize;
        let num_removed = ((metadata & REMOVED) / REMOVED_BIT) as usize;
        (removed, cardinality, ARRAY_SIZE - cardinality - num_removed)
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

    /// Returns the index and a pointer to the key-value pair that is smaller than the given key.
    pub fn max_less(&self, metadata: u64, key: &K) -> (usize, *const (K, V)) {
        let mut max_min_rank = 0;
        let mut max_min_index = ARRAY_SIZE;
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
        if max_min_index < ARRAY_SIZE {
            return (max_min_index, unsafe {
                &*self.entry_array[max_min_index].as_ptr()
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
                    .insert(entry.0.clone(), entry.1.clone());
                iterated += 1;
            } else {
                if high_key_leaf.is_none() {
                    high_key_leaf.replace(Box::new(Leaf::new()));
                }
                high_key_leaf
                    .as_ref()
                    .unwrap()
                    .insert(entry.0.clone(), entry.1.clone());
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

impl<K, V> Drop for Leaf<K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
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

struct Inserter<'a, K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    leaf: &'a Leaf<K, V>,
    committed: bool,
    metadata: u64,
    index: usize,
}

impl<'a, K, V> Inserter<'a, K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
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

    fn commit(&mut self, updated_rank_map: u64) -> bool {
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

impl<'a, K, V> Drop for Inserter<'a, K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
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

/// Leaf list locker.
struct LeafListLocker<'a, K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    leaf: &'a Leaf<K, V>,
    backward_link: Shared<'a, Leaf<K, V>>,
}

impl<'a, K, V> LeafListLocker<'a, K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    fn lock(leaf: &'a Leaf<K, V>, guard: &'a Guard) -> LeafListLocker<'a, K, V> {
        let mut current = leaf.backward_link.load(Relaxed, guard);
        let locked_state = Shared::from(leaf as *const _);
        loop {
            if current == locked_state {
                current = leaf.backward_link.load(Relaxed, guard);
                continue;
            }
            if let Err(err) =
                leaf.backward_link
                    .compare_and_set(current, locked_state, Acquire, guard)
            {
                current = err.current;
                continue;
            }
            break;
        }
        LeafListLocker {
            leaf,
            backward_link: current,
        }
    }

    fn lock_next_self(
        leaf: &'a Leaf<K, V>,
        guard: &'a Guard,
    ) -> (Option<LeafListLocker<'a, K, V>>, LeafListLocker<'a, K, V>) {
        loop {
            // Locks the next leaf.
            let next_leaf_ptr = leaf.forward_link.load(Relaxed, guard);
            let mut next_leaf_locker = None;
            if !next_leaf_ptr.is_null() {
                next_leaf_locker.replace(LeafListLocker::lock(
                    unsafe { next_leaf_ptr.deref() },
                    guard,
                ));
            }

            if next_leaf_locker.as_ref().map_or_else(
                || false,
                |locker| locker.backward_link.as_raw() != leaf as *const _,
            ) {
                // The link has changed in the meantime.
                continue;
            }

            // Lock the leaf.
            //  - Reading backward_link needs to be an Acquire fence to correctly read forward_link.
            let locked_state = Shared::from(leaf as *const _);
            let mut current_state = leaf.backward_link.load(Acquire, guard);
            loop {
                if current_state == locked_state {
                    // Currently, locked.
                    current_state = leaf.backward_link.load(Acquire, guard);
                    continue;
                }
                if next_leaf_ptr != leaf.forward_link.load(Relaxed, guard) {
                    // Pointer changed with the known next leaf is locked.
                    break;
                }
                if let Err(err) =
                    leaf.backward_link
                        .compare_and_set(current_state, locked_state, Acquire, guard)
                {
                    current_state = err.current;
                    continue;
                }
                debug_assert_eq!(
                    next_leaf_locker
                        .as_ref()
                        .map_or_else(|| leaf as *const _, |locker| locker.backward_link.as_raw()),
                    leaf as *const _
                );

                return (
                    next_leaf_locker,
                    LeafListLocker {
                        leaf,
                        backward_link: current_state,
                    },
                );
            }
        }
    }

    fn update_backward_link(&mut self, new_ptr: Shared<'a, Leaf<K, V>>) {
        self.backward_link = new_ptr;
    }
}

impl<'a, K, V> Drop for LeafListLocker<'a, K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    fn drop(&mut self) {
        self.leaf.backward_link.store(self.backward_link, Release);
    }
}

/// Leaf scanner.
pub struct LeafScanner<'a, K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    leaf: &'a Leaf<K, V>,
    metadata: u64,
    removed_entries_to_scan: u64,
    entry_index: usize,
    entry_ptr: *const (K, V),
}

impl<'a, K, V> LeafScanner<'a, K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    pub fn new(leaf: &'a Leaf<K, V>) -> LeafScanner<'a, K, V> {
        LeafScanner {
            leaf,
            metadata: leaf.metadata.load(Acquire),
            removed_entries_to_scan: 0,
            entry_index: ARRAY_SIZE,
            entry_ptr: std::ptr::null(),
        }
    }

    pub fn max_less(leaf: &'a Leaf<K, V>, key: &K) -> LeafScanner<'a, K, V> {
        let metadata = leaf.metadata.load(Acquire);
        let (index, ptr) = leaf.max_less(metadata, key);
        if !ptr.is_null() {
            LeafScanner {
                leaf,
                metadata,
                removed_entries_to_scan: 0,
                entry_index: index,
                entry_ptr: ptr,
            }
        } else {
            LeafScanner::new(leaf)
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
            metadata,
            removed_entries_to_scan,
            entry_index: ARRAY_SIZE,
            entry_ptr: std::ptr::null(),
        }
    }

    pub fn metadata(&self) -> u64 {
        self.metadata
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

    pub fn jump(&self, guard: &'a Guard) -> Option<LeafScanner<'a, K, V>> {
        let next = self.leaf.forward_link.load(Relaxed, guard);
        if !next.is_null() {
            return Some(LeafScanner::new(unsafe { next.deref() }));
        }
        None
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

impl<'a, K, V> Iterator for LeafScanner<'a, K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
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
        assert!(leaf.insert(50, 51).is_none());
        assert_eq!(leaf.max(), Some((&50, &51)));
        assert!(leaf.insert(60, 61).is_none());
        assert!(leaf.insert(70, 71).is_none());
        assert!(leaf.remove(&60, ARRAY_SIZE).0);
        assert!(leaf.insert(60, 61).is_none());
        assert_eq!(leaf.remove(&60, ARRAY_SIZE), (true, 2, 8));
        assert!(!leaf.full());
        assert!(leaf.insert(40, 40).is_none());
        assert!(leaf.insert(30, 31).is_none());
        assert!(!leaf.full());
        assert!(leaf.remove(&40, ARRAY_SIZE).0);
        assert!(leaf.insert(40, 41).is_none());
        assert_eq!(leaf.insert(30, 33), Some(((30, 33), true)));
        assert!(leaf.insert(10, 11).is_none());
        assert!(leaf.insert(11, 12).is_none());
        assert!(leaf.insert(13, 13).is_none());
        assert!(leaf.insert(54, 55).is_none());
        assert!(leaf.remove(&13, ARRAY_SIZE).0);
        assert!(leaf.insert(13, 14).is_none());
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
        let mut found_60_61 = false;
        while let Some(entry) = scanner.next() {
            assert_eq!(scanner.get(), Some(entry));
            if *entry.0 == 60 {
                assert!(prev_key <= *entry.0);
                assert!(scanner.removed());
                if *entry.1 == 61 {
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
        assert!(found_60_61);
        drop(scanner);

        let mut scanner = LeafScanner::max_less(&leaf, &51);
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
        assert!(leaf.insert(20, 21).is_none());
        assert!(leaf.insert(10, 11).is_none());
        assert_eq!(*leaf.search(&10).unwrap(), 11);
        assert!(leaf.insert(11, 12).is_none());
        assert_eq!(leaf.max(), Some((&20, &21)));
        assert_eq!(leaf.insert(11, 12), Some(((11, 12), true)));
        assert_eq!(*leaf.search(&11).unwrap(), 12);
        assert!(leaf.insert(12, 13).is_none());
        assert_eq!(*leaf.search(&12).unwrap(), 13);
        assert_eq!(leaf.min_greater_equal(&21).0, None);
        assert_eq!(leaf.min_greater_equal(&20).0, Some((&20, &21)));
        assert_eq!(leaf.min_greater_equal(&19).0, Some((&20, &21)));
        assert_eq!(leaf.min_greater_equal(&0).0, Some((&10, &11)));
        assert!(leaf.insert(2, 3).is_none());
        assert_eq!(*leaf.search(&2).unwrap(), 3);
        assert_eq!(leaf.insert(2, 3), Some(((2, 3), true)));
        assert_eq!(leaf.min_greater_equal(&8).0, Some((&10, &11)));
        assert_eq!(*leaf.search(&2).unwrap(), 3);
        assert!(leaf.insert(1, 2).is_none());
        assert_eq!(*leaf.search(&1).unwrap(), 2);
        assert!(leaf.insert(13, 14).is_none());
        assert_eq!(*leaf.search(&13).unwrap(), 14);
        assert_eq!(leaf.insert(13, 14), Some(((13, 14), true)));
        assert_eq!(leaf.remove(&10, ARRAY_SIZE), (true, 6, 5));
        assert_eq!(leaf.remove(&11, ARRAY_SIZE), (true, 5, 5));
        assert_eq!(leaf.remove(&12, ARRAY_SIZE), (true, 4, 5));
        assert!(!leaf.full());
        assert!(leaf.remove(&20, ARRAY_SIZE).0);
        assert!(leaf.insert(20, 21).is_none());
        assert!(leaf.insert(12, 13).is_none());
        assert!(leaf.insert(14, 15).is_none());
        assert!(leaf.search(&11).is_none());
        assert_eq!(leaf.remove(&10, ARRAY_SIZE), (false, 6, 2));
        assert_eq!(leaf.remove(&11, ARRAY_SIZE), (false, 6, 2));
        assert_eq!(*leaf.search(&20).unwrap(), 21);
        assert!(leaf.insert(10, 11).is_none());
        assert!(leaf.insert(15, 16).is_none());
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
            assert!(leaf.insert(i, i).is_none());
        }
        for i in 0..ARRAY_SIZE / 2 {
            if i < ARRAY_SIZE / 4 - 1 {
                assert_eq!(
                    leaf.remove(&i, ARRAY_SIZE / 4),
                    (true, ARRAY_SIZE / 2 - i - 1, 6)
                );
            } else {
                assert_eq!(
                    leaf.remove(&i, ARRAY_SIZE / 4),
                    (true, ARRAY_SIZE / 2 - i - 1, 0)
                );
            }
        }
        assert!(leaf.full());
        assert!(leaf.obsolete());
        assert_eq!(
            leaf.insert(ARRAY_SIZE, ARRAY_SIZE),
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
            assert!(leaf.insert(key, key).is_none());
        }
        for key in 0..ARRAY_SIZE {
            assert_eq!(
                leaf.remove(&key, ARRAY_SIZE),
                (true, ARRAY_SIZE - key - 1, 0)
            );
        }
    }

    #[test]
    fn update() {
        let num_threads = (ARRAY_SIZE + 1) as usize;
        for _ in 0..256 {
            let barrier = Arc::new(Barrier::new(num_threads));
            let leaf = Arc::new(Leaf::new());
            leaf.insert(num_threads * 2, 1);
            let mut thread_handles = Vec::with_capacity(num_threads);
            for tid in 0..num_threads {
                let barrier_copied = barrier.clone();
                let leaf_copied = leaf.clone();
                thread_handles.push(thread::spawn(move || {
                    barrier_copied.wait();
                    assert_eq!(
                        leaf_copied.insert(num_threads * 2, num_threads),
                        Some(((num_threads * 2, num_threads), true))
                    );
                    let result = leaf_copied.insert(tid, 1);
                    if result.is_none() {
                        assert_eq!(*leaf_copied.search(&tid).unwrap(), 1);
                        if tid % 2 != 0 {
                            assert!(leaf_copied.remove(&tid, ARRAY_SIZE).0);
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
