use crate::common::linked_list::LinkedList;
use crossbeam_epoch::{Atomic, Guard};
use std::cmp::Ordering;
use std::convert::TryInto;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

pub const ARRAY_SIZE: usize = 8;

/// Entry state.
///  - 0: vacant.
///  - 1-ARRAY_SIZE: valid.
///  - 13: locked.
///  - 14: retired, final state.
///  - 15: removed, final state.
const LOCKED: u32 = 13;
const RETIRED: u32 = 14;
const REMOVED: u32 = 15;

/// All the slots are removed or retired.
const OBSOLETE_MASK: u32 = (RETIRED << 28)
    | (RETIRED << 24)
    | (RETIRED << 20)
    | (RETIRED << 16)
    | (RETIRED << 12)
    | (RETIRED << 8)
    | (RETIRED << 4)
    | RETIRED;

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
    /// A pointer that points to the next adjacent leaf.
    ///
    /// forward_link being tagged 1 means that the next leaf may contain smaller keys.
    forward_link: Atomic<Leaf<K, V>>,
    /// A pointer that points to the previous adjacent leaf.
    ///
    /// backward_link being tagged 1 means the leaf is locked.
    backward_link: Atomic<Leaf<K, V>>,
    /// The metadata that manages the contents.
    metadata: AtomicU32,
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
            forward_link: Atomic::null(),
            backward_link: Atomic::null(),
            metadata: AtomicU32::new(0),
        }
    }

    /// Takes the memory address of the instance as an identifier.
    pub fn id(&self) -> usize {
        self as *const _ as usize
    }

    /// Returns true if the leaf is full.
    pub fn full(&self) -> bool {
        let metadata = self.metadata.load(Relaxed);
        for i in 0..ARRAY_SIZE {
            if Self::rank(i, metadata) == 0 {
                return false;
            }
        }
        true
    }

    /// Returns true if the leaf is obsolete.
    pub fn obsolete(&self) -> bool {
        (self.metadata.load(Relaxed) & OBSOLETE_MASK) == OBSOLETE_MASK
    }

    /// Returns a reference to the max key.
    pub fn max(&self) -> Option<(&K, &V)> {
        let metadata = self.metadata.load(Acquire);
        let mut max_rank = 0;
        let mut max_index = ARRAY_SIZE;
        for i in 0..ARRAY_SIZE {
            let rank = Self::rank(i, metadata);
            if rank > max_rank && rank <= ARRAY_SIZE.try_into().unwrap() {
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
        if let Some(mut inserter) = Inserter::new(self) {
            // Inserts the key value.
            self.write(inserter.index, key, value);
            let entry_ref = self.read(inserter.index);

            // Calculates the rank and updates the metadata.
            loop {
                let mut max_min_rank = 0;
                let mut min_max_rank = (ARRAY_SIZE + 1).try_into().unwrap();
                let mut updated_rank_map = inserter.metadata;
                for i in 0..ARRAY_SIZE {
                    if i == inserter.index {
                        continue;
                    }
                    let rank = Self::rank(i, updated_rank_map);
                    if rank == 0 || rank < max_min_rank || rank > ARRAY_SIZE.try_into().unwrap() {
                        continue;
                    }
                    if rank > min_max_rank {
                        // Updates the rank.
                        let rank_bits = Self::rank_bits(i, rank + 1);
                        updated_rank_map = (updated_rank_map & (!Self::rank_mask(i))) | rank_bits;
                        continue;
                    }
                    match self.compare(i, &entry_ref.0) {
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
                            let rank_bits = Self::rank_bits(i, rank + 1);
                            updated_rank_map =
                                (updated_rank_map & (!Self::rank_mask(i))) | rank_bits;
                        }
                        Ordering::Equal => {
                            // Uniqueness check failed.
                            return Some((self.take(inserter.index), true));
                        }
                    }
                }
                let final_rank = max_min_rank + 1;
                debug_assert!(
                    min_max_rank == (ARRAY_SIZE + 1).try_into().unwrap()
                        || final_rank <= min_max_rank
                );

                // Updates its own rank.
                let rank_bits = Self::rank_bits(inserter.index, final_rank);
                updated_rank_map =
                    (updated_rank_map & (!Self::rank_mask(inserter.index))) | rank_bits;
                if inserter.commit(updated_rank_map) {
                    return None;
                }
            }
        } else {
            // Full.
            debug_assert!(self.full());
            let duplicate_key = self.search(&key).is_some();
            Some(((key, value), duplicate_key))
        }
    }

    /// Removes the key.
    ///
    /// The first boolean value returned from the function indicates that an entry has been removed.
    /// The second boolean value indicates that the leaf is full.
    /// The third boolean value indicates that the leaf is empty.
    pub fn remove(&self, key: &K) -> (bool, bool, bool) {
        let mut removed = false;
        let mut full = true;
        let mut empty = true;
        let mut metadata = self.metadata.load(Acquire);
        let mut max_min_rank = 0;
        let mut min_max_rank = (ARRAY_SIZE + 1).try_into().unwrap();
        for i in 0..ARRAY_SIZE {
            let rank = Self::rank(i, metadata);
            if rank > max_min_rank && rank < min_max_rank {
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
                            let new_metadata =
                                (metadata & (!Self::rank_mask(i))) | Self::rank_bits(i, REMOVED);
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
                                    if Self::rank(i, metadata) == REMOVED {
                                        // Removed by another thread.
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        for i in 0..ARRAY_SIZE {
            let rank = Self::rank(i, metadata);
            if full && rank == 0 {
                full = false;
            }
            if empty && rank != 0 && rank <= ARRAY_SIZE.try_into().unwrap() {
                empty = false;
            }
            if !full && !empty {
                break;
            }
        }

        (removed, full, empty)
    }

    /// Returns a value associated with the key.
    pub fn search(&self, key: &K) -> Option<&V> {
        let metadata = self.metadata.load(Acquire);
        let mut max_min_rank = 0;
        let mut min_max_rank = (ARRAY_SIZE + 1).try_into().unwrap();
        for i in 0..ARRAY_SIZE {
            let rank = Self::rank(i, metadata);
            if rank > max_min_rank && rank < min_max_rank {
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

    /// Retires all the vacant slots.
    ///
    /// Returns true if the leaf has become obsolete.
    pub fn retire(&self) -> bool {
        if let Some(self_locker) = InsertBlocker::new(self) {
            let metadata = self_locker.retire();
            (metadata & OBSOLETE_MASK) == OBSOLETE_MASK
        } else {
            false
        }
    }

    /// Returns the index and a pointer to the key-value pair that is smaller than the given key.
    pub fn max_less(&self, metadata: u32, key: &K) -> (usize, *const (K, V)) {
        let mut max_min_rank = 0;
        let mut max_min_index = ARRAY_SIZE;
        let mut min_max_rank = (ARRAY_SIZE + 1).try_into().unwrap();
        for i in 0..ARRAY_SIZE {
            let rank = Self::rank(i, metadata);
            if rank > max_min_rank && rank < min_max_rank {
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
    pub fn min_greater_equal(&self, key: &K) -> (Option<(&K, &V)>, u32) {
        let metadata = self.metadata.load(Acquire);
        let mut max_min_rank = 0;
        let mut min_max_rank = (ARRAY_SIZE + 1).try_into().unwrap();
        let mut min_max_index = ARRAY_SIZE;
        for i in 0..ARRAY_SIZE {
            let rank = Self::rank(i, metadata);
            if rank > max_min_rank && rank < min_max_rank {
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
        if min_max_rank <= ARRAY_SIZE.try_into().unwrap() {
            return (Some(self.read(min_max_index)), metadata);
        }
        (None, metadata)
    }

    /// Compares the given metadata value with the current one.
    pub fn validate(&self, metadata: u32) -> bool {
        // The acquire fence ensures that a reader having read the latest state must read the updated metadata.
        std::sync::atomic::fence(Acquire);
        self.metadata.load(Relaxed) == metadata
    }

    fn write(&self, index: usize, key: K, value: V) {
        unsafe {
            let entry_array_ptr = &self.entry_array as *const EntryArray<K, V>;
            let entry_array_mut_ptr = entry_array_ptr as *mut EntryArray<K, V>;
            let entry_array_mut_ref = &mut (*entry_array_mut_ptr);
            entry_array_mut_ref[index].as_mut_ptr().write((key, value))
        };
    }

    pub fn next(&self, index: usize, metadata: u32) -> (usize, *const (K, V)) {
        if index != usize::MAX {
            let current_entry_rank = if index < ARRAY_SIZE {
                Self::rank(index, metadata)
            } else {
                0
            };
            if current_entry_rank < ARRAY_SIZE.try_into().unwrap() {
                let mut next_rank = (ARRAY_SIZE + 1).try_into().unwrap();
                let mut next_index = ARRAY_SIZE;
                for i in 0..ARRAY_SIZE {
                    let rank = Self::rank(i, metadata);
                    if rank == 0 || rank == REMOVED {
                        continue;
                    }
                    if current_entry_rank < rank && rank < next_rank {
                        next_rank = rank;
                        next_index = i;
                    }
                }
                if next_rank <= ARRAY_SIZE.try_into().unwrap() {
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

    fn rank(index: usize, metadata: u32) -> u32 {
        (metadata & (REMOVED << (index * 4))) >> (index * 4)
    }

    fn rank_bits(index: usize, rank: u32) -> u32 {
        rank << (index * 4)
    }

    fn rank_mask(index: usize) -> u32 {
        REMOVED << (index * 4)
    }

    fn compare(&self, index: usize, key: &K) -> std::cmp::Ordering {
        let entry_ref = unsafe { &*self.entry_array[index].as_ptr() };
        entry_ref.0.cmp(key)
    }

    fn take(&self, index: usize) -> (K, V) {
        unsafe {
            let entry_array_ptr = &self.entry_array as *const EntryArray<K, V>;
            let entry_array_mut_ptr = entry_array_ptr as *mut EntryArray<K, V>;
            let entry_array_mut_ref = &mut (*entry_array_mut_ptr);
            let entry_ptr = &mut entry_array_mut_ref[index] as *mut MaybeUninit<(K, V)>;
            std::ptr::replace(entry_ptr, MaybeUninit::uninit()).assume_init()
        }
    }

    fn read(&self, index: usize) -> (&K, &V) {
        let entry_ref = unsafe { &*self.entry_array[index].as_ptr() };
        (&entry_ref.0, &entry_ref.1)
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
            let rank = Self::rank(i, metadata);
            if rank != 0 && rank != RETIRED {
                self.take(i);
            }
        }
    }
}

struct Inserter<'l, K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    leaf: &'l Leaf<K, V>,
    metadata: u32,
    index: usize,
    committed: bool,
}

/// Inserter locks a single vacant slot.
impl<'l, K, V> Inserter<'l, K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    /// Returns Some if there is a vacant slot.
    fn new(leaf: &'l Leaf<K, V>) -> Option<Inserter<'l, K, V>> {
        let mut current = leaf.metadata.load(Relaxed);
        loop {
            let mut full = true;
            let mut position = ARRAY_SIZE;
            for i in 0..ARRAY_SIZE {
                let rank = Leaf::<K, V>::rank(i, current);
                if rank == 0 {
                    full = false;
                    position = i;
                    break;
                } else if rank == LOCKED {
                    full = false;
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
            let new_metadata = current | Leaf::<K, V>::rank_bits(position, LOCKED);
            match leaf
                .metadata
                .compare_exchange(current, new_metadata, Acquire, Relaxed)
            {
                Ok(_) => {
                    return Some(Inserter {
                        leaf,
                        metadata: new_metadata,
                        index: position,
                        committed: false,
                    });
                }
                Err(result) => current = result,
            }
        }
    }

    fn commit(&mut self, updated_rank_map: u32) -> bool {
        let mut new_metadata = updated_rank_map;
        while let Err(result) =
            self.leaf
                .metadata
                .compare_exchange(self.metadata, new_metadata, Release, Relaxed)
        {
            for i in 0..ARRAY_SIZE {
                if i == self.index {
                    continue;
                }
                let current_rank = Leaf::<K, V>::rank(i, result);
                let expected_rank = Leaf::<K, V>::rank(i, self.metadata);
                if current_rank != expected_rank {
                    if current_rank != LOCKED {
                        // The rank map has been updated.
                        self.metadata = result;
                        return false;
                    } else {
                        // Never modifies the state.
                        new_metadata = (new_metadata & (!Leaf::<K, V>::rank_mask(i)))
                            | Leaf::<K, V>::rank_bits(i, LOCKED);
                    }
                }
            }
            self.metadata = result;
        }

        self.committed = true;
        true
    }
}

impl<'l, K, V> Drop for Inserter<'l, K, V>
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
                    current & (!Leaf::<K, V>::rank_bits(self.index, LOCKED)),
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

/// InsertBlocker locks all the vacant slots.
struct InsertBlocker<'l, K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    leaf: &'l Leaf<K, V>,
    metadata: u32,
    retired: bool,
}

impl<'l, K, V> InsertBlocker<'l, K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    /// Returns None if there is a locked slot.
    fn new(leaf: &'l Leaf<K, V>) -> Option<InsertBlocker<'l, K, V>> {
        let mut current = leaf.metadata.load(Relaxed);
        loop {
            let mut new_metadata = current;
            for i in 0..ARRAY_SIZE {
                let rank = Leaf::<K, V>::rank(i, current);
                if rank == 0 {
                    new_metadata |= Leaf::<K, V>::rank_bits(i, LOCKED);
                } else if rank == LOCKED {
                    return None;
                }
            }
            match leaf
                .metadata
                .compare_exchange(current, new_metadata, Acquire, Relaxed)
            {
                Ok(_) => {
                    return Some(InsertBlocker {
                        leaf,
                        metadata: new_metadata,
                        retired: false,
                    });
                }
                Err(result) => current = result,
            }
        }
    }

    fn retire(mut self) -> u32 {
        let mut current = self.leaf.metadata.load(Relaxed);
        loop {
            let mut new_metadata = self.metadata;
            for i in 0..ARRAY_SIZE {
                let rank = Leaf::<K, V>::rank(i, current);
                let new_rank = Leaf::<K, V>::rank(i, new_metadata);
                match (rank, new_rank) {
                    (_, LOCKED) => {
                        // Marks retired.
                        debug_assert_eq!(rank, LOCKED);
                        new_metadata = (new_metadata & (!Leaf::<K, V>::rank_mask(i)))
                            | Leaf::<K, V>::rank_bits(i, RETIRED);
                    }
                    (_, _) => {
                        if rank != new_rank {
                            new_metadata = (new_metadata & (!Leaf::<K, V>::rank_mask(i)))
                                | Leaf::<K, V>::rank_bits(i, rank);
                        }
                    }
                };
            }
            if let Err(result) =
                self.leaf
                    .metadata
                    .compare_exchange(current, new_metadata, Release, Relaxed)
            {
                current = result;
                continue;
            }

            current = new_metadata;
            break;
        }
        self.retired = true;
        current
    }
}

impl<'l, K, V> Drop for InsertBlocker<'l, K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    fn drop(&mut self) {
        if !self.retired {
            // Gracefully unlocks all the vacant entries.
            let mut current = self.leaf.metadata.load(Relaxed);
            loop {
                let mut new_metadata = self.metadata;
                for i in 0..ARRAY_SIZE {
                    let rank = Leaf::<K, V>::rank(i, current);
                    let new_rank = Leaf::<K, V>::rank(i, new_metadata);
                    match (rank, new_rank) {
                        (_, LOCKED) => {
                            // Needs to unlock the vacant slot.
                            debug_assert_eq!(rank, LOCKED);
                            new_metadata &= !Leaf::<K, V>::rank_mask(i);
                        }
                        (_, _) => {
                            if rank != new_rank {
                                new_metadata = (new_metadata & (!Leaf::<K, V>::rank_mask(i)))
                                    | Leaf::<K, V>::rank_bits(i, rank);
                            }
                        }
                    };
                }
                if let Err(result) =
                    self.leaf
                        .metadata
                        .compare_exchange(current, new_metadata, Release, Relaxed)
                {
                    current = result;
                    continue;
                }
                break;
            }
        }
    }
}

/// LinkedList implementation for Leaf.
impl<K, V> LinkedList for Leaf<K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    fn forward_link(&self) -> &Atomic<Leaf<K, V>> {
        &self.forward_link
    }

    fn backward_link(&self) -> &Atomic<Leaf<K, V>> {
        &self.backward_link
    }
}

/// Leaf scanner.
pub struct LeafScanner<'l, K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    leaf: &'l Leaf<K, V>,
    metadata: u32,
    entry_index: usize,
    entry_ptr: *const (K, V),
}

impl<'l, K, V> LeafScanner<'l, K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    pub fn new(leaf: &'l Leaf<K, V>) -> LeafScanner<'l, K, V> {
        LeafScanner {
            leaf,
            metadata: leaf.metadata.load(Acquire),
            entry_index: ARRAY_SIZE,
            entry_ptr: std::ptr::null(),
        }
    }

    pub fn max_less(leaf: &'l Leaf<K, V>, key: &K) -> LeafScanner<'l, K, V> {
        let metadata = leaf.metadata.load(Acquire);
        let (index, ptr) = leaf.max_less(metadata, key);
        if !ptr.is_null() {
            LeafScanner {
                leaf,
                metadata,
                entry_index: index,
                entry_ptr: ptr,
            }
        } else {
            LeafScanner::new(leaf)
        }
    }

    pub fn new_including_removed(leaf: &'l Leaf<K, V>) -> LeafScanner<'l, K, V> {
        let mut metadata = leaf.metadata.load(Acquire);
        let mut unused_ranks = 0;
        let mut current_unused_ranks_index = 0;
        for rank in 1..=ARRAY_SIZE.try_into().unwrap() {
            let mut found = false;
            for i in 0..ARRAY_SIZE {
                if Leaf::<K, V>::rank(i, metadata) == rank {
                    found = true;
                    break;
                }
            }
            if !found {
                unused_ranks |= Leaf::<K, V>::rank_bits(current_unused_ranks_index, rank);
                current_unused_ranks_index += 1;
            }
        }

        for i in 0..ARRAY_SIZE {
            let rank = Leaf::<K, V>::rank(i, metadata);
            if rank == REMOVED {
                let new_rank = Leaf::<K, V>::rank(current_unused_ranks_index - 1, unused_ranks);
                current_unused_ranks_index -= 1;
                debug_assert!(new_rank > 0 && new_rank <= ARRAY_SIZE.try_into().unwrap());
                metadata = (metadata & (!Leaf::<K, V>::rank_bits(i, REMOVED)))
                    | Leaf::<K, V>::rank_bits(i, new_rank);
            }
        }
        LeafScanner {
            leaf,
            metadata,
            entry_index: ARRAY_SIZE,
            entry_ptr: std::ptr::null(),
        }
    }

    pub fn metadata(&self) -> u32 {
        self.metadata
    }

    pub fn max_entry(&self) -> Option<(&'l K, &'l V)> {
        self.leaf.max()
    }

    /// Returns a reference to the entry that the scanner is currently pointing to
    pub fn get(&self) -> Option<(&'l K, &'l V)> {
        if self.entry_ptr.is_null() {
            return None;
        }
        unsafe { Some((&(*self.entry_ptr).0, &(*self.entry_ptr).1)) }
    }

    pub fn removed(&self) -> bool {
        Leaf::<K, V>::rank(self.entry_index, self.leaf.metadata.load(Relaxed)) == REMOVED
    }

    pub fn jump<'g>(
        &self,
        min_allowed_key: Option<&K>,
        guard: &'g Guard,
    ) -> Option<LeafScanner<'g, K, V>> {
        let mut next = self.leaf.forward_link().load(Acquire, guard);
        let mut being_split = next.tag() == 1;
        while !next.is_null() {
            let mut leaf_scanner = LeafScanner::new(unsafe { next.deref() });
            if let Some(key) = min_allowed_key.as_ref() {
                if being_split
                    || leaf_scanner
                        .leaf
                        .backward_link()
                        .load(Acquire, guard)
                        .as_raw()
                        != self.leaf as *const _
                {
                    // Data race resolution: compare keys if the current leaf has been unlinked.
                    //
                    // There is a chance that the current leaf has been unlinked,
                    // and smaller keys have been inserted into the next leaf.
                    //  - Next leaf = nl, current leaf = l, leaf node = ln, unlink = u.
                    //  - Remove: u_back(nl)|u_forward(l)|release|update(ln)|
                    //  - Insert:           |                               |insert(nl)|
                    //  - Scan:   load(nl)  |                               |          |load(nl)
                    // Therefore, it reads the backward link of the next leaf if it points to the current leaf.
                    // If not, it compares keys in order not to return keys smaller than the current one.
                    //  - Remove: u_back(nl)|u_forward(l)|release|update(ln)|
                    //  - Insert:           |                               |insert(nl)|
                    //  - Scan:   load(nl)  |                               |          |load(nl)|acquire|validate(nl->l)
                    while let Some(entry) = leaf_scanner.next() {
                        if key.cmp(&entry.0) == Ordering::Less {
                            return Some(leaf_scanner);
                        }
                    }
                }
            }
            if leaf_scanner.next().is_some() {
                return Some(leaf_scanner);
            }
            next = leaf_scanner.leaf.forward_link().load(Acquire, guard);
            being_split = being_split || next.tag() == 1;
        }
        None
    }

    fn proceed(&mut self) {
        self.entry_ptr = std::ptr::null();
        if self.entry_index == usize::MAX {
            return;
        }
        let (index, ptr) = self.leaf.next(self.entry_index, self.metadata);
        self.entry_index = index;
        self.entry_ptr = ptr;
    }
}

impl<'l, K, V> Iterator for LeafScanner<'l, K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    type Item = (&'l K, &'l V);
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
    fn basic() {
        let leaf = Leaf::new();
        assert!(leaf.insert(50, 51).is_none());
        assert_eq!(leaf.max(), Some((&50, &51)));
        assert!(leaf.insert(60, 61).is_none());
        assert!(leaf.insert(70, 71).is_none());
        assert!(leaf.remove(&60).0);
        assert!(leaf.insert(60, 61).is_none());
        assert_eq!(leaf.remove(&60), (true, false, false));
        assert!(!leaf.full());
        assert!(leaf.insert(40, 40).is_none());
        assert!(leaf.insert(30, 31).is_none());
        assert!(!leaf.full());
        assert_eq!(leaf.remove(&40), (true, false, false));
        assert!(leaf.insert(40, 41).is_none());
        assert_eq!(leaf.insert(30, 33), Some(((30, 33), true)));
        assert!(leaf.insert(10, 11).is_none());
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
        assert!(found_60_61);
        drop(scanner);

        let mut scanner = LeafScanner::max_less(&leaf, &50);
        assert_eq!(scanner.get().unwrap(), (&40, &41));
        let mut prev_key = 40;
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
        assert_eq!(leaf.remove(&10), (true, false, false));
        assert_eq!(leaf.remove(&11), (true, false, false));
        assert_eq!(leaf.remove(&12), (true, false, false));
        assert!(!leaf.full());
        assert!(leaf.remove(&20).0);
        assert!(leaf.insert(20, 21).is_none());
        assert!(leaf.search(&11).is_none());
        assert_eq!(leaf.remove(&10), (false, true, false));
        assert_eq!(leaf.remove(&11), (false, true, false));
        assert_eq!(*leaf.search(&20).unwrap(), 21);
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
        assert_eq!(iterated_low, 4);
        drop(scanner_low);
        assert!(leaves_boxed.1.is_none());
    }

    #[test]
    fn complex() {
        let leaf = Leaf::new();
        for i in 0..ARRAY_SIZE {
            assert!(leaf.insert(i, i).is_none());
        }
        for i in 0..ARRAY_SIZE - 1 {
            assert_eq!(leaf.remove(&i), (true, true, false));
        }
        assert!(leaf.full());
        assert!(!leaf.obsolete());
        assert_eq!(leaf.remove(&(ARRAY_SIZE - 1)), (true, true, true));
        assert_eq!(
            leaf.insert(ARRAY_SIZE, ARRAY_SIZE),
            Some(((ARRAY_SIZE, ARRAY_SIZE), false))
        );

        let mut scanner = LeafScanner::new_including_removed(&leaf);
        let mut expected = 0;
        while let Some(_) = scanner.next() {
            assert!(scanner.removed());
            expected += 1;
        }
        assert_eq!(expected, ARRAY_SIZE);
    }

    #[test]
    fn retire() {
        // Retire.
        let leaf1 = Leaf::new();
        let leaf2 = Leaf::new();

        for i in 0..ARRAY_SIZE - 2 {
            assert!(leaf1.insert(i, i).is_none());
            assert!(leaf2.insert(i + 4, i + 4).is_none());
            assert_eq!(leaf2.remove(&(i + 4)), (true, false, true));
        }
        assert!(!leaf1.retire());
        assert!(!leaf1.obsolete());
        assert!(leaf2.retire());
        assert!(leaf2.obsolete());
        assert_eq!(
            leaf1.insert(ARRAY_SIZE, ARRAY_SIZE),
            Some(((ARRAY_SIZE, ARRAY_SIZE), false))
        );
        assert_eq!(leaf2.insert(5, 5), Some(((5, 5), false)));
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
                            assert!(leaf_copied.remove(&tid).0);
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
