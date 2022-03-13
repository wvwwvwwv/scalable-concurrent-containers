use crate::ebr::{Arc, AtomicArc, Barrier};
use crate::LinkedList;

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::mem::{size_of, MaybeUninit};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

/// The result of insertion.
pub enum InsertResult<K, V> {
    /// Insert succeeded.
    Success,

    /// Duplicate key found.
    Duplicate(K, V),

    /// No vacant slot for the key.
    Full(K, V),

    /// Totally unusable.
    Retired(K, V),
}

/// The result of removal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveResult {
    /// Remove succeeded.
    Success,

    /// Remove failed.
    Fail,

    /// Remove succeeded and became unusable.
    Retired,
}

/// The number of entries and number of state bits per entry.
pub struct Dimension {
    pub num_entries: usize,
    pub num_bits_per_entry: usize,
}

impl Dimension {
    /// Checks if the [`Leaf`] is retired.
    fn retired(metadata: usize) -> bool {
        metadata & (1_usize << (size_of::<usize>() * 8 - 1)) != 0
    }

    /// Makes the metadata represent a retired state.
    fn retire(metadata: usize) -> usize {
        metadata | (1_usize << (size_of::<usize>() * 8 - 1))
    }
    /// Returns a bit mask for an entry.
    fn state_mask(&self, index: usize) -> usize {
        ((1_usize << self.num_bits_per_entry) - 1) << (index * self.num_bits_per_entry)
    }

    /// Returns the state of an entry.
    fn state(&self, metadata: usize, index: usize) -> usize {
        (metadata & self.state_mask(index)) >> (index * self.num_bits_per_entry)
    }

    /// Returns an uninitialized state of an entry.
    fn uninit_state(&self) -> usize {
        0
    }

    /// Returns a removed state of an entry.
    fn removed_state(&self) -> usize {
        (1_usize << self.num_bits_per_entry) - 1
    }

    /// Augments the state to the given metadata.
    fn augment(&self, metadata: usize, index: usize, state: usize) -> usize {
        debug_assert_eq!(state & ((1_usize << self.num_bits_per_entry) - 1), state);
        (metadata & (!self.state_mask(index))) | (state << (index * self.num_bits_per_entry))
    }
}

/// The maximum number of entries and the number of metadata bits per entry in a [`Leaf`].
///
/// * M = The maximum number of entries.
/// * B = The minimum number of bits to express the state of an entry.
/// * 2 = The number of special states of an entry: uninit, removed.
/// * 1 = The number of special states of a [`Leaf`]: retired.
/// * U = `size_of::<usize>() * 8`.
/// * Eq1 = M + 2 <= 2^B: B bits represent at least M + 2 states.
/// * Eq2 = B * M + 1 <= U: M entries + 1 special state.
/// * Eq3 = Ceil(Log2(M + 2)) * M + 1 <= U: derived from Eq1 and Eq2.
///
/// Therefore, when U = 64 => M = 14 / B = 4, and U = 32 => M = 7 / B = 4.
pub const DIMENSION: Dimension = match size_of::<usize>() {
    1 => Dimension {
        num_entries: 2,
        num_bits_per_entry: 2,
    },
    2 => Dimension {
        num_entries: 5,
        num_bits_per_entry: 3,
    },
    4 => Dimension {
        num_entries: 7,
        num_bits_per_entry: 4,
    },
    8 => Dimension {
        num_entries: 14,
        num_bits_per_entry: 4,
    },
    _ => Dimension {
        num_entries: 25,
        num_bits_per_entry: 5,
    },
};

/// Each constructed entry in an `EntryArray` is never dropped until the [`Leaf`] is dropped.
pub type EntryArray<K, V> = [MaybeUninit<(K, V)>; DIMENSION.num_entries];

/// [`Leaf`] is an ordered array of key-value pairs.
///
/// A constructed key-value pair entry is never dropped until the entire [`Leaf`] instance is
/// dropped.
pub struct Leaf<K, V>
where
    K: 'static + Clone + Ord + Sync,
    V: 'static + Clone + Sync,
{
    /// The array of key-value pairs.
    entry_array: EntryArray<K, V>,

    /// A pointer that points to the next adjacent [`Leaf`].
    link: AtomicArc<Leaf<K, V>>,

    /// The metadata that manages the contents.
    ///
    /// The state of each entry is as follows.
    /// * 0: uninit.
    /// * 1-ARRAY_SIZE: rank.
    /// * ARRAY_SIZE + 1: removed.
    ///
    /// The entry state transitions as follows.
    /// * Uninit -> removed -> rank -> removed.
    metadata: AtomicUsize,
}

impl<K, V> Leaf<K, V>
where
    K: 'static + Clone + Ord + Sync,
    V: 'static + Clone + Sync,
{
    /// Creates a new [`Leaf`].
    pub fn new() -> Leaf<K, V> {
        Leaf {
            entry_array: unsafe { MaybeUninit::uninit().assume_init() },
            link: AtomicArc::null(),
            metadata: AtomicUsize::new(0),
        }
    }

    /// Returns `true` if the [`Leaf`] has retired.
    pub fn retired(&self) -> bool {
        Dimension::retired(self.metadata.load(Relaxed))
    }

    /// Returns a reference to the max key.
    pub fn max(&self) -> Option<(&K, &V)> {
        let metadata = self.metadata.load(Acquire);
        let mut max_rank = 0;
        let mut max_index = DIMENSION.num_entries;
        for i in 0..DIMENSION.num_entries {
            let rank = DIMENSION.state(metadata, i);
            if rank > max_rank && rank != DIMENSION.removed_state() {
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
    pub fn insert(&self, key: K, value: V) -> InsertResult<K, V> {
        let mut metadata = self.metadata.load(Acquire);
        while !Dimension::retired(metadata) {
            let mut has_free_slot = false;
            for i in 0..DIMENSION.num_entries {
                let rank = DIMENSION.state(metadata, i);
                if rank == DIMENSION.uninit_state() {
                    has_free_slot = true;
                    let interim_metadata =
                        DIMENSION.augment(metadata, i, DIMENSION.removed_state());

                    // Reserve the slot.
                    //
                    // It doesn't have to be a release-store.
                    if let Err(actual) =
                        self.metadata
                            .compare_exchange(metadata, interim_metadata, Acquire, Acquire)
                    {
                        metadata = actual;
                        break;
                    }

                    self.write(i, key, value);
                    return self.post_insert(i, interim_metadata);
                }
            }

            if !has_free_slot {
                if self.search_slot(key.borrow(), metadata).is_some() {
                    return InsertResult::Duplicate(key, value);
                }
                return InsertResult::Full(key, value);
            }
        }

        InsertResult::Retired(key, value)
    }

    /// Removes the key if the condition is met.
    pub fn remove_if<Q, F: FnMut(&V) -> bool>(&self, key: &Q, condition: &mut F) -> RemoveResult
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut metadata = self.metadata.load(Acquire);
        let mut max_min_rank = 0;
        let mut min_max_rank = DIMENSION.removed_state();
        for i in 0..DIMENSION.num_entries {
            let rank = DIMENSION.state(metadata, i);
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
                        // Found the key.
                        loop {
                            if !condition(self.read(i).1) {
                                // The given condition is not met.
                                return RemoveResult::Fail;
                            }
                            let mut empty = true;
                            for j in 0..DIMENSION.num_entries {
                                // Check if other entries are all unreachable.
                                if i == j {
                                    continue;
                                }
                                let rank = DIMENSION.state(metadata, j);
                                if rank != DIMENSION.uninit_state()
                                    && rank != DIMENSION.removed_state()
                                {
                                    empty = false;
                                    break;
                                }
                            }

                            let mut new_metadata = metadata | DIMENSION.state_mask(i);
                            if empty {
                                new_metadata = Dimension::retire(new_metadata);
                            }
                            match self.metadata.compare_exchange(
                                metadata,
                                new_metadata,
                                Release,
                                Relaxed,
                            ) {
                                Ok(_) => {
                                    if empty {
                                        return RemoveResult::Retired;
                                    }
                                    return RemoveResult::Success;
                                }
                                Err(actual) => {
                                    if DIMENSION.state(actual, i) == DIMENSION.removed_state() {
                                        return RemoveResult::Fail;
                                    }
                                    metadata = actual;
                                }
                            }
                        }
                    }
                };
            }
        }

        RemoveResult::Fail
    }

    /// Returns a value associated with the key.
    pub fn search<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let metadata = self.metadata.load(Acquire);
        self.search_slot(key, metadata).map(|i| self.read(i).1)
    }

    /// Returns the index and a pointer to the key-value pair that is smaller than the given key.
    pub fn max_less<Q>(&self, metadata: usize, key: &Q) -> (usize, *const (K, V))
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut max_min_rank = 0;
        let mut max_min_index = DIMENSION.num_entries;
        let mut min_max_rank = DIMENSION.removed_state();
        for i in 0..DIMENSION.num_entries {
            let rank = DIMENSION.state(metadata, i);
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
        if max_min_index != DIMENSION.num_entries {
            return (max_min_index, unsafe {
                &*self.entry_array[max_min_index].as_ptr()
            });
        }
        (usize::MAX, std::ptr::null())
    }

    /// Returns the minimum entry among those that are not `Ordering::Less` than the given key.
    ///
    /// It additionally returns the current version of its metadata in order for the caller to
    /// validate the sanity of the result.
    pub fn min_greater_equal<Q>(&self, key: &Q) -> (Option<(&K, &V)>, usize)
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let metadata = self.metadata.load(Acquire);
        let mut max_min_rank = 0;
        let mut min_max_index = DIMENSION.num_entries;
        let mut min_max_rank = DIMENSION.removed_state();
        for i in 0..DIMENSION.num_entries {
            let rank = DIMENSION.state(metadata, i);
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
        if min_max_rank != DIMENSION.removed_state() {
            return (Some(self.read(min_max_index)), metadata);
        }
        (None, metadata)
    }

    /// Compares the given metadata value with the current one.
    pub fn validate(&self, metadata: usize) -> bool {
        // `Relaxed` is sufficient as long as the caller has read-acquired its contents.
        self.metadata.load(Relaxed) == metadata
    }

    /// Returns the index and a pointer to the corresponding entry of the next higher ranked entry.
    pub fn next(&self, index: usize, metadata: usize) -> (usize, *const (K, V)) {
        let current_entry_rank = if index < DIMENSION.num_entries {
            DIMENSION.state(metadata, index)
        } else {
            0
        };
        if current_entry_rank < DIMENSION.num_entries {
            let mut next_rank = DIMENSION.removed_state();
            let mut next_index = DIMENSION.num_entries;
            for i in 0..DIMENSION.num_entries {
                if i == index {
                    continue;
                }
                let rank = DIMENSION.state(metadata, i);
                if rank == DIMENSION.uninit_state() || rank == DIMENSION.removed_state() {
                    continue;
                }
                debug_assert_ne!(rank, current_entry_rank);
                if current_entry_rank < rank && rank < next_rank {
                    next_rank = rank;
                    next_index = i;
                }
            }
            if next_rank != DIMENSION.removed_state() {
                return (next_index, unsafe {
                    &*self.entry_array[next_index].as_ptr()
                });
            }
        }
        (usize::MAX, std::ptr::null())
    }

    pub fn distribute(
        &self,
        low_key_leaf: &mut Option<Arc<Leaf<K, V>>>,
        high_key_leaf: &mut Option<Arc<Leaf<K, V>>>,
    ) {
        let mut iterated = 0;
        for entry in Scanner::new(self) {
            if iterated < DIMENSION.num_entries / 2 {
                if low_key_leaf.is_none() {
                    low_key_leaf.replace(Arc::new(Leaf::new()));
                }
                low_key_leaf
                    .as_ref()
                    .unwrap()
                    .insert(entry.0.clone(), entry.1.clone());
                iterated += 1;
            } else {
                if high_key_leaf.is_none() {
                    high_key_leaf.replace(Arc::new(Leaf::new()));
                }
                high_key_leaf
                    .as_ref()
                    .unwrap()
                    .insert(entry.0.clone(), entry.1.clone());
            }
        }
    }

    /// Searches for a slot in which the key is stored.
    fn search_slot<Q>(&self, key: &Q, metadata: usize) -> Option<usize>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut max_min_rank = 0;
        let mut min_max_rank = DIMENSION.removed_state();
        for i in 0..DIMENSION.num_entries {
            let rank = DIMENSION.state(metadata, i);
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
                        return Some(i);
                    }
                }
            }
        }
        None
    }

    /// Post-processing after reserving a free slot.
    fn post_insert(&self, free_slot_index: usize, mut metadata: usize) -> InsertResult<K, V> {
        let key_ref = self.read(free_slot_index).0;
        loop {
            let mut new_metadata = metadata;
            let mut max_min_rank = 0;
            let mut min_max_rank = DIMENSION.removed_state();
            for i in 0..DIMENSION.num_entries {
                let rank = DIMENSION.state(metadata, i);
                if rank == DIMENSION.uninit_state() || rank == DIMENSION.removed_state() {
                    continue;
                }
                if rank > max_min_rank && rank < min_max_rank {
                    match self.compare(i, key_ref) {
                        Ordering::Less => {
                            if max_min_rank < rank {
                                max_min_rank = rank;
                            }
                        }
                        Ordering::Greater => {
                            if min_max_rank > rank {
                                min_max_rank = rank;
                            }
                            new_metadata = DIMENSION.augment(new_metadata, i, rank + 1);
                        }
                        Ordering::Equal => {
                            // Duplicate key.
                            return self.rollback(free_slot_index);
                        }
                    }
                } else if rank > min_max_rank {
                    new_metadata = DIMENSION.augment(new_metadata, i, rank + 1);
                }
            }

            // Make the newly inserted value reachable.
            let final_metadata = DIMENSION.augment(new_metadata, free_slot_index, max_min_rank + 1);
            if let Err(actual) =
                self.metadata
                    .compare_exchange(metadata, final_metadata, AcqRel, Acquire)
            {
                if Dimension::retired(actual) {
                    return self.rollback(free_slot_index);
                }
                metadata = actual;
                continue;
            }

            return InsertResult::Success;
        }
    }

    fn rollback(&self, index: usize) -> InsertResult<K, V> {
        let (key, value) = self.take(index);
        let result = self
            .metadata
            .fetch_and(!DIMENSION.state_mask(index), Relaxed)
            & (!DIMENSION.state_mask(index));
        if Dimension::retired(result) {
            InsertResult::Retired(key, value)
        } else {
            InsertResult::Duplicate(key, value)
        }
    }

    fn compare<Q>(&self, index: usize, key: &Q) -> std::cmp::Ordering
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let entry_ref = unsafe { &*self.entry_array[index].as_ptr() };
        (*entry_ref.0.borrow()).cmp(key)
    }

    fn take(&self, index: usize) -> (K, V) {
        unsafe {
            let entry_array_ptr = &self.entry_array as *const EntryArray<K, V>;
            let entry_array_mut_ptr = entry_array_ptr as *mut EntryArray<K, V>;
            let entry_array_mut_ref = &mut (*entry_array_mut_ptr);
            let entry_ptr = entry_array_mut_ref[index].as_mut_ptr();
            std::ptr::read(entry_ptr)
        }
    }

    fn write(&self, index: usize, key: K, value: V) {
        unsafe {
            let entry_array_ptr = &self.entry_array as *const EntryArray<K, V>;
            let entry_array_mut_ptr = entry_array_ptr as *mut EntryArray<K, V>;
            let entry_array_mut_ref = &mut (*entry_array_mut_ptr);
            entry_array_mut_ref[index].as_mut_ptr().write((key, value));
        }
    }

    fn read(&self, index: usize) -> (&K, &V) {
        let entry_ref = unsafe { &*self.entry_array[index].as_ptr() };
        (&entry_ref.0, &entry_ref.1)
    }
}

impl<K, V> Drop for Leaf<K, V>
where
    K: 'static + Clone + Ord + Sync,
    V: 'static + Clone + Sync,
{
    fn drop(&mut self) {
        let metadata = self.metadata.load(Acquire);
        for i in 0..DIMENSION.num_entries {
            let rank = DIMENSION.state(metadata, i);
            if rank != DIMENSION.uninit_state() {
                self.take(i);
            }
        }
    }
}

/// [`LinkedList`] implementation for [`Leaf`].
impl<K, V> LinkedList for Leaf<K, V>
where
    K: 'static + Clone + Ord + Sync,
    V: 'static + Clone + Sync,
{
    fn link_ref(&self) -> &AtomicArc<Leaf<K, V>> {
        &self.link
    }
}

/// Leaf scanner.
pub struct Scanner<'l, K, V>
where
    K: 'static + Clone + Ord + Sync,
    V: 'static + Clone + Sync,
{
    leaf: &'l Leaf<K, V>,
    metadata: usize,
    entry_index: usize,
    entry_ptr: *const (K, V),
}

impl<'l, K, V> Scanner<'l, K, V>
where
    K: 'static + Clone + Ord + Sync,
    V: 'static + Clone + Sync,
{
    /// Creates a new [`Scanner`].
    pub fn new(leaf: &'l Leaf<K, V>) -> Scanner<'l, K, V> {
        Scanner {
            leaf,
            metadata: leaf.metadata.load(Acquire),
            entry_index: DIMENSION.num_entries,
            entry_ptr: std::ptr::null(),
        }
    }

    /// Returns a [`Scanner`] pointing to the max-less entry if there is one.
    ///
    /// If there is no key that is smaller than the given key, it returns a default [`Scanner`].
    pub fn max_less<Q>(leaf: &'l Leaf<K, V>, key: &Q) -> Option<Scanner<'l, K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let metadata = leaf.metadata.load(Acquire);
        let (index, ptr) = leaf.max_less(metadata, key);
        if ptr.is_null() {
            None
        } else {
            Some(Scanner {
                leaf,
                metadata,
                entry_index: index,
                entry_ptr: ptr,
            })
        }
    }

    /// Returns the metadata that the [`Scanner`] is currently using.
    pub fn metadata(&self) -> usize {
        self.metadata
    }

    /// Returns a reference to the entry that the scanner is currently pointing to
    pub fn get(&self) -> Option<(&'l K, &'l V)> {
        if self.entry_ptr.is_null() {
            return None;
        }
        unsafe { Some((&(*self.entry_ptr).0, &(*self.entry_ptr).1)) }
    }

    /// Checks if the entry that the [`Scanner`] is pointing to is now removed.
    pub fn removed(&self) -> bool {
        DIMENSION.state(self.leaf.metadata.load(Relaxed), self.entry_index)
            == DIMENSION.removed_state()
    }

    /// Traverses the linked list.
    pub fn jump<'b, Q>(
        &self,
        min_allowed_key: Option<&Q>,
        barrier: &'b Barrier,
    ) -> Option<Scanner<'b, K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut next_leaf_ptr = self.leaf.next_ptr(Acquire, barrier);
        while let Some(next_leaf_ref) = next_leaf_ptr.as_ref() {
            let mut leaf_scanner = Scanner::new(next_leaf_ref);
            if let Some(key) = min_allowed_key {
                if !self.leaf.is_clear(Relaxed) {
                    // Data race resolution: compare keys if the current leaf has been deleted.
                    //
                    // There is a chance that the current leaf has been deleted, and smaller
                    // keys have been inserted into the next leaf.
                    while let Some(entry) = leaf_scanner.next() {
                        if key.cmp(entry.0.borrow()) == Ordering::Less {
                            return Some(leaf_scanner);
                        }
                    }
                    next_leaf_ptr = next_leaf_ref.next_ptr(Acquire, barrier);
                    continue;
                }
            }
            if leaf_scanner.next().is_some() {
                return Some(leaf_scanner);
            }
            next_leaf_ptr = next_leaf_ref.next_ptr(Acquire, barrier);
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

impl<'l, K, V> Iterator for Scanner<'l, K, V>
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

    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::Relaxed;

    use proptest::prelude::*;
    use tokio::sync;

    #[test]
    fn basic() {
        let leaf: Leaf<String, String> = Leaf::new();
        assert!(matches!(
            leaf.insert("MY GOODNESS!".to_owned(), "OH MY GOD!!".to_owned()),
            InsertResult::Success
        ));
        assert!(matches!(
            leaf.insert("GOOD DAY".to_owned(), "OH MY GOD!!".to_owned()),
            InsertResult::Success
        ));
        assert_eq!(leaf.search("MY GOODNESS!").unwrap(), "OH MY GOD!!");
        assert_eq!(leaf.search("GOOD DAY").unwrap(), "OH MY GOD!!");

        for i in 0..DIMENSION.num_entries {
            if let InsertResult::Full(k, v) = leaf.insert(i.to_string(), i.to_string()) {
                assert_eq!(i + 2, DIMENSION.num_entries);
                assert_eq!(k, i.to_string());
                assert_eq!(v, i.to_string());
                break;
            }
            assert_eq!(leaf.search(&i.to_string()).unwrap(), &i.to_string());
        }

        for i in 0..DIMENSION.num_entries {
            let result = leaf.remove_if(&i.to_string(), &mut |_| i >= 10);
            if i >= 10 && i + 2 < DIMENSION.num_entries {
                assert_eq!(result, RemoveResult::Success);
            } else {
                assert_eq!(result, RemoveResult::Fail);
            }
        }

        assert_eq!(
            leaf.remove_if("GOOD DAY", &mut |v| v == "OH MY"),
            RemoveResult::Fail
        );
        assert_eq!(
            leaf.remove_if("GOOD DAY", &mut |v| v == "OH MY GOD!!"),
            RemoveResult::Success
        );
        assert!(leaf.search("GOOD DAY").is_none());
        assert_eq!(
            leaf.remove_if("MY GOODNESS!", &mut |_| true),
            RemoveResult::Success
        );
        assert!(leaf.search("MY GOODNESS!").is_none());
        assert!(leaf.search("1").is_some());
        assert!(matches!(
            leaf.insert("1".to_owned(), "1".to_owned()),
            InsertResult::Duplicate(..)
        ));
        assert!(matches!(
            leaf.insert("100".to_owned(), "100".to_owned()),
            InsertResult::Full(..)
        ));

        let mut scanner = Scanner::new(&leaf);
        for i in 0..DIMENSION.num_entries {
            if let Some(e) = scanner.next() {
                assert_eq!(e.0, &i.to_string());
                assert_eq!(e.1, &i.to_string());
                assert_ne!(
                    leaf.remove_if(&i.to_string(), &mut |_| true),
                    RemoveResult::Fail
                );
            } else {
                break;
            }
        }

        assert!(matches!(
            leaf.insert("200".to_owned(), "200".to_owned()),
            InsertResult::Retired(..)
        ));
    }

    proptest! {
        #[test]
        fn prop(insert in 0_usize..DIMENSION.num_entries, remove in 0_usize..DIMENSION.num_entries) {
            let leaf: Leaf<usize, usize> = Leaf::new();
            for i in 0..insert {
                assert!(matches!(leaf.insert(i, i), InsertResult::Success));
                if i != 0 {
                    let result = leaf.max_less(leaf.metadata.load(Relaxed), &i);
                    assert_eq!(unsafe { *result.1 }, (i - 1, i - 1));
                }
            }
            for i in 0..insert {
                assert!(matches!(leaf.insert(i, i), InsertResult::Duplicate(..)));
                let result = leaf.min_greater_equal(&i);
                assert_eq!(result.0, Some((&i, &i)));
            }
            for i in 0..insert {
                assert_eq!(*leaf.search(&i).unwrap(), i);
            }
            if insert == DIMENSION.num_entries {
                assert!(matches!(leaf.insert(usize::MAX, usize::MAX), InsertResult::Full(..)));
            }
            for i in 0..remove {
                if i < insert {
                    if i == insert - 1 {
                        assert!(matches!(leaf.remove_if(&i, &mut |_| true), RemoveResult::Retired));
                        for i in 0..insert {
                            assert!(matches!(leaf.insert(i, i), InsertResult::Retired(..)));
                        }
                    } else {
                        assert!(matches!(leaf.remove_if(&i, &mut |_| true), RemoveResult::Success));
                    }
                } else {
                    assert!(matches!(leaf.remove_if(&i, &mut |_| true), RemoveResult::Fail));
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn update() {
        let num_excess = 3;
        let num_tasks = (DIMENSION.num_entries + num_excess) as usize;
        for _ in 0..256 {
            let barrier = Arc::new(sync::Barrier::new(num_tasks));
            let leaf: Arc<Leaf<usize, usize>> = Arc::new(Leaf::new());
            let full: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
            let retire: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
            let mut task_handles = Vec::with_capacity(num_tasks);
            for t in 1..=num_tasks {
                let barrier_clone = barrier.clone();
                let leaf_clone = leaf.clone();
                let full_clone = full.clone();
                let retire_clone = retire.clone();
                task_handles.push(tokio::spawn(async move {
                    barrier_clone.wait().await;
                    let inserted = match leaf_clone.insert(t, t) {
                        InsertResult::Success => {
                            assert_eq!(*leaf_clone.search(&t).unwrap(), t);
                            true
                        }
                        InsertResult::Duplicate(_, _) | InsertResult::Retired(_, _) => {
                            unreachable!();
                        }
                        InsertResult::Full(k, v) => {
                            assert_eq!(k, v);
                            assert_eq!(k, t);
                            full_clone.fetch_add(1, Relaxed);
                            false
                        }
                    };
                    {
                        let mut prev = 0;
                        let mut scanner = Scanner::new(&leaf_clone);
                        for e in scanner.by_ref() {
                            assert_eq!(e.0, e.1);
                            assert!(*e.0 > prev);
                            prev = *e.0;
                        }
                    }

                    barrier_clone.wait().await;
                    assert_eq!(full_clone.load(Relaxed), num_excess);
                    if inserted {
                        assert_eq!(*leaf_clone.search(&t).unwrap(), t);
                    }
                    {
                        let scanner = Scanner::new(&leaf_clone);
                        assert_eq!(scanner.count(), DIMENSION.num_entries);
                    }

                    barrier_clone.wait().await;
                    match leaf_clone.remove_if(&t, &mut |_| true) {
                        RemoveResult::Success => assert!(inserted),
                        RemoveResult::Fail => assert!(!inserted),
                        RemoveResult::Retired => {
                            assert!(inserted);
                            assert_eq!(retire_clone.swap(1, Relaxed), 0);
                        }
                    };
                }));
            }
            for r in futures::future::join_all(task_handles).await {
                assert!(r.is_ok());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn durability() {
        let num_tasks = 16_usize;
        let workload_size = 8_usize;
        for _ in 0..16 {
            for k in 0..=workload_size {
                let barrier = Arc::new(sync::Barrier::new(num_tasks));
                let leaf: Arc<Leaf<usize, usize>> = Arc::new(Leaf::new());
                let inserted: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
                let mut task_handles = Vec::with_capacity(num_tasks);
                for _ in 0..num_tasks {
                    let barrier_clone = barrier.clone();
                    let leaf_clone = leaf.clone();
                    let inserted_clone = inserted.clone();
                    task_handles.push(tokio::spawn(async move {
                        {
                            barrier_clone.wait().await;
                            if let InsertResult::Success = leaf_clone.insert(k, k) {
                                assert!(!inserted_clone.swap(true, Relaxed));
                            }
                        }
                        {
                            barrier_clone.wait().await;
                            for i in 0..workload_size {
                                if i != k {
                                    let _result = leaf_clone.insert(i, i);
                                }
                                assert!(!leaf_clone.retired());
                                assert_eq!(leaf_clone.search(&k).unwrap(), &k);
                            }
                            for i in 0..workload_size {
                                let _result = leaf_clone.remove_if(&i, &mut |v| *v != k);
                                assert_eq!(leaf_clone.search(&k).unwrap(), &k);
                            }
                        }
                    }));
                }
                for r in futures::future::join_all(task_handles).await {
                    assert!(r.is_ok());
                }
                assert!(inserted.load(Relaxed));
            }
        }
    }
}
