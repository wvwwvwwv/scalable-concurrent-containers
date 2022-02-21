#![allow(dead_code)]
#![allow(clippy::needless_pass_by_value)]

use crate::ebr::{Arc, AtomicArc, Barrier};
use crate::LinkedList;

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::mem::{size_of, MaybeUninit};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// The result of insertion.
pub enum InsertResult<K, V> {
    /// Insert succeeded.
    Success,

    /// Duplicate key found.
    Duplicate(K, V),

    /// The [`Leaf`] is full.
    Full(K, V),

    /// The [`Leaf`] has retired.
    Retired(K, V),
}

/// The number of entries and number of state bits per entry.
pub struct Dimension {
    num_entries: usize,
    num_bits_per_entry: usize,
}

impl Dimension {
    /// Checks if the [`Leaf`] is retired.
    fn retired(metadata: usize) -> bool {
        metadata & (1_usize << (size_of::<usize>() * 8 - 1)) != 0
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
    /// Creates a new Leaf.
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
    pub fn insert(&self, mut key: K, mut value: V) -> InsertResult<K, V> {
        let mut metadata = self.metadata.load(Acquire);
        while !Dimension::retired(metadata) {
            let mut new_metadata = 0;
            let mut max_min_rank = 0;
            let mut min_max_rank = DIMENSION.removed_state();
            let mut free_slot_index = DIMENSION.num_entries;
            for i in 0..DIMENSION.num_entries {
                let rank = DIMENSION.state(metadata, i);
                if free_slot_index == DIMENSION.num_entries && rank == 0 {
                    // Found a free slot.
                    free_slot_index = i;
                    new_metadata = DIMENSION.augment(new_metadata, i, DIMENSION.removed_state());
                } else if rank > max_min_rank && rank < min_max_rank {
                    match self.compare(i, &key) {
                        Ordering::Less => {
                            if max_min_rank < rank {
                                max_min_rank = rank;
                            }
                            metadata = DIMENSION.augment(new_metadata, i, rank);
                        }
                        Ordering::Greater => {
                            if min_max_rank > rank {
                                min_max_rank = rank;
                            }
                            metadata = DIMENSION.augment(new_metadata, i, rank + 1);
                        }
                        Ordering::Equal => {
                            // Duplicate key.
                            return InsertResult::Duplicate(key, value);
                        }
                    }
                } else if rank <= max_min_rank {
                    new_metadata = DIMENSION.augment(new_metadata, i, rank);
                } else {
                    new_metadata = DIMENSION.augment(new_metadata, i, rank + 1);
                }
            }

            if free_slot_index == DIMENSION.num_entries {
                // The `Leaf` is full.
                return InsertResult::Full(key, value);
            }

            // Reserve the slot.
            //
            // It doesn't have to be a release-store.
            if let Err(actual) =
                self.metadata
                    .compare_exchange(metadata, new_metadata, Acquire, Acquire)
            {
                metadata = actual;
                continue;
            }

            // Write the key and value.
            self.write(free_slot_index, key, value);

            // Make the newly inserted value reachable.
            let final_metadata = DIMENSION.augment(new_metadata, free_slot_index, max_min_rank + 1);
            if self
                .metadata
                .compare_exchange(new_metadata, final_metadata, Release, Relaxed)
                .is_err()
            {
                let took = self.take(free_slot_index);
                key = took.0;
                value = took.1;
                metadata = self
                    .metadata
                    .fetch_and(!DIMENSION.state_mask(free_slot_index), Acquire)
                    & (!DIMENSION.state_mask(free_slot_index));
                continue;
            }

            return InsertResult::Success;
        }

        InsertResult::Retired(key, value)
    }

    /// Removes the key if the condition is met.
    ///
    /// The first boolean value returned from the function indicates that an entry has been removed.
    /// The second boolean value indicates that the leaf is full.
    /// The third boolean value indicates that the leaf is empty.
    pub fn remove_if<Q, F: FnMut(&V) -> bool>(
        &self,
        key: &Q,
        condition: &mut F,
    ) -> (bool, bool, bool)
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut removed = false;
        let mut full = true;
        let mut empty = true;
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
                        loop {
                            if !condition(self.read(i).1) {
                                // The given condition is not met.
                                break;
                            }

                            let new_metadata = metadata | DIMENSION.state_mask(i);
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
                                    if DIMENSION.state(metadata, i) == DIMENSION.removed_state() {
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

        for i in 0..DIMENSION.num_entries {
            let rank = DIMENSION.state(metadata, i);
            if full && rank == 0 {
                full = false;
            }
            if empty && rank != 0 && rank != DIMENSION.removed_state() {
                empty = false;
            }
            if !full && !empty {
                break;
            }
        }

        (removed, full, empty)
    }

    /// Returns a value associated with the key.
    pub fn search<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let metadata = self.metadata.load(Acquire);
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
                        return Some(self.read(i).1);
                    }
                }
            }
        }
        None
    }

    /// Returns the index and a pointer to the key-value pair that is smaller than the given key.
    pub fn max_less(&self, metadata: usize, key: &K) -> (usize, *const (K, V)) {
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

    pub fn next(&self, index: usize, metadata: usize) -> (usize, *const (K, V)) {
        if index != usize::MAX {
            let current_entry_rank = if index < DIMENSION.num_entries {
                DIMENSION.state(metadata, index)
            } else {
                0
            };
            if current_entry_rank < DIMENSION.num_entries {
                let mut next_rank = DIMENSION.removed_state();
                let mut next_index = DIMENSION.num_entries;
                for i in 0..DIMENSION.num_entries {
                    let rank = DIMENSION.state(metadata, i);
                    if rank == DIMENSION.uninit_state() || rank == DIMENSION.removed_state() {
                        continue;
                    }
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
    pub fn new(leaf: &'l Leaf<K, V>) -> Scanner<'l, K, V> {
        Scanner {
            leaf,
            metadata: leaf.metadata.load(Acquire),
            entry_index: DIMENSION.num_entries,
            entry_ptr: std::ptr::null(),
        }
    }

    pub fn max_less(leaf: &'l Leaf<K, V>, key: &K) -> Scanner<'l, K, V> {
        let metadata = leaf.metadata.load(Acquire);
        let (index, ptr) = leaf.max_less(metadata, key);
        if ptr.is_null() {
            Scanner::new(leaf)
        } else {
            Scanner {
                leaf,
                metadata,
                entry_index: index,
                entry_ptr: ptr,
            }
        }
    }

    pub fn metadata(&self) -> usize {
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
        DIMENSION.state(self.leaf.metadata.load(Relaxed), self.entry_index)
            == DIMENSION.removed_state()
    }

    pub fn jump<'b>(
        &self,
        min_allowed_key: Option<&K>,
        barrier: &'b Barrier,
    ) -> Option<Scanner<'b, K, V>> {
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
                        if key.cmp(entry.0) == Ordering::Less {
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

    use tokio::sync;

    #[test]
    fn basic() {
        let leaf: Leaf<String, String> = Leaf::new();
        assert!(matches!(
            leaf.insert("MY GOODNESS!".to_owned(), "OH MY GOD!!".to_owned()),
            InsertResult::Success
        ));
        assert_eq!(leaf.search("MY GOODNESS!").unwrap(), "OH MY GOD!!");
    }

    #[test]
    fn complex() {}

    #[test]
    fn retire() {}

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn update() {
        let num_tasks = (DIMENSION.num_entries + 1) as usize;
        for _ in 0..256 {
            let barrier = Arc::new(sync::Barrier::new(num_tasks));
            let leaf: Arc<Leaf<String, String>> = Arc::new(Leaf::new());
            let mut task_handles = Vec::with_capacity(num_tasks);
            for _ in 0..num_tasks {
                let barrier_clone = barrier.clone();
                let leaf_clone = leaf.clone();
                task_handles.push(tokio::spawn(async move {
                    barrier_clone.wait().await;
                    drop(leaf_clone);
                }));
            }
            for r in futures::future::join_all(task_handles).await {
                assert!(r.is_ok());
            }
        }
    }
}
