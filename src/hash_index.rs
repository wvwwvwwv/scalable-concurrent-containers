//! The module implements [`HashIndex`].

use super::async_yield::{self, AwaitableBarrier};
use super::ebr::{Arc, AtomicArc, Barrier, Ptr};
use super::hash_table::cell::{EntryIterator, Locker};
use super::hash_table::cell_array::CellArray;
use super::hash_table::HashTable;

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::iter::FusedIterator;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::Acquire;

/// Scalable concurrent hash index.
///
/// [`HashIndex`] is a concurrent hash index data structure that is optimized for read
/// operations. The key characteristics of [`HashIndex`] are similar to that of
/// [`HashMap`](super::HashMap).
///
/// ## Notes
///
/// [`HashIndex`] methods are linearlizable, however [`Visitor`] methods are not; [`Visitor`]
/// is only guaranteed to observe events happened before the first call to [`Iterator::next`].
///
/// ## The key differences between [`HashIndex`] and [`HashMap`](crate::HashMap).
///
/// * Lock-free-read: read and scan operations do not modify shared data and are never blocked.
/// * Immutability: the data in the container is immutable until it becomes unreachable.
///
/// ## The key statistics for [`HashIndex`]
///
/// * The expected size of metadata for a single key-value pair: 2-byte.
/// * The expected number of atomic write operations required for an operation on a single key: 2.
/// * The expected number of atomic variables accessed during a single key operation: 2.
/// * The number of entries managed by a single metadata cell without a linked list: 32.
/// * The expected maximum linked list length when resize is triggered: log(capacity) / 8.
pub struct HashIndex<K, V, H = RandomState>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
    H: BuildHasher,
{
    array: AtomicArc<CellArray<K, V, true>>,
    minimum_capacity: usize,
    resize_mutex: AtomicU8,
    build_hasher: H,
}

impl<K, V, H> HashIndex<K, V, H>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
    H: BuildHasher,
{
    /// Creates an empty [`HashIndex`] with the given capacity and build hasher.
    ///
    /// The actual capacity is equal to or greater than the given capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashindex: HashIndex<u64, u32, RandomState> = HashIndex::new(1000, RandomState::new());
    ///
    /// let result = hashindex.capacity();
    /// assert_eq!(result, 1024);
    ///
    ///
    /// let hashindex: HashIndex<u64, u32, _> = HashIndex::default();
    /// let result = hashindex.capacity();
    /// assert_eq!(result, 64);
    /// ```
    #[inline]
    pub fn new(capacity: usize, build_hasher: H) -> HashIndex<K, V, H> {
        let initial_capacity = capacity.max(Self::default_capacity());
        HashIndex {
            array: AtomicArc::from(Arc::new(CellArray::<K, V, true>::new(
                initial_capacity,
                AtomicArc::null(),
            ))),
            minimum_capacity: initial_capacity,
            resize_mutex: AtomicU8::new(0),
            build_hasher,
        }
    }

    /// Inserts a key-value pair into the [`HashIndex`].
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied key-value pair if the key exists.
    ///
    /// # Panics
    ///
    /// Panics if the number of entries in the target `Cell` reaches `u32::MAX` due to a poor hash
    /// function.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::default();
    ///
    /// assert!(hashindex.insert(1, 0).is_ok());
    /// assert_eq!(hashindex.insert(1, 1).unwrap_err(), (1, 1));
    /// ```
    #[inline]
    pub fn insert(&self, key: K, val: V) -> Result<(), (K, V)> {
        let (hash, partial_hash) = self.hash(&key);
        if let Some((k, v)) = self
            .insert_entry::<false>(key, val, hash, partial_hash, &Barrier::new())
            .ok()
            .unwrap()
        {
            Err((k, v))
        } else {
            Ok(())
        }
    }

    /// Inserts a key-value pair into the [`HashIndex`].
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await or poll.
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied key-value pair if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::default();
    /// let future_insert = hashindex.insert_async(11, 17);
    /// ```
    #[inline]
    pub async fn insert_async(&self, mut key: K, mut val: V) -> Result<(), (K, V)> {
        let (hash, partial_hash) = self.hash(&key);
        loop {
            match self.insert_entry::<true>(key, val, hash, partial_hash, &Barrier::new()) {
                Ok(Some(returned)) => return Err(returned),
                Ok(None) => return Ok(()),
                Err(returned) => {
                    key = returned.0;
                    val = returned.1;
                }
            }
            async_yield::async_yield().await;
        }
    }

    /// Removes a key-value pair if the key exists.
    ///
    /// This method only marks the entry unreachable, and the memory will be reclaimed later.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::default();
    ///
    /// assert!(!hashindex.remove(&1));
    /// assert!(hashindex.insert(1, 0).is_ok());
    /// assert!(hashindex.remove(&1));
    /// ```
    #[inline]
    pub fn remove<Q>(&self, key_ref: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.remove_if(key_ref, |_| true)
    }

    /// Removes a key-value pair if the key exists.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await or poll.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::default();
    /// let future_insert = hashindex.insert_async(11, 17);
    /// let future_remove = hashindex.remove_async(&11);
    /// ```
    #[inline]
    pub async fn remove_async<Q>(&self, key_ref: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.remove_if_async(key_ref, |_| true).await
    }

    /// Removes a key-value pair if the key exists and the given condition is met.
    ///
    /// This method only marks the entry unreachable, and the memory will be reclaimed later.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::default();
    ///
    /// assert!(hashindex.insert(1, 0).is_ok());
    /// assert!(!hashindex.remove_if(&1, |v| *v == 1));
    /// assert!(hashindex.remove_if(&1, |v| *v == 0));
    /// ```
    #[inline]
    pub fn remove_if<Q, F: FnMut(&V) -> bool>(&self, key_ref: &Q, mut condition: F) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (hash, partial_hash) = self.hash(key_ref);
        self.remove_entry::<Q, _, false>(
            key_ref,
            hash,
            partial_hash,
            &mut condition,
            &Barrier::new(),
        )
        .ok()
        .map_or(false, |(_, r)| r)
    }

    /// Removes a key-value pair if the key exists and the given condition is met.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await or poll.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::default();
    /// let future_insert = hashindex.insert_async(11, 17);
    /// let future_remove = hashindex.remove_if_async(&11, |_| true);
    /// ```
    #[inline]
    pub async fn remove_if_async<Q, F: FnMut(&V) -> bool>(
        &self,
        key_ref: &Q,
        mut condition: F,
    ) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (hash, partial_hash) = self.hash(key_ref);
        loop {
            if let Ok(result) = self.remove_entry::<Q, F, true>(
                key_ref,
                hash,
                partial_hash,
                &mut condition,
                &Barrier::new(),
            ) {
                return result.0;
            }
            async_yield::async_yield().await;
        }
    }

    /// Reads a key-value pair.
    ///
    /// It returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::default();
    ///
    /// assert!(hashindex.read(&1, |_, v| *v).is_none());
    /// assert!(hashindex.insert(1, 10).is_ok());
    /// assert_eq!(hashindex.read(&1, |_, v| *v).unwrap(), 10);
    /// ```
    #[inline]
    pub fn read<Q, R, F: Fn(&K, &V) -> R>(&self, key_ref: &Q, reader: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let barrier = Barrier::new();
        self.read_with(key_ref, reader, &barrier)
    }

    /// Reads a key-value pair using the supplied [`Barrier`].
    ///
    /// It enables the caller to use the value reference outside the method. It returns `None`
    /// if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::default();
    ///
    /// assert!(hashindex.insert(1, 10).is_ok());
    ///
    /// let barrier = Barrier::new();
    /// let value_ref = hashindex.read_with(&1, |k, v| v, &barrier).unwrap();
    /// assert_eq!(*value_ref, 10);
    /// ```
    #[inline]
    pub fn read_with<'b, Q, R, F: FnMut(&'b K, &'b V) -> R>(
        &self,
        key_ref: &Q,
        mut reader: F,
        barrier: &'b Barrier,
    ) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (hash, partial_hash) = self.hash(key_ref);
        self.read_entry::<Q, R, F, false>(key_ref, hash, partial_hash, &mut reader, barrier)
            .ok()
            .and_then(|r| r)
    }

    /// Checks if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::default();
    ///
    /// assert!(!hashindex.contains(&1));
    /// assert!(hashindex.insert(1, 0).is_ok());
    /// assert!(hashindex.contains(&1));
    /// ```
    #[inline]
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.read(key, |_, _| ()).is_some()
    }

    /// Clears all the key-value pairs.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::default();
    ///
    /// assert!(hashindex.insert(1, 0).is_ok());
    /// assert_eq!(hashindex.clear(), 1);
    /// ```
    pub fn clear(&self) -> usize {
        let mut num_removed: usize = 0;
        let barrier = Barrier::new();
        let mut current_array_ptr = self.array.load(Acquire, &barrier);
        while let Some(current_array_ref) = current_array_ptr.as_ref() {
            if !current_array_ref.old_array(&barrier).is_null() {
                while !current_array_ref.partial_rehash::<_, _, _, false>(
                    |k| self.hash(k),
                    |k, v| Some((k.clone(), v.clone())),
                    &barrier,
                ) {
                    current_array_ptr = self.array.load(Acquire, &barrier);
                    continue;
                }
            }
            for index in 0..current_array_ref.num_cells() {
                if let Some(locker) = Locker::lock(current_array_ref.cell(index), &barrier) {
                    let mut iterator = locker.cell().iter(&barrier);
                    while iterator.next().is_some() {
                        locker.erase(&mut iterator);
                        num_removed = num_removed.saturating_add(1);
                    }
                }
            }
            let new_current_array_ptr = self.array.load(Acquire, &barrier);
            if current_array_ptr == new_current_array_ptr {
                self.resize(&barrier);
                break;
            }
            current_array_ptr = new_current_array_ptr;
        }
        num_removed
    }

    /// Clears all the key-value pairs.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await or poll.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::default();
    ///
    /// let future_insert = hashindex.insert_async(1, 0);
    /// let future_retain = hashindex.clear_async();
    /// ```
    pub async fn clear_async(&self) -> usize {
        let mut num_removed: usize = 0;

        // An acquire fence is required to correctly load the contents of the array.
        let mut awaitable_barrier = AwaitableBarrier::default();
        let mut current_array_holder = self.array.get_arc(Acquire, awaitable_barrier.barrier());
        while let Some(current_array) = current_array_holder.take() {
            while !current_array
                .old_array(awaitable_barrier.barrier())
                .is_null()
            {
                if current_array.partial_rehash::<_, _, _, true>(
                    |key| self.hash(key),
                    |_, _| None,
                    awaitable_barrier.barrier(),
                ) {
                    continue;
                }
                awaitable_barrier.drop_barrier_and_yield().await;
            }

            for cell_index in 0..current_array.num_cells() {
                loop {
                    {
                        // Limits the scope of `barrier`.
                        let barrier = awaitable_barrier.barrier();
                        if let Ok(result) =
                            Locker::try_lock(current_array.cell(cell_index), barrier)
                        {
                            if let Some(locker) = result {
                                let mut iterator = locker.cell().iter(barrier);
                                while iterator.next().is_some() {
                                    locker.erase(&mut iterator);
                                    num_removed = num_removed.saturating_add(1);
                                }
                            }
                            break;
                        }
                    }
                    awaitable_barrier.drop_barrier_and_yield().await;
                }
            }

            if let Some(new_current_array) =
                self.array.get_arc(Acquire, awaitable_barrier.barrier())
            {
                if new_current_array.as_ptr() == current_array.as_ptr() {
                    break;
                }
                current_array_holder.replace(new_current_array);
                continue;
            }
            break;
        }

        if num_removed != 0 {
            self.resize(&Barrier::new());
        }

        num_removed
    }

    /// Returns the number of entries in the [`HashIndex`].
    ///
    /// It scans the entire array to calculate the number of valid entries, making its time
    /// complexity `O(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::default();
    ///
    /// assert!(hashindex.insert(1, 0).is_ok());
    /// assert_eq!(hashindex.len(), 1);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        self.num_entries(&Barrier::new())
    }

    /// Returns `true` if the [`HashIndex`] is empty.
    ///
    /// It scans the entire array to calculate the number of valid entries, making its time
    /// complexity `O(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::default();
    ///
    /// assert!(hashindex.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the capacity of the [`HashIndex`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashindex: HashIndex<u64, u32, RandomState> = HashIndex::new(1000000, RandomState::new());
    /// assert_eq!(hashindex.capacity(), 1048576);
    /// ```
    #[inline]
    pub fn capacity(&self) -> usize {
        self.num_slots(&Barrier::new())
    }

    /// Returns a [`Visitor`] that iterates over all the entries in the [`HashIndex`].
    ///
    /// It is guaranteed to go through all the key-value pairs pertaining in the [`HashIndex`]
    /// at the moment, however the same key-value pair can be visited more than once if the
    /// [`HashIndex`] is being resized.
    ///
    /// It requires the user to supply a reference to a [`Barrier`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::default();
    ///
    /// assert!(hashindex.insert(1, 0).is_ok());
    ///
    /// let barrier = Barrier::new();
    ///
    /// let mut iter = hashindex.iter(&barrier);
    /// let entry_ref = iter.next().unwrap();
    /// assert_eq!(iter.next(), None);
    ///
    /// for iter in hashindex.iter(&barrier) {
    ///     assert_eq!(iter, (&1, &0));
    /// }
    ///
    /// drop(hashindex);
    ///
    /// assert_eq!(entry_ref, (&1, &0));
    /// ```
    #[inline]
    pub fn iter<'h, 'b>(&'h self, barrier: &'b Barrier) -> Visitor<'h, 'b, K, V, H> {
        Visitor {
            hash_index: self,
            current_array_ptr: Ptr::null(),
            current_index: 0,
            current_entry_iterator: None,
            barrier_ref: barrier,
        }
    }
}

impl<K, V> Default for HashIndex<K, V, RandomState>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
{
    /// Creates a [`HashIndex`] with the default parameters.
    ///
    /// The default hash builder is [`RandomState`], and the default capacity is `64`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32, _> = HashIndex::default();
    ///
    /// let result = hashindex.capacity();
    /// assert_eq!(result, 64);
    /// ```
    #[inline]
    fn default() -> Self {
        HashIndex {
            array: AtomicArc::from(Arc::new(CellArray::<K, V, true>::new(
                Self::default_capacity(),
                AtomicArc::null(),
            ))),
            minimum_capacity: Self::default_capacity(),
            resize_mutex: AtomicU8::new(0),
            build_hasher: RandomState::new(),
        }
    }
}

impl<K, V, H> HashTable<K, V, H, true> for HashIndex<K, V, H>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
    H: BuildHasher,
{
    fn hasher(&self) -> &H {
        &self.build_hasher
    }
    fn copier(key: &K, val: &V) -> Option<(K, V)> {
        Some((key.clone(), val.clone()))
    }
    fn cell_array(&self) -> &AtomicArc<CellArray<K, V, true>> {
        &self.array
    }
    fn minimum_capacity(&self) -> usize {
        self.minimum_capacity
    }
    fn resize_mutex(&self) -> &AtomicU8 {
        &self.resize_mutex
    }
}

/// Visitor traverses all the key-value pairs in the [`HashIndex`].
///
/// It is guaranteed to visit all the key-value pairs that outlive the Visitor.
/// However, the same key-value pair can be visited more than once.
pub struct Visitor<'h, 'b, K, V, H>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
    H: BuildHasher,
{
    hash_index: &'h HashIndex<K, V, H>,
    current_array_ptr: Ptr<'b, CellArray<K, V, true>>,
    current_index: usize,
    current_entry_iterator: Option<EntryIterator<'b, K, V, true>>,
    barrier_ref: &'b Barrier,
}

impl<'h, 'b, K, V, H> Iterator for Visitor<'h, 'b, K, V, H>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
    H: 'static + BuildHasher,
{
    type Item = (&'b K, &'b V);
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_array_ptr.is_null() {
            // Start scanning.
            let current_array_ptr = self.hash_index.array.load(Acquire, self.barrier_ref);
            let current_array_ref = current_array_ptr.as_ref().unwrap();
            let old_array_ptr = current_array_ref.old_array(self.barrier_ref);
            self.current_array_ptr = if old_array_ptr.is_null() {
                current_array_ptr
            } else {
                old_array_ptr
            };
            let cell_ref = self.current_array_ptr.as_ref().unwrap().cell(0);
            self.current_entry_iterator
                .replace(EntryIterator::new(cell_ref, self.barrier_ref));
        }
        loop {
            if let Some(iterator) = self.current_entry_iterator.as_mut() {
                // Go to the next entry in the Cell.
                if let Some(entry) = iterator.next() {
                    return Some((&entry.0 .0, &entry.0 .1));
                }
            }
            // Go to the next Cell.
            let array_ref = self.current_array_ptr.as_ref().unwrap();
            self.current_index += 1;
            if self.current_index == array_ref.num_cells() {
                let current_array_ptr = self.hash_index.array.load(Acquire, self.barrier_ref);
                if self.current_array_ptr == current_array_ptr {
                    // Finished scanning the entire array.
                    break;
                }
                let current_array_ref = current_array_ptr.as_ref().unwrap();
                let old_array_ptr = current_array_ref.old_array(self.barrier_ref);
                if self.current_array_ptr == old_array_ptr {
                    // Starts scanning the current array.
                    self.current_array_ptr = current_array_ptr;
                    self.current_index = 0;
                    self.current_entry_iterator.replace(EntryIterator::new(
                        current_array_ref.cell(0),
                        self.barrier_ref,
                    ));
                    continue;
                }
                // Start from the very beginning.
                self.current_array_ptr = if old_array_ptr.is_null() {
                    current_array_ptr
                } else {
                    old_array_ptr
                };
                self.current_index = 0;
                self.current_entry_iterator.replace(EntryIterator::new(
                    self.current_array_ptr.as_ref().unwrap().cell(0),
                    self.barrier_ref,
                ));
                continue;
            }
            self.current_entry_iterator.replace(EntryIterator::new(
                array_ref.cell(self.current_index),
                self.barrier_ref,
            ));
        }
        None
    }
}

impl<'h, 'b, K, V, H> FusedIterator for Visitor<'h, 'b, K, V, H>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
    H: 'static + BuildHasher,
{
}
