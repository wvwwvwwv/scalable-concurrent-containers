//! [`HashIndex`] is a read-optimized concurrent and asynchronous hash map.

use super::ebr::{Arc, AtomicArc, Barrier};
use super::hash_table::bucket::{Bucket, EntryPtr, Locker};
use super::hash_table::bucket_array::BucketArray;
use super::hash_table::HashTable;
use super::wait_queue::{AsyncWait, DeriveAsyncWait};

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::fmt::{self, Debug};
use std::hash::{BuildHasher, Hash};
use std::iter::FusedIterator;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::Acquire;

/// Scalable concurrent hash index.
///
/// [`HashIndex`] is a concurrent and asynchronous hash map data structure that is optimized for
/// read operations. The key characteristics of [`HashIndex`] are similar to that of
/// [`HashMap`](super::HashMap) except that its read operations are lock-free.
///
/// ## The key differences between [`HashIndex`] and [`HashMap`](crate::HashMap).
///
/// * Lock-free-read: read and scan operations do not modify shared data and are never blocked.
/// * Immutability: the data in the container is immutable until it becomes unreachable.
/// * Linearizability: [`HashIndex`] insert/remove/update methods are linearizable.
///
/// ## The key statistics for [`HashIndex`]
///
/// * The expected size of metadata for a single key-value pair: 2-byte.
/// * The expected number of atomic write operations required for an operation on a single key: 2.
/// * The expected number of atomic variables accessed during a single key operation: 2.
/// * The number of entries managed by a single bucket without a linked list: 32.
/// * The expected maximum linked list length when resize is triggered: log(capacity) / 8.
pub struct HashIndex<K, V, H = RandomState>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
    H: BuildHasher,
{
    array: AtomicArc<BucketArray<K, V, true>>,
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
    /// Creates an empty [`HashIndex`] with the given [`BuildHasher`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashindex: HashIndex<u64, u32, RandomState> =
    ///     HashIndex::with_hasher(RandomState::new());
    /// ```
    #[inline]
    pub fn with_hasher(build_hasher: H) -> HashIndex<K, V, H> {
        HashIndex {
            array: AtomicArc::null(),
            minimum_capacity: 0,
            resize_mutex: AtomicU8::new(0),
            build_hasher,
        }
    }

    /// Creates an empty [`HashIndex`] with the specified capacity and build hasher.
    ///
    /// The actual capacity is equal to or greater than the specified capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashindex: HashIndex<u64, u32, RandomState> =
    ///     HashIndex::with_capacity_and_hasher(1000, RandomState::new());
    ///
    /// let result = hashindex.capacity();
    /// assert_eq!(result, 1024);
    /// ```
    #[inline]
    pub fn with_capacity_and_hasher(capacity: usize, build_hasher: H) -> HashIndex<K, V, H> {
        HashIndex {
            array: AtomicArc::from(Arc::new(BucketArray::<K, V, true>::new(
                capacity,
                AtomicArc::null(),
            ))),
            minimum_capacity: capacity,
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
        let barrier = Barrier::new();
        let hash = self.hash(key.borrow());
        if let Ok(Some((k, v))) = self.insert_entry(key, val, hash, &mut (), &barrier) {
            Err((k, v))
        } else {
            Ok(())
        }
    }

    /// Inserts a key-value pair into the [`HashIndex`].
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
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
        let hash = self.hash(key.borrow());
        loop {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);
            match self.insert_entry(key, val, hash, &mut async_wait_pinned, &Barrier::new()) {
                Ok(Some(returned)) => return Err(returned),
                Ok(None) => return Ok(()),
                Err(returned) => {
                    key = returned.0;
                    val = returned.1;
                }
            }
            async_wait_pinned.await;
        }
    }

    /// Updates an existing key-value pair.
    ///
    /// It returns `None` if the key does not exist.
    ///
    /// # Safety
    ///
    /// The caller has to make sure that there is no reader of the entry, e.g., a reader keeping a
    /// reference to the entry via [`HashIndex::iter`], [`HashIndex::read`], or
    /// [`HashIndex::read_with`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::default();
    ///
    /// assert!(unsafe { hashindex.update(&1, |_, _| true).is_none() });
    /// assert!(hashindex.insert(1, 0).is_ok());
    /// assert_eq!(unsafe { hashindex.update(&1, |_, v| { *v = 2; *v }).unwrap() }, 2);
    /// assert_eq!(hashindex.read(&1, |_, v| *v).unwrap(), 2);
    /// ```
    #[inline]
    pub unsafe fn update<Q, F, R>(&self, key: &Q, updater: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        F: FnOnce(&K, &mut V) -> R,
    {
        let barrier = Barrier::new();
        let (mut locker, data_block, mut entry_ptr) = self
            .acquire_entry(key, self.hash(key), &mut (), &barrier)
            .ok()?;
        if entry_ptr.is_valid() {
            let (k, v) = entry_ptr.get_mut(data_block, &mut locker);
            return Some(updater(k, v));
        }
        None
    }

    /// Updates an existing key-value pair.
    ///
    /// It returns `None` if the key does not exist. It is an asynchronous method returning an
    /// `impl Future` for the caller to await.
    ///
    /// # Safety
    ///
    /// The caller has to make sure that there is no reader of the entry, e.g., a reader keeping a
    /// reference to the entry via [`HashIndex::iter`], [`HashIndex::read`], or
    /// [`HashIndex::read_with`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::default();
    ///
    /// assert!(hashindex.insert(1, 0).is_ok());
    /// let future_update = unsafe { hashindex.update_async(&1, |_, v| { *v = 2; *v }) };
    /// ```
    #[inline]
    pub async unsafe fn update_async<Q, F, R>(&self, key: &Q, updater: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        F: FnOnce(&K, &mut V) -> R,
    {
        let hash = self.hash(key);
        loop {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);
            if let Ok((mut locker, data_block, mut entry_ptr)) =
                self.acquire_entry(key, hash, &mut async_wait_pinned, &Barrier::new())
            {
                if entry_ptr.is_valid() {
                    let (k, v) = entry_ptr.get_mut(data_block, &mut locker);
                    return Some(updater(k, v));
                }
                return None;
            }
            async_wait_pinned.await;
        }
    }

    /// Removes a key-value pair if the key exists.
    ///
    /// It returns `false` if the key does not exist. This method only marks the entry unreachable,
    /// and the memory will be reclaimed later.
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
    pub fn remove<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.remove_if(key, |_| true)
    }

    /// Removes a key-value pair if the key exists.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
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
    pub async fn remove_async<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.remove_if_async(key, |_| true).await
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
    pub fn remove_if<Q, F: FnOnce(&V) -> bool>(&self, key: &Q, condition: F) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.remove_entry(
            key,
            self.hash(key),
            condition,
            |r| r.is_some(),
            &mut (),
            &Barrier::new(),
        )
        .ok()
        .map_or(false, |r| r)
    }

    /// Removes a key-value pair if the key exists and the given condition is met.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
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
    pub async fn remove_if_async<Q, F: FnOnce(&V) -> bool>(&self, key: &Q, mut condition: F) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hash(key);
        loop {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);
            match self.remove_entry(
                key,
                hash,
                condition,
                |r| r.is_some(),
                &mut async_wait_pinned,
                &Barrier::new(),
            ) {
                Ok(r) => return r,
                Err(c) => condition = c,
            };
            async_wait_pinned.await;
        }
    }

    /// Reads a key-value pair.
    ///
    /// It returns `None` if the key does not exist. This method is not linearizable; the key-value
    /// pair being read by this method can be removed from the container or copied to a different
    /// memory location.
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
    pub fn read<Q, R, F: FnOnce(&K, &V) -> R>(&self, key: &Q, reader: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let barrier = Barrier::new();
        self.read_entry(key, self.hash(key), &mut (), &barrier)
            .ok()
            .flatten()
            .map(|(k, v)| reader(k, v))
    }

    /// Reads a key-value pair using the supplied [`Barrier`].
    ///
    /// It enables the caller to use the value reference outside the method. It returns `None`
    /// if the key does not exist. This method is not linearizable; the key-value pair being read
    /// by this method can be removed from the container or copied to a different memory location.
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
    pub fn read_with<'b, Q, R, F: FnOnce(&'b K, &'b V) -> R>(
        &self,
        key: &Q,
        reader: F,
        barrier: &'b Barrier,
    ) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.read_entry(key, self.hash(key), &mut (), barrier)
            .ok()
            .flatten()
            .map(|(k, v)| reader(k, v))
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
        while let Some(current_array) = current_array_ptr.as_ref() {
            while !current_array.old_array(&barrier).is_null() {
                if self.partial_rehash::<_, _, false>(current_array, &mut (), &barrier) == Ok(true)
                {
                    break;
                }
            }
            for index in 0..current_array.num_buckets() {
                let bucket = current_array.bucket_mut(index);
                if let Some(mut locker) = Locker::lock(bucket, &barrier) {
                    let data_block = current_array.data_block(index);
                    let mut entry_ptr = EntryPtr::new(&barrier);
                    while entry_ptr.next(locker.bucket(), &barrier) {
                        locker.erase(data_block, &mut entry_ptr);
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
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
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
        let mut current_array_holder = self.array.get_arc(Acquire, &Barrier::new());
        while let Some(current_array) = current_array_holder.take() {
            while !current_array.old_array(&Barrier::new()).is_null() {
                let mut async_wait = AsyncWait::default();
                let mut async_wait_pinned = Pin::new(&mut async_wait);
                if self.partial_rehash::<_, _, false>(
                    &current_array,
                    &mut async_wait_pinned,
                    &Barrier::new(),
                ) == Ok(true)
                {
                    break;
                }
                async_wait_pinned.await;
            }

            for index in 0..current_array.num_buckets() {
                let killed = loop {
                    let mut async_wait = AsyncWait::default();
                    let mut async_wait_pinned = Pin::new(&mut async_wait);
                    {
                        let barrier = Barrier::new();
                        let bucket = current_array.bucket_mut(index);
                        if let Ok(locker) = Locker::try_lock_or_wait(
                            bucket,
                            unsafe { async_wait_pinned.derive().unwrap_unchecked() },
                            &barrier,
                        ) {
                            if let Some(mut locker) = locker {
                                let data_block = current_array.data_block(index);
                                let mut entry_ptr = EntryPtr::new(&barrier);
                                while entry_ptr.next(locker.bucket(), &barrier) {
                                    locker.erase(data_block, &mut entry_ptr);
                                    num_removed = num_removed.saturating_add(1);
                                }
                                break false;
                            }

                            // The bucket having been killed means that a new array has been
                            // allocated.
                            break true;
                        };
                    }
                    async_wait_pinned.await;
                };
                if killed {
                    break;
                }
            }

            if let Some(new_current_array) = self.array.get_arc(Acquire, &Barrier::new()) {
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
    /// It scans the entire bucket array to calculate the number of valid entries, making its time
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
    /// It may scan the entire bucket array to check if it is empty, therefore the time complexity
    /// is `O(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::default();
    ///
    /// assert!(hashindex.is_empty());
    /// assert!(hashindex.insert(1, 0).is_ok());
    /// assert!(!hashindex.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        !self.has_entry(&Barrier::new())
    }

    /// Returns the capacity of the [`HashIndex`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashindex_default: HashIndex<u64, u32, RandomState> = HashIndex::default();
    /// assert_eq!(hashindex_default.capacity(), 0);
    ///
    /// assert!(hashindex_default.insert(1, 0).is_ok());
    /// assert_eq!(hashindex_default.capacity(), 32);
    ///
    /// let hashindex: HashIndex<u64, u32, RandomState> = HashIndex::with_capacity(1000000);
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
            hashindex: self,
            current_array: None,
            current_index: 0,
            current_bucket: None,
            current_entry_ptr: EntryPtr::new(barrier),
            barrier,
        }
    }
}

impl<K, V, H> Clone for HashIndex<K, V, H>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
    H: BuildHasher + Clone,
{
    #[inline]
    fn clone(&self) -> Self {
        let cloned = Self::with_capacity_and_hasher(self.capacity(), self.hasher().clone());
        for (k, v) in self.iter(&Barrier::new()) {
            let _reuslt = cloned.insert(k.clone(), v.clone());
        }
        cloned
    }
}

impl<K, V, H> Debug for HashIndex<K, V, H>
where
    K: 'static + Clone + Debug + Eq + Hash + Sync,
    V: 'static + Clone + Debug + Sync,
    H: BuildHasher,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let barrier = Barrier::new();
        f.debug_map().entries(self.iter(&barrier)).finish()
    }
}

impl<K, V> HashIndex<K, V, RandomState>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
{
    /// Creates an empty default [`HashIndex`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    ///
    /// let hashindex: HashIndex<u64, u32> = HashIndex::new();
    ///
    /// let result = hashindex.capacity();
    /// assert_eq!(result, 0);
    /// ```
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty [`HashIndex`] with the specified capacity.
    ///
    /// The actual capacity is equal to or greater than the specified capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashIndex;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashindex: HashIndex<u64, u32, RandomState> = HashIndex::with_capacity(1000);
    ///
    /// let result = hashindex.capacity();
    /// assert_eq!(result, 1024);
    /// ```
    #[inline]
    #[must_use]
    pub fn with_capacity(capacity: usize) -> HashIndex<K, V, RandomState> {
        HashIndex {
            array: AtomicArc::from(Arc::new(BucketArray::<K, V, true>::new(
                capacity,
                AtomicArc::null(),
            ))),
            minimum_capacity: capacity,
            resize_mutex: AtomicU8::new(0),
            build_hasher: RandomState::new(),
        }
    }
}

impl<K, V> Default for HashIndex<K, V, RandomState>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
{
    /// Creates an empty default [`HashIndex`].
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
    /// assert_eq!(result, 0);
    /// ```
    #[inline]
    fn default() -> Self {
        HashIndex {
            array: AtomicArc::null(),
            minimum_capacity: 0,
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
    #[inline]
    fn hasher(&self) -> &H {
        &self.build_hasher
    }
    #[inline]
    fn cloner(entry: &(K, V)) -> Option<(K, V)> {
        Some((entry.0.clone(), entry.1.clone()))
    }
    #[inline]
    fn bucket_array(&self) -> &AtomicArc<BucketArray<K, V, true>> {
        &self.array
    }
    #[inline]
    fn minimum_capacity(&self) -> usize {
        self.minimum_capacity
    }
    #[inline]
    fn resize_mutex(&self) -> &AtomicU8 {
        &self.resize_mutex
    }
}

impl<K, V, H> PartialEq for HashIndex<K, V, H>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + PartialEq + Sync,
    H: BuildHasher,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        let barrier = Barrier::new();
        if !self
            .iter(&barrier)
            .any(|(k, v)| other.read(k, |_, ov| v == ov) != Some(true))
        {
            return !other
                .iter(&barrier)
                .any(|(k, v)| self.read(k, |_, sv| v == sv) != Some(true));
        }
        false
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
    hashindex: &'h HashIndex<K, V, H>,
    current_array: Option<&'b BucketArray<K, V, true>>,
    current_index: usize,
    current_bucket: Option<&'b Bucket<K, V, true>>,
    current_entry_ptr: EntryPtr<'b, K, V, true>,
    barrier: &'b Barrier,
}

impl<'h, 'b, K, V, H> Iterator for Visitor<'h, 'b, K, V, H>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
    H: BuildHasher,
{
    type Item = (&'b K, &'b V);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let mut array = if let Some(array) = self.current_array.as_ref().copied() {
            array
        } else {
            // Start scanning.
            let current_array = self
                .hashindex
                .bucket_array()
                .load(Acquire, self.barrier)
                .as_ref()?;
            let old_array_ptr = current_array.old_array(self.barrier);
            let array = if let Some(old_array) = old_array_ptr.as_ref() {
                old_array
            } else {
                current_array
            };
            self.current_array.replace(array);
            self.current_bucket.replace(array.bucket(0));
            self.current_entry_ptr = EntryPtr::new(self.barrier);
            array
        };

        // Go to the next bucket.
        loop {
            if let Some(bucket) = self.current_bucket.take() {
                // Go to the next entry in the bucket.
                if self.current_entry_ptr.next(bucket, self.barrier) {
                    let (k, v) = self
                        .current_entry_ptr
                        .get(array.data_block(self.current_index));
                    self.current_bucket.replace(bucket);
                    return Some((k, v));
                }
            }
            self.current_index += 1;
            if self.current_index == array.num_buckets() {
                let current_array = self
                    .hashindex
                    .bucket_array()
                    .load(Acquire, self.barrier)
                    .as_ref()?;
                if self
                    .current_array
                    .as_ref()
                    .copied()
                    .map_or(false, |a| ptr::eq(a, current_array))
                {
                    // Finished scanning the entire array.
                    break;
                }
                let old_array_ptr = current_array.old_array(self.barrier);
                if self
                    .current_array
                    .as_ref()
                    .copied()
                    .map_or(false, |a| ptr::eq(a, old_array_ptr.as_raw()))
                {
                    // Start scanning the current array.
                    array = current_array;
                    self.current_array.replace(array);
                    self.current_index = 0;
                    self.current_bucket.replace(array.bucket(0));
                    self.current_entry_ptr = EntryPtr::new(self.barrier);
                    continue;
                }

                // Start from the very beginning.
                array = if let Some(old_array) = old_array_ptr.as_ref() {
                    old_array
                } else {
                    current_array
                };
                self.current_array.replace(array);
                self.current_index = 0;
                self.current_bucket.replace(array.bucket(0));
                self.current_entry_ptr = EntryPtr::new(self.barrier);
                continue;
            }
            self.current_bucket
                .replace(array.bucket(self.current_index));
            self.current_entry_ptr = EntryPtr::new(self.barrier);
        }
        None
    }
}

impl<'h, 'b, K, V, H> FusedIterator for Visitor<'h, 'b, K, V, H>
where
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
    H: BuildHasher,
{
}
