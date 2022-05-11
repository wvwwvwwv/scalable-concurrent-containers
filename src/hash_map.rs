//! The module implements [`HashMap`].

use super::ebr::{Arc, AtomicArc, Barrier};
use super::hash_table::cell::{Locker, Reader};
use super::hash_table::cell_array::CellArray;
use super::hash_table::HashTable;
use super::wait_queue::AsyncWait;

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::ptr::addr_of_mut;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::atomic::{AtomicU8, AtomicUsize};

/// Scalable concurrent hash map.
///
/// [`HashMap`] is a concurrent hash map data structure that is targeted at a highly concurrent
/// workload. The use of an epoch-based reclamation technique enables the data structure to
/// implement non-blocking resizing and fine-granular locking. A [`HashMap`] instance only has a
/// single array of entries instead of a fixed number of lock-protected hash tables. An entry of
/// the array is called a `Cell`; it manages a fixed number of key-value pairs using a customized
/// mutex in it, and resolves hash conflicts by allocating a linked list of smaller hash tables.
///
/// ## The key features of [`HashMap`]
///
/// * Non-sharded: the data is stored in a single array of key-value pairs.
/// * Non-blocking resizing: resizing does not block other threads or tasks.
/// * Automatic resizing: it automatically grows or shrinks.
/// * Incremental resizing: each access to the data structure is mandated to rehash a fixed
///   number of key-value pairs.
/// * Optimized resizing: key-value pairs managed by a single `Cell` are guaranteed to be
///   relocated to consecutive `Cell` instances.
/// * No busy waiting: the customized mutex never spins.
/// * Linearlizability: [`HashMap`] methods are linearlizable.
///
/// ## The key statistics for [`HashMap`]
///
/// * The expected size of metadata for a single key-value pair: 2-byte.
/// * The expected number of atomic write operations required for an operation on a single key: 2.
/// * The expected number of atomic variables accessed during a single key operation: 2.
/// * The number of entries managed by a single metadata `Cell` without a linked list: 32.
/// * The expected maximum linked list length when resize is triggered: log(capacity) / 8.
pub struct HashMap<K, V, H = RandomState>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    array: AtomicArc<CellArray<K, V, false>>,
    minimum_capacity: usize,
    additional_capacity: AtomicUsize,
    resize_mutex: AtomicU8,
    build_hasher: H,
}

impl<K, V, H> HashMap<K, V, H>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    /// Creates an empty [`HashMap`] with the given capacity and [`BuildHasher`].
    ///
    /// The actual capacity is equal to or greater than the given capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashmap: HashMap<u64, u32, RandomState> = HashMap::new(1000, RandomState::new());
    ///
    /// let result = hashmap.capacity();
    /// assert_eq!(result, 1024);
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let result = hashmap.capacity();
    /// assert_eq!(result, 64);
    /// ```
    #[inline]
    pub fn new(capacity: usize, build_hasher: H) -> HashMap<K, V, H> {
        let initial_capacity = capacity.max(Self::default_capacity());
        let array = Arc::new(CellArray::<K, V, false>::new(
            initial_capacity,
            AtomicArc::null(),
        ));
        let current_capacity = array.num_entries();
        HashMap {
            array: AtomicArc::from(array),
            minimum_capacity: current_capacity,
            additional_capacity: AtomicUsize::new(0),
            resize_mutex: AtomicU8::new(0),
            build_hasher,
        }
    }

    /// Temporarily increases the minimum capacity of the [`HashMap`].
    ///
    /// The reserved space is not exclusively owned by the [`Ticket`], thus can be overtaken.
    /// Unused space is immediately reclaimed when the [`Ticket`] is dropped.
    ///
    /// # Errors
    ///
    /// Returns `None` if a too large number is given.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashmap: HashMap<usize, usize, RandomState> = HashMap::new(1000, RandomState::new());
    /// assert_eq!(hashmap.capacity(), 1024);
    ///
    /// let ticket = hashmap.reserve(10000);
    /// assert!(ticket.is_some());
    /// assert_eq!(hashmap.capacity(), 16384);
    /// for i in 0..16 {
    ///     assert!(hashmap.insert(i, i).is_ok());
    /// }
    /// drop(ticket);
    ///
    /// assert_eq!(hashmap.capacity(), 1024);
    /// ```
    #[inline]
    pub fn reserve(&self, capacity: usize) -> Option<Ticket<K, V, H>> {
        let mut current_additional_capacity = self.additional_capacity.load(Relaxed);
        loop {
            if usize::MAX - self.minimum_capacity - current_additional_capacity <= capacity {
                // The given value is too large.
                return None;
            }
            match self.additional_capacity.compare_exchange(
                current_additional_capacity,
                current_additional_capacity + capacity,
                Relaxed,
                Relaxed,
            ) {
                Ok(_) => {
                    self.resize(&Barrier::new());
                    return Some(Ticket {
                        hash_map: self,
                        increment: capacity,
                    });
                }
                Err(current) => current_additional_capacity = current,
            }
        }
    }

    /// Inserts a key-value pair into the [`HashMap`].
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
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert_eq!(hashmap.insert(1, 1).unwrap_err(), (1, 1));
    /// ```
    #[inline]
    pub fn insert(&self, key: K, val: V) -> Result<(), (K, V)> {
        let (hash, partial_hash) = self.hash(&key);
        if let Some((k, v)) = self
            .insert_entry(key, val, hash, partial_hash, None, &Barrier::new())
            .ok()
            .unwrap()
        {
            Err((k, v))
        } else {
            Ok(())
        }
    }

    /// Inserts a key-value pair into the [`HashMap`].
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await or poll.
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied key-value pair if the key exists.
    ///
    /// # Panics
    ///
    /// Panics if the number of entries in the target `Cell` reaches `u32::MAX` due to a poor hash
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert_async(11, 17);
    /// ```
    #[inline]
    pub async fn insert_async(&self, mut key: K, mut val: V) -> Result<(), (K, V)> {
        let (hash, partial_hash) = self.hash(&key);
        loop {
            let mut async_wait = AsyncWait::default();
            match self.insert_entry(
                key,
                val,
                hash,
                partial_hash,
                Some(addr_of_mut!(async_wait)),
                &Barrier::new(),
            ) {
                Ok(Some(returned)) => return Err(returned),
                Ok(None) => return Ok(()),
                Err(returned) => {
                    key = returned.0;
                    val = returned.1;
                }
            }
            async_wait.await;
        }
    }

    /// Updates an existing key-value pair.
    ///
    /// It returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.update(&1, |_, _| true).is_none());
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert_eq!(hashmap.update(&1, |_, v| { *v = 2; *v }).unwrap(), 2);
    /// assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
    /// ```
    #[inline]
    pub fn update<Q, F, R>(&self, key_ref: &Q, updater: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        F: FnOnce(&K, &mut V) -> R,
    {
        let (hash, partial_hash) = self.hash(key_ref);
        let barrier = Barrier::new();
        let (_, _locker, iterator) = self
            .acquire::<Q>(key_ref, hash, partial_hash, None, &barrier)
            .ok()?;
        if let Some(iterator) = iterator {
            if let Some((k, v)) = iterator.get() {
                // The presence of `locker` prevents the entry from being modified outside it.
                #[allow(clippy::cast_ref_to_mut)]
                return Some(updater(k, unsafe { &mut *(v as *const V as *mut V) }));
            }
        }
        None
    }

    /// Updates an existing key-value pair.
    ///
    /// It returns `None` if the key does not exist. It is an asynchronous method returning an
    /// `impl Future` for the caller to await or poll.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// let future_update = hashmap.update_async(&1, |_, v| { *v = 2; *v });
    /// ```
    #[inline]
    pub async fn update_async<Q, F, R>(&self, key_ref: &Q, updater: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        F: FnOnce(&K, &mut V) -> R,
    {
        let (hash, partial_hash) = self.hash(key_ref);
        loop {
            let mut async_wait = AsyncWait::default();
            if let Ok((_, _locker, iterator)) = self.acquire::<Q>(
                key_ref,
                hash,
                partial_hash,
                Some(addr_of_mut!(async_wait)),
                &Barrier::new(),
            ) {
                if let Some(iterator) = iterator {
                    if let Some((k, v)) = iterator.get() {
                        #[allow(clippy::cast_ref_to_mut)]
                        return Some(updater(k, unsafe { &mut *(v as *const V as *mut V) }));
                    }
                }
                return None;
            }
            async_wait.await;
        }
    }

    /// Constructs the value in-place, or modifies an existing value corresponding to the key.
    ///
    /// # Panics
    ///
    /// Panics if the number of entries in the target `Cell` reaches `u32::MAX` due to a poor hash
    /// function.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// hashmap.upsert(1, || 2, |_, v| *v = 2);
    /// assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
    /// hashmap.upsert(1, || 2, |_, v| *v = 3);
    /// assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 3);
    /// ```
    #[inline]
    pub fn upsert<FI: FnOnce() -> V, FU: FnOnce(&K, &mut V)>(
        &self,
        key: K,
        constructor: FI,
        updater: FU,
    ) {
        let (hash, partial_hash) = self.hash(&key);
        let barrier = Barrier::new();
        let (_, locker, iterator) = self
            .acquire::<_>(&key, hash, partial_hash, None, &barrier)
            .ok()
            .unwrap();
        if let Some(iterator) = iterator {
            if let Some((k, v)) = iterator.get() {
                // The presence of `locker` prevents the entry from being modified outside it.
                #[allow(clippy::cast_ref_to_mut)]
                updater(k, unsafe { &mut *(v as *const V as *mut V) });
                return;
            }
        }
        locker.insert(key, constructor(), partial_hash, &barrier);
    }

    /// Constructs the value in-place, or modifies an existing value corresponding to the key.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await or poll.
    ///
    /// # Panics
    ///
    /// Panics if the number of entries in the target `Cell` reaches `u32::MAX` due to a poor hash
    /// function.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// let future_upsert = hashmap.upsert_async(1, || 2, |_, v| *v = 3);
    /// ```
    #[inline]
    pub async fn upsert_async<FI: FnOnce() -> V, FU: FnOnce(&K, &mut V)>(
        &self,
        key: K,
        constructor: FI,
        updater: FU,
    ) {
        let (hash, partial_hash) = self.hash(&key);
        loop {
            let mut async_wait = AsyncWait::default();
            if let Ok((_, locker, iterator)) = self.acquire::<_>(
                &key,
                hash,
                partial_hash,
                Some(addr_of_mut!(async_wait)),
                &Barrier::new(),
            ) {
                if let Some(iterator) = iterator {
                    if let Some((k, v)) = iterator.get() {
                        #[allow(clippy::cast_ref_to_mut)]
                        updater(k, unsafe { &mut *(v as *const V as *mut V) });
                        return;
                    }
                }
                locker.insert(key, constructor(), partial_hash, &Barrier::new());
                return;
            }
            async_wait.await;
        }
    }

    /// Removes a key-value pair if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.remove(&1).is_none());
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert_eq!(hashmap.remove(&1).unwrap(), (1, 0));
    /// ```
    #[inline]
    pub fn remove<Q>(&self, key_ref: &Q) -> Option<(K, V)>
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
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert_async(11, 17);
    /// let future_remove = hashmap.remove_async(&11);
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
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.remove_if(&1, |v| *v == 1).is_none());
    /// assert_eq!(hashmap.remove_if(&1, |v| *v == 0).unwrap(), (1, 0));
    /// ```
    #[inline]
    pub fn remove_if<Q, F: FnMut(&V) -> bool>(
        &self,
        key_ref: &Q,
        mut condition: F,
    ) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (hash, partial_hash) = self.hash(key_ref);
        self.remove_entry::<Q, _>(
            key_ref,
            hash,
            partial_hash,
            &mut condition,
            None,
            &Barrier::new(),
        )
        .ok()
        .and_then(|(r, _)| r)
    }

    /// Removes a key-value pair if the key exists and the given condition is met.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await or poll.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert_async(11, 17);
    /// let future_remove = hashmap.remove_if_async(&11, |_| true);
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
            let mut async_wait = AsyncWait::default();
            if let Ok(result) = self.remove_entry::<Q, F>(
                key_ref,
                hash,
                partial_hash,
                &mut condition,
                Some(addr_of_mut!(async_wait)),
                &Barrier::new(),
            ) {
                return result.0;
            }
            async_wait.await;
        }
    }

    /// Reads a key-value pair.
    ///
    /// It returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.read(&1, |_, v| *v).is_none());
    /// assert!(hashmap.insert(1, 10).is_ok());
    /// assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 10);
    /// ```
    #[inline]
    pub fn read<Q, R, F: FnMut(&K, &V) -> R>(&self, key_ref: &Q, mut reader: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let barrier = Barrier::new();
        let (hash, partial_hash) = self.hash(key_ref);
        self.read_entry::<Q, R, F>(key_ref, hash, partial_hash, &mut reader, None, &barrier)
            .ok()
            .and_then(|r| r)
    }

    /// Reads a key-value pair.
    ///
    /// It returns `None` if the key does not exist. It is an asynchronous method returning an
    /// `impl Future` for the caller to await or poll.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert_async(11, 17);
    /// let future_read = hashmap.read_async(&11, |_, v| *v);
    /// ```
    #[inline]
    pub async fn read_async<Q, R, F: FnMut(&K, &V) -> R>(
        &self,
        key_ref: &Q,
        mut reader: F,
    ) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (hash, partial_hash) = self.hash(key_ref);
        loop {
            let mut async_wait = AsyncWait::default();
            if let Ok(result) = self.read_entry::<Q, R, _>(
                key_ref,
                hash,
                partial_hash,
                &mut reader,
                Some(addr_of_mut!(async_wait)),
                &Barrier::new(),
            ) {
                return result;
            }
            async_wait.await;
        }
    }

    /// Checks if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(!hashmap.contains(&1));
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.contains(&1));
    /// ```
    #[inline]
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.read(key, |_, _| ()).is_some()
    }

    /// Checks if the key exists.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await or poll.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// let future_contains = hashmap.contains_async(&1);
    /// ```
    #[inline]
    pub async fn contains_async<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.read_async(key, |_, _| ()).await.is_some()
    }

    /// Scans all the key-value pairs.
    ///
    /// Key-value pairs that have existed since the invocation of the method are guaranteed to be
    /// visited if they are not removed, however the same key-value pair can be visited more than
    /// once if the [`HashMap`] gets resized by another thread.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<usize, usize> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.insert(2, 1).is_ok());
    ///
    /// let mut sum = 0;
    /// hashmap.scan(|k, v| { sum += *k + *v; });
    /// assert_eq!(sum, 4);
    /// ```
    pub fn scan<F: FnMut(&K, &V)>(&self, mut scanner: F) {
        let barrier = Barrier::new();

        // An acquire fence is required to correctly load the contents of the array.
        let mut current_array_ptr = self.array.load(Acquire, &barrier);
        while let Some(current_array_ref) = current_array_ptr.as_ref() {
            while !current_array_ref.old_array(&barrier).is_null() {
                if current_array_ref.partial_rehash::<_, _, _>(
                    |key| self.hash(key),
                    |_, _| None,
                    None,
                    &barrier,
                ) == Ok(true)
                {
                    break;
                }
            }

            for cell_index in 0..current_array_ref.num_cells() {
                if let Some(locker) = Reader::lock(current_array_ref.cell(cell_index), &barrier) {
                    locker
                        .cell()
                        .iter(&barrier)
                        .for_each(|((k, v), _)| scanner(k, v));
                }
            }

            let new_current_array_ptr = self.array.load(Acquire, &barrier);
            if current_array_ptr == new_current_array_ptr {
                break;
            }
            current_array_ptr = new_current_array_ptr;
        }
    }

    /// Scans all the key-value pairs.
    ///
    /// Key-value pairs that have existed since the invocation of the method are guaranteed to be
    /// visited if they are not removed, however the same key-value pair can be visited more than
    /// once if the [`HashMap`] gets resized by another task.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<usize, usize> = HashMap::default();
    ///
    /// let future_insert = hashmap.insert_async(1, 0);
    /// let future_retain = hashmap.scan_async(|k, v| println!("{k} {v}"));
    /// ```
    pub async fn scan_async<F: FnMut(&K, &V)>(&self, mut scanner: F) {
        // An acquire fence is required to correctly load the contents of the array.
        let mut current_array_holder = self.array.get_arc(Acquire, &Barrier::new());
        while let Some(current_array) = current_array_holder.take() {
            while !current_array.old_array(&Barrier::new()).is_null() {
                let mut async_wait = AsyncWait::default();
                if current_array.partial_rehash::<_, _, _>(
                    |key| self.hash(key),
                    |_, _| None,
                    Some(addr_of_mut!(async_wait)),
                    &Barrier::new(),
                ) == Ok(true)
                {
                    break;
                }
                async_wait.await;
            }

            for cell_index in 0..current_array.num_cells() {
                let killed = loop {
                    let mut async_wait = AsyncWait::default();
                    {
                        let barrier = Barrier::new();
                        if let Ok(result) = Reader::try_lock_or_wait(
                            current_array.cell(cell_index),
                            addr_of_mut!(async_wait),
                            &barrier,
                        ) {
                            if let Some(locker) = result {
                                let mut iterator = locker.cell().iter(&barrier);
                                while iterator.next().is_some() {
                                    if let Some((k, v)) = iterator.get() {
                                        #[allow(clippy::cast_ref_to_mut)]
                                        scanner(k, v);
                                    }
                                }
                                break false;
                            }

                            // The `Cell` having been killed means that a new array has been
                            // allocated.
                            break true;
                        };
                    }
                    async_wait.await;
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
    }

    /// Iterates over all the entries in the [`HashMap`].
    ///
    /// Key-value pairs that have existed since the invocation of the method are guaranteed to be
    /// visited if they are not removed, however the same key-value pair can be visited more than
    /// once if the [`HashMap`] gets resized by another thread.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.insert(2, 1).is_ok());
    ///
    /// let mut acc = 0;
    /// hashmap.for_each(|k, v| { acc += *k; *v = 2; });
    /// assert_eq!(acc, 3);
    /// assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
    /// assert_eq!(hashmap.read(&2, |_, v| *v).unwrap(), 2);
    /// ```
    #[inline]
    pub fn for_each<F: FnMut(&K, &mut V)>(&self, mut f: F) {
        self.retain(|k, v| {
            f(k, v);
            true
        });
    }

    /// Iterates over all the entries in the [`HashMap`].
    ///
    /// Key-value pairs that have existed since the invocation of the method are guaranteed to be
    /// visited if they are not removed, however the same key-value pair can be visited more than
    /// once if the [`HashMap`] gets resized by another task.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await or poll.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// let future_insert = hashmap.insert_async(1, 0);
    /// let future_for_each = hashmap.for_each_async(|k, v| println!("{} {}", k, v));
    /// ```
    #[inline]
    pub async fn for_each_async<F: FnMut(&K, &mut V)>(&self, mut f: F) {
        self.retain_async(|k, v| {
            f(k, v);
            true
        })
        .await;
    }

    /// Retains key-value pairs that satisfy the given predicate.
    ///
    /// Key-value pairs that have existed since the invocation of the method are guaranteed to be
    /// visited if they are not removed, however the same key-value pair can be visited more than
    /// once if the [`HashMap`] gets resized by another thread.
    ///
    /// It returns the number of entries remaining and removed.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert!(hashmap.insert(2, 1).is_ok());
    /// assert!(hashmap.insert(3, 2).is_ok());
    ///
    /// assert_eq!(hashmap.retain(|k, v| *k == 1 && *v == 0), (1, 2));
    /// ```
    pub fn retain<F: FnMut(&K, &mut V) -> bool>(&self, mut filter: F) -> (usize, usize) {
        let mut num_retained: usize = 0;
        let mut num_removed: usize = 0;

        let barrier = Barrier::new();

        // An acquire fence is required to correctly load the contents of the array.
        let mut current_array_ptr = self.array.load(Acquire, &barrier);
        while let Some(current_array_ref) = current_array_ptr.as_ref() {
            while !current_array_ref.old_array(&barrier).is_null() {
                if current_array_ref.partial_rehash::<_, _, _>(
                    |key| self.hash(key),
                    |_, _| None,
                    None,
                    &barrier,
                ) == Ok(true)
                {
                    break;
                }
            }

            for cell_index in 0..current_array_ref.num_cells() {
                if let Some(locker) = Locker::lock(current_array_ref.cell(cell_index), &barrier) {
                    let mut iterator = locker.cell().iter(&barrier);
                    while iterator.next().is_some() {
                        let retain = if let Some((k, v)) = iterator.get() {
                            #[allow(clippy::cast_ref_to_mut)]
                            filter(k, unsafe { &mut *(v as *const V as *mut V) })
                        } else {
                            true
                        };
                        if retain {
                            num_retained = num_retained.saturating_add(1);
                        } else {
                            locker.erase(&mut iterator);
                            num_removed = num_removed.saturating_add(1);
                        }
                    }
                }
            }

            let new_current_array_ptr = self.array.load(Acquire, &barrier);
            if current_array_ptr == new_current_array_ptr {
                break;
            }
            num_retained = 0;
            current_array_ptr = new_current_array_ptr;
        }

        if num_removed >= num_retained {
            self.resize(&barrier);
        }

        (num_retained, num_removed)
    }

    /// Retains key-value pairs that satisfy the given predicate.
    ///
    /// Key-value pairs that have existed since the invocation of the method are guaranteed to be
    /// visited if they are not removed, however the same key-value pair can be visited more than
    /// once if the [`HashMap`] gets resized by another task.
    ///
    /// It returns the number of entries remaining and removed. It is an asynchronous method
    /// returning an `impl Future` for the caller to await or poll.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// let future_insert = hashmap.insert_async(1, 0);
    /// let future_retain = hashmap.retain_async(|k, v| *k == 1);
    /// ```
    pub async fn retain_async<F: FnMut(&K, &mut V) -> bool>(
        &self,
        mut filter: F,
    ) -> (usize, usize) {
        let mut num_retained: usize = 0;
        let mut num_removed: usize = 0;

        // An acquire fence is required to correctly load the contents of the array.
        let mut current_array_holder = self.array.get_arc(Acquire, &Barrier::new());
        while let Some(current_array) = current_array_holder.take() {
            while !current_array.old_array(&Barrier::new()).is_null() {
                let mut async_wait = AsyncWait::default();
                if current_array.partial_rehash::<_, _, _>(
                    |key| self.hash(key),
                    |_, _| None,
                    Some(addr_of_mut!(async_wait)),
                    &Barrier::new(),
                ) == Ok(true)
                {
                    break;
                }
                async_wait.await;
            }

            for cell_index in 0..current_array.num_cells() {
                let killed = loop {
                    let mut async_wait = AsyncWait::default();
                    {
                        let barrier = Barrier::new();
                        if let Ok(result) = Locker::try_lock_or_wait(
                            current_array.cell(cell_index),
                            addr_of_mut!(async_wait),
                            &barrier,
                        ) {
                            if let Some(locker) = result {
                                let mut iterator = locker.cell().iter(&barrier);
                                while iterator.next().is_some() {
                                    let retain = if let Some((k, v)) = iterator.get() {
                                        #[allow(clippy::cast_ref_to_mut)]
                                        filter(k, unsafe { &mut *(v as *const V as *mut V) })
                                    } else {
                                        true
                                    };
                                    if retain {
                                        num_retained = num_retained.saturating_add(1);
                                    } else {
                                        locker.erase(&mut iterator);
                                        num_removed = num_removed.saturating_add(1);
                                    }
                                }
                                break false;
                            }

                            // The `Cell` having been killed means that a new array has been
                            // allocated.
                            break true;
                        };
                    }
                    async_wait.await;
                };
                if killed {
                    break;
                }
            }

            if let Some(new_current_array) = self.array.get_arc(Acquire, &Barrier::new()) {
                if new_current_array.as_ptr() == current_array.as_ptr() {
                    break;
                }
                num_retained = 0;
                current_array_holder.replace(new_current_array);
                continue;
            }
            break;
        }

        if num_removed >= num_retained {
            self.resize(&Barrier::new());
        }

        (num_retained, num_removed)
    }

    /// Clears all the key-value pairs.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert_eq!(hashmap.clear(), 1);
    /// ```
    #[inline]
    pub fn clear(&self) -> usize {
        self.retain(|_, _| false).1
    }

    /// Clears all the key-value pairs.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await or poll.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// let future_insert = hashmap.insert_async(1, 0);
    /// let future_clear = hashmap.clear_async();
    /// ```
    #[inline]
    pub async fn clear_async(&self) -> usize {
        self.retain_async(|_, _| false).await.1
    }

    /// Returns the number of entries in the [`HashMap`].
    ///
    /// It scans the entire array to calculate the number of valid entries, making its time
    /// complexity `O(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.insert(1, 0).is_ok());
    /// assert_eq!(hashmap.len(), 1);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        self.num_entries(&Barrier::new())
    }

    /// Returns `true` if the [`HashMap`] is empty.
    ///
    /// It scans the entire array to calculate the number of valid entries, making its time
    /// complexity `O(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert!(hashmap.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the capacity of the [`HashMap`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashmap: HashMap<u64, u32, RandomState> = HashMap::new(1000000, RandomState::new());
    /// assert_eq!(hashmap.capacity(), 1048576);
    /// ```
    #[inline]
    pub fn capacity(&self) -> usize {
        self.num_slots(&Barrier::new())
    }
}

impl<K, V> Default for HashMap<K, V, RandomState>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
{
    /// Creates a [`HashMap`] with the default parameters.
    ///
    /// The default hash builder is [`RandomState`], and the default capacity is `64`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// let result = hashmap.capacity();
    /// assert_eq!(result, 64);
    /// ```
    #[inline]
    fn default() -> Self {
        HashMap {
            array: AtomicArc::new(CellArray::<K, V, false>::new(
                Self::default_capacity(),
                AtomicArc::null(),
            )),
            minimum_capacity: Self::default_capacity(),
            additional_capacity: AtomicUsize::new(0),
            resize_mutex: AtomicU8::new(0),
            build_hasher: RandomState::new(),
        }
    }
}

impl<K, V, H> HashTable<K, V, H, false> for HashMap<K, V, H>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    fn hasher(&self) -> &H {
        &self.build_hasher
    }
    fn copier(_: &K, _: &V) -> Option<(K, V)> {
        None
    }
    fn cell_array(&self) -> &AtomicArc<CellArray<K, V, false>> {
        &self.array
    }
    fn minimum_capacity(&self) -> usize {
        self.minimum_capacity + self.additional_capacity.load(Relaxed)
    }
    fn resize_mutex(&self) -> &AtomicU8 {
        &self.resize_mutex
    }
}

/// [`Ticket`] keeps the increased minimum capacity of the [`HashMap`] during its lifetime.
///
/// The minimum capacity is lowered when the [`Ticket`] is dropped, thereby allowing unused
/// memory to be reclaimed.
pub struct Ticket<'h, K, V, H>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    hash_map: &'h HashMap<K, V, H>,
    increment: usize,
}

impl<'h, K, V, H> Drop for Ticket<'h, K, V, H>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    fn drop(&mut self) {
        let result = self
            .hash_map
            .additional_capacity
            .fetch_sub(self.increment, Relaxed);
        self.hash_map.resize(&Barrier::new());
        debug_assert!(result >= self.increment);
    }
}
