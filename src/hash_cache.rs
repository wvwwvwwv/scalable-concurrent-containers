//! [`HashCache`] is a concurrent and asynchronous pseudo-LRU cache backed by
//! [`HashMap`](super::HashMap).

use super::ebr::{Arc, AtomicArc, Barrier};
use super::hash_table::bucket::{DataBlock, EntryPtr, Evictable, Locker, BUCKET_LEN, CACHE};
use super::hash_table::bucket_array::BucketArray;
use super::hash_table::HashTable;
use super::wait_queue::AsyncWait;
use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::fmt::{self, Debug};
use std::hash::{BuildHasher, Hash};
use std::mem::replace;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Acquire;

/// Scalable concurrent pseudo-LRU cache based on [`HashMap`](super::HashMap).
///
/// ### Unwind safety
///
/// [`HashCache`] is impervious to out-of-memory errors and panics in user specified code on one
/// condition; `H::Hasher::hash`, `K::drop` and `V::drop` must not panic.
pub struct HashCache<K, V, H = RandomState>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    array: AtomicArc<BucketArray<K, Evictable<V>, CACHE>>,
    minimum_capacity: AtomicUsize,
    maximum_capacity: usize,
    build_hasher: H,
}

/// The default maximum capacity of a [`HashCache`] is `256`.
pub const DEFAULT_MAXIMUM_CAPACITY: usize = 256;

/// [`Entry`] represents a single cache entry in a [`HashCache`].
pub enum Entry<'h, K, V, H = RandomState>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    /// An occupied entry.
    Occupied(OccupiedEntry<'h, K, V, H>),

    /// A vacant entry.
    Vacant(VacantEntry<'h, K, V, H>),
}

/// [`OccupiedEntry`] is a view into an occupied cache entry in a [`HashCache`].
pub struct OccupiedEntry<'h, K, V, H = RandomState>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    hashcache: &'h HashCache<K, V, H>,
    index: usize,
    data_block_mut: &'h mut DataBlock<K, Evictable<V>, BUCKET_LEN>,
    locker: Locker<'h, K, Evictable<V>, CACHE>,
    entry_ptr: EntryPtr<'h, K, Evictable<V>, CACHE>,
}

/// [`VacantEntry`] is a view into a vacant cache entry in a [`HashCache`].
pub struct VacantEntry<'h, K, V, H = RandomState>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    hashcache: &'h HashCache<K, V, H>,
    key: K,
    hash: u64,
    index: usize,
    data_block_mut: &'h mut DataBlock<K, Evictable<V>, BUCKET_LEN>,
    locker: Locker<'h, K, Evictable<V>, CACHE>,
}

impl<K, V, H> HashCache<K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    /// Creates an empty [`HashCache`] with the given [`BuildHasher`].
    ///
    /// The minimum capacity is set to `0`, and the maximum capacity is set to
    /// [`DEFAULT_MAXIMUM_CAPACITY`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashcache: HashCache<u64, u32, RandomState> = HashCache::with_hasher(RandomState::new());
    /// ```
    #[inline]
    pub fn with_hasher(build_hasher: H) -> Self {
        HashCache {
            array: AtomicArc::null(),
            minimum_capacity: AtomicUsize::new(0),
            maximum_capacity: DEFAULT_MAXIMUM_CAPACITY,
            build_hasher,
        }
    }

    /// Creates an empty [`HashCache`] with the specified capacity and [`BuildHasher`].
    ///
    /// The actual capacity is equal to or greater than the specified capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashcache: HashCache<u64, u32, RandomState> =
    ///     HashCache::with_capacity_and_hasher(1000, 2000, RandomState::new());
    ///
    /// let result = hashcache.capacity();
    /// assert_eq!(result, 1024);
    /// ```
    #[inline]
    pub fn with_capacity_and_hasher(
        minimum_capacity: usize,
        maximum_capacity: usize,
        build_hasher: H,
    ) -> Self {
        let array = if minimum_capacity == 0 {
            AtomicArc::null()
        } else {
            AtomicArc::from(unsafe {
                Arc::new_unchecked(BucketArray::<K, Evictable<V>, CACHE>::new(
                    minimum_capacity,
                    AtomicArc::null(),
                ))
            })
        };
        let maximum_capacity = maximum_capacity
            .min(1_usize << (usize::BITS - 1))
            .next_power_of_two();
        HashCache {
            array,
            minimum_capacity: AtomicUsize::new(minimum_capacity),
            maximum_capacity,
            build_hasher,
        }
    }

    /// Gets the entry associated with the given key in the map for in-place manipulation.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<char, u32> = HashCache::default();
    ///
    /// for ch in "a short treatise on fungi".chars() {
    ///     hashcache.entry(ch).and_modify(|counter| *counter += 1).or_insert(1);
    /// }
    ///
    /// assert_eq!(hashcache.read(&'s', |_, v| *v), Some(2));
    /// assert_eq!(hashcache.read(&'t', |_, v| *v), Some(3));
    /// assert!(hashcache.read(&'y', |_, v| *v).is_none());
    /// ```
    #[inline]
    pub fn entry(&self, key: K) -> Entry<K, V, H> {
        let barrier = Barrier::new();
        let hash = self.hash(key.borrow());
        let (locker, data_block_mut, entry_ptr, index) = unsafe {
            self.acquire_entry(
                key.borrow(),
                hash,
                &mut (),
                self.prolonged_barrier_ref(&barrier),
            )
            .ok()
            .unwrap_unchecked()
        };
        if entry_ptr.is_valid() {
            Entry::Occupied(OccupiedEntry {
                hashcache: self,
                index,
                data_block_mut,
                locker,
                entry_ptr,
            })
        } else {
            Entry::Vacant(VacantEntry {
                hashcache: self,
                key,
                hash,
                index,
                data_block_mut,
                locker,
            })
        }
    }

    /// Gets the entry associated with the given key in the map for in-place manipulation.
    ///
    /// It is an asynchronous method returning an `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<char, u32> = HashCache::default();
    ///
    /// let future_entry = hashcache.entry_async('b');
    /// ```
    #[inline]
    pub async fn entry_async(&self, key: K) -> Entry<K, V, H> {
        let hash = self.hash(key.borrow());
        loop {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);
            {
                let barrier = Barrier::new();
                if let Ok((locker, data_block_mut, entry_ptr, index)) = self.acquire_entry(
                    key.borrow(),
                    hash,
                    &mut async_wait_pinned,
                    self.prolonged_barrier_ref(&barrier),
                ) {
                    if entry_ptr.is_valid() {
                        return Entry::Occupied(OccupiedEntry {
                            hashcache: self,
                            index,
                            data_block_mut,
                            locker,
                            entry_ptr,
                        });
                    }
                    return Entry::Vacant(VacantEntry {
                        hashcache: self,
                        key,
                        hash,
                        index,
                        data_block_mut,
                        locker,
                    });
                }
            }
            async_wait_pinned.await;
        }
    }

    /// Puts a key-value pair into the [`HashCache`].
    ///
    /// Returns `Some` if an entry was evicted for the new key-value pair.
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied key-value pair if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// assert!(hashcache.put(1, 0).is_ok());
    /// assert_eq!(hashcache.put(1, 1).unwrap_err(), (1, 1));
    /// ```
    #[inline]
    pub fn put(&self, key: K, val: V) -> Result<Option<(K, V)>, (K, V)> {
        let barrier = Barrier::new();
        let hash = self.hash(key.borrow());
        let result = match self.acquire_entry(&key, hash, &mut (), &barrier) {
            Ok((mut locker, data_block_mut, entry_ptr, _)) => {
                if entry_ptr.is_valid() {
                    return Err((key, val));
                }
                let evicted = locker.evict_lru(data_block_mut).map(|(k, v)| (k, v.take()));
                locker.insert_with(
                    data_block_mut,
                    BucketArray::<K, V, CACHE>::partial_hash(hash),
                    || (key, Evictable::new(val)),
                    &barrier,
                );
                Ok(evicted)
            }
            Err(_) => Err((key, val)),
        };
        result
    }

    /// Puts a key-value pair into the [`HashCache`].
    ///
    /// Returns `Some` if an entry was evicted for the new key-value pair. It is an asynchronous
    /// method returning an `impl Future` for the caller to await.
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied key-value pair if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    /// let future_put = hashcache.put_async(11, 17);
    /// ```
    #[inline]
    pub async fn put_async(&self, key: K, val: V) -> Result<Option<(K, V)>, (K, V)> {
        let hash = self.hash(key.borrow());
        loop {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);
            {
                let barrier = Barrier::new();
                if let Ok((mut locker, data_block_mut, entry_ptr, _)) =
                    self.acquire_entry(&key, hash, &mut async_wait_pinned, &barrier)
                {
                    if entry_ptr.is_valid() {
                        return Err((key, val));
                    }
                    let evicted = locker.evict_lru(data_block_mut).map(|(k, v)| (k, v.take()));
                    locker.insert_with(
                        data_block_mut,
                        BucketArray::<K, V, CACHE>::partial_hash(hash),
                        || (key, Evictable::new(val)),
                        &barrier,
                    );
                    return Ok(evicted);
                };
            }
            async_wait_pinned.await;
        }
    }

    /// Reads a key-value pair.
    ///
    /// It returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// assert!(hashcache.read(&1, |_, v| *v).is_none());
    /// assert!(hashcache.put(1, 10).is_ok());
    /// assert_eq!(hashcache.read(&1, |_, v| *v).unwrap(), 10);
    /// ```
    #[inline]
    pub fn read<Q, R, F: FnOnce(&K, &V) -> R>(&self, key: &Q, reader: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.read_entry(key, self.hash(key), &mut (), &Barrier::new())
            .ok()
            .flatten()
            .map(|(k, v)| reader(k, v))
    }

    /// Reads a key-value pair.
    ///
    /// It returns `None` if the key does not exist. It is an asynchronous method returning an
    /// `impl Future` for the caller to await.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    /// let future_put = hashcache.put_async(11, 17);
    /// let future_read = hashcache.read_async(&11, |_, v| *v);
    /// ```
    #[inline]
    pub async fn read_async<Q, R, F: FnOnce(&K, &V) -> R>(&self, key: &Q, reader: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let hash = self.hash(key);
        loop {
            let mut async_wait = AsyncWait::default();
            let mut async_wait_pinned = Pin::new(&mut async_wait);
            if let Ok(result) = self.read_entry(key, hash, &mut async_wait_pinned, &Barrier::new())
            {
                return result.map(|(k, v)| reader(k, v));
            }
            async_wait_pinned.await;
        }
    }

    /// Returns the capacity of the [`HashCache`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashcache_default: HashCache<u64, u32, RandomState> = HashCache::default();
    /// assert_eq!(hashcache_default.capacity(), 0);
    ///
    /// let hashcache: HashCache<u64, u32, RandomState> = HashCache::with_capacity(1000000, 2000000);
    /// assert_eq!(hashcache.capacity(), 1048576);
    /// ```
    #[inline]
    pub fn capacity(&self) -> usize {
        self.num_slots(&Barrier::new())
    }

    /// Returns a reference to the specified [`Barrier`] whose lifetime matches that of `self`.
    fn prolonged_barrier_ref<'h>(&'h self, barrier: &Barrier) -> &'h Barrier {
        let _: &HashCache<_, _, _> = self;
        unsafe { std::mem::transmute::<&Barrier, &'h Barrier>(barrier) }
    }
}

impl<K, V> HashCache<K, V, RandomState>
where
    K: Eq + Hash,
{
    /// Creates an empty default [`HashCache`].
    ///
    /// The maximum capacity is set to [`DEFAULT_MAXIMUM_CAPACITY`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::new();
    ///
    /// let result = hashcache.capacity();
    /// assert_eq!(result, 0);
    /// ```
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty [`HashCache`] with the specified capacity.
    ///
    /// The supplied minimum and maximum capacity values are adjusted to any suitable
    /// `power-of-two` values that are close to them.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashcache: HashCache<u64, u32, RandomState> = HashCache::with_capacity(1000, 2000);
    ///
    /// let result = hashcache.capacity();
    /// assert_eq!(result, 1024);
    /// ```
    #[inline]
    #[must_use]
    pub fn with_capacity(minimum_capacity: usize, maximum_capacity: usize) -> Self {
        Self::with_capacity_and_hasher(minimum_capacity, maximum_capacity, RandomState::new())
    }
}

impl<K, V, H> Default for HashCache<K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher + Default,
{
    /// Creates an empty default [`HashCache`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// let result = hashcache.capacity();
    /// assert_eq!(result, 0);
    /// ```
    #[inline]
    fn default() -> Self {
        Self::with_hasher(H::default())
    }
}

impl<K, V, H> HashTable<K, Evictable<V>, H, CACHE> for HashCache<K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    #[inline]
    fn hasher(&self) -> &H {
        &self.build_hasher
    }
    #[inline]
    fn try_clone(_entry: &(K, Evictable<V>)) -> Option<(K, Evictable<V>)> {
        None
    }
    #[inline]
    fn try_reset(value: &mut Evictable<V>) {
        value.reset_link();
    }
    #[inline]
    fn bucket_array(&self) -> &AtomicArc<BucketArray<K, Evictable<V>, CACHE>> {
        &self.array
    }
    #[inline]
    fn minimum_capacity(&self) -> &AtomicUsize {
        &self.minimum_capacity
    }
    #[inline]
    fn maximum_capacity(&self) -> usize {
        self.maximum_capacity
    }
}

impl<'h, K, V, H> Entry<'h, K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    /// Ensures a value is in the entry by inserting the supplied instance if empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(3).or_insert(7);
    /// assert_eq!(hashcache.read(&3, |_, v| *v), Some(7));
    /// ```
    #[inline]
    pub fn or_insert(self, val: V) -> OccupiedEntry<'h, K, V, H> {
        self.or_insert_with(|| val)
    }

    /// Ensures a value is in the entry by inserting the result of the supplied closure if empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(19).or_insert_with(|| 5);
    /// assert_eq!(hashcache.read(&19, |_, v| *v), Some(5));
    /// ```
    #[inline]
    pub fn or_insert_with<F: FnOnce() -> V>(self, constructor: F) -> OccupiedEntry<'h, K, V, H> {
        self.or_insert_with_key(|_| constructor())
    }

    /// Ensures a value is in the entry by inserting the result of the supplied closure if empty.
    ///
    /// The reference to the moved key is provided, therefore cloning or copying the key is
    /// unnecessary.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(11).or_insert_with_key(|k| if *k == 11 { 7 } else { 3 });
    /// assert_eq!(hashcache.read(&11, |_, v| *v), Some(7));
    /// ```
    #[inline]
    pub fn or_insert_with_key<F: FnOnce(&K) -> V>(
        self,
        constructor: F,
    ) -> OccupiedEntry<'h, K, V, H> {
        match self {
            Self::Occupied(o) => o,
            Self::Vacant(v) => {
                let val = constructor(v.key());
                v.insert_entry(val)
            }
        }
    }

    /// Returns a reference to the key of this entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    /// assert_eq!(hashcache.entry(31).key(), &31);
    /// ```
    #[inline]
    pub fn key(&self) -> &K {
        match self {
            Self::Occupied(o) => o.key(),
            Self::Vacant(v) => v.key(),
        }
    }

    /// Provides in-place mutable access to an occupied entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(37).and_modify(|v| { *v += 1 }).or_insert(47);
    /// assert_eq!(hashcache.read(&37, |_, v| *v), Some(47));
    ///
    /// hashcache.entry(37).and_modify(|v| { *v += 1 }).or_insert(3);
    /// assert_eq!(hashcache.read(&37, |_, v| *v), Some(48));
    /// ```
    #[inline]
    #[must_use]
    pub fn and_modify<F>(self, f: F) -> Self
    where
        F: FnOnce(&mut V),
    {
        match self {
            Self::Occupied(mut o) => {
                f(o.get_mut());
                Self::Occupied(o)
            }
            Self::Vacant(_) => self,
        }
    }

    /// Sets the value of the entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    /// let entry = hashcache.entry(11).insert_entry(17);
    /// assert_eq!(entry.key(), &11);
    /// ```
    #[inline]
    pub fn insert_entry(self, val: V) -> OccupiedEntry<'h, K, V, H> {
        match self {
            Self::Occupied(mut o) => {
                o.insert(val);
                o
            }
            Self::Vacant(v) => v.insert_entry(val),
        }
    }
}

impl<'h, K, V, H> Entry<'h, K, V, H>
where
    K: Eq + Hash,
    V: Default,
    H: BuildHasher,
{
    /// Ensures a value is in the entry by inserting the default value if empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    /// hashcache.entry(11).or_default();
    /// assert_eq!(hashcache.read(&11, |_, v| *v), Some(0));
    /// ```
    #[inline]
    pub fn or_default(self) -> OccupiedEntry<'h, K, V, H> {
        match self {
            Self::Occupied(o) => o,
            Self::Vacant(v) => v.insert_entry(Default::default()),
        }
    }
}

impl<'h, K, V, H> Debug for Entry<'h, K, V, H>
where
    K: Debug + Eq + Hash,
    V: Debug,
    H: BuildHasher,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Vacant(v) => f.debug_tuple("Entry").field(v).finish(),
            Self::Occupied(o) => f.debug_tuple("Entry").field(o).finish(),
        }
    }
}

impl<'h, K, V, H> OccupiedEntry<'h, K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    /// Gets a reference to the key in the entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// assert_eq!(hashcache.entry(29).or_default().key(), &29);
    /// ```
    #[inline]
    #[must_use]
    pub fn key(&self) -> &K {
        &self.entry_ptr.get(self.data_block_mut).0
    }

    /// Takes ownership of the key and value from the [`HashCache`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use scc::hash_cache::Entry;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(11).or_insert(17);
    ///
    /// if let Entry::Occupied(o) = hashcache.entry(11) {
    ///     assert_eq!(o.remove_entry(), (11, 17));
    /// };
    /// ```
    #[inline]
    #[must_use]
    pub fn remove_entry(mut self) -> (K, V) {
        let (k, v) = unsafe {
            self.locker
                .erase(self.data_block_mut, &mut self.entry_ptr)
                .unwrap_unchecked()
        };
        if self.locker.num_entries() <= 1 || self.locker.need_rebuild() {
            let barrier = Barrier::new();
            let hashcache = self.hashcache;
            if let Some(current_array) = hashcache.bucket_array().load(Acquire, &barrier).as_ref() {
                if current_array.old_array(&barrier).is_null() {
                    let index = self.index;
                    if current_array.within_sampling_range(index) {
                        drop(self);
                        hashcache.try_shrink_or_rebuild(current_array, index, &barrier);
                    }
                }
            }
        }
        (k, v.take())
    }

    /// Gets a reference to the value in the entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use scc::hash_cache::Entry;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(19).or_insert(11);
    ///
    /// if let Entry::Occupied(o) = hashcache.entry(19) {
    ///     assert_eq!(o.get(), &11);
    /// };
    /// ```
    #[inline]
    #[must_use]
    pub fn get(&self) -> &V {
        &self.entry_ptr.get(self.data_block_mut).1
    }

    /// Gets a mutable reference to the value in the entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use scc::hash_cache::Entry;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(37).or_insert(11);
    ///
    /// if let Entry::Occupied(mut o) = hashcache.entry(37) {
    ///     *o.get_mut() += 18;
    ///     assert_eq!(*o.get(), 29);
    /// }
    ///
    /// assert_eq!(hashcache.read(&37, |_, v| *v), Some(29));
    /// ```
    #[inline]
    pub fn get_mut(&mut self) -> &mut V {
        &mut self
            .entry_ptr
            .get_mut(self.data_block_mut, &mut self.locker)
            .1
    }

    /// Sets the value of the entry, and returns the old value.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use scc::hash_cache::Entry;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(37).or_insert(11);
    ///
    /// if let Entry::Occupied(mut o) = hashcache.entry(37) {
    ///     assert_eq!(o.insert(17), 11);
    /// }
    ///
    /// assert_eq!(hashcache.read(&37, |_, v| *v), Some(17));
    /// ```
    #[inline]
    pub fn insert(&mut self, val: V) -> V {
        replace(self.get_mut(), val)
    }

    /// Takes the value out of the entry, and returns it.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use scc::hash_cache::Entry;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// hashcache.entry(11).or_insert(17);
    ///
    /// if let Entry::Occupied(o) = hashcache.entry(11) {
    ///     assert_eq!(o.remove(), 17);
    /// };
    /// ```
    #[inline]
    #[must_use]
    pub fn remove(self) -> V {
        self.remove_entry().1
    }
}

impl<'h, K, V, H> Debug for OccupiedEntry<'h, K, V, H>
where
    K: Debug + Eq + Hash,
    V: Debug,
    H: BuildHasher,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OccupiedEntry")
            .field("key", self.key())
            .field("value", self.get())
            .finish_non_exhaustive()
    }
}

/// [`OccupiedEntry`] can be sent across threads and awaits with the lock on the bucket held in it.
unsafe impl<'h, K, V, H> Send for OccupiedEntry<'h, K, V, H>
where
    K: Eq + Hash + Send,
    V: Send,
    H: BuildHasher,
{
}

impl<'h, K, V, H> VacantEntry<'h, K, V, H>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    /// Gets a reference to the key.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    /// assert_eq!(hashcache.entry(11).key(), &11);
    /// ```
    #[inline]
    pub fn key(&self) -> &K {
        &self.key
    }

    /// Takes ownership of the key.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use scc::hash_cache::Entry;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// if let Entry::Vacant(v) = hashcache.entry(17) {
    ///     assert_eq!(v.into_key(), 17);
    /// };
    /// ```
    #[inline]
    pub fn into_key(self) -> K {
        self.key
    }

    /// Sets the value of the entry with its key, and returns an [`OccupiedEntry`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::HashCache;
    /// use scc::hash_cache::Entry;
    ///
    /// let hashcache: HashCache<u64, u32> = HashCache::default();
    ///
    /// if let Entry::Vacant(o) = hashcache.entry(19) {
    ///     o.insert_entry(29);
    /// }
    ///
    /// assert_eq!(hashcache.read(&19, |_, v| *v), Some(29));
    /// ```
    #[inline]
    pub fn insert_entry(mut self, val: V) -> OccupiedEntry<'h, K, V, H> {
        let barrier = Barrier::new();
        let entry_ptr = self.locker.insert_with(
            self.data_block_mut,
            BucketArray::<K, V, CACHE>::partial_hash(self.hash),
            || (self.key, Evictable::new(val)),
            self.hashcache.prolonged_barrier_ref(&barrier),
        );
        OccupiedEntry {
            hashcache: self.hashcache,
            index: self.index,
            data_block_mut: self.data_block_mut,
            locker: self.locker,
            entry_ptr,
        }
    }
}

impl<'h, K, V, H> Debug for VacantEntry<'h, K, V, H>
where
    K: Debug + Eq + Hash,
    V: Debug,
    H: BuildHasher,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("VacantEntry").field(self.key()).finish()
    }
}
