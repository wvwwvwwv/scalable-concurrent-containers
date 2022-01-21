//! The module implements [`HashMap`].

use super::async_yield::{async_yield, AwaitableBarrier};
use super::hash_table::cell::Locker;
use super::hash_table::cell_array::CellArray;
use super::hash_table::HashTable;

use crate::ebr::{Arc, AtomicArc, Barrier};

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::Acquire;

/// [`HashMap`].
pub struct HashMap<K, V, H = RandomState>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    array: AtomicArc<CellArray<K, V, false>>,
    minimum_capacity: usize,
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
    /// # Panics
    ///
    /// Panics if memory allocation fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashmap: HashMap<u64, u32, RandomState> = HashMap::new(1000, RandomState::new());
    ///
    /// let result = hashmap.capacity();
    /// assert_eq!(result, 1024);
    /// ```
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
            resize_mutex: AtomicU8::new(0),
            build_hasher,
        }
    }

    /// Inserts a key-value pair into the [`HashMap`].
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied key-value pair.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert(11, 17);
    /// ```
    #[inline]
    pub async fn insert(&self, mut key: K, mut val: V) -> Result<(), (K, V)> {
        loop {
            match self.insert_entry(key, val, &Barrier::new()) {
                Ok(Some(returned)) => return Err(returned),
                Ok(None) => return Ok(()),
                Err(returned) => {
                    key = returned.0;
                    val = returned.1;
                }
            }
            async_yield().await;
        }
    }

    /// Reads a key-value pair.
    ///
    /// It returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert(11, 17);
    /// let future_read = hashmap.read(&11, |_, v| *v);
    /// ```
    #[inline]
    pub async fn read<Q, R, F: FnMut(&K, &V) -> R>(&self, key_ref: &Q, mut reader: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        loop {
            if let Ok(result) = self.read_entry(key_ref, &mut reader, &Barrier::new()) {
                return result;
            }
            async_yield().await;
        }
    }

    /// Removes a key-value pair if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert(11, 17);
    /// let future_remove = hashmap.remove(&11);
    /// ```
    #[inline]
    pub async fn remove<Q>(&self, key_ref: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.remove_if(key_ref, |_| true).await
    }

    /// Removes a key-value pair if the key exists and the given condition is met.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// let future_insert = hashmap.insert(11, 17);
    /// let future_remove = hashmap.remove_if(&11, |_| true);
    /// ```
    #[inline]
    pub async fn remove_if<Q, F: FnMut(&V) -> bool>(
        &self,
        key_ref: &Q,
        mut condition: F,
    ) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        loop {
            if let Ok(result) = self.remove_entry(key_ref, &mut condition, &Barrier::new()) {
                return result.0;
            }
            async_yield().await;
        }
    }

    /// Iterates over all the entries in the [`HashMap`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// let future_insert = hashmap.insert(1, 0);
    /// let future_for_each = hashmap.for_each(|k, v| println!("{} {}", k, v));
    /// ```
    #[inline]
    pub async fn for_each<F: FnMut(&K, &mut V)>(&self, mut f: F) {
        self.retain(|k, v| {
            f(k, v);
            true
        })
        .await;
    }

    /// Retains key-value pairs that satisfy the given predicate.
    ///
    /// It returns the number of entries remaining and removed.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// let future_insert = hashmap.insert(1, 0);
    /// let future_retain = hashmap.retain(|k, v| *k == 1);
    /// ```
    pub async fn retain<F: FnMut(&K, &mut V) -> bool>(&self, mut filter: F) -> (usize, usize) {
        let mut retained_entries = 0;
        let mut removed_entries = 0;

        // An acquire fence is required to correctly load the contents of the array.
        let mut awaitable_barrier = AwaitableBarrier::default();
        let mut current_array_holder = self.array.get_arc(Acquire, awaitable_barrier.barrier());
        while let Some(current_array) = current_array_holder.take() {
            while !current_array
                .old_array(awaitable_barrier.barrier())
                .is_null()
            {
                if current_array
                    .partial_rehash(
                        |key| self.hash(key),
                        |_, _| None,
                        awaitable_barrier.barrier(),
                    )
                    .is_ok()
                {
                    continue;
                }
                awaitable_barrier.drop_barrier_and_yield().await;
            }

            for cell_index in 0..current_array.num_cells() {
                loop {
                    {
                        // Limits the scope of `barrier`.
                        let barrier = awaitable_barrier.barrier();
                        if let Ok(locker) =
                            Locker::try_lock(current_array.cell(cell_index), barrier)
                        {
                            let mut iterator = locker.cell().iter(barrier);
                            while iterator.next().is_some() {
                                let retain = if let Some((k, v)) = iterator.get() {
                                    #[allow(clippy::cast_ref_to_mut)]
                                    filter(k, unsafe { &mut *(v as *const V as *mut V) })
                                } else {
                                    true
                                };
                                if retain {
                                    retained_entries += 1;
                                } else {
                                    locker.erase(&mut iterator);
                                    removed_entries += 1;
                                }
                                if retained_entries == usize::MAX || removed_entries == usize::MAX {
                                    // Gives up iteration on an integer overflow.
                                    return (retained_entries, removed_entries);
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
                retained_entries = 0;
                current_array_holder.replace(new_current_array);
                continue;
            }
            break;
        }

        if removed_entries >= retained_entries {
            self.resize(&Barrier::new());
        }

        (retained_entries, removed_entries)
    }

    /// Clears all the key-value pairs.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// let future_insert = hashmap.insert(1, 0);
    /// let future_clear = hashmap.clear();
    /// ```
    #[inline]
    pub async fn clear(&self) -> usize {
        self.retain(|_, _| false).await.1
    }

    /// Returns the number of entries in the [`HashMap`].
    ///
    /// It scans the entire array to calculate the number of valid entries, making its time
    /// complexity `O(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    ///
    /// assert_eq!(hashmap.len(), 0);
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
    /// use scc::awaitable::HashMap;
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
    /// use scc::awaitable::HashMap;
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
    /// # Panics
    ///
    /// Panics if memory allocation fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::awaitable::HashMap;
    ///
    /// let hashmap: HashMap<u64, u32> = HashMap::default();
    /// ```
    fn default() -> Self {
        HashMap {
            array: AtomicArc::new(CellArray::<K, V, false>::new(
                Self::default_capacity(),
                AtomicArc::null(),
            )),
            minimum_capacity: Self::default_capacity(),
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

    fn copier(_key: &K, _val: &V) -> Option<(K, V)> {
        None
    }

    fn cell_array(&self) -> &AtomicArc<CellArray<K, V, false>> {
        &self.array
    }

    fn minimum_capacity(&self) -> usize {
        self.minimum_capacity
    }

    fn resize_mutex(&self) -> &AtomicU8 {
        &self.resize_mutex
    }
}
