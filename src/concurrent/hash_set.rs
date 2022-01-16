//! The module implements [`HashSet`].

use super::HashMap;

use crate::ebr::Barrier;

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};

/// A scalable concurrent hash set data structure.
///
/// [`HashSet`] is a concurrent hash set based on [`HashMap`]. It internally uses a [`HashMap`]
/// as its key container, thus sharing all the characteristics of [`HashMap`].
pub struct HashSet<K, H = RandomState>
where
    K: 'static + Eq + Hash + Sync,
    H: BuildHasher,
{
    map: HashMap<K, (), H>,
}

impl<K, H> HashSet<K, H>
where
    K: 'static + Eq + Hash + Sync,
    H: BuildHasher,
{
    /// Creates an empty [`HashSet`] with the given capacity and [`BuildHasher`].
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
    /// use scc::concurrent::HashSet;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashset: HashSet<u64, RandomState> = HashSet::new(1000, RandomState::new());
    ///
    /// let result = hashset.capacity();
    /// assert_eq!(result, 1024);
    ///
    /// let hashset: HashSet<u64> = HashSet::default();
    /// let result = hashset.capacity();
    /// assert_eq!(result, 64);
    /// ```
    pub fn new(capacity: usize, build_hasher: H) -> HashSet<K, H> {
        HashSet {
            map: HashMap::new(capacity, build_hasher),
        }
    }

    /// Temporarily increases the minimum capacity of the [`HashSet`].
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
    /// use scc::concurrent::HashSet;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashset: HashSet<usize, RandomState> = HashSet::new(1000, RandomState::new());
    /// assert_eq!(hashset.capacity(), 1024);
    ///
    /// let ticket = hashset.reserve(10000);
    /// assert!(ticket.is_some());
    /// assert_eq!(hashset.capacity(), 16384);
    /// for i in 0..16 {
    ///     assert!(hashset.insert(i).is_ok());
    /// }
    /// drop(ticket);
    ///
    /// assert_eq!(hashset.capacity(), 1024);
    /// ```
    pub fn reserve(&self, capacity: usize) -> Option<Ticket<K, H>> {
        self.map.reserve(capacity)
    }

    /// Inserts a key-value pair into the [`HashSet`].
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied key if the key exists.
    ///
    /// # Panics
    ///
    /// Panics if memory allocation fails, or the number of entries in the target cell reaches
    /// `u32::MAX`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::concurrent::HashSet;
    ///
    /// let hashset: HashSet<u64> = HashSet::default();
    ///
    /// assert!(hashset.insert(1).is_ok());
    /// assert_eq!(hashset.insert(1).unwrap_err(), 1);
    /// ```
    #[inline]
    pub fn insert(&self, key: K) -> Result<(), K> {
        if let Err((k, _)) = self.map.insert(key, ()) {
            return Err(k);
        }
        Ok(())
    }

    /// Removes a key-value pair if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::concurrent::HashSet;
    ///
    /// let hashset: HashSet<u64> = HashSet::default();
    ///
    /// assert!(hashset.remove(&1).is_none());
    /// assert!(hashset.insert(1).is_ok());
    /// assert_eq!(hashset.remove(&1).unwrap(), 1);
    /// ```
    #[inline]
    pub fn remove<Q>(&self, key_ref: &Q) -> Option<K>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.map.remove(key_ref).map(|(k, _)| k)
    }

    /// Removes a key-value pair if the key exists and the given condition is met.
    ///
    /// The key is locked while evaluating the condition.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::concurrent::HashSet;
    ///
    /// let hashset: HashSet<u64> = HashSet::default();
    ///
    /// assert!(hashset.insert(1).is_ok());
    /// assert!(hashset.remove_if(&1, || false).is_none());
    /// assert_eq!(hashset.remove_if(&1, || true).unwrap(), 1);
    /// ```
    #[inline]
    pub fn remove_if<Q, F: FnOnce() -> bool>(&self, key_ref: &Q, condition: F) -> Option<K>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.map.remove_if(key_ref, |_| condition()).map(|(k, _)| k)
    }

    /// Reads a key.
    ///
    /// It returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::concurrent::HashSet;
    ///
    /// let hashset: HashSet<u64> = HashSet::default();
    ///
    /// assert!(hashset.read(&1, |_| true).is_none());
    /// assert!(hashset.insert(1).is_ok());
    /// assert!(hashset.read(&1, |_| true).unwrap());
    /// ```
    #[inline]
    pub fn read<Q, R, F: FnOnce(&K) -> R>(&self, key_ref: &Q, reader: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let barrier = Barrier::new();
        self.read_with(key_ref, reader, &barrier)
    }

    /// Reads a key using the supplied [`Barrier`].
    ///
    /// It enables the caller to use the value reference outside the method. It returns `None`
    /// if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::concurrent::HashSet;
    ///
    /// let hashset: HashSet<u64> = HashSet::default();
    ///
    /// assert!(hashset.insert(1).is_ok());
    ///
    /// let barrier = Barrier::new();
    /// let key_ref = hashset.read_with(&1, |k| k, &barrier).unwrap();
    /// assert_eq!(*key_ref, 1);
    /// ```
    #[inline]
    pub fn read_with<'b, Q, R, F: FnOnce(&'b K) -> R>(
        &self,
        key_ref: &Q,
        reader: F,
        barrier: &'b Barrier,
    ) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.map.read_with(key_ref, |k, _| reader(k), barrier)
    }

    /// Checks if the key exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::concurrent::HashSet;
    ///
    /// let hashset: HashSet<u64> = HashSet::default();
    ///
    /// assert!(!hashset.contains(&1));
    /// assert!(hashset.insert(1).is_ok());
    /// assert!(hashset.contains(&1));
    /// ```
    #[inline]
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.read(key, |_| ()).is_some()
    }

    /// Iterates over all the keys in the [`HashSet`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::concurrent::HashSet;
    ///
    /// let hashset: HashSet<u64> = HashSet::default();
    ///
    /// assert!(hashset.insert(1).is_ok());
    /// assert!(hashset.insert(2).is_ok());
    ///
    /// let mut acc = 0;
    /// hashset.for_each(|k| { acc += *k; });
    /// assert_eq!(acc, 3);
    /// ```
    #[inline]
    pub fn for_each<F: FnMut(&K)>(&self, mut f: F) {
        self.map.retain(|k, _| {
            f(k);
            true
        });
    }

    /// Retains keys that satisfy the given predicate.
    ///
    /// It returns the number of keys remaining and removed.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::concurrent::HashSet;
    ///
    /// let hashset: HashSet<u64> = HashSet::default();
    ///
    /// assert!(hashset.insert(1).is_ok());
    /// assert!(hashset.insert(2).is_ok());
    /// assert!(hashset.insert(3).is_ok());
    ///
    /// assert_eq!(hashset.retain(|k| *k == 1), (1, 2));
    /// ```
    pub fn retain<F: FnMut(&K) -> bool>(&self, mut filter: F) -> (usize, usize) {
        self.map.retain(|k, _| filter(k))
    }

    /// Clears all the keys.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::concurrent::HashSet;
    ///
    /// let hashset: HashSet<u64> = HashSet::default();
    ///
    /// assert!(hashset.insert(1).is_ok());
    /// assert_eq!(hashset.clear(), 1);
    /// ```
    #[inline]
    pub fn clear(&self) -> usize {
        self.map.clear()
    }

    /// Returns the number of entries in the [`HashSet`].
    ///
    /// It scans the entire array to calculate the number of valid entries, making its time
    /// complexity `O(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::concurrent::HashSet;
    ///
    /// let hashset: HashSet<u64> = HashSet::default();
    ///
    /// assert!(hashset.insert(1).is_ok());
    /// assert_eq!(hashset.len(), 1);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns `true` if the [`HashSet`] is empty.
    ///
    /// It scans the entire array to calculate the number of valid entries, making its time
    /// complexity `O(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::concurrent::HashSet;
    ///
    /// let hashset: HashSet<u64> = HashSet::default();
    ///
    /// assert!(hashset.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns the capacity of the [`HashSet`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::concurrent::HashSet;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let hashset: HashSet<u64, RandomState> = HashSet::new(1000000, RandomState::new());
    /// assert_eq!(hashset.capacity(), 1048576);
    /// ```
    #[inline]
    pub fn capacity(&self) -> usize {
        self.map.capacity()
    }
}

impl<K: 'static + Eq + Hash + Sync> Default for HashSet<K, RandomState> {
    /// Creates a [`HashSet`] with the default parameters.
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
    /// use scc::concurrent::HashSet;
    ///
    /// let hashset: HashSet<u64> = HashSet::default();
    ///
    /// let result = hashset.capacity();
    /// assert_eq!(result, 64);
    /// ```
    fn default() -> Self {
        HashSet {
            map: HashMap::default(),
        }
    }
}

/// [`Ticket`] keeps the increased minimum capacity of the [`HashSet`] during its lifetime.
///
/// The minimum capacity is lowered when the [`Ticket`] is dropped, thereby allowing unused
/// memory to be reclaimed.
pub type Ticket<'h, K, H> = super::hash_map::Ticket<'h, K, (), H>;
