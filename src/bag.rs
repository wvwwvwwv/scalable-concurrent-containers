//! [`Bag`] is a lock-free concurrent unordered instance container.

use super::ebr::Guard;
use super::{LinkedEntry, Stack};
use std::iter::FusedIterator;
use std::mem::{needs_drop, MaybeUninit};
use std::panic::UnwindSafe;
use std::ptr::drop_in_place;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// [`Bag`] is a lock-free concurrent unordered instance container.
///
/// [`Bag`] is a linearizable concurrent instance container where `ARRAY_LEN` instances are stored
/// in a fixed-size array, and the rest are managed by its backup container; this makes a [`Bag`]
/// especially efficient if the expected number of instances does not exceed `ARRAY_LEN`.
///
/// The maximum value of `ARRAY_LEN` is limited to `usize::BITS / 2` which is the default value, and
/// if a larger value is specified, [`Bag::new`] panics.
#[derive(Debug)]
pub struct Bag<T, const ARRAY_LEN: usize = DEFAULT_ARRAY_LEN> {
    /// Primary storage.
    primary_storage: Storage<T, ARRAY_LEN>,

    /// Fallback storage.
    stack: Stack<Storage<T, ARRAY_LEN>>,
}

/// A mutable iterator over the entries of a [`Bag`].
#[derive(Debug)]
pub struct IterMut<'b, T, const ARRAY_LEN: usize = DEFAULT_ARRAY_LEN> {
    bag: &'b mut Bag<T, ARRAY_LEN>,
    current_index: u32,
    current_stack_entry: Option<&'b mut LinkedEntry<Storage<T, ARRAY_LEN>>>,
}

/// An iterator that moves out of a [`Bag`].
#[derive(Debug)]
pub struct IntoIter<T, const ARRAY_LEN: usize = DEFAULT_ARRAY_LEN> {
    bag: Bag<T, ARRAY_LEN>,
}

/// The default length of the fixed-size array in a [`Bag`].
const DEFAULT_ARRAY_LEN: usize = usize::BITS as usize / 2;

#[derive(Debug)]
struct Storage<T, const ARRAY_LEN: usize> {
    /// Storage.
    storage: [MaybeUninit<T>; ARRAY_LEN],

    /// Storage metadata.
    ///
    /// The layout of the metadata is,
    /// - Upper `usize::BITS / 2` bits: initialization bitmap.
    /// - Lower `usize::BITS / 2` bits: owned state bitmap.
    ///
    /// The metadata represents four possible states of a storage slot.
    /// - `!instantiated && !owned`: initial state.
    /// - `!instantiated && owned`: owned for instantiating.
    /// - `instantiated && !owned`: valid and reachable.
    /// - `instantiated && owned`: owned for moving out the instance.
    metadata: AtomicUsize,
}

impl<T, const ARRAY_LEN: usize> Bag<T, ARRAY_LEN> {
    /// Creates a new [`Bag`].
    ///
    /// # Panics
    ///
    /// Panics if the specified `ARRAY_LEN` value is larger than `usize::BITS / 2`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Bag;
    ///
    /// let bag: Bag<usize, 16> = Bag::new();
    /// ```
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        assert!(ARRAY_LEN <= DEFAULT_ARRAY_LEN);
        Self {
            primary_storage: Storage::new(),
            stack: Stack::default(),
        }
    }

    /// Pushes an instance of `T`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Bag;
    ///
    /// let bag: Bag<usize> = Bag::default();
    ///
    /// bag.push(11);
    /// ```
    #[inline]
    pub fn push(&self, val: T) {
        if let Some(val) = self.primary_storage.push(val, true) {
            self.stack.peek_with(|e| {
                if let Some(storage) = e {
                    if let Some(val) = storage.push(val, false) {
                        unsafe {
                            self.stack.push_unchecked(Storage::with_val(val));
                        }
                    }
                } else {
                    unsafe {
                        self.stack.push_unchecked(Storage::with_val(val));
                    }
                }
            });
        }
    }

    /// Pops an instance in the [`Bag`] if not empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Bag;
    ///
    /// let bag: Bag<usize> = Bag::default();
    ///
    /// bag.push(37);
    ///
    /// assert_eq!(bag.pop(), Some(37));
    /// assert!(bag.pop().is_none());
    /// ```
    #[inline]
    pub fn pop(&self) -> Option<T> {
        let guard = Guard::new();
        self.stack.peek_with(|e| {
            let mut current = e;
            while let Some(storage) = current {
                let (val_opt, empty) = storage.pop();
                if empty {
                    storage.delete_self();
                }
                if let Some(val) = val_opt {
                    return Some(val);
                }
                current = storage.next_ptr(&guard).as_ref();
            }
            self.primary_storage.pop().0
        })
    }

    /// Pops all the entries at once, and folds them into an accumulator.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Bag;
    ///
    /// let bag: Bag<usize> = Bag::default();
    ///
    /// bag.push(7);
    /// bag.push(17);
    /// bag.push(37);
    ///
    /// assert_eq!(bag.pop_all(0, |a, v| a + v), 61);
    ///
    /// bag.push(47);
    /// assert_eq!(bag.pop(), Some(47));
    /// assert!(bag.pop().is_none());
    /// assert!(bag.is_empty());
    /// ```
    #[inline]
    pub fn pop_all<B, F: FnMut(B, T) -> B>(&self, init: B, mut fold: F) -> B {
        let mut acc = init;
        let popped = self.stack.pop_all();
        while let Some(storage) = popped.pop() {
            acc = storage.pop_all(acc, &mut fold, false);
        }
        self.primary_storage.pop_all(acc, &mut fold, true)
    }

    /// Returns the number of entries in the [`Bag`].
    ///
    /// This method iterates over all the entry arrays in the [`Bag`] to count the number of
    /// entries, therefore its time complexity is `O(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Bag;
    ///
    /// let bag: Bag<usize> = Bag::default();
    /// assert_eq!(bag.len(), 0);
    ///
    /// bag.push(7);
    /// assert_eq!(bag.len(), 1);
    ///
    /// for v in 0..64 {
    ///    bag.push(v);
    /// }
    /// bag.pop();
    /// assert_eq!(bag.len(), 64);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        self.stack
            .iter(&Guard::new())
            .fold(self.primary_storage.len(), |acc, storage| {
                acc + storage.len()
            })
    }

    /// Returns `true` if the [`Bag`] is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Bag;
    ///
    /// let bag: Bag<usize> = Bag::default();
    /// assert!(bag.is_empty());
    ///
    /// bag.push(7);
    /// assert!(!bag.is_empty());
    ///
    /// assert_eq!(bag.pop(), Some(7));
    /// assert!(bag.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        if self.primary_storage.len() == 0 {
            self.stack.is_empty()
        } else {
            false
        }
    }

    /// Iterates over contained instances for modifying them.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Bag;
    ///
    /// let mut bag: Bag<usize> = Bag::default();
    ///
    /// bag.push(3);
    /// bag.push(3);
    ///
    /// assert_eq!(bag.iter_mut().count(), 2);
    /// bag.iter_mut().for_each(|e| { *e += 1; });
    ///
    /// assert_eq!(bag.pop(), Some(4));
    /// assert_eq!(bag.pop(), Some(4));
    /// assert!(bag.pop().is_none());
    /// ```
    #[inline]
    pub fn iter_mut(&mut self) -> IterMut<T, ARRAY_LEN> {
        IterMut {
            bag: self,
            current_index: 0,
            current_stack_entry: None,
        }
    }
}

impl<T> Default for Bag<T, DEFAULT_ARRAY_LEN> {
    #[inline]
    fn default() -> Self {
        Self {
            primary_storage: Storage::new(),
            stack: Stack::default(),
        }
    }
}

impl<T, const ARRAY_LEN: usize> Drop for Bag<T, ARRAY_LEN> {
    #[inline]
    fn drop(&mut self) {
        if needs_drop::<T>() {
            // It needs to drop all the stored instances in-place.
            while let Some(v) = self.pop() {
                drop(v);
            }
        }
    }
}

impl<T, const ARRAY_LEN: usize> IntoIterator for Bag<T, ARRAY_LEN> {
    type Item = T;
    type IntoIter = IntoIter<T, ARRAY_LEN>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        IntoIter { bag: self }
    }
}

impl<'b, T, const ARRAY_LEN: usize> IntoIterator for &'b mut Bag<T, ARRAY_LEN> {
    type IntoIter = IterMut<'b, T, ARRAY_LEN>;
    type Item = &'b mut T;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<'b, T, const ARRAY_LEN: usize> FusedIterator for IterMut<'b, T, ARRAY_LEN> {}

impl<'b, T, const ARRAY_LEN: usize> Iterator for IterMut<'b, T, ARRAY_LEN> {
    type Item = &'b mut T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while self.current_index != u32::MAX {
            let current_storage = if let Some(linked) = self.current_stack_entry.as_mut() {
                &mut **linked
            } else {
                &mut self.bag.primary_storage
            };

            let instance_bitmap =
                Storage::<T, ARRAY_LEN>::instance_bitmap(current_storage.metadata.load(Acquire));
            let first_occupied =
                (instance_bitmap.wrapping_shr(self.current_index)).trailing_zeros();
            let next_occupied = self.current_index + first_occupied;
            self.current_index = next_occupied + 1;
            if (next_occupied as usize) < ARRAY_LEN {
                return Some(unsafe {
                    &mut *current_storage.storage[next_occupied as usize].as_mut_ptr()
                });
            }
            self.current_index = u32::MAX;

            if let Some(linked) = self.current_stack_entry.as_mut() {
                let guard = Guard::new();
                if let Some(next) = linked.next_ptr(&guard).as_ref() {
                    let entry_mut = (next as *const LinkedEntry<Storage<T, ARRAY_LEN>>).cast_mut();
                    self.current_stack_entry = unsafe { entry_mut.as_mut() };
                    self.current_index = 0;
                }
            } else {
                self.bag.stack.peek_with(|e| {
                    if let Some(e) = e {
                        let entry_mut = (e as *const LinkedEntry<Storage<T, ARRAY_LEN>>).cast_mut();
                        self.current_stack_entry = unsafe { entry_mut.as_mut() };
                        self.current_index = 0;
                    }
                });
            }
        }
        None
    }
}

impl<'b, T, const ARRAY_LEN: usize> UnwindSafe for IterMut<'b, T, ARRAY_LEN> where T: UnwindSafe {}

impl<T, const ARRAY_LEN: usize> FusedIterator for IntoIter<T, ARRAY_LEN> {}

impl<T, const ARRAY_LEN: usize> Iterator for IntoIter<T, ARRAY_LEN> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.bag.pop()
    }
}

impl<T, const ARRAY_LEN: usize> UnwindSafe for IntoIter<T, ARRAY_LEN> where T: UnwindSafe {}

impl<T, const ARRAY_LEN: usize> Storage<T, ARRAY_LEN> {
    /// Creates a new [`Storage`].
    fn new() -> Self {
        #[allow(clippy::uninit_assumed_init)]
        Storage {
            storage: unsafe { MaybeUninit::uninit().assume_init() },
            metadata: AtomicUsize::new(0),
        }
    }

    /// Creates a new [`Storage`] with one inserted.
    fn with_val(val: T) -> Self {
        #[allow(clippy::uninit_assumed_init)]
        let mut storage = Self {
            storage: unsafe { MaybeUninit::uninit().assume_init() },
            metadata: AtomicUsize::new(1_usize << ARRAY_LEN),
        };
        unsafe {
            storage.storage[0].as_mut_ptr().write(val);
        }
        storage
    }

    /// Returns the number of entries.
    fn len(&self) -> usize {
        let metadata = self.metadata.load(Relaxed);
        let instance_bitmap = Self::instance_bitmap(metadata);
        let owned_bitmap = Self::owned_bitmap(metadata);
        let valid_entries_bitmap = instance_bitmap & (!owned_bitmap);
        valid_entries_bitmap.count_ones() as usize
    }

    /// Pushes a new value.
    fn push(&self, val: T, allow_empty: bool) -> Option<T> {
        let mut metadata = self.metadata.load(Relaxed);
        'after_read_metadata: loop {
            // Look for a free slot.
            let mut instance_bitmap = Self::instance_bitmap(metadata);
            let owned_bitmap = Self::owned_bitmap(metadata);

            // Regard entries being removed as removed ones.
            if !allow_empty && (instance_bitmap & (!owned_bitmap)) == 0 {
                return Some(val);
            }
            let mut index = instance_bitmap.trailing_ones() as usize;
            while index < ARRAY_LEN {
                if (owned_bitmap & (1_u32 << index)) == 0 {
                    // Mark the slot `owned`.
                    let new = metadata | (1_usize << index);
                    match self
                        .metadata
                        .compare_exchange_weak(metadata, new, Acquire, Relaxed)
                    {
                        Ok(_) => {
                            // Now the free slot is owned by the thread.
                            unsafe {
                                (self.storage[index].as_ptr().cast_mut()).write(val);
                            }
                            let result = self.metadata.fetch_update(Release, Relaxed, |m| {
                                debug_assert_ne!(m & (1_usize << index), 0);
                                debug_assert_eq!(m & (1_usize << (index + ARRAY_LEN)), 0);
                                if !allow_empty
                                    && (Self::instance_bitmap(m) & (!Self::owned_bitmap(m))) == 0
                                {
                                    // Disallow pushing a value into an empty array.
                                    None
                                } else {
                                    let new = (m & (!(1_usize << index)))
                                        | (1_usize << (index + ARRAY_LEN));
                                    Some(new)
                                }
                            });
                            if result.is_ok() {
                                return None;
                            }

                            // The array was empty, thus rolling back the change.
                            let val = unsafe { self.storage[index].as_ptr().read() };
                            self.metadata.fetch_and(!(1_usize << index), Relaxed);
                            return Some(val);
                        }
                        Err(prev) => {
                            // Metadata has changed.
                            metadata = prev;
                            continue 'after_read_metadata;
                        }
                    }
                }

                // Look for another free slot.
                instance_bitmap |= 1_u32 << index;
                index = instance_bitmap.trailing_ones() as usize;
            }

            // No free slots or all the entries are owned.
            return Some(val);
        }
    }

    /// Pops a value.
    fn pop(&self) -> (Option<T>, bool) {
        let mut metadata = self.metadata.load(Relaxed);
        'after_read_metadata: loop {
            // Look for an instantiated, yet to be owned entry.
            let instance_bitmap = Self::instance_bitmap(metadata);
            let owned_bitmap = Self::owned_bitmap(metadata);
            let mut index = instance_bitmap.trailing_zeros() as usize;
            while index < ARRAY_LEN {
                if (owned_bitmap & (1_u32 << index)) == 0 {
                    // Mark the slot `owned`.
                    let new = metadata | (1_usize << index);
                    match self
                        .metadata
                        .compare_exchange_weak(metadata, new, Acquire, Relaxed)
                    {
                        Ok(_) => {
                            // Now the desired slot is owned by the thread.
                            let inst = unsafe { self.storage[index].as_ptr().read() };
                            let mut empty = false;
                            let result = self.metadata.fetch_update(Relaxed, Relaxed, |m| {
                                debug_assert_ne!(m & (1_usize << index), 0);
                                debug_assert_ne!(m & (1_usize << (index + ARRAY_LEN)), 0);
                                let new =
                                    m & (!((1_usize << index) | (1_usize << (index + ARRAY_LEN))));
                                empty = Self::instance_bitmap(new) == 0;
                                Some(new)
                            });
                            debug_assert!(result.is_ok());
                            return (Some(inst), empty);
                        }
                        Err(prev) => {
                            // Metadata has changed.
                            metadata = prev;
                            continue 'after_read_metadata;
                        }
                    }
                }

                // Look for another valid slot.
                {
                    #![allow(clippy::cast_possible_truncation)]
                    index = (instance_bitmap
                        & (u32::MAX.wrapping_shl(index as u32).wrapping_shl(1)))
                    .trailing_zeros() as usize;
                }
            }

            // All the entries are vacant or owned.
            return (None, instance_bitmap == 0);
        }
    }

    /// Pops all the values, and folds them.
    #[allow(clippy::cast_possible_truncation)]
    fn pop_all<B, F: FnMut(B, T) -> B>(&self, init: B, fold: &mut F, allow_nonempty: bool) -> B {
        let mut acc = init;
        let mut metadata = self.metadata.load(Relaxed);
        loop {
            // Look for an instantiated, and reachable entries.
            let instance_bitmap = Self::instance_bitmap(metadata) as usize;
            let owned_bitmap = Self::owned_bitmap(metadata) as usize;
            let instances_to_pop = instance_bitmap & (!owned_bitmap);
            debug_assert_eq!(instances_to_pop & owned_bitmap, 0);

            if instances_to_pop == 0 {
                return acc;
            }

            let marked_for_removal = metadata | instances_to_pop;
            match self.metadata.compare_exchange_weak(
                metadata,
                marked_for_removal,
                Acquire,
                Relaxed,
            ) {
                Ok(_) => {
                    // Now all the valid slots are locked for removal.
                    let mut index = instances_to_pop.trailing_zeros() as usize;
                    while index < ARRAY_LEN {
                        acc = fold(acc, unsafe { self.storage[index].as_ptr().read() });
                        index = (instances_to_pop & (!((1_usize << (index + 1) as u32) - 1)))
                            .trailing_zeros() as usize;
                    }

                    metadata = marked_for_removal;
                    loop {
                        let new_metadata =
                            metadata & (!((instances_to_pop << ARRAY_LEN) | instances_to_pop));
                        match self.metadata.compare_exchange_weak(
                            metadata,
                            new_metadata,
                            Release,
                            Relaxed,
                        ) {
                            Ok(_) => {
                                if allow_nonempty {
                                    return acc;
                                }

                                // Need to check if a new instance was inserted.
                                metadata = new_metadata;
                                break;
                            }
                            Err(actual) => metadata = actual,
                        }
                    }
                }
                Err(actual) => metadata = actual,
            }
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn instance_bitmap(metadata: usize) -> u32 {
        metadata.wrapping_shr(ARRAY_LEN as u32) as u32
    }

    #[allow(clippy::cast_possible_truncation)]
    fn owned_bitmap(metadata: usize) -> u32 {
        (metadata % (1_usize << ARRAY_LEN)) as u32
    }
}

impl<T, const ARRAY_LEN: usize> Drop for Storage<T, ARRAY_LEN> {
    #[inline]
    fn drop(&mut self) {
        if needs_drop::<T>() {
            let mut instance_bitmap = Self::instance_bitmap(self.metadata.load(Acquire));
            loop {
                let index = instance_bitmap.trailing_zeros();
                if index == 32 {
                    break;
                }
                instance_bitmap &= !(1_u32 << index);
                unsafe { drop_in_place(self.storage[index as usize].as_mut_ptr()) };
            }
        }
    }
}
