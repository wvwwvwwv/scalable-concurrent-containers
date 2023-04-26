//! [`Bag`] is a lock-free concurrent unordered instance container.

use super::ebr::Barrier;
use super::{LinkedList, Stack};
use std::mem::{needs_drop, MaybeUninit};
use std::ptr::drop_in_place;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// [`Bag`] is a lock-free concurrent unordered instance container.
///
/// [`Bag`] is a linearizable concurrent instance container where `ARRAY_LEN` instances are stored
/// in a fixed-size array, and the rest are managed by its backup container; this makes a [`Bag`]
/// especially efficient if the expected number of instances does not exceed `ARRAY_LEN`.
///
/// The maximum value of `ARRAY_LEN` is limited to `usize::MAX / 2` which is the default value, and
/// if a larger value is specified, [`Bag::new`] panics.
#[derive(Debug)]
pub struct Bag<T, const ARRAY_LEN: usize = DEFAULT_ARRAY_LEN> {
    /// Primary storage.
    primary_storage: Storage<T, ARRAY_LEN>,

    /// Fallback storage.
    stack: Stack<Storage<T, ARRAY_LEN>>,
}

/// The default length of the fixed-size array in a [`Bag`].
const DEFAULT_ARRAY_LEN: usize = usize::BITS as usize / 2;

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
    pub fn new() -> Bag<T, ARRAY_LEN> {
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
            self.stack.peek(|e| {
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
        let barrier = Barrier::new();
        self.stack.peek(|e| {
            let mut current = e;
            while let Some(storage) = current {
                let (val_opt, empty) = storage.pop();
                if empty {
                    storage.delete_self(Relaxed);
                }
                if let Some(val) = val_opt {
                    return Some(val);
                }
                current = storage.next_ptr(Acquire, &barrier).as_ref();
            }
            self.primary_storage.pop().0
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
        if self.primary_storage.is_empty() {
            self.stack.is_empty()
        } else {
            false
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

#[derive(Debug)]
struct Storage<T, const ARRAY_LEN: usize> {
    /// Storage.
    storage: [MaybeUninit<T>; ARRAY_LEN],

    /// Storage metadata.
    ///
    /// The layout of the metadata is,
    /// - Upper `usize::BITS / 2` bits = instantiation bitmap.
    /// - Lower `usize::BITS / 2` bits = owned state bitmap.
    ///
    /// The metadata represents four possible states of a storage slot.
    /// - !instantiated && !owned: initial state.
    /// - !instantiated && owned: owned for instantiating.
    /// - instantiated && !owned: valid and reachable.
    /// - instantiated && owned: owned for moving out the instance.
    metadata: AtomicUsize,
}

impl<T, const ARRAY_LEN: usize> Storage<T, ARRAY_LEN> {
    /// Creates a new [`Storage`].
    fn new() -> Storage<T, ARRAY_LEN> {
        #[allow(clippy::uninit_assumed_init)]
        Storage {
            storage: unsafe { MaybeUninit::uninit().assume_init() },
            metadata: AtomicUsize::new(0),
        }
    }

    /// Creates a new [`Storage`] with one inserted.
    fn with_val(val: T) -> Storage<T, ARRAY_LEN> {
        #[allow(clippy::uninit_assumed_init)]
        let mut storage = Storage::<T, ARRAY_LEN> {
            storage: unsafe { MaybeUninit::uninit().assume_init() },
            metadata: AtomicUsize::new(1_usize << ARRAY_LEN),
        };
        unsafe {
            storage.storage[0].as_mut_ptr().write(val);
        }
        storage
    }

    /// Pushes a new value.
    fn push(&self, val: T, allow_empty: bool) -> Option<T> {
        let mut metadata = self.metadata.load(Relaxed);
        'after_read_metadata: loop {
            // Looking for a free slot.
            let mut instance_bitmap = Self::instance_bitmap(metadata);
            if !allow_empty && instance_bitmap == 0 {
                return Some(val);
            }
            let owned_bitmap = Self::owned_bitmap(metadata);
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
                                (self.storage[index].as_ptr() as *mut T).write(val);
                            }
                            let result = self.metadata.fetch_update(Release, Relaxed, |m| {
                                debug_assert_ne!(m & (1_usize << index), 0);
                                debug_assert_eq!(m & (1_usize << (index + ARRAY_LEN)), 0);
                                if !allow_empty && Self::instance_bitmap(m) == 0 {
                                    // Disallowed to push a value into an empty array.
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

                // Looking for another free slot.
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
            // Looking for an instantiated, yet unowned entry.
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

                // Looking for another valid slot.
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

    /// Returns `true` if empty.
    fn is_empty(&self) -> bool {
        let metadata = self.metadata.load(Acquire);
        Self::instance_bitmap(metadata) == 0
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
