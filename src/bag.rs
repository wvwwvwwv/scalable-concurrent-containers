//! [`Bag`] is a lock-free concurrent unordered instance container.

use super::ebr::Barrier;
use super::{LinkedList, Stack};

use std::mem::{needs_drop, size_of, MaybeUninit};
use std::ptr::drop_in_place;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// The length of the fixed-size array in a [`Bag`].
const STORAGE_LEN: usize = size_of::<usize>() * 4;

/// [`Bag`] is a lock-free concurrent unordered instance container.
///
/// [`Bag`] is a linearizable concurrent instance container where `size_of::<usize>() * 4`
/// instances are stored in a fixed-size array, and the rest are managed by its backup container
/// which makes a [`Bag`] especially efficient if the expected number of instances does not exceed
/// `size_of::<usize>() * 4`.
#[derive(Debug)]
pub struct Bag<T: 'static> {
    /// Primary storage.
    primary_storage: Storage<T>,

    /// Fallback storage.
    stack: Stack<Storage<T>>,
}

impl<T: 'static> Bag<T> {
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
            let barrier = Barrier::new();
            if let Some(storage) = self.stack.peek_with(|e| &**e, &barrier) {
                if let Some(val) = storage.push(val, false) {
                    self.stack.push(Storage::with_val(val));
                }
                return;
            }
            self.stack.push(Storage::with_val(val));
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
        let mut current = self.stack.peek_with(|e| e, &barrier);
        while let Some(e) = current {
            let (val_opt, empty) = e.pop();
            if empty {
                e.delete_self(Relaxed);
            }
            if let Some(val) = val_opt {
                return Some(val);
            }
            current = e.next_ptr(Acquire, &barrier).as_ref();
        }
        self.primary_storage.pop().0
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

impl<T: 'static> Default for Bag<T> {
    #[inline]
    fn default() -> Self {
        Self {
            primary_storage: Storage::new(),
            stack: Stack::default(),
        }
    }
}

#[derive(Debug)]
struct Storage<T: 'static> {
    /// Storage.
    storage: [MaybeUninit<T>; STORAGE_LEN],

    /// Storage metadata.
    ///
    /// The layout of the metadata is,
    /// - Upper `size_of::<usize>() * 4` bits = instantiation bitmap.
    /// - Lower `size_of::<usize>() * 4` bits = owned state bitmap.
    ///
    /// The metadata represents four possible states of a storage slot.
    /// - !instantiated && !owned: initial state.
    /// - !instantiated && owned: owned for instantiating.
    /// - instantiated && !owned: valid and reachable.
    /// - instantiated && owned: owned for moving out the instance.
    metadata: AtomicUsize,
}

impl<T: 'static> Storage<T> {
    /// Creates a new [`Storage`].
    fn new() -> Storage<T> {
        Storage {
            storage: unsafe { MaybeUninit::uninit().assume_init() },
            metadata: AtomicUsize::new(0),
        }
    }

    /// Creates a new [`Storage`] with one inserted.
    fn with_val(val: T) -> Storage<T> {
        let mut storage = Storage::<T> {
            storage: unsafe { MaybeUninit::uninit().assume_init() },
            metadata: AtomicUsize::new(1_usize << STORAGE_LEN),
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
            while index != STORAGE_LEN {
                debug_assert!(index < STORAGE_LEN);
                if (owned_bitmap & (1_u32 << index)) == 0 {
                    // Mark the slot `owned`.
                    let new = metadata | (1_usize << index);
                    match self
                        .metadata
                        .compare_exchange(metadata, new, Acquire, Relaxed)
                    {
                        Ok(_) => {
                            // Now the free slot is owned by the thread.
                            unsafe {
                                (self.storage[index].as_ptr() as *mut T).write(val);
                            }
                            let result = self.metadata.fetch_update(Release, Relaxed, |m| {
                                debug_assert_ne!(m & (1_usize << index), 0);
                                debug_assert_eq!(m & (1_usize << (index + STORAGE_LEN)), 0);
                                if !allow_empty && Self::instance_bitmap(m) == 0 {
                                    // Disallowed to push a value into an empty array.
                                    None
                                } else {
                                    let new = (m & (!(1_usize << index)))
                                        | (1_usize << (index + STORAGE_LEN));
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
            let mut index = instance_bitmap.trailing_zeros();
            while index != 32 {
                debug_assert!((index as usize) < STORAGE_LEN);
                if (owned_bitmap & (1_u32 << index)) == 0 {
                    // Mark the slot `owned`.
                    let new = metadata | (1_usize << index);
                    match self
                        .metadata
                        .compare_exchange(metadata, new, Acquire, Relaxed)
                    {
                        Ok(_) => {
                            // Now the desired slot is owned by the thread.
                            let inst = unsafe { self.storage[index as usize].as_ptr().read() };
                            let mut empty = false;
                            let result = self.metadata.fetch_update(Relaxed, Relaxed, |m| {
                                debug_assert_ne!(m & (1_usize << index), 0);
                                debug_assert_ne!(
                                    m & (1_usize << (index as usize + STORAGE_LEN)),
                                    0
                                );
                                let new = m
                                    & (!((1_usize << index)
                                        | (1_usize << (index as usize + STORAGE_LEN))));
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
                index = (instance_bitmap & (u32::MAX.wrapping_shl(index).wrapping_shl(1)))
                    .trailing_zeros();
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
        metadata.wrapping_shr(STORAGE_LEN as u32) as u32
    }

    #[allow(clippy::cast_possible_truncation)]
    fn owned_bitmap(metadata: usize) -> u32 {
        (metadata % (1_usize << STORAGE_LEN)) as u32
    }
}

impl<T: 'static> Drop for Storage<T> {
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
