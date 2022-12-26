//! [`Bag`] is a lock-free concurrent unordered set.

use super::queue::Entry;
use super::Queue;

use std::mem::{needs_drop, size_of, MaybeUninit};
use std::ptr::drop_in_place;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// The length of the fixed-size array in a [`Bag`].
const STORAGE_LEN: usize = size_of::<usize>() * 4;

/// [`Bag`] is a lock-free concurrent unordered instance container.
///
/// [`Bag`] is a linearizable concurrent instance container where `size_of::<usize>() * 4`
/// instances are stored in a fixed-size array, and the rest are managed by its backup [`Queue`];
/// which makes a [`Bag`] especially efficient if the expected number of instances does not exceed
/// `size_of::<usize>() * 4`.
#[derive(Debug)]
pub struct Bag<T: 'static> {
    /// Primary storage.
    storage: [MaybeUninit<T>; STORAGE_LEN],

    /// Primary storage metadata.
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

    /// Fallback storage.
    queue: Queue<T>,
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
        let mut metadata = self.metadata.load(Relaxed);
        'after_read_metadata: loop {
            // Looking for a free slot.
            let mut instance_bitmap = Self::instance_bitmap(metadata);
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
                                let new = (m & (!(1_usize << index)))
                                    | (1_usize << (index + STORAGE_LEN));
                                Some(new)
                            });
                            debug_assert!(result.is_ok());
                            return;
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
            break;
        }

        // Push the instance into the backup storage.
        self.queue.push(val);
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
        // Try to pop a slot from the backup storage.
        if let Some(e) = self.queue.pop() {
            return unsafe { Some((*(e.as_ptr() as *mut Entry<T>)).take_inner()) };
        }

        let mut metadata = self.metadata.load(Relaxed);
        'after_read_metadata: loop {
            // Looking for an instantiated, yet unowned entry.
            let mut instance_bitmap = Self::instance_bitmap(metadata);
            let owned_bitmap = Self::owned_bitmap(metadata);
            let mut index = instance_bitmap.trailing_zeros() as usize;
            while index != 32 {
                debug_assert!(index < STORAGE_LEN);
                if (owned_bitmap & (1_u32 << index)) == 0 {
                    // Mark the slot `owned`.
                    let new = metadata | (1_usize << index);
                    match self
                        .metadata
                        .compare_exchange(metadata, new, Acquire, Relaxed)
                    {
                        Ok(_) => {
                            // Now the desired slot is owned by the thread.
                            let inst = unsafe { self.storage[index].as_ptr().read() };
                            let result = self.metadata.fetch_update(Relaxed, Relaxed, |m| {
                                debug_assert_ne!(m & (1_usize << index), 0);
                                debug_assert_ne!(m & (1_usize << (index + STORAGE_LEN)), 0);
                                let new = m
                                    & (!((1_usize << index) | (1_usize << (index + STORAGE_LEN))));
                                Some(new)
                            });
                            debug_assert!(result.is_ok());
                            return Some(inst);
                        }
                        Err(prev) => {
                            // Metadata has changed.
                            metadata = prev;
                            continue 'after_read_metadata;
                        }
                    }
                }

                // Looking for another valid slot.
                instance_bitmap &= !(1_u32 << index);
                index = instance_bitmap.trailing_zeros() as usize;
            }

            // All the entries are vacant or owned.
            return None;
        }
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
        let metadata = self.metadata.load(Acquire);
        let instance_bitmap = Self::instance_bitmap(metadata);
        if instance_bitmap.trailing_zeros() != 32 {
            return false;
        }
        self.queue.is_empty()
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

impl<T: 'static + Clone> Clone for Bag<T> {
    #[inline]
    fn clone(&self) -> Self {
        Bag {
            storage: unsafe { MaybeUninit::uninit().assume_init() },
            metadata: AtomicUsize::new(0),
            queue: self.queue.clone(),
        }
    }
}

impl<T: 'static> Default for Bag<T> {
    #[inline]
    fn default() -> Self {
        Self {
            storage: unsafe { MaybeUninit::uninit().assume_init() },
            metadata: AtomicUsize::new(0),
            queue: Queue::default(),
        }
    }
}

impl<T: 'static> Drop for Bag<T> {
    #[inline]
    fn drop(&mut self) {
        if needs_drop::<T>() {
            let mut instance_bitmap = Self::instance_bitmap(self.metadata.load(Relaxed));
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
