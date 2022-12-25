//! [`Bag`] is a lock-free concurrent unordered set.
//!
//! Work-in-progress.

use super::Queue;

use std::fmt::{self, Debug};
use std::mem::{needs_drop, size_of, MaybeUninit};
use std::ptr::drop_in_place;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

const STORAGE_LEN: usize = std::mem::size_of::<usize>() * 4;

/// [`Bag`] is a lock-free concurrent unordered instance container.
///
/// Work-in-progress.
pub struct Bag<T: 'static> {
    /// Primary storage.
    storage: [MaybeUninit<T>; STORAGE_LEN],

    /// Primary storage metadata.
    ///
    /// Layout.
    ///
    /// Upper `size_of::<usize>() * 4` bits = instantiated | `..0` = owned.
    ///
    /// Possible states.
    ///
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
        self.queue.push(val);
    }

    /// Pops a random element in the [`Bag`] if not empty.
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
        self.queue
            .pop()
            .map(|mut e| unsafe { e.get_mut().unwrap_unchecked().take_inner() })
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
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
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

impl<T: 'static + Debug> Debug for Bag<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.queue.fmt(f)
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
    #[allow(clippy::cast_possible_truncation)]
    #[inline]
    fn drop(&mut self) {
        if needs_drop::<T>() {
            let mut metadata = self
                .metadata
                .load(Relaxed)
                .wrapping_shr(size_of::<usize>() as u32 * 4) as u32;
            loop {
                let index = metadata.trailing_zeros();
                if index == 32 {
                    break;
                }
                metadata &= !(1_u32 << index);
                unsafe { drop_in_place(self.storage[index as usize].as_mut_ptr()) };
            }
        }
    }
}
