//! [`Bag`] is a lock-free concurrent unordered set.
//!
//! Work-in-progress.

use super::Queue;

use std::fmt::{self, Debug};
use std::mem::MaybeUninit;

const STORAGE_LEN: usize = std::mem::size_of::<usize>() * 4;

/// [`Bag`] is a lock-free concurrent unordered set.
pub struct Bag<T: 'static> {
    /// Primary storage.
    _storage: [MaybeUninit<T>; STORAGE_LEN],

    /// Primary storage metadata.
    _metadata: usize,

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
    #[allow(clippy::missing_panics_doc)]
    #[inline]
    pub fn pop(&self) -> Option<T> {
        self.queue
            .pop()
            .map(|mut e| unsafe { e.get_mut().unwrap().take_inner() })
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
            _storage: unsafe { MaybeUninit::uninit().assume_init() },
            _metadata: 0,
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
            _storage: unsafe { MaybeUninit::uninit().assume_init() },
            _metadata: 0,
            queue: Queue::default(),
        }
    }
}
