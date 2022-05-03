//! [`Queue`] is a lock-free concurrent first-in-first-out queue.

#![allow(clippy::unused_self, clippy::needless_pass_by_value, dead_code)]

use super::ebr::{Arc, AtomicArc, Barrier};
use super::LinkedList;

use std::fmt::{Debug, Display};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering::Acquire;

/// [`Queue`] is a lock-free concurrent first-in-first-out queue.
#[derive(Default)]
pub struct Queue<T: 'static> {
    /// `oldest` points to the oldest entry in the [`Queue`].
    oldest: AtomicArc<Entry<T>>,

    /// `newest` *eventually* points to the newest entry in the [`Queue`].
    newest: AtomicArc<Entry<T>>,
}

impl<T: 'static> Queue<T> {
    /// Pushes a new instance of `T`.
    pub fn push(&self, _val: T, _barrier: &Barrier) {}

    /// Pushes a new instance of `T` if the newest entry satisfies the given condition.
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied instance if the condition is not met.
    pub fn push_if<F: FnMut(Option<&T>) -> bool>(
        &self,
        val: T,
        _cond: F,
        _barrier: &Barrier,
    ) -> Result<(), T> {
        Err(val)
    }

    /// Pops the oldest entry.
    ///
    /// Returns `None` if the [`Queue`] is empty.
    pub fn pop(&self, _barrier: &Barrier) -> Option<Arc<Entry<T>>> {
        None
    }

    /// Pops the oldest entry if the oldest entry satisfies the given condition.
    ///
    /// Returns `None` if the [`Queue`] is empty.
    ///
    /// # Errors
    ///
    /// Returns an error along with a reference to the oldest entry if it does not satisfy the
    /// condition.
    pub fn pop_if<'b, F: FnMut(&T) -> bool>(
        &self,
        _cond: F,
        _barrier: &'b Barrier,
    ) -> Result<Option<Arc<Entry<T>>>, &'b T> {
        Ok(None)
    }

    /// Returns `true` if the [`Queue`] is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Queue;
    ///
    /// let queue: Queue<usize> = Queue::default();
    /// assert!(queue.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.newest.is_null(Acquire)
    }
}

/// [`Entry`] implements [`LinkedList`] to store an instance of `T` in a singly linked list.
pub struct Entry<T: 'static> {
    /// `instance` is always `Some` until [`Self::into_inner`] is called.
    instance: Option<T>,

    /// `next` points to the next entry in a linked list.
    next: AtomicArc<Self>,
}

impl<T: 'static> Entry<T> {
    /// Converts an [`Entry`] of type `T` into `T`.
    pub fn into_inner(mut self) -> T {
        self.instance.take().unwrap()
    }
}

impl<T: 'static> AsRef<T> for Entry<T> {
    fn as_ref(&self) -> &T {
        self.instance.as_ref().unwrap()
    }
}

impl<T: 'static> AsMut<T> for Entry<T> {
    fn as_mut(&mut self) -> &mut T {
        self.instance.as_mut().unwrap()
    }
}

impl<T: 'static + Debug> Debug for Entry<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("instance", &self.instance)
            .field("next", &self.next)
            .finish()
    }
}

impl<T: 'static> Deref for Entry<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.instance.as_ref().unwrap()
    }
}

impl<T: 'static> DerefMut for Entry<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.instance.as_mut().unwrap()
    }
}

impl<T: 'static + Display> Display for Entry<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(instance) = self.instance.as_ref() {
            write!(f, "Some({})", instance)
        } else {
            write!(f, "None")
        }
    }
}

impl<T: 'static> LinkedList for Entry<T> {
    fn link_ref(&self) -> &AtomicArc<Self> {
        &self.next
    }
}
