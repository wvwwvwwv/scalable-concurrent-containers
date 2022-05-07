//! [`Queue`] is a lock-free concurrent first-in-first-out queue.

use super::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};

use std::fmt::{Debug, Display};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

/// [`Queue`] is a lock-free concurrent first-in-first-out queue.
pub struct Queue<T: 'static> {
    /// `oldest` points to the oldest entry in the [`Queue`].
    oldest: AtomicArc<Entry<T>>,

    /// `newest` *eventually* points to the newest entry in the [`Queue`].
    newest: AtomicArc<Entry<T>>,
}

impl<T: 'static> Queue<T> {
    /// Pushes a new instance of `T`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Queue;
    ///
    /// let queue: Queue<usize> = Queue::default();
    ///
    /// queue.push(11);
    /// ```
    #[inline]
    pub fn push(&self, val: T) {
        let result = self.push_if_internal(val, |_| true, &Barrier::new());
        debug_assert!(result.is_ok());
    }

    /// Pushes a new instance of `T` if the newest entry satisfies the given condition.
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied instance if the condition is not met.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Queue;
    ///
    /// let queue: Queue<usize> = Queue::default();
    ///
    /// queue.push(11);
    ///
    /// assert!(queue.push_if(17, |e| e.map_or(false, |x| *x == 11)).is_ok());
    /// assert_eq!(queue.push_if(29, |e| e.map_or(false, |x| *x == 11)), Err(29));
    /// ```
    #[inline]
    pub fn push_if<F: FnMut(Option<&T>) -> bool>(&self, val: T, cond: F) -> Result<(), T> {
        self.push_if_internal(val, cond, &Barrier::new())
    }

    /// Pops the oldest entry.
    ///
    /// Returns `None` if the [`Queue`] is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Queue;
    ///
    /// let queue: Queue<usize> = Queue::default();
    ///
    /// queue.push(37);
    /// queue.push(3);
    ///
    /// assert_eq!(queue.pop().map(|e| **e), Some(37));
    /// assert_eq!(queue.pop().map(|e| **e), Some(3));
    /// assert!(queue.pop().is_none());
    /// ```
    #[inline]
    pub fn pop(&self) -> Option<Arc<Entry<T>>> {
        let barrier = Barrier::new();
        let mut current = self.oldest.load(Acquire, &barrier);
        while let Some(oldest_entry) = current.as_ref() {
            if !oldest_entry
                .next
                .update_tag_if(Tag::First, |t| t == Tag::None, Release)
            {
                // Failed to own the entry.
                current = self.cleanup(&barrier);
                continue;
            }
            match self.oldest.compare_exchange(
                current,
                (oldest_entry.next.get_arc(Acquire, &barrier), Tag::None),
                AcqRel,
                Acquire,
                &barrier,
            ) {
                Ok((oldest_entry, new_ptr)) => {
                    if new_ptr.is_null() {
                        // Reset `newest`.
                        self.newest.swap((None, Tag::None), Relaxed);
                    }
                    return oldest_entry;
                }
                Err((_, actual_ptr)) => {
                    current = actual_ptr;
                }
            }
        }
        None
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
    ///
    /// queue.push(7);
    /// assert!(!queue.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.newest.is_null(Acquire)
    }

    /// Pushes an entry into the [`Queue`].
    fn push_if_internal<F: FnMut(Option<&T>) -> bool>(
        &self,
        val: T,
        mut cond: F,
        barrier: &Barrier,
    ) -> Result<(), T> {
        let mut newest_ptr = self.newest.load(Acquire, barrier);
        if newest_ptr.is_null() {
            // Traverse from the oldest.
            newest_ptr = self.oldest.load(Acquire, barrier);
        }
        newest_ptr = Self::traverse(newest_ptr, barrier);

        if !cond(newest_ptr.as_ref().map(AsRef::as_ref)) {
            // The condition is not met.
            return Err(val);
        }

        let mut new_entry = Arc::new(Entry::new(val));
        loop {
            let result = if let Some(newest_entry) = newest_ptr.as_ref() {
                newest_entry.next.compare_exchange(
                    Ptr::null(),
                    (Some(new_entry.clone()), Tag::None),
                    AcqRel,
                    Acquire,
                    barrier,
                )
            } else {
                self.oldest.compare_exchange(
                    newest_ptr,
                    (Some(new_entry.clone()), Tag::None),
                    AcqRel,
                    Acquire,
                    barrier,
                )
            };
            match result {
                Ok(_) => {
                    self.update_newest(new_entry);
                    return Ok(());
                }
                Err((_, actual_ptr)) => {
                    newest_ptr = if actual_ptr.is_null() {
                        self.oldest.load(Acquire, barrier)
                    } else {
                        actual_ptr
                    };
                    newest_ptr = Self::traverse(newest_ptr, barrier);

                    if !cond(newest_ptr.as_ref().map(AsRef::as_ref)) {
                        // The condition is not met.
                        break;
                    }
                }
            }
        }

        // Extract the instance from the temporary entry.
        Err(unsafe { new_entry.get_mut().unwrap().take_inner() })
    }

    /// Cleans up marked entries.
    fn cleanup<'b>(&self, barrier: &'b Barrier) -> Ptr<'b, Entry<T>> {
        // TODO.
        self.oldest.load(Acquire, barrier)
    }

    /// Updates `newest`.
    fn update_newest(&self, entry: Arc<Entry<T>>) {
        self.newest.swap((Some(entry), Tag::None), AcqRel);
        if self.oldest.is_null(Relaxed) {
            // The `Queue` was emptied in the meantime.
            self.newest.swap((None, Tag::None), Release);
        }
    }

    /// Traverses the linked list to the end.
    fn traverse<'b>(start: Ptr<'b, Entry<T>>, barrier: &'b Barrier) -> Ptr<'b, Entry<T>> {
        let mut current = start;
        while let Some(entry) = current.as_ref() {
            let next = entry.next.load(Acquire, barrier);
            if next.is_null() {
                break;
            }
            current = next;
        }
        current
    }
}

impl<T: 'static> Default for Queue<T> {
    fn default() -> Self {
        Self {
            oldest: AtomicArc::default(),
            newest: AtomicArc::default(),
        }
    }
}

/// [`Entry`] stores an instance of `T` and a link to the next entry.
pub struct Entry<T: 'static> {
    /// `instance` is always `Some` until [`Self::into_inner`] is called.
    instance: Option<T>,

    /// `next` points to the next entry in a linked list.
    next: AtomicArc<Self>,
}

impl<T: 'static> Entry<T> {
    /// Extracts the instance of `T`.
    unsafe fn take_inner(&mut self) -> T {
        self.instance.take().unwrap()
    }

    /// Creates a new [`Entry`].
    fn new(val: T) -> Entry<T> {
        Entry {
            instance: Some(val),
            next: AtomicArc::default(),
        }
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
