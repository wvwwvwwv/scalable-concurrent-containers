//! [`Queue`] is a lock-free concurrent first-in-first-out queue.

use super::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};

use std::fmt::{Debug, Display};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

/// [`Queue`] is a lock-free concurrent first-in-first-out queue.
#[derive(Debug)]
pub struct Queue<T: 'static> {
    /// `oldest` points to the oldest entry in the [`Queue`].
    oldest: AtomicArc<Entry<T>>,

    /// `newest` *eventually* points to the newest entry in the [`Queue`].
    newest: AtomicArc<Entry<T>>,
}

impl<T: 'static> Queue<T> {
    /// Pushes a new instance of `T`.
    ///
    /// Returns an [`Arc`] holding a strong reference to the newly pushed entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Queue;
    ///
    /// let queue: Queue<usize> = Queue::default();
    ///
    /// assert_eq!(**queue.push(11), 11);
    /// ```
    #[inline]
    pub fn push(&self, val: T) -> Arc<Entry<T>> {
        match self.push_if_internal(val, |_| true, &Barrier::new()) {
            Ok(entry) => entry,
            Err(_) => {
                unreachable!();
            }
        }
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
    /// assert!(queue.push_if(29, |e| e.map_or(false, |x| *x == 11)).is_err());
    /// ```
    #[inline]
    pub fn push_if<F: FnMut(Option<&T>) -> bool>(
        &self,
        val: T,
        cond: F,
    ) -> Result<Arc<Entry<T>>, T> {
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
        while !current.is_null() {
            if let Some(oldest_entry) = current.get_arc() {
                if oldest_entry.try_pop() {
                    self.cleanup_oldest(&barrier);
                    return Some(oldest_entry);
                }
            }
            current = self.cleanup_oldest(&barrier);
        }
        None
    }

    /// Peeks the oldest entry.
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
    /// assert!(queue.peek(|v| *v).is_none());
    ///
    /// queue.push(37);
    /// queue.push(3);
    ///
    /// assert_eq!(queue.peek(|v| *v), Some(37));
    /// ```
    #[inline]
    pub fn peek<R, F: FnOnce(&T) -> R>(&self, reader: F) -> Option<R> {
        let barrier = Barrier::new();
        let mut current = self.oldest.load(Acquire, &barrier);
        while let Some(oldest_entry) = current.as_ref() {
            if oldest_entry.is_popped() {
                current = self.cleanup_oldest(&barrier);
                continue;
            }
            return Some(reader(&*oldest_entry));
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
    ) -> Result<Arc<Entry<T>>, T> {
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
                    self.newest
                        .swap((Some(new_entry.clone()), Tag::None), AcqRel);
                    if self.oldest.is_null(Relaxed) {
                        // The `Queue` was emptied in the meantime.
                        self.newest.swap((None, Tag::None), Release);
                    }
                    return Ok(new_entry);
                }
                Err((_, actual_ptr)) => {
                    newest_ptr = if actual_ptr.tag() == Tag::First {
                        self.cleanup_oldest(barrier)
                    } else if actual_ptr.is_null() {
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

    /// Cleans up logically popped entries.
    fn cleanup_oldest<'b>(&self, barrier: &'b Barrier) -> Ptr<'b, Entry<T>> {
        let oldest_ptr = self.oldest.load(Acquire, barrier);
        if let Some(oldest_entry) = oldest_ptr.as_ref() {
            if oldest_entry.is_popped() {
                match self.oldest.compare_exchange(
                    oldest_ptr,
                    (oldest_entry.next.get_arc(Acquire, barrier), Tag::None),
                    AcqRel,
                    Acquire,
                    barrier,
                ) {
                    Ok((_, new_ptr)) => {
                        if new_ptr.is_null() {
                            // Reset `newest`.
                            self.newest.swap((None, Tag::None), Relaxed);
                        }
                        return new_ptr;
                    }
                    Err((_, actual_ptr)) => {
                        return actual_ptr;
                    }
                }
            }
        }
        oldest_ptr
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
    /// Tries to pop the entry from its associated [`Queue`].
    ///
    /// Popped entries are only logically removed from the [`Queue`] and they will be eventually
    /// removed from the [`Queue`] on a subsequent call to [`Queue::pop`] or [`Queue::peek`].
    /// `false` is returned if the entry has already been popped.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Queue;
    ///
    /// let queue: Queue<usize> = Queue::default();
    ///
    /// let entry = queue.push(7);
    /// queue.push(11);
    ///
    /// assert!(entry.try_pop());
    /// assert!(!entry.try_pop());
    ///
    /// assert_eq!(queue.peek(|v| *v), Some(11));
    /// ```
    #[inline]
    pub fn try_pop(&self) -> bool {
        self.next
            .update_tag_if(Tag::First, |t| t == Tag::None, Release)
    }

    /// Checks if the entry has been popped.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Queue;
    ///
    /// let queue: Queue<usize> = Queue::default();
    ///
    /// let entry = queue.push(7);
    /// assert!(!entry.is_popped());
    ///
    /// assert_eq!(queue.pop().map(|e| **e), Some(7));
    /// assert!(entry.is_popped());
    /// ```
    #[inline]
    pub fn is_popped(&self) -> bool {
        self.next.tag(Relaxed) == Tag::First
    }

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

impl<T: Eq + 'static> Eq for Entry<T> {}

impl<T: PartialEq + 'static> PartialEq for Entry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
    }
}
