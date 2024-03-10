//! [`Queue`] is a lock-free concurrent first-in-first-out container.

use super::ebr::{AtomicShared, Guard, Ptr, Shared, Tag};
use super::linked_list::Entry;
use std::fmt::{self, Debug};
use std::iter::FusedIterator;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

/// [`Queue`] is a lock-free concurrent first-in-first-out container.
pub struct Queue<T> {
    /// `oldest` points to the oldest entry in the [`Queue`].
    oldest: AtomicShared<Entry<T>>,

    /// `newest` *eventually* points to the newest entry in the [`Queue`].
    newest: AtomicShared<Entry<T>>,
}

/// An iterator over the entries of a [`Queue`].
///
/// [`Iter`] reads the oldest entry first.
pub struct Iter<'g, T> {
    current: Ptr<'g, Entry<T>>,
    guard: &'g Guard,
}

impl<T: 'static> Queue<T> {
    /// Pushes an instance of `T`.
    ///
    /// Returns a [`Shared`] holding a strong reference to the newly pushed entry.
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
    pub fn push(&self, val: T) -> Shared<Entry<T>> {
        match self.push_if_internal(val, |_| true, &Guard::new()) {
            Ok(entry) => entry,
            Err(_) => {
                unreachable!();
            }
        }
    }

    /// Pushes an instance of `T` if the newest entry satisfies the given condition.
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
    /// assert!(queue.push_if(17, |e| e.map_or(false, |x| **x == 11)).is_ok());
    /// assert!(queue.push_if(29, |e| e.map_or(false, |x| **x == 11)).is_err());
    /// ```
    #[inline]
    pub fn push_if<F: FnMut(Option<&Entry<T>>) -> bool>(
        &self,
        val: T,
        cond: F,
    ) -> Result<Shared<Entry<T>>, T> {
        self.push_if_internal(val, cond, &Guard::new())
    }

    /// Returns a guarded reference to the oldest entry.
    ///
    /// Returns `None` if the [`Queue`] is empty. The returned reference can survive as long as the
    /// associated [`Guard`] is alive.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Guard;
    /// use scc::Queue;
    ///
    /// let queue: Queue<usize> = Queue::default();
    ///
    /// assert!(queue.peek(&Guard::new()).is_none());
    ///
    /// queue.push(37);
    /// queue.push(3);
    ///
    /// assert_eq!(**queue.peek(&Guard::new()).unwrap(), 37);
    /// ```
    #[inline]
    pub fn peek<'g>(&self, guard: &'g Guard) -> Option<&'g Entry<T>> {
        let mut current = self.oldest.load(Acquire, guard);
        while let Some(oldest_entry) = current.as_ref() {
            if oldest_entry.is_deleted() {
                current = self.cleanup_oldest(guard);
                continue;
            }
            return Some(oldest_entry);
        }
        None
    }
}

impl<T> Queue<T> {
    /// Pushes an instance of `T` without checking the lifetime of `T`.
    ///
    /// Returns a [`Shared`] holding a strong reference to the newly pushed entry.
    ///
    /// # Safety
    ///
    /// `T::drop` can be run after the [`Queue`] is dropped, therefore it is safe only if `T::drop`
    /// does not access short-lived data or [`std::mem::needs_drop`] is `false` for `T`,
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Queue;
    ///
    /// let hello = String::from("hello");
    /// let queue: Queue<&str> = Queue::default();
    ///
    /// assert_eq!(unsafe { **queue.push_unchecked(hello.as_str()) }, "hello");
    /// ```
    #[inline]
    pub unsafe fn push_unchecked(&self, val: T) -> Shared<Entry<T>> {
        match self.push_if_internal(val, |_| true, &Guard::new()) {
            Ok(entry) => entry,
            Err(_) => {
                unreachable!();
            }
        }
    }

    /// Pushes an instance of `T` if the newest entry satisfies the given condition without
    /// checking the lifetime of `T`
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied instance if the condition is not met.
    ///
    /// # Safety
    ///
    /// `T::drop` can be run after the [`Queue`] is dropped, therefore it is safe only if `T::drop`
    /// does not access short-lived data or [`std::mem::needs_drop`] is `false` for `T`,
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Queue;
    ///
    /// let hello = String::from("hello");
    /// let queue: Queue<&str> = Queue::default();
    ///
    /// assert!(unsafe { queue.push_if_unchecked(hello.as_str(), |e| e.is_none()).is_ok() });
    /// ```
    #[inline]
    pub unsafe fn push_if_unchecked<F: FnMut(Option<&Entry<T>>) -> bool>(
        &self,
        val: T,
        cond: F,
    ) -> Result<Shared<Entry<T>>, T> {
        self.push_if_internal(val, cond, &Guard::new())
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
    /// queue.push(1);
    ///
    /// assert_eq!(queue.pop().map(|e| **e), Some(37));
    /// assert_eq!(queue.pop().map(|e| **e), Some(3));
    /// assert_eq!(queue.pop().map(|e| **e), Some(1));
    /// assert!(queue.pop().is_none());
    /// ```
    #[inline]
    pub fn pop(&self) -> Option<Shared<Entry<T>>> {
        match self.pop_if(|_| true) {
            Ok(result) => result,
            Err(_) => unreachable!(),
        }
    }

    /// Pops the oldest entry if the entry satisfies the given condition.
    ///
    /// Returns `None` if the [`Queue`] is empty.
    ///
    /// # Errors
    ///
    /// Returns an error along with the oldest entry if the given condition is not met.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Queue;
    ///
    /// let queue: Queue<usize> = Queue::default();
    ///
    /// queue.push(3);
    /// queue.push(1);
    ///
    /// assert!(queue.pop_if(|v| **v == 1).is_err());
    /// assert_eq!(queue.pop().map(|e| **e), Some(3));
    /// assert_eq!(queue.pop_if(|v| **v == 1).ok().and_then(|e| e).map(|e| **e), Some(1));
    /// ```
    #[inline]
    pub fn pop_if<F: FnMut(&Entry<T>) -> bool>(
        &self,
        mut cond: F,
    ) -> Result<Option<Shared<Entry<T>>>, Shared<Entry<T>>> {
        let guard = Guard::new();
        let mut current = self.oldest.load(Acquire, &guard);
        while !current.is_null() {
            if let Some(oldest_entry) = current.get_shared() {
                if !oldest_entry.is_deleted() && !cond(&*oldest_entry) {
                    return Err(oldest_entry);
                }
                if oldest_entry.delete_self() {
                    self.cleanup_oldest(&guard);
                    return Ok(Some(oldest_entry));
                }
            }
            current = self.cleanup_oldest(&guard);
        }
        Ok(None)
    }

    /// Peeks the oldest entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Queue;
    ///
    /// let queue: Queue<usize> = Queue::default();
    ///
    /// assert!(queue.peek_with(|v| v.is_none()));
    ///
    /// queue.push(37);
    /// queue.push(3);
    ///
    /// assert_eq!(queue.peek_with(|v| **v.unwrap()), 37);
    /// ```
    #[inline]
    pub fn peek_with<R, F: FnOnce(Option<&Entry<T>>) -> R>(&self, reader: F) -> R {
        let guard = Guard::new();
        let mut current = self.oldest.load(Acquire, &guard);
        while let Some(oldest_entry) = current.as_ref() {
            if oldest_entry.is_deleted() {
                current = self.cleanup_oldest(&guard);
                continue;
            }
            return reader(Some(oldest_entry));
        }
        reader(None)
    }

    /// Returns the number of entries in the [`Queue`].
    ///
    /// This method iterates over all the entries in the [`Queue`] to count them, therefore its
    /// time complexity is `O(N)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Queue;
    ///
    /// let queue: Queue<usize> = Queue::default();
    /// assert_eq!(queue.len(), 0);
    ///
    /// queue.push(7);
    /// queue.push(11);
    /// assert_eq!(queue.len(), 2);
    ///
    /// queue.pop();
    /// queue.pop();
    /// assert_eq!(queue.len(), 0);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        self.iter(&Guard::new()).count()
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

    /// Returns an [`Iter`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Guard;
    /// use scc::Queue;
    ///
    /// let queue: Queue<usize> = Queue::default();
    /// assert_eq!(queue.iter(&Guard::new()).count(), 0);
    ///
    /// queue.push(7);
    /// queue.push(11);
    /// queue.push(17);
    ///
    /// let guard = Guard::new();
    /// let mut iter = queue.iter(&guard);
    /// assert_eq!(*iter.next().unwrap(), 7);
    /// assert_eq!(*iter.next().unwrap(), 11);
    /// assert_eq!(*iter.next().unwrap(), 17);
    /// assert!(iter.next().is_none());
    /// ```
    #[inline]
    pub fn iter<'g>(&self, guard: &'g Guard) -> Iter<'g, T> {
        Iter {
            current: self.cleanup_oldest(guard),
            guard,
        }
    }

    /// Pushes an entry into the [`Queue`].
    fn push_if_internal<F: FnMut(Option<&Entry<T>>) -> bool>(
        &self,
        val: T,
        mut cond: F,
        guard: &Guard,
    ) -> Result<Shared<Entry<T>>, T> {
        let mut newest_ptr = self.newest.load(Acquire, guard);
        if newest_ptr.is_null() {
            // Traverse from the oldest.
            newest_ptr = self.oldest.load(Acquire, guard);
        }
        newest_ptr = Self::traverse(newest_ptr, guard);

        if !cond(newest_ptr.as_ref()) {
            // The condition is not met.
            return Err(val);
        }

        let mut new_entry = unsafe { Shared::new_unchecked(Entry::new(val)) };
        loop {
            let result = if let Some(newest_entry) = newest_ptr.as_ref() {
                newest_entry.next().compare_exchange(
                    Ptr::null(),
                    (Some(new_entry.clone()), Tag::None),
                    AcqRel,
                    Acquire,
                    guard,
                )
            } else {
                self.oldest.compare_exchange(
                    newest_ptr,
                    (Some(new_entry.clone()), Tag::None),
                    AcqRel,
                    Acquire,
                    guard,
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
                        self.cleanup_oldest(guard)
                    } else if actual_ptr.is_null() {
                        self.oldest.load(Acquire, guard)
                    } else {
                        actual_ptr
                    };
                    newest_ptr = Self::traverse(newest_ptr, guard);

                    if !cond(newest_ptr.as_ref()) {
                        // The condition is not met.
                        break;
                    }
                }
            }
        }

        // Extract the instance from the temporary entry.
        Err(unsafe { new_entry.get_mut().unwrap_unchecked().take_inner() })
    }

    /// Cleans up logically removed entries that are attached to `oldest`.
    fn cleanup_oldest<'g>(&self, guard: &'g Guard) -> Ptr<'g, Entry<T>> {
        let oldest_ptr = self.oldest.load(Acquire, guard);
        if let Some(oldest_entry) = oldest_ptr.as_ref() {
            if oldest_entry.is_deleted() {
                match self.oldest.compare_exchange(
                    oldest_ptr,
                    (oldest_entry.next_ptr(guard).get_shared(), Tag::None),
                    AcqRel,
                    Acquire,
                    guard,
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
    fn traverse<'g>(start: Ptr<'g, Entry<T>>, guard: &'g Guard) -> Ptr<'g, Entry<T>> {
        let mut current = start;
        while let Some(entry) = current.as_ref() {
            let next = entry.next_ptr(guard);
            if next.is_null() {
                break;
            }
            current = next;
        }
        current
    }
}

impl<T: Clone> Clone for Queue<T> {
    #[inline]
    fn clone(&self) -> Self {
        let self_clone = Self::default();
        let guard = Guard::new();
        let mut current = self.oldest.load(Acquire, &guard);
        while let Some(entry) = current.as_ref() {
            let next = entry.next_ptr(&guard);
            let _result = self_clone.push_if_internal((**entry).clone(), |_| true, &guard);
            current = next;
        }
        self_clone
    }
}

impl<T: Debug> Debug for Queue<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_set();
        let guard = Guard::new();
        let mut current = self.oldest.load(Acquire, &guard);
        while let Some(entry) = current.as_ref() {
            let next = entry.next_ptr(&guard);
            d.entry(entry);
            current = next;
        }
        d.finish()
    }
}

impl<T> Default for Queue<T> {
    #[inline]
    fn default() -> Self {
        Self {
            oldest: AtomicShared::default(),
            newest: AtomicShared::default(),
        }
    }
}

impl<'g, T> FusedIterator for Iter<'g, T> {}

impl<'g, T> Iterator for Iter<'g, T> {
    type Item = &'g T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(current) = self.current.as_ref() {
            self.current = current.next_ptr(self.guard);
            Some(current)
        } else {
            None
        }
    }
}
