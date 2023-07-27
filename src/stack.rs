//! [`Stack`] is a lock-free concurrent last-in-first-out container.

use super::ebr::{AtomicArc, Guard, Ptr, Shared, Tag};
use super::linked_list::{Entry, LinkedList};
use std::fmt::{self, Debug};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};

/// [`Stack`] is a lock-free concurrent last-in-first-out container.
pub struct Stack<T> {
    /// `newest` points to the newest entry in the [`Stack`].
    newest: AtomicArc<Entry<T>>,
}

impl<T: 'static> Stack<T> {
    /// Pushes an instance of `T`.
    ///
    /// Returns a [`Shared`] holding a strong reference to the newly pushed entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Stack;
    ///
    /// let stack: Stack<usize> = Stack::default();
    ///
    /// assert_eq!(**stack.push(11), 11);
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
    /// use scc::Stack;
    ///
    /// let stack: Stack<usize> = Stack::default();
    ///
    /// stack.push(11);
    ///
    /// assert!(stack.push_if(17, |e| e.map_or(false, |x| **x == 11)).is_ok());
    /// assert!(stack.push_if(29, |e| e.map_or(false, |x| **x == 11)).is_err());
    /// ```
    #[inline]
    pub fn push_if<F: FnMut(Option<&Entry<T>>) -> bool>(
        &self,
        val: T,
        cond: F,
    ) -> Result<Shared<Entry<T>>, T> {
        self.push_if_internal(val, cond, &Guard::new())
    }

    /// Peeks the newest entry with the supplied [`Guard`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Guard;
    /// use scc::Stack;
    ///
    /// let stack: Stack<usize> = Stack::default();
    ///
    /// assert!(stack.peek_with(|v| v.is_none(), &Guard::new()));
    ///
    /// stack.push(37);
    /// stack.push(3);
    ///
    /// assert_eq!(stack.peek_with(|v| **v.unwrap(), &Guard::new()), 3);
    /// ```
    #[inline]
    pub fn peek_with<'g, R, F: FnOnce(Option<&'g Entry<T>>) -> R>(
        &self,
        reader: F,
        guard: &'g Guard,
    ) -> R {
        reader(
            self.cleanup_newest(self.newest.load(Acquire, guard), guard)
                .as_ref(),
        )
    }
}

impl<T> Stack<T> {
    /// Pushes an instance of `T` without checking the lifetime of `T`.
    ///
    /// Returns a [`Shared`] holding a strong reference to the newly pushed entry.
    ///
    /// # Safety
    ///
    /// `T::drop` can be run after the [`Stack`] is dropped, therefore it is safe only if `T::drop`
    /// does not access short-lived data or [`std::mem::needs_drop`] is `false` for `T`,
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Stack;
    ///
    /// let hello = String::from("hello");
    /// let stack: Stack<&str> = Stack::default();
    ///
    /// assert_eq!(unsafe { **stack.push_unchecked(hello.as_str()) }, "hello");
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
    /// checking the lifetime of `T`.
    ///
    /// # Errors
    ///
    /// Returns an error along with the supplied instance if the condition is not met.
    ///
    /// # Safety
    ///
    /// `T::drop` can be run after the [`Stack`] is dropped, therefore it is safe only if `T::drop`
    /// does not access short-lived data or [`std::mem::needs_drop`] is `false` for `T`,
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Stack;
    ///
    /// let hello = String::from("hello");
    /// let stack: Stack<&str> = Stack::default();
    ///
    /// assert!(unsafe { stack.push_if_unchecked(hello.as_str(), |e| e.is_none()).is_ok() });
    /// ```
    #[inline]
    pub unsafe fn push_if_unchecked<F: FnMut(Option<&Entry<T>>) -> bool>(
        &self,
        val: T,
        cond: F,
    ) -> Result<Shared<Entry<T>>, T> {
        self.push_if_internal(val, cond, &Guard::new())
    }

    /// Pops the newest entry.
    ///
    /// Returns `None` if the [`Stack`] is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Stack;
    ///
    /// let stack: Stack<usize> = Stack::default();
    ///
    /// stack.push(37);
    /// stack.push(3);
    /// stack.push(1);
    ///
    /// assert_eq!(stack.pop().map(|e| **e), Some(1));
    /// assert_eq!(stack.pop().map(|e| **e), Some(3));
    /// assert_eq!(stack.pop().map(|e| **e), Some(37));
    /// assert!(stack.pop().is_none());
    /// ```
    #[inline]
    pub fn pop(&self) -> Option<Shared<Entry<T>>> {
        match self.pop_if(|_| true) {
            Ok(result) => result,
            Err(_) => unreachable!(),
        }
    }

    /// Pops all the entries at once, and passes each one of the popped entries to the supplied
    /// closure.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Stack;
    ///
    /// let stack: Stack<usize> = Stack::default();
    ///
    /// stack.push(37);
    /// stack.push(3);
    ///
    /// let popped = stack.pop_all();
    ///
    /// stack.push(1);
    ///
    /// assert_eq!(stack.pop().map(|e| **e), Some(1));
    /// assert!(stack.pop().is_none());
    /// assert!(stack.is_empty());
    ///
    /// assert_eq!(popped.pop().map(|e| **e), Some(3));
    /// assert_eq!(popped.pop().map(|e| **e), Some(37));
    /// assert!(popped.pop().is_none());
    /// ```

    #[inline]
    #[must_use]
    pub fn pop_all(&self) -> Self {
        let head = self.newest.swap((None, Tag::None), AcqRel).0;
        Self {
            newest: head.map_or_else(AtomicArc::default, AtomicArc::from),
        }
    }

    /// Pops the newest entry if the entry satisfies the given condition.
    ///
    /// Returns `None` if the [`Stack`] is empty.
    ///
    /// # Errors
    ///
    /// Returns an error along with the newest entry if the given condition is not met.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Stack;
    ///
    /// let stack: Stack<usize> = Stack::default();
    ///
    /// stack.push(3);
    /// stack.push(1);
    ///
    /// assert!(stack.pop_if(|v| **v == 3).is_err());
    /// assert_eq!(stack.pop().map(|e| **e), Some(1));
    /// assert_eq!(stack.pop_if(|v| **v == 3).ok().and_then(|e| e).map(|e| **e), Some(3));
    ///
    /// assert!(stack.is_empty());
    /// ```
    #[inline]
    pub fn pop_if<F: FnMut(&Entry<T>) -> bool>(
        &self,
        mut cond: F,
    ) -> Result<Option<Shared<Entry<T>>>, Shared<Entry<T>>> {
        let guard = Guard::new();
        let mut newest_ptr = self.cleanup_newest(self.newest.load(Acquire, &guard), &guard);
        while !newest_ptr.is_null() {
            if let Some(newest_entry) = newest_ptr.get_shared() {
                if !newest_entry.is_deleted(Relaxed) && !cond(&*newest_entry) {
                    return Err(newest_entry);
                }
                if newest_entry.delete_self(Relaxed) {
                    self.cleanup_newest(newest_ptr, &guard);
                    return Ok(Some(newest_entry));
                }
            }
            newest_ptr = self.cleanup_newest(newest_ptr, &guard);
        }
        Ok(None)
    }

    /// Peeks the newest entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Stack;
    ///
    /// let stack: Stack<usize> = Stack::default();
    ///
    /// assert!(stack.peek(|v| v.is_none()));
    ///
    /// stack.push(37);
    /// stack.push(3);
    ///
    /// assert_eq!(stack.peek(|v| **v.unwrap()), 3);
    /// ```
    #[inline]
    pub fn peek<R, F: FnOnce(Option<&Entry<T>>) -> R>(&self, reader: F) -> R {
        let guard = Guard::new();
        reader(
            self.cleanup_newest(self.newest.load(Acquire, &guard), &guard)
                .as_ref(),
        )
    }

    /// Returns `true` if the [`Stack`] is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::Stack;
    ///
    /// let stack: Stack<usize> = Stack::default();
    /// assert!(stack.is_empty());
    ///
    /// stack.push(7);
    /// assert!(!stack.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        let guard = Guard::new();
        self.cleanup_newest(self.newest.load(Acquire, &guard), &guard)
            .is_null()
    }

    /// Pushes an entry into the [`Stack`].
    fn push_if_internal<F: FnMut(Option<&Entry<T>>) -> bool>(
        &self,
        val: T,
        mut cond: F,
        guard: &Guard,
    ) -> Result<Shared<Entry<T>>, T> {
        let mut newest_ptr = self.cleanup_newest(self.newest.load(Acquire, guard), guard);
        if !cond(newest_ptr.as_ref()) {
            // The condition is not met.
            return Err(val);
        }

        let mut new_entry = unsafe { Shared::new_unchecked(Entry::new(val)) };
        loop {
            new_entry
                .next()
                .swap((newest_ptr.get_shared(), Tag::None), Relaxed);
            let result = self.newest.compare_exchange(
                newest_ptr,
                (Some(new_entry.clone()), Tag::None),
                AcqRel,
                Acquire,
                guard,
            );
            match result {
                Ok(_) => return Ok(new_entry),
                Err((_, actual_ptr)) => {
                    newest_ptr = self.cleanup_newest(actual_ptr, guard);
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

    /// Cleans up logically removed entries that are attached to `newest`.
    fn cleanup_newest<'g>(
        &self,
        mut newest_ptr: Ptr<'g, Entry<T>>,
        guard: &'g Guard,
    ) -> Ptr<'g, Entry<T>> {
        while let Some(newest_entry) = newest_ptr.as_ref() {
            if newest_entry.is_deleted(Relaxed) {
                match self.newest.compare_exchange(
                    newest_ptr,
                    (
                        newest_entry.next_ptr(Acquire, guard).get_shared(),
                        Tag::None,
                    ),
                    AcqRel,
                    Acquire,
                    guard,
                ) {
                    Ok((_, ptr)) | Err((_, ptr)) => newest_ptr = ptr,
                }
            } else {
                break;
            }
        }
        newest_ptr
    }
}

impl<T: Clone> Clone for Stack<T> {
    #[inline]
    fn clone(&self) -> Self {
        let self_clone = Self::default();
        let guard = Guard::new();
        let mut current = self.newest.load(Acquire, &guard);
        let mut oldest: Option<Shared<Entry<T>>> = None;
        while let Some(entry) = current.as_ref() {
            let new_entry = unsafe { Shared::new_unchecked(Entry::new((**entry).clone())) };
            if let Some(oldest) = oldest.take() {
                oldest
                    .next()
                    .swap((Some(new_entry.clone()), Tag::None), Relaxed);
            } else {
                self_clone
                    .newest
                    .swap((Some(new_entry.clone()), Tag::None), Relaxed);
            }
            oldest.replace(new_entry);
            current = entry.next_ptr(Acquire, &guard);
        }
        self_clone
    }
}

impl<T: Debug> Debug for Stack<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_set();
        let guard = Guard::new();
        let mut current = self.newest.load(Acquire, &guard);
        while let Some(entry) = current.as_ref() {
            let next = entry.next_ptr(Acquire, &guard);
            d.entry(entry);
            current = next;
        }
        d.finish()
    }
}

impl<T> Default for Stack<T> {
    #[inline]
    fn default() -> Self {
        Self {
            newest: AtomicArc::default(),
        }
    }
}
