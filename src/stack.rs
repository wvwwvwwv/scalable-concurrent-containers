//! [`Stack`] is a lock-free concurrent last-in-first-out container.

use super::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};
use super::linked_list::{Entry, LinkedList};

use std::fmt::{self, Debug};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};

/// [`Stack`] is a lock-free concurrent last-in-first-out container.
pub struct Stack<T: 'static> {
    /// `newest` points to the newest entry in the [`Stack`].
    newest: AtomicArc<Entry<T>>,
}

impl<T: 'static> Stack<T> {
    /// Pushes an instance of `T`.
    ///
    /// Returns an [`Arc`] holding a strong reference to the newly pushed entry.
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
    pub fn push(&self, val: T) -> Arc<Entry<T>> {
        match self.push_if_internal(val, |_| true, &Barrier::new()) {
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
    ) -> Result<Arc<Entry<T>>, T> {
        self.push_if_internal(val, cond, &Barrier::new())
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
    pub fn pop(&self) -> Option<Arc<Entry<T>>> {
        match self.pop_if(|_| true) {
            Ok(result) => result,
            Err(_) => unreachable!(),
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
    ) -> Result<Option<Arc<Entry<T>>>, Arc<Entry<T>>> {
        let barrier = Barrier::new();
        let mut newest_ptr = self.cleanup_newest(self.newest.load(Acquire, &barrier), &barrier);
        while !newest_ptr.is_null() {
            if let Some(newest_entry) = newest_ptr.get_arc() {
                if !newest_entry.is_deleted(Relaxed) && !cond(&*newest_entry) {
                    return Err(newest_entry);
                }
                if newest_entry.delete_self(Relaxed) {
                    self.cleanup_newest(newest_ptr, &barrier);
                    return Ok(Some(newest_entry));
                }
            }
            newest_ptr = self.cleanup_newest(newest_ptr, &barrier);
        }
        Ok(None)
    }

    /// Peeks the newest entry.
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
    /// assert!(stack.peek(|v| **v).is_none());
    ///
    /// stack.push(37);
    /// stack.push(3);
    ///
    /// assert_eq!(stack.peek(|v| **v), Some(3));
    /// ```
    #[inline]
    pub fn peek<R, F: FnOnce(&Entry<T>) -> R>(&self, reader: F) -> Option<R> {
        let barrier = Barrier::new();
        self.peek_with(reader, &barrier)
    }

    /// Peeks the newest entry with the supplied [`Barrier`].
    ///
    /// Returns `None` if the [`Stack`] is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    /// use scc::Stack;
    ///
    /// let stack: Stack<usize> = Stack::default();
    ///
    /// assert!(stack.peek_with(|v| **v, &Barrier::new()).is_none());
    ///
    /// stack.push(37);
    /// stack.push(3);
    ///
    /// assert_eq!(stack.peek_with(|v| **v, &Barrier::new()), Some(3));
    /// ```
    #[inline]
    pub fn peek_with<'b, R, F: FnOnce(&'b Entry<T>) -> R>(
        &self,
        reader: F,
        barrier: &'b Barrier,
    ) -> Option<R> {
        self.cleanup_newest(self.newest.load(Acquire, barrier), barrier)
            .as_ref()
            .map(reader)
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
        let barrier = Barrier::new();
        self.cleanup_newest(self.newest.load(Acquire, &barrier), &barrier)
            .is_null()
    }

    /// Pushes an entry into the [`Stack`].
    fn push_if_internal<F: FnMut(Option<&Entry<T>>) -> bool>(
        &self,
        val: T,
        mut cond: F,
        barrier: &Barrier,
    ) -> Result<Arc<Entry<T>>, T> {
        let mut newest_ptr = self.cleanup_newest(self.newest.load(Acquire, barrier), barrier);
        if !cond(newest_ptr.as_ref()) {
            // The condition is not met.
            return Err(val);
        }

        let mut new_entry = Arc::new(Entry::new(val));
        loop {
            new_entry
                .next()
                .swap((newest_ptr.get_arc(), Tag::None), Relaxed);
            let result = self.newest.compare_exchange(
                newest_ptr,
                (Some(new_entry.clone()), Tag::None),
                AcqRel,
                Acquire,
                barrier,
            );
            match result {
                Ok(_) => return Ok(new_entry),
                Err((_, actual_ptr)) => {
                    newest_ptr = self.cleanup_newest(actual_ptr, barrier);
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
    fn cleanup_newest<'b>(
        &self,
        mut newest_ptr: Ptr<'b, Entry<T>>,
        barrier: &'b Barrier,
    ) -> Ptr<'b, Entry<T>> {
        while let Some(newest_entry) = newest_ptr.as_ref() {
            if newest_entry.is_deleted(Relaxed) {
                match self.newest.compare_exchange(
                    newest_ptr,
                    (newest_entry.next_ptr(Acquire, barrier).get_arc(), Tag::None),
                    AcqRel,
                    Acquire,
                    barrier,
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

impl<T: 'static + Clone> Clone for Stack<T> {
    #[inline]
    fn clone(&self) -> Self {
        let cloned = Self::default();
        let barrier = Barrier::new();
        let mut current = self.newest.load(Acquire, &barrier);
        let mut oldest: Option<Arc<Entry<T>>> = None;
        while let Some(entry) = current.as_ref() {
            let new_entry = Arc::new(Entry::new((**entry).clone()));
            if let Some(oldest) = oldest.take() {
                oldest
                    .next()
                    .swap((Some(new_entry.clone()), Tag::None), Relaxed);
            } else {
                cloned
                    .newest
                    .swap((Some(new_entry.clone()), Tag::None), Relaxed);
            }
            oldest.replace(new_entry);
            current = entry.next_ptr(Acquire, &barrier);
        }
        cloned
    }
}

impl<T: 'static + Debug> Debug for Stack<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_set();
        let barrier = Barrier::new();
        let mut current = self.newest.load(Acquire, &barrier);
        while let Some(entry) = current.as_ref() {
            let next = entry.next_ptr(Acquire, &barrier);
            d.entry(entry);
            current = next;
        }
        d.finish()
    }
}

impl<T: 'static> Default for Stack<T> {
    #[inline]
    fn default() -> Self {
        Self {
            newest: AtomicArc::default(),
        }
    }
}
