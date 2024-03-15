use super::ebr::{AtomicShared, Guard, Ptr, Shared, Tag};
use std::fmt::{self, Debug, Display};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering::{self, Acquire, Relaxed, Release};

/// [`LinkedList`] is a type trait implementing a lock-free singly linked list.
pub trait LinkedList: Sized {
    /// Returns a reference to the forward link.
    ///
    /// The pointer value may be tagged if [`Self::mark`] or [`Self::delete_self`] has been
    /// invoked. The [`AtomicShared`] must only be updated through [`LinkedList`] in order to keep
    /// the linked list consistent.
    fn link_ref(&self) -> &AtomicShared<Self>;

    /// Returns `true` if `self` is reachable and not marked.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::LinkedList;
    /// use scc::ebr::{AtomicShared, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// #[derive(Default)]
    /// struct L(AtomicShared<L>, usize);
    /// impl LinkedList for L {
    ///     fn link_ref(&self) -> &AtomicShared<L> {
    ///         &self.0
    ///     }
    /// }
    ///
    /// let head: L = L::default();
    /// assert!(head.is_clear(Relaxed));
    /// assert!(head.mark(Relaxed));
    /// assert!(!head.is_clear(Relaxed));
    /// assert!(head.delete_self(Relaxed));
    /// assert!(!head.is_clear(Relaxed));
    /// ```
    #[inline]
    fn is_clear(&self, order: Ordering) -> bool {
        is_clear_entry(self.link_ref(), order)
    }

    /// Marks `self` with an internal flag to denote that `self` is in a special state.
    ///
    /// Returns `false` if a flag has already been marked on `self`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::LinkedList;
    /// use scc::ebr::AtomicShared;
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// #[derive(Default)]
    /// struct L(AtomicShared<L>, usize);
    /// impl LinkedList for L {
    ///     fn link_ref(&self) -> &AtomicShared<L> {
    ///         &self.0
    ///     }
    /// }
    ///
    /// let head: L = L::default();
    /// assert!(head.mark(Relaxed));
    /// ```
    #[inline]
    fn mark(&self, order: Ordering) -> bool {
        mark_entry(self.link_ref(), order)
    }

    /// Removes any mark from `self`.
    ///
    /// Returns `false` if no flag has been marked on `self`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::LinkedList;
    /// use scc::ebr::AtomicShared;
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// #[derive(Default)]
    /// struct L(AtomicShared<L>, usize);
    /// impl LinkedList for L {
    ///     fn link_ref(&self) -> &AtomicShared<L> {
    ///         &self.0
    ///     }
    /// }
    ///
    /// let head: L = L::default();
    /// assert!(!head.unmark(Relaxed));
    /// assert!(head.mark(Relaxed));
    /// assert!(head.unmark(Relaxed));
    /// assert!(!head.is_marked(Relaxed));
    /// ```
    #[inline]
    fn unmark(&self, order: Ordering) -> bool {
        unmark_entry(self.link_ref(), order)
    }

    /// Returns `true` if `self` has a mark on it.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::LinkedList;
    /// use scc::ebr::AtomicShared;
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// #[derive(Default)]
    /// struct L(AtomicShared<L>, usize);
    /// impl LinkedList for L {
    ///     fn link_ref(&self) -> &AtomicShared<L> {
    ///         &self.0
    ///     }
    /// }
    ///
    /// let head: L = L::default();
    /// assert!(!head.is_marked(Relaxed));
    /// assert!(head.mark(Relaxed));
    /// assert!(head.is_marked(Relaxed));
    /// ```
    #[inline]
    fn is_marked(&self, order: Ordering) -> bool {
        is_marked_entry(self.link_ref(), order)
    }

    /// Deletes `self`.
    ///
    /// Returns `false` if `self` already has `deleted` marked on it.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::LinkedList;
    /// use scc::ebr::{AtomicShared, Guard, Shared};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// #[derive(Default)]
    /// struct L(AtomicShared<L>, usize);
    /// impl LinkedList for L {
    ///     fn link_ref(&self) -> &AtomicShared<L> {
    ///         &self.0
    ///     }
    /// }
    ///
    /// let guard = Guard::new();
    ///
    /// let head: L = L::default();
    /// let tail: Shared<L> = Shared::new(L::default());
    /// assert!(head.push_back(tail.clone(), false, Relaxed, &guard).is_ok());
    ///
    /// tail.delete_self(Relaxed);
    /// assert!(head.next_ptr(Relaxed, &guard).as_ref().is_none());
    /// ```
    #[inline]
    fn delete_self(&self, order: Ordering) -> bool {
        delete_entry(self.link_ref(), order)
    }

    /// Returns `true` if `self` has been deleted.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::LinkedList;
    /// use scc::ebr::AtomicShared;
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// #[derive(Default)]
    /// struct L(AtomicShared<L>, usize);
    /// impl LinkedList for L {
    ///     fn link_ref(&self) -> &AtomicShared<L> {
    ///         &self.0
    ///     }
    /// }
    ///
    /// let entry: L = L::default();
    /// assert!(!entry.is_deleted(Relaxed));
    /// entry.delete_self(Relaxed);
    /// assert!(entry.is_deleted(Relaxed));
    /// ```
    #[inline]
    fn is_deleted(&self, order: Ordering) -> bool {
        is_deleted_entry(self.link_ref(), order)
    }

    /// Appends the given entry to `self` and returns a pointer to the entry.
    ///
    /// If `mark` is given `true`, it atomically marks an internal flag on `self` when updating
    /// the linked list, otherwise it removes marks.
    ///
    /// # Errors
    ///
    /// Returns the supplied [`Shared`] when it finds `self` deleted.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::LinkedList;
    /// use scc::ebr::{AtomicShared, Guard, Shared};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// #[derive(Default)]
    /// struct L(AtomicShared<L>, usize);
    /// impl LinkedList for L {
    ///     fn link_ref(&self) -> &AtomicShared<L> {
    ///         &self.0
    ///     }
    /// }
    ///
    /// let guard = Guard::new();
    ///
    /// let head: L = L::default();
    /// assert!(head.push_back(Shared::new(L::default()), true, Relaxed, &guard).is_ok());
    /// assert!(head.is_marked(Relaxed));
    /// assert!(head.push_back(Shared::new(L::default()), false, Relaxed, &guard).is_ok());
    /// assert!(!head.is_marked(Relaxed));
    ///
    /// head.delete_self(Relaxed);
    /// assert!(!head.is_marked(Relaxed));
    /// assert!(head.push_back(Shared::new(L::default()), false, Relaxed, &guard).is_err());
    /// ```
    #[inline]
    fn push_back<'g>(
        &self,
        entry: Shared<Self>,
        mark: bool,
        order: Ordering,
        guard: &'g Guard,
    ) -> Result<Ptr<'g, Self>, Shared<Self>> {
        push_back_entry(self.link_ref(), entry, mark, |l| l.link_ref(), order, guard)
    }

    /// Returns the closest next valid entry.
    ///
    /// It unlinks deleted entries until it reaches a valid one.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::LinkedList;
    /// use scc::ebr::{AtomicShared, Guard, Shared};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// #[derive(Default)]
    /// struct L(AtomicShared<L>, usize);
    /// impl LinkedList for L {
    ///     fn link_ref(&self) -> &AtomicShared<L> {
    ///         &self.0
    ///     }
    /// }
    ///
    /// let guard = Guard::new();
    ///
    /// let head: L = L::default();
    /// assert!(
    ///     head.push_back(Shared::new(L(AtomicShared::null(), 1)), false, Relaxed, &guard).is_ok());
    /// head.mark(Relaxed);
    ///
    /// let next_ptr = head.next_ptr(Relaxed, &guard);
    /// assert_eq!(next_ptr.as_ref().unwrap().1, 1);
    /// assert!(head.is_marked(Relaxed));
    /// ```
    #[inline]
    fn next_ptr<'g>(&self, order: Ordering, guard: &'g Guard) -> Ptr<'g, Self> {
        next(self.link_ref(), &|l| l.link_ref(), order, guard)
    }
}

/// [`Entry`] stores an instance of `T` and a link to the next entry.
pub struct Entry<T> {
    /// `instance` is always `Some` unless [`Self::take_inner`] is called.
    instance: Option<T>,

    /// `next` points to the next entry in a linked list.
    next: AtomicShared<Self>,
}

impl<T> Entry<T> {
    #[inline]
    pub(super) fn new(val: T) -> Self {
        Self {
            instance: Some(val),
            next: AtomicShared::default(),
        }
    }

    #[inline]
    pub(super) fn delete_self(&self) -> bool {
        delete_entry(&self.next, Relaxed)
    }

    #[inline]
    pub(super) fn is_deleted(&self) -> bool {
        is_deleted_entry(&self.next, Relaxed)
    }

    #[inline]
    pub(super) fn next_ptr<'g>(&self, guard: &'g Guard) -> Ptr<'g, Self> {
        next(&self.next, &|l| &l.next, Acquire, guard)
    }

    /// Extracts the inner instance of `T`.
    #[inline]
    pub(super) unsafe fn take_inner(&mut self) -> T {
        self.instance.take().unwrap_unchecked()
    }

    /// Returns a reference to `next`.
    #[inline]
    pub(super) fn next(&self) -> &AtomicShared<Self> {
        &self.next
    }
}

impl<T> AsRef<T> for Entry<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        unsafe { self.instance.as_ref().unwrap_unchecked() }
    }
}

impl<T> AsMut<T> for Entry<T> {
    #[inline]
    fn as_mut(&mut self) -> &mut T {
        unsafe { self.instance.as_mut().unwrap_unchecked() }
    }
}

impl<T: Clone> Clone for Entry<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            instance: self.instance.clone(),
            next: AtomicShared::default(),
        }
    }
}

impl<T: Debug> Debug for Entry<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entry")
            .field("instance", &self.instance)
            .field("next", &self.next)
            .field("removed", &self.is_deleted())
            .finish()
    }
}

impl<T> Deref for Entry<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { self.instance.as_ref().unwrap_unchecked() }
    }
}

impl<T> DerefMut for Entry<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.instance.as_mut().unwrap_unchecked() }
    }
}

impl<T> Drop for Entry<T> {
    #[inline]
    fn drop(&mut self) {
        if !self.next.is_null(Relaxed) {
            self.next_ptr(&Guard::new());
        }
    }
}

impl<T: Display> Display for Entry<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(instance) = self.instance.as_ref() {
            write!(f, "Some({instance})")
        } else {
            write!(f, "None")
        }
    }
}

impl<T: Eq> Eq for Entry<T> {}

impl<T: PartialEq> PartialEq for Entry<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
    }
}

/// Returns `true` if `current` is reachable and not marked.
fn is_clear_entry<T>(current: &AtomicShared<T>, order: Ordering) -> bool {
    current.tag(order) == Tag::None
}

/// Marks `current` with an internal flag to denote that `current` is in a user-defined state.
fn mark_entry<T>(current: &AtomicShared<T>, order: Ordering) -> bool {
    current.update_tag_if(Tag::First, |ptr| ptr.tag() == Tag::None, order, Relaxed)
}

/// Removes any mark from `current`.
fn unmark_entry<T>(current: &AtomicShared<T>, order: Ordering) -> bool {
    current.update_tag_if(Tag::None, |ptr| ptr.tag() == Tag::First, order, Relaxed)
}

/// Returns `true` if `current` has a mark on it.
fn is_marked_entry<T>(current: &AtomicShared<T>, order: Ordering) -> bool {
    current.tag(order) == Tag::First
}

/// Deletes `current` from the linked list.
fn delete_entry<T>(current: &AtomicShared<T>, order: Ordering) -> bool {
    current.update_tag_if(
        Tag::Second,
        |ptr| {
            let tag = ptr.tag();
            tag == Tag::None || tag == Tag::First
        },
        order,
        Relaxed,
    )
}

/// Checks if `current` has been deleted from the linked list.
fn is_deleted_entry<T>(current: &AtomicShared<T>, order: Ordering) -> bool {
    let tag = current.tag(order);
    tag == Tag::Second || tag == Tag::Both
}

/// Appends the given entry to `current` and returns a pointer to the entry.
fn push_back_entry<'g, T, F: Fn(&T) -> &AtomicShared<T>>(
    current: &AtomicShared<T>,
    mut entry: Shared<T>,
    mark: bool,
    state_getter: F,
    order: Ordering,
    guard: &'g Guard,
) -> Result<Ptr<'g, T>, Shared<T>> {
    let new_tag = if mark { Tag::First } else { Tag::None };
    let mut next_ptr = current.load(Relaxed, guard);

    loop {
        let tag = next_ptr.tag();
        if tag == Tag::Second || tag == Tag::Both {
            break;
        }

        state_getter(&*entry).swap((next_ptr.get_shared(), Tag::None), Relaxed);
        match current.compare_exchange_weak(next_ptr, (Some(entry), new_tag), order, Relaxed, guard)
        {
            Ok((_, updated)) => {
                return Ok(updated);
            }
            Err((passed, actual)) => {
                entry = unsafe { passed.unwrap_unchecked() };
                next_ptr = actual;
            }
        }
    }

    // `current` has been deleted.
    Err(entry)
}

/// Returns the closest next valid [`Ptr`] reachable from `current`.
///
/// This removes any invalid entry between `current` and the returned [`Ptr`].
fn next<'g, T, F: Fn(&T) -> &AtomicShared<T>>(
    current: &AtomicShared<T>,
    state_getter: &F,
    order: Ordering,
    guard: &'g Guard,
) -> Ptr<'g, T> {
    let current_state = current.load(order, guard);
    let mut entry_ptr = current_state;
    let mut cleanup_target_range = (Ptr::null(), Ptr::null());
    let mut cleanup_range_ended = current_state.tag() == Tag::Both;
    let next_valid_ptr = loop {
        if let Some(entry) = entry_ptr.as_ref() {
            let entry_state = state_getter(entry).load(order, guard);
            let tag = entry_state.tag();
            if tag == Tag::None || tag == Tag::First {
                break entry_ptr;
            } else if tag == Tag::Second
                && !cleanup_range_ended
                && state_getter(entry).update_tag_if(
                    Tag::Both,
                    |ptr| ptr == entry_state,
                    Relaxed,
                    Relaxed,
                )
            {
                if cleanup_target_range.0.is_null() {
                    cleanup_target_range.0 = entry_ptr.without_tag();
                }
                cleanup_target_range.1 = entry_ptr.without_tag();
            } else {
                cleanup_range_ended = true;
            }
            entry_ptr = entry_state;
            continue;
        }
        break entry_ptr;
    };

    // Update its link if an invalid entry was found.
    if current_state.tag() != Tag::Both && current_state != next_valid_ptr {
        let next_valid_entry = next_valid_ptr.get_shared();
        if next_valid_entry.is_none() == next_valid_ptr.is_null() {
            // Keep the tag value.
            if let Ok((Some(next), _)) = current.compare_exchange(
                current_state,
                (next_valid_entry.clone(), current_state.tag()),
                Release,
                Relaxed,
                guard,
            ) {
                let _: bool = next.release(guard);

                // Now `cleanup_range` is unreachable to new readers.
                entry_ptr = cleanup_target_range.0;
                while let Some(entry) = entry_ptr.as_ref() {
                    debug_assert_eq!(state_getter(entry).tag(Relaxed), Tag::Both);
                    let (next, _) =
                        state_getter(entry).swap((next_valid_entry.clone(), Tag::Second), Release);
                    if entry_ptr.without_tag() == cleanup_target_range.1 {
                        break;
                    }
                    entry_ptr = next.map_or_else(Ptr::null, |n| {
                        let ptr = n.get_guarded_ptr(guard);
                        let _: bool = n.release(guard);
                        ptr
                    });
                }

                // Everything went smoothly.
                return next_valid_ptr;
            }
        }

        // Entries in `cleanup_range` need to be cleaned up.
        entry_ptr = cleanup_target_range.0;
        while let Some(entry) = entry_ptr.as_ref() {
            let next_ptr = state_getter(entry).load(Relaxed, guard);
            debug_assert_eq!(next_ptr.tag(), Tag::Both);
            let _: bool =
                state_getter(entry).update_tag_if(Tag::Second, |_| true, Relaxed, Relaxed);
            if entry_ptr.without_tag() == cleanup_target_range.1 {
                break;
            }
            entry_ptr = next_ptr;
        }
    }

    next_valid_ptr
}
