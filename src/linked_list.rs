use super::ebr::{AtomicShared, Guard, Ptr, Shared, Tag};
use std::fmt::{self, Debug, Display};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering::{self, Relaxed, Release};

/// [`LinkedList`] is a type trait implementing a lock-free singly linked list.
pub trait LinkedList: Sized {
    /// Returns a reference to the forward link.
    ///
    /// The pointer value may be tagged if [`Self::mark`] or [`Self::delete_self`] has been
    /// invoked.
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
        self.link_ref().tag(order) == Tag::None
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
        self.link_ref()
            .update_tag_if(Tag::First, |ptr| ptr.tag() == Tag::None, order, Relaxed)
    }

    /// Removes the mark from `self`.
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
        self.link_ref()
            .update_tag_if(Tag::None, |ptr| ptr.tag() == Tag::First, order, Relaxed)
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
        self.link_ref().tag(order) == Tag::First
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
        self.link_ref()
            .update_tag_if(Tag::Second, |ptr| ptr.tag() != Tag::Second, order, Relaxed)
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
        self.link_ref().tag(order) == Tag::Second
    }

    /// Appends the given entry after `self` and returns a pointer to the entry.
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
        mut entry: Shared<Self>,
        mark: bool,
        order: Ordering,
        guard: &'g Guard,
    ) -> Result<Ptr<'g, Self>, Shared<Self>> {
        let new_tag = if mark { Tag::First } else { Tag::None };
        let mut next_ptr = self.link_ref().load(Relaxed, guard);
        while next_ptr.tag() != Tag::Second {
            entry
                .link_ref()
                .swap((next_ptr.get_shared(), Tag::None), Relaxed);
            match self.link_ref().compare_exchange_weak(
                next_ptr,
                (Some(entry), new_tag),
                order,
                Relaxed,
                guard,
            ) {
                Ok((_, updated)) => {
                    return Ok(updated);
                }
                Err((passed, actual)) => {
                    entry = unsafe { passed.unwrap_unchecked() };
                    next_ptr = actual;
                }
            }
        }

        // `self` has been deleted.
        Err(entry)
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
        let self_next_ptr = self.link_ref().load(order, guard);
        let self_tag = self_next_ptr.tag();
        let mut next_ptr = self_next_ptr;
        let mut update_self = false;
        let next_valid_ptr = loop {
            if let Some(next_ref) = next_ptr.as_ref() {
                let next_next_ptr = next_ref.link_ref().load(order, guard);
                if next_next_ptr.tag() != Tag::Second {
                    break next_ptr;
                }
                update_self = true;
                next_ptr = next_next_ptr;
            } else {
                break Ptr::null();
            }
        };

        // Updates its link if an invalid entry has been found, and `self` is a valid one.
        if update_self && self_tag != Tag::Second {
            self.link_ref()
                .compare_exchange(
                    self_next_ptr,
                    (next_valid_ptr.get_shared(), self_tag),
                    Release,
                    Relaxed,
                    guard,
                )
                .ok()
                .map(|(p, _)| p.map(|p| p.release(guard)));
        }

        next_valid_ptr
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
    /// Creates a new [`Entry`].
    #[inline]
    pub(super) fn new(val: T) -> Self {
        Self {
            instance: Some(val),
            next: AtomicShared::default(),
        }
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
            .field("removed", &self.is_deleted(Relaxed))
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
            self.next_ptr(Relaxed, &Guard::new());
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

impl<T> LinkedList for Entry<T> {
    #[inline]
    fn link_ref(&self) -> &AtomicShared<Self> {
        &self.next
    }
}

impl<T: PartialEq> PartialEq for Entry<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
    }
}
