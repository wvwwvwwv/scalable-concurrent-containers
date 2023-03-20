use super::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};

use std::fmt::{self, Debug, Display};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering::{self, Relaxed, Release};

/// [`LinkedList`] is a type trait implementing a lock-free singly linked list.
pub trait LinkedList: 'static + Sized {
    /// Returns a reference to the forward link.
    ///
    /// The pointer value may be tagged if [`Self::mark`] or [`Self::delete_self`] has been
    /// invoked.
    fn link_ref(&self) -> &AtomicArc<Self>;

    /// Returns `true` if `self` is reachable and not marked.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::LinkedList;
    /// use scc::ebr::{AtomicArc, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// #[derive(Default)]
    /// struct L(AtomicArc<L>, usize);
    /// impl LinkedList for L {
    ///     fn link_ref(&self) -> &AtomicArc<L> {
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
    /// It returns `false` if a flag has already been marked on `self`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::LinkedList;
    /// use scc::ebr::AtomicArc;
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// #[derive(Default)]
    /// struct L(AtomicArc<L>, usize);
    /// impl LinkedList for L {
    ///     fn link_ref(&self) -> &AtomicArc<L> {
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
    /// It returns `false` if no flag has been marked on `self`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::LinkedList;
    /// use scc::ebr::AtomicArc;
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// #[derive(Default)]
    /// struct L(AtomicArc<L>, usize);
    /// impl LinkedList for L {
    ///     fn link_ref(&self) -> &AtomicArc<L> {
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
    /// use scc::ebr::AtomicArc;
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// #[derive(Default)]
    /// struct L(AtomicArc<L>, usize);
    /// impl LinkedList for L {
    ///     fn link_ref(&self) -> &AtomicArc<L> {
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
    /// It returns `false` if `self` already has `deleted` marked on it.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::LinkedList;
    /// use scc::ebr::{Arc, AtomicArc, Barrier};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// #[derive(Default)]
    /// struct L(AtomicArc<L>, usize);
    /// impl LinkedList for L {
    ///     fn link_ref(&self) -> &AtomicArc<L> {
    ///         &self.0
    ///     }
    /// }
    ///
    /// let barrier = Barrier::new();
    ///
    /// let head: L = L::default();
    /// let tail: Arc<L> = Arc::new(L::default());
    /// assert!(head.push_back(tail.clone(), false, Relaxed, &barrier).is_ok());
    ///
    /// tail.delete_self(Relaxed);
    /// assert!(head.next_ptr(Relaxed, &barrier).as_ref().is_none());
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
    /// use scc::ebr::AtomicArc;
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// #[derive(Default)]
    /// struct L(AtomicArc<L>, usize);
    /// impl LinkedList for L {
    ///     fn link_ref(&self) -> &AtomicArc<L> {
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
    /// It returns the supplied [`Arc`] when it finds `self` deleted.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::LinkedList;
    /// use scc::ebr::{Arc, AtomicArc, Barrier};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// #[derive(Default)]
    /// struct L(AtomicArc<L>, usize);
    /// impl LinkedList for L {
    ///     fn link_ref(&self) -> &AtomicArc<L> {
    ///         &self.0
    ///     }
    /// }
    ///
    /// let barrier = Barrier::new();
    ///
    /// let head: L = L::default();
    /// assert!(head.push_back(Arc::new(L::default()), true, Relaxed, &barrier).is_ok());
    /// assert!(head.is_marked(Relaxed));
    /// assert!(head.push_back(Arc::new(L::default()), false, Relaxed, &barrier).is_ok());
    /// assert!(!head.is_marked(Relaxed));
    ///
    /// head.delete_self(Relaxed);
    /// assert!(!head.is_marked(Relaxed));
    /// assert!(head.push_back(Arc::new(L::default()), false, Relaxed, &barrier).is_err());
    /// ```
    #[inline]
    fn push_back<'b>(
        &self,
        mut entry: Arc<Self>,
        mark: bool,
        order: Ordering,
        barrier: &'b Barrier,
    ) -> Result<Ptr<'b, Self>, Arc<Self>> {
        let new_tag = if mark { Tag::First } else { Tag::None };
        let mut next_ptr = self.link_ref().load(Relaxed, barrier);
        while next_ptr.tag() != Tag::Second {
            entry
                .link_ref()
                .swap((next_ptr.get_arc(), Tag::None), Relaxed);
            match self.link_ref().compare_exchange(
                next_ptr,
                (Some(entry), new_tag),
                order,
                Relaxed,
                barrier,
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
    /// use scc::ebr::{Arc, AtomicArc, Barrier};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// #[derive(Default)]
    /// struct L(AtomicArc<L>, usize);
    /// impl LinkedList for L {
    ///     fn link_ref(&self) -> &AtomicArc<L> {
    ///         &self.0
    ///     }
    /// }
    ///
    /// let barrier = Barrier::new();
    ///
    /// let head: L = L::default();
    /// assert!(head.push_back(Arc::new(L(AtomicArc::null(), 1)), false, Relaxed, &barrier).is_ok());
    /// head.mark(Relaxed);
    ///
    /// let next_ptr = head.next_ptr(Relaxed, &barrier);
    /// assert_eq!(next_ptr.as_ref().unwrap().1, 1);
    /// assert!(head.is_marked(Relaxed));
    /// ```
    #[inline]
    fn next_ptr<'b>(&self, order: Ordering, barrier: &'b Barrier) -> Ptr<'b, Self> {
        let self_next_ptr = self.link_ref().load(order, barrier);
        let self_tag = self_next_ptr.tag();
        let mut next_ptr = self_next_ptr;
        let mut update_self = false;
        let next_valid_ptr = loop {
            if let Some(next_ref) = next_ptr.as_ref() {
                let next_next_ptr = next_ref.link_ref().load(order, barrier);
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
                    (next_valid_ptr.get_arc(), self_tag),
                    Release,
                    Relaxed,
                    barrier,
                )
                .ok()
                .map(|(p, _)| p.map(|p| p.release(barrier)));
        }

        next_valid_ptr
    }
}

/// [`Entry`] stores an instance of `T` and a link to the next entry.
pub struct Entry<T: 'static> {
    /// `instance` is always `Some` unless [`Self::take_inner`] is called.
    instance: Option<T>,

    /// `next` points to the next entry in a linked list.
    next: AtomicArc<Self>,
}

impl<T: 'static> Entry<T> {
    /// Creates a new [`Entry`].
    #[inline]
    pub(super) fn new(val: T) -> Entry<T> {
        Entry {
            instance: Some(val),
            next: AtomicArc::default(),
        }
    }

    /// Extracts the inner instance of `T`.
    #[inline]
    pub(super) unsafe fn take_inner(&mut self) -> T {
        self.instance.take().unwrap_unchecked()
    }

    /// Returns a reference to `next`.
    #[inline]
    pub(super) fn next(&self) -> &AtomicArc<Self> {
        &self.next
    }
}

impl<T: 'static> AsRef<T> for Entry<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        unsafe { self.instance.as_ref().unwrap_unchecked() }
    }
}

impl<T: 'static> AsMut<T> for Entry<T> {
    #[inline]
    fn as_mut(&mut self) -> &mut T {
        unsafe { self.instance.as_mut().unwrap_unchecked() }
    }
}

impl<T: 'static + Clone> Clone for Entry<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            instance: self.instance.clone(),
            next: AtomicArc::default(),
        }
    }
}

impl<T: 'static + Debug> Debug for Entry<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entry")
            .field("instance", &self.instance)
            .field("next", &self.next)
            .field("removed", &self.is_deleted(Relaxed))
            .finish()
    }
}

impl<T: 'static> Deref for Entry<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { self.instance.as_ref().unwrap_unchecked() }
    }
}

impl<T: 'static> DerefMut for Entry<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.instance.as_mut().unwrap_unchecked() }
    }
}

impl<T: 'static + Display> Display for Entry<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(instance) = self.instance.as_ref() {
            write!(f, "Some({instance})")
        } else {
            write!(f, "None")
        }
    }
}

impl<T: Eq + 'static> Eq for Entry<T> {}

impl<T: 'static> LinkedList for Entry<T> {
    #[inline]
    fn link_ref(&self) -> &AtomicArc<Self> {
        &self.next
    }
}

impl<T: PartialEq + 'static> PartialEq for Entry<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
    }
}
