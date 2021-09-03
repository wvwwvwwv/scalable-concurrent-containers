use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};

use std::sync::atomic::Ordering::{self, Acquire, Relaxed, Release};

/// [`LinkedList`] is a wait-free self-referential singly linked list.
pub trait LinkedList: 'static + Sized {
    /// Returns a reference to the forward link.
    ///
    /// The pointer value may be tagged if the entry is no longer a member of the linked list.
    fn link_ref(&self) -> &AtomicArc<Self>;

    /// Marks `self` with an internal flag to denote that `self` is in a special state.
    ///
    /// It returns `false` if nothing is marked on `self`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Barrier};
    /// use scc::LinkedList;
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
    fn mark(&self, order: Ordering) -> bool {
        self.link_ref()
            .update_tag_if(Tag::Second, |t| t != Tag::First, order)
    }

    /// Removes marks from `self`.
    ///
    /// It returns `false` if nothing is marked on `self`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Barrier};
    /// use scc::LinkedList;
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
    /// ```
    fn unmark(&self, order: Ordering) -> bool {
        self.link_ref()
            .update_tag_if(Tag::Second, |t| t == Tag::Second, order)
    }

    /// Returns `true` if `self` has a mark on it.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Barrier};
    /// use scc::LinkedList;
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
    fn is_marked(&self, order: Ordering) -> bool {
        self.link_ref().tag(order) == Tag::Second
    }

    /// Deletes `self`.
    ///
    /// It returns `false` if `self` already has `deleted` marked on it.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Barrier};
    /// use scc::LinkedList;
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
    /// assert!(head.push_back(tail.clone(), false, &barrier).is_ok());
    ///
    /// tail.delete_self(Relaxed);
    /// assert!(head.next_ptr(&barrier).as_ref().is_none());
    /// ```
    fn delete_self(&self, order: Ordering) -> bool {
        self.link_ref()
            .update_tag_if(Tag::First, |t| t != Tag::First, order)
    }

    /// Returns `true` if `self` has been deleted.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Barrier};
    /// use scc::LinkedList;
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
    fn is_deleted(&self, order: Ordering) -> bool {
        self.link_ref().tag(order) == Tag::First
    }

    /// Appends the given entry, and returns a pointer to the entry.
    ///
    /// If `mark` is given `true`, it atomically marks an internal flag on `self` when updating
    /// the linked list, otherwise it removes marks.
    ///
    /// # Errors
    ///
    /// It returns the supplied [`Arc`], paired with `true` when it finds `self` deleted, or
    /// `false` if the given entry is a part of a linked list instead of a free-floating entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Barrier};
    /// use scc::LinkedList;
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
    /// assert!(head.push_back(Arc::new(L::default()), true, &barrier).is_ok());
    /// assert!(head.is_marked(Relaxed));
    /// assert!(head.push_back(Arc::new(L::default()), false, &barrier).is_ok());
    /// assert!(!head.is_marked(Relaxed));
    ///
    /// head.delete_self(Relaxed);
    /// assert!(!head.is_marked(Relaxed));
    /// assert!(head.push_back(Arc::new(L::default()), false, &barrier).is_err());
    /// ```
    fn push_back<'b>(
        &self,
        mut entry: Arc<Self>,
        mark: bool,
        barrier: &'b Barrier,
    ) -> Result<Ptr<'b, Self>, (Arc<Self>, bool)> {
        let mut next_ptr = self.link_ref().load(Acquire, barrier);
        let new_tag = if mark { Tag::Second } else { Tag::None };
        while next_ptr.tag() != Tag::First {
            if let Some(old_next) = entry
                .link_ref()
                .swap((next_ptr.try_into_arc(), Tag::None), Relaxed)
            {
                barrier.reclaim(old_next);
                return Err((entry, false));
            }
            match self.link_ref().compare_exchange(
                next_ptr,
                (Some(entry), new_tag),
                Release,
                Relaxed,
            ) {
                Ok((_, updated)) => {
                    return Ok(updated);
                }
                Err((passed, actual)) => {
                    entry = passed.unwrap();
                    next_ptr = actual;
                }
            }
        }

        // `self` has been deleted.
        Err((entry, true))
    }

    /// Returns the closest next valid entry.
    ///
    /// It unlinks deleted entries until it reaches a valid one.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Barrier};
    /// use scc::LinkedList;
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
    /// assert!(head.push_back(Arc::new(L(AtomicArc::null(), 1)), false, &barrier).is_ok());
    /// head.mark(Relaxed);
    ///
    /// let next_ptr = head.next_ptr(&barrier);
    /// assert_eq!(next_ptr.as_ref().unwrap().1, 1);
    /// assert!(head.is_marked(Relaxed));
    /// ```
    fn next_ptr<'b>(&self, barrier: &'b Barrier) -> Ptr<'b, Self> {
        let self_next_ptr = self.link_ref().load(Acquire, barrier);
        let self_tag = self_next_ptr.tag();
        let mut next_ptr = self_next_ptr;
        let mut update_self = false;
        let next_valid_ptr = loop {
            if let Some(next_ref) = next_ptr.as_ref() {
                let next_next_ptr = next_ref.link_ref().load(Acquire, barrier);
                if next_next_ptr.tag() != Tag::First {
                    break next_ptr;
                }
                if !update_self {
                    update_self = true;
                }
                next_ptr = next_next_ptr;
            } else {
                break Ptr::null();
            }
        };

        // Updates its link if an invalid entry has been found, and `self` is a valid one.
        if update_self && self_tag != Tag::First {
            let _result = self.link_ref().compare_exchange(
                self_next_ptr,
                (next_valid_ptr.try_into_arc(), self_tag),
                Release,
                Relaxed,
            );
        }

        next_valid_ptr
    }
}
