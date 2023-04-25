use super::Collectible;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{self, Relaxed};

/// [`RefCounted`] stores an instance of type `T`, and a union of the link to the next
/// [`RefCounted`] or the reference counter.
pub(super) struct RefCounted<T> {
    instance: T,
    next_or_refcnt: LinkOrRefCnt,
}

impl<T> RefCounted<T> {
    // Creates a new underlying instance.
    #[inline]
    pub(super) const fn new(t: T) -> RefCounted<T> {
        RefCounted {
            instance: t,
            next_or_refcnt: LinkOrRefCnt::new(),
        }
    }

    /// Tries to add a strong reference to the underlying instance.
    ///
    // `order` must be as strong as `Acquire` for the caller to correctly validate the newest state
    // of the pointer.
    #[inline]
    pub(super) fn try_add_ref(&self, order: Ordering) -> bool {
        self.ref_cnt()
            .fetch_update(
                order,
                order,
                |r| {
                    if r % 2 == 1 {
                        Some(r + 2)
                    } else {
                        None
                    }
                },
            )
            .is_ok()
    }

    /// Returns a mutable reference to the instance if it is owned exclusively.
    #[inline]
    pub(super) fn get_mut(&mut self) -> Option<&mut T> {
        if self.ref_cnt().load(Relaxed) == 1 {
            Some(&mut self.instance)
        } else {
            None
        }
    }

    /// Adds a strong reference to the underlying instance.
    #[inline]
    pub(super) fn add_ref(&self) {
        let mut current = self.ref_cnt().load(Relaxed);

        loop {
            debug_assert_eq!(current % 2, 1);
            debug_assert!(current <= usize::MAX - 2, "reference count overflow");

            match self
                .ref_cnt()
                .compare_exchange_weak(current, current + 2, Relaxed, Relaxed)
            {
                Ok(_) => break,
                Err(actual) => {
                    current = actual;
                }
            }
        }
    }

    /// Drops a strong reference to the underlying instance.
    ///
    /// It returns `true` if it the last reference was dropped.
    #[inline]
    pub(super) fn drop_ref(&self) -> bool {
        // It does not have to be a load-acquire as everything's synchronized via the global
        // epoch. In addition to that, it also does not have to be read-modify-write as a
        // reference count increment is guaranteed to be observed by the one that decrements
        // the last reference.
        let mut current = self.ref_cnt().load(Relaxed);
        debug_assert_ne!(current, 0);
        loop {
            let new = if current <= 1 { 0 } else { current - 2 };
            if let Err(actual) = self
                .ref_cnt()
                .compare_exchange_weak(current, new, Relaxed, Relaxed)
            {
                current = actual;
            } else {
                break;
            }
        }
        current == 1
    }

    /// Returns a reference to its reference count.
    #[inline]
    pub(super) fn ref_cnt(&self) -> &AtomicUsize {
        unsafe { &self.next_or_refcnt.refcnt.0 }
    }
}

impl<T> Deref for RefCounted<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.instance
    }
}

impl<T> Collectible for RefCounted<T> {
    #[inline]
    fn next_ptr_mut(&mut self) -> &mut Option<NonNull<dyn Collectible>> {
        unsafe { &mut self.next_or_refcnt.next }
    }
}

/// [`LinkOrRefCnt`] is a union of a dynamic pointer to [`Collectible`] and a reference count.
pub(super) union LinkOrRefCnt {
    next: Option<NonNull<dyn Collectible>>,
    refcnt: ManuallyDrop<(AtomicUsize, usize)>,
}

impl LinkOrRefCnt {
    #[inline]
    const fn new() -> Self {
        LinkOrRefCnt {
            refcnt: ManuallyDrop::new((AtomicUsize::new(1), 0)),
        }
    }
}
