use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

/// [`Underlying`] stores an instance of type `T`, and a link to the next [`Underlying`].
pub(super) struct Underlying<T> {
    next_or_refcnt: LinkOrRefCnt,
    instance: T,
}

impl<T> Underlying<T> {
    // Creates a new underlying instance.
    pub(super) fn new(t: T) -> Underlying<T> {
        Underlying {
            next_or_refcnt: LinkOrRefCnt::default(),
            instance: t,
        }
    }

    /// Tries to add a strong reference to the underlying instance.
    pub(super) fn try_add_ref(&self) -> bool {
        self.ref_cnt()
            .fetch_update(Relaxed, Relaxed, |r| {
                if r % 2 == 1 {
                    Some(r + 2)
                } else {
                    None
                }
            })
            .is_ok()
    }

    /// Returns a mutable reference to the instance if it is owned exclusively.
    pub(super) fn get_mut(&mut self) -> Option<&mut T> {
        if self.ref_cnt().load(Relaxed) == 1 {
            Some(&mut self.instance)
        } else {
            None
        }
    }

    /// Adds a strong reference to the underlying instance.
    pub(super) fn add_ref(&self) {
        let mut current = self.ref_cnt().load(Relaxed);
        debug_assert_eq!(current % 2, 1);
        if current > usize::MAX - 2 {
            panic!("reference count overflow");
        }
        loop {
            if let Err(actual) =
                self.ref_cnt()
                    .compare_exchange(current, current + 2, Relaxed, Relaxed)
            {
                current = actual;
            } else {
                break;
            }
        }
    }

    /// Drops a strong reference to the underlying instance.
    ///
    /// It returns `true` if it the last reference was dropped.
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
                .compare_exchange(current, new, Relaxed, Relaxed)
            {
                current = actual;
            } else {
                break;
            }
        }
        current == 1
    }

    /// Returns a reference to its reference count.
    pub(super) fn ref_cnt(&self) -> &AtomicUsize {
        unsafe { &self.next_or_refcnt.refcnt.0 }
    }
}

impl<T> Deref for Underlying<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.instance
    }
}

impl<T> Link for Underlying<T> {
    fn set(&mut self, next_ptr: *const dyn Link) {
        self.next_or_refcnt.next = next_ptr;
    }
    fn free(&mut self) -> *mut dyn Link {
        let next = unsafe { self.next_or_refcnt.next as *mut dyn Link };
        unsafe { Box::from_raw(self as *mut Underlying<T>) };
        next
    }
}

/// The [`Link`]` trait defines necessary methods for an instance to be reclaimed by the EBR
/// garbage collector.
pub(super) trait Link {
    /// Sets the next [`Link`] instance.
    fn set(&mut self, next_ptr: *const dyn Link);

    /// Drops itself, frees the memory, and returns the next [`Link`] attached to it.
    fn free(&mut self) -> *mut dyn Link;
}

/// [`LinkOrRefCnt`] is a union of a dynamic pointer to [`Link`] and a reference count.
pub(super) union LinkOrRefCnt {
    next: *const dyn Link,
    refcnt: ManuallyDrop<(AtomicUsize, usize)>,
}

impl Default for LinkOrRefCnt {
    fn default() -> Self {
        LinkOrRefCnt {
            refcnt: ManuallyDrop::new((AtomicUsize::new(1), 0)),
        }
    }
}
