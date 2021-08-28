use super::link::Link;
use super::{Ptr, Reader, Reclaimer};

use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

/// [`Arc`] is a reference-counted handle to an instance.
#[derive(Debug)]
pub struct Arc<T: 'static> {
    pub(super) instance: NonNull<Underlying<T>>,
}

impl<T: 'static> Arc<T> {
    /// Creates a new instance of [`Arc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Arc;
    ///
    /// let arc: Arc<usize> = Arc::new(31);
    /// ```
    pub fn new(t: T) -> Arc<T> {
        let boxed = Box::new(Underlying::new(t));
        Arc {
            instance: unsafe { NonNull::new_unchecked(Box::into_raw(boxed)) },
        }
    }

    /// Generates a pointer out of the [`Arc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, Reclaimer};
    ///
    /// let arc: Arc<usize> = Arc::new(37);
    /// let reader = Reclaimer::read();
    /// let ptr = arc.ptr(&reader);
    /// assert_eq!(*ptr.as_ref().unwrap(), 37);
    /// ```
    pub fn ptr<'r>(&self, _reader: &'r Reader) -> Ptr<'r, T> {
        Ptr::from(self.instance.as_ptr())
    }

    /// Returns a mutable reference to the underlying instance if the instance is exclusively
    /// owned.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Arc;
    ///
    /// let mut arc: Arc<usize> = Arc::new(38);
    /// *arc.get_mut().unwrap() += 1;
    /// assert_eq!(*arc, 39);
    /// ```
    pub fn get_mut(&mut self) -> Option<&mut T> {
        if unsafe { self.instance.as_ref().ref_cnt() } == 0 {
            Some(unsafe { &mut self.instance.as_mut().t })
        } else {
            None
        }
    }

    pub(super) fn from(non_null_ptr: NonNull<Underlying<T>>) -> Arc<T> {
        Arc {
            instance: non_null_ptr,
        }
    }
}

impl<T: 'static> Clone for Arc<T> {
    fn clone(&self) -> Self {
        unsafe { self.instance.as_ref().add_ref() };
        Self {
            instance: self.instance.clone(),
        }
    }
}

impl<T: 'static> Deref for Arc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &self.instance.as_ref().t }
    }
}

impl<T: 'static> Drop for Arc<T> {
    fn drop(&mut self) {
        if unsafe { self.instance.as_ref().drop_ref() } {
            let reader = Reclaimer::read();
            reader.reclaim_underlying(self.instance.as_ptr());
        }
    }
}

pub(super) union LinkOrRefCnt {
    next: *const dyn Link,
    refcnt: ManuallyDrop<(AtomicUsize, usize)>,
}

impl Default for LinkOrRefCnt {
    fn default() -> Self {
        LinkOrRefCnt {
            refcnt: ManuallyDrop::new((AtomicUsize::new(0), 0)),
        }
    }
}

/// [`Underlying`] stores the instance and a link to the next [`Underlying`].
pub(super) struct Underlying<T> {
    next_or_refcnt: LinkOrRefCnt,
    pub(super) t: T,
}

impl<T> Underlying<T> {
    // Creates a new underlying instance.
    pub(super) fn new(t: T) -> Underlying<T> {
        Underlying {
            next_or_refcnt: LinkOrRefCnt::default(),
            t,
        }
    }

    /// Returns the count of strong references.
    fn ref_cnt(&self) -> usize {
        unsafe { self.next_or_refcnt.refcnt.0.load(Relaxed) }
    }

    /// Adds a strong reference to the underlying instance.
    pub(super) fn add_ref(&self) {
        unsafe { self.next_or_refcnt.refcnt.0.fetch_add(1, Relaxed) };
    }

    /// Drops a strong reference to the underlying instance.
    pub(super) fn drop_ref(&self) -> bool {
        // It does not have to be a load-acquire as everything's synchronized via the global
        // epoch. In addition to that, it also does not have to be read-modify-write as a
        // reference count increment is guaranteed to be observed by the one that decrements
        // the last reference.
        let mut current = unsafe { self.next_or_refcnt.refcnt.0.load(Relaxed) };
        while current > 0 {
            if let Err(actual) = unsafe {
                self.next_or_refcnt.refcnt.0.compare_exchange(
                    current,
                    current - 1,
                    Relaxed,
                    Relaxed,
                )
            } {
                current = actual;
            } else {
                break;
            }
        }
        current == 0
    }
}

impl<T> Link for Underlying<T> {
    fn set(&mut self, next_ptr: *const dyn Link) {
        self.next_or_refcnt.next = next_ptr;
    }

    fn dealloc(&mut self) -> *mut dyn Link {
        let next = unsafe { self.next_or_refcnt.next as *mut dyn Link };
        unsafe { Box::from_raw(self as *mut Underlying<T>) };
        next
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn arc() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);
        struct A(AtomicUsize, usize, &'static AtomicBool);
        impl Drop for A {
            fn drop(&mut self) {
                self.2.swap(true, Relaxed);
            }
        }
        let mut arc = Arc::new(A(AtomicUsize::new(10), 10, &DESTROYED));
        if let Some(mut_ref) = arc.get_mut() {
            mut_ref.1 += 1;
        }
        arc.0.fetch_add(1, Relaxed);
        assert_eq!(arc.deref().0.load(Relaxed), 11);
        assert_eq!(arc.deref().1, 11);

        let mut arc_cloned = arc.clone();
        assert!(arc_cloned.get_mut().is_none());
        arc_cloned.0.fetch_add(1, Relaxed);
        assert_eq!(arc_cloned.deref().0.load(Relaxed), 12);
        assert_eq!(arc_cloned.deref().1, 11);

        let mut arc_cloned_again = arc_cloned.clone();
        assert!(arc_cloned_again.get_mut().is_none());
        assert_eq!(arc_cloned_again.deref().0.load(Relaxed), 12);
        assert_eq!(arc_cloned_again.deref().1, 11);

        drop(arc);
        assert!(!DESTROYED.load(Relaxed));
        assert!(arc_cloned_again.get_mut().is_none());

        drop(arc_cloned);
        assert!(!DESTROYED.load(Relaxed));
        assert!(arc_cloned_again.get_mut().is_some());

        drop(arc_cloned_again);
        while !DESTROYED.load(Relaxed) {
            drop(Reclaimer::read());
        }
    }
}
