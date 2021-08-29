use super::underlying::Underlying;
use super::{Barrier, Ptr};

use std::ops::Deref;
use std::ptr::NonNull;

/// [`Arc`] is a reference-counted handle to an instance.
#[derive(Debug)]
pub struct Arc<T: 'static> {
    instance_ptr: NonNull<Underlying<T>>,
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
            instance_ptr: unsafe { NonNull::new_unchecked(Box::into_raw(boxed)) },
        }
    }

    /// Generates a [`Ptr`] out of the [`Arc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, Barrier};
    ///
    /// let arc: Arc<usize> = Arc::new(37);
    /// let barrier = Barrier::new();
    /// let ptr = arc.ptr(&barrier);
    /// assert_eq!(*ptr.as_ref().unwrap(), 37);
    /// ```
    pub fn ptr<'r>(&self, _barrier: &'r Barrier) -> Ptr<'r, T> {
        Ptr::from(self.instance_ptr.as_ptr())
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
        unsafe { self.instance_ptr.as_mut().get_mut() }
    }

    /// Creates a new [`Arc`] from the given pointer.
    pub(super) fn from(ptr: NonNull<Underlying<T>>) -> Arc<T> {
        Arc { instance_ptr: ptr }
    }

    /// Returns its underlying pointer.
    pub(super) fn raw_ptr(&self) -> *mut Underlying<T> {
        self.instance_ptr.as_ptr()
    }

    /// Drops the reference, and returns the underlying pointer if the last reference was
    /// dropped.
    pub(super) fn drop_ref(&self) -> Option<*mut Underlying<T>> {
        if self.underlying().drop_ref() {
            Some(self.instance_ptr.as_ptr())
        } else {
            None
        }
    }

    /// Returns a reference to the underlying instance.
    fn underlying(&self) -> &Underlying<T> {
        unsafe { self.instance_ptr.as_ref() }
    }
}

impl<T: 'static> Clone for Arc<T> {
    fn clone(&self) -> Self {
        self.underlying().add_ref();
        Self {
            instance_ptr: self.instance_ptr.clone(),
        }
    }
}

impl<T: 'static> Deref for Arc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.underlying().deref()
    }
}

impl<T: 'static> Drop for Arc<T> {
    fn drop(&mut self) {
        if self.underlying().drop_ref() {
            let barrier = Barrier::new();
            barrier.reclaim_underlying(self.instance_ptr.as_ptr());
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

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
            drop(Barrier::new());
        }
    }
}
