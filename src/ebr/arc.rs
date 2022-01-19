use super::underlying::{Link, Underlying};
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
    #[inline]
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
    #[must_use]
    #[inline]
    pub fn ptr<'b>(&self, _barrier: &'b Barrier) -> Ptr<'b, T> {
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
    #[inline]
    pub fn get_mut(&mut self) -> Option<&mut T> {
        unsafe { self.instance_ptr.as_mut().get_mut() }
    }

    /// Provides a raw pointer to the underlying instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Arc;
    /// use std::sync::atomic::AtomicBool;
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let arc: Arc<usize> = Arc::new(10);
    /// let arc_cloned: Arc<usize> = arc.clone();
    ///
    /// assert_eq!(arc.as_ptr(), arc_cloned.as_ptr());
    /// assert_eq!(unsafe { *arc.as_ptr() }, unsafe { *arc_cloned.as_ptr() });
    /// ```
    #[inline]
    #[must_use]
    pub fn as_ptr(&self) -> *const T {
        &**self.underlying() as *const T
    }

    /// Drops the underlying instance if the last reference is dropped.
    ///
    /// The instance is not passed to the garbage collector when the last reference is dropped,
    /// instead the method drops the instance immediately. The semantics is the same as that of
    /// [`std::sync::Arc`].
    ///
    /// # Safety
    ///
    /// The caller must ensure that there is no [`Ptr`] pointing to the instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Arc;
    /// use std::sync::atomic::AtomicBool;
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// static DROPPED: AtomicBool = AtomicBool::new(false);
    /// struct T(&'static AtomicBool);
    /// impl Drop for T {
    ///     fn drop(&mut self) {
    ///         self.0.store(true, Relaxed);
    ///     }
    /// }
    ///
    /// let arc: Arc<T> = Arc::new(T(&DROPPED));
    ///
    /// unsafe {
    ///     arc.drop_in_place();
    /// }
    /// assert!(DROPPED.load(Relaxed));
    /// ```
    pub unsafe fn drop_in_place(mut self) {
        if self.underlying().drop_ref() {
            self.instance_ptr.as_mut().free();
            std::mem::forget(self);
        }
    }

    /// Provides a raw pointer to its [`Underlying`].
    pub(super) fn as_underlying_ptr(&self) -> *mut Underlying<T> {
        self.instance_ptr.as_ptr()
    }

    /// Creates a new [`Arc`] from the given pointer.
    pub(super) fn from(ptr: NonNull<Underlying<T>>) -> Arc<T> {
        debug_assert_ne!(
            unsafe {
                ptr.as_ref()
                    .ref_cnt()
                    .load(std::sync::atomic::Ordering::Relaxed)
            },
            0
        );
        Arc { instance_ptr: ptr }
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
    #[inline]
    fn clone(&self) -> Self {
        debug_assert_ne!(
            self.underlying()
                .ref_cnt()
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        self.underlying().add_ref();
        Self {
            instance_ptr: self.instance_ptr,
        }
    }
}

impl<T: 'static> Deref for Arc<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &**self.underlying()
    }
}

impl<T: 'static> Drop for Arc<T> {
    #[inline]
    fn drop(&mut self) {
        if self.underlying().drop_ref() {
            let barrier = Barrier::new();
            barrier.reclaim_underlying(self.instance_ptr.as_ptr());
        }
    }
}

unsafe impl<T: 'static + Send> Send for Arc<T> {}
unsafe impl<T: 'static + Sync> Sync for Arc<T> {}

#[cfg(test)]
mod test {
    use super::*;

    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    struct A(AtomicUsize, usize, &'static AtomicBool);
    impl Drop for A {
        fn drop(&mut self) {
            self.2.swap(true, Relaxed);
        }
    }

    #[test]
    fn arc() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);

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

    #[test]
    fn arc_send() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);

        let arc = Arc::new(A(AtomicUsize::new(14), 14, &DESTROYED));
        let arc_cloned = arc.clone();
        let thread = std::thread::spawn(move || {
            assert_eq!(arc_cloned.0.load(Relaxed), arc_cloned.1);
        });
        assert!(thread.join().is_ok());
        assert_eq!(arc.0.load(Relaxed), arc.1);
    }

    #[test]
    fn arc_arc_send() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);

        let arc_arc = Arc::new(A(AtomicUsize::new(14), 14, &DESTROYED));
        let arc_arc_cloned = arc_arc.clone();
        let thread = std::thread::spawn(move || {
            assert_eq!(arc_arc_cloned.0.load(Relaxed), 14);
        });
        assert!(thread.join().is_ok());
        assert_eq!(arc_arc.0.load(Relaxed), 14);

        unsafe {
            arc_arc.drop_in_place();
        }
        assert!(DESTROYED.load(Relaxed));
    }

    #[test]
    fn arc_nested() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);

        struct Nest(Arc<A>);

        let nested_arc = Arc::new(Nest(Arc::new(A(AtomicUsize::new(10), 10, &DESTROYED))));
        assert!(!DESTROYED.load(Relaxed));
        drop(nested_arc);

        while !DESTROYED.load(Relaxed) {
            drop(Barrier::new());
        }
    }
}
