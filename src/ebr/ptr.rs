use super::arc::Underlying;

use std::ptr;

/// [`Ptr`] points to an instance.
#[derive(Clone, Copy, Debug)]
pub struct Ptr<'r, T> {
    pub(super) ptr: *const Underlying<T>,
    _phantom: std::marker::PhantomData<&'r T>,
}

impl<'r, T> Ptr<'r, T> {
    /// Creates a null [`Ptr`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Ptr;
    ///
    /// let ptr: Ptr<usize> = Ptr::null();
    /// ```
    pub fn null() -> Ptr<'r, T> {
        Ptr {
            ptr: ptr::null(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Tries to create a reference to the underlying instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicArc, Reader};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(21);
    /// let reader = Reader::new();
    /// let ptr = atomic_arc.load(Relaxed, &reader);
    /// assert_eq!(*ptr.as_ref().unwrap(), 21);
    /// ```
    pub fn as_ref(&self) -> Option<&T> {
        unsafe { self.ptr.as_ref().map(|u| &u.t) }
    }

    pub(super) fn from(ptr: *const Underlying<T>) -> Ptr<'r, T> {
        Ptr {
            ptr,
            _phantom: std::marker::PhantomData,
        }
    }
}
