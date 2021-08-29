use super::underlying::Underlying;

use std::{ops::Deref, ptr};

/// [`Ptr`] points to an instance.
#[derive(Clone, Copy, Debug)]
pub struct Ptr<'r, T> {
    instance_ptr: *const Underlying<T>,
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
            instance_ptr: ptr::null(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Tries to create a reference to the underlying instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicArc, Barrier};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(21);
    /// let barrier = Barrier::new();
    /// let ptr = atomic_arc.load(Relaxed, &barrier);
    /// assert_eq!(*ptr.as_ref().unwrap(), 21);
    /// ```
    pub fn as_ref(&self) -> Option<&T> {
        unsafe { self.instance_ptr.as_ref().map(|u| u.deref()) }
    }

    /// Creates a new [`Ptr`] from a raw pointer.
    pub(super) fn from(ptr: *const Underlying<T>) -> Ptr<'r, T> {
        Ptr {
            instance_ptr: ptr,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Returns its pointer value.
    pub(super) fn raw_ptr(&self) -> *const Underlying<T> {
        self.instance_ptr
    }
}
