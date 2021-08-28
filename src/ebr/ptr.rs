use super::arc::Underlying;

use std::ptr;

/// [`Ptr`] points to an instance.
#[derive(Clone, Copy)]
pub struct Ptr<'r, T> {
    ptr: *const Underlying<T>,
    _phantom: std::marker::PhantomData<&'r T>,
}

impl<'r, T> Ptr<'r, T> {
    pub fn null() -> Ptr<'r, T> {
        Ptr {
            ptr: ptr::null(),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn as_ref(&self) -> Option<&T> {
        unsafe { self.ptr.as_ref().map(|u| &u.t) }
    }

    pub(super) fn new(ptr: *const Underlying<T>) -> Ptr<'r, T> {
        Ptr {
            ptr,
            _phantom: std::marker::PhantomData,
        }
    }
}
