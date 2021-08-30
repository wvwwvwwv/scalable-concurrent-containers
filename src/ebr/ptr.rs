use super::underlying::Underlying;
use super::Tag;

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
        unsafe {
            Tag::unset_tag(self.instance_ptr)
                .as_ref()
                .map(|u| u.deref())
        }
    }

    /// Returns its [`Tag`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Ptr, Tag};
    ///
    /// let ptr: Ptr<usize> = Ptr::null();
    /// assert_eq!(ptr.tag(), Tag::None);
    /// ```
    pub fn tag(&self) -> Tag {
        Tag::into_tag(self.instance_ptr)
    }

    /// Sets a [`Tag`], overwriting any existing tag.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Ptr, Tag};
    ///
    /// let mut ptr: Ptr<usize> = Ptr::null();
    /// ptr.set_tag(Tag::Both);
    /// assert_eq!(ptr.tag(), Tag::Both);
    /// ```
    pub fn set_tag(&mut self, tag: Tag) {
        self.instance_ptr = Tag::update_tag(self.instance_ptr, tag);
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
