use super::underlying::Underlying;
use super::{Arc, Tag};

use std::marker::PhantomData;
use std::ptr::addr_of;
use std::sync::atomic::Ordering::Relaxed;
use std::{ops::Deref, ptr, ptr::NonNull};

/// [`Ptr`] points to an instance.
#[derive(Debug)]
pub struct Ptr<'b, T> {
    instance_ptr: *const Underlying<T>,
    _phantom: PhantomData<&'b T>,
}

impl<'b, T> Ptr<'b, T> {
    /// Creates a null [`Ptr`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Ptr;
    ///
    /// let ptr: Ptr<usize> = Ptr::null();
    /// ```
    #[must_use]
    #[inline]
    pub fn null() -> Ptr<'b, T> {
        Ptr {
            instance_ptr: ptr::null(),
            _phantom: PhantomData,
        }
    }

    /// Returns `true` if the [`Ptr`] is null.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Ptr;
    ///
    /// let ptr: Ptr<usize> = Ptr::null();
    /// assert!(ptr.is_null());
    /// ```
    #[must_use]
    #[inline]
    pub fn is_null(&self) -> bool {
        Tag::unset_tag(self.instance_ptr).is_null()
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
    #[must_use]
    #[inline]
    pub fn as_ref(&self) -> Option<&'b T> {
        unsafe { Tag::unset_tag(self.instance_ptr).as_ref().map(Deref::deref) }
    }

    /// Returns a raw pointer to the instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, Barrier};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let arc: Arc<usize> = Arc::new(29);
    /// let barrier = Barrier::new();
    /// let ptr = arc.ptr(&barrier);
    /// assert_eq!(unsafe { *ptr.as_raw() }, 29);
    /// ```
    #[must_use]
    #[inline]
    pub fn as_raw(&self) -> *const T {
        unsafe {
            Tag::unset_tag(self.instance_ptr)
                .as_ref()
                .map_or_else(ptr::null, |u| addr_of!(**u))
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
    #[must_use]
    #[inline]
    pub fn tag(&self) -> Tag {
        Tag::into_tag(self.instance_ptr)
    }

    /// Sets a [`Tag`], overwriting its existing [`Tag`].
    ///
    /// It returns the previous tag value.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Ptr, Tag};
    ///
    /// let mut ptr: Ptr<usize> = Ptr::null();
    /// assert_eq!(ptr.set_tag(Tag::Both), Tag::None);
    /// assert_eq!(ptr.tag(), Tag::Both);
    /// ```
    #[inline]
    pub fn set_tag(&mut self, tag: Tag) -> Tag {
        let old_tag = Tag::into_tag(self.instance_ptr);
        self.instance_ptr = Tag::update_tag(self.instance_ptr, tag);
        old_tag
    }

    /// Clears its [`Tag`].
    ///
    /// It returns the previous tag value.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Ptr, Tag};
    ///
    /// let mut ptr: Ptr<usize> = Ptr::null().with_tag(Tag::Both);
    /// assert_eq!(ptr.unset_tag(), Tag::Both);
    /// ```
    #[inline]
    pub fn unset_tag(&mut self) -> Tag {
        let old_tag = Tag::into_tag(self.instance_ptr);
        self.instance_ptr = Tag::unset_tag(self.instance_ptr);
        old_tag
    }

    /// Returns a copy of `self` with a [`Tag`] set.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Ptr, Tag};
    ///
    /// let mut ptr: Ptr<usize> = Ptr::null();
    /// assert_eq!(ptr.tag(), Tag::None);
    ///
    /// let ptr_with_tag = ptr.with_tag(Tag::First);
    /// assert_eq!(ptr_with_tag.tag(), Tag::First);
    /// ```
    #[must_use]
    pub fn with_tag(self, tag: Tag) -> Ptr<'b, T> {
        Ptr::from(Tag::update_tag(self.instance_ptr, tag))
    }

    /// Returns a copy of `self` with its [`Tag`] erased.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Ptr, Tag};
    ///
    /// let mut ptr: Ptr<usize> = Ptr::null();
    /// ptr.set_tag(Tag::Second);
    /// assert_eq!(ptr.tag(), Tag::Second);
    ///
    /// let ptr_without_tag = ptr.without_tag();
    /// assert_eq!(ptr_without_tag.tag(), Tag::None);
    /// ```
    #[must_use]
    pub fn without_tag(self) -> Ptr<'b, T> {
        Ptr::from(Tag::unset_tag(self.instance_ptr))
    }

    /// Tries to convert itself into an [`Arc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, Barrier};
    ///
    /// let arc: Arc<usize> = Arc::new(83);
    /// let barrier = Barrier::new();
    /// let ptr = arc.ptr(&barrier);
    /// let converted_arc = ptr.get_arc().unwrap();
    /// assert_eq!(*converted_arc, 83);
    ///
    /// drop(arc);
    /// drop(converted_arc);
    ///
    /// assert!(ptr.get_arc().is_none());
    /// ```
    #[must_use]
    #[inline]
    pub fn get_arc(self) -> Option<Arc<T>> {
        unsafe {
            if let Some(ptr) = NonNull::new(Tag::unset_tag(self.instance_ptr) as *mut Underlying<T>)
            {
                if ptr.as_ref().try_add_ref(Relaxed) {
                    return Some(Arc::from(ptr));
                }
            }
        }
        None
    }

    /// Creates a new [`Ptr`] from a raw pointer.
    pub(super) fn from(ptr: *const Underlying<T>) -> Ptr<'b, T> {
        Ptr {
            instance_ptr: ptr,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Provides a raw pointer to its [`Underlying`].
    pub(super) fn as_underlying_ptr(self) -> *const Underlying<T> {
        self.instance_ptr
    }
}

impl<'b, T> Clone for Ptr<'b, T> {
    fn clone(&self) -> Self {
        Self {
            instance_ptr: self.instance_ptr,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'b, T> Copy for Ptr<'b, T> {}

impl<'b, T> Default for Ptr<'b, T> {
    #[inline]
    fn default() -> Self {
        Ptr::null()
    }
}

impl<'b, T> Eq for Ptr<'b, T> {}

impl<'b, T> PartialEq for Ptr<'b, T> {
    fn eq(&self, other: &Self) -> bool {
        self.instance_ptr == other.instance_ptr
    }
}
