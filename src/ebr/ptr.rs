use super::ref_counted::RefCounted;
use super::{Shared, Tag};
use std::marker::PhantomData;
use std::panic::UnwindSafe;
use std::ptr::addr_of;
use std::sync::atomic::Ordering::Relaxed;
use std::{ops::Deref, ptr, ptr::NonNull};

/// [`Ptr`] points to an instance.
#[derive(Debug)]
pub struct Ptr<'g, T> {
    instance_ptr: *const RefCounted<T>,
    _phantom: PhantomData<&'g T>,
}

impl<'g, T> Ptr<'g, T> {
    /// Creates a null [`Ptr`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Ptr;
    ///
    /// let ptr: Ptr<usize> = Ptr::null();
    /// ```
    #[inline]
    #[must_use]
    pub const fn null() -> Self {
        Self {
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
    #[inline]
    #[must_use]
    pub fn is_null(&self) -> bool {
        Tag::unset_tag(self.instance_ptr).is_null()
    }

    /// Tries to create a reference to the underlying instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicShared, Guard};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_shared: AtomicShared<usize> = AtomicShared::new(21);
    /// let guard = Guard::new();
    /// let ptr = atomic_shared.load(Relaxed, &guard);
    /// assert_eq!(*ptr.as_ref().unwrap(), 21);
    /// ```
    #[inline]
    #[must_use]
    pub fn as_ref(&self) -> Option<&'g T> {
        unsafe { Tag::unset_tag(self.instance_ptr).as_ref().map(Deref::deref) }
    }

    /// Provides a raw pointer to the instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Guard, Shared};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let shared: Shared<usize> = Shared::new(29);
    /// let guard = Guard::new();
    /// let ptr = shared.get_guarded_ptr(&guard);
    /// drop(shared);
    ///
    /// assert_eq!(unsafe { *ptr.as_ptr() }, 29);
    /// ```
    #[inline]
    #[must_use]
    pub fn as_ptr(&self) -> *const T {
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
    #[inline]
    #[must_use]
    pub fn tag(&self) -> Tag {
        Tag::into_tag(self.instance_ptr)
    }

    /// Sets a [`Tag`], overwriting its existing [`Tag`].
    ///
    /// Returns the previous tag value.
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
    /// Returns the previous tag value.
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
    #[inline]
    #[must_use]
    pub fn with_tag(self, tag: Tag) -> Self {
        Self::from(Tag::update_tag(self.instance_ptr, tag))
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
    #[inline]
    #[must_use]
    pub fn without_tag(self) -> Self {
        Self::from(Tag::unset_tag(self.instance_ptr))
    }

    /// Tries to convert itself into a [`Shared`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Guard, Shared};
    ///
    /// let shared: Shared<usize> = Shared::new(83);
    /// let guard = Guard::new();
    /// let ptr = shared.get_guarded_ptr(&guard);
    /// let shared_restored = ptr.get_shared().unwrap();
    /// assert_eq!(*shared_restored, 83);
    ///
    /// drop(shared);
    /// drop(shared_restored);
    ///
    /// assert!(ptr.get_shared().is_none());
    /// ```
    #[inline]
    #[must_use]
    pub fn get_shared(self) -> Option<Shared<T>> {
        unsafe {
            if let Some(ptr) = NonNull::new(Tag::unset_tag(self.instance_ptr).cast_mut()) {
                if ptr.as_ref().try_add_ref(Relaxed) {
                    return Some(Shared::from(ptr));
                }
            }
        }
        None
    }

    /// Creates a new [`Ptr`] from a raw pointer.
    #[inline]
    pub(super) const fn from(ptr: *const RefCounted<T>) -> Self {
        Self {
            instance_ptr: ptr,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Provides a raw pointer to its [`RefCounted`].
    #[inline]
    pub(super) const fn as_underlying_ptr(self) -> *const RefCounted<T> {
        self.instance_ptr
    }
}

impl<'g, T> Clone for Ptr<'g, T> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<'g, T> Copy for Ptr<'g, T> {}

impl<'g, T> Default for Ptr<'g, T> {
    #[inline]
    fn default() -> Self {
        Self::null()
    }
}

impl<'g, T> Eq for Ptr<'g, T> {}

impl<'g, T> PartialEq for Ptr<'g, T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.instance_ptr == other.instance_ptr
    }
}

impl<'g, T: UnwindSafe> UnwindSafe for Ptr<'g, T> {}
