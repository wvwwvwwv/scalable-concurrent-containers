use super::underlying::Underlying;
use super::{Arc, Tag};

use std::convert::TryInto;
use std::marker::PhantomData;
use std::{ops::Deref, ptr, ptr::NonNull};

/// [`Ptr`] points to an instance.
#[derive(Copy, Debug, Eq)]
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
    #[inline]
    pub fn as_ref(&self) -> Option<&'b T> {
        unsafe {
            Tag::unset_tag(self.instance_ptr)
                .as_ref()
                .map(|u| u.deref())
        }
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
    #[inline]
    pub fn as_raw(&self) -> *const T {
        unsafe {
            Tag::unset_tag(self.instance_ptr)
                .as_ref()
                .map_or_else(|| ptr::null, |u| u.deref() as *const T)
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
    #[inline]
    pub fn set_tag(&mut self, tag: Tag) {
        self.instance_ptr = Tag::update_tag(self.instance_ptr, tag);
    }

    /// Tries to convert itself into an [`Arc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, Barrier, Ptr};
    ///
    /// let arc: Arc<usize> = Arc::new(10);
    /// let barrier = Barrier::new();
    /// let ptr = arc.ptr(&barrier);
    /// let converted_arc = ptr.try_into_arc().unwrap();
    /// assert_eq!(*converted_arc, 10);
    /// ```
    #[inline]
    pub fn try_into_arc(self) -> Option<Arc<T>> {
        unsafe {
            if let Some(ptr) = NonNull::new(Tag::unset_tag(self.instance_ptr) as *mut Underlying<T>)
            {
                if ptr.as_ref().try_add_ref() {
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

    /// Returns its pointer value.
    pub(super) fn raw_ptr(&self) -> *const Underlying<T> {
        self.instance_ptr
    }
}

impl<'b, T> Clone for Ptr<'b, T> {
    fn clone(&self) -> Self {
        Self {
            instance_ptr: self.instance_ptr.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'b, T> PartialEq for Ptr<'b, T> {
    fn eq(&self, other: &Self) -> bool {
        self.instance_ptr == other.instance_ptr
    }
}

impl<'b, T> TryInto<Arc<T>> for Ptr<'b, T> {
    type Error = ();

    #[inline]
    fn try_into(self) -> Result<Arc<T>, Self::Error> {
        if let Some(arc) = self.try_into_arc() {
            return Ok(arc);
        }
        Err(())
    }
}
