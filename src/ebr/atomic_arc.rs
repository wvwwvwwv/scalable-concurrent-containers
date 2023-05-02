use super::ref_counted::RefCounted;
use super::{Arc, Barrier, Ptr, Tag};
use std::mem::forget;
use std::panic::UnwindSafe;
use std::ptr::{null_mut, NonNull};
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{self, Acquire, Relaxed};

/// [`AtomicArc`] owns the underlying instance, and allows users to perform atomic operations
/// on the pointer to it.
#[derive(Debug)]
pub struct AtomicArc<T> {
    instance_ptr: AtomicPtr<RefCounted<T>>,
}

impl<T: 'static> AtomicArc<T> {
    /// Creates a new [`AtomicArc`] from an instance of `T`.
    ///
    /// The type of the instance must be determined at compile-time, must not contain non-static
    /// references, and must not be a non-static reference since the instance can, theoretically,
    /// live as long as the process. For instance, `struct Disallowed<'l, T>(&'l T)` is not
    /// allowed, because an instance of the type cannot outlive `'l` whereas the garbage collector
    /// does not guarantee that the instance is dropped within `'l`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::AtomicArc;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(10);
    /// ```
    #[inline]
    pub fn new(t: T) -> Self {
        let boxed = Box::new(RefCounted::new(t));
        Self {
            instance_ptr: AtomicPtr::new(Box::into_raw(boxed)),
        }
    }
}

impl<T> AtomicArc<T> {
    /// Creates a new [`AtomicArc`] from an [`Arc`] of `T`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc};
    ///
    /// let arc: Arc<usize> = Arc::new(10);
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::from(arc);
    /// ```
    #[inline]
    #[must_use]
    pub const fn from(arc: Arc<T>) -> Self {
        let ptr = arc.get_underlying_ptr();
        forget(arc);
        Self {
            instance_ptr: AtomicPtr::new(ptr),
        }
    }

    /// Creates a null [`AtomicArc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::AtomicArc;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::null();
    /// ```
    #[inline]
    #[must_use]
    pub const fn null() -> Self {
        Self {
            instance_ptr: AtomicPtr::new(null_mut()),
        }
    }

    /// Returns `true` if the [`AtomicArc`] is null.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicArc, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::null();
    /// atomic_arc.update_tag_if(Tag::Both, |p| p.tag() == Tag::None, Relaxed, Relaxed);
    /// assert!(atomic_arc.is_null(Relaxed));
    /// ```
    #[inline]
    pub fn is_null(&self, order: Ordering) -> bool {
        Tag::unset_tag(self.instance_ptr.load(order)).is_null()
    }

    /// Loads a pointer value from the [`AtomicArc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicArc, Barrier};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(11);
    /// let barrier = Barrier::new();
    /// let ptr = atomic_arc.load(Relaxed, &barrier);
    /// assert_eq!(*ptr.as_ref().unwrap(), 11);
    /// ```
    #[inline]
    pub fn load<'b>(&self, order: Ordering, _barrier: &'b Barrier) -> Ptr<'b, T> {
        Ptr::from(self.instance_ptr.load(order))
    }

    /// Stores the given value into the [`AtomicArc`] and returns the original value.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Barrier, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(14);
    /// let barrier = Barrier::new();
    /// let (old, tag) = atomic_arc.swap((Some(Arc::new(15)), Tag::Second), Relaxed);
    /// assert_eq!(tag, Tag::None);
    /// assert_eq!(*old.unwrap(), 14);
    /// let (old, tag) = atomic_arc.swap((None, Tag::First), Relaxed);
    /// assert_eq!(tag, Tag::Second);
    /// assert_eq!(*old.unwrap(), 15);
    /// let (old, tag) = atomic_arc.swap((None, Tag::None), Relaxed);
    /// assert_eq!(tag, Tag::First);
    /// assert!(old.is_none());
    /// ```
    #[inline]
    pub fn swap(&self, new: (Option<Arc<T>>, Tag), order: Ordering) -> (Option<Arc<T>>, Tag) {
        let desired = Tag::update_tag(
            new.0
                .as_ref()
                .map_or_else(null_mut, Arc::get_underlying_ptr),
            new.1,
        ) as *mut RefCounted<T>;
        let prev = self.instance_ptr.swap(desired, order);
        let tag = Tag::into_tag(prev);
        let prev_ptr = Tag::unset_tag(prev) as *mut RefCounted<T>;
        forget(new);
        (NonNull::new(prev_ptr).map(Arc::from), tag)
    }

    /// Returns its [`Tag`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicArc, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::null();
    /// assert_eq!(atomic_arc.tag(Relaxed), Tag::None);
    /// ```
    #[inline]
    pub fn tag(&self, order: Ordering) -> Tag {
        Tag::into_tag(self.instance_ptr.load(order))
    }

    /// Sets a new [`Tag`] if the given condition is met.
    ///
    /// It returns `true` if the new [`Tag`] has been successfully set.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicArc, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::null();
    /// assert!(atomic_arc.update_tag_if(Tag::Both, |p| p.tag() == Tag::None, Relaxed, Relaxed));
    /// assert_eq!(atomic_arc.tag(Relaxed), Tag::Both);
    /// ```
    #[inline]
    pub fn update_tag_if<F: FnMut(Ptr<T>) -> bool>(
        &self,
        tag: Tag,
        mut condition: F,
        set_order: Ordering,
        fetch_order: Ordering,
    ) -> bool {
        self.instance_ptr
            .fetch_update(set_order, fetch_order, |ptr| {
                if condition(Ptr::from(ptr)) {
                    Some(Tag::update_tag(ptr, tag) as *mut RefCounted<T>)
                } else {
                    None
                }
            })
            .is_ok()
    }

    /// Performs CAS on the [`AtomicArc`].
    ///
    /// It returns `Ok` with the previously held [`Arc`] and the updated [`Ptr`] upon a
    /// successful operation.
    ///
    /// # Errors
    ///
    /// It returns `Err` with the supplied [`Arc`] and the current [`Ptr`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Barrier, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(17);
    /// let barrier = Barrier::new();
    ///
    /// let mut ptr = atomic_arc.load(Relaxed, &barrier);
    /// assert_eq!(*ptr.as_ref().unwrap(), 17);
    ///
    /// atomic_arc.update_tag_if(Tag::Both, |_| true, Relaxed, Relaxed);
    /// assert!(atomic_arc.compare_exchange(
    ///     ptr, (Some(Arc::new(18)), Tag::First), Relaxed, Relaxed, &barrier).is_err());
    ///
    /// ptr.set_tag(Tag::Both);
    /// let old: Arc<usize> = atomic_arc.compare_exchange(
    ///     ptr, (Some(Arc::new(18)), Tag::First), Relaxed, Relaxed, &barrier).unwrap().0.unwrap();
    /// assert_eq!(*old, 17);
    /// drop(old);
    ///
    /// assert!(atomic_arc.compare_exchange(
    ///     ptr, (Some(Arc::new(19)), Tag::None), Relaxed, Relaxed, &barrier).is_err());
    /// assert_eq!(*ptr.as_ref().unwrap(), 17);
    /// ```
    #[allow(clippy::type_complexity)]
    #[inline]
    pub fn compare_exchange<'b>(
        &self,
        current: Ptr<'b, T>,
        new: (Option<Arc<T>>, Tag),
        success: Ordering,
        failure: Ordering,
        _barrier: &'b Barrier,
    ) -> Result<(Option<Arc<T>>, Ptr<'b, T>), (Option<Arc<T>>, Ptr<'b, T>)> {
        let desired = Tag::update_tag(
            new.0
                .as_ref()
                .map_or_else(null_mut, Arc::get_underlying_ptr),
            new.1,
        ) as *mut RefCounted<T>;
        match self.instance_ptr.compare_exchange(
            current.as_underlying_ptr() as *mut _,
            desired,
            success,
            failure,
        ) {
            Ok(prev) => {
                let prev_arc =
                    NonNull::new(Tag::unset_tag(prev) as *mut RefCounted<T>).map(Arc::from);
                forget(new);
                Ok((prev_arc, Ptr::from(desired)))
            }
            Err(actual) => Err((new.0, Ptr::from(actual))),
        }
    }

    /// Clones `self` including tags.
    ///
    /// If `self` is not supposed to be an `AtomicArc::null`, this will never return an
    /// `AtomicArc::null`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Barrier};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(59);
    /// let barrier = Barrier::new();
    /// let atomic_arc_clone = atomic_arc.clone(Relaxed, &barrier);
    /// let ptr = atomic_arc_clone.load(Relaxed, &barrier);
    /// assert_eq!(*ptr.as_ref().unwrap(), 59);
    /// ```
    #[inline]
    #[must_use]
    pub fn clone(&self, order: Ordering, _barrier: &Barrier) -> AtomicArc<T> {
        unsafe {
            let mut ptr = self.instance_ptr.load(order);
            while let Some(underlying) = (Tag::unset_tag(ptr)).as_ref() {
                if underlying.try_add_ref(Acquire) {
                    return Self {
                        instance_ptr: AtomicPtr::new(ptr),
                    };
                }
                let ptr_again = self.instance_ptr.load(order);
                if Tag::unset_tag(ptr) == Tag::unset_tag(ptr_again) {
                    break;
                }
                ptr = ptr_again;
            }
            Self::null()
        }
    }

    /// Tries to create an [`Arc`] out of `self`.
    ///
    /// If `self` is not supposed to be an `AtomicArc::null`, this will never return `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Barrier};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(47);
    /// let barrier = Barrier::new();
    /// let arc: Arc<usize> = atomic_arc.get_arc(Relaxed, &barrier).unwrap();
    /// assert_eq!(*arc, 47);
    /// ```
    #[inline]
    pub fn get_arc(&self, order: Ordering, _barrier: &Barrier) -> Option<Arc<T>> {
        let mut ptr = Tag::unset_tag(self.instance_ptr.load(order));
        while let Some(underlying_ptr) = NonNull::new(ptr as *mut RefCounted<T>) {
            if unsafe { underlying_ptr.as_ref() }.try_add_ref(Acquire) {
                return Some(Arc::from(underlying_ptr));
            }
            let ptr_again = Tag::unset_tag(self.instance_ptr.load(order));
            if ptr == ptr_again {
                break;
            }
            ptr = ptr_again;
        }
        None
    }

    /// Tries to convert `self` into an [`Arc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(55);
    /// let arc: Arc<usize> = atomic_arc.try_into_arc(Relaxed).unwrap();
    /// assert_eq!(*arc, 55);
    /// ```
    #[inline]
    pub fn try_into_arc(self, order: Ordering) -> Option<Arc<T>> {
        let ptr = self.instance_ptr.swap(null_mut(), order);
        if let Some(underlying_ptr) = NonNull::new(Tag::unset_tag(ptr) as *mut RefCounted<T>) {
            return Some(Arc::from(underlying_ptr));
        }
        None
    }
}

impl<T> Clone for AtomicArc<T> {
    #[inline]
    fn clone(&self) -> AtomicArc<T> {
        self.clone(Relaxed, &Barrier::new())
    }
}

impl<T> Default for AtomicArc<T> {
    #[inline]
    fn default() -> Self {
        Self::null()
    }
}

impl<T> Drop for AtomicArc<T> {
    #[inline]
    fn drop(&mut self) {
        if let Some(ptr) = NonNull::new(
            Tag::unset_tag(self.instance_ptr.swap(null_mut(), Relaxed)) as *mut RefCounted<T>
        ) {
            drop(Arc::from(ptr));
        }
    }
}

unsafe impl<T: Send> Send for AtomicArc<T> {}

unsafe impl<T: Sync> Sync for AtomicArc<T> {}

impl<T: UnwindSafe> UnwindSafe for AtomicArc<T> {}
