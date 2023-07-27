use super::ref_counted::RefCounted;
use super::{Guard, Ptr, Shared, Tag};
use std::mem::forget;
use std::panic::UnwindSafe;
use std::ptr::{null_mut, NonNull};
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{self, Acquire, Relaxed};

/// [`AtomicShared`] owns the underlying instance, and allows users to perform atomic operations
/// on the pointer to it.
#[derive(Debug)]
pub struct AtomicShared<T> {
    instance_ptr: AtomicPtr<RefCounted<T>>,
}

/// A pair of [`Shared`] and [`Ptr`] of the same type.
pub type SharedPtrPair<'g, T> = (Option<Shared<T>>, Ptr<'g, T>);

impl<T: 'static> AtomicShared<T> {
    /// Creates a new [`AtomicShared`] from an instance of `T`.
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
    /// use scc::ebr::AtomicShared;
    ///
    /// let atomic_shared: AtomicShared<usize> = AtomicShared::new(10);
    /// ```
    #[inline]
    pub fn new(t: T) -> Self {
        let boxed = Box::new(RefCounted::new_shared(t));
        Self {
            instance_ptr: AtomicPtr::new(Box::into_raw(boxed)),
        }
    }
}

impl<T> AtomicShared<T> {
    /// Creates a new [`AtomicShared`] from a [`Shared`] of `T`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicShared, Shared};
    ///
    /// let shared: Shared<usize> = Shared::new(10);
    /// let atomic_shared: AtomicShared<usize> = AtomicShared::from(shared);
    /// ```
    #[inline]
    #[must_use]
    pub const fn from(arc: Shared<T>) -> Self {
        let ptr = arc.get_underlying_ptr();
        forget(arc);
        Self {
            instance_ptr: AtomicPtr::new(ptr),
        }
    }

    /// Creates a null [`AtomicShared`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::AtomicShared;
    ///
    /// let atomic_shared: AtomicShared<usize> = AtomicShared::null();
    /// ```
    #[inline]
    #[must_use]
    pub const fn null() -> Self {
        Self {
            instance_ptr: AtomicPtr::new(null_mut()),
        }
    }

    /// Returns `true` if the [`AtomicShared`] is null.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicShared, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_shared: AtomicShared<usize> = AtomicShared::null();
    /// atomic_shared.update_tag_if(Tag::Both, |p| p.tag() == Tag::None, Relaxed, Relaxed);
    /// assert!(atomic_shared.is_null(Relaxed));
    /// ```
    #[inline]
    pub fn is_null(&self, order: Ordering) -> bool {
        Tag::unset_tag(self.instance_ptr.load(order)).is_null()
    }

    /// Loads a pointer value from the [`AtomicShared`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicShared, Guard};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_shared: AtomicShared<usize> = AtomicShared::new(11);
    /// let guard = Guard::new();
    /// let ptr = atomic_shared.load(Relaxed, &guard);
    /// assert_eq!(*ptr.as_ref().unwrap(), 11);
    /// ```
    #[inline]
    pub fn load<'g>(&self, order: Ordering, _guard: &'g Guard) -> Ptr<'g, T> {
        Ptr::from(self.instance_ptr.load(order))
    }

    /// Stores the given value into the [`AtomicShared`] and returns the original value.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicShared, Guard, Shared, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_shared: AtomicShared<usize> = AtomicShared::new(14);
    /// let guard = Guard::new();
    /// let (old, tag) = atomic_shared.swap((Some(Shared::new(15)), Tag::Second), Relaxed);
    /// assert_eq!(tag, Tag::None);
    /// assert_eq!(*old.unwrap(), 14);
    /// let (old, tag) = atomic_shared.swap((None, Tag::First), Relaxed);
    /// assert_eq!(tag, Tag::Second);
    /// assert_eq!(*old.unwrap(), 15);
    /// let (old, tag) = atomic_shared.swap((None, Tag::None), Relaxed);
    /// assert_eq!(tag, Tag::First);
    /// assert!(old.is_none());
    /// ```
    #[inline]
    pub fn swap(&self, new: (Option<Shared<T>>, Tag), order: Ordering) -> (Option<Shared<T>>, Tag) {
        let desired = Tag::update_tag(
            new.0
                .as_ref()
                .map_or_else(null_mut, Shared::get_underlying_ptr),
            new.1,
        )
        .cast_mut();
        let prev = self.instance_ptr.swap(desired, order);
        let tag = Tag::into_tag(prev);
        let prev_ptr = Tag::unset_tag(prev).cast_mut();
        forget(new);
        (NonNull::new(prev_ptr).map(Shared::from), tag)
    }

    /// Returns its [`Tag`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicShared, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_shared: AtomicShared<usize> = AtomicShared::null();
    /// assert_eq!(atomic_shared.tag(Relaxed), Tag::None);
    /// ```
    #[inline]
    pub fn tag(&self, order: Ordering) -> Tag {
        Tag::into_tag(self.instance_ptr.load(order))
    }

    /// Sets a new [`Tag`] if the given condition is met.
    ///
    /// Returns `true` if the new [`Tag`] has been successfully set.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicShared, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_shared: AtomicShared<usize> = AtomicShared::null();
    /// assert!(atomic_shared.update_tag_if(Tag::Both, |p| p.tag() == Tag::None, Relaxed, Relaxed));
    /// assert_eq!(atomic_shared.tag(Relaxed), Tag::Both);
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
                    Some(Tag::update_tag(ptr, tag).cast_mut())
                } else {
                    None
                }
            })
            .is_ok()
    }

    /// Stores `new` into the [`AtomicShared`] if the current value is the same as `current`.
    ///
    /// Returns the previously held value and the updated [`Ptr`].
    ///
    /// # Errors
    ///
    /// Returns `Err` with the supplied [`Shared`] and the current [`Ptr`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicShared, Guard, Shared, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_shared: AtomicShared<usize> = AtomicShared::new(17);
    /// let guard = Guard::new();
    ///
    /// let mut ptr = atomic_shared.load(Relaxed, &guard);
    /// assert_eq!(*ptr.as_ref().unwrap(), 17);
    ///
    /// atomic_shared.update_tag_if(Tag::Both, |_| true, Relaxed, Relaxed);
    /// assert!(atomic_shared.compare_exchange(
    ///     ptr, (Some(Shared::new(18)), Tag::First), Relaxed, Relaxed, &guard).is_err());
    ///
    /// ptr.set_tag(Tag::Both);
    /// let old: Shared<usize> = atomic_shared.compare_exchange(
    ///     ptr,
    ///     (Some(Shared::new(18)), Tag::First),
    ///     Relaxed,
    ///     Relaxed,
    ///     &guard).unwrap().0.unwrap();
    /// assert_eq!(*old, 17);
    /// drop(old);
    ///
    /// assert!(atomic_shared.compare_exchange(
    ///     ptr, (Some(Shared::new(19)), Tag::None), Relaxed, Relaxed, &guard).is_err());
    /// assert_eq!(*ptr.as_ref().unwrap(), 17);
    /// ```
    #[inline]
    pub fn compare_exchange<'g>(
        &self,
        current: Ptr<'g, T>,
        new: (Option<Shared<T>>, Tag),
        success: Ordering,
        failure: Ordering,
        _guard: &'g Guard,
    ) -> Result<SharedPtrPair<'g, T>, SharedPtrPair<'g, T>> {
        let desired = Tag::update_tag(
            new.0
                .as_ref()
                .map_or_else(null_mut, Shared::get_underlying_ptr),
            new.1,
        )
        .cast_mut();
        match self.instance_ptr.compare_exchange(
            current.as_underlying_ptr().cast_mut(),
            desired,
            success,
            failure,
        ) {
            Ok(prev) => {
                let prev_arc = NonNull::new(Tag::unset_tag(prev).cast_mut()).map(Shared::from);
                forget(new);
                Ok((prev_arc, Ptr::from(desired)))
            }
            Err(actual) => Err((new.0, Ptr::from(actual))),
        }
    }

    /// Stores `new` into the [`AtomicShared`] if the current value is the same as `current`.
    ///
    /// This method is allowed to spuriously fail even when the comparison succeeds.
    ///
    /// Returns the previously held value and the updated [`Ptr`].
    ///
    /// # Errors
    ///
    /// Returns `Err` with the supplied [`Shared`] and the current [`Ptr`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicShared, Guard, Shared, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_shared: AtomicShared<usize> = AtomicShared::new(17);
    /// let guard = Guard::new();
    ///
    /// let mut ptr = atomic_shared.load(Relaxed, &guard);
    /// assert_eq!(*ptr.as_ref().unwrap(), 17);
    ///
    /// while let Err((_, actual)) = atomic_shared.compare_exchange_weak(
    ///     ptr,
    ///     (Some(Shared::new(18)), Tag::First),
    ///     Relaxed,
    ///     Relaxed,
    ///     &guard) {
    ///     ptr = actual;
    /// }
    ///
    /// let mut ptr = atomic_shared.load(Relaxed, &guard);
    /// assert_eq!(*ptr.as_ref().unwrap(), 18);
    /// ```
    #[inline]
    pub fn compare_exchange_weak<'g>(
        &self,
        current: Ptr<'g, T>,
        new: (Option<Shared<T>>, Tag),
        success: Ordering,
        failure: Ordering,
        _guard: &'g Guard,
    ) -> Result<SharedPtrPair<'g, T>, SharedPtrPair<'g, T>> {
        let desired = Tag::update_tag(
            new.0
                .as_ref()
                .map_or_else(null_mut, Shared::get_underlying_ptr),
            new.1,
        )
        .cast_mut();
        match self.instance_ptr.compare_exchange_weak(
            current.as_underlying_ptr().cast_mut(),
            desired,
            success,
            failure,
        ) {
            Ok(prev) => {
                let prev_arc = NonNull::new(Tag::unset_tag(prev).cast_mut()).map(Shared::from);
                forget(new);
                Ok((prev_arc, Ptr::from(desired)))
            }
            Err(actual) => Err((new.0, Ptr::from(actual))),
        }
    }

    /// Clones `self` including tags.
    ///
    /// If `self` is not supposed to be an `AtomicShared::null`, this will never return an
    /// `AtomicShared::null`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicShared, Guard};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_shared: AtomicShared<usize> = AtomicShared::new(59);
    /// let guard = Guard::new();
    /// let atomic_shared_clone = atomic_shared.clone(Relaxed, &guard);
    /// let ptr = atomic_shared_clone.load(Relaxed, &guard);
    /// assert_eq!(*ptr.as_ref().unwrap(), 59);
    /// ```
    #[inline]
    #[must_use]
    pub fn clone(&self, order: Ordering, _guard: &Guard) -> AtomicShared<T> {
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

    /// Tries to create a [`Shared`] out of `self`.
    ///
    /// If `self` is not supposed to be an `AtomicShared::null`, this will never return `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicShared, Guard, Shared};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_shared: AtomicShared<usize> = AtomicShared::new(47);
    /// let guard = Guard::new();
    /// let shared: Shared<usize> = atomic_shared.get_shared(Relaxed, &guard).unwrap();
    /// assert_eq!(*shared, 47);
    /// ```
    #[inline]
    pub fn get_shared(&self, order: Ordering, _guard: &Guard) -> Option<Shared<T>> {
        let mut ptr = Tag::unset_tag(self.instance_ptr.load(order));
        while let Some(underlying_ptr) = NonNull::new(ptr.cast_mut()) {
            if unsafe { underlying_ptr.as_ref() }.try_add_ref(Acquire) {
                return Some(Shared::from(underlying_ptr));
            }
            let ptr_again = Tag::unset_tag(self.instance_ptr.load(order));
            if ptr == ptr_again {
                break;
            }
            ptr = ptr_again;
        }
        None
    }

    /// Tries to convert `self` into a [`Shared`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicShared, Shared};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_shared: AtomicShared<usize> = AtomicShared::new(55);
    /// let shared: Shared<usize> = atomic_shared.try_into_shared(Relaxed).unwrap();
    /// assert_eq!(*shared, 55);
    /// ```
    #[inline]
    pub fn try_into_shared(self, order: Ordering) -> Option<Shared<T>> {
        let ptr = self.instance_ptr.swap(null_mut(), order);
        if let Some(underlying_ptr) = NonNull::new(Tag::unset_tag(ptr).cast_mut()) {
            return Some(Shared::from(underlying_ptr));
        }
        None
    }
}

impl<T> Clone for AtomicShared<T> {
    #[inline]
    fn clone(&self) -> AtomicShared<T> {
        self.clone(Relaxed, &Guard::new())
    }
}

impl<T> Default for AtomicShared<T> {
    #[inline]
    fn default() -> Self {
        Self::null()
    }
}

impl<T> Drop for AtomicShared<T> {
    #[inline]
    fn drop(&mut self) {
        if let Some(ptr) = NonNull::new(Tag::unset_tag(self.instance_ptr.load(Relaxed)).cast_mut())
        {
            drop(Shared::from(ptr));
        }
    }
}

unsafe impl<T: Send> Send for AtomicShared<T> {}

unsafe impl<T: Sync> Sync for AtomicShared<T> {}

impl<T: UnwindSafe> UnwindSafe for AtomicShared<T> {}
