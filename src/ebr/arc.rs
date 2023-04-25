use super::ref_counted::RefCounted;
use super::{Barrier, Collectible, Ptr};
use std::mem::forget;
use std::ops::Deref;
use std::panic::UnwindSafe;
use std::ptr::{addr_of, NonNull};
use std::sync::atomic::Ordering::Relaxed;

/// [`Arc`] is a reference-counted handle to an instance.
///
/// The instance is passed to the EBR garbage collector when the last strong reference is dropped.
#[derive(Debug)]
pub struct Arc<T> {
    instance_ptr: NonNull<RefCounted<T>>,
}

impl<T: 'static> Arc<T> {
    /// Creates a new instance of [`Arc`].
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
    /// use scc::ebr::Arc;
    ///
    /// let arc: Arc<usize> = Arc::new(31);
    /// ```
    #[inline]
    pub fn new(t: T) -> Arc<T> {
        let boxed = Box::new(RefCounted::new(t));
        Arc {
            instance_ptr: unsafe { NonNull::new_unchecked(Box::into_raw(boxed)) },
        }
    }
}

impl<T> Arc<T> {
    /// Creates a new instance of [`Arc`] without checking the lifetime of `T`.
    ///
    /// # Safety
    ///
    /// `T::drop` can be run after the last strong reference is dropped, therefore it is safe only
    /// if `T::drop` does not access short-lived data or [`std::mem::needs_drop`] is `false` for
    /// `T`. Otherwise, the instance must be manually dropped by invoking
    /// [`release_drop_in_place()`](Self::release_drop_in_place) before the lifetime of `T` is
    /// reached.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Arc;
    ///
    /// let hello = String::from("hello");
    /// let arc: Arc<&str> = unsafe { Arc::new_unchecked(hello.as_str()) };
    ///
    /// assert!(unsafe { arc.release_drop_in_place() });
    /// ```
    #[inline]
    pub unsafe fn new_unchecked(t: T) -> Arc<T> {
        let boxed = Box::new(RefCounted::new(t));
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
    #[inline]
    #[must_use]
    pub fn ptr<'b>(&self, _barrier: &'b Barrier) -> Ptr<'b, T> {
        Ptr::from(self.instance_ptr.as_ptr())
    }

    /// Returns a reference to the underlying instance with the supplied [`Barrier`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, Barrier};
    ///
    /// let arc: Arc<usize> = Arc::new(37);
    /// let barrier = Barrier::new();
    /// let ref_b = arc.get_ref_with(&barrier);
    /// assert_eq!(*ref_b, 37);
    /// ```
    #[inline]
    #[must_use]
    pub fn get_ref_with<'b>(&self, _barrier: &'b Barrier) -> &'b T {
        unsafe { std::mem::transmute(&**self.underlying()) }
    }

    /// Returns a mutable reference to the underlying instance if the instance is exclusively
    /// owned.
    ///
    /// # Safety
    ///
    /// The method is `unsafe` since there can be a [`Ptr`] to the instance; in other words, it is
    /// safe as long as there is no [`Ptr`] to the instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Arc;
    ///
    /// let mut arc: Arc<usize> = Arc::new(38);
    /// unsafe {
    ///     *arc.get_mut().unwrap() += 1;
    /// }
    /// assert_eq!(*arc, 39);
    /// ```
    #[inline]
    pub unsafe fn get_mut(&mut self) -> Option<&mut T> {
        self.instance_ptr.as_mut().get_mut()
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
    /// let arc_clone: Arc<usize> = arc.clone();
    ///
    /// assert_eq!(arc.as_ptr(), arc_clone.as_ptr());
    /// assert_eq!(unsafe { *arc.as_ptr() }, unsafe { *arc_clone.as_ptr() });
    /// ```
    #[inline]
    #[must_use]
    pub fn as_ptr(&self) -> *const T {
        addr_of!(**self.underlying())
    }

    /// Releases the strong reference by passing `self` to the given [`Barrier`].
    ///
    /// Returns `true` if the last reference was released.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, Barrier};
    ///
    /// let arc: Arc<usize> = Arc::new(47);
    /// let arc_clone = arc.clone();
    /// let barrier = Barrier::new();
    /// assert!(!arc.release(&barrier));
    /// assert!(arc_clone.release(&barrier));
    /// ```
    #[inline]
    #[must_use]
    pub fn release(mut self, barrier: &Barrier) -> bool {
        let released = if self.underlying().drop_ref() {
            self.pass_underlying_to_collector(barrier);
            true
        } else {
            false
        };
        forget(self);
        released
    }

    /// Releases the strong reference and drops the instance immediately if it was the last
    /// reference to the instance.
    ///
    /// The instance is not passed to the garbage collector when the last reference is dropped,
    /// instead the method drops the instance immediately.
    ///
    /// Returns `true` if the last reference was released and the instance was dropped.
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
    /// let arc_clone = arc.clone();
    ///
    /// unsafe {
    ///     assert!(!arc.release_drop_in_place());
    ///     assert!(!DROPPED.load(Relaxed));
    ///     assert!(arc_clone.release_drop_in_place());
    ///     assert!(DROPPED.load(Relaxed));
    /// }
    /// ```
    #[inline]
    #[must_use]
    pub unsafe fn release_drop_in_place(mut self) -> bool {
        let dropped = if self.underlying().drop_ref() {
            self.instance_ptr.as_mut().drop_and_dealloc();
            true
        } else {
            false
        };
        forget(self);
        dropped
    }

    /// Provides a raw pointer to its [`RefCounted`].
    #[inline]
    pub(super) const fn get_underlying_ptr(&self) -> *mut RefCounted<T> {
        self.instance_ptr.as_ptr()
    }

    /// Creates a new [`Arc`] from the given pointer.
    #[inline]
    pub(super) fn from(ptr: NonNull<RefCounted<T>>) -> Arc<T> {
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

    /// Returns a reference to the underlying instance.
    #[inline]
    fn underlying(&self) -> &RefCounted<T> {
        unsafe { self.instance_ptr.as_ref() }
    }

    #[inline]
    fn pass_underlying_to_collector(&mut self, barrier: &Barrier) {
        let dyn_mut_ptr: *mut dyn Collectible =
            self.instance_ptr.as_ptr() as *const dyn Collectible as *mut dyn Collectible;
        barrier.collect(dyn_mut_ptr);
    }
}

impl<T> AsRef<T> for Arc<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        self.underlying()
    }
}

impl<T> Clone for Arc<T> {
    #[inline]
    fn clone(&self) -> Self {
        debug_assert_ne!(self.underlying().ref_cnt().load(Relaxed), 0);
        self.underlying().add_ref();
        Self {
            instance_ptr: self.instance_ptr,
        }
    }
}

impl<T> Deref for Arc<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.underlying()
    }
}

impl<T> Drop for Arc<T> {
    #[inline]
    fn drop(&mut self) {
        if self.underlying().drop_ref() {
            let barrier = Barrier::new_for_drop();
            self.pass_underlying_to_collector(&barrier);
        }
    }
}

impl<T: UnwindSafe> UnwindSafe for Arc<T> {}

impl<'b, T> TryFrom<Ptr<'b, T>> for Arc<T> {
    type Error = Ptr<'b, T>;

    #[inline]
    fn try_from(ptr: Ptr<'b, T>) -> Result<Self, Self::Error> {
        if let Some(arc) = ptr.get_arc() {
            Ok(arc)
        } else {
            Err(ptr)
        }
    }
}

unsafe impl<T: Send> Send for Arc<T> {}
unsafe impl<T: Sync> Sync for Arc<T> {}
