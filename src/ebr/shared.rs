use super::ref_counted::RefCounted;
use super::{Collectible, Guard, Ptr};
use std::mem::forget;
use std::ops::Deref;
use std::panic::UnwindSafe;
use std::ptr::{addr_of, NonNull};
use std::sync::atomic::Ordering::Relaxed;

/// [`Shared`] is a reference-counted handle to an instance.
///
/// The instance is passed to the EBR garbage collector when the last strong reference is dropped.
#[derive(Debug)]
pub struct Shared<T> {
    instance_ptr: NonNull<RefCounted<T>>,
}

impl<T: 'static> Shared<T> {
    /// Creates a new [`Shared`].
    ///
    /// The type of the instance must be determined at compile-time, must not contain non-static
    /// references, and must not be a non-static reference since the instance can, theoretically,
    /// survive the process. For instance, `struct Disallowed<'l, T>(&'l T)` is not allowed,
    /// because an instance of the type cannot outlive `'l` whereas the garbage collector does not
    /// guarantee that the instance is dropped within `'l`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Shared;
    ///
    /// let shared: Shared<usize> = Shared::new(31);
    /// ```
    #[inline]
    pub fn new(t: T) -> Self {
        let boxed = Box::new(RefCounted::new_shared(t));
        Self {
            instance_ptr: unsafe { NonNull::new_unchecked(Box::into_raw(boxed)) },
        }
    }
}

impl<T> Shared<T> {
    /// Creates a new [`Shared`] without checking the lifetime of `T`.
    ///
    /// # Safety
    ///
    /// `T::drop` can be run after the last strong reference is dropped, therefore it is safe only
    /// if `T::drop` does not access short-lived data or [`std::mem::needs_drop`] is `false` for
    /// `T`. Otherwise, the instance must be manually dropped by invoking [`Self::drop_in_place`]
    /// before the lifetime of `T` is reached.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Shared;
    ///
    /// let hello = String::from("hello");
    /// let shared: Shared<&str> = unsafe { Shared::new_unchecked(hello.as_str()) };
    ///
    /// assert!(unsafe { shared.drop_in_place() });
    /// ```
    #[inline]
    pub unsafe fn new_unchecked(t: T) -> Self {
        let boxed = Box::new(RefCounted::new_shared(t));
        Self {
            instance_ptr: unsafe { NonNull::new_unchecked(Box::into_raw(boxed)) },
        }
    }

    /// Returns a [`Ptr`] to the instance that may live as long as the supplied [`Guard`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Guard, Shared};
    ///
    /// let shared: Shared<usize> = Shared::new(37);
    /// let guard = Guard::new();
    /// let ptr = shared.get_guarded_ptr(&guard);
    /// drop(shared);
    ///
    /// assert_eq!(*ptr.as_ref().unwrap(), 37);
    /// ```
    #[inline]
    #[must_use]
    pub fn get_guarded_ptr<'g>(&self, _guard: &'g Guard) -> Ptr<'g, T> {
        Ptr::from(self.instance_ptr.as_ptr())
    }

    /// Returns a reference to the instance that may live as long as the supplied [`Guard`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Guard, Shared};
    ///
    /// let shared: Shared<usize> = Shared::new(37);
    /// let guard = Guard::new();
    /// let ref_b = shared.get_guarded_ref(&guard);
    /// drop(shared);
    ///
    /// assert_eq!(*ref_b, 37);
    /// ```
    #[inline]
    #[must_use]
    pub fn get_guarded_ref<'g>(&self, _guard: &'g Guard) -> &'g T {
        unsafe { std::mem::transmute(&**self.underlying()) }
    }

    /// Returns a mutable reference to the instance if the [`Shared`] is holding the only strong
    /// reference.
    ///
    /// # Safety
    ///
    /// The method is `unsafe` since there can be a [`Ptr`] to the instance without holding a
    /// strong reference.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Shared;
    ///
    /// let mut shared: Shared<usize> = Shared::new(38);
    /// unsafe {
    ///     *shared.get_mut().unwrap() += 1;
    /// }
    /// assert_eq!(*shared, 39);
    /// ```
    #[inline]
    pub unsafe fn get_mut(&mut self) -> Option<&mut T> {
        self.instance_ptr.as_mut().get_mut_shared()
    }

    /// Provides a raw pointer to the instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Shared;
    /// use std::sync::atomic::AtomicBool;
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let shared: Shared<usize> = Shared::new(10);
    /// let shared_clone: Shared<usize> = shared.clone();
    ///
    /// assert_eq!(shared.as_ptr(), shared_clone.as_ptr());
    /// assert_eq!(unsafe { *shared.as_ptr() }, unsafe { *shared_clone.as_ptr() });
    /// ```
    #[inline]
    #[must_use]
    pub fn as_ptr(&self) -> *const T {
        addr_of!(**self.underlying())
    }

    /// Releases the strong reference by passing `self` to the given [`Guard`].
    ///
    /// Returns `true` if the last reference was released.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Guard, Shared};
    ///
    /// let shared: Shared<usize> = Shared::new(47);
    /// let shared_clone = shared.clone();
    /// let guard = Guard::new();
    /// assert!(!shared.release(&guard));
    /// assert!(shared_clone.release(&guard));
    /// ```
    #[inline]
    #[must_use]
    pub fn release(mut self, guard: &Guard) -> bool {
        let released = if self.underlying().drop_ref() {
            self.pass_underlying_to_collector(guard);
            true
        } else {
            false
        };
        forget(self);
        released
    }

    /// Drops the instance immediately if it has held the last reference to the instance.
    ///
    /// Returns `true` if the instance was dropped.
    ///
    /// # Safety
    ///
    /// The caller must ensure that there is no [`Ptr`] pointing to the instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Shared;
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
    /// let shared: Shared<T> = Shared::new(T(&DROPPED));
    /// let shared_clone = shared.clone();
    ///
    /// unsafe {
    ///     assert!(!shared.drop_in_place());
    ///     assert!(!DROPPED.load(Relaxed));
    ///     assert!(shared_clone.drop_in_place());
    ///     assert!(DROPPED.load(Relaxed));
    /// }
    /// ```
    #[inline]
    #[must_use]
    pub unsafe fn drop_in_place(mut self) -> bool {
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

    /// Creates a new [`Shared`] from the given pointer.
    #[inline]
    pub(super) fn from(ptr: NonNull<RefCounted<T>>) -> Self {
        debug_assert_ne!(
            unsafe {
                ptr.as_ref()
                    .ref_cnt()
                    .load(std::sync::atomic::Ordering::Relaxed)
            },
            0
        );
        Self { instance_ptr: ptr }
    }

    /// Returns a reference to the instance.
    #[inline]
    fn underlying(&self) -> &RefCounted<T> {
        unsafe { self.instance_ptr.as_ref() }
    }

    #[inline]
    fn pass_underlying_to_collector(&mut self, guard: &Guard) {
        let dyn_ref = self.underlying().as_collectible();
        let dyn_mut_ptr: *mut dyn Collectible = unsafe { std::mem::transmute(dyn_ref) };
        guard.collect(dyn_mut_ptr);
    }
}

impl<T> AsRef<T> for Shared<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        self.underlying()
    }
}

impl<T> Clone for Shared<T> {
    #[inline]
    fn clone(&self) -> Self {
        debug_assert_ne!(self.underlying().ref_cnt().load(Relaxed), 0);
        self.underlying().add_ref();
        Self {
            instance_ptr: self.instance_ptr,
        }
    }
}

impl<T> Deref for Shared<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.underlying()
    }
}

impl<T> Drop for Shared<T> {
    #[inline]
    fn drop(&mut self) {
        if self.underlying().drop_ref() {
            let guard = Guard::new_for_drop();
            self.pass_underlying_to_collector(&guard);
        }
    }
}

impl<'g, T> TryFrom<Ptr<'g, T>> for Shared<T> {
    type Error = Ptr<'g, T>;

    #[inline]
    fn try_from(ptr: Ptr<'g, T>) -> Result<Self, Self::Error> {
        if let Some(shared) = ptr.get_shared() {
            Ok(shared)
        } else {
            Err(ptr)
        }
    }
}

unsafe impl<T: Send> Send for Shared<T> {}

unsafe impl<T: Sync> Sync for Shared<T> {}

impl<T: UnwindSafe> UnwindSafe for Shared<T> {}
