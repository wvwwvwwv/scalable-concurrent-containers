use super::ref_counted::RefCounted;
use super::{Collectible, Guard, Ptr};
use std::mem::forget;
use std::ops::Deref;
use std::panic::UnwindSafe;
use std::ptr::{addr_of, NonNull};

/// [`Owned`] uniquely owns an instance.
///
/// The instance it passed to the `EBR` garbage collector when the [`Owned`] is dropped.
#[derive(Debug)]
pub struct Owned<T> {
    instance_ptr: NonNull<RefCounted<T>>,
}

impl<T: 'static> Owned<T> {
    /// Creates a new instance of [`Owned`].
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
    /// use scc::ebr::Owned;
    ///
    /// let owned: Owned<usize> = Owned::new(31);
    /// ```
    #[inline]
    pub fn new(t: T) -> Self {
        let owned = Box::new(RefCounted::new_unique(t));
        Self {
            instance_ptr: unsafe { NonNull::new_unchecked(Box::into_raw(owned)) },
        }
    }
}

impl<T> Owned<T> {
    /// Creates a new [`Owned`] without checking the lifetime of `T`.
    ///
    /// # Safety
    ///
    /// `T::drop` can be run after the [`Owned`] is dropped, therefore it is safe only if `T::drop`
    /// does not access short-lived data or [`std::mem::needs_drop`] is `false` for `T`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Owned;
    ///
    /// let hello = String::from("hello");
    /// let owned: Owned<&str> = unsafe { Owned::new_unchecked(hello.as_str()) };
    /// ```
    #[inline]
    pub unsafe fn new_unchecked(t: T) -> Self {
        let owned = Box::new(RefCounted::new_unique(t));
        Self {
            instance_ptr: unsafe { NonNull::new_unchecked(Box::into_raw(owned)) },
        }
    }

    /// Returns a [`Ptr`] to the instance that may live as long as the supplied [`Guard`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Guard, Owned};
    ///
    /// let owned: Owned<usize> = Owned::new(37);
    /// let guard = Guard::new();
    /// let ptr = owned.get_guarded_ptr(&guard);
    /// drop(owned);
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
    /// use scc::ebr::{Guard, Owned};
    ///
    /// let owned: Owned<usize> = Owned::new(37);
    /// let guard = Guard::new();
    /// let ref_b = owned.get_guarded_ref(&guard);
    /// drop(owned);
    ///
    /// assert_eq!(*ref_b, 37);
    /// ```
    #[inline]
    #[must_use]
    pub fn get_guarded_ref<'g>(&self, _guard: &'g Guard) -> &'g T {
        unsafe { std::mem::transmute(&**self.underlying()) }
    }

    /// Returns a mutable reference to the instance.
    ///
    /// # Safety
    ///
    /// The method is `unsafe` since there can be a [`Ptr`] to the instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Owned;
    ///
    /// let mut owned: Owned<usize> = Owned::new(38);
    /// unsafe {
    ///     *owned.get_mut() += 1;
    /// }
    /// assert_eq!(*owned, 39);
    /// ```
    #[inline]
    pub unsafe fn get_mut(&mut self) -> &mut T {
        self.instance_ptr.as_mut().get_mut_unique()
    }

    /// Provides a raw pointer to the instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Owned;
    /// use std::sync::atomic::AtomicBool;
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let owned: Owned<usize> = Owned::new(10);
    ///
    /// assert_eq!(unsafe { *owned.as_ptr() }, 10);
    /// ```
    #[inline]
    #[must_use]
    pub fn as_ptr(&self) -> *const T {
        addr_of!(**self.underlying())
    }

    /// Releases the ownership by passing `self` to the given [`Guard`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Guard, Owned};
    ///
    /// let owned: Owned<usize> = Owned::new(47);
    /// let guard = Guard::new();
    /// owned.release(&guard);
    /// ```
    #[inline]
    pub fn release(mut self, guard: &Guard) {
        self.pass_underlying_to_collector(guard);
        forget(self);
    }

    /// Drops the instance immediately.
    ///
    /// # Safety
    ///
    /// The caller must ensure that there is no [`Ptr`] pointing to the instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Owned;
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
    /// let owned: Owned<T> = Owned::new(T(&DROPPED));
    /// assert!(!DROPPED.load(Relaxed));
    ///
    /// unsafe {
    ///     owned.drop_in_place();
    /// }
    ///
    /// assert!(DROPPED.load(Relaxed));
    /// ```
    #[inline]
    pub unsafe fn drop_in_place(mut self) {
        self.instance_ptr.as_mut().drop_and_dealloc();
        forget(self);
    }

    /// Provides a raw pointer to its [`RefCounted`].
    #[inline]
    pub(super) const fn get_underlying_ptr(&self) -> *mut RefCounted<T> {
        self.instance_ptr.as_ptr()
    }

    /// Creates a new [`Owned`] from the given pointer.
    #[inline]
    pub(super) fn from(ptr: NonNull<RefCounted<T>>) -> Self {
        debug_assert_eq!(
            unsafe {
                ptr.as_ref()
                    .ref_cnt()
                    .load(std::sync::atomic::Ordering::Relaxed)
            },
            0
        );
        Self { instance_ptr: ptr }
    }

    /// Returns a reference to the underlying instance.
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

impl<T> AsRef<T> for Owned<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        self.underlying()
    }
}

impl<T> Deref for Owned<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.underlying()
    }
}

impl<T> Drop for Owned<T> {
    #[inline]
    fn drop(&mut self) {
        let guard = Guard::new_for_drop();
        self.pass_underlying_to_collector(&guard);
    }
}

unsafe impl<T: Send> Send for Owned<T> {}

unsafe impl<T: Sync> Sync for Owned<T> {}

impl<T: UnwindSafe> UnwindSafe for Owned<T> {}
