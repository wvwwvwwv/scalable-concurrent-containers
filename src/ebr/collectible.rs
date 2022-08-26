use super::collector::Collector;

use std::ptr::NonNull;

/// [`Collectible`] defines key methods for `Self` to be reclaimed by the EBR garbage collector.
///
/// The [`ebr`](super) module provides managed handles which implement [`Collectible`] in tandem
/// with atomic reference counting, however it is also possible to manually implement the
/// [`Collectible`] trait for a type to pass an instance of the type to the EBR garbage collector
/// via [`Barrier::defer`](super::Barrier::defer).
///
/// # Examples
///
/// ```
/// use scc::ebr::{Barrier, Collectible};
/// use std::ptr::NonNull;
///
/// struct LazyString(String, Option<NonNull<dyn Collectible>>);
///
/// impl Collectible for LazyString {
///     fn next_ptr_mut(&mut self) -> &mut Option<NonNull<dyn Collectible>> {
///         &mut self.1
///     }
/// }
///
/// let boxed: Box<LazyString> = Box::new(LazyString(String::from("Lazy"), None));
///
/// let static_ref: &'static LazyString = unsafe { std::mem::transmute(&*boxed) };
/// let barrier_for_ref = Barrier::new();
///
/// let barrier_to_drop = Barrier::new();
/// barrier_to_drop.defer(boxed);
/// drop(barrier_to_drop);
///
/// // The reference is valid as long as a `Barrier` that had been created before `boxed` was
/// // passed to a `Barrier` survives.
/// assert_eq!(static_ref.0, "Lazy");
/// ```

pub trait Collectible {
    /// Returns a mutable reference to the next [`Collectible`] pointer.
    fn next_ptr_mut(&mut self) -> &mut Option<NonNull<dyn Collectible>>;

    /// Drops itself and frees the memory.
    ///
    /// If the instance of the `Self` type is not created via [`Box::new`] or the like, this method
    /// has to be implemented for the type.
    fn drop_and_dealloc(&mut self) {
        unsafe { Box::from_raw(self as *mut Self) };
    }
}

/// [`DeferredClosure`] implements [`Collectible`] for a closure to execute it after all the
/// readers in the process at the moment are gone.
pub(super) struct DeferredClosure<F: 'static + FnOnce() + Sync> {
    f: Option<F>,
    link: Option<NonNull<dyn Collectible>>,
}

impl<F: 'static + FnOnce() + Sync> DeferredClosure<F> {
    /// Creates a new [`DeferredClosure`].
    #[inline]
    pub fn new(f: F) -> DeferredClosure<F> {
        DeferredClosure {
            f: Some(f),
            link: None,
        }
    }
}

impl<F: 'static + FnOnce() + Sync> Collectible for DeferredClosure<F> {
    fn next_ptr_mut(&mut self) -> &mut Option<NonNull<dyn Collectible>> {
        &mut self.link
    }
    fn drop_and_dealloc(&mut self) {
        if let Some(f) = self.f.take() {
            f();
        }
        unsafe { Box::from_raw(self as *mut Self) };
    }
}

/// [`DeferredIncrementalClosure`] implements [`Collectible`] for a closure to execute it
/// incrementally after all the readers in the process at the moment are gone.
pub(super) struct DeferredIncrementalClosure<F: 'static + FnMut() -> bool + Sync> {
    f: F,
    link: Option<NonNull<dyn Collectible>>,
}

impl<F: 'static + FnMut() -> bool + Sync> DeferredIncrementalClosure<F> {
    /// Creates a new [`DeferredClosure`].
    #[inline]
    pub fn new(f: F) -> DeferredIncrementalClosure<F> {
        DeferredIncrementalClosure { f, link: None }
    }
}

impl<F: 'static + FnMut() -> bool + Sync> Collectible for DeferredIncrementalClosure<F> {
    fn next_ptr_mut(&mut self) -> &mut Option<NonNull<dyn Collectible>> {
        &mut self.link
    }
    fn drop_and_dealloc(&mut self) {
        if (self.f)() {
            // Finished, thus drop `self`.
            unsafe { Box::from_raw(self as *mut Self) };
        } else {
            // Push itself into the garbage queue.
            unsafe {
                (*Collector::current()).reclaim_confirmed(self as *mut Self);
            }
        }
    }
}
