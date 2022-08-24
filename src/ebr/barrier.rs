use super::collector::Collector;
use super::underlying::Link;
use super::Arc;

use std::ptr::null;

/// [`Barrier`] allows the user to read [`AtomicArc`](super::AtomicArc) and keeps the
/// underlying instance pinned to the thread.
///
/// [`Barrier`] internally prevents the global epoch value from passing through the value
/// announced by the current thread, thus keeping reachable instances in the thread from being
/// garbage collected.
pub struct Barrier {
    collector_ptr: *mut Collector,
}

impl Barrier {
    /// Creates a new [`Barrier`].
    ///
    /// # Panics
    ///
    /// The maximum number of [`Barrier`] instances in a thread is limited to `u32::MAX`; a
    /// thread panics when the number of [`Barrier`] instances in the thread exceeds the limit.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    ///
    /// let barrier = Barrier::new();
    /// ```
    #[must_use]
    #[inline]
    pub fn new() -> Barrier {
        let collector_ptr = Collector::current();
        unsafe {
            (*collector_ptr).new_barrier();
        }
        Barrier { collector_ptr }
    }

    /// Executes the supplied closure at a later point of time.
    ///
    /// It is guaranteed that the closure will be executed when every [`Barrier`] at the moment
    /// when the method was invoked is dropped, however it is totally non-deterministic when
    /// exactly the closure will be executed.
    ///
    /// Note that the supplied closure is stored in the heap memory, and it has to be `Sync` as it
    /// can be referred to by another thread.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    ///
    /// let barrier = Barrier::new();
    /// barrier.defer_execute(|| println!("deferred"));
    /// ```
    #[inline]
    pub fn defer_execute<F: 'static + FnOnce() + Sync>(&self, f: F) {
        self.reclaim(Arc::new(DeferredClosure { f: Some(f) }));
    }

    /// Executes the supplied closure incrementally at a later point of time.
    ///
    /// It is guaranteed that the closure will be executed when every [`Barrier`] at the moment
    /// when the method was invoked is dropped, however it is totally non-deterministic when
    /// exactly the closure will be executed.
    ///
    /// Note that the supplied closure is stored in the heap memory, and it has to be `Sync` as it
    /// can be referred to by another thread.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    ///
    /// let barrier = Barrier::new();
    /// let mut data = 3;
    /// barrier.defer_incremental_execute(move || {
    ///     if data == 0 {
    ///         return true;
    ///     }
    ///     data -= 1;
    ///     false
    /// });
    /// ```
    #[inline]
    pub fn defer_incremental_execute<F: 'static + FnMut() -> bool + Sync>(&self, f: F) {
        let null_dyn: *const DeferredIncrementalClosure<F> = null();
        let boxed = Box::new(DeferredIncrementalClosure { f, next: null_dyn });
        self.reclaim_link(Box::into_raw(boxed) as *const DeferredIncrementalClosure<F>
            as *mut DeferredIncrementalClosure<F>);
    }

    /// Reclaims an [`Arc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, Barrier};
    ///
    /// let arc: Arc<usize> = Arc::new(47);
    /// let barrier = Barrier::new();
    /// barrier.reclaim(arc);
    /// ```
    #[inline]
    pub fn reclaim<T: 'static>(&self, arc: Arc<T>) {
        if let Some(ptr) = arc.drop_ref() {
            self.reclaim_link(ptr);
        }
        std::mem::forget(arc);
    }

    /// Reclaims the supplied instance.
    #[inline]
    pub(super) fn reclaim_link(&self, link: *mut dyn Link) {
        unsafe {
            (*self.collector_ptr).reclaim(link);
        }
    }
}

impl Default for Barrier {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Barrier {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            (*self.collector_ptr).end_barrier();
        }
    }
}

struct DeferredClosure<F: 'static + FnOnce() + Sync> {
    f: Option<F>,
}

impl<F: 'static + FnOnce() + Sync> Drop for DeferredClosure<F> {
    #[inline]
    fn drop(&mut self) {
        if let Some(f) = self.f.take() {
            f();
        }
    }
}

struct DeferredIncrementalClosure<F: 'static + FnMut() -> bool + Sync> {
    f: F,
    next: *const dyn Link,
}

impl<F: 'static + FnMut() -> bool + Sync> Link for DeferredIncrementalClosure<F> {
    fn set(&mut self, next_ptr: *const dyn Link) {
        self.next = next_ptr;
    }
    fn free(&mut self) -> *mut dyn Link {
        let next = self.next as *mut dyn Link;
        if (self.f)() {
            // Finished, thus drop `self`.
            unsafe { Box::from_raw(self as *mut Self) };
        } else {
            // Push itself into the garbage queue.
            let null_dyn: *const DeferredIncrementalClosure<F> = null();
            self.next = null_dyn;
            unsafe {
                (*Collector::current()).reclaim_confirmed(self as *mut Self);
            }
        }
        next
    }
}
