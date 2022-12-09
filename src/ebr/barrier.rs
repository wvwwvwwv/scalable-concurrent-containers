use super::collectible::{Collectible, DeferredClosure, DeferredIncrementalClosure};
use super::collector::Collector;

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
    #[inline]
    #[must_use]
    pub fn new() -> Barrier {
        let collector_ptr = Collector::current();
        unsafe {
            (*collector_ptr).new_barrier();
        }
        Barrier { collector_ptr }
    }

    /// Defers dropping and memory reclamation of the supplied [`Box`] of a type implementing
    /// [`Collectible`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Barrier, Collectible};
    /// use std::ptr::NonNull;
    ///
    /// struct C(usize, Option<NonNull<dyn Collectible>>);
    ///
    /// impl Collectible for C {
    ///     fn next_ptr_mut(&mut self) -> &mut Option<NonNull<dyn Collectible>> {
    ///         &mut self.1
    ///     }
    /// }
    ///
    /// let boxed: Box<C> = Box::new(C(7, None));
    ///
    /// let static_ref: &'static C = unsafe { std::mem::transmute(&*boxed) };
    ///
    /// let barrier = Barrier::new();
    /// barrier.defer(boxed);
    ///
    /// assert_eq!(static_ref.0, 7);
    /// ```
    #[inline]
    pub fn defer(&self, collectible: Box<dyn Collectible>) {
        self.collect(Box::into_raw(collectible) as *mut dyn Collectible);
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
        self.defer(Box::new(DeferredClosure::new(f)));
    }

    /// Executes the supplied closure incrementally at a later point of time.
    ///
    /// The closure will be repeatedly invoked until it returns `true`. The closure is able to keep
    /// its internal state by capturing `mut` variables, thus making itself as a state machine;
    /// this implies that the closure is able to emulate incremental execution of arbitrary
    /// `'static` and `Sync` code.
    ///
    /// It is guaranteed that the closure will be executed when every [`Barrier`] at the moment
    /// when the method was invoked is dropped, however it is totally non-deterministic when
    /// exactly the closure will be executed.
    ///
    /// Note that the supplied closure is stored in the heap memory, and it has to be `Sync` as it
    /// can be referred to by another thread. Furthermore, the closure can be invoked at any
    /// arbitrary moment of time once after it was invoked for the first time.
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
        self.defer(Box::new(DeferredIncrementalClosure::new(f)));
    }

    /// Reclaims the supplied instance.
    #[inline]
    pub(super) fn collect(&self, collectible: *mut dyn Collectible) {
        unsafe {
            (*self.collector_ptr).reclaim(collectible);
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
