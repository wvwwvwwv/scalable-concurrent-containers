use super::collector::Collector;
use super::underlying::Underlying;
use super::Arc;

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
            self.reclaim_underlying(ptr);
        }
        std::mem::forget(arc);
    }

    /// Suspends the thread.
    ///
    /// If returns `false` if there is an active [`Barrier`] in the thread. Otherwise, it
    /// immediately relinquishes the thread-local garbage collector, so that other threads are able
    /// to reclaim the garbage collector and all the garbage instances in it.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, Barrier};
    /// {
    ///     let arc: Arc<usize> = Arc::new(47);
    ///     let barrier = Barrier::new();
    ///     barrier.reclaim(arc);
    ///     assert!(!Barrier::suspend());
    /// }
    ///
    /// assert!(Barrier::suspend());
    ///
    /// let new_arc: Arc<usize> = Arc::new(17);
    /// let barrier = Barrier::new();
    /// barrier.reclaim(new_arc);
    /// ```
    #[inline]
    #[must_use]
    pub fn suspend() -> bool {
        Collector::suspend()
    }

    /// Reclaims the underlying instance of an [`Arc`] or [`AtomicArc`](super::AtomicArc).
    pub(super) fn reclaim_underlying<T: 'static>(&self, underlying: *mut Underlying<T>) {
        unsafe {
            (*self.collector_ptr).reclaim(underlying);
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
