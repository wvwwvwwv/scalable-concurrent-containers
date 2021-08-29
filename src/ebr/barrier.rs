use super::underlying::Underlying;
use super::{Arc, Reclaimer};

/// [`Barrier`] allows the user to read [`AtomicArc`](super::AtomicArc), and keeps the
/// underlying instance pinned to the thread.
///
/// [`Barrier`] internally prevents the global epoch value from passing through the value
/// announced by the current thread, thus keeping reachable instances in the thread from being
/// garbage collected.
pub struct Barrier {
    reclaimer_ptr: *mut Reclaimer,
}

impl Barrier {
    /// Creates a new [`Barrier`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Barrier;
    ///
    /// let barrier = Barrier::new();
    /// ```
    pub fn new() -> Barrier {
        let reclaimer_ptr = Reclaimer::current();
        unsafe { (*reclaimer_ptr).new_barrier() };
        Barrier { reclaimer_ptr }
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
    pub fn reclaim<T: 'static>(&self, arc: Arc<T>) {
        if let Some(ptr) = arc.drop_ref() {
            self.reclaim_underlying(ptr);
            std::mem::forget(arc);
        }
    }

    /// Reclaims the underlying instance of an [`Arc`] or [`AtomicArc`](super::AtomicArc).
    pub(super) fn reclaim_underlying<T: 'static>(&self, underlying: *mut Underlying<T>) {
        unsafe { (*self.reclaimer_ptr).reclaim(self, underlying) };
    }
}

impl Drop for Barrier {
    fn drop(&mut self) {
        unsafe { (*self.reclaimer_ptr).end_barrier() };
    }
}
