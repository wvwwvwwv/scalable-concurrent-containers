use super::arc::Underlying;
use super::{Arc, Reclaimer};

/// [`Reader`] allows the user to read [`AtomicArc`](super::AtomicArc).
pub struct Reader {
    reclaimer_ptr: *mut Reclaimer,
}

impl Reader {
    /// Creates a new [`Reader`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::Reader;
    ///
    /// let reader = Reader::new();
    /// ```
    pub fn new() -> Reader {
        let reclaimer_ptr = Reclaimer::current();
        unsafe { (*reclaimer_ptr).start_reader() };
        Reader { reclaimer_ptr }
    }

    /// Reclaims an [`Arc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, Reader};
    ///
    /// let arc: Arc<usize> = Arc::new(47);
    /// let reader = Reader::new();
    /// reader.reclaim(arc);
    /// ```
    pub fn reclaim<T: 'static>(&self, instance: Arc<T>) {
        if unsafe { instance.instance.as_ref().drop_ref() } {
            self.reclaim_underlying(instance.instance.as_ptr());
            std::mem::forget(instance);
        }
    }

    pub(super) fn reclaim_underlying<T: 'static>(&self, underlying: *mut Underlying<T>) {
        unsafe { (*self.reclaimer_ptr).reclaim(self, underlying) };
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        unsafe { (*self.reclaimer_ptr).end_reader() };
    }
}
