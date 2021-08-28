use super::arc::Underlying;
use super::{Arc, Reclaimer};

/// [`Reader`] allows the user to read [`AtomicPtr`].
pub struct Reader {
    reclaimer_ptr: *mut Reclaimer,
}

impl Reader {
    pub fn reclaim<T: 'static>(&self, instance: Arc<T>) {
        if unsafe { instance.instance.as_ref().drop_ref() } {
            self.reclaim_underlying(instance.instance.as_ptr());
            std::mem::forget(instance);
        }
    }

    pub(super) fn reclaim_underlying<T: 'static>(&self, underlying: *mut Underlying<T>) {
        unsafe { (*self.reclaimer_ptr).reclaim(self, underlying) };
    }

    pub(super) fn new(reclaimer_ptr: *mut Reclaimer) -> Reader {
        unsafe { (*reclaimer_ptr).start_reader() };
        Reader { reclaimer_ptr }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        unsafe { (*self.reclaimer_ptr).end_reader() };
    }
}
