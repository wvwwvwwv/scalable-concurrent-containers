use super::Reader;

use std::cell::RefCell;
use std::sync::atomic::{AtomicPtr, AtomicU64};

/// [`Reclaimer`] is a container where unowned instances stay until they become completely
/// unreachable.
pub struct Reclaimer {}

impl Reclaimer {
    pub fn read() -> Reader {
        let reclaimer_ptr = TLS.with(|tls| tls.borrow_mut().as_mut_ref() as *mut _);
        Reader::new(reclaimer_ptr)
    }

    fn as_mut_ref(&mut self) -> &mut Reclaimer {
        self
    }
}

impl Drop for Reclaimer {
    fn drop(&mut self) {
        todo!()
    }
}

static EPOCH: AtomicU64 = AtomicU64::new(0);
static ANCHOR: AtomicPtr<Reclaimer> = AtomicPtr::new(std::ptr::null_mut());

thread_local! {
    static TLS: RefCell<Reclaimer> = RefCell::new(Reclaimer{});
}
