use super::Reader;
use super::link::Link;

use std::cell::RefCell;
use std::ptr;
use std::sync::atomic::Ordering::{Relaxed, Release, SeqCst};
use std::sync::atomic::{AtomicPtr, AtomicU64};

/// [`Reclaimer`] is a container where unowned instances stay until they become completely
/// unreachable.
pub struct Reclaimer {
    announcement: AtomicU64,
    epoch_ref: &'static AtomicU64,
    next_ptr: *mut Reclaimer,
    link: *const dyn Link,
}

impl Reclaimer {
    fn alloc() -> *mut Reclaimer {
        let nullptr: *const Reclaimer = ptr::null();
        let boxed = Box::new(Reclaimer {
            announcement: AtomicU64::new(0),
            epoch_ref: &EPOCH,
            next_ptr: ptr::null_mut(),
            link: nullptr,
        });
        let ptr = Box::into_raw(boxed);

        let mut current = ANCHOR.load(Relaxed);
        unsafe { (*ptr).next_ptr = current };
        while let Err(actual) = ANCHOR.compare_exchange(current, ptr, Release, Relaxed) {
            current = actual;
            unsafe { (*ptr).next_ptr = current };
        }
        ptr
    }
}

impl Reclaimer {
    pub fn read() -> Reader {
        let reclaimer_ptr = TLS.with(|tls| *tls.borrow_mut());
        Reader::new(reclaimer_ptr)
    }
}

impl Drop for Reclaimer {
    fn drop(&mut self) {
        todo!()
    }
}

impl Link for Reclaimer {
    fn next(&self) -> *const dyn Link {
        self.link
    }

    fn set(&mut self, next_ptr: *const dyn Link) {
        self.link = next_ptr;
    }

    fn dealloc(&mut self) {
        unsafe { Box::from_raw(self as *mut Reclaimer) };
    }
}

static EPOCH: AtomicU64 = AtomicU64::new(0);
static ANCHOR: AtomicPtr<Reclaimer> = AtomicPtr::new(std::ptr::null_mut());

thread_local! {
    static TLS: RefCell<*mut Reclaimer> = RefCell::new(Reclaimer::alloc());
}
