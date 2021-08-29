use super::underlying::Link;
use super::Barrier;

use std::cell::RefCell;
use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};
use std::sync::atomic::{AtomicPtr, AtomicU64};

/// [`Reclaimer`] is a container where unowned instances stay until they become completely
/// unreachable.
pub struct Reclaimer {
    num_readers: usize,
    next_epoch_update: usize,
    announcement: AtomicU64,
    epoch_ref: &'static AtomicU64,
    current_instance_link: Option<NonNull<dyn Link>>,
    previous_instance_link: Option<NonNull<dyn Link>>,
    next_instance_link: Option<NonNull<dyn Link>>,
    next_reclaimer: *mut Reclaimer,
    link: *const dyn Link,
}

impl Reclaimer {
    /// The value represents an inactive state of the reclaimer.
    const INACTIVE: u64 = 1_u64 << 63;

    /// Epoch update cadence.
    const CADENCE: usize = 256;

    /// Allocates a new [`Reclaimer`].
    fn alloc() -> *mut Reclaimer {
        let nullptr: *const Reclaimer = ptr::null();
        let boxed = Box::new(Reclaimer {
            num_readers: 0,
            next_epoch_update: Self::CADENCE,
            announcement: AtomicU64::new(Self::INACTIVE),
            epoch_ref: &EPOCH,
            current_instance_link: None,
            previous_instance_link: None,
            next_instance_link: None,
            next_reclaimer: ptr::null_mut(),
            link: nullptr,
        });
        let ptr = Box::into_raw(boxed);

        let mut current = ANCHOR.load(Relaxed);
        unsafe { (*ptr).next_reclaimer = current };
        while let Err(actual) = ANCHOR.compare_exchange(current, ptr, Release, Relaxed) {
            current = actual;
            unsafe { (*ptr).next_reclaimer = current };
        }
        ptr
    }

    pub(super) fn new_barrier(&mut self) {
        self.num_readers += 1;
        if self.num_readers == 1 {
            let announcement = self.announcement.load(Relaxed) & (!Self::INACTIVE);
            self.announcement.store(announcement, SeqCst);
            let new_epoch = self.epoch_ref.load(SeqCst);
            if announcement != new_epoch {
                self.epoch_updated(new_epoch);
            }
        }
    }

    pub(super) fn end_barrier(&mut self) {
        self.num_readers -= 1;
        if self.num_readers == 0 {
            let announcement = self.announcement.load(Relaxed);
            if self.next_epoch_update == 0 {
                // TODO: OPTIMIZE IT.
                let mut reclaimer_ptr = ANCHOR.load(Acquire);
                let mut update_global_epoch = true;
                while let Some(reclaimer_ref) = unsafe { reclaimer_ptr.as_ref() } {
                    if !ptr::eq(self, reclaimer_ref) {
                        let announcement_of_other = reclaimer_ref.announcement.load(SeqCst);
                        if announcement_of_other & Self::INACTIVE == 0
                            && announcement_of_other != announcement
                        {
                            update_global_epoch = false;
                            break;
                        }
                    }
                    reclaimer_ptr = reclaimer_ref.next_reclaimer;
                }
                if update_global_epoch {
                    self.epoch_ref.store(announcement + 1, SeqCst);
                    self.epoch_updated(announcement + 1);
                }
                self.next_epoch_update = Self::CADENCE;
            } else {
                self.next_epoch_update -= 1;
            }
            self.announcement
                .store(announcement | Self::INACTIVE, SeqCst);
        }
    }

    pub(super) fn reclaim(&mut self, _barrier: &Barrier, instance_ptr: *mut dyn Link) {
        debug_assert_eq!(self.announcement.load(Relaxed) & Self::INACTIVE, 0);

        if let Some(mut ptr) = NonNull::new(instance_ptr) {
            unsafe {
                if let Some(head) = self.current_instance_link.as_ref() {
                    ptr.as_mut().set(head.as_ptr());
                }
                self.current_instance_link.replace(ptr);
            }
        }
    }

    fn epoch_updated(&mut self, new_epoch: u64) {
        debug_assert_eq!(self.announcement.load(Relaxed) & Self::INACTIVE, 0);

        // A relaxed store suffices here.
        self.announcement.store(new_epoch, Relaxed);
        let mut garbage_link = self.next_instance_link.take();
        self.next_instance_link = self.previous_instance_link.take();
        self.previous_instance_link = self.current_instance_link.take();
        while let Some(mut instance_ptr) = garbage_link.take() {
            let next = unsafe { instance_ptr.as_mut().free() };
            if let Some(ptr) = NonNull::new(next) {
                garbage_link.replace(ptr);
            }
        }
    }

    pub(super) fn current() -> *mut Reclaimer {
        TLS.with(|tls| *tls.borrow_mut())
    }
}

impl Drop for Reclaimer {
    fn drop(&mut self) {
        todo!()
    }
}

impl Link for Reclaimer {
    fn set(&mut self, next_ptr: *const dyn Link) {
        self.link = next_ptr;
    }
    fn free(&mut self) -> *mut dyn Link {
        let next = self.link as *mut dyn Link;
        unsafe { Box::from_raw(self as *mut Reclaimer) };
        next
    }
}

static EPOCH: AtomicU64 = AtomicU64::new(0);
static ANCHOR: AtomicPtr<Reclaimer> = AtomicPtr::new(std::ptr::null_mut());

thread_local! {
    static TLS: RefCell<*mut Reclaimer> = RefCell::new(Reclaimer::alloc());
}
