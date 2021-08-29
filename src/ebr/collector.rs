use super::tag;
use super::underlying::Link;

use std::cell::RefCell;
use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};
use std::sync::atomic::{AtomicPtr, AtomicU64};

/// [`Collector`] is a garbage collector that collects thread-locally unreachable instances,
/// and reclaims them when it is certain that no other threads can reach the instances.
pub(super) struct Collector {
    num_readers: usize,
    next_epoch_update: usize,
    announcement: AtomicU64,
    epoch_ref: &'static AtomicU64,
    current_instance_link: Option<NonNull<dyn Link>>,
    previous_instance_link: Option<NonNull<dyn Link>>,
    next_instance_link: Option<NonNull<dyn Link>>,
    next_collector: *mut Collector,
    link: *const dyn Link,
}

impl Collector {
    /// The value represents an inactive state of the collector.
    const INACTIVE: u64 = 1_u64 << 63;

    /// Epoch update cadence.
    const CADENCE: usize = 256;

    /// Invalid [`Collector`].
    const INVALID: u64 = u64::MAX;

    /// Allocates a new [`Collector`].
    fn alloc() -> *mut Collector {
        let nullptr: *const Collector = ptr::null();
        let boxed = Box::new(Collector {
            num_readers: 0,
            next_epoch_update: Self::CADENCE,
            announcement: AtomicU64::new(Self::INACTIVE),
            epoch_ref: &EPOCH,
            current_instance_link: None,
            previous_instance_link: None,
            next_instance_link: None,
            next_collector: ptr::null_mut(),
            link: nullptr,
        });
        let ptr = Box::into_raw(boxed);
        let mut current = ANCHOR.load(Relaxed);
        loop {
            unsafe { (*ptr).next_collector = tag::unset_tags(current) as *mut Collector };
            let new = if tag::tags(current).0 {
                // It keeps the tag intact.
                tag::update_tags(ptr, true, false) as *mut Collector
            } else {
                ptr
            };
            if let Err(actual) = ANCHOR.compare_exchange(current, new, Release, Relaxed) {
                current = actual;
            } else {
                break;
            }
        }
        ptr
    }

    /// Acknowledges a new [`Barrier`] being instantiated.
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

    /// Acknowledges an existing [`Barrier`] being dropped.
    pub(super) fn end_barrier(&mut self) {
        if self.num_readers == 1 {
            if self.next_epoch_update == 0 {
                self.next_epoch_update = Self::CADENCE;
                if !tag::tags(ANCHOR.load(Relaxed)).0 {
                    self.try_scan();
                }
            } else {
                self.next_epoch_update -= 1;
            }
            self.announcement
                .store(self.announcement.load(Relaxed) | Self::INACTIVE, SeqCst);
        }
        self.num_readers -= 1;
    }

    /// Reclaims garbage instances.
    pub(super) fn reclaim(&mut self, instance_ptr: *mut dyn Link) {
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

    /// Tries to scan the [`Collector`] instances to update the global epoch.
    fn try_scan(&mut self) {
        // Only one thread that acquires the anchor lock is allowed to scan the thread-local
        // collectors.
        if let Ok(mut collector_ptr) = ANCHOR.fetch_update(Acquire, Acquire, |p| {
            if tag::tags(p).0 {
                None
            } else {
                Some(tag::update_tags(p, true, false) as *mut Collector)
            }
        }) {
            let announcement = self.announcement.load(Relaxed);
            let mut update_global_epoch = true;
            let mut prev_collector_ptr: *mut Collector = ptr::null_mut();
            while let Some(other_collector_ref) = unsafe { collector_ptr.as_ref() } {
                if !ptr::eq(self, other_collector_ref) {
                    let other_announcement = other_collector_ref.announcement.load(SeqCst);
                    if other_announcement & Self::INACTIVE == 0
                        && other_announcement != announcement
                    {
                        // Not ready for an epoch update.
                        update_global_epoch = false;
                        break;
                    } else if other_announcement == Self::INVALID {
                        // The collector is obsolete.
                        let reclaimable = if let Some(prev_collector_ref) =
                            unsafe { prev_collector_ptr.as_mut() }
                        {
                            (*prev_collector_ref).next_collector =
                                other_collector_ref.next_collector;
                            true
                        } else {
                            ANCHOR
                                .fetch_update(Release, Relaxed, |p| {
                                    debug_assert!(tag::tags(p).0);
                                    if ptr::eq(tag::unset_tags(p), collector_ptr) {
                                        Some(tag::update_tags(
                                            other_collector_ref.next_collector,
                                            true,
                                            false,
                                        )
                                            as *mut Collector)
                                    } else {
                                        None
                                    }
                                })
                                .is_ok()
                        };
                        if reclaimable {
                            collector_ptr = other_collector_ref.next_collector;
                            let ptr = other_collector_ref as *const Collector as *mut Collector;
                            self.reclaim(ptr);
                            continue;
                        }
                    }
                }
                prev_collector_ptr = collector_ptr;
                collector_ptr = other_collector_ref.next_collector;
            }
            if update_global_epoch {
                self.epoch_ref.store(announcement + 1, SeqCst);
                self.epoch_updated(announcement + 1);
            }

            // Unlocks the anchor.
            while ANCHOR
                .fetch_update(Release, Relaxed, |p| {
                    debug_assert!(tag::tags(p).0);
                    Some(tag::unset_tags(p) as *mut Collector)
                })
                .is_err()
            {}
        }
    }

    /// Acknowledges a new global epoch.
    fn epoch_updated(&mut self, new_epoch: u64) {
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

    /// Returns the [`Collector`] attached to the current thread.
    pub(super) fn current() -> *mut Collector {
        TLS.with(|tls| tls.borrow_mut().collector_ptr)
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        self.epoch_updated(0);
        self.epoch_updated(1);
        self.epoch_updated(2);
    }
}

impl Link for Collector {
    fn set(&mut self, next_ptr: *const dyn Link) {
        self.link = next_ptr;
    }
    fn free(&mut self) -> *mut dyn Link {
        let next = self.link as *mut dyn Link;
        unsafe { Box::from_raw(self as *mut Collector) };
        next
    }
}

/// The global epoch.
static EPOCH: AtomicU64 = AtomicU64::new(0);

/// The global anchor for thread-local instances of [`Collector`].
static ANCHOR: AtomicPtr<Collector> = AtomicPtr::new(std::ptr::null_mut());

/// A wrapper of [`Collector`] for a thread to properly cleanup collected instances.
struct ThreadLocal {
    collector_ptr: *mut Collector,
}

impl Drop for ThreadLocal {
    fn drop(&mut self) {
        if let Some(collector_ref) = unsafe { self.collector_ptr.as_mut() } {
            collector_ref.announcement.store(Collector::INVALID, SeqCst);
        }
    }
}

thread_local! {
    static TLS: RefCell<ThreadLocal> = RefCell::new(ThreadLocal { collector_ptr: Collector::alloc() });
}
