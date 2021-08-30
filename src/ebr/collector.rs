use super::tag::Tag;
use super::underlying::Link;

use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::fence;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};
use std::sync::atomic::{AtomicPtr, AtomicU8};

/// [`Collector`] is a garbage collector that reclaims thread-locally unreachable instances
/// when they are globally unreachable.
pub(super) struct Collector {
    announcement: u8,
    next_epoch_update: usize,
    num_readers: usize,
    num_instances: usize,
    previous_instance_link: Option<NonNull<dyn Link>>,
    current_instance_link: Option<NonNull<dyn Link>>,
    next_instance_link: Option<NonNull<dyn Link>>,
    next_collector: *mut Collector,
    link: *const dyn Link,
}

impl Collector {
    /// The cadence of an epoch update.
    const CADENCE: usize = 256;

    /// Bits representing an epoch.
    const EPOCH_BITS: u8 = (1_u8 << 2) - 1;

    /// A bit field representing a thread state where the thread does not have a
    /// [`Barrier`](super::Barrier).
    const INACTIVE: u8 = 1_u8 << 2;

    /// A bit field representing a thread state where the thread has been terminated.
    const INVALID: u8 = 1_u8 << 3;

    /// Allocates a new [`Collector`].
    fn alloc() -> *mut Collector {
        let null_ptr: *const Collector = ptr::null();
        let boxed = Box::new(Collector {
            announcement: Self::INACTIVE,
            next_epoch_update: Self::CADENCE,
            num_readers: 0,
            num_instances: 0,
            previous_instance_link: None,
            current_instance_link: None,
            next_instance_link: None,
            next_collector: ptr::null_mut(),
            link: null_ptr,
        });
        let ptr = Box::into_raw(boxed);
        let mut current = ANCHOR.load(Relaxed);
        loop {
            unsafe { (*ptr).next_collector = Tag::unset_tag(current) as *mut Collector };
            let new = if let Tag::First = Tag::into_tag(current) {
                // It keeps the tag intact.
                Tag::update_tag(ptr, Tag::First) as *mut Collector
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
    #[inline]
    pub(super) fn new_barrier(&mut self) {
        self.num_readers += 1;
        if self.num_readers == 1 {
            let known_epoch = self.announcement & Self::EPOCH_BITS;
            let new_epoch = EPOCH.load(Relaxed);
            self.announcement = new_epoch;

            // What will happen after the fence strictly happens after the fence.
            fence(SeqCst);

            if known_epoch != new_epoch {
                self.epoch_updated();
            }
        }
    }

    /// Acknowledges an existing [`Barrier`] being dropped.
    #[inline]
    pub(super) fn end_barrier(&mut self) {
        debug_assert_eq!(self.announcement & Self::INACTIVE, 0);

        if self.num_readers == 1 {
            if self.next_epoch_update == 0 {
                self.next_epoch_update = Self::CADENCE;
                if self.num_instances != 0 && Tag::into_tag(ANCHOR.load(Relaxed)) != Tag::First {
                    self.try_scan();
                }
            } else {
                self.next_epoch_update -= 1;
            }

            // What has happened cannot happen after the thread setting itself inactive.
            fence(Release);
            self.announcement |= Self::INACTIVE;
        }
        self.num_readers -= 1;
    }

    /// Reclaims garbage instances.
    pub(super) fn reclaim(&mut self, instance_ptr: *mut dyn Link) {
        debug_assert_eq!(self.announcement & Self::INACTIVE, 0);

        if let Some(mut ptr) = NonNull::new(instance_ptr) {
            unsafe {
                if let Some(head) = self.current_instance_link.as_ref() {
                    ptr.as_mut().set(head.as_ptr());
                }
                self.current_instance_link.replace(ptr);
                self.num_instances += 1;
            }
        }
    }

    /// Tries to scan the [`Collector`] instances to update the global epoch.
    fn try_scan(&mut self) {
        debug_assert_eq!(self.announcement & Self::INACTIVE, 0);

        // Only one thread that acquires the anchor lock is allowed to scan the thread-local
        // collectors.
        if let Ok(mut collector_ptr) = ANCHOR.fetch_update(Acquire, Acquire, |p| {
            if Tag::into_tag(p) == Tag::First {
                None
            } else {
                Some(Tag::update_tag(p, Tag::First) as *mut Collector)
            }
        }) {
            let _scope = scopeguard::guard(&ANCHOR, |a| {
                // Unlocks the anchor.
                while a
                    .fetch_update(Release, Relaxed, |p| {
                        debug_assert!(Tag::into_tag(p) == Tag::First);
                        Some(Tag::unset_tag(p) as *mut Collector)
                    })
                    .is_err()
                {}
            });

            let known_epoch = self.announcement;
            let mut update_global_epoch = true;
            let mut prev_collector_ptr: *mut Collector = ptr::null_mut();
            while let Some(other_collector_ref) = unsafe { collector_ptr.as_ref() } {
                if !ptr::eq(self, other_collector_ref) {
                    if (other_collector_ref.announcement & Self::INACTIVE) == 0
                        && other_collector_ref.announcement != known_epoch
                    {
                        // Not ready for an epoch update.
                        update_global_epoch = false;
                        break;
                    } else if (other_collector_ref.announcement & Self::INVALID) != 0 {
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
                                    debug_assert!(Tag::into_tag(p) == Tag::First);
                                    if ptr::eq(Tag::unset_tag(p), collector_ptr) {
                                        Some(Tag::update_tag(
                                            other_collector_ref.next_collector,
                                            Tag::First,
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
                // It is a new era; a fence is required.
                fence(SeqCst);
                let next_epoch = match known_epoch {
                    0 => 1,
                    1 => 2,
                    _ => 0,
                };
                EPOCH.store(next_epoch, Relaxed);
                self.announcement = next_epoch;
                self.epoch_updated();
            }
        }
    }

    /// Acknowledges a new global epoch.
    fn epoch_updated(&mut self) {
        debug_assert_eq!(self.announcement & Self::INACTIVE, 0);

        let mut num_reclaimed = 0;
        let mut garbage_link = self.next_instance_link.take();
        self.next_instance_link = self.previous_instance_link.take();
        self.previous_instance_link = self.current_instance_link.take();
        while let Some(mut instance_ptr) = garbage_link.take() {
            let next = unsafe { instance_ptr.as_mut().free() };
            num_reclaimed += 1;
            if let Some(ptr) = NonNull::new(next) {
                garbage_link.replace(ptr);
            }
        }
        self.num_instances -= num_reclaimed;
    }

    /// Returns the [`Collector`] attached to the current thread.
    pub(super) fn current() -> *mut Collector {
        TLS.with(|tls| tls.collector_ptr)
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        self.announcement = 0;
        self.epoch_updated();
        self.epoch_updated();
        self.epoch_updated();
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
///
/// The global epoch can have one of 0, 1, or 2, and a difference in the local announcement of
/// a thread and the global is considered to be an epoch change to the thread.
static EPOCH: AtomicU8 = AtomicU8::new(0);

/// The global anchor for thread-local instances of [`Collector`].
static ANCHOR: AtomicPtr<Collector> = AtomicPtr::new(std::ptr::null_mut());

/// A wrapper of [`Collector`] for a thread to properly cleanup collected instances.
struct ThreadLocal {
    collector_ptr: *mut Collector,
}

impl Drop for ThreadLocal {
    fn drop(&mut self) {
        if let Some(collector_ref) = unsafe { self.collector_ptr.as_mut() } {
            collector_ref.announcement |= Collector::INVALID;
        }
    }
}

thread_local! {
    static TLS: ThreadLocal = ThreadLocal { collector_ptr: Collector::alloc() };
}
