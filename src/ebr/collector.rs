use super::tag::Tag;
use super::underlying::Link;

use std::panic;
use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::fence;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};
use std::sync::atomic::{AtomicPtr, AtomicU8};

/// [`Collector`] is a garbage collector that reclaims thread-locally unreachable instances
/// when they are globally unreachable.
pub(super) struct Collector {
    state: AtomicU8,
    announcement: u8,
    next_epoch_update: u8,
    num_readers: u32,
    num_instances: usize,
    previous_instance_link: Option<NonNull<dyn Link>>,
    current_instance_link: Option<NonNull<dyn Link>>,
    next_instance_link: Option<NonNull<dyn Link>>,
    next_collector: *mut Collector,
    link: *const dyn Link,
}

impl Collector {
    /// The cadence of an epoch update.
    const CADENCE: u8 = u8::MAX;

    /// A bit field representing a thread state where the thread does not have a
    /// [`Barrier`](super::Barrier).
    const INACTIVE: u8 = 1_u8 << 2;

    /// A bit field representing a thread state where the thread has been terminated.
    const INVALID: u8 = 1_u8 << 3;

    /// Allocates a new [`Collector`].
    fn alloc() -> *mut Collector {
        let null_ptr: *const Collector = ptr::null();
        let boxed = Box::new(Collector {
            state: AtomicU8::new(Self::INACTIVE),
            announcement: 0,
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
            unsafe {
                (*ptr).next_collector = Tag::unset_tag(current) as *mut Collector;
            }
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
    ///
    /// # Panics
    ///
    /// The method may panic if the number of readers has reached `u32::MAX`.
    #[inline]
    pub(super) fn new_barrier(&mut self) {
        if self.num_readers == 0 {
            debug_assert_eq!(self.state.load(Relaxed) & Self::INACTIVE, Self::INACTIVE);
            self.num_readers = 1;
            let new_epoch = EPOCH.load(Relaxed);
            if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
                // This special optimization is excerpted from
                // [`crossbeam_epoch`](https://docs.rs/crossbeam-epoch/).
                //
                // The rationale behind the code is, it compiles to `lock xchg` that
                // practically acts as a full memory barrier on `X86`, and is much faster than
                // `mfence`.
                self.state.swap(new_epoch, SeqCst);
            } else {
                // What will happen after the fence strictly happens after the fence.
                self.state.store(new_epoch, Relaxed);
                fence(SeqCst);
            }
            if self.announcement != new_epoch {
                self.announcement = new_epoch;
                self.epoch_updated();
            }
        } else if self.num_readers == u32::MAX {
            panic!("Too many EBR barriers");
        } else {
            debug_assert_eq!(self.state.load(Relaxed) & Self::INACTIVE, 0);
            self.num_readers += 1;
        }
    }

    /// Acknowledges an existing [`Barrier`] being dropped.
    #[inline]
    pub(super) fn end_barrier(&mut self) {
        debug_assert_eq!(self.state.load(Relaxed) & Self::INACTIVE, 0);
        debug_assert_eq!(self.state.load(Relaxed), self.announcement);

        if self.num_readers == 1 {
            if self.next_epoch_update == 0 {
                self.next_epoch_update = Self::CADENCE;
                if self.num_instances != 0 && Tag::into_tag(ANCHOR.load(Relaxed)) != Tag::First {
                    self.try_scan();
                    if self.num_instances != 0 {
                        // If garbage instances remain, the cadence is reduced to a quarter.
                        self.next_epoch_update /= 4;
                    }
                }
            } else {
                self.next_epoch_update -= 1;
            }

            // What has happened cannot happen after the thread setting itself inactive.
            self.state
                .store(self.announcement | Self::INACTIVE, Release);
            self.num_readers = 0;
        } else {
            self.num_readers -= 1;
        }
    }

    /// Reclaims garbage instances.
    pub(super) fn reclaim(&mut self, instance_ptr: *mut dyn Link) {
        debug_assert_eq!(self.state.load(Relaxed) & Self::INACTIVE, 0);
        debug_assert_eq!(self.state.load(Relaxed), self.announcement);

        if let Some(mut ptr) = NonNull::new(instance_ptr) {
            unsafe {
                if let Some(head) = self.current_instance_link.as_ref() {
                    ptr.as_mut().set(head.as_ptr());
                }
                self.current_instance_link.replace(ptr);
                self.num_instances += 1;
                if self.next_epoch_update != 0 {
                    self.next_epoch_update -= 1;
                }
            }
        }
    }

    /// Tries to scan the [`Collector`] instances to update the global epoch.
    fn try_scan(&mut self) {
        debug_assert_eq!(self.state.load(Relaxed) & Self::INACTIVE, 0);
        debug_assert_eq!(self.state.load(Relaxed), self.announcement);

        // Only one thread that acquires the anchor lock is allowed to scan the thread-local
        // collectors.
        let lock_result = ANCHOR.fetch_update(Acquire, Acquire, |p| {
            if Tag::into_tag(p) == Tag::First {
                None
            } else {
                Some(Tag::update_tag(p, Tag::First) as *mut Collector)
            }
        });
        if let Ok(mut collector_ptr) = lock_result {
            #[allow(clippy::blocks_in_if_conditions)]
            let _scope = scopeguard::guard(&ANCHOR, |a| {
                // Unlock the anchor.
                while a
                    .fetch_update(Release, Relaxed, |p| {
                        debug_assert!(Tag::into_tag(p) == Tag::First);
                        Some(Tag::unset_tag(p) as *mut Collector)
                    })
                    .is_err()
                {}
            });

            let known_epoch = self.state.load(Relaxed);
            let mut update_global_epoch = true;
            let mut prev_collector_ptr: *mut Collector = ptr::null_mut();
            while let Some(other_collector_ref) = unsafe { collector_ptr.as_ref() } {
                if !ptr::eq(self, other_collector_ref) {
                    let other_state = other_collector_ref.state.load(Relaxed);
                    if (other_state & Self::INVALID) != 0 {
                        // The collector is obsolete.
                        let reclaimable = unsafe { prev_collector_ptr.as_mut() }.map_or_else(
                            || {
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
                            },
                            |prev_collector_ref| {
                                (*prev_collector_ref).next_collector =
                                    other_collector_ref.next_collector;
                                true
                            },
                        );
                        if reclaimable {
                            collector_ptr = other_collector_ref.next_collector;
                            let ptr = other_collector_ref as *const Collector as *mut Collector;
                            self.reclaim(ptr);
                            continue;
                        }
                    } else if (other_state & Self::INACTIVE) == 0 && other_state != known_epoch {
                        // Not ready for an epoch update.
                        update_global_epoch = false;
                        break;
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
                self.state.store(next_epoch, Relaxed);
                self.announcement = next_epoch;
                self.epoch_updated();
            }
        }
    }

    /// Acknowledges a new global epoch.
    fn epoch_updated(&mut self) {
        debug_assert_eq!(self.state.load(Relaxed) & Self::INACTIVE, 0);
        debug_assert_eq!(self.state.load(Relaxed), self.announcement);

        let mut garbage_link = self.next_instance_link.take();
        self.next_instance_link = self.previous_instance_link.take();
        self.previous_instance_link = self.current_instance_link.take();
        while let Some(mut instance_ptr) = garbage_link.take() {
            let next = unsafe { instance_ptr.as_mut().free() };

            // `self.num_instances` may have been updated when the instance is dropped, therefore
            // `load(self.num_instances)` must not pass through dropping the instance.
            std::sync::atomic::compiler_fence(Acquire);

            self.num_instances -= 1;
            garbage_link = NonNull::new(next);
        }
    }

    /// Returns the [`Collector`] attached to the current thread.
    pub(super) fn current() -> *mut Collector {
        TLS.with(|tls| {
            let ptr = tls.collector_ptr.load(Relaxed);
            if ptr.is_null() {
                let new_collector = Collector::alloc();
                tls.collector_ptr.store(new_collector, Relaxed);
                new_collector
            } else {
                ptr
            }
        })
    }

    /// Suspends the thread by passing the current `Collector` to another thread.
    pub(super) fn suspend() -> bool {
        TLS.with(|tls| {
            let ptr = tls.collector_ptr.load(Relaxed);
            if !ptr.is_null() {
                unsafe {
                    if (*ptr).num_readers == 0 {
                        (*ptr).state.fetch_or(Collector::INVALID, Release);
                        tls.collector_ptr.store(ptr::null_mut(), Relaxed);
                        return true;
                    }
                }
            }
            false
        })
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        self.state.store(0, Relaxed);
        self.announcement = 0;
        self.epoch_updated();
        self.epoch_updated();
        self.epoch_updated();
        debug_assert_eq!(self.num_instances, 0);
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
static ANCHOR: AtomicPtr<Collector> = AtomicPtr::new(ptr::null_mut());

/// A wrapper of [`Collector`] for a thread to properly cleanup collected instances.
struct ThreadLocal {
    collector_ptr: AtomicPtr<Collector>,
}

impl Drop for ThreadLocal {
    fn drop(&mut self) {
        if let Some(collector_ref) = unsafe { self.collector_ptr.load(Relaxed).as_mut() } {
            collector_ref.state.fetch_or(Collector::INVALID, Release);
        }
    }
}

thread_local! {
    static TLS: ThreadLocal = ThreadLocal { collector_ptr: AtomicPtr::default() };
}
