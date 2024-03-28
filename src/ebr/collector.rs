use super::{Collectible, Guard, Tag};
use crate::exit_guard::ExitGuard;
use std::panic;
use std::ptr::{self, NonNull};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};
use std::sync::atomic::{fence, AtomicPtr, AtomicU8};

/// [`Collector`] is a garbage collector that reclaims thread-locally unreachable instances
/// when they are globally unreachable.
#[derive(Debug)]
pub(super) struct Collector {
    state: AtomicU8,
    announcement: u8,
    next_epoch_update: u8,
    has_garbage: bool,
    num_readers: u32,
    previous_instance_link: Option<NonNull<dyn Collectible>>,
    current_instance_link: Option<NonNull<dyn Collectible>>,
    next_instance_link: Option<NonNull<dyn Collectible>>,
    next_link: *mut Collector,
    link: Option<NonNull<dyn Collectible>>,
}

impl Collector {
    /// The cadence of an epoch update.
    const CADENCE: u8 = u8::MAX;

    /// A bit field representing a thread state where the thread does not have a
    /// [`Guard`].
    const INACTIVE: u8 = 1_u8 << 2;

    /// A bit field representing a thread state where the thread has been terminated.
    const INVALID: u8 = 1_u8 << 3;

    /// Acknowledges a new [`Guard`] being instantiated.
    ///
    /// Returns `true` if a new epoch was announced.
    ///
    /// # Panics
    ///
    /// The method may panic if the number of readers has reached `u32::MAX`.
    #[inline]
    pub(super) fn new_guard(&mut self) -> bool {
        if self.num_readers == 0 {
            debug_assert_eq!(self.state.load(Relaxed) & Self::INACTIVE, Self::INACTIVE);
            self.num_readers = 1;
            let new_epoch = EPOCH.load(Relaxed);
            if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
                // This special optimization is excerpted from
                // [`crossbeam_epoch`](https://docs.rs/crossbeam-epoch/).
                //
                // The rationale behind the code is, it compiles to `lock xchg` that
                // practically acts as a full memory guard on `X86`, and is much faster than
                // `mfence`.
                self.state.swap(new_epoch, SeqCst);
            } else {
                // What will happen after the fence strictly happens after the fence.
                self.state.store(new_epoch, Relaxed);
                fence(SeqCst);
            }
            if self.announcement == new_epoch {
                false
            } else {
                self.announcement = new_epoch;
                true
            }
        } else if self.num_readers == u32::MAX {
            panic!("Too many EBR guards");
        } else {
            debug_assert_eq!(self.state.load(Relaxed) & Self::INACTIVE, 0);
            self.num_readers += 1;
            false
        }
    }

    /// Acknowledges an existing [`Guard`] being dropped.
    #[inline]
    pub(super) fn end_guard(&mut self) {
        debug_assert_eq!(self.state.load(Relaxed) & Self::INACTIVE, 0);
        debug_assert_eq!(self.state.load(Relaxed), self.announcement);

        if self.num_readers == 1 {
            if self.next_epoch_update == 0 {
                if self.has_garbage || Tag::into_tag(GLOBAL_ANCHOR.load(Relaxed)) != Tag::First {
                    self.try_scan();
                }
                self.next_epoch_update = if self.has_garbage {
                    Self::CADENCE / 4
                } else {
                    Self::CADENCE
                };
            } else {
                self.next_epoch_update = self.next_epoch_update.saturating_sub(1);
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
    #[inline]
    pub(super) fn reclaim(&mut self, instance_ptr: *mut dyn Collectible) {
        if let Some(mut ptr) = NonNull::new(instance_ptr) {
            unsafe {
                *ptr.as_mut().next_ptr_mut() = self.current_instance_link.take();
                self.current_instance_link.replace(ptr);
                self.next_epoch_update = self
                    .next_epoch_update
                    .saturating_sub(1)
                    .min(Self::CADENCE / 4);
                self.has_garbage = true;
            }
        }
    }

    /// Returns the [`Collector`] attached to the current thread.
    #[inline]
    pub(super) fn current() -> *mut Collector {
        LOCAL_COLLECTOR.with(|local_collector| {
            let mut collector_ptr = local_collector.load(Relaxed);
            if collector_ptr.is_null() {
                collector_ptr = COLLECTOR_ANCHOR.with(CollectorAnchor::alloc);
                local_collector.store(collector_ptr, Relaxed);
            }
            collector_ptr
        })
    }

    /// Passes its garbage instances to other threads.
    #[inline]
    pub(super) fn pass_garbage() -> bool {
        LOCAL_COLLECTOR.with(|local_collector| {
            let collector_ptr = local_collector.load(Relaxed);
            if let Some(collector) = unsafe { collector_ptr.as_mut() } {
                if collector.num_readers != 0 {
                    return false;
                }
                if collector.has_garbage {
                    collector.state.fetch_or(Collector::INVALID, Release);
                    local_collector.store(ptr::null_mut(), Relaxed);
                    mark_scan_enforced();
                }
            }
            true
        })
    }

    /// Acknowledges a new global epoch.
    pub(super) fn epoch_updated(&mut self) {
        debug_assert_eq!(self.state.load(Relaxed) & Self::INACTIVE, 0);
        debug_assert_eq!(self.state.load(Relaxed), self.announcement);

        let mut garbage_link = self.next_instance_link.take();
        self.next_instance_link = self.previous_instance_link.take();
        self.previous_instance_link = self.current_instance_link.take();
        self.has_garbage =
            self.next_instance_link.is_some() || self.previous_instance_link.is_some();
        while let Some(mut instance_ptr) = garbage_link.take() {
            garbage_link = unsafe { *instance_ptr.as_mut().next_ptr_mut() };
            let mut guard = ExitGuard::new(garbage_link, |mut garbage_link| {
                while let Some(mut instance_ptr) = garbage_link.take() {
                    // Something went wrong during dropping and deallocating an instance.
                    garbage_link = unsafe { *instance_ptr.as_mut().next_ptr_mut() };

                    // Previous `drop_and_dealloc` may have accessed `self.current_instance_link`.
                    std::sync::atomic::compiler_fence(Acquire);
                    self.reclaim(instance_ptr.as_ptr());
                }
            });

            // `drop_and_dealloc` may access `self.current_instance_link`.
            std::sync::atomic::compiler_fence(Acquire);
            unsafe {
                instance_ptr.as_mut().drop_and_dealloc();
            }
            garbage_link = guard.take();
        }
    }

    /// Allocates a new [`Collector`].
    fn alloc() -> *mut Collector {
        let boxed = Box::new(Collector {
            state: AtomicU8::new(Self::INACTIVE),
            announcement: 0,
            next_epoch_update: Self::CADENCE,
            has_garbage: false,
            num_readers: 0,
            previous_instance_link: None,
            current_instance_link: None,
            next_instance_link: None,
            next_link: ptr::null_mut(),
            link: None,
        });
        let ptr = Box::into_raw(boxed);
        let mut current = GLOBAL_ANCHOR.load(Relaxed);
        loop {
            unsafe {
                (*ptr).next_link = Tag::unset_tag(current).cast_mut();
            }

            // It keeps the tag intact.
            let tag = Tag::into_tag(current);
            let new = Tag::update_tag(ptr, tag).cast_mut();
            if let Err(actual) = GLOBAL_ANCHOR.compare_exchange_weak(current, new, Release, Relaxed)
            {
                current = actual;
            } else {
                break;
            }
        }
        ptr
    }

    /// Tries to scan the [`Collector`] instances to update the global epoch.
    fn try_scan(&mut self) {
        debug_assert_eq!(self.state.load(Relaxed) & Self::INACTIVE, 0);
        debug_assert_eq!(self.state.load(Relaxed), self.announcement);

        // Only one thread that acquires the anchor lock is allowed to scan the thread-local
        // collectors.
        let lock_result = GLOBAL_ANCHOR
            .fetch_update(Acquire, Acquire, |p| {
                let tag = Tag::into_tag(p);
                if tag == Tag::First || tag == Tag::Both {
                    None
                } else {
                    Some(Tag::update_tag(p, Tag::First).cast_mut())
                }
            })
            .map(|p| Tag::unset_tag(p).cast_mut());
        if let Ok(mut collector_ptr) = lock_result {
            let _guard = ExitGuard::new(&GLOBAL_ANCHOR, |a| {
                // Unlock the anchor.
                loop {
                    let result = a.fetch_update(Release, Relaxed, |p| {
                        let tag = Tag::into_tag(p);
                        debug_assert!(tag == Tag::First || tag == Tag::Both);
                        let new_tag = if tag == Tag::Both {
                            Tag::Second
                        } else {
                            Tag::None
                        };
                        Some(Tag::update_tag(p, new_tag).cast_mut())
                    });
                    if result.is_ok() {
                        break;
                    }
                }
            });

            let known_epoch = self.state.load(Relaxed);
            let mut update_global_epoch = true;
            let mut prev_collector_ptr: *mut Collector = ptr::null_mut();
            while let Some(other_collector) = unsafe { collector_ptr.as_ref() } {
                if !ptr::eq(self, other_collector) {
                    let other_state = other_collector.state.load(Relaxed);
                    if (other_state & Self::INVALID) != 0 {
                        // The collector is obsolete.
                        let reclaimable = unsafe { prev_collector_ptr.as_mut() }.map_or_else(
                            || {
                                GLOBAL_ANCHOR
                                    .fetch_update(Release, Relaxed, |p| {
                                        let tag = Tag::into_tag(p);
                                        debug_assert!(tag == Tag::First || tag == Tag::Both);
                                        if ptr::eq(Tag::unset_tag(p), collector_ptr) {
                                            Some(
                                                Tag::update_tag(other_collector.next_link, tag)
                                                    .cast_mut(),
                                            )
                                        } else {
                                            None
                                        }
                                    })
                                    .is_ok()
                            },
                            |prev_collector| {
                                prev_collector.next_link = other_collector.next_link;
                                true
                            },
                        );
                        if reclaimable {
                            collector_ptr = other_collector.next_link;
                            let ptr = (other_collector as *const Collector).cast_mut();
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
                collector_ptr = other_collector.next_link;
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
            }
        }
    }
}

impl Drop for Collector {
    #[inline]
    fn drop(&mut self) {
        self.state.store(0, Relaxed);
        self.announcement = 0;
        while self.has_garbage {
            self.epoch_updated();
        }
    }
}

impl Collectible for Collector {
    #[inline]
    fn next_ptr_mut(&mut self) -> &mut Option<NonNull<dyn Collectible>> {
        &mut self.link
    }
}

/// [`CollectorAnchor`] helps allocate and cleanup the thread-local [`Collector`].
struct CollectorAnchor;

impl CollectorAnchor {
    fn alloc(&self) -> *mut Collector {
        let _: &CollectorAnchor = self;
        Collector::alloc()
    }
}

impl Drop for CollectorAnchor {
    #[inline]
    fn drop(&mut self) {
        try_drop_local_collector();
    }
}

/// Marks `ANCHOR` that there is a potentially unreachable `Collector`.
fn mark_scan_enforced() {
    // `Tag::Second` indicates that there is a garbage `Collector`.
    let _result = GLOBAL_ANCHOR.fetch_update(Release, Relaxed, |p| {
        let new_tag = match Tag::into_tag(p) {
            Tag::None => Tag::Second,
            Tag::First => Tag::Both,
            Tag::Second | Tag::Both => return None,
        };
        Some(Tag::update_tag(p, new_tag).cast_mut())
    });
}

fn try_drop_local_collector() {
    let collector_ptr = LOCAL_COLLECTOR.with(|local_collector| local_collector.load(Relaxed));
    if let Some(collector) = unsafe { collector_ptr.as_mut() } {
        if collector.next_link.is_null() {
            let anchor_ptr = GLOBAL_ANCHOR.load(Relaxed);
            if ptr::eq(collector_ptr, anchor_ptr)
                && GLOBAL_ANCHOR
                    .compare_exchange(anchor_ptr, ptr::null_mut(), Relaxed, Relaxed)
                    .is_ok()
            {
                // If it is the head, and the only `Collector` in the global list, drop it here.
                let guard = Guard::new_for_drop();
                while collector.has_garbage {
                    collector.epoch_updated();
                }
                drop(guard);
                collector.drop_and_dealloc();
                return;
            }
        }
        collector.state.fetch_or(Collector::INVALID, Release);
        mark_scan_enforced();
    }
}

thread_local! {
    static COLLECTOR_ANCHOR: CollectorAnchor = const { CollectorAnchor };
    static LOCAL_COLLECTOR: AtomicPtr<Collector> = AtomicPtr::default();
}

/// The global epoch.
///
/// The global epoch can have one of 0, 1, or 2, and a difference in the local announcement of
/// a thread and the global is considered to be an epoch change to the thread.
static EPOCH: AtomicU8 = AtomicU8::new(0);

/// The global anchor for thread-local instances of [`Collector`].
static GLOBAL_ANCHOR: AtomicPtr<Collector> = AtomicPtr::new(ptr::null_mut());
