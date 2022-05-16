//! Epoch-based reclamation.
//!
//! The epoch consensus algorithm and the use of memory barriers and RMW semantics are similar to
//! that of [`crossbeam_epoch`](https://docs.rs/crossbeam-epoch/), however the API set is vastly
//! different, for instance, `unsafe` blocks are not required to read an instance subject to EBR.
//!
//! # Examples
//!
//! The following code shows [`Arc`], [`AtomicArc`], [`Barrier`], [`Ptr`], and [`Tag`] usage.
//!
//! ```
//! use scc::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};
//! use std::sync::atomic::Ordering::Relaxed;
//!
//! // `atomic_arc` holds a strong reference to `17`.
//! let atomic_arc: AtomicArc<usize> = AtomicArc::new(17);
//!
//! // `barrier` prevents the garbage collector from dropping reachable instances.
//! let barrier: Barrier = Barrier::new();
//!
//! // `ptr` cannot outlive `barrier`.
//! let mut ptr: Ptr<usize> = atomic_arc.load(Relaxed, &barrier);
//! assert_eq!(*ptr.as_ref().unwrap(), 17);
//!
//! // `atomic_arc` can be tagged.
//! atomic_arc.update_tag_if(Tag::First, |t| t == Tag::None, Relaxed);
//!
//! // `ptr` is not tagged, so CAS fails.
//! assert!(atomic_arc.compare_exchange(
//!     ptr,
//!     (Some(Arc::new(18)), Tag::First),
//!     Relaxed,
//!     Relaxed,
//!     &barrier).is_err());
//!
//! // `ptr` can be tagged.
//! ptr.set_tag(Tag::First);
//!
//! // The result of CAS is a handle to the instance that `atomic_arc` previously owned.
//! let prev: Arc<usize> = atomic_arc.compare_exchange(
//!     ptr,
//!     (Some(Arc::new(18)), Tag::Second),
//!     Relaxed,
//!     Relaxed,
//!     &barrier).unwrap().0.unwrap();
//! assert_eq!(*prev, 17);
//!
//! // `17` will be garbage-collected later.
//! drop(prev);
//!
//! // `ebr::AtomicArc` can be converted into `ebr::Arc`.
//! let arc: Arc<usize> = atomic_arc.try_into_arc(Relaxed).unwrap();
//! assert_eq!(*arc, 18);
//!
//! // `18` will be garbage-collected later.
//! drop(arc);
//!
//! // `17` is still valid as `barrier` keeps the garbage collector from dropping it.
//! assert_eq!(*ptr.as_ref().unwrap(), 17);
//!
//! // If the thread is expected to lie dormant, call `Barrier::suspend()` in order for the
//! // thread-local garbage collector and all the garbage instances in it to be reclaimed by other
//! // threads.
//! Barrier::suspend();
//! ```

mod arc;
pub use arc::Arc;

mod atomic_arc;
pub use atomic_arc::AtomicArc;

mod barrier;
pub use barrier::Barrier;

mod ptr;
pub use ptr::Ptr;

mod tag;
pub use tag::Tag;

mod collector;
mod underlying;
