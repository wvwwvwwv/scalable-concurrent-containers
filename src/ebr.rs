//! Epoch-based reclamation.
//!
//! The epoch consensus algorithm and the use of memory barriers and RMW semantics are similar to
//! that of [`crossbeam_epoch`](https://docs.rs/crossbeam-epoch/), however the API set is vastly
//! different, for instance, `unsafe` blocks are not required to read an instance subject to EBR.

mod arc;
pub use arc::Arc;

mod atomic_arc;
pub use atomic_arc::AtomicArc;

mod atomic_owned;
pub use atomic_owned::AtomicOwned;

mod barrier;
pub use barrier::Barrier;

mod collectible;
pub use collectible::Collectible;

mod owned;
pub use owned::Owned;

mod ptr;
pub use ptr::Ptr;

mod tag;
pub use tag::Tag;

mod collector;
mod ref_counted;

/// Suspends the garbage collector of the current thread.
///
/// If returns `false` if there is an active [`Barrier`] in the thread. Otherwise, it passes all
/// its garbage instances to a free flowing garbage container that can be cleaned up by other
/// threads.
///
/// # Examples
///
/// ```
/// use scc::ebr::{suspend, Arc, Barrier};
///
/// assert!(suspend());
///
/// {
///     let arc: Arc<usize> = Arc::new(47);
///     let barrier = Barrier::new();
///     arc.release(&barrier);
///     assert!(!suspend());
/// }
///
/// assert!(suspend());
///
/// let new_arc: Arc<usize> = Arc::new(17);
/// let barrier = Barrier::new();
/// new_arc.release(&barrier);
/// ```
#[inline]
#[must_use]
pub fn suspend() -> bool {
    collector::Collector::pass_garbage()
}
