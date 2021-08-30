//! Epoch-based reclamation.
//!
//! It replaces `crossbeam_epoch` in order to reduce the number of `unsafe` blocks by not
//! implementing unmanaged handles and pointers.
//!
//! It heavily relies on the `SeqCst` ordering, and therefore it performs best on `AArch64`.

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
