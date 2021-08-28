//! Epoch-based reclamation.
//!
//! It replaces `crossbeam_epoch` in order to reduce the number of `unsafe` blocks by not
//! implementing unmanaged handles and pointers.
//!
//! It heavily relies on the `SeqCst` ordering, and therefore it performs best on an `AArch64`
//! machine.

mod arc;
pub use arc::Arc;

mod atomic_arc;
pub use atomic_arc::AtomicArc;

mod link;

mod ptr;
pub use ptr::Ptr;

mod reclaimer;
pub use reclaimer::Reclaimer;

mod reader;
pub use reader::Reader;

