use super::link::Link;

use std::sync::atomic::AtomicUsize;
use std::mem::ManuallyDrop;

/// [`Arc`] is a reference-counted handle to an instance.
pub struct Arc<T> {
    instance: *const Underlying<T>,
}

union LinkOrRefCnt {
    next: *const dyn Link,
    refcnt: ManuallyDrop<(AtomicUsize, usize)>,
}

/// [`Underlying`] stores the instance and a link to the next [`Underlying`].
struct Underlying<T> {
    next: LinkOrRefCnt,
    t: T,
}