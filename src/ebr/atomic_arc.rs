use super::arc::Underlying;
use super::{Arc, Ptr, Reader};

use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{self, Relaxed};

/// [`AtomicArc`] owns the underlying instance, and allows users to perform atomic operations
/// on it.
pub struct AtomicArc<T: 'static> {
    underlying: AtomicPtr<Underlying<T>>,
}

impl<T: 'static> AtomicArc<T> {
    /// Creates a new [`AtomicArc`] from an instance of `T`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::AtomicArc;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(10);
    /// ```
    pub fn new(t: T) -> AtomicArc<T> {
        let boxed = Box::new(Underlying::new(t));
        AtomicArc {
            underlying: AtomicPtr::new(Box::into_raw(boxed)),
        }
    }

    /// Creates a null [`AtomicArc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::AtomicArc;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::null();
    /// ```
    pub fn null() -> AtomicArc<T> {
        AtomicArc {
            underlying: AtomicPtr::default(),
        }
    }

    /// Loads a pointer value from the [`AtomicArc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicArc, Reclaimer};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(11);
    /// let reader = Reclaimer::read();
    /// let ptr = atomic_arc.load(Relaxed, &reader);
    /// assert_eq!(*ptr.as_ref().unwrap(), 11);
    /// ```
    pub fn load<'r>(&self, order: Ordering, _reader: &'r Reader) -> Ptr<'r, T> {
        Ptr::new(self.underlying.load(order))
    }

    /// Performs CAS on the atomic pointer.
    pub fn compare_exchange<'r>(
        &self,
        current: Ptr<'r, T>,
        new: Option<Arc<T>>,
        success: Ordering,
        failure: Ordering,
        reader: &'r Reader,
    ) -> Result<(Option<Arc<T>>, Ptr<'r, T>), (Option<Arc<T>>, Ptr<'r, T>)> {
        unimplemented!()
    }
}

impl<T: 'static> Drop for AtomicArc<T> {
    fn drop(&mut self) {
        if let Some(non_null_ptr) = NonNull::new(self.underlying.swap(ptr::null_mut(), Relaxed)) {
            drop(Arc {
                instance: non_null_ptr,
            });
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::ebr::Reclaimer;

    use std::sync::atomic::{AtomicBool, AtomicU8};

    #[test]
    fn arc() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);
        struct A(AtomicU8, usize, &'static AtomicBool);
        impl Drop for A {
            fn drop(&mut self) {
                self.2.swap(true, Relaxed);
            }
        }
        let atomic_arc = AtomicArc::new(A(AtomicU8::new(10), 10, &DESTROYED));
        assert!(!DESTROYED.load(Relaxed));

        drop(atomic_arc);

        while !DESTROYED.load(Relaxed) {
            drop(Reclaimer::read());
        }
    }
}
