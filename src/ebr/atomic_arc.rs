use super::arc::Underlying;
use super::{Arc, Ptr, Reader};

use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{self, Relaxed};

/// [`AtomicArc`] owns the underlying instance, and allows users to perform atomic operations
/// on it.
#[derive(Debug)]
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

    /// Returns `true` if the [`AtomicArc`] is null.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::AtomicArc;
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::null();
    /// assert!(atomic_arc.is_null(Relaxed));
    /// ```
    pub fn is_null(&self, order: Ordering) -> bool {
        self.underlying.load(order).is_null()
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
        Ptr::from(self.underlying.load(order))
    }

    /// Performs CAS on the [`AtomicArc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Reclaimer};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(14);
    /// let reader = Reclaimer::read();
    /// let old = atomic_arc.swap(Some(Arc::new(15)), Relaxed);
    /// assert_eq!(*old.unwrap(), 14);
    /// let old = atomic_arc.swap(None, Relaxed);
    /// assert_eq!(*old.unwrap(), 15);
    /// let old = atomic_arc.swap(None, Relaxed);
    /// assert!(old.is_none());
    /// ```
    pub fn swap(&self, val: Option<Arc<T>>, order: Ordering) -> Option<Arc<T>> {
        let new = val
            .as_ref()
            .map_or_else(ptr::null_mut, |a| a.instance.as_ptr());
        let prev = self.underlying.swap(new, order);
        if let Some(non_null_ptr) = NonNull::new(prev) {
            Some(Arc::from(non_null_ptr))
        } else {
            None
        }
    }

    /// Performs CAS on the [`AtomicArc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Reclaimer};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(17);
    /// let reader = Reclaimer::read();
    /// let ptr = atomic_arc.load(Relaxed, &reader);
    /// let old = atomic_arc.compare_exchange(
    ///     ptr, Some(Arc::new(18)), Relaxed, Relaxed).unwrap().0.unwrap();
    /// assert_eq!(*old, 17);
    /// assert!(atomic_arc.compare_exchange(
    ///     ptr, Some(Arc::new(19)), Relaxed, Relaxed).is_err());
    /// ```
    pub fn compare_exchange<'r>(
        &self,
        current: Ptr<'r, T>,
        new: Option<Arc<T>>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<(Option<Arc<T>>, Ptr<'r, T>), (Option<Arc<T>>, Ptr<'r, T>)> {
        let desired = new
            .as_ref()
            .map_or_else(ptr::null_mut, |a| a.instance.as_ptr());
        match self
            .underlying
            .compare_exchange(current.ptr as *mut _, desired, success, failure)
        {
            Ok(prev) => {
                let prev_arc = if let Some(non_null_ptr) = NonNull::new(prev) {
                    Some(Arc::from(non_null_ptr))
                } else {
                    None
                };
                Ok((prev_arc, Ptr::from(prev)))
            }
            Err(actual) => Err((new, Ptr::from(actual))),
        }
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
