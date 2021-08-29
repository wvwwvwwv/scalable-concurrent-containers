use super::underlying::Underlying;
use super::{Arc, Barrier, Ptr};

use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{self, Relaxed};

/// [`AtomicArc`] owns the underlying instance, and allows users to perform atomic operations
/// on the pointer to it.
#[derive(Debug)]
pub struct AtomicArc<T: 'static> {
    instance_ptr: AtomicPtr<Underlying<T>>,
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
            instance_ptr: AtomicPtr::new(Box::into_raw(boxed)),
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
            instance_ptr: AtomicPtr::default(),
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
        self.instance_ptr.load(order).is_null()
    }

    /// Loads a pointer value from the [`AtomicArc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicArc, Barrier};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(11);
    /// let barrier = Barrier::new();
    /// let ptr = atomic_arc.load(Relaxed, &barrier);
    /// assert_eq!(*ptr.as_ref().unwrap(), 11);
    /// ```
    pub fn load<'r>(&self, order: Ordering, _barrier: &'r Barrier) -> Ptr<'r, T> {
        Ptr::from(self.instance_ptr.load(order))
    }

    /// Performs CAS on the [`AtomicArc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Barrier};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(14);
    /// let barrier = Barrier::new();
    /// let old = atomic_arc.swap(Some(Arc::new(15)), Relaxed);
    /// assert_eq!(*old.unwrap(), 14);
    /// let old = atomic_arc.swap(None, Relaxed);
    /// assert_eq!(*old.unwrap(), 15);
    /// let old = atomic_arc.swap(None, Relaxed);
    /// assert!(old.is_none());
    /// ```
    pub fn swap(&self, val: Option<Arc<T>>, order: Ordering) -> Option<Arc<T>> {
        let new = val.as_ref().map_or_else(ptr::null_mut, |a| a.raw_ptr());
        let prev = self.instance_ptr.swap(new, order);
        if let Some(ptr) = NonNull::new(prev) {
            Some(Arc::from(ptr))
        } else {
            None
        }
    }

    /// Performs CAS on the [`AtomicArc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Barrier};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(17);
    /// let barrier = Barrier::new();
    /// let ptr = atomic_arc.load(Relaxed, &barrier);
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
        let desired = new.as_ref().map_or_else(ptr::null_mut, |a| a.raw_ptr());
        match self.instance_ptr.compare_exchange(
            current.raw_ptr() as *mut _,
            desired,
            success,
            failure,
        ) {
            Ok(prev) => {
                let prev_arc = if let Some(ptr) = NonNull::new(prev) {
                    Some(Arc::from(ptr))
                } else {
                    None
                };
                Ok((prev_arc, Ptr::from(prev)))
            }
            Err(actual) => Err((new, Ptr::from(actual))),
        }
    }
}

impl<T: 'static> Clone for AtomicArc<T> {
    fn clone(&self) -> Self {
        let _barrier = Barrier::new();
        unsafe {
            let ptr = self.instance_ptr.load(Relaxed);
            if let Some(underlying_ref) = ptr.as_ref() {
                if underlying_ref.try_add_ref() {
                    return Self {
                        instance_ptr: AtomicPtr::new(ptr),
                    };
                }
            }
            Self::null()
        }
    }
}

impl<T: 'static> Drop for AtomicArc<T> {
    fn drop(&mut self) {
        if let Some(ptr) = NonNull::new(self.instance_ptr.swap(ptr::null_mut(), Relaxed)) {
            drop(Arc::from(ptr));
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

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

        let atomic_arc_cloned = atomic_arc.clone();

        let barrier = Barrier::new();
        assert_eq!(
            atomic_arc_cloned
                .load(Relaxed, &barrier)
                .as_ref()
                .unwrap()
                .1,
            10
        );

        drop(atomic_arc);
        assert!(!DESTROYED.load(Relaxed));

        drop(atomic_arc_cloned);
        drop(barrier);

        while !DESTROYED.load(Relaxed) {
            drop(Barrier::new());
        }
    }
}
