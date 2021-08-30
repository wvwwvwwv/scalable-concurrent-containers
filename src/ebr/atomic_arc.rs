use super::underlying::Underlying;
use super::{Arc, Barrier, Ptr, Tag};

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
    /// use scc::ebr::{AtomicArc, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::null();
    /// atomic_arc.set_tag(Tag::Both, Relaxed);
    /// assert!(atomic_arc.is_null(Relaxed));
    /// ```
    pub fn is_null(&self, order: Ordering) -> bool {
        Tag::unset_tag(self.instance_ptr.load(order)).is_null()
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

    /// Stores the given value into the [`AtomicArc`], and returns the original value.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Barrier, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(14);
    /// let barrier = Barrier::new();
    /// let old = atomic_arc.swap((Some(Arc::new(15)), Tag::Second), Relaxed);
    /// assert_eq!(*old.unwrap(), 14);
    /// let old = atomic_arc.swap((None, Tag::None), Relaxed);
    /// assert_eq!(*old.unwrap(), 15);
    /// let old = atomic_arc.swap((None, Tag::None), Relaxed);
    /// assert!(old.is_none());
    /// ```
    pub fn swap(&self, val: (Option<Arc<T>>, Tag), order: Ordering) -> Option<Arc<T>> {
        let new = Tag::update_tag(
            val.0.as_ref().map_or_else(ptr::null_mut, |a| a.raw_ptr()),
            val.1,
        ) as *mut Underlying<T>;
        let prev = Tag::unset_tag(self.instance_ptr.swap(new, order)) as *mut Underlying<T>;
        if let Some(ptr) = NonNull::new(prev) {
            Some(Arc::from(ptr))
        } else {
            None
        }
    }

    /// Returns its [`Tag`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicArc, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::null();
    /// assert_eq!(atomic_arc.tag(Relaxed), Tag::None);
    /// ```
    pub fn tag(&self, order: Ordering) -> Tag {
        Tag::into_tag(self.instance_ptr.load(order))
    }

    /// Sets a [`Tag`], overwriting any existing tag.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicArc, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::null();
    /// atomic_arc.set_tag(Tag::Both, Relaxed);
    /// assert_eq!(atomic_arc.tag(Relaxed), Tag::Both);
    /// ```
    pub fn set_tag(&self, tag: Tag, order: Ordering) {
        let mut current = self.instance_ptr.load(Relaxed);
        loop {
            let desired = Tag::update_tag(current, tag) as *mut Underlying<T>;
            if let Err(actual) = self
                .instance_ptr
                .compare_exchange(current, desired, order, Relaxed)
            {
                current = actual;
            } else {
                break;
            }
        }
    }

    /// Performs CAS on the [`AtomicArc`].
    ///
    /// It returns `Ok` with the previously held [`Arc`] and the updated [`Ptr`] upon a
    /// successful operation, or it returns `Err` with the supplied [`Arc`] and the current
    /// [`Ptr`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Barrier, Tag};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(17);
    /// let barrier = Barrier::new();
    ///
    /// let mut ptr = atomic_arc.load(Relaxed, &barrier);
    /// atomic_arc.set_tag(Tag::Both, Relaxed);
    /// assert!(atomic_arc.compare_exchange(
    ///     ptr, (Some(Arc::new(18)), Tag::First), Relaxed, Relaxed).is_err());
    ///
    /// ptr.set_tag(Tag::Both);
    /// let old = atomic_arc.compare_exchange(
    ///     ptr, (Some(Arc::new(18)), Tag::First), Relaxed, Relaxed).unwrap().0.unwrap();
    /// assert_eq!(*old, 17);
    ///
    /// assert!(atomic_arc.compare_exchange(
    ///     ptr, (Some(Arc::new(19)), Tag::None), Relaxed, Relaxed).is_err());
    /// ```
    pub fn compare_exchange<'r>(
        &self,
        current: Ptr<'r, T>,
        new: (Option<Arc<T>>, Tag),
        success: Ordering,
        failure: Ordering,
    ) -> Result<(Option<Arc<T>>, Ptr<'r, T>), (Option<Arc<T>>, Ptr<'r, T>)> {
        let desired = Tag::update_tag(
            new.0.as_ref().map_or_else(ptr::null_mut, |a| a.raw_ptr()),
            new.1,
        ) as *mut Underlying<T>;
        match self.instance_ptr.compare_exchange(
            current.raw_ptr() as *mut _,
            desired,
            success,
            failure,
        ) {
            Ok(prev) => {
                let prev_arc =
                    if let Some(ptr) = NonNull::new(Tag::unset_tag(prev) as *mut Underlying<T>) {
                        Some(Arc::from(ptr))
                    } else {
                        None
                    };
                Ok((prev_arc, Ptr::from(desired)))
            }
            Err(actual) => Err((new.0, Ptr::from(actual))),
        }
    }

    /// Clones itself.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc, Barrier};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(59);
    /// let barrier = Barrier::new();
    /// let atomic_arc_cloned = atomic_arc.clone(Relaxed, &barrier);
    /// let ptr = atomic_arc_cloned.load(Relaxed, &barrier);
    /// assert_eq!(*ptr.as_ref().unwrap(), 59);
    /// ```
    pub fn clone<'r>(&self, order: Ordering, _barrier: &'r Barrier) -> AtomicArc<T> {
        unsafe {
            let ptr = self.instance_ptr.load(order);
            if let Some(underlying_ref) = (Tag::unset_tag(ptr)).as_ref() {
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
        if let Some(ptr) = NonNull::new(Tag::unset_tag(
            self.instance_ptr.swap(ptr::null_mut(), Relaxed),
        ) as *mut Underlying<T>)
        {
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

        let barrier = Barrier::new();
        let atomic_arc_cloned = atomic_arc.clone(Relaxed, &barrier);
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

        atomic_arc_cloned.set_tag(Tag::Second, Relaxed);

        drop(atomic_arc_cloned);
        drop(barrier);

        while !DESTROYED.load(Relaxed) {
            drop(Barrier::new());
        }
    }
}
