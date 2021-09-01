use super::underlying::Underlying;
use super::{Arc, Barrier, Ptr, Tag};

use std::mem::forget;
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
    #[inline]
    pub fn new(t: T) -> AtomicArc<T> {
        let boxed = Box::new(Underlying::new(t));
        AtomicArc {
            instance_ptr: AtomicPtr::new(Box::into_raw(boxed)),
        }
    }

    /// Creates a new [`AtomicArc`] from an [`Arc`] of `T`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{Arc, AtomicArc};
    ///
    /// let arc: Arc<usize> = Arc::new(10);
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::from(arc);
    /// ```
    #[inline]
    pub fn from(arc: Arc<T>) -> AtomicArc<T> {
        let ptr = arc.raw_ptr();
        forget(arc);
        AtomicArc {
            instance_ptr: AtomicPtr::new(ptr),
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
    #[inline]
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
    #[inline]
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
    #[inline]
    pub fn load<'b>(&self, order: Ordering, _barrier: &'b Barrier) -> Ptr<'b, T> {
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
    /// let old = atomic_arc.swap((None, Tag::First), Relaxed);
    /// assert_eq!(*old.unwrap(), 15);
    /// let old = atomic_arc.swap((None, Tag::None), Relaxed);
    /// assert!(old.is_none());
    /// ```
    #[inline]
    pub fn swap(&self, new: (Option<Arc<T>>, Tag), order: Ordering) -> Option<Arc<T>> {
        let desired = Tag::update_tag(
            new.0.as_ref().map_or_else(ptr::null_mut, |a| a.raw_ptr()),
            new.1,
        ) as *mut Underlying<T>;
        let prev = Tag::unset_tag(self.instance_ptr.swap(desired, order)) as *mut Underlying<T>;
        forget(new);
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
    #[inline]
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
    #[inline]
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
    #[inline]
    pub fn compare_exchange<'b>(
        &self,
        current: Ptr<'b, T>,
        new: (Option<Arc<T>>, Tag),
        success: Ordering,
        failure: Ordering,
    ) -> Result<(Option<Arc<T>>, Ptr<'b, T>), (Option<Arc<T>>, Ptr<'b, T>)> {
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
                forget(new);
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
    #[inline]
    pub fn clone<'b>(&self, order: Ordering, _barrier: &'b Barrier) -> AtomicArc<T> {
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

    /// Tries to create an [`Arc`] out of `self`.
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicArc, Barrier};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(47);
    /// let barrier = Barrier::new();
    /// let arc: Arc<usize> = atomic_arc.get_arc(Relaxed, &barrier).unwrap();
    /// assert_eq!(*arc, 47);
    /// ```
    #[inline]
    pub fn get_arc(&self, order: Ordering, _barrier: &Barrier) -> Option<Arc<T>> {
        let ptr = self.instance_ptr.load(order);
        if let Some(underlying_ptr) = NonNull::new(Tag::unset_tag(ptr) as *mut Underlying<T>) {
            if unsafe { underlying_ptr.as_ref() }.try_add_ref() {
                return Some(Arc::from(underlying_ptr));
            }
        }
        None
    }

    /// Tries to convert `self` into an [`Arc`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scc::ebr::{AtomicArc, Barrier};
    /// use std::sync::atomic::Ordering::Relaxed;
    ///
    /// let atomic_arc: AtomicArc<usize> = AtomicArc::new(55);
    /// let barrier = Barrier::new();
    /// let arc: Arc<usize> = atomic_arc.try_into_arc(Relaxed, &barrier).unwrap();
    /// assert_eq!(*arc, 55);
    /// ```
    #[inline]
    pub fn try_into_arc(self, order: Ordering, _barrier: &Barrier) -> Option<Arc<T>> {
        let ptr = self.instance_ptr.swap(ptr::null_mut(), order);
        if let Some(underlying_ptr) = NonNull::new(Tag::unset_tag(ptr) as *mut Underlying<T>) {
            Some(Arc::from(underlying_ptr));
        }
        None
    }
}

impl<T: 'static> Clone for AtomicArc<T> {
    #[inline]
    fn clone<'b>(&self) -> AtomicArc<T> {
        self.clone(Relaxed, &Barrier::new())
    }
}

impl<T: 'static> Drop for AtomicArc<T> {
    #[inline]
    fn drop(&mut self) {
        if let Some(ptr) = NonNull::new(Tag::unset_tag(
            self.instance_ptr.swap(ptr::null_mut(), Relaxed),
        ) as *mut Underlying<T>)
        {
            drop(Arc::from(ptr));
        }
    }
}

unsafe impl<T: 'static> Send for AtomicArc<T> {}

#[cfg(test)]
mod test {
    use super::*;

    use std::convert::TryInto;
    use std::sync::atomic::Ordering::{Acquire, Release};
    use std::sync::atomic::{AtomicBool, AtomicU8};
    use std::thread;

    struct A(AtomicU8, usize, &'static AtomicBool);
    impl Drop for A {
        fn drop(&mut self) {
            self.2.swap(true, Relaxed);
        }
    }

    #[test]
    fn atomic_arc() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);

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

    #[test]
    fn atomic_arc_creation() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);

        let atomic_arc = AtomicArc::new(A(AtomicU8::new(11), 11, &DESTROYED));
        assert!(!DESTROYED.load(Relaxed));

        let barrier = Barrier::new();

        let arc = atomic_arc.get_arc(Relaxed, &barrier);

        drop(atomic_arc);
        assert!(!DESTROYED.load(Relaxed));

        if let Some(arc) = arc {
            assert_eq!(arc.1, 11);
            assert!(!DESTROYED.load(Relaxed));
        }
        drop(barrier);

        while !DESTROYED.load(Relaxed) {
            drop(Barrier::new());
        }
    }

    #[test]
    fn atomic_arc_conversion() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);

        let atomic_arc = AtomicArc::new(A(AtomicU8::new(11), 11, &DESTROYED));
        assert!(!DESTROYED.load(Relaxed));

        let barrier = Barrier::new();

        let arc = atomic_arc.try_into_arc(Relaxed, &barrier);
        assert!(!DESTROYED.load(Relaxed));

        if let Some(arc) = arc {
            assert_eq!(arc.1, 11);
            assert!(!DESTROYED.load(Relaxed));
        }
        drop(barrier);

        while !DESTROYED.load(Relaxed) {
            drop(Barrier::new());
        }
    }

    #[test]
    fn atomic_arc_parallel() {
        let atomic_arc: Arc<AtomicArc<String>> =
            Arc::new(AtomicArc::new(String::from("How are you?")));
        let mut thread_handles = Vec::new();
        for _ in 0..4 {
            let atomic_arc = atomic_arc.clone();
            thread_handles.push(thread::spawn(move || {
                for _ in 0..256 {
                    let barrier = Barrier::new();
                    let mut ptr = atomic_arc.load(Acquire, &barrier);
                    assert!(ptr.tag() == Tag::None || ptr.tag() == Tag::Second);
                    if let Some(str_ref) = ptr.as_ref() {
                        assert!(str_ref == "How are you?" || str_ref == "How can I help you?");
                    }
                    let converted: Result<Arc<String>, ()> = ptr.clone().try_into();
                    if let Ok(arc) = converted {
                        assert!(*arc == "How are you?" || *arc == "How can I help you?");
                    }
                    while let Err((passed, current)) = atomic_arc.compare_exchange(
                        ptr,
                        (
                            Some(Arc::new(String::from("How can I help you?"))),
                            Tag::Second,
                        ),
                        Release,
                        Relaxed,
                    ) {
                        if let Some(arc) = passed {
                            assert!(*arc == "How can I help you?");
                        }
                        ptr = current;
                        if let Some(str_ref) = ptr.as_ref() {
                            assert!(str_ref == "How are you?" || str_ref == "How can I help you?");
                        }
                        assert!(ptr.tag() == Tag::None || ptr.tag() == Tag::Second);
                    }
                    drop(barrier);

                    atomic_arc.set_tag(Tag::None, Relaxed);

                    let barrier = Barrier::new();
                    ptr = atomic_arc.load(Acquire, &barrier);
                    assert!(ptr.tag() == Tag::None || ptr.tag() == Tag::Second);
                    if let Some(str_ref) = ptr.as_ref() {
                        assert!(str_ref == "How are you?" || str_ref == "How can I help you?");
                    }
                    drop(barrier);

                    let old = atomic_arc.swap(
                        (Some(Arc::new(String::from("How are you?"))), Tag::Second),
                        Release,
                    );
                    if let Some(arc) = old {
                        assert!(*arc == "How are you?" || *arc == "How can I help you?");
                    }
                }
            }));
        }
        thread_handles.into_iter().for_each(|t| t.join().unwrap());
    }
}
