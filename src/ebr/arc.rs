use super::link::Link;

use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

/// [`Arc`] is a reference-counted handle to an instance.
///
/// TODO: it does not drop the instance when the last reference is dropped, instead it passes
/// it to the EBR garbage collector.
pub struct Arc<T> {
    instance: NonNull<Underlying<T>>,
}

impl<T> Arc<T> {
    /// Creates a new instance of [`Arc`].
    pub fn new(t: T) -> Arc<T> {
        let boxed = Box::new(Underlying::new(t));
        Arc {
            instance: unsafe { NonNull::new_unchecked(Box::into_raw(boxed)) },
        }
    }

    /// Returns a mutable reference to the underlying instance if the instance is exclusively
    /// owned.
    pub fn get_mut(&mut self) -> Option<&mut T> {
        if unsafe { self.instance.as_ref().ref_cnt() } == 0 {
            Some(unsafe { &mut self.instance.as_mut().t })
        } else {
            None
        }
    }
}

impl<T> Clone for Arc<T> {
    fn clone(&self) -> Self {
        unsafe { self.instance.as_ref().add_ref() };
        Self {
            instance: self.instance.clone(),
        }
    }
}

impl<T> Deref for Arc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &self.instance.as_ref().t }
    }
}

impl<T> Drop for Arc<T> {
    fn drop(&mut self) {
        // TODO: push it into a reclaimer.
        if unsafe { self.instance.as_ref().drop_ref() } {
            unsafe { self.instance.as_mut().dealloc() }
        }
    }
}

pub(super) union LinkOrRefCnt {
    next: *const dyn Link,
    refcnt: ManuallyDrop<(AtomicUsize, usize)>,
}

impl Default for LinkOrRefCnt {
    fn default() -> Self {
        LinkOrRefCnt {
            refcnt: ManuallyDrop::new((AtomicUsize::new(0), 0)),
        }
    }
}

/// [`Underlying`] stores the instance and a link to the next [`Underlying`].
pub(super) struct Underlying<T> {
    next_or_refcnt: LinkOrRefCnt,
    t: T,
}

impl<T> Underlying<T> {
    // Creates a new underlying instance.
    pub(super) fn new(t: T) -> Underlying<T> {
        Underlying {
            next_or_refcnt: LinkOrRefCnt::default(),
            t,
        }
    }

    /// Returns the count of strong references.
    fn ref_cnt(&self) -> usize {
        unsafe { self.next_or_refcnt.refcnt.0.load(Relaxed) }
    }

    /// Adds a strong reference to the underlying instance.
    pub(super) fn add_ref(&self) {
        unsafe { self.next_or_refcnt.refcnt.0.fetch_add(1, Relaxed) };
    }

    /// Drops a strong reference to the underlying instance.
    pub(super) fn drop_ref(&self) -> bool {
        // It does not have to be a load-acquire as everything's synchronized via the global
        // epoch. In addition to that, it also does not have to be read-modify-write as a
        // reference count increment is guaranteed to be observed by the one that decrements
        // the last reference.
        let mut current = unsafe { self.next_or_refcnt.refcnt.0.load(Relaxed) };
        while current > 0 {
            if let Err(actual) = unsafe {
                self.next_or_refcnt.refcnt.0.compare_exchange(
                    current,
                    current - 1,
                    Relaxed,
                    Relaxed,
                )
            } {
                current = actual;
            } else {
                break;
            }
        }
        current == 0
    }
}

impl<T> Link for Underlying<T> {
    fn next(&self) -> *const dyn Link {
        unsafe { self.next_or_refcnt.next }
    }

    fn set(&mut self, next_ptr: *const dyn Link) {
        self.next_or_refcnt.next = next_ptr;
    }

    fn dealloc(&mut self) {
        unsafe { Box::from_raw(self as *mut Underlying<T>) };
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn arc() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);
        struct A(AtomicUsize, usize, &'static AtomicBool);
        impl Drop for A {
            fn drop(&mut self) {
                self.2.swap(true, Relaxed);
            }
        }
        let mut arc = Arc::new(A(AtomicUsize::new(10), 10, &DESTROYED));
        if let Some(mut_ref) = arc.get_mut() {
            mut_ref.1 += 1;
        }
        arc.0.fetch_add(1, Relaxed);
        assert_eq!(arc.deref().0.load(Relaxed), 11);
        assert_eq!(arc.deref().1, 11);

        let mut arc_cloned = arc.clone();
        assert!(arc_cloned.get_mut().is_none());
        arc_cloned.0.fetch_add(1, Relaxed);
        assert_eq!(arc_cloned.deref().0.load(Relaxed), 12);
        assert_eq!(arc_cloned.deref().1, 11);

        let mut arc_cloned_again = arc_cloned.clone();
        assert!(arc_cloned_again.get_mut().is_none());
        assert_eq!(arc_cloned_again.deref().0.load(Relaxed), 12);
        assert_eq!(arc_cloned_again.deref().1, 11);

        drop(arc);
        assert!(!DESTROYED.load(Relaxed));
        assert!(arc_cloned_again.get_mut().is_none());

        drop(arc_cloned);
        assert!(!DESTROYED.load(Relaxed));
        assert!(arc_cloned_again.get_mut().is_some());

        drop(arc_cloned_again);
        assert!(DESTROYED.load(Relaxed));
    }
}
