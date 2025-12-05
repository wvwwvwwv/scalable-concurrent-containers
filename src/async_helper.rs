use std::cell::UnsafeCell;
use std::pin::{Pin, pin};
use std::ptr;
use std::sync::atomic::Ordering;

use saa::lock::Mode;
use saa::{Lock, Pager};
use sdd::{AtomicShared, Guard};

/// [`AsyncGuard`] is used when an asynchronous task needs to be suspended without invalidating any
/// references.
///
/// The validity of those references must be checked and verified by the user.
#[derive(Debug, Default)]
pub(crate) struct AsyncGuard {
    /// [`Guard`] that can be dropped without invalidating any references.
    guard: UnsafeCell<Option<Guard>>,
}

#[derive(Debug)]
pub(crate) struct AsyncWait {
    /// Allows the user to await the lock anywhere in the code.
    pager: Pager<'static, Lock>,
}

/// [`TryWait`] allows [`AsyncWait`] to be used in synchronous methods.
pub(crate) trait TryWait {
    /// Registers the [`Pager`] in the [`Lock`], or synchronously waits for the [`Lock`] to be
    /// available.
    fn try_wait(&mut self, lock: &Lock);
}

impl AsyncGuard {
    /// Returns `true` if the [`AsyncGuard`] contains a valid [`Guard`].
    #[inline]
    pub(crate) fn has_guard(&self) -> bool {
        unsafe { (*self.guard.get()).is_some() }
    }

    /// Returns or creates a new [`Guard`].
    ///
    /// # Safety
    ///
    /// The caller must ensure that any references derived from the returned [`Guard`] do not
    /// outlive the underlying instance.
    #[inline]
    pub(crate) fn guard(&self) -> &Guard {
        unsafe { (*self.guard.get()).get_or_insert_with(Guard::new) }
    }

    /// Resets the [`AsyncGuard`] to its initial state.
    #[inline]
    pub(crate) fn reset(&self) {
        unsafe {
            *self.guard.get() = None;
        }
    }

    /// Loads the content of the [`AtomicShared`] without exposing the [`Guard`] or checking tag
    /// bits.
    #[inline]
    pub(crate) fn load_unchecked<T>(
        &self,
        atomic_ptr: &AtomicShared<T>,
        mo: Ordering,
    ) -> Option<&T> {
        unsafe { atomic_ptr.load(mo, self.guard()).as_ref_unchecked() }
    }

    /// Checks if the reference is valid.
    #[inline]
    pub(crate) fn check_ref<T>(&self, atomic_ptr: &AtomicShared<T>, r: &T, mo: Ordering) -> bool {
        self.load_unchecked(atomic_ptr, mo)
            .is_some_and(|s| ptr::eq(s, r))
    }
}

// SAFETY: this is the sole purpose of `AsyncGuard`; Send-safety should be ensured by the user,
// e.g., the `AsyncGuard` should always be reset before the task is suspended.
unsafe impl Send for AsyncGuard {}
unsafe impl Sync for AsyncGuard {}

impl AsyncWait {
    /// Awaits the [`Lock`] to be available.
    #[inline]
    pub async fn wait(self: &mut Pin<&mut Self>) {
        let this = unsafe { ptr::read(self) };
        let mut pinned_pager = unsafe { Pin::new_unchecked(&mut this.get_unchecked_mut().pager) };
        let _result = pinned_pager.poll_async().await;
    }
}

impl Default for AsyncWait {
    #[inline]
    fn default() -> Self {
        Self {
            pager: unsafe {
                std::mem::transmute::<Pager<'_, Lock>, Pager<'static, Lock>>(Pager::default())
            },
        }
    }
}

impl TryWait for Pin<&mut AsyncWait> {
    #[inline]
    fn try_wait(&mut self, lock: &Lock) {
        let this = unsafe { ptr::read(self) };
        let mut pinned_pager = unsafe {
            let pager_ref = std::mem::transmute::<&mut Pager<'static, Lock>, &mut Pager<Lock>>(
                &mut this.get_unchecked_mut().pager,
            );
            Pin::new_unchecked(pager_ref)
        };
        lock.register_pager(&mut pinned_pager, Mode::WaitExclusive, false);
    }
}

impl TryWait for () {
    #[inline]
    fn try_wait(&mut self, lock: &Lock) {
        let mut pinned_pager = pin!(Pager::default());
        lock.register_pager(&mut pinned_pager, Mode::WaitExclusive, true);
        let _: Result<_, _> = pinned_pager.poll_sync();
    }
}
