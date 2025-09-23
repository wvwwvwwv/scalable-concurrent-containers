use std::cell::UnsafeCell;
use std::mem::ManuallyDrop;
use std::pin::{Pin, pin};
use std::ptr;
use std::sync::atomic::Ordering;
use std::sync::atomic::Ordering::Acquire;

use saa::lock::Mode as LockMode;
use saa::{Gate, Lock, Pager};
use sdd::{AtomicShared, Guard};

use crate::exit_guard::ExitGuard;

/// [`SendableGuard`] is used when an asynchronous task needs to be suspended without invalidating
/// any references.
///
/// The validity of those references must be checked and verified by the user.
#[derive(Debug, Default)]
pub(crate) struct SendableGuard {
    /// [`Guard`] that can be dropped without invalidating any references.
    guard: UnsafeCell<Option<Guard>>,
    /// [`Pager`] that enables asynchronous lock acquisition.
    ///
    /// [`Pager`] needs to be dropped before the [`Guard`], therefore manual lifetime management is
    /// required; [`SendableGuard`] manually drops the contained [`Pager`].
    pager: UnsafeCell<ManuallyDrop<Pager<'static, Lock>>>,
}

/// [`WaitQueue`] implements an unfair wait queue.
///
/// The sole purpose of this data structure is to avoid busy-waiting. [`WaitQueue`] should always be
/// protected by [`sdd`].
#[derive(Debug, Default)]
pub(crate) struct WaitQueue {
    /// [`Gate`] to control access to resources.
    gate: Gate,
}

#[derive(Debug)]
pub(crate) struct AsyncWait {
    /// The associated `Pager`.
    pager: Pager<'static, Gate>,
}

/// [`DeriveAsyncWait`] derives a mutable reference to [`AsyncWait`].
pub(crate) trait DeriveAsyncWait {
    /// Returns a mutable reference to [`AsyncWait`] if available.
    fn derive(&mut self) -> Option<&mut AsyncWait>;
}

impl SendableGuard {
    /// Returns `true` if the [`SendableGuard`] contains a valid [`Guard`].
    #[inline]
    pub(crate) fn has_guard(&self) -> bool {
        unsafe { (*self.guard.get()).is_some() }
    }

    /// Acquires a write or read lock after waiting for the lock to be available.
    pub(crate) async fn wait_acquire<'l>(&self, lock: &'l Lock, writer: bool) -> bool {
        if lock.is_poisoned(Acquire) {
            return false;
        }
        let mut unwind_guard = ExitGuard::new(true, |unwind| {
            if unwind {
                // During unwinding the stack, a guard is needed.
                //
                // At this point, the pager is not registered and no guard is present in the stack,
                // allowing anyone to drop the bucket array containing the lock. By instantiating a
                // new guard, use-after-free can be prevented thanks to `SeqCst` operations when
                // instantiating a new guard.
                // - Case 1: drop-bucket-array -> this guard. When dropping the bucket array, the
                //  lock must have been poisoned, so the pager already knows it.
                // - Case 2: this guard -> drop-bucket-array. The dropped bucket array remains in
                //  memory until the guard is dropped.
                self.guard();
            }
        });

        let mut pinned_pager = unsafe {
            let pager_ref = std::mem::transmute::<&mut Pager<'static, Lock>, &mut Pager<'l, Lock>>(
                &mut **self.pager.get(),
            );
            Pin::new_unchecked(pager_ref)
        };
        let lock_mode = if writer {
            LockMode::Exclusive
        } else {
            LockMode::Shared
        };
        lock.register_pager(&mut pinned_pager, lock_mode, false);

        // The guard needs to be released before polling.
        self.reset();

        // The pager is properly registered, therefore it will get a result unless cancelled.
        let result = pinned_pager.poll_async().await.ok().unwrap_or(false);
        *unwind_guard = false;

        result
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

    /// Resets the [`SendableGuard`] to its initial state.
    #[inline]
    pub(crate) fn reset(&self) {
        unsafe {
            *self.guard.get() = None;
        }
    }

    /// Loads the content of the [`AtomicShared`] without exposing the [`Guard`].
    #[inline]
    pub(crate) fn load<T>(&self, atomic_ptr: &AtomicShared<T>, mo: Ordering) -> Option<&T> {
        atomic_ptr.load(mo, self.guard()).as_ref()
    }

    /// Checks if the reference is valid.
    #[inline]
    pub(crate) fn check_ref<T>(&self, atomic_ptr: &AtomicShared<T>, r: &T, mo: Ordering) -> bool {
        atomic_ptr
            .load(mo, self.guard())
            .as_ref()
            .is_some_and(|s| ptr::eq(s, r))
    }
}

impl Drop for SendableGuard {
    fn drop(&mut self) {
        unsafe {
            // The pager needs to be dropped first while the guard is still valid when unwinding.
            ManuallyDrop::drop(&mut *self.pager.get());
        }
    }
}

/// SAFETY: this is the sole purpose of `SendableGuard`; Send-safety should be ensured by the
/// user, e.g., the `SendableGuard` should always be reset before the task is suspended.
unsafe impl Send for SendableGuard {}
unsafe impl Sync for SendableGuard {}

impl WaitQueue {
    /// Waits for the condition to be met or signaled.
    #[inline]
    pub(crate) fn wait_sync<T, F: FnOnce() -> Result<T, ()>>(&self, f: F) -> Result<T, ()> {
        let mut pinned_pager = pin!(Pager::default());
        self.gate.register_pager(&mut pinned_pager, true);

        let result = f();
        if result.is_ok() {
            self.signal();
        }
        let _: Result<_, _> = pinned_pager.poll_sync();
        result
    }

    /// Pushes an [`AsyncWait`] into the [`WaitQueue`].
    ///
    /// Returns `Ok(T)` if the condition is met and signaled.
    #[inline]
    pub(crate) fn push_async_entry<'w, T, F: FnOnce() -> Result<T, ()>>(
        &'w self,
        async_wait: &'w mut AsyncWait,
        f: F,
    ) -> Result<T, ()> {
        let mut pinned_pager = unsafe {
            let pager_ref = std::mem::transmute::<&mut Pager<'static, Gate>, &mut Pager<'w, Gate>>(
                &mut async_wait.pager,
            );
            Pin::new_unchecked(pager_ref)
        };
        self.gate.register_pager(&mut pinned_pager, false);
        if let Ok(result) = f() {
            self.signal();
            if pinned_pager.try_poll().is_ok() {
                async_wait.pager = Pager::default();
                return Ok(result);
            }
        }
        Err(())
    }

    /// Signals waiting threads to wake up.
    #[inline]
    pub(crate) fn signal(&self) {
        let _: Result<_, _> = self.gate.permit();
    }
}

impl AsyncWait {
    /// Awaits the [`Gate`] to be open.
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
                std::mem::transmute::<Pager<'_, Gate>, Pager<'static, Gate>>(Pager::default())
            },
        }
    }
}

impl DeriveAsyncWait for Pin<&mut AsyncWait> {
    #[inline]
    fn derive(&mut self) -> Option<&mut AsyncWait> {
        let this = unsafe { ptr::read(self) };
        Some(unsafe { this.get_unchecked_mut() })
    }
}

impl DeriveAsyncWait for () {
    #[inline]
    fn derive(&mut self) -> Option<&mut AsyncWait> {
        None
    }
}
