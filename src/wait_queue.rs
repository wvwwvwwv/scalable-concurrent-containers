use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use saa::Gate;
use saa::gate::Pager;

/// [`WaitQueue`] implements an unfair wait queue.
///
/// The sole purpose of the data structure is to avoid busy-waiting. [`WaitQueue`] should always
/// protected by [`sdd`].
#[derive(Debug, Default)]
pub(crate) struct WaitQueue {
    /// Gate to control access to resources.
    gate: Gate,
}

#[derive(Debug)]
pub(crate) struct AsyncWait {
    /// The associated `Pager`.
    pager: Pager<'static>,
}

/// [`DeriveAsyncWait`] derives a mutable reference to [`AsyncWait`].
pub(crate) trait DeriveAsyncWait {
    /// Returns a mutable reference to [`AsyncWait`] if available.
    fn derive(&mut self) -> Option<&mut AsyncWait>;
}

impl WaitQueue {
    /// Waits for the condition to be met or signaled.
    #[inline]
    pub(crate) fn wait_sync<T, F: FnOnce() -> Result<T, ()>>(&self, f: F) -> Result<T, ()> {
        let mut pager = Pager::default();
        let mut pinned_pager = Pin::new(&mut pager);
        self.gate.register_sync(&mut pinned_pager);

        let result = f();
        if result.is_ok() {
            return result;
        }
        let _: Result<_, _> = pinned_pager.poll_sync();
        result
    }

    /// Pushes an [`AsyncWait`] into the [`WaitQueue`].
    ///
    /// If it happens to acquire the desired resource, it returns an `Ok(T)` after waking up all
    /// the entries in the [`WaitQueue`].
    #[inline]
    pub(crate) fn push_async_entry<'w, T, F: FnOnce() -> Result<T, ()>>(
        &'w self,
        async_wait: &'w mut AsyncWait,
        f: F,
    ) -> Result<T, ()> {
        let mut pinned_pager = Pin::new(unsafe {
            std::mem::transmute::<&mut Pager<'static>, &mut Pager<'w>>(&mut async_wait.pager)
        });
        self.gate.register_async(&mut pinned_pager);
        if let Ok(result) = f() {
            self.signal();
            async_wait.pager = Pager::default();
            return Ok(result);
        }
        Err(())
    }

    /// Signals the waiting threads to wake up.
    #[inline]
    pub(crate) fn signal(&self) {
        let _: Result<_, _> = self.gate.permit();
    }
}

impl Default for AsyncWait {
    #[inline]
    fn default() -> Self {
        Self {
            pager: unsafe { std::mem::transmute::<Pager<'_>, Pager<'static>>(Pager::default()) },
        }
    }
}

impl DeriveAsyncWait for Pin<&mut AsyncWait> {
    #[inline]
    fn derive(&mut self) -> Option<&mut AsyncWait> {
        Some(self)
    }
}

impl Future for AsyncWait {
    type Output = ();

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let pinned_pager = Pin::new(&mut self.pager);
        if pinned_pager.poll(cx).is_ready() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl DeriveAsyncWait for () {
    #[inline]
    fn derive(&mut self) -> Option<&mut AsyncWait> {
        None
    }
}
