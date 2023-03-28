use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Relaxed};
use std::sync::{Condvar, Mutex};
use std::task::{Context, Poll, Waker};

/// `ASYNC` is a flag indicating that the referenced instance corresponds to an asynchronous
/// operation.
const ASYNC: usize = 1_usize;

/// [`WaitQueue`] implements an unfair wait queue.
///
/// The sole purpose of the data structure is to avoid busy-waiting.
#[derive(Debug, Default)]
pub(crate) struct WaitQueue {
    /// Stores the pointer value of the actual wait queue entry and a flag indicating that the
    /// entry is asynchronous.
    wait_queue: AtomicUsize,
}

impl WaitQueue {
    /// Waits for the condition to be met or signalled.
    #[inline]
    pub(crate) fn wait_sync<T, F: FnOnce() -> Result<T, ()>>(&self, f: F) -> Result<T, ()> {
        let mut current = self.wait_queue.load(Relaxed);
        let mut entry = SyncWait::new(current);
        let mut entry_mut = Pin::new(&mut entry);

        while let Err(actual) = self.wait_queue.compare_exchange(
            current,
            entry_mut.as_mut().get_mut() as *mut SyncWait as usize,
            AcqRel,
            Relaxed,
        ) {
            current = actual;
            entry_mut.next = current;
        }

        // Execute the closure.
        let result = f();
        if result.is_ok() {
            self.signal();
        }

        entry_mut.wait();
        result
    }

    /// Pushes an [`AsyncWait`] into the [`WaitQueue`].
    ///
    /// If it happens to acquire the desired resource, it returns an `Ok(T)` after waking up all
    /// the entries in the [`WaitQueue`].
    #[inline]
    pub(crate) fn push_async_entry<T, F: FnOnce() -> Result<T, ()>>(
        &self,
        async_wait: &mut AsyncWait,
        f: F,
    ) -> Result<T, ()> {
        debug_assert!(async_wait.mutex.is_none());

        let mut current = self.wait_queue.load(Relaxed);
        async_wait.next = current;
        async_wait.mutex.replace(Mutex::new((false, None)));

        while let Err(actual) = self.wait_queue.compare_exchange(
            current,
            (async_wait as *mut AsyncWait as usize) | ASYNC,
            AcqRel,
            Relaxed,
        ) {
            current = actual;
            async_wait.next = current;
        }

        // Execute the closure.
        if let Ok(result) = f() {
            self.signal();
            if async_wait.try_wait() {
                async_wait.mutex.take();
                return Ok(result);
            }
            // Another task is waking up `async_wait`: dispose of `result` which is holding the
            // desired resource.
        }

        // The caller has to await.
        Err(())
    }

    /// Signals the threads in the wait queue.
    #[inline]
    pub(crate) fn signal(&self) {
        let mut current = self.wait_queue.swap(0, AcqRel);
        while (current & (!ASYNC)) != 0 {
            current = if (current & ASYNC) == 0 {
                // Synchronous.
                let entry_ref = unsafe { &*(current as *mut SyncWait) };
                let next = entry_ref.next;
                entry_ref.signal();
                next
            } else {
                // Asynchronous.
                let entry_ref = unsafe { &*((current & (!ASYNC)) as *mut AsyncWait) };
                let next = entry_ref.next;
                entry_ref.signal();
                next
            };
        }
    }
}

/// [`DeriveAsyncWait`] derives a mutable reference to [`AsyncWait`].
pub(crate) trait DeriveAsyncWait {
    /// Returns a mutable reference to [`AsyncWait`] if available.
    fn derive(&mut self) -> Option<&mut AsyncWait>;
}

impl DeriveAsyncWait for Pin<&mut AsyncWait> {
    #[inline]
    fn derive(&mut self) -> Option<&mut AsyncWait> {
        unsafe { Some(self.as_mut().get_unchecked_mut()) }
    }
}

impl DeriveAsyncWait for () {
    #[inline]
    fn derive(&mut self) -> Option<&mut AsyncWait> {
        None
    }
}

/// [`AsyncWait`] is inserted into [`WaitQueue`] for the caller to await until woken up.
///
/// [`AsyncWait`] has to be pinned outside in order to use it correctly. The type is `Unpin`,
/// therefore it can be moved, however the [`DeriveAsyncWait`] trait forces [`AsyncWait`] to be
/// pinned.
#[derive(Debug, Default)]
pub(crate) struct AsyncWait {
    next: usize,
    mutex: Option<Mutex<(bool, Option<Waker>)>>,
}

impl AsyncWait {
    /// Sends a signal.
    fn signal(&self) {
        if let Some(mutex) = self.mutex.as_ref() {
            if let Ok(mut locked) = mutex.lock() {
                locked.0 = true;
                if let Some(waker) = locked.1.take() {
                    waker.wake();
                }
            }
        } else {
            unreachable!();
        }
    }

    /// Tries to receive a signal.
    fn try_wait(&self) -> bool {
        if let Some(mutex) = self.mutex.as_ref() {
            if let Ok(locked) = mutex.lock() {
                if locked.0 {
                    return true;
                }
            }
        }
        false
    }
}

impl Future for AsyncWait {
    type Output = ();

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(mutex) = self.mutex.as_ref() {
            if let Ok(mut locked) = mutex.lock() {
                if locked.0 {
                    return Poll::Ready(());
                }
                locked.1.replace(cx.waker().clone());
            }
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

/// [`SyncWait`] is inserted into [`WaitQueue`] for the caller to synchronously wait until
/// signalled.
#[derive(Debug)]
struct SyncWait {
    next: usize,
    condvar: Condvar,
    mutex: Mutex<bool>,
}

impl SyncWait {
    /// Creates a new [`SyncWait`].
    const fn new(next: usize) -> SyncWait {
        #[allow(clippy::mutex_atomic)]
        SyncWait {
            next,
            condvar: Condvar::new(),
            mutex: Mutex::new(false),
        }
    }

    /// Waits for a signal.
    fn wait(&self) {
        #[allow(clippy::mutex_atomic)]
        let mut completed = unsafe { self.mutex.lock().unwrap_unchecked() };
        while !*completed {
            completed = unsafe { self.condvar.wait(completed).unwrap_unchecked() };
        }
    }

    /// Sends a signal.
    fn signal(&self) {
        #[allow(clippy::mutex_atomic)]
        let mut completed = unsafe { self.mutex.lock().unwrap_unchecked() };
        *completed = true;
        self.condvar.notify_one();
    }
}
