use std::future::Future;
use std::mem::transmute;
use std::pin::Pin;
use std::ptr::addr_of_mut;
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
    /// The wait queue of the [`Cell`].
    wait_queue: AtomicUsize,
}

impl WaitQueue {
    /// Waits for the condition to be met or signalled.
    #[inline]
    pub fn wait_sync<T, F: FnOnce() -> Result<T, ()>>(&self, f: F) -> Result<T, ()> {
        // Inserts the thread into the wait queue.
        let mut current = self.wait_queue.load(Relaxed);
        let mut entry = SyncWait::new(current);

        while let Err(actual) =
            self.wait_queue
                .compare_exchange(current, addr_of_mut!(entry) as usize, AcqRel, Relaxed)
        {
            current = actual;
            entry.next = current;
        }

        // Execute the closure.
        let result = f();
        if result.is_ok() {
            self.signal();
        }

        entry.wait();
        result
    }

    /// Signals the threads in the wait queue.
    #[inline]
    pub fn signal(&self) {
        let mut current = self.wait_queue.swap(0, AcqRel);
        while (current & (!ASYNC)) != 0 {
            if (current & ASYNC) == 0 {
                // Synchronous.
                let entry_ref = unsafe { &*SyncWait::reinterpret(current) };
                let next = entry_ref.next;
                entry_ref.signal();
                current = next;
            } else {
                // Asynchronous.
                let entry_ref = unsafe { &*AsyncWait::reinterpret(current & (!ASYNC)) };
                let next = entry_ref.next;
                entry_ref.signal();
                current = next;
            }
        }
    }
}

/// [`AsyncWait`] is inserted into [`WaitQueue`] for the caller to await until woken up.
///
/// If an [`AsyncWait`] is polled without being inserted into a [`WaitQueue`], the [`AsyncWait`]
/// yields the task executor.
#[derive(Debug, Default)]
pub(crate) struct AsyncWait {
    next: usize,
    mutex: Option<Mutex<(bool, Option<Waker>)>>,
}

impl AsyncWait {
    /// Sends a signal.
    fn signal(&self) {
        if let Some(mutex) = self.mutex.as_ref() {
            let mut locked = mutex.lock().unwrap();
            locked.0 = true;
            if let Some(waker) = locked.1.take() {
                waker.wake();
            }
        } else {
            unreachable!();
        }
    }

    /// Reinterpret `usize` as `*mut AsyncWait`.
    unsafe fn reinterpret(val: usize) -> *mut AsyncWait {
        transmute(val)
    }
}

impl Future for AsyncWait {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(mutex) = self.mutex.as_ref() {
            if let Ok(mut locked) = mutex.try_lock() {
                if locked.0 {
                    return Poll::Ready(());
                }
                locked.1.replace(cx.waker().clone());
            } else {
                // It is being signalled.
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        } else if self.next == 0 {
            // `mutex` is not initialized: use `next` to yield the task executor.
            self.next = ASYNC;
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            // It has yielded a task executor before.
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
    fn new(next: usize) -> SyncWait {
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
        let mut completed = self.mutex.lock().unwrap();
        while !*completed {
            completed = self.condvar.wait(completed).unwrap();
        }
    }

    /// Sends a signal.
    fn signal(&self) {
        #[allow(clippy::mutex_atomic)]
        let mut completed = self.mutex.lock().unwrap();
        *completed = true;
        self.condvar.notify_one();
    }

    /// Reinterpret `usize` as `*mut SyncWait`.
    unsafe fn reinterpret(val: usize) -> *mut SyncWait {
        transmute(val)
    }
}
