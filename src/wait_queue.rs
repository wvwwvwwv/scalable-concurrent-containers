use std::future::Future;
use std::mem::transmute;
use std::pin::Pin;
use std::ptr::addr_of_mut;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Relaxed};
use std::sync::{Condvar, Mutex};
use std::task::{Context, Poll, Waker};

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
        let mut entry = SyncEntry::new(current);

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
        while current != 0 {
            if (current & 1_usize) == 0 {
                // Synchronous.
                let entry_ref = unsafe { &*SyncEntry::reinterpret(current) };
                let next = entry_ref.next;
                entry_ref.signal();
                current = next;
            } else {
                // Asynchronous.
                let entry_ref = unsafe { &*AsyncEntry::reinterpret(current & (!1_usize)) };
                let next = entry_ref.next;
                entry_ref.signal();
                current = next;
            }
        }
    }
}

/// [`AsyncEntry`] is inserted into [`WaitQueue`] for the caller to await until woken up.
#[derive(Debug)]
pub(crate) struct AsyncEntry {
    next: usize,
    mutex: Mutex<(bool, Option<Waker>)>,
}

impl AsyncEntry {
    /// Sends a signal.
    fn signal(&self) {
        let mut locked = self.mutex.lock().unwrap();
        locked.0 = true;
        if let Some(waker) = locked.1.take() {
            waker.wake();
        }
    }

    /// Reinterpret `usize` as `*mut AsyncEntry`.
    unsafe fn reinterpret(val: usize) -> *mut AsyncEntry {
        transmute(val)
    }
}

impl Future for AsyncEntry {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut locked = self.mutex.lock().unwrap();
        if locked.0 {
            return Poll::Ready(());
        }
        locked.1.replace(cx.waker().clone());
        Poll::Pending
    }
}

/// [`SyncEntry`] is inserted into [`WaitQueue`] for the caller to synchronously wait until
/// signalled.
#[derive(Debug)]
struct SyncEntry {
    next: usize,
    condvar: Condvar,
    mutex: Mutex<bool>,
}

impl SyncEntry {
    /// Creates a new [`SyncEntry`].
    fn new(next: usize) -> SyncEntry {
        #[allow(clippy::mutex_atomic)]
        SyncEntry {
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

    /// Reinterpret `usize` as `*mut SyncEntry`.
    unsafe fn reinterpret(val: usize) -> *mut SyncEntry {
        transmute(val)
    }
}
