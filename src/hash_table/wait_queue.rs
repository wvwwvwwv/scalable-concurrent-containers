use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::{Condvar, Mutex};

/// [`WaitQueue`] implements an unfair wait queue.
///
/// The sole purpose of the data structure is to avoid busy-waiting.
#[derive(Default)]
pub struct WaitQueue {
    /// The wait queue of the [`Cell`].
    wait_queue: AtomicPtr<Entry>,
}

impl WaitQueue {
    /// Waits for the condition to be met or signalled.
    #[inline]
    pub fn wait<T, F: FnOnce() -> Option<T>>(&self, f: F) -> Option<T> {
        // Inserts the thread into the wait queue.
        let mut current = self.wait_queue.load(Relaxed);
        let mut entry = Entry::new(current);

        while let Err(actual) =
            self.wait_queue
                .compare_exchange(current, &mut entry as *mut Entry, Release, Relaxed)
        {
            current = actual;
            entry.next_ptr = current;
        }

        // Executes the closure.
        let result = f();
        if result.is_some() {
            self.signal();
        }
        entry.wait();
        result
    }

    /// Signals the threads in the wait queue.
    #[inline]
    pub fn signal(&self) {
        let mut current = self.wait_queue.swap(std::ptr::null_mut(), Acquire);
        while let Some(entry_ref) = unsafe { current.as_ref() } {
            let next_ptr = entry_ref.next_ptr;
            entry_ref.signal();
            current = next_ptr;
        }
    }
}

/// [`Entry`] is inserted into [`WaitQueue`].
struct Entry {
    next_ptr: *mut Entry,
    condvar: Condvar,
    mutex: Mutex<bool>,
}

impl Entry {
    /// Creates a new [`Entry`].
    fn new(next_ptr: *mut Entry) -> Entry {
        #[allow(clippy::mutex_atomic)]
        Entry {
            next_ptr,
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
}
