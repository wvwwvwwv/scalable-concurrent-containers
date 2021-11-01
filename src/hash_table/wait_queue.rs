use std::sync::{Condvar, Mutex};

/// [`WaitQueueEntry`] implements an unfair wait queue.
pub struct WaitQueue {
    mutex: Mutex<u8>,
    condvar: Condvar,
    pub(super) next_ptr: *mut WaitQueue,
}

impl WaitQueue {
    /// Creates a new [`WaitQueueEntry`].
    pub fn new(next_ptr: *mut WaitQueue) -> WaitQueue {
        WaitQueue {
            mutex: Mutex::new(0_u8),
            condvar: Condvar::new(),
            next_ptr,
        }
    }

    /// Waits for a signal.
    pub fn wait(&self) {
        let mut completed = self.mutex.lock().unwrap();
        while *completed == 0_u8 {
            completed = self.condvar.wait(completed).unwrap();
        }
    }

    /// Sends a signal.
    pub fn signal(&self) {
        let mut completed = self.mutex.lock().unwrap();
        *completed = 1_u8;
        self.condvar.notify_one();
    }
}
