use crossbeam_epoch::{Atomic, Guard};
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::{Condvar, Mutex};

pub const ARRAY_SIZE: usize = 16;
pub const MAX_RESIZING_FACTOR: usize = 6;

/// Tags are embedded inside the wait_queue variable.
const LOCK_TAG: usize = 1;
const WAITING_TAG: u32 = 1 << 1;

pub struct Cell<K: Clone + Eq, V: Clone> {
    wait_queue: Atomic<WaitQueueEntry>,
    data: Atomic<[MaybeUninit<(K, V)>; ARRAY_SIZE]>,
}

impl<K: Clone + Eq, V: Clone> Default for Cell<K, V> {
    fn default() -> Self {
        Cell::<K, V> {
            wait_queue: Atomic::null(),
            data: Atomic::null(),
        }
    }
}

impl<K: Clone + Eq, V: Clone> Cell<K, V> {
    fn lock(&self, guard: &Guard) -> bool {
        // [TODO]
        let mut current = self.wait_queue.load(Relaxed, guard);
        loop {
            if (current.tag() & LOCK_TAG) == LOCK_TAG {
                break;
            } else {
                return true;
            }
        }
        false
    }
}

impl<K: Clone + Eq, V: Clone> Drop for Cell<K, V> {
    fn drop(&mut self) {}
}

struct DataArray<K: Clone + Eq, V: Clone> {
    metadata: AtomicU32,
    data: [MaybeUninit<(K, V)>; ARRAY_SIZE],
    link: Atomic<DataArray<K, V>>,
}

impl<K: Clone + Eq, V: Clone> DataArray<K, V> {
    fn new(link: Atomic<DataArray<K, V>>) -> DataArray<K, V> {
        DataArray {
            metadata: AtomicU32::new(0),
            data: unsafe { MaybeUninit::uninit().assume_init() },
            link,
        }
    }
}

struct WaitQueueEntry {
    mutex: Mutex<bool>,
    condvar: Condvar,
    next: *mut WaitQueueEntry,
}

impl WaitQueueEntry {
    fn new(wait_queue: *mut WaitQueueEntry) -> WaitQueueEntry {
        WaitQueueEntry {
            mutex: Mutex::new(false),
            condvar: Condvar::new(),
            next: wait_queue,
        }
    }

    fn wait(&self) {
        let mut completed = self.mutex.lock().unwrap();
        while !*completed {
            completed = self.condvar.wait(completed).unwrap();
        }
    }

    fn signal(&self) {
        let mut completed = self.mutex.lock().unwrap();
        *completed = true;
        self.condvar.notify_one();
    }
}
