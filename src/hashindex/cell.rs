use crossbeam_epoch::{Atomic, Guard, Shared};
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::{Condvar, Mutex};

pub const ARRAY_SIZE: usize = 16;
pub const MAX_RESIZING_FACTOR: usize = 6;

/// Tags are embedded inside the wait_queue variable.
const LOCK_TAG: usize = 1;

/// Flags are embedded inside a partial hash value.
const OCCUPIED: u8 = 1;
const REMOVED: u8 = 1u8 << 1;

pub struct Cell<K: Clone + Eq, V: Clone> {
    wait_queue: Atomic<WaitQueueEntry>,
    /// data being null indicates that the Cell is killed.
    data: Atomic<DataArray<K, V>>,
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
    /// Returns true if the Cell has been killed.
    pub fn killed(&self, guard: &Guard) -> bool {
        self.data.load(Relaxed, guard).is_null()
    }

    fn wait<T, F: FnOnce() -> Option<T>>(&self, f: F, guard: &Guard) -> Option<T> {
        // Inserts the condvar into the wait queue.
        let mut current = self.wait_queue.load(Relaxed, guard);
        let mut condvar = WaitQueueEntry::new(Atomic::from(current));
        let mut next = Shared::from(&condvar as *const _).with_tag(current.tag());
        while let Err(result) = self
            .wait_queue
            .compare_exchange(current, next, Release, Relaxed, guard)
        {
            current = result.current;
            next = Shared::from(&condvar as *const _).with_tag(current.tag());
            condvar.next = Atomic::from(result.current);
        }

        // Tries to lock again once the condvar is inserted into the wait queue.
        let locked = f();
        if locked.is_some() {
            self.wakeup(guard);
        }
        condvar.wait();
        locked
    }

    fn wakeup(&self, guard: &Guard) {
        let mut current = self.wait_queue.load(Acquire, guard);
        let mut next = Shared::null().with_tag(current.tag());
        while let Err(result) = self
            .wait_queue
            .compare_exchange(current, next, Acquire, Relaxed, guard)
        {
            current = result.current;
            if current.is_null() {
                return;
            }
            next = Shared::null().with_tag(current.tag());
        }

        while !current.is_null() {
            let cond_var_ref = unsafe { current.deref() };
            let next_ptr = cond_var_ref.next.load(Acquire, guard);
            cond_var_ref.signal();
            current = next_ptr;
        }
    }
}

impl<K: Clone + Eq, V: Clone> Drop for Cell<K, V> {
    fn drop(&mut self) {}
}

pub struct CellLocker<'c, K: Clone + Eq, V: Clone> {
    cell_ref: &'c Cell<K, V>,
}

impl<'c, K: Clone + Eq, V: Clone> CellLocker<'c, K, V> {
    pub fn lock(cell: &'c Cell<K, V>, guard: &Guard) -> CellLocker<'c, K, V> {
        loop {
            if let Some(result) = Self::try_lock(cell, guard) {
                return result;
            }
            if let Some(result) = cell.wait(|| Self::try_lock(cell, guard), guard) {
                return result;
            }
        }
    }

    fn try_lock(cell: &'c Cell<K, V>, guard: &Guard) -> Option<CellLocker<'c, K, V>> {
        let current = cell.wait_queue.load(Relaxed, guard);
        if (current.tag() & LOCK_TAG) == 0 {
            let next = current.with_tag(LOCK_TAG);
            if cell
                .wait_queue
                .compare_exchange(current, next, Acquire, Relaxed, guard)
                .is_ok()
            {
                return Some(CellLocker { cell_ref: cell });
            }
        }
        None
    }
}

impl<'c, K: Clone + Eq, V: Clone> Drop for CellLocker<'c, K, V> {
    fn drop(&mut self) {
        let mut guard: Option<Guard> = None;
        let mut current = self
            .cell_ref
            .wait_queue
            .load(Relaxed, unsafe { crossbeam_epoch::unprotected() });
        loop {
            let wakeup = if !current.is_null() {
                // In order to prevent the Cell from being dropped while waking up other threads, pins the thread.
                if guard.is_none() {
                    guard.replace(crossbeam_epoch::pin());
                }
                true
            } else {
                false
            };
            debug_assert!(current.tag() == LOCK_TAG);
            match self.cell_ref.wait_queue.compare_exchange(
                current,
                current.with_tag(0),
                Release,
                Relaxed,
                unsafe { crossbeam_epoch::unprotected() },
            ) {
                Ok(_) => {
                    if wakeup {
                        self.cell_ref.wakeup(guard.as_ref().unwrap());
                    }
                    break;
                }
                Err(result) => current = result.current,
            }
        }
    }
}

struct DataArray<K: Clone + Eq, V: Clone> {
    /// The lower two-bit of a partial hash value represents the state of the corresponding entry.
    partial_hash_array: [u8; ARRAY_SIZE],
    data: [MaybeUninit<(K, V)>; ARRAY_SIZE],
    link: Atomic<DataArray<K, V>>,
}

impl<K: Clone + Eq, V: Clone> DataArray<K, V> {
    fn new(link: Atomic<DataArray<K, V>>) -> DataArray<K, V> {
        DataArray {
            partial_hash_array: [0; ARRAY_SIZE],
            data: unsafe { MaybeUninit::uninit().assume_init() },
            link,
        }
    }
}

struct WaitQueueEntry {
    mutex: Mutex<bool>,
    condvar: Condvar,
    next: Atomic<WaitQueueEntry>,
}

impl WaitQueueEntry {
    fn new(wait_queue: Atomic<WaitQueueEntry>) -> WaitQueueEntry {
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

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn cell_locker() {
        let num_threads = (ARRAY_SIZE + 1) as usize;
        let barrier = Arc::new(Barrier::new(num_threads));
        let cell: Arc<Cell<usize, usize>> = Arc::new(Default::default());
        let mut data: [u64; 128] = [0; 128];
        let mut thread_handles = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            let barrier_copied = barrier.clone();
            let cell_copied = cell.clone();
            let data_ptr = std::sync::atomic::AtomicPtr::new(&mut data);
            thread_handles.push(thread::spawn(move || {
                barrier_copied.wait();
                let guard = crossbeam_epoch::pin();
                for i in 0..4096 {
                    let mut xlocker = CellLocker::lock(&*cell_copied, &guard);
                    let mut sum: u64 = 0;
                    for j in 0..128 {
                        unsafe {
                            sum += (*data_ptr.load(Relaxed))[j];
                            (*data_ptr.load(Relaxed))[j] = if i % 4 == 0 { 2 } else { 4 }
                        };
                    }
                    assert_eq!(sum % 256, 0);
                    drop(xlocker);
                }
            }));
        }
        for handle in thread_handles {
            handle.join().unwrap();
        }
        let mut sum: u64 = 0;
        for j in 0..128 {
            sum += data[j];
        }
        assert_eq!(sum % 256, 0);
    }
}
