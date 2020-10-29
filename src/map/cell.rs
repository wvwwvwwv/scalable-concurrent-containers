use std::convert::TryInto;
use std::ptr;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32};
use std::sync::{Condvar, Mutex};

pub struct Cell<K, V> {
    link: Option<Box<EntryLink<K, V>>>,
    /// KILLED_FLAG | XLOCK | SLOCK | OCCUPANCY_BIT | SIZE
    metadata: AtomicU32,
    wait_queue: AtomicPtr<WaitQueueEntry>,
    partial_hash_array: [u32; 10],
}

/// ExclusiveLocker
pub struct ExclusiveLocker<'a, K, V> {
    cell: &'a Cell<K, V>,
    metadata: u32,
}

/// SharedLocker
pub struct SharedLocker<'a, K, V> {
    cell: &'a Cell<K, V>,
}

struct EntryLink<K, V> {
    key_value_pair: (K, V),
    next: Option<Box<EntryLink<K, V>>>,
}

struct WaitQueueEntry {
    mutex: Mutex<bool>,
    condvar: Condvar,
    completed: AtomicBool,
    next: *mut WaitQueueEntry,
}

impl<K, V> Cell<K, V> {
    const KILLED_FLAG: u32 = 1 << 31;
    const LOCK_MASK: u32 = ((1 << 15) - 1) << 16;
    const XLOCK: u32 = 1 << 30;
    const SLOCK_MAX: u32 = Self::LOCK_MASK & (!Self::XLOCK);
    const SLOCK: u32 = 1 << 16;
    const OCCUPANCY_MASK: u32 = ((1 << 10) - 1) << 6;
    const OCCUPANCY_BIT: u32 = 1 << 6;
    const SIZE_MASK: u32 = (1 << 6) - 1;

    pub fn killed(&self) -> bool {
        (self.metadata.load(Relaxed) & Self::KILLED_FLAG) == Self::KILLED_FLAG
    }

    pub fn size(&self) -> usize {
        (self.metadata.load(Relaxed) & Self::SIZE_MASK)
            .try_into()
            .unwrap()
    }

    fn wait<T, F: FnOnce() -> Option<T>>(&self, f: F) -> Option<T> {
        let mut barrier = WaitQueueEntry::new(self.wait_queue.load(Relaxed));
        let barrier_ptr: *mut WaitQueueEntry = &mut barrier;

        // insert itself into the wait queue
        while let Err(result) =
            self.wait_queue
                .compare_exchange(barrier.next, barrier_ptr, Release, Relaxed)
        {
            barrier.next = result;
        }
        // try-lock again once the barrier is inserted into the wait queue
        let locked = f();
        if locked.is_some() {
            self.wakeup();
        }
        barrier.wait();
        locked
    }

    fn wakeup(&self) {
        let mut barrier_ptr: *mut WaitQueueEntry = self.wait_queue.load(Acquire);
        while let Err(result) =
            self.wait_queue
                .compare_exchange(barrier_ptr, ptr::null_mut(), Acquire, Relaxed)
        {
            barrier_ptr = result;
            if barrier_ptr == ptr::null_mut() {
                return;
            }
        }

        while barrier_ptr != ptr::null_mut() {
            let next_ptr = unsafe { (*barrier_ptr).next };
            unsafe {
                (*barrier_ptr).signal();
            };
            barrier_ptr = next_ptr;
        }
    }
}

impl<K, V> Default for Cell<K, V> {
    fn default() -> Self {
        Cell {
            link: None,
            metadata: AtomicU32::new(0),
            wait_queue: AtomicPtr::new(ptr::null_mut()),
            partial_hash_array: [0; 10],
        }
    }
}

impl<'a, K, V> ExclusiveLocker<'a, K, V> {
    /// Creates a new ExclusiveLocker instance with the cell exclusively locked.
    fn lock(cell: &'a Cell<K, V>) -> ExclusiveLocker<'a, K, V> {
        loop {
            if let Some(result) = Self::try_lock(cell) {
                return result;
            }
            if let Some(result) = cell.wait(|| Self::try_lock(cell)) {
                return result;
            }
        }
    }

    /// Creates a new ExclusiveLocker instance if the cell is exclusively locked.
    fn try_lock(cell: &'a Cell<K, V>) -> Option<ExclusiveLocker<'a, K, V>> {
        let mut current = cell.metadata.load(Relaxed);
        loop {
            match cell.metadata.compare_exchange(
                current & (!Cell::<K, V>::LOCK_MASK),
                current | Cell::<K, V>::XLOCK,
                Acquire,
                Relaxed,
            ) {
                Ok(result) => {
                    return Some(ExclusiveLocker {
                        cell: cell,
                        metadata: result | Cell::<K, V>::XLOCK,
                    })
                }
                Err(result) => {
                    if result & Cell::<K, V>::LOCK_MASK != 0 {
                        current = result;
                        return None;
                    }
                    current = result;
                }
            }
        }
    }
}

impl<'a, K, V> SharedLocker<'a, K, V> {
    /// Creates a new SharedLocker instance with the cell shared locked.
    fn lock(cell: &'a Cell<K, V>) -> SharedLocker<'a, K, V> {
        loop {
            if let Some(result) = Self::try_lock(cell) {
                return result;
            }
            if let Some(result) = cell.wait(|| Self::try_lock(cell)) {
                return result;
            }
        }
    }

    /// Creates a new SharedLocker instance if the cell is shared locked.
    fn try_lock(cell: &'a Cell<K, V>) -> Option<SharedLocker<'a, K, V>> {
        let mut current = cell.metadata.load(Relaxed);
        loop {
            if current & Cell::<K, V>::LOCK_MASK >= Cell::<K, V>::SLOCK_MAX {
                current = current & (!Cell::<K, V>::XLOCK) - Cell::<K, V>::SLOCK;
            }
            match cell.metadata.compare_exchange(
                current,
                current + Cell::<K, V>::SLOCK,
                Acquire,
                Relaxed,
            ) {
                Ok(result) => return Some(SharedLocker { cell: cell }),
                Err(result) => {
                    if result & Cell::<K, V>::LOCK_MASK >= Cell::<K, V>::SLOCK_MAX {
                        current = result;
                        return None;
                    }
                    current = result;
                }
            }
        }
    }
}

impl WaitQueueEntry {
    fn new(wait_queue: *mut WaitQueueEntry) -> WaitQueueEntry {
        WaitQueueEntry {
            mutex: Mutex::new(false),
            condvar: Condvar::new(),
            completed: AtomicBool::new(false),
            next: wait_queue,
        }
    }

    fn wait(&self) {
        let mut completed = self.mutex.lock().unwrap();
        while !*completed {
            completed = self.condvar.wait(completed).unwrap();
        }
        while !self.completed.load(Relaxed) {}
    }

    fn signal(&self) {
        let mut completed = self.mutex.lock().unwrap();
        *completed = true;
        self.condvar.notify_one();
        drop(completed);
        self.completed.store(true, Relaxed);
    }
}

impl<'a, K, V> Drop for ExclusiveLocker<'a, K, V> {
    fn drop(&mut self) {
        let mut current = self.metadata;
        loop {
            assert!(current & Cell::<K, V>::LOCK_MASK != 0);
            let new = if current & Cell::<K, V>::XLOCK == Cell::<K, V>::XLOCK {
                current & (!Cell::<K, V>::XLOCK)
            } else {
                current - Cell::<K, V>::SLOCK
            };
            match self
                .cell
                .metadata
                .compare_exchange(current, new, Release, Relaxed)
            {
                Ok(_) => break,
                Err(result) => current = result,
            }
        }
        self.cell.wakeup();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn basic_assumptions() {
        assert_eq!(std::mem::size_of::<Cell<u64, bool>>(), 64);
        assert_eq!(
            Cell::<bool, u32>::KILLED_FLAG & Cell::<bool, u32>::LOCK_MASK,
            0
        );
        assert_eq!(
            Cell::<bool, u32>::LOCK_MASK & Cell::<bool, u32>::OCCUPANCY_MASK,
            0
        );
        assert_eq!(
            Cell::<bool, u32>::OCCUPANCY_MASK & Cell::<bool, u32>::SIZE_MASK,
            0
        );
        assert_eq!(
            Cell::<bool, u32>::KILLED_FLAG
                | Cell::<bool, u32>::LOCK_MASK
                | Cell::<bool, u32>::OCCUPANCY_MASK
                | Cell::<bool, u32>::SIZE_MASK,
            !(0 as u32)
        );
    }

    #[test]
    fn basic_exclusive_locker() {
        let threads = 12;
        let barrier = Arc::new(Barrier::new(threads));
        let cell: Arc<Cell<bool, u8>> = Arc::new(Default::default());
        let mut thread_handles = Vec::with_capacity(threads);
        for tid in 0..threads {
            let barrier_copied = barrier.clone();
            let cell_copied = cell.clone();
            let thread_id = tid;
            thread_handles.push(thread::spawn(move || {
                barrier_copied.wait();
                for i in 0..4096 {
                    let locker = ExclusiveLocker::lock(&*cell_copied);
                    if i % 256 == 255 {
                        println!("locked {}:{}", thread_id, i);
                    }
                    drop(locker);
                }
            }));
        }
        for handle in thread_handles {
            handle.join().unwrap();
        }
    }
}
