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

    fn wait<T, F: Fn() -> Option<T>>(&self, f: F) -> Option<T> {
        let mut locked = f();
        if locked.is_some() {
            return locked;
        }

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
        locked = f();
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

impl<'a, K, V> ExclusiveLocker<'a, K, V> {
    /// Create a new ExclusiveLocker instance with the cell exclusively locked.
    fn lock(cell: &'a Cell<K, V>) -> ExclusiveLocker<'a, K, V> {
        loop {
            if let Some(result) = cell.wait(|| Self::try_lock(cell)) {
                return result;
            }
        }
    }

    /// Create a new ExclusiveLocker instance if the cell is exclusively locked.
    fn try_lock(cell: &'a Cell<K, V>) -> Option<ExclusiveLocker<'a, K, V>> {
        let mut current = cell.metadata.load(Relaxed);
        loop {
            match cell.metadata.compare_exchange(
                current & (!Cell::<K, V>::LOCK_MASK),
                (current & (!Cell::<K, V>::LOCK_MASK)) | Cell::<K, V>::XLOCK,
                Acquire,
                Relaxed,
            ) {
                Ok(result) => {
                    assert_eq!(result & Cell::<K, V>::LOCK_MASK, 0);
                    return Some(ExclusiveLocker {
                        cell: cell,
                        metadata: result | Cell::<K, V>::XLOCK,
                    });
                }
                Err(result) => {
                    if result & Cell::<K, V>::LOCK_MASK != 0 {
                        return None;
                    }
                    current = result;
                }
            }
        }
    }
}

impl<'a, K, V> SharedLocker<'a, K, V> {
    /// Create a new SharedLocker instance with the cell shared locked.
    fn lock(cell: &'a Cell<K, V>) -> SharedLocker<'a, K, V> {
        loop {
            if let Some(result) = cell.wait(|| Self::try_lock(cell)) {
                return result;
            }
        }
    }

    /// Create a new SharedLocker instance if the cell is shared locked.
    fn try_lock(cell: &'a Cell<K, V>) -> Option<SharedLocker<'a, K, V>> {
        let mut current = cell.metadata.load(Relaxed);
        loop {
            if current & Cell::<K, V>::LOCK_MASK >= Cell::<K, V>::SLOCK_MAX {
                current = current & (!Cell::<K, V>::LOCK_MASK);
            }
            assert_eq!(current & Cell::<K, V>::LOCK_MASK & Cell::<K, V>::XLOCK, 0);
            assert!(current & Cell::<K, V>::LOCK_MASK < Cell::<K, V>::SLOCK_MAX);
            match cell.metadata.compare_exchange(
                current,
                current + Cell::<K, V>::SLOCK,
                Acquire,
                Relaxed,
            ) {
                Ok(_) => return Some(SharedLocker { cell: cell }),
                Err(result) => {
                    if result & Cell::<K, V>::LOCK_MASK >= Cell::<K, V>::SLOCK_MAX {
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

impl<'a, K, V> Drop for ExclusiveLocker<'a, K, V> {
    fn drop(&mut self) {
        let mut current = self.metadata;
        loop {
            assert_eq!(current & Cell::<K, V>::LOCK_MASK, Cell::<K, V>::XLOCK);
            match self.cell.metadata.compare_exchange(
                current,
                current & (!Cell::<K, V>::XLOCK),
                Release,
                Relaxed,
            ) {
                Ok(_) => break,
                Err(result) => current = result,
            }
        }
        self.cell.wakeup();
    }
}

impl<'a, K, V> Drop for SharedLocker<'a, K, V> {
    fn drop(&mut self) {
        // no modification is allowed with a SharedLocker held, therefore no memory fences are required
        let mut current = self.cell.metadata.load(Relaxed);
        loop {
            assert!(current & Cell::<K, V>::LOCK_MASK <= Cell::<K, V>::SLOCK_MAX);
            assert!(current & Cell::<K, V>::LOCK_MASK >= Cell::<K, V>::SLOCK);
            match self.cell.metadata.compare_exchange(
                current,
                current - Cell::<K, V>::SLOCK,
                Relaxed,
                Relaxed,
            ) {
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
        assert!(Cell::<bool, u32>::XLOCK > Cell::<bool, u32>::SLOCK_MAX);
        assert!(
            (Cell::<bool, u32>::XLOCK & Cell::<bool, u32>::LOCK_MASK)
                > Cell::<bool, u32>::SLOCK_MAX
        );
        assert_eq!(
            ((Cell::<bool, u32>::XLOCK << 1) & Cell::<bool, u32>::LOCK_MASK),
            0
        );
        assert_eq!(
            Cell::<bool, u32>::XLOCK & Cell::<bool, u32>::LOCK_MASK,
            Cell::<bool, u32>::XLOCK
        );
        assert_eq!(
            Cell::<bool, u32>::SLOCK & (!Cell::<bool, u32>::LOCK_MASK),
            0
        );
        assert_eq!(
            Cell::<bool, u32>::SLOCK & Cell::<bool, u32>::LOCK_MASK,
            Cell::<bool, u32>::SLOCK
        );
        assert_eq!(
            (Cell::<bool, u32>::SLOCK >> 1) & Cell::<bool, u32>::LOCK_MASK,
            0
        );
        assert_eq!(
            Cell::<bool, u32>::SLOCK & Cell::<bool, u32>::SLOCK_MAX,
            Cell::<bool, u32>::SLOCK
        );
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
    fn basic_locker() {
        let threads = 6;
        let barrier = Arc::new(Barrier::new(threads));
        let cell: Arc<Cell<bool, u8>> = Arc::new(Default::default());
        let mut data: [u64; 128] = [0; 128];
        let mut thread_handles = Vec::with_capacity(threads);
        for _ in 0..threads {
            let barrier_copied = barrier.clone();
            let cell_copied = cell.clone();
            let data_ptr = AtomicPtr::new(&mut data);
            thread_handles.push(thread::spawn(move || {
                barrier_copied.wait();
                for i in 0..4096 {
                    if i % 2 == 0 {
                        let xlocker = ExclusiveLocker::lock(&*cell_copied);
                        let mut sum: u64 = 0;
                        for j in 0..128 {
                            unsafe {
                                sum += (*data_ptr.load(Relaxed))[j];
                                (*data_ptr.load(Relaxed))[j] = if i % 4 == 0 { 2 } else { 4 }
                            };
                        }
                        assert_eq!(sum % 256, 0);
                        drop(xlocker);
                    } else {
                        let slocker = SharedLocker::lock(&*cell_copied);
                        let mut sum: u64 = 0;
                        for j in 0..128 {
                            unsafe { sum += (*data_ptr.load(Relaxed))[j] };
                        }
                        assert_eq!(sum % 256, 0);
                        drop(slocker);
                    }
                }
            }));
        }
        for handle in thread_handles {
            handle.join().unwrap();
        }
    }
}
