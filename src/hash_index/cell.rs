use crate::common::cell_array::CellSize;
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::{Condvar, Mutex};

pub const ARRAY_SIZE: usize = 32;
pub const MAX_RESIZING_FACTOR: usize = 6;

/// Tags are embedded inside the wait_queue variable.
const LOCK_TAG: usize = 1;
const KILL_TAG: usize = 1 << 1;

/// Flags are embedded inside a partial hash value.
const OCCUPIED: u8 = 1u8 << 6;
const REMOVED: u8 = 1u8 << 7;

pub struct Cell<K: Clone + Eq, V: Clone> {
    /// wait_queue additionally stores the state of the Cell: locked or killed.
    wait_queue: Atomic<WaitQueueEntry>,
    /// DataArray stores key-value pairs with their metadata.
    data: Atomic<DataArray<K, V>>,
    /// The number of valid entries in the Cell.
    num_entries: AtomicUsize,
}

impl<K: Clone + Eq, V: Clone> Default for Cell<K, V> {
    fn default() -> Self {
        Cell::<K, V> {
            wait_queue: Atomic::null(),
            data: Atomic::null(),
            num_entries: AtomicUsize::new(0),
        }
    }
}

impl<K: Clone + Eq, V: Clone> Cell<K, V> {
    /// Returns true if the Cell has been killed.
    pub fn killed(&self, guard: &Guard) -> bool {
        (self.wait_queue.load(Relaxed, guard).tag() & KILL_TAG) == KILL_TAG
    }

    /// Returns the number of entries in the Cell.
    pub fn num_entries(&self) -> usize {
        self.num_entries.load(Relaxed)
    }

    /// Iterates the contents of the Cell.
    pub fn iter<'g>(&'g self, guard: &'g Guard) -> CellIterator<'g, K, V> {
        CellIterator::new(self, guard)
    }

    /// Searches for an entry associated with the given key.
    pub fn search<'g>(&self, key: &K, partial_hash: u8, guard: &'g Guard) -> Option<&'g (K, V)> {
        // In order to read the linked list correctly, an acquire fence is required.
        let mut data_array = self.data.load(Acquire, guard);
        let preferred_index = partial_hash as usize % ARRAY_SIZE;
        while !data_array.is_null() {
            let data_array_ref = unsafe { data_array.deref() };
            let preferred_index_hash = data_array_ref.partial_hash_array[preferred_index];
            if preferred_index_hash == ((partial_hash & (!REMOVED)) | OCCUPIED) {
                let entry_ptr = data_array_ref.data[preferred_index].as_ptr();
                std::sync::atomic::fence(Acquire);
                if unsafe { &(*entry_ptr) }.0 == *key {
                    return Some(unsafe { &(*entry_ptr) });
                }
            }
            for (index, hash) in data_array_ref.partial_hash_array.iter().enumerate() {
                if index == preferred_index {
                    continue;
                }
                if *hash == ((partial_hash & (!REMOVED)) | OCCUPIED) {
                    let entry_ptr = data_array_ref.data[index].as_ptr();
                    std::sync::atomic::fence(Acquire);
                    if unsafe { &(*entry_ptr) }.0 == *key {
                        return Some(unsafe { &(*entry_ptr) });
                    }
                }
            }
            data_array = data_array_ref.link.load(Acquire, guard);
        }
        None
    }

    fn wait<T, F: FnOnce() -> Option<T>>(&self, f: F, guard: &Guard) -> Option<T> {
        // Inserts the condvar into the wait queue.
        let mut current = self.wait_queue.load(Relaxed, guard);
        if (current.tag() & KILL_TAG) == KILL_TAG {
            return None;
        }
        let mut condvar = WaitQueueEntry::new(Atomic::from(current));

        // Keeps the tag as it is.
        let mut next = Shared::from(&condvar as *const _).with_tag(current.tag());
        while let Err(result) = self
            .wait_queue
            .compare_exchange(current, next, Release, Relaxed, guard)
        {
            current = result.current;
            if (current.tag() & KILL_TAG) == KILL_TAG {
                return None;
            }
            next = Shared::from(&condvar as *const _).with_tag(current.tag());
            condvar.next = Atomic::from(result.current);
        }

        // Tries to lock again once the condvar is inserted into the wait queue.
        let locked = f();
        if locked.is_some() {
            self.wakeup(guard);
        }

        // Locking failed.
        condvar.wait();
        locked
    }

    fn wakeup(&self, guard: &Guard) {
        // Keeps the tag as it is.
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
    fn drop(&mut self) {
        // The Cell must have been killed.
        debug_assert!(self.killed(unsafe { crossbeam_epoch::unprotected() }));
    }
}

impl<K: Clone + Eq, V: Clone> CellSize for Cell<K, V> {
    fn cell_size() -> usize {
        ARRAY_SIZE
    }
}

pub struct CellIterator<'g, K: Clone + Eq, V: Clone> {
    cell_ref: Option<&'g Cell<K, V>>,
    current_array: Shared<'g, DataArray<K, V>>,
    current_index: usize,
    guard_ref: &'g Guard,
}

impl<'g, K: Clone + Eq, V: Clone> CellIterator<'g, K, V> {
    pub fn new(cell: &'g Cell<K, V>, guard: &'g Guard) -> CellIterator<'g, K, V> {
        CellIterator {
            cell_ref: Some(cell),
            current_array: Shared::null(),
            current_index: usize::MAX,
            guard_ref: guard,
        }
    }
}

impl<'g, K: Clone + Eq, V: Clone> Iterator for CellIterator<'g, K, V> {
    type Item = &'g (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(&cell_ref) = self.cell_ref.as_ref() {
            if self.current_array.is_null() {
                // Starts scanning from the beginning.
                self.current_array = cell_ref.data.load(Acquire, self.guard_ref);
            }
            while !self.current_array.is_null() {
                // Search for the next valid entry.
                let array_ref = unsafe { self.current_array.deref() };
                let start_index = if self.current_index == usize::MAX {
                    0
                } else {
                    self.current_index + 1
                };
                for index in start_index..ARRAY_SIZE {
                    let hash = array_ref.partial_hash_array[index];
                    if (hash & OCCUPIED) != 0 && (hash & REMOVED) == 0 {
                        std::sync::atomic::fence(Acquire);
                        self.current_index = index;
                        let entry_ptr = array_ref.data[index].as_ptr();
                        return Some(unsafe { &(*entry_ptr) });
                    }
                }

                // Proceeds to the next DataArray.
                self.current_array = array_ref.link.load(Acquire, self.guard_ref);
                self.current_index = usize::MAX;
            }
            // Fuses itself.
            self.cell_ref.take();
        }
        None
    }
}

pub struct CellLocker<'g, K: Clone + Eq, V: Clone> {
    cell_ref: &'g Cell<K, V>,
    kill_on_drop: bool,
}

impl<'g, K: Clone + Eq, V: Clone> CellLocker<'g, K, V> {
    /// Locks the given Cell.
    pub fn lock(cell: &'g Cell<K, V>, guard: &'g Guard) -> Option<CellLocker<'g, K, V>> {
        loop {
            if let Some(result) = Self::try_lock(cell, guard) {
                return Some(result);
            }
            if let Some(result) = cell.wait(|| Self::try_lock(cell, guard), guard) {
                return Some(result);
            }
            if cell.killed(guard) {
                return None;
            }
        }
    }

    /// Returns a reference to the Cell.
    pub fn cell_ref(&self) -> &Cell<K, V> {
        self.cell_ref
    }

    /// Inserts a new key-value pair into the Cell.
    pub fn insert(&self, key: K, value: V, partial_hash: u8, guard: &Guard) -> Result<(), (K, V)> {
        if self.kill_on_drop {
            // The Cell will be killed.
            return Err((key, value));
        }

        // Starts taking the preferred index first.
        let mut data_array = self.cell_ref.data.load(Relaxed, guard);
        let data_array_head = data_array;
        let preferred_index = partial_hash as usize % ARRAY_SIZE;
        let mut free_data_array_ref: Option<&mut DataArray<K, V>> = None;
        let mut free_index = ARRAY_SIZE;
        while !data_array.is_null() {
            let data_array_ref = unsafe { data_array.deref_mut() };
            let preferred_index_hash = data_array_ref.partial_hash_array[preferred_index];
            if preferred_index_hash == ((partial_hash & (!REMOVED)) | OCCUPIED) {
                let entry_ptr = data_array_ref.data[preferred_index].as_ptr();
                if unsafe { &(*entry_ptr) }.0 == key {
                    return Err((key, value));
                }
            } else if free_data_array_ref.is_none() && preferred_index_hash == 0 {
                free_index = preferred_index;
            }
            for (index, hash) in data_array_ref.partial_hash_array.iter().enumerate() {
                if index == preferred_index {
                    continue;
                }
                if *hash == ((partial_hash & (!REMOVED)) | OCCUPIED) {
                    let entry_ptr = data_array_ref.data[index].as_ptr();
                    if unsafe { &(*entry_ptr) }.0 == key {
                        return Err((key, value));
                    }
                } else if free_data_array_ref.is_none() && *hash == 0 {
                    free_index = index;
                }
            }
            data_array = data_array_ref.link.load(Relaxed, guard);
            if free_data_array_ref.is_none() && free_index != ARRAY_SIZE {
                free_data_array_ref.replace(data_array_ref);
            }
        }

        // A release fence is required to make the contents fully visible to a reader having read the slot as occupied.
        if let Some(array_ref) = free_data_array_ref.take() {
            debug_assert_eq!(array_ref.partial_hash_array[free_index], 0u8);
            unsafe { array_ref.data[free_index].as_mut_ptr().write((key, value)) };
            std::sync::atomic::fence(Release);
            array_ref.partial_hash_array[free_index] = (partial_hash & (!REMOVED)) | OCCUPIED;
        } else {
            // Inserts a new DataArray at the head.
            let mut new_data_array = Owned::new(DataArray::new());
            unsafe {
                new_data_array.data[preferred_index]
                    .as_mut_ptr()
                    .write((key, value))
            };

            std::sync::atomic::fence(Release);
            new_data_array.partial_hash_array[preferred_index] =
                (partial_hash & (!REMOVED)) | OCCUPIED;
            // Relaxed is sufficient as it is unimportant to read the latest state of the partial hash value for readers.
            new_data_array.link.store(data_array_head, Relaxed);
            self.cell_ref.data.swap(new_data_array, Release, guard);
        }
        self.cell_ref.num_entries.fetch_add(1, Relaxed);
        Ok(())
    }

    /// Removes a new key-value pair associated with the given key.
    pub fn remove(&self, key: &K, partial_hash: u8, guard: &Guard) -> bool {
        if self.kill_on_drop {
            // The Cell will be killed.
            return false;
        }

        // Starts Searching the entry at the preferred index first.
        let mut data_array = self.cell_ref.data.load(Relaxed, guard);
        let mut removed = false;
        let preferred_index = partial_hash as usize % ARRAY_SIZE;
        while !data_array.is_null() {
            let data_array_ref = unsafe { data_array.deref_mut() };
            let preferred_index_hash = data_array_ref.partial_hash_array[preferred_index];
            if preferred_index_hash == ((partial_hash & (!REMOVED)) | OCCUPIED) {
                let entry_ptr = data_array_ref.data[preferred_index].as_ptr();
                if unsafe { &(*entry_ptr) }.0 == *key {
                    data_array_ref.partial_hash_array[preferred_index] |= REMOVED;
                    removed = true;
                    break;
                }
            }
            for (index, hash) in data_array_ref.partial_hash_array.iter().enumerate() {
                if index == preferred_index {
                    continue;
                }
                if *hash == ((partial_hash & (!REMOVED)) | OCCUPIED) {
                    let entry_ptr = data_array_ref.data[index].as_ptr();
                    if unsafe { &(*entry_ptr) }.0 == *key {
                        data_array_ref.partial_hash_array[index] |= REMOVED;
                        removed = true;
                        break;
                    }
                }
            }
            if removed {
                break;
            }
            data_array = data_array_ref.link.load(Relaxed, guard);
        }

        if removed {
            self.optimize(
                data_array,
                self.cell_ref.num_entries.fetch_sub(1, Relaxed) - 1,
                guard,
            );
        }
        removed
    }

    /// Kills the Cell.
    pub fn kill(&mut self) {
        self.kill_on_drop = true;
    }

    /// Purges all the data.
    pub fn purge(&mut self, guard: &Guard) -> usize {
        let data_array_shared = self.cell_ref.data.swap(Shared::null(), Relaxed, guard);
        if !data_array_shared.is_null() {
            unsafe { guard.defer_destroy(data_array_shared) };
        }
        self.cell_ref.num_entries.swap(0, Relaxed)
    }

    /// Optimizes the linked list.
    ///
    /// Two strategies.
    ///  1. Clears the entire Cell if there is no valid entry.
    ///  2. Coalesces if the given data array is non-empty and the linked list is sparse.
    ///  3. Unlinks the given data array if the data array is empty.
    fn optimize(&self, data_array: Shared<DataArray<K, V>>, num_entries: usize, guard: &Guard) {
        if num_entries == 0 {
            // Clears the entire Cell.
            let deprecated_data_array = self.cell_ref.data.swap(Shared::null(), Relaxed, guard);
            unsafe { guard.defer_destroy(deprecated_data_array) };
            return;
        }

        for hash in unsafe { data_array.deref() }.partial_hash_array.iter() {
            if (hash & REMOVED) == 0 {
                // The given data array is still valid, therefore it tries to coalesce the linked list.
                let head_data_array = self.cell_ref.data.load(Relaxed, guard);
                let head_data_array_link_shared =
                    unsafe { head_data_array.deref() }.link.load(Relaxed, guard);
                if !head_data_array_link_shared.is_null() && num_entries < ARRAY_SIZE / 4 {
                    // Replaces the head with a new DataArray.
                    let mut new_data_array = Owned::new(DataArray::new());
                    let mut new_array_index = 0;
                    let mut current_data_array = head_data_array;
                    while !current_data_array.is_null() {
                        let current_data_array_ref = unsafe { current_data_array.deref_mut() };
                        for (index, hash) in
                            current_data_array_ref.partial_hash_array.iter().enumerate()
                        {
                            if (hash & (REMOVED | OCCUPIED)) == OCCUPIED {
                                let entry_ptr = current_data_array_ref.data[index].as_ptr();
                                let entry_ref = unsafe { &(*entry_ptr) };
                                unsafe {
                                    new_data_array.data[new_array_index]
                                        .as_mut_ptr()
                                        .write(entry_ref.clone())
                                };
                                new_data_array.partial_hash_array[new_array_index] = *hash;
                                new_array_index += 1;
                            }
                        }
                        if new_array_index == num_entries {
                            break;
                        }
                        current_data_array = current_data_array_ref.link.load(Relaxed, guard);
                    }
                    let old_array_link = self.cell_ref.data.swap(new_data_array, Release, guard);
                    unsafe {
                        guard.defer_destroy(old_array_link);
                    }
                }
                return;
            }
        }

        // Unlinks the given data array from the linked list.
        let mut prev_data_array: Shared<DataArray<K, V>> = Shared::null();
        let mut current_data_array = self.cell_ref.data.load(Relaxed, guard);
        while !current_data_array.is_null() {
            let current_data_array_ref = unsafe { current_data_array.deref() };
            let next_data_array = current_data_array_ref.link.load(Relaxed, guard);
            if current_data_array == data_array {
                if prev_data_array.is_null() {
                    // Updates the head.
                    self.cell_ref.data.store(next_data_array, Relaxed);
                } else {
                    let prev_data_array_ref = unsafe { prev_data_array.deref() };
                    prev_data_array_ref.link.store(next_data_array, Relaxed);
                }
                current_data_array_ref.link.store(Shared::null(), Relaxed);
                unsafe { guard.defer_destroy(current_data_array) };
                break;
            } else {
                prev_data_array = current_data_array;
                current_data_array = next_data_array;
            }
        }
    }

    fn try_lock(cell: &'g Cell<K, V>, guard: &'g Guard) -> Option<CellLocker<'g, K, V>> {
        let current = cell.wait_queue.load(Relaxed, guard);
        if current.tag() == 0 {
            let next = current.with_tag(LOCK_TAG);
            if cell
                .wait_queue
                .compare_exchange(current, next, Acquire, Relaxed, guard)
                .is_ok()
            {
                return Some(CellLocker {
                    cell_ref: cell,
                    kill_on_drop: false,
                });
            }
        }
        None
    }
}

impl<'g, K: Clone + Eq, V: Clone> Drop for CellLocker<'g, K, V> {
    fn drop(&mut self) {
        let mut guard: Option<Guard> = None;
        if self.kill_on_drop {
            // Drops the data.
            guard.replace(crossbeam_epoch::pin());
            self.purge(guard.as_ref().unwrap());
        }

        let mut current = self
            .cell_ref
            .wait_queue
            .load(Relaxed, unsafe { crossbeam_epoch::unprotected() });
        loop {
            debug_assert!(current.tag() == LOCK_TAG);
            let wakeup = if !current.is_null() {
                // In order to prevent the Cell from being dropped while waking up other threads, pins the thread.
                if guard.is_none() {
                    guard.replace(crossbeam_epoch::pin());
                }
                true
            } else {
                false
            };
            let next = if !self.kill_on_drop {
                current.with_tag(current.tag() & (!LOCK_TAG))
            } else {
                current.with_tag((current.tag() & (!LOCK_TAG)) | KILL_TAG)
            };
            match self.cell_ref.wait_queue.compare_exchange(
                current,
                next,
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

pub struct DataArray<K: Clone + Eq, V: Clone> {
    /// The lower two-bit of a partial hash value represents the state of the corresponding entry.
    partial_hash_array: [u8; ARRAY_SIZE],
    data: [MaybeUninit<(K, V)>; ARRAY_SIZE],
    link: Atomic<DataArray<K, V>>,
}

impl<K: Clone + Eq, V: Clone> DataArray<K, V> {
    fn new() -> DataArray<K, V> {
        DataArray {
            partial_hash_array: [0; ARRAY_SIZE],
            data: unsafe { MaybeUninit::uninit().assume_init() },
            link: Atomic::null(),
        }
    }
}

impl<K: Clone + Eq, V: Clone> Drop for DataArray<K, V> {
    fn drop(&mut self) {
        for (index, hash) in self.partial_hash_array.iter().enumerate() {
            if (hash & OCCUPIED) == OCCUPIED {
                let entry_mut_ptr = self.data[index].as_mut_ptr();
                unsafe { std::ptr::drop_in_place(entry_mut_ptr) };
            }
        }
        // It has become unreachable, so has its child.
        let guard = unsafe { crossbeam_epoch::unprotected() };
        let link_shared = self.link.load(Relaxed, guard);
        if !link_shared.is_null() {
            drop(unsafe { link_shared.into_owned() });
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
    use std::convert::TryInto;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn cell_locker() {
        let num_threads = (ARRAY_SIZE * 2) as usize;
        let barrier = Arc::new(Barrier::new(num_threads));
        let cell: Arc<Cell<usize, usize>> = Arc::new(Default::default());
        let mut data: [u64; 128] = [0; 128];
        let mut thread_handles = Vec::with_capacity(num_threads);
        for thread_id in 0..num_threads {
            let barrier_copied = barrier.clone();
            let cell_copied = cell.clone();
            let data_ptr = std::sync::atomic::AtomicPtr::new(&mut data);
            thread_handles.push(thread::spawn(move || {
                barrier_copied.wait();
                let guard = crossbeam_epoch::pin();
                for i in 0..4096 {
                    let xlocker = CellLocker::lock(&*cell_copied, &guard).unwrap();
                    let mut sum: u64 = 0;
                    for j in 0..128 {
                        unsafe {
                            sum += (*data_ptr.load(Relaxed))[j];
                            (*data_ptr.load(Relaxed))[j] = if i % 4 == 0 { 2 } else { 4 }
                        };
                    }
                    assert_eq!(sum % 256, 0);
                    if i == 0 {
                        assert!(xlocker
                            .insert(
                                thread_id,
                                0,
                                (thread_id % ARRAY_SIZE).try_into().unwrap(),
                                &guard,
                            )
                            .is_ok());
                        drop(xlocker);
                    }
                    assert_eq!(
                        cell_copied
                            .search(
                                &thread_id,
                                (thread_id % ARRAY_SIZE).try_into().unwrap(),
                                &guard
                            )
                            .unwrap(),
                        &(thread_id, 0usize)
                    );
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
        assert_eq!(cell.num_entries(), num_threads);

        let guard = unsafe { crossbeam_epoch::unprotected() };
        for thread_id in 0..ARRAY_SIZE {
            assert_eq!(
                cell.search(
                    &thread_id,
                    (thread_id % ARRAY_SIZE).try_into().unwrap(),
                    guard
                ),
                Some(&(thread_id, 0))
            );
        }
        let mut iterated = 0;
        for entry in cell.iter(guard) {
            assert!(entry.0 < num_threads);
            assert_eq!(entry.1, 0);
            iterated += 1;
        }
        assert_eq!(cell.num_entries(), iterated);

        for thread_id in 0..ARRAY_SIZE {
            let xlocker = CellLocker::lock(&*cell, guard).unwrap();
            assert!(xlocker.remove(
                &thread_id,
                (thread_id % ARRAY_SIZE).try_into().unwrap(),
                guard
            ));
        }
        assert_eq!(cell.num_entries(), ARRAY_SIZE);

        let mut xlocker = CellLocker::lock(&*cell, guard).unwrap();
        xlocker.kill();
        drop(xlocker);

        assert!(cell.killed(guard));
        assert_eq!(cell.num_entries(), 0);
        assert!(CellLocker::lock(&*cell, guard).is_none());
    }
}
