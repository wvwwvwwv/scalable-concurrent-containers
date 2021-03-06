use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use std::borrow::Borrow;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::{Condvar, Mutex};

/// Flags are embedded inside a partial hash value.
const OCCUPIED: u8 = 1u8 << 6;
const REMOVED: u8 = 1u8 << 7;

/// State bits.
const KILLED: u32 = 1_u32 << 31;
const WAITING: u32 = 1_u32 << 30;
const LOCK: u32 = 1_u32 << 29;
const SLOCK_MAX: u32 = LOCK - 1;
const LOCK_MASK: u32 = LOCK | SLOCK_MAX;

/// Cell is a small fixed-size hash table that resolves hash conflicts using a linked list of entry arrays.
pub struct Cell<K: Eq, V, const SIZE: usize, const LOCK_FREE: bool> {
    /// wait_queue additionally stores the state of the Cell: locked or killed.
    wait_queue: Atomic<WaitQueueEntry>,
    /// The state of the Cell.
    state: AtomicU32,
    /// The number of valid entries in the Cell.
    num_entries: u32,
    /// DataArray stores key-value pairs with their metadata.
    data: Atomic<DataArray<K, V, SIZE>>,
}

impl<K: Eq, V, const SIZE: usize, const LOCK_FREE: bool> Default for Cell<K, V, SIZE, LOCK_FREE> {
    fn default() -> Self {
        Cell::<K, V, SIZE, LOCK_FREE> {
            wait_queue: Atomic::null(),
            state: AtomicU32::new(0),
            num_entries: 0,
            data: Atomic::null(),
        }
    }
}

impl<K: Eq, V, const SIZE: usize, const LOCK_FREE: bool> Cell<K, V, SIZE, LOCK_FREE> {
    /// Returns true if the Cell has been killed.
    pub fn killed(&self) -> bool {
        (self.state.load(Relaxed) & KILLED) == KILLED
    }

    /// Returns the number of entries in the Cell.
    pub fn num_entries(&self) -> usize {
        self.num_entries as usize
    }

    /// Iterates the contents of the Cell.
    pub fn iter<'g>(&'g self, guard: &'g Guard) -> CellIterator<'g, K, V, SIZE, LOCK_FREE> {
        CellIterator::new(self, guard)
    }

    /// Searches for an entry associated with the given key.
    pub fn search<'g, Q>(&self, key: &Q, partial_hash: u8, guard: &'g Guard) -> Option<&'g (K, V)>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        if self.num_entries == 0 {
            return None;
        }

        // In order to read the linked list correctly, an acquire fence is required.
        let read_order = if LOCK_FREE { Acquire } else { Relaxed };
        let mut data_array = self.data.load(read_order, guard);
        let preferred_index = partial_hash as usize % SIZE;
        let expected_hash = (partial_hash & (!REMOVED)) | OCCUPIED;
        while !data_array.is_null() {
            let data_array_ref = unsafe { data_array.deref() };
            for i in preferred_index..preferred_index + SIZE {
                let index = i % SIZE;
                if data_array_ref.partial_hash_array[index] == expected_hash {
                    let entry_ptr = data_array_ref.data[index].as_ptr();
                    if LOCK_FREE {
                        std::sync::atomic::fence(Acquire);
                    }
                    if *unsafe { &(*entry_ptr) }.0.borrow() == *key {
                        return Some(unsafe { &(*entry_ptr) });
                    }
                }
            }
            data_array = data_array_ref.link.load(read_order, guard);
        }
        None
    }

    /// Gets a CellIterator pointing to an entry associated with the given key.
    pub fn get<'g, Q>(
        &'g self,
        key: &Q,
        partial_hash: u8,
        guard: &'g Guard,
    ) -> Option<CellIterator<'g, K, V, SIZE, LOCK_FREE>>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        if self.num_entries == 0 {
            return None;
        }

        // In order to read the linked list correctly, an acquire fence is required.
        let read_order = if LOCK_FREE { Acquire } else { Relaxed };
        let mut data_array = self.data.load(read_order, guard);
        let preferred_index = partial_hash as usize % SIZE;
        let expected_hash = (partial_hash & (!REMOVED)) | OCCUPIED;
        while !data_array.is_null() {
            let data_array_ref = unsafe { data_array.deref() };
            for i in preferred_index..preferred_index + SIZE {
                let index = i % SIZE;
                if data_array_ref.partial_hash_array[index] == expected_hash {
                    let entry_ptr = data_array_ref.data[index].as_ptr();
                    if LOCK_FREE {
                        std::sync::atomic::fence(Acquire);
                    }
                    if *unsafe { &(*entry_ptr) }.0.borrow() == *key {
                        return Some(CellIterator {
                            cell_ref: Some(self),
                            current_array: data_array,
                            current_index: index,
                            guard_ref: guard,
                        });
                    }
                }
            }
            data_array = data_array_ref.link.load(read_order, guard);
        }
        None
    }

    /// Waits for the owner thread to release the Cell.
    fn wait<T, F: FnOnce() -> Option<T>>(&self, f: F, guard: &Guard) -> Option<T> {
        // Inserts the condvar into the wait queue.
        let mut current = self.wait_queue.load(Relaxed, guard);
        let mut condvar = WaitQueueEntry::new(Atomic::from(current));

        let mut next = Shared::from(&condvar as *const _);
        while let Err(result) = self
            .wait_queue
            .compare_exchange(current, next, Release, Relaxed, guard)
        {
            current = result.current;
            next = Shared::from(&condvar as *const _);
            condvar.next = Atomic::from(result.current);
        }

        // Marks that there is a waiting thread.
        self.state.fetch_or(WAITING, Relaxed);

        // Tries to lock again once the condvar is inserted into the wait queue.
        let locked = f();
        if locked.is_some() {
            self.wakeup(guard);
        }

        // Locking failed.
        condvar.wait();
        locked
    }

    /// Wakes up the threads in the wait queue.
    fn wakeup(&self, guard: &Guard) {
        let mut current = self.wait_queue.load(Acquire, guard);
        let mut next = Shared::null();
        while let Err(result) = self
            .wait_queue
            .compare_exchange(current, next, Acquire, Relaxed, guard)
        {
            current = result.current;
            if current.is_null() {
                return;
            }
            next = Shared::null();
        }

        while !current.is_null() {
            let cond_var_ref = unsafe { current.deref() };
            let next_ptr = cond_var_ref.next.load(Acquire, guard);
            cond_var_ref.signal();
            current = next_ptr;
        }
    }

    /// Returns the max resizing factor.
    pub fn max_resizing_factor() -> usize {
        (SIZE.next_power_of_two().trailing_zeros() + 1) as usize
    }
}

impl<K: Eq, V, const SIZE: usize, const LOCK_FREE: bool> Drop for Cell<K, V, SIZE, LOCK_FREE> {
    fn drop(&mut self) {
        // The Cell must have been killed.
        debug_assert!(self.killed());
    }
}

pub struct CellIterator<'g, K: Eq, V, const SIZE: usize, const LOCK_FREE: bool> {
    cell_ref: Option<&'g Cell<K, V, SIZE, LOCK_FREE>>,
    current_array: Shared<'g, DataArray<K, V, SIZE>>,
    current_index: usize,
    guard_ref: &'g Guard,
}

impl<'g, K: Eq, V, const SIZE: usize, const LOCK_FREE: bool>
    CellIterator<'g, K, V, SIZE, LOCK_FREE>
{
    pub fn new(
        cell: &'g Cell<K, V, SIZE, LOCK_FREE>,
        guard: &'g Guard,
    ) -> CellIterator<'g, K, V, SIZE, LOCK_FREE> {
        CellIterator {
            cell_ref: Some(cell),
            current_array: Shared::null(),
            current_index: usize::MAX,
            guard_ref: guard,
        }
    }

    pub fn get(&self) -> Option<&'g (K, V)> {
        if self.current_array.is_null() {
            None
        } else {
            let data_array_ref = unsafe { self.current_array.deref() };
            let entry_ptr = data_array_ref.data[self.current_index].as_ptr();
            return Some(unsafe { &(*entry_ptr) });
        }
    }
}

impl<'g, K: Eq, V, const SIZE: usize, const LOCK_FREE: bool> Iterator
    for CellIterator<'g, K, V, SIZE, LOCK_FREE>
{
    type Item = (&'g (K, V), u8);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(&cell_ref) = self.cell_ref.as_ref() {
            let read_order = if LOCK_FREE { Acquire } else { Relaxed };
            if self.current_array.is_null() {
                // Starts scanning from the beginning.
                self.current_array = cell_ref.data.load(read_order, self.guard_ref);
            }
            while !self.current_array.is_null() {
                // Search for the next valid entry.
                let array_ref = unsafe { self.current_array.deref() };
                let start_index = if self.current_index == usize::MAX {
                    0
                } else {
                    self.current_index + 1
                };
                for index in start_index..SIZE {
                    let hash = array_ref.partial_hash_array[index];
                    if (hash & OCCUPIED) != 0 && (hash & REMOVED) == 0 {
                        if LOCK_FREE {
                            std::sync::atomic::fence(Acquire);
                        }
                        self.current_index = index;
                        let entry_ptr = array_ref.data[index].as_ptr();
                        return Some((unsafe { &(*entry_ptr) }, hash));
                    }
                }

                // Proceeds to the next DataArray.
                self.current_array = array_ref.link.load(read_order, self.guard_ref);
                self.current_index = usize::MAX;
            }
            // Fuses itself.
            self.cell_ref.take();
        }
        None
    }
}

pub struct CellLocker<'g, K: Eq, V, const SIZE: usize, const LOCK_FREE: bool> {
    cell_ref: &'g Cell<K, V, SIZE, LOCK_FREE>,
    killed: bool,
}

impl<'g, K: Eq, V, const SIZE: usize, const LOCK_FREE: bool> CellLocker<'g, K, V, SIZE, LOCK_FREE> {
    /// Locks the given Cell.
    pub fn lock(
        cell: &'g Cell<K, V, SIZE, LOCK_FREE>,
        guard: &'g Guard,
    ) -> Option<CellLocker<'g, K, V, SIZE, LOCK_FREE>> {
        loop {
            for _ in 0..(SIZE * 4) {
                if let Some(locker) = Self::try_lock(cell, guard) {
                    if locker.killed {
                        return None;
                    }
                    return Some(locker);
                }
            }
            if let Some(locker) = cell.wait(|| Self::try_lock(cell, guard), guard) {
                if locker.killed {
                    return None;
                }
                return Some(locker);
            }
            if cell.killed() {
                return None;
            }
        }
    }

    /// Tries to lock the Cell.
    fn try_lock(
        cell: &'g Cell<K, V, SIZE, LOCK_FREE>,
        _guard: &'g Guard,
    ) -> Option<CellLocker<'g, K, V, SIZE, LOCK_FREE>> {
        let current = cell.state.load(Relaxed);
        if (current & LOCK_MASK) != 0 {
            return None;
        }
        if cell
            .state
            .compare_exchange(current, current | LOCK, Acquire, Relaxed)
            .is_ok()
        {
            return Some(CellLocker {
                cell_ref: cell,
                killed: (current & KILLED) == KILLED,
            });
        }
        None
    }

    /// Returns a reference to the Cell.
    pub fn cell_ref(&self) -> &Cell<K, V, SIZE, LOCK_FREE> {
        self.cell_ref
    }

    /// Inserts a new key-value pair into the Cell.
    pub fn insert(
        &'g self,
        key: K,
        value: V,
        partial_hash: u8,
        guard: &'g Guard,
    ) -> (CellIterator<'g, K, V, SIZE, LOCK_FREE>, Option<(K, V)>) {
        debug_assert!(!self.killed);

        if self.cell_ref.num_entries == u32::MAX {
            panic!("Entries overflow");
        }

        let cell_mut_ref =
            unsafe { &mut *(self.cell_ref as *const _ as *mut Cell<K, V, SIZE, LOCK_FREE>) };
        let mut data_array = self.cell_ref.data.load(Relaxed, guard);
        let data_array_head = data_array;
        let preferred_index = partial_hash as usize % SIZE;
        let expected_hash = (partial_hash & (!REMOVED)) | OCCUPIED;
        let mut free_data_array: Option<Shared<DataArray<K, V, SIZE>>> = None;
        let mut free_index = SIZE;
        while !data_array.is_null() {
            let data_array_ref = unsafe { data_array.deref_mut() };
            for i in preferred_index..preferred_index + SIZE {
                let index = i % SIZE;
                let hash = data_array_ref.partial_hash_array[index];
                if hash == expected_hash {
                    let entry_ptr = data_array_ref.data[index].as_ptr();
                    if unsafe { &(*entry_ptr) }.0 == key {
                        return (
                            CellIterator {
                                cell_ref: Some(self.cell_ref),
                                current_array: data_array,
                                current_index: index,
                                guard_ref: guard,
                            },
                            Some((key, value)),
                        );
                    }
                } else if free_data_array.is_none() && hash == 0 {
                    free_index = index;
                }
            }
            if free_data_array.is_none() && free_index != SIZE {
                free_data_array.replace(data_array);
            }
            data_array = data_array_ref.link.load(Relaxed, guard);
        }

        if let Some(mut free_data_array_shared) = free_data_array.take() {
            let data_array_ref = unsafe { free_data_array_shared.deref_mut() };
            debug_assert_eq!(data_array_ref.partial_hash_array[free_index], 0u8);
            unsafe {
                data_array_ref.data[free_index]
                    .as_mut_ptr()
                    .write((key, value))
            };
            if LOCK_FREE {
                // A release fence is required to make the contents fully visible to a reader having read the slot as occupied.
                std::sync::atomic::fence(Release);
            }
            data_array_ref.partial_hash_array[free_index] = expected_hash;
            cell_mut_ref.num_entries += 1;
            return (
                CellIterator {
                    cell_ref: Some(self.cell_ref),
                    current_array: free_data_array_shared,
                    current_index: free_index,
                    guard_ref: guard,
                },
                None,
            );
        } else {
            // Inserts a new DataArray at the head.
            let mut new_data_array = Owned::new(DataArray::new());
            unsafe {
                new_data_array.data[preferred_index]
                    .as_mut_ptr()
                    .write((key, value))
            };
            if LOCK_FREE {
                // A release fence is required to make the contents fully visible to a reader having read the slot as occupied.
                std::sync::atomic::fence(Release);
            }
            new_data_array.partial_hash_array[preferred_index] = expected_hash;
            // Relaxed is sufficient as it is unimportant to read the latest state of the partial hash value for readers.
            new_data_array.link.store(data_array_head, Relaxed);
            let write_order = if LOCK_FREE { Release } else { Relaxed };
            self.cell_ref.data.swap(new_data_array, write_order, guard);
            cell_mut_ref.num_entries += 1;
            return (
                CellIterator {
                    cell_ref: Some(self.cell_ref),
                    current_array: self.cell_ref.data.load(Relaxed, guard),
                    current_index: preferred_index,
                    guard_ref: guard,
                },
                None,
            );
        }
    }

    /// Removes a new key-value pair being pointed by the given CellIterator.
    pub fn erase(&self, iterator: &mut CellIterator<K, V, SIZE, LOCK_FREE>) -> Option<(K, V)> {
        if self.killed {
            // The Cell has been killed.
            return None;
        }

        if iterator.current_array.is_null() || iterator.current_index == usize::MAX {
            // The iterator is fused.
            return None;
        }

        let data_array_ref = unsafe { iterator.current_array.deref_mut() };
        let hash = data_array_ref.partial_hash_array[iterator.current_index];
        if (hash & OCCUPIED) == 0 {
            // The entry has been dropped, or never been used.
            return None;
        }

        debug_assert!(self.cell_ref.num_entries > 0);
        let cell_mut_ref =
            unsafe { &mut *(self.cell_ref as *const _ as *mut Cell<K, V, SIZE, LOCK_FREE>) };
        cell_mut_ref.num_entries -= 1;
        let entry_ptr = data_array_ref.data[iterator.current_index].as_ptr();
        if LOCK_FREE {
            data_array_ref.partial_hash_array[iterator.current_index] |= REMOVED;
            None
        } else {
            data_array_ref.partial_hash_array[iterator.current_index] = 0;
            let entry_mut_ptr = entry_ptr as *mut MaybeUninit<(K, V)>;
            Some(unsafe { std::ptr::replace(entry_mut_ptr, MaybeUninit::uninit()).assume_init() })
        }
    }

    /// Purges all the data.
    pub fn purge(&mut self, guard: &Guard) -> usize {
        let data_array_shared = self.cell_ref.data.swap(Shared::null(), Relaxed, guard);
        if !data_array_shared.is_null() {
            if LOCK_FREE {
                unsafe { guard.defer_destroy(data_array_shared) };
            } else {
                drop(unsafe { data_array_shared.into_owned() });
            }
        }
        self.killed = true;

        let num_entries = self.cell_ref.num_entries;
        let cell_mut_ref =
            unsafe { &mut *(self.cell_ref as *const _ as *mut Cell<K, V, SIZE, LOCK_FREE>) };
        cell_mut_ref.num_entries = 0;
        num_entries as usize
    }
}

impl<'g, K: Clone + Eq, V: Clone, const SIZE: usize, const LOCK_FREE: bool>
    CellLocker<'g, K, V, SIZE, LOCK_FREE>
{
    /// Removes a new key-value pair associated with the given key with the instances kept intact.
    pub fn mark_removed<Q>(&self, key: &Q, partial_hash: u8, guard: &Guard) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        if self.killed {
            // The Cell has been killed.
            return false;
        }

        // Starts Searching the entry at the preferred index first.
        let mut data_array = self.cell_ref.data.load(Relaxed, guard);
        let mut removed = false;
        let preferred_index = partial_hash as usize % SIZE;
        let expected_hash = (partial_hash & (!REMOVED)) | OCCUPIED;
        while !data_array.is_null() {
            let data_array_ref = unsafe { data_array.deref_mut() };
            for i in preferred_index..preferred_index + SIZE {
                let index = i % SIZE;
                if data_array_ref.partial_hash_array[index] == expected_hash {
                    let entry_ptr = data_array_ref.data[index].as_ptr();
                    if *unsafe { &(*entry_ptr) }.0.borrow() == *key {
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
            let cell_mut_ref =
                unsafe { &mut *(self.cell_ref as *const _ as *mut Cell<K, V, SIZE, LOCK_FREE>) };
            cell_mut_ref.num_entries -= 1;
            self.optimize(data_array, self.cell_ref.num_entries, guard);
        }
        removed
    }

    /// Optimizes the linked list.
    ///
    /// Two strategies.
    ///  1. Clears the entire Cell if there is no valid entry.
    ///  2. Coalesces if the given data array is non-empty and the linked list is sparse.
    ///  3. Unlinks the given data array if the data array is empty.
    fn optimize(&self, data_array: Shared<DataArray<K, V, SIZE>>, num_entries: u32, guard: &Guard) {
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
                if !head_data_array_link_shared.is_null() && (num_entries as usize) < SIZE / 4 {
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
                        if new_array_index == num_entries as usize {
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
        let mut prev_data_array: Shared<DataArray<K, V, SIZE>> = Shared::null();
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
}

impl<'g, K: Eq, V, const SIZE: usize, const LOCK_FREE: bool> Drop
    for CellLocker<'g, K, V, SIZE, LOCK_FREE>
{
    fn drop(&mut self) {
        let mut guard: Option<Guard> = None;
        let mut current = self.cell_ref.state.load(Relaxed);
        loop {
            let wakeup = if (current & WAITING) == WAITING {
                // In order to prevent the Cell from being dropped while waking up other threads, pins the thread.
                if guard.is_none() {
                    guard.replace(crossbeam_epoch::pin());
                }
                true
            } else {
                false
            };
            let next = if !self.killed {
                current & (!(WAITING | LOCK))
            } else {
                KILLED | (current & (!(WAITING | LOCK)))
            };
            match self
                .cell_ref
                .state
                .compare_exchange(current, next, Release, Relaxed)
            {
                Ok(_) => {
                    if wakeup {
                        self.cell_ref.wakeup(guard.as_ref().unwrap());
                    }
                    break;
                }
                Err(result) => current = result,
            }
        }
    }
}

pub struct CellReader<'g, K: Eq, V, const SIZE: usize, const LOCK_FREE: bool> {
    cell_ref: &'g Cell<K, V, SIZE, LOCK_FREE>,
    killed: bool,
}

impl<'g, K: Eq, V, const SIZE: usize, const LOCK_FREE: bool> CellReader<'g, K, V, SIZE, LOCK_FREE> {
    /// Locks the given Cell.
    pub fn lock(
        cell: &'g Cell<K, V, SIZE, LOCK_FREE>,
        guard: &'g Guard,
    ) -> Option<CellReader<'g, K, V, SIZE, LOCK_FREE>> {
        loop {
            for _ in 0..(SIZE * 4) {
                if let Some(reader) = Self::try_lock(cell, guard) {
                    if reader.killed {
                        return None;
                    }
                    return Some(reader);
                }
            }
            if let Some(reader) = cell.wait(|| Self::try_lock(cell, guard), guard) {
                if reader.killed {
                    return None;
                }
                return Some(reader);
            }
            if cell.killed() {
                return None;
            }
        }
    }

    /// Tries to lock the Cell.
    fn try_lock(
        cell: &'g Cell<K, V, SIZE, LOCK_FREE>,
        _guard: &'g Guard,
    ) -> Option<CellReader<'g, K, V, SIZE, LOCK_FREE>> {
        let current = cell.state.load(Relaxed);
        if (current & LOCK_MASK) >= SLOCK_MAX {
            return None;
        }
        if cell
            .state
            .compare_exchange(current, current + 1, Acquire, Relaxed)
            .is_ok()
        {
            return Some(CellReader {
                cell_ref: cell,
                killed: (current & KILLED) == KILLED,
            });
        }
        None
    }

    /// Returns a reference to the Cell.
    pub fn cell_ref(&self) -> &Cell<K, V, SIZE, LOCK_FREE> {
        self.cell_ref
    }
}

impl<'g, K: Eq, V, const SIZE: usize, const LOCK_FREE: bool> Drop
    for CellReader<'g, K, V, SIZE, LOCK_FREE>
{
    fn drop(&mut self) {
        let mut guard: Option<Guard> = None;
        let mut current = self.cell_ref.state.load(Relaxed);
        loop {
            let wakeup = if (current & WAITING) == WAITING {
                // In order to prevent the Cell from being dropped while waking up other threads, pins the thread.
                if guard.is_none() {
                    guard.replace(crossbeam_epoch::pin());
                }
                true
            } else {
                false
            };
            let next = (current - 1) & !(WAITING);
            match self
                .cell_ref
                .state
                .compare_exchange(current, next, Relaxed, Relaxed)
            {
                Ok(_) => {
                    if wakeup {
                        self.cell_ref.wakeup(guard.as_ref().unwrap());
                    }
                    break;
                }
                Err(result) => current = result,
            }
        }
    }
}

pub struct DataArray<K: Eq, V, const SIZE: usize> {
    /// The lower two-bit of a partial hash value represents the state of the corresponding entry.
    partial_hash_array: [u8; SIZE],
    data: [MaybeUninit<(K, V)>; SIZE],
    link: Atomic<DataArray<K, V, SIZE>>,
}

impl<K: Eq, V, const SIZE: usize> DataArray<K, V, SIZE> {
    fn new() -> DataArray<K, V, SIZE> {
        DataArray {
            partial_hash_array: [0; SIZE],
            data: unsafe { MaybeUninit::uninit().assume_init() },
            link: Atomic::null(),
        }
    }
}

impl<K: Eq, V, const SIZE: usize> Drop for DataArray<K, V, SIZE> {
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
        const SIZE: usize = 32;
        let num_threads = (SIZE * 2) as usize;
        let barrier = Arc::new(Barrier::new(num_threads));
        let cell: Arc<Cell<usize, usize, SIZE, true>> = Arc::new(Default::default());
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
                            .insert(thread_id, 0, (thread_id % SIZE).try_into().unwrap(), &guard,)
                            .1
                            .is_none());
                    } else {
                        assert_eq!(
                            xlocker
                                .cell_ref()
                                .search(&thread_id, (thread_id % SIZE).try_into().unwrap(), &guard)
                                .unwrap(),
                            &(thread_id, 0usize)
                        );
                    }
                    drop(xlocker);

                    let slocker = CellReader::lock(&*cell_copied, &guard).unwrap();
                    assert_eq!(
                        slocker
                            .cell_ref()
                            .search(&thread_id, (thread_id % SIZE).try_into().unwrap(), &guard)
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
        for thread_id in 0..SIZE {
            assert_eq!(
                cell.search(&thread_id, (thread_id % SIZE).try_into().unwrap(), guard),
                Some(&(thread_id, 0))
            );
        }
        let mut iterated = 0;
        for entry in cell.iter(guard) {
            assert!(entry.0 .0 < num_threads);
            assert_eq!(entry.0 .1, 0);
            iterated += 1;
        }
        assert_eq!(cell.num_entries(), iterated);

        for thread_id in 0..SIZE {
            let xlocker = CellLocker::lock(&*cell, guard).unwrap();
            assert!(xlocker.mark_removed(
                &thread_id,
                (thread_id % SIZE).try_into().unwrap(),
                guard
            ));
        }
        assert_eq!(cell.num_entries(), SIZE);

        let mut xlocker = CellLocker::lock(&*cell, guard).unwrap();
        xlocker.purge(&guard);
        drop(xlocker);

        assert!(cell.killed());
        assert_eq!(cell.num_entries(), 0);
        assert!(CellLocker::lock(&*cell, guard).is_none());
    }
}
