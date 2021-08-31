use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};

use std::borrow::Borrow;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicPtr, AtomicU32};
use std::sync::{Condvar, Mutex};

/// Flags are embedded inside a partial hash value.
const OCCUPIED: u8 = 1_u8 << 6;
const REMOVED: u8 = 1_u8 << 7;

/// State bits.
const KILLED: u32 = 1_u32 << 31;
const WAITING: u32 = 1_u32 << 30;
const LOCK: u32 = 1_u32 << 29;
const SLOCK_MAX: u32 = LOCK - 1;
const LOCK_MASK: u32 = LOCK | SLOCK_MAX;

/// [`Cell`] is a small fixed-size hash table that resolves hash conflicts using a linked list
/// of entry arrays.
pub struct Cell<K: 'static + Eq, V: 'static, const SIZE: usize, const LOCK_FREE: bool> {
    /// `wait_queue` additionally stores the state of the [`Cell`]: locked or killed.
    wait_queue: AtomicPtr<WaitQueueEntry>,
    /// The state of the [`Cell`].
    state: AtomicU32,
    /// The number of valid entries in the [`Cell`].
    num_entries: u32,
    /// DataArray stores key-value pairs with their metadata.
    data: AtomicArc<DataArray<K, V, SIZE>>,
}

impl<K: 'static + Eq, V: 'static, const SIZE: usize, const LOCK_FREE: bool> Default
    for Cell<K, V, SIZE, LOCK_FREE>
{
    fn default() -> Self {
        Cell::<K, V, SIZE, LOCK_FREE> {
            wait_queue: AtomicPtr::default(),
            state: AtomicU32::new(0),
            num_entries: 0,
            data: AtomicArc::null(),
        }
    }
}

impl<K: 'static + Eq, V: 'static, const SIZE: usize, const LOCK_FREE: bool>
    Cell<K, V, SIZE, LOCK_FREE>
{
    /// Returns true if the [`Cell`] has been killed.
    pub fn killed(&self) -> bool {
        (self.state.load(Relaxed) & KILLED) == KILLED
    }

    /// Returns the number of entries in the [`Cell`].
    pub fn num_entries(&self) -> usize {
        self.num_entries as usize
    }

    /// Iterates the contents of the [`Cell`].
    pub fn iter<'b>(&'b self, guard: &'b Barrier) -> CellIterator<'b, K, V, SIZE, LOCK_FREE> {
        CellIterator::new(self, guard)
    }

    /// Searches for an entry associated with the given key.
    pub fn search<'b, Q>(
        &self,
        key: &Q,
        partial_hash: u8,
        barrier: &'b Barrier,
    ) -> Option<&'b (K, V)>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        if self.num_entries == 0 {
            return None;
        }

        // In order to read the linked list correctly, an acquire fence is required.
        let read_order = if LOCK_FREE { Acquire } else { Relaxed };
        let mut data_array_ptr = self.data.load(read_order, barrier);
        let preferred_index = partial_hash as usize % SIZE;
        let expected_hash = (partial_hash & (!REMOVED)) | OCCUPIED;
        while let Some(data_array_ref) = data_array_ptr.as_ref() {
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
            data_array_ptr = data_array_ref.link.load(read_order, barrier);
        }
        None
    }

    /// Gets a [`CellIterator`] pointing to an entry associated with the given key.
    pub fn get<'b, Q>(
        &'b self,
        key: &Q,
        partial_hash: u8,
        barrier: &'b Barrier,
    ) -> Option<CellIterator<'b, K, V, SIZE, LOCK_FREE>>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        if self.num_entries == 0 {
            return None;
        }

        // In order to read the linked list correctly, a load-acquire is required.
        let read_order = if LOCK_FREE { Acquire } else { Relaxed };
        let mut data_array_ptr = self.data.load(read_order, barrier);
        let preferred_index = partial_hash as usize % SIZE;
        let expected_hash = (partial_hash & (!REMOVED)) | OCCUPIED;
        while let Some(data_array_ref) = data_array_ptr.as_ref() {
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
                            current_array_ptr: data_array_ptr,
                            current_index: index,
                            barrier_ref: barrier,
                        });
                    }
                }
            }
            data_array_ptr = data_array_ref.link.load(read_order, barrier);
        }
        None
    }

    /// Waits for the owner thread to release the [`Cell`].
    fn wait<T, F: FnOnce() -> Option<T>>(&self, f: F) -> Option<T> {
        // Inserts the condvar into the wait queue.
        let mut current = self.wait_queue.load(Relaxed);
        let mut entry = WaitQueueEntry::new(current);

        while let Err(actual) = self.wait_queue.compare_exchange(
            current,
            &mut entry as *mut WaitQueueEntry,
            Release,
            Relaxed,
        ) {
            current = actual;
            entry.next_ptr = current;
        }

        // Marks that there is a waiting thread.
        self.state.fetch_or(WAITING, Relaxed);

        // Tries to lock again once the entry is inserted into the wait queue.
        let locked = f();
        if locked.is_some() {
            self.wakeup();
        }

        // Locking failed.
        entry.wait();
        locked
    }

    /// Wakes up the threads in the wait queue.
    fn wakeup(&self) {
        let mut current = self.wait_queue.load(Acquire);
        while let Err(actual) =
            self.wait_queue
                .compare_exchange(current, ptr::null_mut(), Acquire, Relaxed)
        {
            if actual.is_null() {
                return;
            }
            current = actual;
        }

        while let Some(entry_ref) = unsafe { current.as_ref() } {
            let next_ptr = entry_ref.next_ptr;
            entry_ref.signal();
            current = next_ptr as *mut WaitQueueEntry;
        }
    }

    /// Returns the max resizing factor.
    pub fn max_resizing_factor() -> usize {
        (SIZE.next_power_of_two().trailing_zeros() + 1) as usize
    }
}

impl<K: 'static + Eq, V: 'static, const SIZE: usize, const LOCK_FREE: bool> Drop
    for Cell<K, V, SIZE, LOCK_FREE>
{
    fn drop(&mut self) {
        // The Cell must have been killed.
        debug_assert!(self.killed());
    }
}

pub struct CellIterator<'b, K: 'static + Eq, V: 'static, const SIZE: usize, const LOCK_FREE: bool> {
    cell_ref: Option<&'b Cell<K, V, SIZE, LOCK_FREE>>,
    current_array_ptr: Ptr<'b, DataArray<K, V, SIZE>>,
    current_index: usize,
    barrier_ref: &'b Barrier,
}

impl<'b, K: 'static + Eq, V: 'static, const SIZE: usize, const LOCK_FREE: bool>
    CellIterator<'b, K, V, SIZE, LOCK_FREE>
{
    pub fn new(
        cell: &'b Cell<K, V, SIZE, LOCK_FREE>,
        barrier: &'b Barrier,
    ) -> CellIterator<'b, K, V, SIZE, LOCK_FREE> {
        CellIterator {
            cell_ref: Some(cell),
            current_array_ptr: Ptr::null(),
            current_index: usize::MAX,
            barrier_ref: barrier,
        }
    }

    pub fn get(&self) -> Option<&'b (K, V)> {
        if let Some(data_array_ref) = self.current_array_ptr.as_ref() {
            let entry_ptr = data_array_ref.data[self.current_index].as_ptr();
            return Some(unsafe { &(*entry_ptr) });
        }
        None
    }
}

impl<'b, K: 'static + Eq, V: 'static, const SIZE: usize, const LOCK_FREE: bool> Iterator
    for CellIterator<'b, K, V, SIZE, LOCK_FREE>
{
    type Item = (&'b (K, V), u8);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(&cell_ref) = self.cell_ref.as_ref() {
            let read_order = if LOCK_FREE { Acquire } else { Relaxed };
            if self.current_array_ptr.is_null() {
                // Starts scanning from the beginning.
                self.current_array_ptr = cell_ref.data.load(read_order, self.barrier_ref);
            }
            while let Some(array_ref) = self.current_array_ptr.as_ref() {
                // Searches for the next valid entry.
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
                self.current_array_ptr = array_ref.link.load(read_order, self.barrier_ref);
                self.current_index = usize::MAX;
            }
            // Fuses itself.
            self.cell_ref.take();
        }
        None
    }
}

pub struct CellLocker<'b, K: 'static + Eq, V: 'static, const SIZE: usize, const LOCK_FREE: bool> {
    cell_ref: &'b Cell<K, V, SIZE, LOCK_FREE>,
    killed: bool,
}

impl<'b, K: Eq, V, const SIZE: usize, const LOCK_FREE: bool> CellLocker<'b, K, V, SIZE, LOCK_FREE> {
    /// Locks the given [`Cell`].
    pub fn lock(
        cell: &'b Cell<K, V, SIZE, LOCK_FREE>,
        barrier: &'b Barrier,
    ) -> Option<CellLocker<'b, K, V, SIZE, LOCK_FREE>> {
        loop {
            for _ in 0..SIZE {
                if let Some(locker) = Self::try_lock(cell, barrier) {
                    if locker.killed {
                        return None;
                    }
                    return Some(locker);
                }
            }
            if let Some(locker) = cell.wait(|| Self::try_lock(cell, barrier)) {
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

    /// Tries to lock the [`Cell`].
    fn try_lock(
        cell: &'b Cell<K, V, SIZE, LOCK_FREE>,
        _barrier: &'b Barrier,
    ) -> Option<CellLocker<'b, K, V, SIZE, LOCK_FREE>> {
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

    /// Returns a reference to the [`Cell`].
    pub fn cell_ref(&self) -> &'b Cell<K, V, SIZE, LOCK_FREE> {
        self.cell_ref
    }

    /// Inserts a new key-value pair into the [`Cell`].
    pub fn insert(
        &'b self,
        key: K,
        value: V,
        partial_hash: u8,
        barrier: &'b Barrier,
    ) -> (CellIterator<'b, K, V, SIZE, LOCK_FREE>, Option<(K, V)>) {
        debug_assert!(!self.killed);

        if self.cell_ref.num_entries == u32::MAX {
            panic!("Entries overflow");
        }

        let cell_mut_ref =
            unsafe { &mut *(self.cell_ref as *const _ as *mut Cell<K, V, SIZE, LOCK_FREE>) };
        let mut data_array_ptr = self.cell_ref.data.load(Relaxed, barrier);
        let data_array_head_ptr = data_array_ptr.clone();
        let preferred_index = partial_hash as usize % SIZE;
        let expected_hash = (partial_hash & (!REMOVED)) | OCCUPIED;
        let mut free_data_array: Option<Ptr<DataArray<K, V, SIZE>>> = None;
        let mut free_index = SIZE;
        while let Some(data_array_ref) = data_array_ptr.as_ref() {
            for i in preferred_index..preferred_index + SIZE {
                let index = i % SIZE;
                let hash = data_array_ref.partial_hash_array[index];
                if hash == expected_hash {
                    let entry_ptr = data_array_ref.data[index].as_ptr();
                    if unsafe { &(*entry_ptr) }.0 == key {
                        return (
                            CellIterator {
                                cell_ref: Some(self.cell_ref),
                                current_array_ptr: data_array_ptr,
                                current_index: index,
                                barrier_ref: barrier,
                            },
                            Some((key, value)),
                        );
                    }
                } else if free_data_array.is_none() && hash == 0 {
                    free_index = index;
                }
            }
            if free_data_array.is_none() && free_index != SIZE {
                free_data_array.replace(data_array_ptr);
            }
            data_array_ptr = data_array_ref.link.load(Relaxed, barrier);
        }

        if let Some(free_data_array_ptr) = free_data_array.take() {
            let data_array_ref = free_data_array_ptr.as_ref().unwrap();
            debug_assert_eq!(data_array_ref.partial_hash_array[free_index], 0u8);
            unsafe {
                let data_array_mut_ref = &mut *(data_array_ref as *const DataArray<K, V, SIZE>
                    as *mut DataArray<K, V, SIZE>);
                data_array_mut_ref.data[free_index]
                    .as_mut_ptr()
                    .write((key, value));
                if LOCK_FREE {
                    // A release fence is required to make the contents fully visible to a reader
                    // having found the slot occupied.
                    std::sync::atomic::fence(Release);
                }
                data_array_mut_ref.partial_hash_array[free_index] = expected_hash;
            };

            cell_mut_ref.num_entries += 1;
            return (
                CellIterator {
                    cell_ref: Some(self.cell_ref),
                    current_array_ptr: free_data_array_ptr,
                    current_index: free_index,
                    barrier_ref: barrier,
                },
                None,
            );
        } else {
            // Inserts a new DataArray at the head.
            let mut new_data_array = Arc::new(DataArray::new());
            unsafe {
                new_data_array.get_mut().unwrap().data[preferred_index]
                    .as_mut_ptr()
                    .write((key, value))
            };
            if LOCK_FREE {
                // A release fence is required to make the contents fully visible to a reader
                // having found the slot occupied.
                std::sync::atomic::fence(Release);
            }
            new_data_array.get_mut().unwrap().partial_hash_array[preferred_index] = expected_hash;

            // Relaxed is sufficient as it is unimportant to read the latest state of the
            // partial hash value for readers.
            new_data_array
                .link
                .swap((data_array_head_ptr.try_into_arc(), Tag::None), Relaxed);
            let write_order = if LOCK_FREE { Release } else { Relaxed };
            self.cell_ref
                .data
                .swap((Some(new_data_array), Tag::None), write_order);
            cell_mut_ref.num_entries += 1;
            return (
                CellIterator {
                    cell_ref: Some(self.cell_ref),
                    current_array_ptr: self.cell_ref.data.load(Relaxed, barrier),
                    current_index: preferred_index,
                    barrier_ref: barrier,
                },
                None,
            );
        }
    }

    /// Removes a new key-value pair being pointed by the given [`CellIterator`].
    pub fn erase(&self, iterator: &mut CellIterator<K, V, SIZE, LOCK_FREE>) -> Option<(K, V)> {
        if self.killed {
            // The Cell has been killed.
            return None;
        }

        if iterator.current_array_ptr.is_null() || iterator.current_index == usize::MAX {
            // The iterator is fused.
            return None;
        }

        if let Some(data_array_ref) = iterator.current_array_ptr.as_ref() {
            let hash = data_array_ref.partial_hash_array[iterator.current_index];
            if (hash & OCCUPIED) == 0 {
                // The entry has been dropped, or never been used.
                return None;
            }

            debug_assert!(self.cell_ref.num_entries > 0);
            let cell_mut_ref =
                unsafe { &mut *(self.cell_ref as *const _ as *mut Cell<K, V, SIZE, LOCK_FREE>) };
            cell_mut_ref.num_entries -= 1;
            let data_array_mut_ref = unsafe {
                &mut *(data_array_ref as *const DataArray<K, V, SIZE> as *mut DataArray<K, V, SIZE>)
            };
            let entry_ptr = data_array_mut_ref.data[iterator.current_index].as_mut_ptr();
            if LOCK_FREE {
                data_array_mut_ref.partial_hash_array[iterator.current_index] |= REMOVED;
                return None;
            } else {
                data_array_mut_ref.partial_hash_array[iterator.current_index] = 0;
                return Some(unsafe {
                    ptr::replace(entry_ptr, MaybeUninit::uninit().assume_init())
                });
            }
        }
        None
    }

    /// Purges all the data.
    pub fn purge(&mut self, barrier: &Barrier) -> usize {
        if let Some(data_array) = self.cell_ref.data.swap((None, Tag::None), Relaxed) {
            barrier.reclaim(data_array);
        }
        self.killed = true;

        let num_entries = self.cell_ref.num_entries;
        let cell_mut_ref =
            unsafe { &mut *(self.cell_ref as *const _ as *mut Cell<K, V, SIZE, LOCK_FREE>) };
        cell_mut_ref.num_entries = 0;
        num_entries as usize
    }
}

impl<'b, K: 'static + Clone + Eq, V: 'static + Clone, const SIZE: usize, const LOCK_FREE: bool>
    CellLocker<'b, K, V, SIZE, LOCK_FREE>
{
    /// Removes a new key-value pair associated with the given key with the instances kept
    /// intact.
    pub fn mark_removed<Q>(&self, key_ref: &Q, partial_hash: u8, barrier: &Barrier) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        if self.killed {
            // The Cell has been killed.
            return false;
        }

        // Starts Searching the entry at the preferred index first.
        let mut data_array_ptr = self.cell_ref.data.load(Relaxed, barrier);
        let mut removed = false;
        let preferred_index = partial_hash as usize % SIZE;
        let expected_hash = (partial_hash & (!REMOVED)) | OCCUPIED;
        while let Some(data_array_ref) = data_array_ptr.as_ref() {
            for i in preferred_index..preferred_index + SIZE {
                let index = i % SIZE;
                if data_array_ref.partial_hash_array[index] == expected_hash {
                    let entry_ptr = data_array_ref.data[index].as_ptr();
                    unsafe {
                        let data_array_mut_ref = &mut *(data_array_ref
                            as *const DataArray<K, V, SIZE>
                            as *mut DataArray<K, V, SIZE>);
                        if (*entry_ptr).0.borrow() == key_ref {
                            data_array_mut_ref.partial_hash_array[index] |= REMOVED;
                            removed = true;
                            break;
                        }
                    }
                }
            }
            if removed {
                break;
            }
            data_array_ptr = data_array_ref.link.load(Relaxed, barrier);
        }

        if removed {
            let cell_mut_ref =
                unsafe { &mut *(self.cell_ref as *const _ as *mut Cell<K, V, SIZE, LOCK_FREE>) };
            cell_mut_ref.num_entries -= 1;
            self.optimize(self.cell_ref.num_entries, barrier);
        }
        removed
    }

    /// Optimizes the linked list.
    ///
    /// Three strategies.
    ///  1. Clears the entire [`Cell`] if there is no valid entry.
    ///  2. Coalesces if the given data array is non-empty and the linked list is sparse.
    ///  3. Unlinks the given data array if the data array is empty.
    fn optimize(&self, num_entries: u32, barrier: &Barrier) {
        if num_entries == 0 {
            // Clears the entire Cell.
            if let Some(data_array) = self.cell_ref.data.swap((None, Tag::None), Relaxed) {
                barrier.reclaim(data_array);
            }
            return;
        }

        if num_entries as usize > SIZE / 4 {
            // Too many entries for it to coalesce.
            return;
        }

        let head_data_array_ptr = self.cell_ref.data.load(Relaxed, barrier);
        let head_data_array_link_ptr = head_data_array_ptr
            .as_ref()
            .unwrap()
            .link
            .load(Relaxed, barrier);

        if head_data_array_link_ptr.is_null() {
            // No linked list attached to the head.
            return;
        }

        // Replaces the head with a new `DataArray`.
        let mut new_data_array = Arc::new(DataArray::new());
        let mut new_array_index = 0;
        let mut current_data_array_ptr = head_data_array_ptr;
        while let Some(current_data_array_ref) = current_data_array_ptr.as_ref() {
            for (index, hash) in current_data_array_ref.partial_hash_array.iter().enumerate() {
                if (hash & (REMOVED | OCCUPIED)) == OCCUPIED {
                    let entry_ptr = current_data_array_ref.data[index].as_ptr();
                    unsafe {
                        let entry_ref = &(*entry_ptr);
                        new_data_array.get_mut().unwrap().data[new_array_index]
                            .as_mut_ptr()
                            .write(entry_ref.clone());
                        new_data_array.get_mut().unwrap().partial_hash_array[new_array_index] =
                            *hash;
                    }
                    new_array_index += 1;
                }
            }
            if new_array_index == num_entries as usize {
                break;
            }
            current_data_array_ptr = current_data_array_ref.link.load(Relaxed, barrier);
        }
        if let Some(old_array) = self
            .cell_ref
            .data
            .swap((Some(new_data_array), Tag::None), Release)
        {
            barrier.reclaim(old_array);
        }
    }
}

impl<'b, K: 'static + Eq, V: 'static, const SIZE: usize, const LOCK_FREE: bool> Drop
    for CellLocker<'b, K, V, SIZE, LOCK_FREE>
{
    fn drop(&mut self) {
        let mut current = self.cell_ref.state.load(Relaxed);
        loop {
            let wakeup = if (current & WAITING) == WAITING {
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
                        self.cell_ref.wakeup();
                    }
                    break;
                }
                Err(result) => current = result,
            }
        }
    }
}

pub struct CellReader<'b, K: 'static + Eq, V: 'static, const SIZE: usize, const LOCK_FREE: bool> {
    cell_ref: &'b Cell<K, V, SIZE, LOCK_FREE>,
    killed: bool,
}

impl<'b, K: Eq, V, const SIZE: usize, const LOCK_FREE: bool> CellReader<'b, K, V, SIZE, LOCK_FREE> {
    /// Locks the given [`Cell`].
    pub fn lock(
        cell: &'b Cell<K, V, SIZE, LOCK_FREE>,
        barrier: &'b Barrier,
    ) -> Option<CellReader<'b, K, V, SIZE, LOCK_FREE>> {
        loop {
            for _ in 0..SIZE {
                if let Some(reader) = Self::try_lock(cell, barrier) {
                    if reader.killed {
                        return None;
                    }
                    return Some(reader);
                }
            }
            if let Some(reader) = cell.wait(|| Self::try_lock(cell, barrier)) {
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

    /// Tries to lock the [`Cell`].
    fn try_lock(
        cell: &'b Cell<K, V, SIZE, LOCK_FREE>,
        _barrier: &'b Barrier,
    ) -> Option<CellReader<'b, K, V, SIZE, LOCK_FREE>> {
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

    /// Returns a reference to the [`Cell`].
    pub fn cell_ref(&self) -> &Cell<K, V, SIZE, LOCK_FREE> {
        self.cell_ref
    }
}

impl<'b, K: 'static + Eq, V: 'static, const SIZE: usize, const LOCK_FREE: bool> Drop
    for CellReader<'b, K, V, SIZE, LOCK_FREE>
{
    fn drop(&mut self) {
        let mut current = self.cell_ref.state.load(Relaxed);
        loop {
            let wakeup = if (current & WAITING) == WAITING {
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
                        self.cell_ref.wakeup();
                    }
                    break;
                }
                Err(result) => current = result,
            }
        }
    }
}

pub struct DataArray<K: 'static + Eq, V: 'static, const SIZE: usize> {
    /// The lower two-bit of a partial hash value represents the state of the corresponding entry.
    partial_hash_array: [u8; SIZE],
    data: [MaybeUninit<(K, V)>; SIZE],
    link: AtomicArc<DataArray<K, V, SIZE>>,
}

impl<K: 'static + Eq, V: 'static, const SIZE: usize> DataArray<K, V, SIZE> {
    fn new() -> DataArray<K, V, SIZE> {
        DataArray {
            partial_hash_array: [0; SIZE],
            data: unsafe { MaybeUninit::uninit().assume_init() },
            link: AtomicArc::null(),
        }
    }
}

impl<K: 'static + Eq, V: 'static, const SIZE: usize> Drop for DataArray<K, V, SIZE> {
    fn drop(&mut self) {
        for (index, hash) in self.partial_hash_array.iter().enumerate() {
            if (hash & OCCUPIED) == OCCUPIED {
                let entry_mut_ptr = self.data[index].as_mut_ptr();
                unsafe { ptr::drop_in_place(entry_mut_ptr) };
            }
        }
    }
}

struct WaitQueueEntry {
    mutex: Mutex<bool>,
    condvar: Condvar,
    next_ptr: *const WaitQueueEntry,
}

impl WaitQueueEntry {
    fn new(next_ptr: *const WaitQueueEntry) -> WaitQueueEntry {
        WaitQueueEntry {
            mutex: Mutex::new(false),
            condvar: Condvar::new(),
            next_ptr,
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
    use std::sync;
    use std::thread;

    #[test]
    fn cell_locker() {
        const SIZE: usize = 32;
        let num_threads = (SIZE * 2) as usize;
        let barrier = sync::Arc::new(sync::Barrier::new(num_threads));
        let cell: sync::Arc<Cell<usize, usize, SIZE, true>> = sync::Arc::new(Default::default());
        let mut data: [u64; 128] = [0; 128];
        let mut thread_handles = Vec::with_capacity(num_threads);
        for thread_id in 0..num_threads {
            let barrier_copied = barrier.clone();
            let cell_copied = cell.clone();
            let data_ptr = sync::atomic::AtomicPtr::new(&mut data);
            thread_handles.push(thread::spawn(move || {
                barrier_copied.wait();
                let barrier = Barrier::new();
                for i in 0..4096 {
                    let xlocker = CellLocker::lock(&*cell_copied, &barrier).unwrap();
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
                                (thread_id % SIZE).try_into().unwrap(),
                                &barrier,
                            )
                            .1
                            .is_none());
                    } else {
                        assert_eq!(
                            xlocker
                                .cell_ref()
                                .search(
                                    &thread_id,
                                    (thread_id % SIZE).try_into().unwrap(),
                                    &barrier
                                )
                                .unwrap(),
                            &(thread_id, 0usize)
                        );
                    }
                    drop(xlocker);

                    let slocker = CellReader::lock(&*cell_copied, &barrier).unwrap();
                    assert_eq!(
                        slocker
                            .cell_ref()
                            .search(&thread_id, (thread_id % SIZE).try_into().unwrap(), &barrier)
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

        let epoch_barrier = Barrier::new();
        for thread_id in 0..SIZE {
            assert_eq!(
                cell.search(
                    &thread_id,
                    (thread_id % SIZE).try_into().unwrap(),
                    &epoch_barrier
                ),
                Some(&(thread_id, 0))
            );
        }
        let mut iterated = 0;
        for entry in cell.iter(&epoch_barrier) {
            assert!(entry.0 .0 < num_threads);
            assert_eq!(entry.0 .1, 0);
            iterated += 1;
        }
        assert_eq!(cell.num_entries(), iterated);

        for thread_id in 0..SIZE {
            let xlocker = CellLocker::lock(&*cell, &epoch_barrier).unwrap();
            assert!(xlocker.mark_removed(
                &thread_id,
                (thread_id % SIZE).try_into().unwrap(),
                &epoch_barrier
            ));
        }
        assert_eq!(cell.num_entries(), SIZE);

        let mut xlocker = CellLocker::lock(&*cell, &epoch_barrier).unwrap();
        xlocker.purge(&epoch_barrier);
        drop(xlocker);

        assert!(cell.killed());
        assert_eq!(cell.num_entries(), 0);
        assert!(CellLocker::lock(&*cell, &epoch_barrier).is_none());
    }
}
