use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};

use std::borrow::Borrow;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::fence;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicPtr, AtomicU32};
use std::sync::{Condvar, Mutex};

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
        key_ref: &Q,
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

        let mut data_array_ptr = self.data.load(Relaxed, barrier);
        let preferred_index = partial_hash as usize % SIZE;
        while let Some(data_array_ref) = data_array_ptr.as_ref() {
            if let Some((_, entry_ref)) =
                Self::search_array(data_array_ref, key_ref, preferred_index, partial_hash)
            {
                return Some(entry_ref);
            }
            data_array_ptr = data_array_ref.link.load(Relaxed, barrier);
        }
        None
    }

    /// Gets a [`CellIterator`] pointing to an entry associated with the given key.
    pub fn get<'b, Q>(
        &'b self,
        key_ref: &Q,
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
        while let Some(data_array_ref) = data_array_ptr.as_ref() {
            if let Some((index, _)) =
                Self::search_array(data_array_ref, key_ref, preferred_index, partial_hash)
            {
                return Some(CellIterator {
                    cell_ref: Some(self),
                    current_array_ptr: data_array_ptr,
                    current_index: index,
                    barrier_ref: barrier,
                });
            }
            data_array_ptr = data_array_ref.link.load(read_order, barrier);
        }
        None
    }

    /// Searches the given [`DataArray`] for an entry matching the key.
    fn search_array<'b, Q>(
        data_array_ref: &'b DataArray<K, V, SIZE>,
        key_ref: &Q,
        preferred_index: usize,
        partial_hash: u8,
    ) -> Option<(usize, &'b (K, V))>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let mut occupied = if LOCK_FREE {
            data_array_ref.occupied & (!data_array_ref.removed)
        } else {
            data_array_ref.occupied
        };
        if LOCK_FREE {
            fence(Acquire);
        }

        // Looks into the preferred slot.
        if (occupied & (1_u32 << preferred_index)) != 0
            && data_array_ref.partial_hash_array[preferred_index] == partial_hash
        {
            let entry_ptr = data_array_ref.data[preferred_index].as_ptr();
            let entry_ref = unsafe { &(*entry_ptr) };
            if entry_ref.0.borrow() == key_ref {
                return Some((preferred_index, entry_ref));
            }
            occupied &= !(1_u32 << preferred_index);
        }

        // Looks into other slots.
        let mut current_index = occupied.trailing_zeros();
        while (current_index as usize) < SIZE {
            let entry_ptr = data_array_ref.data[current_index as usize].as_ptr();
            let entry_ref = unsafe { &(*entry_ptr) };
            if entry_ref.0.borrow() == key_ref {
                return Some((current_index as usize, entry_ref));
            }
            occupied &= !(1_u32 << current_index);
            current_index = occupied.trailing_zeros();
        }

        None
    }

    /// Searches for a next closest valid slot to the given slot in the [`DataArray`].
    ///
    /// If the given slot is valid, it returns the given slot.
    fn next_entry<'b, Q>(
        data_array_ref: &'b DataArray<K, V, SIZE>,
        current_index: usize,
    ) -> Option<(usize, &'b (K, V), u8)>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        if current_index >= SIZE {
            return None;
        }

        let occupied = if LOCK_FREE {
            (data_array_ref.occupied & (!data_array_ref.removed))
                & (!((1_u32 << current_index) - 1))
        } else {
            data_array_ref.occupied & (!((1_u32 << current_index) - 1))
        };

        if LOCK_FREE {
            fence(Acquire);
        }

        let next_index = occupied.trailing_zeros() as usize;
        if next_index < SIZE {
            let entry_ptr = data_array_ref.data[next_index].as_ptr();
            return Some((
                next_index,
                unsafe { &(*entry_ptr) },
                data_array_ref.partial_hash_array[next_index],
            ));
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
            while let Some(data_array_ref) = self.current_array_ptr.as_ref() {
                // Searches for the next valid entry.
                let current_index = if self.current_index == usize::MAX {
                    0
                } else {
                    self.current_index + 1
                };
                if let Some((index, entry_ref, hash)) =
                    Cell::<K, V, SIZE, LOCK_FREE>::next_entry(data_array_ref, current_index)
                {
                    self.current_index = index;
                    return Some((entry_ref, hash));
                }

                // Proceeds to the next DataArray.
                self.current_array_ptr = data_array_ref.link.load(read_order, self.barrier_ref);
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

    /// Inserts a new key-value pair into the [`Cell`] without a uniqueness check.
    pub fn insert(&'b self, key: K, value: V, partial_hash: u8, barrier: &'b Barrier) {
        debug_assert!(!self.killed);

        if self.cell_ref.num_entries == u32::MAX {
            panic!("array overflow");
        }

        let cell_mut_ref =
            unsafe { &mut *(self.cell_ref as *const _ as *mut Cell<K, V, SIZE, LOCK_FREE>) };
        let mut data_array_ptr = self.cell_ref.data.load(Relaxed, barrier);
        let data_array_head_ptr = data_array_ptr;
        let preferred_index = partial_hash as usize % SIZE;
        let mut free_index = SIZE;
        while let Some(data_array_ref) = data_array_ptr.as_ref() {
            if (data_array_ref.occupied & (1_u32 << preferred_index)) == 0 {
                free_index = preferred_index;
                break;
            }
            let next_free_index = data_array_ref.occupied.trailing_ones() as usize;
            if next_free_index < SIZE {
                free_index = next_free_index;
                break;
            }

            data_array_ptr = data_array_ref.link.load(Relaxed, barrier);
        }

        if free_index < SIZE {
            let data_array_ref = data_array_ptr.as_ref().unwrap();
            unsafe {
                let data_array_mut_ref = &mut *(data_array_ref as *const DataArray<K, V, SIZE>
                    as *mut DataArray<K, V, SIZE>);
                data_array_mut_ref.data[free_index]
                    .as_mut_ptr()
                    .write((key, value));
                data_array_mut_ref.partial_hash_array[free_index] = partial_hash;

                if LOCK_FREE {
                    fence(Release);
                }

                data_array_mut_ref.occupied |= 1_u32 << free_index;
            };
        } else {
            // Inserts a new DataArray at the head.
            let mut new_data_array = Arc::new(DataArray::new());
            unsafe {
                new_data_array.get_mut().unwrap().data[preferred_index]
                    .as_mut_ptr()
                    .write((key, value))
            };
            new_data_array.get_mut().unwrap().partial_hash_array[preferred_index] = partial_hash;

            if LOCK_FREE {
                fence(Release);
            }

            new_data_array.get_mut().unwrap().occupied |= 1_u32 << preferred_index;

            new_data_array
                .link
                .swap((data_array_head_ptr.try_into_arc(), Tag::None), Relaxed);
            self.cell_ref
                .data
                .swap((Some(new_data_array), Tag::None), Release);
        }
        cell_mut_ref.num_entries += 1;
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
            if data_array_ref.occupied & (1_u32 << iterator.current_index) == 0 {
                return None;
            }

            if LOCK_FREE && data_array_ref.removed & (1_u32 << iterator.current_index) == 0 {
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
                data_array_mut_ref.removed |= 1_u32 << iterator.current_index;
                return None;
            } else {
                data_array_mut_ref.occupied &= !(1_u32 << iterator.current_index);
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

impl<'b, K: 'static + Clone + Eq, V: 'static + Clone, const SIZE: usize>
    CellLocker<'b, K, V, SIZE, true>
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
        let preferred_index = partial_hash as usize % SIZE;
        while let Some(data_array_ref) = data_array_ptr.as_ref() {
            if let Some((index, _)) = Cell::<K, V, SIZE, true>::search_array(
                data_array_ref,
                key_ref,
                preferred_index,
                partial_hash,
            ) {
                debug_assert!(self.cell_ref.num_entries > 0);
                debug_assert!((data_array_ref.removed & (1_u32 << index)) == 0);

                unsafe {
                    let data_array_mut_ref = &mut *(data_array_ref as *const DataArray<K, V, SIZE>
                        as *mut DataArray<K, V, SIZE>);
                    data_array_mut_ref.removed |= 1_u32 << index;
                    let cell_mut_ref =
                        &mut *(self.cell_ref as *const _ as *mut Cell<K, V, SIZE, true>);
                    cell_mut_ref.num_entries -= 1;
                    if cell_mut_ref.num_entries == 0 {
                        if let Some(data_array) =
                            self.cell_ref.data.swap((None, Tag::None), Relaxed)
                        {
                            barrier.reclaim(data_array);
                        }
                    } else if (cell_mut_ref.num_entries as usize) <= SIZE / 4 {
                        self.optimize(self.cell_ref.num_entries, barrier);
                    }
                    return true;
                }
            }
            data_array_ptr = data_array_ref.link.load(Relaxed, barrier);
        }
        false
    }

    /// Optimizes the linked list.
    fn optimize(&self, num_entries: u32, barrier: &Barrier) {
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
        let mut data_array_ptr = head_data_array_ptr;
        while let Some(data_array_ref) = data_array_ptr.as_ref() {
            let mut occupied = data_array_ref.occupied & (!data_array_ref.removed);
            let mut index = occupied.trailing_zeros();
            while (index as usize) < SIZE {
                let entry_ptr = data_array_ref.data[index as usize].as_ptr();
                unsafe {
                    let entry_ref = &(*entry_ptr);
                    new_data_array.get_mut().unwrap().data[new_array_index]
                        .as_mut_ptr()
                        .write(entry_ref.clone());
                    new_data_array.get_mut().unwrap().partial_hash_array[new_array_index] =
                        data_array_ref.partial_hash_array[index as usize];
                    new_data_array.get_mut().unwrap().occupied |= 1_u32 << new_array_index;
                }
                new_array_index += 1;

                occupied &= !(1_u32 << index);
                index = occupied.trailing_zeros();
            }
            if new_array_index == num_entries as usize {
                break;
            }
            data_array_ptr = data_array_ref.link.load(Relaxed, barrier);
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
    link: AtomicArc<DataArray<K, V, SIZE>>,
    occupied: u32,
    removed: u32,
    partial_hash_array: [u8; SIZE],
    data: [MaybeUninit<(K, V)>; SIZE],
}

impl<K: 'static + Eq, V: 'static, const SIZE: usize> DataArray<K, V, SIZE> {
    fn new() -> DataArray<K, V, SIZE> {
        DataArray {
            link: AtomicArc::null(),
            occupied: 0,
            removed: 0,
            partial_hash_array: [0; SIZE],
            data: unsafe { MaybeUninit::uninit().assume_init() },
        }
    }
}

impl<K: 'static + Eq, V: 'static, const SIZE: usize> Drop for DataArray<K, V, SIZE> {
    fn drop(&mut self) {
        let mut occupied = self.occupied;
        let mut index = occupied.trailing_zeros();
        while (index as usize) < SIZE {
            let entry_mut_ptr = self.data[index as usize].as_mut_ptr();
            unsafe { ptr::drop_in_place(entry_mut_ptr) };
            occupied &= !(1_u32 << index);
            index = occupied.trailing_zeros();
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
                        xlocker.insert(
                            thread_id,
                            0,
                            (thread_id % SIZE).try_into().unwrap(),
                            &barrier,
                        );
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
