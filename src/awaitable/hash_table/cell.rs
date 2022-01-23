use crate::ebr::{Arc, AtomicArc, Barrier, Tag};

use std::borrow::Borrow;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::fence;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// The fixed size of [`DataArray`].
///
/// The size cannot exceed `32`.
pub const ARRAY_SIZE: usize = 32;

/// State bits.
const KILLED: u32 = 1_u32 << 31;
const LOCK: u32 = 1_u32 << 30;
const SLOCK_MAX: u32 = LOCK - 1;
const LOCK_MASK: u32 = LOCK | SLOCK_MAX;

/// [`Cell`] is a small fixed-size hash table that resolves hash conflicts using a linked list
/// of entry arrays.
pub struct Cell<K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    /// An array of key-value pairs and their metadata.
    data_array: DataArray<K, V>,

    /// The state of the [`Cell`].
    state: AtomicU32,

    /// The number of valid entries in the [`Cell`].
    num_entries: u32,
}

impl<K: 'static + Eq, V: 'static, const LOCK_FREE: bool> Default for Cell<K, V, LOCK_FREE> {
    fn default() -> Self {
        Cell::<K, V, LOCK_FREE> {
            data_array: DataArray::new(),
            state: AtomicU32::new(0),
            num_entries: 0,
        }
    }
}

impl<K: 'static + Eq, V: 'static, const LOCK_FREE: bool> Cell<K, V, LOCK_FREE> {
    /// Returns true if the [`Cell`] has been killed.
    pub fn killed(&self) -> bool {
        (self.state.load(Relaxed) & KILLED) == KILLED
    }

    /// Returns the number of entries in the [`Cell`].
    pub fn num_entries(&self) -> usize {
        self.num_entries as usize
    }

    /// Iterates the contents of the [`Cell`].
    pub fn iter<'b>(&'b self, barrier: &'b Barrier) -> EntryIterator<'b, K, V, LOCK_FREE> {
        EntryIterator::new(self, barrier)
    }

    /// Searches for an entry associated with the given key.
    #[inline]
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

        let mut data_array_ptr = &self.data_array as *const DataArray<K, V>;
        let preferred_index = partial_hash as usize % ARRAY_SIZE;
        while let Some(data_array_ref) = unsafe { data_array_ptr.as_ref() } {
            if let Some((_, entry_ref)) =
                Self::search_array(data_array_ref, key_ref, preferred_index, partial_hash)
            {
                return Some(entry_ref);
            }
            data_array_ptr = data_array_ref.link.load(Relaxed, barrier).as_raw();
        }
        None
    }

    /// Gets an [`EntryIterator`] pointing to an entry associated with the given key.
    #[inline]
    pub fn get<'b, Q>(
        &'b self,
        key_ref: &Q,
        partial_hash: u8,
        barrier: &'b Barrier,
    ) -> Option<EntryIterator<'b, K, V, LOCK_FREE>>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        if self.num_entries == 0 {
            return None;
        }

        let mut current_array_ptr = &self.data_array as *const DataArray<K, V>;
        let mut prev_array_ptr = ptr::null();
        let preferred_index = partial_hash as usize % ARRAY_SIZE;
        while let Some(data_array_ref) = unsafe { current_array_ptr.as_ref() } {
            if let Some((index, _)) =
                Self::search_array(data_array_ref, key_ref, preferred_index, partial_hash)
            {
                return Some(EntryIterator {
                    cell: Some(self),
                    current_array_ptr,
                    prev_array_ptr,
                    current_index: index,
                    barrier_ref: barrier,
                });
            }
            prev_array_ptr = current_array_ptr;
            current_array_ptr = data_array_ref.link.load(Relaxed, barrier).as_raw();
        }
        None
    }

    /// Kills the [`Cell`] for dropping it.
    pub unsafe fn kill_and_drop(&self, barrier: &Barrier) {
        if !self.data_array.link.load(Relaxed, barrier).is_null() {
            if let Some(data_array) = self.data_array.link.swap((None, Tag::None), Relaxed) {
                barrier.reclaim(data_array);
            }
        }
        self.state.store(KILLED, Relaxed);
        ptr::read(self);
    }

    /// Searches the given [`DataArray`] for an entry matching the key.
    #[inline]
    fn search_array<'b, Q>(
        data_array_ref: &'b DataArray<K, V>,
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
        while (current_index as usize) < ARRAY_SIZE {
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
    fn next_entry<Q>(
        data_array_ref: &DataArray<K, V>,
        current_index: usize,
    ) -> Option<(usize, &(K, V), u8)>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        if current_index >= ARRAY_SIZE {
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
        if next_index < ARRAY_SIZE {
            let entry_ptr = data_array_ref.data[next_index].as_ptr();
            return Some((
                next_index,
                unsafe { &(*entry_ptr) },
                data_array_ref.partial_hash_array[next_index],
            ));
        }

        None
    }
}

impl<K: 'static + Eq, V: 'static, const LOCK_FREE: bool> Drop for Cell<K, V, LOCK_FREE> {
    fn drop(&mut self) {
        // The [`Cell`] must have been killed.
        debug_assert!(self.killed());
    }
}

pub struct EntryIterator<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    cell: Option<&'b Cell<K, V, LOCK_FREE>>,
    current_array_ptr: *const DataArray<K, V>,
    prev_array_ptr: *const DataArray<K, V>,
    current_index: usize,
    barrier_ref: &'b Barrier,
}

impl<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> EntryIterator<'b, K, V, LOCK_FREE> {
    /// Creates a new [`EntryIterator`].
    pub fn new(
        cell: &'b Cell<K, V, LOCK_FREE>,
        barrier: &'b Barrier,
    ) -> EntryIterator<'b, K, V, LOCK_FREE> {
        EntryIterator {
            cell: Some(cell),
            current_array_ptr: ptr::null(),
            prev_array_ptr: ptr::null(),
            current_index: usize::MAX,
            barrier_ref: barrier,
        }
    }

    /// Gets a reference to the key-value pair.
    #[inline]
    pub fn get(&self) -> Option<&'b (K, V)> {
        if let Some(data_array_ref) = unsafe { self.current_array_ptr.as_ref() } {
            let entry_ptr = data_array_ref.data[self.current_index].as_ptr();
            return Some(unsafe { &(*entry_ptr) });
        }
        None
    }

    /// Tries to remove the current data array from the linked list.
    ///
    /// It should only be invoked when the caller is holding a [`Locker`] on the [`Cell`].
    #[inline]
    fn unlink_data_array(&mut self, data_array_ref: &DataArray<K, V>) {
        let next_data_array = if LOCK_FREE {
            data_array_ref.link.get_arc(Relaxed, self.barrier_ref)
        } else {
            data_array_ref.link.swap((None, Tag::None), Relaxed)
        };
        self.current_array_ptr = next_data_array
            .as_ref()
            .map_or_else(ptr::null, |n| n.ptr(self.barrier_ref).as_raw());
        let old_data_array =
            if let Some(prev_data_array_ref) = unsafe { self.prev_array_ptr.as_ref() } {
                prev_data_array_ref
                    .link
                    .swap((next_data_array, Tag::None), Relaxed)
            } else if let Some(cell) = self.cell.as_ref() {
                cell.data_array
                    .link
                    .swap((next_data_array, Tag::None), Relaxed)
            } else {
                None
            };
        if let Some(data_array) = old_data_array {
            self.barrier_ref.reclaim(data_array);
        }
        if self.current_array_ptr.is_null() {
            self.cell.take();
        } else {
            self.current_index = usize::MAX;
        }
    }
}

impl<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> Iterator
    for EntryIterator<'b, K, V, LOCK_FREE>
{
    type Item = (&'b (K, V), u8);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(&cell) = self.cell.as_ref() {
            if self.current_array_ptr.is_null() {
                // Starts scanning from the beginning.
                self.current_array_ptr = &cell.data_array as *const DataArray<K, V>;
            }
            while let Some(data_array_ref) = unsafe { self.current_array_ptr.as_ref() } {
                // Searches for the next valid entry.
                let current_index = if self.current_index == usize::MAX {
                    0
                } else {
                    self.current_index + 1
                };
                if let Some((index, entry_ref, hash)) =
                    Cell::<K, V, LOCK_FREE>::next_entry(data_array_ref, current_index)
                {
                    self.current_index = index;
                    return Some((entry_ref, hash));
                }

                // Proceeds to the next `DataArray`.
                self.prev_array_ptr = self.current_array_ptr;
                self.current_array_ptr =
                    data_array_ref.link.load(Relaxed, self.barrier_ref).as_raw();
                self.current_index = usize::MAX;
            }
            // Fuses itself.
            self.cell.take();
        }
        None
    }
}

pub struct Locker<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    cell: &'b Cell<K, V, LOCK_FREE>,
}

impl<'b, K: Eq, V, const LOCK_FREE: bool> Locker<'b, K, V, LOCK_FREE> {
    /// Tries to lock the [`Cell`].
    #[inline]
    pub fn try_lock(
        cell: &'b Cell<K, V, LOCK_FREE>,
        _barrier: &'b Barrier,
    ) -> Result<Option<Locker<'b, K, V, LOCK_FREE>>, ()> {
        let current = cell.state.load(Relaxed) & (!LOCK_MASK);
        if (current & KILLED) == KILLED {
            return Ok(None);
        }
        if cell
            .state
            .compare_exchange(current, current | LOCK, Acquire, Relaxed)
            .is_ok()
        {
            Ok(Some(Locker { cell }))
        } else {
            Err(())
        }
    }

    /// Returns a reference to the [`Cell`].
    #[inline]
    pub fn cell(&self) -> &'b Cell<K, V, LOCK_FREE> {
        self.cell
    }

    /// Inserts a new key-value pair into the [`Cell`] without a uniqueness check.
    #[inline]
    pub fn insert(&'b self, key: K, value: V, partial_hash: u8, barrier: &'b Barrier) {
        assert!(self.cell.num_entries != u32::MAX, "array overflow");

        let mut data_array_ptr = &self.cell.data_array as *const DataArray<K, V>;
        let preferred_index = partial_hash as usize % ARRAY_SIZE;
        let mut free_index = ARRAY_SIZE;
        while let Some(data_array_ref) = unsafe { data_array_ptr.as_ref() } {
            if (data_array_ref.occupied & (1_u32 << preferred_index)) == 0 {
                free_index = preferred_index;
                break;
            }
            let next_free_index = data_array_ref.occupied.trailing_ones() as usize;
            if next_free_index < ARRAY_SIZE {
                free_index = next_free_index;
                break;
            }

            data_array_ptr = data_array_ref.link.load(Relaxed, barrier).as_raw();
        }

        if free_index == ARRAY_SIZE {
            // Inserts a new `DataArray` at the linked list head.
            let mut new_data_array = Arc::new(DataArray::new());
            unsafe {
                new_data_array.get_mut().unwrap().data[preferred_index]
                    .as_mut_ptr()
                    .write((key, value));
            }
            new_data_array.get_mut().unwrap().partial_hash_array[preferred_index] = partial_hash;

            if LOCK_FREE {
                fence(Release);
            }

            new_data_array.get_mut().unwrap().occupied |= 1_u32 << preferred_index;

            new_data_array.link.swap(
                (
                    self.cell.data_array.link.get_arc(Relaxed, barrier),
                    Tag::None,
                ),
                Relaxed,
            );
            self.cell
                .data_array
                .link
                .swap((Some(new_data_array), Tag::None), Release);
        } else {
            unsafe {
                let data_array_mut_ref = &mut *(data_array_ptr as *mut DataArray<K, V>);
                data_array_mut_ref.data[free_index]
                    .as_mut_ptr()
                    .write((key, value));
                data_array_mut_ref.partial_hash_array[free_index] = partial_hash;

                if LOCK_FREE {
                    fence(Release);
                }

                data_array_mut_ref.occupied |= 1_u32 << free_index;
            };
        }
        self.num_entries_updated(self.cell.num_entries + 1);
    }

    /// Removes a key-value pair being pointed by the given [`EntryIterator`].
    pub fn erase(&self, iterator: &mut EntryIterator<K, V, LOCK_FREE>) -> Option<(K, V)> {
        if iterator.current_index == usize::MAX {
            return None;
        }

        if let Some(data_array_mut_ref) =
            unsafe { (iterator.current_array_ptr as *mut DataArray<K, V>).as_mut() }
        {
            if data_array_mut_ref.occupied & (1_u32 << iterator.current_index) == 0 {
                return None;
            }

            if LOCK_FREE && data_array_mut_ref.removed & (1_u32 << iterator.current_index) != 0 {
                return None;
            }

            self.num_entries_updated(self.cell.num_entries - 1);
            let result = if LOCK_FREE {
                data_array_mut_ref.removed |= 1_u32 << iterator.current_index;
                None
            } else {
                data_array_mut_ref.occupied &= !(1_u32 << iterator.current_index);
                let entry_ptr = data_array_mut_ref.data[iterator.current_index].as_mut_ptr();
                #[allow(clippy::uninit_assumed_init)]
                Some(unsafe { ptr::replace(entry_ptr, MaybeUninit::uninit().assume_init()) })
            };
            if LOCK_FREE && (data_array_mut_ref.occupied & (!data_array_mut_ref.removed)) == 0
                || (!LOCK_FREE && data_array_mut_ref.occupied == 0)
            {
                iterator.unlink_data_array(data_array_mut_ref);
            }
            return result;
        }
        None
    }

    /// Extracts the key-value pair being pointed by `self`.
    #[allow(clippy::unused_self)]
    pub fn extract(&self, iterator: &mut EntryIterator<K, V, LOCK_FREE>) -> (K, V) {
        let data_array_mut_ref =
            unsafe { &mut *(iterator.current_array_ptr as *mut DataArray<K, V>) };
        debug_assert!(!LOCK_FREE);
        debug_assert_ne!(
            data_array_mut_ref.occupied & (1_u32 << iterator.current_index),
            0
        );

        self.num_entries_updated(self.cell.num_entries - 1);
        data_array_mut_ref.occupied &= !(1_u32 << iterator.current_index);
        let entry_ptr = data_array_mut_ref.data[iterator.current_index].as_mut_ptr();
        let result = unsafe { ptr::read(entry_ptr) };
        if data_array_mut_ref.occupied == 0 {
            iterator.unlink_data_array(data_array_mut_ref);
        }
        result
    }

    /// Purges all the data.
    pub fn purge(&mut self, barrier: &Barrier) {
        self.cell.state.fetch_or(KILLED, Release);
        self.num_entries_updated(0);
        if !self.cell.data_array.link.load(Relaxed, barrier).is_null() {
            if let Some(data_array) = self.cell.data_array.link.swap((None, Tag::None), Relaxed) {
                barrier.reclaim(data_array);
            }
        }
    }

    /// Updates the number of entries.
    fn num_entries_updated(&self, num: u32) {
        #[allow(clippy::cast_ref_to_mut)]
        let cell_mut_ref = unsafe { &mut *(self.cell as *const _ as *mut Cell<K, V, LOCK_FREE>) };
        cell_mut_ref.num_entries = num;
    }
}

impl<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> Drop for Locker<'b, K, V, LOCK_FREE> {
    #[inline]
    fn drop(&mut self) {
        let mut current = self.cell.state.load(Relaxed);
        loop {
            match self
                .cell
                .state
                .compare_exchange(current, current & (!LOCK), Release, Relaxed)
            {
                Ok(_) => break,
                Err(result) => current = result,
            }
        }
    }
}

pub struct Reader<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    cell: &'b Cell<K, V, LOCK_FREE>,
}

impl<'b, K: Eq, V, const LOCK_FREE: bool> Reader<'b, K, V, LOCK_FREE> {
    /// Tries to lock the [`Cell`].
    #[inline]
    pub fn try_lock(
        cell: &'b Cell<K, V, LOCK_FREE>,
        _barrier: &'b Barrier,
    ) -> Result<Option<Reader<'b, K, V, LOCK_FREE>>, ()> {
        let current = cell.state.load(Relaxed);
        if (current & LOCK_MASK) >= SLOCK_MAX {
            return Err(());
        }
        if (current & KILLED) >= KILLED {
            return Ok(None);
        }
        if cell
            .state
            .compare_exchange(current, current + 1, Acquire, Relaxed)
            .is_ok()
        {
            Ok(Some(Reader { cell }))
        } else {
            Err(())
        }
    }

    /// Returns a reference to the [`Cell`].
    #[inline]
    pub fn cell(&self) -> &Cell<K, V, LOCK_FREE> {
        self.cell
    }
}

impl<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> Drop for Reader<'b, K, V, LOCK_FREE> {
    #[inline]
    fn drop(&mut self) {
        let mut current = self.cell.state.load(Relaxed);
        loop {
            let next = current - 1;
            match self
                .cell
                .state
                .compare_exchange(current, next, Relaxed, Relaxed)
            {
                Ok(_) => break,
                Err(result) => current = result,
            }
        }
    }
}

/// [`DataArray`] is a fixed size array of key-value pairs.
pub struct DataArray<K: 'static + Eq, V: 'static> {
    link: AtomicArc<DataArray<K, V>>,
    occupied: u32,
    removed: u32,
    partial_hash_array: [u8; ARRAY_SIZE],
    data: [MaybeUninit<(K, V)>; ARRAY_SIZE],
}

impl<K: 'static + Eq, V: 'static> DataArray<K, V> {
    fn new() -> DataArray<K, V> {
        DataArray {
            link: AtomicArc::null(),
            occupied: 0,
            removed: 0,
            partial_hash_array: [0_u8; ARRAY_SIZE],
            data: unsafe { MaybeUninit::uninit().assume_init() },
        }
    }
}

impl<K: 'static + Eq, V: 'static> Drop for DataArray<K, V> {
    fn drop(&mut self) {
        let mut occupied = self.occupied;
        let mut index = occupied.trailing_zeros();
        while (index as usize) < ARRAY_SIZE {
            let entry_mut_ptr = self.data[index as usize].as_mut_ptr();
            unsafe {
                ptr::drop_in_place(entry_mut_ptr);
            }
            occupied &= !(1_u32 << index);
            index = occupied.trailing_zeros();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::convert::TryInto;
    use std::sync::atomic::AtomicPtr;

    use tokio::sync;

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn queue() {
        let num_tasks = ARRAY_SIZE + 2;
        let barrier = Arc::new(sync::Barrier::new(num_tasks));
        let cell: Arc<Cell<usize, usize, true>> = Arc::new(Cell::default());
        let mut data: [u64; 128] = [0; 128];
        let mut task_handles = Vec::with_capacity(num_tasks);
        for task_id in 0..num_tasks {
            let barrier_copied = barrier.clone();
            let cell_copied = cell.clone();
            let data_ptr = AtomicPtr::new(&mut data);
            task_handles.push(tokio::spawn(async move {
                barrier_copied.wait().await;
                let barrier = Barrier::new();
                for i in 0..2048 {
                    let exclusive_locker = loop {
                        if let Ok(Some(locker)) = Locker::try_lock(&*cell_copied, &barrier) {
                            break locker;
                        }
                    };

                    let mut sum: u64 = 0;
                    for j in 0..128 {
                        unsafe {
                            sum += (*data_ptr.load(Relaxed))[j];
                            (*data_ptr.load(Relaxed))[j] = if i % 4 == 0 { 2 } else { 4 }
                        };
                    }
                    assert_eq!(sum % 256, 0);
                    if i == 0 {
                        exclusive_locker.insert(
                            task_id,
                            0,
                            (task_id % ARRAY_SIZE).try_into().unwrap(),
                            &barrier,
                        );
                    } else {
                        assert_eq!(
                            exclusive_locker
                                .cell()
                                .search(
                                    &task_id,
                                    (task_id % ARRAY_SIZE).try_into().unwrap(),
                                    &barrier
                                )
                                .unwrap(),
                            &(task_id, 0_usize)
                        );
                    }
                    drop(exclusive_locker);

                    let read_locker = loop {
                        if let Ok(Some(locker)) = Reader::try_lock(&*cell_copied, &barrier) {
                            break locker;
                        }
                    };
                    assert_eq!(
                        read_locker
                            .cell()
                            .search(
                                &task_id,
                                (task_id % ARRAY_SIZE).try_into().unwrap(),
                                &barrier
                            )
                            .unwrap(),
                        &(task_id, 0_usize)
                    );
                }
            }));
        }
        for r in futures::future::join_all(task_handles).await {
            assert!(r.is_ok());
        }

        let sum: u64 = data.iter().sum();
        assert_eq!(sum % 256, 0);
        assert_eq!(cell.num_entries(), num_tasks);

        let epoch_barrier = Barrier::new();
        for task_id in 0..num_tasks {
            assert_eq!(
                cell.search(
                    &task_id,
                    (task_id % ARRAY_SIZE).try_into().unwrap(),
                    &epoch_barrier
                ),
                Some(&(task_id, 0))
            );
        }
        let mut iterated = 0;
        for entry in cell.iter(&epoch_barrier) {
            assert!(entry.0 .0 < num_tasks);
            assert_eq!(entry.0 .1, 0);
            iterated += 1;
        }
        assert_eq!(cell.num_entries(), iterated);

        let mut xlocker = Locker::try_lock(&*cell, &epoch_barrier).unwrap().unwrap();
        xlocker.purge(&epoch_barrier);
        drop(xlocker);

        assert!(cell.killed());
        assert_eq!(cell.num_entries(), 0);
        assert!(Locker::try_lock(&*cell, &epoch_barrier)
            .ok()
            .unwrap()
            .is_none());
    }
}
