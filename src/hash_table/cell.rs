use super::wait_queue::WaitQueue;

use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};

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
const WAITING: u32 = 1_u32 << 30;
const LOCK: u32 = 1_u32 << 29;
const SLOCK_MAX: u32 = LOCK - 1;
const LOCK_MASK: u32 = LOCK | SLOCK_MAX;

/// [`Cell`] is a small fixed-size hash table that resolves hash conflicts using a linked list
/// of entry arrays.
pub struct Cell<K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    /// An array of key-value pairs and their metadata.
    data_array: AtomicArc<DataArray<K, V>>,

    /// The state of the [`Cell`].
    state: AtomicU32,

    /// The number of valid entries in the [`Cell`].
    num_entries: u32,

    /// The wait queue of the [`Cell`].
    wait_queue: WaitQueue,
}

impl<K: 'static + Eq, V: 'static, const LOCK_FREE: bool> Default for Cell<K, V, LOCK_FREE> {
    fn default() -> Self {
        Cell::<K, V, LOCK_FREE> {
            data_array: AtomicArc::null(),
            state: AtomicU32::new(0),
            num_entries: 0,
            wait_queue: WaitQueue::default(),
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
    pub fn iter<'b>(&'b self, guard: &'b Barrier) -> EntryIterator<'b, K, V, LOCK_FREE> {
        EntryIterator::new(self, guard)
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

        let mut data_array_ptr = self.data_array.load(Relaxed, barrier);
        let preferred_index = partial_hash as usize % ARRAY_SIZE;
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

        let mut current_array_ptr = self.data_array.load(Relaxed, barrier);
        let mut prev_array_ptr = Ptr::null();
        let preferred_index = partial_hash as usize % ARRAY_SIZE;
        while let Some(data_array_ref) = current_array_ptr.as_ref() {
            if let Some((index, _)) =
                Self::search_array(data_array_ref, key_ref, preferred_index, partial_hash)
            {
                return Some(EntryIterator {
                    cell_ref: Some(self),
                    current_array_ptr,
                    prev_array_ptr,
                    current_index: index,
                    barrier_ref: barrier,
                });
            }
            prev_array_ptr = current_array_ptr;
            current_array_ptr = data_array_ref.link.load(Relaxed, barrier);
        }
        None
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
        // The Cell must have been killed.
        debug_assert!(self.killed());
    }
}

pub struct EntryIterator<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    cell_ref: Option<&'b Cell<K, V, LOCK_FREE>>,
    current_array_ptr: Ptr<'b, DataArray<K, V>>,
    prev_array_ptr: Ptr<'b, DataArray<K, V>>,
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
            cell_ref: Some(cell),
            current_array_ptr: Ptr::null(),
            prev_array_ptr: Ptr::null(),
            current_index: usize::MAX,
            barrier_ref: barrier,
        }
    }

    /// Gets a reference to the key-value pair.
    #[inline]
    pub fn get(&self) -> Option<&'b (K, V)> {
        if let Some(data_array_ref) = self.current_array_ptr.as_ref() {
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
            .map_or_else(Ptr::null, |n| n.ptr(self.barrier_ref));
        let old_data_array = if let Some(prev_data_array_ref) = self.prev_array_ptr.as_ref() {
            prev_data_array_ref
                .link
                .swap((next_data_array, Tag::None), Relaxed)
        } else if let Some(cell_ref) = self.cell_ref.as_ref() {
            debug_assert!(!cell_ref
                .data_array
                .load(Relaxed, self.barrier_ref)
                .is_null());
            cell_ref
                .data_array
                .swap((next_data_array, Tag::None), Relaxed)
        } else {
            None
        };
        if let Some(data_array) = old_data_array {
            self.barrier_ref.reclaim(data_array);
        }
        if self.current_array_ptr.is_null() {
            self.cell_ref.take();
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
        if let Some(&cell_ref) = self.cell_ref.as_ref() {
            if self.current_array_ptr.is_null() {
                // Starts scanning from the beginning.
                self.current_array_ptr = cell_ref.data_array.load(Relaxed, self.barrier_ref);
            }
            while let Some(data_array_ref) = self.current_array_ptr.as_ref() {
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

                // Proceeds to the next DataArray.
                self.prev_array_ptr = self.current_array_ptr;
                self.current_array_ptr = data_array_ref.link.load(Relaxed, self.barrier_ref);
                self.current_index = usize::MAX;
            }
            // Fuses itself.
            self.cell_ref.take();
        }
        None
    }
}

pub struct Locker<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    cell_ref: &'b Cell<K, V, LOCK_FREE>,
    killed: bool,
}

impl<'b, K: Eq, V, const LOCK_FREE: bool> Locker<'b, K, V, LOCK_FREE> {
    /// Locks the given [`Cell`].
    #[inline]
    pub fn lock(
        cell: &'b Cell<K, V, LOCK_FREE>,
        barrier: &'b Barrier,
    ) -> Option<Locker<'b, K, V, LOCK_FREE>> {
        loop {
            if let Some(locker) = Self::try_lock(cell, barrier).map_or_else(
                || {
                    cell.wait_queue.wait(|| {
                        // Marks that there is a waiting thread.
                        cell.state.fetch_or(WAITING, Release);
                        Self::try_lock(cell, barrier)
                    })
                },
                Some,
            ) {
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
    #[inline]
    fn try_lock(
        cell: &'b Cell<K, V, LOCK_FREE>,
        _barrier: &'b Barrier,
    ) -> Option<Locker<'b, K, V, LOCK_FREE>> {
        let current = cell.state.load(Relaxed) & (!LOCK_MASK);
        if cell
            .state
            .compare_exchange(current, current | LOCK, Acquire, Relaxed)
            .is_ok()
        {
            Some(Locker {
                cell_ref: cell,
                killed: (current & KILLED) == KILLED,
            })
        } else {
            None
        }
    }

    /// Returns a reference to the [`Cell`].
    #[inline]
    pub fn cell_ref(&self) -> &'b Cell<K, V, LOCK_FREE> {
        self.cell_ref
    }

    /// Inserts a new key-value pair into the [`Cell`] without a uniqueness check.
    #[inline]
    pub fn insert(&'b self, key: K, value: V, partial_hash: u8, barrier: &'b Barrier) {
        debug_assert!(!self.killed);

        if self.cell_ref.num_entries == u32::MAX {
            panic!("array overflow");
        }

        let mut data_array_ptr = self.cell_ref.data_array.load(Relaxed, barrier);
        let data_array_head_ptr = data_array_ptr;
        let preferred_index = partial_hash as usize % ARRAY_SIZE;
        let mut free_index = ARRAY_SIZE;
        while let Some(data_array_ref) = data_array_ptr.as_ref() {
            if (data_array_ref.occupied & (1_u32 << preferred_index)) == 0 {
                free_index = preferred_index;
                break;
            }
            let next_free_index = data_array_ref.occupied.trailing_ones() as usize;
            if next_free_index < ARRAY_SIZE {
                free_index = next_free_index;
                break;
            }

            data_array_ptr = data_array_ref.link.load(Relaxed, barrier);
        }

        if free_index == ARRAY_SIZE {
            // Inserts a new `DataArray` at the head.
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

            new_data_array
                .link
                .swap((data_array_head_ptr.try_into_arc(), Tag::None), Relaxed);
            self.cell_ref
                .data_array
                .swap((Some(new_data_array), Tag::None), Release);
        } else {
            let data_array_ref = data_array_ptr.as_ref().unwrap();
            unsafe {
                #[allow(clippy::cast_ref_to_mut)]
                let data_array_mut_ref =
                    &mut *(data_array_ref as *const DataArray<K, V> as *mut DataArray<K, V>);
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
        self.num_entries_updated(self.cell_ref.num_entries + 1);
    }

    /// Removes a key-value pair being pointed by the given [`EntryIterator`].
    pub fn erase(&self, iterator: &mut EntryIterator<K, V, LOCK_FREE>) -> Option<(K, V)> {
        if self.killed || iterator.current_index == usize::MAX {
            return None;
        }

        if let Some(data_array_ref) = iterator.current_array_ptr.as_ref() {
            if data_array_ref.occupied & (1_u32 << iterator.current_index) == 0 {
                return None;
            }

            if LOCK_FREE && data_array_ref.removed & (1_u32 << iterator.current_index) != 0 {
                return None;
            }
            self.num_entries_updated(self.cell_ref.num_entries - 1);
            #[allow(clippy::cast_ref_to_mut)]
            let data_array_mut_ref =
                unsafe { &mut *(data_array_ref as *const DataArray<K, V> as *mut DataArray<K, V>) };
            let result = if LOCK_FREE {
                data_array_mut_ref.removed |= 1_u32 << iterator.current_index;
                None
            } else {
                data_array_mut_ref.occupied &= !(1_u32 << iterator.current_index);
                let entry_ptr = data_array_mut_ref.data[iterator.current_index].as_mut_ptr();
                #[allow(clippy::uninit_assumed_init)]
                Some(unsafe { ptr::replace(entry_ptr, MaybeUninit::uninit().assume_init()) })
            };
            if LOCK_FREE && (data_array_ref.occupied & (!data_array_ref.removed)) == 0
                || (!LOCK_FREE && data_array_ref.occupied == 0)
            {
                iterator.unlink_data_array(data_array_ref);
            }
            return result;
        }
        None
    }

    /// Extracts the key-value pair being pointed by `self`.
    #[allow(clippy::unused_self)]
    pub fn extract(&self, iterator: &mut EntryIterator<K, V, LOCK_FREE>) -> (K, V) {
        let data_array_ref = iterator.current_array_ptr.as_ref().unwrap();
        debug_assert!(!LOCK_FREE);
        debug_assert_ne!(
            data_array_ref.occupied & (1_u32 << iterator.current_index),
            0
        );

        #[allow(clippy::cast_ref_to_mut)]
        let data_array_mut_ref =
            unsafe { &mut *(data_array_ref as *const DataArray<K, V> as *mut DataArray<K, V>) };
        data_array_mut_ref.occupied &= !(1_u32 << iterator.current_index);
        let entry_ptr = data_array_mut_ref.data[iterator.current_index].as_mut_ptr();
        #[allow(clippy::uninit_assumed_init)]
        let result = unsafe { ptr::replace(entry_ptr, MaybeUninit::uninit().assume_init()) };
        if data_array_mut_ref.occupied == 0 {
            iterator.unlink_data_array(data_array_ref);
        }
        result
    }

    /// Tries to inherit the data array from the other [`Cell`].
    #[inline]
    pub fn try_inherit(&self, other: &Locker<K, V, LOCK_FREE>, barrier: &Barrier) -> bool {
        if self.cell_ref.data_array.load(Relaxed, barrier).is_null() {
            let other_data_array_ptr = other.cell_ref.data_array.load(Relaxed, barrier);
            if let Some(other_data_array_ref) = other_data_array_ptr.as_ref() {
                if other_data_array_ref.link.load(Relaxed, barrier).is_null() {
                    // The conditions are, `self` has none and `other` has a single data array.
                    self.cell_ref.data_array.swap(
                        (
                            other.cell_ref.data_array.swap((None, Tag::None), Relaxed),
                            Tag::None,
                        ),
                        Relaxed,
                    );
                    debug_assert!(other.cell_ref.data_array.load(Relaxed, barrier).is_null());
                    debug_assert!(!self.cell_ref.data_array.load(Relaxed, barrier).is_null());
                    debug_assert_eq!(self.cell_ref.num_entries, 0);
                    self.num_entries_updated(1);
                    return true;
                }
            }
        }
        false
    }

    /// Notifies the [`Cell`] that an entry has been implicitly inherited.
    #[inline]
    pub fn entry_inherited(&self) {
        debug_assert_ne!(self.cell_ref.num_entries, 0);
        self.num_entries_updated(self.cell_ref.num_entries + 1);
    }

    /// Purges all the data.
    pub fn purge(&mut self, barrier: &Barrier) {
        if !self.cell_ref.data_array.load(Relaxed, barrier).is_null() {
            if let Some(data_array) = self.cell_ref.data_array.swap((None, Tag::None), Relaxed) {
                barrier.reclaim(data_array);
            }
        }
        self.killed = true;
        self.num_entries_updated(0);
    }

    /// Updates the number of entries.
    fn num_entries_updated(&self, num: u32) {
        #[allow(clippy::cast_ref_to_mut)]
        let cell_mut_ref =
            unsafe { &mut *(self.cell_ref as *const _ as *mut Cell<K, V, LOCK_FREE>) };
        cell_mut_ref.num_entries = num;
    }
}

impl<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> Drop for Locker<'b, K, V, LOCK_FREE> {
    #[inline]
    fn drop(&mut self) {
        let mut current = self.cell_ref.state.load(Relaxed);
        loop {
            let wakeup = (current & WAITING) == WAITING;
            let next = if self.killed {
                KILLED | (current & (!(WAITING | LOCK)))
            } else {
                current & (!(WAITING | LOCK))
            };
            match self
                .cell_ref
                .state
                .compare_exchange(current, next, Release, Relaxed)
            {
                Ok(_) => {
                    if wakeup {
                        self.cell_ref.wait_queue.signal();
                    }
                    break;
                }
                Err(result) => current = result,
            }
        }
    }
}

pub struct Reader<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    cell_ref: &'b Cell<K, V, LOCK_FREE>,
    killed: bool,
}

impl<'b, K: Eq, V, const LOCK_FREE: bool> Reader<'b, K, V, LOCK_FREE> {
    /// Locks the given [`Cell`].
    #[inline]
    pub fn lock(
        cell: &'b Cell<K, V, LOCK_FREE>,
        barrier: &'b Barrier,
    ) -> Option<Reader<'b, K, V, LOCK_FREE>> {
        loop {
            if let Some(reader) = Self::try_lock(cell, barrier).map_or_else(
                || {
                    cell.wait_queue.wait(|| {
                        // Marks that there is a waiting thread.
                        cell.state.fetch_or(WAITING, Release);
                        Self::try_lock(cell, barrier)
                    })
                },
                Some,
            ) {
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
    #[inline]
    fn try_lock(
        cell: &'b Cell<K, V, LOCK_FREE>,
        _barrier: &'b Barrier,
    ) -> Option<Reader<'b, K, V, LOCK_FREE>> {
        let current = cell.state.load(Relaxed);
        if (current & LOCK_MASK) >= SLOCK_MAX {
            return None;
        }
        if cell
            .state
            .compare_exchange(current, current + 1, Acquire, Relaxed)
            .is_ok()
        {
            return Some(Reader {
                cell_ref: cell,
                killed: (current & KILLED) == KILLED,
            });
        }
        None
    }

    /// Returns a reference to the [`Cell`].
    #[inline]
    pub fn cell_ref(&self) -> &Cell<K, V, LOCK_FREE> {
        self.cell_ref
    }
}

impl<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> Drop for Reader<'b, K, V, LOCK_FREE> {
    #[inline]
    fn drop(&mut self) {
        let mut current = self.cell_ref.state.load(Relaxed);
        loop {
            let wakeup = (current & WAITING) == WAITING;
            let next = (current - 1) & !(WAITING);
            match self
                .cell_ref
                .state
                .compare_exchange(current, next, Relaxed, Relaxed)
            {
                Ok(_) => {
                    if wakeup {
                        self.cell_ref.wait_queue.signal();
                    }
                    break;
                }
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
    use std::sync;
    use std::thread;

    #[test]
    fn cell_locker() {
        let num_threads = (ARRAY_SIZE * 2) as usize;
        let barrier = sync::Arc::new(sync::Barrier::new(num_threads));
        let cell: sync::Arc<Cell<usize, usize, true>> = sync::Arc::new(Cell::default());
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
                    let exclusive_locker = Locker::lock(&*cell_copied, &barrier).unwrap();
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
                            thread_id,
                            0,
                            (thread_id % ARRAY_SIZE).try_into().unwrap(),
                            &barrier,
                        );
                    } else {
                        assert_eq!(
                            exclusive_locker
                                .cell_ref()
                                .search(
                                    &thread_id,
                                    (thread_id % ARRAY_SIZE).try_into().unwrap(),
                                    &barrier
                                )
                                .unwrap(),
                            &(thread_id, 0_usize)
                        );
                    }
                    drop(exclusive_locker);

                    let read_locker = Reader::lock(&*cell_copied, &barrier).unwrap();
                    assert_eq!(
                        read_locker
                            .cell_ref()
                            .search(
                                &thread_id,
                                (thread_id % ARRAY_SIZE).try_into().unwrap(),
                                &barrier
                            )
                            .unwrap(),
                        &(thread_id, 0_usize)
                    );
                }
            }));
        }
        for handle in thread_handles {
            handle.join().unwrap();
        }
        let sum: u64 = data.iter().sum();
        assert_eq!(sum % 256, 0);
        assert_eq!(cell.num_entries(), num_threads);

        let epoch_barrier = Barrier::new();
        for thread_id in 0..ARRAY_SIZE {
            assert_eq!(
                cell.search(
                    &thread_id,
                    (thread_id % ARRAY_SIZE).try_into().unwrap(),
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

        let mut xlocker = Locker::lock(&*cell, &epoch_barrier).unwrap();
        xlocker.purge(&epoch_barrier);
        drop(xlocker);

        assert!(cell.killed());
        assert_eq!(cell.num_entries(), 0);
        assert!(Locker::lock(&*cell, &epoch_barrier).is_none());
    }
}
