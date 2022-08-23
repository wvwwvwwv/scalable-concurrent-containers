use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};
use crate::wait_queue::{AsyncWait, WaitQueue};

use std::borrow::Borrow;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::fence;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// The fixed size of the main [`DataArray`].
///
/// The size cannot exceed `32`.
pub const CELL_LEN: usize = 32;

/// Data block type.
pub type DataBlock<K, V> = [MaybeUninit<(K, V)>; CELL_LEN];

/// The fixed size of the linked [`DataArray`].
const LINKED_LEN: usize = CELL_LEN / 4;

/// State bits.
const KILLED: u32 = 1_u32 << 31;
const WAITING: u32 = 1_u32 << 30;
const LOCK: u32 = 1_u32 << 29;
const SLOCK_MAX: u32 = LOCK - 1;
const LOCK_MASK: u32 = LOCK | SLOCK_MAX;

/// [`Cell`] is a small fixed-size hash table that resolves hash conflicts using a linked list
/// of entry arrays.
pub(crate) struct Cell<K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    /// The state of the [`Cell`].
    state: AtomicU32,
    /// The number of valid entries in the [`Cell`].
    num_entries: u32,
    /// The metadata of the [`Cell`].
    metadata: Metadata<K, V, CELL_LEN>,
    /// The wait queue of the [`Cell`].
    wait_queue: WaitQueue,
}

impl<K: 'static + Eq, V: 'static, const LOCK_FREE: bool> Default for Cell<K, V, LOCK_FREE> {
    fn default() -> Self {
        Cell::<K, V, LOCK_FREE> {
            state: AtomicU32::new(0),
            num_entries: 0,
            metadata: Metadata::default(),
            wait_queue: WaitQueue::default(),
        }
    }
}

impl<K: 'static + Eq, V: 'static, const LOCK_FREE: bool> Cell<K, V, LOCK_FREE> {
    /// Returns true if the [`Cell`] has been killed.
    #[inline]
    pub(crate) fn killed(&self) -> bool {
        (self.state.load(Relaxed) & KILLED) == KILLED
    }

    /// Returns the number of entries in the [`Cell`].
    #[inline]
    pub(crate) fn num_entries(&self) -> usize {
        self.num_entries as usize
    }

    /// Returns `true` if the [`Cell`] requires to be rebuilt.
    #[inline]
    pub(crate) fn need_rebuild(&self) -> bool {
        self.metadata.removed_bitmap == (u32::MAX >> (32 - CELL_LEN))
    }

    /// Iterates the contents of the [`Cell`].
    #[inline]
    pub(crate) fn iter<'b>(&'b self, barrier: &'b Barrier) -> EntryIterator<'b, K, V, LOCK_FREE> {
        EntryIterator::new(self, barrier)
    }

    /// Searches for an entry associated with the given key.
    #[inline]
    pub(crate) fn search<'b, Q>(
        &'b self,
        data_block: &'b DataBlock<K, V>,
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

        if let Some((_, entry_ref)) =
            Self::search_array(&self.metadata, data_block, key_ref, partial_hash)
        {
            return Some(entry_ref);
        }

        let mut link_ptr = self.metadata.link.load(Acquire, barrier);
        while let Some(link) = link_ptr.as_ref() {
            if let Some((_, entry_ref)) =
                Self::search_array(&link.metadata, &link.data_array, key_ref, partial_hash)
            {
                return Some(entry_ref);
            }
            link_ptr = link.metadata.link.load(Acquire, barrier);
        }

        None
    }

    /// Gets an [`EntryIterator`] pointing to an entry associated with the given key.
    ///
    /// The returned [`EntryIterator`] always points to a valid entry.
    #[inline]
    pub(crate) fn get<'b, Q>(
        &'b self,
        data_block: &'b DataBlock<K, V>,
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

        if let Some((index, _)) =
            Self::search_array(&self.metadata, data_block, key_ref, partial_hash)
        {
            return Some(EntryIterator {
                cell: Some(self),
                current_link_ptr: Ptr::null(),
                prev_link_ptr: Ptr::null(),
                current_index: index,
                barrier_ref: barrier,
            });
        }

        let mut current_link_ptr = self.metadata.link.load(Acquire, barrier);
        let mut prev_link_ptr = Ptr::null();
        while let Some(link) = current_link_ptr.as_ref() {
            if let Some((index, _)) =
                Self::search_array(&link.metadata, &link.data_array, key_ref, partial_hash)
            {
                return Some(EntryIterator {
                    cell: Some(self),
                    current_link_ptr,
                    prev_link_ptr,
                    current_index: index,
                    barrier_ref: barrier,
                });
            }
            prev_link_ptr = current_link_ptr;
            current_link_ptr = link.metadata.link.load(Acquire, barrier);
        }

        None
    }

    /// Kills the [`Cell`] and drops entries in the given [`DataBlock`].
    #[inline]
    pub(crate) unsafe fn kill_and_drop(&self, data_block: &DataBlock<K, V>, barrier: &Barrier) {
        if !self.metadata.link.load(Acquire, barrier).is_null() {
            if let Some(link) = self.metadata.link.swap((None, Tag::None), Relaxed).0 {
                barrier.reclaim(link);
            }
        }
        self.state.store(KILLED, Relaxed);

        let mut bitmap = self.metadata.occupied_bitmap;
        let mut index = bitmap.trailing_zeros();
        while (index as usize) < CELL_LEN {
            let entry_mut_ptr = data_block[index as usize].as_ptr() as *mut (K, V);
            ptr::drop_in_place(entry_mut_ptr);
            bitmap &= !(1_u32 << index);
            index = bitmap.trailing_zeros();
        }
    }

    /// Searches the given [`DataArray`] for an entry matching the key.
    #[allow(clippy::cast_possible_truncation)]
    fn search_array<'b, Q, const LEN: usize>(
        metadata: &'b Metadata<K, V, LEN>,
        data_array: &'b [MaybeUninit<(K, V)>; LEN],
        key_ref: &Q,
        partial_hash: u8,
    ) -> Option<(usize, &'b (K, V))>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let mut bitmap = if LOCK_FREE {
            metadata.occupied_bitmap & (!metadata.removed_bitmap)
        } else {
            metadata.occupied_bitmap
        };
        if LOCK_FREE {
            fence(Acquire);
        }

        let len = LEN as u32;
        let preferred_index = u32::from(partial_hash) % len;
        if (bitmap & (1_u32 << preferred_index)) != 0 {
            let entry_ptr = data_array[preferred_index as usize].as_ptr();
            let entry_ref = unsafe { &(*entry_ptr) };
            if entry_ref.0.borrow() == key_ref {
                return Some((preferred_index as usize, entry_ref));
            }
        }

        bitmap = if LEN == 32 {
            bitmap.rotate_right(preferred_index) & (!1_u32)
        } else {
            u32::from((bitmap as u8).rotate_right(preferred_index) & (!1_u8))
        };
        let mut offset = bitmap.trailing_zeros();
        while offset != 32 {
            let index = (preferred_index + offset) % len;
            if metadata.partial_hash_array[index as usize] == partial_hash {
                let entry_ptr = data_array[index as usize].as_ptr();
                let entry_ref = unsafe { &(*entry_ptr) };
                if entry_ref.0.borrow() == key_ref {
                    return Some((index as usize, entry_ref));
                }
            }
            bitmap &= !(1_u32 << offset);
            offset = bitmap.trailing_zeros();
        }

        None
    }

    /// Searches for a next closest valid slot to the given slot in the [`DataArray`].
    ///
    /// If the given slot is valid, it returns the given slot.
    fn next_entry<Q, const LEN: usize>(
        metadata: &Metadata<K, V, LEN>,
        current_index: usize,
    ) -> Option<(usize, u8)>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        if current_index >= CELL_LEN {
            return None;
        }

        let bitmap = if LOCK_FREE {
            (metadata.occupied_bitmap & (!metadata.removed_bitmap))
                & (!((1_u32 << current_index) - 1))
        } else {
            metadata.occupied_bitmap & (!((1_u32 << current_index) - 1))
        };

        if LOCK_FREE {
            fence(Acquire);
        }

        let next_index = bitmap.trailing_zeros() as usize;
        if next_index < CELL_LEN {
            return Some((next_index, metadata.partial_hash_array[next_index]));
        }

        None
    }
}

pub struct EntryIterator<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    cell: Option<&'b Cell<K, V, LOCK_FREE>>,
    current_link_ptr: Ptr<'b, Linked<K, V, LINKED_LEN>>,
    prev_link_ptr: Ptr<'b, Linked<K, V, LINKED_LEN>>,
    current_index: usize,
    barrier_ref: &'b Barrier,
}

impl<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> EntryIterator<'b, K, V, LOCK_FREE> {
    /// Creates a new [`EntryIterator`].
    #[inline]
    pub(crate) fn new(
        cell: &'b Cell<K, V, LOCK_FREE>,
        barrier: &'b Barrier,
    ) -> EntryIterator<'b, K, V, LOCK_FREE> {
        EntryIterator {
            cell: Some(cell),
            current_link_ptr: Ptr::null(),
            prev_link_ptr: Ptr::null(),
            current_index: usize::MAX,
            barrier_ref: barrier,
        }
    }

    /// Gets a reference to the key-value pair.
    #[inline]
    pub(crate) fn get(&self, data_block: &'b DataBlock<K, V>) -> &'b (K, V) {
        let entry_ptr = if let Some(data_array_ref) = self.current_link_ptr.as_ref() {
            data_array_ref.data_array[self.current_index].as_ptr()
        } else {
            data_block[self.current_index].as_ptr()
        };
        unsafe { &(*entry_ptr) }
    }

    /// Tries to remove the current data array from the linked list.
    ///
    /// It should only be invoked when the caller is holding a [`Locker`] on the [`Cell`].
    fn unlink_data_array(&mut self, data_array_ref: &Linked<K, V, LINKED_LEN>) {
        let next_data_array = if LOCK_FREE {
            data_array_ref
                .metadata
                .link
                .get_arc(Relaxed, self.barrier_ref)
        } else {
            data_array_ref
                .metadata
                .link
                .swap((None, Tag::None), Relaxed)
                .0
        };
        self.current_link_ptr = next_data_array
            .as_ref()
            .map_or_else(Ptr::null, |n| n.ptr(self.barrier_ref));
        let old_data_array = if let Some(prev_data_array_ref) = self.prev_link_ptr.as_ref() {
            prev_data_array_ref
                .metadata
                .link
                .swap((next_data_array, Tag::None), Relaxed)
                .0
        } else if let Some(cell) = self.cell.as_ref() {
            cell.metadata
                .link
                .swap((next_data_array, Tag::None), Relaxed)
                .0
        } else {
            None
        };
        if let Some(data_array) = old_data_array {
            self.barrier_ref.reclaim(data_array);
        }
        if self.current_link_ptr.is_null() {
            self.cell.take();
        } else {
            self.current_index = usize::MAX;
        }
    }
}

impl<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> EntryIterator<'b, K, V, LOCK_FREE> {
    fn next_entry<const LEN: usize>(&mut self, metadata: &'b Metadata<K, V, LEN>) -> Option<u8> {
        // Search for the next valid entry.
        let current_index = if self.current_index == usize::MAX {
            0
        } else {
            self.current_index + 1
        };
        if let Some((index, partial_hash)) =
            Cell::<K, V, LOCK_FREE>::next_entry(metadata, current_index)
        {
            self.current_index = index;
            return Some(partial_hash);
        }

        self.prev_link_ptr = self.current_link_ptr;
        self.current_link_ptr = metadata.link.load(Acquire, self.barrier_ref);
        self.current_index = usize::MAX;

        None
    }
}

impl<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> Iterator
    for EntryIterator<'b, K, V, LOCK_FREE>
{
    type Item = u8;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(&cell) = self.cell.as_ref() {
            if self.current_link_ptr.is_null() {
                if let Some(partial_hash) = self.next_entry(&cell.metadata) {
                    return Some(partial_hash);
                }
            }
            while let Some(link) = self.current_link_ptr.as_ref() {
                if let Some(partial_hash) = self.next_entry(&link.metadata) {
                    return Some(partial_hash);
                }
            }
            // Fuse itself.
            self.cell.take();
        }
        None
    }
}

pub struct Locker<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    cell: &'b Cell<K, V, LOCK_FREE>,
}

impl<'b, K: Eq, V, const LOCK_FREE: bool> Locker<'b, K, V, LOCK_FREE> {
    /// Locks the [`Cell`].
    #[inline]
    pub(crate) fn lock(
        cell: &'b Cell<K, V, LOCK_FREE>,
        barrier: &'b Barrier,
    ) -> Option<Locker<'b, K, V, LOCK_FREE>> {
        loop {
            if let Ok(locker) = Self::try_lock(cell, barrier) {
                return locker;
            }
            if let Ok(locker) = cell.wait_queue.wait_sync(|| {
                // Mark that there is a waiting thread.
                cell.state.fetch_or(WAITING, Release);
                Self::try_lock(cell, barrier)
            }) {
                return locker;
            }
        }
    }

    /// Tries to lock the [`Cell`], and if it fails, pushes an [`AsyncWait`].
    #[inline]
    pub(crate) fn try_lock_or_wait(
        cell: &'b Cell<K, V, LOCK_FREE>,
        async_wait: *mut AsyncWait,
        barrier: &'b Barrier,
    ) -> Result<Option<Locker<'b, K, V, LOCK_FREE>>, ()> {
        if let Ok(locker) = Self::try_lock(cell, barrier) {
            return Ok(locker);
        }
        cell.wait_queue.push_async_entry(async_wait, || {
            // Mark that there is a waiting thread.
            cell.state.fetch_or(WAITING, Release);
            Self::try_lock(cell, barrier)
        })
    }

    /// Returns a reference to the [`Cell`].
    #[inline]
    pub(crate) fn cell(&self) -> &'b Cell<K, V, LOCK_FREE> {
        self.cell
    }

    /// Inserts a new key-value pair into the [`Cell`] without a uniqueness check.
    #[inline]
    pub(crate) fn insert(
        &'b self,
        data_block: &'b DataBlock<K, V>,
        key: K,
        value: V,
        partial_hash: u8,
        barrier: &'b Barrier,
    ) {
        assert!(self.cell.num_entries != u32::MAX, "array overflow");

        let preferred_index = partial_hash as usize % CELL_LEN;
        if (self.cell.metadata.occupied_bitmap & (1_u32 << preferred_index)) == 0 {
            self.insert_entry(
                &mut self.cell_mut().metadata,
                data_block,
                preferred_index,
                key,
                value,
                partial_hash,
            );
            return;
        }
        let free_index =
            Self::get_free_index::<CELL_LEN>(self.cell.metadata.occupied_bitmap, preferred_index);
        if free_index != CELL_LEN {
            self.insert_entry(
                &mut self.cell_mut().metadata,
                data_block,
                free_index,
                key,
                value,
                partial_hash,
            );
            return;
        }

        let preferred_index = partial_hash as usize % LINKED_LEN;
        let mut link_ptr = self.cell.metadata.link.load(Acquire, barrier).as_raw()
            as *mut Linked<K, V, LINKED_LEN>;
        while let Some(link) = unsafe { link_ptr.as_mut() } {
            #[allow(clippy::cast_ref_to_mut)]
            let link_mut = unsafe {
                &mut *(link as *const Linked<K, V, LINKED_LEN> as *mut Linked<K, V, LINKED_LEN>)
            };
            if (link.metadata.occupied_bitmap & (1_u32 << preferred_index)) == 0 {
                self.insert_entry(
                    &mut link_mut.metadata,
                    &link.data_array,
                    preferred_index,
                    key,
                    value,
                    partial_hash,
                );
                return;
            }
            let free_index =
                Self::get_free_index::<LINKED_LEN>(link.metadata.occupied_bitmap, preferred_index);
            if free_index != LINKED_LEN {
                self.insert_entry(
                    &mut link_mut.metadata,
                    &link.data_array,
                    free_index,
                    key,
                    value,
                    partial_hash,
                );
                return;
            }

            link_ptr =
                link.metadata.link.load(Acquire, barrier).as_raw() as *mut Linked<K, V, LINKED_LEN>;
        }

        // Insert a new `DataArray` at the linked list head.
        let new_link = Arc::new(Linked::new());
        let new_link_mut = unsafe { &mut *(new_link.as_ptr() as *mut Linked<K, V, LINKED_LEN>) };
        self.insert_entry(
            &mut new_link_mut.metadata,
            &new_link.data_array,
            preferred_index,
            key,
            value,
            partial_hash,
        );
        new_link.metadata.link.swap(
            (self.cell.metadata.link.get_arc(Relaxed, barrier), Tag::None),
            Relaxed,
        );
        self.cell
            .metadata
            .link
            .swap((Some(new_link), Tag::None), Release);
    }

    /// Removes a key-value pair being pointed by the given [`EntryIterator`].
    #[inline]
    pub(crate) fn erase(
        &self,
        data_block: &'b DataBlock<K, V>,
        iter: &mut EntryIterator<K, V, LOCK_FREE>,
    ) -> Option<(K, V)> {
        if iter.current_index == usize::MAX {
            return None;
        }
        if let Some(link) = iter.current_link_ptr.as_ref() {
            #[allow(clippy::cast_ref_to_mut)]
            let link_mut = unsafe {
                &mut *(link as *const Linked<K, V, LINKED_LEN> as *mut Linked<K, V, LINKED_LEN>)
            };
            let result =
                self.erase_entry(&mut link_mut.metadata, &link.data_array, iter.current_index);
            if (LOCK_FREE && (link.metadata.occupied_bitmap & (!link.metadata.removed_bitmap)) == 0)
                || (!LOCK_FREE && link.metadata.occupied_bitmap == 0)
            {
                iter.unlink_data_array(link);
            }
            result
        } else {
            self.erase_entry(
                &mut self.cell_mut().metadata,
                data_block,
                iter.current_index,
            )
        }
    }

    /// Extracts the key-value pair being pointed by `self`.
    #[inline]
    pub(crate) fn extract(
        &self,
        data_block: &'b DataBlock<K, V>,
        iter: &mut EntryIterator<K, V, LOCK_FREE>,
    ) -> (K, V) {
        debug_assert!(!LOCK_FREE);
        if let Some(link) = iter.current_link_ptr.as_ref() {
            #[allow(clippy::cast_ref_to_mut)]
            let link_mut = unsafe {
                &mut *(link as *const Linked<K, V, LINKED_LEN> as *mut Linked<K, V, LINKED_LEN>)
            };
            let extracted =
                self.extract_entry(&mut link_mut.metadata, &link.data_array, iter.current_index);
            if link.metadata.occupied_bitmap == 0 {
                iter.unlink_data_array(link_mut);
            }
            extracted
        } else {
            self.extract_entry(
                &mut self.cell_mut().metadata,
                data_block,
                iter.current_index,
            )
        }
    }

    /// Purges all the data.
    #[inline]
    pub(crate) fn purge(&mut self, barrier: &Barrier) {
        if LOCK_FREE {
            self.cell_mut().metadata.removed_bitmap = self.cell.metadata.occupied_bitmap;
        }
        self.cell.state.fetch_or(KILLED, Release);
        self.num_entries_updated(0);
        if !self.cell.metadata.link.load(Acquire, barrier).is_null() {
            if let Some(data_array) = self.cell.metadata.link.swap((None, Tag::None), Relaxed).0 {
                barrier.reclaim(data_array);
            }
        }
    }

    /// Gets the most optimal free slot index from the given bitmap.
    fn get_free_index<const LEN: usize>(bitmap: u32, preferred_index: usize) -> usize {
        let mut free_index = (bitmap | ((1_u32 << preferred_index) - 1)).trailing_ones() as usize;
        if free_index == LEN {
            free_index = bitmap.trailing_ones() as usize;
        }
        free_index
    }

    /// Inserts a key-value pair in the slot.
    #[allow(clippy::too_many_arguments)]
    fn insert_entry<const LEN: usize>(
        &self,
        metadata: &mut Metadata<K, V, LEN>,
        data_array: &[MaybeUninit<(K, V)>; LEN],
        index: usize,
        key: K,
        value: V,
        partial_hash: u8,
    ) {
        debug_assert!(index < LEN);

        unsafe {
            (data_array[index].as_ptr() as *mut (K, V)).write((key, value));
            metadata.partial_hash_array[index] = partial_hash;
            if LOCK_FREE {
                fence(Release);
            }
            metadata.occupied_bitmap |= 1_u32 << index;
        }
        self.num_entries_updated(self.cell.num_entries + 1);
    }

    /// Removes a key-value pair in the slot.
    fn erase_entry<const LEN: usize>(
        &self,
        metadata: &'b mut Metadata<K, V, LEN>,
        data_array: &'b [MaybeUninit<(K, V)>; LEN],
        index: usize,
    ) -> Option<(K, V)> {
        debug_assert!(index < LEN);

        if metadata.occupied_bitmap & (1_u32 << index) == 0 {
            return None;
        }

        if LOCK_FREE && (metadata.removed_bitmap & (1_u32 << index)) != 0 {
            return None;
        }

        self.num_entries_updated(self.cell.num_entries - 1);
        if LOCK_FREE {
            metadata.removed_bitmap |= 1_u32 << index;
            None
        } else {
            metadata.occupied_bitmap &= !(1_u32 << index);
            let entry_ptr = data_array[index].as_ptr() as *mut (K, V);
            #[allow(clippy::uninit_assumed_init)]
            Some(unsafe { ptr::replace(entry_ptr, MaybeUninit::uninit().assume_init()) })
        }
    }

    /// Extracts and removes the key-value pair in the slot.
    fn extract_entry<const LEN: usize>(
        &self,
        metadata: &'b mut Metadata<K, V, LEN>,
        data_array: &'b [MaybeUninit<(K, V)>; LEN],
        index: usize,
    ) -> (K, V) {
        debug_assert!(index < LEN);

        self.num_entries_updated(self.cell.num_entries - 1);
        metadata.occupied_bitmap &= !(1_u32 << index);
        let entry_ptr = data_array[index].as_ptr() as *mut (K, V);
        unsafe { ptr::read(entry_ptr) }
    }

    /// Updates the number of entries.
    fn num_entries_updated(&self, num: u32) {
        self.cell_mut().num_entries = num;
    }

    /// Returns a mutable reference to the `Cell`.
    #[allow(clippy::mut_from_ref)]
    fn cell_mut(&self) -> &mut Cell<K, V, LOCK_FREE> {
        #[allow(clippy::cast_ref_to_mut)]
        unsafe {
            &mut *(self.cell as *const _ as *mut Cell<K, V, LOCK_FREE>)
        }
    }

    /// Tries to lock the [`Cell`].
    fn try_lock(
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
}

impl<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> Drop for Locker<'b, K, V, LOCK_FREE> {
    #[inline]
    fn drop(&mut self) {
        let mut current = self.cell.state.load(Relaxed);
        loop {
            let wakeup = (current & WAITING) == WAITING;
            match self.cell.state.compare_exchange(
                current,
                current & (!(WAITING | LOCK)),
                Release,
                Relaxed,
            ) {
                Ok(_) => {
                    if wakeup {
                        self.cell.wait_queue.signal();
                    }
                    break;
                }
                Err(result) => current = result,
            }
        }
    }
}

pub struct Reader<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    cell: &'b Cell<K, V, LOCK_FREE>,
}

impl<'b, K: Eq, V, const LOCK_FREE: bool> Reader<'b, K, V, LOCK_FREE> {
    /// Locks the given [`Cell`].
    #[inline]
    pub(crate) fn lock(
        cell: &'b Cell<K, V, LOCK_FREE>,
        barrier: &'b Barrier,
    ) -> Option<Reader<'b, K, V, LOCK_FREE>> {
        loop {
            if let Ok(reader) = Self::try_lock(cell, barrier) {
                return reader;
            }
            if let Ok(reader) = cell.wait_queue.wait_sync(|| {
                // Mark that there is a waiting thread.
                cell.state.fetch_or(WAITING, Release);
                Self::try_lock(cell, barrier)
            }) {
                return reader;
            }
        }
    }

    /// Tries to lock the [`Cell`], and if it fails, pushes an [`AsyncWait`].
    #[inline]
    pub(crate) fn try_lock_or_wait(
        cell: &'b Cell<K, V, LOCK_FREE>,
        async_wait: *mut AsyncWait,
        barrier: &'b Barrier,
    ) -> Result<Option<Reader<'b, K, V, LOCK_FREE>>, ()> {
        if let Ok(reader) = Self::try_lock(cell, barrier) {
            return Ok(reader);
        }
        cell.wait_queue.push_async_entry(async_wait, || {
            // Mark that there is a waiting thread.
            cell.state.fetch_or(WAITING, Release);
            Self::try_lock(cell, barrier)
        })
    }

    /// Returns a reference to the [`Cell`].
    #[inline]
    pub(crate) fn cell(&self) -> &'b Cell<K, V, LOCK_FREE> {
        self.cell
    }

    /// Tries to lock the [`Cell`].
    fn try_lock(
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
}

impl<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> Drop for Reader<'b, K, V, LOCK_FREE> {
    #[inline]
    fn drop(&mut self) {
        let mut current = self.cell.state.load(Relaxed);
        loop {
            let wakeup = (current & WAITING) == WAITING;
            let next = (current - 1) & !(WAITING);
            match self
                .cell
                .state
                .compare_exchange(current, next, Relaxed, Relaxed)
            {
                Ok(_) => {
                    if wakeup {
                        self.cell.wait_queue.signal();
                    }
                    break;
                }
                Err(result) => current = result,
            }
        }
    }
}

/// [`Metadata`] is a collection of metadata fields of [`Cell`] and [`Linked`].
pub(crate) struct Metadata<K: 'static + Eq, V: 'static, const LEN: usize> {
    /// Linked list of entries.
    link: AtomicArc<Linked<K, V, LINKED_LEN>>,
    /// Bitmap for occupied slots.
    occupied_bitmap: u32,
    /// Bitmap for removed slots.
    removed_bitmap: u32,
    /// Partial hash map.
    partial_hash_array: [u8; LEN],
}

impl<K: 'static + Eq, V: 'static, const LEN: usize> Default for Metadata<K, V, LEN> {
    fn default() -> Self {
        Self {
            link: AtomicArc::default(),
            occupied_bitmap: 0,
            removed_bitmap: 0,
            partial_hash_array: [0; LEN],
        }
    }
}

/// [`DataArray`] is a fixed size array of key-value pairs.
///
/// `LEN` can only be `8` or `32`.
pub(crate) struct Linked<K: 'static + Eq, V: 'static, const LEN: usize> {
    metadata: Metadata<K, V, LEN>,
    data_array: [MaybeUninit<(K, V)>; LEN],
}

impl<K: 'static + Eq, V: 'static, const LEN: usize> Linked<K, V, LEN> {
    fn new() -> Linked<K, V, LEN> {
        Linked {
            metadata: Metadata::default(),
            data_array: unsafe { MaybeUninit::uninit().assume_init() },
        }
    }
}

impl<K: 'static + Eq, V: 'static, const LEN: usize> Drop for Linked<K, V, LEN> {
    fn drop(&mut self) {
        let mut index = self.metadata.occupied_bitmap.trailing_zeros();
        while (index as usize) < LEN {
            let entry_mut_ptr = self.data_array[index as usize].as_mut_ptr();
            unsafe {
                ptr::drop_in_place(entry_mut_ptr);
            }
            self.metadata.occupied_bitmap &= !(1_u32 << index);
            index = self.metadata.occupied_bitmap.trailing_zeros();
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
        let num_tasks = CELL_LEN + 2;
        let barrier = Arc::new(sync::Barrier::new(num_tasks));
        let data_block: Arc<DataBlock<usize, usize>> =
            Arc::new(unsafe { MaybeUninit::uninit().assume_init() });
        let cell: Arc<Cell<usize, usize, true>> = Arc::new(Cell::default());
        let mut data: [u64; 128] = [0; 128];
        let mut task_handles = Vec::with_capacity(num_tasks);
        for task_id in 0..num_tasks {
            let barrier_copied = barrier.clone();
            let data_block_copied = data_block.clone();
            let cell_copied = cell.clone();
            let data_ptr = AtomicPtr::new(&mut data);
            task_handles.push(tokio::spawn(async move {
                barrier_copied.wait().await;
                let barrier = Barrier::new();
                for i in 0..2048 {
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
                            &data_block_copied,
                            task_id,
                            0,
                            (task_id % CELL_LEN).try_into().unwrap(),
                            &barrier,
                        );
                    } else {
                        assert_eq!(
                            exclusive_locker
                                .cell()
                                .search(
                                    &data_block_copied,
                                    &task_id,
                                    (task_id % CELL_LEN).try_into().unwrap(),
                                    &barrier
                                )
                                .unwrap(),
                            &(task_id, 0_usize)
                        );
                    }
                    drop(exclusive_locker);

                    let read_locker = Reader::lock(&*cell_copied, &barrier).unwrap();
                    assert_eq!(
                        read_locker
                            .cell()
                            .search(
                                &data_block_copied,
                                &task_id,
                                (task_id % CELL_LEN).try_into().unwrap(),
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
                    &data_block,
                    &task_id,
                    (task_id % CELL_LEN).try_into().unwrap(),
                    &epoch_barrier
                ),
                Some(&(task_id, 0))
            );
        }
        assert_eq!(cell.num_entries(), cell.iter(&epoch_barrier).count());

        let mut xlocker = Locker::lock(&*cell, &epoch_barrier).unwrap();
        xlocker.purge(&epoch_barrier);
        drop(xlocker);

        assert!(cell.killed());
        assert_eq!(cell.num_entries(), 0);
        assert!(Locker::lock(&*cell, &epoch_barrier).is_none());
    }
}
