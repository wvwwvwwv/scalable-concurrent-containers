use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};
use crate::wait_queue::{AsyncWait, WaitQueue};

use std::borrow::Borrow;
use std::mem::{needs_drop, MaybeUninit};
use std::ptr::{self, NonNull};
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{fence, AtomicU32};

/// The fixed size of the bitmap.
///
/// The size cannot exceed `32`.
pub const CELL_LEN: usize = 32;

/// Data block type.
pub type DataBlock<K, V, const LEN: usize> = [MaybeUninit<(K, V)>; LEN];

/// The fixed size of the linked data block.
const LINKED_LEN: usize = CELL_LEN / 4;

/// State bits.
const KILLED: u32 = 1_u32 << 31;
const WAITING: u32 = 1_u32 << 30;
const LOCK: u32 = 1_u32 << 29;
const SLOCK_MAX: u32 = LOCK - 1;
const LOCK_MASK: u32 = LOCK | SLOCK_MAX;

/// [`Cell`] is a small fixed-size hash table that resolves hash conflicts using a linked list
/// of data blocks.
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
    ///
    /// If `LOCK_FREE == true`, removed entries are not dropped occupying the slots, therefore
    /// rebuilding the [`Cell`] might be needed to keep the data structure as small as possible.
    #[inline]
    pub(crate) fn need_rebuild(&self) -> bool {
        self.metadata.removed_bitmap == (u32::MAX >> (32 - CELL_LEN))
    }

    /// Searches for an entry associated with the given key.
    #[inline]
    pub(crate) fn search<'b, Q>(
        &'b self,
        data_block: &'b DataBlock<K, V, CELL_LEN>,
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

        if let Some((_, entry_ref)) =
            Self::search_entry(&self.metadata, data_block, key, partial_hash)
        {
            return Some(entry_ref);
        }

        let mut link_ptr = self.metadata.link.load(Acquire, barrier);
        while let Some(link) = link_ptr.as_ref() {
            if let Some((_, entry_ref)) =
                Self::search_entry(&link.metadata, &link.data_block, key, partial_hash)
            {
                return Some(entry_ref);
            }
            link_ptr = link.metadata.link.load(Acquire, barrier);
        }

        None
    }

    /// Drops entries in the given [`DataBlock`].
    ///
    /// The [`Cell`] and the [`DataBlock`] should never be used afterwards.
    #[inline]
    pub(crate) unsafe fn drop_entries(
        &mut self,
        data_block: &DataBlock<K, V, CELL_LEN>,
        barrier: &Barrier,
    ) {
        if !self.metadata.link.load(Acquire, barrier).is_null() {
            if let Some(link) = self.metadata.link.swap((None, Tag::None), Relaxed).0 {
                let _ = link.release(barrier);
            }
        }
        if needs_drop::<(K, V)>() && self.metadata.occupied_bitmap != 0 {
            let mut bitmap = self.metadata.occupied_bitmap;
            let mut index = bitmap.trailing_zeros();
            while index != 32 {
                let entry_mut_ptr = data_block[index as usize].as_ptr() as *mut (K, V);
                ptr::drop_in_place(entry_mut_ptr);
                bitmap &= !(1_u32 << index);
                index = bitmap.trailing_zeros();
            }
        }
    }

    /// Gets an [`EntryPtr`] pointing to an entry associated with the given key.
    #[inline]
    fn get<'b, Q>(
        &self,
        data_block: &'b DataBlock<K, V, CELL_LEN>,
        key: &Q,
        partial_hash: u8,
        barrier: &'b Barrier,
    ) -> EntryPtr<'b, K, V, LOCK_FREE>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        if self.num_entries == 0 {
            return EntryPtr::new(barrier);
        }

        if let Some((index, _)) = Self::search_entry(&self.metadata, data_block, key, partial_hash)
        {
            return EntryPtr {
                current_link_ptr: Ptr::null(),
                current_index: index,
            };
        }

        let mut current_link_ptr = self.metadata.link.load(Acquire, barrier);
        while let Some(link) = current_link_ptr.as_ref() {
            if let Some((index, _)) =
                Self::search_entry(&link.metadata, &link.data_block, key, partial_hash)
            {
                return EntryPtr {
                    current_link_ptr,
                    current_index: index,
                };
            }
            current_link_ptr = link.metadata.link.load(Acquire, barrier);
        }

        EntryPtr::new(barrier)
    }

    /// Searches the given data block for an entry matching the key.
    #[allow(clippy::cast_possible_truncation)]
    fn search_entry<'b, Q, const LEN: usize>(
        metadata: &'b Metadata<K, V, LEN>,
        data_block: &'b DataBlock<K, V, LEN>,
        key: &Q,
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

        let preferred_index = usize::from(partial_hash) % LEN;
        if (bitmap & (1_u32 << preferred_index)) != 0 {
            let entry_ref = unsafe { &(*data_block[preferred_index].as_ptr()) };
            if entry_ref.0.borrow() == key {
                return Some((preferred_index, entry_ref));
            }
        }

        bitmap = if LEN == CELL_LEN {
            bitmap.rotate_right(preferred_index as u32) & (!1_u32)
        } else {
            u32::from((bitmap as u8).rotate_right(preferred_index as u32) & (!1_u8))
        };
        let mut offset = bitmap.trailing_zeros();
        while offset != 32 {
            let index = (preferred_index + offset as usize) % LEN;
            if metadata.partial_hash_array[index] == partial_hash {
                let entry_ref = unsafe { &(*data_block[index].as_ptr()) };
                if entry_ref.0.borrow() == key {
                    return Some((index, entry_ref));
                }
            }
            bitmap &= !(1_u32 << offset);
            offset = bitmap.trailing_zeros();
        }

        None
    }

    /// Searches for a next closest valid entry slot number from the current one in the bitmap.
    ///
    /// If the specified slot is valid, it returns the specified one.
    fn next_entry<Q, const LEN: usize>(
        metadata: &Metadata<K, V, LEN>,
        current_index: usize,
    ) -> Option<usize>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        if current_index >= LEN {
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
        if next_index < LEN {
            return Some(next_index);
        }

        None
    }
}

pub struct EntryPtr<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    current_link_ptr: Ptr<'b, Linked<K, V, LINKED_LEN>>,
    current_index: usize,
}

impl<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> EntryPtr<'b, K, V, LOCK_FREE> {
    /// Creates a new [`EntryPtr`].
    #[inline]
    pub(crate) fn new(_barrier: &'b Barrier) -> EntryPtr<'b, K, V, LOCK_FREE> {
        EntryPtr {
            current_link_ptr: Ptr::null(),
            current_index: CELL_LEN,
        }
    }

    /// Returns `true` if the [`EntryPtr`] points to a valid entry or fused.
    #[inline]
    pub(crate) fn is_valid(&self) -> bool {
        self.current_index != CELL_LEN
    }

    /// Moves the [`EntryPtr`] to point to the next valid entry.
    ///
    /// Returns `true` if it successfully found the next valid entry.
    #[inline]
    pub(crate) fn next(&mut self, cell: &Cell<K, V, LOCK_FREE>, barrier: &'b Barrier) -> bool {
        if self.current_index != usize::MAX {
            if self.current_link_ptr.is_null() && self.next_entry(&cell.metadata, barrier) {
                return true;
            }
            while let Some(link) = self.current_link_ptr.as_ref() {
                if self.next_entry(&link.metadata, barrier) {
                    return true;
                }
            }

            // Fuse itself.
            self.current_index = usize::MAX;
        }

        false
    }

    /// Gets a reference to the entry.
    ///
    /// The [`EntryPtr`] must point to a valid entry.
    #[inline]
    pub(crate) fn get(&self, data_block: &'b DataBlock<K, V, CELL_LEN>) -> &'b (K, V) {
        debug_assert_ne!(self.current_index, usize::MAX);
        let entry_ptr = if let Some(link) = self.current_link_ptr.as_ref() {
            link.data_block[self.current_index].as_ptr()
        } else {
            data_block[self.current_index].as_ptr()
        };
        unsafe { &(*entry_ptr) }
    }

    /// Gets a mutable reference to the entry.
    ///
    /// The [`EntryPtr`] must point to a valid entry, and the associated [`Cell`] must be locked.
    #[allow(clippy::mut_from_ref)]
    #[inline]
    pub(crate) fn get_mut(
        &mut self,
        data_block: &'b DataBlock<K, V, CELL_LEN>,
        _locker: &mut Locker<K, V, LOCK_FREE>,
    ) -> &'b mut (K, V) {
        debug_assert_ne!(self.current_index, usize::MAX);
        let entry_ptr = if let Some(link) = self.current_link_ptr.as_ref() {
            link.data_block[self.current_index].as_ptr() as *mut (K, V)
        } else {
            data_block[self.current_index].as_ptr() as *mut (K, V)
        };
        unsafe { &mut (*entry_ptr) }
    }

    /// Gets the partial hash value of the entry.
    ///
    /// The [`EntryPtr`] must point to a valid entry.
    #[inline]
    pub(crate) fn partial_hash(&self, cell: &Cell<K, V, LOCK_FREE>) -> u8 {
        debug_assert_ne!(self.current_index, usize::MAX);
        if let Some(link) = self.current_link_ptr.as_ref() {
            link.metadata.partial_hash_array[self.current_index]
        } else {
            cell.metadata.partial_hash_array[self.current_index]
        }
    }

    /// Tries to remove [`Link`] from the linked list.
    ///
    /// It should only be invoked when the caller is holding a [`Locker`] on the [`Cell`].
    fn unlink(
        &mut self,
        locker: &Locker<K, V, LOCK_FREE>,
        link: &Linked<K, V, LINKED_LEN>,
        barrier: &'b Barrier,
    ) {
        let prev_link_ptr = link.prev_link.load(Relaxed);
        let next_link = if LOCK_FREE {
            link.metadata.link.get_arc(Relaxed, barrier)
        } else {
            link.metadata.link.swap((None, Tag::None), Relaxed).0
        };
        if let Some(next_link) = next_link.as_ref() {
            next_link.prev_link.store(prev_link_ptr, Relaxed);
        }

        self.current_link_ptr = next_link
            .as_ref()
            .map_or_else(Ptr::null, |n| n.ptr(barrier));
        let old_link = if let Some(prev_link) = unsafe { prev_link_ptr.as_ref() } {
            prev_link
                .metadata
                .link
                .swap((next_link, Tag::None), Relaxed)
                .0
        } else {
            locker
                .cell
                .metadata
                .link
                .swap((next_link, Tag::None), Relaxed)
                .0
        };
        if let Some(link) = old_link {
            let _ = link.release(barrier);
        }

        if self.current_link_ptr.is_null() {
            // Fuse the pointer.
            self.current_index = usize::MAX;
        } else {
            // Go to the next `Link`.
            self.current_index = LINKED_LEN;
        }
    }

    /// Moves the [`EntryPointer`] to the next valid entry in the [`Cell`].
    fn next_entry<const LEN: usize>(
        &mut self,
        metadata: &Metadata<K, V, LEN>,
        barrier: &'b Barrier,
    ) -> bool {
        // Search for the next valid entry.
        let current_index = if self.current_index == LEN {
            0
        } else {
            self.current_index + 1
        };
        if let Some(index) = Cell::<K, V, LOCK_FREE>::next_entry(metadata, current_index) {
            self.current_index = index;
            return true;
        }

        self.current_link_ptr = metadata.link.load(Acquire, barrier);
        self.current_index = LINKED_LEN;

        false
    }
}

pub struct Locker<'b, K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    cell: &'b mut Cell<K, V, LOCK_FREE>,
}

impl<'b, K: Eq, V, const LOCK_FREE: bool> Locker<'b, K, V, LOCK_FREE> {
    /// Locks the [`Cell`].
    #[inline]
    pub(crate) fn lock(
        cell: &'b mut Cell<K, V, LOCK_FREE>,
        barrier: &'b Barrier,
    ) -> Option<Locker<'b, K, V, LOCK_FREE>> {
        let cell_ptr = cell as *mut Cell<K, V, LOCK_FREE>;
        loop {
            if let Ok(locker) = Self::try_lock(unsafe { &mut *cell_ptr }, barrier) {
                return locker;
            }
            if let Ok(locker) = unsafe { &*cell_ptr }.wait_queue.wait_sync(|| {
                // Mark that there is a waiting thread.
                cell.state.fetch_or(WAITING, Release);
                Self::try_lock(unsafe { &mut *cell_ptr }, barrier)
            }) {
                return locker;
            }
        }
    }

    /// Tries to lock the [`Cell`], and if it fails, pushes an [`AsyncWait`].
    #[inline]
    pub(crate) fn try_lock_or_wait(
        cell: &'b mut Cell<K, V, LOCK_FREE>,
        async_wait: NonNull<AsyncWait>,
        barrier: &'b Barrier,
    ) -> Result<Option<Locker<'b, K, V, LOCK_FREE>>, ()> {
        let cell_ptr = cell as *mut Cell<K, V, LOCK_FREE>;
        if let Ok(locker) = Self::try_lock(unsafe { &mut *cell_ptr }, barrier) {
            return Ok(locker);
        }
        unsafe { &*cell_ptr }
            .wait_queue
            .push_async_entry(async_wait, || {
                // Mark that there is a waiting thread.
                cell.state.fetch_or(WAITING, Release);
                Self::try_lock(cell, barrier)
            })
    }

    /// Returns a reference to the [`Cell`].
    #[inline]
    pub(crate) fn cell(&self) -> &Cell<K, V, LOCK_FREE> {
        self.cell
    }

    /// Gets an [`EntryPtr`] pointing to an entry associated with the given key.
    ///
    /// The returned [`EntryPtr`] points to a valid entry if the key is found.
    #[inline]
    pub(crate) fn get<Q>(
        &self,
        data_block: &'b DataBlock<K, V, CELL_LEN>,
        key: &Q,
        partial_hash: u8,
        barrier: &'b Barrier,
    ) -> EntryPtr<'b, K, V, LOCK_FREE>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        self.cell.get(data_block, key, partial_hash, barrier)
    }

    /// Inserts a new key-value pair into the [`Cell`] without a uniqueness check.
    #[inline]
    pub(crate) fn insert(
        &mut self,
        data_block: &DataBlock<K, V, CELL_LEN>,
        key: K,
        value: V,
        partial_hash: u8,
        barrier: &Barrier,
    ) {
        assert!(self.cell.num_entries != u32::MAX, "array overflow");

        let preferred_index = partial_hash as usize % CELL_LEN;
        if (self.cell.metadata.occupied_bitmap & (1_u32 << preferred_index)) == 0 {
            Self::insert_entry(
                &mut self.cell.metadata,
                data_block,
                preferred_index,
                key,
                value,
                partial_hash,
            );
            self.cell.num_entries += 1;
            return;
        }
        let free_index =
            Self::get_free_index::<CELL_LEN>(self.cell.metadata.occupied_bitmap, preferred_index);
        if free_index != CELL_LEN {
            Self::insert_entry(
                &mut self.cell.metadata,
                data_block,
                free_index,
                key,
                value,
                partial_hash,
            );
            self.cell.num_entries += 1;
            return;
        }

        let preferred_index = partial_hash as usize % LINKED_LEN;
        let mut link_ptr = self.cell.metadata.link.load(Acquire, barrier).as_raw()
            as *mut Linked<K, V, LINKED_LEN>;
        while let Some(link_mut) = unsafe { link_ptr.as_mut() } {
            if (link_mut.metadata.occupied_bitmap & (1_u32 << preferred_index)) == 0 {
                Self::insert_entry(
                    &mut link_mut.metadata,
                    &link_mut.data_block,
                    preferred_index,
                    key,
                    value,
                    partial_hash,
                );
                self.cell.num_entries += 1;
                return;
            }
            let free_index = Self::get_free_index::<LINKED_LEN>(
                link_mut.metadata.occupied_bitmap,
                preferred_index,
            );
            if free_index != LINKED_LEN {
                Self::insert_entry(
                    &mut link_mut.metadata,
                    &link_mut.data_block,
                    free_index,
                    key,
                    value,
                    partial_hash,
                );
                self.cell.num_entries += 1;
                return;
            }

            link_ptr = link_mut.metadata.link.load(Acquire, barrier).as_raw()
                as *mut Linked<K, V, LINKED_LEN>;
        }

        // Insert a new `Linked` at the linked list head.
        let new_link = Arc::new(Linked::new());
        let new_link_mut = unsafe { &mut *(new_link.as_ptr() as *mut Linked<K, V, LINKED_LEN>) };
        Self::insert_entry(
            &mut new_link_mut.metadata,
            &new_link.data_block,
            preferred_index,
            key,
            value,
            partial_hash,
        );
        let head_link = self.cell.metadata.link.get_arc(Relaxed, barrier);
        if let Some(head_link) = head_link.as_ref() {
            head_link
                .prev_link
                .store(new_link.as_ptr() as *mut Linked<K, V, LINKED_LEN>, Relaxed);
        }
        new_link.metadata.link.swap((head_link, Tag::None), Relaxed);
        self.cell
            .metadata
            .link
            .swap((Some(new_link), Tag::None), Release);
        self.cell.num_entries += 1;
    }

    /// Removes the key-value pair being pointed by the given [`EntryPtr`].
    #[inline]
    pub(crate) fn erase(
        &mut self,
        data_block: &DataBlock<K, V, CELL_LEN>,
        entry_ptr: &mut EntryPtr<K, V, LOCK_FREE>,
    ) -> Option<(K, V)> {
        debug_assert_ne!(entry_ptr.current_index, usize::MAX);

        self.cell.num_entries -= 1;
        let link_ptr = entry_ptr.current_link_ptr.as_raw() as *mut Linked<K, V, LINKED_LEN>;
        if let Some(link_mut) = unsafe { link_ptr.as_mut() } {
            if LOCK_FREE {
                debug_assert_eq!(
                    link_mut.metadata.removed_bitmap & (1_u32 << entry_ptr.current_index),
                    0
                );
                link_mut.metadata.removed_bitmap |= 1_u32 << entry_ptr.current_index;
            } else {
                debug_assert_ne!(
                    link_mut.metadata.occupied_bitmap & (1_u32 << entry_ptr.current_index),
                    0
                );
                link_mut.metadata.occupied_bitmap &= !(1_u32 << entry_ptr.current_index);
                return Some(unsafe {
                    link_mut.data_block[entry_ptr.current_index].as_ptr().read()
                });
            }
        } else if LOCK_FREE {
            debug_assert_eq!(
                self.cell.metadata.removed_bitmap & (1_u32 << entry_ptr.current_index),
                0
            );
            self.cell.metadata.removed_bitmap |= 1_u32 << entry_ptr.current_index;
        } else {
            debug_assert_ne!(
                self.cell.metadata.occupied_bitmap & (1_u32 << entry_ptr.current_index),
                0
            );
            self.cell.metadata.occupied_bitmap &= !(1_u32 << entry_ptr.current_index);
            return Some(unsafe { data_block[entry_ptr.current_index].as_ptr().read() });
        }

        None
    }

    /// Extracts the key-value pair being pointed by the [`EntryPtr`].
    #[inline]
    pub(crate) fn extract<'e>(
        &mut self,
        data_block: &DataBlock<K, V, CELL_LEN>,
        entry_ptr: &mut EntryPtr<'e, K, V, LOCK_FREE>,
        barrier: &'e Barrier,
    ) -> (K, V) {
        debug_assert!(!LOCK_FREE);
        let link_ptr = entry_ptr.current_link_ptr.as_raw() as *mut Linked<K, V, LINKED_LEN>;
        if let Some(link_mut) = unsafe { link_ptr.as_mut() } {
            let extracted = Self::extract_entry(
                &mut link_mut.metadata,
                &link_mut.data_block,
                entry_ptr.current_index,
                &mut self.cell.num_entries,
            );
            if link_mut.metadata.occupied_bitmap == 0 {
                entry_ptr.unlink(self, link_mut, barrier);
            }
            extracted
        } else {
            Self::extract_entry(
                &mut self.cell.metadata,
                data_block,
                entry_ptr.current_index,
                &mut self.cell.num_entries,
            )
        }
    }

    /// Purges all the data.
    #[inline]
    pub(crate) fn purge(&mut self, barrier: &Barrier) {
        if LOCK_FREE {
            self.cell.metadata.removed_bitmap = self.cell.metadata.occupied_bitmap;
        }
        self.cell.state.fetch_or(KILLED, Release);
        self.cell.num_entries = 0;
        if !self.cell.metadata.link.load(Acquire, barrier).is_null() {
            if let Some(link) = self.cell.metadata.link.swap((None, Tag::None), Relaxed).0 {
                let _ = link.release(barrier);
            }
        }
    }

    /// Tries to lock the [`Cell`].
    fn try_lock(
        cell: &'b mut Cell<K, V, LOCK_FREE>,
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

    /// Gets the most optimal free slot index from the given bitmap.
    fn get_free_index<const LEN: usize>(bitmap: u32, preferred_index: usize) -> usize {
        let mut free_index = (bitmap | ((1_u32 << preferred_index) - 1)).trailing_ones() as usize;
        if free_index == LEN {
            free_index = bitmap.trailing_ones() as usize;
        }
        free_index
    }

    /// Inserts a key-value pair in the slot.
    fn insert_entry<const LEN: usize>(
        metadata: &mut Metadata<K, V, LEN>,
        data_block: &DataBlock<K, V, LEN>,
        index: usize,
        key: K,
        value: V,
        partial_hash: u8,
    ) {
        debug_assert!(index < LEN);

        unsafe {
            (data_block[index].as_ptr() as *mut (K, V)).write((key, value));
            metadata.partial_hash_array[index] = partial_hash;
            if LOCK_FREE {
                fence(Release);
            }
            metadata.occupied_bitmap |= 1_u32 << index;
        }
    }

    /// Extracts and removes the key-value pair in the slot.
    fn extract_entry<const LEN: usize>(
        metadata: &mut Metadata<K, V, LEN>,
        data_block: &DataBlock<K, V, LEN>,
        index: usize,
        num_entries_field: &mut u32,
    ) -> (K, V) {
        debug_assert!(index < LEN);

        *num_entries_field -= 1;
        metadata.occupied_bitmap &= !(1_u32 << index);
        let entry_ptr = data_block[index].as_ptr() as *mut (K, V);
        unsafe { ptr::read(entry_ptr) }
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
        async_wait: NonNull<AsyncWait>,
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
    /// Partial hash array.
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

/// [`Linked`] is a fixed size data block of key-value pairs.
pub(crate) struct Linked<K: 'static + Eq, V: 'static, const LEN: usize> {
    metadata: Metadata<K, V, LEN>,
    data_block: DataBlock<K, V, LEN>,
    prev_link: AtomicPtr<Linked<K, V, LEN>>,
}

impl<K: 'static + Eq, V: 'static, const LEN: usize> Linked<K, V, LEN> {
    fn new() -> Linked<K, V, LEN> {
        Linked {
            metadata: Metadata::default(),
            data_block: unsafe { MaybeUninit::uninit().assume_init() },
            prev_link: AtomicPtr::default(),
        }
    }
}

impl<K: 'static + Eq, V: 'static, const LEN: usize> Drop for Linked<K, V, LEN> {
    fn drop(&mut self) {
        if needs_drop::<(K, V)>() {
            let mut index = self.metadata.occupied_bitmap.trailing_zeros();
            while index != 32 {
                let entry_mut_ptr = self.data_block[index as usize].as_mut_ptr();
                unsafe {
                    ptr::drop_in_place(entry_mut_ptr);
                }
                self.metadata.occupied_bitmap &= !(1_u32 << index);
                index = self.metadata.occupied_bitmap.trailing_zeros();
            }
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
        let data_block: Arc<DataBlock<usize, usize, CELL_LEN>> =
            Arc::new(unsafe { MaybeUninit::uninit().assume_init() });
        let mut cell: Arc<Cell<usize, usize, true>> = Arc::new(Cell::default());
        let mut data: [u64; 128] = [0; 128];
        let mut task_handles = Vec::with_capacity(num_tasks);
        for task_id in 0..num_tasks {
            let barrier_copied = barrier.clone();
            let data_block_copied = data_block.clone();
            let cell_copied = cell.clone();
            let data_ptr = AtomicPtr::new(&mut data);
            task_handles.push(tokio::spawn(async move {
                barrier_copied.wait().await;
                let cell_mut =
                    unsafe { &mut *(cell_copied.as_ptr() as *mut Cell<usize, usize, true>) };
                let barrier = Barrier::new();
                for i in 0..2048 {
                    let mut exclusive_locker = Locker::lock(cell_mut, &barrier).unwrap();
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

        let mut count = 0;
        let mut entry_ptr = EntryPtr::new(&epoch_barrier);
        while entry_ptr.next(&cell, &epoch_barrier) {
            count += 1;
        }
        assert_eq!(cell.num_entries(), count);

        let mut xlocker = Locker::lock(unsafe { cell.get_mut().unwrap() }, &epoch_barrier).unwrap();
        xlocker.purge(&epoch_barrier);
        drop(xlocker);

        assert!(cell.killed());
        assert_eq!(cell.num_entries(), 0);
        assert!(Locker::lock(unsafe { cell.get_mut().unwrap() }, &epoch_barrier).is_none());
    }
}
