use std::cell::UnsafeCell;
use std::fmt::{self, Debug};
use std::mem::{MaybeUninit, forget, needs_drop};
use std::ops::{Deref, Index};
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicPtr, AtomicU32};

use saa::Lock;
use sdd::{AtomicShared, Guard, Ptr, Shared, Tag};

use crate::Equivalent;
use crate::async_helper::SendableGuard;

/// [`Bucket`] is a lock-protected fixed-size entry array.
///
/// In case the fixed-size entry array overflows, additional entries can be stored in a linked list
/// of [`LinkedBucket`].
#[repr(align(64))]
pub struct Bucket<K, V, L: LruList, const TYPE: char> {
    /// Number of entries in the [`Bucket`].
    len: AtomicU32,
    /// The epoch value when the [`Bucket`] was last updated.
    ///
    /// The field is only used when `TYPE == OPTIMISTIC`.
    epoch: AtomicU8,
    /// [`Bucket`] metadata.
    metadata: Metadata<K, V, BUCKET_LEN>,
    /// Reader-writer lock.
    rw_lock: Lock,
    /// The LRU list of the [`Bucket`].
    lru_list: L,
}

/// Doubly-linked-list interfaces to efficiently manage least-recently-used entries.
pub trait LruList: 'static + Default {
    /// Evicts an entry.
    #[inline]
    fn evict(&self, _tail: u32) -> Option<(u8, u32)> {
        None
    }

    /// Removes an entry.
    #[inline]
    fn remove(&self, _tail: u32, _entry: u8) -> Option<u32> {
        None
    }

    /// Promotes the entry.
    #[inline]
    fn promote(&self, _tail: u32, _entry: u8) -> Option<u32> {
        None
    }
}

/// [`DoublyLinkedList`] is an array of `(AtomicU8, AtomicU8)` implementing [`LruList`].
pub struct DoublyLinkedList([(AtomicU8, AtomicU8); BUCKET_LEN]);

/// The type of [`Bucket`] only allows sequential access to it.
pub const SEQUENTIAL: char = 'S';

/// The type of [`Bucket`] allows lock-free read.
pub const OPTIMISTIC: char = 'O';

/// The type of [`Bucket`] acts as an LRU cache.
pub const CACHE: char = 'C';

/// The size of the fixed-size entry array in a [`Bucket`].
pub const BUCKET_LEN: usize = u32::BITS as usize;

/// [`DataBlock`] is a type alias of a raw memory chunk of entries.
pub struct DataBlock<K, V, const LEN: usize>([UnsafeCell<MaybeUninit<(K, V)>>; LEN]);

/// [`Writer`] holds an exclusive lock on a [`Bucket`].
pub struct Writer<'g, K, V, L: LruList, const TYPE: char> {
    bucket: &'g Bucket<K, V, L, TYPE>,
}

/// [`Reader`] holds a shared lock on a [`Bucket`].
pub struct Reader<'g, K, V, L: LruList, const TYPE: char> {
    bucket: &'g Bucket<K, V, L, TYPE>,
}

/// [`EntryPtr`] points to an entry slot in a [`Bucket`].
pub struct EntryPtr<'g, K, V, const TYPE: char> {
    /// Points to a [`LinkedBucket`].
    current_link_ptr: Ptr<'g, LinkedBucket<K, V, LINKED_BUCKET_LEN>>,
    /// Index of the entry.
    current_index: usize,
}

/// [`Metadata`] is a collection of metadata fields of [`Bucket`] and [`LinkedBucket`].
pub(crate) struct Metadata<K, V, const LEN: usize> {
    /// Linked list of entries.
    link: AtomicShared<LinkedBucket<K, V, LINKED_BUCKET_LEN>>,
    /// Occupied slot bitmap.
    occupied_bitmap: AtomicU32,
    /// Removed slot bitmap, or the `1-based` index of the most recently used entry if
    /// `TYPE = CACHE` where `0` represents `nil`.
    removed_bitmap_or_lru_tail: AtomicU32,
    /// Partial hash array for fast hash lookup.
    partial_hash_array: UnsafeCell<[u8; LEN]>,
}

/// [`LinkedBucket`] is a smaller [`Bucket`] that is attached to a [`Bucket`] as a linked list.
pub(crate) struct LinkedBucket<K, V, const LEN: usize> {
    /// [`LinkedBucket`] metadata.
    metadata: Metadata<K, V, LEN>,
    /// Own data block.
    data_block: DataBlock<K, V, LEN>,
    /// Previous [`LinkedBucket`].
    prev_link: AtomicPtr<LinkedBucket<K, V, LEN>>,
}

/// The size of the linked data block.
const LINKED_BUCKET_LEN: usize = BUCKET_LEN / 4;

impl<K, V, L: LruList, const TYPE: char> Bucket<K, V, L, TYPE> {
    /// Returns the number of occupied and reachable slots in the [`Bucket`].
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len.load(Relaxed) as usize
    }

    /// Returns `true` if the [`Bucket`] needs to be rebuilt.
    ///
    /// If `TYPE == OPTIMISTIC`, removed entries are not dropped, still occupying the slots,
    /// therefore rebuilding the [`Bucket`] might be needed to keep the [`Bucket`] as small as
    /// possible.
    #[inline]
    pub(crate) fn need_rebuild(&self) -> bool {
        TYPE == OPTIMISTIC
            && self.metadata.removed_bitmap_or_lru_tail.load(Relaxed)
                == (u32::MAX >> (32 - BUCKET_LEN))
    }

    /// Reserves memory for insertion, and then constructs the key-value pair in-place.
    #[inline]
    pub(crate) fn insert_with<'g, C: FnOnce() -> (K, V)>(
        &self,
        data_block: &DataBlock<K, V, BUCKET_LEN>,
        partial_hash: u8,
        constructor: C,
        guard: &'g Guard,
    ) -> EntryPtr<'g, K, V, TYPE> {
        let len = self.len.load(Relaxed);
        assert_ne!(len, u32::MAX, "bucket overflow");

        let occupied_bitmap = self.metadata.occupied_bitmap.load(Relaxed);
        let free_index = occupied_bitmap.trailing_ones() as usize;
        if free_index == BUCKET_LEN {
            self.insert_overflow_with(len, partial_hash, constructor, guard)
        } else {
            Self::insert_entry_with(
                &self.metadata,
                data_block,
                free_index,
                occupied_bitmap,
                partial_hash,
                constructor,
            );
            self.len.store(len + 1, Relaxed);
            EntryPtr {
                current_link_ptr: Ptr::null(),
                current_index: free_index,
            }
        }
    }

    /// Inserts an entry into the linked list.
    pub(crate) fn insert_overflow_with<'g, C: FnOnce() -> (K, V)>(
        &self,
        len: u32,
        partial_hash: u8,
        constructor: C,
        guard: &'g Guard,
    ) -> EntryPtr<'g, K, V, TYPE> {
        let mut link_ptr = self.metadata.link.load(Acquire, guard);
        while let Some(link) = link_ptr.as_ref() {
            let occupied_bitmap = link.metadata.occupied_bitmap.load(Relaxed);
            let free_index = occupied_bitmap.trailing_ones() as usize;
            if free_index != LINKED_BUCKET_LEN {
                Self::insert_entry_with(
                    &link.metadata,
                    &link.data_block,
                    free_index,
                    occupied_bitmap,
                    partial_hash,
                    constructor,
                );
                self.len.store(len + 1, Relaxed);
                return EntryPtr {
                    current_link_ptr: link_ptr,
                    current_index: free_index,
                };
            }
            link_ptr = link.metadata.link.load(Acquire, guard);
        }

        // Insert a new `LinkedBucket` at the linked list head.
        let head = self.metadata.link.get_shared(Relaxed, guard);
        let link = unsafe { Shared::new_unchecked(LinkedBucket::new(head)) };
        let link_ptr = link.get_guarded_ptr(guard);
        if let Some(link) = link_ptr.as_ref() {
            Self::write_data_block(&link.data_block, constructor(), 0);
            Self::update_partial_hash(&link.metadata.partial_hash_array, 0, partial_hash);
            link.metadata.occupied_bitmap.store(1, Relaxed);
        }
        if let Some(head) = link.metadata.link.load(Relaxed, guard).as_ref() {
            head.prev_link.store(link.as_ptr().cast_mut(), Relaxed);
        }
        self.metadata.link.swap((Some(link), Tag::None), Release);
        self.len.store(len + 1, Relaxed);
        EntryPtr {
            current_link_ptr: link_ptr,
            current_index: 0,
        }
    }

    /// Removes the entry pointed to by the supplied [`EntryPtr`].
    #[inline]
    pub(crate) fn remove<'g>(
        &self,
        data_block: &DataBlock<K, V, BUCKET_LEN>,
        entry_ptr: &mut EntryPtr<'g, K, V, TYPE>,
        guard: &'g Guard,
    ) -> (K, V) {
        debug_assert_ne!(TYPE, OPTIMISTIC);
        debug_assert_ne!(entry_ptr.current_index, usize::MAX);
        debug_assert_ne!(entry_ptr.current_index, BUCKET_LEN);

        self.len.store(self.len.load(Relaxed) - 1, Relaxed);

        if let Some(link) = entry_ptr.current_link_ptr.as_ref() {
            let mut occupied_bitmap = link.metadata.occupied_bitmap.load(Relaxed);
            debug_assert_ne!(occupied_bitmap & (1_u32 << entry_ptr.current_index), 0);

            occupied_bitmap &= !(1_u32 << entry_ptr.current_index);
            link.metadata
                .occupied_bitmap
                .store(occupied_bitmap, Relaxed);
            let removed = Self::read_data_block(&link.data_block, entry_ptr.current_index);
            if occupied_bitmap == 0 {
                entry_ptr.unlink(self, link, guard);
            }
            removed
        } else {
            let mut occupied_bitmap = self.metadata.occupied_bitmap.load(Relaxed);
            debug_assert_ne!(occupied_bitmap & (1_u32 << entry_ptr.current_index), 0);

            if TYPE == CACHE {
                self.remove_from_lru_list(entry_ptr);
            }

            occupied_bitmap &= !(1_u32 << entry_ptr.current_index);
            self.metadata
                .occupied_bitmap
                .store(occupied_bitmap, Relaxed);
            Self::read_data_block(data_block, entry_ptr.current_index)
        }
    }

    /// Marks the entry removed without dropping the entry.
    #[inline]
    pub(crate) fn mark_removed<'g>(
        &self,
        entry_ptr: &mut EntryPtr<'g, K, V, TYPE>,
        guard: &'g Guard,
    ) {
        debug_assert_eq!(TYPE, OPTIMISTIC);
        debug_assert_ne!(entry_ptr.current_index, usize::MAX);
        debug_assert_ne!(entry_ptr.current_index, BUCKET_LEN);

        self.len.store(self.len.load(Relaxed) - 1, Relaxed);

        if let Some(link) = entry_ptr.current_link_ptr.as_ref() {
            let mut removed_bitmap = link.metadata.removed_bitmap_or_lru_tail.load(Relaxed);
            debug_assert_eq!(removed_bitmap & (1_u32 << entry_ptr.current_index), 0);

            removed_bitmap |= 1_u32 << entry_ptr.current_index;
            link.metadata
                .removed_bitmap_or_lru_tail
                .store(removed_bitmap, Relaxed);
            if link.metadata.occupied_bitmap.load(Relaxed) == removed_bitmap {
                entry_ptr.unlink(self, link, guard);
            }
        } else {
            let mut removed_bitmap = self.metadata.removed_bitmap_or_lru_tail.load(Relaxed);
            debug_assert_eq!(removed_bitmap & (1_u32 << entry_ptr.current_index), 0);

            removed_bitmap |= 1_u32 << entry_ptr.current_index;
            self.metadata
                .removed_bitmap_or_lru_tail
                .store(removed_bitmap, Relaxed);
            self.update_target_epoch(guard);
        }
    }

    /// Keeps or consumes the entry pointed to by the supplied [`EntryPtr`].
    ///
    /// Returns `true` if the entry was consumed.
    #[inline]
    pub(crate) fn keep_or_consume<'g, F: FnMut(&K, V) -> Option<V>>(
        &self,
        data_block: &DataBlock<K, V, BUCKET_LEN>,
        entry_ptr: &mut EntryPtr<'g, K, V, TYPE>,
        pred: &mut F,
        guard: &'g Guard,
    ) -> bool {
        debug_assert_ne!(TYPE, OPTIMISTIC);
        debug_assert_ne!(entry_ptr.current_index, usize::MAX);

        // `pred` may panic, therefore it is safer to assume that the entry will be consumed.
        self.len.store(self.len.load(Relaxed) - 1, Relaxed);

        if let Some(link) = entry_ptr.current_link_ptr.as_ref() {
            let mut occupied_bitmap = link.metadata.occupied_bitmap.load(Relaxed);
            debug_assert_ne!(occupied_bitmap & (1_u32 << entry_ptr.current_index), 0);

            occupied_bitmap &= !(1_u32 << entry_ptr.current_index);
            link.metadata
                .occupied_bitmap
                .store(occupied_bitmap, Relaxed);

            let (k, v) = Self::read_data_block(&link.data_block, entry_ptr.current_index);
            if let Some(v) = pred(&k, v) {
                // The instances returned: revive the entry.
                Self::write_data_block(&link.data_block, (k, v), entry_ptr.current_index);
                self.len.store(self.len.load(Relaxed) + 1, Relaxed);
                link.metadata.occupied_bitmap.store(
                    occupied_bitmap | (1_u32 << entry_ptr.current_index),
                    Relaxed,
                );
                return false;
            }

            if occupied_bitmap == 0 {
                entry_ptr.unlink(self, link, guard);
            }
        } else {
            let mut occupied_bitmap = self.metadata.occupied_bitmap.load(Relaxed);
            debug_assert_ne!(occupied_bitmap & (1_u32 << entry_ptr.current_index), 0);

            occupied_bitmap &= !(1_u32 << entry_ptr.current_index);
            self.metadata
                .occupied_bitmap
                .store(occupied_bitmap, Relaxed);

            let (k, v) = Self::read_data_block(data_block, entry_ptr.current_index);
            if let Some(v) = pred(&k, v) {
                // The instances returned: revive the entry.
                Self::write_data_block(data_block, (k, v), entry_ptr.current_index);
                self.len.store(self.len.load(Relaxed) + 1, Relaxed);
                self.metadata.occupied_bitmap.store(
                    occupied_bitmap | (1_u32 << entry_ptr.current_index),
                    Relaxed,
                );
                return false;
            }
        }
        true
    }

    /// Evicts the least recently used entry if the [`Bucket`] is full.
    pub(crate) fn evict_lru_head(
        &self,
        data_block: &DataBlock<K, V, BUCKET_LEN>,
    ) -> Option<(K, V)> {
        debug_assert_eq!(TYPE, CACHE);

        let occupied_bitmap = self.metadata.occupied_bitmap.load(Relaxed);
        if occupied_bitmap == 0b1111_1111_1111_1111_1111_1111_1111_1111 {
            self.len.store(self.len.load(Relaxed) - 1, Relaxed);

            let tail = self.metadata.removed_bitmap_or_lru_tail.load(Relaxed);
            let evicted = if let Some((evicted, new_tail)) = self.lru_list.evict(tail) {
                self.metadata
                    .removed_bitmap_or_lru_tail
                    .store(new_tail, Relaxed);
                evicted as usize
            } else {
                // Evict the first occupied entry.
                0
            };
            debug_assert_ne!(occupied_bitmap & (1_u32 << evicted), 0);

            self.metadata
                .occupied_bitmap
                .store(occupied_bitmap & !(1_u32 << evicted), Relaxed);
            return Some(Self::read_data_block(data_block, evicted));
        }

        None
    }

    /// Sets the entry having been just accessed.
    pub(crate) fn update_lru_tail(&self, entry_ptr: &EntryPtr<K, V, TYPE>) {
        debug_assert_eq!(TYPE, CACHE);
        debug_assert_ne!(entry_ptr.current_index, usize::MAX);
        debug_assert_ne!(entry_ptr.current_index, BUCKET_LEN);

        if entry_ptr.current_link_ptr.is_null() {
            #[allow(clippy::cast_possible_truncation)]
            let entry = entry_ptr.current_index as u8;
            let tail = self.metadata.removed_bitmap_or_lru_tail.load(Relaxed);
            if let Some(new_tail) = self.lru_list.promote(tail, entry) {
                self.metadata
                    .removed_bitmap_or_lru_tail
                    .store(new_tail, Relaxed);
            }
        }
    }

    /// Extracts the entry pointed to by the [`EntryPtr`].
    #[inline]
    pub(super) fn extract<'g>(
        &self,
        data_block: &DataBlock<K, V, BUCKET_LEN>,
        entry_ptr: &mut EntryPtr<'g, K, V, TYPE>,
        guard: &'g Guard,
    ) -> (K, V) {
        debug_assert_ne!(TYPE, OPTIMISTIC);

        self.len.store(self.len.load(Relaxed) - 1, Relaxed);

        if let Some(link) = entry_ptr.current_link_ptr.as_ref() {
            let mut occupied_bitmap = link.metadata.occupied_bitmap.load(Relaxed);
            debug_assert_ne!(occupied_bitmap & (1_u32 << entry_ptr.current_index), 0);

            occupied_bitmap &= !(1_u32 << entry_ptr.current_index);
            link.metadata
                .occupied_bitmap
                .store(occupied_bitmap, Relaxed);
            let extracted = Self::read_data_block(&link.data_block, entry_ptr.current_index);
            if occupied_bitmap == 0 {
                entry_ptr.unlink(self, link, guard);
            }
            extracted
        } else {
            let occupied_bitmap = self.metadata.occupied_bitmap.load(Relaxed);
            debug_assert_ne!(occupied_bitmap & (1_u32 << entry_ptr.current_index), 0);

            self.metadata.occupied_bitmap.store(
                occupied_bitmap & !(1_u32 << entry_ptr.current_index),
                Relaxed,
            );
            Self::read_data_block(data_block, entry_ptr.current_index)
        }
    }

    /// Returns `true` if the [`Bucket`] has been killed.
    #[inline]
    pub(super) fn killed(&self) -> bool {
        self.rw_lock.is_poisoned(Relaxed)
    }

    /// Drops entries in the [`DataBlock`] based on the metadata of the [`Bucket`].
    ///
    /// The [`Bucket`] and the [`DataBlock`] should never be used afterwards.
    #[inline]
    pub(super) fn drop_entries(&self, data_block: &DataBlock<K, V, BUCKET_LEN>) {
        if !self.metadata.link.is_null(Relaxed) {
            let mut next = self.metadata.link.swap((None, Tag::None), Acquire);
            while let Some(current) = next.0 {
                next = current.metadata.link.swap((None, Tag::None), Acquire);
                let released = if TYPE == OPTIMISTIC {
                    current.release()
                } else {
                    unsafe { current.drop_in_place() }
                };
                debug_assert!(TYPE == OPTIMISTIC || released);
            }
        }
        if needs_drop::<(K, V)>() {
            let mut occupied_bitmap = self.metadata.occupied_bitmap.load(Relaxed);
            if occupied_bitmap != 0 {
                let mut index = occupied_bitmap.trailing_zeros();
                while index != 32 {
                    Self::drop_entry(data_block, index as usize);
                    occupied_bitmap -= 1_u32 << index;
                    index = occupied_bitmap.trailing_zeros();
                }
            }
        }
    }

    /// Drops removed entries if they are completely unreachable, thereby allowing others to reuse
    /// the memory.
    pub(super) fn drop_removed_unreachable_entries(
        &self,
        data_block: &DataBlock<K, V, BUCKET_LEN>,
        guard: &Guard,
    ) {
        debug_assert_eq!(TYPE, OPTIMISTIC);

        let mut removed_bitmap = self.metadata.removed_bitmap_or_lru_tail.load(Relaxed);
        if removed_bitmap == 0 {
            return;
        }

        let current_epoch = u8::from(guard.epoch());
        let target_epoch = self.epoch.load(Acquire);
        if current_epoch == target_epoch {
            let mut occupied_bitmap = self.metadata.occupied_bitmap.load(Relaxed);
            let mut index = removed_bitmap.trailing_zeros();
            while index != 32 {
                let bit = 1_u32 << index;
                debug_assert_ne!(occupied_bitmap | bit, 0);
                occupied_bitmap -= bit;
                removed_bitmap -= bit;
                Self::drop_entry(data_block, index as usize);
                index = removed_bitmap.trailing_zeros();
            }
            self.metadata
                .occupied_bitmap
                .store(occupied_bitmap, Relaxed);
            self.metadata
                .removed_bitmap_or_lru_tail
                .store(removed_bitmap, Relaxed);
        }
    }

    /// Inserts a key-value pair in the slot.
    fn insert_entry_with<C: FnOnce() -> (K, V), const LEN: usize>(
        metadata: &Metadata<K, V, LEN>,
        data_block: &DataBlock<K, V, LEN>,
        index: usize,
        occupied_bitmap: u32,
        partial_hash: u8,
        constructor: C,
    ) {
        debug_assert!(index < LEN);
        debug_assert_eq!(metadata.occupied_bitmap.load(Relaxed) & (1_u32 << index), 0);

        Self::write_data_block(data_block, constructor(), index);
        Self::update_partial_hash(&metadata.partial_hash_array, index, partial_hash);
        metadata.occupied_bitmap.store(
            occupied_bitmap | (1_u32 << index),
            if TYPE == OPTIMISTIC { Release } else { Relaxed },
        );
    }

    /// Removes the entry from the LRU linked list.
    fn remove_from_lru_list(&self, entry_ptr: &EntryPtr<K, V, TYPE>) {
        debug_assert_eq!(TYPE, CACHE);
        debug_assert_ne!(entry_ptr.current_index, usize::MAX);
        debug_assert_ne!(entry_ptr.current_index, BUCKET_LEN);

        if entry_ptr.current_link_ptr.is_null() {
            #[allow(clippy::cast_possible_truncation)]
            let entry = entry_ptr.current_index as u8;
            let tail = self.metadata.removed_bitmap_or_lru_tail.load(Relaxed);
            if let Some(new_tail) = self.lru_list.remove(tail, entry) {
                self.metadata
                    .removed_bitmap_or_lru_tail
                    .store(new_tail, Relaxed);
            }
        }
    }

    /// Updates the target epoch after removing an entry.
    fn update_target_epoch(&self, guard: &Guard) {
        debug_assert_eq!(TYPE, OPTIMISTIC);
        debug_assert_ne!(self.metadata.removed_bitmap_or_lru_tail.load(Relaxed), 0);

        let target_epoch = guard.epoch().next_generation();
        self.epoch.store(u8::from(target_epoch), Release);
    }

    /// Reads the data at the given index.
    #[allow(clippy::inline_always)] // Very trivial function.
    #[inline(always)]
    fn read_data_block<const LEN: usize>(
        data_block: &DataBlock<K, V, LEN>,
        index: usize,
    ) -> (K, V) {
        unsafe { (*data_block[index].get()).as_ptr().read() }
    }

    /// Writes the data on the slot in the [`DataBlock`].
    #[allow(clippy::inline_always)] // Very trivial function.
    #[inline(always)]
    fn write_data_block<const LEN: usize>(
        data_block: &DataBlock<K, V, LEN>,
        entry: (K, V),
        index: usize,
    ) {
        unsafe {
            (*data_block[index].get()).as_mut_ptr().write(entry);
        }
    }

    /// Drops the data in-place.
    #[allow(clippy::inline_always)] // Very trivial function.
    #[inline(always)]
    fn drop_entry<const LEN: usize>(data_block: &DataBlock<K, V, LEN>, index: usize) {
        unsafe {
            (*data_block[index].get()).as_mut_ptr().drop_in_place();
        }
    }

    /// Returns a pointer to the slot in the [`DataBlock`].
    #[allow(clippy::inline_always)] // Very trivial function.
    #[inline(always)]
    fn entry_ptr<const LEN: usize>(
        data_block: &DataBlock<K, V, LEN>,
        index: usize,
    ) -> *const (K, V) {
        unsafe { (*data_block[index].get()).as_ptr() }
    }

    /// Returns a mutable pointer to the slot in the [`DataBlock`].
    #[allow(clippy::inline_always)] // Very trivial function.
    #[inline(always)]
    fn entry_mut_ptr<const LEN: usize>(
        data_block: &DataBlock<K, V, LEN>,
        index: usize,
    ) -> *mut (K, V) {
        unsafe { (*data_block[index].get()).as_mut_ptr() }
    }

    /// Gets the partial hash value.
    #[allow(clippy::inline_always)] // This must be inlined for vectorization.
    #[inline(always)]
    const fn partial_hash<const LEN: usize>(
        partial_hash_array: &UnsafeCell<[u8; LEN]>,
        index: usize,
    ) -> u8 {
        unsafe { (*partial_hash_array.get())[index] }
    }

    /// Updates the partial hash array.
    #[allow(clippy::inline_always)] // Very trivial function.
    #[inline(always)]
    const fn update_partial_hash<const LEN: usize>(
        partial_hash_array: &UnsafeCell<[u8; LEN]>,
        index: usize,
        partial_hash: u8,
    ) {
        unsafe {
            (*partial_hash_array.get())[index] = partial_hash;
        }
    }
}

impl<K: Eq, V, L: LruList, const TYPE: char> Bucket<K, V, L, TYPE> {
    /// Searches for an entry containing the key.
    ///
    /// Returns `None` if the key is not present.
    #[inline]
    pub(super) fn search_entry<'g, Q>(
        &self,
        data_block: &'g DataBlock<K, V, BUCKET_LEN>,
        key: &Q,
        partial_hash: u8,
        guard: &'g Guard,
    ) -> Option<&'g (K, V)>
    where
        Q: Equivalent<K> + ?Sized,
    {
        if self.len() == 0 {
            return None;
        }

        if let Some((entry, _)) =
            Self::search_data_block(&self.metadata, data_block, key, partial_hash)
        {
            return Some(entry);
        }

        let mut link_ptr = self.metadata.link.load(Acquire, guard);
        while let Some(link) = link_ptr.as_ref() {
            if let Some((entry, _)) =
                Self::search_data_block(&link.metadata, &link.data_block, key, partial_hash)
            {
                return Some(entry);
            }
            link_ptr = link.metadata.link.load(Acquire, guard);
        }

        None
    }

    /// Gets an [`EntryPtr`] pointing to the slot containing the key.
    ///
    /// Returns an invalid [`EntryPtr`] if the key is not present.
    #[inline]
    pub(crate) fn get_entry_ptr<'g, Q>(
        &self,
        data_block: &DataBlock<K, V, BUCKET_LEN>,
        key: &Q,
        partial_hash: u8,
        guard: &'g Guard,
    ) -> EntryPtr<'g, K, V, TYPE>
    where
        Q: Equivalent<K> + ?Sized,
    {
        if self.len() == 0 {
            return EntryPtr::new(guard);
        }

        if let Some((_, index)) =
            Self::search_data_block(&self.metadata, data_block, key, partial_hash)
        {
            return EntryPtr {
                current_link_ptr: Ptr::null(),
                current_index: index,
            };
        }

        let mut current_link_ptr = self.metadata.link.load(Acquire, guard);
        while let Some(link) = current_link_ptr.as_ref() {
            if let Some((_, index)) =
                Self::search_data_block(&link.metadata, &link.data_block, key, partial_hash)
            {
                return EntryPtr {
                    current_link_ptr,
                    current_index: index,
                };
            }
            current_link_ptr = link.metadata.link.load(Acquire, guard);
        }

        EntryPtr::new(guard)
    }

    /// Searches the supplied data block for the entry containing the key.
    fn search_data_block<'g, Q, const LEN: usize>(
        metadata: &Metadata<K, V, LEN>,
        data_block: &'g DataBlock<K, V, LEN>,
        key: &Q,
        partial_hash: u8,
    ) -> Option<(&'g (K, V), usize)>
    where
        Q: Equivalent<K> + ?Sized,
    {
        let mut bitmap = if TYPE == OPTIMISTIC {
            metadata.occupied_bitmap.load(Acquire)
                & (!metadata.removed_bitmap_or_lru_tail.load(Relaxed))
        } else {
            metadata.occupied_bitmap.load(Relaxed)
        };

        // Expect that the loop is vectorized by the compiler.
        let mut matching: u32 = 0;
        for i in 0..LEN {
            if Self::partial_hash(&metadata.partial_hash_array, i) == partial_hash {
                matching |= 1_u32 << i;
            }
        }
        bitmap &= matching;

        let mut offset = bitmap.trailing_zeros();
        while offset != u32::BITS {
            let entry = unsafe { &*Self::entry_ptr(data_block, offset as usize) };
            if key.equivalent(&entry.0) {
                return Some((entry, offset as usize));
            }
            bitmap -= 1_u32 << offset;
            offset = bitmap.trailing_zeros();
        }

        None
    }
}

impl<'g, K, V, const TYPE: char> EntryPtr<'g, K, V, TYPE> {
    /// Creates a new invalid [`EntryPtr`].
    #[inline]
    pub(crate) const fn new(_guard: &'g Guard) -> Self {
        Self {
            current_link_ptr: Ptr::null(),
            current_index: BUCKET_LEN,
        }
    }

    /// Returns `true` if the [`EntryPtr`] points to, or has pointed to an occupied entry.
    #[inline]
    pub(crate) const fn is_valid(&self) -> bool {
        self.current_index != BUCKET_LEN
    }

    /// Moves the [`EntryPtr`] to point to the next occupied entry.
    ///
    /// Returns `true` if it successfully found the next occupied entry.
    #[inline]
    pub(crate) fn move_to_next<L: LruList>(
        &mut self,
        bucket: &Bucket<K, V, L, TYPE>,
        guard: &'g Guard,
    ) -> bool {
        if self.current_index != usize::MAX {
            if self.current_link_ptr.is_null()
                && self.next_entry::<L, BUCKET_LEN>(&bucket.metadata, guard)
            {
                return true;
            }
            while let Some(link) = self.current_link_ptr.as_ref() {
                if self.next_entry::<L, LINKED_BUCKET_LEN>(&link.metadata, guard) {
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
    /// The [`EntryPtr`] must point to an occupied entry.
    #[inline]
    pub(crate) fn get(&self, data_block: &'g DataBlock<K, V, BUCKET_LEN>) -> &'g (K, V) {
        debug_assert_ne!(self.current_index, usize::MAX);

        let entry_ptr = if let Some(link) = self.current_link_ptr.as_ref() {
            Bucket::<K, V, (), TYPE>::entry_ptr(&link.data_block, self.current_index)
        } else {
            Bucket::<K, V, (), TYPE>::entry_ptr(data_block, self.current_index)
        };
        unsafe { &(*entry_ptr) }
    }

    /// Gets a mutable reference to the entry.
    ///
    /// The associated [`Bucket`] must be locked, and the [`EntryPtr`] must point to a valid entry.
    #[inline]
    pub(crate) fn get_mut<L: LruList>(
        &mut self,
        data_block: &DataBlock<K, V, BUCKET_LEN>,
        _writer: &Writer<K, V, L, TYPE>,
    ) -> &mut (K, V) {
        debug_assert_ne!(self.current_index, usize::MAX);

        let entry_ptr = if let Some(link) = self.current_link_ptr.as_ref() {
            Bucket::<K, V, L, TYPE>::entry_mut_ptr(&link.data_block, self.current_index)
        } else {
            Bucket::<K, V, L, TYPE>::entry_mut_ptr(data_block, self.current_index)
        };
        unsafe { &mut (*entry_ptr) }
    }

    /// Gets the partial hash value of the entry.
    ///
    /// The [`EntryPtr`] must point to an occupied entry.
    #[inline]
    pub(crate) fn partial_hash<L: LruList>(&self, bucket: &Bucket<K, V, L, TYPE>) -> u8 {
        debug_assert_ne!(self.current_index, usize::MAX);

        if let Some(link) = self.current_link_ptr.as_ref() {
            Bucket::<K, V, L, TYPE>::partial_hash(
                &link.metadata.partial_hash_array,
                self.current_index,
            )
        } else {
            Bucket::<K, V, L, TYPE>::partial_hash(
                &bucket.metadata.partial_hash_array,
                self.current_index,
            )
        }
    }

    /// Unlinks the [`LinkedBucket`] currently pointed to by the [`EntryPtr`] from the linked list.
    ///
    /// The associated [`Bucket`] must be locked.
    fn unlink<L: LruList>(
        &mut self,
        bucket: &Bucket<K, V, L, TYPE>,
        link: &LinkedBucket<K, V, LINKED_BUCKET_LEN>,
        guard: &'g Guard,
    ) {
        let prev_link_ptr = link.prev_link.load(Relaxed);
        let next_link = if TYPE == OPTIMISTIC {
            link.metadata.link.get_shared(Relaxed, guard)
        } else {
            link.metadata.link.swap((None, Tag::None), Relaxed).0
        };
        if let Some(next_link) = next_link.as_ref() {
            next_link.prev_link.store(prev_link_ptr, Relaxed);
        }

        self.current_link_ptr = next_link
            .as_ref()
            .map_or_else(Ptr::null, |n| n.get_guarded_ptr(guard));
        let old_link = if let Some(prev_link) = unsafe { prev_link_ptr.as_ref() } {
            prev_link
                .metadata
                .link
                .swap((next_link, Tag::None), Relaxed)
                .0
        } else {
            bucket.metadata.link.swap((next_link, Tag::None), Relaxed).0
        };
        let released = old_link.is_none_or(|l| {
            if TYPE == OPTIMISTIC {
                l.release()
            } else {
                // The `LinkedBucket` should be dropped immediately.
                unsafe { l.drop_in_place() }
            }
        });
        debug_assert!(TYPE == OPTIMISTIC || released);

        if self.current_link_ptr.is_null() {
            // Fuse the `EntryPtr`.
            self.current_index = usize::MAX;
        } else {
            // Go to the next `Link`.
            self.current_index = LINKED_BUCKET_LEN;
        }
    }

    /// Moves the [`EntryPtr`] to the next occupied entry in the [`Bucket`].
    ///
    /// Returns `false` if it currently points to the last entry.
    fn next_entry<L: LruList, const LEN: usize>(
        &mut self,
        metadata: &Metadata<K, V, LEN>,
        guard: &'g Guard,
    ) -> bool {
        // Search for the next occupied entry.
        let current_index = if self.current_index == LEN {
            0
        } else {
            self.current_index + 1
        };

        if current_index < LEN {
            let bitmap = if TYPE == OPTIMISTIC {
                (metadata.occupied_bitmap.load(Acquire)
                    & (!metadata.removed_bitmap_or_lru_tail.load(Relaxed)))
                    & (!((1_u32 << current_index) - 1))
            } else {
                metadata.occupied_bitmap.load(Relaxed) & (!((1_u32 << current_index) - 1))
            };

            let next_index = bitmap.trailing_zeros() as usize;
            if next_index < LEN {
                self.current_index = next_index;
                return true;
            }
        }

        self.current_link_ptr = metadata.link.load(Acquire, guard);
        self.current_index = LINKED_BUCKET_LEN;

        false
    }
}

impl<K, V, const TYPE: char> Clone for EntryPtr<'_, K, V, TYPE> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            current_link_ptr: self.current_link_ptr,
            current_index: self.current_index,
        }
    }
}

impl<K, V, const TYPE: char> Debug for EntryPtr<'_, K, V, TYPE> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntryPtr")
            .field("current_link_ptr", &self.current_link_ptr)
            .field("current_index", &self.current_index)
            .finish()
    }
}

unsafe impl<K: Send, V: Send, const TYPE: char> Send for EntryPtr<'_, K, V, TYPE> {}
unsafe impl<K: Send + Sync, V: Send + Sync, const TYPE: char> Sync for EntryPtr<'_, K, V, TYPE> {}

impl<K, V, const LEN: usize> Index<usize> for DataBlock<K, V, LEN> {
    type Output = UnsafeCell<MaybeUninit<(K, V)>>;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

unsafe impl<K: Send, V: Send, const LEN: usize> Send for DataBlock<K, V, LEN> {}
unsafe impl<K: Send + Sync, V: Send + Sync, const LEN: usize> Sync for DataBlock<K, V, LEN> {}

impl<'g, K, V, L: LruList, const TYPE: char> Writer<'g, K, V, L, TYPE> {
    /// Locks the [`Bucket`] asynchronously.
    #[inline]
    pub(crate) async fn lock_async(
        bucket: &'g Bucket<K, V, L, TYPE>,
        sendable_guard: &'g SendableGuard,
    ) -> Option<Writer<'g, K, V, L, TYPE>> {
        // `sendable_guard` should be reset when there is a chance that the task may suspend, since
        // `Guard` cannot survive across awaits.
        if bucket
            .rw_lock
            .lock_async_with(|| sendable_guard.reset())
            .await
        {
            // The `bucket` was not killed, and will not be killed until the `Reader` is dropped.
            // This guarantees that the `BucketArray` will survive as long as the `Reader` is alive.
            Some(Writer { bucket })
        } else {
            None
        }
    }

    /// Locks the [`Bucket`] synchronously.
    #[inline]
    pub(crate) fn lock_sync(
        bucket: &'g Bucket<K, V, L, TYPE>,
        _guard: &'g Guard,
    ) -> Option<Writer<'g, K, V, L, TYPE>> {
        if bucket.rw_lock.lock_sync() {
            Some(Writer { bucket })
        } else {
            None
        }
    }

    /// Tries to lock the [`Bucket`].
    #[inline]
    pub(crate) fn try_lock(
        bucket: &'g Bucket<K, V, L, TYPE>,
        _guard: &'g Guard,
    ) -> Result<Option<Writer<'g, K, V, L, TYPE>>, ()> {
        if bucket.rw_lock.try_lock() {
            Ok(Some(Writer { bucket }))
        } else if bucket.rw_lock.is_poisoned(Relaxed) {
            Ok(None)
        } else {
            Err(())
        }
    }

    /// Creates a new [`Writer`] from a [`Bucket`].
    #[inline]
    pub(crate) fn from_bucket(bucket: &'g Bucket<K, V, L, TYPE>) -> Writer<'g, K, V, L, TYPE> {
        Writer { bucket }
    }

    /// Marks the [`Bucket`] killed by poisoning the lock.
    #[inline]
    pub(super) fn kill(self) {
        debug_assert_eq!(self.len(), 0);
        debug_assert!(self.metadata.link.is_null(Relaxed));
        debug_assert!(
            TYPE != OPTIMISTIC
                || self.metadata.removed_bitmap_or_lru_tail.load(Relaxed)
                    == self.metadata.occupied_bitmap.load(Relaxed)
        );

        let poisoned = self.rw_lock.poison_lock();
        debug_assert!(poisoned);
        forget(self);
    }
}

impl<K, V, L: LruList, const TYPE: char> Deref for Writer<'_, K, V, L, TYPE> {
    type Target = Bucket<K, V, L, TYPE>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.bucket
    }
}

impl<K, V, L: LruList, const TYPE: char> Drop for Writer<'_, K, V, L, TYPE> {
    #[inline]
    fn drop(&mut self) {
        self.bucket.rw_lock.release_lock();
    }
}

impl<'g, K, V, L: LruList, const TYPE: char> Reader<'g, K, V, L, TYPE> {
    /// Locks the [`Bucket`] asynchronously.
    #[inline]
    pub(crate) async fn lock_async(
        bucket: &'g Bucket<K, V, L, TYPE>,
        sendable_guard: &'g SendableGuard,
    ) -> Option<Reader<'g, K, V, L, TYPE>> {
        // `sendable_guard` should be reset when there is a chance that the task may suspend, since
        // `Guard` cannot survive across awaits.
        if bucket
            .rw_lock
            .share_async_with(|| sendable_guard.reset())
            .await
        {
            // The `bucket` was not killed, and will not be killed until the `Reader` is dropped.
            // This guarantees that the `BucketArray` will survive as long as the `Reader` is alive.
            Some(Reader { bucket })
        } else {
            None
        }
    }

    /// Locks the [`Bucket`] synchronously.
    ///
    /// Returns `None` if the [`Bucket`] has been killed or empty.
    #[inline]
    pub(crate) fn lock_sync(
        bucket: &'g Bucket<K, V, L, TYPE>,
        _guard: &'g Guard,
    ) -> Option<Reader<'g, K, V, L, TYPE>> {
        if bucket.rw_lock.share_sync() {
            Some(Reader { bucket })
        } else {
            None
        }
    }

    /// Releases the lock.
    #[inline]
    pub(super) fn release(bucket: &Bucket<K, V, L, TYPE>) {
        bucket.rw_lock.release_share();
    }
}

impl<'g, K, V, L: LruList, const TYPE: char> Deref for Reader<'g, K, V, L, TYPE> {
    type Target = &'g Bucket<K, V, L, TYPE>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.bucket
    }
}

impl<K, V, L: LruList, const TYPE: char> Drop for Reader<'_, K, V, L, TYPE> {
    #[inline]
    fn drop(&mut self) {
        Self::release(self.bucket);
    }
}

impl<K, V, const LEN: usize> Default for Metadata<K, V, LEN> {
    #[inline]
    fn default() -> Self {
        Self {
            link: AtomicShared::default(),
            occupied_bitmap: AtomicU32::new(0),
            removed_bitmap_or_lru_tail: AtomicU32::new(0),
            partial_hash_array: UnsafeCell::new([0_u8; LEN]),
        }
    }
}

impl LruList for () {}

impl DoublyLinkedList {
    /// Loads the slot.
    fn load(&self, index: usize) -> (u8, u8) {
        (self.0[index].0.load(Relaxed), self.0[index].1.load(Relaxed))
    }

    /// Stores the value in the slot.
    fn store(&self, index: usize, value: (u8, u8)) {
        self.0[index].0.store(value.0, Relaxed);
        self.0[index].1.store(value.1, Relaxed);
    }
}

impl Default for DoublyLinkedList {
    #[inline]
    fn default() -> Self {
        Self([
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
            (AtomicU8::default(), AtomicU8::default()),
        ])
    }
}

impl LruList for DoublyLinkedList {
    #[inline]
    fn evict(&self, tail: u32) -> Option<(u8, u32)> {
        if tail == 0 {
            None
        } else {
            let lru = self.0[tail as usize - 1].0.load(Relaxed);
            let new_tail = if tail - 1 == u32::from(lru) {
                // Reset the linked list.
                0
            } else {
                let new_lru = self.0[lru as usize].0.load(Relaxed);
                {
                    #![allow(clippy::cast_possible_truncation)]
                    self.0[new_lru as usize].1.store(tail as u8 - 1, Relaxed);
                }
                self.0[tail as usize - 1].0.store(new_lru, Relaxed);
                tail
            };
            self.store(lru as usize, (0, 0));
            Some((lru, new_tail))
        }
    }

    #[inline]
    fn remove(&self, tail: u32, entry: u8) -> Option<u32> {
        if tail == 0
            || (self.load(entry as usize) == (0, 0)
                && (self.load(0) != (entry, entry) || (tail != 1 && tail != u32::from(entry) + 1)))
        {
            // The linked list is empty, or the entry is not a part of the linked list.
            return None;
        }

        if self.0[entry as usize].0.load(Relaxed) == entry {
            // It is the head and the only entry of the linked list.
            debug_assert_eq!(tail, u32::from(entry) + 1);
            self.store(entry as usize, (0, 0));
            return Some(0);
        }

        // Adjust `prev -> current`.
        let (prev, next) = self.load(entry as usize);
        debug_assert_eq!(self.0[prev as usize].1.load(Relaxed), entry);
        self.0[prev as usize].1.store(next, Relaxed);

        // Adjust `next -> current`.
        debug_assert_eq!(self.0[next as usize].0.load(Relaxed), entry);
        self.0[next as usize].0.store(prev, Relaxed);

        let new_tail = if tail == u32::from(entry) + 1 {
            // Update `head`.
            Some(u32::from(next) + 1)
        } else {
            None
        };
        self.store(entry as usize, (0, 0));

        new_tail
    }

    #[inline]
    fn promote(&self, tail: u32, entry: u8) -> Option<u32> {
        if tail == u32::from(entry) + 1 {
            // Nothing to do.
            return None;
        } else if tail == 0 {
            // The linked list is empty.
            self.store(entry as usize, (entry, entry));
            return Some(u32::from(entry) + 1);
        }

        // Remove the entry from the linked list only if it is a part of it.
        if self.load(entry as usize) != (0, 0) || (self.load(0) == (entry, entry) && tail == 1) {
            // Adjust `prev -> current`.
            let (prev, next) = self.load(entry as usize);
            debug_assert_eq!(self.0[prev as usize].1.load(Relaxed), entry);
            self.0[prev as usize].1.store(next, Relaxed);

            // Adjust `next -> current`.
            debug_assert_eq!(self.0[next as usize].0.load(Relaxed), entry);
            self.0[next as usize].0.store(prev, Relaxed);
        }

        // Adjust `oldest -> head`.
        let oldest = self.0[tail as usize - 1].0.load(Relaxed);
        debug_assert_eq!(u32::from(self.0[oldest as usize].1.load(Relaxed)) + 1, tail);
        self.0[oldest as usize].1.store(entry, Relaxed);
        self.0[entry as usize].0.store(oldest, Relaxed);

        // Adjust `head -> new head`
        self.0[tail as usize - 1].0.store(entry, Relaxed);
        {
            #![allow(clippy::cast_possible_truncation)]
            self.0[entry as usize].1.store(tail as u8 - 1, Relaxed);
        }

        // Update `head`.
        Some(u32::from(entry) + 1)
    }
}

unsafe impl<K: Send, V: Send, const LEN: usize> Send for Metadata<K, V, LEN> {}
unsafe impl<K: Send + Sync, V: Send + Sync, const LEN: usize> Sync for Metadata<K, V, LEN> {}

impl<K, V, const LEN: usize> LinkedBucket<K, V, LEN> {
    /// Creates an empty [`LinkedBucket`].
    fn new(next: Option<Shared<LinkedBucket<K, V, LINKED_BUCKET_LEN>>>) -> Self {
        let mut bucket = Self {
            metadata: Metadata::default(),
            data_block: unsafe {
                #[allow(clippy::uninit_assumed_init)]
                MaybeUninit::uninit().assume_init()
            },
            prev_link: AtomicPtr::default(),
        };
        if let Some(next) = next {
            bucket.metadata.link = AtomicShared::from(next);
        }
        bucket
    }
}

impl<K, V, const LEN: usize> Debug for LinkedBucket<K, V, LEN> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LinkedBucket").finish()
    }
}

impl<K, V, const LEN: usize> Drop for LinkedBucket<K, V, LEN> {
    #[inline]
    fn drop(&mut self) {
        if needs_drop::<(K, V)>() {
            let mut occupied_bitmap = self.metadata.occupied_bitmap.load(Relaxed);
            let mut index = occupied_bitmap.trailing_zeros();
            while index != 32 {
                Bucket::<K, V, (), SEQUENTIAL>::drop_entry(&self.data_block, index as usize);
                occupied_bitmap -= 1_u32 << index;
                index = occupied_bitmap.trailing_zeros();
            }
        }
    }
}

#[cfg(not(feature = "loom"))]
#[cfg(test)]
mod test {
    use std::mem::MaybeUninit;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicU32};

    use proptest::prelude::*;
    use saa::Lock;
    use sdd::{Guard, Shared};
    use tokio::sync::Barrier;

    use super::{
        BUCKET_LEN, Bucket, CACHE, DataBlock, DoublyLinkedList, EntryPtr, LruList, Metadata,
        Reader, SEQUENTIAL, Writer,
    };

    #[cfg(not(miri))]
    static_assertions::assert_eq_size!(Bucket<String, String, (), SEQUENTIAL>, [u8; BUCKET_LEN * 2]);
    #[cfg(not(miri))]
    static_assertions::assert_eq_size!(Bucket<String, String, DoublyLinkedList, CACHE>, [u8; BUCKET_LEN * 4]);

    fn default_bucket<K: Eq, V, L: LruList, const TYPE: char>() -> Bucket<K, V, L, TYPE> {
        Bucket {
            len: AtomicU32::new(0),
            epoch: AtomicU8::new(0),
            metadata: Metadata::default(),
            rw_lock: Lock::default(),
            lru_list: L::default(),
        }
    }

    proptest! {
        #[cfg_attr(miri, ignore)]
        #[test]
        fn evict_untracked(xs in 0..BUCKET_LEN * 2) {
            let data_block: DataBlock<usize, usize, BUCKET_LEN> =
                unsafe { MaybeUninit::uninit().assume_init() };
            let bucket: Bucket<usize, usize, DoublyLinkedList, CACHE> = default_bucket();
            for v in 0..xs {
                let guard = Guard::new();
                let writer = Writer::lock_sync(&bucket, &guard).unwrap();
                let evicted = writer.evict_lru_head(&data_block);
                assert_eq!(v >= BUCKET_LEN, evicted.is_some());
                writer.insert_with(&data_block, 0, || (v, v), &guard);
                assert_eq!(writer.metadata.removed_bitmap_or_lru_tail.load(Relaxed), 0);
            }
        }

        #[cfg_attr(miri, ignore)]
        #[test]
        fn evict_overflowed(xs in 1..BUCKET_LEN * 2) {
            let data_block: DataBlock<usize, usize, BUCKET_LEN> =
                unsafe { MaybeUninit::uninit().assume_init() };
            let bucket: Bucket<usize, usize, DoublyLinkedList, CACHE> = default_bucket();
            let guard = Guard::new();
            let writer = Writer::lock_sync(&bucket, &guard).unwrap();
            for _ in 0..3 {
                for v in 0..xs {
                    let entry_ptr = writer.insert_with(&data_block, 0, || (v, v), &guard);
                    writer.update_lru_tail(&entry_ptr);
                    if v < BUCKET_LEN {
                        assert_eq!(writer.metadata.removed_bitmap_or_lru_tail.load(Relaxed) as usize, v + 1);
                    }
                    assert_eq!(
                        writer
                            .lru_list.0[writer.metadata.removed_bitmap_or_lru_tail.load(Relaxed) as usize - 1]
                            .0
                            .load(Relaxed), 0);
                }

                let mut evicted_key = None;
                if xs >= BUCKET_LEN {
                    let evicted = writer.evict_lru_head(&data_block);
                    assert!(evicted.is_some());
                    evicted_key = evicted.map(|(k, _)| k);
                }
                assert_ne!(writer.metadata.removed_bitmap_or_lru_tail.load(Relaxed), 0);

                for v in 0..xs {
                    let mut entry_ptr = writer.get_entry_ptr(&data_block, &v, 0, &guard);
                    if entry_ptr.is_valid() {
                        let _erased = writer.remove(&data_block, &mut entry_ptr, &guard);
                    } else {
                        assert_eq!(v, evicted_key.unwrap());
                    }
                }
                assert_eq!(writer.metadata.removed_bitmap_or_lru_tail.load(Relaxed), 0);
            }
        }

        #[cfg_attr(miri, ignore)]
        #[test]
        fn evict_tracked(xs in 0..BUCKET_LEN * 2) {
            let data_block: DataBlock<usize, usize, BUCKET_LEN> =
                unsafe { MaybeUninit::uninit().assume_init() };
            let bucket: Bucket<usize, usize, DoublyLinkedList, CACHE> = default_bucket();
            for v in 0..xs {
                let guard = Guard::new();
                let writer = Writer::lock_sync(&bucket, &guard).unwrap();
                let evicted = writer.evict_lru_head(&data_block);
                assert_eq!(v >= BUCKET_LEN, evicted.is_some());
                let mut entry_ptr = writer.insert_with(&data_block, 0, || (v, v), &guard);
                writer.update_lru_tail(&entry_ptr);
                assert_eq!(writer.metadata.removed_bitmap_or_lru_tail.load(Relaxed) as usize, entry_ptr.current_index + 1);
                if v >= BUCKET_LEN {
                    entry_ptr.current_index = xs % BUCKET_LEN;
                    writer.update_lru_tail(&entry_ptr);
                    assert_eq!(writer.metadata.removed_bitmap_or_lru_tail.load(Relaxed) as usize, entry_ptr.current_index + 1);
                    let mut iterated = 1;
                    let mut i = writer.lru_list.0[entry_ptr.current_index].1.load(Relaxed) as usize;
                    while i != entry_ptr.current_index {
                        iterated += 1;
                        i = writer.lru_list.0[i].1.load(Relaxed) as usize;
                    }
                    assert_eq!(iterated, BUCKET_LEN);
                    iterated = 1;
                    i = writer.lru_list.0[entry_ptr.current_index].0.load(Relaxed) as usize;
                    while i != entry_ptr.current_index {
                        iterated += 1;
                        i = writer.lru_list.0[i].0.load(Relaxed) as usize;
                    }
                    assert_eq!(iterated, BUCKET_LEN);
                }
            }
        }

        #[cfg_attr(miri, ignore)]
        #[test]
        fn removed(xs in 0..BUCKET_LEN) {
            let data_block: DataBlock<usize, usize, BUCKET_LEN> =
                unsafe { MaybeUninit::uninit().assume_init() };
            let bucket: Bucket<usize, usize, DoublyLinkedList, CACHE> = default_bucket();
            for v in 0..xs {
                let guard = Guard::new();
                let writer = Writer::lock_sync(&bucket, &guard).unwrap();
                let entry_ptr = writer.insert_with(&data_block, 0, || (v, v), &guard);
                writer.update_lru_tail(&entry_ptr);
                let mut iterated = 1;
                let mut i = writer.lru_list.0[entry_ptr.current_index].1.load(Relaxed) as usize;
                while i != entry_ptr.current_index {
                    iterated += 1;
                    i = writer.lru_list.0[i].1.load(Relaxed) as usize;
                }
                assert_eq!(iterated, v + 1);
            }
            for v in 0..xs {
                let guard = Guard::new();
                let writer = Writer::lock_sync(&bucket, &guard).unwrap();
                let entry_ptr = writer.get_entry_ptr(&data_block, &v, 0, &guard);
                let mut iterated = 1;
                let mut i = writer.lru_list.0[entry_ptr.current_index].1.load(Relaxed) as usize;
                while i != entry_ptr.current_index {
                    iterated += 1;
                    i = writer.lru_list.0[i].1.load(Relaxed) as usize;
                }
                assert_eq!(iterated, xs - v);
                writer.remove_from_lru_list(&entry_ptr);
            }
            assert_eq!(bucket.metadata.removed_bitmap_or_lru_tail.load(Relaxed), 0);
        }

    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn bucket_lock_sync() {
        let num_tasks = BUCKET_LEN + 2;
        let barrier = Shared::new(Barrier::new(num_tasks));
        let data_block: Shared<DataBlock<usize, usize, BUCKET_LEN>> =
            Shared::new(unsafe { MaybeUninit::uninit().assume_init() });
        let mut bucket: Shared<Bucket<usize, usize, (), SEQUENTIAL>> =
            Shared::new(default_bucket());
        let mut data: [u64; 128] = [0; 128];
        let mut task_handles = Vec::with_capacity(num_tasks);
        for task_id in 0..num_tasks {
            let barrier_clone = barrier.clone();
            let data_block_clone = data_block.clone();
            let bucket_clone = bucket.clone();
            let data_ptr = AtomicPtr::new(&raw mut data);
            task_handles.push(tokio::spawn(async move {
                barrier_clone.wait().await;
                let partial_hash = (task_id % BUCKET_LEN).try_into().unwrap();
                let guard = Guard::new();
                for i in 0..2048 {
                    let writer = Writer::lock_sync(&bucket_clone, &guard).unwrap();
                    let mut sum: u64 = 0;
                    for j in 0..128 {
                        unsafe {
                            sum += (*data_ptr.load(Relaxed))[j];
                            (*data_ptr.load(Relaxed))[j] = if i % 4 == 0 { 2 } else { 4 }
                        };
                    }
                    assert_eq!(sum % 256, 0);
                    if i == 0 {
                        writer.insert_with(
                            &data_block_clone,
                            partial_hash,
                            || (task_id, 0),
                            &guard,
                        );
                    } else {
                        assert_eq!(
                            writer
                                .search_entry(&data_block_clone, &task_id, partial_hash, &guard)
                                .unwrap(),
                            &(task_id, 0_usize)
                        );
                    }
                    drop(writer);

                    let reader = Reader::lock_sync(&*bucket_clone, &guard).unwrap();
                    assert_eq!(
                        reader
                            .search_entry(&data_block_clone, &task_id, partial_hash, &guard)
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
        assert_eq!(bucket.len(), num_tasks);

        let epoch_guard = Guard::new();
        for task_id in 0..num_tasks {
            assert_eq!(
                bucket.search_entry(
                    &data_block,
                    &task_id,
                    (task_id % BUCKET_LEN).try_into().unwrap(),
                    &epoch_guard
                ),
                Some(&(task_id, 0))
            );
        }

        let mut count = 0;
        let mut entry_ptr = EntryPtr::new(&epoch_guard);
        while entry_ptr.move_to_next(&bucket, &epoch_guard) {
            count += 1;
        }
        assert_eq!(bucket.len(), count);

        entry_ptr = EntryPtr::new(&epoch_guard);
        let writer = Writer::lock_sync(&bucket, &epoch_guard).unwrap();
        while entry_ptr.move_to_next(&writer, &epoch_guard) {
            writer.remove(&data_block, &mut entry_ptr, &epoch_guard);
        }
        assert_eq!(writer.len(), 0);
        writer.kill();

        assert!(bucket.killed());
        assert_eq!(bucket.len(), 0);
        assert!(Writer::lock_sync(unsafe { bucket.get_mut().unwrap() }, &epoch_guard).is_none());
    }
}
