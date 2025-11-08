use std::cell::UnsafeCell;
use std::fmt::{self, Debug};
use std::mem::{MaybeUninit, forget, needs_drop};
use std::ops::{Deref, Index};
use std::ptr::{NonNull, from_ref};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicPtr, AtomicU32};

use saa::Lock;
use sdd::{AtomicShared, Epoch, Guard, Ptr, Shared, Tag};

use crate::Equivalent;
use crate::async_helper::AsyncGuard;

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
    epoch: UnsafeCell<Option<Epoch>>,
    /// [`Bucket`] metadata.
    metadata: Metadata<K, V, BUCKET_LEN>,
    /// Reader-writer lock.
    rw_lock: Lock,
    /// The LRU list of the [`Bucket`].
    lru_list: L,
}

/// The type of [`Bucket`] that only allows sequential access to it.
pub const MAP: char = 'S';

/// The type of [`Bucket`] that allows lock-free read access.
pub const INDEX: char = 'O';

/// The type of [`Bucket`] that acts as an LRU cache.
pub const CACHE: char = 'C';

/// The size of the fixed-size entry array in a [`Bucket`].
pub const BUCKET_LEN: usize = u32::BITS as usize;

/// [`DataBlock`] is a type alias of a raw memory chunk of entries.
pub struct DataBlock<K, V, const LEN: usize>([UnsafeCell<MaybeUninit<(K, V)>>; LEN]);

/// [`Writer`] holds an exclusive lock on a [`Bucket`].
#[derive(Debug)]
pub struct Writer<K, V, L: LruList, const TYPE: char> {
    bucket: NonNull<Bucket<K, V, L, TYPE>>,
}

/// [`Reader`] holds a shared lock on a [`Bucket`].
#[derive(Debug)]
pub struct Reader<K, V, L: LruList, const TYPE: char> {
    bucket: NonNull<Bucket<K, V, L, TYPE>>,
}

/// [`EntryPtr`] points to an entry slot in a [`Bucket`].
pub struct EntryPtr<'g, K, V, const TYPE: char> {
    /// Pointer to a [`LinkedBucket`].
    current_link_ptr: Ptr<'g, LinkedBucket<K, V>>,
    /// Index of the entry.
    current_index: usize,
}

/// Doubly-linked list interfaces to efficiently manage least-recently-used entries.
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

/// [`DoublyLinkedList`] is an array of `(u8, u8)` implementing [`LruList`].
#[derive(Debug, Default)]
pub struct DoublyLinkedList([UnsafeCell<(u8, u8)>; BUCKET_LEN]);

/// [`Metadata`] is a collection of metadata fields of [`Bucket`] and [`LinkedBucket`].
struct Metadata<K, V, const LEN: usize> {
    /// Linked list of entries.
    link: AtomicShared<LinkedBucket<K, V>>,
    /// Occupied slot bitmap.
    occupied_bitmap: AtomicU32,
    /// Removed slot bitmap, or the 1-based index of the most recently used entry if
    /// `TYPE = CACHE` where `0` represents `nil`.
    removed_bitmap_or_lru_tail: AtomicU32,
    /// Partial hash array for fast hash lookup.
    partial_hash_array: [UnsafeCell<u8>; LEN],
}

/// The size of the linked data block.
const LINKED_BUCKET_LEN: usize = BUCKET_LEN / 4;

/// [`LinkedBucket`] is a smaller [`Bucket`] that is attached to a [`Bucket`] as a linked list.
struct LinkedBucket<K, V> {
    /// [`LinkedBucket`] metadata.
    metadata: Metadata<K, V, LINKED_BUCKET_LEN>,
    /// Own data block.
    data_block: DataBlock<K, V, LINKED_BUCKET_LEN>,
    /// Previous [`LinkedBucket`].
    prev_link: AtomicPtr<LinkedBucket<K, V>>,
}

impl<K, V, L: LruList, const TYPE: char> Bucket<K, V, L, TYPE> {
    /// Creates a new [`Bucket`].
    #[cfg(any(test, feature = "loom"))]
    pub fn new() -> Self {
        Self {
            len: AtomicU32::new(0),
            epoch: UnsafeCell::new(None),
            rw_lock: Lock::default(),
            metadata: Metadata {
                link: AtomicShared::default(),
                occupied_bitmap: AtomicU32::default(),
                removed_bitmap_or_lru_tail: AtomicU32::default(),
                partial_hash_array: Default::default(),
            },
            lru_list: L::default(),
        }
    }

    /// Returns the number of occupied and reachable slots in the [`Bucket`].
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len.load(Relaxed) as usize
    }

    /// Returns `true` if the [`Bucket`] needs to be rebuilt.
    ///
    /// If `TYPE == OPTIMISTIC`, removed entries are rarely dropped; therefore, rebuilding the
    /// [`Bucket`] might be needed to keep the [`Bucket`] as small as possible.
    #[inline]
    pub(crate) fn need_rebuild(&self) -> bool {
        TYPE == INDEX
            && self.metadata.removed_bitmap_or_lru_tail.load(Relaxed)
                == (u32::MAX >> (32 - BUCKET_LEN))
    }

    /// Returns the partial hash value of the given hash.
    #[allow(clippy::cast_possible_truncation)]
    #[inline]
    pub const fn partial_hash(hash: u64) -> u8 {
        hash as u8
    }

    /// Reserves memory for insertion and then constructs the key-value pair in-place.
    #[inline]
    pub(crate) fn insert<'g>(
        &self,
        data_block: NonNull<DataBlock<K, V, BUCKET_LEN>>,
        hash: u64,
        entry: (K, V),
        guard: &'g Guard,
    ) -> EntryPtr<'g, K, V, TYPE> {
        let len = self.len.load(Relaxed);
        assert_ne!(len, u32::MAX, "bucket overflow");

        let occupied_bitmap = self.metadata.occupied_bitmap.load(Relaxed);
        let free_index = occupied_bitmap.trailing_ones() as usize;
        if free_index == BUCKET_LEN {
            self.insert_overflow(len, hash, entry, guard)
        } else {
            self.insert_entry(
                &self.metadata,
                unsafe { data_block.as_ref() },
                free_index,
                occupied_bitmap,
                hash,
                entry,
            );
            self.len.store(len + 1, Relaxed);
            EntryPtr {
                current_link_ptr: Ptr::null(),
                current_index: free_index,
            }
        }
    }

    /// Removes the entry pointed to by the supplied [`EntryPtr`].
    #[inline]
    pub(crate) fn remove<'g>(
        &self,
        data_block: NonNull<DataBlock<K, V, BUCKET_LEN>>,
        entry_ptr: &mut EntryPtr<'g, K, V, TYPE>,
        guard: &'g Guard,
    ) -> (K, V) {
        debug_assert_ne!(TYPE, INDEX);
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
            if occupied_bitmap == 0 && TYPE != INDEX {
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
            Self::read_data_block(unsafe { data_block.as_ref() }, entry_ptr.current_index)
        }
    }

    /// Marks the entry removed without dropping the entry.
    #[inline]
    pub(crate) fn mark_removed<'g>(
        &self,
        entry_ptr: &mut EntryPtr<'g, K, V, TYPE>,
        guard: &'g Guard,
    ) {
        debug_assert_eq!(TYPE, INDEX);
        debug_assert_ne!(entry_ptr.current_index, usize::MAX);
        debug_assert_ne!(entry_ptr.current_index, BUCKET_LEN);

        self.len.store(self.len.load(Relaxed) - 1, Relaxed);

        if let Some(link) = entry_ptr.current_link_ptr.as_ref() {
            let mut removed_bitmap = link.metadata.removed_bitmap_or_lru_tail.load(Relaxed);
            debug_assert_eq!(removed_bitmap & (1_u32 << entry_ptr.current_index), 0);

            removed_bitmap |= 1_u32 << entry_ptr.current_index;
            link.metadata
                .removed_bitmap_or_lru_tail
                .store(removed_bitmap, Release);
        } else {
            let mut removed_bitmap = self.metadata.removed_bitmap_or_lru_tail.load(Relaxed);
            debug_assert_eq!(removed_bitmap & (1_u32 << entry_ptr.current_index), 0);

            removed_bitmap |= 1_u32 << entry_ptr.current_index;
            self.metadata
                .removed_bitmap_or_lru_tail
                .store(removed_bitmap, Release);
        }
        self.update_epoch(Some(guard));
    }

    /// Evicts the least recently used entry if the [`Bucket`] is full.
    #[inline]
    pub(crate) fn evict_lru_head(
        &self,
        data_block: NonNull<DataBlock<K, V, BUCKET_LEN>>,
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
            return Some(Self::read_data_block(
                unsafe { data_block.as_ref() },
                evicted,
            ));
        }

        None
    }
    /// Sets the entry as having been just accessed.
    #[inline]
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

    /// Reserves memory for additional entries.
    pub(crate) fn reserve_slots(&self, additional: usize, guard: &Guard) {
        debug_assert!(self.rw_lock.is_locked(Relaxed));

        let mut capacity =
            BUCKET_LEN - self.metadata.occupied_bitmap.load(Relaxed).count_ones() as usize;
        if capacity >= additional {
            return;
        }

        let mut link_ptr = self.metadata.link.load(Acquire, guard);
        while let Some(link) = link_ptr.as_ref() {
            capacity += LINKED_BUCKET_LEN
                - link.metadata.occupied_bitmap.load(Relaxed).count_ones() as usize;
            if capacity >= additional {
                return;
            }
            link_ptr = link.metadata.link.load(Acquire, guard);
        }

        let additional_links = (additional - capacity).div_ceil(LINKED_BUCKET_LEN);
        for _ in 0..additional_links {
            let head = self.metadata.link.get_shared(Relaxed, guard);
            let link = unsafe { Shared::new_unchecked(LinkedBucket::new(head)) };
            if let Some(head) = link.metadata.link.load(Relaxed, guard).as_ref() {
                head.prev_link.store(link.as_ptr().cast_mut(), Relaxed);
            }
            self.metadata.link.swap((Some(link), Tag::None), Release);
        }
    }

    /// Extracts an entry from the given bucket and inserts the entry into itself.
    pub(crate) fn extract_from<'g>(
        &self,
        data_block: NonNull<DataBlock<K, V, BUCKET_LEN>>,
        hash: u64,
        from_writer: &Writer<K, V, L, TYPE>,
        from_data_block: NonNull<DataBlock<K, V, BUCKET_LEN>>,
        from_entry_ptr: &mut EntryPtr<'g, K, V, TYPE>,
        guard: &'g Guard,
    ) {
        debug_assert!(self.rw_lock.is_locked(Relaxed));

        // Copy the data without modifying the original entry.
        let entry = if let Some(link) = from_entry_ptr.current_link_ptr.as_ref() {
            Self::read_data_block(&link.data_block, from_entry_ptr.current_index)
        } else {
            Self::read_data_block(
                unsafe { from_data_block.as_ref() },
                from_entry_ptr.current_index,
            )
        };
        self.insert(data_block, hash, entry, guard);

        // Remove the entry from the old bucket.
        from_writer
            .len
            .store(from_writer.len.load(Relaxed) - 1, Release);
        if let Some(link) = from_entry_ptr.current_link_ptr.as_ref() {
            let mut occupied_bitmap = link.metadata.occupied_bitmap.load(Relaxed);
            debug_assert_ne!(occupied_bitmap & (1_u32 << from_entry_ptr.current_index), 0);

            occupied_bitmap &= !(1_u32 << from_entry_ptr.current_index);
            link.metadata
                .occupied_bitmap
                .store(occupied_bitmap, Relaxed);
            if occupied_bitmap == 0 && TYPE != INDEX {
                from_entry_ptr.unlink(from_writer, link, guard);
            }
        } else {
            let occupied_bitmap = from_writer.metadata.occupied_bitmap.load(Relaxed);
            debug_assert_ne!(occupied_bitmap & (1_u32 << from_entry_ptr.current_index), 0);

            from_writer.metadata.occupied_bitmap.store(
                occupied_bitmap & !(1_u32 << from_entry_ptr.current_index),
                Relaxed,
            );
        }
    }

    /// Drops entries in the [`DataBlock`] when the bucket array is being dropped.
    ///
    /// The [`Bucket`] and the [`DataBlock`] should never be used afterward.
    pub(super) fn drop_entries(&self, data_block: NonNull<DataBlock<K, V, BUCKET_LEN>>) {
        if !self.metadata.link.is_null(Relaxed) {
            let mut next = self.metadata.link.swap((None, Tag::None), Acquire);
            while let Some(current) = next.0 {
                next = current.metadata.link.swap((None, Tag::None), Acquire);
                let dropped = unsafe { current.drop_in_place() };
                debug_assert!(dropped);
            }
        }
        if needs_drop::<(K, V)>() {
            let mut occupied_bitmap = self.metadata.occupied_bitmap.load(Relaxed);
            if occupied_bitmap != 0 {
                let mut index = occupied_bitmap.trailing_zeros();
                while index != 32 {
                    Self::drop_entry(unsafe { data_block.as_ref() }, index as usize);
                    occupied_bitmap -= 1_u32 << index;
                    index = occupied_bitmap.trailing_zeros();
                }
            }
        }
    }

    /// Drops removed entries if they are completely unreachable, allowing others to reuse
    /// the memory.
    pub(super) fn drop_removed_unreachable_entries(
        &self,
        data_block: NonNull<DataBlock<K, V, BUCKET_LEN>>,
        guard: &Guard,
    ) {
        debug_assert_eq!(TYPE, INDEX);

        let prev_epoch = *Self::read_cell(&self.epoch);
        if prev_epoch.is_some_and(|e| !e.in_same_generation(guard.epoch())) {
            let mut empty =
                Self::cleanup_removed_entries(&self.metadata, unsafe { data_block.as_ref() });
            let mut link_ptr = self.metadata.link.load(Acquire, guard);
            while let Some(link) = link_ptr.as_ref() {
                empty &= Self::cleanup_removed_entries(&link.metadata, &link.data_block);
                let next_link_ptr = link.metadata.link.load(Acquire, guard);
                if next_link_ptr.is_null() {
                    self.cleanup_empty_link(link_ptr.as_ptr());
                }
                link_ptr = next_link_ptr;
            }
            if empty {
                self.update_epoch(None);
            }
        }
    }

    /// Clears removed entries if they are completely unreachable, allowing others to reuse
    /// the memory.
    fn cleanup_removed_entries<const LEN: usize>(
        metadata: &Metadata<K, V, LEN>,
        data_block: &DataBlock<K, V, LEN>,
    ) -> bool {
        debug_assert_eq!(TYPE, INDEX);

        let mut removed_bitmap = metadata.removed_bitmap_or_lru_tail.load(Relaxed);
        if removed_bitmap == 0 {
            return true;
        }

        let mut occupied_bitmap = metadata.occupied_bitmap.load(Relaxed);
        let mut index = removed_bitmap.trailing_zeros();
        while index != 32 {
            let bit = 1_u32 << index;
            debug_assert_eq!(occupied_bitmap & bit, bit);
            occupied_bitmap -= bit;
            removed_bitmap -= bit;
            Self::drop_entry(data_block, index as usize);
            index = removed_bitmap.trailing_zeros();
        }

        // The store ordering is important.
        //
        // Peek: `removed_bitmap` -> `acquire` -> `occupied_bitmap`.
        // Drop: `occupied_bitmap` -> `release` -> `removed_bitmap`.
        metadata.occupied_bitmap.store(occupied_bitmap, Release);
        metadata
            .removed_bitmap_or_lru_tail
            .store(removed_bitmap, Release);
        removed_bitmap == 0
    }

    /// Cleans up empty linked buckets.
    fn cleanup_empty_link(&self, mut link_ptr: *const LinkedBucket<K, V>) {
        debug_assert_eq!(TYPE, INDEX);

        while let Some(link) = unsafe { link_ptr.as_ref() } {
            if link.metadata.occupied_bitmap.load(Relaxed) != 0 {
                break;
            }
            link_ptr = link.prev_link.load(Relaxed);
            if let Some(prev) = unsafe { link_ptr.as_ref() } {
                let unlinked = prev.metadata.link.swap((None, Tag::None), Relaxed).0;
                let released = unlinked.is_some_and(Shared::release);
                debug_assert!(released);
            } else {
                let unlinked = self.metadata.link.swap((None, Tag::None), Relaxed).0;
                let released = unlinked.is_some_and(Shared::release);
                debug_assert!(released);
            }
        }
    }

    /// Inserts an entry into the linked list.
    fn insert_overflow<'g>(
        &self,
        len: u32,
        hash: u64,
        entry: (K, V),
        guard: &'g Guard,
    ) -> EntryPtr<'g, K, V, TYPE> {
        let mut link_ptr = self.metadata.link.load(Acquire, guard);
        while let Some(link) = link_ptr.as_ref() {
            let occupied_bitmap = link.metadata.occupied_bitmap.load(Relaxed);
            let free_index = occupied_bitmap.trailing_ones() as usize;
            if free_index != LINKED_BUCKET_LEN {
                debug_assert!(free_index < LINKED_BUCKET_LEN);
                self.insert_entry(
                    &link.metadata,
                    &link.data_block,
                    free_index,
                    occupied_bitmap,
                    hash,
                    entry,
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
            self.write_cell(&link.data_block[0], |block| unsafe {
                block.as_mut_ptr().write(entry);
            });
            self.write_cell(&link.metadata.partial_hash_array[0], |h| {
                *h = Self::partial_hash(hash);
            });
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

    /// Inserts a key-value pair in the slot.
    #[inline]
    fn insert_entry<const LEN: usize>(
        &self,
        metadata: &Metadata<K, V, LEN>,
        data_block: &DataBlock<K, V, LEN>,
        index: usize,
        occupied_bitmap: u32,
        hash: u64,
        entry: (K, V),
    ) {
        debug_assert!(index < LEN);
        debug_assert_eq!(metadata.occupied_bitmap.load(Relaxed) & (1_u32 << index), 0);

        self.write_cell(&data_block[index], |block| unsafe {
            block.as_mut_ptr().write(entry);
        });
        self.write_cell(&metadata.partial_hash_array[index], |h| {
            *h = Self::partial_hash(hash);
        });
        metadata.occupied_bitmap.store(
            occupied_bitmap | (1_u32 << index),
            if TYPE == INDEX { Release } else { Relaxed },
        );
    }

    /// Drops the data in place.
    #[inline]
    fn drop_entry<const LEN: usize>(data_block: &DataBlock<K, V, LEN>, index: usize) {
        unsafe {
            (*data_block[index].get()).as_mut_ptr().drop_in_place();
        }
    }

    /// Removes the entry from the LRU linked list.
    #[inline]
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
    #[inline]
    fn update_epoch(&self, guard: Option<&Guard>) {
        debug_assert_eq!(TYPE, INDEX);

        self.write_cell(&self.epoch, |e| {
            if let Some(guard) = guard {
                e.replace(guard.epoch());
            } else {
                e.take();
            }
        });
    }

    /// Reads the data at the given index.
    #[inline]
    const fn read_data_block<const LEN: usize>(
        data_block: &DataBlock<K, V, LEN>,
        index: usize,
    ) -> (K, V) {
        unsafe { (*data_block.0[index].get()).as_ptr().read() }
    }

    /// Returns a pointer to the slot in the [`DataBlock`].
    #[inline]
    const fn entry_ptr<const LEN: usize>(
        data_block: &DataBlock<K, V, LEN>,
        index: usize,
    ) -> *const (K, V) {
        Self::read_cell(&data_block.0[index]).as_ptr()
    }

    /// Returns a mutable pointer to the slot in the [`DataBlock`].
    #[inline]
    const fn entry_mut_ptr<const LEN: usize>(
        data_block: &DataBlock<K, V, LEN>,
        index: usize,
    ) -> *mut (K, V) {
        unsafe { (*data_block.0[index].get()).as_mut_ptr() }
    }

    /// Reads the cell.
    #[inline]
    const fn read_cell<T>(cell: &UnsafeCell<T>) -> &T {
        unsafe { &*cell.get() }
    }

    /// Writes the cell.
    #[inline]
    fn write_cell<T, R, F: FnOnce(&mut T) -> R>(&self, cell: &UnsafeCell<T>, f: F) -> R {
        debug_assert!(self.rw_lock.is_locked(Relaxed));
        unsafe { f(&mut *cell.get()) }
    }
}

impl<K: Eq, V, L: LruList, const TYPE: char> Bucket<K, V, L, TYPE> {
    /// Searches for an entry containing the key.
    ///
    /// Returns `None` if the key is not present.
    #[inline]
    pub(super) fn search_entry<'g, Q>(
        &self,
        data_block: NonNull<DataBlock<K, V, BUCKET_LEN>>,
        key: &Q,
        hash: u64,
        guard: &'g Guard,
    ) -> Option<&'g (K, V)>
    where
        Q: Equivalent<K> + ?Sized,
    {
        if self.len() == 0 {
            return None;
        }

        if let Some((entry, _)) =
            Self::search_data_block(&self.metadata, unsafe { data_block.as_ref() }, key, hash)
        {
            return Some(entry);
        }

        let mut link_ptr = self.metadata.link.load(Acquire, guard);
        while let Some(link) = link_ptr.as_ref() {
            if let Some((entry, _)) =
                Self::search_data_block(&link.metadata, &link.data_block, key, hash)
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
        data_block: NonNull<DataBlock<K, V, BUCKET_LEN>>,
        key: &Q,
        hash: u64,
        guard: &'g Guard,
    ) -> EntryPtr<'g, K, V, TYPE>
    where
        Q: Equivalent<K> + ?Sized,
    {
        if self.len() != 0 {
            if let Some((_, index)) =
                Self::search_data_block(&self.metadata, unsafe { data_block.as_ref() }, key, hash)
            {
                return EntryPtr {
                    current_link_ptr: Ptr::null(),
                    current_index: index,
                };
            }

            let mut current_link_ptr = self.metadata.link.load(Acquire, guard);
            while let Some(link) = current_link_ptr.as_ref() {
                if let Some((_, index)) =
                    Self::search_data_block(&link.metadata, &link.data_block, key, hash)
                {
                    return EntryPtr {
                        current_link_ptr,
                        current_index: index,
                    };
                }
                current_link_ptr = link.metadata.link.load(Acquire, guard);
            }
        }

        EntryPtr::new(guard)
    }

    /// Searches the supplied data block for the entry containing the key.
    #[inline]
    fn search_data_block<'g, Q, const LEN: usize>(
        metadata: &Metadata<K, V, LEN>,
        data_block: &'g DataBlock<K, V, LEN>,
        key: &Q,
        hash: u64,
    ) -> Option<(&'g (K, V), usize)>
    where
        Q: Equivalent<K> + ?Sized,
    {
        let mut bitmap = if TYPE == INDEX {
            // Load ordering: `removed_bitmap` -> `acquire` -> `occupied_bitmap`.
            (!metadata.removed_bitmap_or_lru_tail.load(Acquire))
                & metadata.occupied_bitmap.load(Acquire)
        } else {
            metadata.occupied_bitmap.load(Relaxed)
        };

        // Expect that the loop is vectorized by the compiler.
        for i in 0..LEN {
            if *Self::read_cell(&metadata.partial_hash_array[i]) != Self::partial_hash(hash) {
                bitmap &= !(1_u32 << i);
            }
        }

        let mut offset = bitmap.trailing_zeros();
        while offset != u32::BITS {
            let entry = unsafe { &*Self::entry_ptr(data_block, offset as usize) };
            if key.equivalent(&entry.0) {
                return Some((entry, offset as usize));
            }
            bitmap &= !(1_u32 << offset);
            offset = bitmap.trailing_zeros();
        }

        None
    }
}

unsafe impl<K: Eq + Send, V: Send, L: LruList, const TYPE: char> Send for Bucket<K, V, L, TYPE> {}
unsafe impl<K: Eq + Send + Sync, V: Send + Sync, L: LruList, const TYPE: char> Sync
    for Bucket<K, V, L, TYPE>
{
}

impl<K, V, const LEN: usize> Index<usize> for DataBlock<K, V, LEN> {
    type Output = UnsafeCell<MaybeUninit<(K, V)>>;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

unsafe impl<K: Send, V: Send, const LEN: usize> Send for DataBlock<K, V, LEN> {}
unsafe impl<K: Send + Sync, V: Send + Sync, const LEN: usize> Sync for DataBlock<K, V, LEN> {}

impl<K, V, L: LruList, const TYPE: char> Writer<K, V, L, TYPE> {
    /// Locks the [`Bucket`] asynchronously.
    #[inline]
    pub(crate) async fn lock_async<'g>(
        bucket: &'g Bucket<K, V, L, TYPE>,
        async_guard: &'g AsyncGuard,
    ) -> Option<Writer<K, V, L, TYPE>> {
        if bucket.rw_lock.lock_async_with(|| async_guard.reset()).await {
            // The `bucket` was not killed, and will not be killed until the `Writer` is dropped.
            // This guarantees that the `BucketArray` will survive as long as the `Writer` is alive.
            Some(Self::from_bucket(bucket))
        } else {
            None
        }
    }

    /// Locks the [`Bucket`] synchronously.
    #[inline]
    pub(crate) fn lock_sync(bucket: &Bucket<K, V, L, TYPE>) -> Option<Writer<K, V, L, TYPE>> {
        if bucket.rw_lock.lock_sync() {
            Some(Self::from_bucket(bucket))
        } else {
            None
        }
    }

    /// Tries to lock the [`Bucket`].
    #[inline]
    pub(crate) fn try_lock(
        bucket: &Bucket<K, V, L, TYPE>,
    ) -> Result<Option<Writer<K, V, L, TYPE>>, ()> {
        if bucket.rw_lock.try_lock() {
            Ok(Some(Self::from_bucket(bucket)))
        } else if bucket.rw_lock.is_poisoned(Relaxed) {
            Ok(None)
        } else {
            Err(())
        }
    }

    /// Creates a new [`Writer`] from a [`Bucket`].
    #[inline]
    pub(crate) const fn from_bucket(bucket: &Bucket<K, V, L, TYPE>) -> Writer<K, V, L, TYPE> {
        Writer {
            bucket: unsafe { NonNull::new_unchecked(from_ref(bucket).cast_mut()) },
        }
    }

    /// Marks the [`Bucket`] killed by poisoning the lock.
    #[inline]
    pub(super) fn kill(self) {
        debug_assert_eq!(self.len(), 0);
        debug_assert!(self.rw_lock.is_locked(Relaxed));
        debug_assert!(
            TYPE != INDEX
                || self.metadata.removed_bitmap_or_lru_tail.load(Relaxed)
                    == self.metadata.occupied_bitmap.load(Relaxed)
        );

        if TYPE != INDEX && !self.metadata.link.is_null(Relaxed) {
            let mut link = self.metadata.link.swap((None, Tag::None), Acquire).0;
            while let Some(current) = link {
                link = current.metadata.link.swap((None, Tag::None), Acquire).0;
                let dropped = unsafe { current.drop_in_place() };
                debug_assert!(dropped);
            }
        }

        let poisoned = self.rw_lock.poison_lock();
        debug_assert!(poisoned);
        forget(self);
    }
}

impl<K, V, L: LruList, const TYPE: char> Deref for Writer<K, V, L, TYPE> {
    type Target = Bucket<K, V, L, TYPE>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { self.bucket.as_ref() }
    }
}

impl<K, V, L: LruList, const TYPE: char> Drop for Writer<K, V, L, TYPE> {
    #[inline]
    fn drop(&mut self) {
        self.rw_lock.release_lock();
    }
}

unsafe impl<K: Send, V: Send, L: LruList, const TYPE: char> Send for Writer<K, V, L, TYPE> {}
unsafe impl<K: Send + Sync, V: Send + Sync, L: LruList, const TYPE: char> Sync
    for Writer<K, V, L, TYPE>
{
}

impl<'g, K, V, L: LruList, const TYPE: char> Reader<K, V, L, TYPE> {
    /// Locks the [`Bucket`] asynchronously.
    #[inline]
    pub(crate) async fn lock_async(
        bucket: &'g Bucket<K, V, L, TYPE>,
        async_guard: &AsyncGuard,
    ) -> Option<Reader<K, V, L, TYPE>> {
        if bucket
            .rw_lock
            .share_async_with(|| async_guard.reset())
            .await
        {
            // The `bucket` was not killed, and will not be killed until the `Reader` is dropped.
            // This guarantees that the `BucketArray` will survive as long as the `Reader` is alive.
            Some(Reader {
                bucket: unsafe { NonNull::new_unchecked(from_ref(bucket).cast_mut()) },
            })
        } else {
            None
        }
    }

    /// Locks the [`Bucket`] synchronously.
    ///
    /// Returns `None` if the [`Bucket`] has been killed or is empty.
    #[inline]
    pub(crate) fn lock_sync(bucket: &Bucket<K, V, L, TYPE>) -> Option<Reader<K, V, L, TYPE>> {
        if bucket.rw_lock.share_sync() {
            Some(Reader {
                bucket: unsafe { NonNull::new_unchecked(from_ref(bucket).cast_mut()) },
            })
        } else {
            None
        }
    }

    /// Tries to lock the [`Bucket`].
    #[inline]
    pub(crate) fn try_lock(bucket: &Bucket<K, V, L, TYPE>) -> Option<Reader<K, V, L, TYPE>> {
        if bucket.rw_lock.try_share() {
            Some(Reader {
                bucket: unsafe { NonNull::new_unchecked(from_ref(bucket).cast_mut()) },
            })
        } else {
            None
        }
    }
}

impl<K, V, L: LruList, const TYPE: char> Deref for Reader<K, V, L, TYPE> {
    type Target = Bucket<K, V, L, TYPE>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { self.bucket.as_ref() }
    }
}

impl<K, V, L: LruList, const TYPE: char> Drop for Reader<K, V, L, TYPE> {
    #[inline]
    fn drop(&mut self) {
        self.rw_lock.release_share();
    }
}

unsafe impl<K: Send, V: Send, L: LruList, const TYPE: char> Send for Reader<K, V, L, TYPE> {}
unsafe impl<K: Send + Sync, V: Send + Sync, L: LruList, const TYPE: char> Sync
    for Reader<K, V, L, TYPE>
{
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

    /// Returns `true` if the [`EntryPtr`] points to, or has pointed to, an occupied entry.
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
    /// The [`EntryPtr`] must point to a valid entry.
    #[inline]
    pub(crate) fn get(&self, data_block: NonNull<DataBlock<K, V, BUCKET_LEN>>) -> &'g (K, V) {
        debug_assert_ne!(self.current_index, usize::MAX);

        let entry_ptr = if let Some(link) = self.current_link_ptr.as_ref() {
            Bucket::<K, V, (), TYPE>::entry_ptr(&link.data_block, self.current_index)
        } else {
            Bucket::<K, V, (), TYPE>::entry_ptr(unsafe { data_block.as_ref() }, self.current_index)
        };
        unsafe { &(*entry_ptr) }
    }

    /// Gets a mutable reference to the entry.
    ///
    /// The associated [`Bucket`] must be locked, and the [`EntryPtr`] must point to a valid entry.
    #[inline]
    pub(crate) fn get_mut<L: LruList>(
        &mut self,
        data_block: NonNull<DataBlock<K, V, BUCKET_LEN>>,
        _writer: &Writer<K, V, L, TYPE>,
    ) -> &mut (K, V) {
        debug_assert_ne!(self.current_index, usize::MAX);

        let entry_ptr = if let Some(link) = self.current_link_ptr.as_ref() {
            Bucket::<K, V, L, TYPE>::entry_mut_ptr(&link.data_block, self.current_index)
        } else {
            Bucket::<K, V, L, TYPE>::entry_mut_ptr(
                unsafe { data_block.as_ref() },
                self.current_index,
            )
        };
        unsafe { &mut (*entry_ptr) }
    }

    /// Gets the partial hash value of the entry.
    ///
    /// The [`EntryPtr`] must point to a valid entry.
    #[inline]
    pub(crate) fn partial_hash<L: LruList>(&self, bucket: &Bucket<K, V, L, TYPE>) -> u8 {
        debug_assert_ne!(self.current_index, usize::MAX);

        if let Some(link) = self.current_link_ptr.as_ref() {
            *Bucket::<K, V, L, TYPE>::read_cell(
                &link.metadata.partial_hash_array[self.current_index],
            )
        } else {
            *Bucket::<K, V, L, TYPE>::read_cell(
                &bucket.metadata.partial_hash_array[self.current_index],
            )
        }
    }

    /// Unlinks the [`LinkedBucket`] currently pointed to by this [`EntryPtr`] from the linked list.
    ///
    /// The associated [`Bucket`] must be locked.
    fn unlink<L: LruList>(
        &mut self,
        bucket: &Bucket<K, V, L, TYPE>,
        link: &LinkedBucket<K, V>,
        guard: &'g Guard,
    ) {
        debug_assert_ne!(TYPE, INDEX);

        let prev_link_ptr = link.prev_link.load(Relaxed);
        let next = link.metadata.link.swap((None, Tag::None), Relaxed).0;
        if let Some(next) = next.as_ref() {
            next.prev_link.store(prev_link_ptr, Relaxed);
        }

        self.current_link_ptr = next
            .as_ref()
            .map_or_else(Ptr::null, |n| n.get_guarded_ptr(guard));
        let unlinked = if let Some(prev) = unsafe { prev_link_ptr.as_ref() } {
            prev.metadata.link.swap((next, Tag::None), Release).0
        } else {
            bucket.metadata.link.swap((next, Tag::None), Release).0
        };
        if let Some(link) = unlinked {
            let dropped = unsafe { link.drop_in_place() };
            debug_assert!(dropped);
        }

        if self.current_link_ptr.is_null() {
            // Fuse the `EntryPtr`.
            self.current_index = usize::MAX;
        } else {
            // Go to the next `Link`.
            self.current_index = LINKED_BUCKET_LEN;
        }
    }

    /// Moves this [`EntryPtr`] to the next occupied entry in the [`Bucket`].
    ///
    /// Returns `false` if this currently points to the last entry.
    #[inline]
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
            let bitmap = if TYPE == INDEX {
                // Load order: `removed_bitmap` -> `acquire` -> `occupied_bitmap`.
                (!metadata.removed_bitmap_or_lru_tail.load(Acquire)
                    & metadata.occupied_bitmap.load(Acquire))
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

impl LruList for () {}

impl DoublyLinkedList {
    /// Reads the slot.
    #[inline]
    fn read(&self, index: usize) -> (u8, u8) {
        unsafe { *self.0[index].get() }
    }

    /// Writes the slot.
    #[inline]
    fn write<R, F: FnOnce(&mut (u8, u8)) -> R>(&self, index: usize, f: F) -> R {
        unsafe { f(&mut *self.0[index].get()) }
    }
}

impl LruList for DoublyLinkedList {
    #[inline]
    fn evict(&self, tail: u32) -> Option<(u8, u32)> {
        if tail == 0 {
            None
        } else {
            let lru = self.read(tail as usize - 1).0;
            let new_tail = if tail - 1 == u32::from(lru) {
                // Reset the linked list.
                0
            } else {
                let new_lru = self.read(lru as usize).0;
                {
                    #![allow(clippy::cast_possible_truncation)]
                    self.write(new_lru as usize, |v| {
                        v.1 = tail as u8 - 1;
                    });
                }
                self.write(tail as usize - 1, |v| {
                    v.0 = new_lru;
                });
                tail
            };
            self.write(lru as usize, |v| {
                *v = (0, 0);
            });
            Some((lru, new_tail))
        }
    }

    #[inline]
    fn remove(&self, tail: u32, entry: u8) -> Option<u32> {
        if tail == 0
            || (self.read(entry as usize) == (0, 0)
                && (self.read(0) != (entry, entry) || (tail != 1 && tail != u32::from(entry) + 1)))
        {
            // The linked list is empty, or the entry is not a part of the linked list.
            return None;
        }

        if self.read(entry as usize).0 == entry {
            // It is the head and the only entry of the linked list.
            debug_assert_eq!(tail, u32::from(entry) + 1);
            self.write(entry as usize, |v| {
                *v = (0, 0);
            });
            return Some(0);
        }

        // Adjust `prev -> current`.
        let (prev, next) = self.read(entry as usize);
        debug_assert_eq!(self.read(prev as usize).1, entry);
        self.write(prev as usize, |v| {
            v.1 = next;
        });

        // Adjust `next -> current`.
        debug_assert_eq!(self.read(next as usize).0, entry);
        self.write(next as usize, |v| {
            v.0 = prev;
        });

        let new_tail = if tail == u32::from(entry) + 1 {
            // Update `head`.
            Some(u32::from(next) + 1)
        } else {
            None
        };
        self.write(entry as usize, |v| {
            *v = (0, 0);
        });

        new_tail
    }

    #[inline]
    fn promote(&self, tail: u32, entry: u8) -> Option<u32> {
        if tail == u32::from(entry) + 1 {
            // Nothing to do.
            return None;
        } else if tail == 0 {
            // The linked list is empty.
            self.write(entry as usize, |v| {
                *v = (entry, entry);
            });
            return Some(u32::from(entry) + 1);
        }

        // Remove the entry from the linked list only if it is a part of it.
        if self.read(entry as usize) != (0, 0) || (self.read(0) == (entry, entry) && tail == 1) {
            // Adjust `prev -> current`.
            let (prev, next) = self.read(entry as usize);
            debug_assert_eq!(self.read(prev as usize).1, entry);
            self.write(prev as usize, |v| {
                v.1 = next;
            });

            // Adjust `next -> current`.
            debug_assert_eq!(self.read(next as usize).0, entry);
            self.write(next as usize, |v| {
                v.0 = prev;
            });
        }

        // Adjust `oldest -> head`.
        let oldest = self.read(tail as usize - 1).0;
        debug_assert_eq!(u32::from(self.read(oldest as usize).1) + 1, tail);
        self.write(oldest as usize, |v| {
            v.1 = entry;
        });
        self.write(entry as usize, |v| {
            v.0 = oldest;
        });

        // Adjust `head -> new head`
        self.write(tail as usize - 1, |v| {
            v.0 = entry;
        });
        {
            #![allow(clippy::cast_possible_truncation)]
            self.write(entry as usize, |v| {
                v.1 = tail as u8 - 1;
            });
        }

        // Update `head`.
        Some(u32::from(entry) + 1)
    }
}

unsafe impl Send for DoublyLinkedList {}
unsafe impl Sync for DoublyLinkedList {}

unsafe impl<K: Send, V: Send, const LEN: usize> Send for Metadata<K, V, LEN> {}
unsafe impl<K: Send + Sync, V: Send + Sync, const LEN: usize> Sync for Metadata<K, V, LEN> {}

impl<K, V> LinkedBucket<K, V> {
    /// Creates an empty [`LinkedBucket`].
    #[inline]
    fn new(next: Option<Shared<LinkedBucket<K, V>>>) -> Self {
        let mut bucket = Self {
            metadata: Metadata {
                link: AtomicShared::default(),
                occupied_bitmap: AtomicU32::default(),
                removed_bitmap_or_lru_tail: AtomicU32::default(),
                partial_hash_array: Default::default(),
            },
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

impl<K, V> Debug for LinkedBucket<K, V> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LinkedBucket").finish()
    }
}

impl<K, V> Drop for LinkedBucket<K, V> {
    #[inline]
    fn drop(&mut self) {
        if needs_drop::<(K, V)>() {
            let mut occupied_bitmap = self.metadata.occupied_bitmap.load(Relaxed);
            let mut index = occupied_bitmap.trailing_zeros();
            while index != 32 {
                Bucket::<K, V, (), MAP>::drop_entry(&self.data_block, index as usize);
                occupied_bitmap -= 1_u32 << index;
                index = occupied_bitmap.trailing_zeros();
            }
        }
    }
}

#[cfg(not(feature = "loom"))]
#[cfg(test)]
mod test {
    use super::*;

    use std::mem::MaybeUninit;
    use std::sync::atomic::AtomicPtr;
    use std::sync::atomic::Ordering::Relaxed;

    use proptest::prelude::*;
    use sdd::{Guard, Shared};
    use tokio::sync::Barrier;

    #[cfg(not(miri))]
    static_assertions::assert_eq_size!(Bucket<String, String, (), MAP>, [u8; BUCKET_LEN * 2]);
    #[cfg(not(miri))]
    static_assertions::assert_eq_size!(Bucket<String, String, DoublyLinkedList, CACHE>, [u8; BUCKET_LEN * 4]);

    proptest! {
        #[cfg_attr(miri, ignore)]
        #[test]
        fn evict_untracked(xs in 0..BUCKET_LEN * 2) {
            let data_block: DataBlock<usize, usize, BUCKET_LEN> =
                unsafe { MaybeUninit::uninit().assume_init() };
            let data_block_ptr =
                unsafe { NonNull::new_unchecked(from_ref(&data_block).cast_mut()) };
            let bucket: Bucket<usize, usize, DoublyLinkedList, CACHE> = Bucket::new();
            for v in 0..xs {
                let guard = Guard::new();
                let writer = Writer::lock_sync(&bucket).unwrap();
                let evicted = writer.evict_lru_head(data_block_ptr);
                assert_eq!(v >= BUCKET_LEN, evicted.is_some());
                writer.insert(data_block_ptr, 0, (v, v), &guard);
                assert_eq!(writer.metadata.removed_bitmap_or_lru_tail.load(Relaxed), 0);
            }
        }

        #[cfg_attr(miri, ignore)]
        #[test]
        fn evict_overflowed(xs in 1..BUCKET_LEN * 2) {
            let data_block: DataBlock<usize, usize, BUCKET_LEN> =
                unsafe { MaybeUninit::uninit().assume_init() };
            let data_block_ptr =
                unsafe { NonNull::new_unchecked(from_ref(&data_block).cast_mut()) };
            let bucket: Bucket<usize, usize, DoublyLinkedList, CACHE> = Bucket::new();
            let guard = Guard::new();
            let writer = Writer::lock_sync(&bucket).unwrap();
            for _ in 0..3 {
                for v in 0..xs {
                    let entry_ptr = writer.insert(data_block_ptr, 0, (v, v), &guard);
                    writer.update_lru_tail(&entry_ptr);
                    if v < BUCKET_LEN {
                        assert_eq!(
                            writer.metadata.removed_bitmap_or_lru_tail.load(Relaxed) as usize,
                            v + 1
                        );
                    }
                    assert_eq!(
                        writer.lru_list.read
                            (writer.metadata.removed_bitmap_or_lru_tail.load(Relaxed) as usize - 1)
                            .0,
                        0
                    );
                }

                let mut evicted_key = None;
                if xs >= BUCKET_LEN {
                    let evicted = writer.evict_lru_head(data_block_ptr);
                    assert!(evicted.is_some());
                    evicted_key = evicted.map(|(k, _)| k);
                }
                assert_ne!(writer.metadata.removed_bitmap_or_lru_tail.load(Relaxed), 0);

                for v in 0..xs {
                    let mut entry_ptr = writer.get_entry_ptr(data_block_ptr, &v, 0, &guard);
                    if entry_ptr.is_valid() {
                        let _erased = writer.remove(data_block_ptr, &mut entry_ptr, &guard);
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
            let data_block_ptr =
                unsafe { NonNull::new_unchecked(from_ref(&data_block).cast_mut()) };
            let bucket: Bucket<usize, usize, DoublyLinkedList, CACHE> = Bucket::new();
            for v in 0..xs {
                let guard = Guard::new();
                let writer = Writer::lock_sync(&bucket).unwrap();
                let evicted = writer.evict_lru_head(data_block_ptr);
                assert_eq!(v >= BUCKET_LEN, evicted.is_some());
                let mut entry_ptr = writer.insert(data_block_ptr, 0, (v, v), &guard);
                writer.update_lru_tail(&entry_ptr);
                assert_eq!(
                    writer.metadata.removed_bitmap_or_lru_tail.load(Relaxed) as usize,
                    entry_ptr.current_index + 1
                );
                if v >= BUCKET_LEN {
                    entry_ptr.current_index = xs % BUCKET_LEN;
                    writer.update_lru_tail(&entry_ptr);
                    assert_eq!(
                        writer.metadata.removed_bitmap_or_lru_tail.load(Relaxed) as usize,
                        entry_ptr.current_index + 1
                    );
                    let mut iterated = 1;
                    let mut i = writer.lru_list.read(entry_ptr.current_index).1 as usize;
                    while i != entry_ptr.current_index {
                        iterated += 1;
                        i = writer.lru_list.read(i).1 as usize;
                    }
                    assert_eq!(iterated, BUCKET_LEN);
                    iterated = 1;
                    i = writer.lru_list.read(entry_ptr.current_index).0 as usize;
                    while i != entry_ptr.current_index {
                        iterated += 1;
                        i = writer.lru_list.read(i).0 as usize;
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
            let data_block_ptr =
                unsafe { NonNull::new_unchecked(from_ref(&data_block).cast_mut()) };
            let bucket: Bucket<usize, usize, DoublyLinkedList, CACHE> = Bucket::new();
            for v in 0..xs {
                let guard = Guard::new();
                let writer = Writer::lock_sync(&bucket).unwrap();
                let entry_ptr = writer.insert(data_block_ptr, 0, (v, v), &guard);
                writer.update_lru_tail(&entry_ptr);
                let mut iterated = 1;
                let mut i = writer.lru_list.read(entry_ptr.current_index).1 as usize;
                while i != entry_ptr.current_index {
                    iterated += 1;
                    i = writer.lru_list.read(i).1 as usize;
                }
                assert_eq!(iterated, v + 1);
            }
            for v in 0..xs {
                let guard = Guard::new();
                let writer = Writer::lock_sync(&bucket).unwrap();
                let data_block_ptr =
                    unsafe { NonNull::new_unchecked(from_ref(&data_block).cast_mut()) };
                let entry_ptr = writer.get_entry_ptr(data_block_ptr, &v, 0, &guard);
                let mut iterated = 1;
                let mut i = writer.lru_list.read(entry_ptr.current_index).1 as usize;
                while i != entry_ptr.current_index {
                    iterated += 1;
                    i = writer.lru_list.read(i).1 as usize;
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
        let mut bucket: Shared<Bucket<usize, usize, (), MAP>> = Shared::new(Bucket::new());
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
                    let writer = Writer::lock_sync(&bucket_clone).unwrap();
                    let mut sum: u64 = 0;
                    for j in 0..128 {
                        unsafe {
                            sum += (*data_ptr.load(Relaxed))[j];
                            (*data_ptr.load(Relaxed))[j] = if i % 4 == 0 { 2 } else { 4 }
                        };
                    }
                    assert_eq!(sum % 256, 0);
                    let data_block_ptr =
                        unsafe { NonNull::new_unchecked(data_block_clone.as_ptr().cast_mut()) };
                    if i == 0 {
                        writer.insert(data_block_ptr, partial_hash, (task_id, 0), &guard);
                    } else {
                        assert_eq!(
                            writer
                                .search_entry(data_block_ptr, &task_id, partial_hash, &guard)
                                .unwrap(),
                            &(task_id, 0_usize)
                        );
                    }
                    drop(writer);

                    let reader = Reader::lock_sync(&*bucket_clone).unwrap();
                    assert_eq!(
                        reader
                            .search_entry(data_block_ptr, &task_id, partial_hash, &guard)
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
        let data_block_ptr = unsafe { NonNull::new_unchecked(data_block.as_ptr().cast_mut()) };
        for task_id in 0..num_tasks {
            assert_eq!(
                bucket.search_entry(
                    data_block_ptr,
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
        let writer = Writer::lock_sync(&bucket).unwrap();
        while entry_ptr.move_to_next(&writer, &epoch_guard) {
            writer.remove(
                unsafe { NonNull::new_unchecked(data_block.as_ptr().cast_mut()) },
                &mut entry_ptr,
                &epoch_guard,
            );
        }
        assert_eq!(writer.len(), 0);
        writer.kill();

        assert_eq!(bucket.len(), 0);
        assert!(Writer::lock_sync(unsafe { bucket.get_mut().unwrap() }).is_none());
    }
}
