use crate::ebr::{AtomicShared, Guard, Ptr, Shared, Tag};
use crate::wait_queue::{AsyncWait, WaitQueue};
use std::borrow::Borrow;
use std::fmt::{self, Debug};
use std::mem::{needs_drop, MaybeUninit};
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{fence, AtomicU32};

/// [`Bucket`] is a fixed-size hash table with linear probing.
///
/// `TYPE` is either one of [`SEQUENTIAL`], [`OPTIMISTIC`], or [`CACHE`].
#[repr(align(64))]
pub struct Bucket<K: Eq, V, L: LruList, const TYPE: char> {
    /// The state of the [`Bucket`].
    state: AtomicU32,

    /// The number of occupied entries in the [`Bucket`].
    num_entries: u32,

    /// The metadata of the [`Bucket`].
    metadata: Metadata<K, V, BUCKET_LEN>,

    /// The wait queue of the [`Bucket`].
    wait_queue: WaitQueue,

    /// The LRU list of the [`Bucket`].
    lru_list: L,
}

/// Least-recently-used list interface.
pub trait LruList: 'static + Copy + Default {
    /// Evicts an entry.
    fn evict(&mut self, _tail: u32) -> Option<(u8, u32)> {
        None
    }

    /// Removes an entry.
    fn remove(&mut self, _tail: u32, _entry: u8) -> Option<u32> {
        None
    }

    /// Promotes the entry.
    fn promote(&mut self, _tail: u32, _entry: u8) -> Option<u32> {
        None
    }
}

/// [`DoublyLinkedList`] is an array of `(u8, u8)`.
pub type DoublyLinkedList = [(u8, u8); BUCKET_LEN];

/// The type of [`Bucket`] only allows sequential access to it.
pub const SEQUENTIAL: char = 'S';

/// The type of [`Bucket`] allows lock-free read.
pub const OPTIMISTIC: char = 'O';

/// The type of [`Bucket`] acts as an LRU cache.
pub const CACHE: char = 'C';

/// The size of a [`Bucket`].
pub const BUCKET_LEN: usize = u32::BITS as usize;

/// [`DataBlock`] is a type alias of a raw memory chunk that may contain entry instances.
pub type DataBlock<K, V, const LEN: usize> = [MaybeUninit<(K, V)>; LEN];

/// [`Locker`] owns a [`Bucket`] by holding the exclusive lock on it.
pub struct Locker<'g, K: Eq, V, L: LruList, const TYPE: char> {
    bucket: &'g mut Bucket<K, V, L, TYPE>,
}

/// [`Locker`] owns a [`Bucket`] by holding a shared lock on it.
pub struct Reader<'g, K: Eq, V, L: LruList, const TYPE: char> {
    bucket: &'g Bucket<K, V, L, TYPE>,
}

/// [`EntryPtr`] points to an occupied slot in a [`Bucket`].
pub struct EntryPtr<'g, K: Eq, V, const TYPE: char> {
    /// Points to the current [`LinkedBucket`].
    current_link_ptr: Ptr<'g, LinkedBucket<K, V, LINKED_BUCKET_LEN>>,

    /// Points to the current slot.
    current_index: usize,
}

/// [`Metadata`] is a collection of metadata fields of [`Bucket`] and [`LinkedBucket`].
pub(crate) struct Metadata<K: Eq, V, const LEN: usize> {
    /// Linked list of entries.
    link: AtomicShared<LinkedBucket<K, V, LINKED_BUCKET_LEN>>,

    /// Bitmap for occupied slots.
    occupied_bitmap: u32,

    /// Bitmap for removed slots or recently-used entry linked list head where the head points to
    /// the most recently used entry slot if `TYPE = CACHE`.
    ///
    /// If the field is used as a linked list of entries, the value represents `1-based` index of
    /// the entry where `0` represents `nil`.
    removed_bitmap_or_lru_tail: u32,

    /// Partial hash array.
    partial_hash_array: [u8; LEN],
}

/// [`LinkedBucket`] is a smaller [`Bucket`] that is attached to a [`Bucket`] as a linked list.
pub(crate) struct LinkedBucket<K: Eq, V, const LEN: usize> {
    metadata: Metadata<K, V, LEN>,
    data_block: DataBlock<K, V, LEN>,
    prev_link: AtomicPtr<LinkedBucket<K, V, LEN>>,
}

/// The size of the linked data block.
const LINKED_BUCKET_LEN: usize = BUCKET_LEN / 4;

/// State bits.
const KILLED: u32 = 1_u32 << 31;
const WAITING: u32 = 1_u32 << 30;
const LOCK: u32 = 1_u32 << 29;
const SLOCK_MAX: u32 = LOCK - 1;
const LOCK_MASK: u32 = LOCK | SLOCK_MAX;

impl<K: Eq, V, L: LruList, const TYPE: char> Bucket<K, V, L, TYPE> {
    /// Returns the number of occupied and reachable slots in the [`Bucket`].
    #[inline]
    pub(crate) const fn num_entries(&self) -> usize {
        self.num_entries as usize
    }

    /// Returns `true` if the [`Bucket`] needs to be rebuilt.
    ///
    /// If `TYPE == OPTIMISTIC`, removed entries are not dropped, still occupying the slots,
    /// therefore rebuilding the [`Bucket`] might be needed to keep the [`Bucket`] as small as
    /// possible.
    #[inline]
    pub(crate) const fn need_rebuild(&self) -> bool {
        TYPE == OPTIMISTIC
            && self.metadata.removed_bitmap_or_lru_tail == (u32::MAX >> (32 - BUCKET_LEN))
    }

    /// Returns `true` if the [`Bucket`] has been killed.
    #[inline]
    pub(crate) fn killed(&self) -> bool {
        (self.state.load(Relaxed) & KILLED) == KILLED
    }

    /// Searches for an entry associated with the given key.
    #[inline]
    pub(crate) fn search<'g, Q>(
        &'g self,
        data_block: &'g DataBlock<K, V, BUCKET_LEN>,
        key: &Q,
        partial_hash: u8,
        guard: &'g Guard,
    ) -> Option<&'g (K, V)>
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

        let mut link_ptr = self.metadata.link.load(Acquire, guard);
        while let Some(link) = link_ptr.as_ref() {
            if let Some((_, entry_ref)) =
                Self::search_entry(&link.metadata, &link.data_block, key, partial_hash)
            {
                return Some(entry_ref);
            }
            link_ptr = link.metadata.link.load(Acquire, guard);
        }

        None
    }

    /// Kills the bucket by marking it `KILLED` and unlinking [`LinkedBucket`].
    #[inline]
    pub(crate) fn kill(&mut self, guard: &Guard) {
        if TYPE == OPTIMISTIC {
            self.metadata.removed_bitmap_or_lru_tail = self.metadata.occupied_bitmap;
        }
        self.state.fetch_or(KILLED, Release);
        self.num_entries = 0;
        if !self.metadata.link.is_null(Relaxed) {
            self.clear_links(guard);
        }
    }

    /// Drops entries in the given [`DataBlock`] using the information stored in the [`Bucket`].
    ///
    /// The [`Bucket`] and the [`DataBlock`] should never be used afterwards.
    #[inline]
    pub(crate) unsafe fn drop_entries(
        &mut self,
        data_block: &mut DataBlock<K, V, BUCKET_LEN>,
        guard: &Guard,
    ) {
        if !self.metadata.link.is_null(Relaxed) {
            self.clear_links(guard);
        }
        if needs_drop::<(K, V)>() && self.metadata.occupied_bitmap != 0 {
            let mut index = self.metadata.occupied_bitmap.trailing_zeros();
            while index != 32 {
                ptr::drop_in_place(data_block[index as usize].as_mut_ptr());
                self.metadata.occupied_bitmap -= 1_u32 << index;
                index = self.metadata.occupied_bitmap.trailing_zeros();
            }
        }
    }

    /// Gets an [`EntryPtr`] pointing to the slot containing the given key.
    #[inline]
    fn get<'g, Q>(
        &self,
        data_block: &DataBlock<K, V, BUCKET_LEN>,
        key: &Q,
        partial_hash: u8,
        guard: &'g Guard,
    ) -> EntryPtr<'g, K, V, TYPE>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        if self.num_entries == 0 {
            return EntryPtr::new(guard);
        }

        if let Some((index, _)) = Self::search_entry(&self.metadata, data_block, key, partial_hash)
        {
            return EntryPtr {
                current_link_ptr: Ptr::null(),
                current_index: index,
            };
        }

        let mut current_link_ptr = self.metadata.link.load(Acquire, guard);
        while let Some(link) = current_link_ptr.as_ref() {
            if let Some((index, _)) =
                Self::search_entry(&link.metadata, &link.data_block, key, partial_hash)
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

    /// Searches the given data block for an entry matching the key.
    fn search_entry<'g, Q, const LEN: usize>(
        metadata: &'g Metadata<K, V, LEN>,
        data_block: &'g DataBlock<K, V, LEN>,
        key: &Q,
        partial_hash: u8,
    ) -> Option<(usize, &'g (K, V))>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        let mut bitmap = if TYPE == OPTIMISTIC {
            metadata.occupied_bitmap & (!metadata.removed_bitmap_or_lru_tail)
        } else {
            metadata.occupied_bitmap
        };
        if TYPE == OPTIMISTIC {
            fence(Acquire);
        }

        // Expect that the loop is vectorized by the compiler.
        let mut matching: u32 = 0;
        for i in 0..LEN {
            if metadata.partial_hash_array[i] == partial_hash {
                matching |= 1_u32 << i;
            }
        }
        bitmap &= matching;

        let mut offset = bitmap.trailing_zeros();
        while offset != u32::BITS {
            let entry_ref = unsafe { &(*data_block[offset as usize].as_ptr()) };
            if entry_ref.0.borrow() == key {
                return Some((offset as usize, entry_ref));
            }
            bitmap -= 1_u32 << offset;
            offset = bitmap.trailing_zeros();
        }

        None
    }

    /// Clears all the linked arrays iteratively.
    fn clear_links(&mut self, guard: &Guard) {
        if let (Some(mut next), _) = self.metadata.link.swap((None, Tag::None), Acquire) {
            loop {
                let next_next = next.metadata.link.swap((None, Tag::None), Acquire);
                let released = if TYPE == OPTIMISTIC {
                    next.release(guard)
                } else {
                    // The `LinkedBucket` should be dropped immediately.
                    unsafe { next.drop_in_place() }
                };
                debug_assert!(released);
                if let (Some(next_next), _) = next_next {
                    next = next_next;
                } else {
                    break;
                }
            }
        }
    }

    /// Searches for the next closest occupied entry slot number from the current one in the bitmap.
    ///
    /// If the specified slot is occupied and reachable, just returns its index number.
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

        let bitmap = if TYPE == OPTIMISTIC {
            (metadata.occupied_bitmap & (!metadata.removed_bitmap_or_lru_tail))
                & (!((1_u32 << current_index) - 1))
        } else {
            metadata.occupied_bitmap & (!((1_u32 << current_index) - 1))
        };

        let next_index = bitmap.trailing_zeros() as usize;
        if next_index < LEN {
            if TYPE == OPTIMISTIC {
                fence(Acquire);
            }
            return Some(next_index);
        }

        None
    }
}

impl<'g, K: Eq, V, const TYPE: char> EntryPtr<'g, K, V, TYPE> {
    /// Creates a new invalid [`EntryPtr`].
    #[inline]
    pub(crate) const fn new(_guard: &'g Guard) -> Self {
        Self {
            current_link_ptr: Ptr::null(),
            current_index: BUCKET_LEN,
        }
    }

    /// Returns `true` if the [`EntryPtr`] points to an occupied entry or fused.
    #[inline]
    pub(crate) const fn is_valid(&self) -> bool {
        self.current_index != BUCKET_LEN
    }

    /// Moves the [`EntryPtr`] to point to the next occupied entry.
    ///
    /// Returns `true` if it successfully found the next occupied entry.
    #[inline]
    pub(crate) fn next<L: LruList>(
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
            link.data_block[self.current_index].as_ptr()
        } else {
            data_block[self.current_index].as_ptr()
        };
        unsafe { &(*entry_ptr) }
    }

    /// Gets a mutable reference to the entry.
    ///
    /// The [`EntryPtr`] must point to an occupied entry, and the associated [`Bucket`] must be
    /// locked.
    #[inline]
    pub(crate) fn get_mut<L: LruList>(
        &mut self,
        data_block: &mut DataBlock<K, V, BUCKET_LEN>,
        _locker: &mut Locker<K, V, L, TYPE>,
    ) -> &mut (K, V) {
        debug_assert_ne!(self.current_index, usize::MAX);
        let link_ptr = self.current_link_ptr.as_ptr().cast_mut();
        let entry_ptr = if let Some(link_mut) = unsafe { link_ptr.as_mut() } {
            link_mut.data_block[self.current_index].as_mut_ptr()
        } else {
            data_block[self.current_index].as_mut_ptr()
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
            link.metadata.partial_hash_array[self.current_index]
        } else {
            bucket.metadata.partial_hash_array[self.current_index]
        }
    }

    /// Tries to remove the [`LinkedBucket`] from the linked list.
    ///
    /// It should only be invoked when the caller is holding a [`Locker`] on the [`Bucket`].
    fn unlink<L: LruList>(
        &mut self,
        locker: &Locker<K, V, L, TYPE>,
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
            locker
                .bucket
                .metadata
                .link
                .swap((next_link, Tag::None), Relaxed)
                .0
        };
        let released = old_link.map_or(true, |l| {
            if TYPE == OPTIMISTIC {
                l.release(guard)
            } else {
                // The `LinkedBucket` should be dropped immediately.
                unsafe { l.drop_in_place() }
            }
        });
        debug_assert!(released);

        if self.current_link_ptr.is_null() {
            // Fuse the pointer.
            self.current_index = usize::MAX;
        } else {
            // Go to the next `Link`.
            self.current_index = LINKED_BUCKET_LEN;
        }
    }

    /// Moves the [`EntryPtr`] to the next occupied entry in the [`Bucket`].
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
        if let Some(index) = Bucket::<K, V, L, TYPE>::next_entry(metadata, current_index) {
            self.current_index = index;
            return true;
        }

        self.current_link_ptr = metadata.link.load(Acquire, guard);
        self.current_index = LINKED_BUCKET_LEN;

        false
    }
}

impl<'g, K: Eq, V, const TYPE: char> Debug for EntryPtr<'g, K, V, TYPE> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntryPtr")
            .field("current_link_ptr", &self.current_link_ptr)
            .field("current_index", &self.current_index)
            .finish()
    }
}

unsafe impl<'g, K: Eq + Sync, V: Sync, const TYPE: char> Sync for EntryPtr<'g, K, V, TYPE> {}

impl<'g, K: Eq, V, L: LruList, const TYPE: char> Locker<'g, K, V, L, TYPE> {
    /// Locks the [`Bucket`].
    #[inline]
    pub(crate) fn lock(
        bucket: &'g mut Bucket<K, V, L, TYPE>,
        guard: &'g Guard,
    ) -> Option<Locker<'g, K, V, L, TYPE>> {
        let bucket_ptr = bucket as *mut Bucket<K, V, L, TYPE>;
        loop {
            if let Ok(locker) = Self::try_lock(unsafe { &mut *bucket_ptr }, guard) {
                return locker;
            }
            if let Ok(locker) = unsafe { &*bucket_ptr }.wait_queue.wait_sync(|| {
                // Mark that there is a waiting thread.
                bucket.state.fetch_or(WAITING, Release);
                Self::try_lock(unsafe { &mut *bucket_ptr }, guard)
            }) {
                return locker;
            }
        }
    }

    /// Tries to lock the [`Bucket`].
    #[inline]
    pub(crate) fn try_lock(
        bucket: &'g mut Bucket<K, V, L, TYPE>,
        _guard: &'g Guard,
    ) -> Result<Option<Locker<'g, K, V, L, TYPE>>, ()> {
        let current = bucket.state.load(Relaxed) & (!LOCK_MASK);
        if (current & KILLED) == KILLED {
            return Ok(None);
        }
        if bucket
            .state
            .compare_exchange(current, current | LOCK, Acquire, Relaxed)
            .is_ok()
        {
            Ok(Some(Locker { bucket }))
        } else {
            Err(())
        }
    }

    /// Tries to lock the [`Bucket`], and if it fails, pushes an [`AsyncWait`].
    #[inline]
    pub(crate) fn try_lock_or_wait(
        bucket: &'g mut Bucket<K, V, L, TYPE>,
        async_wait: &mut AsyncWait,
        guard: &'g Guard,
    ) -> Result<Option<Locker<'g, K, V, L, TYPE>>, ()> {
        let bucket_ptr = bucket as *mut Bucket<K, V, L, TYPE>;
        if let Ok(locker) = Self::try_lock(unsafe { &mut *bucket_ptr }, guard) {
            return Ok(locker);
        }
        unsafe { &*bucket_ptr }
            .wait_queue
            .push_async_entry(async_wait, || {
                // Mark that there is a waiting thread.
                bucket.state.fetch_or(WAITING, Release);
                Self::try_lock(bucket, guard)
            })
    }

    /// Gets an [`EntryPtr`] pointing to an entry associated with the given key.
    ///
    /// The returned [`EntryPtr`] points to an occupied entry if the key is found.
    #[inline]
    pub(crate) fn get<Q>(
        &self,
        data_block: &DataBlock<K, V, BUCKET_LEN>,
        key: &Q,
        partial_hash: u8,
        guard: &'g Guard,
    ) -> EntryPtr<'g, K, V, TYPE>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        self.bucket.get(data_block, key, partial_hash, guard)
    }

    /// Reserves memory for insertion, and then constructs the key-value pair.
    #[inline]
    pub(crate) fn insert_with<C: FnOnce() -> (K, V)>(
        &mut self,
        data_block: &mut DataBlock<K, V, BUCKET_LEN>,
        partial_hash: u8,
        constructor: C,
        guard: &'g Guard,
    ) -> EntryPtr<'g, K, V, TYPE> {
        assert!(self.bucket.num_entries != u32::MAX, "array overflow");

        let free_index = self.bucket.metadata.occupied_bitmap.trailing_ones() as usize;
        if free_index == BUCKET_LEN {
            let mut link_ptr = self.bucket.metadata.link.load(Acquire, guard);
            while let Some(link_mut) = unsafe { link_ptr.as_ptr().cast_mut().as_mut() } {
                let free_index = link_mut.metadata.occupied_bitmap.trailing_ones() as usize;
                if free_index != LINKED_BUCKET_LEN {
                    Self::insert_entry_with(
                        &mut link_mut.metadata,
                        &mut link_mut.data_block,
                        free_index,
                        partial_hash,
                        constructor,
                    );
                    self.bucket.num_entries += 1;
                    return EntryPtr {
                        current_link_ptr: link_ptr,
                        current_index: free_index,
                    };
                }
                link_ptr = link_mut.metadata.link.load(Acquire, guard);
            }

            // Insert a new `LinkedBucket` at the linked list head.
            let head = self.bucket.metadata.link.get_shared(Relaxed, guard);
            let link = unsafe { Shared::new_unchecked(LinkedBucket::new(head)) };
            let link_ptr = link.get_guarded_ptr(guard);
            unsafe {
                let link_mut = &mut *link_ptr.as_ptr().cast_mut();
                link_mut.data_block[0].as_mut_ptr().write(constructor());
                link_mut.metadata.partial_hash_array[0] = partial_hash;
                link_mut.metadata.occupied_bitmap = 1;
            }
            if let Some(head) = link.metadata.link.load(Relaxed, guard).as_ref() {
                head.prev_link.store(link.as_ptr().cast_mut(), Relaxed);
            }
            self.bucket
                .metadata
                .link
                .swap((Some(link), Tag::None), Release);
            self.bucket.num_entries += 1;
            EntryPtr {
                current_link_ptr: link_ptr,
                current_index: 0,
            }
        } else {
            Self::insert_entry_with(
                &mut self.bucket.metadata,
                data_block,
                free_index,
                partial_hash,
                constructor,
            );
            self.bucket.num_entries += 1;
            EntryPtr {
                current_link_ptr: Ptr::null(),
                current_index: free_index,
            }
        }
    }

    /// Removes the key-value pair being pointed by the given [`EntryPtr`].
    #[inline]
    pub(crate) fn erase(
        &mut self,
        data_block: &mut DataBlock<K, V, BUCKET_LEN>,
        entry_ptr: &EntryPtr<K, V, TYPE>,
    ) -> Option<(K, V)> {
        debug_assert_ne!(entry_ptr.current_index, usize::MAX);
        debug_assert_ne!(entry_ptr.current_index, BUCKET_LEN);

        self.bucket.num_entries -= 1;
        let link_ptr = entry_ptr.current_link_ptr.as_ptr().cast_mut();
        if let Some(link_mut) = unsafe { link_ptr.as_mut() } {
            if TYPE == OPTIMISTIC {
                debug_assert_eq!(
                    link_mut.metadata.removed_bitmap_or_lru_tail
                        & (1_u32 << entry_ptr.current_index),
                    0
                );
                link_mut.metadata.removed_bitmap_or_lru_tail |= 1_u32 << entry_ptr.current_index;
            } else {
                debug_assert_ne!(
                    link_mut.metadata.occupied_bitmap & (1_u32 << entry_ptr.current_index),
                    0
                );
                link_mut.metadata.occupied_bitmap &= !(1_u32 << entry_ptr.current_index);
                return Some(unsafe {
                    link_mut.data_block[entry_ptr.current_index]
                        .as_mut_ptr()
                        .read()
                });
            }
        } else if TYPE == OPTIMISTIC {
            debug_assert_eq!(
                self.bucket.metadata.removed_bitmap_or_lru_tail
                    & (1_u32 << entry_ptr.current_index),
                0
            );
            self.bucket.metadata.removed_bitmap_or_lru_tail |= 1_u32 << entry_ptr.current_index;
        } else {
            debug_assert_ne!(
                self.bucket.metadata.occupied_bitmap & (1_u32 << entry_ptr.current_index),
                0
            );

            if TYPE == CACHE {
                self.remove_from_lru_list(entry_ptr);
            }

            self.bucket.metadata.occupied_bitmap &= !(1_u32 << entry_ptr.current_index);
            return Some(unsafe { data_block[entry_ptr.current_index].as_mut_ptr().read() });
        }

        None
    }

    /// Keeps or consumes the key-value pair being pointed by the given [`EntryPtr`].
    ///
    /// Returns `true` if the entry was consumed.
    #[inline]
    pub(crate) fn keep_or_consume<F: FnMut(&K, V) -> Option<V>>(
        &mut self,
        data_block: &mut DataBlock<K, V, BUCKET_LEN>,
        entry_ptr: &EntryPtr<K, V, TYPE>,
        pred: &mut F,
    ) -> bool {
        debug_assert_ne!(TYPE, OPTIMISTIC);
        debug_assert_ne!(entry_ptr.current_index, usize::MAX);

        // `pred` may panic, therefore it is safe to assume that the entry will be consumed.
        self.bucket.num_entries -= 1;

        let link_ptr = entry_ptr.current_link_ptr.as_ptr().cast_mut();
        if let Some(link_mut) = unsafe { link_ptr.as_mut() } {
            debug_assert_ne!(
                link_mut.metadata.occupied_bitmap & (1_u32 << entry_ptr.current_index),
                0
            );
            let (k, v) = unsafe {
                link_mut.data_block[entry_ptr.current_index]
                    .as_mut_ptr()
                    .read()
            };
            link_mut.metadata.occupied_bitmap &= !(1_u32 << entry_ptr.current_index);
            if let Some(v) = pred(&k, v) {
                // The instances returned: revive the entry.
                unsafe {
                    link_mut.data_block[entry_ptr.current_index]
                        .as_mut_ptr()
                        .write((k, v));
                }
                link_mut.metadata.occupied_bitmap |= 1_u32 << entry_ptr.current_index;
                self.bucket.num_entries += 1;
                return false;
            }
        } else {
            debug_assert_ne!(
                self.bucket.metadata.occupied_bitmap & (1_u32 << entry_ptr.current_index),
                0
            );
            self.bucket.metadata.occupied_bitmap &= !(1_u32 << entry_ptr.current_index);
            let (k, v) = unsafe { data_block[entry_ptr.current_index].as_mut_ptr().read() };
            if let Some(v) = pred(&k, v) {
                unsafe {
                    data_block[entry_ptr.current_index]
                        .as_mut_ptr()
                        .write((k, v));
                }
                self.bucket.metadata.occupied_bitmap |= 1_u32 << entry_ptr.current_index;
                self.bucket.num_entries += 1;
                return false;
            }
        }
        true
    }

    /// Extracts the key-value pair being pointed by the [`EntryPtr`].
    #[inline]
    pub(crate) fn extract<'e>(
        &mut self,
        data_block: &mut DataBlock<K, V, BUCKET_LEN>,
        entry_ptr: &mut EntryPtr<'e, K, V, TYPE>,
        guard: &'e Guard,
    ) -> (K, V) {
        debug_assert_ne!(TYPE, OPTIMISTIC);
        let link_ptr = entry_ptr.current_link_ptr.as_ptr().cast_mut();
        if let Some(link_mut) = unsafe { link_ptr.as_mut() } {
            let extracted = Self::extract_entry(
                &mut link_mut.metadata,
                &mut link_mut.data_block,
                entry_ptr.current_index,
                &mut self.bucket.num_entries,
            );
            if link_mut.metadata.occupied_bitmap == 0 {
                entry_ptr.unlink(self, link_mut, guard);
            }
            extracted
        } else {
            Self::extract_entry(
                &mut self.bucket.metadata,
                data_block,
                entry_ptr.current_index,
                &mut self.bucket.num_entries,
            )
        }
    }

    /// Evicts the least recently used entry if the [`Bucket`] is full.
    pub(crate) fn evict_lru_head(
        &mut self,
        data_block: &mut DataBlock<K, V, BUCKET_LEN>,
    ) -> Option<(K, V)> {
        debug_assert_eq!(TYPE, CACHE);

        if self.metadata.occupied_bitmap == 0b1111_1111_1111_1111_1111_1111_1111_1111 {
            self.num_entries -= 1;
            let tail = self.metadata.removed_bitmap_or_lru_tail;
            let evicted = if let Some((evicted, new_tail)) = self.lru_list.evict(tail) {
                self.metadata.removed_bitmap_or_lru_tail = new_tail;
                evicted as usize
            } else {
                // Evict the first occupied entry.
                0
            };
            debug_assert_ne!(self.metadata.occupied_bitmap & (1_u32 << evicted), 0);
            self.metadata.occupied_bitmap &= !(1_u32 << evicted);
            return Some(unsafe { data_block[evicted].as_mut_ptr().read() });
        }
        None
    }

    /// Sets the entry having been just accessed.
    pub(crate) fn update_lru_tail(&mut self, entry_ptr: &EntryPtr<K, V, TYPE>) {
        debug_assert_eq!(TYPE, CACHE);
        debug_assert_ne!(entry_ptr.current_index, usize::MAX);
        debug_assert_ne!(entry_ptr.current_index, BUCKET_LEN);

        if entry_ptr.current_link_ptr.is_null() {
            #[allow(clippy::cast_possible_truncation)]
            let entry = entry_ptr.current_index as u8;
            let tail = self.metadata.removed_bitmap_or_lru_tail;
            if let Some(new_tail) = self.lru_list.promote(tail, entry) {
                self.metadata.removed_bitmap_or_lru_tail = new_tail;
            }
        }
    }

    /// Removes the entry from the LRU linked list.
    fn remove_from_lru_list(&mut self, entry_ptr: &EntryPtr<K, V, TYPE>) {
        debug_assert_eq!(TYPE, CACHE);
        debug_assert_ne!(entry_ptr.current_index, usize::MAX);
        debug_assert_ne!(entry_ptr.current_index, BUCKET_LEN);

        if entry_ptr.current_link_ptr.is_null() {
            #[allow(clippy::cast_possible_truncation)]
            let entry = entry_ptr.current_index as u8;
            let tail = self.metadata.removed_bitmap_or_lru_tail;
            if let Some(new_tail) = self.lru_list.remove(tail, entry) {
                self.metadata.removed_bitmap_or_lru_tail = new_tail;
            }
        }
    }

    /// Inserts a key-value pair in the slot.
    fn insert_entry_with<C: FnOnce() -> (K, V), const LEN: usize>(
        metadata: &mut Metadata<K, V, LEN>,
        data_block: &mut DataBlock<K, V, LEN>,
        index: usize,
        partial_hash: u8,
        constructor: C,
    ) {
        debug_assert!(index < LEN);

        unsafe {
            data_block[index].as_mut_ptr().write(constructor());
            metadata.partial_hash_array[index] = partial_hash;
            if TYPE == OPTIMISTIC {
                fence(Release);
            }
            metadata.occupied_bitmap |= 1_u32 << index;
        }
    }

    /// Extracts and removes the key-value pair in the slot.
    fn extract_entry<const LEN: usize>(
        metadata: &mut Metadata<K, V, LEN>,
        data_block: &mut DataBlock<K, V, LEN>,
        index: usize,
        num_entries_field: &mut u32,
    ) -> (K, V) {
        debug_assert!(index < LEN);

        *num_entries_field -= 1;
        metadata.occupied_bitmap &= !(1_u32 << index);
        unsafe { data_block[index].as_mut_ptr().read() }
    }
}

impl<'g, K: Eq, V, L: LruList, const TYPE: char> Deref for Locker<'g, K, V, L, TYPE> {
    type Target = Bucket<K, V, L, TYPE>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.bucket
    }
}

impl<'g, K: Eq, V, L: LruList, const TYPE: char> DerefMut for Locker<'g, K, V, L, TYPE> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.bucket
    }
}

impl<'g, K: Eq, V, L: LruList, const TYPE: char> Drop for Locker<'g, K, V, L, TYPE> {
    #[inline]
    fn drop(&mut self) {
        let mut current = self.bucket.state.load(Relaxed);
        while let Err(result) = self.bucket.state.compare_exchange_weak(
            current,
            current & (!(WAITING | LOCK)),
            Release,
            Relaxed,
        ) {
            current = result;
        }

        if (current & WAITING) == WAITING {
            self.bucket.wait_queue.signal();
        }
    }
}

impl<'g, K: Eq, V, L: LruList, const TYPE: char> Reader<'g, K, V, L, TYPE> {
    /// Locks the given [`Bucket`].
    ///
    /// Returns `None` if the [`Bucket`] has been killed or empty.
    #[inline]
    pub(crate) fn lock(
        bucket: &'g Bucket<K, V, L, TYPE>,
        guard: &'g Guard,
    ) -> Option<Reader<'g, K, V, L, TYPE>> {
        loop {
            if let Ok(reader) = Self::try_lock(bucket, guard) {
                return reader;
            }
            if let Ok(reader) = bucket.wait_queue.wait_sync(|| {
                // Mark that there is a waiting thread.
                bucket.state.fetch_or(WAITING, Release);
                Self::try_lock(bucket, guard)
            }) {
                return reader;
            }
        }
    }

    /// Tries to lock the [`Bucket`], and if it fails, pushes an [`AsyncWait`].
    #[inline]
    pub(crate) fn try_lock_or_wait(
        bucket: &'g Bucket<K, V, L, TYPE>,
        async_wait: &mut AsyncWait,
        guard: &'g Guard,
    ) -> Result<Option<Reader<'g, K, V, L, TYPE>>, ()> {
        if let Ok(reader) = Self::try_lock(bucket, guard) {
            return Ok(reader);
        }
        bucket.wait_queue.push_async_entry(async_wait, || {
            // Mark that there is a waiting thread.
            bucket.state.fetch_or(WAITING, Release);
            Self::try_lock(bucket, guard)
        })
    }

    /// Tries to lock the [`Bucket`].
    pub(crate) fn try_lock(
        bucket: &'g Bucket<K, V, L, TYPE>,
        _guard: &'g Guard,
    ) -> Result<Option<Reader<'g, K, V, L, TYPE>>, ()> {
        let current = bucket.state.load(Relaxed);
        if (current & LOCK_MASK) >= SLOCK_MAX {
            return Err(());
        }
        if (current & KILLED) == KILLED {
            return Ok(None);
        }
        if bucket
            .state
            .compare_exchange(current, current + 1, Acquire, Relaxed)
            .is_ok()
        {
            Ok(Some(Reader { bucket }))
        } else {
            Err(())
        }
    }

    /// Releases the lock.
    #[inline]
    pub(crate) fn release(bucket: &Bucket<K, V, L, TYPE>) {
        let mut current = bucket.state.load(Relaxed);
        loop {
            let wakeup = (current & WAITING) == WAITING;
            let next = (current - 1) & !(WAITING);
            match bucket
                .state
                .compare_exchange_weak(current, next, Relaxed, Relaxed)
            {
                Ok(_) => {
                    if wakeup {
                        bucket.wait_queue.signal();
                    }
                    break;
                }
                Err(result) => current = result,
            }
        }
    }
}

impl<'g, K: Eq, V, L: LruList, const TYPE: char> Deref for Reader<'g, K, V, L, TYPE> {
    type Target = &'g Bucket<K, V, L, TYPE>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.bucket
    }
}

impl<'g, K: Eq, V, L: LruList, const TYPE: char> Drop for Reader<'g, K, V, L, TYPE> {
    #[inline]
    fn drop(&mut self) {
        Self::release(self.bucket);
    }
}

impl<K: Eq, V, const LEN: usize> Default for Metadata<K, V, LEN> {
    #[inline]
    fn default() -> Self {
        Self {
            link: AtomicShared::default(),
            occupied_bitmap: 0,
            removed_bitmap_or_lru_tail: 0,
            partial_hash_array: [0; LEN],
        }
    }
}

impl LruList for () {}

impl LruList for DoublyLinkedList {
    #[inline]
    fn evict(&mut self, tail: u32) -> Option<(u8, u32)> {
        if tail == 0 {
            None
        } else {
            let lru = self[tail as usize - 1].0;
            let new_tail = if tail - 1 == u32::from(lru) {
                // Reset the linked list.
                0
            } else {
                let new_lru = self[lru as usize].0;
                {
                    #![allow(clippy::cast_possible_truncation)]
                    self[new_lru as usize].1 = tail as u8 - 1;
                }
                self[tail as usize - 1].0 = new_lru;
                tail
            };
            self[lru as usize] = (0, 0);
            Some((lru, new_tail))
        }
    }

    #[inline]
    fn remove(&mut self, tail: u32, entry: u8) -> Option<u32> {
        if tail == 0
            || (self[entry as usize] == (0, 0)
                && (self[0] != (entry, entry) || (tail != 1 && tail != u32::from(entry) + 1)))
        {
            // The linked list is empty, or the entry is not a part of the linked list.
            return None;
        }

        if self[entry as usize].0 == entry {
            // It is the head and the only entry of the linked list.
            debug_assert_eq!(tail, u32::from(entry) + 1);
            self[entry as usize] = (0, 0);
            return Some(0);
        }

        // Adjust `prev -> current`.
        let prev = self[entry as usize].0;
        debug_assert_eq!(self[prev as usize].1, entry);
        self[prev as usize].1 = self[entry as usize].1;

        // Adjust `next -> current`.
        let next = self[entry as usize].1;
        debug_assert_eq!(self[next as usize].0, entry);
        self[next as usize].0 = self[entry as usize].0;

        let new_tail = if tail == u32::from(entry) + 1 {
            // Update `head`.
            Some(u32::from(self[entry as usize].1) + 1)
        } else {
            None
        };
        self[entry as usize] = (0, 0);

        new_tail
    }

    #[inline]
    fn promote(&mut self, tail: u32, entry: u8) -> Option<u32> {
        if tail == u32::from(entry) + 1 {
            // Nothing to do.
            return None;
        } else if tail == 0 {
            // The linked list is empty.
            self[entry as usize].0 = entry;
            self[entry as usize].1 = entry;
            return Some(u32::from(entry) + 1);
        }

        // Remove the entry from the linked list only if it is a part of it.
        if self[entry as usize] != (0, 0) || (self[0] == (entry, entry) && tail == 1) {
            // Adjust `prev -> current`.
            let prev = self[entry as usize].0;
            debug_assert_eq!(self[prev as usize].1, entry);
            self[prev as usize].1 = self[entry as usize].1;

            // Adjust `next -> current`.
            let next = self[entry as usize].1;
            debug_assert_eq!(self[next as usize].0, entry);
            self[next as usize].0 = self[entry as usize].0;
        }

        // Adjust `oldest -> head`.
        let oldest = self[tail as usize - 1].0;
        debug_assert_eq!(u32::from(self[oldest as usize].1) + 1, tail);
        self[oldest as usize].1 = entry;
        self[entry as usize].0 = oldest;

        // Adjust `head -> new head`
        self[tail as usize - 1].0 = entry;
        {
            #![allow(clippy::cast_possible_truncation)]
            self[entry as usize].1 = tail as u8 - 1;
        }

        // Update `head`.
        Some(u32::from(entry) + 1)
    }
}

impl<K: Eq, V, const LEN: usize> LinkedBucket<K, V, LEN> {
    /// Creates an empty [`LinkedBucket`].
    fn new(next: Option<Shared<LinkedBucket<K, V, LINKED_BUCKET_LEN>>>) -> Self {
        Self {
            metadata: Metadata {
                link: next.map_or_else(AtomicShared::null, AtomicShared::from),
                occupied_bitmap: 0,
                removed_bitmap_or_lru_tail: 0,
                partial_hash_array: [0; LEN],
            },
            data_block: unsafe {
                #[allow(clippy::uninit_assumed_init)]
                MaybeUninit::uninit().assume_init()
            },
            prev_link: AtomicPtr::default(),
        }
    }
}

impl<K: Eq, V, const LEN: usize> Debug for LinkedBucket<K, V, LEN> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LinkedBucket").finish()
    }
}

impl<K: Eq, V, const LEN: usize> Drop for LinkedBucket<K, V, LEN> {
    #[inline]
    fn drop(&mut self) {
        if needs_drop::<(K, V)>() {
            let mut index = self.metadata.occupied_bitmap.trailing_zeros();
            while index != 32 {
                unsafe {
                    ptr::drop_in_place(self.data_block[index as usize].as_mut_ptr());
                }
                self.metadata.occupied_bitmap -= 1_u32 << index;
                index = self.metadata.occupied_bitmap.trailing_zeros();
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::wait_queue::DeriveAsyncWait;
    use proptest::prelude::*;
    use std::pin::Pin;
    use tokio::sync::Barrier;

    static_assertions::assert_eq_size!(Bucket<String, String, (), OPTIMISTIC>, [u8; BUCKET_LEN * 2]);
    static_assertions::assert_eq_size!(Bucket<String, String, DoublyLinkedList, CACHE>, [u8; BUCKET_LEN * 4]);

    fn default_bucket<K: Eq, V, L: LruList, const TYPE: char>() -> Bucket<K, V, L, TYPE> {
        Bucket {
            state: AtomicU32::new(0),
            num_entries: 0,
            metadata: Metadata::default(),
            wait_queue: WaitQueue::default(),
            lru_list: L::default(),
        }
    }

    proptest! {
        #[cfg_attr(miri, ignore)]
        #[test]
        fn evict_untracked(xs in 0..BUCKET_LEN * 2) {
            let mut data_block: DataBlock<usize, usize, BUCKET_LEN> =
                unsafe { MaybeUninit::uninit().assume_init() };
            let mut bucket: Bucket<usize, usize, DoublyLinkedList, CACHE> = default_bucket();
            for v in 0..xs {
                let guard = Guard::new();
                let mut locker = Locker::lock(&mut bucket, &guard).unwrap();
                let evicted = locker.evict_lru_head(&mut data_block);
                assert_eq!(v >= BUCKET_LEN, evicted.is_some());
                locker.insert_with(&mut data_block, 0, || (v, v), &guard);
                assert_eq!(locker.metadata.removed_bitmap_or_lru_tail, 0);
            }
        }

        #[cfg_attr(miri, ignore)]
        #[test]
        fn evict_overflowed(xs in 1..BUCKET_LEN * 2) {
            let mut data_block: DataBlock<usize, usize, BUCKET_LEN> =
                unsafe { MaybeUninit::uninit().assume_init() };
            let mut bucket: Bucket<usize, usize, DoublyLinkedList, CACHE> = default_bucket();
            let guard = Guard::new();
            let mut locker = Locker::lock(&mut bucket, &guard).unwrap();
            for _ in 0..3 {
                for v in 0..xs {
                    let entry_ptr = locker.insert_with(&mut data_block, 0, || (v, v), &guard);
                    locker.update_lru_tail(&entry_ptr);
                    if v < BUCKET_LEN {
                        assert_eq!(locker.metadata.removed_bitmap_or_lru_tail as usize, v + 1);
                    }
                    assert_eq!(locker.lru_list[locker.metadata.removed_bitmap_or_lru_tail as usize - 1].0, 0);
                }

                let mut evicted_key = None;
                if xs >= BUCKET_LEN {
                    let evicted = locker.evict_lru_head(&mut data_block);
                    assert!(evicted.is_some());
                    evicted_key = evicted.map(|(k, _)| k);
                }
                assert_ne!(locker.metadata.removed_bitmap_or_lru_tail, 0);

                for v in 0..xs {
                    let entry_ptr = locker.get(&data_block, &v, 0, &guard);
                    if entry_ptr.is_valid() {
                        let erased = locker.erase(&mut data_block, &entry_ptr);
                        assert!(erased.is_some());
                    } else {
                        assert_eq!(v, evicted_key.unwrap());
                    }
                }
                assert_eq!(locker.metadata.removed_bitmap_or_lru_tail, 0);
            }
        }

        #[cfg_attr(miri, ignore)]
        #[test]
        fn evict_tracked(xs in 0..BUCKET_LEN * 2) {
            let mut data_block: DataBlock<usize, usize, BUCKET_LEN> =
                unsafe { MaybeUninit::uninit().assume_init() };
            let mut bucket: Bucket<usize, usize, DoublyLinkedList, CACHE> = default_bucket();
            for v in 0..xs {
                let guard = Guard::new();
                let mut locker = Locker::lock(&mut bucket, &guard).unwrap();
                let evicted = locker.evict_lru_head(&mut data_block);
                assert_eq!(v >= BUCKET_LEN, evicted.is_some());
                let mut entry_ptr = locker.insert_with(&mut data_block, 0, || (v, v), &guard);
                locker.update_lru_tail(&entry_ptr);
                assert_eq!(locker.metadata.removed_bitmap_or_lru_tail as usize, entry_ptr.current_index + 1);
                if v >= BUCKET_LEN {
                    entry_ptr.current_index = xs % BUCKET_LEN;
                    locker.update_lru_tail(&entry_ptr);
                    assert_eq!(locker.metadata.removed_bitmap_or_lru_tail as usize, entry_ptr.current_index + 1);
                    let mut iterated = 1;
                    let mut i = locker.lru_list[entry_ptr.current_index].1 as usize;
                    while i != entry_ptr.current_index {
                        iterated += 1;
                        i = locker.lru_list[i].1 as usize;
                    }
                    assert_eq!(iterated, BUCKET_LEN);
                    iterated = 1;
                    i = locker.lru_list[entry_ptr.current_index].0 as usize;
                    while i != entry_ptr.current_index {
                        iterated += 1;
                        i = locker.lru_list[i].0 as usize;
                    }
                    assert_eq!(iterated, BUCKET_LEN);
                }
            }
        }

        #[cfg_attr(miri, ignore)]
        #[test]
        fn removed(xs in 0..BUCKET_LEN) {
            let mut data_block: DataBlock<usize, usize, BUCKET_LEN> =
                unsafe { MaybeUninit::uninit().assume_init() };
            let mut bucket: Bucket<usize, usize, DoublyLinkedList, CACHE> = default_bucket();
            for v in 0..xs {
                let guard = Guard::new();
                let mut locker = Locker::lock(&mut bucket, &guard).unwrap();
                let entry_ptr = locker.insert_with(&mut data_block, 0, || (v, v), &guard);
                locker.update_lru_tail(&entry_ptr);
                let mut iterated = 1;
                let mut i = locker.lru_list[entry_ptr.current_index].1 as usize;
                while i != entry_ptr.current_index {
                    iterated += 1;
                    i = locker.lru_list[i].1 as usize;
                }
                assert_eq!(iterated, v + 1);
            }
            for v in 0..xs {
                let guard = Guard::new();
                let mut locker = Locker::lock(&mut bucket, &guard).unwrap();
                let entry_ptr = locker.get(&data_block, &v, 0, &guard);
                let mut iterated = 1;
                let mut i = locker.lru_list[entry_ptr.current_index].1 as usize;
                while i != entry_ptr.current_index {
                    iterated += 1;
                    i = locker.lru_list[i].1 as usize;
                }
                assert_eq!(iterated, xs - v);
                locker.remove_from_lru_list(&entry_ptr);
            }
            assert_eq!(bucket.metadata.removed_bitmap_or_lru_tail, 0);
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
            let data_ptr = AtomicPtr::new(&mut data);
            task_handles.push(tokio::spawn(async move {
                barrier_clone.wait().await;
                let partial_hash = (task_id % BUCKET_LEN).try_into().unwrap();
                let bucket_mut = unsafe { &mut *bucket_clone.as_ptr().cast_mut() };
                let data_block_mut = unsafe { &mut *data_block_clone.as_ptr().cast_mut() };
                let guard = Guard::new();
                for i in 0..2048 {
                    let mut exclusive_locker = Locker::lock(bucket_mut, &guard).unwrap();
                    let mut sum: u64 = 0;
                    for j in 0..128 {
                        unsafe {
                            sum += (*data_ptr.load(Relaxed))[j];
                            (*data_ptr.load(Relaxed))[j] = if i % 4 == 0 { 2 } else { 4 }
                        };
                    }
                    assert_eq!(sum % 256, 0);
                    if i == 0 {
                        exclusive_locker.insert_with(
                            data_block_mut,
                            partial_hash,
                            || (task_id, 0),
                            &guard,
                        );
                    } else {
                        assert_eq!(
                            exclusive_locker
                                .search(&data_block_clone, &task_id, partial_hash, &guard)
                                .unwrap(),
                            &(task_id, 0_usize)
                        );
                    }
                    drop(exclusive_locker);

                    let read_locker = Reader::lock(&*bucket_clone, &guard).unwrap();
                    assert_eq!(
                        read_locker
                            .search(&data_block_clone, &task_id, partial_hash, &guard)
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
        assert_eq!(bucket.num_entries(), num_tasks);

        let epoch_guard = Guard::new();
        for task_id in 0..num_tasks {
            assert_eq!(
                bucket.search(
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
        while entry_ptr.next(&bucket, &epoch_guard) {
            count += 1;
        }
        assert_eq!(bucket.num_entries(), count);

        let mut xlocker = Locker::lock(unsafe { bucket.get_mut().unwrap() }, &epoch_guard).unwrap();
        (*xlocker).kill(&epoch_guard);
        drop(xlocker);

        assert!(bucket.killed());
        assert_eq!(bucket.num_entries(), 0);
        assert!(Locker::lock(unsafe { bucket.get_mut().unwrap() }, &epoch_guard).is_none());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn bucket_lock_async() {
        let num_tasks = BUCKET_LEN + 2;
        let barrier = Shared::new(Barrier::new(num_tasks));
        let data_block: Shared<DataBlock<usize, usize, BUCKET_LEN>> =
            Shared::new(unsafe { MaybeUninit::uninit().assume_init() });
        let bucket: Shared<Bucket<usize, usize, (), SEQUENTIAL>> = Shared::new(default_bucket());
        let mut task_handles = Vec::with_capacity(num_tasks);
        for task_id in 0..num_tasks {
            let barrier_clone = barrier.clone();
            let data_block_clone = data_block.clone();
            let bucket_clone = bucket.clone();
            task_handles.push(tokio::spawn(async move {
                let partial_hash = (task_id % BUCKET_LEN).try_into().unwrap();
                barrier_clone.wait().await;
                for _ in 0..256 {
                    loop {
                        let mut async_wait = AsyncWait::default();
                        let mut async_wait_pinned = Pin::new(&mut async_wait);
                        {
                            let guard = Guard::new();
                            if let Ok(exclusive_locker) = Locker::try_lock_or_wait(
                                unsafe { &mut *bucket_clone.as_ptr().cast_mut() },
                                async_wait_pinned.derive().unwrap(),
                                &guard,
                            ) {
                                let data_block_mut =
                                    unsafe { &mut *data_block_clone.as_ptr().cast_mut() };
                                let mut exclusive_locker = exclusive_locker.unwrap();
                                exclusive_locker.insert_with(
                                    data_block_mut,
                                    partial_hash,
                                    || (task_id, 0),
                                    &guard,
                                );
                                break;
                            };
                        }
                        async_wait_pinned.await;
                    }
                    loop {
                        let mut async_wait = AsyncWait::default();
                        let mut async_wait_pinned = Pin::new(&mut async_wait);
                        {
                            let guard = Guard::new();
                            if let Ok(read_locker) = Reader::try_lock_or_wait(
                                &*bucket_clone,
                                async_wait_pinned.derive().unwrap(),
                                &guard,
                            ) {
                                assert_eq!(
                                    read_locker
                                        .unwrap()
                                        .search(&data_block_clone, &task_id, partial_hash, &guard,)
                                        .unwrap(),
                                    &(task_id, 0_usize)
                                );
                                break;
                            };
                        }
                        async_wait_pinned.await;
                    }
                    {
                        let bucket_mut = unsafe { &mut *bucket_clone.as_ptr().cast_mut() };
                        let data_block_mut = unsafe { &mut *data_block_clone.as_ptr().cast_mut() };
                        let guard = Guard::new();
                        let mut exclusive_locker = Locker::lock(bucket_mut, &guard).unwrap();
                        let entry_ptr =
                            exclusive_locker.get(&data_block_clone, &task_id, partial_hash, &guard);
                        assert_eq!(
                            exclusive_locker.erase(data_block_mut, &entry_ptr).unwrap(),
                            (task_id, 0_usize)
                        );
                    }
                }
            }));
        }
        for r in futures::future::join_all(task_handles).await {
            assert!(r.is_ok());
        }
    }
}
