pub mod bucket;
pub mod bucket_array;

use std::hash::{BuildHasher, Hash};
use std::ptr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

use bucket::{BUCKET_LEN, CACHE, DataBlock, EntryPtr, LruList, OPTIMISTIC, Reader, Writer};
use bucket_array::BucketArray;
use sdd::{AtomicShared, Guard, Ptr, Shared, Tag};

use super::Equivalent;
use super::exit_guard::ExitGuard;
use crate::async_helper::SendableGuard;

/// The maximum resize factor.
const MAX_RESIZE_FACTOR: usize = (usize::BITS / 2) as usize;

/// `HashTable` defines common functions for hash table implementations.
pub(super) trait HashTable<K, V, H, L: LruList, const TYPE: char>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    /// Returns the hash value of the key.
    #[inline]
    fn hash<Q>(&self, key: &Q) -> u64
    where
        Q: Equivalent<K> + Hash + ?Sized,
    {
        self.hasher().hash_one(key)
    }

    /// Returns the partial hash value of the given hash.
    #[allow(clippy::cast_possible_truncation)]
    #[inline]
    fn partial_hash(hash: u64) -> u8 {
        (hash & (u64::from(u8::MAX))) as u8
    }

    /// Returns a reference to its [`BuildHasher`].
    fn hasher(&self) -> &H;

    /// Returns a reference to the [`BucketArray`] pointer.
    fn bucket_array(&self) -> &AtomicShared<BucketArray<K, V, L, TYPE>>;

    /// Calculates the bucket index from the supplied key.
    #[inline]
    fn calculate_bucket_index<Q>(&self, key: &Q) -> usize
    where
        Q: Equivalent<K> + Hash + ?Sized,
    {
        self.bucket_array()
            .load(Acquire, &Guard::new())
            .as_ref()
            .map_or(0, |a| a.calculate_bucket_index(self.hash(key)))
    }

    /// Returns the minimum allowed capacity.
    fn minimum_capacity(&self) -> &AtomicUsize;

    /// Returns the maximum capacity.
    ///
    /// The maximum capacity must be a power of `2`.
    fn maximum_capacity(&self) -> usize;

    /// Reserves the specified capacity.
    ///
    /// Returns the actually allocated capacity.
    fn reserve_capacity(&self, additional_capacity: usize) -> usize {
        let mut current_minimum_capacity = self.minimum_capacity().load(Relaxed);
        loop {
            let Some(new_minimum_capacity) =
                current_minimum_capacity.checked_add(additional_capacity)
            else {
                return 0;
            };

            match self.minimum_capacity().compare_exchange_weak(
                current_minimum_capacity,
                new_minimum_capacity,
                Relaxed,
                Relaxed,
            ) {
                Ok(_) => {
                    let guard = Guard::new();
                    if let Some(current_array) = self.bucket_array().load(Acquire, &guard).as_ref()
                    {
                        if !current_array.has_old_array() {
                            self.try_resize(current_array, 0, &guard);
                        }
                    }
                    return additional_capacity;
                }
                Err(actual) => current_minimum_capacity = actual,
            }
        }
    }

    /// Returns a reference to the bucket array.
    ///
    /// Allocates a new one if If no bucket array has been allocated.
    #[inline]
    fn get_or_create_bucket_array<'g>(&self, guard: &'g Guard) -> &'g BucketArray<K, V, L, TYPE> {
        if let Some(current_array) = self.bucket_array().load(Acquire, guard).as_ref() {
            current_array
        } else {
            unsafe {
                match self.bucket_array().compare_exchange(
                    Ptr::null(),
                    (
                        Some(Shared::new_unchecked(BucketArray::<K, V, L, TYPE>::new(
                            self.minimum_capacity().load(Relaxed),
                            AtomicShared::null(),
                        ))),
                        Tag::None,
                    ),
                    AcqRel,
                    Acquire,
                    guard,
                ) {
                    Ok((_, ptr)) | Err((_, ptr)) => ptr.as_ref().unwrap_unchecked(),
                }
            }
        }
    }

    /// Returns the number of entry slots.
    #[inline]
    fn num_slots(&self, guard: &Guard) -> usize {
        if let Some(current_array) = self.bucket_array().load(Acquire, guard).as_ref() {
            current_array.num_slots()
        } else {
            0
        }
    }

    /// Returns the number of entries.
    #[inline]
    fn num_entries(&self, guard: &Guard) -> usize {
        let mut num_entries = 0;
        if let Some(current_array) = self.bucket_array().load(Acquire, guard).as_ref() {
            let old_array_ptr = current_array.old_array(guard);
            if let Some(old_array) = old_array_ptr.as_ref() {
                if !self.incremental_rehash_sync::<true>(current_array, guard) {
                    for i in 0..old_array.len() {
                        num_entries += old_array.bucket(i).len();
                    }
                }
            }
            for i in 0..current_array.len() {
                num_entries += current_array.bucket(i).len();
            }
            if num_entries == 0
                && self.minimum_capacity().load(Relaxed) == 0
                && !current_array.has_old_array()
            {
                self.try_resize(current_array, 0, guard);
            }
        }
        num_entries
    }

    /// For the given index in the current array, calculate the respective range in the old array.
    #[inline]
    fn from_index_to_range(from_len: usize, to_len: usize, from_index: usize) -> (usize, usize) {
        debug_assert!(from_len.is_power_of_two() && to_len.is_power_of_two());
        if from_len < to_len {
            let ratio = to_len / from_len;
            let start_index = from_index * ratio;
            debug_assert!(
                start_index + ratio <= to_len,
                "+ {start_index} < {to_len}, {from_len} {to_len} {ratio} {from_index}"
            );
            (start_index, start_index + ratio)
        } else {
            let ratio = from_len / to_len;
            let start_index = from_index / ratio;
            debug_assert!(
                start_index < to_len,
                "- {start_index} < {to_len}, {from_len} {to_len} {ratio} {from_index}"
            );
            (start_index, start_index + 1)
        }
    }

    /// Returns `true` if the number of entries is non-zero.
    fn has_entry(&self, guard: &Guard) -> bool {
        if let Some(current_array) = self.bucket_array().load(Acquire, guard).as_ref() {
            let old_array_ptr = current_array.old_array(guard);
            if let Some(old_array) = old_array_ptr.as_ref() {
                if !self.incremental_rehash_sync::<true>(current_array, guard) {
                    for i in 0..old_array.len() {
                        if old_array.bucket(i).len() != 0 {
                            return true;
                        }
                    }
                }
            }
            for i in 0..current_array.len() {
                if current_array.bucket(i).len() != 0 {
                    return true;
                }
            }
            if self.minimum_capacity().load(Relaxed) == 0 && !current_array.has_old_array() {
                self.try_resize(current_array, 0, guard);
            }
        }
        false
    }

    /// Estimates the number of entries by sampling the specified number of buckets.
    #[inline]
    fn sample(
        current_array: &BucketArray<K, V, L, TYPE>,
        sampling_index: usize,
        sample_size: usize,
    ) -> usize {
        let mut num_entries = 0;
        for i in sampling_index..(sampling_index + sample_size) {
            num_entries += current_array.bucket(i % current_array.len()).len();
        }
        num_entries * (current_array.len() / sample_size)
    }

    /// Checks whether rebuilding the entire hash table is required.
    #[inline]
    fn check_rebuild(
        current_array: &BucketArray<K, V, L, TYPE>,
        sampling_index: usize,
        sample_size: usize,
    ) -> bool {
        let mut num_buckets_to_rebuild = 0;
        for i in sampling_index..(sampling_index + sample_size) {
            if current_array.bucket(i % current_array.len()).need_rebuild() {
                num_buckets_to_rebuild += 1;
                if num_buckets_to_rebuild >= sample_size / 2 {
                    return true;
                }
            }
        }
        false
    }

    /// Peeks an entry from the [`HashTable`].
    #[inline]
    fn peek_entry<'g, Q>(&self, key: &Q, hash: u64, guard: &'g Guard) -> Option<&'g (K, V)>
    where
        Q: Equivalent<K> + Hash + ?Sized,
    {
        debug_assert_eq!(TYPE, OPTIMISTIC);

        let partial_hash = Self::partial_hash(hash);
        let mut current_array_ptr = self.bucket_array().load(Acquire, guard);
        while let Some(current_array) = current_array_ptr.as_ref() {
            let index = current_array.calculate_bucket_index(hash);
            if let Some(old_array) = current_array.old_array(guard).as_ref() {
                if !self.dedup_bucket_sync::<true>(current_array, old_array, index, guard) {
                    let index = old_array.calculate_bucket_index(hash);
                    if let Some(entry) = old_array.bucket(index).search_entry(
                        old_array.data_block(index),
                        key,
                        partial_hash,
                        guard,
                    ) {
                        return Some(entry);
                    }
                }
            }

            if let Some(entry) = current_array.bucket(index).search_entry(
                current_array.data_block(index),
                key,
                partial_hash,
                guard,
            ) {
                return Some(entry);
            }

            let new_current_array_ptr = self.bucket_array().load(Acquire, guard);
            if current_array_ptr == new_current_array_ptr {
                break;
            }
            current_array_ptr = new_current_array_ptr;
        }
        None
    }

    /// Reads an entry asynchronously from the [`HashTable`] with a shared lock acquired on the
    /// bucket.
    #[inline]
    async fn reader_async_with<Q, R, F: FnOnce(&K, &V) -> R>(
        &self,
        key: &Q,
        hash: u64,
        f: F,
        sendable_guard: &SendableGuard,
    ) -> Option<R>
    where
        Q: Equivalent<K> + Hash + ?Sized,
    {
        let partial_hash = Self::partial_hash(hash);
        while let Some(current_array) = sendable_guard.load(self.bucket_array(), Acquire) {
            let index = current_array.calculate_bucket_index(hash);
            if current_array.has_old_array()
                && !self
                    .dedup_bucket_async(current_array, index, sendable_guard)
                    .await
            {
                continue;
            }

            let bucket = current_array.bucket(index);
            if let Some(reader) = Reader::lock_async(bucket, sendable_guard).await {
                if let Some(entry) = reader.search_entry(
                    current_array.data_block(index),
                    key,
                    partial_hash,
                    sendable_guard.guard(),
                ) {
                    return Some(f(&entry.0, &entry.1));
                }
                break;
            }
        }
        None
    }

    /// Reads an entry synchronously from the [`HashTable`] with a shared lock acquired on the
    /// bucket.
    #[inline]
    fn reader_sync_with<Q, R, F: FnOnce(&K, &V) -> R>(
        &self,
        key: &Q,
        hash: u64,
        f: F,
        guard: &Guard,
    ) -> Option<R>
    where
        Q: Equivalent<K> + Hash + ?Sized,
    {
        let partial_hash = Self::partial_hash(hash);
        while let Some(current_array) = self.bucket_array().load(Acquire, guard).as_ref() {
            let index = current_array.calculate_bucket_index(hash);
            if let Some(old_array) = current_array.old_array(guard).as_ref() {
                self.dedup_bucket_sync::<false>(current_array, old_array, index, guard);
            }

            let bucket = current_array.bucket(index);
            if let Some(reader) = Reader::lock_sync(bucket) {
                if let Some(entry) =
                    reader.search_entry(current_array.data_block(index), key, partial_hash, guard)
                {
                    return Some(f(&entry.0, &entry.1));
                }
                break;
            }
        }
        None
    }

    /// Writes an entry asynchronously with an exclusive lock acquired on the bucket.
    ///
    /// If the corresponding bucket does not exist, a new one is created.
    #[inline]
    async fn writer_async_with<
        R,
        F: FnOnce(Writer<K, V, L, TYPE>, &DataBlock<K, V, BUCKET_LEN>, usize, usize) -> R,
    >(
        &self,
        hash: u64,
        sendable_guard: &SendableGuard,
        f: F,
    ) -> R {
        loop {
            let current_array = self.get_or_create_bucket_array(sendable_guard.guard());
            let index = current_array.calculate_bucket_index(hash);
            if current_array.has_old_array()
                && !self
                    .dedup_bucket_async(current_array, index, sendable_guard)
                    .await
            {
                continue;
            }

            let bucket = current_array.bucket(index);
            if (TYPE != CACHE || current_array.num_slots() < self.maximum_capacity())
                && current_array.initiate_sampling(index)
                && bucket.len() >= BUCKET_LEN - 1
            {
                self.try_enlarge(current_array, index, bucket.len(), sendable_guard.guard());
            }

            if let Some(writer) = Writer::lock_async(bucket, sendable_guard).await {
                return f(
                    writer,
                    current_array.data_block(index),
                    index,
                    current_array.len(),
                );
            }
        }
    }

    /// Writes an entry synchronously with an exclusive lock acquired on the bucket.
    ///
    /// If the corresponding bucket does not exist, a new one is created.
    #[inline]
    fn writer_sync_with<
        R,
        F: FnOnce(Writer<K, V, L, TYPE>, &DataBlock<K, V, BUCKET_LEN>, usize, usize) -> R,
    >(
        &self,
        hash: u64,
        guard: &Guard,
        f: F,
    ) -> R {
        loop {
            let current_array = self.get_or_create_bucket_array(guard);
            let index = current_array.calculate_bucket_index(hash);
            if let Some(old_array) = current_array.old_array(guard).as_ref() {
                self.dedup_bucket_sync::<false>(current_array, old_array, index, guard);
            }

            let bucket = current_array.bucket(index);
            if (TYPE != CACHE || current_array.num_slots() < self.maximum_capacity())
                && current_array.initiate_sampling(index)
                && bucket.len() >= BUCKET_LEN - 1
            {
                self.try_enlarge(current_array, index, bucket.len(), guard);
            }

            if let Some(writer) = Writer::lock_sync(bucket) {
                return f(
                    writer,
                    current_array.data_block(index),
                    index,
                    current_array.len(),
                );
            }
        }
    }

    /// Writes an entry asynchronously with an exclusive lock acquired on the bucket.
    ///
    /// The [`Writer`] passed to the closure may not contain the desired entry. The supplied closure
    /// is only invoked if the bucket possibly containing the key exists. The closure returning
    /// `(_, true)` indicates that entries were removed from the bucket.
    #[inline]
    async fn optional_writer_async_with<
        R,
        F: FnOnce(Writer<K, V, L, TYPE>, &DataBlock<K, V, BUCKET_LEN>, usize, usize) -> (R, bool),
    >(
        &self,
        hash: u64,
        sendable_guard: &SendableGuard,
        f: F,
    ) -> Result<R, F> {
        while let Some(current_array) = sendable_guard.load(self.bucket_array(), Acquire) {
            let index = current_array.calculate_bucket_index(hash);
            if current_array.has_old_array()
                && !self
                    .dedup_bucket_async(current_array, index, sendable_guard)
                    .await
            {
                continue;
            }

            let bucket = current_array.bucket(index);
            if let Some(writer) = Writer::lock_async(bucket, sendable_guard).await {
                let (result, try_shrink) = f(
                    writer,
                    current_array.data_block(index),
                    index,
                    current_array.len(),
                );
                if try_shrink {
                    if let Some(current_array) = sendable_guard.load(self.bucket_array(), Acquire) {
                        if current_array.initiate_sampling(index) {
                            self.try_shrink_or_rebuild(
                                current_array,
                                index,
                                sendable_guard.guard(),
                            );
                        }
                    }
                }
                return Ok(result);
            }
        }
        Err(f)
    }

    /// Writes an entry synchronously with an exclusive lock acquired on the bucket.
    ///
    /// The [`Writer`] passed to the closure may not contain the desired entry. The supplied closure
    /// is only invoked if the bucket possibly containing the key exists. The closure returning
    /// `(_, true)` indicates that entries were removed from the bucket.
    #[inline]
    fn optional_writer_sync_with<
        R,
        F: FnOnce(Writer<K, V, L, TYPE>, &DataBlock<K, V, BUCKET_LEN>, usize, usize) -> (R, bool),
    >(
        &self,
        hash: u64,
        guard: &Guard,
        f: F,
    ) -> Result<R, F> {
        while let Some(current_array) = self.bucket_array().load(Acquire, guard).as_ref() {
            let index = current_array.calculate_bucket_index(hash);
            if let Some(old_array) = current_array.old_array(guard).as_ref() {
                self.dedup_bucket_sync::<false>(current_array, old_array, index, guard);
            }

            let bucket = current_array.bucket(index);
            if let Some(writer) = Writer::lock_sync(bucket) {
                let (result, try_shrink) = f(
                    writer,
                    current_array.data_block(index),
                    index,
                    current_array.len(),
                );
                if try_shrink && current_array.initiate_sampling(index) {
                    self.try_shrink_or_rebuild(current_array, index, guard);
                }
                return Ok(result);
            }
        }
        Err(f)
    }

    /// Iterates over all the buckets in the [`HashTable`].
    ///
    /// This methods stops iterating when the closure returns `(true, _)`. The closure returning
    /// `(_, true)` means that entries were removed from the bucket.
    #[inline]
    async fn for_each_writer_async_with<
        F: FnMut(Writer<K, V, L, TYPE>, &DataBlock<K, V, BUCKET_LEN>, usize, usize) -> (bool, bool),
    >(
        &self,
        mut start_index: usize,
        expected_array_len: usize,
        sendable_guard: &SendableGuard,
        mut f: F,
    ) {
        let mut try_shrink = false;
        let mut prev_len = expected_array_len;
        while let Some(current_array) = sendable_guard.load(self.bucket_array(), Acquire) {
            // In case the method is repeating the routine, iterate over entries from the middle of
            // the array.
            let current_array_len = current_array.len();
            start_index = if prev_len == 0 || prev_len == current_array_len {
                start_index
            } else {
                Self::from_index_to_range(prev_len, current_array_len, start_index).0
            };
            prev_len = current_array_len;

            while start_index < current_array_len {
                let index = start_index;
                if current_array.has_old_array()
                    && !self
                        .dedup_bucket_async(current_array, index, sendable_guard)
                        .await
                {
                    // Retry the operation since there is a possibility that the current bucket
                    // array was replaced by a new one.
                    break;
                }

                let bucket = current_array.bucket(index);
                if let Some(writer) = Writer::lock_async(bucket, sendable_guard).await {
                    let data_block = current_array.data_block(index);
                    let (found, removed) = f(writer, data_block, index, current_array_len);
                    try_shrink |= removed;
                    if found {
                        // Stop iterating over buckets.
                        if try_shrink {
                            self.try_shrink_or_rebuild(current_array, 0, sendable_guard.guard());
                        }
                        return;
                    }
                } else {
                    // Retry the operation for the same reason above.
                    break;
                }
                start_index += 1;
            }

            if start_index == current_array_len {
                if try_shrink {
                    self.try_shrink_or_rebuild(current_array, 0, sendable_guard.guard());
                }
                break;
            }
        }
    }

    /// Iterates over all the buckets in the [`HashTable`].
    ///
    /// This methods stops iterating when the closure returns `(true, _)`. The closure returning
    /// `(_, true)` means that entries were removed from the bucket.
    #[inline]
    fn for_each_writer_sync_with<
        F: FnMut(Writer<K, V, L, TYPE>, &DataBlock<K, V, BUCKET_LEN>, usize, usize) -> (bool, bool),
    >(
        &self,
        mut start_index: usize,
        expected_array_len: usize,
        guard: &Guard,
        mut f: F,
    ) {
        let mut try_shrink = false;
        let mut prev_len = expected_array_len;
        while let Some(current_array) = self.bucket_array().load(Acquire, guard).as_ref() {
            // In case the method is repeating the routine, iterate over entries from the middle of
            // the array.
            let current_array_len = current_array.len();
            start_index = if prev_len == 0 || prev_len == current_array_len {
                start_index
            } else {
                Self::from_index_to_range(prev_len, current_array_len, start_index).0
            };
            prev_len = current_array_len;

            while start_index < current_array_len {
                let index = start_index;
                if let Some(old_array) = current_array.old_array(guard).as_ref() {
                    self.dedup_bucket_sync::<false>(current_array, old_array, index, guard);
                }

                let bucket = current_array.bucket(index);
                if let Some(writer) = Writer::lock_sync(bucket) {
                    let data_block = current_array.data_block(index);
                    let (found, removed) = f(writer, data_block, index, current_array_len);
                    try_shrink |= removed;
                    if found {
                        // Stop iterating over buckets.
                        if try_shrink {
                            self.try_shrink_or_rebuild(current_array, 0, guard);
                        }
                        return;
                    }
                } else {
                    // `current_array` is no longer the current one.
                    break;
                }
                start_index += 1;
            }

            if start_index == current_array_len {
                if try_shrink {
                    self.try_shrink_or_rebuild(current_array, 0, guard);
                }
                break;
            }
        }
    }

    /// Tries to reserve an entry and returns a [`Writer`] and [`EntryPtr`] corresponding to the
    /// key.
    ///
    /// The returned [`EntryPtr`] points to an occupied entry if the key exists. Returns `None` if
    /// locking failed.
    #[inline]
    fn try_reserve_entry<'g, Q>(
        &self,
        key: &Q,
        hash: u64,
        guard: &'g Guard,
    ) -> Option<LockedEntry<'g, K, V, L, TYPE>>
    where
        Q: Equivalent<K> + Hash + ?Sized,
    {
        loop {
            let current_array = self.get_or_create_bucket_array(guard);
            let index = current_array.calculate_bucket_index(hash);
            if let Some(old_array) = current_array.old_array(guard).as_ref() {
                if !self.dedup_bucket_sync::<true>(current_array, old_array, index, guard) {
                    return None;
                }
            }

            let mut bucket = current_array.bucket(index);
            if (TYPE != CACHE || current_array.num_slots() < self.maximum_capacity())
                && current_array.initiate_sampling(index)
                && bucket.len() >= BUCKET_LEN - 1
            {
                self.try_enlarge(current_array, index, bucket.len(), guard);
                bucket = current_array.bucket(index);
            }

            let Ok(writer) = Writer::try_lock(bucket) else {
                return None;
            };
            if let Some(writer) = writer {
                let data_block = current_array.data_block(index);
                let entry_ptr =
                    writer.get_entry_ptr(data_block, key, Self::partial_hash(hash), guard);
                return Some(LockedEntry::new(
                    writer,
                    data_block,
                    entry_ptr,
                    index,
                    current_array.len(),
                    guard,
                ));
            }
        }
    }

    /// Deduplicates buckets that may share the same hash values asynchronously.
    ///
    /// Returns `false` if the old buckets may remain in the old bucket array, or the whole
    /// operation has to be retried due to an ABA problem.
    ///
    /// # Note
    ///
    /// There is a possibility of an ABA problem where the bucket array was deallocated and a new
    /// bucket array of a different size has been allocated in the same memory region. In order
    /// to avoid this problem, if it finds a killed bucket and the task was suspended, returns
    /// `false`.
    async fn dedup_bucket_async<'g>(
        &self,
        current_array: &'g BucketArray<K, V, L, TYPE>,
        index: usize,
        sendable_guard: &'g SendableGuard,
    ) -> bool {
        self.incremental_rehash_async(current_array, sendable_guard)
            .await;

        if !sendable_guard
            .load(self.bucket_array(), Acquire)
            .is_some_and(|r| ptr::eq(r, current_array))
        {
            return false;
        }

        if let Some(old_array) = sendable_guard.load(current_array.old_array_ptr(), Acquire) {
            let range = Self::from_index_to_range(current_array.len(), old_array.len(), index);
            for old_index in range.0..range.1 {
                let bucket = old_array.bucket(old_index);
                let writer = Writer::lock_async(bucket, sendable_guard).await;
                if let Some(writer) = writer {
                    self.relocate_bucket_async(
                        current_array,
                        old_array,
                        old_index,
                        writer,
                        sendable_guard,
                    )
                    .await;
                } else if !sendable_guard.is_valid() {
                    // The bucket was killed and the guard has been invalidated. Validating the
                    // reference is not sufficient in this case since the current bucket array could
                    // have been replaced with a new one.
                    return false;
                } else if !sendable_guard
                    .load(self.bucket_array(), Acquire)
                    .is_some_and(|a| ptr::eq(a, current_array))
                {
                    // A new bucket array was created in the meantime.
                    return false;
                }

                // The old bucket array was removed, no point in trying to move entries from it.
                if !current_array.has_old_array() {
                    break;
                }
            }
        }

        true
    }

    /// Deduplicates buckets that may share the same hash values synchronously.
    ///
    /// Returns `true` if the corresponding entries were successfully moved.
    fn dedup_bucket_sync<'g, const TRY_LOCK: bool>(
        &self,
        current_array: &'g BucketArray<K, V, L, TYPE>,
        old_array: &'g BucketArray<K, V, L, TYPE>,
        index: usize,
        guard: &'g Guard,
    ) -> bool {
        if self.incremental_rehash_sync::<TRY_LOCK>(current_array, guard) {
            return true;
        }
        let range = Self::from_index_to_range(current_array.len(), old_array.len(), index);
        for old_index in range.0..range.1 {
            let bucket = old_array.bucket(old_index);
            let writer = if TRY_LOCK {
                let Ok(writer) = Writer::try_lock(bucket) else {
                    return false;
                };
                writer
            } else {
                Writer::lock_sync(bucket)
            };
            if let Some(writer) = writer {
                if !self.relocate_bucket_sync::<TRY_LOCK>(
                    current_array,
                    old_array,
                    old_index,
                    writer,
                    guard,
                ) {
                    return false;
                }
            }
        }
        true
    }

    /// Relocates the bucket to the current bucket array.
    async fn relocate_bucket_async<'g>(
        &self,
        current_array: &'g BucketArray<K, V, L, TYPE>,
        old_array: &'g BucketArray<K, V, L, TYPE>,
        old_index: usize,
        old_writer: Writer<'g, K, V, L, TYPE>,
        sendable_guard: &'g SendableGuard,
    ) {
        if old_writer.len() == 0 {
            old_writer.kill();
            return;
        }

        let target_index =
            Self::from_index_to_range(old_array.len(), current_array.len(), old_index).0;
        let mut target_buckets: [Option<Writer<K, V, L, TYPE>>; MAX_RESIZE_FACTOR] =
            Default::default();
        let mut max_index = 0;
        let mut entry_ptr = EntryPtr::new(sendable_guard.guard());
        let old_data_block = old_array.data_block(old_index);
        while entry_ptr.move_to_next(&old_writer, sendable_guard.guard()) {
            let old_entry = entry_ptr.get(old_data_block);
            let (new_index, partial_hash) = if old_array.len() >= current_array.len() {
                debug_assert_eq!(
                    current_array.calculate_bucket_index(self.hash(&old_entry.0)),
                    target_index
                );
                (target_index, entry_ptr.partial_hash(&*old_writer))
            } else {
                let hash = self.hash(&old_entry.0);
                let new_index = current_array.calculate_bucket_index(hash);
                debug_assert!(new_index - target_index < (current_array.len() / old_array.len()));
                let partial_hash = Self::partial_hash(hash);
                (new_index, partial_hash)
            };

            while max_index <= new_index - target_index {
                let target_bucket = current_array.bucket(max_index + target_index);
                let writer = unsafe {
                    Writer::lock_async(target_bucket, sendable_guard)
                        .await
                        .unwrap_unchecked()
                };
                target_buckets[max_index].replace(writer);
                max_index += 1;
            }

            let target_bucket = unsafe {
                target_buckets[new_index - target_index]
                    .as_mut()
                    .unwrap_unchecked()
            };

            target_bucket.extract_from(
                current_array.data_block(new_index),
                partial_hash,
                &old_writer,
                old_data_block,
                &mut entry_ptr,
                sendable_guard.guard(),
            );
        }
        old_writer.kill();
    }

    /// Relocates the bucket to the current bucket array.
    ///
    /// Returns `false` if locking failed.
    fn relocate_bucket_sync<'g, const TRY_LOCK: bool>(
        &self,
        current_array: &'g BucketArray<K, V, L, TYPE>,
        old_array: &'g BucketArray<K, V, L, TYPE>,
        old_index: usize,
        old_writer: Writer<'g, K, V, L, TYPE>,
        guard: &'g Guard,
    ) -> bool {
        if old_writer.len() == 0 {
            old_writer.kill();
            return true;
        }

        let target_index =
            Self::from_index_to_range(old_array.len(), current_array.len(), old_index).0;
        let mut target_buckets: [Option<Writer<K, V, L, TYPE>>; MAX_RESIZE_FACTOR] =
            Default::default();
        let mut max_index = 0;
        let mut entry_ptr = EntryPtr::new(guard);
        let old_data_block = old_array.data_block(old_index);
        while entry_ptr.move_to_next(&old_writer, guard) {
            let old_entry = entry_ptr.get(old_data_block);
            let (new_index, partial_hash) = if old_array.len() >= current_array.len() {
                debug_assert_eq!(
                    current_array.calculate_bucket_index(self.hash(&old_entry.0)),
                    target_index
                );
                (target_index, entry_ptr.partial_hash(&*old_writer))
            } else {
                let hash = self.hash(&old_entry.0);
                let new_index = current_array.calculate_bucket_index(hash);
                debug_assert!(new_index - target_index < (current_array.len() / old_array.len()));
                let partial_hash = Self::partial_hash(hash);
                (new_index, partial_hash)
            };

            while max_index <= new_index - target_index {
                let target_bucket = current_array.bucket(max_index + target_index);
                let writer = if TRY_LOCK {
                    let Ok(writer) = Writer::try_lock(target_bucket) else {
                        return false;
                    };
                    writer
                } else {
                    Writer::lock_sync(target_bucket)
                };
                target_buckets[max_index].replace(unsafe { writer.unwrap_unchecked() });
                max_index += 1;
            }

            let target_bucket = unsafe {
                target_buckets[new_index - target_index]
                    .as_mut()
                    .unwrap_unchecked()
            };

            target_bucket.extract_from(
                current_array.data_block(new_index),
                partial_hash,
                &old_writer,
                old_data_block,
                &mut entry_ptr,
                guard,
            );
        }
        old_writer.kill();
        true
    }

    /// Starts incremental rehashing.
    #[inline]
    fn start_incremental_rehash(&self, old_array: &BucketArray<K, V, L, TYPE>) -> Option<usize> {
        // Assign itself a range of `Bucket` instances to rehash.
        //
        // Aside from the range, it increments the implicit reference counting field in
        // `old_array.rehashing`.
        let rehashing_metadata = old_array.rehashing_metadata();
        let mut current = rehashing_metadata.load(Relaxed);
        loop {
            if current >= old_array.len() || (current & (BUCKET_LEN - 1)) == BUCKET_LEN - 1 {
                // Only `BUCKET_LEN` threads are allowed to rehash a `Bucket` at a moment.
                return None;
            }
            match rehashing_metadata.compare_exchange_weak(
                current,
                current + BUCKET_LEN + 1,
                Acquire,
                Relaxed,
            ) {
                Ok(_) => {
                    current &= !(BUCKET_LEN - 1);
                    return Some(current);
                }
                Err(result) => current = result,
            }
        }
    }

    /// Ends incremental rehashing.
    #[inline]
    fn end_incremental_rehash(
        &self,
        current_array: &BucketArray<K, V, L, TYPE>,
        old_array: &BucketArray<K, V, L, TYPE>,
        prev: usize,
        success: bool,
    ) {
        let rehashing_metadata = old_array.rehashing_metadata();
        if success {
            // Keep the index as it is.
            let old_array_len = old_array.len();
            let current = rehashing_metadata.fetch_sub(1, Release) - 1;
            if (current & (BUCKET_LEN - 1) == 0) && current >= old_array_len {
                // The last one trying to relocate old entries gets rid of the old array.
                current_array.drop_old_array();
            }
        } else {
            // On failure, `rehashing` reverts to its previous state.
            let mut current = rehashing_metadata.load(Relaxed);
            loop {
                let new = if current <= prev {
                    current - 1
                } else {
                    let ref_cnt = current & (BUCKET_LEN - 1);
                    prev | (ref_cnt - 1)
                };
                match rehashing_metadata.compare_exchange_weak(current, new, Release, Relaxed) {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }
        }
    }

    /// Relocates a fixed number of buckets from the old bucket array to the current array
    /// asynchronously.
    ///
    /// Once this methods successfully started rehashing, there is no possibility that the bucket
    /// array is deallocated.
    async fn incremental_rehash_async<'g>(
        &self,
        current_array: &'g BucketArray<K, V, L, TYPE>,
        sendable_guard: &'g SendableGuard,
    ) {
        if let Some(old_array) = sendable_guard.load(current_array.old_array_ptr(), Acquire) {
            let Some(current) = self.start_incremental_rehash(old_array) else {
                return;
            };

            let mut rehashing_guard = ExitGuard::new((current, false), |(prev, success)| {
                self.end_incremental_rehash(current_array, old_array, prev, success);
            });

            for index in current..(current + BUCKET_LEN).min(old_array.len()) {
                let old_bucket = old_array.bucket(index);
                let writer = Writer::lock_async(old_bucket, sendable_guard).await;
                if let Some(writer) = writer {
                    self.relocate_bucket_async(
                        current_array,
                        old_array,
                        index,
                        writer,
                        sendable_guard,
                    )
                    .await;
                }
                debug_assert!(current_array.has_old_array());
            }

            rehashing_guard.1 = true;
        }
    }

    /// Relocates a fixed number of buckets from the old array to the current array synchronously.
    ///
    /// Returns `true` if `old_array` is null.
    fn incremental_rehash_sync<'g, const TRY_LOCK: bool>(
        &self,
        current_array: &'g BucketArray<K, V, L, TYPE>,
        guard: &'g Guard,
    ) -> bool {
        if let Some(old_array) = current_array.old_array(guard).as_ref() {
            let Some(current) = self.start_incremental_rehash(old_array) else {
                return !current_array.has_old_array();
            };

            let mut rehashing_guard = ExitGuard::new((current, false), |(prev, success)| {
                self.end_incremental_rehash(current_array, old_array, prev, success);
            });

            for index in current..(current + BUCKET_LEN).min(old_array.len()) {
                let old_bucket = old_array.bucket(index);
                let writer = if TRY_LOCK {
                    let Ok(writer) = Writer::try_lock(old_bucket) else {
                        return false;
                    };
                    writer
                } else {
                    Writer::lock_sync(old_bucket)
                };
                if let Some(writer) = writer {
                    if !self.relocate_bucket_sync::<TRY_LOCK>(
                        current_array,
                        old_array,
                        index,
                        writer,
                        guard,
                    ) {
                        return false;
                    }
                }
            }

            rehashing_guard.1 = true;
        }
        !current_array.has_old_array()
    }

    /// Tries to enlarge [`HashTable`] if the estimated load factor is greater than `7/8`.
    fn try_enlarge(
        &self,
        current_array: &BucketArray<K, V, L, TYPE>,
        index: usize,
        mut num_entries: usize,
        guard: &Guard,
    ) {
        if current_array.has_old_array() {
            return;
        }

        let sample_size = current_array.sample_size();
        let threshold = sample_size * (BUCKET_LEN / 8) * 7;
        if num_entries > threshold
            || (1..sample_size).any(|i| {
                num_entries += current_array
                    .bucket((index + i) % current_array.len())
                    .len();
                num_entries > threshold
            })
        {
            self.try_resize(current_array, index, guard);
        }
    }

    /// Tries to shrink the [`HashTable`] to fit the estimated number of entries, or rebuild it to
    /// optimize the storage.
    fn try_shrink_or_rebuild(
        &self,
        current_array: &BucketArray<K, V, L, TYPE>,
        index: usize,
        guard: &Guard,
    ) {
        if current_array.has_old_array() {
            return;
        }

        if current_array.num_slots() > self.minimum_capacity().load(Relaxed).next_power_of_two()
            || TYPE == OPTIMISTIC
        {
            let sample_size = current_array.sample_size();
            let shrink_threshold = sample_size * BUCKET_LEN / 16;
            let rebuild_threshold = sample_size / 2;
            let mut num_entries = 0;
            let mut num_buckets_to_rebuild = 0;
            for i in 0..sample_size {
                let bucket = current_array.bucket((index + i) % current_array.len());
                num_entries += bucket.len();
                if num_entries > shrink_threshold
                    && (TYPE != OPTIMISTIC
                        || num_buckets_to_rebuild + (sample_size - i) < rebuild_threshold)
                {
                    // Early exit.
                    return;
                }
                if TYPE == OPTIMISTIC && bucket.need_rebuild() {
                    if num_buckets_to_rebuild >= rebuild_threshold {
                        self.try_resize(current_array, index, guard);
                        return;
                    }
                    num_buckets_to_rebuild += 1;
                }
            }
            if TYPE != OPTIMISTIC || num_entries <= shrink_threshold {
                self.try_resize(current_array, index, guard);
            }
        }
    }

    /// Tries to resize the array.
    fn try_resize(
        &self,
        sampled_array: &BucketArray<K, V, L, TYPE>,
        sampling_index: usize,
        guard: &Guard,
    ) {
        let current_array_ptr = self.bucket_array().load(Acquire, guard);
        if current_array_ptr.tag() != Tag::None {
            // Another thread is currently allocating a new bucket array.
            return;
        }
        let Some(current_array) = current_array_ptr.as_ref() else {
            // The hash table is empty.
            return;
        };
        if !ptr::eq(current_array, sampled_array) {
            // The preliminary sampling result cannot be trusted anymore.
            return;
        }
        debug_assert!(!current_array.has_old_array());

        // If the estimated load factor is greater than `13/16`, then the hash table grows up to
        // `usize::BITS / 2` times larger. Oh the other hand, if The estimated load factor is less
        // than `1/8`, then the hash table shrinks to fit.
        let minimum_capacity = self.minimum_capacity().load(Relaxed);
        let capacity = current_array.num_slots();
        let sample_size = current_array.full_sample_size();
        let estimated_num_entries = Self::sample(current_array, sampling_index, sample_size);
        let new_capacity = if estimated_num_entries > (capacity / 16) * 13 {
            if capacity == self.maximum_capacity() {
                // Do not resize if the capacity cannot be increased.
                capacity
            } else {
                let mut new_capacity = capacity;
                while new_capacity <= (estimated_num_entries / 8) * 15 {
                    // Double `new_capacity` until the expected load factor is below 0.5.
                    if new_capacity == self.maximum_capacity() {
                        break;
                    }
                    if new_capacity / capacity == MAX_RESIZE_FACTOR {
                        break;
                    }
                    new_capacity *= 2;
                }
                new_capacity
            }
        } else if estimated_num_entries < capacity / 8 {
            // Shrink to fit.
            estimated_num_entries
                .max(minimum_capacity)
                .max(BucketArray::<K, V, L, TYPE>::minimum_capacity())
                .next_power_of_two()
        } else {
            capacity
        };

        let try_resize = new_capacity != capacity;
        let try_drop_table = estimated_num_entries == 0 && minimum_capacity == 0;
        let try_rebuild = TYPE == OPTIMISTIC
            && !try_resize
            && Self::check_rebuild(current_array, sampling_index, sample_size);

        if !try_resize && !try_drop_table && !try_rebuild {
            // Nothing to do.
            return;
        }

        // Mark that the thread may allocate a new array to prevent multiple threads from
        // allocating bucket arrays at the same time.
        if !self.bucket_array().update_tag_if(
            Tag::First,
            |ptr| ptr == current_array_ptr,
            Relaxed,
            Relaxed,
        ) {
            // The bucket array is being replaced with a new one.
            return;
        }

        if try_drop_table {
            // Try to drop the hash table with all the buckets locked.
            let mut writer_guard = ExitGuard::new((0, false), |(len, success): (usize, bool)| {
                for i in 0..len {
                    let writer = Writer::from_bucket(current_array.bucket(i));
                    if success {
                        writer.kill();
                    }
                }
            });

            if !(0..current_array.len()).any(|i| {
                if let Ok(Some(writer)) = Writer::try_lock(current_array.bucket(i)) {
                    if writer.len() == 0 {
                        // The bucket will be unlocked later.
                        writer_guard.0 = i + 1;
                        std::mem::forget(writer);
                        return false;
                    }
                }
                true
            }) {
                // All the buckets are empty and locked.
                writer_guard.1 = true;
                self.bucket_array().swap((None, Tag::None), Release);
                return;
            }
        }

        let allocated_array: Option<Shared<BucketArray<K, V, L, TYPE>>> = None;
        let mut mutex_guard = ExitGuard::new(allocated_array, |allocated_array| {
            if let Some(allocated_array) = allocated_array {
                // A new array was allocated.
                self.bucket_array()
                    .swap((Some(allocated_array), Tag::None), Release);
            } else {
                // Release the lock.
                self.bucket_array()
                    .update_tag_if(Tag::None, |_| true, Release, Relaxed);
            }
        });
        if try_resize || try_rebuild {
            mutex_guard.replace(unsafe {
                Shared::new_unchecked(BucketArray::<K, V, L, TYPE>::new(
                    new_capacity,
                    self.bucket_array().clone(Relaxed, guard),
                ))
            });
        }
    }

    /// Entries may have been removed during iteration.
    #[inline]
    fn entry_removed(&self, index: usize, guard: &Guard) {
        if let Some(current_array) = self.bucket_array().load(Acquire, guard).as_ref() {
            if current_array.len() > index && current_array.bucket(index).len() == 0 {
                self.try_shrink_or_rebuild(current_array, index, guard);
            }
        }
    }
    // Returns an estimated required size of the container based on the size hint.
    #[inline]
    fn capacity_from_size_hint(size_hint: (usize, Option<usize>)) -> usize {
        // A resize can be triggered when the load factor reaches ~80%.
        (size_hint
            .1
            .unwrap_or(size_hint.0)
            .min(1_usize << (usize::BITS - 2))
            / 4)
            * 5
    }

    /// Returns a reference to the specified [`Guard`] whose lifetime matches that of `self`.
    #[inline]
    fn prolonged_guard_ref<'h>(&'h self, guard: &Guard) -> &'h Guard {
        let _: &Self = self;
        unsafe { std::mem::transmute::<&Guard, &'h Guard>(guard) }
    }
}

/// [`LockedEntry`] comprises pieces of data that are required for exclusive access to an entry.
pub(super) struct LockedEntry<'h, K, V, L: LruList, const TYPE: char> {
    /// The [`Writer`] holding an exclusive lock on the bucket.
    pub(super) writer: Writer<'h, K, V, L, TYPE>,
    /// The [`DataBlock`] that may contain desired entry data.
    pub(super) data_block: &'h DataBlock<K, V, BUCKET_LEN>,
    /// [`EntryPtr`] pointing to the actual entry in the bucket.
    pub(super) entry_ptr: EntryPtr<'h, K, V, TYPE>,
    /// The index in the bucket array.
    pub(super) index: usize,
    /// The length of the bucket array.
    pub(super) len: usize,
}

impl<'h, K: Eq + Hash + 'h, V: 'h, L: LruList, const TYPE: char> LockedEntry<'h, K, V, L, TYPE> {
    /// Creates a new [`LockedEntry`].
    #[inline]
    pub(super) fn new(
        writer: Writer<'h, K, V, L, TYPE>,
        data_block: &'h DataBlock<K, V, BUCKET_LEN>,
        entry_ptr: EntryPtr<'h, K, V, TYPE>,
        index: usize,
        len: usize,
        guard: &Guard,
    ) -> LockedEntry<'h, K, V, L, TYPE> {
        if TYPE == OPTIMISTIC {
            writer.drop_removed_unreachable_entries(data_block, guard);
        }
        LockedEntry {
            writer,
            data_block,
            entry_ptr,
            index,
            len,
        }
    }

    /// Prolongs its lifetime.
    #[inline]
    pub(super) fn prolong_lifetime<T>(self, _ref: &T) -> LockedEntry<'_, K, V, L, TYPE> {
        unsafe { std::mem::transmute::<_, _>(self) }
    }

    /// Returns a [`LockedEntry`] owning the next entry.
    #[inline]
    pub(super) async fn next_async<H: BuildHasher, T: HashTable<K, V, H, L, TYPE>>(
        mut self,
        hash_table: &'h T,
    ) -> Option<LockedEntry<'h, K, V, L, TYPE>> {
        if self
            .entry_ptr
            .move_to_next(&self.writer, hash_table.prolonged_guard_ref(&Guard::new()))
        {
            return Some(self);
        }

        let try_shrink = self.writer.len() == 0;
        let sendable_guard = SendableGuard::default();
        let next_index = self.index + 1;
        let len = self.len;
        drop(self);

        if try_shrink {
            hash_table.entry_removed(next_index - 1, sendable_guard.guard());
        }

        if next_index == len {
            return None;
        }

        let mut next_entry = None;
        hash_table
            .for_each_writer_async_with(
                next_index,
                len,
                &sendable_guard,
                |writer, data_block, index, len| {
                    let guard = sendable_guard.guard();
                    let mut entry_ptr = EntryPtr::new(guard);
                    if entry_ptr.move_to_next(&writer, guard) {
                        let locked_entry =
                            LockedEntry::new(writer, data_block, entry_ptr, index, len, guard)
                                .prolong_lifetime(hash_table);
                        next_entry = Some(locked_entry);
                        return (true, false);
                    }
                    (false, false)
                },
            )
            .await;

        next_entry
    }

    /// Returns a [`LockedEntry`] owning the next entry.
    #[inline]
    pub(super) fn next_sync<H: BuildHasher, T: HashTable<K, V, H, L, TYPE>>(
        mut self,
        hash_table: &'h T,
    ) -> Option<Self> {
        if self
            .entry_ptr
            .move_to_next(&self.writer, hash_table.prolonged_guard_ref(&Guard::new()))
        {
            return Some(self);
        }

        let try_shrink = self.writer.len() == 0;
        let guard = Guard::new();
        let next_index = self.index + 1;
        let len = self.len;
        drop(self);

        if try_shrink {
            hash_table.entry_removed(next_index - 1, &guard);
        }

        if next_index == len {
            return None;
        }

        let mut next_entry = None;
        hash_table.for_each_writer_sync_with(
            next_index,
            len,
            &guard,
            |writer, data_block, index, len| {
                let mut entry_ptr = EntryPtr::new(&guard);
                if entry_ptr.move_to_next(&writer, &guard) {
                    let locked_entry =
                        LockedEntry::new(writer, data_block, entry_ptr, index, len, &guard)
                            .prolong_lifetime(hash_table);
                    next_entry = Some(locked_entry);
                    return (true, false);
                }
                (false, false)
            },
        );

        next_entry
    }
}

/// [`LockedEntry`] is safe to be sent across threads and awaits as long as the entry is.
unsafe impl<K: Eq + Hash + Send, V: Send, L: LruList, const TYPE: char> Send
    for LockedEntry<'_, K, V, L, TYPE>
{
}

/// [`LockedEntry`] is safe to be shared with other threads.
unsafe impl<K: Eq + Hash + Send + Sync, V: Send + Sync, L: LruList, const TYPE: char> Sync
    for LockedEntry<'_, K, V, L, TYPE>
{
}
