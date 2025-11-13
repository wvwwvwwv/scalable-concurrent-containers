pub mod bucket;
pub mod bucket_array;

use std::hash::{BuildHasher, Hash};
use std::mem::forget;
use std::ops::Deref;
use std::pin::Pin;
use std::ptr::{self, NonNull, from_ref};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

use bucket::{BUCKET_LEN, CACHE, DataBlock, EntryPtr, INDEX, LruList, Reader, Writer};
use bucket_array::BucketArray;
use sdd::{AtomicShared, Guard, Ptr, Shared, Tag};

use super::Equivalent;
use super::exit_guard::ExitGuard;
use crate::async_helper::AsyncGuard;
use crate::hash_table::bucket::Bucket;

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

    /// Returns a reference to its [`BuildHasher`].
    fn hasher(&self) -> &H;

    /// Returns a reference to the [`BucketArray`] pointer.
    fn bucket_array(&self) -> &AtomicShared<BucketArray<K, V, L, TYPE>>;

    /// Passes the bucket array to the garbage collector associated with the hash table type.
    #[inline]
    fn defer_reclaim(&self, bucket_array: Shared<BucketArray<K, V, L, TYPE>>, _guard: &Guard) {
        drop(bucket_array);
    }

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
    fn maximum_capacity(&self) -> usize {
        1_usize << (usize::BITS - 1)
    }

    /// Reserves the specified capacity.
    ///
    /// Returns the actually allocated capacity. Return `0` if the sum of the current minimum
    /// capacity and the additional capacity exceeds [`Self::maximum_capacity`].
    fn reserve_capacity(&self, additional_capacity: usize) -> usize {
        let mut current_minimum_capacity = self.minimum_capacity().load(Relaxed);
        loop {
            if additional_capacity > self.maximum_capacity() - current_minimum_capacity {
                return 0;
            }
            match self.minimum_capacity().compare_exchange_weak(
                current_minimum_capacity,
                additional_capacity + current_minimum_capacity,
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
    /// Allocates a new one if no bucket array has been allocated.
    #[inline]
    fn get_or_create_bucket_array<'g>(&self, guard: &'g Guard) -> &'g BucketArray<K, V, L, TYPE> {
        if let Some(current_array) = self.bucket_array().load(Acquire, guard).as_ref() {
            current_array
        } else {
            self.allocate_bucket_array(guard)
        }
    }

    /// Allocates a new bucket array.
    fn allocate_bucket_array<'g>(&self, guard: &'g Guard) -> &'g BucketArray<K, V, L, TYPE> {
        unsafe {
            let capacity = self.minimum_capacity().load(Relaxed);
            let allocated = Shared::new_unchecked(BucketArray::new(capacity, AtomicShared::null()));
            match self.bucket_array().compare_exchange(
                Ptr::null(),
                (Some(allocated), Tag::None),
                AcqRel,
                Acquire,
                guard,
            ) {
                Ok((_, ptr)) | Err((_, ptr)) => ptr.as_ref().unwrap_unchecked(),
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
    ///
    /// In case there are more than `usize::MAX` entries, it returns `usize::MAX`.
    fn num_entries(&self, guard: &Guard) -> usize {
        let mut num_entries: usize = 0;
        if let Some(current_array) = self.bucket_array().load(Acquire, guard).as_ref() {
            let old_array_ptr = current_array.old_array(guard);
            if let Some(old_array) = old_array_ptr.as_ref() {
                if !self.incremental_rehash_sync::<true>(current_array, guard) {
                    for i in 0..old_array.len() {
                        num_entries = num_entries.saturating_add(old_array.bucket(i).len());
                    }
                }
            }
            for i in 0..current_array.len() {
                num_entries = num_entries.saturating_add(current_array.bucket(i).len());
            }
            if old_array_ptr.is_null()
                && num_entries == 0
                && self.minimum_capacity().load(Relaxed) == 0
            {
                self.try_resize(current_array, 0, guard);
            }
        }
        num_entries
    }

    /// Returns `true` if a valid entry is found.
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
            if old_array_ptr.is_null() && self.minimum_capacity().load(Relaxed) == 0 {
                self.try_resize(current_array, 0, guard);
            }
        }
        false
    }

    /// Estimates the number of entries by sampling the specified number of buckets.
    ///
    /// The number of entries is overestimated if buckets contain overflow buckets.
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
        debug_assert_eq!(TYPE, INDEX);

        let mut current_array_ptr = self.bucket_array().load(Acquire, guard);
        while let Some(current_array) = current_array_ptr.as_ref() {
            let index = current_array.calculate_bucket_index(hash);
            if let Some(old_array) = current_array.old_array(guard).as_ref() {
                if !self.incremental_rehash_sync::<true>(current_array, guard)
                    && !self.dedup_bucket_sync::<true>(current_array, old_array, index, guard)
                {
                    let index = old_array.calculate_bucket_index(hash);
                    if let Some(entry) = old_array.bucket(index).search_entry(
                        old_array.data_block(index),
                        key,
                        hash,
                        guard,
                    ) {
                        return Some(entry);
                    }
                }
            }

            if let Some(entry) = current_array.bucket(index).search_entry(
                current_array.data_block(index),
                key,
                hash,
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
    async fn reader_async<Q, R, F: FnOnce(&K, &V) -> R>(
        &self,
        key: &Q,
        hash: u64,
        f: F,
        async_guard: &AsyncGuard,
    ) -> Option<R>
    where
        Q: Equivalent<K> + Hash + ?Sized,
    {
        while let Some(current_array) = async_guard.load(self.bucket_array(), Acquire) {
            let index = current_array.calculate_bucket_index(hash);
            if current_array.has_old_array() {
                self.incremental_rehash_async(current_array, async_guard)
                    .await;
                if !self
                    .dedup_bucket_async(current_array, index, async_guard)
                    .await
                {
                    continue;
                }
            }

            let bucket = current_array.bucket(index);
            if let Some(reader) = Reader::try_lock(bucket) {
                // The compiler is not efficient enough to eliminate unnecessary await points even
                // though `try_lock` is called during `lock_async`; it is a manual optimization to
                // avoid awaiting a future.
                if let Some(entry) = reader.search_entry(
                    current_array.data_block(index),
                    key,
                    hash,
                    async_guard.guard(),
                ) {
                    return Some(f(&entry.0, &entry.1));
                }
                break;
            }
            if let Some(reader) = Reader::lock_async(bucket, async_guard).await {
                if let Some(entry) = reader.search_entry(
                    current_array.data_block(index),
                    key,
                    hash,
                    async_guard.guard(),
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
    fn reader_sync<Q, R, F: FnOnce(&K, &V) -> R>(
        &self,
        key: &Q,
        hash: u64,
        f: F,
        guard: &Guard,
    ) -> Option<R>
    where
        Q: Equivalent<K> + Hash + ?Sized,
    {
        while let Some(current_array) = self.bucket_array().load(Acquire, guard).as_ref() {
            let index = current_array.calculate_bucket_index(hash);
            if let Some(old_array) = current_array.old_array(guard).as_ref() {
                if !self.incremental_rehash_sync::<false>(current_array, guard) {
                    self.dedup_bucket_sync::<false>(current_array, old_array, index, guard);
                }
            }

            let bucket = current_array.bucket(index);
            if let Some(reader) = Reader::lock_sync(bucket) {
                if let Some(entry) =
                    reader.search_entry(current_array.data_block(index), key, hash, guard)
                {
                    return Some(f(&entry.0, &entry.1));
                }
                break;
            }
        }
        None
    }

    /// Returns a [`LockedBucket`] for writing an entry asynchronously.
    ///
    /// If the container is empty, a new bucket array is allocated.
    #[inline]
    async fn writer_async(
        &self,
        hash: u64,
        async_guard: &AsyncGuard,
    ) -> LockedBucket<K, V, L, TYPE> {
        loop {
            let current_array = self.get_or_create_bucket_array(async_guard.guard());
            let bucket_index = current_array.calculate_bucket_index(hash);
            if current_array.has_old_array() {
                self.incremental_rehash_async(current_array, async_guard)
                    .await;
                if !self
                    .dedup_bucket_async(current_array, bucket_index, async_guard)
                    .await
                {
                    continue;
                }
            }

            let bucket = current_array.bucket(bucket_index);
            if (TYPE != CACHE || current_array.num_slots() < self.maximum_capacity())
                && bucket.len() >= BUCKET_LEN - 1
                && current_array.initiate_sampling(hash)
            {
                self.try_enlarge(
                    current_array,
                    bucket_index,
                    bucket.len(),
                    async_guard.guard(),
                );
            }

            if let Ok(Some(writer)) = Writer::try_lock(bucket) {
                // The compiler is not efficient enough to eliminate unnecessary await points even
                // though `try_lock` is called during `lock_async`; it is a manual optimization to
                // avoid awaiting a future.
                return LockedBucket {
                    writer,
                    data_block: current_array.data_block(bucket_index),
                    bucket_index,
                    bucket_array: into_non_null(current_array),
                };
            }
            if let Some(writer) = Writer::lock_async(bucket, async_guard).await {
                return LockedBucket {
                    writer,
                    data_block: current_array.data_block(bucket_index),
                    bucket_index,
                    bucket_array: into_non_null(current_array),
                };
            }
        }
    }

    /// Returns a [`LockedBucket`] for writing an entry synchronously.
    ///
    /// If the container is empty, a new bucket array is allocated.
    #[inline]
    fn writer_sync(&self, hash: u64, guard: &Guard) -> LockedBucket<K, V, L, TYPE> {
        loop {
            let current_array = self.get_or_create_bucket_array(guard);
            let bucket_index = current_array.calculate_bucket_index(hash);
            if let Some(old_array) = current_array.old_array(guard).as_ref() {
                if !self.incremental_rehash_sync::<false>(current_array, guard) {
                    self.dedup_bucket_sync::<false>(current_array, old_array, bucket_index, guard);
                }
            }

            let bucket = current_array.bucket(bucket_index);
            if (TYPE != CACHE || current_array.num_slots() < self.maximum_capacity())
                && bucket.len() >= BUCKET_LEN - 1
                && current_array.initiate_sampling(hash)
            {
                self.try_enlarge(current_array, bucket_index, bucket.len(), guard);
            }

            if let Some(writer) = Writer::lock_sync(bucket) {
                return LockedBucket {
                    writer,
                    data_block: current_array.data_block(bucket_index),
                    bucket_index,
                    bucket_array: into_non_null(current_array),
                };
            }
        }
    }

    /// Returns a [`LockedBucket`] for writing an entry asynchronously.
    ///
    /// If the container is empty, `None` is returned.
    #[inline]
    async fn optional_writer_async(
        &self,
        hash: u64,
        async_guard: &AsyncGuard,
    ) -> Option<LockedBucket<K, V, L, TYPE>> {
        while let Some(current_array) = async_guard.load(self.bucket_array(), Acquire) {
            let bucket_index = current_array.calculate_bucket_index(hash);
            if current_array.has_old_array() {
                self.incremental_rehash_async(current_array, async_guard)
                    .await;
                if !self
                    .dedup_bucket_async(current_array, bucket_index, async_guard)
                    .await
                {
                    continue;
                }
            }

            let bucket = current_array.bucket(bucket_index);
            if let Ok(Some(writer)) = Writer::try_lock(bucket) {
                // The compiler is not efficient enough to eliminate unnecessary await points even
                // though `try_lock` is called during `lock_async`; it is a manual optimization to
                // avoid awaiting a future.
                return Some(LockedBucket {
                    writer,
                    data_block: current_array.data_block(bucket_index),
                    bucket_index,
                    bucket_array: into_non_null(current_array),
                });
            }
            if let Some(writer) = Writer::lock_async(bucket, async_guard).await {
                return Some(LockedBucket {
                    writer,
                    data_block: current_array.data_block(bucket_index),
                    bucket_index,
                    bucket_array: into_non_null(current_array),
                });
            }
        }
        None
    }

    /// Returns a [`LockedBucket`] for writing an entry synchronously.
    ///
    /// If the container is empty, `None` is returned.
    #[inline]
    fn optional_writer_sync(
        &self,
        hash: u64,
        guard: &Guard,
    ) -> Option<LockedBucket<K, V, L, TYPE>> {
        while let Some(current_array) = self.bucket_array().load(Acquire, guard).as_ref() {
            let bucket_index = current_array.calculate_bucket_index(hash);
            if let Some(old_array) = current_array.old_array(guard).as_ref() {
                if !self.incremental_rehash_sync::<false>(current_array, guard) {
                    self.dedup_bucket_sync::<false>(current_array, old_array, bucket_index, guard);
                }
            }

            let bucket = current_array.bucket(bucket_index);
            if let Some(writer) = Writer::lock_sync(bucket) {
                return Some(LockedBucket {
                    writer,
                    data_block: current_array.data_block(bucket_index),
                    bucket_index,
                    bucket_array: into_non_null(current_array),
                });
            }
        }
        None
    }

    /// Iterates over all the buckets in the [`HashTable`] asynchronously.
    ///
    /// This method stops iterating when the closure returns `false`.
    #[inline]
    async fn for_each_reader_async<F>(&self, async_guard: &AsyncGuard, mut f: F)
    where
        F: FnMut(Reader<K, V, L, TYPE>, NonNull<DataBlock<K, V, BUCKET_LEN>>) -> bool,
    {
        let mut start_index = 0;
        let mut prev_len = 0;
        while let Some(current_array) = async_guard.load(self.bucket_array(), Acquire) {
            // In case the method is repeating the routine, iterate over entries from the middle of
            // the array.
            start_index = if prev_len == 0 || prev_len == current_array.len() {
                start_index
            } else {
                from_index_to_range(prev_len, current_array.len(), start_index).0
            };
            prev_len = current_array.len();

            while start_index < current_array.len() {
                let index = start_index;
                if current_array.has_old_array() {
                    self.incremental_rehash_async(current_array, async_guard)
                        .await;
                    if !self
                        .dedup_bucket_async(current_array, index, async_guard)
                        .await
                    {
                        // Retry the operation since there is a possibility that the current bucket
                        // array was replaced by a new one.
                        break;
                    }
                }

                let bucket = current_array.bucket(index);
                if let Some(reader) = Reader::lock_async(bucket, async_guard).await {
                    if !async_guard.check_ref(self.bucket_array(), current_array, Acquire) {
                        // `current_array` is no longer the current one.
                        break;
                    }
                    let data_block = current_array.data_block(index);
                    if !f(reader, data_block) {
                        return;
                    }
                } else {
                    // `current_array` is no longer the current one.
                    break;
                }

                start_index += 1;
            }

            if start_index == current_array.len() {
                break;
            }
        }
    }

    /// Iterates over all the buckets in the [`HashTable`] synchronously.
    ///
    /// This method stops iterating when the closure returns `false`.
    #[inline]
    fn for_each_reader_sync<F>(&self, guard: &Guard, mut f: F)
    where
        F: FnMut(Reader<K, V, L, TYPE>, NonNull<DataBlock<K, V, BUCKET_LEN>>) -> bool,
    {
        let mut start_index = 0;
        let mut prev_len = 0;
        while let Some(current_array) = self.bucket_array().load(Acquire, guard).as_ref() {
            // In case the method is repeating the routine, iterate over entries from the middle of
            // the array.
            start_index = if prev_len == 0 || prev_len == current_array.len() {
                start_index
            } else {
                from_index_to_range(prev_len, current_array.len(), start_index).0
            };
            prev_len = current_array.len();

            while start_index < current_array.len() {
                let index = start_index;
                if let Some(old_array) = current_array.old_array(guard).as_ref() {
                    if !self.incremental_rehash_sync::<false>(current_array, guard) {
                        self.dedup_bucket_sync::<false>(current_array, old_array, index, guard);
                    }
                }

                let bucket = current_array.bucket(index);
                if let Some(reader) = Reader::lock_sync(bucket) {
                    let data_block = current_array.data_block(index);
                    if !f(reader, data_block) {
                        return;
                    }
                } else {
                    // `current_array` is no longer the current one.
                    break;
                }
                start_index += 1;
            }

            if start_index == current_array.len() {
                break;
            }
        }
    }

    /// Iterates over all the buckets in the [`HashTable`].
    ///
    /// This method stops iterating when the closure returns `true`.
    #[inline]
    async fn for_each_writer_async<F>(
        &self,
        mut start_index: usize,
        expected_array_len: usize,
        async_guard: &AsyncGuard,
        mut f: F,
    ) where
        F: FnMut(LockedBucket<K, V, L, TYPE>, &mut bool) -> bool,
    {
        let mut prev_len = expected_array_len;
        let mut removed = false;
        while let Some(current_array) = async_guard.load(self.bucket_array(), Acquire) {
            // In case the method is repeating the routine, iterate over entries from the middle of
            // the array.
            let current_array_len = current_array.len();
            start_index = if prev_len == 0 || prev_len == current_array_len {
                start_index
            } else {
                from_index_to_range(prev_len, current_array_len, start_index).0
            };
            prev_len = current_array_len;

            while start_index < current_array_len {
                let bucket_index = start_index;
                if current_array.has_old_array() {
                    self.incremental_rehash_async(current_array, async_guard)
                        .await;
                    if !self
                        .dedup_bucket_async(current_array, bucket_index, async_guard)
                        .await
                    {
                        // Retry the operation since there is a possibility that the current bucket
                        // array was replaced by a new one.
                        break;
                    }
                }

                let bucket = current_array.bucket(bucket_index);
                if let Some(writer) = Writer::lock_async(bucket, async_guard).await {
                    if !async_guard.check_ref(self.bucket_array(), current_array, Acquire) {
                        // `current_array` is no longer the current one.
                        break;
                    }
                    let locked_bucket = LockedBucket {
                        writer,
                        data_block: current_array.data_block(bucket_index),
                        bucket_index,
                        bucket_array: into_non_null(current_array),
                    };
                    let stop = f(locked_bucket, &mut removed);
                    if stop {
                        // Stop iterating over buckets.
                        start_index = current_array_len;
                        break;
                    }
                } else {
                    // `current_array` is no longer the current one.
                    break;
                }
                start_index += 1;
            }

            if start_index == current_array_len {
                break;
            }
        }

        if removed {
            if let Some(current_array) = self
                .bucket_array()
                .load(Acquire, async_guard.guard())
                .as_ref()
            {
                self.try_shrink_or_rebuild(current_array, 0, async_guard.guard());
            }
        }
    }

    /// Iterates over all the buckets in the [`HashTable`].
    ///
    /// This methods stops iterating when the closure returns `true`.
    #[inline]
    fn for_each_writer_sync<F>(
        &self,
        mut start_index: usize,
        expected_array_len: usize,
        guard: &Guard,
        mut f: F,
    ) where
        F: FnMut(LockedBucket<K, V, L, TYPE>, &mut bool) -> bool,
    {
        let mut prev_len = expected_array_len;
        let mut removed = false;
        while let Some(current_array) = self.bucket_array().load(Acquire, guard).as_ref() {
            // In case the method is repeating the routine, iterate over entries from the middle of
            // the array.
            let current_array_len = current_array.len();
            start_index = if prev_len == 0 || prev_len == current_array_len {
                start_index
            } else {
                from_index_to_range(prev_len, current_array_len, start_index).0
            };
            prev_len = current_array_len;

            while start_index < current_array_len {
                let bucket_index = start_index;
                if let Some(old_array) = current_array.old_array(guard).as_ref() {
                    if !self.incremental_rehash_sync::<false>(current_array, guard) {
                        self.dedup_bucket_sync::<false>(
                            current_array,
                            old_array,
                            bucket_index,
                            guard,
                        );
                    }
                }

                let bucket = current_array.bucket(bucket_index);
                if let Some(writer) = Writer::lock_sync(bucket) {
                    let locked_bucket = LockedBucket {
                        writer,
                        data_block: current_array.data_block(bucket_index),
                        bucket_index,
                        bucket_array: into_non_null(current_array),
                    };
                    let stop = f(locked_bucket, &mut removed);
                    if stop {
                        // Stop iterating over buckets.
                        start_index = current_array_len;
                        break;
                    }
                } else {
                    // `current_array` is no longer the current one.
                    break;
                }
                start_index += 1;
            }

            if start_index == current_array_len {
                break;
            }
        }

        if removed {
            if let Some(current_array) = self.bucket_array().load(Acquire, guard).as_ref() {
                self.try_shrink_or_rebuild(current_array, 0, guard);
            }
        }
    }

    /// Tries to reserve a [`Bucket`] and returns a [`LockedBucket`].
    #[inline]
    fn try_reserve_bucket(&self, hash: u64, guard: &Guard) -> Option<LockedBucket<K, V, L, TYPE>> {
        loop {
            let current_array = self.get_or_create_bucket_array(guard);
            let bucket_index = current_array.calculate_bucket_index(hash);
            if let Some(old_array) = current_array.old_array(guard).as_ref() {
                if !self.incremental_rehash_sync::<true>(current_array, guard)
                    && !self.dedup_bucket_sync::<true>(
                        current_array,
                        old_array,
                        bucket_index,
                        guard,
                    )
                {
                    return None;
                }
            }

            let mut bucket = current_array.bucket(bucket_index);
            if (TYPE != CACHE || current_array.num_slots() < self.maximum_capacity())
                && bucket.len() >= BUCKET_LEN - 1
                && current_array.initiate_sampling(hash)
            {
                self.try_enlarge(current_array, bucket_index, bucket.len(), guard);
                bucket = current_array.bucket(bucket_index);
            }

            let Ok(writer) = Writer::try_lock(bucket) else {
                return None;
            };
            if let Some(writer) = writer {
                return Some(LockedBucket {
                    writer,
                    data_block: current_array.data_block(bucket_index),
                    bucket_index,
                    bucket_array: into_non_null(current_array),
                });
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
    /// bucket array of a different size has been allocated in the same memory region. To avoid
    /// this problem, the method returns `false` if it finds a killed bucket and the task was
    /// suspended.
    async fn dedup_bucket_async<'g>(
        &self,
        current_array: &'g BucketArray<K, V, L, TYPE>,
        index: usize,
        async_guard: &'g AsyncGuard,
    ) -> bool {
        if !async_guard.check_ref(self.bucket_array(), current_array, Acquire) {
            // A new bucket array was created in the meantime.
            return false;
        }

        if let Some(old_array) = async_guard.load(current_array.bucket_link(), Acquire) {
            let range = from_index_to_range(current_array.len(), old_array.len(), index);
            for old_index in range.0..range.1 {
                let bucket = old_array.bucket(old_index);
                let writer = Writer::lock_async(bucket, async_guard).await;
                if let Some(writer) = writer {
                    self.relocate_bucket_async(
                        current_array,
                        old_array,
                        old_index,
                        writer,
                        async_guard,
                    )
                    .await;
                } else if !async_guard.has_guard() {
                    // The bucket was killed and the guard has been invalidated. Validating the
                    // reference is not sufficient in this case since the current bucket array could
                    // have been replaced with a new one.
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
        let range = from_index_to_range(current_array.len(), old_array.len(), index);
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
        old_writer: Writer<K, V, L, TYPE>,
        async_guard: &'g AsyncGuard,
    ) {
        if old_writer.len() == 0 {
            // Instantiate a guard while the lock is held to ensure that the bucket arrays are not
            // dropped.
            async_guard.guard();
            old_writer.kill();
            return;
        }

        // Lock the target buckets.
        let (target_index, end_target_index) =
            from_index_to_range(old_array.len(), current_array.len(), old_index);
        for i in target_index..end_target_index {
            let writer = unsafe {
                Writer::lock_async(current_array.bucket(i), async_guard)
                    .await
                    .unwrap_unchecked()
            };
            forget(writer);
        }

        // It may seem inefficient to reevaluate the same values, but it is beneficial for reducing
        // the `Future` size.
        let Some(old_array) = current_array.old_array(async_guard.guard()).as_ref() else {
            return;
        };
        let (target_index, end_target_index) =
            from_index_to_range(old_array.len(), current_array.len(), old_index);
        let unlock = ExitGuard::new(
            (current_array, target_index, end_target_index),
            |(current_array, target_index, end_target_index)| {
                for i in target_index..end_target_index {
                    let writer = Writer::from_bucket(current_array.bucket(i));
                    drop(writer);
                }
            },
        );

        self.relocate_bucket(
            unlock.0,
            unlock.1,
            old_array,
            old_index,
            &old_writer,
            async_guard.guard(),
        );
        drop(unlock);

        // Instantiate a guard while the lock is held to ensure that the bucket arrays are not
        // dropped.
        async_guard.guard();
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
        old_writer: Writer<K, V, L, TYPE>,
        guard: &'g Guard,
    ) -> bool {
        if old_writer.len() == 0 {
            old_writer.kill();
            return true;
        }

        let (target_index, end_target_index) =
            from_index_to_range(old_array.len(), current_array.len(), old_index);

        // Lock the target buckets.
        for i in target_index..end_target_index {
            let writer = if TRY_LOCK {
                let Ok(Some(writer)) = Writer::try_lock(current_array.bucket(i)) else {
                    for j in target_index..i {
                        let writer = Writer::from_bucket(current_array.bucket(j));
                        drop(writer);
                    }
                    return false;
                };
                writer
            } else {
                unsafe { Writer::lock_sync(current_array.bucket(i)).unwrap_unchecked() }
            };
            forget(writer);
        }
        let unlock = ExitGuard::new((), |()| {
            for i in target_index..end_target_index {
                let writer = Writer::from_bucket(current_array.bucket(i));
                drop(writer);
            }
        });

        self.relocate_bucket(
            current_array,
            target_index,
            old_array,
            old_index,
            &old_writer,
            guard,
        );
        drop(unlock);

        old_writer.kill();
        true
    }

    /// Relocates entries from the old bucket to the corresponding buckets in the current bucket
    /// array.
    ///
    /// This assumes that all the target buckets are locked.
    fn relocate_bucket(
        &self,
        current_array: &BucketArray<K, V, L, TYPE>,
        target_index: usize,
        old_array: &BucketArray<K, V, L, TYPE>,
        old_index: usize,
        old_writer: &Writer<K, V, L, TYPE>,
        guard: &Guard,
    ) {
        // Need to pre-allocate slots if the container is shrinking or the old bucket overflows,
        // because incomplete relocation of entries may result in duplicate key problems.
        let pre_allocate_slots =
            old_array.len() > current_array.len() || old_writer.len() > BUCKET_LEN;
        let mut distribution = [0_u32; 8];
        let mut extended_distribution: Vec<u32> = Vec::new();
        let mut hash_data = [0_u64; BUCKET_LEN];
        let old_data_block = old_array.data_block(old_index);

        // Collect data for relocation.
        let mut entry_ptr = EntryPtr::new(guard);
        let mut position = 0;
        while entry_ptr.move_to_next(old_writer, guard) {
            let (offset, hash) = if old_array.len() >= current_array.len() {
                (0, u64::from(entry_ptr.partial_hash(&**old_writer)))
            } else {
                let hash = self.hash(&entry_ptr.get(old_data_block).0);
                let new_index = current_array.calculate_bucket_index(hash);
                debug_assert!(new_index - target_index < (current_array.len() / old_array.len()));
                (new_index - target_index, hash)
            };

            if pre_allocate_slots {
                if position != BUCKET_LEN {
                    hash_data[position] = hash;
                    position += 1;
                }
                if offset < 8 {
                    distribution[offset] += 1;
                } else {
                    if extended_distribution.len() < offset - 7 {
                        extended_distribution.resize(offset - 7, 0);
                    }
                    extended_distribution[offset - 8] += 1;
                }
            } else {
                let bucket = current_array.bucket(target_index + offset);
                bucket.extract_from(
                    current_array.data_block(target_index + offset),
                    hash,
                    old_writer,
                    old_data_block,
                    &mut entry_ptr,
                    guard,
                );
            }
        }

        if !pre_allocate_slots {
            return;
        }

        // Allocate memory.
        for (i, d) in distribution.iter().enumerate() {
            if *d != 0 {
                let bucket = current_array.bucket(target_index + i);
                bucket.reserve_slots((*d) as usize, guard);
            }
        }
        for (i, d) in extended_distribution.iter().enumerate() {
            if *d != 0 {
                let bucket = current_array.bucket(target_index + i + 8);
                bucket.reserve_slots((*d) as usize, guard);
            }
        }

        // Relocate entries; it is infallible.
        entry_ptr = EntryPtr::new(guard);
        position = 0;
        while entry_ptr.move_to_next(old_writer, guard) {
            let hash = if old_array.len() >= current_array.len() {
                u64::from(entry_ptr.partial_hash(&**old_writer))
            } else if position == BUCKET_LEN {
                self.hash(&entry_ptr.get(old_data_block).0)
            } else {
                position += 1;
                hash_data[position - 1]
            };
            let index = if old_array.len() >= current_array.len() {
                target_index
            } else {
                current_array.calculate_bucket_index(hash)
            };
            let bucket = current_array.bucket(index);
            bucket.extract_from(
                current_array.data_block(index),
                hash,
                old_writer,
                old_data_block,
                &mut entry_ptr,
                guard,
            );
        }
    }

    /// Starts incremental rehashing.
    #[inline]
    fn start_incremental_rehash(old_array: &BucketArray<K, V, L, TYPE>) -> Option<usize> {
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
        old_array: &BucketArray<K, V, L, TYPE>,
        prev: usize,
        success: bool,
    ) -> bool {
        let rehashing_metadata = old_array.rehashing_metadata();
        if success {
            // Keep the index as it is.
            let metadata = rehashing_metadata.fetch_sub(1, Release) - 1;
            (metadata & (BUCKET_LEN - 1) == 0) && metadata >= old_array.len()
        } else {
            // On failure, `rehashing` reverts to its previous state.
            let mut current = rehashing_metadata.load(Relaxed);
            loop {
                let new = if current <= prev {
                    current - 1
                } else {
                    let refs = current & (BUCKET_LEN - 1);
                    prev | (refs - 1)
                };
                match rehashing_metadata.compare_exchange_weak(current, new, Release, Relaxed) {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }
            false
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
        async_guard: &'g AsyncGuard,
    ) {
        if let Some(old_array) = async_guard.load(current_array.bucket_link(), Acquire) {
            if let Some(current) = Self::start_incremental_rehash(old_array) {
                let mut rehashing_guard =
                    ExitGuard::new((old_array, current), |(old_array, prev)| {
                        if prev != usize::MAX {
                            Self::end_incremental_rehash(old_array, prev, false);
                        }
                    });

                for bucket_index in
                    rehashing_guard.1..(rehashing_guard.1 + BUCKET_LEN).min(old_array.len())
                {
                    let old_bucket = rehashing_guard.0.bucket(bucket_index);
                    let writer = Writer::lock_async(old_bucket, async_guard).await;
                    if let Some(writer) = writer {
                        self.relocate_bucket_async(
                            current_array,
                            rehashing_guard.0,
                            bucket_index,
                            writer,
                            async_guard,
                        )
                        .await;
                    }
                    debug_assert!(current_array.has_old_array());
                }

                if Self::end_incremental_rehash(rehashing_guard.0, rehashing_guard.1, true) {
                    if let Some(bucket_array) = current_array
                        .bucket_link()
                        .swap((None, Tag::None), Release)
                        .0
                    {
                        self.defer_reclaim(bucket_array, async_guard.guard());
                    }
                }
                // `usize::MAX` indicates that the rehashing is complete.
                rehashing_guard.1 = usize::MAX;
            }
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
            if let Some(current) = Self::start_incremental_rehash(old_array) {
                let mut rehashing_guard =
                    ExitGuard::new((old_array, current), |(old_array, prev)| {
                        if prev != usize::MAX {
                            Self::end_incremental_rehash(old_array, prev, false);
                        }
                    });

                for bucket_index in
                    rehashing_guard.1..(rehashing_guard.1 + BUCKET_LEN).min(old_array.len())
                {
                    let old_bucket = rehashing_guard.0.bucket(bucket_index);
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
                            rehashing_guard.0,
                            bucket_index,
                            writer,
                            guard,
                        ) {
                            return false;
                        }
                    }
                }

                if Self::end_incremental_rehash(rehashing_guard.0, rehashing_guard.1, true) {
                    if let Some(bucket_array) = current_array
                        .bucket_link()
                        .swap((None, Tag::None), Release)
                        .0
                    {
                        self.defer_reclaim(bucket_array, guard);
                    }
                }
                // `usize::MAX` indicates that the rehashing is complete.
                rehashing_guard.1 = usize::MAX;
            }
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
        if !current_array.has_old_array() {
            // Try to grow if the estimated load factor is greater than `25/32`.
            let threshold = current_array.sample_size() * (BUCKET_LEN / 32) * 25;
            if num_entries > threshold
                || (1..current_array.sample_size()).any(|i| {
                    num_entries += current_array
                        .bucket((index + i) % current_array.len())
                        .len();
                    num_entries > threshold
                })
            {
                self.try_resize(current_array, index, guard);
            }
        }
    }

    /// Tries to shrink the [`HashTable`] to fit the estimated number of entries or rebuild it to
    /// optimize the storage.
    fn try_shrink_or_rebuild(
        &self,
        current_array: &BucketArray<K, V, L, TYPE>,
        index: usize,
        guard: &Guard,
    ) {
        if !current_array.has_old_array() {
            let minimum_capacity = self.minimum_capacity().load(Relaxed);
            if TYPE == INDEX || current_array.num_slots() > minimum_capacity {
                // Try to shrink if the estimated load factor is less than `1/8`.
                let shrink_threshold = current_array.sample_size() * BUCKET_LEN / 8;
                let rebuild_threshold = current_array.sample_size() / 2;
                let mut num_entries = 0;
                let mut num_buckets_to_rebuild = 0;
                for i in 0..current_array.sample_size() {
                    let bucket = current_array.bucket((index + i) % current_array.len());
                    num_entries += bucket.len();
                    if num_entries >= shrink_threshold
                        && (TYPE != INDEX
                            || num_buckets_to_rebuild + (current_array.sample_size() - i)
                                < rebuild_threshold)
                    {
                        // Early exit.
                        return;
                    }
                    if TYPE == INDEX && bucket.need_rebuild() {
                        num_buckets_to_rebuild += 1;
                        if num_buckets_to_rebuild > rebuild_threshold {
                            self.try_resize(current_array, index, guard);
                            return;
                        }
                    }
                }
                if TYPE != INDEX || num_entries <= shrink_threshold {
                    self.try_resize(current_array, index, guard);
                }
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

        let minimum_capacity = self.minimum_capacity().load(Relaxed);
        let capacity = current_array.num_slots();
        let sample_size = current_array.full_sample_size();
        let estimated_num_entries = Self::sample(current_array, sampling_index, sample_size);

        let new_capacity =
            if capacity < minimum_capacity || estimated_num_entries >= (capacity / 32) * 25 {
                // Double the capacity if the estimated load factor is equal to or greater than
                // `25/32`; `~7%` of buckets are expected to have overflow buckets.
                if capacity == self.maximum_capacity() {
                    // Do not resize if the capacity cannot be increased.
                    capacity
                } else {
                    // Double `new_capacity` until the expected load factor becomes `~0.4`.
                    let mut new_capacity = minimum_capacity.next_power_of_two().max(capacity);
                    while new_capacity / 2 < estimated_num_entries {
                        if new_capacity >= self.maximum_capacity() {
                            break;
                        }
                        new_capacity *= 2;
                    }
                    new_capacity
                }
            } else if estimated_num_entries <= capacity / 8 {
                // Shrink to fit if the estimated load factor is equal to or less than `1/8`.
                (estimated_num_entries * 2)
                    .max(minimum_capacity)
                    .max(BucketArray::<K, V, L, TYPE>::minimum_capacity())
                    .next_power_of_two()
            } else {
                capacity
            };

        let try_resize = new_capacity != capacity;
        let try_drop_table = estimated_num_entries == 0 && minimum_capacity == 0;
        let try_rebuild = TYPE == INDEX
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
            AcqRel,
            Relaxed,
        ) {
            // The bucket array is being replaced with a new one.
            return;
        }
        debug_assert!(!current_array.has_old_array());

        if try_drop_table {
            // Try to drop the hash table with all the buckets locked.
            let mut writer_guard = ExitGuard::new((0, false), |(len, success): (usize, bool)| {
                for i in 0..len {
                    let writer = Writer::from_bucket(current_array.bucket(i));
                    if success {
                        debug_assert_eq!(writer.len(), 0);
                        writer.kill();
                    }
                }
            });

            if !(0..current_array.len()).any(|i| {
                if let Ok(Some(writer)) = Writer::try_lock(current_array.bucket(i)) {
                    if writer.len() == 0 {
                        // The bucket will be unlocked later.
                        writer_guard.0 = i + 1;
                        forget(writer);
                        return false;
                    }
                }
                true
            }) {
                // All the buckets are empty and locked.
                writer_guard.1 = true;
                if let Some(bucket_array) = self.bucket_array().swap((None, Tag::None), Release).0 {
                    self.defer_reclaim(bucket_array, guard);
                }
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
                    (*self.bucket_array()).clone(Relaxed, guard),
                ))
            });
        }
    }

    /// Returns an estimated required size of the container based on the size hint.
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

/// [`LockedBucket`] has exclusive access to a [`Bucket`].
#[derive(Debug)]
pub(crate) struct LockedBucket<K, V, L: LruList, const TYPE: char> {
    /// Holds an exclusive lock on the [`Bucket`].
    pub writer: Writer<K, V, L, TYPE>,
    /// Corresponding [`DataBlock`].
    pub data_block: NonNull<DataBlock<K, V, BUCKET_LEN>>,
    /// The index of the [`Bucket`] within the [`BucketArray`].
    pub bucket_index: usize,
    /// Corresponding [`BucketArray`].
    ///
    /// The [`BucketArray`] is not dropped as long as it holds an exclusive lock on the [`Bucket`].
    pub bucket_array: NonNull<BucketArray<K, V, L, TYPE>>,
}

impl<K, V, L: LruList, const TYPE: char> LockedBucket<K, V, L, TYPE> {
    /// Returns a reference to the [`BucketArray`] that contains this [`LockedBucket`].
    #[inline]
    pub(crate) const fn bucket_array(&self) -> &BucketArray<K, V, L, TYPE> {
        unsafe { self.bucket_array.as_ref() }
    }

    /// Gets a mutable reference to the entry.
    #[inline]
    pub(crate) fn entry<'b, 'g: 'b>(
        &'b self,
        entry_ptr: &'b EntryPtr<'g, K, V, TYPE>,
    ) -> &'b (K, V) {
        entry_ptr.get(self.data_block)
    }

    /// Gets a mutable reference to the entry.
    #[inline]
    pub(crate) fn entry_mut<'b, 'g: 'b>(
        &'b mut self,
        entry_ptr: &'b mut EntryPtr<'g, K, V, TYPE>,
    ) -> &'b mut (K, V) {
        entry_ptr.get_mut(self.data_block, &self.writer)
    }

    /// Inserts a new entry with the supplied constructor function.
    #[inline]
    pub(crate) fn insert<'g>(
        &self,
        hash: u64,
        entry: (K, V),
        guard: &'g Guard,
    ) -> EntryPtr<'g, K, V, TYPE> {
        if TYPE == INDEX {
            self.writer
                .drop_removed_unreachable_entries(self.data_block, guard);
        }
        match self.writer.insert(self.data_block, hash, entry, guard) {
            Ok(entry_ptr) => entry_ptr,
            Err(entry) => self.writer.insert_overflow(hash, entry, guard),
        }
    }
}

impl<K: Eq + Hash, V, L: LruList, const TYPE: char> LockedBucket<K, V, L, TYPE> {
    /// Searches for an entry with the given key.
    #[inline]
    pub(crate) fn search<'g, Q>(
        &self,
        key: &Q,
        hash: u64,
        guard: &'g Guard,
    ) -> EntryPtr<'g, K, V, TYPE>
    where
        Q: Equivalent<K> + ?Sized,
    {
        (*self.writer).get_entry_ptr(self.data_block, key, hash, guard)
    }

    /// Removes the entry and tries to shrink the container.
    #[inline]
    pub(crate) fn remove<'g, H, T: HashTable<K, V, H, L, TYPE>>(
        self,
        hash_table: &T,
        entry_ptr: &mut EntryPtr<'g, K, V, TYPE>,
        guard: &'g Guard,
    ) -> (K, V)
    where
        H: BuildHasher,
    {
        let removed = self.writer.remove(self.data_block, entry_ptr, guard);
        self.try_shrink_or_rebuild(hash_table, guard);
        removed
    }

    /// Removes the entry and tries to shrink or rebuild the container.
    #[inline]
    pub(crate) fn mark_removed<'g, H, T: HashTable<K, V, H, L, TYPE>>(
        self,
        hash_table: &T,
        entry_ptr: &mut EntryPtr<'g, K, V, TYPE>,
        guard: &'g Guard,
    ) where
        H: BuildHasher,
    {
        self.writer.mark_removed(entry_ptr, guard);
        if TYPE == INDEX {
            self.writer
                .drop_removed_unreachable_entries(self.data_block, guard);
        }
        self.try_shrink_or_rebuild(hash_table, guard);
    }

    /// Tries to shrink or rebuild the container.
    pub(crate) fn try_shrink_or_rebuild<H, T: HashTable<K, V, H, L, TYPE>>(
        self,
        hash_table: &T,
        guard: &Guard,
    ) where
        H: BuildHasher,
    {
        if self.writer.need_rebuild() || self.writer.len() == 0 {
            if let Some(current_array) = hash_table.bucket_array().load(Acquire, guard).as_ref() {
                if ptr::eq(current_array, self.bucket_array()) {
                    let bucket_index = self.bucket_index;
                    drop(self);

                    // Tries to shrink or rebuild the container after unlocking the bucket.
                    hash_table.try_shrink_or_rebuild(current_array, bucket_index, guard);
                }
            }
        }
    }

    /// Returns a [`LockedBucket`] owning the next bucket asynchronously.
    #[inline]
    pub(super) async fn next_async<'h, H, T: HashTable<K, V, H, L, TYPE>>(
        self,
        hash_table: &'h T,
        entry_ptr: &mut EntryPtr<'h, K, V, TYPE>,
        async_guard: &mut Pin<&mut AsyncGuard>,
    ) -> Option<LockedBucket<K, V, L, TYPE>>
    where
        H: BuildHasher,
    {
        let prolonged_guard = hash_table.prolonged_guard_ref(async_guard.guard());
        if entry_ptr.move_to_next(&self.writer, prolonged_guard) {
            return Some(self);
        }

        let next_index = self.bucket_index + 1;
        let len = self.bucket_array().len();

        if self.writer.len() == 0 {
            self.try_shrink_or_rebuild(hash_table, prolonged_guard);
        } else {
            drop(self);
        }

        if next_index == len {
            return None;
        }

        let mut next_entry = None;
        hash_table
            .for_each_writer_async(next_index, len, async_guard, |locked_bucket, _| {
                let guard = hash_table.prolonged_guard_ref(async_guard.guard());
                *entry_ptr = EntryPtr::new(guard);
                if entry_ptr.move_to_next(&locked_bucket.writer, guard) {
                    next_entry = Some(locked_bucket);
                    return true;
                }
                false
            })
            .await;

        next_entry
    }

    /// Returns a [`LockedBucket`] owning the next bucket synchronously.
    #[inline]
    pub(super) fn next_sync<'h, H, T: HashTable<K, V, H, L, TYPE>>(
        self,
        hash_table: &'h T,
        entry_ptr: &mut EntryPtr<'h, K, V, TYPE>,
        guard: &'h Guard,
    ) -> Option<Self>
    where
        H: BuildHasher,
    {
        if entry_ptr.move_to_next(&self.writer, guard) {
            return Some(self);
        }

        let next_index = self.bucket_index + 1;
        let len = self.bucket_array().len();

        if self.writer.len() == 0 {
            self.try_shrink_or_rebuild(hash_table, guard);
        } else {
            drop(self);
        }

        if next_index == len {
            return None;
        }

        let mut next_entry = None;
        hash_table.for_each_writer_sync(next_index, len, guard, |locked_bucket, _| {
            *entry_ptr = EntryPtr::new(guard);
            if entry_ptr.move_to_next(&locked_bucket.writer, guard) {
                next_entry = Some(locked_bucket);
                return true;
            }
            false
        });

        next_entry
    }
}

impl<K, V, L: LruList, const TYPE: char> Deref for LockedBucket<K, V, L, TYPE> {
    type Target = Bucket<K, V, L, TYPE>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.writer
    }
}

unsafe impl<K: Send, V: Send, L: LruList, const TYPE: char> Send for LockedBucket<K, V, L, TYPE> {}
unsafe impl<K: Send + Sync, V: Send + Sync, L: LruList, const TYPE: char> Sync
    for LockedBucket<K, V, L, TYPE>
{
}

/// For the given index in the current array, calculate the respective range in the old array.
#[inline]
const fn from_index_to_range(from_len: usize, to_len: usize, from_index: usize) -> (usize, usize) {
    debug_assert!(from_len.is_power_of_two() && to_len.is_power_of_two());
    if from_len < to_len {
        let ratio = to_len / from_len;
        let start_index = from_index * ratio;
        debug_assert!(start_index + ratio <= to_len,);
        (start_index, start_index + ratio)
    } else {
        let ratio = from_len / to_len;
        let start_index = from_index / ratio;
        debug_assert!(start_index < to_len,);
        (start_index, start_index + 1)
    }
}

/// Turns a reference into a [`NonNull`] pointer.
const fn into_non_null<T: Sized>(t: &T) -> NonNull<T> {
    unsafe { NonNull::new_unchecked(from_ref(t).cast_mut()) }
}
