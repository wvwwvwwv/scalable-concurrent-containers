pub mod bucket;
pub mod bucket_array;

use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};
use crate::exit_guard::ExitGuard;
use crate::wait_queue::DeriveAsyncWait;
use bucket::{DataBlock, EntryPtr, Locker, Reader, BUCKET_LEN};
use bucket_array::BucketArray;
use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::atomic::{fence, AtomicUsize};

/// `HashTable` defines common functions for hash table implementations.
pub(super) trait HashTable<K, V, H, const LOCK_FREE: bool>
where
    K: Eq + Hash,
    H: BuildHasher,
{
    /// Returns the hash value of the key.
    #[inline]
    fn hash<Q>(&self, key: &Q) -> u64
    where
        K: Borrow<Q>,
        Q: Hash + ?Sized,
    {
        let mut h = self.hasher().build_hasher();
        key.hash(&mut h);
        h.finish()
    }

    /// Returns a reference to its [`BuildHasher`].
    fn hasher(&self) -> &H;

    /// Tries to clone the instances pointed by `entry`.
    ///
    /// It does not clone unless `LOCK_FREE` is `true` thus `K` and `V` both being `Clone`.
    fn try_clone(entry: &(K, V)) -> Option<(K, V)>;

    /// Returns a reference to the [`BucketArray`] pointer.
    fn bucket_array(&self) -> &AtomicArc<BucketArray<K, V, LOCK_FREE>>;

    /// Returns the minimum allowed capacity.
    fn minimum_capacity(&self) -> &AtomicUsize;

    /// Reserves the specified capacity.
    ///
    /// Returns the actually allocated capacity.
    #[inline]
    fn reserve_capacity(&self, additional_capacity: usize) -> usize {
        let mut current_minimum_capacity = self.minimum_capacity().load(Relaxed);
        loop {
            if usize::MAX - current_minimum_capacity <= additional_capacity {
                return 0;
            }
            match self.minimum_capacity().compare_exchange(
                current_minimum_capacity,
                current_minimum_capacity + additional_capacity,
                Relaxed,
                Relaxed,
            ) {
                Ok(_) => {
                    self.try_resize(0, &Barrier::new());
                    return additional_capacity;
                }
                Err(actual) => current_minimum_capacity = actual,
            }
        }
    }

    /// Returns a reference to the current array.
    ///
    /// If no array has been allocated, it allocates a new one and returns it.
    #[inline]
    fn get_current_array<'b>(&self, barrier: &'b Barrier) -> &'b BucketArray<K, V, LOCK_FREE> {
        // An acquire fence is required to correctly load the contents of the array.
        let current_array_ptr = self.bucket_array().load(Acquire, barrier);
        if let Some(current_array) = current_array_ptr.as_ref() {
            return current_array;
        }

        unsafe {
            let current_array_ptr = match self.bucket_array().compare_exchange(
                Ptr::null(),
                (
                    Some(Arc::new_unchecked(BucketArray::<K, V, LOCK_FREE>::new(
                        self.minimum_capacity().load(Relaxed),
                        AtomicArc::null(),
                    ))),
                    Tag::None,
                ),
                AcqRel,
                Acquire,
                barrier,
            ) {
                Ok((_, ptr)) | Err((_, ptr)) => ptr,
            };
            current_array_ptr.as_ref().unwrap_unchecked()
        }
    }

    /// Returns the number of entries.
    #[inline]
    fn num_entries(&self, barrier: &Barrier) -> usize {
        let mut num_entries = 0;
        if let Some(current_array) = self.bucket_array().load(Acquire, barrier).as_ref() {
            let old_array_ptr = current_array.old_array(barrier);
            if let Some(old_array) = old_array_ptr.as_ref() {
                for i in 0..old_array.num_buckets() {
                    num_entries += old_array.bucket(i).num_entries();
                }
            }
            for i in 0..current_array.num_buckets() {
                num_entries += current_array.bucket(i).num_entries();
            }
            if num_entries == 0 && self.minimum_capacity().load(Relaxed) == 0 {
                self.try_resize(0, barrier);
            }
        }
        num_entries
    }

    /// Returns `true` if the number of entries is non-zero.
    #[inline]
    fn has_entry(&self, barrier: &Barrier) -> bool {
        if let Some(current_array) = self.bucket_array().load(Acquire, barrier).as_ref() {
            let old_array_ptr = current_array.old_array(barrier);
            if let Some(old_array) = old_array_ptr.as_ref() {
                for i in 0..old_array.num_buckets() {
                    if old_array.bucket(i).num_entries() != 0 {
                        return true;
                    }
                }
            }
            for i in 0..current_array.num_buckets() {
                if current_array.bucket(i).num_entries() != 0 {
                    return true;
                }
            }
            if self.minimum_capacity().load(Relaxed) == 0 {
                self.try_resize(0, barrier);
            }
        }
        false
    }

    /// Returns the number of slots.
    #[inline]
    fn num_slots(&self, barrier: &Barrier) -> usize {
        if let Some(current_array) = self.bucket_array().load(Acquire, barrier).as_ref() {
            current_array.num_entries()
        } else {
            0
        }
    }

    /// Estimates the number of entries by sampling the specified number of buckets.
    #[inline]
    fn sample(
        current_array: &BucketArray<K, V, LOCK_FREE>,
        sampling_index: usize,
        sample_size: usize,
    ) -> usize {
        let mut num_entries = 0;
        for i in sampling_index..(sampling_index + sample_size) {
            num_entries += current_array
                .bucket(i % current_array.num_buckets())
                .num_entries();
        }
        num_entries * (current_array.num_buckets() / sample_size)
    }

    /// Checks whether rebuilding the entire hash table is required.
    #[inline]
    fn check_rebuild(
        current_array: &BucketArray<K, V, LOCK_FREE>,
        sampling_index: usize,
        sample_size: usize,
    ) -> bool {
        let mut num_buckets_to_rebuild = 0;
        for i in sampling_index..(sampling_index + sample_size) {
            if current_array
                .bucket(i % current_array.num_buckets())
                .need_rebuild()
            {
                num_buckets_to_rebuild += 1;
                if num_buckets_to_rebuild >= sample_size / 2 {
                    return true;
                }
            }
        }
        false
    }

    /// Inserts an entry into the [`HashTable`].
    #[inline]
    fn insert_entry<D: DeriveAsyncWait>(
        &self,
        key: K,
        val: V,
        hash: u64,
        async_wait: &mut D,
        barrier: &Barrier,
    ) -> Result<Option<(K, V)>, (K, V)> {
        match self.acquire_entry(&key, hash, async_wait, barrier) {
            Ok((mut locker, data_block_mut, entry_ptr, _)) => {
                if entry_ptr.is_valid() {
                    return Ok(Some((key, val)));
                }
                locker.insert_with(
                    data_block_mut,
                    BucketArray::<K, V, LOCK_FREE>::partial_hash(hash),
                    || (key, val),
                    barrier,
                );
                Ok(None)
            }
            Err(_) => Err((key, val)),
        }
    }

    /// Reads an entry from the [`HashTable`].
    #[inline]
    fn read_entry<'b, Q, D>(
        &self,
        key: &Q,
        hash: u64,
        async_wait: &mut D,
        barrier: &'b Barrier,
    ) -> Result<Option<(&'b K, &'b V)>, ()>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        D: DeriveAsyncWait,
    {
        let mut current_array_ptr = self.bucket_array().load(Acquire, barrier);
        while let Some(current_array) = current_array_ptr.as_ref() {
            if let Some(old_array) = current_array.old_array(barrier).as_ref() {
                if LOCK_FREE {
                    if self.incremental_rehash::<Q, D, true>(current_array, async_wait, barrier)
                        != Ok(true)
                    {
                        let index = old_array.calculate_bucket_index(hash);
                        if let Some((k, v)) = old_array.bucket(index).search(
                            old_array.data_block(index),
                            key,
                            BucketArray::<K, V, LOCK_FREE>::partial_hash(hash),
                            barrier,
                        ) {
                            return Ok(Some((k, v)));
                        }
                    }
                } else {
                    self.move_entry(current_array, old_array, hash, async_wait, barrier)?;
                }
            };

            let index = current_array.calculate_bucket_index(hash);
            let bucket = current_array.bucket(index);
            if LOCK_FREE {
                if let Some(entry) = bucket.search(
                    current_array.data_block(index),
                    key,
                    BucketArray::<K, V, LOCK_FREE>::partial_hash(hash),
                    barrier,
                ) {
                    return Ok(Some((&entry.0, &entry.1)));
                }
            } else {
                let lock_result = if let Some(async_wait) = async_wait.derive() {
                    Reader::try_lock_or_wait(bucket, async_wait, barrier)?
                } else {
                    Reader::lock(bucket, barrier)
                };
                if let Some(reader) = lock_result {
                    if let Some((key, val)) = reader.search(
                        current_array.data_block(index),
                        key,
                        BucketArray::<K, V, LOCK_FREE>::partial_hash(hash),
                        barrier,
                    ) {
                        return Ok(Some((key, val)));
                    }
                }
            }

            let new_current_array_ptr = self.bucket_array().load(Acquire, barrier);
            if current_array_ptr == new_current_array_ptr {
                break;
            }

            // A new array has been allocated.
            current_array_ptr = new_current_array_ptr;
        }

        Ok(None)
    }

    /// Removes an entry if the condition is met.
    #[inline]
    fn remove_entry<Q, F: FnOnce(&mut V) -> bool, D, R, P: FnOnce(Option<Option<(K, V)>>) -> R>(
        &self,
        key: &Q,
        hash: u64,
        condition: F,
        post_processor: P,
        async_wait: &mut D,
        barrier: &Barrier,
    ) -> Result<R, F>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        D: DeriveAsyncWait,
    {
        while let Some(current_array) = self.bucket_array().load(Acquire, barrier).as_ref() {
            // The reasoning behind this loop can be found in `acquire_entry`.
            let shrinkable = if let Some(old_array) = current_array.old_array(barrier).as_ref() {
                match self.move_entry(current_array, old_array, hash, async_wait, barrier) {
                    Ok(r) => r,
                    Err(_) => return Err(condition),
                }
            } else {
                true
            };

            let index = current_array.calculate_bucket_index(hash);
            let bucket = current_array.bucket_mut(index);
            let lock_result = if let Some(async_wait) = async_wait.derive() {
                match Locker::try_lock_or_wait(bucket, async_wait, barrier) {
                    Ok(l) => l,
                    Err(_) => return Err(condition),
                }
            } else {
                Locker::lock(bucket, barrier)
            };
            if let Some(mut locker) = lock_result {
                let data_block_mut = current_array.data_block_mut(index);
                let mut entry_ptr = locker.get(
                    data_block_mut,
                    key,
                    BucketArray::<K, V, LOCK_FREE>::partial_hash(hash),
                    barrier,
                );
                if entry_ptr.is_valid()
                    && condition(&mut entry_ptr.get_mut(data_block_mut, &mut locker).1)
                {
                    let result = locker.erase(data_block_mut, &mut entry_ptr);
                    if shrinkable
                        && (locker.num_entries() <= 1 || locker.need_rebuild())
                        && current_array.within_sampling_range(index)
                    {
                        drop(locker);
                        self.try_shrink_or_rebuild(current_array, index, barrier);
                    }
                    return Ok(post_processor(Some(result)));
                }
                break;
            }
        }
        Ok(post_processor(None))
    }

    /// Acquires a [`Locker`] and [`EntryPtr`] corresponding to the key.
    ///
    /// It returns an error if locking failed, or returns an [`EntryPtr`] if the key exists,
    /// otherwise `None` is returned.
    #[allow(clippy::type_complexity)]
    #[inline]
    fn acquire_entry<'b, Q, D>(
        &self,
        key: &Q,
        hash: u64,
        async_wait: &mut D,
        barrier: &'b Barrier,
    ) -> Result<
        (
            Locker<'b, K, V, LOCK_FREE>,
            &'b mut DataBlock<K, V, BUCKET_LEN>,
            EntryPtr<'b, K, V, LOCK_FREE>,
            usize,
        ),
        (),
    >
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
        D: DeriveAsyncWait,
    {
        // It is guaranteed that the thread reads a consistent snapshot of the current and old
        // array pair by a release memory barrier in the resize function, hence the following
        // procedure is correct.
        //  - The thread reads `self.array`, and it kills the target bucket in the old array if
        //    there is one attached to it, and inserts the key into `self.array`.
        // There are two cases.
        //  1. The thread reads an old version of `self.array`.
        //    If there is another thread having read the latest version of `self.array`,
        //    trying to insert the same key, it will try to kill the bucket in the old version
        //    of `self.array`, thus competing with each other.
        //  2. The thread reads the latest version of `self.array`.
        //    If the array is deprecated while inserting the key, it falls into case 1.
        loop {
            let current_array = self.get_current_array(barrier);
            let resizable = if let Some(old_array) = current_array.old_array(barrier).as_ref() {
                self.move_entry(current_array, old_array, hash, async_wait, barrier)?;
                false
            } else {
                true
            };

            let index = current_array.calculate_bucket_index(hash);
            let bucket = current_array.bucket_mut(index);

            // Try to resize the array.
            if resizable
                && current_array.within_sampling_range(index)
                && bucket.num_entries() >= BUCKET_LEN - 1
            {
                self.try_enlarge(current_array, index, bucket.num_entries(), barrier);
            }

            let lock_result = if let Some(async_wait) = async_wait.derive() {
                Locker::try_lock_or_wait(bucket, async_wait, barrier)?
            } else {
                Locker::lock(bucket, barrier)
            };
            if let Some(locker) = lock_result {
                let data_block_mut = current_array.data_block_mut(index);
                let entry_ptr = locker.get(
                    data_block_mut,
                    key,
                    BucketArray::<K, V, LOCK_FREE>::partial_hash(hash),
                    barrier,
                );
                return Ok((locker, data_block_mut, entry_ptr, index));
            }

            // Reaching here means that `self.bucket_array()` has been updated.
        }
    }

    /// Moves an entry in the old array to the current one.
    ///
    /// Returns `true` if no old array is attached to the current one.
    #[inline]
    fn move_entry<Q, D>(
        &self,
        current_array: &BucketArray<K, V, LOCK_FREE>,
        old_array: &BucketArray<K, V, LOCK_FREE>,
        hash: u64,
        async_wait: &mut D,
        barrier: &Barrier,
    ) -> Result<bool, ()>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
        D: DeriveAsyncWait,
    {
        if !self.incremental_rehash::<Q, D, false>(current_array, async_wait, barrier)? {
            let index = old_array.calculate_bucket_index(hash);
            let bucket = old_array.bucket_mut(index);
            let lock_result = if let Some(async_wait) = async_wait.derive() {
                Locker::try_lock_or_wait(bucket, async_wait, barrier)?
            } else {
                Locker::lock(bucket, barrier)
            };
            if let Some(mut locker) = lock_result {
                self.relocate_bucket::<Q, _, false>(
                    current_array,
                    old_array,
                    index,
                    &mut locker,
                    async_wait,
                    barrier,
                )?;
            }
            return Ok(false);
        }
        Ok(true)
    }

    /// Relocates the bucket to the current bucket array.
    ///
    /// It returns an error if locking failed.
    #[inline]
    fn relocate_bucket<Q, D, const TRY_LOCK: bool>(
        &self,
        current_array: &BucketArray<K, V, LOCK_FREE>,
        old_array: &BucketArray<K, V, LOCK_FREE>,
        old_index: usize,
        old_locker: &mut Locker<K, V, LOCK_FREE>,
        async_wait: &mut D,
        barrier: &Barrier,
    ) -> Result<(), ()>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        D: DeriveAsyncWait,
    {
        debug_assert!(!old_locker.killed());
        if old_locker.num_entries() != 0 {
            let target_index = if old_array.num_buckets() >= current_array.num_buckets() {
                let ratio = old_array.num_buckets() / current_array.num_buckets();
                old_index / ratio
            } else {
                let ratio = current_array.num_buckets() / old_array.num_buckets();
                debug_assert!(ratio <= BUCKET_LEN);
                old_index * ratio
            };

            let mut target_buckets: [Option<Locker<K, V, LOCK_FREE>>; usize::BITS as usize / 2] =
                Default::default();
            let mut max_index = 0;
            let mut entry_ptr = EntryPtr::new(barrier);
            let old_data_block_mut = old_array.data_block_mut(old_index);
            while entry_ptr.next(old_locker, barrier) {
                let old_entry = entry_ptr.get(old_data_block_mut);
                let (new_index, partial_hash) =
                    if old_array.num_buckets() >= current_array.num_buckets() {
                        debug_assert_eq!(
                            current_array.calculate_bucket_index(self.hash(old_entry.0.borrow())),
                            target_index
                        );
                        (target_index, entry_ptr.partial_hash(&*old_locker))
                    } else {
                        let hash = self.hash(old_entry.0.borrow());
                        let new_index = current_array.calculate_bucket_index(hash);
                        debug_assert!(
                            new_index - target_index
                                < (current_array.num_buckets() / old_array.num_buckets())
                        );
                        let partial_hash = BucketArray::<K, V, LOCK_FREE>::partial_hash(hash);
                        (new_index, partial_hash)
                    };

                while max_index <= new_index - target_index {
                    let target_bucket = current_array.bucket_mut(max_index + target_index);
                    let locker = unsafe {
                        if TRY_LOCK {
                            Locker::try_lock(target_bucket, barrier)?.unwrap_unchecked()
                        } else if let Some(async_wait) = async_wait.derive() {
                            Locker::try_lock_or_wait(target_bucket, async_wait, barrier)?
                                .unwrap_unchecked()
                        } else {
                            Locker::lock(target_bucket, barrier).unwrap_unchecked()
                        }
                    };
                    target_buckets[max_index].replace(locker);
                    max_index += 1;
                }

                let target_bucket = unsafe {
                    target_buckets[new_index - target_index]
                        .as_mut()
                        .unwrap_unchecked()
                };
                let entry_clone = Self::try_clone(old_entry);
                target_bucket.insert_with(
                    current_array.data_block_mut(new_index),
                    partial_hash,
                    || {
                        // Stack unwinding during a call to `insert` will result in the entry being
                        // removed from the map, any map entry modification should take place after all
                        // the memory is reserved.
                        entry_clone.unwrap_or_else(|| {
                            old_locker.extract(old_data_block_mut, &mut entry_ptr, barrier)
                        })
                    },
                    barrier,
                );

                if LOCK_FREE {
                    // In order for readers that have observed the following erasure to see the above
                    // insertion, a `Release` fence is needed.
                    fence(Release);
                    old_locker.erase(old_data_block_mut, &mut entry_ptr);
                }
            }
        }
        old_locker.kill(barrier);
        Ok(())
    }

    /// Clears the old array.
    fn clear_old_array(&self, current_array: &BucketArray<K, V, LOCK_FREE>, barrier: &Barrier) {
        while !current_array.old_array(barrier).is_null() {
            if self.incremental_rehash::<_, _, false>(current_array, &mut (), barrier) == Ok(true) {
                break;
            }
        }
    }

    /// Relocates a fixed number of buckets from the old array to the current array.
    ///
    /// Returns `true` if `old_array` is null.
    fn incremental_rehash<Q, D, const TRY_LOCK: bool>(
        &self,
        current_array: &BucketArray<K, V, LOCK_FREE>,
        async_wait: &mut D,
        barrier: &Barrier,
    ) -> Result<bool, ()>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        D: DeriveAsyncWait,
    {
        if let Some(old_array) = current_array.old_array(barrier).as_ref() {
            // Assign itself a range of `Bucket` instances to rehash.
            //
            // Aside from the range, it increments the implicit reference counting field in
            // `old_array.rehashing`.
            let rehashing_metadata = old_array.rehashing_metadata();
            let mut current = rehashing_metadata.load(Relaxed);
            loop {
                if current >= old_array.num_buckets()
                    || (current & (BUCKET_LEN - 1)) == BUCKET_LEN - 1
                {
                    // Only `BUCKET_LEN - 1` threads are allowed to rehash a `Bucket` at a moment.
                    return Ok(current_array.old_array(barrier).is_null());
                }
                match rehashing_metadata.compare_exchange(
                    current,
                    current + BUCKET_LEN + 1,
                    Relaxed,
                    Relaxed,
                ) {
                    Ok(_) => {
                        current &= !(BUCKET_LEN - 1);
                        break;
                    }
                    Err(result) => current = result,
                }
            }

            // The guard ensures dropping one reference in `old_array.rehashing`.
            let mut rehashing_guard = ExitGuard::new((current, false), |(prev, success)| {
                if success {
                    // Keep the index as it is.
                    let current = rehashing_metadata.fetch_sub(1, Relaxed) - 1;
                    if (current & (BUCKET_LEN - 1) == 0) && current >= old_array.num_buckets() {
                        // The last one trying to relocate old entries gets rid of the old array.
                        current_array.drop_old_array(barrier);
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
                        match rehashing_metadata.compare_exchange(current, new, Relaxed, Relaxed) {
                            Ok(_) => break,
                            Err(actual) => current = actual,
                        }
                    }
                }
            });

            for index in current..(current + BUCKET_LEN).min(old_array.num_buckets()) {
                let old_bucket = old_array.bucket_mut(index);
                let lock_result = if TRY_LOCK {
                    Locker::try_lock(old_bucket, barrier)?
                } else if let Some(async_wait) = async_wait.derive() {
                    Locker::try_lock_or_wait(old_bucket, async_wait, barrier)?
                } else {
                    Locker::lock(old_bucket, barrier)
                };
                if let Some(mut locker) = lock_result {
                    self.relocate_bucket::<Q, D, TRY_LOCK>(
                        current_array,
                        old_array,
                        index,
                        &mut locker,
                        async_wait,
                        barrier,
                    )?;
                }
            }

            // Successfully rehashed all the assigned buckets.
            rehashing_guard.1 = true;
        }
        Ok(current_array.old_array(barrier).is_null())
    }

    /// Tries to enlarge the array if the estimated load factor is greater than `7/8`.
    #[inline]
    fn try_enlarge(
        &self,
        current_array: &BucketArray<K, V, LOCK_FREE>,
        index: usize,
        mut num_entries: usize,
        barrier: &Barrier,
    ) {
        let sample_size = current_array.sample_size();
        let threshold = sample_size * (BUCKET_LEN / 8) * 7;
        if num_entries > threshold
            || (1..sample_size).any(|i| {
                num_entries += current_array
                    .bucket((index + i) % current_array.num_buckets())
                    .num_entries();
                num_entries > threshold
            })
        {
            self.try_resize(index, barrier);
        }
    }

    /// Tries to shrink the hash table to fit the estimated number of entries, or rebuild it to
    /// optimize the storage.
    #[inline]
    fn try_shrink_or_rebuild(
        &self,
        current_array: &BucketArray<K, V, LOCK_FREE>,
        index: usize,
        barrier: &Barrier,
    ) {
        debug_assert!(current_array.old_array(barrier).is_null());

        if current_array.num_entries() > self.minimum_capacity().load(Relaxed).next_power_of_two()
            || LOCK_FREE
        {
            let sample_size = current_array.sample_size();
            let shrink_threshold = sample_size * BUCKET_LEN / 16;
            let rebuild_threshold = sample_size / 2;
            let mut num_entries = 0;
            let mut num_buckets_to_rebuild = 0;
            for i in 0..sample_size {
                let bucket = current_array.bucket((index + i) % current_array.num_buckets());
                num_entries += bucket.num_entries();
                if num_entries >= shrink_threshold
                    && (!LOCK_FREE
                        || num_buckets_to_rebuild + (sample_size - i) < rebuild_threshold)
                {
                    // Early exit.
                    return;
                }
                if LOCK_FREE && bucket.need_rebuild() {
                    if num_buckets_to_rebuild >= rebuild_threshold {
                        self.try_resize(index, barrier);
                        return;
                    }
                    num_buckets_to_rebuild += 1;
                }
            }
            if !LOCK_FREE || num_entries < shrink_threshold {
                self.try_resize(index, barrier);
            }
        }
    }

    /// Tries to resize the array.
    fn try_resize(&self, sampling_index: usize, barrier: &Barrier) {
        let current_array_ptr = self.bucket_array().load(Acquire, barrier);
        if current_array_ptr.tag() != Tag::None {
            // Another thread is currently allocating a new bucket array.
            return;
        }

        if let Some(current_array) = current_array_ptr.as_ref() {
            if !current_array.old_array(barrier).is_null() {
                // The hash table cannot be resized with an old array attached to it.
                return;
            }

            // The resizing policies are as follows.
            //  - The estimated load factor >= `7/8`, then the hash table grows up to 32x.
            //  - The estimated load factor <= `1/16`, then the hash table shrinks to fit.
            let minimum_capacity = self.minimum_capacity().load(Relaxed);
            let capacity = current_array.num_entries();
            let sample_size = current_array.full_sample_size();
            let estimated_num_entries = Self::sample(current_array, sampling_index, sample_size);
            let new_capacity = if estimated_num_entries >= (capacity / 8) * 7 {
                let max_capacity = 1_usize << (usize::BITS - 1);
                if capacity == max_capacity {
                    // Do not resize if the capacity cannot be increased.
                    capacity
                } else {
                    let mut new_capacity = capacity;
                    while new_capacity <= (estimated_num_entries / 8) * 15 {
                        // Double `new_capacity` until the expected load factor is below 0.5.
                        if new_capacity == max_capacity {
                            break;
                        }
                        if new_capacity / capacity == 32 {
                            break;
                        }
                        new_capacity *= 2;
                    }
                    new_capacity
                }
            } else if estimated_num_entries <= capacity / 16 {
                // Shrink to fit.
                estimated_num_entries
                    .max(minimum_capacity)
                    .max(BucketArray::<K, V, LOCK_FREE>::minimum_capacity())
                    .next_power_of_two()
            } else {
                capacity
            };

            let try_resize = new_capacity != capacity;
            let try_drop_table = estimated_num_entries == 0 && minimum_capacity == 0;
            let try_rebuild = LOCK_FREE
                && !try_resize
                && Self::check_rebuild(current_array, sampling_index, sample_size);

            if try_resize || try_drop_table || try_rebuild {
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
                    // Try to drop the hash table with all the buckets read-locked if empty.
                    let mut reader_guard = ExitGuard::new(
                        (current_array.num_buckets(), true),
                        |(num_locked_buckets, success): (usize, bool)| {
                            for i in 0..num_locked_buckets {
                                let bucket = current_array.bucket_mut(i);
                                if success {
                                    bucket.kill(barrier);
                                }
                                Reader::release(bucket);
                            }
                        },
                    );

                    if !(0..current_array.num_buckets()).any(|i| {
                        if let Ok(Some(reader)) = Reader::try_lock(current_array.bucket(i), barrier)
                        {
                            if reader.num_entries() == 0 {
                                // The bucket will be unlocked later.
                                std::mem::forget(reader);
                                return false;
                            }
                        }
                        reader_guard.0 = i;
                        reader_guard.1 = false;
                        true
                    }) {
                        // All the buckets are empty and locked.
                        self.bucket_array().swap((None, Tag::None), Relaxed);
                        return;
                    }
                }

                let allocated_array: Option<Arc<BucketArray<K, V, LOCK_FREE>>> = None;
                let mut mutex_guard = ExitGuard::new(allocated_array, |allocated_array| {
                    if let Some(allocated_array) = allocated_array {
                        // A new array was allocated.
                        self.bucket_array()
                            .swap((Some(allocated_array), Tag::None), Release);
                    } else {
                        // Release the lock.
                        self.bucket_array()
                            .update_tag_if(Tag::None, |_| true, Relaxed, Relaxed);
                    }
                });
                if try_resize || try_rebuild {
                    mutex_guard.replace(unsafe {
                        Arc::new_unchecked(BucketArray::<K, V, LOCK_FREE>::new(
                            new_capacity,
                            self.bucket_array().clone(Relaxed, barrier),
                        ))
                    });
                }
            }
        }
    }
}
