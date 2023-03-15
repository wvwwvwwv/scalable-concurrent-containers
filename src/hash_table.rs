pub mod bucket;
pub mod bucket_array;

use bucket::{DataBlock, EntryPtr, Locker, Reader, BUCKET_LEN};
use bucket_array::BucketArray;

use crate::ebr::{Arc, AtomicArc, Barrier, Tag};
use crate::exit_guard::ExitGuard;
use crate::wait_queue::DeriveAsyncWait;

use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash, Hasher};
use std::ptr;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{fence, AtomicU8};

/// `HashTable` defines common functions for hash table implementations.
pub(super) trait HashTable<K, V, H, const LOCK_FREE: bool>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    /// The default capacity.
    const DEFAULT_CAPACITY: usize = BUCKET_LEN * 2;

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

    /// Cloning function which is only invoked when `LOCK_FREE` is `true` thus `K` and `V` both
    /// being `Clone`.
    fn cloner(entry: &(K, V)) -> Option<(K, V)>;

    /// Returns a reference to the [`BucketArray`] pointer.
    fn bucket_array(&self) -> &AtomicArc<BucketArray<K, V, LOCK_FREE>>;

    /// Returns the minimum allowed capacity.
    fn minimum_capacity(&self) -> usize;

    /// Returns a reference to the resizing mutex.
    fn resize_mutex(&self) -> &AtomicU8;

    /// Returns a reference to the current array without checking the pointer value.
    #[inline]
    fn current_array_unchecked<'b>(
        &self,
        barrier: &'b Barrier,
    ) -> &'b BucketArray<K, V, LOCK_FREE> {
        // An acquire fence is required to correctly load the contents of the array.
        let current_array_ptr = self.bucket_array().load(Acquire, barrier);
        unsafe { current_array_ptr.as_ref().unwrap_unchecked() }
    }

    /// Returns the number of entries.
    #[inline]
    fn num_entries(&self, barrier: &Barrier) -> usize {
        let current_array = self.current_array_unchecked(barrier);
        let mut num_entries = 0;
        let old_array_ptr = current_array.old_array(barrier);
        if let Some(old_array) = old_array_ptr.as_ref() {
            for i in 0..old_array.num_buckets() {
                num_entries += old_array.bucket(i).num_entries();
            }
        }
        for i in 0..current_array.num_buckets() {
            num_entries += current_array.bucket(i).num_entries();
        }
        num_entries
    }

    /// Returns `true` if the number of entries is non-zero.
    #[inline]
    fn has_entry(&self, barrier: &Barrier) -> bool {
        let current_array = self.current_array_unchecked(barrier);
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
        false
    }

    /// Returns the number of slots.
    #[inline]
    fn num_slots(&self, barrier: &Barrier) -> usize {
        let current_array = self.current_array_unchecked(barrier);
        current_array.num_entries()
    }

    /// Estimates the number of entries by sampling the specified number of buckets.
    #[inline]
    fn estimate(
        array: &BucketArray<K, V, LOCK_FREE>,
        sampling_index: usize,
        num_buckets_to_sample: usize,
    ) -> usize {
        let mut num_entries = 0;
        let start = if sampling_index + num_buckets_to_sample >= array.num_buckets() {
            0
        } else {
            sampling_index
        };
        for i in start..(start + num_buckets_to_sample) {
            num_entries += array.bucket(i).num_entries();
        }
        num_entries * (array.num_buckets() / num_buckets_to_sample)
    }

    /// Checks whether rebuilding the entire hash table is required.
    #[inline]
    fn check_rebuild(
        array: &BucketArray<K, V, LOCK_FREE>,
        sampling_index: usize,
        num_buckets_to_sample: usize,
    ) -> bool {
        let mut num_buckets_to_rebuild = 0;
        let start = if sampling_index + num_buckets_to_sample >= array.num_buckets() {
            0
        } else {
            sampling_index
        };
        for i in start..(start + num_buckets_to_sample) {
            if array.bucket(i).need_rebuild() {
                num_buckets_to_rebuild += 1;
                if num_buckets_to_rebuild > num_buckets_to_sample / 2 {
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
            Ok((mut locker, data_block, entry_ptr)) => {
                if entry_ptr.is_valid() {
                    return Ok(Some((key, val)));
                }
                locker.insert_with(
                    data_block,
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
        let mut current_array = self.current_array_unchecked(barrier);
        loop {
            if let Some(old_array) = current_array.old_array(barrier).as_ref() {
                if LOCK_FREE {
                    if self.partial_rehash::<Q, D, true>(current_array, async_wait, barrier)
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
                    if let Some((key, val)) = reader.bucket().search(
                        current_array.data_block(index),
                        key,
                        BucketArray::<K, V, LOCK_FREE>::partial_hash(hash),
                        barrier,
                    ) {
                        return Ok(Some((key, val)));
                    }
                }
            }

            let new_current_array = self.current_array_unchecked(barrier);
            if ptr::eq(current_array, new_current_array) {
                break;
            }

            // A new array has been allocated.
            current_array = new_current_array;
        }

        Ok(None)
    }

    /// Removes an entry if the condition is met.
    #[inline]
    fn remove_entry<Q, F: FnOnce(&V) -> bool, D, R, P: FnOnce(Option<Option<(K, V)>>) -> R>(
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
        loop {
            // The reasoning behind this loop can be found in `acquire_entry`.
            let current_array = self.current_array_unchecked(barrier);
            let shrinkable = if let Some(old_array) = current_array.old_array(barrier).as_ref() {
                match self.move_entry(current_array, old_array, hash, async_wait, barrier) {
                    Ok(r) => r,
                    Err(_) => break,
                }
            } else {
                true
            };

            let index = current_array.calculate_bucket_index(hash);
            let bucket = current_array.bucket_mut(index);
            let lock_result = if let Some(async_wait) = async_wait.derive() {
                match Locker::try_lock_or_wait(bucket, async_wait, barrier) {
                    Ok(l) => l,
                    Err(_) => break,
                }
            } else {
                Locker::lock(bucket, barrier)
            };
            if let Some(mut locker) = lock_result {
                let data_block = current_array.data_block(index);
                let mut entry_ptr = locker.get(
                    data_block,
                    key,
                    BucketArray::<K, V, LOCK_FREE>::partial_hash(hash),
                    barrier,
                );
                if entry_ptr.is_valid() && condition(&entry_ptr.get(data_block).1) {
                    let result = locker.erase(data_block, &mut entry_ptr);
                    if shrinkable && index % BUCKET_LEN == 0 {
                        if locker.bucket().num_entries() < BUCKET_LEN / 16
                            && current_array.num_entries() > self.minimum_capacity()
                        {
                            drop(locker);
                            self.try_shrink(current_array, index, barrier);
                        } else if LOCK_FREE && locker.bucket().need_rebuild() {
                            drop(locker);
                            self.try_rebuild(current_array, index, barrier);
                        }
                    }
                    return Ok(post_processor(Some(result)));
                }
                return Ok(post_processor(None));
            }
        }
        Err(condition)
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
            &'b DataBlock<K, V, BUCKET_LEN>,
            EntryPtr<'b, K, V, LOCK_FREE>,
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
            let current_array = self.current_array_unchecked(barrier);
            if let Some(old_array) = current_array.old_array(barrier).as_ref() {
                self.move_entry(current_array, old_array, hash, async_wait, barrier)?;
            }

            let index = current_array.calculate_bucket_index(hash);
            let bucket = current_array.bucket_mut(index);

            // Try to resize the array.
            if index % BUCKET_LEN == 0 && bucket.num_entries() >= BUCKET_LEN - 1 {
                self.try_enlarge(current_array, index, bucket.num_entries(), barrier);
            }

            let lock_result = if let Some(async_wait) = async_wait.derive() {
                Locker::try_lock_or_wait(bucket, async_wait, barrier)?
            } else {
                Locker::lock(bucket, barrier)
            };
            if let Some(locker) = lock_result {
                let data_block = current_array.data_block(index);
                let entry_ptr = locker.get(
                    data_block,
                    key,
                    BucketArray::<K, V, LOCK_FREE>::partial_hash(hash),
                    barrier,
                );
                return Ok((locker, data_block, entry_ptr));
            }

            // Reaching here means that `self.array` has been updated.
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
        if !self.partial_rehash::<Q, D, false>(current_array, async_wait, barrier)? {
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
        debug_assert!(!old_locker.bucket().killed());
        if old_locker.bucket().num_entries() != 0 {
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
            let old_data_block = old_array.data_block(old_index);
            while entry_ptr.next(old_locker.bucket(), barrier) {
                let old_entry = entry_ptr.get(old_data_block);
                let (new_index, partial_hash) =
                    if old_array.num_buckets() >= current_array.num_buckets() {
                        debug_assert_eq!(
                            current_array.calculate_bucket_index(self.hash(old_entry.0.borrow())),
                            target_index
                        );
                        (target_index, entry_ptr.partial_hash(old_locker.bucket()))
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
                let cloned_entry = Self::cloner(old_entry);
                target_bucket.insert_with(
                    current_array.data_block(new_index),
                    partial_hash,
                    || {
                        // Stack unwinding during a call to `insert` will result in the entry being
                        // removed from the map, any map entry modification should take place after all
                        // the memory is reserved.
                        cloned_entry.unwrap_or_else(|| {
                            old_locker.extract(old_data_block, &mut entry_ptr, barrier)
                        })
                    },
                    barrier,
                );

                if LOCK_FREE {
                    // In order for readers that have observed the following erasure to see the above
                    // insertion, a `Release` fence is needed.
                    fence(Release);
                    old_locker.erase(old_data_block, &mut entry_ptr);
                }
            }
        }
        old_locker.purge(barrier);
        Ok(())
    }

    /// Tries to enlarge the array if the estimated load factor is greater than `7/8`.
    #[inline]
    fn try_enlarge(
        &self,
        array: &BucketArray<K, V, LOCK_FREE>,
        index: usize,
        mut num_entries: usize,
        barrier: &Barrier,
    ) {
        let sample_size = array.sample_size();
        let threshold = sample_size * (BUCKET_LEN / 8) * 7;
        if num_entries > threshold
            || (1..sample_size).any(|i| {
                num_entries += array
                    .bucket((index + i) % array.num_buckets())
                    .num_entries();
                num_entries > threshold
            })
        {
            self.resize(barrier);
        }
    }

    /// Tries to shrink the array if the load factor is estimated at around `1/16`.
    #[inline]
    fn try_shrink(&self, array: &BucketArray<K, V, LOCK_FREE>, index: usize, barrier: &Barrier) {
        let sample_size = array.sample_size();
        let threshold = sample_size * BUCKET_LEN / 16;
        let mut num_entries = 0;
        if !(1..sample_size).any(|i| {
            num_entries += array
                .bucket((index + i) % array.num_buckets())
                .num_entries();
            num_entries >= threshold
        }) {
            self.resize(barrier);
        }
    }

    /// Tries to rebuild the array if there are no usable entry slots in at least half of the
    /// buckets in the sampling range.
    #[inline]
    fn try_rebuild(&self, array: &BucketArray<K, V, LOCK_FREE>, index: usize, barrier: &Barrier) {
        let sample_size = array.sample_size();
        let threshold = sample_size / 2;
        let mut num_buckets_to_rebuild = 1;
        if (1..sample_size).any(|i| {
            if array
                .bucket((index + i) % array.num_buckets())
                .need_rebuild()
            {
                num_buckets_to_rebuild += 1;
                num_buckets_to_rebuild > threshold
            } else {
                false
            }
        }) {
            self.resize(barrier);
        }
    }

    /// Relocates a fixed number of buckets from the old array to the current array.
    ///
    /// Returns `true` if `old_array` is null.
    fn partial_rehash<Q, D, const TRY_LOCK: bool>(
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

    /// Resizes the array.
    fn resize(&self, barrier: &Barrier) {
        let mut mutex_state = self.resize_mutex().load(Acquire);
        loop {
            if mutex_state == 2_u8 {
                // Another thread is resizing the table, and will retry.
                return;
            }
            let new_state = if mutex_state == 1_u8 {
                // Let the mutex owner know that a new resize was requested.
                2_u8
            } else {
                // This thread will acquire the mutex.
                1_u8
            };
            match self
                .resize_mutex()
                .compare_exchange(mutex_state, new_state, Acquire, Acquire)
            {
                Ok(_) => {
                    if new_state == 2_u8 {
                        // Retry requested.
                        return;
                    }
                    // Lock acquired.
                    break;
                }
                Err(actual) => mutex_state = actual,
            }
        }

        let mut sampling_index = 0;
        let mut resize = true;
        while resize {
            let _mutex_guard = ExitGuard::new(&mut resize, |resize| {
                *resize = self.resize_mutex().fetch_sub(1, Release) == 2_u8;
            });

            let current_array = self.current_array_unchecked(barrier);
            if !current_array.old_array(barrier).is_null() {
                // With a deprecated array present, it cannot be resized.
                continue;
            }

            // The resizing policies are as follows.
            //  - The load factor reaches 7/8, then the array grows up to 32x.
            //  - The load factor reaches 1/16, then the array shrinks to fit.
            let capacity = current_array.num_entries();
            let num_buckets = current_array.num_buckets();
            let num_buckets_to_sample = (num_buckets / 8).clamp(2, 4096);
            let mut rebuild = false;
            let estimated_num_entries =
                Self::estimate(current_array, sampling_index, num_buckets_to_sample);
            sampling_index = sampling_index.wrapping_add(num_buckets_to_sample);
            let new_capacity = if estimated_num_entries >= (capacity / 8) * 7 {
                let max_capacity = 1_usize << (usize::BITS - 1);
                if capacity == max_capacity {
                    // Do not resize if the capacity cannot be increased.
                    capacity
                } else {
                    let mut new_capacity = capacity;
                    while new_capacity < (estimated_num_entries / 8) * 15 {
                        // Double the new capacity until it can accommodate the estimated number of entries * 15/8.
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
                    .next_power_of_two()
                    .max(self.minimum_capacity())
            } else {
                if LOCK_FREE {
                    rebuild =
                        Self::check_rebuild(current_array, sampling_index, num_buckets_to_sample);
                }
                capacity
            };

            if new_capacity != capacity || (LOCK_FREE && rebuild) {
                self.bucket_array().swap(
                    (
                        Some(unsafe {
                            Arc::new_unchecked(BucketArray::<K, V, LOCK_FREE>::new(
                                new_capacity,
                                self.bucket_array().clone(Relaxed, barrier),
                            ))
                        }),
                        Tag::None,
                    ),
                    Release,
                );
            }
        }
    }
}
