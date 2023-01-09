use super::bucket::{Bucket, DataBlock, EntryPtr, Locker, BUCKET_LEN};

use crate::ebr::{AtomicArc, Barrier, Ptr, Tag};
use crate::exit_guard::ExitGuard;
use crate::wait_queue::DeriveAsyncWait;

use std::alloc::{alloc, alloc_zeroed, dealloc, Layout};
use std::borrow::Borrow;
use std::hash::Hash;
use std::mem::{align_of, needs_drop, size_of};
use std::sync::atomic::Ordering::{Relaxed, Release};
use std::sync::atomic::{fence, AtomicUsize};

/// [`BucketArray`] is a special purpose array to manage [`Bucket`] and [`DataBlock`].
pub struct BucketArray<K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    bucket_ptr: *const Bucket<K, V, LOCK_FREE>,
    data_block_ptr: *const DataBlock<K, V, BUCKET_LEN>,
    array_len: usize,
    hash_offset: u16,
    sample_size: u16,
    bucket_ptr_offset: u32,
    old_array: AtomicArc<BucketArray<K, V, LOCK_FREE>>,
    num_cleared_buckets: AtomicUsize,
}

impl<K: 'static + Eq, V: 'static, const LOCK_FREE: bool> BucketArray<K, V, LOCK_FREE> {
    /// Returns the partial hash value of the given hash.
    #[allow(clippy::cast_possible_truncation)]
    #[inline]
    pub(crate) fn partial_hash(hash: u64) -> u8 {
        (hash % (1 << 8)) as u8
    }

    /// Creates a new [`BucketArray`] of the given capacity.
    ///
    /// `capacity` is the desired number entries, not the number of [`Bucket`] instances.
    pub(crate) fn new(
        capacity: usize,
        old_array: AtomicArc<BucketArray<K, V, LOCK_FREE>>,
    ) -> BucketArray<K, V, LOCK_FREE> {
        let log2_array_len = Self::calculate_log2_array_size(capacity);
        let array_len = 1_usize << log2_array_len;
        unsafe {
            let (bucket_size, bucket_array_allocation_size, bucket_array_layout) =
                Self::calculate_memory_layout::<Bucket<K, V, LOCK_FREE>>(array_len);
            let bucket_array_ptr = alloc_zeroed(bucket_array_layout);
            assert!(
                !bucket_array_ptr.is_null(),
                "memory allocation failure: {bucket_array_allocation_size} bytes",
            );
            let bucket_array_ptr_offset =
                bucket_array_ptr as usize % bucket_size.next_power_of_two();
            assert!(
                bucket_array_ptr_offset + bucket_size * array_len <= bucket_array_allocation_size,
            );

            #[allow(clippy::cast_ptr_alignment)]
            let bucket_array_ptr =
                bucket_array_ptr
                    .add(bucket_array_ptr_offset)
                    .cast::<Bucket<K, V, LOCK_FREE>>();
            #[allow(clippy::cast_possible_truncation)]
            let bucket_array_ptr_offset = bucket_array_ptr_offset as u32;

            let data_block_array_layout = Layout::from_size_align(
                size_of::<DataBlock<K, V, BUCKET_LEN>>() * array_len,
                align_of::<[DataBlock<K, V, BUCKET_LEN>; 0]>(),
            )
            .unwrap();

            let data_block_array_ptr =
                alloc(data_block_array_layout).cast::<DataBlock<K, V, BUCKET_LEN>>();
            assert!(
                !data_block_array_ptr.is_null(),
                "memory allocation failure: {} bytes",
                data_block_array_layout.size(),
            );

            BucketArray {
                bucket_ptr: bucket_array_ptr,
                data_block_ptr: data_block_array_ptr,
                array_len,
                hash_offset: 64 - u16::from(log2_array_len),
                sample_size: u16::from(log2_array_len).next_power_of_two(),
                bucket_ptr_offset: bucket_array_ptr_offset,
                old_array,
                num_cleared_buckets: AtomicUsize::new(0),
            }
        }
    }

    /// Returns a reference to a [`Bucket`] at the given position.
    #[inline]
    pub(crate) fn bucket(&self, index: usize) -> &Bucket<K, V, LOCK_FREE> {
        debug_assert!(index < self.num_buckets());
        unsafe { &(*(self.bucket_ptr.add(index))) }
    }

    /// Returns a mutable reference to a [`Bucket`] at the given position.
    #[allow(clippy::mut_from_ref)]
    #[inline]
    pub(crate) fn bucket_mut(&self, index: usize) -> &mut Bucket<K, V, LOCK_FREE> {
        debug_assert!(index < self.num_buckets());
        unsafe { &mut (*(self.bucket_ptr.add(index) as *mut Bucket<K, V, LOCK_FREE>)) }
    }

    /// Returns a reference to a [`DataBlock`] at the given position.
    #[inline]
    pub(crate) fn data_block(&self, index: usize) -> &DataBlock<K, V, BUCKET_LEN> {
        debug_assert!(index < self.num_buckets());
        unsafe { &(*(self.data_block_ptr.add(index))) }
    }

    /// Returns the recommended sampling size.
    #[inline]
    pub(crate) fn sample_size(&self) -> usize {
        self.sample_size as usize
    }

    /// Returns the number of [`Bucket`] instances in the [`BucketArray`].
    #[inline]
    pub(crate) fn num_buckets(&self) -> usize {
        self.array_len
    }

    /// Returns the number of total entries.
    #[inline]
    pub(crate) fn num_entries(&self) -> usize {
        self.array_len * BUCKET_LEN
    }

    /// Returns a [`Ptr`] to the old array.
    #[inline]
    pub(crate) fn old_array<'b>(
        &self,
        barrier: &'b Barrier,
    ) -> Ptr<'b, BucketArray<K, V, LOCK_FREE>> {
        self.old_array.load(Relaxed, barrier)
    }

    /// Calculates the [`Bucket`] index for the hash value.
    #[allow(clippy::cast_possible_truncation)]
    #[inline]
    pub(crate) fn calculate_bucket_index(&self, hash: u64) -> usize {
        hash.wrapping_shr(u32::from(self.hash_offset)) as usize
    }

    /// Kills the [`Bucket`].
    ///
    /// It returns an error if locking failed.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub(crate) fn kill_bucket<
        Q,
        F: Fn(&Q) -> u64,
        C: Fn(&K, &V) -> (K, V),
        D,
        const TRY_LOCK: bool,
    >(
        &self,
        old_locker: &mut Locker<K, V, LOCK_FREE>,
        old_index: usize,
        old_array: &BucketArray<K, V, LOCK_FREE>,
        hasher: &F,
        copier: &C,
        async_wait: &mut D,
        barrier: &Barrier,
    ) -> Result<(), ()>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        D: DeriveAsyncWait,
    {
        debug_assert!(!old_locker.bucket().killed());
        if old_locker.bucket().num_entries() == 0 {
            old_locker.purge(barrier);
            return Ok(());
        }

        let shrink = old_array.num_buckets() > self.num_buckets();
        let ratio = if shrink {
            old_array.num_buckets() / self.array_len
        } else {
            self.array_len / old_array.num_buckets()
        };
        let target_index = if shrink {
            old_index / ratio
        } else {
            debug_assert!(ratio <= 32);
            old_index * ratio
        };

        let mut target_buckets: [Option<Locker<K, V, LOCK_FREE>>; usize::BITS as usize / 2] =
            Default::default();
        let mut max_index = 0;
        let mut entry_ptr = EntryPtr::new(barrier);
        let old_data_block = old_array.data_block(old_index);
        while entry_ptr.next(old_locker.bucket(), barrier) {
            let old_entry = entry_ptr.get(old_data_block);
            let (new_index, partial_hash) = if shrink {
                debug_assert!(
                    self.calculate_bucket_index(hasher(old_entry.0.borrow())) == target_index
                );
                (target_index, entry_ptr.partial_hash(old_locker.bucket()))
            } else {
                let hash = hasher(old_entry.0.borrow());
                let new_index = self.calculate_bucket_index(hash);
                debug_assert!((new_index - target_index) < ratio);
                (
                    new_index,
                    BucketArray::<K, V, LOCK_FREE>::partial_hash(hash),
                )
            };

            let offset = new_index - target_index;
            while max_index <= offset {
                let target_bucket = self.bucket_mut(max_index + target_index);
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

            let target_bucket = unsafe { target_buckets[offset].as_mut().unwrap_unchecked() };
            target_bucket.insert_with(
                self.data_block(target_index + offset),
                partial_hash,
                || {
                    // Stack unwinding during a call to `insert` will result in the entry being
                    // removed from the map, any map entry modification should take place after all
                    // the memory is reserved.
                    if LOCK_FREE {
                        copier(&old_entry.0, &old_entry.1)
                    } else {
                        old_locker.extract(old_data_block, &mut entry_ptr, barrier)
                    }
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
        old_locker.purge(barrier);
        Ok(())
    }

    /// Relocates a fixed number of `Bucket` instances from the old array to the current array.
    ///
    /// Returns `true` if `old_array` is null.
    #[inline]
    pub(crate) fn partial_rehash<
        Q,
        F: Fn(&Q) -> u64,
        C: Fn(&K, &V) -> (K, V),
        D,
        const TRY_LOCK: bool,
    >(
        &self,
        hasher: F,
        copier: C,
        async_wait: &mut D,
        barrier: &Barrier,
    ) -> Result<bool, ()>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        D: DeriveAsyncWait,
    {
        if let Some(old_array) = self.old_array(barrier).as_ref() {
            // Assign itself a range of `Bucket` instances to rehash.
            //
            // Aside from the range, it increments the implicit reference counting field in
            // `old_array.rehashing`.
            let old_array_num_buckets = old_array.num_buckets();
            let mut current = old_array.num_cleared_buckets.load(Relaxed);
            loop {
                if current >= old_array_num_buckets
                    || (current & (BUCKET_LEN - 1)) == BUCKET_LEN - 1
                {
                    // Only `BUCKET_LEN - 1` threads are allowed to rehash a `Bucket` at a moment.
                    return Ok(self.old_array.is_null(Relaxed));
                }
                match old_array.num_cleared_buckets.compare_exchange(
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
                    let current = old_array.num_cleared_buckets.fetch_sub(1, Relaxed) - 1;
                    if (current & (BUCKET_LEN - 1) == 0) && current >= old_array_num_buckets {
                        // The last one trying to relocate old entries gets rid of the old array.
                        self.old_array
                            .swap((None, Tag::None), Relaxed)
                            .0
                            .map(|a| a.release(barrier));
                    }
                } else {
                    // On failure, `rehashing` reverts to its previous state.
                    let mut current = old_array.num_cleared_buckets.load(Relaxed);
                    loop {
                        let new = if current <= prev {
                            current - 1
                        } else {
                            let ref_cnt = current & (BUCKET_LEN - 1);
                            prev | (ref_cnt - 1)
                        };
                        match old_array
                            .num_cleared_buckets
                            .compare_exchange(current, new, Relaxed, Relaxed)
                        {
                            Ok(_) => break,
                            Err(actual) => current = actual,
                        }
                    }
                }
            });

            for old_index in current..(current + BUCKET_LEN).min(old_array_num_buckets) {
                let old_bucket = old_array.bucket_mut(old_index);
                let lock_result = if TRY_LOCK {
                    Locker::try_lock(old_bucket, barrier)?
                } else if let Some(async_wait) = async_wait.derive() {
                    Locker::try_lock_or_wait(old_bucket, async_wait, barrier)?
                } else {
                    Locker::lock(old_bucket, barrier)
                };
                if let Some(mut locker) = lock_result {
                    self.kill_bucket::<Q, F, C, D, TRY_LOCK>(
                        &mut locker,
                        old_index,
                        old_array,
                        &hasher,
                        &copier,
                        async_wait,
                        barrier,
                    )?;
                }
            }

            // Successfully rehashed all the assigned buckets.
            rehashing_guard.1 = true;
        }
        Ok(self.old_array.is_null(Relaxed))
    }

    /// Calculates `log_2` of the array size from the given capacity.
    #[allow(clippy::cast_possible_truncation)]
    fn calculate_log2_array_size(capacity: usize) -> u8 {
        let adjusted_capacity = capacity.min((usize::MAX / 2) - (BUCKET_LEN - 1));
        let required_buckets =
            ((adjusted_capacity + BUCKET_LEN - 1) / BUCKET_LEN).next_power_of_two();
        let log2_capacity =
            (usize::BITS as usize - (required_buckets.leading_zeros() as usize) - 1).max(1);

        // `2^lb_capacity * BUCKET_LEN >= capacity`.
        debug_assert!(log2_capacity > 0);
        debug_assert!(log2_capacity < (usize::BITS as usize));
        debug_assert!((1_usize << log2_capacity) * BUCKET_LEN >= adjusted_capacity);
        log2_capacity as u8
    }

    /// Calculates the layout of the memory block for an array of `T`.
    fn calculate_memory_layout<T: Sized>(array_len: usize) -> (usize, usize, Layout) {
        let size_of_t = size_of::<T>();
        let aligned_size = size_of_t.next_power_of_two();
        let allocation_size = aligned_size + array_len * size_of_t;
        (size_of_t, allocation_size, unsafe {
            Layout::from_size_align_unchecked(allocation_size, 1)
        })
    }
}

impl<K: Eq, V, const LOCK_FREE: bool> Drop for BucketArray<K, V, LOCK_FREE> {
    fn drop(&mut self) {
        let num_cleared_buckets = if LOCK_FREE && needs_drop::<(K, V)>() {
            // No instances are dropped when the array is reachable.
            0
        } else {
            // Linked lists should be cleaned up.
            self.num_cleared_buckets.load(Relaxed)
        };

        if num_cleared_buckets < self.array_len {
            let barrier = Barrier::new();
            for index in num_cleared_buckets..self.array_len {
                unsafe {
                    self.bucket_mut(index)
                        .drop_entries(self.data_block(index), &barrier);
                }
            }
        }

        unsafe {
            dealloc(
                (self.bucket_ptr as *mut Bucket<K, V, LOCK_FREE>)
                    .cast::<u8>()
                    .sub(self.bucket_ptr_offset as usize),
                Self::calculate_memory_layout::<Bucket<K, V, LOCK_FREE>>(self.array_len).2,
            );
            dealloc(
                (self.data_block_ptr as *mut DataBlock<K, V, BUCKET_LEN>).cast::<u8>(),
                Layout::from_size_align(
                    size_of::<DataBlock<K, V, BUCKET_LEN>>() * self.array_len,
                    align_of::<[DataBlock<K, V, BUCKET_LEN>; 0]>(),
                )
                .unwrap(),
            );
        }
    }
}

unsafe impl<K, V, const LOCK_FREE: bool> Send for BucketArray<K, V, LOCK_FREE>
where
    K: 'static + Eq + Send,
    V: 'static + Send,
{
}

unsafe impl<K, V, const LOCK_FREE: bool> Sync for BucketArray<K, V, LOCK_FREE>
where
    K: 'static + Eq + Sync,
    V: 'static + Sync,
{
}

#[cfg(test)]
mod test {
    use super::*;

    use std::time::Instant;

    #[test]
    fn alloc() {
        let start = Instant::now();
        let array: BucketArray<usize, usize, true> =
            BucketArray::new(1024 * 1024 * 32, AtomicArc::default());
        assert_eq!(array.num_buckets(), 1024 * 1024);
        let after_alloc = Instant::now();
        println!("allocation took {:?}", after_alloc - start);
        drop(array);
        let after_dealloc = Instant::now();
        println!("deallocation took {:?}", after_dealloc - after_alloc);
    }

    #[test]
    fn array() {
        for s in 0..BUCKET_LEN * 2 {
            let array: BucketArray<usize, usize, true> = BucketArray::new(s, AtomicArc::default());
            assert!(array.num_buckets() >= s.max(BUCKET_LEN) / BUCKET_LEN);
            assert!(array.num_buckets() <= 2 * (s.max(BUCKET_LEN) / BUCKET_LEN));
            assert!(array.num_entries() >= s.max(BUCKET_LEN));
            assert!(array.num_entries() <= 2 * s.max(BUCKET_LEN));
        }
    }
}
