use super::bucket::{Bucket, DataBlock, BUCKET_LEN};

use crate::ebr::{AtomicArc, Barrier, Ptr, Tag};

use std::alloc::{alloc, alloc_zeroed, dealloc, Layout};
use std::mem::{align_of, needs_drop, size_of};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

/// [`BucketArray`] is a special purpose array to manage [`Bucket`] and [`DataBlock`].
pub struct BucketArray<K: Eq, V, const LOCK_FREE: bool> {
    bucket_ptr: *const Bucket<K, V, LOCK_FREE>,
    data_block_ptr: *const DataBlock<K, V, BUCKET_LEN>,
    array_len: usize,
    hash_offset: u16,
    sample_size: u16,
    bucket_ptr_offset: u32,
    old_array: AtomicArc<BucketArray<K, V, LOCK_FREE>>,
    num_cleared_buckets: AtomicUsize,
}

impl<K: Eq, V, const LOCK_FREE: bool> BucketArray<K, V, LOCK_FREE> {
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
            let bucket_array_ptr_offset = bucket_size.next_power_of_two()
                - (bucket_array_ptr as usize % bucket_size.next_power_of_two());
            assert!(
                bucket_array_ptr_offset + bucket_size * array_len <= bucket_array_allocation_size,
            );
            assert_eq!(
                (bucket_array_ptr as usize + bucket_array_ptr_offset) % bucket_size,
                0
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

    /// Returns a reference to its rehashing metadata.
    #[inline]
    pub(crate) fn rehashing_metadata(&self) -> &AtomicUsize {
        &self.num_cleared_buckets
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

    /// Drops the old array.
    #[inline]
    pub(crate) fn drop_old_array(&self, barrier: &Barrier) {
        self.old_array.swap((None, Tag::None), Relaxed).0.map(|a| {
            // It is OK to pass the old array instance to the garbage collector, deferring destruction.
            debug_assert_eq!(
                a.num_cleared_buckets.load(Relaxed),
                a.array_len.max(BUCKET_LEN)
            );
            a.release(barrier)
        });
    }

    /// Calculates the [`Bucket`] index for the hash value.
    #[allow(clippy::cast_possible_truncation)]
    #[inline]
    pub(crate) fn calculate_bucket_index(&self, hash: u64) -> usize {
        hash.wrapping_shr(u32::from(self.hash_offset)) as usize
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
        if !LOCK_FREE && !self.old_array.is_null(Relaxed) {
            // The `BucketArray` should be dropped immediately.
            unsafe {
                self.old_array
                    .swap((None, Tag::None), Relaxed)
                    .0
                    .map(|a| a.release_drop_in_place());
            }
        }

        let num_cleared_buckets = if LOCK_FREE && needs_drop::<(K, V)>() {
            // No instances are dropped when the array is reachable.
            0
        } else {
            // `LinkedBucket` instances should be cleaned up.
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

unsafe impl<K: Eq + Send, V: Send, const LOCK_FREE: bool> Send for BucketArray<K, V, LOCK_FREE> {}

unsafe impl<K: Eq + Sync, V: Sync, const LOCK_FREE: bool> Sync for BucketArray<K, V, LOCK_FREE> {}

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
        array.num_cleared_buckets.store(array.array_len, Relaxed);
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
            array.num_cleared_buckets.store(array.array_len, Relaxed);
        }
    }
}
