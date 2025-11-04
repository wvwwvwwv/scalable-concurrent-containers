use std::alloc::{Layout, alloc, alloc_zeroed, dealloc};
use std::mem::{align_of, size_of};
use std::panic::UnwindSafe;
use std::ptr::NonNull;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed};

use sdd::{AtomicShared, Guard, Ptr, Tag};

use super::bucket::{BUCKET_LEN, Bucket, DataBlock, INDEX, LruList};
use crate::exit_guard::ExitGuard;

/// [`BucketArray`] is a special purpose array to manage [`Bucket`] and [`DataBlock`].
pub struct BucketArray<K, V, L: LruList, const TYPE: char> {
    buckets: NonNull<Bucket<K, V, L, TYPE>>,
    data_blocks: NonNull<DataBlock<K, V, BUCKET_LEN>>,
    array_len: usize,
    hash_offset: u16,
    sample_size_mask: u16,
    bucket_ptr_offset: u16,
    old_array: AtomicShared<BucketArray<K, V, L, TYPE>>,
    num_cleared_buckets: AtomicUsize,
    garbage_array: AtomicShared<BucketArray<K, V, L, TYPE>>,
}

impl<K, V, L: LruList, const TYPE: char> BucketArray<K, V, L, TYPE> {
    /// Creates a new [`BucketArray`] of the given capacity.
    ///
    /// `capacity` is the desired number of entries, not the length of the bucket array.
    pub(crate) fn new(
        capacity: usize,
        old_array: AtomicShared<BucketArray<K, V, L, TYPE>>,
    ) -> Self {
        let log2_array_len = Self::calculate_log2_array_size(capacity);
        assert_ne!(log2_array_len, 0);

        let array_len = 1_usize << log2_array_len;
        unsafe {
            let (bucket_size, bucket_array_allocation_size, bucket_array_layout) =
                Self::bucket_array_layout(array_len);
            let Some(unalignedbucket_array_ptr) = NonNull::new(alloc_zeroed(bucket_array_layout))
            else {
                panic!("memory allocation failure: {bucket_array_allocation_size} bytes");
            };
            let bucket_array_ptr_offset = bucket_size.next_power_of_two()
                - (unalignedbucket_array_ptr.addr().get() % bucket_size.next_power_of_two());
            assert!(
                bucket_array_ptr_offset + bucket_size * array_len <= bucket_array_allocation_size,
            );
            assert_eq!(
                (unalignedbucket_array_ptr.addr().get() + bucket_array_ptr_offset)
                    % bucket_size.next_power_of_two(),
                0
            );

            #[allow(clippy::cast_ptr_alignment)] // The alignment was just asserted.
            let buckets = unalignedbucket_array_ptr
                .add(bucket_array_ptr_offset)
                .cast::<Bucket<K, V, L, TYPE>>();
            let bucket_array_ptr_offset = u16::try_from(bucket_array_ptr_offset).unwrap_or(0);

            let data_block_array_layout = Layout::from_size_align(
                size_of::<DataBlock<K, V, BUCKET_LEN>>() * array_len,
                align_of::<[DataBlock<K, V, BUCKET_LEN>; 0]>(),
            )
            .unwrap();

            // In case the below data block allocation fails, deallocate the bucket array.
            let mut alloc_guard = ExitGuard::new(false, |allocated| {
                if !allocated {
                    dealloc(
                        unalignedbucket_array_ptr.cast::<u8>().as_ptr(),
                        bucket_array_layout,
                    );
                }
            });

            let Some(data_blocks) =
                NonNull::new(alloc(data_block_array_layout).cast::<DataBlock<K, V, BUCKET_LEN>>())
            else {
                panic!(
                    "memory allocation failure: {} bytes",
                    data_block_array_layout.size()
                );
            };
            *alloc_guard = true;

            Self {
                buckets,
                data_blocks,
                array_len,
                hash_offset: u16::try_from(u64::BITS).unwrap_or(64) - u16::from(log2_array_len),
                sample_size_mask: u16::from(log2_array_len).next_power_of_two() - 1,
                bucket_ptr_offset: bucket_array_ptr_offset,
                old_array,
                num_cleared_buckets: AtomicUsize::new(0),
                garbage_array: AtomicShared::null(),
            }
        }
    }

    /// Returns the number of [`Bucket`] instances in the [`BucketArray`].
    #[inline]
    pub(crate) const fn len(&self) -> usize {
        self.array_len
    }

    /// Returns the number of entry slots in the bucket array.
    #[inline]
    pub(crate) const fn num_slots(&self) -> usize {
        self.array_len * BUCKET_LEN
    }

    /// Calculates the [`Bucket`] index for the hash value.
    #[allow(clippy::cast_possible_truncation)] // Intended truncation.
    #[inline]
    pub(crate) const fn calculate_bucket_index(&self, hash: u64) -> usize {
        // Take the upper n-bits to make sure that a single bucket is spread across a few adjacent
        // buckets when the hash table is resized.
        (hash >> self.hash_offset) as usize
    }

    /// Returns the minimum capacity.
    #[inline]
    pub const fn minimum_capacity() -> usize {
        BUCKET_LEN << 1
    }

    /// Returns a reference to its rehashing metadata.
    #[inline]
    pub(crate) const fn rehashing_metadata(&self) -> &AtomicUsize {
        &self.num_cleared_buckets
    }

    /// Checks if the bucket is allowed to initiate sampling.
    #[inline]
    pub(crate) const fn initiate_sampling(&self, index: usize) -> bool {
        (index & self.sample_size_mask as usize) == 0
    }

    /// Returns the recommended sampling size.
    #[inline]
    pub(crate) const fn sample_size(&self) -> usize {
        (self.sample_size_mask + 1) as usize
    }

    /// Returns a reference to a [`Bucket`] at the given position.
    #[inline]
    pub(crate) const fn bucket(&self, index: usize) -> &Bucket<K, V, L, TYPE> {
        debug_assert!(index < self.len());
        unsafe { self.buckets.add(index).as_ref() }
    }

    /// Returns a pointer to a [`DataBlock`] at the given position.
    #[inline]
    pub(crate) const fn data_block(&self, index: usize) -> NonNull<DataBlock<K, V, BUCKET_LEN>> {
        debug_assert!(index < self.len());
        unsafe { self.data_blocks.add(index) }
    }

    /// Returns a reference to the old array pointer.
    #[inline]
    pub(crate) const fn bucket_link(&self) -> &AtomicShared<BucketArray<K, V, L, TYPE>> {
        &self.old_array
    }

    /// Returns a reference to the garbage array pointer.
    #[inline]
    pub(crate) const fn garbage_link(&self) -> &AtomicShared<BucketArray<K, V, L, TYPE>> {
        &self.garbage_array
    }

    /// Returns the recommended sampling size.
    #[inline]
    pub(crate) fn full_sample_size(&self) -> usize {
        (self.sample_size() * self.sample_size()).min(self.len())
    }

    /// Returns `true` if the old array exists.
    #[inline]
    pub(crate) fn has_old_array(&self) -> bool {
        !self.old_array.is_null(Acquire)
    }

    /// Returns a [`Ptr`] to the old array.
    #[inline]
    pub(crate) fn old_array<'g>(&self, guard: &'g Guard) -> Ptr<'g, BucketArray<K, V, L, TYPE>> {
        self.old_array.load(Acquire, guard)
    }

    /// Calculates the layout of the memory block for an array of `T`.
    #[inline]
    const fn bucket_array_layout(array_len: usize) -> (usize, usize, Layout) {
        let size_of_t = size_of::<Bucket<K, V, L, TYPE>>();
        let aligned_size = size_of_t.next_power_of_two();
        let allocation_size = aligned_size + array_len * size_of_t;
        (size_of_t, allocation_size, unsafe {
            // Intentionally misaligned in order to take full advantage of demand paging.
            Layout::from_size_align_unchecked(allocation_size, 1)
        })
    }

    /// Calculates log₂ of the array size from the given capacity.
    ///
    /// Returns a non-zero `u8`, even when `capacity < 2 * BUCKET_LEN`.
    #[inline]
    fn calculate_log2_array_size(capacity: usize) -> u8 {
        let adjusted_capacity = capacity.clamp(64, (usize::MAX / 2) - (BUCKET_LEN - 1));
        let required_buckets = adjusted_capacity.div_ceil(BUCKET_LEN).next_power_of_two();
        let log2_capacity = usize::BITS as usize - (required_buckets.leading_zeros() as usize) - 1;

        // `2^log2_capacity * BUCKET_LEN >= capacity`.
        debug_assert!(log2_capacity < (usize::BITS as usize));
        debug_assert!((1_usize << log2_capacity) * BUCKET_LEN >= adjusted_capacity);
        u8::try_from(log2_capacity).unwrap_or(0)
    }
}

impl<K, V, L: LruList, const TYPE: char> Drop for BucketArray<K, V, L, TYPE> {
    fn drop(&mut self) {
        if !self.old_array.is_null(Relaxed) {
            unsafe {
                self.old_array
                    .swap((None, Tag::None), Relaxed)
                    .0
                    .map(|a| a.drop_in_place());
            }
        }

        let num_cleared_buckets = if TYPE == INDEX {
            0
        } else {
            self.num_cleared_buckets.load(Relaxed)
        };
        if num_cleared_buckets < self.array_len {
            for index in num_cleared_buckets..self.array_len {
                self.bucket(index).drop_entries(self.data_block(index));
            }
        }

        unsafe {
            dealloc(
                self.buckets
                    .cast::<u8>()
                    .sub(self.bucket_ptr_offset as usize)
                    .as_ptr(),
                Self::bucket_array_layout(self.array_len).2,
            );
            dealloc(
                self.data_blocks.cast::<u8>().as_ptr(),
                Layout::from_size_align(
                    size_of::<DataBlock<K, V, BUCKET_LEN>>() * self.array_len,
                    align_of::<[DataBlock<K, V, BUCKET_LEN>; 0]>(),
                )
                .unwrap(),
            );
        }
    }
}

unsafe impl<K: Send, V: Send, L: LruList, const TYPE: char> Send for BucketArray<K, V, L, TYPE> {}

unsafe impl<K: Send + Sync, V: Send + Sync, L: LruList, const TYPE: char> Sync
    for BucketArray<K, V, L, TYPE>
{
}

impl<K: UnwindSafe, V: UnwindSafe, L: LruList, const TYPE: char> UnwindSafe
    for BucketArray<K, V, L, TYPE>
{
}

#[cfg(not(feature = "loom"))]
#[cfg(test)]
mod test {
    use super::*;
    use crate::hash_table::bucket::INDEX;

    #[test]
    fn array() {
        for s in 0..BUCKET_LEN * 4 {
            let array: BucketArray<usize, usize, (), INDEX> =
                BucketArray::new(s, AtomicShared::default());
            assert!(
                array.len() >= s.max(1).div_ceil(BUCKET_LEN),
                "{s} {}",
                array.len()
            );
            assert!(
                array.len() <= 2 * (s.max(1) + BUCKET_LEN - 1) / BUCKET_LEN,
                "{s} {}",
                array.len()
            );
            assert!(
                array.full_sample_size() <= array.len(),
                "{} {}",
                array.full_sample_size(),
                array.len()
            );
            assert!(array.num_slots() >= s, "{s} {}", array.num_slots());
            array.num_cleared_buckets.store(array.array_len, Relaxed);
        }
    }
}
