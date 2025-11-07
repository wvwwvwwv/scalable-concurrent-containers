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
}

impl<K, V, L: LruList, const TYPE: char> BucketArray<K, V, L, TYPE> {
    /// Creates a new [`BucketArray`] of the given capacity.
    ///
    /// `capacity` is the desired number of entries, not the length of the bucket array.
    pub(crate) fn new(
        capacity: usize,
        old_array: AtomicShared<BucketArray<K, V, L, TYPE>>,
    ) -> Self {
        let adjusted_capacity = capacity
            .min(1_usize << (usize::BITS - 1))
            .next_power_of_two()
            .max(Self::minimum_capacity());
        let array_len = adjusted_capacity / BUCKET_LEN;
        let log2_array_len =
            u16::try_from(usize::BITS - array_len.leading_zeros() - 1).unwrap_or(0);
        assert_eq!(1_usize << log2_array_len, array_len);

        let alignment = align_of::<Bucket<K, V, L, TYPE>>();
        let layout = Self::bucket_array_layout(array_len);

        unsafe {
            let Some(unaligned_bucket_array_ptr) = NonNull::new(alloc_zeroed(layout)) else {
                panic!("memory allocation failure: {layout:?}");
            };
            let bucket_array_ptr_offset = unaligned_bucket_array_ptr.align_offset(alignment);
            assert_eq!(
                (unaligned_bucket_array_ptr.addr().get() + bucket_array_ptr_offset) % alignment,
                0
            );

            #[allow(clippy::cast_ptr_alignment)] // The alignment was just asserted.
            let buckets = unaligned_bucket_array_ptr
                .add(bucket_array_ptr_offset)
                .cast::<Bucket<K, V, L, TYPE>>();
            let bucket_array_ptr_offset = u16::try_from(bucket_array_ptr_offset).unwrap_or(0);

            let data_block_array_layout = Layout::from_size_align(
                size_of::<DataBlock<K, V, BUCKET_LEN>>() * array_len,
                align_of::<[DataBlock<K, V, BUCKET_LEN>; 0]>(),
            )
            .unwrap();

            #[cfg(feature = "loom")]
            for i in 0..array_len {
                // `loom` types need proper initialization.
                buckets.add(i).write(Bucket::new());
            }

            // In case the below data block allocation fails, deallocate the bucket array.
            let mut alloc_guard = ExitGuard::new(false, |allocated| {
                if !allocated {
                    dealloc(unaligned_bucket_array_ptr.cast::<u8>().as_ptr(), layout);
                }
            });

            let Some(data_blocks) =
                NonNull::new(alloc(data_block_array_layout).cast::<DataBlock<K, V, BUCKET_LEN>>())
            else {
                panic!("memory allocation failure: {data_block_array_layout:?}");
            };
            *alloc_guard = true;

            Self {
                buckets,
                data_blocks,
                array_len,
                hash_offset: u16::try_from(u64::BITS).unwrap_or(64) - log2_array_len,
                sample_size_mask: log2_array_len.next_power_of_two() - 1,
                bucket_ptr_offset: bucket_array_ptr_offset,
                old_array,
                num_cleared_buckets: AtomicUsize::new(0),
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
    pub(crate) const fn minimum_capacity() -> usize {
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
    const fn bucket_array_layout(array_len: usize) -> Layout {
        let size_of_t = size_of::<Bucket<K, V, L, TYPE>>();
        let allocation_size = (array_len + 1) * size_of_t;
        // Intentionally misaligned in order to take full advantage of demand paging.
        unsafe { Layout::from_size_align_unchecked(allocation_size, 1) }
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

        #[cfg(feature = "loom")]
        for i in 0..self.array_len {
            // `loom` types need proper cleanup.
            drop(unsafe { self.buckets.add(i).read() });
        }

        unsafe {
            dealloc(
                self.buckets
                    .cast::<u8>()
                    .sub(self.bucket_ptr_offset as usize)
                    .as_ptr(),
                Self::bucket_array_layout(self.array_len),
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
