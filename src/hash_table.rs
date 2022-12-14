pub mod cell;
pub mod cell_array;

use cell::{DataBlock, EntryPtr, Locker, Reader, CELL_LEN};
use cell_array::CellArray;

use crate::ebr::{Arc, AtomicArc, Barrier, Tag};
use crate::wait_queue::AsyncWait;

use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash, Hasher};
use std::ptr::{self, NonNull};
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// `HashTable` defines common functions for hash table implementations.
pub(super) trait HashTable<K, V, H, const LOCK_FREE: bool>
where
    K: 'static + Eq + Hash + Sync,
    V: 'static + Sync,
    H: BuildHasher,
{
    /// Returns the default capacity.
    #[inline]
    fn default_capacity() -> usize {
        CELL_LEN * 2
    }

    /// Returns the hash value of the given key.
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

    /// Copying function.
    fn copier(key: &K, val: &V) -> (K, V);

    /// Returns a reference to the [`CellArray`] pointer.
    fn cell_array(&self) -> &AtomicArc<CellArray<K, V, LOCK_FREE>>;

    /// Returns the minimum allowed capacity.
    fn minimum_capacity(&self) -> usize;

    /// Returns a reference to the resizing mutex.
    fn resize_mutex(&self) -> &AtomicU8;

    /// Returns a reference to the current array without checking the pointer.
    #[inline]
    fn current_array_unchecked<'b>(&self, barrier: &'b Barrier) -> &'b CellArray<K, V, LOCK_FREE> {
        // An acquire fence is required to correctly load the contents of the array.
        let current_array_ptr = self.cell_array().load(Acquire, barrier);
        unsafe { current_array_ptr.as_ref().unwrap_unchecked() }
    }

    /// Returns the number of entries.
    #[inline]
    fn num_entries(&self, barrier: &Barrier) -> usize {
        let current_array = self.current_array_unchecked(barrier);
        let mut num_entries = 0;
        for i in 0..current_array.num_cells() {
            num_entries += current_array.cell(i).num_entries();
        }
        let old_array_ptr = current_array.old_array(barrier);
        if let Some(old_array) = old_array_ptr.as_ref() {
            for i in 0..old_array.num_cells() {
                num_entries += old_array.cell(i).num_entries();
            }
        }
        num_entries
    }

    /// Returns the number of slots.
    #[inline]
    fn num_slots(&self, barrier: &Barrier) -> usize {
        let current_array = self.current_array_unchecked(barrier);
        current_array.num_entries()
    }

    /// Estimates the number of entries using the given number of cells.
    #[inline]
    fn estimate(
        array: &CellArray<K, V, LOCK_FREE>,
        sampling_index: usize,
        num_cells_to_sample: usize,
    ) -> usize {
        let mut num_entries = 0;
        let start = if sampling_index + num_cells_to_sample >= array.num_cells() {
            0
        } else {
            sampling_index
        };
        for i in start..(start + num_cells_to_sample) {
            num_entries += array.cell(i).num_entries();
        }
        num_entries * (array.num_cells() / num_cells_to_sample)
    }

    /// Checks whether rebuilding the entire hash table is required.
    #[inline]
    fn check_rebuild(
        array: &CellArray<K, V, LOCK_FREE>,
        sampling_index: usize,
        num_cells_to_sample: usize,
    ) -> bool {
        let mut num_cells_to_rebuild = 0;
        let start = if sampling_index + num_cells_to_sample >= array.num_cells() {
            0
        } else {
            sampling_index
        };
        for i in start..(start + num_cells_to_sample) {
            if array.cell(i).need_rebuild() {
                num_cells_to_rebuild += 1;
                if num_cells_to_rebuild > num_cells_to_sample / 2 {
                    return true;
                }
            }
        }
        false
    }

    /// Inserts an entry into the [`HashTable`].
    #[inline]
    fn insert_entry(
        &self,
        key: K,
        val: V,
        hash: u64,
        async_wait: Option<NonNull<AsyncWait>>,
        barrier: &Barrier,
    ) -> Result<Option<(K, V)>, (K, V)> {
        match self.acquire_entry::<_>(&key, hash, async_wait, barrier) {
            Ok((mut locker, data_block, entry_ptr)) => {
                if entry_ptr.is_valid() {
                    return Ok(Some((key, val)));
                }
                locker.insert(
                    data_block,
                    key,
                    val,
                    CellArray::<K, V, LOCK_FREE>::partial_hash(hash),
                    barrier,
                );
                Ok(None)
            }
            Err(_) => Err((key, val)),
        }
    }

    /// Reads an entry from the [`HashTable`].
    #[inline]
    fn read_entry<'b, Q>(
        &self,
        key: &Q,
        hash: u64,
        async_wait: Option<NonNull<AsyncWait>>,
        barrier: &'b Barrier,
    ) -> Result<Option<(&'b K, &'b V)>, ()>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let mut current_array = self.current_array_unchecked(barrier);
        loop {
            if let Some(old_array) = current_array.old_array(barrier).as_ref() {
                self.move_entry(current_array, old_array, hash, async_wait, barrier)?;
            };

            let cell_index = current_array.calculate_cell_index(hash);
            let cell = current_array.cell(cell_index);
            if LOCK_FREE {
                if let Some(entry) = cell.search(
                    current_array.data_block(cell_index),
                    key,
                    CellArray::<K, V, LOCK_FREE>::partial_hash(hash),
                    barrier,
                ) {
                    return Ok(Some((&entry.0, &entry.1)));
                }
            } else {
                let lock_result = if let Some(&async_wait) = async_wait.as_ref() {
                    Reader::try_lock_or_wait(cell, async_wait, barrier)?
                } else {
                    Reader::lock(cell, barrier)
                };
                if let Some(reader) = lock_result {
                    if let Some((key, value)) = reader.cell().search(
                        current_array.data_block(cell_index),
                        key,
                        CellArray::<K, V, LOCK_FREE>::partial_hash(hash),
                        barrier,
                    ) {
                        return Ok(Some((key, value)));
                    }
                }
            }

            let new_current_array = self.current_array_unchecked(barrier);
            if ptr::eq(current_array, new_current_array) {
                break;
            }

            // A new array has been installed.
            current_array = new_current_array;
        }

        Ok(None)
    }

    /// Removes an entry if the condition is met.
    #[inline]
    fn remove_entry<Q, F: FnMut(&V) -> bool>(
        &self,
        key: &Q,
        hash: u64,
        condition: &mut F,
        async_wait: Option<NonNull<AsyncWait>>,
        barrier: &Barrier,
    ) -> Result<(Option<(K, V)>, bool), ()>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        loop {
            // The reasoning behind this loop can be found in `acquire_entry`.
            let current_array = self.current_array_unchecked(barrier);
            let shrinkable = if let Some(old_array) = current_array.old_array(barrier).as_ref() {
                self.move_entry(current_array, old_array, hash, async_wait, barrier)?
            } else {
                true
            };

            let cell_index = current_array.calculate_cell_index(hash);
            let cell = current_array.cell_mut(cell_index);
            let lock_result = if let Some(&async_wait) = async_wait.as_ref() {
                Locker::try_lock_or_wait(cell, async_wait, barrier)?
            } else {
                Locker::lock(cell, barrier)
            };
            if let Some(mut locker) = lock_result {
                let data_block = current_array.data_block(cell_index);
                let mut entry_ptr = locker.get(
                    data_block,
                    key,
                    CellArray::<K, V, LOCK_FREE>::partial_hash(hash),
                    barrier,
                );
                if entry_ptr.is_valid() && condition(&entry_ptr.get(data_block).1) {
                    let result = locker.erase(data_block, &mut entry_ptr);
                    if shrinkable && cell_index % CELL_LEN == 0 {
                        if locker.cell().num_entries() < CELL_LEN / 16
                            && current_array.num_entries() > self.minimum_capacity()
                        {
                            drop(locker);
                            self.try_shrink(current_array, cell_index, barrier);
                        } else if LOCK_FREE && locker.cell().need_rebuild() {
                            drop(locker);
                            self.try_rebuild(current_array, cell_index, barrier);
                        }
                    }
                    return Ok((result, true));
                }
                return Ok((None, false));
            }
        }
    }

    /// Acquires a [`Locker`] and [`EntryPtr`] corresponding to the key.
    ///
    /// It returns an error if locking failed, or returns an [`EntryPtr`] if the key exists,
    /// otherwise `None` is returned.
    #[allow(clippy::type_complexity)]
    #[inline]
    fn acquire_entry<'b, Q>(
        &self,
        key: &Q,
        hash: u64,
        async_wait: Option<NonNull<AsyncWait>>,
        barrier: &'b Barrier,
    ) -> Result<
        (
            Locker<'b, K, V, LOCK_FREE>,
            &'b DataBlock<K, V, CELL_LEN>,
            EntryPtr<'b, K, V, LOCK_FREE>,
        ),
        (),
    >
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        // It is guaranteed that the thread reads a consistent snapshot of the current and old
        // array pair by a release memory barrier in the resize function, hence the following
        // procedure is correct.
        //  - The thread reads `self.array`, and it kills the target cell in the old array if
        //    there is one attached to it, and inserts the key into array.
        // There are two cases.
        //  1. The thread reads an old version of `self.array`.
        //    If there is another thread having read the latest version of `self.array`,
        //    trying to insert the same key, it will try to kill the Cell in the old version
        //    of `self.array`, thus competing with each other.
        //  2. The thread reads the latest version of `self.array`.
        //    If the array is deprecated while inserting the key, it falls into case 1.
        loop {
            // An acquire fence is required to correctly load the contents of the array.
            let current_array = self.current_array_unchecked(barrier);
            if let Some(old_array) = current_array.old_array(barrier).as_ref() {
                self.move_entry(current_array, old_array, hash, async_wait, barrier)?;
            }

            let cell_index = current_array.calculate_cell_index(hash);
            let cell = current_array.cell_mut(cell_index);

            // Try to resize the array.
            if cell_index % CELL_LEN == 0 && cell.num_entries() >= CELL_LEN - 1 {
                self.try_enlarge(current_array, cell_index, cell.num_entries(), barrier);
            }

            let lock_result = if let Some(&async_wait) = async_wait.as_ref() {
                Locker::try_lock_or_wait(cell, async_wait, barrier)?
            } else {
                Locker::lock(cell, barrier)
            };
            if let Some(locker) = lock_result {
                let data_block = current_array.data_block(cell_index);
                let entry_ptr = locker.get(
                    data_block,
                    key,
                    CellArray::<K, V, LOCK_FREE>::partial_hash(hash),
                    barrier,
                );
                return Ok((locker, data_block, entry_ptr));
            }

            // Reaching here means that `self.array` is updated.
        }
    }

    /// Moves an entry in the old array to the current one.
    ///
    /// Returns `true` if no old array is attached to the current one.
    #[inline]
    fn move_entry<Q>(
        &self,
        current_array: &CellArray<K, V, LOCK_FREE>,
        old_array: &CellArray<K, V, LOCK_FREE>,
        hash: u64,
        async_wait: Option<NonNull<AsyncWait>>,
        barrier: &Barrier,
    ) -> Result<bool, ()>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if !current_array.partial_rehash::<Q, _, _>(
            |key| self.hash(key),
            Self::copier,
            async_wait,
            barrier,
        )? {
            let cell_index = old_array.calculate_cell_index(hash);
            let cell = old_array.cell_mut(cell_index);
            let lock_result = if let Some(&async_wait) = async_wait.as_ref() {
                Locker::try_lock_or_wait(cell, async_wait, barrier)?
            } else {
                Locker::lock(cell, barrier)
            };
            if let Some(mut locker) = lock_result {
                current_array.kill_cell::<Q, _, _>(
                    &mut locker,
                    cell_index,
                    old_array,
                    &|key| self.hash(key),
                    &Self::copier,
                    async_wait,
                    barrier,
                )?;
            }
            return Ok(false);
        }
        Ok(true)
    }

    /// Tries to enlarge the array if the estimated load factor is greater than `7/8`.
    #[inline]
    fn try_enlarge(
        &self,
        array: &CellArray<K, V, LOCK_FREE>,
        cell_index: usize,
        mut num_entries: usize,
        barrier: &Barrier,
    ) {
        let sample_size = array.sample_size();
        let threshold = sample_size * (CELL_LEN / 8) * 7;
        if num_entries > threshold
            || (1..sample_size).any(|i| {
                num_entries += array
                    .cell((cell_index + i) % array.num_cells())
                    .num_entries();
                num_entries > threshold
            })
        {
            self.resize(barrier);
        }
    }

    /// Tries to shrink the array if the load factor is estimated at around `1/16`.
    #[inline]
    fn try_shrink(&self, array: &CellArray<K, V, LOCK_FREE>, cell_index: usize, barrier: &Barrier) {
        let sample_size = array.sample_size();
        let threshold = sample_size * CELL_LEN / 16;
        let mut num_entries = 0;
        if !(1..sample_size).any(|i| {
            num_entries += array
                .cell((cell_index + i) % array.num_cells())
                .num_entries();
            num_entries >= threshold
        }) {
            self.resize(barrier);
        }
    }

    /// Tries to rebuild the array if there are no usable entry slots in at least half of the
    /// `Cells` in the sampling range.
    fn try_rebuild(
        &self,
        array: &CellArray<K, V, LOCK_FREE>,
        cell_index: usize,
        barrier: &Barrier,
    ) {
        let sample_size = array.sample_size();
        let threshold = sample_size / 2;
        let mut num_cells_to_rebuild = 1;
        if (1..sample_size).any(|i| {
            if array
                .cell((cell_index + i) % array.num_cells())
                .need_rebuild()
            {
                num_cells_to_rebuild += 1;
                num_cells_to_rebuild > threshold
            } else {
                false
            }
        }) {
            self.resize(barrier);
        }
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
                // Another thread is resizing the table, and needs to retry.
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
            let _mutex_guard = scopeguard::guard(&mut resize, |resize| {
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
            let num_cells = current_array.num_cells();
            let num_cells_to_sample = (num_cells / 8).clamp(2, 4096);
            let mut rebuild = false;
            let estimated_num_entries =
                Self::estimate(current_array, sampling_index, num_cells_to_sample);
            sampling_index = sampling_index.wrapping_add(num_cells_to_sample);
            let new_capacity = if estimated_num_entries >= (capacity / 8) * 7 {
                let max_capacity = 1_usize << (std::mem::size_of::<usize>() * 8 - 1);
                if capacity == max_capacity {
                    // Do not resize if the capacity cannot be increased.
                    capacity
                } else {
                    let mut new_capacity = capacity;
                    while new_capacity < (estimated_num_entries / 8) * 15 {
                        // Doubles the new capacity until it can accommodate the estimated number of entries * 15/8.
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
                // Shrinks to fit.
                estimated_num_entries
                    .next_power_of_two()
                    .max(self.minimum_capacity())
            } else {
                if LOCK_FREE {
                    rebuild =
                        Self::check_rebuild(current_array, sampling_index, num_cells_to_sample);
                }
                capacity
            };

            // `CellArray::new` may not be able to allocate the requested number of cells.
            if new_capacity != capacity || (LOCK_FREE && rebuild) {
                self.cell_array().swap(
                    (
                        Some(Arc::new(CellArray::<K, V, LOCK_FREE>::new(
                            new_capacity,
                            self.cell_array().clone(Relaxed, barrier),
                        ))),
                        Tag::None,
                    ),
                    Release,
                );
            }
        }
    }
}
