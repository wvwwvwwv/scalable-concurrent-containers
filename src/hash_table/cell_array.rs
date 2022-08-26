use super::cell::{Cell, DataBlock, Locker, CELL_LEN};

use crate::ebr::{AtomicArc, Barrier, Ptr, Tag};
use crate::wait_queue::AsyncWait;

use std::alloc::{alloc, alloc_zeroed, dealloc, Layout};
use std::borrow::Borrow;
use std::hash::Hash;
use std::mem::{align_of, size_of};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

/// [`CellArray`] is a special purpose array being initialized by zero.
pub struct CellArray<K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    cell_array_ptr: *const Cell<K, V, LOCK_FREE>,
    data_block_array_ptr: *const DataBlock<K, V>,
    array_len: usize,
    hash_offset: u16,
    sample_size: u16,
    cell_array_ptr_offset: u32,
    old_array: AtomicArc<CellArray<K, V, LOCK_FREE>>,
    next_cell_to_kill: AtomicUsize,
}

impl<K: 'static + Eq, V: 'static, const LOCK_FREE: bool> CellArray<K, V, LOCK_FREE> {
    /// Creates a new [`CellArray`] of the given capacity.
    ///
    /// `total_cell_capacity` is the desired number entries, not the number of [`Cell`] instances.
    #[inline]
    pub(crate) fn new(
        total_cell_capacity: usize,
        old_array: AtomicArc<CellArray<K, V, LOCK_FREE>>,
    ) -> CellArray<K, V, LOCK_FREE> {
        let log2_array_len = Self::calculate_log2_array_size(total_cell_capacity);
        let array_len = 1_usize << log2_array_len;
        unsafe {
            let (cell_size, cell_array_allocation_size, cell_array_layout) =
                Self::calculate_memory_layout::<Cell<K, V, LOCK_FREE>>(array_len);
            let cell_array_ptr = alloc_zeroed(cell_array_layout);
            assert!(
                !cell_array_ptr.is_null(),
                "memory allocation failure: {} bytes",
                cell_array_allocation_size
            );
            let cell_array_ptr_offset = cell_array_ptr as usize % cell_size.next_power_of_two();
            assert!(cell_array_ptr_offset + cell_size * array_len <= cell_array_allocation_size,);

            #[allow(clippy::cast_ptr_alignment)]
            let cell_array_ptr = cell_array_ptr
                .add(cell_array_ptr_offset)
                .cast::<Cell<K, V, LOCK_FREE>>();
            #[allow(clippy::cast_possible_truncation)]
            let cell_array_ptr_offset = cell_array_ptr_offset as u32;

            let data_block_array_layout = Layout::from_size_align(
                size_of::<DataBlock<K, V>>() * array_len,
                align_of::<[DataBlock<K, V>; 0]>(),
            )
            .unwrap();

            let data_block_array_ptr = alloc(data_block_array_layout).cast::<DataBlock<K, V>>();
            assert!(
                !data_block_array_ptr.is_null(),
                "memory allocation failure: {} bytes",
                data_block_array_layout.size(),
            );

            CellArray {
                cell_array_ptr,
                data_block_array_ptr,
                array_len,
                hash_offset: 64 - u16::from(log2_array_len),
                sample_size: u16::from(log2_array_len).next_power_of_two(),
                cell_array_ptr_offset,
                old_array,
                next_cell_to_kill: AtomicUsize::new(0),
            }
        }
    }

    /// Returns a reference to a [`Cell`] at the given position.
    #[inline]
    pub(crate) fn cell(&self, index: usize) -> &Cell<K, V, LOCK_FREE> {
        debug_assert!(index < self.num_cells());
        unsafe { &(*(self.cell_array_ptr.add(index))) }
    }

    /// Returns a reference to a [`DataBlock`] at the given position.
    #[inline]
    pub(crate) fn data_block(&self, index: usize) -> &DataBlock<K, V> {
        debug_assert!(index < self.num_cells());
        unsafe { &(*(self.data_block_array_ptr.add(index))) }
    }

    /// Returns the recommended sampling size.
    #[inline]
    pub(crate) fn sample_size(&self) -> usize {
        self.sample_size as usize
    }

    /// Returns the number of [`Cell`] instances in the [`CellArray`].
    #[inline]
    pub(crate) fn num_cells(&self) -> usize {
        self.array_len
    }

    /// Returns the number of total entries.
    #[inline]
    pub(crate) fn num_entries(&self) -> usize {
        self.array_len * CELL_LEN
    }

    /// Returns a [`Ptr`] to the old array.
    #[inline]
    pub(crate) fn old_array<'b>(
        &self,
        barrier: &'b Barrier,
    ) -> Ptr<'b, CellArray<K, V, LOCK_FREE>> {
        self.old_array.load(Relaxed, barrier)
    }

    /// Calculates the [`Cell`] index for the hash value.
    #[allow(clippy::cast_possible_truncation)]
    #[inline]
    pub(crate) fn calculate_cell_index(&self, hash: u64) -> usize {
        hash.wrapping_shr(u32::from(self.hash_offset)) as usize
    }

    /// Kills the [`Cell`].
    ///
    /// It returns an error if locking failed.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn kill_cell<Q, F: Fn(&Q) -> (u64, u8), C: Fn(&K, &V) -> Option<(K, V)>>(
        &self,
        cell_locker: &mut Locker<K, V, LOCK_FREE>,
        old_array: &CellArray<K, V, LOCK_FREE>,
        old_cell_index: usize,
        hasher: &F,
        copier: &C,
        async_wait: Option<*mut AsyncWait>,
        barrier: &Barrier,
    ) -> Result<(), ()>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        if cell_locker.cell().killed() {
            return Ok(());
        } else if cell_locker.cell().num_entries() == 0 {
            cell_locker.purge(barrier);
            return Ok(());
        }

        let old_data_block = old_array.data_block(old_cell_index);
        let shrink = old_array.num_cells() > self.num_cells();
        let ratio = if shrink {
            old_array.num_cells() / self.num_cells()
        } else {
            self.num_cells() / old_array.num_cells()
        };
        let target_cell_index = if shrink {
            old_cell_index / ratio
        } else {
            debug_assert!(ratio <= 32);
            old_cell_index * ratio
        };

        let mut target_cells: [Option<Locker<K, V, LOCK_FREE>>; size_of::<usize>() * 4] =
            Default::default();
        let mut max_index = 0;
        let mut iter = cell_locker.cell().iter(barrier);
        while let Some(partial_hash) = iter.next() {
            let old_entry = iter.get(old_data_block);
            let new_cell_index = if shrink {
                debug_assert!(
                    self.calculate_cell_index(hasher(old_entry.0.borrow()).0) == target_cell_index
                );
                target_cell_index
            } else {
                let hash = hasher(old_entry.0.borrow()).0;
                let new_cell_index = self.calculate_cell_index(hash);
                debug_assert!((new_cell_index - target_cell_index) < ratio);
                new_cell_index
            };

            let offset = new_cell_index - target_cell_index;
            while max_index <= offset {
                let locker = if let Some(&async_wait) = async_wait.as_ref() {
                    Locker::try_lock_or_wait(
                        self.cell(max_index + target_cell_index),
                        async_wait,
                        barrier,
                    )?
                    .unwrap()
                } else {
                    Locker::lock(self.cell(max_index + target_cell_index), barrier).unwrap()
                };
                target_cells[max_index].replace(locker);
                max_index += 1;
            }

            let target_cell = target_cells[offset].as_ref().unwrap();
            let new_entry = if let Some(entry) = copier(&old_entry.0, &old_entry.1) {
                // HashIndex.
                debug_assert!(LOCK_FREE);
                entry
            } else {
                // HashMap.
                debug_assert!(!LOCK_FREE);
                cell_locker.extract(old_data_block, &mut iter)
            };
            target_cell.insert(
                self.data_block(target_cell_index + offset),
                new_entry.0,
                new_entry.1,
                partial_hash,
                barrier,
            );
        }
        cell_locker.purge(barrier);
        Ok(())
    }

    /// Relocates a fixed number of `Cells` from the old array to the current array.
    ///
    /// Returns `true` if `old_array` is null.
    pub(crate) fn partial_rehash<Q, F: Fn(&Q) -> (u64, u8), C: Fn(&K, &V) -> Option<(K, V)>>(
        &self,
        hasher: F,
        copier: C,
        async_wait: Option<*mut AsyncWait>,
        barrier: &Barrier,
    ) -> Result<bool, ()>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        if let Some(old_array_ref) = self.old_array(barrier).as_ref() {
            // Assign itself a range of `Cells` to rehash.
            //
            // Aside from the range, it increments the implicit reference counting field in
            // `old_array_ref.rehashing`.
            let old_array_size = old_array_ref.num_cells();
            let mut current = old_array_ref.next_cell_to_kill.load(Relaxed);
            loop {
                if current >= old_array_size || (current & (CELL_LEN - 1)) == CELL_LEN - 1 {
                    // Only `CELL_LEN - 1` threads are allowed to rehash `Cells` atl
                    // a moment.
                    return Ok(self.old_array.is_null(Relaxed));
                }
                match old_array_ref.next_cell_to_kill.compare_exchange(
                    current,
                    current + CELL_LEN + 1,
                    Relaxed,
                    Relaxed,
                ) {
                    Ok(_) => {
                        current &= !(CELL_LEN - 1);
                        break;
                    }
                    Err(result) => current = result,
                }
            }

            // The guard ensures dropping one reference in `old_array_ref.rehashing`.
            let mut rehashing_guard = scopeguard::guard((current, false), |(prev, success)| {
                if success {
                    // Keep the index as it is.
                    let current = old_array_ref.next_cell_to_kill.fetch_sub(1, Relaxed) - 1;
                    if (current & (CELL_LEN - 1) == 0) && current >= old_array_size {
                        // The last one trying to relocate old entries gets rid of the old array.
                        if let Some(old_array) = self.old_array.swap((None, Tag::None), Relaxed).0 {
                            old_array.release(barrier);
                        }
                    }
                } else {
                    // On failure, `rehashing` reverts to its previous state.
                    let mut current = old_array_ref.next_cell_to_kill.load(Relaxed);
                    loop {
                        let new = if current <= prev {
                            current - 1
                        } else {
                            let ref_cnt = current & (CELL_LEN - 1);
                            prev | (ref_cnt - 1)
                        };
                        match old_array_ref
                            .next_cell_to_kill
                            .compare_exchange(current, new, Relaxed, Relaxed)
                        {
                            Ok(_) => break,
                            Err(actual) => current = actual,
                        }
                    }
                }
            });

            for old_cell_index in current..(current + CELL_LEN).min(old_array_size) {
                let old_cell = old_array_ref.cell(old_cell_index);
                let lock_result = if let Some(&async_wait) = async_wait.as_ref() {
                    Locker::try_lock_or_wait(old_cell, async_wait, barrier)?
                } else {
                    Locker::lock(old_cell, barrier)
                };
                if let Some(mut locker) = lock_result {
                    self.kill_cell::<Q, F, C>(
                        &mut locker,
                        old_array_ref,
                        old_cell_index,
                        &hasher,
                        &copier,
                        async_wait,
                        barrier,
                    )?;
                }
            }

            // Successfully rehashed all the `Cells` in the range.
            (*rehashing_guard).1 = true;
        }
        Ok(self.old_array.is_null(Relaxed))
    }

    /// Calculates `log_2` of the array size from the given cell capacity.
    #[allow(clippy::cast_possible_truncation)]
    fn calculate_log2_array_size(total_cell_capacity: usize) -> u8 {
        let adjusted_total_cell_capacity =
            total_cell_capacity.min((usize::MAX / 2) - (CELL_LEN - 1));
        let required_cells =
            ((adjusted_total_cell_capacity + CELL_LEN - 1) / CELL_LEN).next_power_of_two();
        let log2_capacity =
            (usize::BITS as usize - (required_cells.leading_zeros() as usize) - 1).max(1);

        // 2^lb_capacity * C::cell_size() >= capacity
        debug_assert!(log2_capacity > 0);
        debug_assert!(log2_capacity < (std::mem::size_of::<usize>() * 8));
        debug_assert!((1_usize << log2_capacity) * CELL_LEN >= adjusted_total_cell_capacity);
        log2_capacity as u8
    }

    /// Calculates the layout of the memory block for an array of [`Cell`].
    fn calculate_memory_layout<T: Sized>(array_capacity: usize) -> (usize, usize, Layout) {
        let size_of_cell = size_of::<T>();
        let aligned_size = size_of_cell.next_power_of_two();
        let allocation_size = aligned_size + array_capacity * size_of_cell;
        (size_of_cell, allocation_size, unsafe {
            Layout::from_size_align_unchecked(allocation_size, 1)
        })
    }

    /// Deallocates data arrays.
    fn dealloc_arrays(
        cell_array_ptr: *const Cell<K, V, LOCK_FREE>,
        cell_array_ptr_offset: u32,
        data_block_array_ptr: *const DataBlock<K, V>,
        len: usize,
    ) {
        unsafe {
            dealloc(
                (cell_array_ptr as *mut Cell<K, V, LOCK_FREE>)
                    .cast::<u8>()
                    .sub(cell_array_ptr_offset as usize),
                Self::calculate_memory_layout::<Cell<K, V, LOCK_FREE>>(len).2,
            );
            dealloc(
                (data_block_array_ptr as *mut DataBlock<K, V>).cast::<u8>(),
                Layout::from_size_align(
                    size_of::<DataBlock<K, V>>() * len,
                    align_of::<[DataBlock<K, V>; 0]>(),
                )
                .unwrap(),
            );
        }
    }
}

impl<K: Eq, V, const LOCK_FREE: bool> Drop for CellArray<K, V, LOCK_FREE> {
    fn drop(&mut self) {
        const JOB_SIZE: usize = 1_usize << 12;

        let num_cleared_cells = if LOCK_FREE {
            // No instances are dropped when the array is reachable.
            0
        } else {
            self.next_cell_to_kill.load(Relaxed)
        };
        if num_cleared_cells < self.array_len {
            let barrier = Barrier::new();
            let end_index = self
                .array_len
                .min((num_cleared_cells + JOB_SIZE).next_power_of_two());
            for index in num_cleared_cells..end_index {
                let cell = self.cell(index);
                if LOCK_FREE || !cell.killed() {
                    unsafe {
                        cell.drop_entries(self.data_block(index), &barrier);
                    }
                }
            }

            // If clearing entries was not finished, defer the job.
            if end_index != self.array_len {
                // This allocates heap memory, and may lead to an out-of-memory error or panic.
                let _unwind_guard = scopeguard::guard_on_unwind((), |_| {
                    for index in end_index..self.array_len {
                        let cell = self.cell(index);
                        if LOCK_FREE || !cell.killed() {
                            unsafe {
                                cell.drop_entries(self.data_block(index), &barrier);
                            }
                        }
                    }
                    Self::dealloc_arrays(
                        self.cell_array_ptr,
                        self.cell_array_ptr_offset,
                        self.data_block_array_ptr,
                        self.array_len,
                    );
                });

                let cell_array_ptr_val = self.cell_array_ptr as usize;
                let cell_array_ptr_offset = self.cell_array_ptr_offset;
                let data_block_array_ptr_val = self.data_block_array_ptr as usize;
                let array_len = self.array_len;
                let mut num_cleared_cells = end_index;
                barrier.defer_incremental_execute(move || {
                    let barrier = Barrier::new();
                    let cell_array_ptr = cell_array_ptr_val as *const Cell<K, V, LOCK_FREE>;
                    let data_block_array_ptr = data_block_array_ptr_val as *const DataBlock<K, V>;
                    let end_index = array_len.min(num_cleared_cells + JOB_SIZE);
                    for index in num_cleared_cells..end_index {
                        let cell = unsafe { &(*(cell_array_ptr.add(index))) };
                        if LOCK_FREE || !cell.killed() {
                            unsafe {
                                cell.drop_entries(&(*(data_block_array_ptr.add(index))), &barrier);
                            }
                        }
                    }
                    if end_index == array_len {
                        Self::dealloc_arrays(
                            cell_array_ptr,
                            cell_array_ptr_offset,
                            data_block_array_ptr,
                            array_len,
                        );
                        true
                    } else {
                        num_cleared_cells = end_index;
                        false
                    }
                });

                // Do not deallocate its arrays.
                return;
            }
        }
        Self::dealloc_arrays(
            self.cell_array_ptr,
            self.cell_array_ptr_offset,
            self.data_block_array_ptr,
            self.array_len,
        );
    }
}

unsafe impl<K, V, const LOCK_FREE: bool> Send for CellArray<K, V, LOCK_FREE>
where
    K: 'static + Eq + Send,
    V: 'static + Send,
{
}

unsafe impl<K, V, const LOCK_FREE: bool> Sync for CellArray<K, V, LOCK_FREE>
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
        let array: CellArray<usize, usize, true> =
            CellArray::new(1024 * 1024 * 32, AtomicArc::default());
        assert_eq!(array.num_cells(), 1024 * 1024);
        let after_alloc = Instant::now();
        println!("allocation took {:?}", after_alloc - start);
        drop(array);
        let after_dealloc = Instant::now();
        println!("deallocation took {:?}", after_dealloc - after_alloc);
    }

    #[test]
    fn array() {
        for s in 0..CELL_LEN * 2 {
            let array: CellArray<usize, usize, true> = CellArray::new(s, AtomicArc::default());
            assert!(array.num_cells() >= s.max(CELL_LEN) / CELL_LEN);
            assert!(array.num_cells() <= 2 * (s.max(CELL_LEN) / CELL_LEN));
            assert!(array.num_entries() >= s.max(CELL_LEN));
            assert!(array.num_entries() <= 2 * s.max(CELL_LEN));
        }
    }
}
