use super::cell::{Cell, Locker, ARRAY_SIZE};

use crate::ebr::{AtomicArc, Barrier, Ptr, Tag};

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::borrow::Borrow;
use std::convert::TryInto;
use std::hash::Hash;
use std::mem::size_of;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicUsize};

/// [`CellArray`] is a special purpose array being initialized by zero.
pub struct CellArray<K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    array_ptr: *const Cell<K, V, LOCK_FREE>,
    array_ptr_offset: usize,
    array_capacity: usize,
    log2_capacity: u8,
    cleared: AtomicBool,
    old_array: AtomicArc<CellArray<K, V, LOCK_FREE>>,
    rehashing: AtomicUsize,
}

impl<K: 'static + Eq, V: 'static, const LOCK_FREE: bool> CellArray<K, V, LOCK_FREE> {
    /// The number of `Cells` a task has to relocate on a single call to `partial_rehash`.
    const UNIT_SIZE: usize = ARRAY_SIZE;

    /// Creates a new Array of given capacity.
    ///
    /// `total_cell_capacity` is the desired number entries, not the number of [`Cell`]
    /// instances.
    pub fn new(
        total_cell_capacity: usize,
        old_array: AtomicArc<CellArray<K, V, LOCK_FREE>>,
    ) -> CellArray<K, V, LOCK_FREE> {
        let log2_capacity = Self::calculate_log2_array_size(total_cell_capacity);
        let array_capacity = 1_usize << log2_capacity;
        unsafe {
            let (cell_size, allocation_size, layout) = Self::calculate_layout(array_capacity);
            let ptr = alloc_zeroed(layout);
            assert!(
                !ptr.is_null(),
                "memory allocation failure: {} bytes",
                allocation_size
            );
            let mut array_ptr_offset = ptr.align_offset(cell_size.next_power_of_two());
            if array_ptr_offset == usize::MAX {
                array_ptr_offset = 0;
            }
            assert!(array_ptr_offset + cell_size * array_capacity <= allocation_size,);
            #[allow(clippy::cast_ptr_alignment)]
            let array_ptr = ptr.add(array_ptr_offset).cast::<Cell<K, V, LOCK_FREE>>();
            CellArray {
                array_ptr,
                array_ptr_offset,
                array_capacity,
                log2_capacity,
                cleared: AtomicBool::new(false),
                old_array,
                rehashing: AtomicUsize::new(0),
            }
        }
    }

    /// Returns a reference to a [`Cell`] at the given position.
    #[inline]
    pub fn cell(&self, index: usize) -> &Cell<K, V, LOCK_FREE> {
        debug_assert!(index < self.num_cells());
        unsafe { &(*(self.array_ptr.add(index))) }
    }

    /// Returns the recommended sampling size.
    #[inline]
    pub fn sample_size(&self) -> usize {
        (self.log2_capacity as usize).next_power_of_two()
    }

    /// Returns the number of [`Cell`] instances in the [`CellArray`].
    #[inline]
    pub fn num_cells(&self) -> usize {
        self.array_capacity
    }

    /// Returns the number of total entries.
    #[inline]
    pub fn num_entries(&self) -> usize {
        self.array_capacity * ARRAY_SIZE
    }

    /// Returns a [`Ptr`] to the old array.
    #[inline]
    pub fn old_array<'b>(&self, barrier: &'b Barrier) -> Ptr<'b, CellArray<K, V, LOCK_FREE>> {
        self.old_array.load(Relaxed, barrier)
    }

    /// Calculates the [`Cell`] index for the hash value.
    #[inline]
    pub fn calculate_cell_index(&self, hash: u64) -> usize {
        (hash >> (64 - self.log2_capacity)).try_into().unwrap()
    }

    /// Kills the [`Cell`].
    ///
    /// It returns an error if locking failed.
    pub fn kill_cell<
        Q,
        F: Fn(&Q) -> (u64, u8),
        C: Fn(&K, &V) -> Option<(K, V)>,
        const TRY_LOCK: bool,
    >(
        &self,
        cell_locker: &mut Locker<K, V, LOCK_FREE>,
        old_array: &CellArray<K, V, LOCK_FREE>,
        old_cell_index: usize,
        hasher: &F,
        copier: &C,
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
        while let Some(entry) = iter.next() {
            let (new_cell_index, partial_hash) = if shrink {
                debug_assert!(
                    self.calculate_cell_index(hasher(entry.0 .0.borrow()).0) == target_cell_index
                );
                (target_cell_index, entry.1)
            } else {
                let (hash, partial_hash) = hasher(entry.0 .0.borrow());
                let new_cell_index = self.calculate_cell_index(hash);
                debug_assert!((new_cell_index - target_cell_index) < ratio);
                (new_cell_index, partial_hash)
            };

            let offset = new_cell_index - target_cell_index;
            while max_index <= offset {
                let locker = if TRY_LOCK {
                    Locker::try_lock(self.cell(max_index + target_cell_index), barrier)?.unwrap()
                } else {
                    Locker::lock(self.cell(max_index + target_cell_index), barrier).unwrap()
                };
                target_cells[max_index].replace(locker);
                max_index += 1;
            }

            let target_cell = target_cells[offset].as_ref().unwrap();
            let new_entry = if let Some(entry) = copier(&entry.0 .0, &entry.0 .1) {
                // HashIndex.
                debug_assert!(LOCK_FREE);
                entry
            } else {
                // HashMap.
                debug_assert!(!LOCK_FREE);
                cell_locker.extract(&mut iter)
            };
            target_cell.insert(new_entry.0, new_entry.1, partial_hash, barrier);
        }
        cell_locker.purge(barrier);
        Ok(())
    }

    /// Relocates a fixed number of `Cells` from the old array to the current array.
    ///
    /// Returns `true` if `old_array` is null.
    pub fn partial_rehash<
        Q,
        F: Fn(&Q) -> (u64, u8),
        C: Fn(&K, &V) -> Option<(K, V)>,
        const TRY_LOCK: bool,
    >(
        &self,
        hasher: F,
        copier: C,
        barrier: &Barrier,
    ) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        if let Some(old_array_ref) = self.old_array(barrier).as_ref() {
            // Assigns itself a range of `Cells` to rehash.
            //
            // Aside from the range, it increments the implicit reference counting field in
            // `old_array_ref.rehashing`.
            let old_array_size = old_array_ref.num_cells();
            let mut current = old_array_ref.rehashing.load(Relaxed);
            loop {
                if current >= old_array_size
                    || (current & (Self::UNIT_SIZE - 1)) == Self::UNIT_SIZE - 1
                {
                    // Only `UNIT_SIZE - 1` tasks are allowed to rehash `Cells` at a moment.
                    return self.old_array.is_null(Relaxed);
                }
                match old_array_ref.rehashing.compare_exchange(
                    current,
                    current + Self::UNIT_SIZE + 1,
                    Relaxed,
                    Relaxed,
                ) {
                    Ok(_) => {
                        current &= !(Self::UNIT_SIZE - 1);
                        break;
                    }
                    Err(result) => current = result,
                }
            }

            // The guard ensures dropping one reference in `old_array_ref.rehashing`.
            let mut rehashing_guard = scopeguard::guard((current, false), |(prev, success)| {
                if success {
                    // Keeps the index as it is.
                    let old = old_array_ref.rehashing.fetch_sub(1, Relaxed);
                    if (old & (Self::UNIT_SIZE - 1) == 1) && old >= old_array_size {
                        // The last one trying to relocate old entries gets rid of the old array.
                        if let Some(old_array) = self.old_array.swap((None, Tag::None), Relaxed) {
                            old_array.cleared.store(true, Relaxed);
                            barrier.reclaim(old_array);
                        }
                    }
                } else {
                    // On failure, `rehashing` reverts to its previous state.
                    let mut current = old_array_ref.rehashing.load(Relaxed);
                    loop {
                        let new = if current <= prev {
                            current - 1
                        } else {
                            let ref_cnt = current & (Self::UNIT_SIZE - 1);
                            prev | (ref_cnt - 1)
                        };
                        match old_array_ref
                            .rehashing
                            .compare_exchange(current, new, Relaxed, Relaxed)
                        {
                            Ok(_) => break,
                            Err(actual) => current = actual,
                        }
                    }
                }
            });

            for old_cell_index in current..(current + Self::UNIT_SIZE).min(old_array_size) {
                let old_cell_ref = old_array_ref.cell(old_cell_index);
                let lock_result = if TRY_LOCK {
                    if let Ok(locker) = Locker::try_lock(old_cell_ref, barrier) {
                        locker
                    } else {
                        return false;
                    }
                } else {
                    Locker::lock(old_cell_ref, barrier)
                };
                if let Some(mut locker) = lock_result {
                    if self
                        .kill_cell::<Q, F, C, TRY_LOCK>(
                            &mut locker,
                            old_array_ref,
                            old_cell_index,
                            &hasher,
                            &copier,
                            barrier,
                        )
                        .is_err()
                    {
                        return false;
                    }
                }
            }

            // Successfully rehashed all the `Cells` in the range.
            (*rehashing_guard).1 = true;
        }
        self.old_array.is_null(Relaxed)
    }

    /// Calculates `log_2` of the array size from the given cell capacity.
    fn calculate_log2_array_size(total_cell_capacity: usize) -> u8 {
        let adjusted_total_cell_capacity =
            total_cell_capacity.min((usize::MAX / 2) - (ARRAY_SIZE - 1));
        let required_cells =
            ((adjusted_total_cell_capacity + ARRAY_SIZE - 1) / ARRAY_SIZE).next_power_of_two();
        let log2_capacity =
            (usize::BITS as usize - (required_cells.leading_zeros() as usize) - 1).max(1);

        // 2^lb_capacity * C::cell_size() >= capacity
        debug_assert!(log2_capacity > 0);
        debug_assert!(log2_capacity < (std::mem::size_of::<usize>() * 8));
        debug_assert!((1_usize << log2_capacity) * ARRAY_SIZE >= adjusted_total_cell_capacity);
        log2_capacity.try_into().unwrap()
    }

    /// Calculates the layout of the memory block.
    fn calculate_layout(array_capacity: usize) -> (usize, usize, Layout) {
        let size_of_cell = size_of::<Cell<K, V, LOCK_FREE>>();
        let aligned_size = size_of_cell.next_power_of_two();
        let allocation_size = aligned_size + array_capacity * size_of_cell;
        (size_of_cell, allocation_size, unsafe {
            Layout::from_size_align_unchecked(allocation_size, 1)
        })
    }
}

impl<K: Eq, V, const LOCK_FREE: bool> Drop for CellArray<K, V, LOCK_FREE> {
    fn drop(&mut self) {
        if !self.cleared.load(Relaxed) {
            let barrier = Barrier::new();
            for index in 0..self.num_cells() {
                let cell_ref = self.cell(index);
                if !cell_ref.killed() {
                    unsafe {
                        cell_ref.kill_and_drop(&barrier);
                    }
                }
            }
        }
        unsafe {
            dealloc(
                (self.array_ptr as *mut Cell<K, V, LOCK_FREE>)
                    .cast::<u8>()
                    .sub(self.array_ptr_offset),
                Self::calculate_layout(self.array_capacity).2,
            );
        }
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

    #[test]
    fn array() {
        for s in 0..ARRAY_SIZE * 2 {
            let array: CellArray<usize, usize, true> = CellArray::new(s, AtomicArc::default());
            assert!(array.num_cells() >= s.max(ARRAY_SIZE) / ARRAY_SIZE);
            assert!(array.num_cells() <= 2 * (s.max(ARRAY_SIZE) / ARRAY_SIZE));
            assert!(array.num_entries() >= s.max(ARRAY_SIZE));
            assert!(array.num_entries() <= 2 * s.max(ARRAY_SIZE));
        }
    }
}
