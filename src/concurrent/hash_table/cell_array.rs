use super::cell::{Cell, Locker, ARRAY_SIZE};

use crate::ebr::{AtomicArc, Barrier, Ptr, Tag};

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::borrow::Borrow;
use std::convert::TryInto;
use std::hash::Hash;
use std::mem::size_of;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Relaxed, Release};

/// [`CellArray`] is used by [`HashIndex`](crate::HashIndex) and [`HashMap`](crate::HashMap).
///
/// It is a special purpose array since it does not construct instances, instead only does it
/// allocate a large chunk of zeroed heap memory.
pub struct CellArray<K: 'static + Eq, V: 'static, const LOCK_FREE: bool> {
    array_ptr: *const Cell<K, V, LOCK_FREE>,
    array_ptr_offset: usize,
    array_capacity: usize,
    log2_capacity: u8,
    old_array: AtomicArc<CellArray<K, V, LOCK_FREE>>,
    rehashing: AtomicUsize,
    rehashed: AtomicUsize,
}

impl<K: 'static + Eq, V: 'static, const LOCK_FREE: bool> CellArray<K, V, LOCK_FREE> {
    /// Creates a new Array of given capacity.
    ///
    /// `total_cell_capacity` is the desired number of cell entries that the `CellArray `can
    /// accommodate. The given array instance is attached to the newly created Array instance.
    pub fn new(
        total_cell_capacity: usize,
        old_array: AtomicArc<CellArray<K, V, LOCK_FREE>>,
    ) -> CellArray<K, V, LOCK_FREE> {
        let log2_capacity = Self::calculate_lb_array_size(total_cell_capacity);
        let array_capacity = 1_usize << log2_capacity;
        unsafe {
            let size_of_cell = size_of::<Cell<K, V, LOCK_FREE>>();
            let allocation_size = (array_capacity + 1) * size_of_cell;
            let ptr = alloc_zeroed(Layout::from_size_align_unchecked(allocation_size, 1));
            assert!(
                !ptr.is_null(),
                "memory allocation failure: {} bytes",
                allocation_size
            );
            let mut array_ptr_offset = ptr.align_offset(size_of_cell.next_power_of_two());
            if array_ptr_offset == usize::MAX {
                array_ptr_offset = 0;
            }
            #[allow(clippy::cast_ptr_alignment)]
            let array_ptr = ptr.add(array_ptr_offset).cast::<Cell<K, V, LOCK_FREE>>();
            CellArray {
                array_ptr,
                array_ptr_offset,
                array_capacity,
                log2_capacity,
                old_array,
                rehashing: AtomicUsize::new(0),
                rehashed: AtomicUsize::new(0),
            }
        }
    }

    /// Returns a reference to a [`Cell`] at the given position.
    #[inline]
    pub fn cell(&self, index: usize) -> &Cell<K, V, LOCK_FREE> {
        unsafe { &(*(self.array_ptr.add(index))) }
    }

    /// Returns the recommended sampling size.
    #[inline]
    pub fn sample_size(&self) -> usize {
        (self.log2_capacity as usize).next_power_of_two()
    }

    /// Returns the number of `Cell` in the `CellArray`.
    #[inline]
    pub fn num_cells(&self) -> usize {
        self.array_capacity
    }

    /// Returns the number of total cell entries.
    #[inline]
    pub fn num_entries(&self) -> usize {
        self.array_capacity * ARRAY_SIZE
    }

    /// Returns a shared pointer to the old array.
    #[inline]
    pub fn old_array<'b>(&self, barrier: &'b Barrier) -> Ptr<'b, CellArray<K, V, LOCK_FREE>> {
        self.old_array.load(Relaxed, barrier)
    }

    /// Calculates the cell index for the hash value.
    #[inline]
    pub fn calculate_cell_index(&self, hash: u64) -> usize {
        (hash >> (64 - self.log2_capacity)).try_into().unwrap()
    }

    /// Drops the old array.
    pub fn drop_old_array(&self, barrier: &Barrier) {
        if let Some(old_array) = self.old_array.swap((None, Tag::None), Relaxed) {
            barrier.reclaim(old_array);
        }
    }

    /// Kills the Cell.
    pub fn kill_cell<Q, F: Fn(&Q) -> (u64, u8), C: Fn(&K, &V) -> Option<(K, V)>>(
        &self,
        cell_locker: &mut Locker<K, V, LOCK_FREE>,
        old_array: &CellArray<K, V, LOCK_FREE>,
        old_cell_index: usize,
        hasher: &F,
        copier: &C,
        barrier: &Barrier,
    ) where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        if cell_locker.cell_ref().killed() {
            return;
        } else if cell_locker.cell_ref().num_entries() == 0 {
            cell_locker.purge(barrier);
            return;
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

        let mut target_cells: [Option<Locker<K, V, LOCK_FREE>>; std::mem::size_of::<usize>() * 4] =
            Default::default();
        let mut inherited = usize::MAX;
        let mut max_index = 0;
        let mut iter = cell_locker.cell_ref().iter(barrier);
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
                target_cells[max_index].replace(
                    Locker::lock(self.cell(max_index + target_cell_index), barrier).unwrap(),
                );
                max_index += 1;
            }

            let target_cell = target_cells[offset].as_ref().unwrap();
            if !LOCK_FREE && inherited == usize::MAX {
                // Tries to move the entire data array of the old `Cell` to the new one.
                if target_cell.try_inherit(cell_locker, barrier) {
                    inherited = offset;
                    continue;
                }
            } else if inherited == offset {
                target_cell.entry_inherited();
                continue;
            }

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
    }

    /// Relocates a fixed number of Cells from the old array to the current array.
    pub fn partial_rehash<Q, F: Fn(&Q) -> (u64, u8), C: Fn(&K, &V) -> Option<(K, V)>>(
        &self,
        hasher: F,
        copier: C,
        barrier: &Barrier,
    ) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let unit_size = ARRAY_SIZE;
        let old_array_ptr = self.old_array(barrier);
        if let Some(old_array_ref) = old_array_ptr.as_ref() {
            let old_array_size = old_array_ref.num_cells();
            let mut current = self.rehashing.load(Relaxed);
            loop {
                if current >= old_array_size {
                    return false;
                }
                match self.rehashing.compare_exchange(
                    current,
                    current + unit_size,
                    Relaxed,
                    Relaxed,
                ) {
                    Ok(_) => break,
                    Err(result) => current = result,
                }
            }

            for old_cell_index in current..(current + unit_size).min(old_array_size) {
                let old_cell_ref = old_array_ref.cell(old_cell_index);
                if old_cell_ref.killed() {
                    continue;
                }
                if let Some(mut locker) = Locker::lock(old_cell_ref, barrier) {
                    self.kill_cell(
                        &mut locker,
                        old_array_ref,
                        old_cell_index,
                        &hasher,
                        &copier,
                        barrier,
                    );
                }
            }

            if old_array_size <= self.rehashed.fetch_add(unit_size, Release) + unit_size {
                self.drop_old_array(barrier);
                return true;
            }
            false
        } else {
            true
        }
    }

    /// Calculates `log_2` of the array size from the given cell capacity.
    fn calculate_lb_array_size(total_cell_capacity: usize) -> u8 {
        let adjusted_total_cell_capacity =
            total_cell_capacity.min((usize::MAX / 2) - (ARRAY_SIZE - 1));
        let required_cells =
            ((adjusted_total_cell_capacity + ARRAY_SIZE - 1) / ARRAY_SIZE).next_power_of_two();
        let log2_capacity =
            ((size_of::<usize>() * 8) - (required_cells.leading_zeros() as usize) - 1).max(1);

        // 2^lb_capacity * C::cell_size() >= capacity
        debug_assert!(log2_capacity > 0);
        debug_assert!(log2_capacity < (std::mem::size_of::<usize>() * 8));
        debug_assert!((1_usize << log2_capacity) * ARRAY_SIZE >= adjusted_total_cell_capacity);
        log2_capacity.try_into().unwrap()
    }
}

impl<K: Eq, V, const LOCK_FREE: bool> Drop for CellArray<K, V, LOCK_FREE> {
    fn drop(&mut self) {
        let size_of_cell = size_of::<Cell<K, V, LOCK_FREE>>();
        unsafe {
            dealloc(
                (self.array_ptr as *mut Cell<K, V, LOCK_FREE>)
                    .cast::<u8>()
                    .sub(self.array_ptr_offset),
                Layout::from_size_align_unchecked((self.array_capacity + 1) * size_of_cell, 1),
            );
        }
    }
}
