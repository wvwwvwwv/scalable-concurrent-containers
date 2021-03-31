use super::cell::{Cell, CellLocker, MAX_RESIZING_FACTOR};
use crate::common::cell_array::{CellArray, CellSize};
use crossbeam_epoch::Guard;
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

pub type Array<K, V> = CellArray<Cell<K, V>>;

impl<K: Eq, V> Array<K, V> {
    /// Extracts the key-value pair instance from the given pointer.
    pub fn extract_key_value(entry_ptr: *const (K, V)) -> (K, V) {
        let entry_mut_ptr = entry_ptr as *mut MaybeUninit<(K, V)>;
        unsafe { std::ptr::replace(entry_mut_ptr, MaybeUninit::uninit()).assume_init() }
    }

    /// Kills the Cell.
    pub fn kill_cell<F: Fn(&K) -> (u64, u8)>(
        &self,
        cell_locker: &mut CellLocker<K, V>,
        old_array: &Array<K, V>,
        old_cell_index: usize,
        hasher: &F,
        guard: &Guard,
    ) {
        if cell_locker.killed() {
            return;
        } else if cell_locker.empty() {
            cell_locker.kill();
            return;
        }

        let shrink = old_array.array_size() > self.array_size();
        let ratio = if shrink {
            old_array.array_size() / self.array_size()
        } else {
            self.array_size() / old_array.array_size()
        };
        let target_cell_index = if shrink {
            old_cell_index / ratio
        } else {
            old_cell_index * ratio
        };
        debug_assert!(ratio <= (1 << MAX_RESIZING_FACTOR));

        let mut target_cells: [Option<CellLocker<K, V>>; 1 << MAX_RESIZING_FACTOR] = [
            None, None, None, None, None, None, None, None, None, None, None, None, None, None,
            None, None, None, None, None, None, None, None, None, None, None, None, None, None,
            None, None, None, None, None, None, None, None, None, None, None, None, None, None,
            None, None, None, None, None, None, None, None, None, None, None, None, None, None,
            None, None, None, None, None, None, None, None,
        ];
        let mut num_target_cells = 0;
        let mut current = cell_locker.first();
        while let Some((sub_index, entry_array_link_ptr, entry_ptr)) = current {
            let (key, value) = Self::extract_key_value(entry_ptr);
            let (new_cell_index, partial_hash) = if shrink && sub_index != u8::MAX {
                (target_cell_index, cell_locker.partial_hash(sub_index))
            } else {
                let (hash, partial_hash) = hasher(&key);
                (self.calculate_cell_index(hash), partial_hash)
            };
            debug_assert!(
                (!shrink && (new_cell_index - target_cell_index) < ratio)
                    || (shrink && new_cell_index == target_cell_index)
            );

            for (i, cell_locker_mut_ref) in target_cells
                .iter_mut()
                .enumerate()
                .take((new_cell_index - target_cell_index) + 1)
                .skip(num_target_cells)
            {
                cell_locker_mut_ref
                    .replace(CellLocker::lock(self.cell(target_cell_index + i), guard));
            }
            num_target_cells = num_target_cells.max(new_cell_index - target_cell_index + 1);

            if let Some(target_cell_locker) =
                target_cells[new_cell_index - target_cell_index].as_mut()
            {
                target_cell_locker.insert(key, partial_hash, value);
            }

            current = cell_locker.next(true, false, sub_index, entry_array_link_ptr, entry_ptr);
        }
        cell_locker.kill();
    }

    /// Relocates a fixed number of Cell from the old array to the current array.
    pub fn partial_rehash<F: Fn(&K) -> (u64, u8)>(&self, hasher: F, guard: &Guard) -> bool {
        let old_array = self.old_array(guard);
        if old_array.is_null() {
            return true;
        }

        let old_array_ref = unsafe { old_array.deref() };
        let old_array_size = old_array_ref.array_size();
        let mut current = self.rehashing.load(Relaxed);
        loop {
            if current >= old_array_size {
                return false;
            }
            match self.rehashing.compare_exchange(
                current,
                current + Cell::<K, V>::cell_size(),
                Acquire,
                Relaxed,
            ) {
                Ok(_) => break,
                Err(result) => current = result,
            }
        }

        for old_cell_index in current..(current + Cell::<K, V>::cell_size()).min(old_array_size) {
            let old_cell_ref = old_array_ref.cell(old_cell_index);
            if old_cell_ref.killed() {
                continue;
            }
            let mut old_cell = CellLocker::lock(old_cell_ref, guard);
            self.kill_cell(&mut old_cell, old_array_ref, old_cell_index, &hasher, guard);
        }

        let completed =
            self.rehashed.fetch_add(Cell::<K, V>::cell_size(), Release) + Cell::<K, V>::cell_size();
        if old_array_size <= completed {
            self.drop_old_array(false, guard);
            return true;
        }
        false
    }
}
