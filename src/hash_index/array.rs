use super::cell::{Cell, CellLocker, ARRAY_SIZE, MAX_RESIZING_FACTOR};
use crate::common::cell_array::{CellArray, CellSize};
use crossbeam_epoch::Guard;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

pub type Array<K, V> = CellArray<Cell<K, V>>;

impl<K: Clone + Eq, V: Clone> Array<K, V> {
    /// Kills the Cell.
    pub fn kill_cell<F: Fn(&K) -> (u64, u8)>(
        &self,
        cell_locker: &mut CellLocker<K, V>,
        old_array: &Array<K, V>,
        old_cell_index: usize,
        hasher: &F,
        guard: &Guard,
    ) {
        if cell_locker.cell_ref().killed(guard) {
            return;
        } else if cell_locker.cell_ref().num_entries() == 0 {
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
        for entry in cell_locker.cell_ref().iter(guard) {
            let (hash, partial_hash) = hasher(&entry.0);
            let new_cell_index = self.calculate_cell_index(hash);
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
                    .replace(CellLocker::lock(self.cell(target_cell_index + i), guard).unwrap());
            }
            num_target_cells = num_target_cells.max(new_cell_index - target_cell_index + 1);

            if let Some(target_cell_locker) =
                target_cells[new_cell_index - target_cell_index].as_mut()
            {
                let result = target_cell_locker.insert(
                    entry.0.clone(),
                    entry.1.clone(),
                    partial_hash,
                    guard,
                );
                debug_assert!(result.is_ok());
            }
        }
        cell_locker.kill();
    }

    /// Relocates a fixed number of Cells from the old array to the current array.
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
            match self
                .rehashing
                .compare_exchange(current, current + ARRAY_SIZE, Acquire, Relaxed)
            {
                Ok(_) => break,
                Err(result) => current = result,
            }
        }

        for old_cell_index in current..(current + Cell::<K, V>::cell_size()).min(old_array_size) {
            let old_cell_ref = old_array_ref.cell(old_cell_index);
            if old_cell_ref.killed(guard) {
                continue;
            }
            if let Some(mut locker) = CellLocker::lock(old_cell_ref, guard) {
                self.kill_cell(&mut locker, old_array_ref, old_cell_index, &hasher, guard);
            }
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
