use crate::common::cell_array::{CellArray, CellSize};
use crate::common::hash_cell::{Cell, CellLocker};
use crossbeam_epoch::Guard;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

pub const CELL_ARRAY_SIZE: usize = 32;
pub type Array<K, V> = CellArray<Cell<K, V, CELL_ARRAY_SIZE, true>>;

impl<K: Clone + Eq, V: Clone> Array<K, V> {
    /// Kills the Cell.
    pub fn kill_cell<F: Fn(&K) -> (u64, u8)>(
        &self,
        cell_locker: &mut CellLocker<K, V, CELL_ARRAY_SIZE, true>,
        old_array: &Array<K, V>,
        old_cell_index: usize,
        hasher: &F,
        guard: &Guard,
    ) {
        if cell_locker.cell_ref().killed(guard) {
            return;
        } else if cell_locker.cell_ref().num_entries() == 0 {
            cell_locker.purge(guard);
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
        debug_assert!(ratio <= (1 << Cell::<K, V, CELL_ARRAY_SIZE, true>::max_resizing_factor()));

        let mut target_cells: Vec<CellLocker<K, V, CELL_ARRAY_SIZE, true>> =
            Vec::with_capacity(1 << Cell::<K, V, CELL_ARRAY_SIZE, true>::max_resizing_factor());
        for entry in cell_locker.cell_ref().iter(guard) {
            let (hash, partial_hash) = hasher(&entry.0);
            let new_cell_index = self.calculate_cell_index(hash);
            debug_assert!(
                (!shrink && (new_cell_index - target_cell_index) < ratio)
                    || (shrink && new_cell_index == target_cell_index)
            );

            while target_cells.len() <= new_cell_index - target_cell_index {
                target_cells.push(
                    CellLocker::lock(self.cell(target_cell_index + target_cells.len()), guard)
                        .unwrap(),
                );
            }

            let result = target_cells[new_cell_index - target_cell_index].insert(
                entry.0.clone(),
                entry.1.clone(),
                partial_hash,
                guard,
            );
            debug_assert!(result.is_ok());
        }
        cell_locker.purge(guard);
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
            match self.rehashing.compare_exchange(
                current,
                current + Cell::<K, V, CELL_ARRAY_SIZE, true>::cell_size(),
                Acquire,
                Relaxed,
            ) {
                Ok(_) => break,
                Err(result) => current = result,
            }
        }

        for old_cell_index in current
            ..(current + Cell::<K, V, CELL_ARRAY_SIZE, true>::cell_size()).min(old_array_size)
        {
            let old_cell_ref = old_array_ref.cell(old_cell_index);
            if old_cell_ref.killed(guard) {
                continue;
            }
            if let Some(mut locker) = CellLocker::lock(old_cell_ref, guard) {
                self.kill_cell(&mut locker, old_array_ref, old_cell_index, &hasher, guard);
            }
        }

        let completed = self
            .rehashed
            .fetch_add(Cell::<K, V, CELL_ARRAY_SIZE, true>::cell_size(), Release)
            + Cell::<K, V, CELL_ARRAY_SIZE, true>::cell_size();
        if old_array_size <= completed {
            self.drop_old_array(false, guard);
            return true;
        }
        false
    }
}
