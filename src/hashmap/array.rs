use super::cell::{Cell, CellLocker, ARRAY_SIZE, MAX_RESIZING_FACTOR};
use crossbeam_epoch::{Atomic, Guard, Shared};
use std::convert::TryInto;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

pub struct Array<K: Eq, V> {
    cell_array: Vec<Cell<K, V>>,
    cell_array_capacity: usize,
    lb_capacity: u8,
    rehashing: AtomicUsize,
    rehashed: AtomicUsize,
    old_array: Atomic<Array<K, V>>,
}

impl<K: Eq, V> Array<K, V> {
    pub fn new(capacity: usize, current_array: Atomic<Array<K, V>>) -> Array<K, V> {
        let lb_capacity = Self::calculate_lb_metadata_array_size(capacity);
        let cell_array_capacity = 1usize << lb_capacity;
        let mut array = Array {
            cell_array: Vec::with_capacity(cell_array_capacity),
            cell_array_capacity,
            lb_capacity,
            rehashing: AtomicUsize::new(0),
            rehashed: AtomicUsize::new(0),
            old_array: current_array,
        };
        array
            .cell_array
            .resize_with(cell_array_capacity, Default::default);
        array
    }

    pub fn cell(&self, index: usize) -> &Cell<K, V> {
        &self.cell_array[index]
    }

    pub fn num_sample_size(&self) -> usize {
        (self.lb_capacity as usize).next_power_of_two()
    }
    pub fn num_cells(&self) -> usize {
        self.cell_array_capacity
    }

    pub fn capacity(&self) -> usize {
        self.cell_array_capacity * ARRAY_SIZE
    }

    pub fn old_array<'a>(&self, guard: &'a Guard) -> Shared<'a, Array<K, V>> {
        self.old_array.load(Relaxed, &guard)
    }

    pub fn calculate_cell_index(&self, hash: u64) -> usize {
        (hash >> (64 - self.lb_capacity)).try_into().unwrap()
    }

    pub fn calculate_lb_metadata_array_size(capacity: usize) -> u8 {
        let adjusted_capacity = capacity.min((usize::MAX / 2) - (ARRAY_SIZE - 1));
        let required_cells =
            ((adjusted_capacity + ARRAY_SIZE - 1) / ARRAY_SIZE).next_power_of_two();
        let lb_capacity =
            ((std::mem::size_of::<usize>() * 8) - (required_cells.leading_zeros() as usize) - 1)
                .max(1);

        // 2^lb_capacity * ARRAY_SIZE >= capacity
        debug_assert!(lb_capacity > 0);
        debug_assert!(lb_capacity < (std::mem::size_of::<usize>() * 8));
        debug_assert!((1usize << lb_capacity) * ARRAY_SIZE >= adjusted_capacity);
        lb_capacity.try_into().unwrap()
    }

    pub fn extract_key_value(entry_ptr: *const (K, V)) -> (K, V) {
        let entry_mut_ptr = entry_ptr as *mut MaybeUninit<(K, V)>;
        unsafe { std::ptr::replace(entry_mut_ptr, MaybeUninit::uninit()).assume_init() }
    }

    pub fn kill_cell<F: Fn(&K) -> (u64, u8)>(
        &self,
        cell_locker: &mut CellLocker<K, V>,
        old_array: &Array<K, V>,
        old_cell_index: usize,
        hasher: &F,
    ) {
        if cell_locker.killed() {
            return;
        } else if cell_locker.empty() {
            cell_locker.kill();
            return;
        }

        let shrink = old_array.cell_array_capacity > self.cell_array_capacity;
        let ratio = if shrink {
            old_array.cell_array_capacity / self.cell_array_capacity
        } else {
            self.cell_array_capacity / old_array.cell_array_capacity
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
                cell_locker_mut_ref.replace(CellLocker::lock(self.cell(target_cell_index + i)));
            }
            num_target_cells = num_target_cells.max(new_cell_index - target_cell_index + 1);

            if let Some(target_cell_locker) =
                target_cells[new_cell_index - target_cell_index].as_mut()
            {
                let result = target_cell_locker.insert(key, partial_hash, value, false);
                debug_assert!(result.is_ok());
            }

            current = cell_locker.next(true, false, sub_index, entry_array_link_ptr, entry_ptr);
        }
        cell_locker.kill();
    }

    pub fn partial_rehash<F: Fn(&K) -> (u64, u8)>(&self, guard: &Guard, hasher: F) -> bool {
        let old_array = self.old_array.load(Relaxed, guard);
        if old_array.is_null() {
            return true;
        }

        let old_array_ref = unsafe { old_array.deref() };
        let old_array_size = old_array_ref.num_cells();
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

        for old_cell_index in current..(current + ARRAY_SIZE).min(old_array_size) {
            let old_cell_ref = &old_array_ref.cell_array[old_cell_index];
            if old_cell_ref.killed() {
                continue;
            }
            let mut old_cell = CellLocker::lock(old_cell_ref);
            self.kill_cell(&mut old_cell, old_array_ref, old_cell_index, &hasher);
        }

        let completed = self.rehashed.fetch_add(ARRAY_SIZE, Release) + ARRAY_SIZE;
        if old_array_size <= completed {
            self.drop_old_array(false, guard);
            return true;
        }
        false
    }

    pub fn drop_old_array(&self, immediate_drop: bool, guard: &Guard) {
        let old_array = self.old_array.swap(Shared::null(), Relaxed, guard);
        if !old_array.is_null() {
            unsafe {
                if immediate_drop {
                    drop(old_array.into_owned());
                } else {
                    guard.defer_destroy(old_array);
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn static_assertions() {
        for capacity in 0..1024 as usize {
            assert!(
                (1usize << Array::<bool, bool>::calculate_lb_metadata_array_size(capacity))
                    * ARRAY_SIZE
                    >= capacity
            );
        }
        assert!(
            (1usize << Array::<bool, bool>::calculate_lb_metadata_array_size(usize::MAX))
                * ARRAY_SIZE
                >= (usize::MAX / 2)
        );
        for i in 2..(std::mem::size_of::<usize>() - 3) {
            let capacity = (1usize << i) * ARRAY_SIZE;
            assert_eq!(
                Array::<bool, bool>::calculate_lb_metadata_array_size(capacity) as usize,
                i
            );
        }
    }
}
