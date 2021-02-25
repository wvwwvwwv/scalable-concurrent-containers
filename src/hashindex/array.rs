use super::cell::{Cell, ARRAY_SIZE};
use crossbeam_epoch::{Atomic, Guard, Shared};
use std::convert::TryInto;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

pub struct Array<K: Eq, V> {
    cell_array: Vec<Atomic<Cell<K, V>>>,
    cell_array_capacity: usize,
    _lb_capacity: u8,
    _rehashing: AtomicUsize,
    _rehashed: AtomicUsize,
    _old_array: Atomic<Array<K, V>>,
}

impl<K: Eq, V> Array<K, V> {
    pub fn new(capacity: usize, current_array: Atomic<Array<K, V>>) -> Array<K, V> {
        let lb_capacity = Self::calculate_lb_metadata_array_size(capacity);
        let cell_array_capacity = 1usize << lb_capacity;
        let mut cell_array: Vec<Atomic<Cell<K, V>>> = Vec::with_capacity(cell_array_capacity);
        cell_array.resize(cell_array_capacity, Atomic::null());
        Array {
            cell_array,
            cell_array_capacity,
            _lb_capacity: lb_capacity,
            _rehashing: AtomicUsize::new(0),
            _rehashed: AtomicUsize::new(0),
            _old_array: current_array,
        }
    }

    pub fn _num_sample_size(&self) -> usize {
        (self._lb_capacity as usize).next_power_of_two()
    }
    pub fn num_cells(&self) -> usize {
        self.cell_array_capacity
    }

    pub fn capacity(&self) -> usize {
        self.cell_array_capacity * ARRAY_SIZE
    }

    pub fn old_array<'g>(&self, guard: &'g Guard) -> Shared<'g, Array<K, V>> {
        self._old_array.load(Relaxed, &guard)
    }

    pub fn _calculate_cell_index(&self, hash: u64) -> usize {
        (hash >> (64 - self._lb_capacity)).try_into().unwrap()
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

    pub fn _drop_old_array(&self, immediate_drop: bool, guard: &Guard) {
        let old_array = self._old_array.swap(Shared::null(), Relaxed, guard);
        if !old_array.is_null() {
            unsafe {
                if immediate_drop {
                    // There is no possibility that the old array contains valid cells.
                    drop(old_array.into_owned());
                } else {
                    guard.defer_destroy(old_array);
                }
            }
        }
    }
}

impl<K: Eq, V> Drop for Array<K, V> {
    fn drop(&mut self) {
        let guard = unsafe { crossbeam_epoch::unprotected() };
        for cell in self.cell_array.iter() {
            let cell_shared = cell.load(Relaxed, guard);
            if !cell_shared.is_null() {
                drop(unsafe { cell_shared.into_owned() });
            }
        }
    }
}
