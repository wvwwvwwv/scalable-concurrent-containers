use super::cell::{Cell, CellLocker, ARRAY_SIZE};
use crossbeam_epoch::{Atomic, Guard, Shared};
use std::convert::TryInto;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

pub struct Array<K: Clone + Eq, V: Clone> {
    cell_array: Vec<Cell<K, V>>,
    cell_array_capacity: usize,
    lb_capacity: u8,
    rehashing: AtomicUsize,
    rehashed: AtomicUsize,
    old_array: Atomic<Array<K, V>>,
}

impl<K: Clone + Eq, V: Clone> Array<K, V> {
    pub fn new(capacity: usize, current_array: Atomic<Array<K, V>>) -> Array<K, V> {
        let lb_capacity = Self::calculate_lb_metadata_array_size(capacity);
        let cell_array_capacity = 1usize << lb_capacity;
        let mut cell_array: Vec<Cell<K, V>> = Vec::with_capacity(cell_array_capacity);
        cell_array.resize_with(cell_array_capacity, Default::default);
        Array {
            cell_array,
            cell_array_capacity,
            lb_capacity: lb_capacity,
            rehashing: AtomicUsize::new(0),
            rehashed: AtomicUsize::new(0),
            old_array: current_array,
        }
    }

    pub fn cell_ref(&self, index: usize) -> &Cell<K, V> {
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

    pub fn old_array<'g>(&self, guard: &'g Guard) -> Shared<'g, Array<K, V>> {
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
        }
    }

    pub fn partial_rehash<F: Fn(&K) -> (u64, u8)>(&self, hasher: F, guard: &Guard) -> bool {
        false
    }

    pub fn drop_old_array(&self, immediate_drop: bool, guard: &Guard) {
        let old_array = self.old_array.swap(Shared::null(), Relaxed, guard);
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
