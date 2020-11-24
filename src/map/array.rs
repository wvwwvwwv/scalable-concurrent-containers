extern crate libc;

use super::cell::{Cell, CellLocker, EntryArray, ARRAY_SIZE};
use crossbeam_epoch::{Atomic, Guard, Shared};
use std::convert::TryInto;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

pub const MAX_ENLARGE_FACTOR: u8 = 6;

pub struct Array<K: Eq, V> {
    cell_array: Option<Box<Cell<K, V>>>,
    entry_array: Option<Box<EntryArray<K, V>>>,
    lb_capacity: u8,
    rehashing: AtomicUsize,
    rehashed: AtomicUsize,
    old_array: Atomic<Array<K, V>>,
}

impl<K: Eq, V> Array<K, V> {
    pub fn new(capacity: usize, old_array: Atomic<Array<K, V>>) -> Array<K, V> {
        let mut array = Array {
            cell_array: None,
            entry_array: None,
            lb_capacity: Self::calculate_lb_metadata_array_size(capacity),
            rehashing: AtomicUsize::new(0),
            rehashed: AtomicUsize::new(0),
            old_array: old_array,
        };
        let cell_capacity = 1usize << array.lb_capacity;

        // calloc zeroes the allocated heap memory region
        let cell_array_ptr: *mut Cell<K, V> = unsafe {
            libc::calloc(cell_capacity, std::mem::size_of::<Cell<K, V>>()) as *mut Cell<K, V>
        };

        // memory allocation failure: panic
        if cell_array_ptr.is_null() {
            panic!(
                "memory allocation failure: {} bytes",
                cell_capacity * std::mem::size_of::<Cell<K, V>>()
            )
        }
        array.cell_array = Some(unsafe { Box::from_raw(cell_array_ptr) });

        // MaybeUninit does not need the memory to be zeroed
        let entry_array_ptr: *mut EntryArray<K, V> = unsafe {
            libc::malloc(cell_capacity * std::mem::size_of::<EntryArray<K, V>>())
                as *mut EntryArray<K, V>
        };

        // memory allocation failure: panic
        if entry_array_ptr.is_null() {
            panic!(
                "memory allocation failure: {} bytes",
                cell_capacity * std::mem::size_of::<EntryArray<K, V>>()
            )
        }
        array.entry_array = Some(unsafe { Box::from_raw(entry_array_ptr) });

        array
    }

    pub fn cell(&self, index: usize) -> &Cell<K, V> {
        let array_ptr = &(**self.cell_array.as_ref().unwrap()) as *const Cell<K, V>;
        unsafe { &(*(array_ptr.add(index))) }
    }

    pub fn entry_array(&self, index: usize) -> &EntryArray<K, V> {
        let array_ptr = &(**self.entry_array.as_ref().unwrap()) as *const EntryArray<K, V>;
        unsafe { &(*(array_ptr.add(index))) }
    }

    pub fn num_sample_size(&self) -> usize {
        (self.lb_capacity as usize).next_power_of_two()
    }
    pub fn num_cells(&self) -> usize {
        1usize << self.lb_capacity
    }

    pub fn capacity(&self) -> usize {
        (1usize << self.lb_capacity) * (ARRAY_SIZE as usize)
    }

    pub fn old_array<'a>(&self, guard: &'a Guard) -> Shared<'a, Array<K, V>> {
        self.old_array.load(Relaxed, &guard)
    }

    pub fn calculate_cell_index(&self, hash: u64) -> usize {
        (hash >> (64 - self.lb_capacity)).try_into().unwrap()
    }

    pub fn calculate_lb_metadata_array_size(capacity: usize) -> u8 {
        let adjusted_capacity = capacity.min((usize::MAX / 2) - (ARRAY_SIZE as usize - 1));
        let required_cells = ((adjusted_capacity + (ARRAY_SIZE as usize - 1))
            / (ARRAY_SIZE as usize))
            .next_power_of_two();
        let lb_capacity =
            ((std::mem::size_of::<usize>() * 8) - (required_cells.leading_zeros() as usize) - 1)
                .max(1);

        // 2^lb_capacity * ARRAY_SIZE >= capacity
        debug_assert!(lb_capacity > 0);
        debug_assert!(lb_capacity < (std::mem::size_of::<usize>() * 8));
        debug_assert!((1usize << lb_capacity) * (ARRAY_SIZE as usize) >= adjusted_capacity);
        lb_capacity.try_into().unwrap()
    }

    pub fn extract_key_value(entry_ptr: *const (K, V)) -> (K, V) {
        let entry_mut_ptr = entry_ptr as *mut MaybeUninit<(K, V)>;
        unsafe { std::ptr::replace(entry_mut_ptr, MaybeUninit::uninit()).assume_init() }
    }

    pub fn kill_cell<F: Fn(&K) -> (u64, u16)>(
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

        let shrink = old_array.lb_capacity > self.lb_capacity;
        let ratio = if shrink {
            1usize << (old_array.lb_capacity - self.lb_capacity)
        } else {
            1usize << (self.lb_capacity - old_array.lb_capacity)
        };
        let target_cell_index = if shrink {
            old_cell_index / ratio
        } else {
            old_cell_index * ratio
        };
        debug_assert!(ratio <= (1 << MAX_ENLARGE_FACTOR as usize));

        let mut target_cells: [Option<CellLocker<K, V>>; 1 << MAX_ENLARGE_FACTOR as usize] = [
            None, None, None, None, None, None, None, None, None, None, None, None, None, None,
            None, None, None, None, None, None, None, None, None, None, None, None, None, None,
            None, None, None, None, None, None, None, None, None, None, None, None, None, None,
            None, None, None, None, None, None, None, None, None, None, None, None, None, None,
            None, None, None, None, None, None, None, None,
        ];
        let mut current = cell_locker.first();
        while let Some((sub_index, entry_array_link_ptr, entry_ptr)) = current {
            let (key, value) = Self::extract_key_value(entry_ptr);
            let (hash, partial_hash) = hasher(&key);
            let new_cell_index = self.calculate_cell_index(hash);

            debug_assert!(
                (!shrink && (new_cell_index - target_cell_index) < ratio)
                    || (shrink && new_cell_index == old_cell_index / ratio)
            );

            for i in 0..=(new_cell_index - target_cell_index) {
                if target_cells[i].is_none() {
                    target_cells[i] = Some(CellLocker::lock(
                        self.cell(target_cell_index + i),
                        self.entry_array(target_cell_index + i),
                    ));
                }
            }
            target_cells[new_cell_index - target_cell_index]
                .as_mut()
                .map(|cell_locker| cell_locker.insert(key, partial_hash, value));

            current = cell_locker.next(true, false, sub_index, entry_array_link_ptr, entry_ptr);
        }
        cell_locker.kill();
    }

    pub fn partial_rehash<F: Fn(&K) -> (u64, u16)>(&self, guard: &Guard, hasher: F) -> bool {
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
            match self.rehashing.compare_exchange(
                current,
                current + ARRAY_SIZE as usize,
                Acquire,
                Relaxed,
            ) {
                Ok(_) => break,
                Err(result) => current = result,
            }
        }

        for old_cell_index in current..(current + ARRAY_SIZE as usize).min(old_array_size) {
            let old_cell_array_ptr =
                &(**old_array_ref.cell_array.as_ref().unwrap()) as *const Cell<K, V>;
            let old_cell_ref = unsafe { &(*(old_cell_array_ptr.add(old_cell_index))) };
            if old_cell_ref.killed() {
                continue;
            }
            let old_entry_array_ptr =
                &(**old_array_ref.entry_array.as_ref().unwrap()) as *const EntryArray<K, V>;
            let old_entry_array_ref = unsafe { &(*(old_entry_array_ptr.add(old_cell_index))) };
            let mut old_cell = CellLocker::lock(old_cell_ref, old_entry_array_ref);
            self.kill_cell(&mut old_cell, old_array_ref, old_cell_index, &hasher);
        }

        let completed = self.rehashed.fetch_add(ARRAY_SIZE as usize, Release) + ARRAY_SIZE as usize;
        if old_array_size <= completed {
            let old_array = self.old_array.swap(Shared::null(), Relaxed, guard);
            if !old_array.is_null() {
                unsafe { guard.defer_destroy(old_array) };
            }
            return true;
        }
        false
    }
}

impl<K: Eq, V> Drop for Array<K, V> {
    fn drop(&mut self) {
        let entry_array = self.entry_array.take();
        entry_array.map(|entry_array_box| {
            let entry_array_ptr = Box::into_raw(entry_array_box);
            unsafe { libc::free(entry_array_ptr as *mut libc::c_void) };
        });
        let cell_array = self.cell_array.take();
        cell_array.map(|cell_array_box| {
            let cell_array_ptr = Box::into_raw(cell_array_box);
            unsafe { libc::free(cell_array_ptr as *mut libc::c_void) };
        });
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn static_assertions() {
        assert_eq!(0usize.next_power_of_two(), 1);
        assert_eq!(1usize.next_power_of_two(), 1);
        assert_eq!(2usize.next_power_of_two(), 2);
        assert_eq!(3usize.next_power_of_two(), 4);
        assert_eq!(1 << 0, 1);
        assert_eq!(0usize.is_power_of_two(), false);
        assert_eq!(1usize.is_power_of_two(), true);
        assert_eq!(19usize / (ARRAY_SIZE as usize), 1);
        for capacity in 0..1024 as usize {
            assert!(
                (1usize << Array::<bool, bool>::calculate_lb_metadata_array_size(capacity))
                    * (ARRAY_SIZE as usize)
                    >= capacity
            );
        }
        assert!(
            (1usize << Array::<bool, bool>::calculate_lb_metadata_array_size(usize::MAX))
                * (ARRAY_SIZE as usize)
                >= (usize::MAX / 2)
        );
        for i in 2..(std::mem::size_of::<usize>() - 3) {
            let capacity = (1usize << i) * (ARRAY_SIZE as usize);
            assert_eq!(
                Array::<bool, bool>::calculate_lb_metadata_array_size(capacity) as usize,
                i
            );
        }
    }
}
