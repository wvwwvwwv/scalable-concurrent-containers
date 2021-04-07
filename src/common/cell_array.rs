use super::hash_cell::{Cell, CellIterator, CellLocker};
use crossbeam_epoch::{Atomic, Guard, Pointable, Shared};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::convert::TryInto;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// CellArray is used by HashIndex and HashMap.
///
/// It is a special purpose array since it does not construct instances of C,
/// instead only does it allocate a large chunk of zeroed heap memory.
pub struct CellArray<K: Eq, V, const SIZE: usize, const LOCK_FREE: bool> {
    array: Option<Box<Cell<K, V, SIZE, LOCK_FREE>>>,
    array_ptr_offset: usize,
    array_capacity: usize,
    lb_capacity: u8,
    old_array: Atomic<CellArray<K, V, SIZE, LOCK_FREE>>,
    rehashing: AtomicUsize,
    rehashed: AtomicUsize,
}

impl<K: Eq, V, const SIZE: usize, const LOCK_FREE: bool> CellArray<K, V, SIZE, LOCK_FREE> {
    /// Creates a new Array of given capacity.
    ///
    /// total_cell_capacity is the desired number of cell entries that the CellArray can accommodate.
    /// The given array instance is attached to the newly created Array instance.
    pub fn new(
        total_cell_capacity: usize,
        old_array: Atomic<CellArray<K, V, SIZE, LOCK_FREE>>,
    ) -> CellArray<K, V, SIZE, LOCK_FREE> {
        let lb_capacity = Self::calculate_lb_array_size(total_cell_capacity);
        let array_capacity = 1usize << lb_capacity;
        let (array, array_ptr_offset) = unsafe {
            let size_of_cell = std::mem::size_of::<Cell<K, V, SIZE, LOCK_FREE>>();
            let allocation_size = (array_capacity + 1) * size_of_cell;
            let ptr = alloc_zeroed(Layout::from_size_align_unchecked(allocation_size, 1));
            if ptr.is_null() {
                // Memory allocation failure: panic.
                panic!("memory allocation failure: {} bytes", allocation_size)
            }
            let mut offset = ptr.align_offset(size_of_cell.next_power_of_two());
            if offset == usize::MAX {
                offset = 0;
            }
            let array_ptr = ptr.add(offset) as *mut Cell<K, V, SIZE, LOCK_FREE>;
            (Some(Box::from_raw(array_ptr)), offset)
        };
        CellArray {
            array,
            array_ptr_offset,
            array_capacity,
            lb_capacity,
            old_array,
            rehashing: AtomicUsize::new(0),
            rehashed: AtomicUsize::new(0),
        }
    }

    /// Returns a reference to a Cell at the given position.
    pub fn cell(&self, index: usize) -> &C {
        let array_ptr = &(**self.array.as_ref().unwrap()) as *const C;
        unsafe { &(*(array_ptr.add(index))) }
    }

    /// Returns the recommended sampling size.
    pub fn sample_size(&self) -> usize {
        (self.lb_capacity as usize).next_power_of_two()
    }

    /// Returns the size of the CellArray.
    pub fn array_size(&self) -> usize {
        self.array_capacity
    }

    /// Returns the number of total cell entries.
    pub fn num_cell_entries(&self) -> usize {
        self.array_capacity * Cell::<K, V, SIZE, LOCK_FREE>::cell_size()
    }

    /// Returns a shared pointer to the old array.
    pub fn old_array<'g>(&self, guard: &'g Guard) -> Shared<'g, CellArray<K, V, SIZE, LOCK_FREE>> {
        self.old_array.load(Relaxed, &guard)
    }

    /// Calculates the cell index for the hash value.
    pub fn calculate_cell_index(&self, hash: u64) -> usize {
        (hash >> (64 - self.lb_capacity)).try_into().unwrap()
    }

    /// Drops the old array.
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

    /// Kills the Cell.
    pub fn kill_cell<F: Fn(&K) -> (u64, u8), C: Fn(&K, &V) -> (K, V)>(
        &self,
        cell_locker: &mut CellLocker<K, V, SIZE, LOCK_FREE>,
        old_array: &CellArray<K, V, SIZE, LOCK_FREE>,
        old_cell_index: usize,
        hasher: &F,
        copier: &Option<C>,
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
        debug_assert!(ratio <= (1 << Cell::<K, V, SIZE, LOCK_FREE>::max_resizing_factor()));

        let mut target_cells: Vec<CellLocker<K, V, SIZE, LOCK_FREE>> =
            Vec::with_capacity(1 << Cell::<K, V, SIZE, LOCK_FREE>::max_resizing_factor());
        let mut iter = cell_locker.cell_ref().iter(guard);
        while let Some(entry) = iter.next() {
            let (new_cell_index, partial_hash) = if !shrink {
                let (hash, partial_hash) = hasher(&entry.0 .0);
                let new_cell_index = self.calculate_cell_index(hash);
                debug_assert!((new_cell_index - target_cell_index) < ratio);
                (new_cell_index, partial_hash)
            } else {
                debug_assert!(
                    self.calculate_cell_index(hasher(&entry.0 .0).0) == target_cell_index
                );
                (target_cell_index, entry.1)
            };

            while target_cells.len() <= new_cell_index - target_cell_index {
                target_cells.push(
                    CellLocker::lock(self.cell(target_cell_index + target_cells.len()), guard)
                        .unwrap(),
                );
            }

            let new_entry = if let Some(copier) = copier.as_ref() {
                // HashIndex.
                debug_assert!(LOCK_FREE);
                copier(&entry.0 .0, &entry.0 .1)
            } else {
                // HashMap.
                debug_assert!(!LOCK_FREE);
                cell_locker.erase(&mut iter).unwrap()
            };
            let result = target_cells[new_cell_index - target_cell_index].insert(
                new_entry.0,
                new_entry.1,
                partial_hash,
                guard,
            );
            debug_assert!(result.is_ok());
        }
        cell_locker.purge(guard);
    }

    /// Moves an existing entry.
    fn move_entry(
        &self,
        from: &CellLocker<K, V, SIZE, LOCK_FREE>,
        to: &CellLocker<K, V, SIZE, LOCK_FREE>,
        partial_hash: u8,
        guard: &Guard,
    ) {
    }

    /// Relocates a fixed number of Cells from the old array to the current array.
    pub fn partial_rehash<F: Fn(&K) -> (u64, u8), C: Fn(&K, &V) -> (K, V)>(
        &self,
        hasher: F,
        copier: Option<C>,
        guard: &Guard,
    ) -> bool {
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
                .compare_exchange(current, current + SIZE, Acquire, Relaxed)
            {
                Ok(_) => break,
                Err(result) => current = result,
            }
        }

        for old_cell_index in current..(current + SIZE).min(old_array_size) {
            let old_cell_ref = old_array_ref.cell(old_cell_index);
            if old_cell_ref.killed(guard) {
                continue;
            }
            if let Some(mut locker) = CellLocker::lock(old_cell_ref, guard) {
                self.kill_cell(&mut locker, old_array_ref, old_cell_index, &hasher, guard);
            }
        }

        let completed = self.rehashed.fetch_add(SIZE, Release) + SIZE;
        if old_array_size <= completed {
            self.drop_old_array(false, guard);
            return true;
        }
        false
    }

    /// Calculates log_2 of the array size from the given cell capacity.
    fn calculate_lb_array_size(total_cell_capacity: usize) -> u8 {
        let adjusted_total_cell_capacity = total_cell_capacity.min((usize::MAX / 2) - (SIZE - 1));
        let required_cells = ((adjusted_total_cell_capacity + SIZE - 1) / SIZE).next_power_of_two();
        let lb_capacity =
            ((std::mem::size_of::<usize>() * 8) - (required_cells.leading_zeros() as usize) - 1)
                .max(1);

        // 2^lb_capacity * C::cell_size() >= capacity
        debug_assert!(lb_capacity > 0);
        debug_assert!(lb_capacity < (std::mem::size_of::<usize>() * 8));
        debug_assert!((1usize << lb_capacity) * SIZE >= adjusted_total_cell_capacity);
        lb_capacity.try_into().unwrap()
    }
}

impl<K: Eq, V, const SIZE: usize, const LOCK_FREE: bool> Drop for CellArray<K, V, SIZE, LOCK_FREE> {
    fn drop(&mut self) {
        let size_of_cell = std::mem::size_of::<Cell<K, V, SIZE, LOCK_FREE>>();
        unsafe {
            let array = self.array.take().unwrap();
            dealloc(
                (Box::into_raw(array) as *mut u8).sub(self.array_ptr_offset),
                Layout::from_size_align_unchecked((self.array_capacity + 1) * size_of_cell, 1),
            )
        }
    }
}
