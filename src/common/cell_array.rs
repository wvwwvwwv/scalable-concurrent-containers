use crossbeam_epoch::{Atomic, Guard, Pointable, Shared};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::convert::TryInto;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

/// CellArray is used by HashIndex and HashMap.
///
/// It is a special purpose array since it does not construct instances of C,
/// instead only does it allocate a large chunk of zeroed heap memory.
pub struct CellArray<C: CellSize + Pointable + Sized> {
    array: Option<Box<C>>,
    array_ptr_offset: usize,
    array_capacity: usize,
    lb_capacity: u8,
    old_array: Atomic<CellArray<C>>,
    pub rehashing: AtomicUsize,
    pub rehashed: AtomicUsize,
}

impl<C: CellSize + Pointable + Sized> CellArray<C> {
    /// Create a new Array of given capacity.
    ///
    /// total_cell_capacity is the desired number of cell entries that the CellArray can accommodate.
    /// The given array instance is attached to the newly created Array instance.
    pub fn new(total_cell_capacity: usize, old_array: Atomic<CellArray<C>>) -> CellArray<C> {
        let lb_capacity = Self::calculate_lb_array_size(total_cell_capacity);
        let array_capacity = 1usize << lb_capacity;
        let (array, array_ptr_offset) = unsafe {
            let size_of_cell = std::mem::size_of::<C>();
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
            let array_ptr = ptr.add(offset) as *mut C;
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
        self.array_capacity * C::cell_size()
    }

    /// Returns a shared pointer to the old array.
    pub fn old_array<'g>(&self, guard: &'g Guard) -> Shared<'g, CellArray<C>> {
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

    /// Calculates log_2 of the array size from the given cell capacity.
    fn calculate_lb_array_size(total_cell_capacity: usize) -> u8 {
        let adjusted_total_cell_capacity =
            total_cell_capacity.min((usize::MAX / 2) - (C::cell_size() - 1));
        let required_cells = ((adjusted_total_cell_capacity + C::cell_size() - 1) / C::cell_size())
            .next_power_of_two();
        let lb_capacity =
            ((std::mem::size_of::<usize>() * 8) - (required_cells.leading_zeros() as usize) - 1)
                .max(1);

        // 2^lb_capacity * C::cell_size() >= capacity
        debug_assert!(lb_capacity > 0);
        debug_assert!(lb_capacity < (std::mem::size_of::<usize>() * 8));
        debug_assert!((1usize << lb_capacity) * C::cell_size() >= adjusted_total_cell_capacity);
        lb_capacity.try_into().unwrap()
    }
}

impl<C: CellSize + Pointable + Sized> Drop for CellArray<C> {
    fn drop(&mut self) {
        let size_of_cell = std::mem::size_of::<C>();
        unsafe {
            let array = self.array.take().unwrap();
            dealloc(
                (Box::into_raw(array) as *mut u8).sub(self.array_ptr_offset),
                Layout::from_size_align_unchecked((self.array_capacity + 1) * size_of_cell, 1),
            )
        }
    }
}

/// The CellSize trait enforces an entry of CellArray has a fixed size cell entries.
pub trait CellSize {
    /// Returns an unsigned integer.
    fn cell_size() -> usize;
}
