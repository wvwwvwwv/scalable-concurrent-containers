use super::cell::Cell;
use super::cell_array::CellArray;

use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};

use std::borrow::Borrow;
use std::convert::TryInto;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// `HashTable` define common functions for `HashIndex` and `HashMap`.
pub trait HashTable<K, V, H, const CELL_SIZE: usize, const LOCK_FREE: bool>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    /// Returns the hash value of the given key.
    fn hash<Q>(&self, key: &Q) -> (u64, u8)
    where
        K: Borrow<Q>,
        Q: Hash + ?Sized,
    {
        // Generates a hash value.
        let mut h = self.hasher().build_hasher();
        key.hash(&mut h);
        let mut hash = h.finish();

        // Bitmix: https://mostlymangling.blogspot.com/2019/01/better-stronger-mixer-and-test-procedure.html
        hash = hash ^ (hash.rotate_right(25) ^ hash.rotate_right(50));
        hash = hash.overflowing_mul(0xA24BAED4963EE407u64).0;
        hash = hash ^ (hash.rotate_right(24) ^ hash.rotate_right(49));
        hash = hash.overflowing_mul(0x9FB21C651E98DF25u64).0;
        hash = hash ^ (hash >> 28);
        (hash, (hash & ((1 << 8) - 1)).try_into().unwrap())
    }

    /// Returns a reference to its build hasher.
    fn hasher(&self) -> &H;

    /// Returns a reference to the `CellArray` pointer.
    fn cell_array_ptr(&self) -> &AtomicArc<CellArray<K, V, CELL_SIZE, LOCK_FREE>>;

    /// Returns a reference to the `CellArray` instance.
    fn cell_array_ref(
        cell_array_ptr: Ptr<CellArray<K, V, CELL_SIZE, LOCK_FREE>>,
    ) -> &CellArray<K, V, CELL_SIZE, LOCK_FREE> {
        cell_array_ptr.as_ref().unwrap()
    }

    /// Returns the minimum allowed capacity.
    fn minimum_capacity(&self) -> usize;

    /// Returns a reference to the resizing flag.
    fn resizing_flag_ref(&self) -> &AtomicBool;

    /// Returns the number of entries.
    fn num_entries(&self, barrier: &Barrier) -> usize {
        let current_array_ptr = self.cell_array_ptr().load(Acquire, barrier);
        let current_array_ref = Self::cell_array_ref(current_array_ptr);
        let mut num_entries = 0;
        for i in 0..current_array_ref.array_size() {
            num_entries += current_array_ref.cell(i).num_entries();
        }
        let old_array_ptr = current_array_ref.old_array(barrier);
        if let Some(old_array_ref) = old_array_ptr.as_ref() {
            for i in 0..old_array_ref.array_size() {
                num_entries += old_array_ref.cell(i).num_entries();
            }
        }
        num_entries
    }

    /// Returns the number of slots.
    fn num_slots(&self, barrier: &Barrier) -> usize {
        let current_array_ptr = self.cell_array_ptr().load(Acquire, barrier);
        let current_array_ref = Self::cell_array_ref(current_array_ptr);
        current_array_ref.num_cell_entries()
    }

    /// Estimates the number of entries using the given number of cells.
    fn estimate(
        array_ref: &CellArray<K, V, CELL_SIZE, LOCK_FREE>,
        num_cells_to_sample: usize,
    ) -> usize {
        let mut num_entries = 0;
        for i in 0..num_cells_to_sample {
            num_entries += array_ref.cell(i).num_entries();
        }
        num_entries * (array_ref.array_size() / num_cells_to_sample)
    }

    /// Resizes the array.
    fn resize(&self, barrier: &Barrier) {
        // Initial rough size estimation using a small number of cells.
        let current_array = self.cell_array_ptr().load(Acquire, barrier);
        let current_array_ref = Self::cell_array_ref(current_array);
        let old_array = current_array_ref.old_array(&barrier);
        if !old_array.is_null() {
            // With a deprecated array present, it cannot be resized.
            return;
        }

        if !self.resizing_flag_ref().swap(true, Acquire) {
            let memory_ordering = Relaxed;
            let mut mutex_guard = scopeguard::guard(memory_ordering, |memory_ordering| {
                self.resizing_flag_ref().store(false, memory_ordering);
            });
            if current_array != self.cell_array_ptr().load(Acquire, barrier) {
                return;
            }

            // The resizing policies are as follows.
            //  - The load factor reaches 7/8, then the array grows up to 64x.
            //  - The load factor reaches 1/16, then the array shrinks to fit.
            let capacity = current_array_ref.num_cell_entries();
            let num_cells = current_array_ref.array_size();
            let num_cells_to_sample = (num_cells / 8).max(2).min(4096);
            let estimated_num_entries = Self::estimate(current_array_ref, num_cells_to_sample);
            let new_capacity = if estimated_num_entries >= (capacity / 8) * 7 {
                let max_capacity = 1usize << (std::mem::size_of::<usize>() * 8 - 1);
                if capacity == max_capacity {
                    // Do not resize if the capacity cannot be increased.
                    capacity
                } else {
                    let mut new_capacity = capacity;
                    while new_capacity < (estimated_num_entries / 8) * 15 {
                        // Doubles the new capacity until it can accommodate the estimated number of entries * 15/8.
                        if new_capacity == max_capacity {
                            break;
                        }
                        if new_capacity / capacity
                            >= Cell::<K, V, CELL_SIZE, LOCK_FREE>::max_resizing_factor()
                        {
                            break;
                        }
                        new_capacity *= 2;
                    }
                    new_capacity
                }
            } else if estimated_num_entries <= capacity / 16 {
                // Shrinks to fit.
                estimated_num_entries
                    .next_power_of_two()
                    .max(self.minimum_capacity())
            } else {
                capacity
            };

            // Array::new may not be able to allocate the requested number of cells.
            if new_capacity != capacity {
                self.cell_array_ptr().swap(
                    (
                        Some(Arc::new(CellArray::<K, V, CELL_SIZE, LOCK_FREE>::new(
                            new_capacity,
                            self.cell_array_ptr().clone(Relaxed, barrier),
                        ))),
                        Tag::None,
                    ),
                    Release,
                );
                // The release fence assures that future calls to the function see the latest state.
                *mutex_guard = Release;
            }
        }
    }
}
