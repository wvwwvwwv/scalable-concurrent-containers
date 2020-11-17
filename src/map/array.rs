use super::cell::{Cell, CellLocker, ARRAY_SIZE};
use super::link;
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use std::convert::TryInto;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

pub struct Array<K: Clone + Eq, V> {
    metadata_array: Vec<Cell<K, V>>,
    entry_array: Vec<MaybeUninit<(K, V)>>,
    /// The log (base 2) capacity of the array
    ///
    /// Trigger resize when the max depth of linked lists reaches lb_capacity / 4
    lb_capacity: u8,
    rehashing: AtomicUsize,
    rehashed: AtomicUsize,
    old_array: Atomic<Array<K, V>>,
}

impl<K: Clone + Eq, V> Array<K, V> {
    pub fn new(capacity: usize, old_array: Atomic<Array<K, V>>) -> Array<K, V> {
        let lb_capacity = Self::calculate_lb_metadata_array_size(capacity);
        let mut array = Array {
            metadata_array: Vec::with_capacity(1 << lb_capacity),
            entry_array: Vec::with_capacity((1 << lb_capacity) * (ARRAY_SIZE as usize)),
            lb_capacity: lb_capacity,
            rehashing: AtomicUsize::new(0),
            rehashed: AtomicUsize::new(0),
            old_array: old_array,
        };
        for _ in 0..(1 << lb_capacity) {
            array.metadata_array.push(Default::default());
        }
        for _ in 0..(1 << lb_capacity) * (ARRAY_SIZE as usize) {
            array.entry_array.push(MaybeUninit::uninit());
        }
        array
    }

    pub fn get_cell(&self, index: usize) -> &Cell<K, V> {
        &self.metadata_array[index]
    }

    pub fn get_key_value_pair(&self, index: usize) -> *const (K, V) {
        self.entry_array[index].as_ptr()
    }

    pub fn num_cells(&self) -> usize {
        1 << self.lb_capacity
    }

    pub fn capacity(&self) -> usize {
        (1 << self.lb_capacity) * (ARRAY_SIZE as usize)
    }

    pub fn get_old_array<'a>(&self, guard: &'a Guard) -> Shared<'a, Array<K, V>> {
        self.old_array.load(Relaxed, &guard)
    }

    pub fn calculate_metadata_array_index(&self, hash: u64) -> usize {
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
        debug_assert!((1 << lb_capacity) * (ARRAY_SIZE as usize) >= adjusted_capacity);
        lb_capacity.try_into().unwrap()
    }

    pub fn advise_expand(&self, linked_entries: usize) -> bool {
        // num(linked_entries + delta) >= log(capacity)
        (linked_entries + link::ARRAY_SIZE / 2) >= self.lb_capacity as usize
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

        let shrink = self.num_cells() < old_array.num_cells();
        let target_cell_index = if shrink {
            old_cell_index / 2
        } else {
            old_cell_index * 2
        };
        let mut target_cell = CellLocker::lock(self.get_cell(target_cell_index));
        let mut optional_target_cell: Option<CellLocker<K, V>> = None;

        let mut current = cell_locker.next_occupied(u8::MAX);
        while current != u8::MAX {
            let old_index = old_cell_index * ARRAY_SIZE as usize + current as usize;
            let key_value_pair_entry_ptr =
                &old_array.entry_array[old_index] as *const MaybeUninit<(K, V)>;
            let key_value_pair_entry_mut_ptr = key_value_pair_entry_ptr as *mut MaybeUninit<(K, V)>;
            let entry =
                unsafe { std::ptr::replace(key_value_pair_entry_mut_ptr, MaybeUninit::uninit()) };
            let (key, value) = unsafe { entry.assume_init() };
            let (hash, partial_hash) = hasher(&key);
            let new_cell_index = self.calculate_metadata_array_index(hash);
            if optional_target_cell.is_none() && new_cell_index != target_cell_index {
                optional_target_cell.replace(CellLocker::lock(self.get_cell(new_cell_index)));
            }

            // the new location is /2, *2 or *2+1
            debug_assert!(
                (!shrink
                    && (new_cell_index == old_cell_index * 2
                        || new_cell_index == old_cell_index * 2 + 1))
                    || (shrink && new_cell_index == old_cell_index / 2)
            );

            self.insert(
                &key,
                partial_hash,
                value,
                new_cell_index,
                target_cell_index,
                &mut target_cell,
                &mut optional_target_cell,
            );
            cell_locker.remove(current);
            current = cell_locker.next_occupied(current);
        }

        if cell_locker.overflowing() {
            while let Some((key, value)) = cell_locker.consume_link() {
                let (hash, partial_hash) = hasher(&key);
                let new_cell_index = self.calculate_metadata_array_index(hash);
                if optional_target_cell.is_none() && new_cell_index != target_cell_index {
                    optional_target_cell.replace(CellLocker::lock(self.get_cell(new_cell_index)));
                }

                // the new location is /2, *2 or *2+1
                debug_assert!(
                    (!shrink
                        && (new_cell_index == old_cell_index * 2
                            || new_cell_index == old_cell_index * 2 + 1))
                        || (shrink && new_cell_index == old_cell_index / 2)
                );

                self.insert(
                    &key,
                    partial_hash,
                    value,
                    new_cell_index,
                    target_cell_index,
                    &mut target_cell,
                    &mut optional_target_cell,
                );
            }
        }

        cell_locker.kill();
    }

    fn insert(
        &self,
        key: &K,
        partial_hash: u16,
        value: V,
        new_cell_index: usize,
        target_cell_index: usize,
        target_cell: &mut CellLocker<K, V>,
        optional_target_cell: &mut Option<CellLocker<K, V>>,
    ) {
        let mut new_sub_index = u8::MAX;
        if new_cell_index != target_cell_index {
            debug_assert!(new_cell_index == target_cell_index + 1);
            if let Some(sub_index) = optional_target_cell
                .as_mut()
                .map_or(None, |cell| cell.insert(partial_hash))
            {
                new_sub_index = sub_index;
            }
        } else {
            if let Some(sub_index) = target_cell.insert(partial_hash) {
                new_sub_index = sub_index;
            }
        }

        if new_sub_index != u8::MAX {
            let key_value_array_index =
                new_cell_index * (ARRAY_SIZE as usize) + (new_sub_index as usize);
            let key_value_pair_mut_ptr =
                self.get_key_value_pair(key_value_array_index) as *mut (K, V);
            unsafe { key_value_pair_mut_ptr.write((key.clone(), value)) };
        } else if new_cell_index != target_cell_index {
            optional_target_cell
                .as_mut()
                .map(|cell| cell.insert_link(&key, partial_hash, value));
        } else {
            target_cell.insert_link(&key, partial_hash, value);
        }
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
            if current >= self.num_cells() {
                return false;
            }
            match self
                .rehashing
                .compare_exchange(current, current + 16, Acquire, Relaxed)
            {
                Ok(_) => break,
                Err(result) => current = result,
            }
        }

        for old_cell_index in current..(current + ARRAY_SIZE as usize).min(old_array_size) {
            if old_array_ref.metadata_array[old_cell_index].killed() {
                continue;
            }
            let mut old_cell = CellLocker::lock(&old_array_ref.metadata_array[old_cell_index]);
            self.kill_cell(&mut old_cell, old_array_ref, old_cell_index, &hasher);
        }

        let completed = self.rehashed.fetch_add(ARRAY_SIZE as usize, Release) + ARRAY_SIZE as usize;
        if old_array_size <= completed {
            let old_array = self.old_array.swap(Shared::null(), Relaxed, guard);
            unsafe { guard.defer_destroy(old_array) };
            return true;
        }
        false
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
                (1 << Array::<bool, bool>::calculate_lb_metadata_array_size(capacity))
                    * (ARRAY_SIZE as usize)
                    >= capacity
            );
        }
        assert!(
            (1 << Array::<bool, bool>::calculate_lb_metadata_array_size(usize::MAX))
                * (ARRAY_SIZE as usize)
                >= (usize::MAX / 2)
        );
        for i in 2..(std::mem::size_of::<usize>() - 3) {
            let capacity = (1 << i) * (ARRAY_SIZE as usize);
            assert_eq!(
                Array::<bool, bool>::calculate_lb_metadata_array_size(capacity) as usize,
                i
            );
        }
    }
}
