use crossbeam::epoch::{Atomic, Guard, Shared};
use std::convert::TryInto;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

pub struct Array<K, V, M: Default> {
    metadata_array: Vec<M>,
    entry_array: Vec<MaybeUninit<(K, V)>>,
    lb_capacity: u8,
    rehashing: AtomicUsize,
    old_array: Atomic<Array<K, V, M>>,
}

impl<K, V, M: Default> Array<K, V, M> {
    pub fn new(capacity: usize, old_array: Atomic<Array<K, V, M>>) -> Array<K, V, M> {
        let lb_capacity = Self::calculate_lb_metadata_array_size(capacity);
        let mut array = Array {
            metadata_array: Vec::with_capacity(1 << lb_capacity),
            entry_array: Vec::with_capacity((1 << lb_capacity) * 10),
            lb_capacity: lb_capacity,
            rehashing: AtomicUsize::new(0),
            old_array: old_array,
        };
        for _ in 0..(1 << lb_capacity) {
            array.metadata_array.push(Default::default());
        }
        for _ in 0..(1 << lb_capacity) * 10 {
            array
                .entry_array
                .push(unsafe { MaybeUninit::uninit().assume_init() });
        }
        array
    }

    pub fn get_cell(&self, index: usize) -> &M {
        &self.metadata_array[index]
    }

    pub fn get_key_value_pair(&self, index: usize) -> *const (K, V) {
        self.entry_array[index].as_ptr()
    }

    pub fn num_cells(&self) -> usize {
        (1 << self.lb_capacity)
    }

    pub fn get_old_array<'a>(&self, guard: &'a Guard) -> Shared<'a, Array<K, V, M>> {
        self.old_array.load(Relaxed, guard)
    }

    pub fn calculate_metadata_array_index(&self, hash: u64) -> usize {
        (hash >> (64 - self.lb_capacity)).try_into().unwrap()
    }

    pub fn calculate_lb_metadata_array_size(capacity: usize) -> u8 {
        let adjusted_capacity = capacity.min((usize::MAX / 2) - 9);
        let required_cells = ((adjusted_capacity + 9) / 10).next_power_of_two();
        let lb_capacity =
            ((std::mem::size_of::<usize>() * 8) - (required_cells.leading_zeros() as usize) - 1)
                .max(1);

        // 2^lb_capacity * 10 >= capacity
        debug_assert!(lb_capacity > 0);
        debug_assert!(lb_capacity < (std::mem::size_of::<usize>() * 8));
        debug_assert!((1 << lb_capacity) * 10 >= adjusted_capacity);
        lb_capacity.try_into().unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::atomic::Ordering::Relaxed;

    #[test]
    fn static_assertions() {
        assert_eq!(0usize.next_power_of_two(), 1);
        assert_eq!(1usize.next_power_of_two(), 1);
        assert_eq!(2usize.next_power_of_two(), 2);
        assert_eq!(3usize.next_power_of_two(), 4);
        assert_eq!(1 << 0, 1);
        assert_eq!(0usize.is_power_of_two(), false);
        assert_eq!(1usize.is_power_of_two(), true);
        assert_eq!(19usize / 10, 1);
        for capacity in 0..1024 as usize {
            assert!(
                (1 << Array::<bool, bool, bool>::calculate_lb_metadata_array_size(capacity)) * 10
                    >= capacity
            );
        }
        assert!(
            (1 << Array::<bool, bool, bool>::calculate_lb_metadata_array_size(usize::MAX)) * 10
                >= (usize::MAX / 2)
        );
        for i in 2..(std::mem::size_of::<usize>() - 3) {
            let capacity = (1 << i) * 10;
            assert_eq!(
                Array::<bool, bool, bool>::calculate_lb_metadata_array_size(capacity) as usize,
                i
            );
        }
    }
}
