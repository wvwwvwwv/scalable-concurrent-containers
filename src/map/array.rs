use std::convert::TryInto;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicUsize;

pub struct Array<K, V, M: Default> {
    metadata_array: Vec<M>,
    entry_array: Vec<MaybeUninit<(K, V)>>,
    lb_capacity: u8,
    rehashing: AtomicUsize,
}

impl<K, V, M: Default> Array<K, V, M> {
    pub fn new(capacity: usize) -> Array<K, V, M> {
        let lb_capacity = Self::calculate_lb_metadata_array_size(capacity);
        let mut array = Array {
            metadata_array: Vec::with_capacity(1 << lb_capacity),
            entry_array: Vec::with_capacity((1 << lb_capacity) * 10),
            lb_capacity: lb_capacity,
            rehashing: AtomicUsize::new(0),
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

    pub fn get_key_value_pair(&self, index: usize) -> &(K, V) {
        unsafe { &*self.entry_array[index].as_ptr() }
    }

    fn insert(&mut self, index: usize, key: K, value: V) {
        self.entry_array[index] = MaybeUninit::new((key, value));
    }

    fn remove(&mut self, index: usize) {
        unsafe {
            std::ptr::drop_in_place(self.entry_array[index].as_mut_ptr());
        }
    }

    fn capacity(&self) -> usize {
        (1 << self.lb_capacity) * 10
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

    struct Checker<'a> {
        data: &'a AtomicUsize,
    }

    impl<'a> Checker<'a> {
        fn new(data: &'a AtomicUsize) -> Checker<'a> {
            data.fetch_add(1, Relaxed);
            Checker { data: data }
        }
    }

    impl<'a> Drop for Checker<'a> {
        fn drop(&mut self) {
            self.data.fetch_sub(1, Relaxed);
        }
    }

    #[test]
    fn basic_insert_remove() {
        let data = AtomicUsize::new(0);
        let mut array: Array<usize, Checker, i8> = Array::new(160);
        assert_eq!(array.capacity(), 160);
        for i in 0..array.capacity() {
            if i % 2 == 0 {
                array.insert(i, i, Checker::new(&data));
                assert_eq!(unsafe { (*array.entry_array[i].as_ptr()).0 }, i);
            }
        }
        assert_eq!(data.load(Relaxed), array.capacity() / 2);
        for i in 0..array.capacity() {
            if i % 2 == 0 {
                array.remove(i);
            }
        }
        assert_eq!(data.load(Relaxed), 0);
    }
}
