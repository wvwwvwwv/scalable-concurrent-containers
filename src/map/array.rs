use std::convert::TryInto;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicUsize;

pub struct Array<K, V, M: Default> {
    metadata_array: Vec<M>,
    entry_array: Vec<MaybeUninit<(K, V)>>,
    log_base_two_capacity: u8,
    rehashing: AtomicUsize,
}

impl<K, V, M: Default> Array<K, V, M> {
    pub fn new(log_base_two_capacity: u8) -> Array<K, V, M> {
        debug_assert!(log_base_two_capacity > 0);
        let mut array = Array {
            metadata_array: Vec::with_capacity(1 << log_base_two_capacity),
            entry_array: Vec::with_capacity((1 << log_base_two_capacity) * 10),
            log_base_two_capacity: log_base_two_capacity,
            rehashing: AtomicUsize::new(0),
        };
        for _ in 0..(1 << log_base_two_capacity) {
            array.metadata_array.push(Default::default());
        }
        for _ in 0..(1 << log_base_two_capacity) * 10 {
            array
                .entry_array
                .push(unsafe { MaybeUninit::uninit().assume_init() });
        }
        array
    }

    fn get_cell(&self, index: usize) -> &M {
        &self.metadata_array[index]
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
        (1 << self.log_base_two_capacity) * 10
    }

    fn calculate_metadata_array_index(&self, hash: u64) -> usize {
        (hash >> (64 - self.log_base_two_capacity)).try_into().unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::atomic::Ordering::Relaxed;

    #[test]
    fn basic_array() {
        let small: Array<u32, i32, i16> = Array::new(1);
        assert_eq!(small.capacity(), 20);
        assert_eq!(small.calculate_metadata_array_index(1 << 63), 1);
        assert_eq!(small.calculate_metadata_array_index(1 << 62), 0);
        assert_eq!(small.calculate_metadata_array_index(1 << 61), 0);
        let medium: Array<u8, i64, i8> = Array::new(4);
        assert_eq!(medium.capacity(), 160);
        assert_eq!(medium.calculate_metadata_array_index(1 << 63), 8);
        assert_eq!(medium.calculate_metadata_array_index(1 << 60), 1);
        assert_eq!(medium.calculate_metadata_array_index(1 << 59), 0);
        let large: Array<usize, u16, bool> = Array::new(9);
        assert_eq!(large.capacity(), 5120);
        assert_eq!(large.calculate_metadata_array_index(1 << 63), 256);
        assert_eq!(large.calculate_metadata_array_index(1 << 55), 1);
        assert_eq!(large.calculate_metadata_array_index(1 << 54), 0);
        let very_large: Array<usize, u16, bool> = Array::new(10);
        assert_eq!(very_large.capacity(), 10240);
        assert_eq!(very_large.calculate_metadata_array_index(1 << 63), 512);
        assert_eq!(very_large.calculate_metadata_array_index(1 << 54), 1);
        assert_eq!(very_large.calculate_metadata_array_index(1 << 53), 0);
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
        let mut array: Array<usize, Checker, i8> = Array::new(4);
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
