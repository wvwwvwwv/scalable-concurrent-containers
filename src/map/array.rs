use std::convert::TryInto;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicUsize;

pub struct Array<K, V, M: Default> {
    metadata_array: Vec<M>,
    entry_array: Vec<MaybeUninit<(K, V)>>,
    log_capacity: u8,
    rehashing: AtomicUsize,
}

impl<K, V, M: Default> Array<K, V, M> {
    pub fn new(capacity: usize) -> Array<K, V, M> {
        let (adjusted_capacity, log_capacity) = Self::calculated_adjusted_capacity(capacity);
        let mut array = Array {
            metadata_array: Vec::with_capacity(adjusted_capacity),
            entry_array: Vec::with_capacity(adjusted_capacity * 10),
            log_capacity: log_capacity,
            rehashing: AtomicUsize::new(0),
        };
        for _ in 0..adjusted_capacity {
            array.metadata_array.push(Default::default());
        }
        for _ in 0..adjusted_capacity * 10 {
            array
                .entry_array
                .push(unsafe { MaybeUninit::uninit().assume_init() });
        }
        array
    }

    pub fn calculated_adjusted_capacity(capacity: usize) -> (usize, u8) {
        let mut adjusted_capacity = 2;
        let mut log_capacity = 1;

        // it supports both 32-bit and 64-bit processors
        let max_consumable_bits = if std::mem::size_of::<usize>() >= 8 {
            60
        } else {
            28
        };

        // the maximum size of the key-value pair array is (1 << 60) * 10
        for i in 1..max_consumable_bits {
            if adjusted_capacity >= (capacity / 10 + 1) {
                log_capacity = i;
                break;
            }
            adjusted_capacity = adjusted_capacity << 1;
        }
        assert_eq!(adjusted_capacity, 1 << log_capacity);
        (adjusted_capacity, log_capacity)
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
        (1 << self.log_capacity) * 10
    }

    fn calculate_metadata_array_index(&self, hash: u64) -> usize {
        (hash >> (64 - self.log_capacity)).try_into().unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::atomic::Ordering::Relaxed;

    #[test]
    fn basic_array() {
        let very_small: Array<u64, bool, bool> = Array::new(0);
        assert_eq!(very_small.capacity(), 20);
        assert_eq!(very_small.calculate_metadata_array_index(1 << 63), 1);
        let small: Array<u32, i32, i16> = Array::new(16);
        assert_eq!(small.capacity(), 20);
        assert_eq!(small.calculate_metadata_array_index(1 << 63), 1);
        assert_eq!(small.calculate_metadata_array_index(1 << 62), 0);
        let medium: Array<u8, i64, i8> = Array::new(190);
        assert_eq!(medium.capacity(), 320);
        assert_eq!(medium.calculate_metadata_array_index(1 << 63), 16);
        assert_eq!(medium.calculate_metadata_array_index(1 << 59), 1);
        let large: Array<usize, u16, bool> = Array::new(5000);
        assert_eq!(large.capacity(), 5120);
        assert_eq!(large.calculate_metadata_array_index(1 << 63), 256);
        assert_eq!(large.calculate_metadata_array_index(1 << 55), 1);
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
        let mut array: Array<usize, Checker, i8> = Array::new(190);
        assert_eq!(array.capacity(), 320);
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
