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
    fn new(capacity: usize) -> Array<K, V, M> {
        let mut adjusted_capacity = 1;
        let mut log_capacity = 0;

        // it supports both 32-bit and 64-bit processors
        let max_consumable_bits = if std::mem::size_of::<usize>() >= 8 {
            60
        } else {
            28
        };

        // the maximum size of the key-value pair array is (1 << 60) * 10
        for i in 0..max_consumable_bits {
            if adjusted_capacity >= (capacity / 10 + 1) {
                log_capacity = i;
                break;
            }
            adjusted_capacity = adjusted_capacity << 1;
        }
        assert_eq!(adjusted_capacity, 1 << log_capacity);

        let mut array = Array {
            metadata_array: Vec::with_capacity(adjusted_capacity),
            entry_array: Vec::with_capacity(adjusted_capacity * 10),
            log_capacity: log_capacity,
            rehashing: AtomicUsize::new(0),
        };
        for _ in 0..capacity {
            array.metadata_array.push(Default::default());
        }
        for _ in 0..capacity {
            array
                .entry_array
                .push(unsafe { MaybeUninit::uninit().assume_init() });
        }
        array
    }

    fn capacity(&self) -> usize {
        (1 << self.log_capacity) * 10
    }

    fn calculate_metadata_array_index(&self, hash: u64) -> usize {
        (hash >> (64 - self.log_capacity)).try_into().unwrap()
    }

    fn calculate_partial_hash(&self, hash: u64) -> u32 {
        (hash & ((1 << 32) - 1)).try_into().unwrap()
    }
}

impl<K, V, M: Default> Drop for Array<K, V, M> {
    fn drop(&mut self) {
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic_array() {
        let very_small: Array<u64, bool, bool> = Array::new(0);
        assert_eq!(very_small.capacity(), 10);
        let small: Array<u32, i32, i16> = Array::new(16);
        assert_eq!(small.capacity(), 20);
        let medium: Array<u8, i64, i8> = Array::new(190);
        assert_eq!(medium.capacity(), 320);
        let large: Array<usize, u16, bool> = Array::new(5000);
        assert_eq!(large.capacity(), 5120);
    }
}
