use std::mem::MaybeUninit;
use std::sync::atomic::AtomicUsize;

pub struct Array<K, V, M: Default> {
    metadata_array: Vec<M>,
    entry_array: Vec<MaybeUninit<(K, V)>>,
    capacity: usize,
    rehashing: AtomicUsize,
}

impl<K, V, M: Default> Array<K, V, M> {
    fn new(capacity: usize) -> Array<K, V, M> {
        let mut array = Array {
            metadata_array: Vec::with_capacity(capacity),
            entry_array: Vec::with_capacity(capacity * 10),
            capacity: capacity,
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
}

impl<K, V, M: Default> Drop for Array<K, V, M> {
    fn drop(&mut self) {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic_assumptions() {}
}
