use std::mem::MaybeUninit;
use std::sync::atomic::AtomicUsize;

pub struct Array<K, V, M> {
    metadata_array: Vec<M>,
    entry_array: Vec<MaybeUninit<(K, V)>>,
    capacity: usize,
    rehashing: AtomicUsize,
}

impl<K, V, M> Array<K, V, M> {
    fn new(capacity: usize) -> Array<K, V, M> {
        Array {
            metadata_array: Vec::with_capacity(capacity),
            entry_array: Vec::with_capacity(capacity),
            capacity: capacity,
            rehashing: AtomicUsize::new(0),
        }
    }
}

impl<K, V, M> Drop for Array<K, V, M> {
    fn drop(&mut self) {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic_assumptions() {
    }
}
