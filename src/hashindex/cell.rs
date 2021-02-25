use crossbeam_epoch::Atomic;

pub const ARRAY_SIZE: usize = 32;
pub const MAX_RESIZING_FACTOR: usize = 6;

pub struct Cell<K: Eq, V> {
    _dummy: Atomic<(K, V)>,
}

impl<K: Eq, V> Default for Cell<K, V> {
    fn default() -> Self {
        Cell::<K, V> {
            _dummy: Atomic::null(),
        }
    }
}
