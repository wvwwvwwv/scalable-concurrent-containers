use crossbeam_epoch::Atomic;
use std::mem::MaybeUninit;

pub const ARRAY_SIZE: usize = 8;

/// Each entry has four states: initial = 0, locked = 1, occupied = 2, removed = 3
const INITIAL: u64 = 0;
const LOCKED: u64 = 1;
const OCCUPIED: u64 = 2;
const REMOVED: u64 = 3;
const STATE_MASK: u64 = (1u64 << ARRAY_SIZE * 2) - 1;

/// Ench entry is assigned a rank
const RANK_MASK: u64 = ((1u64 << ARRAY_SIZE * 4) - 1) << (ARRAY_SIZE * 2);

/// Reserved
const RESERVED: u64 = ((1u64 << (ARRAY_SIZE * 2)) - 1) << (ARRAY_SIZE * 6);

pub type EntryArray<K, V> = [MaybeUninit<(K, V)>; ARRAY_SIZE];

/// Leaf stores key-value pairs.
pub struct Leaf<K: Clone + Ord + Sync, V: Clone + Sync> {
    metadata: u64,
    entry_array: EntryArray<K, V>,
    next: Atomic<Leaf<K, V>>,
}

impl<K: Clone + Ord + Sync, V: Clone + Sync> Leaf<K, V> {
    pub fn new(capacity: usize) {}
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn static_assertions() {
        assert_eq!(RESERVED | RANK_MASK | STATE_MASK, !0u64);
    }

    #[test]
    fn iteration() {}
}
