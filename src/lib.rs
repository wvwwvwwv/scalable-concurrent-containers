#![deny(missing_docs, warnings, clippy::all, clippy::pedantic)]
#![doc = include_str!("../README.md")]

pub mod hash_map;
pub use hash_map::HashMap;

pub mod hash_set;
pub use hash_set::HashSet;

pub mod hash_index;
pub use hash_index::HashIndex;

pub mod hash_cache;
pub use hash_cache::HashCache;

mod linked_list;
pub use linked_list::Entry as LinkedEntry;
pub use linked_list::LinkedList;

pub mod bag;
pub use bag::Bag;

pub mod queue;
pub use queue::Queue;

pub mod stack;
pub use stack::Stack;

pub mod tree_index;
pub use tree_index::TreeIndex;

mod exit_guard;
mod hash_table;
mod wait_queue;

#[cfg(not(feature = "equivalent"))]
mod equivalent;

#[cfg(feature = "serde")]
mod serde;

#[cfg(feature = "loom")]
mod maybe_std {
    pub(crate) use loom::sync::atomic::{AtomicU8, AtomicUsize};
    pub(crate) use loom::thread::yield_now;
}

#[cfg(not(feature = "loom"))]
mod maybe_std {
    pub(crate) use std::sync::atomic::{AtomicU8, AtomicUsize};
    pub(crate) use std::thread::yield_now;
}

mod range_helper {
    use crate::Comparable;
    use std::ops::Bound::{Excluded, Included, Unbounded};
    use std::ops::RangeBounds;

    /// Emulates `RangeBounds::contains`.
    pub(crate) fn contains<K, Q, R: RangeBounds<Q>>(range: &R, key: &K) -> bool
    where
        Q: Comparable<K> + ?Sized,
    {
        (match range.start_bound() {
            Included(start) => start.compare(key).is_le(),
            Excluded(start) => start.compare(key).is_lt(),
            Unbounded => true,
        }) && (match range.end_bound() {
            Included(end) => end.compare(key).is_ge(),
            Excluded(end) => end.compare(key).is_gt(),
            Unbounded => true,
        })
    }
}

/// Re-exports the [`sdd`](https://crates.io/crates/sdd) crate for backward compatibility.
pub use sdd as ebr;

pub use equivalent::{Comparable, Equivalent};

#[cfg(test)]
mod tests;
