#![deny(missing_docs, warnings, clippy::all, clippy::pedantic)]
#![doc = include_str!("../README.md")]

pub mod bag;
pub use bag::Bag;

#[cfg(not(feature = "equivalent"))]
mod equivalent;
pub use equivalent::{Comparable, Equivalent};

mod exit_guard;

pub mod hash_cache;
pub use hash_cache::HashCache;

pub mod hash_index;
pub use hash_index::HashIndex;

pub mod hash_map;
pub use hash_map::HashMap;

pub mod hash_set;
pub use hash_set::HashSet;

mod hash_table;

mod linked_list;
pub use linked_list::Entry as LinkedEntry;
pub use linked_list::LinkedList;

#[cfg(feature = "loom")]
mod maybe_std {
    pub(crate) use loom::sync::atomic::{AtomicU8, AtomicUsize};
}

#[cfg(not(feature = "loom"))]
mod maybe_std {
    pub(crate) use std::sync::atomic::{AtomicU8, AtomicUsize};
}

pub mod queue;
pub use queue::Queue;

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

pub use sdd::{AtomicShared, Guard, Shared, Tag};

#[cfg(feature = "serde")]
mod serde;

pub mod stack;
pub use stack::Stack;

#[cfg(test)]
mod tests;

pub mod tree_index;
pub use tree_index::TreeIndex;

mod async_helper;
