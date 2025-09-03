#![deny(missing_docs, warnings, clippy::all, clippy::pedantic)]
#![doc = include_str!("../README.md")]

pub mod bag;
pub use bag::Bag;

#[cfg(not(feature = "equivalent"))]
mod equivalent;
pub use equivalent::{Comparable, Equivalent};

pub mod hash_cache;
pub use hash_cache::HashCache;

pub mod hash_index;
pub use hash_index::HashIndex;

pub mod hash_map;
pub use hash_map::HashMap;

pub mod hash_set;
pub use hash_set::HashSet;

mod linked_list;
pub use linked_list::Entry as LinkedEntry;
pub use linked_list::LinkedList;

pub mod queue;
pub use queue::Queue;

pub use sdd::{AtomicShared, Guard, Shared, Tag};

#[cfg(feature = "serde")]
mod serde;

pub mod stack;
pub use stack::Stack;

pub mod tree_index;
pub use tree_index::TreeIndex;

mod async_helper;
mod exit_guard;
mod hash_table;

#[cfg(feature = "loom")]
mod maybe_std {
    pub(crate) use loom::sync::atomic::{AtomicU8, AtomicUsize};
}

#[cfg(not(feature = "loom"))]
mod maybe_std {
    pub(crate) use std::sync::atomic::{AtomicU8, AtomicUsize};
}

#[cfg(test)]
mod tests;
