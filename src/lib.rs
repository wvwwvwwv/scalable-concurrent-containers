#![deny(missing_docs, warnings, clippy::all, clippy::pedantic)]
#![doc = include_str!("../README.md")]

pub mod bag;
pub use bag::Bag;

// Re-export [`sdd`](https://crates.io/crates/sdd) modules for backward compatibility.
pub use sdd::{AtomicShared, Guard, Shared, Tag};
pub use sdd::{LinkedEntry, LinkedList, Queue, Stack};
pub use sdd::{queue, stack};

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

pub mod tree_index;
pub use tree_index::TreeIndex;

mod async_helper;
mod exit_guard;
mod hash_table;

#[cfg(feature = "serde")]
mod serde;

#[cfg(test)]
mod tests;
