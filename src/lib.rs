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

/// Re-exports the [`sdd`](https://crates.io/crates/sdd) crate for backward compatibility.
pub use sdd as ebr;

mod exit_guard;
mod hash_table;
mod wait_queue;

#[cfg(feature = "serde")]
mod serde;

#[cfg(test)]
mod tests;
