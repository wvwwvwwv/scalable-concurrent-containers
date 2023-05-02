#![deny(missing_docs, warnings, clippy::all, clippy::pedantic)]

//! Concurrent and asynchronous containers.
//!
//! * [`HashMap`]: concurrent and asynchronous hash map.
//! * [`HashSet`]: concurrent and asynchronous hash set.
//! * [`HashIndex`]: read-optimized concurrent and asynchronous hash map.
//! * [`HashCache`]: concurrent and asynchronous pseudo-LRU cache backed by [`HashMap`].
//! * [`TreeIndex`]: read-optimized concurrent and asynchronous B+ tree.
//!
//! Utilities for concurrent programming.
//!
//! * [`ebr`]: epoch-based reclamation.
//! * [`LinkedList`]: lock-free concurrent linked list type trait.
//! * [`Bag`]: lock-free concurrent unordered instance container.
//! * [`Queue`]: lock-free concurrent first-in-first-out container.
//! * [`Stack`]: lock-free concurrent last-in-first-out container.

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

mod bag;
pub use bag::Bag;

mod queue;
pub use queue::Queue;

mod stack;
pub use stack::Stack;

pub mod tree_index;
pub use tree_index::TreeIndex;

pub mod ebr;

mod exit_guard;
mod hash_table;
mod wait_queue;

#[cfg(feature = "serde")]
mod serde;

#[cfg(test)]
mod tests;
