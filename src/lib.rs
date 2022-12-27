#![deny(missing_docs, warnings, clippy::all, clippy::pedantic)]

//! Concurrent and asynchronous containers.
//!
//! * [`HashMap`]: concurrent and asynchronous hash map.
//! * [`HashSet`]: concurrent and asynchronous hash set.
//! * [`HashIndex`]: concurrent and asynchronous hash map optimized for read.
//! * [`TreeIndex`]: concurrent and asynchronous B+ tree optimized for read.
//! * [`Bag`]: lock-free concurrent unordered instance container.
//! * [`Queue`]: lock-free concurrent container.
//! * [`Stack`]: lock-free concurrent container.
//!
//! Utilities.
//!
//! * [`ebr`]: epoch-based reclamation.
//! * [`LinkedList`]: lock-free concurrent linked list type trait.

pub mod hash_map;
pub use hash_map::HashMap;

pub mod hash_index;
pub use hash_index::HashIndex;

pub mod hash_set;
pub use hash_set::HashSet;

mod linked_list;
pub use linked_list::LinkedList;

mod bag;
pub use bag::Bag;

mod queue;
pub use queue::Queue;

mod stack;
pub use stack::Stack;

mod channel;

pub mod tree_index;
pub use tree_index::TreeIndex;

pub mod ebr;

mod exit_guard;
mod hash_table;
mod wait_queue;

#[cfg(test)]
mod tests;
