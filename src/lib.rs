#![deny(missing_docs, warnings, clippy::all, clippy::pedantic)]

//! Concurrent containers.
//!
//! * [`HashMap`]: concurrent hash map.
//! * [`HashIndex`]: concurrent hash map optimized for read.
//! * [`TreeIndex`]: concurrent B+ tree optimized for read.
//!
//! Utilities.
//!
//! * [Epoch-based-reclamation](ebr).
//! * [`LinkedList`]: lock-free concurrent linked list type trait.
//! * [`Queue`]: lock-free concurrent queue.

pub mod hash_map;
pub use hash_map::HashMap;

pub mod hash_index;
pub use hash_index::HashIndex;

pub mod hash_set;
pub use hash_set::HashSet;

mod linked_list;
pub use linked_list::LinkedList;

mod queue;
pub use queue::Queue;

pub mod tree_index;
pub use tree_index::TreeIndex;

pub mod ebr;

mod async_yield;
mod hash_table;
mod wait_queue;

mod tests;
