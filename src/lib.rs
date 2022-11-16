#![deny(missing_docs, warnings, clippy::all, clippy::pedantic)]

//! Concurrent and asynchronous containers.
//!
//! * [`HashMap`]: concurrent and asynchronous hash map.
//! * [`HashIndex`]: concurrent and asynchronous hash map optimized for read.
//! * [`TreeIndex`]: concurrent and asynchronous B+ tree optimized for read.
//! * [`Queue`]: lock-free concurrent queue.
//!
//! Utilities.
//!
//! * [`ebr`]: epoch-based reclamation.
//! * [`LinkedList`]: lock-free concurrent linked list type trait.

mod bag;
pub use bag::Bag;

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

mod hash_table;
mod wait_queue;

mod tests;
