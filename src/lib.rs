#![deny(missing_docs, warnings, clippy::all, clippy::pedantic)]

//! Scalable concurrent containers.
//!
//! * [`LinkedList`](LinkedList).
//! * [`HashMap`](HashMap).
//! * [`HashIndex`](HashIndex).
//! * [`TreeIndex`](TreeIndex).
//!
//! # [Awaitable concurrent containers](awaitable)
//!
//! The concurrent container types in `awaitable` are meant for asynchronous code blocks as their
//! key operations are implemented in asynchronous methods where execution can be suspended on a
//! conflict over resources among threads.
//!
//! * [`HashMap`](awaitable::HashMap).
//! * [`TreeIndex`](awaitable::TreeIndex).
//!
//! # [`EBR`](ebr)
//!
//! The [`ebr`] module implements epoch-based reclamation for every container type in this crate.

mod linked_list;
pub use linked_list::LinkedList;

pub mod hash_map;
pub use hash_map::HashMap;

pub mod hash_index;
pub use hash_index::HashIndex;

pub mod hash_set;
pub use hash_set::HashSet;

pub mod tree_index;
pub use tree_index::TreeIndex;

pub mod awaitable;
pub mod ebr;

mod hash_table;
mod tests;
