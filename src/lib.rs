//! Scalable concurrent containers.
//!
//! # scc::HashMap
//! [`HashMap`] is a concurrent hash map that dynamically grows and shrinks without blocking other operations.
//!
//! # scc::HashIndex
//! [`HashIndex`] is a read-optimized concurrent hash index that is similar to scc::HashMap.
//!
//! # scc::TreeIndex
//! [`TreeIndex`] is a read-optimized concurrent B+ tree index.

// Common modules.
mod common;

pub mod ebr;

pub mod hash_map;
pub use hash_map::HashMap;

pub mod hash_index;
pub use hash_index::HashIndex;

pub mod tree_index;
pub use tree_index::TreeIndex;
