#![deny(missing_docs, warnings, clippy::all, clippy::pedantic)]

//! Scalable concurrent containers.
//!
//! # [`EBR`](ebr)
//!
//! The [`ebr`] module implements epoch-based reclamation for [`LinkedList`], [`HashMap`],
//! [`HashIndex`], and [`TreeIndex`].
//!
//! # [`LinkedList`]
//! [`LinkedList`] is a type trait that implements wait-free list modification operations for
//! a generic concurrent list.
//!
//! # [`HashMap`]
//! [`HashMap`] is a concurrent hash map that dynamically grows and shrinks without blocking
//! other operations.
//!
//! # [`HashIndex`]
//! [`HashIndex`] is a read-optimized concurrent hash index that is similar to [`HashMap`].
//!
//! # [`TreeIndex`]
//! [`TreeIndex`] is a read-optimized concurrent B+ tree index.

pub mod ebr;

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

mod hash_table;

mod tests;
