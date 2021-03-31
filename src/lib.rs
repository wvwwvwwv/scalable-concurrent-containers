//! Scalable concurrent containers.
//!
//! # scc::HashMap
//! [`scc::HashMap`] is a concurrent hash map that dynamically grows and shrinks in a non-blocking manner without sharding.
//!
//! # scc::HashIndex
//! [`scc::HashIndex`] is a concurrent hash index that is similar to scc::HashMap, but optimized for read operations.
//!
//! # scc::TreeIndex
//! [`scc::TreeIndex`] is a concurrent B+ tree index optimized for scan and read.
//!
//! [`scc::HashMap`]: hashmap::HashMap
//! [`scc::HashIndex`]: hashindex::HashIndex
//! [`scc::TreeIndex`]: treeindex::TreeIndex

// Common modules.
mod common;

// scc::HashMap.
mod hash_map;
pub use hash_map::Accessor;
pub use hash_map::Cursor;
pub use hash_map::HashMap;
pub use hash_map::Ticket;

// scc::HashIndex.
mod hash_index;
pub use hash_index::HashIndex;
pub use hash_index::Visitor;

// scc::TreeIndex.
mod tree_index;
pub use tree_index::Range;
pub use tree_index::Scanner;
pub use tree_index::TreeIndex;
