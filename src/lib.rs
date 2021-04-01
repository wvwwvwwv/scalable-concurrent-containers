//! Scalable concurrent containers.
//!
//! # scc::HashMap
//! [`scc::HashMap`] is a concurrent hash map that dynamically grows and shrinks without blocking other operations.
//!
//! # scc::HashIndex
//! [`scc::HashIndex`] is a read-optimized concurrent hash index that is similar to scc::HashMap.
//!
//! # scc::TreeIndex
//! [`scc::TreeIndex`] is a read-optimized concurrent B+ tree index.
//!
//! [`scc::HashMap`]: hash_map::HashMap
//! [`scc::HashIndex`]: hash_index::HashIndex
//! [`scc::TreeIndex`]: tree_index::TreeIndex

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
