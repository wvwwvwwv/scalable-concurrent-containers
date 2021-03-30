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
mod hashmap;
pub use hashmap::Accessor;
pub use hashmap::Cursor;
pub use hashmap::HashMap;
pub use hashmap::Ticket;

// scc::HashIndex.
mod hashindex;
pub use hashindex::HashIndex;
pub use hashindex::Visitor;

// scc::TreeIndex.
mod treeindex;
pub use treeindex::Range;
pub use treeindex::Scanner;
pub use treeindex::TreeIndex;
