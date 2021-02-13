//! Scalable concurrent containers.
//!
//! # [`scc::HashMap`]
//! [`scc::HashMap`] is a concurrent hash map that dynamically grows and shrinks in a non-blocking manner without sharding.
//!
//! # [`scc::TreeIndex`]
//! [`scc::TreeIndex`] is a concurrent B+ tree index optimized for scan and read.
//!
//! [`scc::HashMap`]: hashmap::HashMap
//! [`scc::TreeIndex`]: treeindex::TreeIndex

mod hashmap;
mod treeindex;

// scc::HashMap
pub use hashmap::Accessor;
pub use hashmap::Cursor;
pub use hashmap::HashMap;

// scc::TreeIndex
pub use treeindex::Scanner;
pub use treeindex::TreeIndex;
