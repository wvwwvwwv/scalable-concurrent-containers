//! Scalable concurrent containers.
//!
//! # scc::HashMap
//! A concurrent hash map that dynamically grows and shrinks in a non-blocking manner without sharding.
//!
//! # scc::TreeIndex
//! A concurrent tree index optimized for scan and read.

mod hashmap;
mod treeindex;

// scc::HashMap
pub use hashmap::Accessor;
pub use hashmap::Cursor;
pub use hashmap::HashMap;

// scc::TreeIndex
pub use treeindex::Scanner;
pub use treeindex::TreeIndex;
