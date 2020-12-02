//! Scalable concurrent containers.
//!
//! # scc::HashMap
//! A concurrent hash map that dynamically grows and shrinks in a non-blocking manner without sharding.
//!
//! # scc::TreeIndex
//! A concurrent tree index optimized for scan and read.

mod hashmap;
mod treeindex;

pub use hashmap::Accessor;
pub use hashmap::HashMap;
pub use hashmap::Scanner;
pub use hashmap::Statistics;
