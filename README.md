# SCC: Scalable Concurrent Containers

SCC offers scalable concurrent containers written in the Rust language.

The first version of SCC only includes a concurrent hash map.

## scc::HashMap

scc::HashMap is a scalable in-memory unique key-value store that is targeted at highly concurrent workloads. It does not distribute data to multiple shards as most concurrent hash map implementation would do, instead it only has a single array of buckets, and each bucket has a space-optimized read-write lock installed to protect the data. scc::HashMap automatically shrinks and enlarges the size of its internal array, and it happens without blocking other operations. In order to keep the predictable latency of each operation, it does not rehash every entry in the container at once when resizing, instead it distributes the resizing workload to future access to the data structure.

## scc::Heap

[To be implemented]
It implements a latch-free skip-list.

## scc::Tree

[To be implemented]
It implements BwTree.
