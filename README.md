# SCC: Scalable Concurrent Containers

SCC offers scalable concurrent containers written in the Rust language.

## scc::HashMap

scc::HashMap is a scalable in-memory unique key-value store that is targeted at highly concurrent workloads. It does not distribute data to multiple shards as most concurrent hash maps would do, instead only does it have a single array of buckets, and each bucket governing ten consecutive key-value pairs has a space-optimized read-write lock installed to protect the data. scc::HashMap automatically doubles and halves the capacity of its internal array, and it happens without blocking other operations. In order to keep the predictable latency of each operation, it does not rehash every entry in the container at once when resizing, instead it distributes the resizing workload to future access to the data structure. The metadata management strategy is similar to that of Swisstable; in a bucket, a 64-byte area is reserved for ten keys, and key-value pairs are stores in a separate array.
