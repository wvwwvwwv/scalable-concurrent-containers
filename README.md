# Scalable Concurrent Containers

The crate offers highly scalable concurrent containers written in the Rust language.

- [scc::HashMap](#hashmap) is a concurrent hash map.
- [scc::HashIndex](#hashindex) is a concurrent hash index allowing lock-free read and scan.
- [scc::TreeIndex](#treeindex) is a concurrent B+ tree allowing lock-free read and scan.

The read/write operations on a single key-value pair of scc::HashMap are linearizable while that of scc::HashIndex and scc::TreeIndex are similar to snapshot isolation.

## scc::HashMap <a name="hashmap"></a>

scc::HashMap is a scalable in-memory unique key-value store that is targeted at highly concurrent heavy workloads. It does not distribute data to multiple shards as most concurrent hash maps do, instead only does it have a single array of structured data. The metadata management strategy is similar to that of Swisstable; a metadata cell which is separated from key-value pairs, is a 64-byte data structure for managing consecutive 32 key-value pairs. The metadata cell also has a linked list of key-value pair arrays for hash collision resolution. scc::HashMap automatically enlarges and shrinks the capacity of its internal array, and resizing happens without blocking other operations and threads. In order to keep the predictable latency of each operation, it does not rehash every entry in the container at once when resizing, instead it distributes the resizing workload to future access to the data structure. Once the hardware transactional memory intrinsics are generally available, read operations on a small key-value pair (~16KB) can be write-free.

### Performance

#### Test setup
- OS: SUSE Linux Enterprise Server 15 SP1
- CPU: Intel(R) Xeon(R) CPU E7-8880 v4 @ 2.20GHz x 4
- RAM: 1TB
- Rust compiler version: 1.50.0
- SCC version: 0.4.1
- The hashmap is generated using the default parameters: the RandomState hasher builder, and 256 preallocated entries.
- Each test is run twice in a single process in order to minimize the effect of page faults as the overhead is unpredictable.

#### Test data
- Each thread is assigned a disjoint range of u64 integers.
- The entropy of the test input is very low, however it does not undermine the test result as the key distribution method is agnostic to the input pattern.
- The performance test code asserts the expected outcome of each operation, and the post state of the hashmap instance.

#### Test workload: local
- Insert: each thread inserts 128M records.
- Read: each thread reads 128M records.
- Remove: each thread removes 128M records.
- The data for Read/Remove tests is populated by the Insert test.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Insert | 129.704352035s | 153.744335742s | 220.257070192s | 299.582540084s |
| Read   |  94.333388963s | 106.670376931s | 118.535401915s | 130.963194287s |
| Remove | 109.926310571s | 123.499814546s | 139.1093042s   | 169.109640038s |

#### Test workload: local-remote
- Insert/Remove: each thread additionally tries to perform assigned operations using keys belonging to other threads.
- Mixed: each thread performs 128M insert-local -> insert-remote -> read-local -> read-remote -> remove-local -> remove-remote.
- The data for Mixed/Remove tests is populated by the Insert test.
- The target remote thread is randomly chosen.
- The total operation count per Insert/Remove thread is 256M, and half of the operations are bound to fail.
- The total operation count per Mixed thread is 768M, and about half of the operations are bound to fail.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Insert | 229.7548302s   | 261.184001135s | 296.469319497s | 364.441279906s |
| Mixed  | 303.608915125s | 325.812785309s | 365.778084065s | 409.283309922s |
| Remove | 192.868693139s | 211.50268085s  | 230.968140497s | 265.453334202s |

## scc::HashIndex <a name="hashindex"></a>

- Not fully implemented.
- Not optimized.

scc::HashIndex is an index version of scc::HashMap. It allows readers to access key-value pairs without performing a single write operation on the data structure. In order to take advantage of immutability and epoch-based reclamation, it requires the key and value types to implement the Clone trait.

## scc::TreeIndex <a name="treeindex"></a>

- Not fully optimized; benchmark results will come once adequately optimized.

scc::TreeIndex is a B+ tree optimized for read operations. Locks are only acquired on structural changes, and read/scan operations are neither blocked nor interrupted by other threads. The semantics of the read operation on a single key is similar to snapshot isolation in terms of database management software, as readers may not see the snapshot of data that is newer than the read snapshot. All the key-value pairs stored in a leaf are never dropped until the leaf becomes completely unreachable, thereby ensuring immutability of all the reachable key-value pairs. scc::TreeIndex harnesses this immutability of the leaf data structure to allow read operations to access key-value pairs without modifying the data structure.

## Changelog

#### 0.4.9
API change: TreeIndex::from -> TreeIndex::range
#### 0.4.8
Optimize HashIndex: #32, add HashMap::book: #31, change HashMap::len
#### 0.4.7
HashIndex initial implementation
#### 0.4.6
HashMap: fix #28 and API change #30
#### 0.4.5
Update crossbeam-epoch

## Milestones <a name="milestones"></a>

[Milestones](https://github.com/wvwwvwwv/scalable-concurrent-containers/milestones)
