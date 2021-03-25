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
- SCC version: 0.4.11
- The hashmap is generated using the default parameters: HashMap<usize, usize, RandomState> = Default::default().
- Each test is run twice in a single process in order to minimize the effect of page faults as the overhead is unpredictable.

#### Test data
- Each thread is assigned a disjoint range of u64 integers.
- The entropy of the test input is very low, however it does not undermine the test result as the key distribution method is agnostic to the input pattern.
- The performance test code asserts the expected outcome of each operation, and the post state of the hashmap instance.

#### Test workload: local
- Insert: each thread inserts 128M records.
- Read: each thread reads 128M records.
- Scan: each thread scans the entire hashmap.
- Remove: each thread removes 128M records.
- The data for Read/Remove tests is populated by the Insert test.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Insert | 134.519639194s | 165.001678899s | 231.081117542s | 351.286311763s |
| Read   |  92.83194805s  | 104.560364479s | 114.468443191s | 124.8641862s   |
| Scan   |  42.086156353s | 108.655462554s | 229.909702447s | 474.113480956s |
| Remove | 109.926310571s | 123.499814546s | 139.1093042s   | 154.684509984s |

#### Test workload: local-remote
- Insert/Remove: each thread additionally tries to perform assigned operations using keys belonging to other threads.
- Mixed: each thread performs 128M insert-local -> insert-remote -> read-local -> read-remote -> remove-local -> remove-remote.
- The data for Mixed/Remove tests is populated by the Insert test.
- The target remote thread is randomly chosen.
- The total operation count per Insert/Remove thread is 256M, and half of the operations are bound to fail.
- The total operation count per Mixed thread is 768M, and about half of the operations are bound to fail.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Insert | 249.260816589s | 301.757140479s | 399.315496693s | 598.363026383s |
| Mixed  | 310.705241166s | 337.750491321s | 363.707265976s | 410.698464196s |
| Remove | 208.355622788s | 226.59800359s  | 251.086396624s | 266.482387949s |

## scc::HashIndex <a name="hashindex"></a>

scc::HashIndex is an index version of scc::HashMap. It allows readers to access key-value pairs without performing a single write operation on the data structure. In order to take advantage of immutability and epoch-based reclamation, it requires the key and value types to implement the Clone trait.

### Performance

#### Test setup
- OS: SUSE Linux Enterprise Server 15 SP1
- CPU: Intel(R) Xeon(R) CPU E7-8880 v4 @ 2.20GHz x 4
- RAM: 1TB
- Rust compiler version: 1.50.0
- SCC version: 0.4.11
- The hashindex is generated using the default parameters: HashIndex<String, String, RandomState> = Default::default().
- Each test is run twice in a single process in order to minimize the effect of page faults as the overhead is unpredictable.

#### Test data
- Each thread is assigned a disjoint range of u64 integers, and each u64 integer is converted into a String.
- The performance test code asserts the expected outcome of each operation, and the post state of the hashindex instance.

#### Test workload
- Insert: each thread inserts 16M records.
- Read: each thread reads 16M records.
- Scan: each thread scans the entire hashindex.
- Remove: each thread removes 16M records.
- The data for Read/Remove tests is populated by the Insert test.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Insert |  89.015914443s | 116.402345094s | 143.86420979s  | 223.296876115s |
| Read   |  18.975302649s |  19.858082662s |  20.862552983s |  22.646245396s |
| Scan   |   3.640621149s |   7.327157641s |  15.847438364s |  31.771622377s |
| Remove |  69.259331734s |  82.053630018s |  98.725056905s | 109.829727509s |

## scc::TreeIndex <a name="treeindex"></a>

scc::TreeIndex is a B+ tree optimized for read operations. Locks are only acquired on structural changes, and read/scan operations are neither blocked nor interrupted by other threads. The semantics of the read operation on a single key is similar to snapshot isolation in terms of database management software, as readers may not see the snapshot of data that is newer than the read snapshot. All the key-value pairs stored in a leaf are never dropped until the leaf becomes completely unreachable, thereby ensuring immutability of all the reachable key-value pairs. scc::TreeIndex harnesses this immutability of the leaf data structure to allow read operations to access key-value pairs without modifying the data structure.

### Performance

#### Test setup
- OS: SUSE Linux Enterprise Server 15 SP1
- CPU: Intel(R) Xeon(R) CPU E7-8880 v4 @ 2.20GHz x 4
- RAM: 1TB
- Rust compiler version: 1.50.0
- SCC version: 0.4.11
- The treeindex is generated using the default parameters: TreeIndex<String, String> = Default::default().
- Each test is run twice in a single process in order to minimize the effect of page faults as the overhead is unpredictable.

#### Test data
- Each thread is assigned a disjoint range of u64 integers, and each u64 integer is converted into a String.
- The performance test code asserts the expected outcome of each operation, and the post state of the treeindex instance.

#### Test workload
- Insert: each thread inserts 16M records.
- Read: each thread reads 16M records.
- Scan: each thread scans the entire treeindex.
- Remove: each thread removes 16M records.
- The data for Read/Remove tests is populated by the Insert test.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Insert |  71.105692153s |  71.60182204s  |  69.300164192s |  70.050318852s |
| Read   |  25.959253558s |  28.21858048s  |  31.636098149s |  32.678456754s |
| Scan   |  36.059027123s |  68.349526938s | 129.710438186s | 218.026359862s |
| Remove |  82.910451376s | 120.185809134s | 162.444301189s | 241.25877992s  |

## Changelog

#### 0.4.11
Optimize memory-usage of TreeIndex: #24 (partial)
#### 0.4.10
Fix HashIndex::remove: #32, API change HashIndex::len
#### 0.4.9
API change: TreeIndex::from -> TreeIndex::range, #33, fix #32

## Milestones <a name="milestones"></a>

[Milestones](https://github.com/wvwwvwwv/scalable-concurrent-containers/milestones)
