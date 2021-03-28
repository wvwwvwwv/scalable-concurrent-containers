# Scalable Concurrent Containers

The crate offers highly scalable concurrent containers written in the Rust language.

- [scc::HashMap](#hashmap) is a concurrent hash map.
- [scc::HashIndex](#hashindex) is a concurrent hash index allowing lock-free read and scan.
- [scc::TreeIndex](#treeindex) is a concurrent B+ tree allowing lock-free read and scan.

## scc::HashMap <a name="hash map"></a>

scc::HashMap is a scalable in-memory unique key-value store that is targeted at highly concurrent heavy workloads. It does not distribute data to a fixed number of shards as most concurrent hash maps do, instead it has only one single dynamically resizable array of entry metadata. The entry metadata, called a cell, is a 64-byte data structure for managing an array of consecutive 32 key-value pairs. The fixed size key-value pair array is only reachable through its corresponding cell, and its entries are protected by a mutex in the cell; this means that the number of mutex instances increases as the hash map grows, thereby reducing the chance of multiple threads trying to acquire the same mutex. The metadata cell also has a linked list of key-value pair arrays for hash collision resolution. Apart from scc::HashMap having a single cell array, it automatically enlarges and shrinks the capacity of the cell array without blocking other operations and threads; the cell array is resized without relocating all the entries at once, instead it delegates the rehashing workload to future access to the data structure to keep the latency predictable.

### Performance

#### Test setup
- OS: SUSE Linux Enterprise Server 15 SP1
- CPU: Intel(R) Xeon(R) CPU E7-8880 v4 @ 2.20GHz x 4
- RAM: 1TB
- Rust: 1.51.0
- SCC: 0.4.12
- HashMap<usize, usize, RandomState> = Default::default()

#### Test data
- Each thread is assigned a disjoint range of u64 integers.
- The entropy of the test input is low, however it does not undermine the test result as scc::HashMap shuffles the hash value to maximize entropy.
- The performance test code asserts the expected outcome of each operation, and the post state of the hash map instance.

#### Test workload: local
- Each test is run twice in a single process in order to minimize the effect of page faults as the overhead is unpredictable.
- Insert: each thread inserts 128M records.
- Read: each thread reads 128M records.
- Scan: each thread scans the entire hash map once.
- Remove: each thread removes 128M records.
- The read/scan/remove data is populated by the insert test.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Insert | 134.519639194s | 165.001678899s | 231.081117542s | 351.286311763s |
| Read   |  92.83194805s  | 104.560364479s | 114.468443191s | 124.8641862s   |
| Scan   |  42.086156353s | 108.655462554s | 229.909702447s | 474.113480956s |
| Remove | 109.926310571s | 123.499814546s | 139.1093042s   | 154.684509984s |

#### Test workload: local-remote
- Insert, remove: each thread additionally tries to perform operations using keys belonging to other threads.
- Mixed: each thread performs 128M insert-local -> insert-remote -> read-local -> read-remote -> remove-local -> remove-remote.
- The data for mixed/remove tests is populated by the insert test.
- The target remote thread is randomly chosen.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Insert | 249.260816589s | 301.757140479s | 399.315496693s | 598.363026383s |
| Mixed  | 310.705241166s | 337.750491321s | 363.707265976s | 410.698464196s |
| Remove | 208.355622788s | 226.59800359s  | 251.086396624s | 266.482387949s |

## scc::HashIndex <a name="hash index"></a>

scc::HashIndex is an index version of scc::HashMap. It inherits all the characteristics of scc::HashMap except for the fact that it allows readers to access key-value pairs without performing a single write operation on the data structure. In order to make read and scan operations write-free, the key-value pair array is immutable; once a key-value pair is inserted into the array, it never gets modified until the entire array is dropped. The array is seldom coalesced when the array is full and the majority of key-value pairs are marked invalid. Due to the immutability and coalescing operation, it requires the key and value types to implement the Clone trait. Since no locks are acquire when reading a key-value pair, the semantics of scc::HashIndex::read is different from that of scc::HashMap; it is not guaranteed to read the latest snapshot of the key-value pair.

### Performance

#### Test setup
- OS: SUSE Linux Enterprise Server 15 SP1
- CPU: Intel(R) Xeon(R) CPU E7-8880 v4 @ 2.20GHz x 4
- RAM: 1TB
- Rust: 1.51.0
- SCC: 0.4.12
- HashIndex<String, String, RandomState> = Default::default()

#### Test data
- Each thread is assigned a disjoint range of u64 integers, and each u64 integer is converted into a String.
- The performance test code asserts the expected outcome of each operation, and the post state of the hash index instance.

#### Test workload
- Each test is run twice in a single process in order to minimize the effect of page faults as the overhead is unpredictable.
- Insert: each thread inserts 16M records.
- Read: each thread reads 16M records.
- Scan: each thread scans the entire hash index once.
- Remove: each thread removes 16M records.
- The read/scan/remove data is populated by the insert test.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Insert |  89.015914443s | 116.402345094s | 143.86420979s  | 223.296876115s |
| Read   |  18.975302649s |  19.858082662s |  20.862552983s |  22.646245396s |
| Scan   |   3.640621149s |   7.327157641s |  15.847438364s |  31.771622377s |
| Remove |  69.259331734s |  82.053630018s |  98.725056905s | 109.829727509s |

## scc::TreeIndex <a name="tree index"></a>

scc::TreeIndex is an order-8 B+ tree variant optimized for read operations. Locks are only acquired on structural changes, and read/scan operations are neither blocked nor interrupted by other threads. The semantics of the read operation on a single key is similar to snapshot isolation in terms of database management software, as readers may not see the latest snapshot of data. The strategy to make the data structure lock-free is similar to that of scc::HashIndex; it harnesses immutability. All the key-value pairs stored in a leaf are never dropped until the leaf becomes completely unreachable, thereby ensuring immutability of all the reachable key-value pairs.

### Performance

#### Test setup
- OS: SUSE Linux Enterprise Server 15 SP1
- CPU: Intel(R) Xeon(R) CPU E7-8880 v4 @ 2.20GHz x 4
- RAM: 1TB
- Rust: 1.51.0
- SCC: 0.4.12
- TreeIndex<String, String> = Default::default()

#### Test data

- Each thread is assigned a disjoint range of u64 integers, and each u64 integer is converted into a String.
- The performance test code asserts the expected outcome of each operation, and the post state of the tree index instance.

#### Test workload
- Each test is run twice in a single process in order to minimize the effect of page faults as the overhead is unpredictable.
- Insert: each thread inserts 16M records.
- Read: each thread reads 16M records.
- Scan: each thread scans the entire tree index once.
- Remove: each thread removes 16M records.
- The read/scan/remove data is populated by the insert test.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Insert |  71.105692153s |  71.60182204s  |  69.300164192s |  70.050318852s |
| Read   |  25.959253558s |  28.21858048s  |  31.636098149s |  32.678456754s |
| Scan   |  36.059027123s |  68.349526938s | 129.710438186s | 218.026359862s |
| Remove |  82.910451376s | 120.185809134s | 162.444301189s | 241.25877992s  |

## Changelog

#### 0.4.12
Optimize TreeIndex: #24
#### 0.4.11
Optimize memory-usage of TreeIndex: #24 (partial)
#### 0.4.10
Fix HashIndex::remove: #32, API change HashIndex::len

## Milestones <a name="milestones"></a>

[Milestones](https://github.com/wvwwvwwv/scalable-concurrent-containers/milestones)
