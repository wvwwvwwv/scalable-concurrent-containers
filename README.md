# SCC: Scalable Concurrent Containers

[Work-in-progress]
[HashMap:not-fully-optimized]
[HashMap:not-fully-unwinding-safe]
[HashMap:APIs-half-stabilized]
[BwTree:come-in-Feb-2021]

SCC offers scalable concurrent containers written in the Rust language. The data structures in SCC assume to be used by a database management software running on a server, ane therefore they may not efficiently work on small systems.

## scc::HashMap

scc::HashMap is a scalable in-memory unique key-value store that is targeted at highly concurrent heavy workloads. It does not distribute data to multiple shards as most concurrent hash maps do, instead only does it have a single array of entries and corresponding metadata cell array. The metadata management strategy is similar to that of Swisstable; a metadata cell which is separated from the key-value array, is a 64-byte data structure for managing consecutive sixteen entries in the key-value array. The metadata cell also has a linked list of entry arrays for hash collision resolution. scc::HashMap automatically enlarges and shrinks the capacity of its internal array automatically, and it happens without blocking other operations and threads. In order to keep the predictable latency of each operation, it does not rehash every entry in the container at once when resizing, instead it distributes the resizing workload to future access to the data structure.

### Performance

#### Test setup.
- OS: SUSE Linux Enterprise Server 15 SP1
- CPU: Intel(R) Xeon(R) CPU E7-8880 v4 @ 2.20GHz x 4
- RAM: 1TB
- Rust compiler version: 1.48.0
- SCC: 0.2.6
- The hashtable is generated using the default parameters: the RandomState hasher builder, and 256 preallocated entries.
- In order to minimize the cost of page fault handling, all the tests were run twice, and only the best results were taken.

#### Test data.
- Each thread is assigned a disjoint range of u64 integers.
- The entropy of the test input is very low, however it does not undermine the test result as the key distribution method is agnostic to the input pattern.
- The performance test code asserts the expected outcome of each operation, and the post-state of the hashtable instance.

** TEST RESULTS: TO BE UPDATED, SHORTLY

#### Test workload: local.
- Insert: each thread inserts 128M records.
- Read: each thread reads 128M  records.
- Remove: each thread removes 128M records.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Insert | 173.144712239s | 246.541850542s | 281.454809275s | 471.991919119s |
| Read   | 76.763939972s  | 110.250855322s | 123.870267714s | 143.606594002s |
| Remove | 93.043478471s  | 141.48738765s  | 169.476767746s | 280.781299976s |

#### Test workload: local-remote.
- Each Insert/Read/Remove thread additionally tries to perform the operation using keys belonging to other threads.
- The target remote thread is randomly chosen.
- The total operation count per thread is 256M, and half of the Insert/Remove operations are bound to fail.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Insert | 200.997180266s | 246.541850542s | 281.454809275s | 471.991919119s |
| Read   | 76.005210454s  | 110.250855322s | 123.870267714s | 143.606594002s |
| Remove | 97.686499857s  | 141.48738765s  | 169.476767746s | 280.781299976s |

#### Test workload: mixed.
- Each thread performs 1. Insert-local, 2. Insert-remote, 3. Read-local, 4. Read-remote, 5. Remove-local, and 6. Remove-remote.
- The total operation count per thread is 768M, and half of the Insert/Remove operations are bound to fail.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Mixed  | 248.86387233s  | 246.541850542s | 281.454809275s | 471.991919119s |
