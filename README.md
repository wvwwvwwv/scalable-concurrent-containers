# SCC: Scalable Concurrent Containers

[Work-in-progress](https://github.com/wvwwvwwv/scc/milestones)

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
- The performance test code asserts the expected outcome of each operation, and the post state of the hashtable instance.

#### Test workload: local.
- Insert: each thread inserts 128M records.
- Read: each thread reads 128M records.
- Remove: each thread removes 128M records.
- The data for Read/Remove tests is populated by the Insert test.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Insert | 182.425985671s | 205.141640515s | 274.732659832s | 449.748250864s |
| Read   | 84.803851576s  | 84.604536547s  | 98.492147135s  | 120.014568598s |
| Remove | 91.934335899s  | 114.706894435s | 128.019035168s | 175.823523048s |

#### Test workload: local-remote.
- Insert/Remove: each thread additionally tries to perform assigned operations using keys belonging to other threads.
- Mixed: each thread performs 128M insert-local -> insert-remote -> read-local -> read-remote -> remove-local -> remove-remote.
- The data for Mixed/Remove tests is populated by the Insert test.
- The target remote thread is randomly chosen.
- The total operation count per Insert/Read thread is 256M, and half of the operations are bound to fail.
- The total operation count per Mixed thread is 768M, and about half of the operations are bound to fail.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Insert | 270.442900433s | 347.144686171s | 438.534754158s | 684.925329641s |
| Mixed  | 337.862498549s | 359.250368494s | 383.997076178s | 433.138958047s |
| Remove | 182.425985671s | 204.476775721s | 227.617657582s | 281.109044156s |

## Milestones

[Milestones](https://github.com/wvwwvwwv/scc/milestones)