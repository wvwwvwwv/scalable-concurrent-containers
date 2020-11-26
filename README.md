# SCC: Scalable Concurrent Containers

[Work-in-progress](https://github.com/wvwwvwwv/scc/milestones)

SCC offers scalable concurrent containers written in the Rust language. The data structures in SCC assume to be used by a database management software running on a server, ane therefore they may not efficiently work with a small set of data.

## scc::HashMap

scc::HashMap is a scalable in-memory unique key-value store that is targeted at highly concurrent heavy workloads. It does not distribute data to multiple shards as most concurrent hash maps do, instead only does it have a single array of entries and corresponding metadata cell array. The metadata management strategy is similar to that of Swisstable; a metadata cell which is separated from the key-value array, is a 64-byte data structure for managing consecutive sixteen entries in the key-value array. The metadata cell also has a linked list of entry arrays for hash collision resolution. scc::HashMap automatically enlarges and shrinks the capacity of its internal array, and resizing happens without blocking other operations and threads. In order to keep the predictable latency of each operation, it does not rehash every entry in the container at once when resizing, instead it distributes the resizing workload to future access to the data structure.

### Changelog

#### 0.2.11

- Remove libc dependencies
- Adjust memory alignment

#### 0.2.10

- Fix memory leak

#### 0.2.8

- Make scc::HashMap stack-unwinding-safe, meaning that it does not leave resources (memory, locks) unreleased after stack-unwinding on one condition; moving instances of K, and V types must always be successful (in C++ terms, K and V satisfy std::is_nothrow_move_constructible).
- Refine resizing strategies

#### 0.2.7

- Remove unnecessary heap allocation during read

### Performance

#### Test setup.
- OS: SUSE Linux Enterprise Server 15 SP1
- CPU: Intel(R) Xeon(R) CPU E7-8880 v4 @ 2.20GHz x 4
- RAM: 1TB
- Rust compiler version: 1.48.0
- SCC version: 0.2.12
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
| Insert | 153.725430095s | 181.501483645s | 258.285511858s | 462.73899472s  |
| Read   | 79.495212025s  | 90.859734163s  | 106.374841654s | 137.359072343s |
| Remove | 88.457419533s  | 103.189953895s | 118.811285809s | 142.061171296s |

#### Test workload: local-remote.
- Insert/Remove: each thread additionally tries to perform assigned operations using keys belonging to other threads.
- Mixed: each thread performs 128M insert-local -> insert-remote -> read-local -> read-remote -> remove-local -> remove-remote sequences.
- The data for Mixed/Remove tests is populated by the Insert test.
- The target remote thread is randomly chosen.
- The total operation count per Insert/Read thread is 256M, and half of the operations are bound to fail.
- The total operation count per Mixed thread is 768M, and about half of the operations are bound to fail.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Insert | 266.724033515s | 309.213404015s | 424.906462015s | 773.520934754s |
| Mixed  | 327.811948114s | 351.511457701s | 378.930084569s | 436.735096193s |
| Remove | 167.174273401s | 186.59768589s  | 209.027055204s | 254.330255812s |

## Milestones

[Milestones](https://github.com/wvwwvwwv/scc/milestones)