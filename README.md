# SCC: Scalable Concurrent Containers

[Work-in-progress](https://github.com/wvwwvwwv/scc/milestones)

SCC offers scalable concurrent containers written in the Rust language. The data structures in SCC assume to be used by a database management software running on a server, ane therefore they may not efficiently work with a small set of data.

## Changelog

### 0.2.8

- Make scc::HashMap stack-unwinding-safe, meaning that it does not leave resources (memory, locks) unreleased after stack-unwinding on one condition; moving instances of K, and V types must always be successful (in C++ terms, K and V satisfy std::is_nothrow_move_constructible).
- Refine resizing strategies

### 0.2.7

- Remove unnecessary heap allocation during read

## scc::HashMap

scc::HashMap is a scalable in-memory unique key-value store that is targeted at highly concurrent heavy workloads. It does not distribute data to multiple shards as most concurrent hash maps do, instead only does it have a single array of entries and corresponding metadata cell array. The metadata management strategy is similar to that of Swisstable; a metadata cell which is separated from the key-value array, is a 64-byte data structure for managing consecutive sixteen entries in the key-value array. The metadata cell also has a linked list of entry arrays for hash collision resolution. scc::HashMap automatically enlarges and shrinks the capacity of its internal array, and resizing happens without blocking other operations and threads. In order to keep the predictable latency of each operation, it does not rehash every entry in the container at once when resizing, instead it distributes the resizing workload to future access to the data structure.

### Performance

#### Test setup.
- OS: SUSE Linux Enterprise Server 15 SP1
- CPU: Intel(R) Xeon(R) CPU E7-8880 v4 @ 2.20GHz x 4
- RAM: 1TB
- Rust compiler version: 1.48.0
- SCC version: 0.2.8
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
| Insert | 160.0296045s   | 193.795963356s | 277.626057713s | 481.870984015s |
| Read   | 80.402082433s  | 85.180396045s  | 89.37547222s   | 100.88658824s  |
| Remove | 89.642203773s  | 104.70431303s  | 127.851116779s | 170.375524741s |

#### Test workload: local-remote.
- Insert/Remove: each thread additionally tries to perform assigned operations using keys belonging to other threads.
- Mixed: each thread performs 128M insert-local -> insert-remote -> read-local -> read-remote -> remove-local -> remove-remote sequences.
- The data for Mixed/Remove tests is populated by the Insert test.
- The target remote thread is randomly chosen.
- The total operation count per Insert/Read thread is 256M, and half of the operations are bound to fail.
- The total operation count per Mixed thread is 768M, and about half of the operations are bound to fail.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads    |
|--------|----------------|----------------|----------------|----------------|
| Insert | 287.074169883s | 331.645274067s | 454.448331025s | 676.909568328s |
| Mixed  | 338.147534267s | 356.44845854s  | 377.938664239s | 427.24426793s  |
| Remove | 178.737543249s | 197.831223618s | 225.811047389s | 276.429387396s |

## Milestones

[Milestones](https://github.com/wvwwvwwv/scc/milestones)