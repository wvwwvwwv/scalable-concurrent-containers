# SCC: Scalable Concurrent Containers

SCC offers scalable concurrent containers written in the Rust language. The data structures in SCC assume to be used by a database management software running on a server, ane therefore they may not efficiently work with a small set of data.

## scc::HashMap

scc::HashMap is a scalable in-memory unique key-value store that is targeted at highly concurrent heavy workloads. It does not distribute data to multiple shards as most concurrent hash maps do, instead only does it have a single array of entries and corresponding metadata cell array. The metadata management strategy is similar to that of Swisstable; a metadata cell which is separated from the key-value array, is a 64-byte data structure for managing consecutive sixteen entries in the key-value array. The metadata cell also has a linked list of entry arrays for hash collision resolution. scc::HashMap automatically enlarges and shrinks the capacity of its internal array, and resizing happens without blocking other operations and threads. In order to keep the predictable latency of each operation, it does not rehash every entry in the container at once when resizing, instead it distributes the resizing workload to future access to the data structure.

* It is clear that experimental hardware-transactional-memory functions (such as https://stdrs.dev/nightly/x86_64-pc-windows-gnu/core/core_arch/x86/rtm/index.html) make read operations entirely memory-write-free, boosting performance without compromising semantics and memory consumption. scc::HashMap will start use TSX/HTM intrinsics as soon as the functions become stabilized.

### Performance

#### Test setup.
- OS: SUSE Linux Enterprise Server 15 SP1
- CPU: Intel(R) Xeon(R) CPU E7-8880 v4 @ 2.20GHz x 4
- RAM: 1TB
- Rust compiler version: 1.48.0
- SCC version: 0.3.0
- The hashmap is generated using the default parameters: the RandomState hasher builder, and 256 preallocated entries.
- In order to minimize the cost of page fault handling, all the tests were run twice, and only the best results were taken.

#### Test data.
- Each thread is assigned a disjoint range of u64 integers.
- The entropy of the test input is very low, however it does not undermine the test result as the key distribution method is agnostic to the input pattern.
- The performance test code asserts the expected outcome of each operation, and the post state of the hashmap instance.

#### Test workload: local.
- Insert: each thread inserts 128M records.
- Read: each thread reads 128M records.
- Remove: each thread removes 128M records.
- The data for Read/Remove tests is populated by the Insert test.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Insert | 156.423361787s | 187.157442477s | 264.075874751s | 463.032489985s |
| Read   | 81.03393205s   | 92.933046817s  | 109.303575217s | 137.802145824s |
| Remove | 85.563265194s  | 102.896206291s | 117.072458551s | 167.450069665s |

#### Test workload: local-remote.
- Insert/Remove: each thread additionally tries to perform assigned operations using keys belonging to other threads.
- Mixed: each thread performs 128M insert-local -> insert-remote -> read-local -> read-remote -> remove-local -> remove-remote sequences.
- The data for Mixed/Remove tests is populated by the Insert test.
- The target remote thread is randomly chosen.
- The total operation count per Insert/Read thread is 256M, and half of the operations are bound to fail.
- The total operation count per Mixed thread is 768M, and about half of the operations are bound to fail.

|        | 11 threads     | 22 threads     | 44 threads     | 88 threads     |
|--------|----------------|----------------|----------------|----------------|
| Insert | 272.420310927s | 314.424537182s | 432.493505328s | 772.267595819s |
| Mixed  | 326.767954659s | 350.603202721s | 375.987412301s | 433.899012681s |
| Remove | 164.857461617s | 184.528933216s | 199.187884668s | 250.735616868s |


## Changelog

#### 0.3.0
APIs stabilized
#### 0.2.13
Add 'contains' and 'hasher' APIs
#### 0.2.12
Update crossbeam_epoch to 0.9.1
#### 0.2.11
Remove libc dependencies

Adjust memory alignment
#### 0.2.10
Fix memory leak
#### 0.2.8
Make scc::HashMap stack-unwinding-safe, meaning that it does not leave resources (memory, locks) unreleased after stack-unwinding on one condition; moving instances of K, and V types must always be successful (in C++ terms, K and V satisfy std::is_nothrow_move_constructible).

Refine resizing strategies
#### 0.2.7
Remove unnecessary heap allocation during read

## Milestones

[Milestones](https://github.com/wvwwvwwv/scc/milestones)