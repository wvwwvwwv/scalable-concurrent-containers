# Scalable Concurrent Containers

[![Cargo](https://img.shields.io/crates/v/scc?style=flat-square)](https://crates.io/crates/scc)
![Crates.io](https://img.shields.io/crates/l/scc?style=flat-square)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/wvwwvwwv/scalable-concurrent-containers/SCC?style=flat-square)

A collection of high performance concurrent containers and utilities for asynchronous and concurrent programming.

#### Concurrent Containers
- [HashMap](#HashMap) is a concurrent hash map.
- [HashSet](#HashSet) is a variant of [HashMap](#HashMap).
- [HashIndex](#HashIndex) is a read-optimized concurrent hash index.
- [TreeIndex](#TreeIndex) is a read-optimized concurrent B+ tree.
- [Queue](#Queue) is a concurrent lock-free first-in-first-out queue.

#### Utilities for Concurrent Programming
- [EBR](#EBR) implements epoch-based reclamation.
- [LinkedList](#LinkedList) is a type trait implementing a lock-free concurrent singly linked list.

_See [Performance](#Performance) for benchmark results for the containers and comparison with other concurrent hash maps_.


## HashMap

[HashMap](#HashMap) is a scalable in-memory unique key-value container that is targeted at highly concurrent write-heavy workloads. It uses [EBR](#EBR) for its hash table memory management in order to implement non-blocking resizing and fine-granular locking; *it is not a lock-free data structure, and each access to a single key is serialized by a bucket-level mutex*. [HashMap](#HashMap) is optimized for frequently updated large data sets, such as the lock table in database management software.

### Examples

A unique key can be inserted along with its corresponding value, and then it can be updated, read, and removed.

```rust
use scc::HashMap;

let hashmap: HashMap<u64, u32> = HashMap::default();

assert!(hashmap.insert(1, 0).is_ok());
assert_eq!(hashmap.update(&1, |v| { *v = 2; *v }).unwrap(), 2);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
assert_eq!(hashmap.remove(&1).unwrap(), (1, 2));
```

It supports `upsert` as in database management software; it tries to insert the given key-value pair, and if the key exists, it updates the value field with the supplied closure.

```rust
use scc::HashMap;

let hashmap: HashMap<u64, u32> = HashMap::default();

hashmap.upsert(1, || 2, |_, v| *v = 2);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
hashmap.upsert(1, || 2, |_, v| *v = 3);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 3);
```

There is no method to confine the lifetime of references derived from an [Iterator](https://doc.rust-lang.org/std/iter/trait.Iterator.html) to the [Iterator](https://doc.rust-lang.org/std/iter/trait.Iterator.html), and it is illegal to let them live as long as the [HashMap](#HashMap). Therefore [Iterator](https://doc.rust-lang.org/std/iter/trait.Iterator.html) is not implemented, instead, it provides a number of methods as substitutes for [Iterator](https://doc.rust-lang.org/std/iter/trait.Iterator.html): `for_each`, `for_each_async`, `scan`, `scan_async`, `retain`, and `retain_async`.

```rust
use scc::HashMap;

let hashmap: HashMap<u64, u32> = HashMap::default();

assert!(hashmap.insert(1, 0).is_ok());
assert!(hashmap.insert(2, 1).is_ok());

// Inside `for_each`, an `ebr::Barrier` protects the entry array.
let mut acc = 0;
hashmap.for_each(|k, v_mut| { acc += *k; *v_mut = 2; });
assert_eq!(acc, 3);

// `for_each` can modify the entries.
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
assert_eq!(hashmap.read(&2, |_, v| *v).unwrap(), 2);

assert!(hashmap.insert(3, 2).is_ok());

// Inside `retain`, an `ebr::Barrier` protects the entry array.
assert_eq!(hashmap.retain(|key, value| *key == 1 && *value == 0), (1, 2));
```

Asynchronous methods can be used in asynchronous code blocks.

```rust
use scc::HashMap;

let hashmap: HashMap<u64, u32> = HashMap::default();

assert!(hashmap.insert(1, 0).is_ok());

let future_insert = hashmap.insert_async(11, 17);
let result = future_insert.await;
```


## HashSet

[HashSet](#HashSet) is identical to [HashMap](#HashMap) except that the value type is always `()`.

### Examples

All the [HashSet](#HashSet) methods do not receive a value argument.

```rust
use scc::HashSet;

let hashset: HashSet<u64> = HashSet::default();

assert!(hashset.read(&1, |_| true).is_none());
assert!(hashset.insert(1).is_ok());
assert!(hashset.read(&1, |_| true).unwrap());
```


## HashIndex

[HashIndex](#HashIndex) is a read-optimized version of [HashMap](#HashMap). It applies [EBR](#EBR) to its entry management as well, enabling it to perform read operations without acquiring locks.

### Examples

Its `read` method is completely lock-free and does not modify any shared data.

```rust
use scc::HashIndex;

let hashindex: HashIndex<u64, u32> = HashIndex::default();

assert!(hashindex.insert(1, 0).is_ok());
assert_eq!(hashindex.read(&1, |_, v| *v).unwrap(), 0);
```

An [Iterator](https://doc.rust-lang.org/std/iter/trait.Iterator.html) is implemented for [HashIndex](#HashIndex), because any derived references can survive as long as the associated `ebr::Barrier` lives.

```rust
use scc::ebr::Barrier;
use scc::HashIndex;

let hashindex: HashIndex<u64, u32> = HashIndex::default();

assert!(hashindex.insert(1, 0).is_ok());

let barrier = Barrier::new();

// An `ebr::Barrier` has to be supplied to `iter`.
let mut iter = hashindex.iter(&barrier);

// The derived reference can live as long as `barrier`.
let entry_ref = iter.next().unwrap();
assert_eq!(iter.next(), None);

drop(hashindex);

// The entry can be read after `hashindex` is dropped.
assert_eq!(entry_ref, (&1, &0));
```


## TreeIndex

[TreeIndex](#TreeIndex) is a B+ tree variant optimized for read operations. The `ebr` module enables it to implement lock-free read and scan methods.

### Examples

Key-value pairs can be inserted, read, and removed, and the `read` method is lock-free.

```rust
use scc::TreeIndex;

let treeindex: TreeIndex<u64, u32> = TreeIndex::new();

assert!(treeindex.insert(1, 10).is_ok());
assert_eq!(treeindex.read(&1, |_, value| *value).unwrap(), 10);
assert!(treeindex.remove(&1));
```

Key-value pairs can be scanned and the `scan` method is lock-free.

```rust
use scc::ebr::Barrier;
use scc::TreeIndex;

let treeindex: TreeIndex<u64, u32> = TreeIndex::new();

assert!(treeindex.insert(1, 10).is_ok());
assert!(treeindex.insert(2, 11).is_ok());
assert!(treeindex.insert(3, 13).is_ok());

let barrier = Barrier::new();

let mut visitor = treeindex.iter(&barrier);
assert_eq!(visitor.next().unwrap(), (&1, &10));
assert_eq!(visitor.next().unwrap(), (&2, &11));
assert_eq!(visitor.next().unwrap(), (&3, &13));
assert!(visitor.next().is_none());
```

Key-value pairs in a specific range can be scanned.

```rust
use scc::ebr::Barrier;
use scc::TreeIndex;

let treeindex: TreeIndex<u64, u32> = TreeIndex::new();

for i in 0..10 {
    assert!(treeindex.insert(i, 10).is_ok());
}

let barrier = Barrier::new();

assert_eq!(treeindex.range(1..1, &barrier).count(), 0);
assert_eq!(treeindex.range(4..8, &barrier).count(), 4);
assert_eq!(treeindex.range(4..=8, &barrier).count(), 5);
```

Asynchronous methods can be used in asynchronous code blocks; *asynchronous methods yield the task executor when the target mutex cannot be acquired ([#49](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/49))*.

```rust
use scc::TreeIndex;

let treeindex: TreeIndex<u64, u32> = TreeIndex::default();

assert!(treeindex.insert(1, 0).is_ok());

let future_insert = treeindex.insert_async(11, 17);
let result = future_insert.await;
```


## Queue

[Queue](#Queue) is a concurrent lock-free first-in-first-out queue.

### Examples

```rust
use scc::Queue;

let queue: Queue<usize> = Queue::default();

queue.push(1);
assert!(queue.push_if(2, |e| e.map_or(false, |x| *x == 1)).is_ok());
assert!(queue.push_if(3, |e| e.map_or(false, |x| *x == 1)).is_err());
assert_eq!(queue.pop().map(|e| **e), Some(1));
assert_eq!(queue.pop().map(|e| **e), Some(2));
assert!(queue.pop().is_none());
```


## EBR

The `ebr` module implements epoch-based reclamation and various types of auxiliary data structures to make use of it. Its epoch-based reclamation algorithm is similar to that implemented in [crossbeam_epoch](https://docs.rs/crossbeam-epoch/), however users may find it easier to use as the lifetime of an instance is safely managed. For instance, `ebr::AtomicArc` and `ebr::Arc` hold a strong reference to the underlying instance, and the instance is automatically passed to the garbage collector when the reference count drops to zero.

### Examples

The `ebr` module can be used without an `unsafe` block.

```rust
use scc::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};

use std::sync::atomic::Ordering::Relaxed;

// `atomic_arc` holds a strong reference to `17`.
let atomic_arc: AtomicArc<usize> = AtomicArc::new(17);

// `barrier` prevents the garbage collector from dropping reachable instances.
let barrier: Barrier = Barrier::new();

// `ptr` cannot outlive `barrier`.
let mut ptr: Ptr<usize> = atomic_arc.load(Relaxed, &barrier);
assert_eq!(*ptr.as_ref().unwrap(), 17);

// `atomic_arc` can be tagged.
atomic_arc.update_tag_if(Tag::First, |t| t == Tag::None, Relaxed);

// `ptr` is not tagged, so CAS fails.
assert!(atomic_arc.compare_exchange(
    ptr,
    (Some(Arc::new(18)), Tag::First),
    Relaxed,
    Relaxed,
    &barrier).is_err());

// `ptr` can be tagged.
ptr.set_tag(Tag::First);

// The return value of CAS is a handle to the instance that `atomic_arc` previously owned.
let prev: Arc<usize> = atomic_arc.compare_exchange(
    ptr,
    (Some(Arc::new(18)), Tag::Second),
    Relaxed,
    Relaxed,
    &barrier).unwrap().0.unwrap();
assert_eq!(*prev, 17);

// `17` will be garbage-collected later.
drop(prev);

// `ebr::AtomicArc` can be converted into `ebr::Arc`.
let arc: Arc<usize> = atomic_arc.try_into_arc(Relaxed).unwrap();
assert_eq!(*arc, 18);

// `18` will be garbage-collected later.
drop(arc);

// `17` is still valid as `barrier` keeps the garbage collector from dropping it.
assert_eq!(*ptr.as_ref().unwrap(), 17);
```


## LinkedList

[LinkedList](#LinkedList) is a type trait that implements lock-free concurrent singly linked list operations, backed by [EBR](#EBR). It additionally provides support for marking an entry of a linked list to denote a user-defined state.

### Examples

```rust
use scc::ebr::{Arc, AtomicArc, Barrier};
use scc::LinkedList;

use std::sync::atomic::Ordering::Relaxed;

#[derive(Default)]
struct L(AtomicArc<L>, usize);
impl LinkedList for L {
    fn link_ref(&self) -> &AtomicArc<L> {
        &self.0
    }
}

let barrier = Barrier::new();

let head: L = L::default();
let tail: Arc<L> = Arc::new(L(AtomicArc::null(), 1));

// A new entry is pushed.
assert!(head.push_back(tail.clone(), false, Relaxed, &barrier).is_ok());
assert!(!head.is_marked(Relaxed));

// Users can mark a flag on an entry.
head.mark(Relaxed);
assert!(head.is_marked(Relaxed));

// `next_ptr` traverses the linked list.
let next_ptr = head.next_ptr(Relaxed, &barrier);
assert_eq!(next_ptr.as_ref().unwrap().1, 1);

// Once `tail` is deleted, it becomes invisible.
tail.delete_self(Relaxed);
assert!(head.next_ptr(Relaxed, &barrier).is_null());
```


## Performance

**Interpret the results cautiously as benchmarks do not represent real world workloads**.

### Setup

- OS: SUSE Linux Enterprise Server 15 SP2
- CPU: Intel(R) Xeon(R) CPU E7-8880 v4 @ 2.20GHz x 4
- RAM: 1TB
- Rust: 1.60.0
- SCC: 0.6.9

### Workload

- A disjoint **range** of 16M `usize` integers is assigned to each thread.
- Insert: each thread inserts its own records.
- Read: each thread reads its own records in the container.
- Scan: each thread scans the entire container once.
- Remove: each thread removes its own records from the container.
- InsertR, RemoveR: each thread additionally operates using keys belonging to a randomly chosen remote thread.
- MixedR: each thread performs `InsertR` -> `ReadR` -> `RemoveR`.

### Results

- [HashMap](#HashMap)

|         |  1 thread  |  4 threads | 16 threads | 64 threads |
|---------|------------|------------|------------|------------|
| InsertL |   9.411s   |  16.041s   |  43.012s   |  46.540s   |
| ReadL   |   3.934s   |   4.955s   |   6.548s   |   8.612s   |
| ScanL   |   0.147s   |   0.801s   |   3.021s   |  13.186s   |
| RemoveL |   4.654s   |   6.315s   |  10.651s   |  23.05s    |
| InsertR |  11.116s   |  27.104s   |  54.909s   |  58.564s   |
| MixedR  |  14.976s   |  29.388s   |  30.518s   |  33.081s   |
| RemoveR |   7.057s   |  12.565s   |  18.873s   |  26.77s    |

- [HashIndex](#HashIndex)

|         |  1 thread  |  4 threads | 16 threads | 64 threads |
|---------|------------|------------|------------|------------|
| InsertL |   9.73s    |  17.11s    |  44.599s   |  52.276s   |
| ReadL   |   3.59s    |   4.977s   |   6.108s   |   8.3s     |
| ScanL   |   0.279s   |   1.279s   |   5.079s   |  20.317s   |
| RemoveL |   4.755s   |   7.406s   |  12.329s   |  33.509s   |
| InsertR |  11.416s   |  26.998s   |  54.513s   |  65.274s   |
| MixedR  |  18.224s   |  35.357s   |  39.05s    |  42.37s    |
| RemoveR |   8.553s   |  13.314s   |  19.362s   |  38.209s   |

- [TreeIndex](#TreeIndex)

|         |  1 thread  |  4 threads | 16 threads | 64 threads |
|---------|------------|------------|------------|------------|
| InsertL |  14.839s   |  16.196s   |  18.644s   |  43.914s   |
| ReadL   |   3.584s   |   4.168s   |   4.531s   |   5.208s   |
| ScanL   |   1.239s   |   5.088s   |  20.778s   |  85.319s   |
| RemoveL |   5.736s   |   8.327s   |  10.5s     |  10.465s   |
| InsertR |  20.543s   |  77.743s   |  56.254s   |  65.242s   |
| MixedR  |  27.587s   | 164.433s   | 429.19s    | 453.633s   |
| RemoveR |   9.38s    |  20.262s   |  30.455s   |  39.091s   |

### [HashMap](#HashMap) Performance Comparison with [DashMap](https://github.com/xacrimon/dashmap) and [flurry](https://github.com/jonhoo/flurry)

- [Results on Apple M1 (8 cores)](https://github.com/wvwwvwwv/conc-map-bench).
- [Results on Intel Xeon (88 cores)](https://github.com/wvwwvwwv/conc-map-bench/tree/Intel).
- *Interpret the results cautiously as benchmarks do not represent real world workloads.*
- [HashMap](#HashMap) outperforms the others *[according to the benchmark test](https://github.com/xacrimon/conc-map-bench)* under highly concurrent or write-heavy workloads.
- The benchmark test is forked from [conc-map-bench](https://github.com/xacrimon/conc-map-bench).


## Changelog

0.6.11

* Fix [#49](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/49) for [HashMap](#HashMap) and [HashIndex](#HashIndex).
* Fix incorrect methods: `HashIndex::{remove_async, remove_if_async}`.

0.6.10

* New data structure: [Queue](#Queue).
* Remove incorrect methods: `{HashMap, HashSet}::read_with`.

0.6.9

* Improve [TreeIndex](#TreeIndex) performance.
* Fix [#66](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/66).
* Fix [#71](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/71).
