# Scalable Concurrent Containers

[![Cargo](https://img.shields.io/crates/v/scc?style=flat-square)](https://crates.io/crates/scc)
![Crates.io](https://img.shields.io/crates/l/scc?style=flat-square)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/wvwwvwwv/scalable-concurrent-containers/SCC?style=flat-square)

A collection of high performance containers and utilities for concurrent and asynchronous programming.

#### Concurrent and Asynchronous Containers
- [HashMap](#HashMap) is a concurrent and asynchronous hash map.
- [HashSet](#HashSet) is a concurrent and asynchronous hash set.
- [HashIndex](#HashIndex) is a read-optimized concurrent and asynchronous hash map.
- [TreeIndex](#TreeIndex) is a read-optimized concurrent and asynchronous B+ tree.
- [Queue](#Queue) is a concurrent lock-free first-in-first-out queue.

#### Utilities for Concurrent Programming
- [EBR](#EBR) implements epoch-based reclamation.
- [LinkedList](#LinkedList) is a type trait implementing a lock-free concurrent singly linked list.

_See [Performance](#Performance) for benchmark results for the containers and comparison with other concurrent maps_.

## HashMap

[HashMap](#HashMap) is a scalable in-memory unique key-value container that is targeted at highly concurrent write-heavy workloads. It uses [EBR](#EBR) for its hash table memory management in order to implement non-blocking resizing and fine-granular locking without static data sharding; *it is not a lock-free data structure, and each access to a single key is serialized by a bucket-level mutex*. [HashMap](#HashMap) is optimized for frequently updated large data sets, such as the lock table in database management software.

### Examples

A unique key can be inserted along with its corresponding value, and then the inserted entry can be updated, read, and removed synchronously or asynchronously.

```rust
use scc::HashMap;

let hashmap: HashMap<u64, u32> = HashMap::default();

assert!(hashmap.insert(1, 0).is_ok());
assert_eq!(hashmap.update(&1, |v| { *v = 2; *v }).unwrap(), 2);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
assert_eq!(hashmap.remove(&1).unwrap(), (1, 2));

let future_insert = hashmap.insert_async(2, 1);
let future_remove = hashmap.remove_async(&1);
```

It supports `upsert` as in database management software; it tries to insert the given key-value pair, and if the key exists, it updates the value field with the supplied closure.

```rust
use scc::HashMap;

let hashmap: HashMap<u64, u32> = HashMap::default();

hashmap.upsert(1, || 2, |_, v| *v = 2);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
hashmap.upsert(1, || 2, |_, v| *v = 3);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 3);

let future_upsert = hashmap.upsert_async(2, || 1, |_, v| *v = 3);
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
assert_eq!(hashmap.retain(|k, v| *k == 1 && *v == 0), (1, 2));

// It is possible to scan the entries asynchronously.
let future_scan = hashmap.scan_async(|k, v| println!("{k} {v}"));
let future_for_each = hashmap.for_each_async(|k, v_mut| { *v_mut = *k; });
```

## HashSet

[HashSet](#HashSet) is a version of [HashMap](#HashMap) where the value type is `()`.

### Examples

All the [HashSet](#HashSet) methods do not receive a value argument.

```rust
use scc::HashSet;

let hashset: HashSet<u64> = HashSet::default();

assert!(hashset.read(&1, |_| true).is_none());
assert!(hashset.insert(1).is_ok());
assert!(hashset.read(&1, |_| true).unwrap());

let future_insert = hashset.insert_async(2);
let future_remove = hashset.remove_async(&1);
```

## HashIndex

[HashIndex](#HashIndex) is a read-optimized version of [HashMap](#HashMap). It applies [EBR](#EBR) to its entry management as well, enabling it to perform read operations without blocking or being blocked.

### Examples

Its `read` method is completely lock-free and does not modify any shared data.

```rust
use scc::HashIndex;

let hashindex: HashIndex<u64, u32> = HashIndex::default();

assert!(hashindex.insert(1, 0).is_ok());
assert_eq!(hashindex.read(&1, |_, v| *v).unwrap(), 0);

let future_insert = hashindex.insert_async(2, 1);
let future_remove = hashindex.remove_if(&1, |_| true);
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

assert!(treeindex.insert(1, 2).is_ok());
assert_eq!(treeindex.read(&1, |_, v| *v).unwrap(), 2);
assert!(treeindex.remove(&1));

let future_insert = treeindex.insert_async(2, 3);
let future_remove = treeindex.remove_if_async(&1, |v| *v == 2);
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
use scc::ebr::{suspend, Arc, AtomicArc, Barrier, Ptr, Tag};

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

// Execution of a closure can be deferred until all the current readers are gone.
barrier.defer_execute(|| println!("deferred"));
drop(barrier);

// If the thread is expected to lie dormant for a while, call `suspend()` to allow other threads
// to reclaim its own retired instances.
suspend();
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
- Rust: 1.63.0
- SCC: 0.10.0

### Workload

- A disjoint **range** of 16M `usize` integers is assigned to each thread.
- Insert: each thread inserts its own records.
- Scan: each thread scans the entire container once.
- Read: each thread reads its own records in the container.
- Remove: each thread removes its own records from the container.
- InsertR, RemoveR: each thread additionally operates using keys belonging to a randomly chosen remote thread.
- Mixed: each thread performs `InsertR` -> `ReadR` -> `RemoveR`.

### Results

- [HashMap](#HashMap)

|         |  1 thread  |  4 threads | 16 threads | 64 threads |
|---------|------------|------------|------------|------------|
| Insert  |   7.615s   |  10.873s   |  13.687s   |  23.582s   |
| Scan    |   0.146s   |   0.711s   |   3.35s    |  10.881s   |
| Read    |   3.973s   |   5.096s   |   6.234s   |   7.902s   |
| Remove  |   4.607s   |   6.192s   |   8.252s   |  16.634s   |
| InsertR |   9.015s   |  20.825s   |  23.612s   |  34.309s   |
| Mixed   |  12.406s   |  26.141s   |  25.821s   |  29.053s   |
| RemoveR |   6.231s   |  13.644s   |  15.317s   |  23.501s   |

- [HashIndex](#HashIndex)

|         |  1 thread  |  4 threads | 16 threads | 64 threads |
|---------|------------|------------|------------|------------|
| Insert  |   7.626s   |  11.088s   |  14.18s    |  26.74s    |
| Scan    |   0.151s   |   0.606s   |   2.444s   |  11.125s   |
| Read    |   3.505s   |   4.82s    |   5.819s   |   7.481s   |
| Remove  |   4.628s   |   6.78s    |   9.198s   |  21.57s    |
| InsertR |   9.099s   |  20.905s   |  24.129s   |  37.178s   |
| Mixed   |  13.087s   |  31.43s    |  34.054s   |  35.209s   |
| RemoveR |   6.207s   |  13.589s   |  16.268s   |  30.492s   |

- [TreeIndex](#TreeIndex)

|         |  1 thread  |  4 threads | 16 threads | 64 threads |
|---------|------------|------------|------------|------------|
| Insert  |  14.694s   |  16.427s   |  18.76s    |  45.409s   |
| Scan    |   1.234s   |   4.974s   |  20.689s   |  81.438s   |
| Read    |   3.733s   |   4.119s   |   4.898s   |   5.178s   |
| Remove  |   6.114s   |   8.453s   |  10.363s   |  10.497s   |
| InsertR |  20.341s   |  69.452s   |  82.766s   |  67.623s   |
| Mixed   |  28.026s   | 180.701s   | 422.104s   | 443.7s     |
| RemoveR |   9.75s    |  21.534s   |  31.032s   |  50.028s   |

### [HashMap](#HashMap) and [HashIndex](#HashIndex) Performance Comparison with [DashMap](https://github.com/xacrimon/dashmap) and [flurry](https://github.com/jonhoo/flurry)

- [Results on Apple M1 (8 cores)](https://github.com/wvwwvwwv/conc-map-bench).
- [Results on Intel Xeon (88 cores)](https://github.com/wvwwvwwv/conc-map-bench/tree/Intel).
- *Interpret the results cautiously as benchmarks do not represent real world workloads.*
- [HashMap](#HashMap) outperforms the others *[according to the benchmark test](https://github.com/xacrimon/conc-map-bench)* under highly concurrent workloads.
- The benchmark test is forked from [conc-map-bench](https://github.com/xacrimon/conc-map-bench).

## [Changelog](https://github.com/wvwwvwwv/scalable-concurrent-containers/blob/main/CHANGELOG.md)
