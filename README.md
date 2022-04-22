# Scalable Concurrent Containers

[![Cargo](https://img.shields.io/crates/v/scc?style=flat-square)](https://crates.io/crates/scc)
![Crates.io](https://img.shields.io/crates/l/scc?style=flat-square)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/wvwwvwwv/scalable-concurrent-containers/SCC?style=flat-square)

A collection of concurrent data structures and building blocks for concurrent programming.

- [ebr](#EBR) implements epoch-based reclamation.
- [LinkedList](#LinkedList) is a type trait implementing a wait-free concurrent singly linked list.
- [HashMap](#HashMap) is a concurrent hash map.
- [HashSet](#HashSet) is a concurrent hash set based on [scc::HashMap](#HashMap).
- [HashIndex](#HashIndex) is a concurrent hash index allowing lock-free read and scan.
- [TreeIndex](#TreeIndex) is a concurrent B+ tree allowing lock-free read and scan.


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
    Relaxed).is_err());

// `ptr` can be tagged.
ptr.set_tag(Tag::First);

// The return value of CAS is a handle to the instance that `atomic_arc` previously owned.
let prev: Arc<usize> = atomic_arc.compare_exchange(
    ptr,
    (Some(Arc::new(18)), Tag::Second),
    Relaxed,
    Relaxed).unwrap().0.unwrap();
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

[LinkedList](#LinkedList) is a type trait that implements wait-free concurrent singly linked list operations, backed by [EBR](#EBR). It additionally provides support for marking an entry of a linked list to indicate that the entry is in a user-defined state.

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

## HashMap

[HashMap](#HashMap) is a scalable in-memory unique key-value container that is targeted at highly concurrent heavy workloads. It applies [EBR](#EBR) to its entry array management, thus enabling it to avoid container-level locking and data sharding.

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

It supports `upsert` as in database management software; it tries to insert the given key-value pair, and if it fails, it updates the value field with the supplied closure.

```rust
use scc::HashMap;

let hashmap: HashMap<u64, u32> = HashMap::default();

hashmap.upsert(1, || 2, |_, v| *v = 2);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
hashmap.upsert(1, || 2, |_, v| *v = 3);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 3);
```

There is no method to confine the lifetime of references derived from an [Iterator](https://doc.rust-lang.org/std/iter/trait.Iterator.html) to the [Iterator](https://doc.rust-lang.org/std/iter/trait.Iterator.html), and it is illegal to let them live as long as the [HashMap](#HashMap). Therefore [Iterator](https://doc.rust-lang.org/std/iter/trait.Iterator.html) is not implemented, instead, it provides two methods that allow a [HashMap](#HashMap) to iterate over its entries: `for_each`, and `retain`.

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
let future_insert = hashmap.insert_async(11, 17);
let result = future_insert.await;
```


## HashSet

[HashSet](#HashSet) is a variant of [HashMap](#HashMap) where the value type is `()`.

### Examples

All the [HashSet](#HashSet) methods do not receive a value argument.

```rust
use scc::HashSet;

let hashset: HashSet<u64> = HashSet::default();

assert!(hashset.read(&1, |_| true).is_none());
assert!(hashset.insert(1).is_ok());
assert!(hashset.read(&1, |_| true).unwrap());
```

The capacity of a [HashSet](#HashSet) can be specified.

```rust
use scc::HashSet;
use std::collections::hash_map::RandomState;

let hashset: HashSet<u64, RandomState> = HashSet::new(1000000, RandomState::new());
assert_eq!(hashset.capacity(), 1048576);
```


## HashIndex

[HashIndex](#HashIndex) is a read-optimized version of [HashMap](#HashMap). It applies [EBR](#EBR) to its entry management as well, enabling it to perform read operations without acquiring locks.

### Examples

Its `read` method does not modify any shared data.

```rust
use scc::HashIndex;

let hashindex: HashIndex<u64, u32> = HashIndex::default();

assert!(hashindex.insert(1, 0).is_ok());
assert_eq!(hashindex.read(&1, |_, v| *v).unwrap(), 0);
```

An [Iterator](https://doc.rust-lang.org/std/iter/trait.Iterator.html) is implemented for [HashIndex](#HashIndex), because derived references can survive as long as the associated `ebr::Barrier` lives.

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

Key-value pairs can be inserted, read, and removed.

```rust
use scc::TreeIndex;

let treeindex: TreeIndex<u64, u32> = TreeIndex::new();

assert!(treeindex.insert(1, 10).is_ok());
assert_eq!(treeindex.read(&1, |_, value| *value).unwrap(), 10);
assert!(treeindex.remove(&1));
```

Key-value pairs can be scanned.

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

Asynchronous methods can be used in asynchronous code blocks.

```rust
use scc::TreeIndex;

let treeindex: TreeIndex<u64, u32> = TreeIndex::default();

let future_insert = treeindex.insert_async(11, 17);
let result = future_insert.await;
```


## Performance

### Comparison with [DashMap](https://github.com/xacrimon/dashmap) and [flurry](https://github.com/jonhoo/flurry)

- [Link](https://github.com/wvwwvwwv/conc-map-bench): based on [conc-map-bench](https://github.com/xacrimon/conc-map-bench).
- SCC [HashMap](#HashMap) is 0-25% slower than [DashMap](https://github.com/xacrimon/dashmap).
- SCC [HashMap](#HashMap) and [DashMap](https://github.com/xacrimon/dashmap) outperform [flurry](https://github.com/jonhoo/flurry).


### Setup

- OS: SUSE Linux Enterprise Server 15 SP2
- CPU: Intel(R) Xeon(R) CPU E7-8880 v4 @ 2.20GHz x 4
- RAM: 1TB
- Rust: 1.60.0
- SCC: 0.6.4 (HashMap, HashIndex), 0.6.5 (TreeIndex)

### Workload

- A disjoint range of 16M `usize` integers is assigned to each thread.
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
| InsertL |   9.816s   |  16.688s   |  42.589s   |  47.589s   |
| ReadL   |   4.079s   |   5.486s   |   6.862s   |   8.636s   |
| ScanL   |   0.15s    |   0.727s   |   3.001s   |  12.512s   |
| RemoveL |   4.627s   |   6.644s   |  10.987s   |  23.826s   |
| InsertR |  11.424s   |  29.784s   |  56.771s   |  60.953s   |
| MixedR  |  14.851s   |  30.712s   |  33.401s   |  34.612s   |
| RemoveR |   8.316s   |  14.438s   |  21.737s   |  27.819s   |

- [HashIndex](#HashIndex)

|         |  1 thread  |  4 threads | 16 threads | 64 threads |
|---------|------------|------------|------------|------------|
| InsertL |  10.287s   |  17.044s   |  43.286s   |  47.085s   |
| ReadL   |   3.679s   |   4.839s   |   6.565s   |   8.004s   |
| ScanL   |   0.204s   |   1.094s   |   4.194s   |  16.974s   |
| RemoveL |   4.747s   |   6.564s   |  11.333s   |  23.429s   |
| InsertR |  11.922s   |  30.295s   |  57.612s   |  61.138s   |
| MixedR  |  16.403s   |  38.545s   |  43.204s   |  45.267s   |
| RemoveR |   8.407s   |  14.786s   |  22.02s    |  28.118s   |

- [TreeIndex](#TreeIndex)

|         |  1 thread  |  4 threads | 16 threads | 64 threads |
|---------|------------|------------|------------|------------|
| InsertL |  16.833s   |  19.778s   |  26.651s   |  68.163s   |
| ReadL   |   3.783s   |   4.112s   |   4.696s   |   5.367s   |
| ScanL   |   1.26s    |   4.958s   |  19.781s   |  88.587s   |
| RemoveL |   5.986s   |   7.479s   |   8.825s   |   9.639s   |
| InsertR |  22.233s   |  60.892s   |  70.324s   | 105.639s   |
| MixedR  |  28.956s   | 186.537s   | 476.771s   | 672.449s   |
| RemoveR |   9.281s   |  18.568s   |  25.262s   |  83.017s   |


## Changelog

0.6.6

* Add `{HashMap, HashSet}::{scan, scan_async}` for a read-only scan.

0.6.5

* Add more asynchronous methods to [HashMap](#HashMap), [HashIndex](#HashIndex), and [HashSet](#HashSet).
* [TreeIndex](#TreeIndex) remove performance improvement.

0.6.4

* Consolidate synchronous and asynchronous [HashMap](#HashMap) implementations.
* [HashMap](#HashMap) performance improvement.

0.6.3

* Consolidate synchronous and asynchronous [TreeIndex](#TreeIndex) implementations.

0.6.2

* Asynchronous [TreeIndex](#TreeIndex).
* [TreeIndex](#TreeIndex) performance improvement.
* Fix ebr API: `ebr::Arc::get_mut` is now unsafe.
* Fix [#65](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/65).
* Fix [#66](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/66).
