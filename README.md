# Scalable Concurrent Containers

A collection of concurrent data structures and building blocks for concurrent programming.

- [scc::ebr](#EBR) implements epoch-based reclamation.
- [scc::HashMap](#HashMap) is a concurrent hash map.
- [scc::HashIndex](#HashIndex) is a concurrent hash index allowing lock-free read and scan.
- [scc::TreeIndex](#TreeIndex) is a concurrent B+ tree allowing lock-free read and scan.

## EBR

The `ebr` module implements epoch-based reclamation and various types of auxiliary data structures to make use of it. Its epoch-based reclamation algorithm is similar to that implemented in [crossbeam_epoch](https://docs.rs/crossbeam-epoch/), however users may find it easier to use as the lifetime of an instance is automatically managed. For instance, `ebr::AtomicArc` and `ebr::Arc` hold a strong reference to the underlying instance, and the instance is passed to the garbage collector when the reference count drops to zero.

### Examples

The `ebr` module can be used without relying on `unsafe` blocks.

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
atomic_arc.set_tag(Tag::First, Relaxed);

// `ptr` is not tagged, so CAS fails.
assert!(atomic_arc.compare_exchange(
    ptr,
    (Some(Arc::new(18)), Tag::First),
    Relaxed,
    Relaxed).is_err());

// `ptr` can be tagged.
ptr.set_tag(Tag::First);

// The result of CAS is a handle to the instance that `atomic_arc` previously owned.
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

## HashMap

`HashMap` is a scalable in-memory unique key-value store that is targeted at highly concurrent heavy workloads. It applies [`EBR`](#EBR) to its entry array management, thus allowing it to reduce the number of locks and avoid data sharding.

### Examples

A unique key can be inserted along with its corresponding value, then it can be updated, read, and removed.

```rust
use scc::HashMap;

let hashmap: HashMap<u64, u32> = Default::default();

assert!(hashmap.insert(1, 0).is_ok());
assert_eq!(hashmap.update(&1, |v| { *v = 2; *v }).unwrap(), 2);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
assert_eq!(hashmap.remove(&1).unwrap(), (1, 2));
```

It supports `upsert` as in database management software; it tries to insert the given key-value pair, and if it fails, it updates the value field of an existing entry corresponding to the key.

```rust
use scc::HashMap;

let hashmap: HashMap<u64, u32> = Default::default();

hashmap.upsert(1, || 2, |_, v| *v = 2);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
hashmap.upsert(1, || 2, |_, v| *v = 3);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 3);
```

There is no method to confine the lifetime of references derived from an [`Iterator`](https://doc.rust-lang.org/std/iter/trait.Iterator.html) to the [`Iterator`](https://doc.rust-lang.org/std/iter/trait.Iterator.html), and it is illegal for them to live as long as the [`HashMap`](#HashMap) due to the lack of global lock inside it. Therefore [`Iterator`](https://doc.rust-lang.org/std/iter/trait.Iterator.html) is not implemented, instead, it provides two methods that enable a [`HashMap`](#HashMap) to iterate over its entries: `for_each`, and `retain`.

```rust
use scc::HashMap;

let hashmap: HashMap<u64, u32> = Default::default();

assert!(hashmap.insert(1, 0).is_ok());
assert!(hashmap.insert(2, 1).is_ok());

// Inside `for_each`, a `ebr::Barrier` protects the entry array.
let mut acc = 0;
hashmap.for_each(|k, v_mut| { acc += *k; *v_mut = 2; });
assert_eq!(acc, 3);

// `for_each` can modify the entries.
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
assert_eq!(hashmap.read(&2, |_, v| *v).unwrap(), 2);

assert!(hashmap.insert(3, 2).is_ok());

// Inside `retain`, a `ebr::Barrier` protects the entry array.
assert_eq!(hashmap.retain(|key, value| *key == 1 && *value == 0), (1, 2));
```

## HashIndex

`HashIndex` is a read-optimized version of [`HashMap`](#HashMap). It applies [`EBR`](#EBR) to its entry management as well, enabling it to perform read operations without acquiring locks.

### Examples

Its `read` method neither acquire locks nor modify any shared data.

```rust
use scc::HashIndex;

let hashindex: HashIndex<u64, u32> = Default::default();

assert!(hashindex.insert(1, 0).is_ok());
assert_eq!(hashindex.read(&1, |_, v| *v).unwrap(), 0);
```

An `Iterator` is implemented for `HashIndex`, because entry references can survive as long as the supplied `ebr::Barrier` survives.

```rust
use scc::ebr::Barrier;
use scc::HashIndex;

let hashindex: HashIndex<u64, u32> = Default::default();

assert!(hashindex.insert(1, 0).is_ok());

let barrier = Barrier::new();

// An `ebr::Barrier` has to be given to `iter`.
let mut iter = hashindex.iter(&barrier);

// The derived reference can live as long as `barrier`.
let entry_ref = iter.next().unwrap();
assert_eq!(iter.next(), None);

drop(hashindex);

// The entry can be read after `hashindex` is dropped.
assert_eq!(entry_ref, (&1, &0));
```

## TreeIndex

`TreeIndex` is a B+ tree variant optimized for read operations. The `ebr` module enables it to implement lock-free read and scan methods.

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

let mut scanner = treeindex.iter(&barrier);
assert_eq!(scanner.next().unwrap(), (&1, &10));
assert_eq!(scanner.next().unwrap(), (&2, &11));
assert_eq!(scanner.next().unwrap(), (&3, &13));
assert!(scanner.next().is_none());
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

## Performance

### Test setup
- OS: SUSE Linux Enterprise Server 15 SP1
- CPU: Intel(R) Xeon(R) CPU E7-8880 v4 @ 2.20GHz x 4
- RAM: 1TB
- Rust: 1.54.0
- SCC: 0.5.0
- HashMap<usize, usize, RandomState> = Default::default()
- HashIndex<usize, usize, RandomState> = Default::default()
- TreeIndex<usize, usize> = Default::default()

### Test data

- Each thread is assigned a disjoint range of `usize` integers.
- The performance test code asserts the expected outcome of each operation, and the post state of the container.
- The number of records in the test data dedicated to a thread for [`HashMap`](#HashMap) tests is 128M, and 4M for [`HashIndex`](#HashIndex) and [`TreeIndex`](#TreeIndex) tests.

### Test workload: local

- Each test is run twice in a single process in order to minimize the effect of page faults as the overhead is unpredictable.
- Insert: each thread inserts its own records.
- Read: each thread reads its own records in the container.
- Scan: each thread scans the entire container once.
- Remove: each thread removes its own records from the container.
- The read/scan/remove data is populated by the insert test.

### Test workload: local-remote

- Insert, remove: each thread additionally tries to perform operations using records belonging to other threads.
- Mixed: each thread performs insert-local -> insert-remote -> read-local -> read-remote -> remove-local -> remove-remote.
- The target remote thread is randomly chosen.

### Test Results

- [`HashMap`](#HashMap)

|         | 11 threads     | 22 threads     | 44 threads     |
|---------|----------------|----------------|----------------|
| InsertL | 134.519639194s | 165.001678899s | 231.081117542s |
| ReadL   |  92.83194805s  | 104.560364479s | 114.468443191s |
| ScanL   |  42.086156353s | 108.655462554s | 229.909702447s |
| RemoveL | 109.926310571s | 123.499814546s | 139.1093042s   |
| InsertR | 249.260816589s | 301.757140479s | 399.315496693s |
| MixedR  | 310.705241166s | 337.750491321s | 363.707265976s |
| RemoveR | 208.355622788s | 226.59800359s  | 251.086396624s |

- [`HashIndex`](#HashIndex)

|         | 11 threads     | 22 threads     | 44 threads     |
|---------|----------------|----------------|----------------|
| InsertL | 134.519639194s | 165.001678899s | 231.081117542s |
| ReadL   |  92.83194805s  | 104.560364479s | 114.468443191s |
| ScanL   |  42.086156353s | 108.655462554s | 229.909702447s |
| RemoveL | 109.926310571s | 123.499814546s | 139.1093042s   |
| InsertR | 249.260816589s | 301.757140479s | 399.315496693s |
| MixedR  | 310.705241166s | 337.750491321s | 363.707265976s |
| RemoveR | 208.355622788s | 226.59800359s  | 251.086396624s |

- [`TreeIndex`](#TreeIndex)

|         | 11 threads     | 22 threads     | 44 threads     |
|---------|----------------|----------------|----------------|
| InsertL | 134.519639194s | 165.001678899s | 231.081117542s |
| ReadL   |  92.83194805s  | 104.560364479s | 114.468443191s |
| ScanL   |  42.086156353s | 108.655462554s | 229.909702447s |
| RemoveL | 109.926310571s | 123.499814546s | 139.1093042s   |
| InsertR | 249.260816589s | 301.757140479s | 399.315496693s |
| MixedR  | 310.705241166s | 337.750491321s | 363.707265976s |
| RemoveR | 208.355622788s | 226.59800359s  | 251.086396624s |

## Changelog

#### 0.5.0

* Own EBR implementation.
* API changes.

