# Scalable Concurrent Containers

[![Cargo](https://img.shields.io/crates/v/scc?style=for-the-badge)](https://crates.io/crates/scc)
![Crates.io](https://img.shields.io/crates/l/scc?style=for-the-badge)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/wvwwvwwv/scalable-concurrent-containers/SCC?style=for-the-badge)

A collection of concurrent data structures and building blocks for concurrent programming.

- [scc::ebr](#EBR) implements epoch-based reclamation.
- [scc::awaitable::HashMap](#Awaitable-HashMap) is a non-blocking awaitable concurrent hash map.
- [scc::concurrent::LinkedList](#LinkedList) is a type trait implementing a wait-free concurrent singly linked list.
- [scc::concurrent::HashMap](#HashMap) is a concurrent hash map.
- [scc::concurrent::HashSet](#HashSet) is a concurrent hash set based on [scc::concurrent::HashMap](#HashMap).
- [scc::concurrent::HashIndex](#HashIndex) is a concurrent hash index allowing lock-free read and scan.
- [scc::concurrent::TreeIndex](#TreeIndex) is a concurrent B+ tree allowing lock-free read and scan.


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


## Awaitable HashMap

```rust
use scc::awaitable::HashMap;

let hashmap: HashMap<u64, u32> = HashMap::default();
let future_insert = hashmap.insert(11, 17);
```


## LinkedList

[`LinkedList`](#LinkedList) is a type trait that implements wait-free concurrent singly linked list operations, backed by [`EBR`](#EBR). It additionally provides support for marking an entry of a linked list to indicate that the entry is in a user-defined state.

### Examples

```rust
use scc::concurrent::LinkedList;
use scc::ebr::{Arc, AtomicArc, Barrier};
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

[`HashMap`](#HashMap) is a scalable in-memory unique key-value container that is targeted at highly concurrent heavy workloads. It applies [`EBR`](#EBR) to its entry array management, thus enabling it to avoid container-level locking and data sharding.

### Examples

A unique key can be inserted along with its corresponding value, and then it can be updated, read, and removed.

```rust
use scc::concurrent::HashMap;

let hashmap: HashMap<u64, u32> = HashMap::default();

assert!(hashmap.insert(1, 0).is_ok());
assert_eq!(hashmap.update(&1, |v| { *v = 2; *v }).unwrap(), 2);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
assert_eq!(hashmap.remove(&1).unwrap(), (1, 2));
```

It supports `upsert` as in database management software; it tries to insert the given key-value pair, and if it fails, it updates the value field with the supplied closure.

```rust
use scc::concurrent::HashMap;

let hashmap: HashMap<u64, u32> = HashMap::default();

hashmap.upsert(1, || 2, |_, v| *v = 2);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
hashmap.upsert(1, || 2, |_, v| *v = 3);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 3);
```

There is no method to confine the lifetime of references derived from an [`Iterator`](https://doc.rust-lang.org/std/iter/trait.Iterator.html) to the [`Iterator`](https://doc.rust-lang.org/std/iter/trait.Iterator.html), and it is illegal to let them live as long as the [`HashMap`](#HashMap) stays valid due to the lack of a global lock. Therefore [`Iterator`](https://doc.rust-lang.org/std/iter/trait.Iterator.html) is not implemented, instead, it provides two methods that allow a [`HashMap`](#HashMap) to iterate over its entries: `for_each`, and `retain`.

```rust
use scc::concurrent::HashMap;

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

// Inside `retain`, a `ebr::Barrier` protects the entry array.
assert_eq!(hashmap.retain(|key, value| *key == 1 && *value == 0), (1, 2));
```


## HashSet

[`HashSet`](#HashSet) is a variant of [`HashMap`](#HashMap) where the value type is fixed `()`.

### Examples

All the [`HashSet`](#HashSet) methods do not receive a value argument.

```rust
use scc::concurrent::HashSet;

let hashset: HashSet<u64> = HashSet::default();

assert!(hashset.read(&1, |_| true).is_none());
assert!(hashset.insert(1).is_ok());
assert!(hashset.read(&1, |_| true).unwrap());
```

The capacity of a [`HashSet`](#HashSet) can be specified.

```rust
use scc::concurrent::HashSet;
use std::collections::hash_map::RandomState;

let hashset: HashSet<u64, RandomState> = HashSet::new(1000000, RandomState::new());
assert_eq!(hashset.capacity(), 1048576);
```


## HashIndex

[`HashIndex`](#HashIndex) is a read-optimized version of [`HashMap`](#HashMap). It applies [`EBR`](#EBR) to its entry management as well, enabling it to perform read operations without acquiring locks.

### Examples

Its `read` method does not modify any shared data.

```rust
use scc::concurrent::HashIndex;

let hashindex: HashIndex<u64, u32> = HashIndex::default();

assert!(hashindex.insert(1, 0).is_ok());
assert_eq!(hashindex.read(&1, |_, v| *v).unwrap(), 0);
```

An [`Iterator`](https://doc.rust-lang.org/std/iter/trait.Iterator.html) is implemented for [`HashIndex`](#HashIndex), because derived references can survive as long as the associated `ebr::Barrier` lives.

```rust
use scc::concurrent::HashIndex;
use scc::ebr::Barrier;

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

[`TreeIndex`](#TreeIndex) is a B+ tree variant optimized for read operations. The `ebr` module enables it to implement lock-free read and scan methods.

### Examples

Key-value pairs can be inserted, read, and removed.

```rust
use scc::concurrent::TreeIndex;

let treeindex: TreeIndex<u64, u32> = TreeIndex::new();

assert!(treeindex.insert(1, 10).is_ok());
assert_eq!(treeindex.read(&1, |_, value| *value).unwrap(), 10);
assert!(treeindex.remove(&1));
```

Key-value pairs can be scanned.

```rust
use scc::concurrent::TreeIndex;
use scc::ebr::Barrier;

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
use scc::concurrent::TreeIndex;
use scc::ebr::Barrier;

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
- Rust: 1.56.0
- SCC: 0.5.8
- HashMap<usize, usize, RandomState> = HashMap::default()
- HashIndex<usize, usize, RandomState> = HashIndex::default()
- TreeIndex<usize, usize> = TreeIndex::default()

### Test data

- A disjoint range of `usize` integers is assigned to each thread: 128M integers for [`HashMap`](#HashMap) and 4M for others.
- The performance test code asserts the expected outcome of each operation and the post state of the container.

### Test workload: local

- Each test is run twice in a single process in order to minimize the effect of page faults as the overhead is unpredictable.
- Insert: each thread inserts its own records.
- Read: each thread reads its own records in the container.
- Scan: each thread scans the entire container once.
- Remove: each thread removes its own records from the container.
- The read/scan/remove data is populated by the insert test.

### Test workload: local-remote

- Insert, remove: each thread additionally operates using keys belonging to a randomly chosen remote thread.
- Mixed: each thread performs insert-local -> insert-remote -> read-local -> read-remote -> remove-local -> remove-remote.

### Test Results

- [`HashMap`](#HashMap)

|         | 11 threads | 22 threads | 44 threads |
|---------|------------|------------|------------|
| InsertL | 169.888s   | 182.077s   | 208.961s   |
| ReadL   | 104.574s   | 112.154s   | 118.647s   |
| ScanL   |  29.299s   |  60.656s   | 125.701s   |
| RemoveL | 127.436s   | 135.593s   | 145.353s   |
| InsertR | 324.540s   | 341.395s   | 365.969s   |
| MixedR  | 376.788s   | 396.557s   | 412.073s   |
| RemoveR | 260.364s   | 277.673s   | 295.6s     |

- [`HashIndex`](#HashIndex)

|         | 11 threads | 22 threads | 44 threads |
|---------|------------|------------|------------|
| InsertL |   4.574s   |   4.871s   |   6.692s   |
| ReadL   |   2.313s   |   2.397s   |   2.693s   |
| ScanL   |   0.757s   |   1.548s   |   3.094s   |
| RemoveL |   2.858s   |   2.927s   |   3.603s   |
| InsertR |   7.554s   |   8.434s   |  10.151s   |
| MixedR  |  11.321s   |  11.697s   |  13.955s   |
| RemoveR |   5.813s   |   5.92s    |   7.623s   |

- [`TreeIndex`](#TreeIndex)

|         | 11 threads | 22 threads | 44 threads |
|---------|------------|------------|------------|
| InsertL |   5.887s   |  10.731s   |  15.248s   |
| ReadL   |   1.069s   |   1.128s   |   1.24s    |
| ScanL   |  11.834s   |  26.22s    |  56.101s   |
| RemoveL |   5.06s    |   8.964s   |  17.051s   |
| InsertR |  16.986s   |  14.763s   |  23.677s   |
| MixedR  | 101.077s   | 128.350s   | 158.627s   |
| RemoveR |   6.771s   |   6.768s   |   8.102s   |


## Changelog

0.5.8

* Optimize [`HashMap`](#HashMap) and [`HashSet`](#HashSet).
* Fix a problem with the `retain` method erasing some entries satisfying the given predicate.

0.5.7

* Fix [`#63`](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/63).
* Add [`HashSet`](#HashSet).
