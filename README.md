# Scalable Concurrent Containers

[![Cargo](https://img.shields.io/crates/v/scc)](https://crates.io/crates/scc)
![Crates.io](https://img.shields.io/crates/l/scc)
![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/wvwwvwwv/scalable-concurrent-containers/scc.yml?branch=main)

A collection of high performance containers and utilities for concurrent and asynchronous programming.

#### Features

- Asynchronous methods for blocking operations.
- Formally verified [EBR](#EBR) implementation.
- No spin-locks and no busy loops.
- Zero dependencies on other crates.
- [Serde](https://serde.rs) support: `features = ["serde"]`.

#### Concurrent and Asynchronous Containers

- [HashMap](#HashMap) is a concurrent and asynchronous hash map.
- [HashSet](#HashSet) is a concurrent and asynchronous hash set.
- [HashIndex](#HashIndex) is a read-optimized concurrent and asynchronous hash map.
- [TreeIndex](#TreeIndex) is a read-optimized concurrent and asynchronous B+ tree.

#### Utilities for Concurrent Programming

- [EBR](#EBR) implements epoch-based reclamation.
- [LinkedList](#LinkedList) is a type trait implementing a lock-free concurrent singly linked list.
- [Queue](#Queue) is an [EBR](#EBR) backed concurrent lock-free first-in-first-out container.
- [Stack](#Stack) is an [EBR](#EBR) backed concurrent lock-free last-in-first-out container.
- [Bag](#Bag) is a concurrent lock-free unordered opaque container.

## HashMap

[HashMap](#HashMap) is a concurrent hash map that is targeted at highly concurrent write-heavy workloads. [HashMap](#HashMap) is basically an array of entry buckets where each bucket is protected by a special read-write lock providing both blocking and asynchronous methods. The bucket array is fully managed by [EBR](#EBR) enabling lock-free access to it and non-blocking array resizing.

### Locking behavior

#### Entry access: fine-grained locking

Read/write access to an entry is serialized by the read-write lock in the bucket containing the entry. There are no container-level locks, therefore, the larger the container gets, the lower the chance of the bucket-level lock being contended.

#### Resize: lock-free

Resizing of the container is totally non-blocking and lock-free; resizing does not block any other read/write access to the container or resizing attempts. _Resizing is analogous to pushing a new bucket array into a lock-free stack_. Each individual entry in the old bucket array will be incrementally relocated to the new bucket array on future access to the container, and the old bucket array gets dropped eventually when it becomes empty.

### Examples

An entry can be inserted if the key is unique. The inserted entry can be updated, read, and removed synchronously or asynchronously.

```rust
use scc::HashMap;

let hashmap: HashMap<u64, u32> = HashMap::default();

assert!(hashmap.insert(1, 0).is_ok());
assert_eq!(hashmap.update(&1, |v| { *v = 2; *v }).unwrap(), 2);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
assert_eq!(hashmap.remove(&1).unwrap(), (1, 2));

hashmap.entry(7).or_insert(17);
assert_eq!(hashmap.read(&7, |_, v| *v).unwrap(), 17);

let future_insert = hashmap.insert_async(2, 1);
let future_remove = hashmap.remove_async(&1);
```

`upsert` will insert a new entry if the key does not exist, otherwise update the value field.

```rust
use scc::HashMap;

let hashmap: HashMap<u64, u32> = HashMap::default();

hashmap.upsert(1, || 2, |_, v| *v = 2);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
hashmap.upsert(1, || 2, |_, v| *v = 3);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 3);

let future_upsert = hashmap.upsert_async(2, || 1, |_, v| *v = 3);
```

[HashMap](#HashMap) does not provide an [Iterator](https://doc.rust-lang.org/std/iter/trait.Iterator.html) since it is impossible to confine the lifetime of [Iterator::Item](https://doc.rust-lang.org/std/iter/trait.Iterator.html#associatedtype.Item) to the [Iterator](https://doc.rust-lang.org/std/iter/trait.Iterator.html). The limitation can be circumvented by relying on interior mutability, e.g., let the returned reference hold a lock, however it will easily lead to a deadlock if not correctly used, and frequent acquisition of locks may impact performance. Therefore, [Iterator](https://doc.rust-lang.org/std/iter/trait.Iterator.html) is not implemented, instead, [HashMap](#HashMap) provides a number of methods to iterate over entries synchronously or asynchronously: `any`, `any_async`, `for_each`, `for_each_async`, `OccupiedEntry::next`, `OccupiedEntry::next_async`, `retain`, `retain_async`, `scan`,  and `scan_async`.

```rust
use scc::HashMap;

let hashmap: HashMap<u64, u32> = HashMap::default();

assert!(hashmap.insert(1, 0).is_ok());
assert!(hashmap.insert(2, 1).is_ok());

// `for_each` allows entry modification.
let mut acc = 0;
hashmap.for_each(|k, v_mut| { acc += *k; *v_mut = 2; });
assert_eq!(acc, 3);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
assert_eq!(hashmap.read(&2, |_, v| *v).unwrap(), 2);

// `any` returns `true` as soon as an entry satisfying the predicate is found.
assert!(hashmap.insert(3, 2).is_ok());
assert!(hashmap.any(|k, _| *k == 3));

// `retain` enables entry removal.
assert_eq!(hashmap.retain(|k, v| *k == 1 && *v == 0), (1, 2));

// `hash_map::OccupiedEntry` also can return the next closest occupied entry.
let first_entry = hashmap.first_occupied_entry();
let second_entry = first_entry.and_then(|e| e.next());
assert!(first_entry.is_some() && second_entry.is_some());
assert!(second_entry.and_then(|e| e.next()).is_none());

// Asynchronous iteration over entries using `scan_async` and `for_each_async`.
let future_scan = hashmap.scan_async(|k, v| println!("{k} {v}"));
let future_for_each = hashmap.for_each_async(|k, v_mut| { *v_mut = *k; });
```

## HashSet

[HashSet](#HashSet) is a special version of [HashMap](#HashMap) where the value type is `()`.

### Examples

Most [HashSet](#HashSet) methods are identical to that of [HashMap](#HashMap) except that they do not receive a value argument, and some [HashMap](#HashMap) methods for value modification are not implemented for [HashSet](#HashSet).

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

[HashIndex](#HashIndex) is a read-optimized version of [HashMap](#HashMap). In a [HashIndex](#HashIndex), not only is the memory of the bucket array managed by [EBR](#EBR), but also that of entry buckets is protected by [EBR](#EBR), enabling lock-free read access to individual entries.

### Examples

The `read` method is completely lock-free.

```rust
use scc::HashIndex;

let hashindex: HashIndex<u64, u32> = HashIndex::default();

assert!(hashindex.insert(1, 0).is_ok());
assert_eq!(hashindex.read(&1, |_, v| *v).unwrap(), 0);

let future_insert = hashindex.insert_async(2, 1);
let future_remove = hashindex.remove_if_async(&1, |_| true);
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

[TreeIndex](#TreeIndex) is a B+ tree variant optimized for read operations. [EBR](#EBR) protects the memory used by individual entries, thus enabling lock-free read access to them.

### Locking behavior

Read access is always lock-free and non-blocking. Write access to an entry is also lock-free and non-blocking as long as no structural changes are required. However, when nodes are being split or merged by a write operation, other write operations on keys in the affected range are blocked.

### Examples

An entry can be inserted if the key is unique, and it can be read, and removed afterwards. Locks are acquired or awaited only when internal nodes are split or merged.

```rust
use scc::TreeIndex;

let treeindex: TreeIndex<u64, u32> = TreeIndex::new();

assert!(treeindex.insert(1, 2).is_ok());

// `read` is lock-free.
assert_eq!(treeindex.read(&1, |_, v| *v).unwrap(), 2);
assert!(treeindex.remove(&1));

let future_insert = treeindex.insert_async(2, 3);
let future_remove = treeindex.remove_if_async(&1, |v| *v == 2);
```

Entries can be scanned without acquiring any locks.

```rust
use scc::ebr::Barrier;
use scc::TreeIndex;

let treeindex: TreeIndex<u64, u32> = TreeIndex::new();

assert!(treeindex.insert(1, 10).is_ok());
assert!(treeindex.insert(2, 11).is_ok());
assert!(treeindex.insert(3, 13).is_ok());

let barrier = Barrier::new();

// `visitor` iterates over entries without acquiring a lock.
let mut visitor = treeindex.iter(&barrier);
assert_eq!(visitor.next().unwrap(), (&1, &10));
assert_eq!(visitor.next().unwrap(), (&2, &11));
assert_eq!(visitor.next().unwrap(), (&3, &13));
assert!(visitor.next().is_none());
```

A specific range of keys can be scanned.

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

## Bag

[Bag](#Bag) is a concurrent lock-free unordered container. [Bag](#Bag) is completely opaque, disallowing access to contained instances until they are popped. [Bag](#Bag) is especially efficient if the number of contained instances can be maintained under `ARRAY_LEN (default: usize::MAX / 2)`

### Examples

```rust
use scc::Bag;

let bag: Bag<usize> = Bag::default();

bag.push(1);
assert!(!bag.is_empty());
assert_eq!(bag.pop(), Some(1));
assert!(bag.is_empty());
```

## Queue

[Queue](#Queue) is an [EBR](#EBR) backed concurrent lock-free first-in-first-out container.

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

## Stack

[Stack](#Stack) is an [EBR](#EBR) backed concurrent lock-free last-in-first-out container.

### Examples

```rust
use scc::Stack;

let stack: Stack<usize> = Stack::default();

stack.push(1);
stack.push(2);
assert_eq!(stack.pop().map(|e| **e), Some(2));
assert_eq!(stack.pop().map(|e| **e), Some(1));
assert!(stack.pop().is_none());
```

## EBR

The `ebr` module implements epoch-based reclamation and various types of auxiliary data structures to make use of it safely. Its epoch-based reclamation algorithm is similar to that implemented in [crossbeam_epoch](https://docs.rs/crossbeam-epoch/), however users may find it easier to use as the lifetime of an instance is safely managed. For instance, `ebr::AtomicArc` and `ebr::Arc` hold a strong reference to the underlying instance, and the instance is automatically passed to the garbage collector when the reference count drops to zero.

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
atomic_arc.update_tag_if(Tag::First, |p| p.tag() == Tag::None, Relaxed, Relaxed);

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

// The closure will be repeatedly invoked until it returns `true`.
let barrier = Barrier::new();
let mut data = 3;
barrier.defer_incremental_execute(move || {
    if data == 0 {
        return true;
    }
    data -= 1;
    false
});
drop(barrier);

// If the thread is expected to lie dormant for a while, call `suspend()` to allow other threads
// to reclaim its own retired instances.
suspend();
```

## LinkedList

[LinkedList](#LinkedList) is a type trait that implements lock-free concurrent singly linked list operations, backed by [EBR](#EBR). It additionally provides a method for marking an entry of a linked list to denote a user-defined state.

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

### [HashMap](#HashMap) and [HashIndex](#HashIndex)

Comparison with [DashMap](https://github.com/xacrimon/dashmap).

- [Results on Apple M1 (8 cores)](https://github.com/wvwwvwwv/conc-map-bench).
- [Results on Intel Xeon (VM, 40 cores)](https://github.com/wvwwvwwv/conc-map-bench/tree/Intel).
- The benchmark test is a fork of [conc-map-bench](https://github.com/xacrimon/conc-map-bench).
- *Interpret the results cautiously as benchmarks usually do not represent real world workloads.*

### [EBR](#EBR)

- The average time taken to enter and exit a protected region: 2.1ns on Apple M1.

## [Changelog](https://github.com/wvwwvwwv/scalable-concurrent-containers/blob/main/CHANGELOG.md)
