# Scalable Concurrent Containers

[![Cargo](https://img.shields.io/crates/v/scc)](https://crates.io/crates/scc)
![Crates.io](https://img.shields.io/crates/l/scc)
![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/wvwwvwwv/scalable-concurrent-containers/scc.yml?branch=main)

A collection of high performance containers and utilities for concurrent and asynchronous programming.

#### Features

- Asynchronous counterparts of blocking and synchronous methods.
- Formally verified [EBR](#EBR) implementation.
- Near-linear scalability.
- No spin-locks and no busy loops.
- SIMD lookup to scan multiple entries in parallel [^note].
- Zero dependencies on other crates.
- [Serde](https://serde.rs) support: `features = ["serde"]`.

[^note]: Advanced SIMD instructions are used only when respective target features are enabled, e.g., `-C target_feature=+avx2`.

#### Concurrent and Asynchronous Containers

- [HashMap](#HashMap) is a concurrent and asynchronous hash map.
- [HashSet](#HashSet) is a concurrent and asynchronous hash set.
- [HashIndex](#HashIndex) is a read-optimized concurrent and asynchronous hash map.
- [HashCache](#HashCache) is a sampling-based LRU cache backed by [HashMap](#HashMap).
- [TreeIndex](#TreeIndex) is a read-optimized concurrent and asynchronous B-plus tree.

#### Utilities for Concurrent Programming

- [EBR](#EBR) implements lock-free epoch-based reclamation.
- [LinkedList](#LinkedList) is a type trait implementing a lock-free concurrent singly linked list.
- [Queue](#Queue) is a concurrent lock-free first-in-first-out container.
- [Stack](#Stack) is a concurrent lock-free last-in-first-out container.
- [Bag](#Bag) is a concurrent lock-free unordered opaque container.

## HashMap

[HashMap](#HashMap) is a concurrent hash map, optimized for highly parallel write-heavy workloads. [HashMap](#HashMap) is structured as a lock-free stack of entry bucket arrays. The entry bucket array is managed by [EBR](#EBR), thus enabling lock-free access to it and non-blocking container resizing. Each bucket is a fixed-size array of entries, and it is protected by a special read-write lock which provides both blocking and asynchronous methods.

### Locking behavior

#### Entry access: fine-grained locking

Read/write access to an entry is serialized by the read-write lock in the bucket containing the entry. There are no container-level locks, therefore, the larger the container gets, the lower the chance of the bucket-level lock being contended.

#### Resize: lock-free

Resizing of a [HashMap](#HashMap) is completely non-blocking and lock-free; resizing does not block any other read/write access to the container or resizing attempts. _Resizing is analogous to pushing a new bucket array into a lock-free stack_. Each entry in the old bucket array will be incrementally relocated to the new bucket array on future access to the container, and the old bucket array gets dropped eventually after it becomes empty.

### Examples

An entry can be inserted if the key is unique. The inserted entry can be updated, read, and removed synchronously or asynchronously.

```rust
use scc::HashMap;

let hashmap: HashMap<u64, u32> = HashMap::default();

assert!(hashmap.insert(1, 0).is_ok());
assert_eq!(hashmap.update(&1, |_, v| { *v = 2; *v }).unwrap(), 2);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
assert_eq!(hashmap.remove(&1).unwrap(), (1, 2));

hashmap.entry(7).or_insert(17);
assert_eq!(hashmap.read(&7, |_, v| *v).unwrap(), 17);

let future_insert = hashmap.insert_async(2, 1);
let future_remove = hashmap.remove_async(&1);
```

The `Entry` API of [HashMap](#HashMap) is useful if the workflow is complicated.

```rust
use scc::HashMap;

let hashmap: HashMap<u64, u32> = HashMap::default();

hashmap.entry(3).or_insert(7);
assert_eq!(hashmap.read(&3, |_, v| *v), Some(7));

hashmap.entry(4).and_modify(|v| { *v += 1 }).or_insert(5);
assert_eq!(hashmap.read(&4, |_, v| *v), Some(5));

let future_entry = hashmap.entry_async(3);
```

[HashMap](#HashMap) does not provide an [Iterator](https://doc.rust-lang.org/std/iter/trait.Iterator.html) since it is impossible to confine the lifetime of [Iterator::Item](https://doc.rust-lang.org/std/iter/trait.Iterator.html#associatedtype.Item) to the [Iterator](https://doc.rust-lang.org/std/iter/trait.Iterator.html). The limitation can be circumvented by relying on interior mutability, e.g., let the returned reference hold a lock, however it will easily lead to a deadlock if not correctly used, and frequent acquisition of locks may impact performance. Therefore, [Iterator](https://doc.rust-lang.org/std/iter/trait.Iterator.html) is not implemented, instead, [HashMap](#HashMap) provides a number of methods to iterate over entries synchronously or asynchronously: `any`, `any_async`, `prune`, `prune_async`, `retain`, `retain_async`, `scan`, `scan_async`, `OccupiedEntry::next`, and `OccupiedEntry::next_async`.

```rust
use scc::HashMap;

let hashmap: HashMap<u64, u32> = HashMap::default();

assert!(hashmap.insert(1, 0).is_ok());
assert!(hashmap.insert(2, 1).is_ok());

// Entries can be modified or removed via `retain`.
let mut acc = 0;
hashmap.retain(|k, v_mut| { acc += *k; *v_mut = 2; true });
assert_eq!(acc, 3);
assert_eq!(hashmap.read(&1, |_, v| *v).unwrap(), 2);
assert_eq!(hashmap.read(&2, |_, v| *v).unwrap(), 2);

// `any` returns `true` as soon as an entry satisfying the predicate is found.
assert!(hashmap.insert(3, 2).is_ok());
assert!(hashmap.any(|k, _| *k == 3));

// Multiple entries can be removed through `retain`.
hashmap.retain(|k, v| *k == 1 && *v == 2);

// `hash_map::OccupiedEntry` also can return the next closest occupied entry.
let first_entry = hashmap.first_entry();
assert!(first_entry.is_some());
let second_entry = first_entry.and_then(|e| e.next());
assert!(second_entry.is_none());

// Asynchronous iteration over entries using `scan_async`.
let future_scan = hashmap.scan_async(|k, v| println!("{k} {v}"));
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

The `peek` and `peek_with` methods are completely lock-free.

```rust
use scc::HashIndex;

let hashindex: HashIndex<u64, u32> = HashIndex::default();

assert!(hashindex.insert(1, 0).is_ok());

// `peek` and `peek_with` are lock-free.
assert_eq!(hashindex.peek_with(&1, |_, v| *v).unwrap(), 0);

let future_insert = hashindex.insert_async(2, 1);
let future_remove = hashindex.remove_if_async(&1, |_| true);
```

The `Entry` API of [HashIndex](#HashIndex) can be used to update an entry in-place.

```rust
use scc::HashIndex;

let hashindex: HashIndex<u64, u32> = HashIndex::default();
assert!(hashindex.insert(1, 1).is_ok());

if let Some(mut o) = hashindex.get(&1) {
    // Create a new version of the entry.
    o.update(2);
};

if let Some(mut o) = hashindex.get(&1) {
    // Update the entry in-place.
    unsafe { *o.get_mut() = 3; }
};
```

An [Iterator](https://doc.rust-lang.org/std/iter/trait.Iterator.html) is implemented for [HashIndex](#HashIndex), because any derived references can survive as long as the associated `ebr::Guard` lives.

```rust
use scc::ebr::Guard;
use scc::HashIndex;

let hashindex: HashIndex<u64, u32> = HashIndex::default();

assert!(hashindex.insert(1, 0).is_ok());

// Existing values can be replaced with a new one.
hashindex.get(&1).unwrap().update(1);

let guard = Guard::new();

// An `ebr::Guard` has to be supplied to `iter`.
let mut iter = hashindex.iter(&guard);

// The derived reference can live as long as `guard`.
let entry_ref = iter.next().unwrap();
assert_eq!(iter.next(), None);

drop(hashindex);

// The entry can be read after `hashindex` is dropped.
assert_eq!(entry_ref, (&1, &1));
```

## HashCache

[HashCache](#HashCache) is a concurrent sampling-based LRU cache that is based on the [HashMap](#HashMap) implementation. [HashCache](#HashCache) does not keep track of the least recently used entry in the entire cache, instead each bucket maintains a doubly linked list of occupied entries which is updated on access to entries in order to keep track of the least recently used entry within the bucket.

### Examples

The LRU entry in a bucket is evicted when a new entry is being inserted and the bucket is full.

```rust
use scc::HashCache;

let hashcache: HashCache<u64, u32> = HashCache::with_capacity(100, 2000);

/// The capacity cannot exceed the maximum capacity.
assert_eq!(hashcache.capacity_range(), 128..=2048);

/// If the bucket corresponding to `1` or `2` is full, the LRU entry will be evicted.
assert!(hashcache.put(1, 0).is_ok());
assert!(hashcache.put(2, 0).is_ok());

/// `1` becomes the most recently accessed entry in the bucket.
assert!(hashcache.get(&1).is_some());

/// An entry can be normally removed.
assert_eq!(hashcache.remove(&2).unwrap(), (2, 0));
```

## TreeIndex

[TreeIndex](#TreeIndex) is a B-plus tree variant optimized for read operations. [EBR](#EBR) protects the memory used by individual entries, thus enabling lock-free read access to them.

### Locking behavior

Read access is always lock-free and non-blocking. Write access to an entry is also lock-free and non-blocking as long as no structural changes are required. However, when nodes are being split or merged by a write operation, other write operations on keys in the affected range are blocked.

### Examples

An entry can be inserted if the key is unique, and it can be read, and removed afterwards. Locks are acquired or awaited only when internal nodes are split or merged.

```rust
use scc::TreeIndex;

let treeindex: TreeIndex<u64, u32> = TreeIndex::new();

assert!(treeindex.insert(1, 2).is_ok());

// `peek` and `peek_with` are lock-free.
assert_eq!(treeindex.peek_with(&1, |_, v| *v).unwrap(), 2);
assert!(treeindex.remove(&1));

let future_insert = treeindex.insert_async(2, 3);
let future_remove = treeindex.remove_if_async(&1, |v| *v == 2);
```

Entries can be scanned without acquiring any locks.

```rust
use scc::ebr::Guard;
use scc::TreeIndex;

let treeindex: TreeIndex<u64, u32> = TreeIndex::new();

assert!(treeindex.insert(1, 10).is_ok());
assert!(treeindex.insert(2, 11).is_ok());
assert!(treeindex.insert(3, 13).is_ok());

let guard = Guard::new();

// `visitor` iterates over entries without acquiring a lock.
let mut visitor = treeindex.iter(&guard);
assert_eq!(visitor.next().unwrap(), (&1, &10));
assert_eq!(visitor.next().unwrap(), (&2, &11));
assert_eq!(visitor.next().unwrap(), (&3, &13));
assert!(visitor.next().is_none());
```

A specific range of keys can be scanned.

```rust
use scc::ebr::Guard;
use scc::TreeIndex;

let treeindex: TreeIndex<u64, u32> = TreeIndex::new();

for i in 0..10 {
    assert!(treeindex.insert(i, 10).is_ok());
}

let guard = Guard::new();

assert_eq!(treeindex.range(1..1, &guard).count(), 0);
assert_eq!(treeindex.range(4..8, &guard).count(), 4);
assert_eq!(treeindex.range(4..=8, &guard).count(), 5);
```

## Bag

[Bag](#Bag) is a concurrent lock-free unordered container. [Bag](#Bag) is completely opaque, disallowing access to contained instances until they are popped. [Bag](#Bag) is especially efficient if the number of contained instances can be maintained under `ARRAY_LEN (default: usize::BITS / 2)`

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
assert!(queue.push_if(2, |e| e.map_or(false, |x| **x == 1)).is_ok());
assert!(queue.push_if(3, |e| e.map_or(false, |x| **x == 1)).is_err());
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

The `ebr` module implements epoch-based reclamation and various types of auxiliary data structures to make use of it safely. Its epoch-based reclamation algorithm is similar to that implemented in [crossbeam_epoch](https://docs.rs/crossbeam-epoch/), however users may find it easier to use as the lifetime of an instance is safely managed. For instance, `ebr::AtomicOwned` and `ebr::Owned` automatically retire the contained instance and `ebr::AtomicShared` and `ebr::Shared` hold a reference-counted instance which is retired when the last strong reference is dropped.

### Memory Overhead

Retired instances are stored in intrusive queues in thread-local storage, and therefore additional 16-byte space for `Option<NonNull<dyn Collectible>>` is allocated per instance.

### Examples

The `ebr` module can be used without an `unsafe` block.

```rust
use scc::ebr::{suspend, AtomicOwned, AtomicShared, Guard, Ptr, Shared, Tag};

use std::sync::atomic::Ordering::Relaxed;

// `atomic_shared` holds a strong reference to `17`.
let atomic_shared: AtomicShared<usize> = AtomicShared::new(17);

// `atomic_owned` owns `19`.
let atomic_owned: AtomicOwned<usize> = AtomicOwned::new(19);

// `guard` prevents the garbage collector from dropping reachable instances.
let guard = Guard::new();

// `ptr` cannot outlive `guard`.
let mut ptr: Ptr<usize> = atomic_shared.load(Relaxed, &guard);
assert_eq!(*ptr.as_ref().unwrap(), 17);

// `atomic_shared` can be tagged.
atomic_shared.update_tag_if(Tag::First, |p| p.tag() == Tag::None, Relaxed, Relaxed);

// `ptr` is not tagged, so CAS fails.
assert!(atomic_shared.compare_exchange(
    ptr,
    (Some(Shared::new(18)), Tag::First),
    Relaxed,
    Relaxed,
    &guard).is_err());

// `ptr` can be tagged.
ptr.set_tag(Tag::First);

// The return value of CAS is a handle to the instance that `atomic_shared` previously owned.
let prev: Shared<usize> = atomic_shared.compare_exchange(
    ptr,
    (Some(Shared::new(18)), Tag::Second),
    Relaxed,
    Relaxed,
    &guard).unwrap().0.unwrap();
assert_eq!(*prev, 17);

// `17` will be garbage-collected later.
drop(prev);

// `ebr::AtomicShared` can be converted into `ebr::Shared`.
let shared: Shared<usize> = atomic_shared.into_shared(Relaxed).unwrap();
assert_eq!(*shared, 18);

// `18` and `19` will be garbage-collected later.
drop(shared);
drop(atomic_owned);

// `17` is still valid as `guard` keeps the garbage collector from dropping it.
assert_eq!(*ptr.as_ref().unwrap(), 17);

// Execution of a closure can be deferred until all the current readers are gone.
guard.defer_execute(|| println!("deferred"));
drop(guard);

// If the thread is expected to lie dormant for a while, call `suspend()` to allow other threads
// to reclaim its own retired instances.
suspend();
```

## LinkedList

[LinkedList](#LinkedList) is a type trait that implements lock-free concurrent singly linked list operations, backed by [EBR](#EBR). It additionally provides a method for marking an entry of a linked list to denote a user-defined state.

### Examples

```rust
use scc::ebr::{AtomicShared, Guard, Shared};
use scc::LinkedList;

use std::sync::atomic::Ordering::Relaxed;

#[derive(Default)]
struct L(AtomicShared<L>, usize);
impl LinkedList for L {
    fn link_ref(&self) -> &AtomicShared<L> {
        &self.0
    }
}

let guard = Guard::new();

let head: L = L::default();
let tail: Shared<L> = Shared::new(L(AtomicShared::null(), 1));

// A new entry is pushed.
assert!(head.push_back(tail.clone(), false, Relaxed, &guard).is_ok());
assert!(!head.is_marked(Relaxed));

// Users can mark a flag on an entry.
head.mark(Relaxed);
assert!(head.is_marked(Relaxed));

// `next_ptr` traverses the linked list.
let next_ptr = head.next_ptr(Relaxed, &guard);
assert_eq!(next_ptr.as_ref().unwrap().1, 1);

// Once `tail` is deleted, it becomes invisible.
tail.delete_self(Relaxed);
assert!(head.next_ptr(Relaxed, &guard).is_null());
```

## Performance

### [HashMap](#HashMap) and [HashIndex](#HashIndex)

Comparison with [DashMap](https://github.com/xacrimon/dashmap).

- [Results on Apple M1 (8 cores)](https://github.com/wvwwvwwv/conc-map-bench).
- [Results on Intel Xeon (40 cores, avx2)](https://github.com/wvwwvwwv/conc-map-bench/tree/Intel).
- *Interpret the results cautiously as benchmarks usually do not represent real world workloads.*

### [EBR](#EBR)

- The average time taken to enter and exit a protected region: 2.1 nanoseconds on Apple M1.

## [Changelog](https://github.com/wvwwvwwv/scalable-concurrent-containers/blob/main/CHANGELOG.md)
