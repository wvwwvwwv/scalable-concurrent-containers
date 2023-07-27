# Changelog

## Version 2

### API update

- `*::Visitor` -> `*::Iter`.
- `*::Accessor` -> `*::IterMut`.
- `ebr::Barrier` -> `ebr::Guard`.
- `ebr::Arc` -> `ebr::Shared`.
- `ebr::AtomicArc` -> `ebr::AtomicShared`.
- `ebr::AtomicArc::get_arc` -> `ebr::AtomicShared::get_shared`.
- `ebr::AtomicArc::try_into_arc` -> `ebr::AtomicShared::try_into_shared`.
- `ebr::Ptr::get_arc` -> `ebr::Ptr::get_shared`.
- `*::first_occupied_entry*` -> `*::first_entry*`.
- Remove `HashIndex::update*` and `HashIndex::modify*`: superseded by `HashIndex::entry*`, `HashIndex::get*`, and `hash_index::OccupiedEntry::update`.
- Remove `Hash*::for_each*`: superseded by `HashMap::retain*`.
- `Hash*::clear*`, `Hash*::prune*`, and `Hash*::retain*` return `()`.

2.0.0

* New API.

## Version 1

1.9.1

* API update: add `hash_index::Entry` API.

1.9.0

* API update: add `ebr::{AtomicOwned, Owned}` for non-reference-counted instances.

1.8.3

* API update: add `ebr::AtomicArc::compare_exchange_weak`.

1.8.2

* API update: [#107](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/107) add `HashMap::{prune, prune_async}`.

1.8.1

* API update: add `HashCache::{contains, contains_async}`.

1.8.0

* API update: overhaul `hash_cache::Entry` API; values can be evicted through `hash_cache::Entry` API.

1.7.3

* Add `Bag::pop_all` and `Stack::pop_all`.

1.7.2

* Add `HashCache::{any, any_async, for_each, for_each_async, read, read_async}`.

1.7.1

* Add `Serde` support to `HashCache`.

1.7.0

* Optimize `Hash*::update*` and `HashIndex::modify*`.
* API update 1: [#94](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/94) a _WORK IN PROGRESS_ `HashCache` minimal implementation.
* API update 2: add `HashMap::{get, get_async}` returning an `OccupiedEntry`.
* API update 3: add `Hash*::capacity_range`.

1.6.3

* API update 1: [#96](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/96) - add `HashIndex::{modify, modify_async}`, special thanks to [novacrazy](https://github.com/novacrazy).
* API update 2: `Hash*::default()` for any `H: BuildHasher + Default`, by [novacrazy](https://github.com/novacrazy).

1.6.2

* API update: add `HashIndex::{retain, retain_async}`.

1.6.1

* API update: add a mutable `Bag` iterator.
* Replace `compare_exchange` with `compare_exchange_weak` where spurious failures do not cost much.
* Fix an issue with `hash_map::Reserve::fmt` which printed superfluous information.

1.6.0

* API update 1: remove `ebr::Barrier::defer_incremental_execute` in favor of unwind-safety.
* API update 2: all the data structures except for `hash_map::OccupiedEntry` and `hash_map::VacantEntry` are now `UnwindSafe`.
* API update 3: export `linked_list::Entry` as `LinkedEntry`.

1.5.0

* API update: `HashMap::remove_if*` passes `&mut V` to the supplied predicate.

1.4.4

* Major `Hash*` performance boost: vectorize `Hash*` bucket loopup operations.

1.4.3

* Add `const ARRAY_LEN: usize` type parameter to `Bag`.
* Minor optimization.

1.4.2

* Optimize `TreeIndex::is_empty`.
* Update documentation.

1.4.1

* Add `hash_index::Reserve` and `HashIndex::reserve`.
* Add missing `H = RandomState` to several types.
* Add `const` to several trivial functions.

1.4.0

* **Fix a correctness issue with LLVM 16 (Rust 1.70.0)**.
* API update: `{Stack, Queue}::{peek*}` receive `FnOnce(Option<&Entry<T>>) -> R`.
* `RandomState` is now the default type parameter for `hash_*` structures.
* Remove explicit `Sync` requirements.
* Remove `'static` lifetime constraints from `Bag`, `LinkedList`, `Queue`, and `Stack`.
* Minor `Hash*` optimization.

1.3.0

* Add `HashMap::first_occupied_entry*` for more flexible mutable iteration over entries.
* Add `ebr::Arc::get_ref_with`.
* Implement `Send` for `hash_map::Entry` if `(K, V): Send`.
* `Hash*::remove*` methods may deallocate the entire hash table when they find the container empty.

1.2.0

* API update 1: `AtomicArc::update_tag_if` now receives `fetch_order`, and the closure can access the pointer value.
* API update 2: rename `hash_map::Ticket` `hash_map::Reserve`.
* `Hash*` do not allocate bucket arrays until the first write access.
* `Hash*::remove*` more aggressively shrinks the bucket array.

1.1.4 - 1.1.5

* Optimize `Hash*::is_empty`.
* Remove unnecessary lifetime constraints on `BuildHasher`.

1.1.3

* Fix [#86](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/86) completely.
* EBR garbage instances and the garbage collector instance of a thread is now deallocated immediately when the thread exits if certain conditions are met.

1.1.2

* Fix [#86](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/86).

1.1.1

* Fix a rare problem with `HashMap` and `HashSet` violating lifetime contracts on drop.

1.1.0

* Remove `'static` bounds from `HashMap`, `HashSet`, and `ebr::{Arc, AtomicArc}`.

1.0.9

* Add `HashMap::{entry, entry_async}`.

1.0.8

* More robust panic handling.
* Doc update.

1.0.7

* Minor performance optimization.
* Identified a piece of blocking code in `HashIndex::read`, and make it non-blocking.

1.0.6

* Optimize `TreeIndex` for low-entropy input patterns.

1.0.5

* Add `{HashMap, HashSet}::{any, any_async}` to emulate `Iterator::any`.
* Implement `PartialEq` for `{HashMap, HashSet, HashIndex, TreeIndex}`.
* Add `serde` support to `{HashMap, HashSet, HashIndex, TreeIndex}`.
* Remove the unnecessary `Send` bound from `TreeIndex`.

1.0.4

* Minor `Hash*` optimization.

1.0.3

* Major `TreeIndex` performance improvement.
* Add `From<ebr::Tag> for u8`.

1.0.2

* Optimize `TreeIndex`.

1.0.1

* Add `Stack`.
* API update 1: remove `Bag::clone`.
* API update 2: replace `Queue::Entry` with `<linked_list::Entry as LinkedList>`.
* Optimize `Bag`.
* Fix memory ordering in `Bag::drop`.

1.0.0

* Implement `Bag`.

## Version 0

0.12.4

* Remove `scopeguard`.

0.12.3

* Minor `ebr` optimization.

0.12.2

* `Hash*::remove*` accept `FnOnce`.

0.12.1

* `HashMap::read`, `HashIndex::read`, and `HashIndex::read_with` accept `FnOnce`.
* Proper optimization for `T: Copy` and `!needs_drop::<T>()`.

0.12.0

* More aggressive EBR garbage collection.

0.11.5

* Fix [#84](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/84) completely.
* Micro-optimization.

0.11.4

* Optimize performance for `T: Copy`.

0.11.3

* Fix [#84](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/84).
* 0.11.2 and any older versions have a serious correctness problem with Rust 1.65.0 and newer.

0.11.2

* `HashIndex` and `HashMap` cleanup entries immediately when the instance is dropped.

0.11.1

* Adjust `HashIndex` parameters to suppress latency spikes.

0.11.0

* Replace `ebr::Barrer::reclaim` with `ebr::Arc::release`.
* Rename `ebr::Arc::drop_in_place` `ebr::Arc::release_drop_in_place`.
* Implement `ebr::Barrier::defer`.
* Make `ebr::Collectible` public.

0.10.2

* Fix [#82](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/82).
* Implement `ebr::Barrier::defer_incremental_execute`.

0.10.1

* Significant `HashMap`, `HashSet`, and `HashIndex` insert performance improvement by segregating zero and non-zero memory regions.

0.9.1

* `HashMap`, `HashSet`, and `HashIndex` performance improvement.

0.9.0

* API update: `HashMap::new`, `HashIndex::new`, and `HashSet::new`.
* Add `unsafe HashIndex::update` for linearizability.

0.8.4

* Implement `ebr::Barrier::defer_execute` for deferred closure execution.
* Fix [#78](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/78).

0.8.3

* Fix `ebr::AtomicArc::{clone, get_arc}` to never return a null pointer if the `ebr::AtomicArc` is always non-null.

0.8.2

* Fix [#77](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/77).

0.8.1

* Implement `Debug` for container types.

0.8.0

* Add `ebr::suspend` which enables garbage instances in a dormant thread to be reclaimed by other threads.
* Minor `Queue` API update.
* Reduce `HashMap` memory usage.
