# Changelog

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
