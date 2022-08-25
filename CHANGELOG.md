# Changelog

0.10.3

* Implement `ebr::Barrier::defer`.
* Make `ebr::Collectible` public to enable implementation of unmanaged handles.

0.10.2

* Fix [#82](https://github.com/wvwwvwwv/scalable-concurrent-containers/issues/82).
* Implement `ebr::Barrier::defer_incremental_execute'.

0.10.1

* Significant [HashMap](#HashMap), [HashSet](#HashSet), and [HashIndex](#HashIndex) insert performance improvement by segregating zero and non-zero memory regions.

0.9.1

* [HashMap](#HashMap), [HashSet](#HashSet), and [HashIndex](#HashIndex) performance improvement.

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
* Minor [Queue](#Queue) API update.
* Reduce [HashMap](#HashMap) memory usage.
