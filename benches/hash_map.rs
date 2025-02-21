use criterion::{criterion_group, criterion_main, Criterion};
use dashmap::DashMap;
use scc::HashMap;
use std::time::{Duration, Instant};

fn insert_cold(c: &mut Criterion) {
    c.bench_function("HashMap: insert, cold", |b| {
        b.iter_custom(|iters| {
            let hashmap: HashMap<u64, u64> = HashMap::default();
            let start = Instant::now();
            for i in 0..iters {
                assert!(hashmap.insert(i, i).is_ok());
            }
            start.elapsed()
        })
    });
}

fn insert_cold_dashmap(c: &mut Criterion) {
    c.bench_function("DashMap: insert, cold", |b| {
        b.iter_custom(|iters| {
            let hashmap: DashMap<u64, u64> = DashMap::default();
            let start = Instant::now();
            for i in 0..iters {
                assert!(hashmap.insert(i, i).is_none());
            }
            start.elapsed()
        })
    });
}

fn insert_warmed_up(c: &mut Criterion) {
    c.bench_function("HashMap: insert, warmed up", |b| {
        b.iter_custom(|iters| {
            let hashmap: HashMap<u64, u64> = HashMap::with_capacity(iters as usize * 2);
            let start = Instant::now();
            for i in 0..iters {
                assert!(hashmap.insert(i, i).is_ok());
            }
            start.elapsed()
        })
    });
}

fn insert_warmed_up_dashmap(c: &mut Criterion) {
    c.bench_function("DashMap: insert, warmed up", |b| {
        b.iter_custom(|iters| {
            let hashmap: DashMap<u64, u64> = DashMap::with_capacity(iters as usize * 2);
            let start = Instant::now();
            for i in 0..iters {
                assert!(hashmap.insert(i, i).is_none());
            }
            start.elapsed()
        })
    });
}

fn read(c: &mut Criterion) {
    c.bench_function("HashMap: read", |b| {
        b.iter_custom(|iters| {
            let hashmap: HashMap<u64, u64> = HashMap::with_capacity(iters as usize * 2);
            for i in 0..iters {
                assert!(hashmap.insert(i, i).is_ok());
            }
            let start = Instant::now();
            for i in 0..iters {
                assert_eq!(hashmap.read(&i, |_, v| *v == i), Some(true));
            }
            start.elapsed()
        })
    });
}

fn read_dashmap(c: &mut Criterion) {
    c.bench_function("DashMap: read", |b| {
        b.iter_custom(|iters| {
            let hashmap: DashMap<u64, u64> = DashMap::with_capacity(iters as usize * 2);
            for i in 0..iters {
                assert!(hashmap.insert(i, i).is_none());
            }
            let start = Instant::now();
            for i in 0..iters {
                assert_eq!(hashmap.get(&i).map(|v| *v == i), Some(true));
            }
            start.elapsed()
        })
    });
}

fn insert_tail_latency(c: &mut Criterion) {
    c.bench_function("HashMap: insert, tail latency", move |b| {
        b.iter_custom(|iters| {
            let mut agg_max_latency = Duration::default();
            for _ in 0..iters {
                let hashmap: HashMap<u64, u64> = HashMap::default();
                let mut key = 0;
                let mut max_latency = Duration::default();
                (0..1048576).for_each(|_| {
                    key += 1;
                    let start = Instant::now();
                    assert!(hashmap.insert(key, key).is_ok());
                    let elapsed = start.elapsed();
                    if elapsed > max_latency {
                        max_latency = elapsed;
                    }
                });
                agg_max_latency += max_latency;
            }
            agg_max_latency
        })
    });
}

fn insert_tail_latency_dashmap(c: &mut Criterion) {
    c.bench_function("DashMap: insert, tail latency", move |b| {
        b.iter_custom(|iters| {
            let mut agg_max_latency = Duration::default();
            for _ in 0..iters {
                let hashmap: DashMap<u64, u64> = DashMap::default();
                let mut key = 0;
                let mut max_latency = Duration::default();
                (0..1048576).for_each(|_| {
                    key += 1;
                    let start = Instant::now();
                    assert!(hashmap.insert(key, key).is_none());
                    let elapsed = start.elapsed();
                    if elapsed > max_latency {
                        max_latency = elapsed;
                    }
                });
                agg_max_latency += max_latency;
            }
            agg_max_latency
        })
    });
}

criterion_group!(
    hash_map,
    insert_cold,
    insert_tail_latency,
    insert_warmed_up,
    read,
    insert_cold_dashmap,
    insert_tail_latency_dashmap,
    insert_warmed_up_dashmap,
    read_dashmap
);
criterion_main!(hash_map);
