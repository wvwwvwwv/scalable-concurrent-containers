use criterion::{criterion_group, criterion_main, Criterion};

use scc::sync::HashMap;

use std::convert::TryInto;
use std::time::Instant;

fn insert_cold(c: &mut Criterion) {
    c.bench_function("HashMap: insert, cold", |b| {
        b.iter_custom(|iters| {
            let hashmap: HashMap<u64, u64> = HashMap::default();
            let start = Instant::now();
            for i in 0..iters {
                assert!(hashmap.insert(i, i).is_ok());
            }
            let elapsed = start.elapsed();
            drop(hashmap);
            elapsed
        })
    });
}

fn insert_array_warmed_up(c: &mut Criterion) {
    c.bench_function("HashMap: insert, array warmed up", |b| {
        b.iter_custom(|iters| {
            let hashmap: HashMap<u64, u64> = HashMap::default();
            let ticket = hashmap.reserve((iters * 2).try_into().unwrap());
            assert!(ticket.is_some());
            let start = Instant::now();
            for i in 0..iters {
                assert!(hashmap.insert(i, i).is_ok());
            }
            let elapsed = start.elapsed();
            drop(ticket);
            drop(hashmap);
            elapsed
        })
    });
}

fn insert_fully_warmed_up(c: &mut Criterion) {
    c.bench_function("HashMap: insert, fully warmed up", |b| {
        b.iter_custom(|iters| {
            let hashmap: HashMap<u64, u64> = HashMap::default();
            let ticket = hashmap.reserve((iters * 2).try_into().unwrap());
            assert!(ticket.is_some());
            for i in 0..iters {
                assert!(hashmap.insert(i, i).is_ok());
                assert!(hashmap.remove(&i).is_some());
            }
            let start = Instant::now();
            for i in 0..iters {
                assert!(hashmap.insert(i, i).is_ok());
            }
            let elapsed = start.elapsed();
            drop(ticket);
            drop(hashmap);
            elapsed
        })
    });
}

fn read(c: &mut Criterion) {
    let hashmap: HashMap<usize, usize> = HashMap::default();
    assert!(hashmap.insert(1, 1).is_ok());
    c.bench_function("HashMap: read", |b| {
        b.iter(|| {
            hashmap.read(&1, |_, v| assert_eq!(*v, 1));
        })
    });
}

criterion_group!(
    hash_map,
    insert_cold,
    insert_array_warmed_up,
    insert_fully_warmed_up,
    read
);
criterion_main!(hash_map);
