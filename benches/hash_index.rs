use criterion::{criterion_group, criterion_main, Criterion};
use scc::ebr::Guard;
use scc::HashIndex;
use std::time::Instant;

fn iter_with(c: &mut Criterion) {
    c.bench_function("HashIndex: iter_with", |b| {
        b.iter_custom(|iters| {
            let hashindex: HashIndex<u64, u64> = HashIndex::with_capacity(iters as usize * 2);
            for i in 0..iters {
                assert!(hashindex.insert(i, i).is_ok());
            }
            let start = Instant::now();
            let guard = Guard::new();
            let iter = hashindex.iter(&guard);
            for e in iter {
                assert_eq!(e.0, e.1);
            }
            start.elapsed()
        })
    });
}

fn read(c: &mut Criterion) {
    c.bench_function("HashIndex: read", |b| {
        b.iter_custom(|iters| {
            let hashindex: HashIndex<u64, u64> = HashIndex::with_capacity(iters as usize * 2);
            for i in 0..iters {
                assert!(hashindex.insert(i, i).is_ok());
            }
            let start = Instant::now();
            for i in 0..iters {
                assert_eq!(hashindex.read(&i, |_, v| *v == i), Some(true));
            }
            start.elapsed()
        })
    });
}

fn read_with(c: &mut Criterion) {
    c.bench_function("HashIndex: read_with", |b| {
        b.iter_custom(|iters| {
            let hashindex: HashIndex<u64, u64> = HashIndex::with_capacity(iters as usize * 2);
            for i in 0..iters {
                assert!(hashindex.insert(i, i).is_ok());
            }
            let start = Instant::now();
            let guard = Guard::new();
            for i in 0..iters {
                assert_eq!(hashindex.read_with(&i, |_, v| *v == i, &guard), Some(true));
            }
            start.elapsed()
        })
    });
}

criterion_group!(hash_index, iter_with, read, read_with);
criterion_main!(hash_index);
