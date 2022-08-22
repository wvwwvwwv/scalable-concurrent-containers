use scc::HashIndex;

use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};

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

criterion_group!(hash_index, read);
criterion_main!(hash_index);
