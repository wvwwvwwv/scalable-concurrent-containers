use criterion::{criterion_group, criterion_main, Criterion};

use scc::ebr::Barrier;

fn barrier_single(c: &mut Criterion) {
    c.bench_function("barrier", |b| {
        b.iter(|| {
            let _barrier = Barrier::new();
        })
    });
}

fn barrier_superposed(c: &mut Criterion) {
    let _barrier = Barrier::new();
    c.bench_function("superposed barrier", |b| {
        b.iter(|| {
            let _barrier = Barrier::new();
        })
    });
}

criterion_group!(benches, barrier_single, barrier_superposed);
criterion_main!(benches);
