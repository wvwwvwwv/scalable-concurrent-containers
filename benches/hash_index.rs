use criterion::{criterion_group, criterion_main, Criterion};

use scc::concurrent::HashIndex;

fn read(c: &mut Criterion) {
    let hashindex: HashIndex<usize, usize> = HashIndex::default();
    assert!(hashindex.insert(1, 1).is_ok());
    c.bench_function("HashIndex: read", |b| {
        b.iter(|| {
            hashindex.read(&1, |_, v| assert_eq!(*v, 1));
        })
    });
}

criterion_group!(hash_index, read);
criterion_main!(hash_index);
