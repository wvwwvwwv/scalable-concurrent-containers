use scc::TreeIndex;

use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};

fn read(c: &mut Criterion) {
    let treeindex: TreeIndex<usize, usize> = TreeIndex::default();
    assert!(treeindex.insert(1, 1).is_ok());
    c.bench_function("TreeIndex: read", |b| {
        b.iter_custom(|iters| {
            let treeindex: TreeIndex<u64, u64> = TreeIndex::default();
            for i in 0..iters {
                assert!(treeindex.insert(i, i).is_ok());
            }
            let start = Instant::now();
            for i in 0..iters {
                assert_eq!(treeindex.read(&i, |_, v| *v == i), Some(true));
            }
            start.elapsed()
        })
    });
}

criterion_group!(tree_index, read);
criterion_main!(tree_index);
