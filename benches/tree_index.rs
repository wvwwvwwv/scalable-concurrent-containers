use criterion::{criterion_group, criterion_main, Criterion};

use scc::sync::TreeIndex;

fn read(c: &mut Criterion) {
    let treeindex: TreeIndex<usize, usize> = TreeIndex::default();
    assert!(treeindex.insert(1, 1).is_ok());
    c.bench_function("TreeIndex: read", |b| {
        b.iter(|| {
            treeindex.read(&1, |_, v| assert_eq!(*v, 1));
        })
    });
}

criterion_group!(tree_index, read);
criterion_main!(tree_index);
