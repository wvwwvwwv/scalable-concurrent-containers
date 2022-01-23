use criterion::{criterion_group, criterion_main, Criterion};

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::mem::{align_of, size_of};

fn align_1(c: &mut Criterion) {
    c.bench_function("align_1", |b| {
        b.iter(|| unsafe {
            let size = size_of::<[usize; 16]>() * 1048576 * 64;
            let layout = Layout::from_size_align_unchecked(size, 1);
            let ptr = alloc_zeroed(layout);
            assert!(!ptr.is_null());
            dealloc(ptr, layout);
        })
    });
}

fn align_auto(c: &mut Criterion) {
    c.bench_function("align_auto", |b| {
        b.iter(|| unsafe {
            let size = size_of::<[usize; 16]>() * 1048576 * 64;
            let align = align_of::<[usize; 16]>();
            let layout = Layout::from_size_align_unchecked(size, align);
            let ptr = alloc_zeroed(layout);
            assert!(!ptr.is_null());
            dealloc(ptr, layout);
        })
    });
}

criterion_group!(allocation, align_1, align_auto);
criterion_main!(allocation);
