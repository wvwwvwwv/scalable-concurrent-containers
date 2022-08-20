use scc::HashMap;

use std::convert::TryInto;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};

static BITMAP: AtomicU32 = AtomicU32::new(0);

fn load_bitmap() -> u32 {
    let mut bitmap = BITMAP.load(Relaxed);
    loop {
        if bitmap != 0 {
            return bitmap;
        }
        let new_bitmap: u32 = rand::random::<u32>() | (1_u32 << 16);
        if let Err(actual) = BITMAP.compare_exchange_weak(bitmap, new_bitmap, Relaxed, Relaxed) {
            bitmap = actual;
        } else {
            return new_bitmap;
        }
    }
}

fn search_circular(c: &mut Criterion) {
    let preferred_index: usize = rand::random::<usize>() % 32;
    let mut bitmap = load_bitmap();
    let mut num_hit = 0;
    c.bench_function("HashMap: search_circular", |b| {
        b.iter(|| {
            let occupied = bitmap;
            if (occupied & (1_u32 << preferred_index)) != 0 {
                num_hit += 1;
            }
            for i in 1..32 {
                let current_index = (preferred_index + i) % 32;
                if (occupied & (1_u32 << current_index)) != 0 {
                    num_hit += 1;
                }
            }
            bitmap = bitmap.rotate_left(1);
        })
    });
    assert_ne!(num_hit, 0);
}

fn search_fixed(c: &mut Criterion) {
    let preferred_index: usize = rand::random::<usize>() % 32;
    let mut bitmap = load_bitmap();
    let mut num_hit = 0;
    c.bench_function("HashMap: search_fixed", |b| {
        b.iter(|| {
            let mut occupied = bitmap;
            if (occupied & (1_u32 << preferred_index)) != 0 {
                occupied &= !(1_u32 << preferred_index);
                num_hit += 1;
            }
            let mut current_index = occupied.trailing_zeros();
            while (current_index as usize) < 32 {
                if (occupied & (1_u32 << current_index)) != 0 {
                    occupied &= !(1_u32 << current_index);
                    num_hit += 1;
                }
                current_index = occupied.trailing_zeros();
            }
            bitmap = bitmap.rotate_left(1);
        })
    });
    assert_ne!(num_hit, 0);
}

fn search_opt(c: &mut Criterion) {
    let preferred_index: usize = rand::random::<usize>() % 32;
    let mut bitmap = load_bitmap();
    let mut num_hit = 0;
    c.bench_function("HashMap: search_opt", |b| {
        b.iter(|| {
            let mut occupied = bitmap;
            if (occupied & (1_u32 << preferred_index)) != 0 {
                occupied &= !(1_u32 << preferred_index);
                num_hit += 1;
            }

            let mut phase1 = occupied & !((1_u32 << preferred_index) - 1);
            let mut current_index = phase1.trailing_zeros();
            while (current_index as usize) < 32 {
                if (phase1 & (1_u32 << current_index)) != 0 {
                    phase1 &= !(1_u32 << current_index);
                    num_hit += 1;
                }
                current_index = phase1.trailing_zeros();
            }

            let mut phase2 = occupied & ((1_u32 << preferred_index) - 1);
            let mut current_index = phase2.trailing_zeros();
            while (current_index as usize) < 32 {
                if (phase2 & (1_u32 << current_index)) != 0 {
                    phase2 &= !(1_u32 << current_index);
                    num_hit += 1;
                }
                current_index = phase2.trailing_zeros();
            }

            bitmap = bitmap.rotate_left(1);
        })
    });
    assert_ne!(num_hit, 0);
}

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
    search_circular,
    search_fixed,
    search_opt,
    insert_cold,
    insert_array_warmed_up,
    insert_fully_warmed_up,
    read
);
criterion_main!(hash_map);
