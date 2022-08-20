use scc::HashMap;

use std::convert::TryInto;
use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};

fn search_slot(c: &mut Criterion) {
    let preferred_index: usize = rand::random::<usize>() % 32;
    let mut bitmap: u32 = rand::random::<u32>() | (1_u32 << 16);
    let mut num_hit = 0;
    c.bench_function("HashMap: search_slot", |b| {
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
    search_slot,
    insert_cold,
    insert_array_warmed_up,
    insert_fully_warmed_up,
    read
);
criterion_main!(hash_map);
