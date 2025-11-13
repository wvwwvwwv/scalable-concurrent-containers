use std::hash::{BuildHasher, RandomState};

use crate::SERIALIZER;

static BUCKET_LEN: usize = 32;

#[allow(clippy::cast_precision_loss)]
fn to_f64(value: usize) -> f64 {
    value as f64
}

#[ignore = "experiment"]
#[test]
fn sample() {
    let _guard = SERIALIZER.lock().unwrap();

    let build_hasher = RandomState::new();
    for b in [
        2, 8, 32, 128, 512, 2048, 8192, 32768, 131_072, 524_288, 2_097_152,
    ] {
        let mut buckets = vec![0; b];
        for key in 0..b * BUCKET_LEN {
            #[allow(clippy::cast_possible_truncation)]
            let hash = build_hasher.hash_one(key) as usize;
            buckets[hash % b] += 1;
        }
        println!("---------------");
        let mut s = 1;
        while s < b {
            let estimation: usize = buckets.iter().take(s).sum::<usize>() * (b / s);
            println!(
                "Num buckets: {b}, sample size: {s}, entries: {}, estimation: {estimation}, accuracy: {:.4}%",
                b * BUCKET_LEN,
                (to_f64(estimation) / to_f64(b * BUCKET_LEN)) * 100.0
            );
            s *= 2;
        }
        println!("---------------");
        let c = b.trailing_zeros().next_power_of_two() as usize;
        let estimation: usize = buckets.iter().take(c).sum::<usize>() * (b / c);
        println!(
            "(Log2({b})) Num buckets: {b}, sample size: {c}, entries: {}, estimation: {estimation}, accuracy: {:.4}%",
            b * BUCKET_LEN,
            (to_f64(estimation) / to_f64(b * BUCKET_LEN)) * 100.0
        );
        let estimation: usize = buckets.iter().take(c * 2).sum::<usize>() * (b / (c * 2));
        println!(
            "(Log2({b}) * 2) Num buckets: {b}, sample size: {}, entries: {}, estimation: {estimation}, accuracy: {:.4}%",
            c * 2,
            b * BUCKET_LEN,
            (to_f64(estimation) / to_f64(b * BUCKET_LEN)) * 100.0
        );
    }
}

#[ignore = "experiment"]
#[test]
fn overflow() {
    let _guard = SERIALIZER.lock().unwrap();

    let build_hasher = RandomState::new();
    for b in [32, 128, 512, 2048, 8192, 32768, 131_072, 524_288, 2_097_152] {
        for r in 11..16 {
            let mut buckets: Vec<usize> = vec![0; b];
            let n = (b / 16) * r * BUCKET_LEN;
            for key in 0..n {
                #[allow(clippy::cast_possible_truncation)]
                let hash = build_hasher.hash_one(key) as usize;
                buckets[hash % b] += 1;
            }
            if r == 13 {
                let mut o = 0;
                let mut m = 0;
                let mut map = Vec::new();
                for c in &buckets {
                    let offset = c.saturating_sub(BUCKET_LEN);
                    if offset != 0 {
                        o += 1;
                        if m < offset {
                            m = offset;
                        }
                    }
                    if map.len() < offset + 1 {
                        map.resize(offset + 1, 0);
                    }
                    map[offset] += 1;
                }
                println!(
                    "Num buckets: {b}, ratio: {r}/16, entries: {n}, overflows: {o}, max: {m}, rate: {:.4}%",
                    (to_f64(o) / to_f64(b)) * 100.0
                );
                for d in map.iter().enumerate() {
                    print!("{}:{} ", d.0, d.1);
                }
                println!();
            }
        }
    }
}
