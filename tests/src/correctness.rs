#[cfg(test)]
mod hashmap_test {
    use proptest::prelude::*;
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    use scc::HashMap;
    use std::collections::hash_map::RandomState;
    use std::collections::BTreeSet;
    use std::hash::{Hash, Hasher};
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    use std::sync::atomic::{AtomicU64, AtomicUsize};
    use std::sync::{Arc, Barrier};
    use std::thread;

    proptest! {
        #[test]
        fn basic(key in 0u64..10) {
            let hashmap: HashMap<u64, u32, RandomState> = Default::default();
            assert!(hashmap.iter().next().is_none());

            let result1 = hashmap.insert(key, 0);
            assert!(result1.is_ok());
            if let Ok(result) = result1 {
                assert_eq!(result.get(), (&key, &mut 0));
            }

            let result2 = hashmap.insert(key, 0);
            assert!(result2.is_err());
            if let Err((result, _, _)) = result2 {
                assert_eq!(result.get(), (&key, &mut 0));
            }

            let result3 = hashmap.upsert(key, 1);
            assert_eq!(result3.get(), (&key, &mut 1));
        drop(result3);

            let result4 = hashmap.insert(key, 10);
            assert!(result4.is_err());
            if let Err((result, _, _)) = result4 {
                assert_eq!(result.get(), (&key, &mut 1));
                *result.get().1 = 2;
            }

            let mut result5 = hashmap.iter();
            assert_eq!(result5.next(), Some((&key, &mut 2)));
            assert_eq!(result5.next(), None);

            for iter in hashmap.iter() {
                assert_eq!(iter, (&key, &mut 2));
                *iter.1 = 3;
            }

            let result6 = hashmap.get(&key);
            assert_eq!(result6.unwrap().get(), (&key, &mut 3));

            let result7 = hashmap.get(&(key + 1));
            assert!(result7.is_none());

            let result8 = hashmap.remove(&key);
            assert_eq!(result8.unwrap(), 3);

            let result9 = hashmap.insert(key + 2, 10);
            assert!(result9.is_ok());
            if let Ok(result) = result9 {
                assert_eq!(result.get(), (&(key + 2), &mut 10));
                result.erase();
            }

            let result10 = hashmap.get(&(key + 2));
            assert!(result10.is_none());
        }
    }

    #[test]
    fn string_key() {
        let hashmap1: HashMap<String, u32, RandomState> = Default::default();
        let hashmap2: HashMap<u32, String, RandomState> = Default::default();
        let mut checker1 = BTreeSet::new();
        let mut checker2 = BTreeSet::new();
        let mut runner = TestRunner::default();
        let test_size = 4096;
        for i in 0..test_size {
            let prop_str = "[a-z]{1,16}".new_tree(&mut runner).unwrap();
            let str_val = prop_str.current();
            if hashmap1.insert(str_val.clone(), i).is_ok() {
                checker1.insert((str_val.clone(), i));
            }
            if hashmap2.insert(i, str_val.clone()).is_ok() {
                checker2.insert((i, str_val.clone()));
            }
        }
        assert_eq!(hashmap1.len(|_| 65536), checker1.len());
        assert_eq!(hashmap2.len(|_| 65536), checker2.len());
        for iter in checker1 {
            let v = hashmap1.remove(&iter.0);
            assert_eq!(v.unwrap(), iter.1);
        }
        for iter in checker2 {
            let v = hashmap2.remove(&iter.0);
            assert_eq!(v.unwrap(), iter.1);
        }
        assert_eq!(hashmap1.len(|_| 65536), 0);
        assert_eq!(hashmap2.len(|_| 65536), 0);
    }

    #[test]
    fn cursor() {
        let data_size = 4096;
        for _ in 0..64 {
            let hashmap: Arc<HashMap<u64, u64, RandomState>> = Arc::new(Default::default());
            let hashmap_copied = hashmap.clone();
            let barrier = Arc::new(Barrier::new(2));
            let barrier_copied = barrier.clone();
            let inserted = Arc::new(AtomicU64::new(0));
            let inserted_copied = inserted.clone();
            let removed = Arc::new(AtomicU64::new(data_size));
            let removed_copied = removed.clone();
            let thread_handle = thread::spawn(move || {
                // test insert
                for _ in 0..2 {
                    barrier_copied.wait();
                    let mut scanned = 0;
                    let mut checker = BTreeSet::new();
                    let max = inserted_copied.load(Acquire);
                    for iter in hashmap_copied.iter() {
                        scanned += 1;
                        checker.insert(*iter.0);
                    }
                    println!("scanned: {}, max: {}", scanned, max);
                    for key in 0..max {
                        assert!(checker.contains(&key));
                    }
                }
                // test remove
                for _ in 0..2 {
                    barrier_copied.wait();
                    let mut scanned = 0;
                    let max = removed_copied.load(Acquire);
                    for iter in hashmap_copied.iter() {
                        scanned += 1;
                        assert!(*iter.0 < max);
                    }
                    println!("scanned: {}, max: {}", scanned, max);
                }
            });
            // insert
            barrier.wait();
            for i in 0..data_size {
                if i == data_size / 2 {
                    barrier.wait();
                }
                assert!(hashmap.insert(i, i).is_ok());
                inserted.store(i, Release);
            }
            // remove
            barrier.wait();
            for i in (0..data_size).rev() {
                if i == data_size / 2 {
                    barrier.wait();
                }
                assert!(hashmap.remove(&i).is_some());
                removed.store(i, Release);
            }
            thread_handle.join().unwrap();
        }
    }

    struct Data<'a> {
        data: u64,
        checker: &'a AtomicUsize,
    }

    impl<'a> Data<'a> {
        fn new(data: u64, checker: &'a AtomicUsize) -> Data<'a> {
            checker.fetch_add(1, Relaxed);
            Data {
                data: data,
                checker: checker,
            }
        }
    }

    impl<'a> Clone for Data<'a> {
        fn clone(&self) -> Self {
            Data::new(self.data, self.checker)
        }
    }

    impl<'a> Drop for Data<'a> {
        fn drop(&mut self) {
            self.checker.fetch_sub(1, Relaxed);
        }
    }

    impl<'a> Eq for Data<'a> {}

    impl<'a> Hash for Data<'a> {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.data.hash(state);
        }
    }

    impl<'a> PartialEq for Data<'a> {
        fn eq(&self, other: &Self) -> bool {
            self.data == other.data
        }
    }

    proptest! {
        #[test]
        fn insert(key in 0u64..16) {
            let range = 1024;
            let checker = AtomicUsize::new(0);
            let hashmap: HashMap<Data, Data, RandomState> = Default::default();
            for d in key..(key + range) {
                let result = hashmap.insert(Data::new(d, &checker), Data::new(d, &checker));
                assert!(result.is_ok());
                drop(result);
                let result = hashmap.upsert(Data::new(d, &checker), Data::new(d + 1, &checker));
                (*result.get().1) = Data::new(d + 2, &checker);
            }
            let statistics = hashmap.statistics();
            println!("{}", statistics);

            for d in (key + range)..(key + range + range) {
                let result = hashmap.insert(Data::new(d, &checker), Data::new(d, &checker));
                assert!(result.is_ok());
                drop(result);
                let result = hashmap.upsert(Data::new(d, &checker), Data::new(d + 1, &checker));
                (*result.get().1) = Data::new(d + 2, &checker);
            }
            let statistics = hashmap.statistics();
            println!("before retain: {}", statistics);

            let result = hashmap.retain(|k, _| k.data < key + range);
            assert_eq!(result, (range as usize, range as usize));

            let statistics = hashmap.statistics();
            println!("after retain: {}", statistics);

            assert_eq!(statistics.num_entries() as u64, range);
            let mut found_keys = 0;
            for iter in hashmap.iter() {
                assert!(iter.0.data < key + range);
                assert!(iter.0.data >= key);
                found_keys += 1;
            }
            assert_eq!(found_keys, range);
            assert_eq!(checker.load(Relaxed) as u64, range * 2);
            for d in key..(key + range) {
                let result = hashmap.get(&Data::new(d, &checker));
                result.unwrap().erase();
            }
            assert_eq!(checker.load(Relaxed), 0);

            let statistics = hashmap.statistics();
            println!("after erase: {}", statistics);

            for d in key..(key + range) {
                let result = hashmap.insert(Data::new(d, &checker), Data::new(d, &checker));
                assert!(result.is_ok());
                drop(result);
                let result = hashmap.upsert(Data::new(d, &checker), Data::new(d + 1, &checker));
               (*result.get().1) = Data::new(d + 2, &checker);
            }
            let result = hashmap.clear();
            assert_eq!(result, range as usize);
            assert_eq!(checker.load(Relaxed), 0);

            let statistics = hashmap.statistics();
            println!("after clear: {}", statistics);

            for d in key..(key + range) {
                let result = hashmap.insert(Data::new(d, &checker), Data::new(d, &checker));
                assert!(result.is_ok());
                drop(result);
                let result = hashmap.upsert(Data::new(d, &checker), Data::new(d + 1, &checker));
                (*result.get().1) = Data::new(d + 2, &checker);
            }
            assert_eq!(checker.load(Relaxed) as u64, range * 2);
            drop(hashmap);
            assert_eq!(checker.load(Relaxed), 0);
        }
    }

    #[test]
    fn sample() {
        for s in vec![65536, 2097152] {
            let hashmap: HashMap<usize, u8, RandomState> = HashMap::new(s, RandomState::new());
            let step_size = s / 16;
            let mut sample_warning_count = [(0usize, 0.0f32); 16];
            for p in 0..16 {
                for i in (p * step_size)..((p + 1) * step_size) {
                    assert!(hashmap.insert(i, 0).is_ok());
                }
                let statistics = hashmap.statistics();
                println!("{}/16: {}", p + 1, statistics);
                for sample_size in 0..16 {
                    let len = hashmap.len(|_| (1 << sample_size) * 16);
                    let diff = if statistics.num_entries() > len {
                        statistics.num_entries() - len
                    } else {
                        len - statistics.num_entries()
                    };
                    let div = diff as f32 / statistics.num_entries() as f32;
                    if div > 0.05f32 {
                        sample_warning_count[sample_size].0 += 1;
                    }
                    if div > sample_warning_count[sample_size].1 {
                        sample_warning_count[sample_size].1 = div;
                    }
                    println!("{}/16: {};{};{}", p + 1, 1 << sample_size, len, div);
                }
            }
            for (i, w) in sample_warning_count.iter().enumerate() {
                println!(
                    "sample cells: {}, errors > 0.05: {}, max: {}",
                    1 << i,
                    w.0,
                    w.1
                );
            }
        }
    }
}

#[cfg(test)]
mod hashindex_test {
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    use scc::HashIndex;
    use std::collections::hash_map::RandomState;
    use std::collections::BTreeSet;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::{Acquire, Release};
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn string_key() {
        let hashindex1: HashIndex<String, u32, RandomState> = Default::default();
        let hashindex2: HashIndex<u32, String, RandomState> = Default::default();
        let mut checker1 = BTreeSet::new();
        let mut checker2 = BTreeSet::new();
        let mut runner = TestRunner::default();
        let test_size = 4096;
        for i in 0..test_size {
            let prop_str = "[a-z]{1,16}".new_tree(&mut runner).unwrap();
            let str_val = prop_str.current();
            if hashindex1.insert(str_val.clone(), i).is_ok() {
                checker1.insert((str_val.clone(), i));
            }
            if hashindex2.insert(i, str_val.clone()).is_ok() {
                checker2.insert((i, str_val.clone()));
            }
        }
        assert_eq!(hashindex1.len(|_| 65536), checker1.len());
        assert_eq!(hashindex2.len(|_| 65536), checker2.len());
        for iter in checker1 {
            assert!(hashindex1.remove(&iter.0));
        }
        for iter in checker2 {
            assert!(hashindex2.remove(&iter.0));
        }
        assert_eq!(hashindex1.len(|_| 65536), 0);
        assert_eq!(hashindex2.len(|_| 65536), 0);
    }

    #[test]
    fn cursor() {
        let data_size = 4096;
        for _ in 0..64 {
            let hashindex: Arc<HashIndex<u64, u64, RandomState>> = Arc::new(Default::default());
            let hashindex_copied = hashindex.clone();
            let barrier = Arc::new(Barrier::new(2));
            let barrier_copied = barrier.clone();
            let inserted = Arc::new(AtomicU64::new(0));
            let inserted_copied = inserted.clone();
            let removed = Arc::new(AtomicU64::new(data_size));
            let removed_copied = removed.clone();
            let thread_handle = thread::spawn(move || {
                // test insert
                for _ in 0..2 {
                    barrier_copied.wait();
                    let mut scanned = 0;
                    let mut checker = BTreeSet::new();
                    let max = inserted_copied.load(Acquire);
                    for iter in hashindex_copied.iter() {
                        scanned += 1;
                        checker.insert(*iter.0);
                    }
                    println!("scanned: {}, max: {}", scanned, max);
                    for key in 0..max {
                        assert!(checker.contains(&key));
                    }
                }
                // test remove
                for _ in 0..2 {
                    barrier_copied.wait();
                    let mut scanned = 0;
                    let max = removed_copied.load(Acquire);
                    for iter in hashindex_copied.iter() {
                        scanned += 1;
                        assert!(*iter.0 < max);
                    }
                    println!("scanned: {}, max: {}", scanned, max);
                }
            });
            // insert
            barrier.wait();
            for i in 0..data_size {
                if i == data_size / 2 {
                    barrier.wait();
                }
                assert!(hashindex.insert(i, i).is_ok());
                inserted.store(i, Release);
            }
            // remove
            barrier.wait();
            for i in (0..data_size).rev() {
                if i == data_size / 2 {
                    barrier.wait();
                }
                assert!(hashindex.remove(&i));
                removed.store(i, Release);
            }
            thread_handle.join().unwrap();
        }
    }
}

#[cfg(test)]
mod treeindex_test {
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    use scc::TreeIndex;
    use std::collections::BTreeSet;
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn basic() {
        let range = 4096;
        let num_threads = 16;
        let tree: Arc<TreeIndex<usize, usize>> = Arc::new(TreeIndex::new());
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut thread_handles = Vec::with_capacity(num_threads);
        for thread_id in 0..num_threads {
            let tree_copied = tree.clone();
            let barrier_copied = barrier.clone();
            thread_handles.push(thread::spawn(move || {
                let first_key = thread_id * range;
                barrier_copied.wait();
                for key in first_key..(first_key + range / 2) {
                    assert!(tree_copied.insert(key, key).is_ok());
                }
                for key in first_key..(first_key + range / 2) {
                    assert!(tree_copied
                        .read(&key, |key, value| assert_eq!(key, value))
                        .is_some());
                }
                for key in (first_key + range / 2)..(first_key + range) {
                    assert!(tree_copied.insert(key, key).is_ok());
                }
                for key in (first_key + range / 2)..(first_key + range) {
                    assert!(tree_copied
                        .read(&key, |key, value| assert_eq!(key, value))
                        .is_some());
                }
            }));
        }
        for handle in thread_handles {
            handle.join().unwrap();
        }
        let mut found = 0;
        for key in 0..num_threads * range {
            if tree
                .read(&key, |key, value| assert_eq!(key, value))
                .is_some()
            {
                found += 1;
            }
        }
        assert_eq!(found, num_threads * range);
        for key in 0..num_threads * range {
            assert!(tree
                .read(&key, |key, value| assert_eq!(key, value))
                .is_some());
        }

        let mut scanner = tree.iter();
        let mut prev = 0;
        while let Some(entry) = scanner.next() {
            assert!(prev == 0 || prev < *entry.0);
            assert_eq!(*entry.0, *entry.1);
            prev = *entry.0;
        }
    }

    #[test]
    fn complex() {
        let range = 4096;
        let num_threads = 16;
        let tree: Arc<TreeIndex<usize, usize>> = Arc::new(TreeIndex::new());
        for t in 0..num_threads {
            // insert markers
            tree.insert(t * range, t * range).unwrap();
        }
        let stopped: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let mut thread_handles = Vec::with_capacity(num_threads);
        for thread_id in 0..num_threads {
            let tree_copied = tree.clone();
            let stopped_copied = stopped.clone();
            let barrier_copied = barrier.clone();
            thread_handles.push(thread::spawn(move || {
                let first_key = thread_id * range;
                barrier_copied.wait();
                while !stopped_copied.load(Relaxed) {
                    for key in (first_key + 1)..(first_key + range) {
                        assert!(tree_copied.insert(key, key).is_ok());
                    }
                    for key in (first_key + 1)..(first_key + range) {
                        assert!(tree_copied
                            .read(&key, |key, value| assert_eq!(key, value))
                            .is_some());
                    }
                    let mut from_scanner = tree_copied.from(&first_key);
                    let entry = from_scanner.next().unwrap();
                    assert_eq!(entry, (&first_key, &first_key));
                    let entry = from_scanner.next().unwrap();
                    assert_eq!(entry, (&(first_key + 1), &(first_key + 1)));

                    let key_at_halfway = first_key + range / 2;
                    for key in (first_key + 1)..(first_key + range) {
                        if key == key_at_halfway {
                            let mut from_scanner = tree_copied.from(&(first_key + 1));
                            let entry = from_scanner.next().unwrap();
                            assert_eq!(entry, (&key_at_halfway, &key_at_halfway));
                            let entry = from_scanner.next().unwrap();
                            assert_eq!(entry, (&(key_at_halfway + 1), &(key_at_halfway + 1)));
                        }
                        assert!(tree_copied.remove(&key));
                        assert!(!tree_copied.remove(&key));
                        assert!(tree_copied.read(&(first_key + 1), |_, _| ()).is_none());
                    }
                    for key in (first_key + 1)..(first_key + range) {
                        assert!(tree_copied
                            .read(&key, |key, value| assert_eq!(key, value))
                            .is_none());
                    }
                }
            }));
        }
        barrier.wait();
        for i in 0..512 {
            let mut found_0 = false;
            let mut found_markers = 0;
            let mut prev = 0;
            for iter in tree.iter() {
                let current = *iter.0;
                if current % range == 0 {
                    found_markers += 1;
                    if current == 0 {
                        found_0 = true;
                    }
                }
                assert!(prev == 0 || prev < current);
                prev = current
            }
            if i % 64 == 0 {
                println!("{} {}", i, found_markers);
            }
            assert!(found_0);
            assert_eq!(found_markers, num_threads);
        }

        stopped.store(true, Release);
        for handle in thread_handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn string_key() {
        let tree1: TreeIndex<String, u32> = Default::default();
        let tree2: TreeIndex<u32, String> = Default::default();
        let mut checker1 = BTreeSet::new();
        let mut checker2 = BTreeSet::new();
        let mut runner = TestRunner::default();
        let test_size = 4096;
        for i in 0..test_size {
            let prop_str = "[a-z]{1,16}".new_tree(&mut runner).unwrap();
            let str_val = prop_str.current();
            if tree1.insert(str_val.clone(), i).is_ok() {
                checker1.insert((str_val.clone(), i));
            }
            if tree2.insert(i, str_val.clone()).is_ok() {
                checker2.insert((i, str_val.clone()));
            }
        }
        for iter in checker1.iter() {
            let v = tree1.read(&iter.0, |_, v| v.clone());
            assert_eq!(v.unwrap(), iter.1);
        }
        for iter in checker2.iter() {
            let v = tree2.read(&iter.0, |_, v| v.clone());
            assert_eq!(v.unwrap(), iter.1);
        }
    }

    #[test]
    fn scanner() {
        let data_size = 4096;
        for _ in 0..64 {
            let tree: Arc<TreeIndex<usize, u64>> = Arc::new(Default::default());
            let tree_copied = tree.clone();
            let barrier = Arc::new(Barrier::new(2));
            let barrier_copied = barrier.clone();
            let inserted = Arc::new(AtomicUsize::new(0));
            let inserted_copied = inserted.clone();
            let removed = Arc::new(AtomicUsize::new(data_size));
            let removed_copied = removed.clone();
            let thread_handle = thread::spawn(move || {
                // test insert
                for _ in 0..2 {
                    barrier_copied.wait();
                    let mut checker = BTreeSet::new();
                    let max = inserted_copied.load(Acquire);
                    let mut prev = 0;
                    for iter in tree_copied.iter() {
                        checker.insert(*iter.0);
                        assert!(prev == 0 || prev < *iter.0);
                        prev = *iter.0;
                    }
                    for key in 0..max {
                        assert!(checker.contains(&key));
                    }
                }
                // test remove
                for _ in 0..2 {
                    barrier_copied.wait();
                    let mut prev = 0;
                    let max = removed_copied.load(Acquire);
                    for iter in tree_copied.iter() {
                        let current = *iter.0;
                        assert!(current < max);
                        assert!(prev + 1 == current || prev == 0);
                        prev = current;
                    }
                }
            });
            // insert
            barrier.wait();
            for i in 0..data_size {
                if i == data_size / 2 {
                    barrier.wait();
                }
                assert!(tree.insert(i, 0).is_ok());
                inserted.store(i, Release);
            }
            // remove
            barrier.wait();
            for i in (0..data_size).rev() {
                if i == data_size / 2 {
                    barrier.wait();
                }
                assert!(tree.remove(&i));
                removed.store(i, Release);
            }
            thread_handle.join().unwrap();
        }
    }
}
