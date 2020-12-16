#[cfg(test)]
mod hashmap_test {
    use proptest::prelude::*;
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    use scc::HashMap;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::collections::hash_map::RandomState;
    use std::collections::BTreeSet;
    use std::hash::{Hash, Hasher};
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
    use std::sync::Arc;
    use std::thread;

    struct MemoryTester {
        allocated_size: AtomicUsize,
        random_panic: AtomicBool,
    }

    unsafe impl GlobalAlloc for MemoryTester {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            debug_assert!(!self.random_panic.load(Relaxed) || rand::random::<u32>() % 4 != 0);
            let ret = System.alloc(layout);
            if !ret.is_null() {
                debug_assert!(self.allocated_size.fetch_add(layout.size(), Relaxed) != usize::MAX);
            } else {
                panic!("memory allocation failed");
            }
            return ret;
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            System.dealloc(ptr, layout);
            debug_assert!(self.allocated_size.fetch_sub(layout.size(), Relaxed) > 0);
        }
    }

    impl MemoryTester {
        fn panic_test(&self) {
            std::panic::set_hook(Box::new(|_| println!("panic occurred somewhere")));

            // panicking.rs:527 not OoM-safe - this test is disabled
            for _ in 0..1024 {
                let last_successful_insert = Arc::new(AtomicUsize::new(0));
                let last_successful_insert_cloned = last_successful_insert.clone();
                let hashmap: Arc<HashMap<usize, u16, _>> = Arc::new(Default::default());
                let hashmap_cloned = hashmap.clone();
                self.random_panic.store(true, Release);
                if let Err(_) = std::panic::catch_unwind(move || {
                    for i in 1..1025 {
                        if hashmap_cloned.insert(i, 1).is_ok() {
                            last_successful_insert_cloned.fetch_add(1, Relaxed);
                        }
                    }
                }) {
                    println!("panicked");
                }
                self.random_panic.store(false, Release);
            }

            let _ = std::panic::take_hook();
        }
    }

    #[global_allocator]
    static ALLOCATOR: MemoryTester = MemoryTester {
        allocated_size: AtomicUsize::new(0),
        random_panic: AtomicBool::new(false),
    };

    #[test]
    #[ignore]
    fn panic() {
        ALLOCATOR.panic_test();
    }

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
            if let Err((result, _)) = result2 {
                assert_eq!(result.get(), (&key, &mut 0));
            }

            let result3 = hashmap.upsert(key, 1);
            assert_eq!(result3.get(), (&key, &mut 1));
            drop(result3);

            let result4 = hashmap.insert(key, 10);
            assert!(result4.is_err());
            if let Err((result, _)) = result4 {
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
    fn basic_scanner() {
        for _ in 0..256 {
            let hashmap: Arc<HashMap<u64, u64, RandomState>> = Arc::new(Default::default());
            let hashmap_copied = hashmap.clone();
            let inserted = Arc::new(AtomicU64::new(0));
            let inserted_copied = inserted.clone();
            let thread_handle = thread::spawn(move || {
                for _ in 0..8 {
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
            });
            for i in 0..4096 {
                assert!(hashmap.insert(i, i).is_ok());
                inserted.store(i, Release);
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
        for s in vec![65536, 2097152, 16777216] {
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

mod treemap_test {
    use proptest::prelude::*;
    use scc::TreeIndex;

    proptest! {
        #[test]
        fn basic(_ in 0u64..10) {
        }
    }

    #[test]
    fn basic_tree() {
        // sequential
        let tree = TreeIndex::new();
        assert!(tree.insert(10, 10).is_ok());
    }
}
