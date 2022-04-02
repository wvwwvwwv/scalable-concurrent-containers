#[cfg(test)]
mod hashmap_test {
    use crate::HashMap;

    use proptest::prelude::*;
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    use std::collections::BTreeSet;
    use std::hash::{Hash, Hasher};
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    use std::sync::atomic::{AtomicU64, AtomicUsize};
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn string_key() {
        let hashmap1: HashMap<String, u32> = HashMap::default();
        let hashmap2: HashMap<u32, String> = HashMap::default();
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
            let str_borrowed = str_val.as_str();
            assert!(hashmap1.contains(str_borrowed));
            assert!(hashmap1.read(str_borrowed, |_, _| ()).is_some());

            if hashmap2.insert(i, str_val.clone()).is_ok() {
                checker2.insert((i, str_val.clone()));
            }
        }
        assert_eq!(hashmap1.len(), checker1.len());
        assert_eq!(hashmap2.len(), checker2.len());
        for iter in checker1 {
            let v = hashmap1.remove(iter.0.as_str());
            assert_eq!(v.unwrap().1, iter.1);
        }
        for iter in checker2 {
            let v = hashmap2.remove(&iter.0);
            assert_eq!(v.unwrap().1, iter.1);
        }
        assert_eq!(hashmap1.len(), 0);
        assert_eq!(hashmap2.len(), 0);
    }

    #[test]
    fn accessor() {
        let data_size = 4096;
        for _ in 0..16 {
            let hashmap: Arc<HashMap<u64, u64>> = Arc::new(HashMap::default());
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
                    hashmap_copied.for_each(|k, _| {
                        scanned += 1;
                        checker.insert(*k);
                    });
                    for key in 0..max {
                        assert!(checker.contains(&key));
                    }
                }
                // test remove
                for _ in 0..2 {
                    barrier_copied.wait();
                    let mut scanned = 0;
                    let max = removed_copied.load(Acquire);
                    hashmap_copied.for_each(|k, _| {
                        scanned += 1;
                        assert!(*k < max);
                    });
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

    struct Data {
        data: usize,
        checker: Arc<AtomicUsize>,
    }

    impl Data {
        fn new(data: usize, checker: Arc<AtomicUsize>) -> Data {
            checker.fetch_add(1, Relaxed);
            Data { data, checker }
        }
    }

    impl Clone for Data {
        fn clone(&self) -> Self {
            Data::new(self.data, self.checker.clone())
        }
    }

    impl Drop for Data {
        fn drop(&mut self) {
            self.checker.fetch_sub(1, Relaxed);
        }
    }

    impl Eq for Data {}

    impl Hash for Data {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.data.hash(state);
        }
    }

    impl PartialEq for Data {
        fn eq(&self, other: &Self) -> bool {
            self.data == other.data
        }
    }

    proptest! {
        #[test]
        fn insert(key in 0_usize..16) {
            let range = 4096;
            let checker = Arc::new(AtomicUsize::new(0));
            let hashmap: HashMap<Data, Data> = HashMap::default();
            for d in key..(key + range) {
                assert!(hashmap.insert(Data::new(d, checker.clone()), Data::new(d, checker.clone())).is_ok());
                hashmap.upsert(Data::new(d, checker.clone()), || Data::new(d + 1, checker.clone()), |_, v| *v = Data::new(d + 2, checker.clone()));
            }

            for d in (key + range)..(key + range + range) {
                assert!(hashmap.insert(Data::new(d, checker.clone()), Data::new(d, checker.clone())).is_ok());
                hashmap.upsert(Data::new(d, checker.clone()), || Data::new(d, checker.clone()), |_, v| *v = Data::new(d + 1, checker.clone()));
            }

            let result = hashmap.retain(|k, _| k.data < key + range);
            assert_eq!(result, (range, range));

            assert_eq!(hashmap.len(), range);
            let mut found_keys = 0;
            hashmap.for_each(|k, v| {
                assert!(k.data < key + range);
                assert!(v.data >= key);
                found_keys += 1;
            });
            assert_eq!(found_keys, range);
            assert_eq!(checker.load(Relaxed), range * 2);
            for d in key..(key + range) {
                assert!(hashmap.contains(&Data::new(d, checker.clone())));
            }
            for d in key..(key + range) {
                assert!(hashmap.remove(&Data::new(d, checker.clone())).is_some());
            }
            assert_eq!(checker.load(Relaxed), 0);

            for d in key..(key + range) {
                assert!(hashmap.insert(Data::new(d, checker.clone()), Data::new(d, checker.clone())).is_ok());
                hashmap.upsert(Data::new(d, checker.clone()), || Data::new(d, checker.clone()), |_, v| *v = Data::new(d + 2, checker.clone()));
            }
            let result = hashmap.clear();
            assert_eq!(result, range as usize);
            assert_eq!(checker.load(Relaxed), 0);

            for d in key..(key + range) {
                assert!(hashmap.insert(Data::new(d, checker.clone()), Data::new(d, checker.clone())).is_ok());
                hashmap.upsert(Data::new(d, checker.clone()), || Data::new(d, checker.clone()), |_, v| *v = Data::new(d + 2, checker.clone()));
            }
            assert_eq!(checker.load(Relaxed), range * 2);
            drop(hashmap);
            assert_eq!(checker.load(Relaxed), 0);
        }
    }
}

#[cfg(test)]
mod hashindex_test {
    use crate::ebr;
    use crate::HashIndex;

    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    use std::collections::BTreeSet;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::{Acquire, Release};
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn string_key() {
        let hashindex1: HashIndex<String, u32> = HashIndex::default();
        let hashindex2: HashIndex<u32, String> = HashIndex::default();
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
            let str_borrowed = str_val.as_str();
            assert!(hashindex1.read(str_borrowed, |_, _| ()).is_some());

            if hashindex2.insert(i, str_val.clone()).is_ok() {
                checker2.insert((i, str_val.clone()));
            }
        }
        assert_eq!(hashindex1.len(), checker1.len());
        assert_eq!(hashindex2.len(), checker2.len());
        for iter in checker1 {
            assert!(hashindex1.remove(iter.0.as_str()));
        }
        for iter in checker2 {
            assert!(hashindex2.remove(&iter.0));
        }
        assert_eq!(hashindex1.len(), 0);
        assert_eq!(hashindex2.len(), 0);
    }

    #[test]
    fn visitor() {
        let data_size = 4096;
        for _ in 0..64 {
            let hashindex: Arc<HashIndex<u64, u64>> = Arc::new(HashIndex::default());
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
                    let mut checker = BTreeSet::new();
                    let max = inserted_copied.load(Acquire);
                    for iter in hashindex_copied.iter(&ebr::Barrier::new()) {
                        checker.insert(*iter.0);
                    }
                    for key in 0..max {
                        assert!(checker.contains(&key));
                    }
                }
                // test remove
                for _ in 0..2 {
                    barrier_copied.wait();
                    let max = removed_copied.load(Acquire);
                    for iter in hashindex_copied.iter(&ebr::Barrier::new()) {
                        assert!(*iter.0 < max);
                    }
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
    use crate::ebr;
    use crate::TreeIndex;

    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    use std::collections::BTreeSet;
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Arc, Barrier};
    use std::thread;

    use tokio::sync::Barrier as AsyncBarrier;

    struct R(&'static AtomicUsize);
    impl R {
        fn new(cnt: &'static AtomicUsize) -> R {
            cnt.fetch_add(1, Relaxed);
            R(cnt)
        }
    }
    impl Clone for R {
        fn clone(&self) -> Self {
            self.0.fetch_add(1, Relaxed);
            R(self.0)
        }
    }
    impl Drop for R {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Relaxed);
        }
    }

    #[tokio::test]
    async fn insert_drop() {
        static CNT: AtomicUsize = AtomicUsize::new(0);
        let tree: TreeIndex<usize, R> = TreeIndex::default();

        let workload_size = 1024;
        for k in 0..workload_size {
            assert!(tree.insert_async(k, R::new(&CNT)).await.is_ok());
        }
        assert!(CNT.load(Relaxed) >= workload_size);
        assert_eq!(tree.len(), workload_size);
        drop(tree);

        while CNT.load(Relaxed) != 0 {
            drop(crate::ebr::Barrier::new());
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn insert_remove() {
        static CNT: AtomicUsize = AtomicUsize::new(0);
        let tree: TreeIndex<usize, R> = TreeIndex::default();

        let workload_size = 1024;
        for k in 0..workload_size {
            assert!(tree.insert_async(k, R::new(&CNT)).await.is_ok());
        }
        assert!(CNT.load(Relaxed) >= workload_size);
        assert_eq!(tree.len(), workload_size);
        for k in 0..workload_size {
            assert!(tree.remove_async(&k).await);
        }
        assert_eq!(tree.len(), 0);

        while CNT.load(Relaxed) != 0 {
            drop(crate::ebr::Barrier::new());
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn clear() {
        static CNT: AtomicUsize = AtomicUsize::new(0);
        let tree: TreeIndex<usize, R> = TreeIndex::default();

        let workload_size = 1024;
        for k in 0..workload_size {
            assert!(tree.insert_async(k, R::new(&CNT)).await.is_ok());
        }
        assert!(CNT.load(Relaxed) >= workload_size);
        assert_eq!(tree.len(), workload_size);
        tree.clear();

        while CNT.load(Relaxed) != 0 {
            drop(crate::ebr::Barrier::new());
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn integer_key() {
        let num_tasks = 8;
        let workload_size = 256;
        for _ in 0..256 {
            let tree: Arc<TreeIndex<usize, usize>> = Arc::new(TreeIndex::default());
            let mut task_handles = Vec::with_capacity(num_tasks);
            let barrier = Arc::new(AsyncBarrier::new(num_tasks));
            for task_id in 0..num_tasks {
                let barrier_cloned = barrier.clone();
                let tree_cloned = tree.clone();
                task_handles.push(tokio::task::spawn(async move {
                    barrier_cloned.wait().await;
                    let range = (task_id * workload_size)..((task_id + 1) * workload_size);
                    for id in range.clone() {
                        assert!(tree_cloned.insert_async(id, id).await.is_ok());
                        assert!(tree_cloned.insert_async(id, id).await.is_err());
                    }
                    for id in range.clone() {
                        let result = tree_cloned.read(&id, |_, v| *v);
                        assert_eq!(result, Some(id));
                    }
                    for id in range.clone() {
                        assert!(tree_cloned.remove_if_async(&id, |v| *v == id).await);
                    }
                    for id in range {
                        assert!(!tree_cloned.remove_if_async(&id, |v| *v == id).await);
                    }
                }));
            }

            for r in futures::future::join_all(task_handles).await {
                assert!(r.is_ok());
            }
            assert_eq!(tree.len(), 0);
        }
    }

    #[test]
    fn reclaim() {
        static INST_CNT: AtomicUsize = AtomicUsize::new(0);
        struct R(usize);
        impl R {
            fn new() -> R {
                R(INST_CNT.fetch_add(1, Relaxed))
            }
        }
        impl Clone for R {
            fn clone(&self) -> Self {
                INST_CNT.fetch_add(1, Relaxed);
                R(self.0)
            }
        }
        impl Drop for R {
            fn drop(&mut self) {
                INST_CNT.fetch_sub(1, Relaxed);
            }
        }

        let data_size = 65536; // 1_048_576;
        let tree: TreeIndex<usize, R> = TreeIndex::new();
        for k in 0..data_size {
            assert!(tree.insert(k, R::new()).is_ok());
        }
        for k in 0..data_size {
            assert!(tree.remove(&k));
        }
        while INST_CNT.load(Relaxed) > 0 {
            let barrier = ebr::Barrier::new();
            drop(barrier);
        }

        let tree: TreeIndex<usize, R> = TreeIndex::new();
        for k in 0..data_size {
            assert!(tree.insert(k, R::new()).is_ok());
        }
        tree.clear();
        while INST_CNT.load(Relaxed) > 0 {
            let barrier = ebr::Barrier::new();
            drop(barrier);
        }
    }

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

        let barrier = ebr::Barrier::new();
        let scanner = tree.iter(&barrier);
        let mut prev = 0;
        for entry in scanner {
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
            assert!(tree.insert(t * range, t * range).is_ok());
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
                    {
                        let ebr_barrier = ebr::Barrier::new();
                        let mut range_scanner = tree_copied.range(first_key.., &ebr_barrier);
                        let mut entry = range_scanner.next().unwrap();
                        assert_eq!(entry, (&first_key, &first_key));
                        entry = range_scanner.next().unwrap();
                        assert_eq!(entry, (&(first_key + 1), &(first_key + 1)));
                        entry = range_scanner.next().unwrap();
                        assert_eq!(entry, (&(first_key + 2), &(first_key + 2)));
                        entry = range_scanner.next().unwrap();
                        assert_eq!(entry, (&(first_key + 3), &(first_key + 3)));
                    }

                    let key_at_halfway = first_key + range / 2;
                    for key in (first_key + 1)..(first_key + range) {
                        if key == key_at_halfway {
                            let ebr_barrier = ebr::Barrier::new();
                            let mut range_scanner =
                                tree_copied.range((first_key + 1).., &ebr_barrier);
                            let entry = range_scanner.next().unwrap();
                            assert_eq!(entry, (&key_at_halfway, &key_at_halfway));
                            let entry = range_scanner.next().unwrap();
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
        for _ in 0..512 {
            let mut found_0 = false;
            let mut found_markers = 0;
            let mut prev_marker = 0;
            let mut prev = 0;
            let ebr_barrier = ebr::Barrier::new();
            for iter in tree.iter(&ebr_barrier) {
                let current = *iter.0;
                if current % range == 0 {
                    found_markers += 1;
                    if current == 0 {
                        found_0 = true;
                    }
                    if current > 0 {
                        assert_eq!(prev_marker + range, current);
                    }
                    prev_marker = current;
                }
                assert!(prev == 0 || prev < current);
                prev = current;
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
    fn remove() {
        let num_threads = 16;
        let tree: Arc<TreeIndex<usize, usize>> = Arc::new(TreeIndex::new());
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut thread_handles = Vec::with_capacity(num_threads);
        for thread_id in 0..num_threads {
            let tree_copied = tree.clone();
            let barrier_copied = barrier.clone();
            thread_handles.push(thread::spawn(move || {
                barrier_copied.wait();
                for _ in 0..4096 {
                    let range = 0..32;
                    let inserted = range
                        .clone()
                        .filter(|i| tree_copied.insert(*i, thread_id).is_ok())
                        .count();
                    let found = range
                        .clone()
                        .filter(|i| {
                            tree_copied
                                .read(i, |_, v| *v == thread_id)
                                .map_or(false, |t| t)
                        })
                        .count();
                    let removed = range
                        .clone()
                        .filter(|i| tree_copied.remove_if(i, |v| *v == thread_id))
                        .count();
                    let removed_again = range
                        .clone()
                        .filter(|i| tree_copied.remove_if(i, |v| *v == thread_id))
                        .count();
                    assert_eq!(removed_again, 0);
                    assert_eq!(found, removed, "{} {} {}", inserted, found, removed);
                    assert_eq!(inserted, found, "{} {} {}", inserted, found, removed);
                }
            }));
        }
        for handle in thread_handles {
            handle.join().unwrap();
        }
        assert_eq!(tree.len(), 0);
        assert_eq!(tree.depth(), 0);
    }

    #[test]
    fn string_key() {
        let tree1: TreeIndex<String, u32> = TreeIndex::default();
        let tree2: TreeIndex<u32, String> = TreeIndex::default();
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
            let str_borrowed = str_val.as_str();
            assert!(tree1.read(str_borrowed, |_, _| ()).is_some());

            if tree2.insert(i, str_val.clone()).is_ok() {
                checker2.insert((i, str_val.clone()));
            }
        }
        for iter in &checker1 {
            let v = tree1.read(iter.0.as_str(), |_, v| *v);
            assert_eq!(v.unwrap(), iter.1);
        }
        for iter in &checker2 {
            let v = tree2.read(&iter.0, |_, v| v.clone());
            assert_eq!(v.unwrap(), iter.1);
        }
    }

    #[test]
    fn scanner() {
        let data_size = 4096;
        for _ in 0..64 {
            let tree: Arc<TreeIndex<usize, u64>> = Arc::new(TreeIndex::default());
            let barrier = Arc::new(Barrier::new(3));
            let inserted = Arc::new(AtomicUsize::new(0));
            let removed = Arc::new(AtomicUsize::new(data_size));
            let mut thread_handles = Vec::new();
            for _ in 0..2 {
                let tree_copied = tree.clone();
                let barrier_copied = barrier.clone();
                let inserted_copied = inserted.clone();
                let removed_copied = removed.clone();
                let thread_handle = thread::spawn(move || {
                    // test insert
                    for _ in 0..2 {
                        barrier_copied.wait();
                        let max = inserted_copied.load(Acquire);
                        let mut prev = 0;
                        let mut iterated = 0;
                        let ebr_barrier = ebr::Barrier::new();
                        for iter in tree_copied.iter(&ebr_barrier) {
                            assert!(
                                prev == 0
                                    || (*iter.0 <= max && prev + 1 == *iter.0)
                                    || *iter.0 > prev
                            );
                            prev = *iter.0;
                            iterated += 1;
                        }
                        assert!(iterated >= max);
                    }
                    // test remove
                    for _ in 0..2 {
                        barrier_copied.wait();
                        let mut prev = 0;
                        let max = removed_copied.load(Acquire);
                        let ebr_barrier = ebr::Barrier::new();
                        for iter in tree_copied.iter(&ebr_barrier) {
                            let current = *iter.0;
                            assert!(current < max);
                            assert!(prev + 1 == current || prev == 0);
                            prev = current;
                        }
                    }
                });
                thread_handles.push(thread_handle);
            }
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
            thread_handles.into_iter().for_each(|t| t.join().unwrap());
        }
    }
}

#[cfg(test)]
mod hashmap_test_async {
    use crate::awaitable::HashMap;

    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::Arc;

    use tokio::sync::Barrier;

    struct R(&'static AtomicUsize);
    impl R {
        fn new(cnt: &'static AtomicUsize) -> R {
            cnt.fetch_add(1, Relaxed);
            R(cnt)
        }
    }
    impl Drop for R {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Relaxed);
        }
    }

    #[tokio::test]
    async fn insert_drop() {
        static CNT: AtomicUsize = AtomicUsize::new(0);
        let hashmap: HashMap<usize, R> = HashMap::default();

        let workload_size = 1024;
        for k in 0..workload_size {
            assert!(hashmap.insert(k, R::new(&CNT)).await.is_ok());
        }
        assert_eq!(CNT.load(Relaxed), workload_size);
        assert_eq!(hashmap.len(), workload_size);
        drop(hashmap);

        while CNT.load(Relaxed) != 0 {
            drop(crate::ebr::Barrier::new());
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn clear() {
        static CNT: AtomicUsize = AtomicUsize::new(0);
        let hashmap: HashMap<usize, R> = HashMap::default();

        let workload_size = 1024;
        for k in 0..workload_size {
            assert!(hashmap.insert(k, R::new(&CNT)).await.is_ok());
        }
        assert_eq!(CNT.load(Relaxed), workload_size);
        assert_eq!(hashmap.len(), workload_size);
        assert_eq!(hashmap.clear().await, workload_size);

        while CNT.load(Relaxed) != 0 {
            drop(crate::ebr::Barrier::new());
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn integer_key() {
        let hashmap: Arc<HashMap<usize, usize>> = Arc::new(HashMap::default());

        let num_tasks = 8;
        let workload_size = 256;
        let mut task_handles = Vec::with_capacity(num_tasks);
        let barrier = Arc::new(Barrier::new(num_tasks));
        for task_id in 0..num_tasks {
            let barrier_cloned = barrier.clone();
            let hashmap_cloned = hashmap.clone();
            task_handles.push(tokio::task::spawn(async move {
                barrier_cloned.wait().await;
                let range = (task_id * workload_size)..((task_id + 1) * workload_size);
                for id in range.clone() {
                    let result = hashmap_cloned.insert(id, id).await;
                    assert!(result.is_ok());
                }
                for id in range.clone() {
                    let result = hashmap_cloned.read(&id, |_, v| *v).await;
                    assert_eq!(result, Some(id));
                }
                for id in range.clone() {
                    let result = hashmap_cloned.remove_if(&id, |v| *v == id).await;
                    assert_eq!(result, Some((id, id)));
                }
                for id in range {
                    let result = hashmap_cloned.remove_if(&id, |v| *v == id).await;
                    assert_eq!(result, None);
                }
            }));
        }

        for r in futures::future::join_all(task_handles).await {
            assert!(r.is_ok());
        }

        assert_eq!(hashmap.len(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn retain_for_each() {
        let hashmap: Arc<HashMap<usize, usize>> = Arc::new(HashMap::default());

        let num_tasks = 8;
        let workload_size = 256;
        let mut task_handles = Vec::with_capacity(num_tasks);
        let barrier = Arc::new(Barrier::new(num_tasks));
        for task_id in 0..num_tasks {
            let barrier_cloned = barrier.clone();
            let hashmap_cloned = hashmap.clone();
            task_handles.push(tokio::task::spawn(async move {
                barrier_cloned.wait().await;
                let range = (task_id * workload_size)..((task_id + 1) * workload_size);
                for id in range.clone() {
                    let result = hashmap_cloned.insert(id, id).await;
                    assert!(result.is_ok());
                }
                for id in range.clone() {
                    let result = hashmap_cloned.insert(id, id).await;
                    assert_eq!(result, Err((id, id)));
                }
                let mut iterated = 0;
                hashmap_cloned
                    .for_each(|k, _| {
                        if range.contains(k) {
                            iterated += 1;
                        }
                    })
                    .await;
                assert!(iterated >= workload_size);

                let (_, removed) = hashmap_cloned.retain(|k, _| !range.contains(k)).await;
                assert_eq!(removed, workload_size);
            }));
        }

        for r in futures::future::join_all(task_handles).await {
            assert!(r.is_ok());
        }

        assert_eq!(hashmap.len(), 0);
    }
}
