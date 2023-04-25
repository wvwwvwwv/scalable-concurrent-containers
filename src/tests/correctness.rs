#[cfg(test)]
mod hashmap_test {
    use crate::hash_map::Entry;
    use crate::HashMap;
    use crate::{ebr, hash_map};
    use proptest::prelude::*;
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    use std::collections::BTreeSet;
    use std::hash::{Hash, Hasher};
    use std::panic::UnwindSafe;
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    use std::sync::atomic::{AtomicU64, AtomicUsize};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tokio::sync::Barrier as AsyncBarrier;

    static_assertions::assert_impl_all!(HashMap<String, String>: Send, Sync, UnwindSafe);
    static_assertions::assert_not_impl_all!(HashMap<String, *const String>: Send, Sync, UnwindSafe);
    static_assertions::assert_impl_all!(hash_map::OccupiedEntry<String, String>: Send, Sync);
    static_assertions::assert_not_impl_all!(hash_map::OccupiedEntry<String, *const String>: Send, Sync, UnwindSafe);
    static_assertions::assert_impl_all!(hash_map::VacantEntry<String, String>: Send, Sync);
    static_assertions::assert_not_impl_all!(hash_map::VacantEntry<String, *const String>: Send, Sync, UnwindSafe);

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

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn insert_drop() {
        static INST_CNT: AtomicUsize = AtomicUsize::new(0);
        let hashmap: HashMap<usize, R> = HashMap::default();

        let workload_size = 1024;
        for k in 0..workload_size {
            assert!(hashmap.insert_async(k, R::new(&INST_CNT)).await.is_ok());
        }
        assert_eq!(INST_CNT.load(Relaxed), workload_size);
        assert_eq!(hashmap.len(), workload_size);
        drop(hashmap);

        while INST_CNT.load(Relaxed) != 0 {
            drop(ebr::Barrier::new());
            tokio::task::yield_now().await;
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn clear() {
        static INST_CNT: AtomicUsize = AtomicUsize::new(0);
        let hashmap: HashMap<usize, R> = HashMap::default();

        let workload_size = 1_usize << 18;

        for _ in 0..2 {
            for k in 0..workload_size {
                assert!(hashmap.insert_async(k, R::new(&INST_CNT)).await.is_ok());
            }
            assert_eq!(INST_CNT.load(Relaxed), workload_size);
            assert_eq!(hashmap.len(), workload_size);
            assert_eq!(hashmap.clear_async().await, workload_size);
        }

        while INST_CNT.load(Relaxed) != 0 {
            drop(ebr::Barrier::new());
            tokio::task::yield_now().await;
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn clone() {
        static INST_CNT: AtomicUsize = AtomicUsize::new(0);
        let hashmap: HashMap<usize, R> = HashMap::default();

        let workload_size = 1024;

        for k in 0..workload_size {
            assert!(hashmap.insert_async(k, R::new(&INST_CNT)).await.is_ok());
        }
        let hashmap_clone = hashmap.clone();
        hashmap.clear();
        for k in 0..workload_size {
            assert!(hashmap_clone.read(&k, |_, _| ()).is_some());
        }
        hashmap_clone.clear();

        while INST_CNT.load(Relaxed) != 0 {
            drop(ebr::Barrier::new());
            tokio::task::yield_now().await;
        }
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn compare() {
        let hashmap1: HashMap<String, usize> = HashMap::new();
        let hashmap2: HashMap<String, usize> = HashMap::new();
        assert_eq!(hashmap1, hashmap2);

        assert!(hashmap1.insert("Hi".to_string(), 1).is_ok());
        assert_ne!(hashmap1, hashmap2);

        assert!(hashmap2.insert("Hello".to_string(), 2).is_ok());
        assert_ne!(hashmap1, hashmap2);

        assert!(hashmap1.insert("Hello".to_string(), 2).is_ok());
        assert_ne!(hashmap1, hashmap2);

        assert!(hashmap2.insert("Hi".to_string(), 1).is_ok());
        assert_eq!(hashmap1, hashmap2);

        assert!(hashmap1.remove("Hi").is_some());
        assert_ne!(hashmap1, hashmap2);
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn local_ref() {
        struct L<'a>(&'a AtomicUsize);
        impl<'a> L<'a> {
            fn new(cnt: &'a AtomicUsize) -> Self {
                cnt.fetch_add(1, Relaxed);
                L(cnt)
            }
        }
        impl<'a> Drop for L<'a> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Relaxed);
            }
        }

        let workload_size = 65536;
        let cnt = AtomicUsize::new(0);
        let hashmap: HashMap<usize, L> = HashMap::default();

        for k in 0..workload_size {
            assert!(hashmap.insert(k, L::new(&cnt)).is_ok());
        }
        hashmap.for_each(|k, _| assert!(*k < workload_size));
        assert_eq!(cnt.load(Relaxed), workload_size);

        for k in 0..workload_size / 2 {
            assert!(hashmap.remove(&k).is_some());
        }
        hashmap.for_each(|k, _| assert!(*k >= workload_size / 2));
        assert_eq!(cnt.load(Relaxed), workload_size / 2);

        drop(hashmap);
        assert_eq!(cnt.load(Relaxed), 0);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn integer_key() {
        let hashmap: Arc<HashMap<usize, usize>> = Arc::new(HashMap::default());

        let num_tasks = 8;
        let workload_size = 256;
        let mut task_handles = Vec::with_capacity(num_tasks);
        let barrier = Arc::new(AsyncBarrier::new(num_tasks));
        for task_id in 0..num_tasks {
            let barrier_clone = barrier.clone();
            let hashmap_clone = hashmap.clone();
            task_handles.push(tokio::task::spawn(async move {
                barrier_clone.wait().await;
                let range = (task_id * workload_size)..((task_id + 1) * workload_size);
                for id in range.clone() {
                    let result = hashmap_clone.update_async(&id, |_, _| 1).await;
                    assert!(result.is_none());
                }
                for id in range.clone() {
                    if id % 10 == 0 {
                        hashmap_clone.upsert_async(id, || id, |_, v| *v = id).await;
                    } else if id % 5 == 0 {
                        hashmap_clone.upsert(id, || id, |_, v| *v = id);
                    } else if id % 2 == 0 {
                        let result = hashmap_clone.insert_async(id, id).await;
                        assert!(result.is_ok());
                    } else if id % 3 == 0 {
                        let entry = if id % 6 == 0 {
                            hashmap_clone.entry(id)
                        } else {
                            hashmap_clone.entry_async(id).await
                        };
                        let o = match entry {
                            Entry::Occupied(mut o) => {
                                *o.get_mut() = id;
                                o
                            }
                            Entry::Vacant(v) => v.insert_entry(id),
                        };
                        assert_eq!(*o.get(), id);
                    } else {
                        let result = hashmap_clone.insert(id, id);
                        assert!(result.is_ok());
                    }
                }
                for id in range.clone() {
                    if id % 7 == 0 {
                        hashmap_clone
                            .upsert_async(id, || id, |_, v| *v = id + 1)
                            .await;
                    } else if id % 7 == 5 {
                        let entry = hashmap_clone.entry(id);
                        match entry {
                            Entry::Occupied(mut o) => {
                                *o.get_mut() += 1;
                            }
                            Entry::Vacant(v) => {
                                v.insert_entry(id);
                            }
                        }
                    } else {
                        let result = hashmap_clone
                            .update_async(&id, |_, v| {
                                *v += 1;
                                *v
                            })
                            .await;
                        assert_eq!(result, Some(id + 1));
                    }
                }
                for id in range.clone() {
                    let result = hashmap_clone.read_async(&id, |_, v| *v).await;
                    assert_eq!(result, Some(id + 1));
                    let result = hashmap_clone.read(&id, |_, v| *v);
                    assert_eq!(result, Some(id + 1));
                }
                for id in range.clone() {
                    if id % 2 == 0 {
                        let result = hashmap_clone.remove_if_async(&id, |v| *v == id + 1).await;
                        assert_eq!(result, Some((id, id + 1)));
                    } else {
                        let result = hashmap_clone.remove_if(&id, |v| *v == id + 1);
                        assert_eq!(result, Some((id, id + 1)));
                    }
                }
                for id in range {
                    let result = hashmap_clone.remove_if_async(&id, |v| *v == id + 1).await;
                    assert_eq!(result, None);
                }
            }));
        }

        for r in futures::future::join_all(task_handles).await {
            assert!(r.is_ok());
        }

        assert_eq!(hashmap.len(), 0);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn insert_read_remove() {
        let hashmap: Arc<HashMap<usize, usize>> = Arc::new(HashMap::default());
        for _ in 0..256 {
            let num_tasks = 8;
            let workload_size = 256;
            let mut task_handles = Vec::with_capacity(num_tasks);
            let barrier = Arc::new(AsyncBarrier::new(num_tasks));
            for task_id in 0..num_tasks {
                let barrier_clone = barrier.clone();
                let hashmap_clone = hashmap.clone();
                task_handles.push(tokio::task::spawn(async move {
                    barrier_clone.wait().await;
                    let range = (task_id * workload_size)..((task_id + 1) * workload_size);
                    for id in range.clone() {
                        let result = hashmap_clone.insert_async(id, id).await;
                        assert!(result.is_ok());
                    }
                    for id in range.clone() {
                        assert!(hashmap_clone.read_async(&id, |_, _| ()).await.is_some());
                    }
                    for id in range.clone() {
                        assert!(hashmap_clone.remove_async(&id).await.is_some());
                    }
                }));
            }

            for r in futures::future::join_all(task_handles).await {
                assert!(r.is_ok());
            }

            assert_eq!(hashmap.len(), 0);
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn entry_next_retain() {
        let hashmap: Arc<HashMap<usize, usize>> = Arc::new(HashMap::default());
        for _ in 0..256 {
            let num_tasks = 8;
            let workload_size = 256;
            let mut task_handles = Vec::with_capacity(num_tasks);
            let barrier = Arc::new(AsyncBarrier::new(num_tasks));
            for task_id in 0..num_tasks {
                let barrier_clone = barrier.clone();
                let hashmap_clone = hashmap.clone();
                task_handles.push(tokio::task::spawn(async move {
                    barrier_clone.wait().await;
                    let range = (task_id * workload_size)..((task_id + 1) * workload_size);
                    for id in range.clone() {
                        let result = hashmap_clone.insert_async(id, id).await;
                        assert!(result.is_ok());
                    }
                    for id in range.clone() {
                        assert!(hashmap_clone.read_async(&id, |_, _| ()).await.is_some());
                    }

                    let mut call_async = false;
                    let mut in_range = 0;
                    let mut entry = if task_id % 2 == 0 {
                        hashmap_clone.first_occupied_entry()
                    } else {
                        hashmap_clone.first_occupied_entry_async().await
                    };
                    while let Some(current_entry) = entry.take() {
                        if range.contains(current_entry.key()) {
                            in_range += 1;
                        }
                        if call_async {
                            entry = current_entry.next_async().await;
                        } else {
                            entry = current_entry.next();
                        }
                        call_async = !call_async;
                    }
                    assert!(in_range >= workload_size, "{in_range} {workload_size}");

                    let (_, removed) = hashmap_clone.retain_async(|k, _| !range.contains(k)).await;
                    assert_eq!(removed, workload_size);

                    let mut entry = if task_id % 2 == 0 {
                        hashmap_clone.first_occupied_entry()
                    } else {
                        hashmap_clone.first_occupied_entry_async().await
                    };
                    while let Some(current_entry) = entry.take() {
                        assert!(!range.contains(current_entry.key()));
                        if call_async {
                            entry = current_entry.next_async().await;
                        } else {
                            entry = current_entry.next();
                        }
                        call_async = !call_async;
                    }
                }));
            }

            for r in futures::future::join_all(task_handles).await {
                assert!(r.is_ok());
            }

            assert_eq!(hashmap.len(), 0);
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn retain_for_each_any() {
        let hashmap: Arc<HashMap<usize, usize>> = Arc::new(HashMap::default());
        for _ in 0..256 {
            let num_tasks = 8;
            let workload_size = 256;
            let mut task_handles = Vec::with_capacity(num_tasks);
            let barrier = Arc::new(AsyncBarrier::new(num_tasks));
            for task_id in 0..num_tasks {
                let barrier_clone = barrier.clone();
                let hashmap_clone = hashmap.clone();
                task_handles.push(tokio::task::spawn(async move {
                    barrier_clone.wait().await;
                    let range = (task_id * workload_size)..((task_id + 1) * workload_size);
                    for id in range.clone() {
                        let result = hashmap_clone.insert_async(id, id).await;
                        assert!(result.is_ok());
                    }
                    for id in range.clone() {
                        let result = hashmap_clone.insert_async(id, id).await;
                        assert_eq!(result, Err((id, id)));
                    }
                    let mut iterated = 0;
                    hashmap_clone
                        .for_each_async(|k, _| {
                            if range.contains(k) {
                                iterated += 1;
                            }
                        })
                        .await;
                    assert!(iterated >= workload_size);
                    assert!(hashmap_clone.any(|k, _| range.contains(k)));
                    assert!(hashmap_clone.any_async(|k, _| range.contains(k)).await);

                    let (_, removed) = hashmap_clone.retain_async(|k, _| !range.contains(k)).await;
                    assert_eq!(removed, workload_size);

                    assert!(!hashmap_clone.any(|k, _| range.contains(k)));
                    assert!(!hashmap_clone.any_async(|k, _| range.contains(k)).await);
                }));
            }

            for r in futures::future::join_all(task_handles).await {
                assert!(r.is_ok());
            }

            assert_eq!(hashmap.len(), 0);
        }
    }

    #[cfg_attr(miri, ignore)]
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
            let e = hashmap2.entry(iter.0);
            match e {
                Entry::Occupied(o) => assert_eq!(o.remove(), iter.1),
                Entry::Vacant(_) => unreachable!(),
            }
        }
        assert_eq!(hashmap1.len(), 0);
        assert_eq!(hashmap2.len(), 0);
    }

    #[cfg_attr(miri, ignore)]
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

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read() {
        let hashmap: Arc<HashMap<usize, usize>> = Arc::new(HashMap::default());
        let num_tasks = 4;
        let workload_size = 1024 * 1024;

        for k in 0..num_tasks {
            assert!(hashmap.insert(k, k).is_ok());
        }

        let mut task_handles = Vec::with_capacity(num_tasks);
        let barrier = Arc::new(AsyncBarrier::new(num_tasks));
        for task_id in 0..num_tasks {
            let barrier_clone = barrier.clone();
            let hashmap_clone = hashmap.clone();
            task_handles.push(tokio::task::spawn(async move {
                barrier_clone.wait().await;
                if task_id == 0 {
                    for k in num_tasks..workload_size {
                        assert!(hashmap_clone.insert(k, k).is_ok());
                    }
                    for k in num_tasks..workload_size {
                        assert!(hashmap_clone.remove(&k).is_some());
                    }
                } else {
                    for k in 0..num_tasks {
                        assert!(hashmap_clone.read(&k, |_, _| ()).is_some());
                    }
                }
            }));
        }

        for r in futures::future::join_all(task_handles).await {
            assert!(r.is_ok());
        }
    }

    proptest! {
        #[cfg_attr(miri, ignore)]
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
            assert_eq!(result, range);
            assert_eq!(checker.load(Relaxed), 0);

            for d in key..(key + range) {
                assert!(hashmap.insert(Data::new(d, checker.clone()), Data::new(d, checker.clone())).is_ok());
                hashmap.upsert(Data::new(d, checker.clone()), || Data::new(d, checker.clone()), |_, v| *v = Data::new(d + 2, checker.clone()));
            }
            assert_eq!(checker.load(Relaxed), range * 2);
            drop(hashmap);

            while checker.load(Relaxed) != 0 {
                drop(ebr::Barrier::new());
                std::thread::yield_now();
            }
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
    use std::panic::UnwindSafe;
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    use std::sync::atomic::{AtomicU64, AtomicUsize};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tokio::sync::Barrier as AsyncBarrier;

    static_assertions::assert_impl_all!(HashIndex<String, String>: Send, Sync, UnwindSafe);
    static_assertions::assert_not_impl_all!(HashIndex<String, *const String>: Send, Sync, UnwindSafe);

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

    #[cfg_attr(miri, ignore)]
    #[test]
    fn compare() {
        let hashindex1: HashIndex<String, usize> = HashIndex::new();
        let hashindex2: HashIndex<String, usize> = HashIndex::new();
        assert_eq!(hashindex1, hashindex2);

        assert!(hashindex1.insert("Hi".to_string(), 1).is_ok());
        assert_ne!(hashindex1, hashindex2);

        assert!(hashindex2.insert("Hello".to_string(), 2).is_ok());
        assert_ne!(hashindex1, hashindex2);

        assert!(hashindex1.insert("Hello".to_string(), 2).is_ok());
        assert_ne!(hashindex1, hashindex2);

        assert!(hashindex2.insert("Hi".to_string(), 1).is_ok());
        assert_eq!(hashindex1, hashindex2);

        assert!(hashindex1.remove("Hi"));
        assert_ne!(hashindex1, hashindex2);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn clear() {
        static INST_CNT: AtomicUsize = AtomicUsize::new(0);
        let hashindex: HashIndex<usize, R> = HashIndex::default();

        let workload_size = 1_usize << 18;

        for _ in 0..2 {
            for k in 0..workload_size {
                assert!(hashindex.insert_async(k, R::new(&INST_CNT)).await.is_ok());
            }
            assert!(INST_CNT.load(Relaxed) >= workload_size);
            assert_eq!(hashindex.len(), workload_size);
            assert_eq!(hashindex.clear_async().await, workload_size);
        }
        drop(hashindex);

        while INST_CNT.load(Relaxed) != 0 {
            drop(ebr::Barrier::new());
            tokio::task::yield_now().await;
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn clone() {
        static INST_CNT: AtomicUsize = AtomicUsize::new(0);
        let hashindex: HashIndex<usize, R> = HashIndex::default();

        let workload_size = 1024;

        for k in 0..workload_size {
            assert!(hashindex.insert_async(k, R::new(&INST_CNT)).await.is_ok());
        }
        let hashindex_clone = hashindex.clone();
        drop(hashindex);
        for k in 0..workload_size {
            assert!(hashindex_clone.read(&k, |_, _| ()).is_some());
        }
        drop(hashindex_clone);

        while INST_CNT.load(Relaxed) != 0 {
            drop(ebr::Barrier::new());
            tokio::task::yield_now().await;
        }
    }

    #[cfg_attr(miri, ignore)]
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

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read() {
        let hashindex: Arc<HashIndex<usize, usize>> = Arc::new(HashIndex::default());
        let num_tasks = 4;
        let workload_size = 1024 * 1024;

        for k in 0..num_tasks {
            assert!(hashindex.insert(k, k).is_ok());
        }

        let mut task_handles = Vec::with_capacity(num_tasks);
        let barrier = Arc::new(AsyncBarrier::new(num_tasks));
        for task_id in 0..num_tasks {
            let barrier_clone = barrier.clone();
            let hashindex_clone = hashindex.clone();
            task_handles.push(tokio::task::spawn(async move {
                barrier_clone.wait().await;
                if task_id == 0 {
                    for k in num_tasks..workload_size {
                        assert!(hashindex_clone.insert(k, k).is_ok());
                    }
                    for k in num_tasks..workload_size {
                        assert!(hashindex_clone.remove(&k));
                    }
                } else {
                    for k in 0..num_tasks {
                        assert!(hashindex_clone.read(&k, |_, _| ()).is_some());
                    }
                }
            }));
        }

        for r in futures::future::join_all(task_handles).await {
            assert!(r.is_ok());
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rebuild() {
        let hashindex: Arc<HashIndex<usize, usize>> = Arc::new(HashIndex::default());
        let num_tasks = 4;
        let num_iter = 64;
        let workload_size = 256;

        for k in 0..num_tasks * workload_size {
            assert!(hashindex.insert(k, k).is_ok());
        }

        let mut task_handles = Vec::with_capacity(num_tasks);
        let barrier = Arc::new(AsyncBarrier::new(num_tasks));
        for task_id in 0..num_tasks {
            let barrier_clone = barrier.clone();
            let hashindex_clone = hashindex.clone();
            task_handles.push(tokio::task::spawn(async move {
                barrier_clone.wait().await;
                let range = (task_id * workload_size)..((task_id + 1) * workload_size);
                for _ in 0..num_iter {
                    for id in range.clone() {
                        assert!(hashindex_clone.remove_async(&id).await);
                        assert!(hashindex_clone.insert_async(id, id).await.is_ok());
                    }
                }
            }));
        }

        for r in futures::future::join_all(task_handles).await {
            assert!(r.is_ok());
        }

        assert_eq!(hashindex.len(), num_tasks * workload_size);
    }

    #[cfg_attr(miri, ignore)]
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
                assert!(hashindex.read(&i, |_, _| ()).is_none());
                removed.store(i, Release);
            }
            thread_handle.join().unwrap();
        }
    }
}

#[cfg(test)]
mod hashset_test {
    use crate::HashSet;
    use std::panic::UnwindSafe;

    static_assertions::assert_impl_all!(HashSet<String>: Send, Sync, UnwindSafe);
    static_assertions::assert_not_impl_all!(HashSet<*const String>: Send, Sync, UnwindSafe);

    #[cfg_attr(miri, ignore)]
    #[test]
    fn compare() {
        let hashset1: HashSet<String> = HashSet::new();
        let hashset2: HashSet<String> = HashSet::new();
        assert_eq!(hashset1, hashset2);

        assert!(hashset1.insert("Hi".to_string()).is_ok());
        assert_ne!(hashset1, hashset2);

        assert!(hashset2.insert("Hello".to_string()).is_ok());
        assert_ne!(hashset1, hashset2);

        assert!(hashset1.insert("Hello".to_string()).is_ok());
        assert_ne!(hashset1, hashset2);

        assert!(hashset2.insert("Hi".to_string()).is_ok());
        assert_eq!(hashset1, hashset2);

        assert!(hashset1.remove("Hi").is_some());
        assert_ne!(hashset1, hashset2);
    }
}

#[cfg(test)]
mod treeindex_test {
    use crate::ebr;
    use crate::TreeIndex;
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    use std::collections::BTreeSet;
    use std::panic::UnwindSafe;
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tokio::sync::Barrier as AsyncBarrier;

    static_assertions::assert_impl_all!(TreeIndex<String, String>: Send, Sync, UnwindSafe);
    static_assertions::assert_not_impl_all!(TreeIndex<String, *const String>: Send, Sync, UnwindSafe);

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

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn insert_drop() {
        static INST_CNT: AtomicUsize = AtomicUsize::new(0);
        let tree: TreeIndex<usize, R> = TreeIndex::default();

        let workload_size = 1024;
        for k in 0..workload_size {
            assert!(tree.insert_async(k, R::new(&INST_CNT)).await.is_ok());
        }
        assert!(INST_CNT.load(Relaxed) >= workload_size);
        assert_eq!(tree.len(), workload_size);
        drop(tree);

        while INST_CNT.load(Relaxed) != 0 {
            drop(ebr::Barrier::new());
            tokio::task::yield_now().await;
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn insert_remove() {
        static INST_CNT: AtomicUsize = AtomicUsize::new(0);
        let tree: TreeIndex<usize, R> = TreeIndex::default();

        let workload_size = 1024;
        for k in 0..workload_size {
            assert!(tree.insert_async(k, R::new(&INST_CNT)).await.is_ok());
        }
        assert!(INST_CNT.load(Relaxed) >= workload_size);
        assert_eq!(tree.len(), workload_size);
        for k in 0..workload_size {
            assert!(tree.remove_async(&k).await);
        }
        assert_eq!(tree.len(), 0);

        while INST_CNT.load(Relaxed) != 0 {
            drop(ebr::Barrier::new());
            tokio::task::yield_now().await;
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn clear() {
        static INST_CNT: AtomicUsize = AtomicUsize::new(0);
        let tree: TreeIndex<usize, R> = TreeIndex::default();

        let workload_size = 1024;
        for k in 0..workload_size {
            assert!(tree.insert_async(k, R::new(&INST_CNT)).await.is_ok());
        }
        assert!(INST_CNT.load(Relaxed) >= workload_size);
        assert_eq!(tree.len(), workload_size);
        tree.clear();

        while INST_CNT.load(Relaxed) != 0 {
            drop(ebr::Barrier::new());
            tokio::task::yield_now().await;
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn clone() {
        static INST_CNT: AtomicUsize = AtomicUsize::new(0);
        let tree: TreeIndex<usize, R> = TreeIndex::default();

        let workload_size = 1024;
        for k in 0..workload_size {
            assert!(tree.insert_async(k, R::new(&INST_CNT)).await.is_ok());
        }
        let tree_clone = tree.clone();
        tree.clear();
        for k in 0..workload_size {
            assert!(tree_clone.read(&k, |_, _| ()).is_some());
        }
        tree_clone.clear();

        while INST_CNT.load(Relaxed) != 0 {
            drop(ebr::Barrier::new());
            tokio::task::yield_now().await;
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn integer_key() {
        let num_tasks = 8;
        let workload_size = 256;
        for _ in 0..256 {
            let tree: Arc<TreeIndex<usize, usize>> = Arc::new(TreeIndex::default());
            let mut task_handles = Vec::with_capacity(num_tasks);
            let barrier = Arc::new(AsyncBarrier::new(num_tasks));
            for task_id in 0..num_tasks {
                let barrier_clone = barrier.clone();
                let tree_clone = tree.clone();
                task_handles.push(tokio::task::spawn(async move {
                    barrier_clone.wait().await;
                    let range = (task_id * workload_size)..((task_id + 1) * workload_size);
                    for id in range.clone() {
                        assert!(tree_clone.insert_async(id, id).await.is_ok());
                        assert!(tree_clone.insert_async(id, id).await.is_err());
                    }
                    for id in range.clone() {
                        let result = tree_clone.read(&id, |_, v| *v);
                        assert_eq!(result, Some(id));
                    }
                    for id in range.clone() {
                        assert!(tree_clone.remove_if_async(&id, |v| *v == id).await);
                    }
                    for id in range {
                        assert!(!tree_clone.remove_if_async(&id, |v| *v == id).await);
                    }
                }));
            }

            for r in futures::future::join_all(task_handles).await {
                assert!(r.is_ok());
            }
            assert_eq!(tree.len(), 0);
        }
    }

    #[cfg_attr(miri, ignore)]
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

        let data_size = 1_048_576;
        let tree: TreeIndex<usize, R> = TreeIndex::new();
        for k in 0..data_size {
            assert!(tree.insert(k, R::new()).is_ok());
        }
        for k in (0..data_size).rev() {
            assert!(tree.remove(&k));
        }

        let mut cnt = 0;
        while INST_CNT.load(Relaxed) > 0 {
            let barrier = ebr::Barrier::new();
            drop(barrier);
            cnt += 1;
        }
        println!("{cnt}");
        assert!(cnt >= INST_CNT.load(Relaxed));

        let tree: TreeIndex<usize, R> = TreeIndex::new();
        for k in 0..(data_size / 16) {
            assert!(tree.insert(k, R::new()).is_ok());
        }
        tree.clear();

        while INST_CNT.load(Relaxed) > 0 {
            let barrier = ebr::Barrier::new();
            drop(barrier);
        }
    }

    #[cfg_attr(miri, ignore)]
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
                        .read(&key, |key, val| assert_eq!(key, val))
                        .is_some());
                }
                for key in (first_key + range / 2)..(first_key + range) {
                    assert!(tree_copied.insert(key, key).is_ok());
                }
                for key in (first_key + range / 2)..(first_key + range) {
                    assert!(tree_copied
                        .read(&key, |key, val| assert_eq!(key, val))
                        .is_some());
                }
            }));
        }
        for handle in thread_handles {
            handle.join().unwrap();
        }
        let mut found = 0;
        for key in 0..num_threads * range {
            if tree.read(&key, |key, val| assert_eq!(key, val)).is_some() {
                found += 1;
            }
        }
        assert_eq!(found, num_threads * range);
        for key in 0..num_threads * range {
            assert!(tree.read(&key, |key, val| assert_eq!(key, val)).is_some());
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

    #[cfg_attr(miri, ignore)]
    #[test]
    fn compare() {
        let tree1: TreeIndex<String, usize> = TreeIndex::new();
        let tree2: TreeIndex<String, usize> = TreeIndex::new();
        assert_eq!(tree1, tree2);

        assert!(tree1.insert("Hi".to_string(), 1).is_ok());
        assert_ne!(tree1, tree2);

        assert!(tree2.insert("Hello".to_string(), 2).is_ok());
        assert_ne!(tree1, tree2);

        assert!(tree1.insert("Hello".to_string(), 2).is_ok());
        assert_ne!(tree1, tree2);

        assert!(tree2.insert("Hi".to_string(), 1).is_ok());
        assert_eq!(tree1, tree2);

        assert!(tree1.remove("Hi"));
        assert_ne!(tree1, tree2);
    }

    #[cfg_attr(miri, ignore)]
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
                            .read(&key, |key, val| assert_eq!(key, val))
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
                        assert!(tree_copied.read(&key, |_, _| ()).is_none());
                    }
                    for key in (first_key + 1)..(first_key + range) {
                        assert!(tree_copied
                            .read(&key, |key, val| assert_eq!(key, val))
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

    #[cfg_attr(miri, ignore)]
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
                    assert_eq!(found, removed, "{inserted} {found} {removed}");
                    assert_eq!(inserted, found, "{inserted} {found} {removed}");
                }
            }));
        }
        for handle in thread_handles {
            handle.join().unwrap();
        }
        assert_eq!(tree.len(), 0);
        assert_eq!(tree.depth(), 0);
    }

    #[cfg_attr(miri, ignore)]
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

    #[cfg_attr(miri, ignore)]
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
mod bag_test {
    use crate::Bag;
    use std::panic::UnwindSafe;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::Arc;
    use tokio::sync::Barrier as AsyncBarrier;

    static_assertions::assert_impl_all!(Bag<String>: Send, Sync, UnwindSafe);
    static_assertions::assert_not_impl_all!(Bag<*const String>: Send, Sync, UnwindSafe);

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

    #[cfg_attr(miri, ignore)]
    #[test]
    fn reclaim() {
        static INST_CNT: AtomicUsize = AtomicUsize::new(0);
        for workload_size in [2, 18, 32, 40, 120] {
            let bag: Bag<R> = Bag::default();
            for _ in 0..workload_size {
                bag.push(R::new(&INST_CNT));
            }
            assert_eq!(INST_CNT.load(Relaxed), workload_size);

            for _ in 0..workload_size / 2 {
                bag.pop();
            }
            assert_eq!(INST_CNT.load(Relaxed), workload_size / 2);
            drop(bag);
            assert_eq!(INST_CNT.load(Relaxed), 0);
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn mpmc() {
        static INST_CNT: AtomicUsize = AtomicUsize::new(0);
        const NUM_TASKS: usize = 6;
        let workload_size = 256;
        let bag: Arc<Bag<R>> = Arc::new(Bag::default());
        let bag17: Arc<Bag<R, 17>> = Arc::new(Bag::new());
        for _ in 0..256 {
            let mut task_handles = Vec::with_capacity(NUM_TASKS);
            let barrier = Arc::new(AsyncBarrier::new(NUM_TASKS));
            for _ in 0..NUM_TASKS {
                let barrier_clone = barrier.clone();
                let bag_clone = bag.clone();
                let bag17_clone = bag17.clone();
                task_handles.push(tokio::task::spawn(async move {
                    barrier_clone.wait().await;
                    for _ in 0..workload_size {
                        bag_clone.push(R::new(&INST_CNT));
                        bag17_clone.push(R::new(&INST_CNT));
                    }
                    for _ in 0..workload_size {
                        assert!(!bag_clone.is_empty());
                        assert!(bag_clone.pop().is_some());
                        assert!(!bag17_clone.is_empty());
                        assert!(bag17_clone.pop().is_some());
                    }
                }));
            }

            for r in futures::future::join_all(task_handles).await {
                assert!(r.is_ok());
            }
            assert!(bag.pop().is_none());
            assert!(bag.is_empty());
            assert!(bag17.pop().is_none());
            assert!(bag17.is_empty());
        }
        assert_eq!(INST_CNT.load(Relaxed), 0);
    }
}

#[cfg(test)]
mod queue_test {
    use crate::Queue;
    use std::panic::UnwindSafe;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::Arc;
    use tokio::sync::Barrier as AsyncBarrier;

    static_assertions::assert_impl_all!(Queue<String>: Send, Sync, UnwindSafe);
    static_assertions::assert_not_impl_all!(Queue<*const String>: Send, Sync, UnwindSafe);

    struct R(usize, usize);
    impl R {
        fn new(task_id: usize, seq: usize) -> R {
            R(task_id, seq)
        }
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn clone() {
        let queue = Queue::default();
        queue.push(37);
        queue.push(3);
        queue.push(1);

        let queue_clone = queue.clone();

        assert_eq!(queue.pop().map(|e| **e), Some(37));
        assert_eq!(queue.pop().map(|e| **e), Some(3));
        assert_eq!(queue.pop().map(|e| **e), Some(1));
        assert!(queue.pop().is_none());

        assert_eq!(queue_clone.pop().map(|e| **e), Some(37));
        assert_eq!(queue_clone.pop().map(|e| **e), Some(3));
        assert_eq!(queue_clone.pop().map(|e| **e), Some(1));
        assert!(queue_clone.pop().is_none());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn mpmc() {
        const NUM_TASKS: usize = 12;
        const NUM_PRODUCERS: usize = NUM_TASKS / 2;
        let queue: Arc<Queue<R>> = Arc::new(Queue::default());
        let workload_size = 256;
        for _ in 0..16 {
            let num_popped: Arc<AtomicUsize> = Arc::new(AtomicUsize::default());
            let mut task_handles = Vec::with_capacity(NUM_TASKS);
            let barrier = Arc::new(AsyncBarrier::new(NUM_TASKS));
            for task_id in 0..NUM_TASKS {
                let barrier_clone = barrier.clone();
                let queue_clone = queue.clone();
                let num_popped_clone = num_popped.clone();
                task_handles.push(tokio::task::spawn(async move {
                    barrier_clone.wait().await;
                    if task_id < NUM_PRODUCERS {
                        for seq in 1..=workload_size {
                            assert_eq!(queue_clone.push(R::new(task_id, seq)).1, seq);
                        }
                    } else {
                        let mut popped_acc: [usize; NUM_PRODUCERS] = Default::default();
                        loop {
                            let mut cnt = 0;
                            while let Some(popped) = queue_clone.pop() {
                                cnt += 1;
                                assert!(popped_acc[popped.0] < popped.1);
                                popped_acc[popped.0] = popped.1;
                            }
                            if num_popped_clone.fetch_add(cnt, Relaxed) + cnt
                                == workload_size * NUM_PRODUCERS
                            {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    }
                }));
            }

            for r in futures::future::join_all(task_handles).await {
                assert!(r.is_ok());
            }
        }
        assert!(queue.is_empty());
    }
}

#[cfg(test)]
mod stack_test {
    use crate::Stack;
    use std::{panic::UnwindSafe, sync::Arc};
    use tokio::sync::Barrier as AsyncBarrier;

    static_assertions::assert_impl_all!(Stack<String>: Send, Sync, UnwindSafe);
    static_assertions::assert_not_impl_all!(Stack<*const String>: Send, Sync, UnwindSafe);

    #[derive(Debug)]
    struct R(usize, usize);
    impl R {
        fn new(task_id: usize, seq: usize) -> R {
            R(task_id, seq)
        }
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn clone() {
        let stack = Stack::default();
        stack.push(37);
        stack.push(3);
        stack.push(1);

        let stack_clone = stack.clone();

        assert_eq!(stack.pop().map(|e| **e), Some(1));
        assert_eq!(stack.pop().map(|e| **e), Some(3));
        assert_eq!(stack.pop().map(|e| **e), Some(37));
        assert!(stack.pop().is_none());

        assert_eq!(stack_clone.pop().map(|e| **e), Some(1));
        assert_eq!(stack_clone.pop().map(|e| **e), Some(3));
        assert_eq!(stack_clone.pop().map(|e| **e), Some(37));
        assert!(stack_clone.pop().is_none());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn mpmc() {
        const NUM_TASKS: usize = 12;
        let stack: Arc<Stack<R>> = Arc::new(Stack::default());
        let workload_size = 256;
        for _ in 0..16 {
            let mut task_handles = Vec::with_capacity(NUM_TASKS);
            let barrier = Arc::new(AsyncBarrier::new(NUM_TASKS));
            for task_id in 0..NUM_TASKS {
                let barrier_clone = barrier.clone();
                let stack_clone = stack.clone();
                task_handles.push(tokio::task::spawn(async move {
                    barrier_clone.wait().await;
                    for seq in 0..workload_size {
                        assert_eq!(stack_clone.push(R::new(task_id, seq)).1, seq);
                    }
                    let mut last_popped = usize::MAX;
                    let mut cnt = 0;
                    while cnt < workload_size {
                        while let Ok(Some(popped)) = stack_clone.pop_if(|e| e.0 == task_id) {
                            assert_eq!(popped.0, task_id);
                            assert!(last_popped > popped.1);
                            last_popped = popped.1;
                            cnt += 1;
                        }
                        tokio::task::yield_now().await;
                    }
                }));
            }

            for r in futures::future::join_all(task_handles).await {
                assert!(r.is_ok());
            }
        }
        assert!(stack.is_empty());
    }
}

#[cfg(test)]
mod ebr_test {
    use crate::ebr::{suspend, Arc, AtomicArc, Barrier, Ptr, Tag};
    use std::ops::Deref;
    use std::panic::UnwindSafe;
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    static_assertions::assert_impl_all!(Arc<String>: Send, Sync, UnwindSafe);
    static_assertions::assert_impl_all!(AtomicArc<String>: Send, Sync, UnwindSafe);
    static_assertions::assert_impl_all!(Ptr<String>: UnwindSafe);
    static_assertions::assert_not_impl_all!(Arc<*const u8>: Send, Sync, UnwindSafe);
    static_assertions::assert_not_impl_all!(AtomicArc<*const u8>: Send, Sync, UnwindSafe);
    static_assertions::assert_not_impl_all!(Ptr<String>: Send, Sync);
    static_assertions::assert_not_impl_all!(Ptr<*const u8>: Send, Sync, UnwindSafe);
    static_assertions::assert_impl_all!(Barrier: UnwindSafe);
    static_assertions::assert_not_impl_all!(Barrier: Send, Sync);

    struct A(AtomicUsize, usize, &'static AtomicBool);
    impl Drop for A {
        fn drop(&mut self) {
            self.2.swap(true, Relaxed);
        }
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn deferred() {
        static EXECUTED: AtomicBool = AtomicBool::new(false);

        let barrier = Barrier::new();
        barrier.defer_execute(|| EXECUTED.store(true, Relaxed));
        drop(barrier);

        while !EXECUTED.load(Relaxed) {
            drop(Barrier::new());
        }
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn arc() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);

        let mut arc = Arc::new(A(AtomicUsize::new(10), 10, &DESTROYED));
        if let Some(mut_ref) = unsafe { arc.get_mut() } {
            mut_ref.1 += 1;
        }
        arc.0.fetch_add(1, Relaxed);
        assert_eq!(arc.deref().0.load(Relaxed), 11);
        assert_eq!(arc.deref().1, 11);

        let mut arc_clone = arc.clone();
        assert!(unsafe { arc_clone.get_mut().is_none() });
        arc_clone.0.fetch_add(1, Relaxed);
        assert_eq!(arc_clone.deref().0.load(Relaxed), 12);
        assert_eq!(arc_clone.deref().1, 11);

        let mut arc_clone_again = arc_clone.clone();
        assert!(unsafe { arc_clone_again.get_mut().is_none() });
        assert_eq!(arc_clone_again.deref().0.load(Relaxed), 12);
        assert_eq!(arc_clone_again.deref().1, 11);

        drop(arc);
        assert!(!DESTROYED.load(Relaxed));
        assert!(unsafe { arc_clone_again.get_mut().is_none() });

        drop(arc_clone);
        assert!(!DESTROYED.load(Relaxed));
        assert!(unsafe { arc_clone_again.get_mut().is_some() });

        drop(arc_clone_again);
        while !DESTROYED.load(Relaxed) {
            drop(Barrier::new());
        }
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn arc_send() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);

        let arc = Arc::new(A(AtomicUsize::new(14), 14, &DESTROYED));
        let arc_clone = arc.clone();
        let thread = std::thread::spawn(move || {
            assert_eq!(arc_clone.0.load(Relaxed), arc_clone.1);
        });
        assert!(thread.join().is_ok());
        assert_eq!(arc.0.load(Relaxed), arc.1);
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn arc_arc_send() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);

        let arc = Arc::new(A(AtomicUsize::new(14), 14, &DESTROYED));
        let arc_clone = arc.clone();
        let thread = std::thread::spawn(move || {
            assert_eq!(arc_clone.0.load(Relaxed), 14);
            unsafe {
                assert!(!arc_clone.release_drop_in_place());
            }
        });
        assert!(thread.join().is_ok());
        assert_eq!(arc.0.load(Relaxed), 14);

        unsafe {
            assert!(arc.release_drop_in_place());
        }

        assert!(DESTROYED.load(Relaxed));
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn arc_nested() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);

        struct Nest(Arc<A>);

        let nested_arc = Arc::new(Nest(Arc::new(A(AtomicUsize::new(10), 10, &DESTROYED))));
        assert!(!DESTROYED.load(Relaxed));
        drop(nested_arc);

        while !DESTROYED.load(Relaxed) {
            drop(Barrier::new());
        }
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn atomic_arc() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);

        let atomic_arc = AtomicArc::new(A(AtomicUsize::new(10), 10, &DESTROYED));
        assert!(!DESTROYED.load(Relaxed));

        let barrier = Barrier::new();
        let atomic_arc_clone = atomic_arc.clone(Relaxed, &barrier);
        assert_eq!(
            atomic_arc_clone.load(Relaxed, &barrier).as_ref().unwrap().1,
            10
        );

        drop(atomic_arc);
        assert!(!DESTROYED.load(Relaxed));

        atomic_arc_clone.update_tag_if(Tag::Second, |_| true, Relaxed, Relaxed);

        drop(atomic_arc_clone);
        drop(barrier);

        while !DESTROYED.load(Relaxed) {
            drop(Barrier::new());
        }
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn atomic_arc_send() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);

        let atomic_arc = AtomicArc::new(A(AtomicUsize::new(17), 17, &DESTROYED));
        assert!(!DESTROYED.load(Relaxed));

        let thread = std::thread::spawn(move || {
            let barrier = Barrier::new();
            let ptr = atomic_arc.load(Relaxed, &barrier);
            assert_eq!(ptr.as_ref().unwrap().0.load(Relaxed), 17);
        });
        assert!(thread.join().is_ok());

        while !DESTROYED.load(Relaxed) {
            drop(Barrier::new());
        }
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn atomic_arc_creation() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);

        let atomic_arc = AtomicArc::new(A(AtomicUsize::new(11), 11, &DESTROYED));
        assert!(!DESTROYED.load(Relaxed));

        let barrier = Barrier::new();

        let arc = atomic_arc.get_arc(Relaxed, &barrier);

        drop(atomic_arc);
        assert!(!DESTROYED.load(Relaxed));

        if let Some(arc) = arc {
            assert_eq!(arc.1, 11);
            assert!(!DESTROYED.load(Relaxed));
        }
        drop(barrier);

        while !DESTROYED.load(Relaxed) {
            drop(Barrier::new());
        }
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn atomic_arc_conversion() {
        static DESTROYED: AtomicBool = AtomicBool::new(false);

        let atomic_arc = AtomicArc::new(A(AtomicUsize::new(11), 11, &DESTROYED));
        assert!(!DESTROYED.load(Relaxed));

        let barrier = Barrier::new();

        let arc = atomic_arc.try_into_arc(Relaxed);
        assert!(!DESTROYED.load(Relaxed));

        if let Some(arc) = arc {
            assert_eq!(arc.1, 11);
            assert!(!DESTROYED.load(Relaxed));
        }
        drop(barrier);

        while !DESTROYED.load(Relaxed) {
            drop(Barrier::new());
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn atomic_arc_parallel() {
        let atomic_arc: Arc<AtomicArc<String>> =
            Arc::new(AtomicArc::new(String::from("How are you?")));
        let mut task_handles = Vec::new();
        for _ in 0..16 {
            let atomic_arc = atomic_arc.clone();
            task_handles.push(tokio::task::spawn(async move {
                for _ in 0..64 {
                    let barrier = Barrier::new();
                    let mut ptr = atomic_arc.load(Acquire, &barrier);
                    assert!(ptr.tag() == Tag::None || ptr.tag() == Tag::Second);
                    if let Some(str_ref) = ptr.as_ref() {
                        assert!(str_ref == "How are you?" || str_ref == "How can I help you?");
                    }
                    let converted: Result<Arc<String>, _> = Arc::try_from(ptr);
                    if let Ok(arc) = converted {
                        assert!(*arc == "How are you?" || *arc == "How can I help you?");
                    }
                    while let Err((passed, current)) = atomic_arc.compare_exchange(
                        ptr,
                        (
                            Some(Arc::new(String::from("How can I help you?"))),
                            Tag::Second,
                        ),
                        Release,
                        Relaxed,
                        &barrier,
                    ) {
                        if let Some(arc) = passed {
                            assert!(*arc == "How can I help you?");
                        }
                        ptr = current;
                        if let Some(str_ref) = ptr.as_ref() {
                            assert!(str_ref == "How are you?" || str_ref == "How can I help you?");
                        }
                        assert!(ptr.tag() == Tag::None || ptr.tag() == Tag::Second);
                    }
                    assert!(!suspend());
                    drop(barrier);

                    assert!(suspend());

                    atomic_arc.update_tag_if(Tag::None, |_| true, Relaxed, Relaxed);

                    let barrier = Barrier::new();
                    ptr = atomic_arc.load(Acquire, &barrier);
                    assert!(ptr.tag() == Tag::None || ptr.tag() == Tag::Second);
                    if let Some(str_ref) = ptr.as_ref() {
                        assert!(str_ref == "How are you?" || str_ref == "How can I help you?");
                    }
                    drop(barrier);

                    let (old, _) = atomic_arc.swap(
                        (Some(Arc::new(String::from("How are you?"))), Tag::Second),
                        Release,
                    );
                    if let Some(arc) = old {
                        assert!(*arc == "How are you?" || *arc == "How can I help you?");
                    }
                }
            }));
        }
        for r in futures::future::join_all(task_handles).await {
            assert!(r.is_ok());
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn atomic_arc_clone() {
        let atomic_arc: Arc<AtomicArc<String>> =
            Arc::new(AtomicArc::new(String::from("How are you?")));
        let mut task_handles = Vec::new();
        for t in 0..4 {
            let atomic_arc = atomic_arc.clone();
            task_handles.push(tokio::task::spawn(async move {
                for i in 0..256 {
                    if t == 0 {
                        let tag = if i % 3 == 0 {
                            Tag::First
                        } else if i % 2 == 0 {
                            Tag::Second
                        } else {
                            Tag::None
                        };
                        let (old, _) = atomic_arc
                            .swap((Some(Arc::new(String::from("How are you?"))), tag), Release);
                        assert!(old.is_some());
                        if let Some(arc) = old {
                            assert!(*arc == "How are you?");
                        }
                    } else {
                        let (arc_clone, _) = (*atomic_arc)
                            .clone(Acquire, &Barrier::new())
                            .swap((None, Tag::First), Release);
                        assert!(arc_clone.is_some());
                        if let Some(arc) = arc_clone {
                            assert!(*arc == "How are you?");
                        }
                        let arc_clone = atomic_arc.get_arc(Acquire, &Barrier::new());
                        assert!(arc_clone.is_some());
                        if let Some(arc) = arc_clone {
                            assert!(*arc == "How are you?");
                        }
                    }
                }
            }));
        }
        for r in futures::future::join_all(task_handles).await {
            assert!(r.is_ok());
        }
    }
}

#[cfg(test)]
mod random_failure_test {
    use crate::ebr;
    use crate::ebr::Arc;
    use crate::{HashIndex, HashMap, TreeIndex};
    use std::any::Any;
    use std::panic::catch_unwind;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::{AcqRel, Relaxed};

    struct R(&'static AtomicUsize, bool);
    impl R {
        fn new(cnt: &'static AtomicUsize) -> R {
            assert!(rand::random::<u8>() % 4 != 0);
            cnt.fetch_add(1, AcqRel);
            R(cnt, false)
        }
        fn new_panic_free_drop(cnt: &'static AtomicUsize) -> R {
            cnt.fetch_add(1, AcqRel);
            R(cnt, true)
        }
    }
    impl Clone for R {
        fn clone(&self) -> Self {
            assert!(rand::random::<u8>() % 8 != 0);
            self.0.fetch_add(1, AcqRel);
            Self(self.0, self.1)
        }
    }
    impl Drop for R {
        fn drop(&mut self) {
            self.0.fetch_sub(1, AcqRel);
            assert!(self.1 || rand::random::<u8>() % 16 != 0);
        }
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn panic_safety() {
        static INST_CNT: AtomicUsize = AtomicUsize::new(0);

        let workload_size = u8::MAX;

        // EBR.
        for _ in 0..workload_size {
            let _: Result<(), Box<dyn Any + Send>> = catch_unwind(|| {
                let r = Arc::new(R::new(&INST_CNT));
                assert_ne!(INST_CNT.load(Relaxed), 0);
                drop(r);
            });
        }
        while INST_CNT.load(Relaxed) != 0 {
            let _: Result<(), Box<dyn Any + Send>> = catch_unwind(|| {
                drop(ebr::Barrier::new());
            });
            std::thread::yield_now();
        }

        // HashMap.
        let hashmap: HashMap<usize, R> = HashMap::default();
        for k in 0..workload_size {
            let _result: Result<(), Box<dyn Any + Send>> = catch_unwind(|| {
                hashmap.upsert(
                    k as usize,
                    || {
                        let mut r = R::new(&INST_CNT);
                        r.1 = true;
                        r
                    },
                    |_, _| (),
                );
                for _ in 0..workload_size {
                    assert!(hashmap.read(&(k as usize), |_, _| ()).is_some());
                }
            });
        }
        drop(hashmap);

        while INST_CNT.load(Relaxed) != 0 {
            let _: Result<(), Box<dyn Any + Send>> = catch_unwind(|| {
                drop(ebr::Barrier::new());
            });
            std::thread::yield_now();
        }

        // HashIndex.
        let hashmap: HashIndex<usize, R> = HashIndex::default();
        for k in 0..workload_size {
            let _result: Result<(), Box<dyn Any + Send>> = catch_unwind(|| {
                assert!(hashmap
                    .insert(k as usize, R::new_panic_free_drop(&INST_CNT))
                    .is_ok());
                for _ in 0..workload_size {
                    assert!(hashmap.read(&(k as usize), |_, _| ()).is_some());
                }
            });
        }
        drop(hashmap);

        while INST_CNT.load(Relaxed) != 0 {
            let _: Result<(), Box<dyn Any + Send>> = catch_unwind(|| {
                drop(ebr::Barrier::new());
            });
            std::thread::yield_now();
        }

        // TreeIndex.
        let treeindex: TreeIndex<usize, R> = TreeIndex::default();
        for k in 0..14 * 14 * 14 {
            let _result: Result<(), Box<dyn Any + Send>> = catch_unwind(|| {
                assert!(treeindex
                    .insert(k, R::new_panic_free_drop(&INST_CNT))
                    .is_ok());
                assert!(treeindex.read(&k, |_, _| ()).is_some());
            });
        }
        drop(treeindex);

        while INST_CNT.load(Relaxed) != 0 {
            let _: Result<(), Box<dyn Any + Send>> = catch_unwind(|| {
                drop(ebr::Barrier::new());
            });
            std::thread::yield_now();
        }
    }
}

#[cfg(feature = "serde")]
#[cfg(test)]
mod serde_test {
    use crate::{HashIndex, HashMap, HashSet, TreeIndex};

    use serde_test::{assert_tokens, Token};

    #[test]
    fn hashmap() {
        let hashmap: HashMap<u64, i16> = HashMap::new();
        assert!(hashmap.insert(2, -6).is_ok());
        assert_tokens(
            &hashmap,
            &[
                Token::Map { len: Some(1) },
                Token::U64(2),
                Token::I16(-6),
                Token::MapEnd,
            ],
        );
    }

    #[test]
    fn hashset() {
        let hashset: HashSet<u64> = HashSet::new();
        assert!(hashset.insert(2).is_ok());
        assert_tokens(
            &hashset,
            &[Token::Seq { len: Some(1) }, Token::U64(2), Token::SeqEnd],
        );
    }

    #[test]
    fn hashindex() {
        let hashindex: HashIndex<u64, i16> = HashIndex::new();
        assert!(hashindex.insert(2, -6).is_ok());
        assert_tokens(
            &hashindex,
            &[
                Token::Map { len: Some(1) },
                Token::U64(2),
                Token::I16(-6),
                Token::MapEnd,
            ],
        );
    }

    #[test]
    fn treeindex() {
        let treeindex: TreeIndex<u64, i16> = TreeIndex::new();
        assert!(treeindex.insert(4, -4).is_ok());
        assert!(treeindex.insert(2, -6).is_ok());
        assert!(treeindex.insert(3, -5).is_ok());
        assert_tokens(
            &treeindex,
            &[
                Token::Map { len: Some(3) },
                Token::U64(2),
                Token::I16(-6),
                Token::U64(3),
                Token::I16(-5),
                Token::U64(4),
                Token::I16(-4),
                Token::MapEnd,
            ],
        );
    }
}
