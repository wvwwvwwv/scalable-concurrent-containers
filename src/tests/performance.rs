#![allow(clippy::inline_always)]
#![allow(clippy::needless_pass_by_value)]

#[cfg(test)]
mod benchmark {
    use crate::ebr;
    use crate::{HashIndex, HashMap, TreeIndex};

    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hash};
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};

    #[derive(Clone)]
    struct Workload {
        size: usize,
        insert_local: usize,
        insert_remote: usize,
        scan: usize,
        read_local: usize,
        read_remote: usize,
        remove_local: usize,
        remove_remote: usize,
    }

    impl Workload {
        pub fn max_per_op_size(&self) -> usize {
            self.insert_local.max(
                self.insert_remote.max(
                    self.read_local.max(
                        self.read_remote
                            .max(self.remove_local.max(self.remove_remote)),
                    ),
                ),
            )
        }
        pub fn has_remote_op(&self) -> bool {
            self.insert_remote > 0 || self.read_remote > 0 || self.remove_remote > 0
        }
    }

    trait BenchmarkOperation<
        K: Clone + Eq + Hash + Ord + Send + Sync,
        V: Clone + Send + Sync + Unpin,
        H: BuildHasher,
    >
    {
        fn insert_test(&self, k: K, v: V) -> bool;
        fn read_test(&self, k: &K) -> bool;
        fn scan_test(&self) -> usize;
        fn remove_test(&self, k: &K) -> bool;
    }

    impl<
            K: Clone + Eq + Hash + Ord + Send + Sync,
            V: Clone + Send + Sync + Unpin,
            H: BuildHasher,
        > BenchmarkOperation<K, V, H> for HashMap<K, V, H>
    {
        #[inline(always)]
        fn insert_test(&self, k: K, v: V) -> bool {
            self.insert(k, v).is_ok()
        }
        #[inline(always)]
        fn read_test(&self, k: &K) -> bool {
            self.read(k, |_, _| ()).is_some()
        }
        #[inline(always)]
        fn scan_test(&self) -> usize {
            let mut scanned = 0;
            self.for_each(|_, _| scanned += 1);
            scanned
        }

        #[inline(always)]
        fn remove_test(&self, k: &K) -> bool {
            self.remove(k).is_some()
        }
    }

    impl<
            K: Clone + Eq + Hash + Ord + Send + Sync,
            V: Clone + Send + Sync + Unpin,
            H: 'static + BuildHasher,
        > BenchmarkOperation<K, V, H> for HashIndex<K, V, H>
    {
        #[inline(always)]
        fn insert_test(&self, k: K, v: V) -> bool {
            self.insert(k, v).is_ok()
        }
        #[inline(always)]
        fn read_test(&self, k: &K) -> bool {
            self.read(k, |_, _| ()).is_some()
        }
        #[inline(always)]
        fn scan_test(&self) -> usize {
            let barrier = ebr::Barrier::new();
            self.iter(&barrier).count()
        }
        #[inline(always)]
        fn remove_test(&self, k: &K) -> bool {
            self.remove(k)
        }
    }

    impl<
            K: Clone + Eq + Hash + Ord + Send + Sync,
            V: Clone + Send + Sync + Unpin,
            H: BuildHasher,
        > BenchmarkOperation<K, V, H> for TreeIndex<K, V>
    {
        #[inline(always)]
        fn insert_test(&self, k: K, v: V) -> bool {
            self.insert(k, v).is_ok()
        }
        #[inline(always)]
        fn read_test(&self, k: &K) -> bool {
            self.read(k, |_, _| ()).is_some()
        }
        #[inline(always)]
        fn scan_test(&self) -> usize {
            let ebr_barrier = ebr::Barrier::new();
            self.iter(&ebr_barrier).count()
        }
        #[inline(always)]
        fn remove_test(&self, k: &K) -> bool {
            self.remove(k)
        }
    }

    trait ConvertFromUsize {
        fn convert(from: usize) -> Self;
    }

    impl ConvertFromUsize for usize {
        #[inline(always)]
        fn convert(from: usize) -> usize {
            from
        }
    }

    impl ConvertFromUsize for String {
        #[inline(always)]
        fn convert(from: usize) -> String {
            from.to_string()
        }
    }

    fn perform<
        K: Clone + ConvertFromUsize + Eq + Hash + Ord + Send + Sync,
        V: Clone + ConvertFromUsize + Send + Sync + Unpin,
        C: BenchmarkOperation<K, V, RandomState> + 'static + Send + Sync,
    >(
        num_threads: usize,
        start_index: usize,
        container: Arc<C>,
        workload: Workload,
    ) -> (Duration, usize) {
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let total_num_operations = Arc::new(AtomicUsize::new(0));
        let mut thread_handles = Vec::with_capacity(num_threads);
        for thread_id in 0..num_threads {
            let container_copied = container.clone();
            let barrier_copied = barrier.clone();
            let total_num_operations_copied = total_num_operations.clone();
            let workload_copied = workload.clone();
            thread_handles.push(thread::spawn(move || {
                let mut num_operations = 0;
                let per_op_workload_size = workload_copied.max_per_op_size();
                let per_thread_workload_size = workload_copied.size * per_op_workload_size;
                barrier_copied.wait();
                for _ in 0..workload_copied.scan {
                    num_operations += container_copied.scan_test();
                }
                for i in 0..per_thread_workload_size {
                    let remote_thread_id = if num_threads < 2 {
                        0
                    } else {
                        (thread_id + 1 + i % (num_threads - 1)) % num_threads
                    };
                    assert!(num_threads < 2 || thread_id != remote_thread_id);
                    for j in 0..workload_copied.insert_local {
                        let local_index = thread_id * per_thread_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        let result =
                            container_copied.insert_test(K::convert(local_index), V::convert(i));
                        assert!(result || workload_copied.has_remote_op());
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.insert_remote {
                        let remote_index = remote_thread_id * per_thread_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        container_copied.insert_test(K::convert(remote_index), V::convert(i));
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.read_local {
                        let local_index = thread_id * per_thread_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        let result = container_copied.read_test(&K::convert(local_index));
                        assert!(result || workload_copied.has_remote_op());
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.read_remote {
                        let remote_index = remote_thread_id * per_thread_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        container_copied.read_test(&K::convert(remote_index));
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.remove_local {
                        let local_index = thread_id * per_thread_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        let result = container_copied.remove_test(&K::convert(local_index));
                        assert!(result || workload_copied.has_remote_op());
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.remove_remote {
                        let remote_index = remote_thread_id * per_thread_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        container_copied.remove_test(&K::convert(remote_index));
                        num_operations += 1;
                    }
                }
                barrier_copied.wait();
                total_num_operations_copied.fetch_add(num_operations, Relaxed);
            }));
        }
        barrier.wait();
        let start_time = Instant::now();
        barrier.wait();
        let end_time = Instant::now();
        for handle in thread_handles {
            handle.join().unwrap();
        }
        (
            end_time.saturating_duration_since(start_time),
            total_num_operations.load(Relaxed),
        )
    }

    #[allow(clippy::too_many_lines)]
    fn hashmap_benchmark<
        T: 'static + ConvertFromUsize + Clone + Eq + Hash + Ord + Send + Sync + Unpin,
    >(
        workload_size: usize,
        num_threads: Vec<usize>,
    ) {
        for num_threads in num_threads {
            let hashmap: Arc<HashMap<usize, usize, RandomState>> = Arc::new(HashMap::default());

            // 1. insert-local
            let insert = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 0,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashmap.clone(), insert.clone());
            println!(
                "hashmap-insert-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashmap.len(), workload_size * num_threads);

            // 2. scan
            let scan = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 1,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashmap.clone(), scan.clone());
            println!(
                "hashmap-scan: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );

            // 3. read-local
            let read = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 0,
                read_local: 1,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashmap.clone(), read.clone());
            println!(
                "hashmap-read-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );

            // 4. remove-local
            let remove = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 1,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashmap.clone(), remove.clone());
            println!(
                "hashmap-remove-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashmap.len(), 0);

            // 5. insert-local-remote
            let insert = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 1,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashmap.clone(), insert.clone());
            println!(
                "hashmap-insert-local-remote: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashmap.len(), workload_size * num_threads);

            // 6. mixed
            let mixed = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 1,
                scan: 0,
                read_local: 1,
                read_remote: 1,
                remove_local: 1,
                remove_remote: 1,
            };
            let (duration, total_num_operations) = perform(
                num_threads,
                workload_size * num_threads,
                hashmap.clone(),
                mixed.clone(),
            );
            println!(
                "hashmap-mixed: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashmap.len(), workload_size * num_threads);

            // 7. remove-local-remote
            let remove = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 1,
                remove_remote: 1,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashmap.clone(), remove.clone());
            println!(
                "hashmap-remove-local-remote: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashmap.len(), 0);
        }
    }

    #[allow(clippy::too_many_lines)]
    fn hashindex_benchmark<
        T: 'static + ConvertFromUsize + Clone + Eq + Hash + Ord + Send + Sync + Unpin,
    >(
        workload_size: usize,
        num_threads: Vec<usize>,
    ) {
        for num_threads in num_threads {
            let hashindex: Arc<HashIndex<T, T, RandomState>> = Arc::new(HashIndex::default());

            // 1. insert-local
            let insert = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 0,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashindex.clone(), insert.clone());
            println!(
                "hashindex-insert-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashindex.len(), workload_size * num_threads);

            // 2. scan
            let scan = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 1,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashindex.clone(), scan.clone());
            println!(
                "hashindex-scan: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );

            // 3. read-local
            let read = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 0,
                read_local: 1,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashindex.clone(), read.clone());
            println!(
                "hashindex-read-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );

            // 4. remove-local
            let remove = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 1,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashindex.clone(), remove.clone());
            println!(
                "hashindex-remove-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashindex.len(), 0);

            // 5. insert-local-remote
            let insert = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 1,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashindex.clone(), insert.clone());
            println!(
                "hashindex-insert-local-remote: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashindex.len(), workload_size * num_threads);

            // 6. mixed
            let mixed = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 1,
                scan: 0,
                read_local: 1,
                read_remote: 1,
                remove_local: 1,
                remove_remote: 1,
            };
            let (duration, total_num_operations) = perform(
                num_threads,
                workload_size * num_threads,
                hashindex.clone(),
                mixed.clone(),
            );
            println!(
                "hashindex-mixed: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashindex.len(), workload_size * num_threads);

            // 7. remove-local-remote
            let remove = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 1,
                remove_remote: 1,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashindex.clone(), remove.clone());
            println!(
                "hashindex-remove-local-remote: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashindex.len(), 0);
        }
    }

    #[allow(clippy::too_many_lines)]
    fn treeindex_benchmark<
        T: 'static + ConvertFromUsize + Clone + Hash + Ord + Send + Sync + Unpin,
    >(
        workload_size: usize,
        num_threads: Vec<usize>,
    ) {
        for num_threads in num_threads {
            let treeindex: Arc<TreeIndex<T, T>> = Arc::new(TreeIndex::default());

            // 1. insert-local
            let insert = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 0,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, treeindex.clone(), insert.clone());
            println!(
                "treeindex-insert-local: {}, {:?}, {}, depth = {}",
                num_threads,
                duration,
                total_num_operations,
                treeindex.depth()
            );

            // 2. scan
            let scan = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 1,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, treeindex.clone(), scan.clone());
            println!(
                "treeindex-scan: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );

            // 3. read-local
            let read = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 0,
                read_local: 1,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, treeindex.clone(), read.clone());
            println!(
                "treeindex-read-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );

            // 4. remove-local
            let remove = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 1,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, treeindex.clone(), remove.clone());
            println!(
                "treeindex-remove-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(treeindex.len(), 0);

            // 5. insert-local-remote
            let insert = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 1,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, treeindex.clone(), insert.clone());
            println!(
                "treeindex-insert-local-remote: {}, {:?}, {}, depth = {}",
                num_threads,
                duration,
                total_num_operations,
                treeindex.depth()
            );

            // 6. mixed
            let mixed = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 1,
                scan: 0,
                read_local: 1,
                read_remote: 1,
                remove_local: 1,
                remove_remote: 1,
            };
            let (duration, total_num_operations) = perform(
                num_threads,
                treeindex.len(),
                treeindex.clone(),
                mixed.clone(),
            );
            println!(
                "treeindex-mixed: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );

            // 7. remove-local-remote
            let remove = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 1,
                remove_remote: 1,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, treeindex.clone(), remove.clone());
            println!(
                "treeindex-remove-local-remote: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
        }
    }

    #[test]
    fn hashmap_benchmarks() {
        hashmap_benchmark::<String>(16384, vec![1, 2, 4]);
        hashmap_benchmark::<usize>(65536, vec![1, 2, 4]);
    }

    #[test]
    fn hashindex_benchmarks() {
        hashindex_benchmark::<String>(16384, vec![1, 2, 4]);
        hashindex_benchmark::<usize>(65536, vec![1, 2, 4]);
    }

    #[test]
    fn treeindex_benchmarks() {
        treeindex_benchmark::<String>(16384, vec![1, 2, 4]);
        treeindex_benchmark::<usize>(65536, vec![1, 2, 4]);
    }

    #[ignore]
    #[test]
    fn full_scale_benchmarks() {
        hashmap_benchmark::<usize>(1024 * 1024 * 128, vec![11, 11, 11, 22, 22, 22, 44, 44, 44]);
        println!("----");
        hashindex_benchmark::<usize>(1024 * 1024 * 4, vec![11, 11, 11, 22, 22, 22, 44, 44, 44]);
        println!("----");
        treeindex_benchmark::<usize>(1024 * 1024 * 4, vec![11, 11, 11, 22, 22, 22, 44, 44, 44]);
        println!("----");
    }
}

#[cfg(test)]
mod benchmark_async {
    use crate::HashMap;

    use std::collections::hash_map::RandomState;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tokio::sync::Barrier;

    #[derive(Clone)]
    struct Workload {
        size: usize,
        insert_local: usize,
        insert_remote: usize,
        scan: usize,
        read_local: usize,
        read_remote: usize,
        remove_local: usize,
        remove_remote: usize,
    }

    impl Workload {
        pub fn max_per_op_size(&self) -> usize {
            self.insert_local.max(
                self.insert_remote.max(
                    self.read_local.max(
                        self.read_remote
                            .max(self.remove_local.max(self.remove_remote)),
                    ),
                ),
            )
        }
        pub fn has_remote_op(&self) -> bool {
            self.insert_remote > 0 || self.read_remote > 0 || self.remove_remote > 0
        }
    }

    async fn perform(
        num_tasks: usize,
        start_index: usize,
        hashmap: Arc<HashMap<usize, usize, RandomState>>,
        workload: Workload,
    ) -> (Duration, usize) {
        let total_num_operations = Arc::new(AtomicUsize::new(0));
        let mut task_handles = Vec::with_capacity(num_tasks);
        let barrier = Arc::new(Barrier::new(num_tasks + 1));
        for task_id in 0..num_tasks {
            let barrier_cloned = barrier.clone();
            let hashmap_cloned = hashmap.clone();
            let workload_cloned = Arc::new(workload.clone());
            let total_num_operations_cloned = total_num_operations.clone();
            task_handles.push(tokio::task::spawn(async move {
                let mut num_operations = 0;
                let per_op_workload_size = workload_cloned.max_per_op_size();
                let per_task_workload_size = workload_cloned.size * per_op_workload_size;
                barrier_cloned.wait().await;
                for _ in 0..workload_cloned.scan {
                    hashmap_cloned
                        .for_each_async(|_, _| num_operations += 1)
                        .await;
                }
                for i in 0..per_task_workload_size {
                    let remote_task_id = if num_tasks < 2 {
                        0
                    } else {
                        (task_id + 1 + i % (num_tasks - 1)) % num_tasks
                    };
                    assert!(num_tasks < 2 || task_id != remote_task_id);
                    for j in 0..workload_cloned.insert_local {
                        let local_index = task_id * per_task_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        let result = hashmap_cloned.insert_async(local_index, i).await;
                        assert!(result.is_ok() || workload_cloned.has_remote_op());
                        num_operations += 1;
                    }
                    for j in 0..workload_cloned.insert_remote {
                        let remote_index = remote_task_id * per_task_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        let _result = hashmap_cloned.insert_async(remote_index, i).await;
                        num_operations += 1;
                    }
                    for j in 0..workload_cloned.read_local {
                        let local_index = task_id * per_task_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        let result = hashmap_cloned.read_async(&local_index, |_, _| ()).await;
                        assert!(result.is_some() || workload_cloned.has_remote_op());
                        num_operations += 1;
                    }
                    for j in 0..workload_cloned.read_remote {
                        let remote_index = remote_task_id * per_task_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        let _result = hashmap_cloned.read_async(&remote_index, |_, _| ()).await;
                        num_operations += 1;
                    }
                    for j in 0..workload_cloned.remove_local {
                        let local_index = task_id * per_task_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        let result = hashmap_cloned.remove_async(&local_index).await;
                        assert!(result.is_some() || workload_cloned.has_remote_op());
                        num_operations += 1;
                    }
                    for j in 0..workload_cloned.remove_remote {
                        let remote_index = remote_task_id * per_task_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        let _result = hashmap_cloned.remove_async(&remote_index).await;
                        num_operations += 1;
                    }
                }
                barrier_cloned.wait().await;
                total_num_operations_cloned.fetch_add(num_operations, Relaxed);
            }));
        }

        barrier.wait().await;
        let start_time = Instant::now();
        barrier.wait().await;
        let end_time = Instant::now();

        for r in futures::future::join_all(task_handles).await {
            assert!(r.is_ok());
        }

        (
            end_time.saturating_duration_since(start_time),
            total_num_operations.load(Relaxed),
        )
    }

    #[allow(clippy::too_many_lines)]
    async fn hashmap_benchmark(workload_size: usize, num_tasks_list: Vec<usize>) {
        for num_tasks in num_tasks_list {
            let hashmap: Arc<HashMap<usize, usize, RandomState>> = Arc::new(HashMap::default());

            // 1. insert-local
            let insert = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 0,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_tasks, 0, hashmap.clone(), insert.clone()).await;
            println!(
                "hashmap-insert-local: {}, {:?}, {}",
                num_tasks, duration, total_num_operations
            );
            assert_eq!(hashmap.len(), workload_size * num_tasks);

            // 2. scan
            let scan = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 1,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_tasks, 0, hashmap.clone(), scan.clone()).await;
            println!(
                "hashmap-scan: {}, {:?}, {}",
                num_tasks, duration, total_num_operations
            );

            // 3. read-local
            let read = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 0,
                read_local: 1,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_tasks, 0, hashmap.clone(), read.clone()).await;
            println!(
                "hashmap-read-local: {}, {:?}, {}",
                num_tasks, duration, total_num_operations
            );

            // 4. remove-local
            let remove = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 1,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_tasks, 0, hashmap.clone(), remove.clone()).await;
            println!(
                "hashmap-remove-local: {}, {:?}, {}",
                num_tasks, duration, total_num_operations
            );
            assert_eq!(hashmap.len(), 0);

            // 5. insert-local-remote
            let insert = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 1,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_tasks, 0, hashmap.clone(), insert.clone()).await;
            println!(
                "hashmap-insert-local-remote: {}, {:?}, {}",
                num_tasks, duration, total_num_operations
            );
            assert_eq!(hashmap.len(), workload_size * num_tasks);

            // 6. mixed
            let mixed = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 1,
                scan: 0,
                read_local: 1,
                read_remote: 1,
                remove_local: 1,
                remove_remote: 1,
            };
            let (duration, total_num_operations) = perform(
                num_tasks,
                workload_size * num_tasks,
                hashmap.clone(),
                mixed.clone(),
            )
            .await;
            println!(
                "hashmap-mixed: {}, {:?}, {}",
                num_tasks, duration, total_num_operations
            );
            assert_eq!(hashmap.len(), workload_size * num_tasks);

            // 7. remove-local-remote
            let remove = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 1,
                remove_remote: 1,
            };
            let (duration, total_num_operations) =
                perform(num_tasks, 0, hashmap.clone(), remove.clone()).await;
            println!(
                "hashmap-remove-local-remote: {}, {:?}, {}",
                num_tasks, duration, total_num_operations
            );
            assert_eq!(hashmap.len(), 0);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn hashmap_benchmarks() {
        hashmap_benchmark(16384, vec![1, 2, 4]).await;
        hashmap_benchmark(65536, vec![1, 2, 4]).await;
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 88)]
    async fn async_full_scale_benchmarks() {
        hashmap_benchmark(1024 * 1024 * 128, vec![11, 11, 11, 22, 22, 22, 44, 44, 44]).await;
    }
}
