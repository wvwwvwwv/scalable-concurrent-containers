#[cfg(test)]
mod benchmark {
    use scc::{HashIndex, HashMap, TreeIndex};
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
        fn remove_test(&self, k: &K) -> bool {
            self.remove(k).is_some()
        }
    }

    impl<
            K: Clone + Eq + Hash + Ord + Send + Sync,
            V: Clone + Send + Sync + Unpin,
            H: BuildHasher,
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
            String::from(from.to_string())
        }
    }

    fn perform<
        K: Clone + ConvertFromUsize + Eq + Hash + Ord + Send + Sync,
        V: Clone + ConvertFromUsize + Send + Sync + Unpin,
        C: BenchmarkOperation<K, V, RandomState> + 'static + Send + Sync,
    >(
        num_threads: usize,
        start_index: usize,
        map: Arc<C>,
        workload: Workload,
    ) -> (Duration, usize) {
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let total_num_operations = Arc::new(AtomicUsize::new(0));
        let mut thread_handles = Vec::with_capacity(num_threads);
        for thread_id in 0..num_threads {
            let map_copied = map.clone();
            let barrier_copied = barrier.clone();
            let total_num_operations_copied = total_num_operations.clone();
            let workload_copied = workload.clone();
            thread_handles.push(thread::spawn(move || {
                let mut num_operations = 0;
                let per_op_workload_size = workload_copied.max_per_op_size();
                let per_thread_workload_size = workload_copied.size * per_op_workload_size;
                barrier_copied.wait();
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
                        let result = map_copied.insert_test(K::convert(local_index), V::convert(i));
                        assert!(result || workload_copied.has_remote_op());
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.insert_remote {
                        let remote_index = remote_thread_id * per_thread_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        map_copied.insert_test(K::convert(remote_index), V::convert(i));
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.read_local {
                        let local_index = thread_id * per_thread_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        let result = map_copied.read_test(&K::convert(local_index));
                        assert!(result || workload_copied.has_remote_op());
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.read_remote {
                        let remote_index = remote_thread_id * per_thread_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        map_copied.read_test(&K::convert(remote_index));
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.remove_local {
                        let local_index = thread_id * per_thread_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        let result = map_copied.remove_test(&K::convert(local_index));
                        assert!(result || workload_copied.has_remote_op());
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.remove_remote {
                        let remote_index = remote_thread_id * per_thread_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        map_copied.remove_test(&K::convert(remote_index));
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

    #[test]
    fn hashmap_benchmark() {
        let num_threads_vector = vec![1, 4, 16];
        for num_threads in num_threads_vector {
            let hashmap: Arc<HashMap<usize, usize, RandomState>> = Arc::new(Default::default());
            let workload_size = 262144;

            // 1. insert-local
            let insert = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashmap.clone(), insert.clone());
            println!(
                "insert-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashmap.len(), workload_size * num_threads);

            // 2. read-local
            let read = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                read_local: 1,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashmap.clone(), read.clone());
            println!(
                "read-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );

            // 3. remove-local
            let remove = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 1,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashmap.clone(), remove.clone());
            println!(
                "remove-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashmap.len(), 0);

            if num_threads < 2 {
                continue;
            }

            // 4. insert-local-remote
            let insert = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 1,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashmap.clone(), insert.clone());
            println!(
                "insert-local-remote: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashmap.len(), workload_size * num_threads);

            // 5. mixed
            let mixed = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 1,
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
                "mixed: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashmap.len(), workload_size * num_threads);

            // 6. remove-local-remote
            let remove = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 1,
                remove_remote: 1,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashmap.clone(), remove.clone());
            println!(
                "remove-local-remote: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            println!("final capacity: {}", hashmap.capacity());
            assert_eq!(hashmap.len(), 0);
        }
    }

    #[test]
    fn hashindex_benchmark() {
        let num_threads_vector = vec![1, 4, 16];
        for num_threads in num_threads_vector {
            let hashindex: Arc<HashIndex<usize, usize, RandomState>> = Arc::new(Default::default());
            let workload_size = 262144;

            // 1. insert-local
            let insert = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashindex.clone(), insert.clone());
            println!(
                "insert-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashindex.len(), workload_size * num_threads);

            // 2. read-local
            let read = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                read_local: 1,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashindex.clone(), read.clone());
            println!(
                "read-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );

            // 3. remove-local
            let remove = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 1,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashindex.clone(), remove.clone());
            println!(
                "remove-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashindex.len(), 0);

            if num_threads < 2 {
                continue;
            }

            // 4. insert-local-remote
            let insert = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 1,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashindex.clone(), insert.clone());
            println!(
                "insert-local-remote: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashindex.len(), workload_size * num_threads);

            // 5. mixed
            let mixed = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 1,
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
                "mixed: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(hashindex.len(), workload_size * num_threads);

            // 6. remove-local-remote
            let remove = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 1,
                remove_remote: 1,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, hashindex.clone(), remove.clone());
            println!(
                "remove-local-remote: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            println!("final capacity: {}", hashindex.capacity());
            assert_eq!(hashindex.len(), 0);
        }
    }

    #[test]
    fn treeindex_benchmark() {
        let num_threads_vector = vec![1, 4, 16];
        for num_threads in num_threads_vector {
            let treeindex: Arc<TreeIndex<String, String>> = Arc::new(Default::default());
            let workload_size = 65536;

            // 1. insert-local
            let insert = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, treeindex.clone(), insert.clone());
            println!(
                "insert-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            let (len, depth) = (treeindex.len(), treeindex.depth());
            println!("after insert-local: num_elements {}, depth {}", len, depth);
            assert_eq!(len, workload_size * num_threads);

            // 2. read-local
            let read = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                read_local: 1,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, treeindex.clone(), read.clone());
            println!(
                "read-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );

            // 3. remove-local
            let remove = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 1,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, treeindex.clone(), remove.clone());
            println!(
                "remove-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            let (len, depth) = (treeindex.len(), treeindex.depth());
            println!("after remove-local: num_elements {}, depth {}", len, depth);
            assert_eq!(len, 0);

            // 4. insert-local-remote
            let insert = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 1,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, treeindex.clone(), insert.clone());
            println!(
                "insert-local-remote: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            let (len, depth) = (treeindex.len(), treeindex.depth());
            println!(
                "after insert-local-remote: num_elements {}, depth {}",
                len, depth
            );

            // 5. mixed
            let mixed = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 1,
                read_local: 1,
                read_remote: 1,
                remove_local: 1,
                remove_remote: 1,
            };
            let (duration, total_num_operations) =
                perform(num_threads, len, treeindex.clone(), mixed.clone());
            println!(
                "mixed: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            let (len, depth) = (treeindex.len(), treeindex.depth());
            println!("after mixed: num_elements {}, depth {}", len, depth);

            // 6. remove-local-remote
            let remove = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 1,
                remove_remote: 1,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, treeindex.clone(), remove.clone());
            println!(
                "remove-local-remote: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            let (len, depth) = (treeindex.len(), treeindex.depth());
            println!(
                "after remove-local-remote: num_elements {}, depth {}",
                len, depth
            );
        }
    }
}
