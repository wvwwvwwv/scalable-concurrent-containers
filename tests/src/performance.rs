#[cfg(test)]
mod test {
    use scc::HashMap;
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hash};
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn basic_latency() {
        let hashmap: HashMap<u64, u64, RandomState> = HashMap::new(RandomState::new(), None);
    }

    #[derive(Clone)]
    struct Workload {
        size: usize,
        overlap: bool,
        insert: u8,
        update: u8,
        read: u8,
        remove: u8,
    }

    impl Workload {
        pub fn subop_size(&self) -> usize {
            self.insert.max(self.update.max(self.read.max(self.remove))) as usize
        }
    }

    trait HashMapOperation<K: Clone + Eq + Hash + Sync, V: Sync + Unpin, H: BuildHasher> {
        fn insert_test(&self, k: K, v: V) -> bool;
        fn update_test(&self, k: K, v: V);
        fn read_test(&self, k: K) -> bool;
        fn remove_test(&self, k: K) -> bool;
    }

    impl<K: Clone + Eq + Hash + Sync, V: Sync + Unpin, H: BuildHasher> HashMapOperation<K, V, H>
        for HashMap<K, V, H>
    {
        fn insert_test(&self, k: K, v: V) -> bool {
            self.insert(k, v).is_ok()
        }
        fn update_test(&self, k: K, v: V) {
            self.upsert(k, v);
        }
        fn read_test(&self, k: K) -> bool {
            self.read(k, |_, _| ()).is_some()
        }
        fn remove_test(&self, k: K) -> bool {
            self.remove(k)
        }
    }

    fn perform<M: HashMapOperation<usize, usize, RandomState> + 'static + Send + Sync>(
        num_threads: usize,
        map: Arc<M>,
        workload: Workload,
    ) -> (Duration, usize) {
        let barrier = Arc::new(Barrier::new(num_threads));
        let start_time = Arc::new(Mutex::new(Instant::now()));
        let total_num_operations = Arc::new(AtomicUsize::new(0));
        let mut thread_handles = Vec::with_capacity(num_threads);
        for thread_id in 0..num_threads {
            let map_copied = map.clone();
            let barrier_copied = barrier.clone();
            let start_time_copied = start_time.clone();
            let total_num_operations_copied = total_num_operations.clone();
            let workload_copied = workload.clone();
            thread_handles.push(thread::spawn(move || {
                if barrier_copied.wait().is_leader() {
                    let mut start_time = start_time_copied.lock().unwrap();
                    *start_time = Instant::now();
                }
                let mut num_operations = 0;
                let workload_size = workload_copied.size * workload_copied.subop_size();
                let start_index = if workload_copied.overlap {
                    thread_id * (workload_size / 2)
                } else {
                    thread_id * workload_size
                };
                for i in start_index..(start_index + workload_size) {
                    for j in 0..workload_copied.insert {
                        assert!(map_copied.insert_test(i + j as usize, i));
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.update {
                        map_copied.update_test(i + j as usize, i);
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.read {
                        assert!(map_copied.read_test(i + j as usize));
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.remove {
                        assert!(map_copied.remove_test(i + j as usize));
                        num_operations += 1;
                    }
                }
                total_num_operations_copied.fetch_add(num_operations, Relaxed);
            }));
        }
        for handle in thread_handles {
            handle.join().unwrap();
        }
        let start_time = *start_time.lock().unwrap();
        let end_time = Instant::now();
        (
            end_time.saturating_duration_since(start_time),
            total_num_operations.load(Relaxed),
        )
    }

    #[test]
    fn hashmap_benchmark() {
        let num_threads_vector = vec![1, 2, 4, 8, 16];

        for num_threads in num_threads_vector {
            let hashmap: Arc<HashMap<usize, usize, RandomState>> =
                Arc::new(HashMap::new(RandomState::new(), None));
            // 1. insert
            let insert = Workload {
                size: 1048576,
                overlap: false,
                insert: 1,
                update: 0,
                read: 0,
                remove: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, hashmap.clone(), insert.clone());
            println!(
                "insert: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            let statistics = hashmap.statistics();
            println!("after insert: {}", statistics);
            // 2. read
            let read = Workload {
                size: 1048576,
                overlap: false,
                insert: 0,
                update: 0,
                read: 1,
                remove: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, hashmap.clone(), read.clone());
            println!(
                "read: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            let statistics = hashmap.statistics();
            println!("after insert: {}", statistics);
            // 3. remove
            let remove = Workload {
                size: 1048576,
                overlap: false,
                insert: 0,
                update: 0,
                read: 0,
                remove: 1,
            };
            let (duration, total_num_operations) =
                perform(num_threads, hashmap.clone(), remove.clone());
            println!(
                "remove: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            let statistics = hashmap.statistics();
            println!("after remove: {}", statistics);
        }
    }
}
