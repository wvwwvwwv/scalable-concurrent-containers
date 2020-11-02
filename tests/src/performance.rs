use scc::HashMap;

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hash, Hasher};
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn basic_latency() {
        let hashmap: HashMap<u64, u64, RandomState> = HashMap::new(RandomState::new(), None);
    }

    struct Workload {
        insert: u8,
        update: u8,
        read: u8,
        remove: u8,
    }

    trait HashMapOperation<K: Clone + Eq + Hash + Sync, V: Sync + Unpin, H: BuildHasher> {
        fn insert_test(&self, k: &K, v: V) -> bool;
    }

    impl<K: Clone + Eq + Hash + Sync, V: Sync + Unpin, H: BuildHasher> HashMapOperation<K, V, H> for HashMap<K, V, H> {
        fn insert_test(&self, k: &K, v: V) -> bool {
            self.insert(k, v).is_ok()
        }
    }

    fn perform<H>(num_threads: usize, h: &H, workload: &Workload) -> (Duration, usize) {
        let barrier = Arc::new(Barrier::new(num_threads));
        let start_time = Arc::new(Mutex::new(Instant::now()));
        let end_time = Arc::new(Mutex::new(Instant::now()));
        let stop_measurement = Arc::new(AtomicBool::new(false));
        let total_num_operations = Arc::new(AtomicUsize::new(0));
        let mut thread_handles = Vec::with_capacity(num_threads);
        for thread_id in 0..num_threads {
            let barrier_copied = barrier.clone();
            let start_time_copied = start_time.clone();
            let end_time_copied = end_time.clone();
            let stop_measurement_copied = stop_measurement.clone();
            let total_num_operations_copied = total_num_operations.clone();
            let workload_copied = workload.clone();
            thread_handles.push(thread::spawn(move || {
                if barrier_copied.wait().is_leader() {
                    let mut start_time = start_time_copied.lock().unwrap();
                    *start_time = Instant::now();
                }
                if !stop_measurement_copied.swap(true, Relaxed) {
                    let mut end_time = end_time_copied.lock().unwrap();
                    *end_time = Instant::now();
                }
            }));
        }
        for handle in thread_handles {
            handle.join().unwrap();
        }
        let start_time = *start_time.lock().unwrap();
        let end_time = *end_time.lock().unwrap();
        (
            end_time.saturating_duration_since(start_time),
            total_num_operations.load(Relaxed),
        )
    }

    #[test]
    fn hashmap_benchmark() {
        let hashmap: HashMap<u64, u64, RandomState> = HashMap::new(RandomState::new(), None);
        let num_threads_vector = vec![4, 16];
        for num_threads in num_threads_vector {
            let workload = Workload {
                insert: 1,
                update: 1,
                read: 1,
                remove: 1,
            };
            let (duration, total_num_operations) = perform(num_threads, &hashmap, &workload);
            println!("{:?}, {}", duration, total_num_operations);
        }
    }
}
