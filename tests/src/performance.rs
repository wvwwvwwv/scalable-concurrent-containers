use scc::HashMap;

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::hash_map::RandomState;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};
    use std::sync::atomic::Ordering::Relaxed;

    #[test]
    fn basic_latency() {
        let hashmap: HashMap<u64, u32, RandomState> = HashMap::new(RandomState::new(), Some(10));
    }

    fn workload<F: FnMut() -> usize>(num_threads: usize, f: F) -> (Duration, usize) {
        let barrier = Arc::new(Barrier::new(num_threads));
        let start_time = Arc::new(Mutex::new(Instant::now()));
        let end_time = Arc::new(Mutex::new(Instant::now()));
        let stop_measurement = Arc::new(AtomicBool::new(false));
        let total_num_operations = Arc::new(AtomicUsize::new(0));
        let mut thread_handles = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            let barrier_copied = barrier.clone();
            let start_time_copied = start_time.clone();
            let end_time_copied = end_time.clone();
            let stop_measurement_copied = stop_measurement.clone();
            let total_num_operations_copied = total_num_operations.clone();
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
        (end_time.duration_since(start_time), total_num_operations.load(Relaxed))
    }

    #[test]
    fn hashmap_benchmark() {
        let num_threads_vector = vec![4, 16];
        for num_threads in num_threads_vector {
            let (duration, total_num_operations) = workload(num_threads, || 0);
            println!("{:?}, {}", duration, total_num_operations);
        }
    }
}
